--!strict
-- SyncManager.luau
-- High-level owner of a server's channels. Drives the shared publish/
-- combine/read loop, maintains the merged cache and the userId -> serverId
-- directory, and exposes events so consumers react to changes instead of
-- polling raw data.
--
-- Per-tick order: publish this server's chunk -> if we hold the master
-- lease, combine everyone's chunks into shards -> read the current shards.
-- All three run on ONE shared interval. Whether we currently hold the
-- master lease is decided by MasterElection on its OWN separate timer -
-- fully decoupled from this loop.
--
-- This file sits directly inside the SyncSystem folder (a plain Folder,
-- not a special init module) alongside SyncBridge.luau and the Config/
-- Sync/Validators subfolders, so requires below go through script.Parent.

local Players = game:GetService("Players")
local HttpService = game:GetService("HttpService")
local MemoryStoreService = game:GetService("MemoryStoreService")

local SyncChannel = require(script.Parent.Sync.SyncChannel)
local SyncEngine = require(script.Parent.Sync.SyncEngine)
local RegistryStore = require(script.Parent.Sync.RegistryStore)
local MasterElection = require(script.Parent.Sync.MasterElection)
local Signal = require(script.Parent.Sync.Signal)
local SyncEvents = require(script.Parent.Config.SyncEvents)

local SyncManager = {}
SyncManager.__index = SyncManager
SyncManager.Events = SyncEvents

local DIRECTORY_KEY = "__directory"
local CHUNK_STORE_NAME = "SyncManager_Chunks_v1"
local SHARD_STORE_NAME = "SyncManager_Shards_v1"
local REGISTRY_HASHMAP_NAME = "SyncManager_Registry_v1"
local FREE_QUEUE_NAME = "SyncManager_FreeSlots_v1"
local MASTER_HASHMAP_NAME = "SyncManager_Master_v1"
local MAX_JSON_SECTION_BYTES = 32000

export type SyncManagerConfig = {
	interval: number?,
	ttl: number?,
	autoTriggerOnPlayerEvents: boolean?,
	autoTriggerDebounce: number?,
	leaseDuration: number?,
	renewInterval: number?,
	sweepEveryNCycles: number?,
}

export type SyncManager = typeof(setmetatable(
	{} :: {
		_serverId: string,
		_interval: number,
		_ttl: number,
		_autoTrigger: boolean,
		_autoTriggerDebounce: number,
		_sweepEveryNCycles: number,
		_cycleCount: number,
		_channels: { [string]: SyncChannel.SyncChannel },
		_cache: { [string]: { [string]: any } },
		_directory: { [string]: string },
		_engine: SyncEngine.SyncEngine,
		_registry: RegistryStore.RegistryStore,
		_election: MasterElection.MasterElection,
		_myIndex: number?,
		_running: boolean,
		_playerAddedConn: RBXScriptConnection?,
		_playerRemovingConn: RBXScriptConnection?,
		_autoTriggerPending: boolean,
		Updated: Signal.Signal,
		ServerAdded: Signal.Signal,
		ServerRemoved: Signal.Signal,
		PublishFailed: Signal.Signal,
		ReadFailed: Signal.Signal,
	},
	SyncManager
))

-- Any ModuleScript dropped in the Validators folder is picked up here,
-- same mechanic as AbilitySystem's Validators folder.
for _, child in script.Parent.Validators:GetChildren() do
	if child:IsA("ModuleScript") then
		local ok, impl = pcall(require, child)
		if ok and type(impl) == "table" and impl.type then
			SyncChannel._registerValidatorImpl(impl)
		else
			warn(("[SyncManager] Failed to auto-register validator '%s'"):format(child.Name))
		end
	end
end

function SyncManager.registerValidator(validatorType: string, impl: SyncChannel.ValidatorImpl)
	impl.type = validatorType
	SyncChannel._registerValidatorImpl(impl)
end

-- game.JobId is "" when running in Studio (only populated on real
-- published servers), so fall back to a generated id for local testing.
local function resolveServerId(): string
	if game.JobId ~= "" then
		return game.JobId
	end
	return "STUDIO_" .. HttpService:GenerateGUID(false)
end

local function deepEqual(a: any, b: any): boolean
	if a == b then
		return true
	end
	if type(a) ~= "table" or type(b) ~= "table" then
		return false
	end
	for k, v in a do
		if not deepEqual(v, b[k]) then
			return false
		end
	end
	for k in b do
		if a[k] == nil then
			return false
		end
	end
	return true
end

-- ===== Construction =====

function SyncManager.new(config: SyncManagerConfig?): SyncManager
	config = config or {}

	local interval = config.interval or 30
	local self = setmetatable({
		_serverId = resolveServerId(),
		_interval = interval,
		_ttl = config.ttl or interval * 3,
		_autoTrigger = config.autoTriggerOnPlayerEvents or false,
		_autoTriggerDebounce = config.autoTriggerDebounce or 1,
		_sweepEveryNCycles = config.sweepEveryNCycles or math.max(1, math.floor(600 / interval)), -- ~10 min default
		_cycleCount = 0,
		_channels = {},
		_cache = {},
		_directory = {},
		_myIndex = nil,
		_running = false,
		_playerAddedConn = nil,
		_playerRemovingConn = nil,
		_autoTriggerPending = false,
		Updated = Signal.new(),
		ServerAdded = Signal.new(),
		ServerRemoved = Signal.new(),
		PublishFailed = Signal.new(),
		ReadFailed = Signal.new(),
	}, SyncManager) :: any

	self._engine = SyncEngine.new(CHUNK_STORE_NAME, SHARD_STORE_NAME, self)

	local registryHashMap = MemoryStoreService:GetHashMap(REGISTRY_HASHMAP_NAME)
	local freeQueue = MemoryStoreService:GetQueue(FREE_QUEUE_NAME)
	self._registry = RegistryStore.new(registryHashMap, freeQueue, "nextIndex", self._ttl * 8, interval * 4)

	local masterHashMap = MemoryStoreService:GetHashMap(MASTER_HASHMAP_NAME)
	self._election = MasterElection.new(masterHashMap, self._serverId, {
		leaseDuration = config.leaseDuration or 15,
		renewInterval = config.renewInterval or 5,
	})

	return self
end

function SyncManager._context(self: SyncManager)
	return { manager = self, localServerId = self._serverId }
end

-- ===== Channel management =====

function SyncManager.give(self: SyncManager, channel: SyncChannel.SyncChannel)
	assert(self._channels[channel.name] == nil, ("[SyncManager] Channel '%s' already exists"):format(channel.name))
	self._channels[channel.name] = channel
end

function SyncManager.take(self: SyncManager, channelName: string)
	self._channels[channelName] = nil
end

function SyncManager.get(self: SyncManager, channelName: string): SyncChannel.SyncChannel?
	return self._channels[channelName]
end

-- ===== Directory =====

function SyncManager._processDirectory(self: SyncManager, serverId: string, newList: { number }, oldList: { number }?)
	local newSet: { [number]: boolean } = {}
	for _, userId in newList do
		newSet[userId] = true
		self._directory[tostring(userId)] = serverId
	end

	if oldList then
		for _, userId in oldList do
			if not newSet[userId] and self._directory[tostring(userId)] == serverId then
				self._directory[tostring(userId)] = nil
			end
		end
	end
end

function SyncManager.getServerForPlayer(self: SyncManager, userId: number): string?
	return self._directory[tostring(userId)]
end

-- ===== Cache access =====

function SyncManager.getCache(self: SyncManager, channelName: string, serverId: string?): any
	if serverId then
		local data = self._cache[serverId]
		return data and data[channelName]
	end

	local out = {}
	for sid, data in self._cache do
		if data[channelName] ~= nil then
			out[sid] = data[channelName]
		end
	end
	return out
end

-- ===== SyncEngine output interface (called by SyncEngine on changes) =====

function SyncManager.onReceive(self: SyncManager, serverId: string, jsonData: { [string]: any }, binaryBlobs: { [string]: buffer })
	-- Unpack binary channels using their own layout before merging into
	-- one flat table for diffing. Engine hands us raw bytes; only we know
	-- (via each channel's binaryLayout) how to interpret them.
	local data = table.clone(jsonData)
	for channelName, blobBuf in binaryBlobs do
		local channel = self._channels[channelName]
		if channel and channel.binaryLayout then
			local ok, records = pcall(SyncChannel.unpackRecords, channel.binaryLayout, blobBuf)
			if ok then
				data[channelName] = records
			else
				warn(("[SyncManager] Failed to unpack binary channel '%s' for %s"):format(channelName, serverId))
			end
		end
	end

	local old = self._cache[serverId]
	self._cache[serverId] = data
	local context = self:_context()

	for key, value in data do
		if key == DIRECTORY_KEY then
			self:_processDirectory(serverId, value, old and old[DIRECTORY_KEY])
			continue
		end

		local oldValue = old and old[key]
		local changed = oldValue == nil or not deepEqual(oldValue, value)
		if not changed then
			continue
		end

		if oldValue == nil then
			self.ServerAdded:Fire(key, serverId, value)
		end
		self.Updated:Fire(key, serverId, value)

		local channel = self._channels[key]
		if channel and channel.onReceive then
			pcall(channel.onReceive, channel, context, serverId, value)
		end
	end

	-- Channels present last time but missing now (dropped for size, or
	-- that provider no longer returns them) count as removed for this server.
	if old then
		for key in old do
			if key ~= DIRECTORY_KEY and data[key] == nil then
				self.ServerRemoved:Fire(key, serverId)
				local channel = self._channels[key]
				if channel and channel.onExpire then
					pcall(channel.onExpire, channel, context, serverId)
				end
			end
		end
	end
end

function SyncManager.onExpire(self: SyncManager, serverId: string)
	local old = self._cache[serverId]
	self._cache[serverId] = nil

	if not old then
		return
	end

	local context = self:_context()
	for key in old do
		if key ~= DIRECTORY_KEY then
			self.ServerRemoved:Fire(key, serverId)
			local channel = self._channels[key]
			if channel and channel.onExpire then
				pcall(channel.onExpire, channel, context, serverId)
			end
		end
	end

	self:_processDirectory(serverId, {}, old[DIRECTORY_KEY])
end

-- ===== Publish / master-combine / read =====

function SyncManager.triggerPublish(self: SyncManager)
	local context = self:_context()
	local jsonChannels: { [string]: any } = {}
	local binaryChannels: { [string]: buffer } = {}

	for name, channel in self._channels do
		local value = channel:resolve(context)
		if value == nil then
			continue
		end

		if channel.binaryLayout then
			local ok, packed = pcall(SyncChannel.packRecords, channel.binaryLayout, value)
			if ok then
				binaryChannels[name] = packed
			else
				warn(("[SyncManager] Failed to pack binary channel '%s': %s"):format(name, tostring(packed)))
				self.PublishFailed:Fire(name, "Binary pack failed")
			end
		else
			jsonChannels[name] = value
		end
	end

	local playerIds = {}
	for _, player in Players:GetPlayers() do
		table.insert(playerIds, player.UserId)
	end
	jsonChannels[DIRECTORY_KEY] = playerIds

	-- Drop JSON channels (arbitrary order) until the json section fits,
	-- rather than failing the whole server's publish over one channel.
	-- Binary channels are exempt - their size is already fixed/known via
	-- the layout, so there's nothing unbounded to drop there.
	while true do
		local ok, encoded = pcall(HttpService.JSONEncode, HttpService, jsonChannels)
		if ok and #encoded <= MAX_JSON_SECTION_BYTES then
			break
		end

		local dropKey: string? = nil
		for k in jsonChannels do
			if k ~= DIRECTORY_KEY then
				dropKey = k
				break
			end
		end

		if not dropKey then
			break -- nothing left to drop; publishChunk below will report the failure
		end

		warn(("[SyncManager] Dropping channel '%s' this tick - json section too large or unencodable"):format(dropKey))
		self.PublishFailed:Fire(dropKey, "Payload exceeded size limit or was invalid; channel dropped")
		jsonChannels[dropKey] = nil
	end

	if not self._myIndex then
		return -- not registered with the registry yet; skip this tick's write
	end

	self._registry:Heartbeat(self._myIndex, self._serverId)

	local success, err = self._engine:publishChunk(self._serverId, self._myIndex, jsonChannels, binaryChannels, self._ttl)
	if not success then
		self.PublishFailed:Fire("*", err)
		return
	end

	-- Reflect our own publish locally immediately, so getCache/getServerForPlayer
	-- are correct on this server without waiting for the next shard read.
	self:onReceive(self._serverId, jsonChannels, binaryChannels)
end

function SyncManager.triggerRead(self: SyncManager)
	local ok = self._engine:readShards()
	if not ok then
		self.ReadFailed:Fire("Failed to read/decode combined shards")
	end
end

-- Runs on every server every tick, but only does anything if we currently
-- hold the master lease (checked via MasterElection, which renews on its
-- own independent timer - see that module for details).
function SyncManager._runMasterCycleIfMaster(self: SyncManager)
	if not self._election.isMaster then
		return
	end

	local maxIndex = self._registry:GetMaxIndex()
	if maxIndex == 0 then
		return
	end

	self._cycleCount += 1
	if self._cycleCount % self._sweepEveryNCycles == 0 then
		self._registry:SweepDeadSlots(maxIndex)
	end

	local ok, err = self._engine:runMasterCombineCycle(maxIndex, self._ttl)
	if not ok then
		self.PublishFailed:Fire("*_master_combine", err)
	end
end

-- publish -> (if master) combine -> read, in that order, on one tick.
function SyncManager.trigger(self: SyncManager)
	self:triggerPublish()
	self:_runMasterCycleIfMaster()
	self:triggerRead()
end

-- Targeted refresh of a single server's own chunk (e.g. Bridge-triggered),
-- bypassing the wait for the next master combine cycle. Cheap: 1 request.
function SyncManager.refreshServer(self: SyncManager, serverId: string)
	self._engine:readOneChunk(serverId)
end

-- ===== Lifecycle =====

function SyncManager.start(self: SyncManager)
	if self._running then
		return
	end
	self._running = true

	local ok, index = self._registry:Push(self._serverId)
	if ok then
		self._myIndex = index
	else
		warn("[SyncManager] Failed to join registry on start")
	end

	self._election:start()

	task.spawn(function()
    while self._running do
        self:triggerPublish()
        self:triggerRead()
        task.wait(self._interval)
    end
end)

task.spawn(function()
    local combining = false
    while self._running do
        if self._election.isMaster and not combining then
            combining = true
            task.spawn(function()
                self:_runMasterCycleIfMaster()
                combining = false
            end)
        end
        task.wait(self._interval)
    end
end)


	if self._autoTrigger then
		local function schedule()
			if self._autoTriggerPending then
				return
			end
			self._autoTriggerPending = true
			task.delay(self._autoTriggerDebounce, function()
				self._autoTriggerPending = false
				if self._running then
					self:triggerPublish()
				end
			end)
		end

		self._playerAddedConn = Players.PlayerAdded:Connect(schedule)
		self._playerRemovingConn = Players.PlayerRemoving:Connect(schedule)
	end

	game:BindToClose(function()
		self:stop()
	end)
end

function SyncManager.stop(self: SyncManager)
	self._running = false
	self._election:stop()

	if self._myIndex then
		self._registry:RemoveAt(self._myIndex)
	end

	if self._playerAddedConn then
		self._playerAddedConn:Disconnect()
		self._playerAddedConn = nil
	end
	if self._playerRemovingConn then
		self._playerRemovingConn:Disconnect()
		self._playerRemovingConn = nil
	end
end

return SyncManager
