--!strict
-- SyncManager/init.lua
-- High-level owner of a server's channels. Drives the shared publish/read
-- loop, maintains the merged cache and the userId -> serverId directory,
-- and exposes events so consumers react to changes instead of polling.

local Players = game:GetService("Players")
local HttpService = game:GetService("HttpService")

local SyncChannel = require(script.Parent.Sync.SyncChannel)
local SyncEngine = require(script.Parent.Sync.SyncEngine)
local SyncEvents = require(script.Parent.Config.SyncEvents)
local Signal = require(script.Parent.Sync.Signal)

local SyncManager = {}
SyncManager.__index = SyncManager
SyncManager.Events = SyncEvents

-- Reserved sub-key inside every server's combined payload, used for the
-- auto-maintained player directory. Not a user-defined channel.
local DIRECTORY_KEY = "__directory"

local STORE_NAME = "SyncManager_v1"
local MAX_ITEM_BYTES = 32000 -- stay under the 32,768 byte hard cap, leave margin

export type SyncManagerConfig = {
	interval: number?,
	ttl: number?,
	autoTriggerOnPlayerEvents: boolean?,
	autoTriggerDebounce: number?,
}

export type SyncManager = typeof(setmetatable(
	{} :: {
		_serverId: string,
		_interval: number,
		_ttl: number,
		_autoTrigger: boolean,
		_autoTriggerDebounce: number,
		_channels: { [string]: SyncChannel.SyncChannel },
		_cache: { [string]: { [string]: any } },
		_directory: { [string]: string },
		_engine: SyncEngine.SyncEngine,
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

-- ===== Built-in + folder-based validator auto-registration =====
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

-- ===== Helpers =====

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

	local self = setmetatable({
		_serverId = resolveServerId(),
		_interval = config.interval or 30,
		_ttl = config.ttl or (config.interval or 30) * 3,
		_autoTrigger = config.autoTriggerOnPlayerEvents or false,
		_autoTriggerDebounce = config.autoTriggerDebounce or 1,
		_channels = {},
		_cache = {},
		_directory = {},
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

	self._engine = SyncEngine.new(STORE_NAME, self)

	return self
end

function SyncManager._context(self: SyncManager)
	return {
		manager = self,
		localServerId = self._serverId,
	}
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

function SyncManager.onReceive(self: SyncManager, serverId: string, data: { [string]: any })
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

	-- Channels present last time but missing now (e.g. dropped for size,
	-- or that provider no longer returns them) count as removed for this server.
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

-- ===== Publish / read =====

function SyncManager.triggerPublish(self: SyncManager)
	local context = self:_context()
	local combined: { [string]: any } = {}

	for name, channel in self._channels do
		local value = channel:resolve(context)
		if value ~= nil then
			combined[name] = value
		end
	end

	local playerIds = {}
	for _, player in Players:GetPlayers() do
		table.insert(playerIds, player.UserId)
	end
	combined[DIRECTORY_KEY] = playerIds

	-- Drop channels (largest concern first would need a priority field;
	-- for now drop in arbitrary order) until the combined payload fits,
	-- rather than failing the whole server's publish over one channel.
	while true do
		local ok, encoded = pcall(HttpService.JSONEncode, HttpService, combined)
		if ok and #encoded <= MAX_ITEM_BYTES then
			break
		end

		local dropKey: string? = nil
		for k in combined do
			if k ~= DIRECTORY_KEY then
				dropKey = k
				break
			end
		end

		if not dropKey then
			break -- nothing left to drop; publish() below will report the failure
		end

		warn(("[SyncManager] Dropping channel '%s' this tick - combined payload too large or unencodable"):format(dropKey))
		self.PublishFailed:Fire(dropKey, "Payload exceeded size limit or was invalid; channel dropped")
		combined[dropKey] = nil
	end

	local success, err = self._engine:publish(self._serverId, combined, self._ttl)
	if not success then
		self.PublishFailed:Fire("*", err)
		return
	end

	-- Reflect our own publish locally immediately, so getCache/getServerForPlayer
	-- are correct on this server without waiting for the next read pass.
	self:onReceive(self._serverId, combined)
end

function SyncManager.triggerRead(self: SyncManager)
	local result, err = self._engine:readAll()
	if result == nil then
		self.ReadFailed:Fire(err)
	end
end

function SyncManager.trigger(self: SyncManager)
	self:triggerPublish()
	self:triggerRead()
end

function SyncManager.refreshServer(self: SyncManager, serverId: string)
	self._engine:read(serverId)
end

-- ===== Lifecycle =====

function SyncManager.start(self: SyncManager)
	if self._running then
		return
	end
	self._running = true

	task.spawn(function()
		while self._running do
			self:trigger()
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
end

function SyncManager.stop(self: SyncManager)
	self._running = false

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
