--!strict
-- SyncEngine.luau
-- Chunk store stays on MemoryStore (needs sorted range reads by registry
-- index). Combined/shard store moves to DataStore: one key, no sharding,
-- since DataStore's per-value limit (~4MB) comfortably covers realistic
-- combined payload sizes. Cache explicitly disabled on every read so data
-- is always live, never the ~4s stale cached copy.

local MemoryStoreService = game:GetService("MemoryStoreService")
local DataStoreService = game:GetService("DataStoreService")
local HttpService = game:GetService("HttpService")

local SyncEngine = {}
SyncEngine.__index = SyncEngine

export type Output = {
	onReceive: (self: any, serverId: string, jsonData: { [string]: any }, binaryBlobs: { [string]: buffer }) -> (),
	onExpire: (self: any, serverId: string) -> (),
}

export type SyncEngine = typeof(setmetatable(
	{} :: {
		_chunkMap: any,
		_combinedStore: DataStore,
		_combinedKey: string,
		_output: Output,
		_knownServerIds: { [string]: boolean },
	},
	SyncEngine
))

local RANGE_WIDTH = 200
local COMBINED_KEY = "combined"

local NO_CACHE_OPTIONS = Instance.new("DataStoreGetOptions")
NO_CACHE_OPTIONS.UseCache = false

function SyncEngine.new(chunkStoreName: string, combinedStoreName: string, output: Output): SyncEngine
	return setmetatable({
		_chunkMap = MemoryStoreService:GetSortedMap(chunkStoreName),
		_combinedStore = DataStoreService:GetDataStore(combinedStoreName),
		_combinedKey = COMBINED_KEY,
		_output = output,
		_knownServerIds = {},
	}, SyncEngine) :: any
end

-- ===== generic pack/unpack: {jsonPart: table, blobs: {[key]: buffer}} <-> buffer =====
-- Unchanged from before - still used for CHUNK store packing (MemoryStore
-- accepts buffer natively). The combined/DataStore boundary additionally
-- stringifies this buffer, since DataStore only accepts JSON-safe types.

local function packWithBlobs(jsonPart: any, blobs: { [string]: buffer }): buffer
	local jsonOk, jsonStr = pcall(HttpService.JSONEncode, HttpService, jsonPart)
	if not jsonOk then
		jsonStr = "{}"
	end

	local blobList = {}
	local totalSize = 4 + #jsonStr + 4
	for key, buf in blobs do
		table.insert(blobList, { key = key, buf = buf })
		totalSize += 4 + #key + 4 + buffer.len(buf)
	end

	local out = buffer.create(totalSize)
	local offset = 0

	buffer.writeu32(out, offset, #jsonStr)
	offset += 4
	buffer.writestring(out, offset, jsonStr)
	offset += #jsonStr

	buffer.writeu32(out, offset, #blobList)
	offset += 4

	for _, entry in blobList do
		buffer.writeu32(out, offset, #entry.key)
		offset += 4
		buffer.writestring(out, offset, entry.key)
		offset += #entry.key

		local len = buffer.len(entry.buf)
		buffer.writeu32(out, offset, len)
		offset += 4
		buffer.copy(out, offset, entry.buf, 0, len)
		offset += len
	end

	return out
end

local function unpackWithBlobs(buf: buffer): (any, { [string]: buffer })
	local offset = 0

	local jsonLen = buffer.readu32(buf, offset)
	offset += 4
	local jsonStr = buffer.readstring(buf, offset, jsonLen)
	offset += jsonLen

	local jsonOk, jsonPart = pcall(HttpService.JSONDecode, HttpService, jsonStr)
	if not jsonOk then
		jsonPart = {}
	end

	local blobCount = buffer.readu32(buf, offset)
	offset += 4

	local blobs: { [string]: buffer } = {}
	for _ = 1, blobCount do
		local keyLen = buffer.readu32(buf, offset)
		offset += 4
		local key = buffer.readstring(buf, offset, keyLen)
		offset += keyLen

		local dataLen = buffer.readu32(buf, offset)
		offset += 4
		local dataBuf = buffer.create(dataLen)
		buffer.copy(dataBuf, 0, buf, offset, dataLen)
		offset += dataLen

		blobs[key] = dataBuf
	end

	return jsonPart, blobs
end

-- ===== Every server: write its own chunk (unchanged - still MemoryStore) =====

function SyncEngine.publishChunk(
	self: SyncEngine,
	serverId: string,
	sortKeyIndex: number,
	jsonChannels: { [string]: any },
	binaryChannels: { [string]: buffer },
	ttl: number
): (boolean, string?)
	local ok, packedBuf = pcall(packWithBlobs, jsonChannels, binaryChannels)
	if not ok then
		return false, ("Pack failed: %s"):format(tostring(packedBuf))
	end

	local writeOk, err = pcall(function()
		self._chunkMap:SetAsync(serverId, packedBuf, ttl, sortKeyIndex)
	end)
	if not writeOk then
		return false, tostring(err)
	end
	return true
end

-- ===== Master only: read every chunk, merge, write ONE DataStore key =====
-- No sharding: the whole combined blob goes to a single key, converted to
-- a string right at the boundary since DataStore doesn't accept buffer.

function SyncEngine.runMasterCombineCycle(self: SyncEngine, maxIndex: number): (boolean, string?)
	local numRanges = math.max(1, math.ceil((maxIndex + 1) / RANGE_WIDTH))
	local pageResults: { any } = {}
	local threads = {}

	for i = 1, numRanges do
		local lower = (i - 1) * RANGE_WIDTH
		local upper = lower + RANGE_WIDTH
		threads[i] = task.spawn(function()
			local ok, items = pcall(function()
				return self._chunkMap:GetRangeAsync(
					Enum.SortDirection.Ascending,
					RANGE_WIDTH,
					if lower == 0 then nil else { sortKey = lower },
					{ sortKey = upper }
				)
			end)
			pageResults[i] = if ok then items else {}
		end)
	end

	for _, thread in threads do
		while coroutine.status(thread) ~= "dead" do
			task.wait()
		end
	end

	local mergedJson: { [string]: any } = {}
	local mergedBlobs: { [string]: buffer } = {}

	for i = 1, numRanges do
		for _, item in pageResults[i] or {} do
			local serverId = item.key
			local ok, jsonPart, blobs = pcall(unpackWithBlobs, item.value)
			if ok then
				mergedJson[serverId] = jsonPart
				for channelName, blobBuf in blobs :: { [string]: buffer } do
					mergedBlobs[serverId .. ":" .. channelName] = blobBuf
				end
			else
				warn(("[SyncEngine] Failed to unpack chunk for %s"):format(serverId))
			end
		end
	end

	local packOk, combinedBuf = pcall(packWithBlobs, mergedJson, mergedBlobs)
	if not packOk then
		return false, ("Combine pack failed: %s"):format(tostring(combinedBuf))
	end

	-- DataStore accepts buffer natively (updated alongside MemoryStore) -
	-- no string conversion needed, no round-trip corruption risk.
	local writeOk, err = pcall(function()
		self._combinedStore:SetAsync(self._combinedKey, combinedBuf)
	end)
	if not writeOk then
		return false, ("DataStore write failed: %s"):format(tostring(err))
	end

	return true
end

-- ===== Every server: read the single combined DataStore key =====
-- Cache explicitly disabled - always a live fetch, never stale.

function SyncEngine.readShards(self: SyncEngine): boolean
	local ok, combinedBuf = pcall(function()
		return self._combinedStore:GetAsync(self._combinedKey, NO_CACHE_OPTIONS)
	end)

	if not ok or combinedBuf == nil then
		return false
	end

	local decodeOk, mergedJson, mergedBlobs = pcall(unpackWithBlobs, combinedBuf)

	if not decodeOk then
		warn("[SyncEngine] Failed to decode combined DataStore value")
		return false
	end

	local blobsByServer: { [string]: { [string]: buffer } } = {}
	for compositeKey, blobBuf in mergedBlobs :: { [string]: buffer } do
		local colonPos = string.find(compositeKey, ":")
		if colonPos then
			local serverId = string.sub(compositeKey, 1, colonPos - 1)
			local channelName = string.sub(compositeKey, colonPos + 1)
			blobsByServer[serverId] = blobsByServer[serverId] or {}
			blobsByServer[serverId][channelName] = blobBuf
		end
	end

	local seen: { [string]: boolean } = {}
	for serverId, jsonPart in mergedJson :: { [string]: any } do
		seen[serverId] = true
		pcall(self._output.onReceive, self._output, serverId, jsonPart, blobsByServer[serverId] or {})
	end

	for serverId in self._knownServerIds do
		if not seen[serverId] then
			pcall(self._output.onExpire, self._output, serverId)
		end
	end
	self._knownServerIds = seen

	return true
end

-- Targeted single-chunk read - unchanged, still MemoryStore, still cheap.

function SyncEngine.readOneChunk(self: SyncEngine, serverId: string): (any, { [string]: buffer })?
	local ok, raw = pcall(function()
		return self._chunkMap:GetAsync(serverId)
	end)
	if not ok or raw == nil then
		return nil
	end

	local decodeOk, jsonPart, blobs = pcall(unpackWithBlobs, raw)
	if not decodeOk then
		return nil
	end

	pcall(self._output.onReceive, self._output, serverId, jsonPart, blobs)
	return jsonPart, blobs
end

function SyncEngine.Destroy(self: SyncEngine)
	table.clear(self._knownServerIds)
	self._chunkMap = nil :: any
	self._combinedStore = nil :: any
	self._output = nil :: any
end

return SyncEngine
