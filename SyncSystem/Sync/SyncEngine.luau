--!strict
-- SyncEngine.luau
-- The only module that touches MemoryStoreService/HttpService JSON directly.
--
-- Chunk store: each server writes its own combined payload here, keyed by
-- JobId, sorted by its permanent registry index. A chunk is one buffer:
-- a length-prefixed JSON section (all non-binary channels + directory)
-- followed by N length-prefixed named binary blobs (one per binary channel).
--
-- Shard store: only the current master writes here. Every server's chunk
-- gets unpacked, re-merged into one big json-part + blobs-part pair using
-- the same scheme one level up, then sliced by raw BYTE offset into
-- <=32000-byte pieces. Byte slicing is safe here because shards are always
-- fully reassembled via buffer.copy before anything tries to decode them --
-- nothing ever parses a single shard in isolation.
--
-- Engine never needs to understand binary field layouts - it treats binary
-- channels as opaque blobs it faithfully preserves through combine/shard.
-- Only SyncManager (via SyncChannel.packRecords/unpackRecords) interprets
-- the actual bytes, since only it knows each channel's layout.

local MemoryStoreService = game:GetService("MemoryStoreService")
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
		_shardMap: any,
		_output: Output,
		_knownServerIds: { [string]: boolean },
	},
	SyncEngine
))

local RANGE_WIDTH = 200
local SHARD_CAPACITY_BYTES = 32000 -- margin under the 32768 hard cap

function SyncEngine.new(chunkStoreName: string, shardStoreName: string, output: Output): SyncEngine
	return setmetatable({
		_chunkMap = MemoryStoreService:GetSortedMap(chunkStoreName),
		_shardMap = MemoryStoreService:GetSortedMap(shardStoreName),
		_output = output,
		_knownServerIds = {},
	}, SyncEngine) :: any
end

-- ===== generic pack/unpack: {jsonPart: table, blobs: {[key]: buffer}} <-> buffer =====

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

-- ===== Every server: write its own chunk =====
-- jsonChannels: plain-shape channels + directory, merged into one table.
-- binaryChannels: {[channelName]: buffer} - already packed by the Manager
-- via SyncChannel.packRecords before calling this.

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

-- ===== Master only: read every chunk, merge, reshard =====

function SyncEngine.runMasterCombineCycle(self: SyncEngine, maxIndex: number, ttl: number): (boolean, string?)
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

	-- Merge every server's chunk into one big json-part + blobs-part pair.
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

	local totalBytes = buffer.len(combinedBuf)
	local numShards = math.max(1, math.ceil(totalBytes / SHARD_CAPACITY_BYTES))
	local writeThreads = {}

	for s = 1, numShards do
		local startOffset = (s - 1) * SHARD_CAPACITY_BYTES
		local sliceLen = math.min(SHARD_CAPACITY_BYTES, totalBytes - startOffset)
		local shardBuf = buffer.create(sliceLen)
		buffer.copy(shardBuf, 0, combinedBuf, startOffset, sliceLen)

		writeThreads[#writeThreads + 1] = task.spawn(function()
			pcall(function()
				self._shardMap:SetAsync(tostring(s), shardBuf, ttl, s)
			end)
		end)
	end

	writeThreads[#writeThreads + 1] = task.spawn(function()
		pcall(function()
			self._shardMap:SetAsync("meta", { count = numShards, totalBytes = totalBytes }, ttl, 0)
		end)
	end)

	for _, thread in writeThreads do
		while coroutine.status(thread) ~= "dead" do
			task.wait()
		end
	end

	return true
end

-- ===== Every server: cheap broadcast read of the master's shards =====

function SyncEngine.readShards(self: SyncEngine): boolean
	local metaOk, meta = pcall(function()
		return self._shardMap:GetAsync("meta")
	end)
	if not metaOk or not meta or meta.count == 0 then
		return false
	end

	local numShards = meta.count :: number
	local totalBytes = meta.totalBytes :: number
	local pieceInfo: { { offset: number, buf: buffer } } = table.create(numShards)
	local threads = {}

	for s = 1, numShards do
		threads[s] = task.spawn(function()
			local ok, value = pcall(function()
				return self._shardMap:GetAsync(tostring(s))
			end)
			pieceInfo[s] = { offset = (s - 1) * SHARD_CAPACITY_BYTES, buf = if ok and value then value else buffer.create(0) }
		end)
	end

	for _, thread in threads do
		while coroutine.status(thread) ~= "dead" do
			task.wait()
		end
	end

	local combined = buffer.create(totalBytes)
	for _, piece in pieceInfo do
		buffer.copy(combined, piece.offset, piece.buf, 0, buffer.len(piece.buf))
	end

	local decodeOk, mergedJson, mergedBlobs = pcall(unpackWithBlobs, combined)
	if not decodeOk then
		warn("[SyncEngine] Failed to decode combined shard buffer")
		return false
	end

	-- Group blobs back by serverId (they're keyed "serverId:channelName").
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

-- Targeted single-chunk read, e.g. for Bridge-triggered refresh. Cheap: 1 request.
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
	self._shardMap = nil :: any
	self._output = nil :: any
end

return SyncEngine
