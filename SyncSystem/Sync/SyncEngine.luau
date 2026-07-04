--!strict
-- SyncEngine.lua
-- The only module that touches MemoryStoreService and HttpService's JSON
-- encode/decode. Deals purely in serverId -> combined payload tables;
-- has no concept of "channels" - that's the Manager's job.
--
-- Diffs against its own last-seen raw strings so it only calls
-- output:onReceive / output:onExpire when something actually changed,
-- not on every read regardless of content.

local MemoryStoreService = game:GetService("MemoryStoreService")
local HttpService = game:GetService("HttpService")

local SyncEngine = {}
SyncEngine.__index = SyncEngine

export type Output = {
	onReceive: (self: any, serverId: string, data: { [string]: any }) -> (),
	onExpire: (self: any, serverId: string) -> (),
}

export type SyncEngine = typeof(setmetatable(
	{} :: {
		_map: any,
		_output: Output,
		_knownRaw: { [string]: string },
	},
	SyncEngine
))

local LIST_PAGE_SIZE = 200

function SyncEngine.new(storeName: string, output: Output): SyncEngine
	return setmetatable({
		_map = MemoryStoreService:GetSortedMap(storeName),
		_output = output,
		_knownRaw = {},
	}, SyncEngine)
end

-- Encodes and writes one server's combined payload. Returns true, or false + error.
function SyncEngine.publish(self: SyncEngine, serverId: string, combinedData: { [string]: any }, ttl: number): (boolean, string?)
	local encodeOk, encoded = pcall(HttpService.JSONEncode, HttpService, combinedData)
	if not encodeOk then
		return false, ("JSON encode failed: %s"):format(tostring(encoded))
	end

	local writeOk, err = pcall(function()
		self._map:SetAsync(serverId, encoded, ttl)
	end)

	if not writeOk then
		return false, tostring(err)
	end

	return true
end

-- Single-key targeted fetch. Cheap alternative to readAll() for Bridge-triggered refreshes.
-- Fires output:onReceive if the value changed since last seen.
function SyncEngine.read(self: SyncEngine, serverId: string): { [string]: any }?
	local ok, raw = pcall(function()
		return self._map:GetAsync(serverId)
	end)

	if not ok then
		warn(("[SyncEngine] read(%s) failed: %s"):format(serverId, tostring(raw)))
		return nil
	end

	if raw == nil then
		return nil
	end

	local decodeOk, decoded = pcall(HttpService.JSONDecode, HttpService, raw)
	if not decodeOk then
		warn(("[SyncEngine] Bad JSON for key %s"):format(serverId))
		return nil
	end

	if self._knownRaw[serverId] ~= raw then
		self._knownRaw[serverId] = raw
		pcall(self._output.onReceive, self._output, serverId, decoded)
	end

	return decoded
end

-- Full paginated pass over every server's entry. Fires onReceive for each
-- changed/new entry, and onExpire for any previously-known entry that's
-- no longer present (TTL lapsed / server crashed).
function SyncEngine.readAll(self: SyncEngine): { [string]: any }?
	local merged: { [string]: any } = {}
	local seen: { [string]: boolean } = {}

	local ok, err = pcall(function()
		local exclusiveLowerBound: string? = nil

		while true do
			-- GetRangeAsync returns only the array of entries, no cursor -
			-- paginate by feeding the last entry's key back in as the bound.
			local items = self._map:GetRangeAsync(
				Enum.SortDirection.Ascending,
				LIST_PAGE_SIZE,
				exclusiveLowerBound
			)

			if #items == 0 then
				break
			end

			for _, item in items do
				seen[item.key] = true

				local decodeOk, decoded = pcall(HttpService.JSONDecode, HttpService, item.value)
				if decodeOk then
					merged[item.key] = decoded

					if self._knownRaw[item.key] ~= item.value then
						self._knownRaw[item.key] = item.value
						pcall(self._output.onReceive, self._output, item.key, decoded)
					end
				else
					warn(("[SyncEngine] Bad JSON for key %s"):format(item.key))
				end
			end

			if #items < LIST_PAGE_SIZE then
				break
			end

			exclusiveLowerBound = items[#items].key
		end
	end)

	if not ok then
		return nil, tostring(err)
	end

	for serverId in self._knownRaw do
		if not seen[serverId] then
			self._knownRaw[serverId] = nil
			pcall(self._output.onExpire, self._output, serverId)
		end
	end

	return merged
end

function SyncEngine.Destroy(self: SyncEngine)
	table.clear(self._knownRaw)
	self._map = nil :: any
	self._output = nil :: any
end

return SyncEngine
