--!strict
-- RegistryStore.luau
-- Hands out permanent, dense-ish index numbers to servers, used as the
-- sortKey on their chunk writes so the master can split reads into fixed,
-- predictable index ranges instead of paginating through JobId order.
-- Sharded (so the registry itself can exceed 32KB), reused via a Queue,
-- with heartbeat + sweep to reclaim indices from servers that died without
-- running BindToClose.

local RegistryStore = {}
RegistryStore.__index = RegistryStore

local SHARD_SIZE = 1000
local MAX_PUSH_RETRIES = 3

export type SlotData = {
	jobId: string,
	lastAlive: number,
}

export type RegistryStore = typeof(setmetatable(
	{} :: {
		_hashMap: MemoryStoreHashMap,
		_freeQueue: MemoryStoreQueue,
		_counterKey: string,
		_defaultExpiration: number,
		_heartbeatStaleSeconds: number,
	},
	RegistryStore
))

function RegistryStore.new(
	hashMap: MemoryStoreHashMap,
	freeQueue: MemoryStoreQueue,
	counterKey: string,
	defaultExpiration: number,
	heartbeatStaleSeconds: number
): RegistryStore
	return setmetatable({
		_hashMap = hashMap,
		_freeQueue = freeQueue,
		_counterKey = counterKey,
		_defaultExpiration = defaultExpiration,
		_heartbeatStaleSeconds = heartbeatStaleSeconds,
	}, RegistryStore) :: any
end

local function shardKeyFor(index: number): string
	return "shard_" .. math.floor(index / SHARD_SIZE)
end

-- Claim a slot for jobId. Tries the free queue first (reusing a departed
-- server's index); falls back to minting a brand new one via an atomic
-- counter if the queue is empty or exhausted after retries.
function RegistryStore.Push(self: RegistryStore, jobId: string, expiration: number?): (boolean, number?)
	local exp = expiration or self._defaultExpiration

	for _ = 1, MAX_PUSH_RETRIES do
		local ok, freedIndex = pcall(function()
			return self._freeQueue:RemoveAsync()
		end)

		if ok and freedIndex then
			local index = freedIndex :: number
			local shardKey = shardKeyFor(index)
			local claimed = false

			pcall(function()
				self._hashMap:UpdateAsync(shardKey, function(dict: { [number]: SlotData }?)
					dict = dict or {}
					if dict[index] then
						-- Leaked pointer from a crash between AddAsync and
						-- the actual fill step. Skip it, try the next one.
						claimed = false
						return dict
					end
					dict[index] = { jobId = jobId, lastAlive = os.time() }
					claimed = true
					return dict
				end, exp)
			end)

			if claimed then
				return true, index
			end
		else
			break -- queue empty, fall through to minting
		end
	end

	local mintOk, index = pcall(function()
		return self._hashMap:UpdateAsync(self._counterKey, function(n: number?)
			return (n or 0) + 1
		end, exp)
	end)
	if not mintOk or not index then
		return false, nil
	end

	local shardKey = shardKeyFor(index :: number)
	pcall(function()
		self._hashMap:UpdateAsync(shardKey, function(dict: { [number]: SlotData }?)
			dict = dict or {}
			dict[index :: number] = { jobId = jobId, lastAlive = os.time() }
			return dict
		end, exp)
	end)

	return true, index :: number
end

-- Server calls this periodically (e.g. every publish tick) to prove it's alive.
function RegistryStore.Heartbeat(self: RegistryStore, index: number, jobId: string, expiration: number?)
	local exp = expiration or self._defaultExpiration
	local shardKey = shardKeyFor(index)
	pcall(function()
		self._hashMap:UpdateAsync(shardKey, function(dict: { [number]: SlotData }?)
			if dict and dict[index] and dict[index].jobId == jobId then
				dict[index].lastAlive = os.time()
			end
			return dict
		end, exp)
	end)
end

-- Graceful leave: clear the slot, push index back onto the free queue for reuse.
function RegistryStore.RemoveAt(self: RegistryStore, index: number, expiration: number?): boolean
	local exp = expiration or self._defaultExpiration
	local shardKey = shardKeyFor(index)

	local ok = pcall(function()
		self._hashMap:UpdateAsync(shardKey, function(dict: { [number]: SlotData }?)
			if dict then
				dict[index] = nil
			end
			return dict
		end, exp)
	end)

	if ok then
		pcall(function()
			self._freeQueue:AddAsync(index, exp)
		end)
	end
	return ok
end

-- Reclaims indices whose heartbeat has gone stale (crash without BindToClose).
-- Call this occasionally (e.g. every N cycles), not necessarily every tick.
function RegistryStore.SweepDeadSlots(self: RegistryStore, maxIndex: number, expiration: number?)
	local exp = expiration or self._defaultExpiration
	local numShards = math.floor(maxIndex / SHARD_SIZE) + 1
	local now = os.time()

	for s = 0, numShards - 1 do
		local shardKey = "shard_" .. s
		pcall(function()
			self._hashMap:UpdateAsync(shardKey, function(dict: { [number]: SlotData }?)
				if not dict then
					return dict
				end
				for index, slot in dict do
					if now - slot.lastAlive > self._heartbeatStaleSeconds then
						dict[index] = nil
						pcall(function()
							self._freeQueue:AddAsync(index, exp)
						end)
					end
				end
				return dict
			end, exp)
		end)
	end
end

-- Highest index ever minted -- tells the master how wide the index space
-- is, independent of how many slots are currently occupied.
function RegistryStore.GetMaxIndex(self: RegistryStore): number
	local ok, n = pcall(function()
		return self._hashMap:GetAsync(self._counterKey)
	end)
	return (ok and n) or 0
end

return RegistryStore
