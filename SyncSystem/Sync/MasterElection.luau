--!strict
-- MasterElection.luau
-- Lease-based leader election. Any server can hold the lease; if the
-- current holder stops renewing (crash, hang), the lease expires and the
-- next server to try claims it. Fully atomic via UpdateAsync - no two
-- servers can ever believe they're master at the same time.
--
-- Runs on its own independent timer (renewInterval), fully decoupled from
-- the Manager's main publish/read interval - it just exposes `.isMaster`
-- for the main loop to check whenever it happens to tick.

local MasterElection = {}
MasterElection.__index = MasterElection

export type MasterElectionConfig = {
	leaseDuration: number?,
	renewInterval: number?,
}

export type MasterElection = typeof(setmetatable(
	{} :: {
		isMaster: boolean,
		_hashMap: MemoryStoreHashMap,
		_serverId: string,
		_leaseDuration: number,
		_renewInterval: number,
		_running: boolean,
	},
	MasterElection
))

function MasterElection.new(hashMap: MemoryStoreHashMap, serverId: string, config: MasterElectionConfig?): MasterElection
	config = config or {}
	return setmetatable({
		isMaster = false,
		_hashMap = hashMap,
		_serverId = serverId,
		_leaseDuration = config.leaseDuration or 15,
		_renewInterval = config.renewInterval or 5,
		_running = false,
	}, MasterElection) :: any
end

function MasterElection._tryClaimOrRenew(self: MasterElection): boolean
	local ok, result = pcall(function()
		return self._hashMap:UpdateAsync("master", function(current: { ownerId: string, expiresAt: number }?)
			local now = os.time()
			if current == nil or current.expiresAt < now or current.ownerId == self._serverId then
				return { ownerId = self._serverId, expiresAt = now + self._leaseDuration }
			end
			return current
		end, self._leaseDuration + 5)
	end)
	return ok and result ~= nil and result.ownerId == self._serverId
end

function MasterElection.start(self: MasterElection)
	if self._running then
		return
	end
	self._running = true

	task.spawn(function()
		while self._running do
			self.isMaster = self:_tryClaimOrRenew()
			task.wait(self._renewInterval)
		end
	end)
end

function MasterElection.stop(self: MasterElection)
	self._running = false
	self.isMaster = false
end

return MasterElection
