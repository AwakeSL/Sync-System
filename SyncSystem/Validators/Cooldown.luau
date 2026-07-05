--!strict
-- Validators/Cooldown.luau
-- Blocks a channel's provide() recompute until `Time` seconds have passed
-- since the last recompute for the given `Key`. This is a LOCAL throttle only
-- (see SyncChannel docs) - it does not gate any network call.

local Cooldown = {}
Cooldown.type = "Cooldown"

-- Shared timestamps across all channels using this validator, keyed by `Key`.
-- Lets multiple channels share one cooldown bucket if they pass the same Key.
local lastUsed: { [string]: number } = {}

-- Allows runtime speed-up/slow-down of a specific cooldown key, e.g. for
-- testing or debug tools. Multiplier of 2 means the cooldown effectively
-- takes half as long (time is scaled down), 0.5 means it takes twice as long.
local timescales: { [string]: number } = {}

function Cooldown.setTimescale(key: string, multiplier: number)
	timescales[key] = multiplier
end

-- check(channel, context, params, state) -> boolean
-- Returning false blocks the recompute this tick.
function Cooldown.check(_channel, _context, params: { Time: number, Key: string }, _state): boolean
	local key = params.Key
	local scale = timescales[key] or 1
	local effectiveTime = params.Time / math.max(scale, 1e-6)

	local last = lastUsed[key]
	local now = os.clock()

	if last ~= nil and (now - last) < effectiveTime then
		return false
	end

	return true
end

-- commit runs only after all validators on the channel have passed,
-- so the cooldown timestamp only advances on an actual recompute.
function Cooldown.commit(_channel, _context, params: { Time: number, Key: string }, _state)
	lastUsed[params.Key] = os.clock()
end

return Cooldown
