--!strict
-- SyncPriorities.lua
-- Suggested priority constants for ordering local channel processing.
-- Not enforced by SyncManager itself, just convenient reference points
-- (mirrors AbilitySystem's AbilityPriorities).

return table.freeze({
	Early = 100,
	Normal = 0,
	Late = -100,
})
