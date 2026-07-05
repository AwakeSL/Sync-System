--!strict
-- SyncPriorities.luau
-- Suggested priority constants for ordering local channel processing.
-- Not enforced by SyncManager itself, just convenient reference points.

return table.freeze({
	Early = 100,
	Normal = 0,
	Late = -100,
})
