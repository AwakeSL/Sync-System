--!strict
-- SyncValidators.lua
-- Enum of built-in validator type names, for referencing in channel config
-- instead of hardcoding strings (mirrors AbilityValidators).

return table.freeze({
	Cooldown = "Cooldown",
	Conditions = "Conditions",
})
