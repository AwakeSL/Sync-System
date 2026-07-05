--!strict
-- Validators/Conditions.luau
-- Blocks a channel's provide() recompute unless every named condition
-- function returns the expected boolean value.

local Conditions = {}
Conditions.type = "Conditions"

local registry: { [string]: (context: any) -> boolean } = {}

function Conditions.register(name: string, fn: (context: any) -> boolean)
	registry[name] = fn
end

-- params is shaped { [conditionName: string]: expectedBool }
function Conditions.check(_channel, context, params: { [string]: boolean }, _state): boolean
	for conditionName, expected in params do
		local fn = registry[conditionName]
		if not fn then
			warn(("[SyncManager] Conditions validator: no condition registered named '%s'"):format(conditionName))
			return false
		end

		local ok, result = pcall(fn, context)
		if not ok then
			warn(("[SyncManager] Conditions validator: condition '%s' errored: %s"):format(conditionName, tostring(result)))
			return false
		end

		if result ~= expected then
			return false
		end
	end

	return true
end

return Conditions
