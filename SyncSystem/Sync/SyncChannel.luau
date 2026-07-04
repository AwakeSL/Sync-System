--!strict
-- SyncChannel.lua
-- Declarative definition of a named slice of synced data.
-- Mirrors AbilitySystem's Ability object: a config-driven unit with
-- validators gating whether its "work" (provide()) runs this tick.

local SyncValidators = require(script.Parent.Parent.Config.SyncValidators)

local SyncChannel = {}
SyncChannel.__index = SyncChannel
SyncChannel.Validators = SyncValidators

export type ValidatorImpl = {
	type: string,
	check: (channel: any, context: any, params: any, state: any) -> boolean,
	commit: ((channel: any, context: any, params: any, state: any) -> ())?,
}

export type SyncChannelConfig = {
	name: string,
	refreshInterval: number?,
	validators: { [string]: any }?,
	provide: (context: any) -> { [string]: any },
	onReceive: ((channel: any, context: any, serverId: string, data: any) -> ())?,
	onExpire: ((channel: any, context: any, serverId: string) -> ())?,
}

export type SyncChannel = typeof(setmetatable(
	{} :: SyncChannelConfig & {
		_lastValue: { [string]: any }?,
		_lastComputedAt: number?,
		_validatorState: { [string]: any },
	},
	SyncChannel
))

-- Registered validator implementations, shared at the module level.
-- Populated by SyncManager.registerValidator / folder auto-registration.
local validatorImpls: { [string]: ValidatorImpl } = {}

function SyncChannel._registerValidatorImpl(impl: ValidatorImpl)
	validatorImpls[impl.type] = impl
end

function SyncChannel.new(config: SyncChannelConfig): SyncChannel
	assert(type(config.name) == "string" and #config.name > 0, "[SyncChannel] name is required")
	assert(type(config.provide) == "function", "[SyncChannel] provide(context) is required")

	local self = setmetatable({
		name = config.name,
		refreshInterval = config.refreshInterval,
		validators = config.validators or {},
		provide = config.provide,
		onReceive = config.onReceive,
		onExpire = config.onExpire,
		_lastValue = nil,
		_lastComputedAt = nil,
		_validatorState = {},
	}, SyncChannel)

	return self
end

-- Runs every configured validator's check(). Returns false if any fail.
local function passesValidators(self: SyncChannel, context: any): boolean
	for validatorType, params in self.validators do
		local impl = validatorImpls[validatorType]
		if not impl then
			warn(("[SyncChannel:%s] No validator registered for type '%s' - skipping gate"):format(self.name, validatorType))
			continue
		end

		self._validatorState[validatorType] = self._validatorState[validatorType] or {}
		local ok, passed = pcall(impl.check, self, context, params, self._validatorState[validatorType])
		if not ok then
			warn(("[SyncChannel:%s] Validator '%s' errored: %s"):format(self.name, validatorType, tostring(passed)))
			return false
		end

		if not passed then
			return false
		end
	end

	return true
end

local function commitValidators(self: SyncChannel, context: any)
	for validatorType, params in self.validators do
		local impl = validatorImpls[validatorType]
		if impl and impl.commit then
			pcall(impl.commit, self, context, params, self._validatorState[validatorType])
		end
	end
end

-- Resolves this tick's value for the channel: recomputes via provide()
-- if refreshInterval/validators allow it, otherwise returns the cached
-- last value. Returns nil if provide() has never successfully run yet.
function SyncChannel.resolve(self: SyncChannel, context: any): { [string]: any }?
	local now = os.clock()
	local interval = self.refreshInterval

	if interval and self._lastComputedAt and (now - self._lastComputedAt) < interval then
		return self._lastValue
	end

	if not passesValidators(self, context) then
		return self._lastValue
	end

	local ok, value = pcall(self.provide, context)
	if not ok then
		warn(("[SyncChannel:%s] provide() errored: %s"):format(self.name, tostring(value)))
		return self._lastValue
	end

	commitValidators(self, context)
	self._lastValue = value
	self._lastComputedAt = now

	return value
end

return SyncChannel
