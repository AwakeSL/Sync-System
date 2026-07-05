--!strict
-- SyncChannel.luau
-- Declarative definition of a named slice of synced data.
-- Mirrors AbilitySystem's Ability object: a config-driven unit with
-- validators gating whether its "work" (provide()) runs this tick.
--
-- A channel is normally JSON-shaped (any table). Opting into `binaryLayout`
-- makes it a fixed-shape array-of-records channel instead, packed into a
-- tight buffer (see packRecords/unpackRecords below) rather than JSON --
-- worth it for high-volume, fixed-shape data like per-player positions.
--
-- File lives at SyncSystem/Sync/SyncChannel.luau, so its parent is the
-- Sync folder, and Sync's parent is the SyncSystem folder -- Config is a
-- sibling of Sync, hence script.Parent.Parent.Config below.

local SyncValidators = require(script.Parent.Parent.Config.SyncValidators)

local SyncChannel = {}
SyncChannel.__index = SyncChannel
SyncChannel.Validators = SyncValidators

export type ValidatorImpl = {
	type: string,
	check: (channel: any, context: any, params: any, state: any) -> boolean,
	commit: ((channel: any, context: any, params: any, state: any) -> ())?,
}

export type BinaryField = { name: string, type: "i32" | "u32" | "u16" | "u8" | "f32" | "f64" }

export type SyncChannelConfig = {
	name: string,
	refreshInterval: number?,
	validators: { [string]: any }?,
	binaryLayout: { BinaryField }?, -- opt-in fixed-shape binary packing
	provide: (context: any) -> any,
	onReceive: ((channel: any, context: any, serverId: string, data: any) -> ())?,
	onExpire: ((channel: any, context: any, serverId: string) -> ())?,
}

export type SyncChannel = typeof(setmetatable(
	{} :: SyncChannelConfig & {
		_lastValue: any,
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
		binaryLayout = config.binaryLayout,
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
function SyncChannel.resolve(self: SyncChannel, context: any): any
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

-- ===== Binary layout pack/unpack (used only by channels that opt in) =====

local FIELD_SIZES: { [string]: number } = { i32 = 4, u32 = 4, u16 = 2, u8 = 1, f32 = 4, f64 = 8 }

local FIELD_WRITERS: { [string]: (buf: buffer, offset: number, value: number) -> () } = {
	i32 = buffer.writei32,
	u32 = buffer.writeu32,
	u16 = buffer.writeu16,
	u8 = buffer.writeu8,
	f32 = buffer.writef32,
	f64 = buffer.writef64,
}

local FIELD_READERS: { [string]: (buf: buffer, offset: number) -> number } = {
	i32 = buffer.readi32,
	u32 = buffer.readu32,
	u16 = buffer.readu16,
	u8 = buffer.readu8,
	f32 = buffer.readf32,
	f64 = buffer.readf64,
}

local function recordSize(layout: { BinaryField }): number
	local size = 0
	for _, field in layout do
		size += FIELD_SIZES[field.type]
	end
	return size
end

-- Packs an array of flat records into a buffer, per the given layout.
-- records[i] must have every field named in layout.
function SyncChannel.packRecords(layout: { BinaryField }, records: { { [string]: number } }): buffer
	local size = recordSize(layout)
	local buf = buffer.create(#records * size)

	for i, record in records do
		local offset = (i - 1) * size
		for _, field in layout do
			FIELD_WRITERS[field.type](buf, offset, record[field.name])
			offset += FIELD_SIZES[field.type]
		end
	end

	return buf
end

-- Inverse of packRecords. Returns an array of flat record tables.
function SyncChannel.unpackRecords(layout: { BinaryField }, buf: buffer): { { [string]: number } }
	local size = recordSize(layout)
	local count = if size > 0 then buffer.len(buf) // size else 0
	local records = table.create(count)

	for i = 0, count - 1 do
		local offset = i * size
		local record = {}
		for _, field in layout do
			record[field.name] = FIELD_READERS[field.type](buf, offset)
			offset += FIELD_SIZES[field.type]
		end
		records[i + 1] = record
	end

	return records
end

return SyncChannel
