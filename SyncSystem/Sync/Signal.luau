--!strict
-- Signal.luau
-- Minimal internal Signal implementation used for SyncManager's public events
-- (Updated, ServerAdded, ServerRemoved, PublishFailed, ReadFailed).
-- Not part of the public API surface directly, just the thing those fields are.

local Signal = {}
Signal.__index = Signal

export type Signal = typeof(setmetatable(
	{} :: { _bindable: BindableEvent },
	Signal
))

function Signal.new(): Signal
	return setmetatable({
		_bindable = Instance.new("BindableEvent"),
	}, Signal)
end

function Signal.Connect(self: Signal, callback: (...any) -> ()): RBXScriptConnection
	return self._bindable.Event:Connect(callback)
end

function Signal.Fire(self: Signal, ...: any)
	self._bindable:Fire(...)
end

function Signal.Destroy(self: Signal)
	self._bindable:Destroy()
end

return Signal
