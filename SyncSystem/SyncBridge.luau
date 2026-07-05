--!strict
-- SyncBridge.luau
-- Optional targeted, near-instant messaging layered on top of the
-- MemoryStore-backed state. Each server subscribes only to its OWN topic
-- (topicPrefix .. serverId) - never a shared/global topic - so a message
-- addressed to one server is only ever received by that server, keeping
-- MessagingService's per-topic receive quota cost proportional to messages
-- actually addressed to you, not total messages in the game.
--
-- Best-effort, no delivery/ordering guarantee - SyncManager's normal
-- interval loop is still the source-of-truth reconciliation path underneath.

local MessagingService = game:GetService("MessagingService")

local SyncBridge = {}
SyncBridge.__index = SyncBridge

local DEFAULT_TOPIC_PREFIX = "SyncBridge_"

export type SyncBridgeConfig = {
	topicPrefix: string?,
}

export type SyncBridge = typeof(setmetatable(
	{} :: {
		_manager: any,
		_topicPrefix: string,
		_topic: string,
		_handlers: { [string]: { (payload: any) -> () } },
		_subscription: RBXScriptConnection?,
	},
	SyncBridge
))

function SyncBridge.new(manager: any, config: SyncBridgeConfig?): SyncBridge
	config = config or {}
	local topicPrefix = config.topicPrefix or DEFAULT_TOPIC_PREFIX

	return setmetatable({
		_manager = manager,
		_topicPrefix = topicPrefix,
		_topic = topicPrefix .. manager._serverId,
		_handlers = {},
		_subscription = nil,
	}, SyncBridge)
end

function SyncBridge._handleMessage(self: SyncBridge, message: { Data: any })
	local payload = message.Data
	if type(payload) ~= "table" or type(payload.eventName) ~= "string" then
		return
	end

	local callbacks = self._handlers[payload.eventName]
	if not callbacks then
		return
	end

	for _, callback in callbacks do
		pcall(callback, payload.data)
	end
end

function SyncBridge.start(self: SyncBridge)
	local ok, connOrErr = pcall(function()
		return MessagingService:SubscribeAsync(self._topic, function(message)
			self:_handleMessage(message)
		end)
	end)

	if ok then
		self._subscription = connOrErr
	else
		warn(("[SyncBridge] Failed to subscribe to topic '%s': %s"):format(self._topic, tostring(connOrErr)))
	end
end

function SyncBridge.stop(self: SyncBridge)
	if self._subscription then
		self._subscription:Disconnect()
		self._subscription = nil
	end
end

-- Publishes directly to another server's own topic. Only that server is
-- subscribed to it, so only they receive it.
function SyncBridge.sendToServer(self: SyncBridge, serverId: string, eventName: string, payload: any)
	local topic = self._topicPrefix .. serverId
	local ok, err = pcall(function()
		MessagingService:PublishAsync(topic, {
			eventName = eventName,
			data = payload,
		})
	end)

	if not ok then
		warn(("[SyncBridge] Failed to publish to '%s': %s"):format(topic, tostring(err)))
	end
end

-- Resolves the player's current server via the Manager's directory, then
-- sends directly to it. Warns (does not error) if the player isn't known
-- yet, since that's a legitimate transient state, not a hard failure.
function SyncBridge.sendToPlayer(self: SyncBridge, userId: number, eventName: string, payload: any)
	local serverId = self._manager:getServerForPlayer(userId)
	if not serverId then
		warn(("[SyncBridge] sendToPlayer: no known server for userId %d yet"):format(userId))
		return
	end

	self:sendToServer(serverId, eventName, payload)
end

function SyncBridge.onMessage(self: SyncBridge, eventName: string, callback: (payload: any) -> ())
	self._handlers[eventName] = self._handlers[eventName] or {}
	table.insert(self._handlers[eventName], callback)
end

function SyncBridge.Destroy(self: SyncBridge)
	self:stop()
	table.clear(self._handlers)
end

return SyncBridge
