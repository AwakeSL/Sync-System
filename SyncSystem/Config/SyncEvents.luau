--!strict
-- SyncEvents.luau
-- Enum of event names fired on a SyncManager instance.
-- Purely a reference table so consumers don't hardcode strings.

return table.freeze({
	Updated = "Updated",
	ServerAdded = "ServerAdded",
	ServerRemoved = "ServerRemoved",
	PublishFailed = "PublishFailed",
	ReadFailed = "ReadFailed",
})
