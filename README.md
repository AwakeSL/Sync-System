# SyncManager

A modular, near-real-time cross-server data sync framework built around Channels, Providers, and events. Servers publish and read data through MemoryStore-backed Channels; consumers react to changes via events instead of polling raw data themselves. An optional MessagingService Bridge layers targeted, low-latency server-to-server messaging on top.

Four core pieces:

- **SyncChannel** — defines a named slice of synced data and how it's produced/consumed.
- **SyncManager** — high-level wrapper; owns a server's channels, drives publish/read timing, exposes cache + events, maintains the player directory.
- **SyncEngine** — the low-level MemoryStore wrapper. Swap this in with a custom output if you don't want the built-in cache.
- **SyncBridge** — optional MessagingService layer for targeted, near-instant server-to-server pings on top of the MemoryStore-backed state.

## Core Concepts

**Channel**: a named unit of synced data (e.g. `Leaderboard`, `PartyStatus`) made up of a provider and optional receive/expire hooks. A channel is not per-player — its `provide()` loops over all of the current server's players itself and returns one table covering all of them.
**Provider**: the function on a channel that produces this server's data for that channel each publish tick.
**Combined payload**: all of a server's channels are merged into one `{ [channelName] = data }` table and sent as a single MemoryStore entry — one write and one read per tick, regardless of channel count.
**Event**: a signal fired when a channel's data changes (`Updated`, `ServerAdded`, `ServerRemoved`, `PublishFailed`, `ReadFailed`).
**Validator**: a reusable, stateful gate (e.g. `Cooldown`, `Conditions`) that gates whether a channel's `provide()` is recomputed this tick.
**Directory**: an internally maintained `userId -> serverId` reverse index, updated incrementally as server data is processed — not scanned on demand.
**Bridge**: optional per-server MessagingService topic for targeted messages, layered on top of the MemoryStore state as a low-latency notification path, not a replacement for it.

## Defining a Channel

```lua
local rs = game:GetService("ReplicatedStorage")
local SyncChannel = require(rs.SyncManager.SyncChannel)
local Validators = SyncChannel.Validators

return function()
    local leaderboard = SyncChannel.new({
        name = "Leaderboard",
        refreshInterval = 5, -- local-only throttle on recomputing provide(); does not affect network timing
        validators = {
            [Validators.Cooldown] = { Time = 5, Key = "Leaderboard" },
        },
        provide = function(context)
            local data = {}
            for _, player in game.Players:GetPlayers() do
                data[tostring(player.UserId)] = {
                    score = player:GetAttribute("Score"),
                }
            end
            return data
        end,
        onReceive = function(channel, context, serverId, data)
            -- another server's data for this channel just changed
        end,
        onExpire = function(channel, context, serverId)
            -- that server's whole entry expired (crash / TTL lapse)
        end,
    })

    return leaderboard
end
```

`refreshInterval` and `validators` are local-only — they gate whether `provide()` recomputes this tick, not whether a network call happens. All channels on a server always ship together in one combined payload on the Manager's shared interval.

## Using the Manager

```lua
local SyncManager = require(rs.SyncManager)
local Leaderboard = require(sss.SyncChannels.Leaderboard)

local manager = SyncManager.new({
    interval = 10,
    ttl = 30,
    autoTriggerOnPlayerEvents = true,
    autoTriggerDebounce = 1,
})

manager:give(Leaderboard())
manager:start()

manager.Updated:Connect(function(channelName, serverId, data)
    print(serverId, "updated", channelName, data)
end)
```

## Channel Config Reference

| Field | Type | Description |
|---|---|---|
| `name` | `string` | Unique channel name; also the sub-key inside the server's combined payload. |
| `refreshInterval` | `number` | Local-only throttle on recomputing `provide()`. Does not affect network timing. Defaults to the manager's interval (recompute every tick). |
| `validators` | `{[validatorType]: params}` | Gates whether `provide()` is recomputed this tick. |
| `provide(context)` | `function` | Returns this channel's data for the whole server. Merged into the server's combined payload. |
| `onReceive(channel, context, serverId, data)` | `function` | Runs when this channel's slice of another server's payload changes. |
| `onExpire(channel, context, serverId)` | `function` | Runs when a server's whole entry expires — every channel on that server expires together. |

`context` exposes `context.manager` and `context.localServerId`.

## Built-in Validators

| Validator | Params | Behavior |
|---|---|---|
| `Cooldown` | `Time, Key` | Blocks recompute until `Time` seconds have passed since last recompute for this `Key`. Last computed value is reused until it clears. |
| `Conditions` | `{[conditionName]: bool}` | Blocks recompute unless each named condition function returns the expected value. Register via `Conditions.register(name, fn)`. |

### Adding a custom validator

Drop a ModuleScript in the `Validators` folder returning a table with `type`, `check(channel, context, params, state)`, and optional `commit(...)`. Picked up automatically on startup — no manual registration needed.

## Oversized / invalid payload handling

- If the combined payload for a server exceeds 32KB, or a channel's `provide()` returns non-JSON-safe data, **that channel is dropped from the payload with a warning** — the rest of the server's channels still publish normally. No auto-splitting into multiple keys; keeping one key per server avoids partial-write inconsistency and doubled request cost.

## SyncManager API

- `SyncManager.new(config?): SyncManager`
  - `config.interval` — shared publish/read interval (seconds).
  - `config.ttl` — shared entry expiration (seconds). Defaults to `interval * 3`.
  - `config.autoTriggerOnPlayerEvents` — `boolean`, default `false`. Hooks `Players.PlayerAdded`/`PlayerRemoving` and triggers a publish immediately on each, subject to `autoTriggerDebounce`.
  - `config.autoTriggerDebounce` — `number`, default `1`. Minimum seconds between player-event-triggered publishes; rapid joins/leaves within this window collapse into a single trailing trigger.
- `SyncManager.registerValidator(validatorType: string, impl: table)`
- `:give(channel: SyncChannel)` — adds a channel. Errors if one with the same name already exists.
- `:take(channelName: string)` — removes a channel.
- `:get(channelName: string): SyncChannel?`
- `:start()` — begins the shared publish/read loop.
- `:stop()`
- `:trigger()` — forces an immediate publish + read of the combined payload.
- `:triggerPublish()` — forces just a publish, without forcing a full-store read.
- `:triggerRead()` — forces just a full-store read.
- `:refreshServer(serverId: string)` — fetches and merges just one server's entry via a single-key read, firing normal events for whatever changed. Used internally by `SyncBridge` on incoming pings.
- `:getCache(channelName: string, serverId: string?)` — cached data for one server, or all servers if `serverId` is omitted.
- `:getServerForPlayer(userId: number): string?` — `O(1)` lookup against the internally maintained directory. Returns `nil` if the player isn't known yet (not online, or not synced yet).

## Events

Fired on the manager, subscribed via `manager.EventName:Connect(...)`:

| Event | Args | Fires when |
|---|---|---|
| `Updated` | `channelName, serverId, data` | A server's data for a channel changed. |
| `ServerAdded` | `channelName, serverId, data` | A server published to a channel for the first time. |
| `ServerRemoved` | `channelName, serverId` | A server's channel entry expired or was removed. |
| `PublishFailed` | `channelName, err` | This server's publish for a channel failed (including channel drops from oversized/invalid data). |
| `ReadFailed` | `channelName, err` | Reading failed for a channel. |

## SyncEngine API

The lower-level MemoryStore wrapper. The only piece that touches `HttpService:JSONEncode`/`JSONDecode` and `MemoryStoreService` directly. Swap this in with a custom output to replace the built-in cache entirely — mirrors `InputManager`'s custom output pattern.

- `SyncEngine.new(output: table)` — `output` must implement `:onReceive(serverId, data)` and `:onExpire(serverId)`.
- `:publish(serverId: string, combinedData: table, ttl: number)` — JSON-encodes the whole `{channelName: data}` table once, checks the 32KB limit, drops offending channels with a warning, then writes.
- `:readAll(): {serverId: combinedData}` — one paginated pass (via `GetRangeAsync`, keyed off the last entry's key per page — no cursor is returned by the API), decodes each entry once.
- `:read(serverId: string): data?` — single-key fetch, decodes just that one entry. Cheap alternative to `readAll()` for targeted refreshes.
- `:Destroy()`

## SyncBridge API

Optional. Layers targeted, best-effort, near-instant messaging on top of the MemoryStore-backed state using per-server MessagingService topics — not one shared topic, since every subscriber to a shared topic receives every message regardless of who it's addressed to, and MessagingService receive quotas scale with total messages received.

- `SyncBridge.new(manager: SyncManager, config?: table): SyncBridge`
  - `config.topicPrefix` — defaults to something namespaced to avoid colliding with your game's other topics.
- `:start()` — subscribes only to this server's own topic (`topicPrefix .. serverId`).
- `:sendToServer(serverId: string, eventName: string, payload: table)` — publishes directly to `topicPrefix .. serverId`. Only that server is subscribed, so only they receive it.
- `:sendToPlayer(userId: number, eventName: string, payload: table)` — convenience wrapper: resolves `manager:getServerForPlayer(userId)`, then `:sendToServer(...)`. **Warns** (does not error) if the player isn't found in the directory yet, since that's a legitimate transient state rather than a hard failure.
- `:onMessage(eventName: string, callback: function)` — registers a handler for incoming messages on this server's own topic.
- `:stop()`
- `:Destroy()`

MessagingService is best-effort with no delivery or ordering guarantee — Bridge messages are a low-latency notification path, not a source of truth. The Manager's normal interval loop (and `autoTriggerOnPlayerEvents`) keeps running underneath regardless, so a dropped or out-of-order message just means that update arrives slightly later via the regular sync cycle instead of never.

## Config Modules

- **SyncChannels** — enum of channel name constants, populated per-project (avoids magic strings).
- **SyncEvents** — enum of event names (`Updated`, `ServerAdded`, `ServerRemoved`, `PublishFailed`, `ReadFailed`).
- **SyncPriorities** — suggested constants (`Early`, `Normal`, `Late`) for ordering multiple channels' local processing if it ever matters; not enforced, just reference points.

## Known edge cases (by design, not bugs)

- **Player mid-transfer between servers**: a brief window exists where a player can appear on both the old and new server's directory, or briefly on neither, until each side's next publish. `autoTriggerOnPlayerEvents` shrinks this to roughly one publish latency; it isn't eliminated entirely without atomic cross-server coordination, which this system doesn't attempt.
- **Server crash**: a crashed server never gets to publish its own removal. Its entry (and every channel on it) only clears once `ttl` lapses — same handling as any other MemoryStore-based liveness pattern.
- **Out-of-order Bridge/targeted updates**: the directory is updated incrementally as data is processed, so a late-arriving update could briefly overwrite a newer one if messages arrive out of order. The next regular `readAll()` reconciliation pass corrects this.