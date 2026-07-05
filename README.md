# SyncManager

A modular, near-real-time cross-server data sync framework built around Channels, Providers, and events. Servers publish their own data through MemoryStore-backed Channels; one elected server combines everyone's data each tick, and every server (including the elected one) reads that combined result cheaply — instead of every server reading every other server directly. Consumers react to changes via events rather than polling raw data themselves. An optional MessagingService Bridge layers targeted, low-latency server-to-server messaging on top.

Six core pieces:

- **SyncChannel** — defines a named slice of synced data and how it's produced/consumed. Normally JSON-shaped; can opt into a fixed **binary layout** for high-volume, fixed-shape data.
- **SyncManager** — high-level wrapper; owns a server's channels, drives the publish/combine/read timing, exposes cache + events, maintains the player directory.
- **SyncEngine** — the low-level MemoryStore + buffer wrapper. The only piece that touches `MemoryStoreService` and `HttpService:JSONEncode`/`JSONDecode` directly.
- **RegistryStore** — hands out permanent, reusable numeric indices to servers, used to split reads into predictable parallel ranges instead of paginating through random server-id order.
- **MasterElection** — lease-based leader election deciding which single server runs the combine step each cycle. Fully decoupled from the Manager's main interval.
- **SyncBridge** — optional MessagingService layer for targeted, near-instant server-to-server pings on top of the MemoryStore-backed state.

## Why this shape

The naive version of this system — every server reads every other server's entry every tick — costs roughly `O(S²)` requests as server count `S` grows, since each of `S` servers pays for reading all `S` entries. SyncManager instead uses a **hub model**: every server writes its own data once, one elected server combines everything into sharded output, and everyone reads that shared output. Cost becomes roughly `O(S + K)` where `K` is the number of output shards — far cheaper at any real scale, and the gap widens as `S` grows.

The elected server ("master") isn't a separate dedicated machine — it's an ordinary game server with real players on it, just also doing the combine work on top of its normal duties. If it crashes or hangs, its lease expires and another server automatically picks up the role.

## Core Concepts

**Channel**: a named unit of synced data (e.g. `Leaderboard`, `PlayerPositions`) made up of a provider and optional receive/expire hooks. A channel is not per-player — its `provide()` loops over all of the current server's players itself and returns one value covering all of them (a table for JSON channels, an array of records for binary channels).
**Provider**: the function on a channel that produces this server's data for that channel each publish tick.
**Registry index**: a small, permanent integer assigned to each server on join (reused from departed servers via a free-list queue once they leave). Used as the sort key on a server's chunk write, so combine reads can be split into fixed, predictable, parallel-safe ranges instead of paginating through random server-id order.
**Chunk**: one server's own published data — its JSON channels plus any binary channel blobs, packed into a single buffer, written to one MemoryStore key.
**Master**: whichever server currently holds the election lease. Every tick, only the master reads all chunks, merges them, and writes the combined result as shards. Every server (including the master) then reads those shards.
**Shard**: a `<=32KB` slice of the master's combined output. Slicing is done by raw byte offset and is safe because shards are always fully reassembled before anything tries to decode them — nothing ever parses a single shard in isolation.
**Event**: a signal fired when a channel's data changes (`Updated`, `ServerAdded`, `ServerRemoved`, `PublishFailed`, `ReadFailed`).
**Validator**: a reusable, stateful gate (e.g. `Cooldown`, `Conditions`) that gates whether a channel's `provide()` is recomputed this tick.
**Directory**: an internally maintained `userId -> serverId` reverse index, updated incrementally as server data is processed — not scanned on demand.
**Bridge**: optional per-server MessagingService topic for targeted messages, layered on top of the MemoryStore state as a low-latency notification path, not a replacement for it.

## Defining a Channel

### JSON channel (default — arbitrary table shape)

```lua
local rs = game:GetService("ReplicatedStorage")
local SyncChannel = require(rs.SyncSystem.Sync.SyncChannel)
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
            -- that server's whole entry expired
        end,
    })

    return leaderboard
end
```

### Binary channel (opt-in — fixed-shape, high-volume data)

Worth using for anything published every tick at real volume, where JSON's per-record overhead (quotes, keys, decimal text) adds up — e.g. per-player positions. `provide()` must return an **array of flat records** matching `binaryLayout` exactly, instead of an arbitrary table.

```lua
local playerPositions = SyncChannel.new({
    name = "PlayerPositions",
    binaryLayout = {
        { name = "userId", type = "i32" },
        { name = "posX", type = "f32" },
        { name = "posY", type = "f32" },
        { name = "posZ", type = "f32" },
        { name = "yaw", type = "f32" },
    },
    provide = function(context)
        local records = {}
        for _, player in game.Players:GetPlayers() do
            local root = player.Character and player.Character:FindFirstChild("HumanoidRootPart")
            if root then
                table.insert(records, {
                    userId = player.UserId,
                    posX = root.Position.X,
                    posY = root.Position.Y,
                    posZ = root.Position.Z,
                    yaw = root.Orientation.Y,
                })
            end
        end
        return records
    end,
})
```

Supported field types: `i32`, `u32`, `u16`, `u8`, `f32`, `f64`. A binary channel's data arrives at `onReceive`/`getCache` already unpacked back into an array of record tables — consumers never see raw bytes.

`refreshInterval` and `validators` are local-only for both channel kinds — they gate whether `provide()` recomputes this tick, not whether a network call happens. All of a server's channels (JSON and binary) always ship together in one combined chunk on the Manager's shared interval.

## Using the Manager

```lua
local SyncSystem = game:GetService("ReplicatedStorage").SyncSystem
local SyncManager = require(SyncSystem.SyncManager)
local Leaderboard = require(sss.SyncChannels.Leaderboard)

local manager = SyncManager.new({
    interval = 10,
    ttl = 30,
    autoTriggerOnPlayerEvents = true,
    autoTriggerDebounce = 1,
    leaseDuration = 15,       -- how long the master election lease lasts without renewal
    renewInterval = 5,        -- how often a server tries to claim/renew the lease
    sweepEveryNCycles = 60,   -- how often the master reclaims dead servers' registry slots
})

manager:give(Leaderboard())
manager:start()

manager.Updated:Connect(function(channelName, serverId, data)
    print(serverId, "updated", channelName, data)
end)
```

`start()` registers this server with the shared registry (claiming a permanent index), begins the election loop, and begins the main publish/combine/read loop on `interval`. `stop()` (also called automatically via `BindToClose`) releases the registry slot and stops the election loop.

## Channel Config Reference

| Field | Type | Description |
|---|---|---|
| `name` | `string` | Unique channel name; also the sub-key inside the server's combined chunk. |
| `refreshInterval` | `number` | Local-only throttle on recomputing `provide()`. Does not affect network timing. Defaults to the manager's interval (recompute every tick). |
| `validators` | `{[validatorType]: params}` | Gates whether `provide()` is recomputed this tick. |
| `binaryLayout` | `{ {name, type} }` | Opt-in. If set, `provide()` must return an array of flat records matching this layout; the channel is packed as a fixed-size buffer instead of JSON. |
| `provide(context)` | `function` | Returns this channel's data for the whole server — a table for JSON channels, an array of records for binary channels. |
| `onReceive(channel, context, serverId, data)` | `function` | Runs when this channel's slice of another server's payload changes. For binary channels, `data` is already unpacked into an array of records. |
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

- If a server's **JSON section** (all non-binary channels + the player directory, combined) exceeds ~32KB, or a channel's `provide()` returns non-JSON-safe data, **that channel is dropped from this tick's chunk with a warning** — the rest of the server's channels still publish normally.
- **Binary channels are exempt** from this drop logic — their size is fixed and known in advance via `binaryLayout`, so there's nothing unbounded to check or drop.
- The combined multi-server output (built by the master each cycle) is **not** subject to a single-key size limit — it's automatically split across as many output shards as needed and reassembled on read, so overall data volume across all servers isn't bounded by any single 32KB cap.

## SyncManager API

- `SyncManager.new(config?): SyncManager`
  - `config.interval` — shared publish/combine/read interval (seconds).
  - `config.ttl` — shared entry expiration (seconds). Defaults to `interval * 3`.
  - `config.autoTriggerOnPlayerEvents` — `boolean`, default `false`. Hooks `Players.PlayerAdded`/`PlayerRemoving` and triggers a publish immediately on each, subject to `autoTriggerDebounce`.
  - `config.autoTriggerDebounce` — `number`, default `1`. Minimum seconds between player-event-triggered publishes; rapid joins/leaves within this window collapse into a single trailing trigger.
  - `config.leaseDuration` — `number`, default `15`. Seconds the master election lease is valid without renewal.
  - `config.renewInterval` — `number`, default `5`. How often a server attempts to claim/renew the master lease. Fully independent of `interval`.
  - `config.sweepEveryNCycles` — `number`, defaults to roughly a 10-minute cadence based on `interval`. How often the master reclaims registry slots from servers whose heartbeat has gone stale (crashed without a graceful shutdown).
- `SyncManager.registerValidator(validatorType: string, impl: table)`
- `:give(channel: SyncChannel)` — adds a channel. Errors if one with the same name already exists.
- `:take(channelName: string)` — removes a channel.
- `:get(channelName: string): SyncChannel?`
- `:start()` — joins the registry, starts the election loop, begins the shared publish/combine/read loop.
- `:stop()` — leaves the registry, stops the election loop. Called automatically on `BindToClose`.
- `:trigger()` — forces an immediate publish, then a combine cycle if this server is currently master, then a read.
- `:triggerPublish()` — forces just this server's chunk publish.
- `:triggerRead()` — forces just a shard read.
- `:refreshServer(serverId: string)` — fetches and merges just one server's own chunk via a single-key read (1 request), firing normal events for whatever changed. Cheaper and lower-latency than waiting for the next combine cycle; used internally by `SyncBridge` on incoming pings.
- `:getCache(channelName: string, serverId: string?)` — cached data for one server, or all servers if `serverId` is omitted. Binary channel data is already unpacked into records.
- `:getServerForPlayer(userId: number): string?` — `O(1)` lookup against the internally maintained directory. Returns `nil` if the player isn't known yet (not online, or not synced yet).

## Events

Fired on the manager, subscribed via `manager.EventName:Connect(...)`:

| Event | Args | Fires when |
|---|---|---|
| `Updated` | `channelName, serverId, data` | A server's data for a channel changed. |
| `ServerAdded` | `channelName, serverId, data` | A server published to a channel for the first time. |
| `ServerRemoved` | `channelName, serverId` | A server's channel entry expired or was removed. |
| `PublishFailed` | `channelName, err` | This server's publish for a channel failed (including channel drops from oversized/invalid JSON data, binary pack failures, or a failed master combine cycle). |
| `ReadFailed` | `channelName, err` | Reading/decoding the combined shard data failed. |

## SyncEngine API

The lower-level MemoryStore + buffer wrapper. The only piece that touches `MemoryStoreService` and `HttpService:JSONEncode`/`JSONDecode` directly. Treats binary channel data as opaque byte blobs it faithfully preserves through combine/shard — it never needs to understand field layouts, only `SyncChannel`/`SyncManager` do that.

- `SyncEngine.new(chunkStoreName: string, shardStoreName: string, output: table)` — `output` must implement `:onReceive(serverId, jsonData, binaryBlobs)` and `:onExpire(serverId)`.
- `:publishChunk(serverId, sortKeyIndex, jsonChannels, binaryChannels, ttl)` — packs the JSON section and any binary blobs into one buffer, writes it to the chunk store keyed by `serverId`, sorted by `sortKeyIndex`.
- `:runMasterCombineCycle(maxIndex, ttl)` — **master only.** Reads every server's chunk in parallel (split into fixed index-space ranges derived from `maxIndex`), merges them, and writes the result as `<=32KB` shards.
- `:readShards(): boolean` — reads and reassembles all current shards, decodes them, and fires `onReceive`/`onExpire` for whatever changed. This is the cheap, everyone-runs-it read path that replaces a flat `readAll()`.
- `:readOneChunk(serverId): (jsonData, binaryBlobs)?` — single-key fetch of just one server's own chunk, bypassing shards entirely. Cheap alternative for targeted refreshes.
- `:Destroy()`

## RegistryStore API

Hands out permanent, reusable numeric indices, used purely so combine reads can be split into predictable parallel ranges. Sharded internally so the registry itself can exceed 32KB at scale.

- `RegistryStore.new(hashMap, freeQueue, counterKey, defaultExpiration, heartbeatStaleSeconds)`
- `:Push(jobId, expiration?): (success, index?)` — claims an index: reuses one from the free queue if available (retrying a few times past leaked/crash-orphaned entries), otherwise mints a new one via an atomic counter.
- `:Heartbeat(index, jobId, expiration?)` — call periodically (e.g. every publish tick) to prove the server holding `index` is still alive.
- `:RemoveAt(index, expiration?)` — releases an index back to the free queue for reuse. Called on graceful shutdown.
- `:SweepDeadSlots(maxIndex, expiration?)` — reclaims indices whose heartbeat has gone stale (a server that crashed without releasing its slot). Intended to run occasionally (see `sweepEveryNCycles`), not every tick.
- `:GetMaxIndex(): number` — the highest index ever minted; tells the master how wide the index space is, independent of how many servers are currently active.

## MasterElection API

Lease-based leader election. Runs entirely on its own timer, independent of the Manager's publish/combine/read interval.

- `MasterElection.new(hashMap, serverId, config?)`
  - `config.leaseDuration` — seconds a claimed lease is valid without renewal.
  - `config.renewInterval` — how often this server attempts to claim/renew.
- `:start()` — begins the background renewal loop; updates `.isMaster` on each attempt.
- `:stop()` — stops the loop; sets `.isMaster` to `false`.
- `.isMaster` — `boolean`, safe to read at any time from the main loop.

If the current master stops renewing (crash, hang, network loss), its lease expires and the next server to attempt a claim becomes master automatically — no manual failover handling needed.

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

- **SyncEvents** — enum of event names (`Updated`, `ServerAdded`, `ServerRemoved`, `PublishFailed`, `ReadFailed`).
- **SyncPriorities** — suggested constants (`Early`, `Normal`, `Late`) for ordering multiple channels' local processing if it ever matters; not enforced, just reference points.
- **SyncValidators** — enum of built-in validator type names (`Cooldown`, `Conditions`), for referencing in channel config instead of hardcoding strings.

## Folder Structure

```
SyncSystem/                 (plain folder)
├── SyncManager.luau         require this directly for the Manager
├── SyncBridge.luau
├── Config/
│   ├── SyncEvents.luau
│   ├── SyncPriorities.luau
│   └── SyncValidators.luau
├── Sync/
│   ├── SyncChannel.luau
│   ├── SyncEngine.luau
│   ├── Signal.luau
│   ├── RegistryStore.luau
│   └── MasterElection.luau
└── Validators/
    ├── Cooldown.luau
    └── Conditions.luau
```

## Known edge cases (by design, not bugs)

- **Player mid-transfer between servers**: a brief window exists where a player can appear on both the old and new server's directory, or briefly on neither, until each side's next publish and the next combine cycle. `autoTriggerOnPlayerEvents` shrinks this window; it isn't eliminated entirely without atomic cross-server coordination, which this system doesn't attempt.
- **Server crash**: a crashed server never gets to release its registry index or publish a graceful removal. Its chunk clears once its `ttl` lapses, and its registry slot is reclaimed by the next `SweepDeadSlots` pass once its heartbeat goes stale — same general shape as any MemoryStore-based liveness pattern, just with an extra explicit reclaim step for the index itself.
- **Master handoff**: if the master crashes mid-cycle, the current shard data simply goes stale until the lease expires and a new master runs a fresh combine cycle. Readers see slightly outdated data for at most `leaseDuration` seconds, not corrupted data — a combine cycle either completes and writes a consistent shard set, or doesn't run at all.
- **Out-of-order Bridge/targeted updates**: the directory is updated incrementally as data is processed, so a late-arriving update could briefly overwrite a newer one if messages arrive out of order. The next regular combine + read cycle corrects this.
- **Leaked registry slots**: on rare occasions (crash between claiming a free index and actually writing to it), a slot can be marked occupied with no real server behind it. This self-heals via the heartbeat sweep the same way a crashed server's slot does — it's treated identically to any other stale entry.