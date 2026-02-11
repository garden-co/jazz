# Batched Tick Orchestration — Status Quo

RuntimeCore is the entry point where all components meet. It owns the [Schema Manager](schema_manager.md) (which wraps the [Query Manager](query_manager.md)), the [Sync Manager](sync_manager.md), the [Object Manager](object_manager.md), and the [Storage](storage.md). Application code calls methods on RuntimeCore; RuntimeCore orchestrates the components.

The central design problem: mutations need to be synchronous (the UI shouldn't wait for the network), but sync messages need to be batched (sending one HTTP request per keystroke would be wasteful). The solution is a two-tick architecture that separates local processing from network I/O.

## RuntimeCore Generics

RuntimeCore is generic over three platform traits. This is how the same core logic runs on native (Tokio), browser (WASM), and tests (no-op) without any `#[cfg]` branching in the business logic:

| Trait        | Purpose                                                | Implementations                                 |
| ------------ | ------------------------------------------------------ | ----------------------------------------------- |
| `Storage`    | Synchronous data persistence (objects, blobs, indices) | MemoryStorage, BfTreeStorage                    |
| `Scheduler`  | Async batched_tick scheduling with debounce            | TokioScheduler, WasmScheduler, NoopScheduler    |
| `SyncSender` | Network message dispatch                               | CallbackSyncSender, JsSyncSender, VecSyncSender |

> `crates/groove/src/runtime_core.rs:216-237` (RuntimeCore definition)
> `crates/groove/src/runtime_core.rs:47-61` (Scheduler and SyncSender traits)
> `crates/groove/src/storage/mod.rs:67-195` (Storage trait)

## Two Tick Methods

### immediate_tick()

The "fast path" — runs synchronously within the current call. This is what makes local mutations feel instant: insert a row, and the query subscription fires in the same call stack.

1. Calls `schema_manager.process()` **twice** (second pass handles deferred schema availability)
2. Collects subscription updates
3. Handles one-shot query resolution
4. Processes received persistence acks and resolves durability watchers
5. Schedules `batched_tick()` if sync messages are pending

All storage operations happen **synchronously during process()** via the Storage trait — they are NOT deferred to batched_tick.

> `crates/groove/src/runtime_core.rs:306-402`

### batched_tick()

The "network path" — runs asynchronously, scheduled by the platform's Scheduler. Multiple mutations may happen between batched ticks, and their sync messages get coalesced into a single batch. This is the only place where network I/O happens.

1. Takes all queued sync messages from outbox, sends via SyncSender
2. Calls `handle_sync_messages()` which applies parked sync messages and calls `immediate_tick()`
3. Flushes any NEW sync messages generated during step 2 (second outbox drain)

The second flush is critical: the scheduler's debounce prevents `immediate_tick()` from scheduling another `batched_tick()` while one is in progress, so new outbox entries must be flushed here.

> `crates/groove/src/runtime_core.rs:404-436`

### Parked Sync Messages

`park_sync_message()` stores incoming sync messages and schedules a `batched_tick()` for processing. This decouples message receipt from processing.

> `crates/groove/src/runtime_core.rs:461-464`

## Scheduler Debounce

Both platform implementations use a boolean flag to prevent overlapping batched_tick calls:

- **Tokio**: Sets flag before spawn, clears after completion
- **WASM**: Sets flag before spawn_local, clears before calling batched_tick

> `crates/groove-tokio/src/lib.rs:72-90` (TokioScheduler)
> `crates/groove-wasm/src/runtime.rs:91-106` (WasmScheduler)

## Platform Implementations

### Tokio (groove-tokio)

`TokioScheduler` spawns `batched_tick()` via tokio task. `CallbackSyncSender` sends sync messages via closure.

> `crates/groove-tokio/src/lib.rs:42-91` (TokioScheduler), `164-206` (TokioRuntime)

### WASM (groove-wasm)

`WasmScheduler` uses `spawn_local`. `JsSyncSender` serializes sync messages to JSON and calls a JS callback.

> `crates/groove-wasm/src/runtime.rs:66-141`

## CRUD Operation Flow

Each CRUD method (insert, update, delete) on RuntimeCore:

1. Calls SchemaManager method (which synchronously persists via Storage)
2. Calls `immediate_tick()` to settle subscriptions and queue sync messages
3. batched_tick is scheduled to send queued sync messages

> `crates/groove/src/runtime_core.rs:629-695`

## Testing

Tests use `NoopScheduler` (no-op scheduling) and `VecSyncSender` (collects messages in a Vec). Manual calls to `immediate_tick()` and `batched_tick()` drive execution.

> `crates/groove/src/runtime_core.rs:68-101` (NoopScheduler, VecSyncSender)
> `crates/groove/src/runtime_core.rs:904-1049` (test setup)

## Key Files

| File                         | Purpose                                            |
| ---------------------------- | -------------------------------------------------- |
| `groove/src/runtime_core.rs` | RuntimeCore with immediate_tick/batched_tick       |
| `groove/src/storage/mod.rs`  | Storage trait (replaces IoHandler for persistence) |
| `groove-tokio/src/lib.rs`    | TokioScheduler and TokioRuntime                    |
| `groove-wasm/src/runtime.rs` | WasmScheduler and WasmRuntime                      |
