# Batched Tick Orchestration — Status Quo

`RuntimeCore` is where Jazz's local-first promise becomes concrete.

Application code wants two things at the same time:

- local writes and reads should feel immediate
- network traffic should still be batched and orderly

The runtime gets both by splitting work into two cooperating loops:

- `immediate_tick()` for local settling
- `batched_tick()` for queued sync I/O

## What RuntimeCore Owns

Today, `RuntimeCore` owns:

- `SchemaManager`
- `SyncManager`
- `Storage`
- a small `MonotonicClock`
- a `Scheduler`
- a `SyncSender`

`QueryManager` lives inside `SchemaManager`, so the whole relational stack still settles inside one runtime entry point.

## Why There Are Two Ticks

### `immediate_tick()`

This is the fast local path.

It is responsible for:

- applying pending local/runtime work
- letting schema and query state settle
- collecting subscription updates
- resolving one-shot query futures
- preparing outbound sync messages
- scheduling a batched tick if network work is waiting

This is why a local insert can update a subscription immediately instead of waiting for an async round-trip.

### `batched_tick()`

This is the queued sync path.

It is responsible for:

- draining the current outbox
- handing those payloads to the `SyncSender`
- applying parked inbound sync messages
- running local settling again if those messages changed state
- draining any newly generated outbound messages before finishing

That second drain is important. Sync work often produces more sync work, and the runtime should flush that in the same batched turn instead of relying on a lucky second scheduling pass.

## The Current Execution Shape

```text
local mutation
  -> storage update happens synchronously
  -> immediate_tick settles local queries/subscriptions
  -> sync payloads are queued
  -> batched_tick sends them later as one batch
```

For inbound sync:

```text
message arrives
  -> runtime parks it
  -> batched_tick applies it
  -> immediate_tick settles local subscriptions
  -> any resulting outgoing sync is flushed
```

## Storage Flushing

The runtime no longer flushes storage blindly on every batched tick.

Instead it tracks whether the tick actually performed writes. That gives a nice balance:

- read-only ticks stay cheap
- write ticks still advance durability
- browser OPFS and native backends do not pay a full durability barrier when nothing changed

## Scheduler and SyncSender

The same core logic runs on several platforms because `RuntimeCore` is generic over two small platform traits:

- `Scheduler` decides how a future batched tick gets scheduled
- `SyncSender` decides how outbound sync messages leave the runtime

That means:

- tests can use immediate/no-op scheduling
- browser runtimes can use `spawn_local`
- Tokio/native runtimes can use async tasks

without changing the relational logic itself.

## Key Files

| File | Purpose |
| --- | --- |
| `crates/jazz-tools/src/runtime_core.rs` | RuntimeCore definition and top-level API |
| `crates/jazz-tools/src/runtime_core/ticks.rs` | Tick orchestration |
| `crates/jazz-tools/src/runtime_core/writes.rs` | Local write helpers |
| `crates/jazz-tools/src/monotonic_clock.rs` | Monotonic timestamp allocation |
| `crates/jazz-wasm/src/runtime.rs` | Browser scheduler/runtime bridge |
| `crates/jazz-napi/src/lib.rs` | Native embedded runtime bridge |
