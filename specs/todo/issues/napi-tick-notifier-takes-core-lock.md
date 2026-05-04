# NapiTickNotifier::notify acquires the core mutex just to schedule a tick

## What

`crates/jazz-napi/src/lib.rs:1265-1275` locks `self.core` only to call `scheduler().schedule_batched_tick()`. Every inbound WS frame on NAPI goes through `tick.notify()`, which means every frame serialises behind every other core operation (queries, writes, subscribes). Under heavy sync load (initial replay, bulk updates) this creates unnecessary contention on the single core mutex.

## Priority

medium

## Notes

- Compare to `NativeTickNotifier` and `RnTickNotifier`, which hold a cloned scheduler directly and don't take the core lock.
- Fix: store a cloned `NapiScheduler` on the notifier (it clones cheaply via internal Arcs) and call `schedule_batched_tick()` without going through the core.
