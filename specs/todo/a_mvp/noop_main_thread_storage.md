# No-Op Main Thread Storage — TODO

Replace the main-thread storage implementation with a no-op that stores nothing locally. The worker (OPFS) is the source of truth; the main thread doesn't need its own copy of the data.

## Motivation

Today the main thread holds a full in-memory storage (`MemoryIoHandler` or similar). This duplicates RAM for every object and index already held by the worker's OPFS-backed storage. The main thread only needs:

- Objects/rows it receives over sync from the worker
- Enough structure to evaluate reactive queries against incoming data

Since the query graph already processes incoming sync messages and emits to subscribers, the storage layer on the main thread can be a no-op: writes succeed silently (data lives in the query graph's materialized state), reads return empty (anything not in memory comes from the worker via sync).

## Design

Implement a `NoopIoHandler` (or `MainThreadIoHandler`) that satisfies the `IoHandler` trait:

- `object_get` → `None`
- `object_put` → no-op (silent success)
- `object_delete` → no-op
- `index_insert` / `index_remove` → no-op
- `index_range` → empty iterator

The main thread runtime uses this instead of `MemoryIoHandler`. All persistent state is delegated to the worker.

## Open Questions

- Does the query graph's `row_loader` need to change behavior when storage returns nothing? (It may already handle `None` gracefully.)
- Are there any main-thread code paths that depend on reading back what was written to storage (read-after-write)?
- Should this be the default for browser builds, or opt-in?
