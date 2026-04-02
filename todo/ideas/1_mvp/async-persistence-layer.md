# Async Persistence Layer

## What

Non-blocking persistence for mobile. The Storage trait is synchronous, so RN currently blocks the UI on I/O. We need an async boundary between Jazz and the persisted storage.

## Why

RN <-> Jazz can only be sync due to how React works. Since we can't block the UI on I/O, we need a separate async boundary for persistence — same problem the web solves with MemoryStorage <-> PersistedStorage via WebWorkers.

## Options discussed

**Option A — Replicate the web architecture:** RN <-> Jazz (MemoryStorage) <-> Jazz (PersistedStorage). On RN we don't have WebWorkers, but we can stay in Rust with threads and channels. Needs exploration since RN will differ from web at implementation level. It would be nice to come up with a model that abstracts the WebWorker layer.

**Option B — Async Storage trait:** Make the Storage trait async. The main risk is that supporting both sync and async storage would be hard to abstract correctly, and unclear if the complexity is worth it. Could explore having only the async trait, which would also open new optimizations for opfs-btree (WebWorkers can spawn WebWorkers). Antonio notes this is viable on RN since we have Rust threads, channels, and lock primitives.

## Rough appetite

medium
