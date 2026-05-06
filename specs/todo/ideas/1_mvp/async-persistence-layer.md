# Async Persistence Layer

## What

Non-blocking persistence for mobile. The Storage trait is synchronous, so RN currently blocks the UI on I/O. We need an async boundary between Jazz and the persisted storage.

## Notes

- RN <-> Jazz can only be sync due to how React works. Since we can't block the UI on I/O, we need a separate async boundary for persistence, similar to the web split between `MemoryStorage` and `PersistedStorage`.
- Option A: replicate the web architecture with RN <-> Jazz (`MemoryStorage`) <-> Jazz (`PersistedStorage`), using Rust threads and channels instead of WebWorkers.
- Option B: make the `Storage` trait async. The main risk is complexity around supporting both sync and async storage cleanly.
- RN has Rust threads, channels, and locking primitives available, so both directions are technically viable.
