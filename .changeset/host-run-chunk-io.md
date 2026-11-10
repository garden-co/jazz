---
"jazz-tools": patch
---

A Rust host can run a store's chunk-store I/O itself: with `DbConfig::with_host_chunk_io()`, `Db::tick` never waits on the chunk store, and the host takes each batch with `Db::take_chunk_io`, runs it with `ChunkIo::run` wherever it likes and applies the outcome with `Db::finish_chunk_io`. A network store then can't stall a synchronous tick.
