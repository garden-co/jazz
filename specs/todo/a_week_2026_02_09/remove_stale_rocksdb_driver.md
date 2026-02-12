# Remove Stale RocksDB Driver

`crates/groove-rocksdb/` is dead code. It uses the old async `StorageRequest`/`StorageResponse` API that was removed during the synchronous storage rewrite. It's not in the workspace `Cargo.toml`, doesn't compile, and has been fully superseded by `BfTreeStorage`.

## Action

Delete `crates/groove-rocksdb/` entirely.
