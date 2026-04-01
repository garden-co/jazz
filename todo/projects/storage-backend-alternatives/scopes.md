# Scopes

## Storage Conformance Suite — shared test harness for all backends (DONE)

## RocksDB Server Backend — replace Fjall on server/Node.js/CLI

- [ ] Add `rust-rocksdb` dependency behind a `rocksdb` feature flag
- [ ] Implement `RocksDBStorage` struct with `Storage` trait, reusing `storage_core.rs` callbacks
- [ ] Map key scheme to RocksDB (prefix iterators for range/scan ops)
- [ ] Wire write atomicity via RocksDB `WriteBatch` or transactions
- [ ] Configure block cache + compaction defaults for server workloads
- [ ] Pass the full conformance suite
- [ ] Wire into `ServerBuilder` and `ClientBuilder` as a selectable backend
- [ ] Verify CI cross-compilation (linux-musl, macOS arm64)

## SQLite Mobile Backend — React Native storage

- [ ] Add `rusqlite` dependency behind a `sqlite` feature flag
- [ ] Implement `SqliteStorage` struct with `Storage` trait, reusing `storage_core.rs` callbacks
- [ ] Map key scheme to a single `kv(key TEXT PRIMARY KEY, value BLOB)` table with prefix range queries
- [ ] Configure WAL mode + appropriate PRAGMA settings for mobile
- [ ] Pass the full conformance suite
- [ ] Verify builds for iOS (xcframework) and Android (JNI/NDK) targets
- [ ] Add SQLite engine to `crates/opfs-btree/benches/compare_native.rs` cross-engine benchmark and compare against Fjall, RocksDB, OpfsBTree, BfTree
