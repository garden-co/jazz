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

## redb Mobile Backend — pure-Rust candidate for React Native

- [ ] Add `redb` dependency behind a `redb` feature flag
- [ ] Implement `RedbStorage` struct with `Storage` trait, reusing `storage_core.rs` callbacks
- [ ] Map key scheme to redb tables + range scans
- [ ] Pass the full conformance suite
- [ ] Verify builds for iOS (xcframework) and Android (JNI/NDK) targets
- [ ] Add redb engine to `crates/opfs-btree/benches/compare_native.rs` cross-engine benchmark and compare against Fjall, RocksDB, OpfsBTree, BfTree

## heed Mobile Backend — LMDB candidate for React Native

- [ ] Add `heed` dependency behind a `heed` feature flag
- [ ] Implement `HeedStorage` struct with `Storage` trait, reusing `storage_core.rs` callbacks
- [ ] Map key scheme to LMDB databases + cursor range scans
- [ ] Handle map size configuration (sensible default + growth strategy)
- [ ] Pass the full conformance suite
- [ ] Verify builds for iOS (xcframework) and Android (JNI/NDK) targets
- [ ] Add heed/LMDB engine to `crates/opfs-btree/benches/compare_native.rs` cross-engine benchmark and compare against Fjall, RocksDB, OpfsBTree, BfTree
- [ ] Test under iOS memory pressure (mmap reclaim behavior)
