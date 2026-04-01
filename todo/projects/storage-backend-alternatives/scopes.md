# Scopes

## Storage Conformance Suite — shared test harness for all backends (DONE)

## RocksDB Server Backend — replace Fjall on server/Node.js/CLI (DONE)

- [x] Add `rust-rocksdb` dependency behind a `rocksdb` feature flag
- [x] Implement `RocksDBStorage` struct with `Storage` trait, reusing `storage_core.rs` callbacks
- [x] Map key scheme to RocksDB (prefix iterators for range/scan ops)
- [x] Wire write atomicity via RocksDB `WriteBatch` or transactions
- [x] Configure block cache + compaction defaults for server workloads
- [x] Pass the full conformance suite
- [x] Wire into `ServerBuilder` and `ClientBuilder` as a selectable backend
- [x] Verify CI cross-compilation (linux-musl, macOS arm64)

## SQLite Mobile Backend — React Native storage

See `sqlite-mobile-backend-design.md` for implementation details.

## Build Infra — reduce RocksDB toolchain impact

- [ ] Review feature flags so targets that don't need RocksDB (WASM, docs, inspector) never pull it in
- [ ] Ensure Vercel, CI, and local dev builds only install libclang when actually needed
- [ ] Consider making `cli` and `client` features work without a concrete storage backend, letting consumers opt in to `rocksdb` or `fjall` explicitly
