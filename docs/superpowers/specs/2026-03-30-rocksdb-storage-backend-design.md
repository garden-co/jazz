# RocksDB Storage Backend

Replace Fjall with RocksDB as the default persistent storage engine for all native platforms (server, Node.js client, CLI).

## Context

Jazz uses a `Storage` trait with callback-based core logic (`storage_core.rs`). Backends provide `get`/`set`/`delete`/`scan_prefix`/`scan_key_range` primitives; all higher-level logic (objects, branches, commits, indices, catalogue manifests) is shared. FjallStorage is the current native backend, gated behind a `fjall` feature flag. RocksDB replaces it as the default — Fjall remains available as a non-default feature flag.

## Architecture

### Module

New file: `crates/jazz-tools/src/storage/rocksdb.rs`

Gated: `#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]`

### Struct

```rust
pub struct RocksDBStorage {
    inner: RefCell<Option<RocksDBInner>>,
}

struct RocksDBInner {
    db: TransactionDB,
}
```

Same `RefCell<Option<...>>` pattern as FjallStorage. `close()` takes the inner via `Option::take()`, `with_inner()` helper for all operations.

### Key organization

Single column family (default). The existing composite key scheme (`obj:`, `idx:`, `ack:`, `catman:`) maps directly — RocksDB's default lexicographic byte ordering matches Fjall's. No prefix extractor; range scans use iterator seeks with prefix/bound checks.

### Write atomicity

`TransactionDB` with pessimistic transactions. This provides read-your-writes semantics within a single Storage method call, matching Fjall's `write_tx()` behavior. Required because `storage_core.rs` interleaves reads and writes within operations (e.g., `append_commit_core` reads the current commit list, appends, then writes it back).

Read-only operations (`load_object_metadata`, `load_branch`, `index_lookup`, `index_range`, `index_scan_all`) use direct `db.get()` / `db.iterator()` without transactions.

### Backend callbacks

Each mutating Storage method:

1. Opens a pessimistic transaction: `inner.db.transaction()`
2. Passes closures to the corresponding `storage_core.rs` function:
   - `get(key)` → `txn.get(key.as_bytes())`
   - `set(key, value)` → `txn.put(key.as_bytes(), value)`
   - `delete(key)` → `txn.delete(key.as_bytes())`
   - `scan_prefix(prefix)` → `txn.iterator()` with `IteratorMode::From(prefix, Direction::Forward)`, take while key starts with prefix
   - `scan_key_range(start, end)` → iterator from start bound, take while key < end bound
3. Commits: `txn.commit()`

### Lifecycle

- `open(path: &Path, cache_size_bytes: usize)` → opens `TransactionDB` with tuning options
- `flush()` → `db.flush()`
- `flush_wal()` → `db.flush_wal(true)`
- `close()` → `self.inner.take()` — RocksDB cleans up on `Drop`

### Tuning defaults

| Setting      | Value                                    | Rationale                             |
| ------------ | ---------------------------------------- | ------------------------------------- |
| Block cache  | `cache_size_bytes` param (64 MB default) | Matches current Fjall default         |
| Compaction   | Level compaction                         | RocksDB default, good general-purpose |
| Bloom filter | 10 bits/key on block-based table         | Speeds up point lookups               |
| Compression  | LZ4 all levels, Zstd bottommost          | Good throughput/size tradeoff         |
| WAL          | Enabled                                  | Crash recovery                        |

All tuneable later without data migration.

## Feature flag wiring

### Cargo.toml changes

```toml
[dependencies]
rocksdb = { version = "...", optional = true }

[features]
rocksdb = ["dep:rocksdb"]
client = ["runtime-tokio", "transport-http", "rocksdb", "dep:thiserror", "dep:tokio"]
cli = ["runtime-tokio", "transport", "rocksdb", ...]
```

`fjall` feature stays but is no longer a dependency of `client` or `cli`.

### ServerBuilder

`build_main_storage()` switches from `FjallStorage::open` to `RocksDBStorage::open`. File extension changes from `jazz.fjall` to `jazz.rocksdb` (or similar) to avoid confusion.

### ClientStorage enum

```rust
pub enum ClientStorage {
    RocksDB,  // replaces Fjall as default
    Memory,
}
```

## Testing

### Conformance suite

```rust
storage_conformance_tests_persistent!(
    rocksdb,
    || {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.rocksdb");
        let storage = RocksDBStorage::open(&path, 8 * 1024 * 1024).unwrap();
        std::mem::forget(dir);
        Box::new(storage) as Box<dyn Storage>
    },
    |path: &std::path::Path| {
        Box::new(RocksDBStorage::open(path, 8 * 1024 * 1024).unwrap()) as Box<dyn Storage>
    }
);
```

All 25+ conformance tests + 2 persistence tests must pass.

### Integration tests

Existing Fjall integration tests (`fjall_storage_integration.rs`) get a RocksDB counterpart covering: large dataset correctness, update/delete, deep update history, multi-table isolation, index queries, restart persistence, catalogue manifest survival.

### CI

Verify cross-compilation for linux-musl and macOS arm64 targets. RocksDB's C++ build chain is the main risk — `rust-rocksdb` crate handles this via bundled source or system library detection.
