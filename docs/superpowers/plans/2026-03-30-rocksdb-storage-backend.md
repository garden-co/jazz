# RocksDB Storage Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Fjall with RocksDB as the default persistent storage engine for all native platforms.

**Architecture:** New `RocksDBStorage` struct implementing the `Storage` trait via `storage_core.rs` callbacks, using `TransactionDB` for read-your-writes semantics. Single column family, same composite key scheme as Fjall. Feature flag `rocksdb` replaces `fjall` as the default for `client` and `cli` features.

**Tech Stack:** `rocksdb` crate (rust-rocksdb), RocksDB `TransactionDB`, LZ4/Zstd compression, bloom filters.

---

### Task 1: Add `rocksdb` dependency and feature flag

**Files:**

- Modify: `crates/jazz-tools/Cargo.toml`

- [ ] **Step 1: Add the `rocksdb` crate dependency and feature flag**

In `crates/jazz-tools/Cargo.toml`, add the dependency alongside `fjall`:

```toml
# In [dependencies], after the fjall line:
rocksdb = { version = "0.22", features = ["lz4", "zstd"], optional = true }
```

Add the feature flag in `[features]`:

```toml
rocksdb = ["dep:rocksdb"]
```

Change `client` and `cli` features to use `rocksdb` instead of `fjall`:

```toml
client = ["runtime-tokio", "transport-http", "rocksdb", "dep:thiserror", "dep:tokio"]
cli = [
  "runtime-tokio",
  "transport",
  "rocksdb",
  "dep:tokio",
  "dep:axum",
  "dep:clap",
  "dep:tracing-subscriber",
  "dep:tower-http",
  "dep:async-stream",
  "dep:jsonwebtoken",
  "dep:base64",
  "dep:bytes",
  "dep:reqwest",
]
```

Keep the `fjall` feature definition unchanged — it's still available, just no longer a default dependency of `client`/`cli`.

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p jazz-tools --features rocksdb`
Expected: compiles (the feature flag exists, the dep is available, nothing uses it yet)

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-tools/Cargo.toml
git commit -m "feat: add rocksdb dependency and feature flag"
```

---

### Task 2: Implement `RocksDBStorage` struct with `open` and `close`

**Files:**

- Create: `crates/jazz-tools/src/storage/rocksdb.rs`
- Modify: `crates/jazz-tools/src/storage/mod.rs`

- [ ] **Step 1: Write a test for open and close**

Create `crates/jazz-tools/src/storage/rocksdb.rs` with:

```rust
//! RocksDB-backed Storage implementation.
//!
//! Uses one TransactionDB with a single column family and the same
//! UTF-8 key encoding scheme as the other native backends.

use std::cell::RefCell;
use std::path::Path;

use rocksdb::{
    BlockBasedOptions, Options, TransactionDB, TransactionDBOptions,
};

use super::StorageError;

struct RocksDBInner {
    db: TransactionDB,
}

pub struct RocksDBStorage {
    inner: RefCell<Option<RocksDBInner>>,
}

impl RocksDBStorage {
    pub fn open(path: impl AsRef<Path>, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        // Block-based table with bloom filter and LRU cache
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        let cache = rocksdb::Cache::new_lru_cache(cache_size_bytes);
        block_opts.set_block_cache(&cache);
        opts.set_block_based_table_factory(&block_opts);

        // Compression: LZ4 default, Zstd for bottommost level
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);

        let txn_db_opts = TransactionDBOptions::default();
        let db = TransactionDB::open(&opts, &txn_db_opts, path.as_ref())
            .map_err(|e| StorageError::IoError(format!("rocksdb open: {e}")))?;

        Ok(Self {
            inner: RefCell::new(Some(RocksDBInner { db })),
        })
    }

    fn with_inner<T>(
        &self,
        f: impl FnOnce(&RocksDBInner) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let inner = self.inner.borrow();
        let inner = inner
            .as_ref()
            .ok_or_else(|| StorageError::IoError("rocksdb storage already closed".to_string()))?;
        f(inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_and_close() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.rocksdb");
        let storage = RocksDBStorage::open(&db_path, 8 * 1024 * 1024).unwrap();
        storage.close().unwrap();

        // Reopen should work after close
        let reopened = RocksDBStorage::open(&db_path, 8 * 1024 * 1024).unwrap();
        reopened.close().unwrap();
    }
}
```

- [ ] **Step 2: Register the module in `mod.rs`**

In `crates/jazz-tools/src/storage/mod.rs`, after the fjall module declarations (lines 18-21), add:

```rust
#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
mod rocksdb;
#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
pub use rocksdb::RocksDBStorage;
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p jazz-tools --features rocksdb open_and_close -- --nocapture`
Expected: FAIL — `close()` method doesn't exist yet on `RocksDBStorage`.

- [ ] **Step 4: Implement `Storage` trait stubs with `close`**

Add to `rocksdb.rs`, after `with_inner`:

```rust
use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::Value;
use crate::sync_manager::DurabilityTier;

use super::{
    CatalogueManifest, CatalogueManifestOp, LoadedBranch, Storage,
};

impl Storage for RocksDBStorage {
    fn create_object(&mut self, _id: ObjectId, _metadata: HashMap<String, String>) -> Result<(), StorageError> {
        todo!()
    }

    fn load_object_metadata(&self, _id: ObjectId) -> Result<Option<HashMap<String, String>>, StorageError> {
        todo!()
    }

    fn load_branch(&self, _object_id: ObjectId, _branch: &BranchName) -> Result<Option<LoadedBranch>, StorageError> {
        todo!()
    }

    fn append_commit(&mut self, _object_id: ObjectId, _branch: &BranchName, _commit: Commit) -> Result<(), StorageError> {
        todo!()
    }

    fn delete_commit(&mut self, _object_id: ObjectId, _branch: &BranchName, _commit_id: CommitId) -> Result<(), StorageError> {
        todo!()
    }

    fn set_branch_tails(&mut self, _object_id: ObjectId, _branch: &BranchName, _tails: Option<HashSet<CommitId>>) -> Result<(), StorageError> {
        todo!()
    }

    fn store_ack_tier(&mut self, _commit_id: CommitId, _tier: DurabilityTier) -> Result<(), StorageError> {
        todo!()
    }

    fn append_catalogue_manifest_op(&mut self, _app_id: ObjectId, _op: CatalogueManifestOp) -> Result<(), StorageError> {
        todo!()
    }

    fn append_catalogue_manifest_ops(&mut self, _app_id: ObjectId, _ops: &[CatalogueManifestOp]) -> Result<(), StorageError> {
        todo!()
    }

    fn load_catalogue_manifest(&self, _app_id: ObjectId) -> Result<Option<CatalogueManifest>, StorageError> {
        todo!()
    }

    fn index_insert(&mut self, _table: &str, _column: &str, _branch: &str, _value: &Value, _row_id: ObjectId) -> Result<(), StorageError> {
        todo!()
    }

    fn index_remove(&mut self, _table: &str, _column: &str, _branch: &str, _value: &Value, _row_id: ObjectId) -> Result<(), StorageError> {
        todo!()
    }

    fn index_lookup(&self, _table: &str, _column: &str, _branch: &str, _value: &Value) -> Vec<ObjectId> {
        todo!()
    }

    fn index_range(&self, _table: &str, _column: &str, _branch: &str, _start: Bound<&Value>, _end: Bound<&Value>) -> Vec<ObjectId> {
        todo!()
    }

    fn index_scan_all(&self, _table: &str, _column: &str, _branch: &str) -> Vec<ObjectId> {
        todo!()
    }

    fn flush(&self) {
        if let Some(inner) = self.inner.borrow().as_ref() {
            let _ = inner.db.flush();
        }
    }

    fn flush_wal(&self) {
        if let Some(inner) = self.inner.borrow().as_ref() {
            let _ = inner.db.flush_wal(true);
        }
    }

    fn close(&self) -> Result<(), StorageError> {
        let Some(inner) = self.inner.borrow_mut().take() else {
            return Ok(());
        };

        inner
            .db
            .flush()
            .map_err(|e| StorageError::IoError(format!("rocksdb flush on close: {e}")))?;
        drop(inner);
        Ok(())
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p jazz-tools --features rocksdb open_and_close -- --nocapture`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-tools/src/storage/rocksdb.rs crates/jazz-tools/src/storage/mod.rs
git commit -m "feat: add RocksDBStorage struct with open/close lifecycle"
```

---

### Task 3: Implement read/write helpers and object storage methods

**Files:**

- Modify: `crates/jazz-tools/src/storage/rocksdb.rs`

- [ ] **Step 1: Add the conformance test invocation**

Add at the bottom of the `#[cfg(test)] mod tests` block in `rocksdb.rs`:

```rust
    mod rocksdb_conformance {
        use crate::storage::Storage;
        use crate::storage::rocksdb::RocksDBStorage;
        use crate::storage_conformance_tests_persistent;

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
    }
```

- [ ] **Step 2: Run conformance tests to see them fail**

Run: `cargo test -p jazz-tools --features rocksdb rocksdb_conformance -- --nocapture 2>&1 | head -30`
Expected: FAIL — all tests panic with `todo!()`.

- [ ] **Step 3: Add read/write helper methods**

Add these helper methods to the `impl RocksDBStorage` block, after `with_inner`. These mirror Fjall's helpers but use RocksDB's `Transaction` type:

```rust
    fn get_from_db(
        db: &TransactionDB,
        key: &str,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        db.get(key.as_bytes())
            .map_err(|e| StorageError::IoError(format!("rocksdb get: {e}")))
    }

    fn get_from_txn(
        txn: &rocksdb::Transaction<'_, TransactionDB>,
        key: &str,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        txn.get(key.as_bytes())
            .map_err(|e| StorageError::IoError(format!("rocksdb txn get: {e}")))
    }

    fn scan_prefix_from_db(
        db: &TransactionDB,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let prefix_bytes = prefix.as_bytes();
        let mut out = Vec::new();
        let iter = db.iterator(rocksdb::IteratorMode::From(prefix_bytes, rocksdb::Direction::Forward));
        for item in iter {
            let (key, value) = item
                .map_err(|e| StorageError::IoError(format!("rocksdb prefix iter: {e}")))?;
            if !key.starts_with(prefix_bytes) {
                break;
            }
            let key_str = String::from_utf8(key.to_vec())
                .map_err(|e| StorageError::IoError(format!("rocksdb invalid key utf8: {e}")))?;
            out.push((key_str, value.to_vec()));
        }
        Ok(out)
    }

    fn scan_prefix_keys_from_db(
        db: &TransactionDB,
        prefix: &str,
    ) -> Result<Vec<String>, StorageError> {
        let prefix_bytes = prefix.as_bytes();
        let mut out = Vec::new();
        let iter = db.iterator(rocksdb::IteratorMode::From(prefix_bytes, rocksdb::Direction::Forward));
        for item in iter {
            let (key, _) = item
                .map_err(|e| StorageError::IoError(format!("rocksdb prefix key iter: {e}")))?;
            if !key.starts_with(prefix_bytes) {
                break;
            }
            let key_str = String::from_utf8(key.to_vec())
                .map_err(|e| StorageError::IoError(format!("rocksdb invalid key utf8: {e}")))?;
            out.push(key_str);
        }
        Ok(out)
    }

    fn scan_key_range_from_db(
        db: &TransactionDB,
        start: &str,
        end: &str,
    ) -> Result<Vec<String>, StorageError> {
        let end_bytes = end.as_bytes();
        let mut out = Vec::new();
        let iter = db.iterator(rocksdb::IteratorMode::From(start.as_bytes(), rocksdb::Direction::Forward));
        for item in iter {
            let (key, _) = item
                .map_err(|e| StorageError::IoError(format!("rocksdb range iter: {e}")))?;
            if key.as_ref() >= end_bytes {
                break;
            }
            let key_str = String::from_utf8(key.to_vec())
                .map_err(|e| StorageError::IoError(format!("rocksdb invalid key utf8: {e}")))?;
            out.push(key_str);
        }
        Ok(out)
    }

    fn commit_txn(txn: rocksdb::Transaction<'_, TransactionDB>) -> Result<(), StorageError> {
        txn.commit()
            .map_err(|e| StorageError::IoError(format!("rocksdb commit: {e}")))
    }
```

- [ ] **Step 4: Implement object storage methods**

Replace the `todo!()` stubs for `create_object` and `load_object_metadata` with real implementations using `storage_core.rs`. Add the necessary imports at the top of the file:

```rust
use super::storage_core::{
    append_catalogue_manifest_op_core, append_catalogue_manifest_ops_core, append_commit_core,
    create_object_core, delete_commit_core, index_insert_core, index_lookup_core,
    index_range_core, index_remove_core, index_scan_all_core, load_branch_core,
    load_catalogue_manifest_core, load_object_metadata_core, set_branch_tails_core,
    store_ack_tier_core,
};
```

Replace `create_object`:

```rust
    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = inner.db.transaction();
            create_object_core(id, metadata, |key, value| {
                txn.put(key.as_bytes(), value)
                    .map_err(|e| StorageError::IoError(format!("rocksdb put: {e}")))
            })?;
            Self::commit_txn(txn)
        })
    }
```

Replace `load_object_metadata`:

```rust
    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        self.with_inner(|inner| {
            load_object_metadata_core(id, |key| Self::get_from_db(&inner.db, key))
        })
    }
```

- [ ] **Step 5: Run the object conformance tests**

Run: `cargo test -p jazz-tools --features rocksdb rocksdb_conformance::rocksdb::test_object -- --nocapture`
Expected: PASS for `test_object_create_and_load_metadata`, `test_object_load_nonexistent_returns_none`, `test_object_metadata_isolation`

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-tools/src/storage/rocksdb.rs
git commit -m "feat: implement RocksDBStorage object storage and read/write helpers"
```

---

### Task 4: Implement branch and commit methods

**Files:**

- Modify: `crates/jazz-tools/src/storage/rocksdb.rs`

- [ ] **Step 1: Implement `load_branch`**

Replace the `todo!()` stub:

```rust
    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        self.with_inner(|inner| {
            load_branch_core(
                object_id,
                branch,
                |key| Self::get_from_db(&inner.db, key),
                |prefix| Self::scan_prefix_from_db(&inner.db, prefix),
            )
        })
    }
```

- [ ] **Step 2: Implement `append_commit`**

Replace the `todo!()` stub. This needs a transaction for read-your-writes:

```rust
    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = inner.db.transaction();
            append_commit_core(
                object_id,
                branch,
                commit,
                |key| Self::get_from_txn(&txn, key),
                |key, value| {
                    txn.put(key.as_bytes(), value)
                        .map_err(|e| StorageError::IoError(format!("rocksdb put: {e}")))
                },
            )?;
            Self::commit_txn(txn)
        })
    }
```

- [ ] **Step 3: Implement `delete_commit`**

```rust
    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = inner.db.transaction();
            delete_commit_core(
                object_id,
                branch,
                commit_id,
                |key| Self::get_from_txn(&txn, key),
                |key, value| {
                    txn.put(key.as_bytes(), value)
                        .map_err(|e| StorageError::IoError(format!("rocksdb put: {e}")))
                },
                |key| {
                    txn.delete(key.as_bytes())
                        .map_err(|e| StorageError::IoError(format!("rocksdb delete: {e}")))
                },
            )?;
            Self::commit_txn(txn)
        })
    }
```

- [ ] **Step 4: Implement `set_branch_tails`**

```rust
    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = inner.db.transaction();
            set_branch_tails_core(
                object_id,
                branch,
                tails,
                |key, value| {
                    txn.put(key.as_bytes(), value)
                        .map_err(|e| StorageError::IoError(format!("rocksdb put: {e}")))
                },
                |key| {
                    txn.delete(key.as_bytes())
                        .map_err(|e| StorageError::IoError(format!("rocksdb delete: {e}")))
                },
            )?;
            Self::commit_txn(txn)
        })
    }
```

- [ ] **Step 5: Run branch/commit conformance tests**

Run: `cargo test -p jazz-tools --features rocksdb rocksdb_conformance::rocksdb::test_branch -- --nocapture && cargo test -p jazz-tools --features rocksdb rocksdb_conformance::rocksdb::test_commit -- --nocapture`
Expected: PASS for all branch and commit tests.

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-tools/src/storage/rocksdb.rs
git commit -m "feat: implement RocksDBStorage branch and commit operations"
```

---

### Task 5: Implement index operations

**Files:**

- Modify: `crates/jazz-tools/src/storage/rocksdb.rs`

- [ ] **Step 1: Implement `index_insert` and `index_remove`**

Replace the `todo!()` stubs:

```rust
    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = inner.db.transaction();
            index_insert_core(table, column, branch, value, row_id, |key, bytes| {
                txn.put(key.as_bytes(), bytes)
                    .map_err(|e| StorageError::IoError(format!("rocksdb put: {e}")))
            })?;
            Self::commit_txn(txn)
        })
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = inner.db.transaction();
            index_remove_core(table, column, branch, value, row_id, |key| {
                txn.delete(key.as_bytes())
                    .map_err(|e| StorageError::IoError(format!("rocksdb delete: {e}")))
            })?;
            Self::commit_txn(txn)
        })
    }
```

- [ ] **Step 2: Implement `index_lookup`, `index_range`, and `index_scan_all`**

```rust
    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        self.with_inner(|inner| {
            Ok(index_lookup_core(table, column, branch, value, |prefix| {
                Self::scan_prefix_keys_from_db(&inner.db, prefix)
            }))
        })
        .unwrap_or_default()
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        self.with_inner(|inner| {
            Ok(index_range_core(
                table,
                column,
                branch,
                start,
                end,
                |start_key, end_key| Self::scan_key_range_from_db(&inner.db, start_key, end_key),
            ))
        })
        .unwrap_or_default()
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        self.with_inner(|inner| {
            Ok(index_scan_all_core(table, column, branch, |prefix| {
                Self::scan_prefix_keys_from_db(&inner.db, prefix)
            }))
        })
        .unwrap_or_default()
    }
```

- [ ] **Step 3: Run index conformance tests**

Run: `cargo test -p jazz-tools --features rocksdb rocksdb_conformance::rocksdb::test_index -- --nocapture`
Expected: PASS for all index tests (insert, lookup, remove, range, scan_all, cross-table isolation, cross-branch isolation, value types).

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-tools/src/storage/rocksdb.rs
git commit -m "feat: implement RocksDBStorage index operations"
```

---

### Task 6: Implement ack tier and catalogue manifest methods

**Files:**

- Modify: `crates/jazz-tools/src/storage/rocksdb.rs`

- [ ] **Step 1: Implement `store_ack_tier`**

```rust
    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: DurabilityTier,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = inner.db.transaction();
            store_ack_tier_core(
                commit_id,
                tier,
                |key| Self::get_from_txn(&txn, key),
                |key, value| {
                    txn.put(key.as_bytes(), value)
                        .map_err(|e| StorageError::IoError(format!("rocksdb put: {e}")))
                },
            )?;
            Self::commit_txn(txn)
        })
    }
```

- [ ] **Step 2: Implement catalogue manifest methods**

```rust
    fn append_catalogue_manifest_op(
        &mut self,
        app_id: ObjectId,
        op: CatalogueManifestOp,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = inner.db.transaction();
            append_catalogue_manifest_op_core(
                app_id,
                op,
                |key| Self::get_from_txn(&txn, key),
                |key, value| {
                    txn.put(key.as_bytes(), value)
                        .map_err(|e| StorageError::IoError(format!("rocksdb put: {e}")))
                },
            )?;
            Self::commit_txn(txn)
        })
    }

    fn append_catalogue_manifest_ops(
        &mut self,
        app_id: ObjectId,
        ops: &[CatalogueManifestOp],
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = inner.db.transaction();
            append_catalogue_manifest_ops_core(
                app_id,
                ops,
                |key| Self::get_from_txn(&txn, key),
                |key, value| {
                    txn.put(key.as_bytes(), value)
                        .map_err(|e| StorageError::IoError(format!("rocksdb put: {e}")))
                },
            )?;
            Self::commit_txn(txn)
        })
    }

    fn load_catalogue_manifest(
        &self,
        app_id: ObjectId,
    ) -> Result<Option<CatalogueManifest>, StorageError> {
        self.with_inner(|inner| {
            load_catalogue_manifest_core(app_id, |prefix| {
                Self::scan_prefix_from_db(&inner.db, prefix)
            })
        })
    }
```

- [ ] **Step 3: Run remaining conformance tests**

Run: `cargo test -p jazz-tools --features rocksdb rocksdb_conformance -- --nocapture`
Expected: ALL 25+ conformance tests + 2 persistence tests PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-tools/src/storage/rocksdb.rs
git commit -m "feat: implement RocksDBStorage ack tier and catalogue manifest ops"
```

---

### Task 7: Wire RocksDB into ServerBuilder

**Files:**

- Modify: `crates/jazz-tools/src/server/builder.rs`

- [ ] **Step 1: Update imports**

In `crates/jazz-tools/src/server/builder.rs`, change the storage import on line 17:

From:

```rust
use crate::storage::{FjallStorage, MemoryStorage, Storage};
```

To:

```rust
#[cfg(feature = "rocksdb")]
use crate::storage::RocksDBStorage;
#[cfg(all(feature = "fjall", not(feature = "rocksdb")))]
use crate::storage::FjallStorage;
use crate::storage::{MemoryStorage, Storage};
```

- [ ] **Step 2: Update `build_main_storage`**

Replace the `ServerStorageMode::Persistent` arm (lines 159-169) to use RocksDB when available, falling back to Fjall:

```rust
    fn build_main_storage(&self) -> Result<DynStorage, String> {
        match &self.storage_mode {
            ServerStorageMode::Persistent { data_dir } => {
                std::fs::create_dir_all(data_dir)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", data_dir))?;

                #[cfg(feature = "rocksdb")]
                {
                    let db_path = Path::new(data_dir).join("jazz.rocksdb");
                    let storage =
                        RocksDBStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES).map_err(|e| {
                            format!("failed to open storage '{}': {e:?}", db_path.display())
                        })?;
                    Ok(Box::new(storage))
                }
                #[cfg(all(feature = "fjall", not(feature = "rocksdb")))]
                {
                    let db_path = Path::new(data_dir).join("jazz.fjall");
                    let storage =
                        FjallStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES).map_err(|e| {
                            format!("failed to open storage '{}': {e:?}", db_path.display())
                        })?;
                    Ok(Box::new(storage))
                }
            }
            ServerStorageMode::InMemory => Ok(Box::new(MemoryStorage::new())),
        }
    }
```

- [ ] **Step 3: Update `build_external_identity_store`**

Apply the same pattern to the `build_external_identity_store` method (lines 174-188):

```rust
    fn build_external_identity_store(&self) -> Result<ExternalIdentityStore, String> {
        match &self.storage_mode {
            ServerStorageMode::Persistent { data_dir } => {
                let meta_dir = Path::new(data_dir).join("meta");
                std::fs::create_dir_all(&meta_dir).map_err(|e| {
                    format!("failed to create meta dir '{}': {e}", meta_dir.display())
                })?;

                #[cfg(feature = "rocksdb")]
                {
                    let db_path = meta_dir.join("jazz.rocksdb");
                    let storage =
                        RocksDBStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES).map_err(|e| {
                            format!("failed to open meta storage '{}': {e:?}", db_path.display())
                        })?;
                    ExternalIdentityStore::new_with_storage(Box::new(storage))
                }
                #[cfg(all(feature = "fjall", not(feature = "rocksdb")))]
                {
                    let db_path = meta_dir.join("jazz.fjall");
                    let storage =
                        FjallStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES).map_err(|e| {
                            format!("failed to open meta storage '{}': {e:?}", db_path.display())
                        })?;
                    ExternalIdentityStore::new_with_storage(Box::new(storage))
                }
            }
            ServerStorageMode::InMemory => {
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p jazz-tools --features cli,rocksdb`
Expected: compiles

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/server/builder.rs
git commit -m "feat: wire RocksDB into ServerBuilder with fjall fallback"
```

---

### Task 8: Wire RocksDB into client

**Files:**

- Modify: `crates/jazz-tools/src/lib.rs`
- Modify: `crates/jazz-tools/src/client.rs`

- [ ] **Step 1: Update `ClientStorage` enum**

In `crates/jazz-tools/src/lib.rs`, replace the `ClientStorage` enum (lines 96-105):

```rust
/// Local storage backend for a client application.
#[cfg(feature = "client")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientStorage {
    /// Persist client state to disk using RocksDB under `AppContext::data_dir`.
    #[default]
    RocksDB,
    /// Keep all client state in memory for the lifetime of the process only.
    Memory,
}
```

- [ ] **Step 2: Update client.rs imports**

In `crates/jazz-tools/src/client.rs`, change the storage import on line 16:

From:

```rust
use crate::storage::{FjallStorage, MemoryStorage, Storage, StorageError};
```

To:

```rust
#[cfg(feature = "rocksdb")]
use crate::storage::RocksDBStorage;
#[cfg(all(feature = "fjall", not(feature = "rocksdb")))]
use crate::storage::FjallStorage;
use crate::storage::{MemoryStorage, Storage, StorageError};
```

- [ ] **Step 3: Update `client_id` loading**

Replace the match on `context.storage` for client ID (lines 65-68):

```rust
        let client_id = match context.storage {
            ClientStorage::RocksDB => load_or_create_persistent_client_id(&context)?,
            ClientStorage::Memory => context.client_id.unwrap_or_default(),
        };
```

- [ ] **Step 4: Update storage creation**

Replace the storage match (lines 95-98):

```rust
        let storage: DynStorage = match context.storage {
            ClientStorage::RocksDB => Box::new(open_rocksdb_storage(&context.data_dir).await?),
            ClientStorage::Memory => Box::new(MemoryStorage::new()),
        };
```

- [ ] **Step 5: Add `open_rocksdb_storage` helper**

Add this function alongside the existing `open_fjall_storage` (or replace it if `fjall` feature is not active). Follow the same retry-on-lock pattern:

```rust
#[cfg(feature = "rocksdb")]
async fn open_rocksdb_storage(data_dir: &std::path::Path) -> Result<RocksDBStorage> {
    const MAX_ATTEMPTS: usize = 100;
    const RETRY_DELAY_MS: u64 = 25;

    std::fs::create_dir_all(data_dir)?;

    let db_path = data_dir.join("jazz.rocksdb");
    let mut opened = None;
    let mut last_err = None;

    for attempt in 0..MAX_ATTEMPTS {
        match RocksDBStorage::open(&db_path, 64 * 1024 * 1024) {
            Ok(storage) => {
                opened = Some(storage);
                break;
            }
            Err(err) => {
                let is_lock_error = matches!(
                    &err,
                    StorageError::IoError(msg)
                        if msg.contains("lock") || msg.contains("Lock") || msg.contains("busy")
                );
                if !is_lock_error || attempt + 1 == MAX_ATTEMPTS {
                    last_err = Some(err);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
            }
        }
    }

    opened.ok_or_else(|| {
        JazzError::Connection(format!(
            "failed to open rocksdb storage '{}': {:?}",
            db_path.display(),
            last_err
        ))
    })
}
```

- [ ] **Step 6: Gate the old `open_fjall_storage` behind `#[cfg(feature = "fjall")]`**

Add `#[cfg(all(feature = "fjall", not(feature = "rocksdb")))]` above the existing `open_fjall_storage` function.

- [ ] **Step 7: Update testing.rs**

In `crates/jazz-tools/src/server/testing.rs`, the `ClientStorage::Memory` reference on line 338 doesn't need changing since `Memory` variant stays the same.

- [ ] **Step 8: Verify it compiles**

Run: `cargo check -p jazz-tools --features client,rocksdb`
Expected: compiles

- [ ] **Step 9: Commit**

```bash
git add crates/jazz-tools/src/lib.rs crates/jazz-tools/src/client.rs
git commit -m "feat: wire RocksDB into client as default storage backend"
```

---

### Task 9: Run full test suite and fix issues

**Files:**

- Potentially modify: `crates/jazz-tools/src/storage/rocksdb.rs`, any file with compile errors

- [ ] **Step 1: Run all conformance tests**

Run: `cargo test -p jazz-tools --features rocksdb rocksdb_conformance -- --nocapture`
Expected: ALL tests PASS

- [ ] **Step 2: Run full crate tests with rocksdb feature**

Run: `cargo test -p jazz-tools --features test,rocksdb`
Expected: all existing tests pass. If any fail due to the `ClientStorage::Fjall` → `ClientStorage::RocksDB` rename, fix the references.

- [ ] **Step 3: Run integration tests**

Run: `cargo test -p jazz-tools --features test,rocksdb --test fjall_storage_integration`
Expected: integration tests pass (they use `ClientStorage::Memory` in testing, so they should be unaffected by the backend switch).

- [ ] **Step 4: Build the CLI binary**

Run: `cargo build -p jazz-tools --features cli,rocksdb --bin jazz-tools`
Expected: compiles and links successfully (this verifies the full RocksDB C++ build chain works).

- [ ] **Step 5: Fix any issues found and commit**

If any tests fail or compilation issues arise, fix them. Then:

```bash
git add -u
git commit -m "fix: resolve issues from RocksDB integration"
```

If everything passed clean, skip this step.

---

### Task 10: Add RocksDB to the cross-engine benchmark

**Files:**

- Modify: `crates/opfs-btree/benches/compare_native.rs` (if it exists, otherwise skip)

- [ ] **Step 1: Check if the benchmark file exists**

Run: `ls crates/opfs-btree/benches/compare_native.rs`

If it doesn't exist, skip this task entirely.

- [ ] **Step 2: Add RocksDB to the benchmark**

Follow the existing pattern for Fjall/OpfsBTree in the benchmark file. Add a `RocksDBStorage` setup alongside the existing engines and run the same benchmark operations.

- [ ] **Step 3: Run the benchmark to verify it works**

Run: `cargo bench -p opfs-btree --bench compare_native -- --test`
Expected: benchmark compiles and runs (the `--test` flag runs it quickly without full measurement).

- [ ] **Step 4: Commit**

```bash
git add crates/opfs-btree/benches/compare_native.rs
git commit -m "feat: add RocksDB to cross-engine benchmark"
```
