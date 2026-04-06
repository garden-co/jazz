//! RocksDB-backed Storage implementation.
//!
//! Uses `TransactionDB` (pessimistic transactions) for write operations and
//! direct DB access for read-only operations. Follows the same structural
//! pattern as FjallStorage, delegating all logic to `storage_core` callbacks.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use rocksdb::{
    BlockBasedOptions, Cache, IteratorMode, Options, ReadOptions, Transaction, TransactionDB,
    TransactionDBOptions,
};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::sync_manager::DurabilityTier;

use super::{
    LoadedBranch, Storage, StorageError,
    storage_core::{
        append_commit_core, create_object_core, delete_commit_core, load_branch_core,
        load_object_metadata_core, raw_table_delete_core, raw_table_get_core, raw_table_put_core,
        raw_table_scan_prefix_core, raw_table_scan_range_core, set_branch_tails_core,
        store_ack_tier_core,
    },
};

struct RocksDBInner {
    db: TransactionDB,
}

pub struct RocksDBStorage {
    inner: RefCell<Option<RocksDBInner>>,
}

impl RocksDBStorage {
    pub fn open(path: impl AsRef<Path>, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        let cache = Cache::new_lru_cache(cache_size_bytes);
        block_opts.set_block_cache(&cache);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_block_based_table_factory(&block_opts);
        // LZ4 for L0-L2 (fast), Zstd for deeper levels (compact)
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);

        let txdb_opts = TransactionDBOptions::default();
        let db = TransactionDB::open(&opts, &txdb_opts, path.as_ref())
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

    /// Compute the lexicographic successor of a byte prefix for use as an
    /// exclusive upper bound. Returns `None` when the prefix is all `0xFF`
    /// bytes (practically never for our key scheme).
    fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
        let mut bound = prefix.to_vec();
        while let Some(last) = bound.last_mut() {
            if *last < 0xFF {
                *last += 1;
                return Some(bound);
            }
            bound.pop();
        }
        None
    }

    // ---- read helpers (direct DB, no transaction) ----

    fn get_from_db(db: &TransactionDB, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        db.get(key.as_bytes())
            .map_err(|e| StorageError::IoError(format!("rocksdb get: {e}")))
    }

    fn scan_prefix_from_db(
        db: &TransactionDB,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let prefix_bytes = prefix.as_bytes();
        let mut read_opts = ReadOptions::default();
        if let Some(ub) = Self::prefix_upper_bound(prefix_bytes) {
            read_opts.set_iterate_upper_bound(ub);
        }
        let mut out = Vec::new();
        let iter = db.iterator_opt(
            IteratorMode::From(prefix_bytes, rocksdb::Direction::Forward),
            read_opts,
        );
        for item in iter {
            let (key, value) =
                item.map_err(|e| StorageError::IoError(format!("rocksdb iter: {e}")))?;
            let key_str = String::from_utf8(key.to_vec())
                .map_err(|e| StorageError::IoError(format!("rocksdb invalid key utf8: {e}")))?;
            out.push((key_str, value.to_vec()));
        }
        Ok(out)
    }

    fn scan_range_from_db(
        db: &TransactionDB,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let start_bytes = start.as_bytes();
        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_upper_bound(end.as_bytes().to_vec());
        let mut out = Vec::new();
        let iter = db.iterator_opt(
            IteratorMode::From(start_bytes, rocksdb::Direction::Forward),
            read_opts,
        );
        for item in iter {
            let (key, value) =
                item.map_err(|e| StorageError::IoError(format!("rocksdb iter: {e}")))?;
            let key_str = String::from_utf8(key.to_vec())
                .map_err(|e| StorageError::IoError(format!("rocksdb invalid key utf8: {e}")))?;
            out.push((key_str, value.to_vec()));
        }
        Ok(out)
    }

    // ---- transaction helpers ----

    fn get_from_txn<'a>(
        txn: &Transaction<'a, TransactionDB>,
        key: &str,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        txn.get(key.as_bytes())
            .map_err(|e| StorageError::IoError(format!("rocksdb txn get: {e}")))
    }

    fn get_from_txn_cell<'a>(
        txn: &RefCell<Transaction<'a, TransactionDB>>,
        key: &str,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        Self::get_from_txn(&txn.borrow(), key)
    }

    fn put_on_txn<'a>(
        txn: &Transaction<'a, TransactionDB>,
        key: &str,
        value: &[u8],
    ) -> Result<(), StorageError> {
        txn.put(key.as_bytes(), value)
            .map_err(|e| StorageError::IoError(format!("rocksdb txn put: {e}")))
    }

    fn put_on_txn_cell<'a>(
        txn: &RefCell<Transaction<'a, TransactionDB>>,
        key: &str,
        value: &[u8],
    ) -> Result<(), StorageError> {
        Self::put_on_txn(&txn.borrow(), key, value)
    }

    fn delete_on_txn<'a>(
        txn: &Transaction<'a, TransactionDB>,
        key: &str,
    ) -> Result<(), StorageError> {
        txn.delete(key.as_bytes())
            .map_err(|e| StorageError::IoError(format!("rocksdb txn delete: {e}")))
    }

    fn delete_on_txn_cell<'a>(
        txn: &RefCell<Transaction<'a, TransactionDB>>,
        key: &str,
    ) -> Result<(), StorageError> {
        Self::delete_on_txn(&txn.borrow(), key)
    }

    fn commit_txn(txn: Transaction<'_, TransactionDB>) -> Result<(), StorageError> {
        txn.commit()
            .map_err(|e| StorageError::IoError(format!("rocksdb txn commit: {e}")))
    }
}

impl Storage for RocksDBStorage {
    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = inner.db.transaction();
            create_object_core(id, metadata, |key, value| {
                Self::put_on_txn(&txn, key, value)
            })?;
            Self::commit_txn(txn)
        })
    }

    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        self.with_inner(|inner| {
            load_object_metadata_core(id, |key| Self::get_from_db(&inner.db, key))
        })
    }

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

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            append_commit_core(
                object_id,
                branch,
                commit,
                |key| Self::get_from_txn_cell(&txn, key),
                |key, value| Self::put_on_txn_cell(&txn, key, value),
            )?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            delete_commit_core(
                object_id,
                branch,
                commit_id,
                |key| Self::get_from_txn_cell(&txn, key),
                |key, value| Self::put_on_txn_cell(&txn, key, value),
                |key| Self::delete_on_txn_cell(&txn, key),
            )?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            set_branch_tails_core(
                object_id,
                branch,
                tails,
                |key, value| Self::put_on_txn_cell(&txn, key, value),
                |key| Self::delete_on_txn_cell(&txn, key),
            )?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: DurabilityTier,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            store_ack_tier_core(
                commit_id,
                tier,
                |key| Self::get_from_txn_cell(&txn, key),
                |key, value| Self::put_on_txn_cell(&txn, key, value),
            )?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            raw_table_put_core(table, key, value, |storage_key, bytes| {
                Self::put_on_txn_cell(&txn, storage_key, bytes)
            })?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            raw_table_delete_core(table, key, |storage_key| {
                Self::delete_on_txn_cell(&txn, storage_key)
            })?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.with_inner(|inner| {
            raw_table_get_core(table, key, |storage_key| {
                Self::get_from_db(&inner.db, storage_key)
            })
        })
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<super::RawTableRows, StorageError> {
        self.with_inner(|inner| {
            raw_table_scan_prefix_core(table, prefix, |storage_prefix| {
                Self::scan_prefix_from_db(&inner.db, storage_prefix)
            })
        })
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<super::RawTableRows, StorageError> {
        self.with_inner(|inner| {
            raw_table_scan_range_core(table, start, end, |start_key, end_key| {
                Self::scan_range_from_db(&inner.db, start_key, end_key)
            })
        })
    }

    fn flush(&self) {
        let _ = self.with_inner(|inner| {
            inner
                .db
                .flush()
                .map_err(|e| StorageError::IoError(format!("rocksdb flush: {e}")))
        });
    }

    fn flush_wal(&self) {
        let _ = self.with_inner(|inner| {
            inner
                .db
                .flush_wal(true)
                .map_err(|e| StorageError::IoError(format!("rocksdb flush_wal: {e}")))
        });
    }

    fn close(&self) -> Result<(), StorageError> {
        let Some(inner) = self.inner.borrow_mut().take() else {
            return Ok(());
        };
        drop(inner);
        Ok(())
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
        let reopened = RocksDBStorage::open(&db_path, 8 * 1024 * 1024).unwrap();
        reopened.close().unwrap();
    }

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
}
