//! RocksDB-backed Storage implementation.
//!
//! Uses `TransactionDB` (pessimistic transactions) for write operations and
//! direct DB access for read-only operations. Follows the same structural
//! pattern as FjallStorage, delegating all logic to `storage_core` callbacks.

use std::cell::RefCell;
use std::path::Path;

use rocksdb::{
    BlockBasedOptions, Cache, IteratorMode, Options, ReadOptions, Transaction, TransactionDB,
    TransactionDBOptions,
};

use super::{
    Storage, StorageError,
    storage_core::{
        append_history_region_rows_core, load_visible_region_row_core,
        patch_row_region_rows_by_batch_core, raw_table_delete_core, raw_table_get_core,
        raw_table_put_core, raw_table_scan_prefix_core, raw_table_scan_range_core,
        scan_history_region_core, scan_history_row_versions_core, scan_visible_region_core,
        scan_visible_region_row_versions_core, upsert_visible_region_rows_core,
    },
};
use crate::object::ObjectId;
use crate::row_regions::{HistoryScan, RowState, StoredRowVersion};
use crate::sync_manager::DurabilityTier;

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

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowVersion],
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            append_history_region_rows_core(table, rows, |key, bytes| {
                Self::put_on_txn_cell(&txn, key, bytes)
            })?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowVersion],
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            upsert_visible_region_rows_core(table, rows, |key, bytes| {
                Self::put_on_txn_cell(&txn, key, bytes)
            })?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_regions::BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            patch_row_region_rows_by_batch_core(
                table,
                batch_id,
                state,
                confirmed_tier,
                |prefix| Self::scan_prefix_from_db(&inner.db, prefix),
                |key, bytes| Self::put_on_txn_cell(&txn, key, bytes),
            )?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        self.with_inner(|inner| {
            scan_visible_region_core(table, branch, |prefix| {
                Self::scan_prefix_from_db(&inner.db, prefix)
            })
        })
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<StoredRowVersion>, StorageError> {
        self.with_inner(|inner| {
            load_visible_region_row_core(table, branch, row_id, |key| {
                Self::get_from_db(&inner.db, key)
            })
        })
    }

    fn scan_visible_region_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        self.with_inner(|inner| {
            scan_visible_region_row_versions_core(table, row_id, |prefix| {
                Self::scan_prefix_from_db(&inner.db, prefix)
            })
        })
    }

    fn scan_history_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        self.with_inner(|inner| {
            scan_history_row_versions_core(table, row_id, |prefix| {
                Self::scan_prefix_from_db(&inner.db, prefix)
            })
        })
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: HistoryScan,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        self.with_inner(|inner| {
            scan_history_region_core(table, branch, scan, |prefix| {
                Self::scan_prefix_from_db(&inner.db, prefix)
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
