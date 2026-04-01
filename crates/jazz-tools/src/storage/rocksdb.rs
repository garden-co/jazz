//! RocksDB-backed Storage implementation.
//!
//! Uses `TransactionDB` (pessimistic transactions) for write operations and
//! direct DB access for read-only operations. Follows the same structural
//! pattern as FjallStorage, delegating all logic to `storage_core` callbacks.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::path::Path;

use rocksdb::{
    BlockBasedOptions, Cache, IteratorMode, Options, ReadOptions, Transaction, TransactionDB,
    TransactionDBOptions,
};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::{BatchBranchKey, QueryBranchRef, Value};
#[cfg(test)]
use crate::query_manager::types::{BatchId, BranchPrefixName, ComposedBranchName, SchemaHash};
use crate::sync_manager::DurabilityTier;

use super::{
    CatalogueManifest, CatalogueManifestOp, LoadedBranch, LoadedBranchTips, PrefixBatchCatalog,
    PrefixBatchUpdate, Storage, StorageError,
    storage_core::{
        adjust_table_prefix_batch_refcount_core, append_catalogue_manifest_op_core,
        append_catalogue_manifest_ops_core, append_commit_core, create_object_core,
        index_insert_core, index_lookup_core, index_range_core, index_remove_core,
        index_scan_all_core, load_branch_core, load_branch_tips_core, load_catalogue_manifest_core,
        load_commit_branch_core, load_object_metadata_core, load_prefix_batch_catalog_core,
        load_table_prefix_batch_keys_core, replace_branch_core, store_ack_tier_core,
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

    fn scan_prefix_keys_from_db(
        db: &TransactionDB,
        prefix: &str,
    ) -> Result<Vec<String>, StorageError> {
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
            let (key, _) = item.map_err(|e| StorageError::IoError(format!("rocksdb iter: {e}")))?;
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
        let start_bytes = start.as_bytes();
        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_upper_bound(end.as_bytes().to_vec());
        let mut out = Vec::new();
        let iter = db.iterator_opt(
            IteratorMode::From(start_bytes, rocksdb::Direction::Forward),
            read_opts,
        );
        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::IoError(format!("rocksdb iter: {e}")))?;
            let key_str = String::from_utf8(key.to_vec())
                .map_err(|e| StorageError::IoError(format!("rocksdb invalid key utf8: {e}")))?;
            out.push(key_str);
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

    #[cfg(test)]
    #[allow(clippy::needless_pass_by_value)]
    fn branch_ref(branch: impl Into<String>) -> QueryBranchRef {
        let branch = branch.into();
        let branch_name = BranchName::new(branch.clone());
        if ComposedBranchName::parse(&branch_name).is_some() {
            return QueryBranchRef::from_branch_name(branch_name);
        }

        let prefix = BranchPrefixName::new("dev", SchemaHash::from_bytes([7; 32]), &branch);
        let batch_id = BatchId::from_uuid(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            branch.as_bytes(),
        ));
        QueryBranchRef::from_prefix_and_batch(&prefix, batch_id)
    }

    #[cfg(test)]
    pub(crate) fn load_table_prefix_batches(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<HashSet<BatchId>, StorageError> {
        Ok(self
            .load_table_prefix_batch_keys(table, BranchName::new(prefix))?
            .into_iter()
            .map(|branch_key| branch_key.batch_id())
            .collect())
    }

    #[cfg(test)]
    pub(crate) fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        <Self as Storage>::index_insert(
            self,
            table,
            column,
            &Self::branch_ref(branch),
            value,
            row_id,
        )
    }

    #[cfg(test)]
    pub(crate) fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        <Self as Storage>::index_remove(
            self,
            table,
            column,
            &Self::branch_ref(branch),
            value,
            row_id,
        )
    }

    #[cfg(test)]
    pub(crate) fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        <Self as Storage>::index_lookup(self, table, column, &Self::branch_ref(branch), value)
    }

    #[cfg(test)]
    pub(crate) fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        <Self as Storage>::index_range(self, table, column, &Self::branch_ref(branch), start, end)
    }

    #[cfg(test)]
    pub(crate) fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        <Self as Storage>::index_scan_all(self, table, column, &Self::branch_ref(branch))
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
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        self.with_inner(|inner| {
            load_branch_core(object_id, branch, |key| Self::get_from_db(&inner.db, key))
        })
    }

    fn load_branch_tips(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranchTips>, StorageError> {
        self.with_inner(|inner| {
            load_branch_tips_core(object_id, branch, |key| Self::get_from_db(&inner.db, key))
        })
    }

    fn load_commit_branch(
        &self,
        object_id: ObjectId,
        commit_id: CommitId,
    ) -> Result<Option<QueryBranchRef>, StorageError> {
        self.with_inner(|inner| {
            load_commit_branch_core(object_id, commit_id, |key| {
                Self::get_from_db(&inner.db, key)
            })
        })
    }

    fn load_prefix_batch_catalog(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Option<PrefixBatchCatalog>, StorageError> {
        self.with_inner(|inner| {
            load_prefix_batch_catalog_core(object_id, prefix, |key| {
                Self::get_from_db(&inner.db, key)
            })
        })
    }

    fn load_table_prefix_batch_keys(
        &self,
        table: &str,
        prefix: BranchName,
    ) -> Result<Vec<BatchBranchKey>, StorageError> {
        self.with_inner(|inner| {
            load_table_prefix_batch_keys_core(table, prefix, |key| {
                Self::get_from_db(&inner.db, key)
            })
        })
    }

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
        commit: Commit,
        prefix_batch_update: Option<PrefixBatchUpdate>,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            append_commit_core(
                object_id,
                branch,
                commit,
                prefix_batch_update,
                |key| Self::get_from_txn_cell(&txn, key),
                |key, value| Self::put_on_txn_cell(&txn, key, value),
            )?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn replace_branch(
        &mut self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
        commits: Vec<Commit>,
        tails: HashSet<CommitId>,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            replace_branch_core(
                object_id,
                branch,
                commits,
                tails,
                |key| Self::get_from_txn_cell(&txn, key),
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

    fn append_catalogue_manifest_op(
        &mut self,
        app_id: ObjectId,
        op: CatalogueManifestOp,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            append_catalogue_manifest_op_core(
                app_id,
                op,
                |key| Self::get_from_txn_cell(&txn, key),
                |key, value| Self::put_on_txn_cell(&txn, key, value),
            )?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn append_catalogue_manifest_ops(
        &mut self,
        app_id: ObjectId,
        ops: &[CatalogueManifestOp],
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            append_catalogue_manifest_ops_core(
                app_id,
                ops,
                |key| Self::get_from_txn_cell(&txn, key),
                |key, value| Self::put_on_txn_cell(&txn, key, value),
            )?;
            Self::commit_txn(txn.into_inner())
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

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &QueryBranchRef,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            let inserted = index_insert_core(
                table,
                column,
                branch,
                value,
                row_id,
                |key| Self::get_from_txn_cell(&txn, key),
                |key, bytes| Self::put_on_txn_cell(&txn, key, bytes),
            )?;
            if inserted && matches!(column, "_id" | "_id_deleted") {
                adjust_table_prefix_batch_refcount_core(
                    table,
                    branch,
                    1,
                    |key| Self::get_from_txn_cell(&txn, key),
                    |key, bytes| Self::put_on_txn_cell(&txn, key, bytes),
                    |key| Self::delete_on_txn_cell(&txn, key),
                )?;
            }
            Self::commit_txn(txn.into_inner())
        })
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &QueryBranchRef,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            let removed = index_remove_core(
                table,
                column,
                branch,
                value,
                row_id,
                |key| Self::get_from_txn_cell(&txn, key),
                |key| Self::delete_on_txn_cell(&txn, key),
            )?;
            if removed && matches!(column, "_id" | "_id_deleted") {
                adjust_table_prefix_batch_refcount_core(
                    table,
                    branch,
                    -1,
                    |key| Self::get_from_txn_cell(&txn, key),
                    |key, bytes| Self::put_on_txn_cell(&txn, key, bytes),
                    |key| Self::delete_on_txn_cell(&txn, key),
                )?;
            }
            Self::commit_txn(txn.into_inner())
        })
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &QueryBranchRef,
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
        branch: &QueryBranchRef,
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

    fn index_scan_all(&self, table: &str, column: &str, branch: &QueryBranchRef) -> Vec<ObjectId> {
        self.with_inner(|inner| {
            Ok(index_scan_all_core(table, column, branch, |prefix| {
                Self::scan_prefix_keys_from_db(&inner.db, prefix)
            }))
        })
        .unwrap_or_default()
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
