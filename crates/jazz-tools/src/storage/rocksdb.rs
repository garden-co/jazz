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

use super::{
    HistoryRowBytes, IndexMutation, Storage, StorageError, VisibleRowBytes, key_codec,
    storage_core::{
        append_history_region_row_bytes_core, raw_table_delete_core, raw_table_get_core,
        raw_table_put_core, raw_table_scan_prefix_core, raw_table_scan_prefix_keys_core,
        raw_table_scan_range_core, raw_table_scan_range_keys_core,
        upsert_visible_region_row_bytes_core,
    },
};
use crate::object::ObjectId;
use crate::row_histories::{HistoryScan, RowState, StoredRowBatch};
use crate::sync_manager::DurabilityTier;

struct RocksDBInner {
    db: TransactionDB,
    ensured_raw_table_headers: HashSet<String>,
    visible_row_table_locators: HashMap<(String, ObjectId), super::ExactRowTableLocator>,
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
            inner: RefCell::new(Some(RocksDBInner {
                db,
                ensured_raw_table_headers: HashSet::new(),
                visible_row_table_locators: HashMap::new(),
            })),
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

    fn with_inner_mut<T>(
        &self,
        f: impl FnOnce(&mut RocksDBInner) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let mut inner = self.inner.borrow_mut();
        let inner = inner
            .as_mut()
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

    fn scan_range_keys_from_db(
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

    fn apply_index_mutations_on_txn<'a>(
        txn: &RefCell<Transaction<'a, TransactionDB>>,
        mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        for mutation in mutations {
            match mutation {
                IndexMutation::Insert {
                    table,
                    column,
                    branch,
                    value,
                    row_id,
                } => {
                    let raw_table = key_codec::index_raw_table(table, column, branch);
                    let key = key_codec::index_entry_key(table, column, branch, value, *row_id)?;
                    raw_table_put_core(&raw_table, &key, &[0x01], |storage_key, bytes| {
                        Self::put_on_txn_cell(txn, storage_key, bytes)
                    })?;
                }
                IndexMutation::Remove {
                    table,
                    column,
                    branch,
                    value,
                    row_id,
                } => {
                    let key =
                        match key_codec::index_entry_key(table, column, branch, value, *row_id) {
                            Ok(key) => key,
                            Err(StorageError::IndexKeyTooLarge { .. }) => continue,
                            Err(error) => return Err(error),
                        };
                    let raw_table = key_codec::index_raw_table(table, column, branch);
                    raw_table_delete_core(&raw_table, &key, |storage_key| {
                        Self::delete_on_txn_cell(txn, storage_key)
                    })?;
                }
            }
        }
        Ok(())
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

    fn apply_index_mutations(
        &mut self,
        mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        if mutations.is_empty() {
            return Ok(());
        }

        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            Self::apply_index_mutations_on_txn(&txn, mutations)?;
            Self::commit_txn(txn.into_inner())
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

    fn raw_table_scan_prefix_keys(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<super::RawTableKeys, StorageError> {
        self.with_inner(|inner| {
            raw_table_scan_prefix_keys_core(table, prefix, |storage_prefix| {
                Self::scan_prefix_keys_from_db(&inner.db, storage_prefix)
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

    fn raw_table_scan_range_keys(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<super::RawTableKeys, StorageError> {
        self.with_inner(|inner| {
            raw_table_scan_range_keys_core(table, start, end, |start_key, end_key| {
                Self::scan_range_keys_from_db(&inner.db, start_key, end_key)
            })
        })
    }

    fn append_history_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            append_history_region_row_bytes_core(table, rows, |key, bytes| {
                Self::put_on_txn_cell(&txn, key, bytes)
            })?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn upsert_visible_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[VisibleRowBytes<'_>],
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            upsert_visible_region_row_bytes_core(table, rows, |key, bytes| {
                Self::put_on_txn_cell(&txn, key, bytes)
            })?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn delete_visible_region_row(
        &mut self,
        _table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            let locator = inner
                .visible_row_table_locators
                .remove(&(branch.to_string(), row_id));
            let key = super::key_codec::visible_row_raw_table_key(branch, row_id);
            if let Some(locator) = locator.as_ref() {
                raw_table_delete_core(locator.row_raw_table.as_str(), &key, |storage_key| {
                    Self::delete_on_txn_cell(&txn, storage_key)
                })?;
            }
            raw_table_delete_core(
                super::VISIBLE_ROW_TABLE_LOCATOR_TABLE,
                &super::visible_row_table_locator_key(branch, row_id),
                |storage_key| Self::delete_on_txn_cell(&txn, storage_key),
            )?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn apply_encoded_row_mutation(
        &mut self,
        table: &str,
        encoded_history_rows: &[super::OwnedHistoryRowBytes],
        encoded_visible_rows: &[super::OwnedVisibleRowBytes],
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            let txn = RefCell::new(inner.db.transaction());
            let mut seen_row_raw_tables = std::collections::HashSet::new();
            for row in encoded_history_rows {
                if seen_row_raw_tables.insert(row.row_raw_table.clone())
                    && inner
                        .ensured_raw_table_headers
                        .insert(row.row_raw_table.clone())
                {
                    let header = super::encode_raw_table_header(&super::row_raw_table_header(
                        &row.row_raw_table_id,
                        &row.user_descriptor,
                    ))?;
                    raw_table_put_core(
                        super::RAW_TABLE_HEADER_TABLE,
                        row.row_raw_table.as_str(),
                        &header,
                        |storage_key, bytes| Self::put_on_txn_cell(&txn, storage_key, bytes),
                    )?;
                }
            }
            for row in encoded_visible_rows {
                if seen_row_raw_tables.insert(row.row_raw_table.clone())
                    && inner
                        .ensured_raw_table_headers
                        .insert(row.row_raw_table.clone())
                {
                    let header = super::encode_raw_table_header(&super::row_raw_table_header(
                        &row.row_raw_table_id,
                        &row.user_descriptor,
                    ))?;
                    raw_table_put_core(
                        super::RAW_TABLE_HEADER_TABLE,
                        row.row_raw_table.as_str(),
                        &header,
                        |storage_key, bytes| Self::put_on_txn_cell(&txn, storage_key, bytes),
                    )?;
                }
            }
            if encoded_history_rows
                .iter()
                .any(|row| row.needs_exact_locator)
                && inner
                    .ensured_raw_table_headers
                    .insert(super::HISTORY_ROW_BATCH_TABLE_LOCATOR_TABLE.to_string())
            {
                let header = super::encode_raw_table_header(&super::RawTableHeader::system(
                    super::STORAGE_KIND_HISTORY_ROW_BATCH_TABLE_LOCATOR,
                    1,
                ))?;
                raw_table_put_core(
                    super::RAW_TABLE_HEADER_TABLE,
                    super::HISTORY_ROW_BATCH_TABLE_LOCATOR_TABLE,
                    &header,
                    |storage_key, bytes| Self::put_on_txn_cell(&txn, storage_key, bytes),
                )?;
            }
            if encoded_visible_rows
                .iter()
                .any(|row| row.needs_exact_locator)
                && inner
                    .ensured_raw_table_headers
                    .insert(super::VISIBLE_ROW_TABLE_LOCATOR_TABLE.to_string())
            {
                let header = super::encode_raw_table_header(&super::RawTableHeader::system(
                    super::STORAGE_KIND_VISIBLE_ROW_TABLE_LOCATOR,
                    1,
                ))?;
                raw_table_put_core(
                    super::RAW_TABLE_HEADER_TABLE,
                    super::VISIBLE_ROW_TABLE_LOCATOR_TABLE,
                    &header,
                    |storage_key, bytes| Self::put_on_txn_cell(&txn, storage_key, bytes),
                )?;
            }
            let borrowed_history_rows = encoded_history_rows
                .iter()
                .map(|row| HistoryRowBytes {
                    row_raw_table: row.row_raw_table.as_str(),
                    branch: row.branch.as_str(),
                    row_id: row.row_id,
                    batch_id: row.batch_id,
                    bytes: &row.bytes,
                })
                .collect::<Vec<_>>();
            append_history_region_row_bytes_core(table, &borrowed_history_rows, |key, bytes| {
                Self::put_on_txn_cell(&txn, key, bytes)
            })?;
            for row in encoded_history_rows {
                if !row.needs_exact_locator {
                    continue;
                }
                let locator =
                    super::encode_exact_row_table_locator(&super::ExactRowTableLocator {
                        row_raw_table: row.row_raw_table.clone().into(),
                        table_name: row.row_raw_table_id.table_name.clone(),
                        schema_hash: row.row_raw_table_id.schema_hash,
                    })?;
                raw_table_put_core(
                    super::HISTORY_ROW_BATCH_TABLE_LOCATOR_TABLE,
                    &super::history_row_batch_table_locator_key(
                        row.row_id,
                        row.branch.as_str(),
                        row.batch_id,
                    ),
                    &locator,
                    |storage_key, bytes| Self::put_on_txn_cell(&txn, storage_key, bytes),
                )?;
            }
            let borrowed_visible_rows = encoded_visible_rows
                .iter()
                .map(|row| VisibleRowBytes {
                    row_raw_table: row.row_raw_table.as_str(),
                    branch: row.branch.as_str(),
                    row_id: row.row_id,
                    bytes: &row.bytes,
                })
                .collect::<Vec<_>>();
            upsert_visible_region_row_bytes_core(table, &borrowed_visible_rows, |key, bytes| {
                Self::put_on_txn_cell(&txn, key, bytes)
            })?;
            for row in encoded_visible_rows {
                if !row.needs_exact_locator {
                    continue;
                }
                let locator = super::ExactRowTableLocator {
                    row_raw_table: row.row_raw_table.clone().into(),
                    table_name: row.row_raw_table_id.table_name.clone(),
                    schema_hash: row.row_raw_table_id.schema_hash,
                };
                let cache_key = (row.branch.clone(), row.row_id);
                if inner.visible_row_table_locators.get(&cache_key) != Some(&locator) {
                    let locator_bytes = super::encode_exact_row_table_locator(&locator)?;
                    raw_table_put_core(
                        super::VISIBLE_ROW_TABLE_LOCATOR_TABLE,
                        &super::visible_row_table_locator_key(row.branch.as_str(), row.row_id),
                        &locator_bytes,
                        |storage_key, bytes| Self::put_on_txn_cell(&txn, storage_key, bytes),
                    )?;
                    inner.visible_row_table_locators.insert(cache_key, locator);
                }
            }
            Self::apply_index_mutations_on_txn(&txn, index_mutations)?;
            Self::commit_txn(txn.into_inner())
        })
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_histories::BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        super::patch_row_region_rows_by_batch_with_storage(
            self,
            table,
            batch_id,
            state,
            confirmed_tier,
        )
    }

    fn load_visible_region_row_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(
            super::load_visible_region_row_bytes_with_storage(self, table, branch, row_id)?
                .map(|row| row.bytes),
        )
    }

    fn scan_visible_region_bytes(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        Ok(
            super::scan_visible_row_bytes_with_storage(self, table, branch)?
                .into_iter()
                .map(|row| row.bytes)
                .collect(),
        )
    }

    fn scan_visible_region_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let branches =
            super::scan_visible_region_row_batch_branches_with_storage(self, table, row_id)?;

        let mut rows = Vec::new();
        for branch in branches {
            if let Some(row) = self.load_visible_region_row(table, &branch, row_id)? {
                rows.push(row);
            }
        }
        rows.sort_by_key(|row| row.branch.clone());
        Ok(rows)
    }

    fn load_history_row_batch_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(super::load_history_row_batch_row_bytes_with_storage(
            self, table, branch, row_id, batch_id,
        )?
        .map(|row| row.bytes))
    }

    fn scan_history_region_bytes(
        &self,
        table: &str,
        scan: HistoryScan,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        Ok(
            super::scan_history_row_bytes_with_storage(self, table, scan)?
                .into_iter()
                .map(|row| row.bytes)
                .collect(),
        )
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
