//! Fjall-backed Storage implementation.
//!
//! Uses one transactional Fjall database with a single keyspace and the same
//! UTF-8 key encoding scheme as the other native backends.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;

use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
    SingleWriterWriteTx,
};

use crate::object::ObjectId;
use crate::row_regions::{HistoryScan, RowState, StoredRowVersion};
use crate::sync_manager::DurabilityTier;

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

const KEYSPACE_NAME: &str = "jazz";

struct FjallInner {
    db: SingleWriterTxDatabase,
    keyspace: SingleWriterTxKeyspace,
}

pub struct FjallStorage {
    inner: RefCell<Option<FjallInner>>,
}

impl FjallStorage {
    pub fn open(path: impl AsRef<Path>, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let db = SingleWriterTxDatabase::builder(path.as_ref())
            .cache_size(cache_size_bytes as u64)
            .manual_journal_persist(true)
            .max_cached_files(Some(64))
            .open()
            .map_err(|e| StorageError::IoError(format!("fjall open: {e}")))?;
        let keyspace = db
            .keyspace(KEYSPACE_NAME, KeyspaceCreateOptions::default)
            .map_err(|e| StorageError::IoError(format!("fjall keyspace: {e}")))?;
        Ok(Self {
            inner: RefCell::new(Some(FjallInner { db, keyspace })),
        })
    }

    fn with_inner<T>(
        &self,
        f: impl FnOnce(&FjallInner) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let inner = self.inner.borrow();
        let inner = inner
            .as_ref()
            .ok_or_else(|| StorageError::IoError("fjall storage already closed".to_string()))?;
        f(inner)
    }

    fn commit_tx(tx: SingleWriterWriteTx<'_>) -> Result<(), StorageError> {
        tx.commit()
            .map_err(|e| StorageError::IoError(format!("fjall commit: {e}")))
    }

    fn read_get(
        txn: &impl Readable,
        keyspace: &SingleWriterTxKeyspace,
        key: &str,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        txn.get(keyspace, key.as_bytes())
            .map(|value| value.map(|value| value.to_vec()))
            .map_err(|e| StorageError::IoError(format!("fjall get: {e}")))
    }

    fn scan_prefix(
        txn: &impl Readable,
        keyspace: &SingleWriterTxKeyspace,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let mut out = Vec::new();
        for item in txn.prefix(keyspace, prefix.as_bytes()) {
            let (key, value) = item
                .into_inner()
                .map_err(|e| StorageError::IoError(format!("fjall prefix item: {e}")))?;
            let key = String::from_utf8(key.to_vec())
                .map_err(|e| StorageError::IoError(format!("fjall invalid key utf8: {e}")))?;
            out.push((key, value.to_vec()));
        }
        Ok(out)
    }

    fn scan_range(
        txn: &impl Readable,
        keyspace: &SingleWriterTxKeyspace,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let mut out = Vec::new();
        for item in txn.range(keyspace, start.as_bytes()..end.as_bytes()) {
            let (key, value) = item
                .into_inner()
                .map_err(|e| StorageError::IoError(format!("fjall range item: {e}")))?;
            let key = String::from_utf8(key.to_vec())
                .map_err(|e| StorageError::IoError(format!("fjall invalid key utf8: {e}")))?;
            out.push((key, value.to_vec()));
        }
        Ok(out)
    }

    fn set_on_tx(
        tx: &mut SingleWriterWriteTx<'_>,
        keyspace: &SingleWriterTxKeyspace,
        key: &str,
        value: &[u8],
    ) -> Result<(), StorageError> {
        tx.insert(keyspace, key.as_bytes(), value);
        Ok(())
    }

    fn delete_on_tx(
        tx: &mut SingleWriterWriteTx<'_>,
        keyspace: &SingleWriterTxKeyspace,
        key: &str,
    ) -> Result<(), StorageError> {
        tx.remove(keyspace, key.as_bytes());
        Ok(())
    }

    fn read_get_cell(
        tx: &RefCell<SingleWriterWriteTx<'_>>,
        keyspace: &SingleWriterTxKeyspace,
        key: &str,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let tx = tx.borrow();
        Self::read_get(&*tx, keyspace, key)
    }

    fn set_on_cell(
        tx: &RefCell<SingleWriterWriteTx<'_>>,
        keyspace: &SingleWriterTxKeyspace,
        key: &str,
        value: &[u8],
    ) -> Result<(), StorageError> {
        let mut tx = tx.borrow_mut();
        Self::set_on_tx(&mut tx, keyspace, key, value)
    }

    fn delete_on_cell(
        tx: &RefCell<SingleWriterWriteTx<'_>>,
        keyspace: &SingleWriterTxKeyspace,
        key: &str,
    ) -> Result<(), StorageError> {
        let mut tx = tx.borrow_mut();
        Self::delete_on_tx(&mut tx, keyspace, key)
    }
}

impl Storage for FjallStorage {
    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let mut tx = inner.db.write_tx();
            raw_table_put_core(table, key, value, |storage_key, bytes| {
                Self::set_on_tx(&mut tx, &inner.keyspace, storage_key, bytes)
            })?;
            Self::commit_tx(tx)
        })
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let mut tx = inner.db.write_tx();
            raw_table_delete_core(table, key, |storage_key| {
                Self::delete_on_tx(&mut tx, &inner.keyspace, storage_key)
            })?;
            Self::commit_tx(tx)
        })
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            raw_table_get_core(table, key, |storage_key| {
                Self::read_get(&tx, &inner.keyspace, storage_key)
            })
        })
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<super::RawTableRows, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            raw_table_scan_prefix_core(table, prefix, |storage_prefix| {
                Self::scan_prefix(&tx, &inner.keyspace, storage_prefix)
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
            let tx = inner.db.read_tx();
            raw_table_scan_range_core(table, start, end, |start_key, end_key| {
                Self::scan_range(&tx, &inner.keyspace, start_key, end_key)
            })
        })
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowVersion],
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let mut tx = inner.db.write_tx();
            append_history_region_rows_core(table, rows, |key, bytes| {
                Self::set_on_tx(&mut tx, &inner.keyspace, key, bytes)
            })?;
            Self::commit_tx(tx)
        })
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowVersion],
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let mut tx = inner.db.write_tx();
            upsert_visible_region_rows_core(table, rows, |key, bytes| {
                Self::set_on_tx(&mut tx, &inner.keyspace, key, bytes)
            })?;
            Self::commit_tx(tx)
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
            let tx = RefCell::new(inner.db.write_tx());
            patch_row_region_rows_by_batch_core(
                table,
                batch_id,
                state,
                confirmed_tier,
                |prefix| Self::scan_prefix(&*tx.borrow(), &inner.keyspace, prefix),
                |key, bytes| Self::set_on_cell(&tx, &inner.keyspace, key, bytes),
            )?;
            Self::commit_tx(tx.into_inner())
        })
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            scan_visible_region_core(table, branch, |prefix| {
                Self::scan_prefix(&tx, &inner.keyspace, prefix)
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
            let tx = inner.db.read_tx();
            load_visible_region_row_core(table, branch, row_id, |key| {
                Self::read_get(&tx, &inner.keyspace, key)
            })
        })
    }

    fn scan_visible_region_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            scan_visible_region_row_versions_core(table, row_id, |prefix| {
                Self::scan_prefix(&tx, &inner.keyspace, prefix)
            })
        })
    }

    fn scan_history_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            scan_history_row_versions_core(table, row_id, |prefix| {
                Self::scan_prefix(&tx, &inner.keyspace, prefix)
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
            let tx = inner.db.read_tx();
            scan_history_region_core(table, branch, scan, |prefix| {
                Self::scan_prefix(&tx, &inner.keyspace, prefix)
            })
        })
    }

    fn flush(&self) {
        if let Some(inner) = self.inner.borrow().as_ref() {
            let _ = inner.db.persist(PersistMode::SyncData);
        }
    }

    fn flush_wal(&self) {
        self.flush();
    }

    fn close(&self) -> Result<(), StorageError> {
        let Some(inner) = self.inner.borrow_mut().take() else {
            return Ok(());
        };

        inner
            .db
            .persist(PersistMode::SyncData)
            .map_err(|e| StorageError::IoError(format!("fjall persist on close: {e}")))?;
        drop(inner);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_releases_lock_for_reopen() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.fjall");
        let storage = FjallStorage::open(&db_path, 8 * 1024 * 1024).unwrap();
        storage.close().unwrap();

        let reopened = FjallStorage::open(&db_path, 8 * 1024 * 1024).unwrap();
        reopened.close().unwrap();
    }

    #[test]
    fn fjall_storage_row_regions_visible_and_history_round_trip() {
        use crate::row_regions::{BatchId, HistoryScan, RowState, StoredRowVersion};

        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.fjall");
        let mut storage = FjallStorage::open(&db_path, 8 * 1024 * 1024).unwrap();
        let row_id = ObjectId::new();
        let batch_id = BatchId::new();

        let version = StoredRowVersion {
            row_id,
            branch: "dev/main".to_string(),
            parents: Vec::new(),
            updated_at: 10,
            created_by: "alice".to_string(),
            created_at: 10,
            updated_by: "alice".to_string(),
            batch_id,
            state: RowState::VisibleDirect,
            confirmed_tier: Some(DurabilityTier::Worker),
            is_deleted: false,
            data: b"alice".to_vec(),
            metadata: HashMap::new(),
        };

        storage
            .append_history_region_rows("users", &[version.clone()])
            .unwrap();
        storage
            .upsert_visible_region_rows("users", &[version.clone()])
            .unwrap();

        let visible = storage.scan_visible_region("users", "dev/main").unwrap();
        let history_by_row = storage.scan_history_row_versions("users", row_id).unwrap();
        let history = storage
            .scan_history_region("users", "dev/main", HistoryScan::Row { row_id })
            .unwrap();

        assert_eq!(visible, vec![version.clone()]);
        assert_eq!(history_by_row, vec![version.clone()]);
        assert_eq!(history, vec![version]);
    }

    mod fjall_conformance {
        use crate::storage::Storage;
        use crate::storage::fjall::FjallStorage;
        use crate::storage_conformance_tests_persistent;

        storage_conformance_tests_persistent!(
            fjall,
            || {
                let dir = tempfile::TempDir::new().unwrap();
                let path = dir.path().join("test.fjall");
                let storage = FjallStorage::open(&path, 8 * 1024 * 1024).unwrap();
                // Leak TempDir so the directory lives as long as the storage.
                std::mem::forget(dir);
                Box::new(storage) as Box<dyn Storage>
            },
            |path: &std::path::Path| {
                Box::new(FjallStorage::open(path, 8 * 1024 * 1024).unwrap()) as Box<dyn Storage>
            }
        );
    }
}
