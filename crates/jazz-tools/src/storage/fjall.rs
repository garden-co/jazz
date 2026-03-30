//! Fjall-backed Storage implementation.
//!
//! Uses one transactional Fjall database with a single keyspace and the same
//! UTF-8 key encoding scheme as the other native backends.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::path::Path;

use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
    SingleWriterWriteTx,
};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::Value;
use crate::sync_manager::DurabilityTier;

use super::{
    CatalogueManifest, CatalogueManifestOp, LoadedBranch, Storage, StorageError,
    storage_core::{
        append_catalogue_manifest_op_core, append_catalogue_manifest_ops_core, append_commit_core,
        create_object_core, delete_commit_core, index_insert_core, index_lookup_core,
        index_range_core, index_remove_core, index_scan_all_core, load_branch_core,
        load_catalogue_manifest_core, load_object_metadata_core, set_branch_tails_core,
        store_ack_tier_core,
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

    fn scan_prefix_keys(
        txn: &impl Readable,
        keyspace: &SingleWriterTxKeyspace,
        prefix: &str,
    ) -> Result<Vec<String>, StorageError> {
        let mut out = Vec::new();
        for item in txn.prefix(keyspace, prefix.as_bytes()) {
            let key = item
                .key()
                .map_err(|e| StorageError::IoError(format!("fjall prefix key: {e}")))?;
            let key = String::from_utf8(key.to_vec())
                .map_err(|e| StorageError::IoError(format!("fjall invalid key utf8: {e}")))?;
            out.push(key);
        }
        Ok(out)
    }

    fn scan_key_range(
        txn: &impl Readable,
        keyspace: &SingleWriterTxKeyspace,
        start: &str,
        end: &str,
    ) -> Result<Vec<String>, StorageError> {
        let mut out = Vec::new();
        for item in txn.range(keyspace, start.as_bytes()..end.as_bytes()) {
            let key = item
                .key()
                .map_err(|e| StorageError::IoError(format!("fjall range key: {e}")))?;
            let key = String::from_utf8(key.to_vec())
                .map_err(|e| StorageError::IoError(format!("fjall invalid key utf8: {e}")))?;
            out.push(key);
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
    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let mut tx = inner.db.write_tx();
            create_object_core(id, metadata, |key, value| {
                Self::set_on_tx(&mut tx, &inner.keyspace, key, value)
            })?;
            Self::commit_tx(tx)
        })
    }

    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            load_object_metadata_core(id, |key| Self::read_get(&tx, &inner.keyspace, key))
        })
    }

    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            load_branch_core(
                object_id,
                branch,
                |key| Self::read_get(&tx, &inner.keyspace, key),
                |prefix| Self::scan_prefix(&tx, &inner.keyspace, prefix),
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
            let tx = RefCell::new(inner.db.write_tx());
            append_commit_core(
                object_id,
                branch,
                commit,
                |key| Self::read_get_cell(&tx, &inner.keyspace, key),
                |key, value| Self::set_on_cell(&tx, &inner.keyspace, key, value),
            )?;
            Self::commit_tx(tx.into_inner())
        })
    }

    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let tx = RefCell::new(inner.db.write_tx());
            delete_commit_core(
                object_id,
                branch,
                commit_id,
                |key| Self::read_get_cell(&tx, &inner.keyspace, key),
                |key, value| Self::set_on_cell(&tx, &inner.keyspace, key, value),
                |key| Self::delete_on_cell(&tx, &inner.keyspace, key),
            )?;
            Self::commit_tx(tx.into_inner())
        })
    }

    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let tx = RefCell::new(inner.db.write_tx());
            set_branch_tails_core(
                object_id,
                branch,
                tails,
                |key, value| Self::set_on_cell(&tx, &inner.keyspace, key, value),
                |key| Self::delete_on_cell(&tx, &inner.keyspace, key),
            )?;
            Self::commit_tx(tx.into_inner())
        })
    }

    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: DurabilityTier,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let tx = RefCell::new(inner.db.write_tx());
            store_ack_tier_core(
                commit_id,
                tier,
                |key| Self::read_get_cell(&tx, &inner.keyspace, key),
                |key, value| Self::set_on_cell(&tx, &inner.keyspace, key, value),
            )?;
            Self::commit_tx(tx.into_inner())
        })
    }

    fn append_catalogue_manifest_op(
        &mut self,
        app_id: ObjectId,
        op: CatalogueManifestOp,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let tx = RefCell::new(inner.db.write_tx());
            append_catalogue_manifest_op_core(
                app_id,
                op,
                |key| Self::read_get_cell(&tx, &inner.keyspace, key),
                |key, value| Self::set_on_cell(&tx, &inner.keyspace, key, value),
            )?;
            Self::commit_tx(tx.into_inner())
        })
    }

    fn append_catalogue_manifest_ops(
        &mut self,
        app_id: ObjectId,
        ops: &[CatalogueManifestOp],
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let tx = RefCell::new(inner.db.write_tx());
            append_catalogue_manifest_ops_core(
                app_id,
                ops,
                |key| Self::read_get_cell(&tx, &inner.keyspace, key),
                |key, value| Self::set_on_cell(&tx, &inner.keyspace, key, value),
            )?;
            Self::commit_tx(tx.into_inner())
        })
    }

    fn load_catalogue_manifest(
        &self,
        app_id: ObjectId,
    ) -> Result<Option<CatalogueManifest>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            load_catalogue_manifest_core(app_id, |prefix| {
                Self::scan_prefix(&tx, &inner.keyspace, prefix)
            })
        })
    }

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let mut tx = inner.db.write_tx();
            index_insert_core(table, column, branch, value, row_id, |key, bytes| {
                Self::set_on_tx(&mut tx, &inner.keyspace, key, bytes)
            })?;
            Self::commit_tx(tx)
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
            let mut tx = inner.db.write_tx();
            index_remove_core(table, column, branch, value, row_id, |key| {
                Self::delete_on_tx(&mut tx, &inner.keyspace, key)
            })?;
            Self::commit_tx(tx)
        })
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            Ok(index_lookup_core(table, column, branch, value, |prefix| {
                Self::scan_prefix_keys(&tx, &inner.keyspace, prefix)
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
            let tx = inner.db.read_tx();
            Ok(index_range_core(
                table,
                column,
                branch,
                start,
                end,
                |start_key, end_key| Self::scan_key_range(&tx, &inner.keyspace, start_key, end_key),
            ))
        })
        .unwrap_or_default()
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            Ok(index_scan_all_core(table, column, branch, |prefix| {
                Self::scan_prefix_keys(&tx, &inner.keyspace, prefix)
            }))
        })
        .unwrap_or_default()
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
