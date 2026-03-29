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
#[cfg(test)]
use crate::query_manager::types::BatchId;
#[cfg(test)]
use crate::query_manager::types::SchemaHash;
use crate::query_manager::types::{QueryBranchRef, Value};
use crate::sync_manager::DurabilityTier;

#[cfg(test)]
use super::PrefixBatchMeta;
use super::{
    CatalogueManifest, CatalogueManifestOp, LoadedBranch, LoadedBranchTips, PrefixBatchCatalog,
    PrefixBatchUpdate, Storage, StorageError,
    storage_core::{
        adjust_table_prefix_batch_refcount_core, append_catalogue_manifest_op_core,
        append_catalogue_manifest_ops_core, append_commit_core, create_object_core,
        index_insert_core, index_lookup_core, index_range_core, index_remove_core,
        index_scan_all_core, load_branch_core, load_branch_tips_core, load_catalogue_manifest_core,
        load_commit_branch_core, load_object_metadata_core, load_prefix_batch_catalog_core,
        load_table_prefix_branches_core, replace_branch_core, store_ack_tier_core,
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

    #[cfg(test)]
    #[allow(clippy::needless_pass_by_value)]
    fn branch_ref(branch: impl Into<String>) -> QueryBranchRef {
        let branch = branch.into();
        let branch_name = BranchName::new(branch.clone());
        if crate::query_manager::types::ComposedBranchName::parse(&branch_name).is_some() {
            return QueryBranchRef::from_branch_name(branch_name);
        }

        let prefix = crate::query_manager::types::BranchPrefixName::new(
            "dev",
            SchemaHash::from_bytes([7; 32]),
            &branch,
        );
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
            .load_table_prefix_branches(table, BranchName::new(prefix))?
            .into_iter()
            .map(|branch| branch.batch_id())
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
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            load_branch_core(object_id, branch, |key| {
                Self::read_get(&tx, &inner.keyspace, key)
            })
        })
    }

    fn load_branch_tips(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranchTips>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            load_branch_tips_core(object_id, branch, |key| {
                Self::read_get(&tx, &inner.keyspace, key)
            })
        })
    }

    fn load_commit_branch(
        &self,
        object_id: ObjectId,
        commit_id: CommitId,
    ) -> Result<Option<QueryBranchRef>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            load_commit_branch_core(object_id, commit_id, |key| {
                Self::read_get(&tx, &inner.keyspace, key)
            })
        })
    }

    fn load_prefix_batch_catalog(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Option<PrefixBatchCatalog>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            load_prefix_batch_catalog_core(object_id, prefix, |key| {
                Self::read_get(&tx, &inner.keyspace, key)
            })
        })
    }

    fn load_table_prefix_branches(
        &self,
        table: &str,
        prefix: BranchName,
    ) -> Result<Vec<QueryBranchRef>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            load_table_prefix_branches_core(table, prefix, |key| {
                Self::read_get(&tx, &inner.keyspace, key)
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
            let tx = RefCell::new(inner.db.write_tx());
            append_commit_core(
                object_id,
                branch,
                commit,
                prefix_batch_update,
                |key| Self::read_get_cell(&tx, &inner.keyspace, key),
                |key, value| Self::set_on_cell(&tx, &inner.keyspace, key, value),
            )?;
            Self::commit_tx(tx.into_inner())
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
            let tx = RefCell::new(inner.db.write_tx());
            replace_branch_core(
                object_id,
                branch,
                commits,
                tails,
                |key| Self::read_get_cell(&tx, &inner.keyspace, key),
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
        branch: &QueryBranchRef,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let tx = RefCell::new(inner.db.write_tx());
            let inserted = index_insert_core(
                table,
                column,
                branch,
                value,
                row_id,
                |key| Self::read_get_cell(&tx, &inner.keyspace, key),
                |key, bytes| Self::set_on_cell(&tx, &inner.keyspace, key, bytes),
            )?;
            if inserted && matches!(column, "_id" | "_id_deleted") {
                adjust_table_prefix_batch_refcount_core(
                    table,
                    branch,
                    1,
                    |key| Self::read_get_cell(&tx, &inner.keyspace, key),
                    |key, bytes| Self::set_on_cell(&tx, &inner.keyspace, key, bytes),
                    |key| Self::delete_on_cell(&tx, &inner.keyspace, key),
                )?;
            }
            Self::commit_tx(tx.into_inner())
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
            let tx = RefCell::new(inner.db.write_tx());
            let removed = index_remove_core(
                table,
                column,
                branch,
                value,
                row_id,
                |key| Self::read_get_cell(&tx, &inner.keyspace, key),
                |key| Self::delete_on_cell(&tx, &inner.keyspace, key),
            )?;
            if removed && matches!(column, "_id" | "_id_deleted") {
                adjust_table_prefix_batch_refcount_core(
                    table,
                    branch,
                    -1,
                    |key| Self::read_get_cell(&tx, &inner.keyspace, key),
                    |key, bytes| Self::set_on_cell(&tx, &inner.keyspace, key, bytes),
                    |key| Self::delete_on_cell(&tx, &inner.keyspace, key),
                )?;
            }
            Self::commit_tx(tx.into_inner())
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
        branch: &QueryBranchRef,
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

    fn index_scan_all(&self, table: &str, column: &str, branch: &QueryBranchRef) -> Vec<ObjectId> {
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
    use smallvec::smallvec;

    fn test_branch_ref(user_branch: &str) -> QueryBranchRef {
        let prefix = crate::query_manager::types::BranchPrefixName::new(
            "dev",
            crate::query_manager::types::SchemaHash::from_bytes([7; 32]),
            user_branch,
        );
        let batch_id = crate::query_manager::types::BatchId::from_uuid(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            user_branch.as_bytes(),
        ));
        QueryBranchRef::from_prefix_and_batch(&prefix, batch_id)
    }

    fn make_commit(content: &[u8]) -> Commit {
        Commit {
            parents: smallvec![],
            content: content.to_vec(),
            timestamp: 12345,
            author: ObjectId::new(),
            metadata: None,
            stored_state: Default::default(),
            ack_state: Default::default(),
        }
    }

    fn test_storage() -> (tempfile::TempDir, FjallStorage) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.fjall");
        let storage = FjallStorage::open(&db_path, 8 * 1024 * 1024).unwrap();
        (temp_dir, storage)
    }

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
    fn fjall_object_roundtrip() {
        let (_temp_dir, mut storage) = test_storage();

        let id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        );
        metadata.insert("app".to_string(), "test".to_string());

        storage.create_object(id, metadata.clone()).unwrap();

        let loaded = storage.load_object_metadata(id).unwrap();
        assert_eq!(loaded, Some(metadata));

        let other = ObjectId::new();
        assert_eq!(storage.load_object_metadata(other).unwrap(), None);
    }

    #[test]
    fn fjall_commit_roundtrip() {
        let (_temp_dir, mut storage) = test_storage();

        let id = ObjectId::new();
        let branch = test_branch_ref("main");
        storage.create_object(id, HashMap::new()).unwrap();

        assert_eq!(storage.load_branch(id, &branch).unwrap(), None);

        let commit = make_commit(b"first");
        let commit_id = commit.id();
        storage.append_commit(id, &branch, commit, None).unwrap();

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert!(loaded.tails.contains(&commit_id));
        assert_eq!(loaded.commits[0].content, b"first");
        let loaded_tips = storage.load_branch_tips(id, &branch).unwrap().unwrap();
        assert_eq!(loaded_tips.tips.len(), 1);
        assert_eq!(loaded_tips.tips[0].id(), commit_id);

        let mut commit2 = make_commit(b"second");
        commit2.parents = smallvec![commit_id];
        let commit2_id = commit2.id();
        storage.append_commit(id, &branch, commit2, None).unwrap();

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 2);
        assert!(!loaded.tails.contains(&commit_id));
        assert!(loaded.tails.contains(&commit2_id));

        storage
            .replace_branch(
                id,
                &branch,
                vec![loaded.commits[1].clone()],
                [commit2_id].into(),
            )
            .unwrap();
        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert_eq!(loaded.commits[0].content, b"second");
        assert_eq!(storage.load_commit_branch(id, commit_id).unwrap(), None);
    }

    #[test]
    fn fjall_commit_roundtrip_spans_segments() {
        let (_temp_dir, mut storage) = test_storage();

        let id = ObjectId::new();
        let branch = test_branch_ref("main");
        storage.create_object(id, HashMap::new()).unwrap();

        let mut parent_id = None;
        for idx in 0..40 {
            let mut commit = make_commit(format!("commit-{idx}").as_bytes());
            if let Some(parent_id) = parent_id {
                commit.parents = smallvec![parent_id];
            }
            parent_id = Some(commit.id());
            storage.append_commit(id, &branch, commit, None).unwrap();
        }

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 40);
        assert_eq!(loaded.tails, [parent_id.unwrap()].into());
        let loaded_tips = storage.load_branch_tips(id, &branch).unwrap().unwrap();
        assert_eq!(loaded_tips.tips.len(), 1);
        assert_eq!(loaded_tips.tips[0].id(), parent_id.unwrap());
    }

    #[test]
    fn fjall_tracks_commit_branches_and_prefix_leaves() {
        let (_temp_dir, mut storage) = test_storage();
        let id = ObjectId::new();
        storage.create_object(id, HashMap::new()).unwrap();

        let prefix = "dev-070707070707-main";
        let branch1 = crate::query_manager::types::QueryBranchRef::from_branch_name(
            BranchName::new(format!("{prefix}-b{:032x}", 1)),
        );
        let branch2 = crate::query_manager::types::QueryBranchRef::from_branch_name(
            BranchName::new(format!("{prefix}-b{:032x}", 2)),
        );
        let batch1_id = crate::query_manager::types::BatchId::from_uuid(uuid::Uuid::from_u128(1));
        let batch2_id = crate::query_manager::types::BatchId::from_uuid(uuid::Uuid::from_u128(2));

        let commit1 = make_commit(b"first");
        let commit1_id = commit1.id();
        storage
            .append_commit(
                id,
                &branch1,
                commit1.clone(),
                Some(PrefixBatchUpdate {
                    prefix: prefix.to_string(),
                    batch_meta: PrefixBatchMeta {
                        batch_id: batch1_id,
                        batch_ord: 0,
                        root_commit_id: commit1_id,
                        head_commit_id: commit1_id,
                        first_timestamp: commit1.timestamp,
                        last_timestamp: commit1.timestamp,
                        parent_batch_ords: Vec::new(),
                        child_count: 0,
                    },
                    remove_leaf_batches: HashSet::new(),
                    increment_parent_child_counts: Vec::new(),
                }),
            )
            .unwrap();

        let mut commit2 = make_commit(b"second");
        commit2.parents = smallvec![commit1_id];
        let commit2_id = commit2.id();
        storage
            .append_commit(
                id,
                &branch2,
                commit2.clone(),
                Some(PrefixBatchUpdate {
                    prefix: prefix.to_string(),
                    batch_meta: PrefixBatchMeta {
                        batch_id: batch2_id,
                        batch_ord: 1,
                        root_commit_id: commit2_id,
                        head_commit_id: commit2_id,
                        first_timestamp: commit2.timestamp,
                        last_timestamp: commit2.timestamp,
                        parent_batch_ords: vec![0],
                        child_count: 0,
                    },
                    remove_leaf_batches: HashSet::from([batch1_id]),
                    increment_parent_child_counts: vec![batch1_id],
                }),
            )
            .unwrap();

        assert_eq!(
            storage.load_commit_branch(id, commit1_id).unwrap(),
            Some(branch1)
        );
        assert_eq!(
            storage.load_commit_branch(id, commit2_id).unwrap(),
            Some(branch2)
        );
        assert_eq!(
            storage
                .load_prefix_batch_catalog(id, prefix)
                .unwrap()
                .map(|catalog| catalog.leaf_batch_ids().collect::<HashSet<_>>()),
            Some(HashSet::from([batch2_id]))
        );

        let branch1_name = format!("{prefix}-{}", batch1_id.branch_segment());
        let branch2_name = format!("{prefix}-{}", batch2_id.branch_segment());
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        storage
            .index_insert("users", "_id", &branch1_name, &Value::Uuid(row1), row1)
            .unwrap();
        storage
            .index_insert(
                "users",
                "_id_deleted",
                &branch2_name,
                &Value::Uuid(row2),
                row2,
            )
            .unwrap();
        assert_eq!(
            storage.load_table_prefix_batches("users", prefix).unwrap(),
            HashSet::from([batch1_id, batch2_id])
        );

        storage
            .index_remove("users", "_id", &branch1_name, &Value::Uuid(row1), row1)
            .unwrap();
        storage
            .index_remove(
                "users",
                "_id_deleted",
                &branch2_name,
                &Value::Uuid(row2),
                row2,
            )
            .unwrap();
        assert_eq!(
            storage.load_table_prefix_batches("users", prefix).unwrap(),
            HashSet::new()
        );
    }

    #[test]
    fn fjall_index_ops() {
        let (_temp_dir, mut storage) = test_storage();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();
        let row4 = ObjectId::new();

        storage
            .index_insert("users", "age", "main", &Value::Integer(20), row1)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row2)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row3)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(30), row4)
            .unwrap();

        let results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));

        let results = storage.index_range(
            "users",
            "age",
            "main",
            Bound::Included(&Value::Integer(25)),
            Bound::Excluded(&Value::Integer(30)),
        );
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));

        let results = storage.index_range(
            "users",
            "age",
            "main",
            Bound::Unbounded,
            Bound::Excluded(&Value::Integer(26)),
        );
        assert_eq!(results.len(), 3);
        assert!(results.contains(&row1));
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));

        let results = storage.index_range(
            "users",
            "age",
            "main",
            Bound::Included(&Value::Integer(30)),
            Bound::Unbounded,
        );
        assert_eq!(results.len(), 1);
        assert!(results.contains(&row4));

        let results = storage.index_scan_all("users", "age", "main");
        assert_eq!(results.len(), 4);

        storage
            .index_remove("users", "age", "main", &Value::Integer(25), row2)
            .unwrap();
        let results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(results.len(), 1);
        assert!(results.contains(&row3));
    }

    #[test]
    fn fjall_persistence() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("persist.fjall");

        let id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        );

        let commit_content = b"persistent data";
        let branch = test_branch_ref("main");

        {
            let mut storage = FjallStorage::open(&db_path, 8 * 1024 * 1024).unwrap();
            storage.create_object(id, metadata.clone()).unwrap();

            let commit = make_commit(commit_content);
            storage.append_commit(id, &branch, commit, None).unwrap();

            storage
                .index_insert(
                    "users",
                    "name",
                    "main",
                    &Value::Text("Alice".to_string()),
                    id,
                )
                .unwrap();
            storage.flush();
        }

        {
            let storage = FjallStorage::open(&db_path, 8 * 1024 * 1024).unwrap();

            let loaded_meta = storage.load_object_metadata(id).unwrap();
            assert_eq!(loaded_meta, Some(metadata));

            let loaded_branch = storage.load_branch(id, &branch).unwrap().unwrap();
            assert_eq!(loaded_branch.commits.len(), 1);
            assert_eq!(loaded_branch.commits[0].content, commit_content);

            let results =
                storage.index_lookup("users", "name", "main", &Value::Text("Alice".to_string()));
            assert_eq!(results.len(), 1);
            assert!(results.contains(&id));
        }
    }

    #[test]
    fn fjall_catalogue_manifest_roundtrip() {
        let (_temp_dir, mut storage) = test_storage();
        let app_id = ObjectId::new();
        let schema_object_id = ObjectId::new();
        let lens_object_id = ObjectId::new();
        let schema_hash = crate::query_manager::types::SchemaHash::from_bytes([0x11; 32]);
        let source_hash = crate::query_manager::types::SchemaHash::from_bytes([0x22; 32]);
        let target_hash = crate::query_manager::types::SchemaHash::from_bytes([0x33; 32]);

        storage
            .append_catalogue_manifest_op(
                app_id,
                crate::storage::CatalogueManifestOp::SchemaSeen {
                    object_id: schema_object_id,
                    schema_hash,
                },
            )
            .unwrap();
        storage
            .append_catalogue_manifest_op(
                app_id,
                crate::storage::CatalogueManifestOp::LensSeen {
                    object_id: lens_object_id,
                    source_hash,
                    target_hash,
                },
            )
            .unwrap();
        storage
            .append_catalogue_manifest_op(
                app_id,
                crate::storage::CatalogueManifestOp::SchemaSeen {
                    object_id: schema_object_id,
                    schema_hash,
                },
            )
            .unwrap();

        let manifest = storage.load_catalogue_manifest(app_id).unwrap().unwrap();
        assert_eq!(
            manifest.schema_seen.get(&schema_object_id),
            Some(&schema_hash)
        );
        assert_eq!(
            manifest.lens_seen.get(&lens_object_id),
            Some(&crate::storage::CatalogueLensSeen {
                source_hash,
                target_hash,
            })
        );
    }
}
