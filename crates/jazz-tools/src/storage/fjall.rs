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
    CatalogueManifest, CatalogueManifestOp, LoadedBranch, PrefixLeafUpdate, Storage, StorageError,
    storage_core::{
        append_catalogue_manifest_op_core, append_catalogue_manifest_ops_core, append_commit_core,
        create_object_core, delete_commit_core, index_insert_core, index_lookup_core,
        index_range_core, index_remove_core, index_scan_all_core, load_branch_core,
        load_catalogue_manifest_core, load_commit_branch_core, load_object_metadata_core,
        load_prefix_leaf_branches_core, load_table_prefix_branches_core,
        register_table_prefix_branch_core, set_branch_tails_core, store_ack_tier_core,
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

    fn load_commit_branch(
        &self,
        object_id: ObjectId,
        commit_id: CommitId,
    ) -> Result<Option<BranchName>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            load_commit_branch_core(object_id, commit_id, |key| {
                Self::read_get(&tx, &inner.keyspace, key)
            })
        })
    }

    fn load_prefix_leaf_branches(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Option<HashSet<BranchName>>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            load_prefix_leaf_branches_core(object_id, prefix, |key| {
                Self::read_get(&tx, &inner.keyspace, key)
            })
        })
    }

    fn register_table_prefix_branch(
        &mut self,
        table: &str,
        prefix: &str,
        branch: &BranchName,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let mut tx = inner.db.write_tx();
            register_table_prefix_branch_core(table, prefix, branch, |key, value| {
                Self::set_on_tx(&mut tx, &inner.keyspace, key, value)
            })?;
            Self::commit_tx(tx)
        })
    }

    fn load_table_prefix_branches(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<HashSet<BranchName>, StorageError> {
        self.with_inner(|inner| {
            let tx = inner.db.read_tx();
            load_table_prefix_branches_core(table, prefix, |key_prefix| {
                Self::scan_prefix(&tx, &inner.keyspace, key_prefix)
            })
        })
    }

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
        prefix_leaf_update: Option<PrefixLeafUpdate>,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            let tx = RefCell::new(inner.db.write_tx());
            append_commit_core(
                object_id,
                branch,
                commit,
                prefix_leaf_update,
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
    use smallvec::smallvec;

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
        let branch = BranchName::new("main");
        storage.create_object(id, HashMap::new()).unwrap();

        assert_eq!(storage.load_branch(id, &branch).unwrap(), None);

        let commit = make_commit(b"first");
        let commit_id = commit.id();
        storage.append_commit(id, &branch, commit, None).unwrap();

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert!(loaded.tails.contains(&commit_id));
        assert_eq!(loaded.commits[0].content, b"first");

        let mut commit2 = make_commit(b"second");
        commit2.parents = smallvec![commit_id];
        let commit2_id = commit2.id();
        storage.append_commit(id, &branch, commit2, None).unwrap();

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 2);
        assert!(!loaded.tails.contains(&commit_id));
        assert!(loaded.tails.contains(&commit2_id));

        storage.delete_commit(id, &branch, commit_id).unwrap();
        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert_eq!(loaded.commits[0].content, b"second");
    }

    #[test]
    fn fjall_tracks_commit_branches_and_prefix_leaves() {
        let (_temp_dir, mut storage) = test_storage();
        let id = ObjectId::new();
        storage.create_object(id, HashMap::new()).unwrap();

        let prefix = "dev-070707070707-main";
        let branch1 = BranchName::new(format!("{prefix}-b{:032x}", 1));
        let branch2 = BranchName::new(format!("{prefix}-b{:032x}", 2));

        let commit1 = make_commit(b"first");
        let commit1_id = commit1.id();
        storage
            .append_commit(
                id,
                &branch1,
                commit1,
                Some(PrefixLeafUpdate {
                    prefix: prefix.to_string(),
                    remove_leaf_branches: HashSet::new(),
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
                commit2,
                Some(PrefixLeafUpdate {
                    prefix: prefix.to_string(),
                    remove_leaf_branches: HashSet::from([branch1]),
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
            storage.load_prefix_leaf_branches(id, prefix).unwrap(),
            Some(HashSet::from([branch2]))
        );

        storage
            .register_table_prefix_branch("users", prefix, &branch1)
            .unwrap();
        storage
            .register_table_prefix_branch("users", prefix, &branch2)
            .unwrap();
        assert_eq!(
            storage.load_table_prefix_branches("users", prefix).unwrap(),
            HashSet::from([branch1, branch2])
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
        let branch = BranchName::new("main");

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
