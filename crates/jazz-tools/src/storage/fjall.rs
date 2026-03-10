//! Fjall-backed Storage implementation.
//!
//! Uses a single Fjall keyspace with key-encoded namespaces for all data:
//! objects, commits, ack tiers, catalogue manifest ops, and indices.
//!
//! Key encoding scheme (all keys are UTF-8 strings with hex-encoded binary parts):
//!
//! ```text
//! "obj:{uuid}:meta"                                       -> JSON metadata
//! "obj:{uuid}:br:{branch}:tips"                           -> JSON HashSet<CommitId>
//! "obj:{uuid}:br:{branch}:c:{commit_uuid}"                -> JSON Commit
//! "ack:{commit_hex}"                                      -> JSON HashSet<DurabilityTier>
//! "catman:{app_uuid}:op:{object_uuid}"                    -> JSON CatalogueManifestOp
//! "idx:{table}:{col}:{branch}:{hex_encoded_value}:{uuid}" -> empty (existence is the signal)
//! ```

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;
use std::path::Path;

use fjall::{
    Database as FjallDatabase, Keyspace as FjallKeyspace, KeyspaceCreateOptions,
    OwnedWriteBatch as FjallWriteBatch, PersistMode,
};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::Value;
use crate::sync_manager::DurabilityTier;

use super::{
    CatalogueManifest, CatalogueManifestOp, LoadedBranch, Storage, StorageError,
    key_codec::increment_bytes,
    storage_core::{
        append_catalogue_manifest_op_core, append_catalogue_manifest_ops_core, append_commit_core,
        create_object_core, delete_commit_core, index_insert_core, index_lookup_core,
        index_range_core, index_remove_core, index_scan_all_core, load_branch_core,
        load_catalogue_manifest_core, load_object_metadata_core, set_branch_tails_core,
        store_ack_tier_core,
    },
};

const DATA_KEYSPACE_NAME: &str = "data";

struct FjallInner {
    database: FjallDatabase,
    keyspace: FjallKeyspace,
}

pub struct FjallStorage {
    inner: RefCell<Option<FjallInner>>,
}

struct WriteBatchTxn {
    batch: RefCell<FjallWriteBatch>,
    keyspace: FjallKeyspace,
    pending: RefCell<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
}

impl WriteBatchTxn {
    fn new(database: FjallDatabase, keyspace: FjallKeyspace) -> Self {
        Self {
            batch: RefCell::new(database.batch()),
            keyspace,
            pending: RefCell::new(BTreeMap::new()),
        }
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        if let Some(value) = self.pending.borrow().get(key.as_bytes()) {
            return Ok(value.clone());
        }

        FjallStorage::keyspace_get(&self.keyspace, key)
    }

    fn set(&self, key: &str, value: &[u8]) {
        let key_bytes = key.as_bytes().to_vec();
        let value_bytes = value.to_vec();
        self.batch
            .borrow_mut()
            .insert(&self.keyspace, key_bytes.clone(), value_bytes.clone());
        self.pending
            .borrow_mut()
            .insert(key_bytes, Some(value_bytes));
    }

    fn delete(&self, key: &str) {
        let key_bytes = key.as_bytes().to_vec();
        self.batch
            .borrow_mut()
            .remove(&self.keyspace, key_bytes.clone());
        self.pending.borrow_mut().insert(key_bytes, None);
    }

    fn commit(self) -> Result<(), StorageError> {
        self.batch
            .into_inner()
            .commit()
            .map_err(|e| StorageError::IoError(format!("fjall batch commit: {e}")))
    }
}

impl FjallStorage {
    /// Open a Fjall-backed storage directory at the given path.
    pub fn open(path: impl AsRef<Path>, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let database = FjallDatabase::builder(path.as_ref())
            .cache_size(cache_size_bytes as u64)
            .open()
            .map_err(|e| StorageError::IoError(format!("fjall open: {e}")))?;
        let keyspace = database
            .keyspace(DATA_KEYSPACE_NAME, KeyspaceCreateOptions::default)
            .map_err(|e| StorageError::IoError(format!("fjall keyspace open: {e}")))?;

        Ok(Self {
            inner: RefCell::new(Some(FjallInner { database, keyspace })),
        })
    }

    fn handles(&self) -> Result<(FjallDatabase, FjallKeyspace), StorageError> {
        let inner = self.inner.borrow();
        let inner = inner.as_ref().ok_or_else(Self::closed_error)?;
        Ok((inner.database.clone(), inner.keyspace.clone()))
    }

    fn closed_error() -> StorageError {
        StorageError::IoError("fjall storage is closed".to_string())
    }

    fn keyspace_get(keyspace: &FjallKeyspace, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        keyspace
            .get(key.as_bytes())
            .map_err(|e| StorageError::IoError(format!("fjall get: {e}")))
            .map(|value| value.map(|value| value.to_vec()))
    }

    fn with_batch<R>(
        &self,
        op: impl FnOnce(&WriteBatchTxn) -> Result<R, StorageError>,
    ) -> Result<R, StorageError> {
        let (database, keyspace) = self.handles()?;
        let txn = WriteBatchTxn::new(database, keyspace);
        let output = op(&txn)?;
        txn.commit()?;
        Ok(output)
    }

    fn scan_prefix(
        keyspace: &FjallKeyspace,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let mut end = prefix.as_bytes().to_vec();
        increment_bytes(&mut end);
        Self::scan_range(keyspace, prefix.as_bytes(), &end)
    }

    fn scan_range(
        keyspace: &FjallKeyspace,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let mut out = Vec::new();

        for item in keyspace.range(start.to_vec()..end.to_vec()) {
            let (key, value) = item
                .into_inner()
                .map_err(|e| StorageError::IoError(format!("fjall range item: {e}")))?;
            let key = String::from_utf8(key.as_ref().to_vec())
                .map_err(|e| StorageError::IoError(format!("fjall invalid key utf8: {e}")))?;
            out.push((key, value.to_vec()));
        }

        Ok(out)
    }

    fn scan_prefix_keys(
        keyspace: &FjallKeyspace,
        prefix: &str,
    ) -> Result<Vec<String>, StorageError> {
        Ok(Self::scan_prefix(keyspace, prefix)?
            .into_iter()
            .map(|(key, _)| key)
            .collect())
    }

    fn scan_key_range(
        keyspace: &FjallKeyspace,
        start: &str,
        end: &str,
    ) -> Result<Vec<String>, StorageError> {
        Ok(
            Self::scan_range(keyspace, start.as_bytes(), end.as_bytes())?
                .into_iter()
                .map(|(key, _)| key)
                .collect(),
        )
    }

    /// Persist buffered journal data and drop the database handles.
    pub fn close(&self) -> Result<(), StorageError> {
        let inner = self.inner.borrow_mut().take();
        let Some(inner) = inner else {
            return Ok(());
        };

        let persist_result = inner
            .database
            .persist(PersistMode::SyncAll)
            .map_err(|e| StorageError::IoError(format!("fjall close persist: {e}")));

        drop(inner);

        persist_result
    }
}

impl Storage for FjallStorage {
    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        self.with_batch(|txn| {
            create_object_core(id, metadata, |key, value| {
                txn.set(key, value);
                Ok(())
            })
        })
    }

    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        let (_, keyspace) = self.handles()?;
        load_object_metadata_core(id, |key| Self::keyspace_get(&keyspace, key))
    }

    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        let (_, keyspace) = self.handles()?;
        load_branch_core(
            object_id,
            branch,
            |key| Self::keyspace_get(&keyspace, key),
            |prefix| Self::scan_prefix(&keyspace, prefix),
        )
    }

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
    ) -> Result<(), StorageError> {
        self.with_batch(|txn| {
            append_commit_core(
                object_id,
                branch,
                commit,
                |key| txn.get(key),
                |key, value| {
                    txn.set(key, value);
                    Ok(())
                },
            )
        })
    }

    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        self.with_batch(|txn| {
            delete_commit_core(
                object_id,
                branch,
                commit_id,
                |key| txn.get(key),
                |key, value| {
                    txn.set(key, value);
                    Ok(())
                },
                |key| {
                    txn.delete(key);
                    Ok(())
                },
            )
        })
    }

    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        self.with_batch(|txn| {
            set_branch_tails_core(
                object_id,
                branch,
                tails,
                |key, value| {
                    txn.set(key, value);
                    Ok(())
                },
                |key| {
                    txn.delete(key);
                    Ok(())
                },
            )
        })
    }

    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: DurabilityTier,
    ) -> Result<(), StorageError> {
        self.with_batch(|txn| {
            store_ack_tier_core(
                commit_id,
                tier,
                |key| txn.get(key),
                |key, value| {
                    txn.set(key, value);
                    Ok(())
                },
            )
        })
    }

    fn append_catalogue_manifest_op(
        &mut self,
        app_id: ObjectId,
        op: CatalogueManifestOp,
    ) -> Result<(), StorageError> {
        self.with_batch(|txn| {
            append_catalogue_manifest_op_core(
                app_id,
                op,
                |key| txn.get(key),
                |key, value| {
                    txn.set(key, value);
                    Ok(())
                },
            )
        })
    }

    fn append_catalogue_manifest_ops(
        &mut self,
        app_id: ObjectId,
        ops: &[CatalogueManifestOp],
    ) -> Result<(), StorageError> {
        self.with_batch(|txn| {
            append_catalogue_manifest_ops_core(
                app_id,
                ops,
                |key| txn.get(key),
                |key, value| {
                    txn.set(key, value);
                    Ok(())
                },
            )
        })
    }

    fn load_catalogue_manifest(
        &self,
        app_id: ObjectId,
    ) -> Result<Option<CatalogueManifest>, StorageError> {
        let (_, keyspace) = self.handles()?;
        load_catalogue_manifest_core(app_id, |prefix| Self::scan_prefix(&keyspace, prefix))
    }

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        tracing::trace!(table, column, branch, ?row_id, "index_insert");
        self.with_batch(|txn| {
            index_insert_core(table, column, branch, value, row_id, |key, bytes| {
                txn.set(key, bytes);
                Ok(())
            })
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
        tracing::trace!(table, column, branch, ?row_id, "index_remove");
        self.with_batch(|txn| {
            index_remove_core(table, column, branch, value, row_id, |key| {
                txn.delete(key);
                Ok(())
            })
        })
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        tracing::trace!(table, column, branch, "index_lookup");
        let Ok((_, keyspace)) = self.handles() else {
            return Vec::new();
        };
        index_lookup_core(table, column, branch, value, |prefix| {
            Self::scan_prefix_keys(&keyspace, prefix)
        })
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        let Ok((_, keyspace)) = self.handles() else {
            return Vec::new();
        };
        index_range_core(table, column, branch, start, end, |start_key, end_key| {
            Self::scan_key_range(&keyspace, start_key, end_key)
        })
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        let Ok((_, keyspace)) = self.handles() else {
            return Vec::new();
        };
        index_scan_all_core(table, column, branch, |prefix| {
            Self::scan_prefix_keys(&keyspace, prefix)
        })
    }

    fn flush(&self) {
        let _span = tracing::debug_span!("FjallStorage::flush").entered();
        let Ok((database, _)) = self.handles() else {
            return;
        };
        let _ = database.persist(PersistMode::SyncAll);
    }

    fn flush_wal(&self) {
        let _span = tracing::debug_span!("FjallStorage::flush_wal").entered();
        let Ok((database, _)) = self.handles() else {
            return;
        };
        let _ = database.persist(PersistMode::SyncData);
    }

    fn close(&self) -> Result<(), StorageError> {
        FjallStorage::close(self)
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
        storage.append_commit(id, &branch, commit).unwrap();

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert!(loaded.tails.contains(&commit_id));
        assert_eq!(loaded.commits[0].content, b"first");

        let mut commit2 = make_commit(b"second");
        commit2.parents = smallvec![commit_id];
        let commit2_id = commit2.id();
        storage.append_commit(id, &branch, commit2).unwrap();

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
            storage.append_commit(id, &branch, commit).unwrap();

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

    #[test]
    fn fjall_close_releases_lock_for_immediate_reopen() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("reopen.fjall");

        let storage = FjallStorage::open(&db_path, 8 * 1024 * 1024).unwrap();
        storage.close().unwrap();

        let reopened = FjallStorage::open(&db_path, 8 * 1024 * 1024);
        assert!(reopened.is_ok());
    }
}
