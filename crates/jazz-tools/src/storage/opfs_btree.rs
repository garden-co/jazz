//! opfs-btree-backed Storage implementation.
//!
//! Uses a single opfs-btree instance with key-encoded namespaces for all data:
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
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

#[cfg(target_arch = "wasm32")]
use opfs_btree::OpfsFile;
#[cfg(not(target_arch = "wasm32"))]
use opfs_btree::StdFile;
use opfs_btree::{
    BTreeError, BTreeOptions, DurableBTreeFiles, DurableOpfsBTree, MemoryFile, SyncFile,
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

const MIN_CACHE_SIZE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
enum AnyFile {
    Memory(MemoryFile),
    #[cfg(not(target_arch = "wasm32"))]
    Std(StdFile),
    #[cfg(target_arch = "wasm32")]
    Opfs(OpfsFile),
}

impl SyncFile for AnyFile {
    fn len(&self) -> Result<u64, BTreeError> {
        match self {
            Self::Memory(file) => file.len(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.len(),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.len(),
        }
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError> {
        match self {
            Self::Memory(file) => file.read_exact_at(offset, buf),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.read_exact_at(offset, buf),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.read_exact_at(offset, buf),
        }
    }

    fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError> {
        match self {
            Self::Memory(file) => file.write_all_at(offset, buf),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.write_all_at(offset, buf),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.write_all_at(offset, buf),
        }
    }

    fn truncate(&self, len: u64) -> Result<(), BTreeError> {
        match self {
            Self::Memory(file) => file.truncate(len),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.truncate(len),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.truncate(len),
        }
    }

    fn flush(&self) -> Result<(), BTreeError> {
        match self {
            Self::Memory(file) => file.flush(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.flush(),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.flush(),
        }
    }
}

pub struct OpfsBTreeStorage {
    tree: RefCell<DurableOpfsBTree<AnyFile>>,
}

impl OpfsBTreeStorage {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(path: impl AsRef<Path>, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let files = DurableBTreeFiles::<StdFile>::open(path)
            .map(|files| files.map(AnyFile::Std))
            .map_err(map_storage_err)?;
        Self::open_with_files(files, cache_size_bytes)
    }

    pub fn memory(cache_size_bytes: usize) -> Result<Self, StorageError> {
        let files = DurableBTreeFiles::<MemoryFile>::memory().map(AnyFile::Memory);
        Self::open_with_files(files, cache_size_bytes)
    }

    #[cfg(target_arch = "wasm32")]
    pub fn with_opfs(_file: OpfsFile, _cache_size_bytes: usize) -> Result<Self, StorageError> {
        Err(StorageError::IoError(
            "OpfsBTreeStorage::with_opfs is unsupported for durable mode; use open_opfs"
                .to_string(),
        ))
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn open_opfs(namespace: &str, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let files = DurableBTreeFiles::<OpfsFile>::open_opfs(namespace)
            .await
            .map_err(map_storage_err)?
            .map(AnyFile::Opfs);
        Self::open_with_files(files, cache_size_bytes)
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn destroy_opfs(namespace: &str) -> Result<(), StorageError> {
        DurableBTreeFiles::<OpfsFile>::destroy_opfs(namespace)
            .await
            .map_err(map_storage_err)
    }

    fn open_with_files(
        files: DurableBTreeFiles<AnyFile>,
        cache_size_bytes: usize,
    ) -> Result<Self, StorageError> {
        let tree = DurableOpfsBTree::open(files, Self::options(cache_size_bytes))
            .map_err(map_storage_err)?;
        Ok(Self {
            tree: RefCell::new(tree),
        })
    }

    fn options(cache_size_bytes: usize) -> BTreeOptions {
        BTreeOptions {
            cache_bytes: cache_size_bytes.max(MIN_CACHE_SIZE_BYTES),
            pin_internal_pages: true,
            read_coalesce_pages: 4,
            ..Default::default()
        }
    }

    fn with_tree_mut<R>(
        &self,
        f: impl FnOnce(&mut DurableOpfsBTree<AnyFile>) -> Result<R, StorageError>,
    ) -> Result<R, StorageError> {
        let mut tree = self
            .tree
            .try_borrow_mut()
            .map_err(|_| StorageError::IoError("opfs-btree already borrowed".to_string()))?;
        f(&mut tree)
    }

    fn tree_insert(&self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.with_tree_mut(|tree| tree.put(key.as_bytes(), value).map_err(map_storage_err))
    }

    fn tree_read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.with_tree_mut(|tree| tree.get(key.as_bytes()).map_err(map_storage_err))
    }

    fn tree_delete(&self, key: &str) -> Result<(), StorageError> {
        self.with_tree_mut(|tree| tree.delete(key.as_bytes()).map_err(map_storage_err))
    }

    fn tree_scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let start = prefix.as_bytes();
        let mut end = start.to_vec();
        increment_bytes(&mut end);
        self.tree_scan_range_bytes(start, &end)
    }

    fn tree_scan_keys(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        Ok(self
            .tree_scan_prefix(prefix)?
            .into_iter()
            .map(|(key, _)| key)
            .collect())
    }

    fn tree_scan_key_range(&self, start: &str, end: &str) -> Result<Vec<String>, StorageError> {
        Ok(self
            .tree_scan_range_bytes(start.as_bytes(), end.as_bytes())?
            .into_iter()
            .map(|(key, _)| key)
            .collect())
    }

    fn tree_scan_range_bytes(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        if start >= end {
            return Ok(Vec::new());
        }

        self.with_tree_mut(|tree| {
            let entries = tree
                .range(start, end, usize::MAX)
                .map_err(map_storage_err)?;

            entries
                .into_iter()
                .map(|(key, value)| {
                    let key = String::from_utf8(key)
                        .map_err(|e| StorageError::IoError(format!("invalid key utf8: {}", e)))?;
                    Ok((key, value))
                })
                .collect()
        })
    }
}

impl Storage for OpfsBTreeStorage {
    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        create_object_core(id, metadata, |key, value| self.tree_insert(key, value))
    }

    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        load_object_metadata_core(id, |key| self.tree_read(key))
    }

    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        load_branch_core(
            object_id,
            branch,
            |key| self.tree_read(key),
            |prefix| self.tree_scan_prefix(prefix),
        )
    }

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
    ) -> Result<(), StorageError> {
        append_commit_core(
            object_id,
            branch,
            commit,
            |key| self.tree_read(key),
            |key, value| self.tree_insert(key, value),
        )
    }

    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        delete_commit_core(
            object_id,
            branch,
            commit_id,
            |key| self.tree_read(key),
            |key, value| self.tree_insert(key, value),
            |key| self.tree_delete(key),
        )
    }

    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        set_branch_tails_core(
            object_id,
            branch,
            tails,
            |key, value| self.tree_insert(key, value),
            |key| self.tree_delete(key),
        )
    }

    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: DurabilityTier,
    ) -> Result<(), StorageError> {
        store_ack_tier_core(
            commit_id,
            tier,
            |key| self.tree_read(key),
            |key, value| self.tree_insert(key, value),
        )
    }

    fn append_catalogue_manifest_op(
        &mut self,
        app_id: ObjectId,
        op: CatalogueManifestOp,
    ) -> Result<(), StorageError> {
        append_catalogue_manifest_op_core(
            app_id,
            op,
            |key| self.tree_read(key),
            |key, value| self.tree_insert(key, value),
        )
    }

    fn append_catalogue_manifest_ops(
        &mut self,
        app_id: ObjectId,
        ops: &[CatalogueManifestOp],
    ) -> Result<(), StorageError> {
        append_catalogue_manifest_ops_core(
            app_id,
            ops,
            |key| self.tree_read(key),
            |key, value| self.tree_insert(key, value),
        )
    }

    fn load_catalogue_manifest(
        &self,
        app_id: ObjectId,
    ) -> Result<Option<CatalogueManifest>, StorageError> {
        load_catalogue_manifest_core(app_id, |prefix| self.tree_scan_prefix(prefix))
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
        index_insert_core(table, column, branch, value, row_id, |key, bytes| {
            self.tree_insert(key, bytes)
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
        index_remove_core(table, column, branch, value, row_id, |key| {
            self.tree_delete(key)
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
        index_lookup_core(table, column, branch, value, |prefix| {
            self.tree_scan_keys(prefix)
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
        index_range_core(table, column, branch, start, end, |start_key, end_key| {
            self.tree_scan_key_range(start_key, end_key)
        })
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        index_scan_all_core(table, column, branch, |prefix| self.tree_scan_keys(prefix))
    }

    fn flush(&self) {
        let _span = tracing::debug_span!("OpfsBTreeStorage::flush").entered();
        if let Err(error) = self.with_tree_mut(|tree| tree.checkpoint().map_err(map_storage_err)) {
            tracing::warn!(?error, "OpfsBTreeStorage flush failed");
        }
    }

    fn flush_wal(&self) {
        let _span = tracing::debug_span!("OpfsBTreeStorage::flush_wal").entered();
        if let Err(error) = self.with_tree_mut(|tree| tree.flush_wal().map_err(map_storage_err)) {
            tracing::warn!(?error, "OpfsBTreeStorage WAL flush failed");
        }
    }
}

fn map_storage_err(error: BTreeError) -> StorageError {
    StorageError::IoError(format!("opfs-btree: {}", error))
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

    fn test_storage() -> OpfsBTreeStorage {
        OpfsBTreeStorage::memory(4 * 1024 * 1024).unwrap()
    }

    #[test]
    fn opfs_btree_object_roundtrip() {
        let mut storage = test_storage();

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
    fn opfs_btree_commit_roundtrip() {
        let mut storage = test_storage();

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
    fn opfs_btree_index_ops() {
        let mut storage = test_storage();

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

        let results = storage.index_lookup("users", "age", "main", &Value::Integer(99));
        assert!(results.is_empty());

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
    fn opfs_btree_index_branch_isolation() {
        let mut storage = test_storage();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row1)
            .unwrap();
        storage
            .index_insert("users", "age", "feature", &Value::Integer(25), row2)
            .unwrap();

        let main_results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(main_results.len(), 1);
        assert!(main_results.contains(&row1));

        let feature_results = storage.index_lookup("users", "age", "feature", &Value::Integer(25));
        assert_eq!(feature_results.len(), 1);
        assert!(feature_results.contains(&row2));
    }

    #[test]
    fn opfs_btree_ack_tier_roundtrip() {
        let mut storage = test_storage();

        let commit_id = CommitId([99u8; 32]);

        storage
            .store_ack_tier(commit_id, DurabilityTier::Worker)
            .unwrap();
        storage
            .store_ack_tier(commit_id, DurabilityTier::EdgeServer)
            .unwrap();

        let key = super::super::key_codec::ack_key(commit_id);
        let data = storage.tree_read(&key).unwrap().unwrap();
        let tiers: HashSet<DurabilityTier> = serde_json::from_slice(&data).unwrap();
        assert!(tiers.contains(&DurabilityTier::Worker));
        assert!(tiers.contains(&DurabilityTier::EdgeServer));
    }

    #[test]
    fn opfs_btree_persistence() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.opfsbtree");

        let id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        );

        let commit_content = b"persistent data";
        let branch = BranchName::new("main");

        {
            let mut storage = OpfsBTreeStorage::open(&db_path, 4 * 1024 * 1024).unwrap();
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
            let storage = OpfsBTreeStorage::open(&db_path, 4 * 1024 * 1024).unwrap();

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
    fn opfs_btree_flush_wal_persists_without_snapshot() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("wal-only.opfsbtree");

        {
            let storage = OpfsBTreeStorage::open(&db_path, 4 * 1024 * 1024).unwrap();
            storage.tree_insert("kv:stable", b"wal-value").unwrap();
            storage.flush_wal();
        }

        {
            let storage = OpfsBTreeStorage::open(&db_path, 4 * 1024 * 1024).unwrap();
            assert_eq!(
                storage.tree_read("kv:stable").unwrap(),
                Some(b"wal-value".to_vec())
            );
        }
    }

    #[test]
    fn opfs_btree_catalogue_manifest_roundtrip() {
        let mut storage = test_storage();
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
