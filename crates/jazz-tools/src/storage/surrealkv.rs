//! SurrealKV-backed Storage implementation.
//!
//! Uses a single SurrealKV tree with key-encoded namespaces for all data:
//! objects, commits, ack tiers, and indices.
//!
//! Key encoding scheme (all keys are UTF-8 strings with hex-encoded binary parts):
//!
//! ```text
//! "obj:{uuid}:meta"                                       → JSON metadata
//! "obj:{uuid}:br:{branch}:tips"                           → JSON HashSet<CommitId>
//! "obj:{uuid}:br:{branch}:c:{commit_uuid}"                → JSON Commit
//! "ack:{commit_hex}"                                      → JSON HashSet<PersistenceTier>
//! "idx:{table}:{col}:{branch}:{hex_encoded_value}:{uuid}" → empty (existence is the signal)
//! ```

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::path::Path;
use std::sync::OnceLock;
use std::sync::mpsc;

use surrealkv::{
    Durability as SurrealDurability, LSMIterator, Mode as SurrealMode,
    Transaction as SurrealTransaction, Tree as SurrealTree, TreeBuilder as SurrealTreeBuilder,
};
use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime as TokioRuntime};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::Value;
use crate::sync_manager::PersistenceTier;

use super::{
    LoadedBranch, Storage, StorageError,
    key_codec::{
        ack_key, branch_tips_key, commit_key, commit_prefix, increment_bytes, index_entry_key,
        index_prefix, index_range_scan_bounds, index_value_prefix, obj_meta_key,
        parse_uuid_from_index_key,
    },
};

/// Minimum memtable size for SurrealKV.
const MIN_MEMTABLE_SIZE: usize = 4 * 1024 * 1024;

/// A dedicated key used for `flush()` durability barriers.
const FLUSH_MARKER_KEY: &str = "sys:flush_marker";

pub struct SurrealKvStorage {
    tree: SurrealTree,
    runtime: &'static TokioRuntime,
}

impl SurrealKvStorage {
    /// Open a file-backed SurrealKvStorage at the given path.
    pub fn open(path: impl AsRef<Path>, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let runtime = shared_runtime();

        let tree = {
            let _guard = runtime.enter();
            SurrealTreeBuilder::new()
                .with_path(path.as_ref().to_path_buf())
                .with_level_count(4)
                .with_max_memtable_size(cache_size_bytes.max(MIN_MEMTABLE_SIZE))
                .without_compression()
                .build()
                .map_err(|e| StorageError::IoError(format!("surrealkv open: {}", e)))?
        };

        Ok(Self { tree, runtime })
    }

    fn begin_read_txn(&self) -> Result<SurrealTransaction, StorageError> {
        let _guard = self.runtime.enter();
        self.tree
            .begin_with_mode(SurrealMode::ReadOnly)
            .map_err(|e| StorageError::IoError(format!("surrealkv begin read txn: {}", e)))
    }

    fn begin_write_txn(
        &self,
        durability: SurrealDurability,
    ) -> Result<SurrealTransaction, StorageError> {
        let _guard = self.runtime.enter();
        self.tree
            .begin()
            .map(|txn| txn.with_durability(durability))
            .map_err(|e| StorageError::IoError(format!("surrealkv begin write txn: {}", e)))
    }

    /// Close the underlying SurrealKV tree and release lock files.
    pub fn close(&self) -> Result<(), StorageError> {
        let tree = self.tree.clone();
        let (tx, rx) = mpsc::sync_channel(1);
        self.runtime.spawn(async move {
            let _ = tx.send(tree.close().await);
        });
        let close_result = rx
            .recv()
            .map_err(|e| StorageError::IoError(format!("surrealkv close recv: {}", e)))?;
        close_result.map_err(|e| StorageError::IoError(format!("surrealkv close: {}", e)))
    }

    fn commit_txn(&self, txn: &mut SurrealTransaction) -> Result<(), StorageError> {
        futures::executor::block_on(async { txn.commit().await })
            .map_err(|e| StorageError::IoError(format!("surrealkv commit: {}", e)))
    }

    fn txn_get(txn: &SurrealTransaction, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        txn.get(key.as_bytes())
            .map_err(|e| StorageError::IoError(format!("surrealkv get: {}", e)))
    }

    fn txn_set(txn: &mut SurrealTransaction, key: &str, value: &[u8]) -> Result<(), StorageError> {
        txn.set(key.as_bytes(), value)
            .map_err(|e| StorageError::IoError(format!("surrealkv set: {}", e)))
    }

    fn txn_delete(txn: &mut SurrealTransaction, key: &str) -> Result<(), StorageError> {
        txn.delete(key.as_bytes())
            .map_err(|e| StorageError::IoError(format!("surrealkv delete: {}", e)))
    }

    fn scan_prefix(
        txn: &SurrealTransaction,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let mut end = prefix.as_bytes().to_vec();
        increment_bytes(&mut end);
        Self::scan_range(txn, prefix.as_bytes(), &end)
    }

    fn scan_range(
        txn: &SurrealTransaction,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let mut iter = txn
            .range(start, end)
            .map_err(|e| StorageError::IoError(format!("surrealkv range: {}", e)))?;

        let mut out = Vec::new();
        let mut has_more = iter
            .seek_first()
            .map_err(|e| StorageError::IoError(format!("surrealkv seek_first: {}", e)))?;

        while has_more && iter.valid() {
            let key = String::from_utf8(iter.key().user_key().to_vec())
                .map_err(|e| StorageError::IoError(format!("surrealkv invalid key utf8: {}", e)))?;
            let value = iter
                .value()
                .map_err(|e| StorageError::IoError(format!("surrealkv iter value: {}", e)))?;
            out.push((key, value));
            has_more = iter
                .next()
                .map_err(|e| StorageError::IoError(format!("surrealkv iter next: {}", e)))?;
        }

        Ok(out)
    }

    fn scan_prefix_keys(
        txn: &SurrealTransaction,
        prefix: &str,
    ) -> Result<Vec<String>, StorageError> {
        Ok(Self::scan_prefix(txn, prefix)?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }

    fn scan_key_range(
        txn: &SurrealTransaction,
        start: &str,
        end: &str,
    ) -> Result<Vec<String>, StorageError> {
        Ok(Self::scan_range(txn, start.as_bytes(), end.as_bytes())?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }
}

impl Drop for SurrealKvStorage {
    fn drop(&mut self) {
        // If we're already inside a Tokio runtime, let SurrealKV's Tree::drop()
        // schedule the async close itself.
        if tokio::runtime::Handle::try_current().is_ok() {
            return;
        }

        // Best-effort close so lock files are released before drop.
        let _ = self.close();
    }
}

fn shared_runtime() -> &'static TokioRuntime {
    static RUNTIME: OnceLock<TokioRuntime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        TokioRuntimeBuilder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build surrealkv shared runtime")
    })
}

impl Storage for SurrealKvStorage {
    // ================================================================
    // Object storage
    // ================================================================

    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        let key = obj_meta_key(id);
        let json = serde_json::to_vec(&metadata)
            .map_err(|e| StorageError::IoError(format!("serialize metadata: {}", e)))?;

        let mut txn = self.begin_write_txn(SurrealDurability::Eventual)?;
        Self::txn_set(&mut txn, &key, &json)?;
        self.commit_txn(&mut txn)
    }

    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        let key = obj_meta_key(id);
        let txn = self.begin_read_txn()?;
        match Self::txn_get(&txn, &key)? {
            Some(data) => {
                let meta: HashMap<String, String> = serde_json::from_slice(&data)
                    .map_err(|e| StorageError::IoError(format!("deserialize metadata: {}", e)))?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        let txn = self.begin_read_txn()?;

        // Check if object exists
        let meta_key = obj_meta_key(object_id);
        if Self::txn_get(&txn, &meta_key)?.is_none() {
            return Ok(None);
        }

        // Load commits via prefix scan
        let commit_prefix = commit_prefix(object_id, branch);
        let commit_entries = Self::scan_prefix(&txn, &commit_prefix)?;

        if commit_entries.is_empty() {
            // Check if tips exist (branch could exist with only tips set)
            let tips_key = branch_tips_key(object_id, branch);
            if Self::txn_get(&txn, &tips_key)?.is_none() {
                return Ok(None);
            }
        }

        let mut commits = Vec::new();
        for (_key, data) in &commit_entries {
            let mut commit: Commit = serde_json::from_slice(data)
                .map_err(|e| StorageError::IoError(format!("deserialize commit: {}", e)))?;

            // Load ack state for this commit
            let ack_key = ack_key(commit.id());
            if let Some(ack_data) = Self::txn_get(&txn, &ack_key)? {
                let tiers: HashSet<PersistenceTier> = serde_json::from_slice(&ack_data)
                    .map_err(|e| StorageError::IoError(format!("deserialize ack: {}", e)))?;
                commit.ack_state.confirmed_tiers = tiers;
            }

            commits.push(commit);
        }

        // Load tips
        let tips_key = branch_tips_key(object_id, branch);
        let tails = match Self::txn_get(&txn, &tips_key)? {
            Some(data) => serde_json::from_slice(&data)
                .map_err(|e| StorageError::IoError(format!("deserialize tips: {}", e)))?,
            None => HashSet::new(),
        };

        Ok(Some(LoadedBranch { commits, tails }))
    }

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
    ) -> Result<(), StorageError> {
        let commit_id = commit.id();
        let mut txn = self.begin_write_txn(SurrealDurability::Eventual)?;

        // Store the commit
        let commit_key = commit_key(object_id, branch, commit_id);
        let commit_json = serde_json::to_vec(&commit)
            .map_err(|e| StorageError::IoError(format!("serialize commit: {}", e)))?;
        Self::txn_set(&mut txn, &commit_key, &commit_json)?;

        // Read-modify-write tips
        let tips_key = branch_tips_key(object_id, branch);
        let mut tips: HashSet<CommitId> = match Self::txn_get(&txn, &tips_key)? {
            Some(data) => serde_json::from_slice(&data)
                .map_err(|e| StorageError::IoError(format!("deserialize tips: {}", e)))?,
            None => HashSet::new(),
        };

        // Remove parents from tips
        for parent in &commit.parents {
            tips.remove(parent);
        }
        // Add this commit as a tip
        tips.insert(commit_id);

        let tips_json = serde_json::to_vec(&tips)
            .map_err(|e| StorageError::IoError(format!("serialize tips: {}", e)))?;
        Self::txn_set(&mut txn, &tips_key, &tips_json)?;

        self.commit_txn(&mut txn)
    }

    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        let mut txn = self.begin_write_txn(SurrealDurability::Eventual)?;

        // Delete the commit
        let commit_key = commit_key(object_id, branch, commit_id);
        Self::txn_delete(&mut txn, &commit_key)?;

        // Remove from tips
        let tips_key = branch_tips_key(object_id, branch);
        if let Some(data) = Self::txn_get(&txn, &tips_key)? {
            let mut tips: HashSet<CommitId> = serde_json::from_slice(&data)
                .map_err(|e| StorageError::IoError(format!("deserialize tips: {}", e)))?;
            tips.remove(&commit_id);
            let tips_json = serde_json::to_vec(&tips)
                .map_err(|e| StorageError::IoError(format!("serialize tips: {}", e)))?;
            Self::txn_set(&mut txn, &tips_key, &tips_json)?;
        }

        self.commit_txn(&mut txn)
    }

    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        let tips_key = branch_tips_key(object_id, branch);
        let mut txn = self.begin_write_txn(SurrealDurability::Eventual)?;
        match tails {
            Some(t) => {
                let json = serde_json::to_vec(&t)
                    .map_err(|e| StorageError::IoError(format!("serialize tails: {}", e)))?;
                Self::txn_set(&mut txn, &tips_key, &json)?;
            }
            None => {
                Self::txn_delete(&mut txn, &tips_key)?;
            }
        }
        self.commit_txn(&mut txn)
    }

    // ================================================================
    // Persistence ack storage
    // ================================================================

    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: PersistenceTier,
    ) -> Result<(), StorageError> {
        let key = ack_key(commit_id);
        let mut txn = self.begin_write_txn(SurrealDurability::Eventual)?;
        let mut tiers: HashSet<PersistenceTier> = match Self::txn_get(&txn, &key)? {
            Some(data) => serde_json::from_slice(&data)
                .map_err(|e| StorageError::IoError(format!("deserialize ack: {}", e)))?,
            None => HashSet::new(),
        };
        tiers.insert(tier);
        let json = serde_json::to_vec(&tiers)
            .map_err(|e| StorageError::IoError(format!("serialize ack: {}", e)))?;
        Self::txn_set(&mut txn, &key, &json)?;
        self.commit_txn(&mut txn)
    }

    // ================================================================
    // Index operations
    // ================================================================

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        tracing::trace!(table, column, branch, ?row_id, "index_insert");
        let key = index_entry_key(table, column, branch, value, row_id);
        let mut txn = self.begin_write_txn(SurrealDurability::Eventual)?;
        // Sentinel byte — existence is the signal.
        Self::txn_set(&mut txn, &key, &[0x01])?;
        self.commit_txn(&mut txn)
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
        let key = index_entry_key(table, column, branch, value, row_id);
        let mut txn = self.begin_write_txn(SurrealDurability::Eventual)?;
        Self::txn_delete(&mut txn, &key)?;
        self.commit_txn(&mut txn)
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        tracing::trace!(table, column, branch, "index_lookup");
        let prefix = index_value_prefix(table, column, branch, value);
        let Ok(txn) = self.begin_read_txn() else {
            return Vec::new();
        };
        match Self::scan_prefix_keys(&txn, &prefix) {
            Ok(keys) => keys
                .iter()
                .filter_map(|k| parse_uuid_from_index_key(k))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        let Some((start_key, end_key)) = index_range_scan_bounds(table, column, branch, start, end)
        else {
            return Vec::new();
        };

        let Ok(txn) = self.begin_read_txn() else {
            return Vec::new();
        };
        match Self::scan_key_range(&txn, &start_key, &end_key) {
            Ok(keys) => keys
                .iter()
                .filter_map(|k| parse_uuid_from_index_key(k))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        let prefix = index_prefix(table, column, branch);
        let Ok(txn) = self.begin_read_txn() else {
            return Vec::new();
        };
        match Self::scan_prefix_keys(&txn, &prefix) {
            Ok(keys) => keys
                .iter()
                .filter_map(|k| parse_uuid_from_index_key(k))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn flush(&self) {
        let _span = tracing::debug_span!("SurrealKvStorage::flush").entered();
        let Ok(mut txn) = self.begin_write_txn(SurrealDurability::Immediate) else {
            return;
        };
        if Self::txn_set(&mut txn, FLUSH_MARKER_KEY, b"1").is_ok() {
            let _ = self.commit_txn(&mut txn);
        }
    }

    fn flush_wal(&self) {
        let _span = tracing::debug_span!("SurrealKvStorage::flush_wal").entered();
        self.flush();
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

    fn test_storage() -> (tempfile::TempDir, SurrealKvStorage) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.surrealkv");
        let storage = SurrealKvStorage::open(&db_path, 8 * 1024 * 1024).unwrap();
        (temp_dir, storage)
    }

    #[test]
    fn surrealkv_object_roundtrip() {
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
    fn surrealkv_commit_roundtrip() {
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
    fn surrealkv_index_ops() {
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
    fn surrealkv_persistence() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("persist.surrealkv");

        let id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        );

        let commit_content = b"persistent data";
        let branch = BranchName::new("main");

        // Phase 1: Write data
        {
            let mut storage = SurrealKvStorage::open(&db_path, 8 * 1024 * 1024).unwrap();
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

        // Phase 2: Reopen and verify
        {
            let storage = SurrealKvStorage::open(&db_path, 8 * 1024 * 1024).unwrap();

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
}
