//! RocksDB-backed Storage implementation.
//!
//! Uses the same key namespace as other persistent backends so object, commit,
//! ack, and index persistence semantics remain aligned.

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::path::Path;

use ::rocksdb::{DB, Direction, IteratorMode, Options};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::Value;
use crate::sync_manager::PersistenceTier;

use super::{LoadedBranch, Storage, StorageError, encode_value};

/// Persistent Storage backed by RocksDB.
pub struct RocksDbStorage {
    db: DB,
}

impl RocksDbStorage {
    /// Open a filesystem-backed RocksDB at the given path.
    pub fn open(path: impl AsRef<Path>, _cache_size_bytes: usize) -> Result<Self, StorageError> {
        let mut options = Options::default();
        options.create_if_missing(true);

        let db = DB::open(&options, path).map_err(map_rocks_error)?;
        Ok(Self { db })
    }

    // ========================================================================
    // Key encoding helpers
    // ========================================================================

    fn obj_meta_key(id: ObjectId) -> Vec<u8> {
        format!("obj:{}:meta", format_uuid(id)).into_bytes()
    }

    fn branch_tips_key(object_id: ObjectId, branch: &BranchName) -> Vec<u8> {
        format!("obj:{}:br:{}:tips", format_uuid(object_id), branch).into_bytes()
    }

    fn commit_key(object_id: ObjectId, branch: &BranchName, commit_id: CommitId) -> Vec<u8> {
        format!(
            "obj:{}:br:{}:c:{}",
            format_uuid(object_id),
            branch,
            hex::encode(commit_id.0)
        )
        .into_bytes()
    }

    /// Prefix for scanning all commits of a branch.
    fn commit_prefix(object_id: ObjectId, branch: &BranchName) -> Vec<u8> {
        format!("obj:{}:br:{}:c:", format_uuid(object_id), branch).into_bytes()
    }

    fn ack_key(commit_id: CommitId) -> Vec<u8> {
        format!("ack:{}", hex::encode(commit_id.0)).into_bytes()
    }

    fn index_entry_key(
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Vec<u8> {
        format!(
            "idx:{}:{}:{}:{}:{}",
            table,
            column,
            branch,
            hex::encode(encode_value(value)),
            format_uuid(row_id)
        )
        .into_bytes()
    }

    /// Prefix for scanning all entries with a specific index value.
    fn index_value_prefix(table: &str, column: &str, branch: &str, value: &Value) -> Vec<u8> {
        format!(
            "idx:{}:{}:{}:{}:",
            table,
            column,
            branch,
            hex::encode(encode_value(value))
        )
        .into_bytes()
    }

    /// Prefix for scanning all entries in an index (table/col/branch).
    fn index_prefix(table: &str, column: &str, branch: &str) -> String {
        format!("idx:{}:{}:{}:", table, column, branch)
    }

    // ========================================================================
    // RocksDB read/write helpers
    // ========================================================================

    fn tree_put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.db.put(key, value).map_err(map_rocks_error)
    }

    fn tree_delete(&self, key: &[u8]) -> Result<(), StorageError> {
        self.db.delete(key).map_err(map_rocks_error)
    }

    fn tree_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.db.get(key).map_err(map_rocks_error)
    }

    fn tree_scan_range(
        &self,
        start_inclusive: Option<&[u8]>,
        end_exclusive: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let mode = match start_inclusive {
            Some(start) => IteratorMode::From(start, Direction::Forward),
            None => IteratorMode::Start,
        };

        let mut out = Vec::new();
        for entry in self.db.iterator(mode) {
            let (key, value) = entry.map_err(map_rocks_error)?;
            if let Some(end) = end_exclusive {
                if key.as_ref() >= end {
                    break;
                }
            }
            out.push((key.to_vec(), value.to_vec()));
        }
        Ok(out)
    }

    fn tree_scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let end = prefix_upper_bound(prefix);
        self.tree_scan_range(Some(prefix), Some(&end))
    }

    fn tree_scan_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StorageError> {
        Ok(self
            .tree_scan_prefix(prefix)?
            .into_iter()
            .map(|(key, _)| key)
            .collect())
    }

    fn tree_scan_key_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<Vec<u8>>, StorageError> {
        Ok(self
            .tree_scan_range(Some(start), Some(end))?
            .into_iter()
            .map(|(key, _)| key)
            .collect())
    }
}

impl Storage for RocksDbStorage {
    // ================================================================
    // Object storage
    // ================================================================

    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        let key = Self::obj_meta_key(id);
        let json = serde_json::to_vec(&metadata)
            .map_err(|e| StorageError::IoError(format!("serialize metadata: {}", e)))?;
        self.tree_put(&key, &json)
    }

    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        let key = Self::obj_meta_key(id);
        match self.tree_get(&key)? {
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
        // Branch existence is tied to object metadata, matching other backends.
        let meta_key = Self::obj_meta_key(object_id);
        if self.tree_get(&meta_key)?.is_none() {
            return Ok(None);
        }

        let commit_prefix = Self::commit_prefix(object_id, branch);
        let commit_entries = self.tree_scan_prefix(&commit_prefix)?;

        if commit_entries.is_empty() {
            let tips_key = Self::branch_tips_key(object_id, branch);
            if self.tree_get(&tips_key)?.is_none() {
                return Ok(None);
            }
        }

        let mut commits = Vec::new();
        for (_key, data) in &commit_entries {
            let mut commit: Commit = serde_json::from_slice(data)
                .map_err(|e| StorageError::IoError(format!("deserialize commit: {}", e)))?;

            let ack_key = Self::ack_key(commit.id());
            if let Some(ack_data) = self.tree_get(&ack_key)? {
                let tiers: HashSet<PersistenceTier> = serde_json::from_slice(&ack_data)
                    .map_err(|e| StorageError::IoError(format!("deserialize ack: {}", e)))?;
                commit.ack_state.confirmed_tiers = tiers;
            }

            commits.push(commit);
        }

        let tips_key = Self::branch_tips_key(object_id, branch);
        let tails = match self.tree_get(&tips_key)? {
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
        let commit_key = Self::commit_key(object_id, branch, commit_id);
        let commit_json = serde_json::to_vec(&commit)
            .map_err(|e| StorageError::IoError(format!("serialize commit: {}", e)))?;
        self.tree_put(&commit_key, &commit_json)?;

        let tips_key = Self::branch_tips_key(object_id, branch);
        let mut tips: HashSet<CommitId> = match self.tree_get(&tips_key)? {
            Some(data) => serde_json::from_slice(&data)
                .map_err(|e| StorageError::IoError(format!("deserialize tips: {}", e)))?,
            None => HashSet::new(),
        };

        for parent in &commit.parents {
            tips.remove(parent);
        }
        tips.insert(commit_id);

        let tips_json = serde_json::to_vec(&tips)
            .map_err(|e| StorageError::IoError(format!("serialize tips: {}", e)))?;
        self.tree_put(&tips_key, &tips_json)?;

        Ok(())
    }

    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        let commit_key = Self::commit_key(object_id, branch, commit_id);
        self.tree_delete(&commit_key)?;

        let tips_key = Self::branch_tips_key(object_id, branch);
        if let Some(data) = self.tree_get(&tips_key)? {
            let mut tips: HashSet<CommitId> = serde_json::from_slice(&data)
                .map_err(|e| StorageError::IoError(format!("deserialize tips: {}", e)))?;
            tips.remove(&commit_id);
            let tips_json = serde_json::to_vec(&tips)
                .map_err(|e| StorageError::IoError(format!("serialize tips: {}", e)))?;
            self.tree_put(&tips_key, &tips_json)?;
        }

        Ok(())
    }

    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        let tips_key = Self::branch_tips_key(object_id, branch);
        match tails {
            Some(t) => {
                let json = serde_json::to_vec(&t)
                    .map_err(|e| StorageError::IoError(format!("serialize tails: {}", e)))?;
                self.tree_put(&tips_key, &json)?;
            }
            None => {
                self.tree_delete(&tips_key)?;
            }
        }
        Ok(())
    }

    // ================================================================
    // Persistence ack storage
    // ================================================================

    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: PersistenceTier,
    ) -> Result<(), StorageError> {
        let key = Self::ack_key(commit_id);
        let mut tiers: HashSet<PersistenceTier> = match self.tree_get(&key)? {
            Some(data) => serde_json::from_slice(&data)
                .map_err(|e| StorageError::IoError(format!("deserialize ack: {}", e)))?,
            None => HashSet::new(),
        };
        tiers.insert(tier);

        let json = serde_json::to_vec(&tiers)
            .map_err(|e| StorageError::IoError(format!("serialize ack: {}", e)))?;
        self.tree_put(&key, &json)
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
        let key = Self::index_entry_key(table, column, branch, value, row_id);
        // Sentinel value - existence of key is the index signal.
        self.tree_put(&key, &[0x01])
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let key = Self::index_entry_key(table, column, branch, value, row_id);
        self.tree_delete(&key)
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        let prefix = Self::index_value_prefix(table, column, branch, value);
        match self.tree_scan_keys(&prefix) {
            Ok(keys) => keys
                .iter()
                .filter_map(|key| parse_uuid_from_index_key(key))
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
        let base_prefix = Self::index_prefix(table, column, branch);

        let start_key = match start {
            Bound::Included(v) => format!("{}{}", base_prefix, hex::encode(encode_value(v))),
            Bound::Excluded(v) => {
                let encoded = hex::encode(encode_value(v));
                let mut key = format!("{}{}:", base_prefix, encoded);
                increment_string(&mut key);
                key
            }
            Bound::Unbounded => base_prefix.clone(),
        };

        let end_key = match end {
            Bound::Included(v) => {
                let encoded = hex::encode(encode_value(v));
                let mut key = format!("{}{}:", base_prefix, encoded);
                increment_string(&mut key);
                key
            }
            Bound::Excluded(v) => format!("{}{}", base_prefix, hex::encode(encode_value(v))),
            Bound::Unbounded => {
                let mut end = base_prefix.clone();
                increment_string(&mut end);
                end
            }
        };

        let start_bytes = start_key.into_bytes();
        let end_bytes = end_key.into_bytes();
        if start_bytes >= end_bytes {
            return Vec::new();
        }

        match self.tree_scan_key_range(&start_bytes, &end_bytes) {
            Ok(keys) => keys
                .iter()
                .filter_map(|key| parse_uuid_from_index_key(key))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        let prefix = Self::index_prefix(table, column, branch).into_bytes();
        match self.tree_scan_keys(&prefix) {
            Ok(keys) => keys
                .iter()
                .filter_map(|key| parse_uuid_from_index_key(key))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn flush(&self) {
        if let Err(error) = self.db.flush() {
            tracing::warn!(?error, "rocksdb flush failed");
        }
    }

    fn flush_wal(&self) {
        if let Err(error) = self.db.flush_wal(true) {
            tracing::warn!(?error, "rocksdb flush_wal failed");
        }
    }
}

fn map_rocks_error(error: ::rocksdb::Error) -> StorageError {
    StorageError::IoError(error.to_string())
}

/// Format an ObjectId as a compact hex string (no dashes).
fn format_uuid(id: ObjectId) -> String {
    hex::encode(id.uuid().as_bytes())
}

/// Parse a UUID from the last segment of an index key.
/// Key format: `idx:{table}:{col}:{branch}:{hex_value}:{uuid_hex}`
fn parse_uuid_from_index_key(key: &[u8]) -> Option<ObjectId> {
    let uuid_hex = key.rsplit(|b| *b == b':').next()?;
    let uuid_hex = std::str::from_utf8(uuid_hex).ok()?;
    let bytes = hex::decode(uuid_hex).ok()?;
    if bytes.len() != 16 {
        return None;
    }
    let uuid = uuid::Uuid::from_bytes(bytes.try_into().ok()?);
    Some(ObjectId::from_uuid(uuid))
}

fn prefix_upper_bound(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    increment_bytes(&mut end);
    end
}

/// Increment the last byte of a byte slice to create an exclusive upper bound.
fn increment_bytes(bytes: &mut Vec<u8>) {
    for i in (0..bytes.len()).rev() {
        if bytes[i] < 0xFF {
            bytes[i] += 1;
            bytes.truncate(i + 1);
            return;
        }
    }
    bytes.push(0x00);
}

/// Increment the last character of a string for exclusive upper bound.
fn increment_string(s: &mut String) {
    let mut bytes = std::mem::take(s).into_bytes();
    increment_bytes(&mut bytes);
    *s = String::from_utf8(bytes).unwrap_or_default();
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;
    use tempfile::TempDir;

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

    fn test_storage() -> (TempDir, RocksDbStorage) {
        let temp_dir = TempDir::new().unwrap();
        let db_dir = temp_dir.path().join("test-rocksdb");
        let storage = RocksDbStorage::open(&db_dir, 4 * 1024 * 1024).unwrap();
        (temp_dir, storage)
    }

    #[test]
    fn rocksdb_object_roundtrip() {
        let (_tmp, mut storage) = test_storage();

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
    }

    #[test]
    fn rocksdb_commit_roundtrip() {
        let (_tmp, mut storage) = test_storage();

        let id = ObjectId::new();
        let branch = BranchName::new("main");
        storage.create_object(id, HashMap::new()).unwrap();

        let commit = make_commit(b"first");
        let commit_id = commit.id();
        storage.append_commit(id, &branch, commit).unwrap();

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert!(loaded.tails.contains(&commit_id));
    }

    #[test]
    fn rocksdb_index_ops() {
        let (_tmp, mut storage) = test_storage();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();

        storage
            .index_insert("users", "age", "main", &Value::Integer(20), row1)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row2)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(30), row3)
            .unwrap();

        let results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(results.len(), 1);
        assert!(results.contains(&row2));

        let range = storage.index_range(
            "users",
            "age",
            "main",
            Bound::Included(&Value::Integer(20)),
            Bound::Excluded(&Value::Integer(30)),
        );
        assert_eq!(range.len(), 2);
        assert!(range.contains(&row1));
        assert!(range.contains(&row2));
    }

    #[test]
    fn rocksdb_ack_tier_roundtrip() {
        let (_tmp, mut storage) = test_storage();
        let commit_id = CommitId([7u8; 32]);

        storage
            .store_ack_tier(commit_id, PersistenceTier::Worker)
            .unwrap();
        storage
            .store_ack_tier(commit_id, PersistenceTier::EdgeServer)
            .unwrap();

        let key = RocksDbStorage::ack_key(commit_id);
        let raw = storage.tree_get(&key).unwrap().unwrap();
        let tiers: HashSet<PersistenceTier> = serde_json::from_slice(&raw).unwrap();
        assert!(tiers.contains(&PersistenceTier::Worker));
        assert!(tiers.contains(&PersistenceTier::EdgeServer));
    }

    #[test]
    fn rocksdb_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let db_dir = temp_dir.path().join("persistent-rocksdb");

        let id = ObjectId::new();
        let branch = BranchName::new("main");
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        );

        {
            let mut storage = RocksDbStorage::open(&db_dir, 4 * 1024 * 1024).unwrap();
            storage.create_object(id, metadata.clone()).unwrap();
            storage
                .append_commit(id, &branch, make_commit(b"persistent"))
                .unwrap();
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
            let storage = RocksDbStorage::open(&db_dir, 4 * 1024 * 1024).unwrap();
            let loaded_meta = storage.load_object_metadata(id).unwrap();
            assert_eq!(loaded_meta, Some(metadata));

            let loaded_branch = storage.load_branch(id, &branch).unwrap().unwrap();
            assert_eq!(loaded_branch.commits.len(), 1);
            assert_eq!(loaded_branch.commits[0].content, b"persistent");

            let results =
                storage.index_lookup("users", "name", "main", &Value::Text("Alice".to_string()));
            assert_eq!(results.len(), 1);
            assert!(results.contains(&id));
        }
    }
}
