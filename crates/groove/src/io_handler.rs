//! Synchronous IoHandler trait and implementations.
//!
//! This is the foundation of the sync storage architecture. All storage
//! and index operations are synchronous - they return immediately with results.
//!
//! # Design: Single-threaded
//!
//! No `Send + Sync` bounds. Each thread (main, worker) has its own IoHandler
//! instance. Cross-thread communication uses the sync protocol over postMessage,
//! not shared mutable state.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::Value;
use crate::storage::{ContentHash, StorageError};
use crate::sync_manager::{OutboxEntry, PersistenceTier};

// ============================================================================
// LoadedBranch - Branch data returned from storage
// ============================================================================

/// Branch data loaded from storage.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadedBranch {
    pub commits: Vec<Commit>,
    pub tails: HashSet<CommitId>,
}

// ============================================================================
// IoHandler Trait
// ============================================================================

/// Synchronous I/O handler for storage, indices, and sync messages.
///
/// All storage and index operations are **synchronous** - they return
/// immediately with results. This eliminates the async response/callback
/// pattern that permeated the old architecture.
///
/// # Single-threaded
///
/// No `Send + Sync` bounds. Each thread has its own IoHandler instance.
/// Cross-thread communication uses the sync protocol, not shared state.
pub trait IoHandler {
    // ================================================================
    // Object storage (sync - returns immediately with result)
    // ================================================================

    /// Create a new object with the given ID and metadata.
    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError>;

    /// Load object metadata. Returns None if object doesn't exist.
    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError>;

    /// Load a branch's commits and tails. Returns None if branch doesn't exist.
    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Result<Option<LoadedBranch>, StorageError>;

    /// Append a commit to a branch.
    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
    ) -> Result<(), StorageError>;

    /// Delete a commit from a branch.
    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError>;

    /// Set or clear the branch truncation tails.
    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError>;

    // ================================================================
    // Blob storage (sync)
    // ================================================================

    /// Store a blob by content hash.
    fn store_blob(&mut self, hash: ContentHash, data: &[u8]) -> Result<(), StorageError>;

    /// Load a blob by content hash. Returns None if not found.
    fn load_blob(&self, hash: ContentHash) -> Result<Option<Vec<u8>>, StorageError>;

    /// Delete a blob by content hash.
    fn delete_blob(&mut self, hash: ContentHash) -> Result<(), StorageError>;

    // ================================================================
    // Persistence ack storage
    // ================================================================

    /// Record that a commit was persisted at the given tier.
    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: PersistenceTier,
    ) -> Result<(), StorageError>;

    // ================================================================
    // Index operations (sync - THE KEY INNOVATION)
    // ================================================================
    //
    // These replace our entire BTreeIndex implementation.
    // MemoryIoHandler uses BTreeMaps. BfTreeIoHandler (Phase 7) uses bf-tree.
    //
    // NOTE: Branch is included in all index methods to support multi-branch
    // scenarios (e.g., user branch vs main branch).
    //
    // NOTE: Methods take `Value` not raw bytes - each implementation handles
    // encoding internally. This keeps encoding concerns inside IoHandler.

    /// Insert an index entry.
    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError>;

    /// Remove an index entry.
    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError>;

    /// Lookup exact value - returns all row IDs with this value.
    fn index_lookup(&self, table: &str, column: &str, branch: &str, value: &Value)
    -> Vec<ObjectId>;

    /// Range scan - returns row IDs matching the range bounds.
    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId>;

    /// Full scan - returns all row IDs in this index.
    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId>;

    // ================================================================
    // Sync messages (already sync in current design)
    // ================================================================

    /// Send a sync message to the network.
    fn send_sync_message(&mut self, message: OutboxEntry);

    // ================================================================
    // Scheduling (may still be needed for subscription batching)
    // ================================================================

    /// Schedule a batched tick. May be a no-op in some implementations.
    fn schedule_batched_tick(&self);
}

// ============================================================================
// MemoryIoHandler - In-memory implementation for testing and main thread
// ============================================================================

/// Index key: (table, column, branch).
type IndexKey = (String, String, String);

/// Index storage: encoded_value -> row_ids. BTreeMap for correct range query ordering.
type IndexEntries = BTreeMap<Vec<u8>, HashSet<ObjectId>>;

/// In-memory IoHandler for testing and main-thread use.
///
/// Stores objects, blobs, and indices in HashMaps/BTreeMaps. No persistence.
/// This is sufficient for:
/// - All groove unit tests
/// - All groove integration tests
/// - Main thread in browser (acts as cache of worker state)
#[derive(Default)]
pub struct MemoryIoHandler {
    /// Object storage: object_id -> ObjectData
    objects: HashMap<ObjectId, ObjectData>,

    /// Blob storage: content_hash -> data
    blobs: HashMap<ContentHash, Vec<u8>>,

    /// Index storage: key -> (encoded_value -> row_ids)
    indices: HashMap<IndexKey, IndexEntries>,

    /// Sync message outbox (taken by caller)
    outbox: Vec<OutboxEntry>,

    /// Persistence ack tiers per commit.
    ack_tiers: HashMap<CommitId, HashSet<PersistenceTier>>,
}

/// Internal object storage structure.
#[derive(Debug, Clone, Default)]
struct ObjectData {
    metadata: HashMap<String, String>,
    branches: HashMap<BranchName, BranchData>,
}

/// Internal branch storage structure.
#[derive(Debug, Clone, Default)]
struct BranchData {
    commits: Vec<Commit>,
    tails: HashSet<CommitId>,
}

impl MemoryIoHandler {
    /// Create a new empty MemoryIoHandler.
    pub fn new() -> Self {
        Self::default()
    }

    /// Take all pending outbox messages.
    pub fn take_outbox(&mut self) -> Vec<OutboxEntry> {
        std::mem::take(&mut self.outbox)
    }

    /// Check if there are pending outbox messages.
    pub fn has_outbox_messages(&self) -> bool {
        !self.outbox.is_empty()
    }
}

// ============================================================================
// Value Encoding for Index Keys
// ============================================================================
//
// Values must be encoded so lexicographic byte ordering equals semantic ordering.
// This enables range queries via BTreeMap::range().

/// Encode a Value into bytes that sort correctly for range queries.
fn encode_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![0x00], // Null sorts first

        Value::Boolean(b) => {
            // false (0x01) < true (0x02)
            vec![0x01, if *b { 0x02 } else { 0x01 }]
        }

        Value::Integer(n) => {
            // Flip sign bit so negative < positive, big-endian for correct ordering
            let mut bytes = vec![0x02];
            bytes.extend_from_slice(&((*n as i64) ^ i64::MIN).to_be_bytes());
            bytes
        }

        Value::BigInt(n) => {
            // Flip sign bit so negative < positive, big-endian for correct ordering
            let mut bytes = vec![0x03];
            bytes.extend_from_slice(&(*n ^ i64::MIN).to_be_bytes());
            bytes
        }

        Value::Timestamp(ts) => {
            // Unsigned, big-endian (already sorts correctly)
            let mut bytes = vec![0x04];
            bytes.extend_from_slice(&ts.to_be_bytes());
            bytes
        }

        Value::Text(s) => {
            // UTF-8 bytes sort correctly for ASCII; good enough for now
            let mut bytes = vec![0x05];
            bytes.extend_from_slice(s.as_bytes());
            bytes
        }

        Value::Uuid(id) => {
            // UUID bytes (UUIDv7 is time-ordered)
            let mut bytes = vec![0x06];
            bytes.extend_from_slice(id.uuid().as_bytes());
            bytes
        }

        Value::Array(_) => {
            // Arrays not typically indexed; use hash for equality only
            let mut bytes = vec![0x07];
            // Simple approach: serialize and hash. Not order-preserving.
            let json = serde_json::to_string(value).unwrap_or_default();
            bytes.extend_from_slice(json.as_bytes());
            bytes
        }

        Value::Row(_) => {
            // Rows not typically indexed; use hash for equality only
            let mut bytes = vec![0x08];
            let json = serde_json::to_string(value).unwrap_or_default();
            bytes.extend_from_slice(json.as_bytes());
            bytes
        }
    }
}

impl IoHandler for MemoryIoHandler {
    // ================================================================
    // Object storage
    // ================================================================

    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        self.objects.insert(
            id,
            ObjectData {
                metadata,
                branches: HashMap::new(),
            },
        );
        Ok(())
    }

    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        Ok(self.objects.get(&id).map(|obj| obj.metadata.clone()))
    }

    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        let Some(obj) = self.objects.get(&object_id) else {
            return Ok(None);
        };
        let Some(branch_data) = obj.branches.get(branch) else {
            return Ok(None);
        };
        let mut commits = branch_data.commits.clone();
        for commit in &mut commits {
            if let Some(tiers) = self.ack_tiers.get(&commit.id()) {
                commit.ack_state.confirmed_tiers = tiers.clone();
            }
        }
        Ok(Some(LoadedBranch {
            commits,
            tails: branch_data.tails.clone(),
        }))
    }

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
    ) -> Result<(), StorageError> {
        let obj = self.objects.entry(object_id).or_default();
        let branch_data = obj.branches.entry(*branch).or_default();

        let commit_id = commit.id();

        // Remove parents from tips
        for parent in &commit.parents {
            branch_data.tails.remove(parent);
        }

        // Add this commit as a tip
        branch_data.tails.insert(commit_id);
        branch_data.commits.push(commit);

        Ok(())
    }

    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        if let Some(branch_data) = self
            .objects
            .get_mut(&object_id)
            .and_then(|obj| obj.branches.get_mut(branch))
        {
            branch_data.commits.retain(|c| c.id() != commit_id);
            branch_data.tails.remove(&commit_id);
        }
        Ok(())
    }

    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        if let Some(branch_data) = self
            .objects
            .get_mut(&object_id)
            .and_then(|obj| obj.branches.get_mut(branch))
        {
            branch_data.tails = tails.unwrap_or_default();
        }
        Ok(())
    }

    // ================================================================
    // Blob storage
    // ================================================================

    fn store_blob(&mut self, hash: ContentHash, data: &[u8]) -> Result<(), StorageError> {
        self.blobs.insert(hash, data.to_vec());
        Ok(())
    }

    fn load_blob(&self, hash: ContentHash) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.blobs.get(&hash).cloned())
    }

    fn delete_blob(&mut self, hash: ContentHash) -> Result<(), StorageError> {
        self.blobs.remove(&hash);
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
        self.ack_tiers.entry(commit_id).or_default().insert(tier);
        Ok(())
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
        let key = (table.to_string(), column.to_string(), branch.to_string());
        let index = self.indices.entry(key).or_default();
        let encoded = encode_value(value);
        index.entry(encoded).or_default().insert(row_id);
        Ok(())
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let key = (table.to_string(), column.to_string(), branch.to_string());
        if let Some(index) = self.indices.get_mut(&key) {
            let encoded = encode_value(value);
            if let Some(row_ids) = index.get_mut(&encoded) {
                row_ids.remove(&row_id);
                if row_ids.is_empty() {
                    index.remove(&encoded);
                }
            }
        }
        Ok(())
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        let key = (table.to_string(), column.to_string(), branch.to_string());
        let Some(index) = self.indices.get(&key) else {
            return Vec::new();
        };
        let encoded = encode_value(value);
        index
            .get(&encoded)
            .map(|ids| ids.iter().copied().collect())
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
        let key = (table.to_string(), column.to_string(), branch.to_string());
        let Some(index) = self.indices.get(&key) else {
            return Vec::new();
        };

        let start_bound = match start {
            Bound::Included(v) => Bound::Included(encode_value(v)),
            Bound::Excluded(v) => Bound::Excluded(encode_value(v)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_bound = match end {
            Bound::Included(v) => Bound::Included(encode_value(v)),
            Bound::Excluded(v) => Bound::Excluded(encode_value(v)),
            Bound::Unbounded => Bound::Unbounded,
        };

        index
            .range((start_bound, end_bound))
            .flat_map(|(_, ids)| ids.iter().copied())
            .collect()
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        let key = (table.to_string(), column.to_string(), branch.to_string());
        let Some(index) = self.indices.get(&key) else {
            return Vec::new();
        };
        index.values().flat_map(|ids| ids.iter().copied()).collect()
    }

    // ================================================================
    // Sync messages
    // ================================================================

    fn send_sync_message(&mut self, message: OutboxEntry) {
        self.outbox.push(message);
    }

    // ================================================================
    // Scheduling
    // ================================================================

    fn schedule_batched_tick(&self) {
        // No-op for memory handler - tests call tick explicitly
    }
}

// ============================================================================
// Tests
// ============================================================================

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

    #[test]
    fn memory_io_handler_object_lifecycle() {
        let mut io = MemoryIoHandler::new();

        let id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert("table".to_string(), "users".to_string());

        // Create object
        io.create_object(id, metadata.clone()).unwrap();

        // Load metadata
        let loaded = io.load_object_metadata(id).unwrap();
        assert_eq!(loaded, Some(metadata));

        // Non-existent object
        let other_id = ObjectId::new();
        assert_eq!(io.load_object_metadata(other_id).unwrap(), None);
    }

    #[test]
    fn memory_io_handler_branch_and_commits() {
        let mut io = MemoryIoHandler::new();

        let id = ObjectId::new();
        let branch = BranchName::new("main");

        io.create_object(id, HashMap::new()).unwrap();

        // Initially no branch
        assert_eq!(io.load_branch(id, &branch).unwrap(), None);

        // Append commit creates branch
        let commit = make_commit(b"first");
        let commit_id = commit.id();
        io.append_commit(id, &branch, commit).unwrap();

        let loaded = io.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert!(loaded.tails.contains(&commit_id));

        // Delete commit
        io.delete_commit(id, &branch, commit_id).unwrap();
        let loaded = io.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 0);
    }

    #[test]
    fn memory_io_handler_blob_storage() {
        let mut io = MemoryIoHandler::new();

        let hash = ContentHash([42u8; 32]);
        let data = b"hello world";

        // Store
        io.store_blob(hash, data).unwrap();

        // Load
        let loaded = io.load_blob(hash).unwrap();
        assert_eq!(loaded, Some(data.to_vec()));

        // Delete
        io.delete_blob(hash).unwrap();
        assert_eq!(io.load_blob(hash).unwrap(), None);
    }

    #[test]
    fn memory_io_handler_index_insert_lookup() {
        let mut io = MemoryIoHandler::new();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        // Insert two rows with same value
        io.index_insert("users", "age", "main", &Value::Integer(25), row1)
            .unwrap();
        io.index_insert("users", "age", "main", &Value::Integer(25), row2)
            .unwrap();

        // Lookup should return both
        let results = io.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row1));
        assert!(results.contains(&row2));

        // Different value returns empty
        let results = io.index_lookup("users", "age", "main", &Value::Integer(30));
        assert!(results.is_empty());
    }

    #[test]
    fn memory_io_handler_index_remove() {
        let mut io = MemoryIoHandler::new();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        io.index_insert("users", "age", "main", &Value::Integer(25), row1)
            .unwrap();
        io.index_insert("users", "age", "main", &Value::Integer(25), row2)
            .unwrap();

        // Remove one
        io.index_remove("users", "age", "main", &Value::Integer(25), row1)
            .unwrap();

        let results = io.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(results.len(), 1);
        assert!(results.contains(&row2));
    }

    #[test]
    fn memory_io_handler_index_range() {
        let mut io = MemoryIoHandler::new();

        let row20 = ObjectId::new();
        let row25 = ObjectId::new();
        let row30 = ObjectId::new();
        let row35 = ObjectId::new();

        io.index_insert("users", "age", "main", &Value::Integer(20), row20)
            .unwrap();
        io.index_insert("users", "age", "main", &Value::Integer(25), row25)
            .unwrap();
        io.index_insert("users", "age", "main", &Value::Integer(30), row30)
            .unwrap();
        io.index_insert("users", "age", "main", &Value::Integer(35), row35)
            .unwrap();

        // Range [25, 35) should return 25 and 30
        let results = io.index_range(
            "users",
            "age",
            "main",
            Bound::Included(&Value::Integer(25)),
            Bound::Excluded(&Value::Integer(35)),
        );
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row25));
        assert!(results.contains(&row30));

        // Unbounded start, exclusive end
        let results = io.index_range(
            "users",
            "age",
            "main",
            Bound::Unbounded,
            Bound::Excluded(&Value::Integer(26)),
        );
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row20));
        assert!(results.contains(&row25));

        // Inclusive start, unbounded end
        let results = io.index_range(
            "users",
            "age",
            "main",
            Bound::Included(&Value::Integer(30)),
            Bound::Unbounded,
        );
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row30));
        assert!(results.contains(&row35));
    }

    #[test]
    fn memory_io_handler_index_scan_all() {
        let mut io = MemoryIoHandler::new();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();

        io.index_insert("users", "age", "main", &Value::Integer(20), row1)
            .unwrap();
        io.index_insert("users", "age", "main", &Value::Integer(25), row2)
            .unwrap();
        io.index_insert("users", "age", "main", &Value::Integer(30), row3)
            .unwrap();

        let results = io.index_scan_all("users", "age", "main");
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn memory_io_handler_index_branch_isolation() {
        let mut io = MemoryIoHandler::new();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        io.index_insert("users", "age", "main", &Value::Integer(25), row1)
            .unwrap();
        io.index_insert("users", "age", "feature", &Value::Integer(25), row2)
            .unwrap();

        // Each branch sees only its own data
        let main_results = io.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(main_results.len(), 1);
        assert!(main_results.contains(&row1));

        let feature_results = io.index_lookup("users", "age", "feature", &Value::Integer(25));
        assert_eq!(feature_results.len(), 1);
        assert!(feature_results.contains(&row2));
    }

    #[test]
    fn memory_io_handler_outbox() {
        use crate::sync_manager::{Destination, ServerId, SyncPayload};

        let mut io = MemoryIoHandler::new();

        assert!(!io.has_outbox_messages());

        io.send_sync_message(OutboxEntry {
            destination: Destination::Server(ServerId::new()),
            payload: SyncPayload::ObjectUpdated {
                object_id: ObjectId::new(),
                metadata: None,
                branch_name: BranchName::new("main"),
                commits: vec![],
            },
        });

        assert!(io.has_outbox_messages());

        let messages = io.take_outbox();
        assert_eq!(messages.len(), 1);
        assert!(!io.has_outbox_messages());
    }

    #[test]
    fn encode_value_ordering() {
        // Null < Boolean < Integer < BigInt < Timestamp < Text < Uuid

        let null = encode_value(&Value::Null);
        let bool_false = encode_value(&Value::Boolean(false));
        let bool_true = encode_value(&Value::Boolean(true));
        let int_neg = encode_value(&Value::Integer(-100));
        let int_zero = encode_value(&Value::Integer(0));
        let int_pos = encode_value(&Value::Integer(100));

        assert!(null < bool_false);
        assert!(bool_false < bool_true);
        assert!(bool_true < int_neg);
        assert!(int_neg < int_zero);
        assert!(int_zero < int_pos);
    }
}
