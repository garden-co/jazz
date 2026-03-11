//! Synchronous Storage trait and implementations.
//!
//! This is the foundation of the sync storage architecture. All storage
//! and index operations are synchronous - they return immediately with results.
//!
//! # Design: Single-threaded
//!
//! No `Send + Sync` bounds on Storage. Each thread (main, worker) has its own
//! Storage instance. Cross-thread communication uses the sync protocol over
//! postMessage, not shared mutable state.

mod key_codec;
mod opfs_btree;
mod storage_core;
pub use opfs_btree::OpfsBTreeStorage;
#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
mod fjall;
#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
pub use fjall::FjallStorage;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;

use serde::{Deserialize, Serialize};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::{SchemaHash, Value};
use crate::sync_manager::DurabilityTier;

// ============================================================================
// Storage Types
// ============================================================================

/// Errors from storage operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageError {
    NotFound,
    IoError(String),
}

// ============================================================================
// LoadedBranch - Branch data returned from storage
// ============================================================================

/// Branch data loaded from storage.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadedBranch {
    pub commits: Vec<Commit>,
    pub tails: HashSet<CommitId>,
}

/// Lens edge stored in the catalogue manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogueLensSeen {
    pub source_hash: SchemaHash,
    pub target_hash: SchemaHash,
}

/// Append-only catalogue manifest operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CatalogueManifestOp {
    SchemaSeen {
        object_id: ObjectId,
        schema_hash: SchemaHash,
    },
    LensSeen {
        object_id: ObjectId,
        source_hash: SchemaHash,
        target_hash: SchemaHash,
    },
}

impl CatalogueManifestOp {
    pub fn object_id(&self) -> ObjectId {
        match self {
            Self::SchemaSeen { object_id, .. } | Self::LensSeen { object_id, .. } => *object_id,
        }
    }
}

/// Materialized view of catalogue objects known for an app.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogueManifest {
    pub schema_seen: HashMap<ObjectId, SchemaHash>,
    pub lens_seen: HashMap<ObjectId, CatalogueLensSeen>,
}

impl CatalogueManifest {
    pub fn apply(&mut self, op: &CatalogueManifestOp) {
        match op {
            CatalogueManifestOp::SchemaSeen {
                object_id,
                schema_hash,
            } => {
                self.schema_seen.insert(*object_id, *schema_hash);
            }
            CatalogueManifestOp::LensSeen {
                object_id,
                source_hash,
                target_hash,
            } => {
                self.lens_seen.insert(
                    *object_id,
                    CatalogueLensSeen {
                        source_hash: *source_hash,
                        target_hash: *target_hash,
                    },
                );
            }
        }
    }
}

// ============================================================================
// Storage Trait
// ============================================================================

/// Synchronous storage for objects and indices.
///
/// All operations are **synchronous** - they return immediately with results.
/// This eliminates the async response/callback pattern that permeated the
/// old architecture.
///
/// # Single-threaded
///
/// No `Send + Sync` bounds. Each thread has its own Storage instance.
/// Cross-thread communication uses the sync protocol, not shared state.
pub trait Storage {
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
    // Persistence ack storage
    // ================================================================

    /// Record that a commit was persisted at the given tier.
    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: DurabilityTier,
    ) -> Result<(), StorageError>;

    // ================================================================
    // Catalogue manifest storage
    // ================================================================

    /// Append one catalogue manifest operation for an app.
    ///
    /// Implementations must be idempotent by operation `object_id`.
    fn append_catalogue_manifest_op(
        &mut self,
        app_id: ObjectId,
        op: CatalogueManifestOp,
    ) -> Result<(), StorageError>;

    /// Append multiple catalogue manifest operations for an app.
    fn append_catalogue_manifest_ops(
        &mut self,
        app_id: ObjectId,
        ops: &[CatalogueManifestOp],
    ) -> Result<(), StorageError>;

    /// Load the materialized catalogue manifest for an app.
    fn load_catalogue_manifest(
        &self,
        app_id: ObjectId,
    ) -> Result<Option<CatalogueManifest>, StorageError>;

    // ================================================================
    // Index operations (sync)
    // ================================================================
    //
    // These replace our entire BTreeIndex implementation.
    // MemoryStorage uses BTreeMaps. OpfsBTreeStorage uses opfs-btree.
    //
    // NOTE: Branch is included in all index methods to support multi-branch
    // scenarios (e.g., user branch vs main branch).
    //
    // NOTE: Methods take `Value` not raw bytes - each implementation handles
    // encoding internally. This keeps encoding concerns inside Storage.

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

    /// Flush buffered data to persistent storage. No-op for in-memory storage.
    fn flush(&self) {}

    /// Flush only the WAL buffer (not the snapshot). No-op for storage without WAL.
    fn flush_wal(&self) {}

    /// Close and release storage resources (e.g. file locks). No-op by default.
    fn close(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

// Box<Storage> is used to allow for dynamic dispatch of the Storage trait.
impl<T: Storage + ?Sized> Storage for Box<T> {
    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        (**self).create_object(id, metadata)
    }

    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        (**self).load_object_metadata(id)
    }

    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        (**self).load_branch(object_id, branch)
    }

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
    ) -> Result<(), StorageError> {
        (**self).append_commit(object_id, branch, commit)
    }

    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        (**self).delete_commit(object_id, branch, commit_id)
    }

    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        (**self).set_branch_tails(object_id, branch, tails)
    }

    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: DurabilityTier,
    ) -> Result<(), StorageError> {
        (**self).store_ack_tier(commit_id, tier)
    }

    fn append_catalogue_manifest_op(
        &mut self,
        app_id: ObjectId,
        op: CatalogueManifestOp,
    ) -> Result<(), StorageError> {
        (**self).append_catalogue_manifest_op(app_id, op)
    }

    fn append_catalogue_manifest_ops(
        &mut self,
        app_id: ObjectId,
        ops: &[CatalogueManifestOp],
    ) -> Result<(), StorageError> {
        (**self).append_catalogue_manifest_ops(app_id, ops)
    }

    fn load_catalogue_manifest(
        &self,
        app_id: ObjectId,
    ) -> Result<Option<CatalogueManifest>, StorageError> {
        (**self).load_catalogue_manifest(app_id)
    }

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        (**self).index_insert(table, column, branch, value, row_id)
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        (**self).index_remove(table, column, branch, value, row_id)
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        (**self).index_lookup(table, column, branch, value)
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        (**self).index_range(table, column, branch, start, end)
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        (**self).index_scan_all(table, column, branch)
    }

    fn flush(&self) {
        (**self).flush();
    }

    fn flush_wal(&self) {
        (**self).flush_wal();
    }

    fn close(&self) -> Result<(), StorageError> {
        (**self).close()
    }
}

// ============================================================================
// MemoryStorage - In-memory implementation for testing and main thread
// ============================================================================

/// Index key: (table, column, branch).
type IndexKey = (String, String, String);

/// Index storage: encoded_value -> row_ids. BTreeMap for correct range query ordering.
type IndexEntries = BTreeMap<Vec<u8>, HashSet<ObjectId>>;

/// In-memory Storage for testing and main-thread use.
///
/// Stores objects and indices in HashMaps/BTreeMaps. No persistence.
/// This is sufficient for:
/// - All jazz unit tests
/// - All jazz integration tests
/// - Main thread in browser (acts as cache of worker state)
#[derive(Default)]
pub struct MemoryStorage {
    /// Object storage: object_id -> ObjectData
    objects: HashMap<ObjectId, ObjectData>,

    /// Index storage: key -> (encoded_value -> row_ids)
    indices: HashMap<IndexKey, IndexEntries>,

    /// Persistence ack tiers per commit.
    ack_tiers: HashMap<CommitId, HashSet<DurabilityTier>>,
    /// Append-only manifest ops keyed by app_id then op object_id.
    catalogue_manifest_ops: HashMap<ObjectId, HashMap<ObjectId, CatalogueManifestOp>>,
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

impl MemoryStorage {
    /// Create a new empty MemoryStorage.
    pub fn new() -> Self {
        Self::default()
    }
}

// ============================================================================
// Value Encoding for Index Keys
// ============================================================================
//
// Values must be encoded so lexicographic byte ordering equals semantic ordering.
// This enables range queries via BTreeMap::range().

/// Returns true if the value is Double(0.0) or Double(-0.0).
///
/// IEEE 754 defines -0.0 == 0.0, but they have distinct bit patterns and
/// therefore distinct index encodings. Query operations must check both.
pub(crate) fn is_double_zero(value: &Value) -> bool {
    matches!(value, Value::Double(f) if *f == 0.0)
}

/// Encode a Value into bytes that sort correctly for range queries.
pub(crate) fn encode_value(value: &Value) -> Vec<u8> {
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

        Value::Double(f) => {
            let mut bytes = vec![0x09];
            let bits = f.to_bits();
            // Flip for lexicographic ordering: if sign bit set, flip all bits;
            // otherwise flip only the sign bit.
            let ordered = if bits & (1u64 << 63) != 0 {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            bytes.extend_from_slice(&ordered.to_be_bytes());
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

        Value::Bytea(bytes_value) => {
            // Raw bytes for exact-match index semantics.
            let mut bytes = vec![0x09];
            bytes.extend_from_slice(bytes_value);
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

        Value::Row { .. } => {
            // Rows not typically indexed; use hash for equality only
            let mut bytes = vec![0x08];
            let json = serde_json::to_string(value).unwrap_or_default();
            bytes.extend_from_slice(json.as_bytes());
            bytes
        }
    }
}

impl Storage for MemoryStorage {
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
    // Persistence ack storage
    // ================================================================

    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: DurabilityTier,
    ) -> Result<(), StorageError> {
        self.ack_tiers.entry(commit_id).or_default().insert(tier);
        Ok(())
    }

    fn append_catalogue_manifest_op(
        &mut self,
        app_id: ObjectId,
        op: CatalogueManifestOp,
    ) -> Result<(), StorageError> {
        let object_id = op.object_id();
        let app_ops = self.catalogue_manifest_ops.entry(app_id).or_default();

        match app_ops.get(&object_id) {
            Some(existing) if existing == &op => Ok(()),
            Some(existing) => Err(StorageError::IoError(format!(
                "conflicting catalogue manifest op for object {object_id}: existing={existing:?} new={op:?}"
            ))),
            None => {
                app_ops.insert(object_id, op);
                Ok(())
            }
        }
    }

    fn append_catalogue_manifest_ops(
        &mut self,
        app_id: ObjectId,
        ops: &[CatalogueManifestOp],
    ) -> Result<(), StorageError> {
        for op in ops {
            self.append_catalogue_manifest_op(app_id, op.clone())?;
        }
        Ok(())
    }

    fn load_catalogue_manifest(
        &self,
        app_id: ObjectId,
    ) -> Result<Option<CatalogueManifest>, StorageError> {
        let Some(ops) = self.catalogue_manifest_ops.get(&app_id) else {
            return Ok(None);
        };

        if ops.is_empty() {
            return Ok(None);
        }

        let mut manifest = CatalogueManifest::default();
        for op in ops.values() {
            manifest.apply(op);
        }
        Ok(Some(manifest))
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

        // IEEE 754: -0.0 == 0.0, so look up both encodings and merge.
        if is_double_zero(value) {
            let mut result = HashSet::new();
            for zero in &[Value::Double(0.0), Value::Double(-0.0)] {
                if let Some(ids) = index.get(&encode_value(zero)) {
                    result.extend(ids.iter().copied());
                }
            }
            return result.into_iter().collect();
        }

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

        // IEEE 754: -0.0 == 0.0 but they have distinct encodings where
        // encoded(-0.0) < encoded(+0.0). Adjust bounds so that both zeros
        // are treated as the same point:
        //   Start Included(zero) → use -0.0 (widen to include the lesser encoding)
        //   Start Excluded(zero) → use +0.0 (skip past both encodings)
        //   End Included(zero)   → use +0.0 (widen to include the greater encoding)
        //   End Excluded(zero)   → use -0.0 (stop before both encodings)
        let start_bound = match start {
            Bound::Included(v) if is_double_zero(v) => {
                Bound::Included(encode_value(&Value::Double(-0.0)))
            }
            Bound::Excluded(v) if is_double_zero(v) => {
                Bound::Excluded(encode_value(&Value::Double(0.0)))
            }
            Bound::Included(v) => Bound::Included(encode_value(v)),
            Bound::Excluded(v) => Bound::Excluded(encode_value(v)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_bound = match end {
            Bound::Included(v) if is_double_zero(v) => {
                Bound::Included(encode_value(&Value::Double(0.0)))
            }
            Bound::Excluded(v) if is_double_zero(v) => {
                Bound::Excluded(encode_value(&Value::Double(-0.0)))
            }
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
    fn memory_storage_object_lifecycle() {
        let mut storage = MemoryStorage::new();

        let id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        );

        // Create object
        storage.create_object(id, metadata.clone()).unwrap();

        // Load metadata
        let loaded = storage.load_object_metadata(id).unwrap();
        assert_eq!(loaded, Some(metadata));

        // Non-existent object
        let other_id = ObjectId::new();
        assert_eq!(storage.load_object_metadata(other_id).unwrap(), None);
    }

    #[test]
    fn memory_storage_branch_and_commits() {
        let mut storage = MemoryStorage::new();

        let id = ObjectId::new();
        let branch = BranchName::new("main");

        storage.create_object(id, HashMap::new()).unwrap();

        // Initially no branch
        assert_eq!(storage.load_branch(id, &branch).unwrap(), None);

        // Append commit creates branch
        let commit = make_commit(b"first");
        let commit_id = commit.id();
        storage.append_commit(id, &branch, commit).unwrap();

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert!(loaded.tails.contains(&commit_id));

        // Delete commit
        storage.delete_commit(id, &branch, commit_id).unwrap();
        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 0);
    }

    #[test]
    fn memory_storage_index_insert_lookup() {
        let mut storage = MemoryStorage::new();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        // Insert two rows with same value
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row1)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row2)
            .unwrap();

        // Lookup should return both
        let results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row1));
        assert!(results.contains(&row2));

        // Different value returns empty
        let results = storage.index_lookup("users", "age", "main", &Value::Integer(30));
        assert!(results.is_empty());
    }

    #[test]
    fn memory_storage_index_remove() {
        let mut storage = MemoryStorage::new();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row1)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row2)
            .unwrap();

        // Remove one
        storage
            .index_remove("users", "age", "main", &Value::Integer(25), row1)
            .unwrap();

        let results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(results.len(), 1);
        assert!(results.contains(&row2));
    }

    #[test]
    fn memory_storage_index_range() {
        let mut storage = MemoryStorage::new();

        let row20 = ObjectId::new();
        let row25 = ObjectId::new();
        let row30 = ObjectId::new();
        let row35 = ObjectId::new();

        storage
            .index_insert("users", "age", "main", &Value::Integer(20), row20)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row25)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(30), row30)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(35), row35)
            .unwrap();

        // Range [25, 35) should return 25 and 30
        let results = storage.index_range(
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
        let results = storage.index_range(
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
        let results = storage.index_range(
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
    fn memory_storage_index_scan_all() {
        let mut storage = MemoryStorage::new();

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

        let results = storage.index_scan_all("users", "age", "main");
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn memory_storage_index_branch_isolation() {
        let mut storage = MemoryStorage::new();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row1)
            .unwrap();
        storage
            .index_insert("users", "age", "feature", &Value::Integer(25), row2)
            .unwrap();

        // Each branch sees only its own data
        let main_results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(main_results.len(), 1);
        assert!(main_results.contains(&row1));

        let feature_results = storage.index_lookup("users", "age", "feature", &Value::Integer(25));
        assert_eq!(feature_results.len(), 1);
        assert!(feature_results.contains(&row2));
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

    #[test]
    fn memory_storage_catalogue_manifest_roundtrip() {
        let mut storage = MemoryStorage::new();
        let app_id = ObjectId::new();
        let schema_object_id = ObjectId::new();
        let lens_object_id = ObjectId::new();
        let schema_hash = SchemaHash::from_bytes([0x11; 32]);
        let source_hash = SchemaHash::from_bytes([0x22; 32]);
        let target_hash = SchemaHash::from_bytes([0x33; 32]);

        storage
            .append_catalogue_manifest_op(
                app_id,
                CatalogueManifestOp::SchemaSeen {
                    object_id: schema_object_id,
                    schema_hash,
                },
            )
            .unwrap();
        storage
            .append_catalogue_manifest_op(
                app_id,
                CatalogueManifestOp::LensSeen {
                    object_id: lens_object_id,
                    source_hash,
                    target_hash,
                },
            )
            .unwrap();

        // Idempotent append for the same object/op.
        storage
            .append_catalogue_manifest_op(
                app_id,
                CatalogueManifestOp::SchemaSeen {
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
            Some(&CatalogueLensSeen {
                source_hash,
                target_hash,
            })
        );
    }

    #[test]
    fn real_encode_value_ordering() {
        let neg_inf = encode_value(&Value::Double(f64::NEG_INFINITY));
        let neg_big = encode_value(&Value::Double(-1000.0));
        let neg_small = encode_value(&Value::Double(-0.001));
        let neg_zero = encode_value(&Value::Double(-0.0));
        let pos_zero = encode_value(&Value::Double(0.0));
        let pos_small = encode_value(&Value::Double(0.001));
        let pos_big = encode_value(&Value::Double(1000.0));
        let pos_inf = encode_value(&Value::Double(f64::INFINITY));

        assert!(neg_inf < neg_big);
        assert!(neg_big < neg_small);
        assert!(neg_small < neg_zero);
        assert!(neg_zero < pos_zero);
        assert!(pos_zero < pos_small);
        assert!(pos_small < pos_big);
        assert!(pos_big < pos_inf);
    }

    #[test]
    fn real_cross_type_ordering() {
        // Double should sort after all existing types (tag 0x09 > 0x08)
        let row = encode_value(&Value::Row {
            id: None,
            values: vec![],
        });
        let double = encode_value(&Value::Double(0.0));

        assert!(row < double);
    }

    // ----------------------------------------------------------------
    // Negative zero IEEE 754 semantics: -0.0 and 0.0 are equal per the
    // standard, so index lookups and range queries must treat them as
    // the same value even though they have distinct bit patterns.
    // ----------------------------------------------------------------

    #[test]
    fn real_negative_zero_exact_lookup() {
        // Store a value as -0.0, look it up with 0.0 (and vice versa).
        let mut storage = MemoryStorage::new();

        let row_neg = ObjectId::new();
        let row_pos = ObjectId::new();

        storage
            .index_insert("prices", "amount", "main", &Value::Double(-0.0), row_neg)
            .unwrap();
        storage
            .index_insert("prices", "amount", "main", &Value::Double(0.0), row_pos)
            .unwrap();

        // Looking up 0.0 should find both (IEEE 754: -0.0 == 0.0)
        let results = storage.index_lookup("prices", "amount", "main", &Value::Double(0.0));
        assert_eq!(results.len(), 2, "lookup 0.0 should match both zeros");
        assert!(results.contains(&row_neg));
        assert!(results.contains(&row_pos));

        // Looking up -0.0 should also find both
        let results = storage.index_lookup("prices", "amount", "main", &Value::Double(-0.0));
        assert_eq!(results.len(), 2, "lookup -0.0 should match both zeros");
        assert!(results.contains(&row_neg));
        assert!(results.contains(&row_pos));
    }

    #[test]
    fn real_negative_zero_range_gte() {
        // WHERE amount >= 0.0 should include -0.0 (equal per IEEE 754)
        let mut storage = MemoryStorage::new();

        let row_neg_zero = ObjectId::new();
        let row_pos_zero = ObjectId::new();
        let row_negative = ObjectId::new();

        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(-0.0),
                row_neg_zero,
            )
            .unwrap();
        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(0.0),
                row_pos_zero,
            )
            .unwrap();
        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(-1.0),
                row_negative,
            )
            .unwrap();

        // >= 0.0 should include -0.0 and 0.0, but not -1.0
        let results = storage.index_range(
            "prices",
            "amount",
            "main",
            Bound::Included(&Value::Double(0.0)),
            Bound::Unbounded,
        );
        assert!(
            results.contains(&row_neg_zero),
            ">= 0.0 should include -0.0"
        );
        assert!(results.contains(&row_pos_zero), ">= 0.0 should include 0.0");
        assert!(
            !results.contains(&row_negative),
            ">= 0.0 should exclude -1.0"
        );
    }

    #[test]
    fn real_negative_zero_range_lt() {
        // WHERE amount < 0.0 should exclude -0.0 (equal per IEEE 754, not strictly less)
        let mut storage = MemoryStorage::new();

        let row_neg_zero = ObjectId::new();
        let row_negative = ObjectId::new();

        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(-0.0),
                row_neg_zero,
            )
            .unwrap();
        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(-1.0),
                row_negative,
            )
            .unwrap();

        // < 0.0 should exclude -0.0 but include -1.0
        let results = storage.index_range(
            "prices",
            "amount",
            "main",
            Bound::Unbounded,
            Bound::Excluded(&Value::Double(0.0)),
        );
        assert!(
            !results.contains(&row_neg_zero),
            "< 0.0 should exclude -0.0"
        );
        assert!(results.contains(&row_negative), "< 0.0 should include -1.0");
    }

    #[test]
    fn memory_storage_catalogue_manifest_conflict_is_rejected() {
        let mut storage = MemoryStorage::new();
        let app_id = ObjectId::new();
        let schema_object_id = ObjectId::new();

        storage
            .append_catalogue_manifest_op(
                app_id,
                CatalogueManifestOp::SchemaSeen {
                    object_id: schema_object_id,
                    schema_hash: SchemaHash::from_bytes([0x44; 32]),
                },
            )
            .unwrap();

        let conflict = storage.append_catalogue_manifest_op(
            app_id,
            CatalogueManifestOp::SchemaSeen {
                object_id: schema_object_id,
                schema_hash: SchemaHash::from_bytes([0x55; 32]),
            },
        );
        assert!(matches!(conflict, Err(StorageError::IoError(_))));
    }
}
