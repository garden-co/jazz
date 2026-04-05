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

#[cfg(test)]
pub mod conformance;
mod key_codec;
mod opfs_btree;
mod storage_core;
pub use opfs_btree::OpfsBTreeStorage;
#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
mod fjall;
#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
pub use fjall::FjallStorage;
#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
mod rocksdb;
#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
pub use rocksdb::RocksDBStorage;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
mod sqlite;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub use sqlite::SqliteStorage;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smolset::SmolSet;

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId, PrefixBatchCatalog, PrefixBatchMeta};
use crate::query_manager::types::{
    BatchBranchKey, BatchId, BatchOrd, ColumnName, PrefixId, QueryBranchRef, SchemaHash,
    ScopedObject, TableName, Value,
};
use crate::sync_manager::DurabilityTier;

// ============================================================================
// Storage Types
// ============================================================================

/// Errors from storage operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageError {
    NotFound,
    IoError(String),
    IndexKeyTooLarge {
        table: String,
        column: String,
        branch: String,
        key_bytes: usize,
        max_key_bytes: usize,
    },
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::NotFound => write!(f, "not found"),
            StorageError::IoError(message) => write!(f, "{message}"),
            StorageError::IndexKeyTooLarge {
                table,
                column,
                branch,
                key_bytes,
                max_key_bytes,
            } => write!(
                f,
                "indexed value too large for {table}.{column} on branch {branch}: index key would be {key_bytes} bytes (max {max_key_bytes})"
            ),
        }
    }
}

impl std::error::Error for StorageError {}

pub(crate) fn validate_index_value_size(
    table: &str,
    column: &str,
    branch: &QueryBranchRef,
    value: &Value,
) -> Result<(), StorageError> {
    key_codec::validate_index_entry_size(table, column, branch, value)
}

// ============================================================================
// LoadedBranch - Branch data returned from storage
// ============================================================================

/// Branch data loaded from storage.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadedBranch {
    pub commits: Vec<Commit>,
    pub tips: SmallVec<[CommitId; 2]>,
    pub tails: SmallVec<[CommitId; 2]>,
}

/// Tip commits loaded from storage without replaying full branch history.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadedBranchTips {
    pub tips: Vec<Commit>,
}

/// Batch catalog updates applied when appending one commit on a composed batch branch.
#[derive(Debug, Clone, PartialEq)]
pub struct PrefixBatchUpdate {
    pub prefix: BranchName,
    pub batch_meta: PrefixBatchMeta,
    pub remove_leaf_batch_ords: SmolSet<[BatchOrd; 4]>,
    pub increment_parent_child_counts: SmallVec<[BatchOrd; 4]>,
}

/// One active table batch with its visible-row refcount.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TablePrefixBatchEntry {
    pub batch_id: BatchId,
    pub ref_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct TablePrefixBatchLookupEntry {
    batch_id: BatchId,
    batch_ord: BatchOrd,
}

/// Compact active-batch manifest for one `(table, prefix)` pair.
///
/// Batch ords are dense positions in `entries_by_ord`. A compact sorted lookup
/// table maps `BatchId -> BatchOrd` for binary-search membership updates.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TablePrefixBatchManifest {
    pub entries_by_ord: Vec<TablePrefixBatchEntry>,
    #[serde(skip)]
    lookup_by_id: Vec<TablePrefixBatchLookupEntry>,
}

impl TablePrefixBatchManifest {
    fn ensure_lookup(&mut self) {
        if self.lookup_by_id.len() != self.entries_by_ord.len() {
            self.rebuild_lookup();
        }
    }

    fn rebuild_lookup(&mut self) {
        self.lookup_by_id = self
            .entries_by_ord
            .iter()
            .enumerate()
            .map(|(index, entry)| TablePrefixBatchLookupEntry {
                batch_id: entry.batch_id,
                batch_ord: BatchOrd(index as u32),
            })
            .collect();
        self.lookup_by_id
            .sort_by_key(|entry| *entry.batch_id.as_bytes());
    }

    fn lookup_ord(&self, batch_id: &BatchId) -> Option<BatchOrd> {
        let key = *batch_id.as_bytes();
        self.lookup_by_id
            .binary_search_by_key(&key, |entry| *entry.batch_id.as_bytes())
            .ok()
            .map(|index| self.lookup_by_id[index].batch_ord)
    }

    fn insert_lookup(&mut self, batch_id: BatchId, batch_ord: BatchOrd) {
        let key = *batch_id.as_bytes();
        match self
            .lookup_by_id
            .binary_search_by_key(&key, |entry| *entry.batch_id.as_bytes())
        {
            Ok(index) => self.lookup_by_id[index].batch_ord = batch_ord,
            Err(index) => self.lookup_by_id.insert(
                index,
                TablePrefixBatchLookupEntry {
                    batch_id,
                    batch_ord,
                },
            ),
        }
    }

    fn remove_lookup_and_shift_after(&mut self, batch_id: &BatchId, removed_ord: BatchOrd) {
        let key = *batch_id.as_bytes();
        if let Ok(index) = self
            .lookup_by_id
            .binary_search_by_key(&key, |entry| *entry.batch_id.as_bytes())
        {
            self.lookup_by_id.remove(index);
        }

        for entry in &mut self.lookup_by_id {
            if entry.batch_ord.as_usize() > removed_ord.as_usize() {
                entry.batch_ord = BatchOrd(entry.batch_ord.0 - 1);
            }
        }
    }

    pub fn branch_keys(&self, prefix: BranchName) -> Vec<BatchBranchKey> {
        self.entries_by_ord
            .iter()
            .map(|entry| BatchBranchKey::from_prefix_name_and_batch(prefix, entry.batch_id))
            .collect()
    }

    pub fn adjust_refcount(&mut self, batch_id: BatchId, delta: i64) {
        self.ensure_lookup();

        if let Some(batch_ord) = self.lookup_ord(&batch_id) {
            let index = batch_ord.as_usize();
            let current = self.entries_by_ord[index].ref_count;
            let next = if delta >= 0 {
                current.saturating_add(delta as u64)
            } else {
                current.saturating_sub(delta.unsigned_abs())
            };
            if next == 0 {
                self.entries_by_ord.remove(index);
                self.remove_lookup_and_shift_after(&batch_id, batch_ord);
            } else {
                self.entries_by_ord[index].ref_count = next;
            }
        } else if delta > 0 {
            let batch_ord = BatchOrd(self.entries_by_ord.len() as u32);
            self.entries_by_ord.push(TablePrefixBatchEntry {
                batch_id,
                ref_count: delta as u64,
            });
            self.insert_lookup(batch_id, batch_ord);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries_by_ord.is_empty()
    }
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
    PermissionsSeen {
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
            Self::SchemaSeen { object_id, .. }
            | Self::PermissionsSeen { object_id, .. }
            | Self::LensSeen { object_id, .. } => *object_id,
        }
    }
}

/// Materialized view of catalogue objects known for an app.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogueManifest {
    pub schema_seen: HashMap<ObjectId, SchemaHash>,
    pub permissions_seen: HashMap<ObjectId, SchemaHash>,
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
            CatalogueManifestOp::PermissionsSeen {
                object_id,
                schema_hash,
            } => {
                self.permissions_seen.insert(*object_id, *schema_hash);
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

    /// Cheap existence check for an object.
    fn object_exists(&self, id: ObjectId) -> Result<bool, StorageError> {
        Ok(self.load_object_metadata(id)?.is_some())
    }

    /// Load a branch's commits and tails. Returns None if branch doesn't exist.
    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranch>, StorageError>;

    /// Load a branch when the caller already knows the object exists.
    ///
    /// Durable backends can override this to skip redundant object-existence
    /// probes on hot multi-branch load paths.
    fn load_branch_existing_object(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        self.load_branch(object_id, branch)
    }

    /// Load only the visible tip commits for a branch.
    fn load_branch_tips(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranchTips>, StorageError>;

    /// Load only the visible tip commits when the caller already knows the
    /// object exists.
    fn load_branch_tips_existing_object(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranchTips>, StorageError> {
        self.load_branch_tips(object_id, branch)
    }

    /// Resolve which branch owns a persisted commit.
    fn load_commit_branch(
        &self,
        object_id: ObjectId,
        commit_id: CommitId,
    ) -> Result<Option<QueryBranchRef>, StorageError>;

    /// Load the current batch catalog for one shared batch prefix.
    fn load_prefix_batch_catalog(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Option<PrefixBatchCatalog>, StorageError>;

    /// Load `(batch_id, head_commit_id)` pairs for all batches in a prefix.
    fn load_prefix_head_entries(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Vec<(BatchId, CommitId)>, StorageError> {
        Ok(self
            .load_prefix_batch_catalog(object_id, prefix)?
            .map(|catalog| {
                catalog
                    .batch_metas()
                    .map(|batch_meta| (batch_meta.batch_id, batch_meta.head_commit_id))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Load only leaf `(batch_id, head_commit_id)` pairs for a prefix.
    fn load_prefix_leaf_head_entries(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Vec<(BatchId, CommitId)>, StorageError> {
        Ok(self
            .load_prefix_batch_catalog(object_id, prefix)?
            .map(|catalog| {
                catalog
                    .leaf_batch_ords()
                    .filter_map(|batch_ord| {
                        catalog
                            .batch_meta_by_ord(batch_ord)
                            .map(|batch_meta| (batch_meta.batch_id, batch_meta.head_commit_id))
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Load all currently indexed table batches for one shared batch prefix.
    fn load_table_prefix_batch_keys(
        &self,
        table: &str,
        prefix: BranchName,
    ) -> Result<Vec<BatchBranchKey>, StorageError>;

    fn resolve_active_table_batch_keys(
        &self,
        table: &str,
        seed_branches: &[BatchBranchKey],
    ) -> Result<Vec<BatchBranchKey>, StorageError> {
        let mut branch_keys = Vec::new();
        for branch in seed_branches {
            let mut table_branches =
                self.load_table_prefix_batch_keys(table, branch.prefix_name())?;
            if table_branches.is_empty() {
                branch_keys.push(*branch);
            } else {
                branch_keys.append(&mut table_branches);
            }
        }
        branch_keys.sort_by(|left, right| {
            left.prefix_name()
                .as_str()
                .cmp(right.prefix_name().as_str())
                .then_with(|| left.batch_id().as_bytes().cmp(right.batch_id().as_bytes()))
        });
        branch_keys.dedup();
        Ok(branch_keys)
    }

    /// Append a commit to a branch.
    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
        commit: &Commit,
        prefix_batch_update: Option<&PrefixBatchUpdate>,
    ) -> Result<(), StorageError>;

    /// Replace the persisted state for a branch.
    ///
    /// Used for operations like truncation that rewrite the visible history.
    fn replace_branch(
        &mut self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
        commits: Vec<Commit>,
        tails: SmolSet<[CommitId; 2]>,
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
        branch: &QueryBranchRef,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError>;

    /// Remove an index entry.
    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &QueryBranchRef,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError>;

    /// Lookup exact value - returns all row IDs with this value.
    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &QueryBranchRef,
        value: &Value,
    ) -> Vec<ObjectId>;

    /// Range scan - returns row IDs matching the range bounds.
    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &QueryBranchRef,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId>;

    /// Full scan - returns all row IDs in this index.
    fn index_scan_all(&self, table: &str, column: &str, branch: &QueryBranchRef) -> Vec<ObjectId>;

    /// Lookup exact value across a branch set, returning scoped `(row_id, branch)` pairs.
    fn index_lookup_scoped(
        &self,
        table: &str,
        column: &str,
        branches: &[QueryBranchRef],
        value: &Value,
    ) -> Vec<ScopedObject> {
        let mut scoped_ids = HashSet::new();
        for branch in branches {
            let branch_key = branch.batch_branch_key();
            for row_id in self.index_lookup(table, column, branch, value) {
                scoped_ids.insert((row_id, branch_key));
            }
        }
        scoped_ids.into_iter().collect()
    }

    /// Range scan across a branch set, returning scoped `(row_id, branch)` pairs.
    fn index_range_scoped(
        &self,
        table: &str,
        column: &str,
        branches: &[QueryBranchRef],
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ScopedObject> {
        let mut scoped_ids = HashSet::new();
        for branch in branches {
            let branch_key = branch.batch_branch_key();
            for row_id in self.index_range(table, column, branch, start, end) {
                scoped_ids.insert((row_id, branch_key));
            }
        }
        scoped_ids.into_iter().collect()
    }

    /// Full scan across a branch set, returning scoped `(row_id, branch)` pairs.
    fn index_scan_all_scoped(
        &self,
        table: &str,
        column: &str,
        branches: &[QueryBranchRef],
    ) -> Vec<ScopedObject> {
        let mut scoped_ids = HashSet::new();
        for branch in branches {
            let branch_key = branch.batch_branch_key();
            for row_id in self.index_scan_all(table, column, branch) {
                scoped_ids.insert((row_id, branch_key));
            }
        }
        scoped_ids.into_iter().collect()
    }

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

    fn object_exists(&self, id: ObjectId) -> Result<bool, StorageError> {
        (**self).object_exists(id)
    }

    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        (**self).load_branch(object_id, branch)
    }

    fn load_branch_tips(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranchTips>, StorageError> {
        (**self).load_branch_tips(object_id, branch)
    }

    fn load_commit_branch(
        &self,
        object_id: ObjectId,
        commit_id: CommitId,
    ) -> Result<Option<QueryBranchRef>, StorageError> {
        (**self).load_commit_branch(object_id, commit_id)
    }

    fn load_prefix_batch_catalog(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Option<PrefixBatchCatalog>, StorageError> {
        (**self).load_prefix_batch_catalog(object_id, prefix)
    }

    fn load_prefix_head_entries(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Vec<(BatchId, CommitId)>, StorageError> {
        (**self).load_prefix_head_entries(object_id, prefix)
    }

    fn load_prefix_leaf_head_entries(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Vec<(BatchId, CommitId)>, StorageError> {
        (**self).load_prefix_leaf_head_entries(object_id, prefix)
    }

    fn load_table_prefix_batch_keys(
        &self,
        table: &str,
        prefix: BranchName,
    ) -> Result<Vec<BatchBranchKey>, StorageError> {
        (**self).load_table_prefix_batch_keys(table, prefix)
    }

    fn resolve_active_table_batch_keys(
        &self,
        table: &str,
        seed_branches: &[BatchBranchKey],
    ) -> Result<Vec<BatchBranchKey>, StorageError> {
        (**self).resolve_active_table_batch_keys(table, seed_branches)
    }

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
        commit: &Commit,
        prefix_batch_update: Option<&PrefixBatchUpdate>,
    ) -> Result<(), StorageError> {
        (**self).append_commit(object_id, branch, commit, prefix_batch_update)
    }

    fn replace_branch(
        &mut self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
        commits: Vec<Commit>,
        tails: SmolSet<[CommitId; 2]>,
    ) -> Result<(), StorageError> {
        (**self).replace_branch(object_id, branch, commits, tails)
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
        branch: &QueryBranchRef,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        (**self).index_insert(table, column, branch, value, row_id)
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &QueryBranchRef,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        (**self).index_remove(table, column, branch, value, row_id)
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &QueryBranchRef,
        value: &Value,
    ) -> Vec<ObjectId> {
        (**self).index_lookup(table, column, branch, value)
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &QueryBranchRef,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        (**self).index_range(table, column, branch, start, end)
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &QueryBranchRef) -> Vec<ObjectId> {
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

/// Index key: (table, column, compact prefix id, batch id).
type IndexKey = (TableName, ColumnName, PrefixId, BatchId);

const INLINE_INDEX_ROW_IDS: usize = 4;

#[derive(Debug, Clone, Default)]
enum IndexRowIdSet {
    #[default]
    Inline,
    Packed(SmallVec<[ObjectId; INLINE_INDEX_ROW_IDS]>),
    Spill(HashSet<ObjectId>),
}

impl IndexRowIdSet {
    fn insert(&mut self, row_id: ObjectId) -> bool {
        match self {
            Self::Inline => {
                let mut rows = SmallVec::<[ObjectId; INLINE_INDEX_ROW_IDS]>::new();
                rows.push(row_id);
                *self = Self::Packed(rows);
                true
            }
            Self::Packed(rows) => {
                if rows.contains(&row_id) {
                    return false;
                }

                rows.push(row_id);
                if rows.len() > INLINE_INDEX_ROW_IDS {
                    let mut spill = HashSet::with_capacity(rows.len());
                    spill.extend(rows.iter().copied());
                    *self = Self::Spill(spill);
                }
                true
            }
            Self::Spill(rows) => rows.insert(row_id),
        }
    }

    fn remove(&mut self, row_id: &ObjectId) -> bool {
        match self {
            Self::Inline => false,
            Self::Packed(rows) => {
                let Some(index) = rows.iter().position(|existing| existing == row_id) else {
                    return false;
                };
                rows.swap_remove(index);
                if rows.is_empty() {
                    *self = Self::Inline;
                }
                true
            }
            Self::Spill(rows) => {
                let removed = rows.remove(row_id);
                if removed && rows.len() <= INLINE_INDEX_ROW_IDS {
                    let mut packed = SmallVec::<[ObjectId; INLINE_INDEX_ROW_IDS]>::new();
                    packed.extend(rows.iter().copied());
                    *self = if packed.is_empty() {
                        Self::Inline
                    } else {
                        Self::Packed(packed)
                    };
                }
                removed
            }
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Inline => true,
            Self::Packed(rows) => rows.is_empty(),
            Self::Spill(rows) => rows.is_empty(),
        }
    }

    fn extend_vec(&self, out: &mut Vec<ObjectId>) {
        match self {
            Self::Inline => {}
            Self::Packed(rows) => out.extend(rows.iter().copied()),
            Self::Spill(rows) => out.extend(rows.iter().copied()),
        }
    }

    fn extend_hash_set(&self, out: &mut HashSet<ObjectId>) {
        match self {
            Self::Inline => {}
            Self::Packed(rows) => out.extend(rows.iter().copied()),
            Self::Spill(rows) => out.extend(rows.iter().copied()),
        }
    }
}

/// Index storage: encoded_value -> row_ids. BTreeMap for correct range query ordering.
type IndexEntries = BTreeMap<Vec<u8>, IndexRowIdSet>;

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
    /// Active table batches keyed by shared batch prefix.
    table_batches_by_prefix: HashMap<(TableName, BranchName), TablePrefixBatchManifest>,
    /// Append-only manifest ops keyed by app_id then op object_id.
    catalogue_manifest_ops: HashMap<ObjectId, HashMap<ObjectId, CatalogueManifestOp>>,
}

/// Internal object storage structure.
#[derive(Debug, Clone, Default)]
struct ObjectData {
    metadata: HashMap<String, String>,
    branches: ObjectBranchDataStore,
    commit_branches: HashMap<CommitId, BatchBranchKey>,
}

/// Internal branch storage structure.
#[derive(Debug, Clone, Default)]
struct BranchData {
    commits: Vec<Commit>,
    tails: SmolSet<[CommitId; 2]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BranchDataSlotLookupEntry {
    batch_id: BatchId,
    slot_index: u32,
}

#[derive(Debug, Clone)]
struct BranchDataSlot {
    branch_data: BranchData,
}

#[derive(Debug, Clone, Default)]
struct PrefixBranchDataStore {
    branch_slots: Vec<BranchDataSlot>,
    lookup_by_id: Vec<BranchDataSlotLookupEntry>,
}

impl PrefixBranchDataStore {
    fn lookup_index(&self, batch_id: &BatchId) -> Result<usize, usize> {
        let key = *batch_id.as_bytes();
        self.lookup_by_id
            .binary_search_by_key(&key, |entry| *entry.batch_id.as_bytes())
    }

    fn slot_index(&self, batch_id: &BatchId) -> Option<usize> {
        self.lookup_index(batch_id)
            .ok()
            .map(|index| self.lookup_by_id[index].slot_index as usize)
    }

    fn get(&self, batch_id: &BatchId) -> Option<&BranchData> {
        let slot_index = self.slot_index(batch_id)?;
        self.branch_slots
            .get(slot_index)
            .map(|slot| &slot.branch_data)
    }

    fn get_or_insert_default(&mut self, batch_id: BatchId) -> &mut BranchData {
        if let Some(slot_index) = self.slot_index(&batch_id) {
            return &mut self.branch_slots[slot_index].branch_data;
        }

        let slot_index = self.branch_slots.len() as u32;
        self.branch_slots.push(BranchDataSlot {
            branch_data: BranchData::default(),
        });
        let insert_index = self
            .lookup_index(&batch_id)
            .expect_err("branch batch should be absent before insert");
        self.lookup_by_id.insert(
            insert_index,
            BranchDataSlotLookupEntry {
                batch_id,
                slot_index,
            },
        );
        &mut self.branch_slots[slot_index as usize].branch_data
    }
}

#[derive(Debug, Clone, Default)]
struct PrefixObjectDataState {
    branches: PrefixBranchDataStore,
    batch_catalog: Option<PrefixBatchCatalog>,
}

#[derive(Debug, Clone, Default)]
struct ObjectBranchDataStore {
    branches_by_prefix: HashMap<BranchName, PrefixObjectDataState>,
}

impl ObjectBranchDataStore {
    fn get(&self, branch_key: &BatchBranchKey) -> Option<&BranchData> {
        self.branches_by_prefix
            .get(&branch_key.prefix_name())?
            .branches
            .get(&branch_key.batch_id())
    }

    fn get_or_insert_default(&mut self, branch_key: BatchBranchKey) -> &mut BranchData {
        self.branches_by_prefix
            .entry(branch_key.prefix_name())
            .or_default()
            .branches
            .get_or_insert_default(branch_key.batch_id())
    }

    fn prefix_catalog(&self, prefix_name: &BranchName) -> Option<&PrefixBatchCatalog> {
        self.branches_by_prefix
            .get(prefix_name)?
            .batch_catalog
            .as_ref()
    }

    fn prefix_catalog_mut_or_default(
        &mut self,
        prefix_name: BranchName,
    ) -> &mut PrefixBatchCatalog {
        self.branches_by_prefix
            .entry(prefix_name)
            .or_default()
            .batch_catalog
            .get_or_insert_with(PrefixBatchCatalog::default)
    }
}

impl MemoryStorage {
    /// Create a new empty MemoryStorage.
    pub fn new() -> Self {
        Self::default()
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
            .load_table_prefix_batch_keys(table, BranchName::new(prefix))?
            .into_iter()
            .map(|branch_key| branch_key.batch_id())
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

    fn composed_table_batch(branch: &QueryBranchRef) -> (BranchName, BatchId) {
        (branch.prefix_name(), branch.batch_id())
    }

    fn index_branch_key(branch: &QueryBranchRef) -> (PrefixId, BatchId) {
        let branch_key = branch.batch_branch_key();
        (branch_key.prefix_id(), branch_key.batch_id())
    }

    fn index_key(table: &str, column: &str, branch: &QueryBranchRef) -> IndexKey {
        let (prefix_id, batch_id) = Self::index_branch_key(branch);
        (
            TableName::new(table),
            ColumnName::new(column),
            prefix_id,
            batch_id,
        )
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
            // Arrays use serialized bytes for equality semantics.
            // The durable key codec hashes oversized segments if needed.
            let mut bytes = vec![0x07];
            let json = serde_json::to_string(value).unwrap_or_default();
            bytes.extend_from_slice(json.as_bytes());
            bytes
        }

        Value::Row { .. } => {
            // Rows use serialized bytes for equality semantics.
            // The durable key codec hashes oversized segments if needed.
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
                branches: ObjectBranchDataStore::default(),
                commit_branches: HashMap::new(),
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

    fn object_exists(&self, id: ObjectId) -> Result<bool, StorageError> {
        Ok(self.objects.contains_key(&id))
    }

    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        let Some(obj) = self.objects.get(&object_id) else {
            return Ok(None);
        };
        let Some(branch_data) = obj.branches.get(&branch.batch_branch_key()) else {
            return Ok(None);
        };
        let mut commits = branch_data.commits.clone();
        for commit in &mut commits {
            if let Some(tiers) = self.ack_tiers.get(&commit.id()) {
                commit.ack_state.confirmed_tiers = tiers.clone();
            }
        }
        let mut tips = branch_data
            .tails
            .iter()
            .copied()
            .collect::<SmallVec<[CommitId; 2]>>();
        tips.sort_unstable();
        let mut tails = branch_data
            .tails
            .iter()
            .copied()
            .collect::<SmallVec<[CommitId; 2]>>();
        tails.sort_unstable();
        Ok(Some(LoadedBranch {
            commits,
            tips,
            tails,
        }))
    }

    fn load_branch_existing_object(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        self.load_branch(object_id, branch)
    }

    fn load_branch_tips(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranchTips>, StorageError> {
        let Some(obj) = self.objects.get(&object_id) else {
            return Ok(None);
        };
        let Some(branch_data) = obj.branches.get(&branch.batch_branch_key()) else {
            return Ok(None);
        };

        let mut tips = Vec::new();
        for tip_id in &branch_data.tails {
            let Some(commit) = branch_data
                .commits
                .iter()
                .find(|commit| commit.id() == *tip_id)
            else {
                continue;
            };
            let mut commit = commit.clone();
            if let Some(tiers) = self.ack_tiers.get(tip_id) {
                commit.ack_state.confirmed_tiers = tiers.clone();
            }
            tips.push(commit);
        }

        Ok(Some(LoadedBranchTips { tips }))
    }

    fn load_branch_tips_existing_object(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranchTips>, StorageError> {
        self.load_branch_tips(object_id, branch)
    }

    fn load_commit_branch(
        &self,
        object_id: ObjectId,
        commit_id: CommitId,
    ) -> Result<Option<QueryBranchRef>, StorageError> {
        Ok(self
            .objects
            .get(&object_id)
            .and_then(|obj| obj.commit_branches.get(&commit_id).copied())
            .map(QueryBranchRef::from_batch_branch_key))
    }

    fn load_prefix_batch_catalog(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Option<PrefixBatchCatalog>, StorageError> {
        Ok(self.objects.get(&object_id).and_then(|obj| {
            obj.branches
                .prefix_catalog(&BranchName::new(prefix))
                .cloned()
        }))
    }

    fn load_prefix_head_entries(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Vec<(BatchId, CommitId)>, StorageError> {
        let Some(obj) = self.objects.get(&object_id) else {
            return Ok(Vec::new());
        };
        let Some(catalog) = obj.branches.prefix_catalog(&BranchName::new(prefix)) else {
            return Ok(Vec::new());
        };
        Ok(catalog
            .batch_metas()
            .map(|batch_meta| (batch_meta.batch_id, batch_meta.head_commit_id))
            .collect())
    }

    fn load_prefix_leaf_head_entries(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Vec<(BatchId, CommitId)>, StorageError> {
        let Some(obj) = self.objects.get(&object_id) else {
            return Ok(Vec::new());
        };
        let Some(catalog) = obj.branches.prefix_catalog(&BranchName::new(prefix)) else {
            return Ok(Vec::new());
        };
        Ok(catalog
            .leaf_batch_ords()
            .filter_map(|batch_ord| {
                catalog
                    .batch_meta_by_ord(batch_ord)
                    .map(|batch_meta| (batch_meta.batch_id, batch_meta.head_commit_id))
            })
            .collect())
    }

    fn load_table_prefix_batch_keys(
        &self,
        table: &str,
        prefix: BranchName,
    ) -> Result<Vec<BatchBranchKey>, StorageError> {
        Ok(self
            .table_batches_by_prefix
            .get(&(TableName::new(table), prefix))
            .map(|manifest| manifest.branch_keys(prefix))
            .unwrap_or_default())
    }

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
        commit: &Commit,
        prefix_batch_update: Option<&PrefixBatchUpdate>,
    ) -> Result<(), StorageError> {
        let obj = self.objects.entry(object_id).or_default();
        let branch_key = branch.batch_branch_key();
        let branch_data = obj.branches.get_or_insert_default(branch_key);

        let commit_id = commit.id();

        // Remove parents from tips
        for parent in &commit.parents {
            branch_data.tails.remove(parent);
        }

        // Add this commit as a tip
        branch_data.tails.insert(commit_id);
        branch_data.commits.push(commit.clone());
        obj.commit_branches.insert(commit_id, branch_key);

        if let Some(update) = prefix_batch_update {
            let catalog = obj.branches.prefix_catalog_mut_or_default(update.prefix);
            for parent_batch_ord in &update.increment_parent_child_counts {
                if let Some(parent_meta) = catalog.batch_meta_by_ord_mut(*parent_batch_ord) {
                    parent_meta.child_count = parent_meta.child_count.saturating_add(1);
                }
            }
            for removed_batch_ord in &update.remove_leaf_batch_ords {
                catalog.remove_leaf_batch_ord(*removed_batch_ord);
            }
            catalog.insert_batch_meta(update.batch_meta.clone());
            catalog.insert_leaf_batch_ord(update.batch_meta.batch_ord);
        }

        Ok(())
    }

    fn replace_branch(
        &mut self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
        commits: Vec<Commit>,
        tails: SmolSet<[CommitId; 2]>,
    ) -> Result<(), StorageError> {
        if let Some(obj) = self.objects.get_mut(&object_id) {
            let branch_key = branch.batch_branch_key();
            let branch_data = obj.branches.get_or_insert_default(branch_key);
            let old_commit_ids: HashSet<CommitId> =
                branch_data.commits.iter().map(Commit::id).collect();
            let new_commit_ids: HashSet<CommitId> = commits.iter().map(Commit::id).collect();

            for removed_commit_id in old_commit_ids.difference(&new_commit_ids) {
                obj.commit_branches.remove(removed_commit_id);
            }
            for commit in &commits {
                obj.commit_branches.insert(commit.id(), branch_key);
            }

            branch_data.commits = commits;
            branch_data.tails = tails;
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
        branch: &QueryBranchRef,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let key = Self::index_key(table, column, branch);
        let index = self.indices.entry(key).or_default();
        let encoded = encode_value(value);
        let inserted = index.entry(encoded).or_default().insert(row_id);
        if inserted && matches!(column, "_id" | "_id_deleted") {
            let (prefix, batch_id) = Self::composed_table_batch(branch);
            self.table_batches_by_prefix
                .entry((TableName::new(table), prefix))
                .or_default()
                .adjust_refcount(batch_id, 1);
        }
        Ok(())
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &QueryBranchRef,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let key = Self::index_key(table, column, branch);
        let mut removed = false;
        if let Some(index) = self.indices.get_mut(&key) {
            let encoded = encode_value(value);
            if let Some(row_ids) = index.get_mut(&encoded) {
                removed = row_ids.remove(&row_id);
                if row_ids.is_empty() {
                    index.remove(&encoded);
                }
            }
        }
        if removed && matches!(column, "_id" | "_id_deleted") {
            let (prefix, batch_id) = Self::composed_table_batch(branch);
            if let Some(manifest) = self
                .table_batches_by_prefix
                .get_mut(&(TableName::new(table), prefix))
            {
                manifest.adjust_refcount(batch_id, -1);
                if manifest.is_empty() {
                    self.table_batches_by_prefix
                        .remove(&(TableName::new(table), prefix));
                }
            }
        }
        Ok(())
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &QueryBranchRef,
        value: &Value,
    ) -> Vec<ObjectId> {
        let key = Self::index_key(table, column, branch);
        let Some(index) = self.indices.get(&key) else {
            return Vec::new();
        };

        // IEEE 754: -0.0 == 0.0, so look up both encodings and merge.
        if is_double_zero(value) {
            let mut result = HashSet::new();
            for zero in &[Value::Double(0.0), Value::Double(-0.0)] {
                if let Some(ids) = index.get(&encode_value(zero)) {
                    ids.extend_hash_set(&mut result);
                }
            }
            return result.into_iter().collect();
        }

        let encoded = encode_value(value);
        let Some(ids) = index.get(&encoded) else {
            return Vec::new();
        };
        let mut result = Vec::new();
        ids.extend_vec(&mut result);
        result
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &QueryBranchRef,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        let key = Self::index_key(table, column, branch);
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
            .fold(Vec::new(), |mut result, (_, ids)| {
                ids.extend_vec(&mut result);
                result
            })
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &QueryBranchRef) -> Vec<ObjectId> {
        let key = Self::index_key(table, column, branch);
        let Some(index) = self.indices.get(&key) else {
            return Vec::new();
        };
        index.values().fold(Vec::new(), |mut result, ids| {
            ids.extend_vec(&mut result);
            result
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

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

    #[test]
    fn table_prefix_batch_manifest_shifts_dense_ords_after_middle_removal() {
        let prefix = BranchName::new(format!("dev-{}-alice", SchemaHash::from_bytes([0x44; 32])));
        let batch_a = BatchId::parse_segment("b0000000000000000000000000000000a").unwrap();
        let batch_b = BatchId::parse_segment("b0000000000000000000000000000000b").unwrap();
        let batch_c = BatchId::parse_segment("b0000000000000000000000000000000c").unwrap();
        let batch_d = BatchId::parse_segment("b0000000000000000000000000000000d").unwrap();

        let mut manifest = TablePrefixBatchManifest::default();
        manifest.adjust_refcount(batch_a, 1);
        manifest.adjust_refcount(batch_b, 1);
        manifest.adjust_refcount(batch_c, 1);

        assert_eq!(manifest.lookup_ord(&batch_a), Some(BatchOrd(0)));
        assert_eq!(manifest.lookup_ord(&batch_b), Some(BatchOrd(1)));
        assert_eq!(manifest.lookup_ord(&batch_c), Some(BatchOrd(2)));

        manifest.adjust_refcount(batch_b, -1);

        assert_eq!(manifest.lookup_ord(&batch_a), Some(BatchOrd(0)));
        assert_eq!(manifest.lookup_ord(&batch_b), None);
        assert_eq!(manifest.lookup_ord(&batch_c), Some(BatchOrd(1)));
        assert_eq!(
            manifest
                .branch_keys(prefix)
                .into_iter()
                .map(|branch_key| branch_key.batch_id())
                .collect::<Vec<_>>(),
            vec![batch_a, batch_c]
        );

        manifest.adjust_refcount(batch_d, 1);
        assert_eq!(manifest.lookup_ord(&batch_d), Some(BatchOrd(2)));
    }

    #[test]
    fn index_row_id_set_promotes_and_compacts_without_duplicates() {
        let rows = [
            ObjectId::new(),
            ObjectId::new(),
            ObjectId::new(),
            ObjectId::new(),
            ObjectId::new(),
        ];

        let mut row_ids = IndexRowIdSet::default();
        for row_id in rows {
            assert!(row_ids.insert(row_id));
        }
        assert!(!row_ids.insert(rows[0]));

        let mut seen = Vec::new();
        row_ids.extend_vec(&mut seen);
        let seen: HashSet<_> = seen.into_iter().collect();
        assert_eq!(seen, HashSet::from(rows));

        assert!(row_ids.remove(&rows[4]));
        assert!(row_ids.remove(&rows[3]));
        assert!(!row_ids.remove(&rows[3]));

        let mut seen_after_compact = Vec::new();
        row_ids.extend_vec(&mut seen_after_compact);
        let seen_after_compact: HashSet<_> = seen_after_compact.into_iter().collect();
        assert_eq!(
            seen_after_compact,
            HashSet::from([rows[0], rows[1], rows[2]])
        );
    }

    fn make_commit(content: &[u8]) -> Commit {
        Commit {
            parents: smallvec![],
            content: content.to_vec(),
            timestamp: 12345,
            author: ObjectId::new().to_string(),
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
        let branch = test_branch_ref("main");

        storage.create_object(id, HashMap::new()).unwrap();

        // Initially no branch
        assert_eq!(storage.load_branch(id, &branch).unwrap(), None);

        // Append commit creates branch
        let commit = make_commit(b"first");
        let commit_id = commit.id();
        storage.append_commit(id, &branch, &commit, None).unwrap();

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert_eq!(
            loaded.tips,
            SmallVec::<[CommitId; 2]>::from_iter([commit_id])
        );
        assert!(loaded.tails.contains(&commit_id));
        let loaded_tips = storage.load_branch_tips(id, &branch).unwrap().unwrap();
        assert_eq!(loaded_tips.tips.len(), 1);
        assert_eq!(loaded_tips.tips[0].id(), commit_id);

        storage
            .replace_branch(id, &branch, Vec::new(), SmolSet::new())
            .unwrap();
        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 0);
        assert_eq!(storage.load_commit_branch(id, commit_id).unwrap(), None);
        let loaded_tips = storage.load_branch_tips(id, &branch).unwrap().unwrap();
        assert!(loaded_tips.tips.is_empty());
    }

    #[test]
    fn memory_storage_keeps_same_prefix_batches_in_separate_branch_slots() {
        let mut storage = MemoryStorage::new();
        let id = ObjectId::new();
        storage.create_object(id, HashMap::new()).unwrap();

        let prefix = crate::query_manager::types::BranchPrefixName::new(
            "dev",
            crate::query_manager::types::SchemaHash::from_bytes([7; 32]),
            "main",
        );
        let branch_a = QueryBranchRef::from_prefix_and_batch(
            &prefix,
            BatchId::from_uuid(uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, b"batch-a")),
        );
        let branch_b = QueryBranchRef::from_prefix_and_batch(
            &prefix,
            BatchId::from_uuid(uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, b"batch-b")),
        );

        storage
            .append_commit(id, &branch_a, &make_commit(b"one"), None)
            .unwrap();
        storage
            .append_commit(id, &branch_b, &make_commit(b"two"), None)
            .unwrap();

        let loaded_a = storage.load_branch(id, &branch_a).unwrap().unwrap();
        let loaded_b = storage.load_branch(id, &branch_b).unwrap().unwrap();
        assert_eq!(loaded_a.commits.len(), 1);
        assert_eq!(loaded_b.commits.len(), 1);
        assert_eq!(loaded_a.commits[0].content, b"one".to_vec());
        assert_eq!(loaded_b.commits[0].content, b"two".to_vec());
    }

    #[test]
    fn memory_storage_tracks_commit_branches_and_prefix_leaves() {
        let mut storage = MemoryStorage::new();
        let id = ObjectId::new();
        storage.create_object(id, HashMap::new()).unwrap();

        let prefix = format!("dev-{}-main", SchemaHash::from_bytes([7; 32]));
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
                &commit1,
                Some(&PrefixBatchUpdate {
                    prefix: prefix.clone().into(),
                    batch_meta: PrefixBatchMeta {
                        batch_id: batch1_id,
                        batch_ord: crate::query_manager::types::BatchOrd(0),
                        root_commit_id: commit1_id,
                        head_commit_id: commit1_id,
                        first_timestamp: commit1.timestamp,
                        last_timestamp: commit1.timestamp,
                        parent_batch_ords: smallvec::smallvec![],
                        child_count: 0,
                    },
                    remove_leaf_batch_ords: smolset::SmolSet::new(),
                    increment_parent_child_counts: SmallVec::new(),
                }),
            )
            .unwrap();

        assert_eq!(
            storage.load_commit_branch(id, commit1_id).unwrap(),
            Some(branch1)
        );
        assert_eq!(
            storage
                .load_prefix_batch_catalog(id, &prefix)
                .unwrap()
                .map(|catalog| catalog.leaf_batch_ids().collect::<HashSet<_>>()),
            Some(HashSet::from([batch1_id]))
        );

        let mut commit2 = make_commit(b"second");
        commit2.parents = smallvec![commit1_id];
        let commit2_id = commit2.id();
        storage
            .append_commit(
                id,
                &branch2,
                &commit2,
                Some(&PrefixBatchUpdate {
                    prefix: prefix.clone().into(),
                    batch_meta: PrefixBatchMeta {
                        batch_id: batch2_id,
                        batch_ord: crate::query_manager::types::BatchOrd(1),
                        root_commit_id: commit2_id,
                        head_commit_id: commit2_id,
                        first_timestamp: commit2.timestamp,
                        last_timestamp: commit2.timestamp,
                        parent_batch_ords: smallvec::smallvec![
                            crate::query_manager::types::BatchOrd(0)
                        ],
                        child_count: 0,
                    },
                    remove_leaf_batch_ords: [crate::query_manager::types::BatchOrd(0)]
                        .into_iter()
                        .collect(),
                    increment_parent_child_counts: smallvec::smallvec![
                        crate::query_manager::types::BatchOrd(0)
                    ],
                }),
            )
            .unwrap();

        assert_eq!(
            storage.load_commit_branch(id, commit2_id).unwrap(),
            Some(branch2)
        );
        assert_eq!(
            storage
                .load_prefix_batch_catalog(id, &prefix)
                .unwrap()
                .map(|catalog| catalog.leaf_batch_ids().collect::<HashSet<_>>()),
            Some(HashSet::from([batch2_id]))
        );
    }

    #[test]
    fn memory_storage_tracks_table_prefix_batches() {
        let mut storage = MemoryStorage::new();
        let prefix = format!("dev-{}-main", SchemaHash::from_bytes([7; 32]));
        let batch1 = BatchId::parse_segment(&format!("b{:032x}", 1)).unwrap();
        let batch2 = BatchId::parse_segment(&format!("b{:032x}", 2)).unwrap();
        let users_branch1 = format!("{prefix}-{}", batch1.branch_segment());
        let users_branch2 = format!("{prefix}-{}", batch2.branch_segment());
        let posts_branch1 = format!("{prefix}-{}", batch1.branch_segment());
        let user_row1 = ObjectId::new();
        let user_row2 = ObjectId::new();
        let post_row1 = ObjectId::new();

        storage
            .index_insert(
                "users",
                "_id",
                &users_branch1,
                &Value::Uuid(user_row1),
                user_row1,
            )
            .unwrap();
        storage
            .index_insert(
                "users",
                "_id_deleted",
                &users_branch2,
                &Value::Uuid(user_row2),
                user_row2,
            )
            .unwrap();
        storage
            .index_insert(
                "posts",
                "_id",
                &posts_branch1,
                &Value::Uuid(post_row1),
                post_row1,
            )
            .unwrap();

        assert_eq!(
            storage.load_table_prefix_batches("users", &prefix).unwrap(),
            HashSet::from([batch1, batch2])
        );
        assert_eq!(
            storage.load_table_prefix_batches("posts", &prefix).unwrap(),
            HashSet::from([batch1])
        );

        storage
            .index_remove(
                "users",
                "_id",
                &users_branch1,
                &Value::Uuid(user_row1),
                user_row1,
            )
            .unwrap();
        storage
            .index_remove(
                "users",
                "_id_deleted",
                &users_branch2,
                &Value::Uuid(user_row2),
                user_row2,
            )
            .unwrap();

        assert_eq!(
            storage.load_table_prefix_batches("users", &prefix).unwrap(),
            HashSet::new()
        );
    }

    #[test]
    fn memory_storage_keeps_same_batch_id_isolated_across_prefixes() {
        let mut storage = MemoryStorage::new();
        let shared_batch = BatchId::parse_segment("b0000000000000000000000000000002a").unwrap();
        let prefix_a = format!("dev-{}-main", SchemaHash::from_bytes([0x11; 32]));
        let prefix_b = format!("dev-{}-main", SchemaHash::from_bytes([0x22; 32]));
        let branch_a = format!("{prefix_a}-{}", shared_batch.branch_segment());
        let branch_b = format!("{prefix_b}-{}", shared_batch.branch_segment());
        let row_a = ObjectId::new();
        let row_b = ObjectId::new();

        storage
            .index_insert(
                "users",
                "name",
                &branch_a,
                &Value::Text("alice".to_string()),
                row_a,
            )
            .unwrap();
        storage
            .index_insert(
                "users",
                "name",
                &branch_b,
                &Value::Text("alice".to_string()),
                row_b,
            )
            .unwrap();

        assert_eq!(
            storage.index_lookup(
                "users",
                "name",
                &branch_a,
                &Value::Text("alice".to_string())
            ),
            vec![row_a]
        );
        assert_eq!(
            storage.index_lookup(
                "users",
                "name",
                &branch_b,
                &Value::Text("alice".to_string())
            ),
            vec![row_b]
        );
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
