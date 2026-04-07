use std::collections::HashMap;
use web_time::{SystemTime, UNIX_EPOCH};

use smolset::SmolSet;

use crate::commit::CommitId;
use crate::metadata::MetadataKey;
use crate::object::{BranchName, ObjectId};
use crate::row_regions::{HistoryScan, RowState, StoredRowVersion, VisibleRowEntry};
use crate::storage::{Storage, StorageError};
use crate::sync_manager::DurabilityTier;

/// Visible row change emitted when a row object's winning version changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleRowUpdate {
    pub object_id: ObjectId,
    pub metadata: HashMap<String, String>,
    pub row: StoredRowVersion,
    pub previous_row: Option<StoredRowVersion>,
    pub is_new_object: bool,
}

/// Errors that can occur when managing objects and commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ObjectNotFound(ObjectId),
    BranchNotFound(BranchName),
    ParentNotFound(CommitId),
    /// Storage operation failed.
    StorageError(StorageError),
}

#[derive(Debug, Clone, Default)]
struct RowBranch {
    versions: HashMap<CommitId, StoredRowVersion>,
    tips: SmolSet<[CommitId; 2]>,
    current_visible: Option<CommitId>,
}

#[derive(Debug, Clone)]
struct RowBranchApply {
    previous_visible: Option<StoredRowVersion>,
    current_visible: Option<StoredRowVersion>,
    visible_changed: bool,
}

impl RowBranch {
    fn visible_row_wins(incoming: &StoredRowVersion, current: &StoredRowVersion) -> bool {
        (incoming.updated_at, incoming.version_id()) > (current.updated_at, current.version_id())
    }

    fn current_visible_row(&self) -> Option<StoredRowVersion> {
        let version_id = self.current_visible?;
        self.versions.get(&version_id).cloned()
    }

    fn visible_entry(&self) -> Option<VisibleRowEntry> {
        let current_row = self.current_visible_row()?;
        let history_rows = self.versions.values().cloned().collect::<Vec<_>>();
        Some(VisibleRowEntry::rebuild(current_row, &history_rows))
    }

    fn contains(&self, version_id: CommitId) -> bool {
        self.versions.contains_key(&version_id)
    }

    fn recompute_current_visible(&mut self) -> Option<CommitId> {
        let current = self
            .versions
            .values()
            .filter(|row| row.state.is_visible())
            .max_by_key(|row| (row.updated_at, row.version_id()))
            .map(StoredRowVersion::version_id);
        self.current_visible = current;
        current
    }

    fn remember(&mut self, row: StoredRowVersion) -> RowBranchApply {
        let previous_visible = self.current_visible_row();
        let version_id = row.version_id();

        if self.versions.contains_key(&version_id) {
            let current_visible = self.current_visible_row();
            return RowBranchApply {
                visible_changed: previous_visible != current_visible,
                previous_visible,
                current_visible,
            };
        }

        for parent in &row.parents {
            self.tips.remove(parent);
        }
        self.tips.insert(version_id);
        self.versions.insert(version_id, row);

        let current_visible_id = match previous_visible.as_ref() {
            Some(current) => {
                let incoming = self
                    .versions
                    .get(&version_id)
                    .expect("just inserted row version");
                if incoming.state.is_visible() && Self::visible_row_wins(incoming, current) {
                    Some(version_id)
                } else {
                    self.current_visible
                }
            }
            None => self.recompute_current_visible(),
        };
        self.current_visible = current_visible_id;

        let current_visible = self.current_visible_row();
        RowBranchApply {
            visible_changed: previous_visible != current_visible,
            previous_visible,
            current_visible,
        }
    }

    fn patch_state(
        &mut self,
        version_id: CommitId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Option<RowBranchApply> {
        let previous_visible = self.current_visible_row();
        let row = self.versions.get_mut(&version_id)?;

        if let Some(state) = state {
            row.state = state;
        }
        row.confirmed_tier = match (row.confirmed_tier, confirmed_tier) {
            (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
            (Some(existing), None) => Some(existing),
            (None, incoming) => incoming,
        };

        self.recompute_current_visible();
        let current_visible = self.current_visible_row();
        Some(RowBranchApply {
            visible_changed: previous_visible != current_visible,
            previous_visible,
            current_visible,
        })
    }
}

/// Manages a collection of objects.
///
/// With sync storage (Phase 2), objects are stored directly in the HashMap -
/// no ObjectState enum, no Loading state, no async request/response cycle.
#[derive(Debug, Clone, Default)]
pub struct ObjectManager {
    pub metadata_by_id: HashMap<ObjectId, HashMap<String, String>>,
    row_branches: HashMap<(ObjectId, BranchName), RowBranch>,
    /// Outbox for visible row changes.
    pub visible_row_updates: Vec<VisibleRowUpdate>,
    /// Last timestamp used, for monotonic ordering.
    last_timestamp: u64,
}

impl ObjectManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get next monotonic timestamp (microseconds since epoch).
    /// Guarantees each call returns a value greater than the previous.
    fn next_timestamp(&mut self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_micros() as u64;

        self.last_timestamp = if now > self.last_timestamp {
            now
        } else {
            self.last_timestamp + 1
        };

        self.last_timestamp
    }

    pub fn reserve_timestamp(&mut self) -> u64 {
        self.next_timestamp()
    }

    /// Create a new metadata entry with optional metadata, returning its id.
    /// Persists to storage via Storage synchronously.
    pub fn create<H: Storage>(
        &mut self,
        io: &mut H,
        metadata: Option<HashMap<String, String>>,
    ) -> ObjectId {
        let _span = tracing::debug_span!("OM::create").entered();
        let metadata = metadata.unwrap_or_default();
        let id = ObjectId::new();

        // Sync storage - returns immediately
        let _ = io.put_metadata(id, metadata.clone());

        self.metadata_by_id.insert(id, metadata);
        tracing::debug!(%id, "created object");
        id
    }

    /// Create a metadata entry with a specific ObjectId (for deterministic IDs).
    ///
    /// Unlike `create`, this uses the provided ObjectId rather than generating a new one.
    /// Used for index root nodes that have deterministic IDs based on table/column name.
    /// Persists to storage via Storage synchronously.
    pub fn create_with_id<H: Storage>(
        &mut self,
        io: &mut H,
        id: ObjectId,
        metadata: Option<HashMap<String, String>>,
    ) -> ObjectId {
        let metadata = metadata.unwrap_or_default();

        // Sync storage - returns immediately
        let _ = io.put_metadata(id, metadata.clone());

        self.metadata_by_id.insert(id, metadata);
        id
    }

    fn is_row_metadata(metadata: &HashMap<String, String>) -> bool {
        metadata.contains_key(MetadataKey::Table.as_str())
    }

    fn row_branch(&self, object_id: ObjectId, branch_name: &BranchName) -> Option<&RowBranch> {
        self.row_branches.get(&(object_id, *branch_name))
    }

    fn row_branch_mut(
        &mut self,
        object_id: ObjectId,
        branch_name: &BranchName,
    ) -> Option<&mut RowBranch> {
        self.row_branches.get_mut(&(object_id, *branch_name))
    }

    fn apply_row_version<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: BranchName,
        row: StoredRowVersion,
    ) -> Result<CommitId, Error> {
        let object_metadata = self
            .get(object_id)
            .ok_or(Error::ObjectNotFound(object_id))?
            .clone();

        debug_assert!(
            Self::is_row_metadata(&object_metadata),
            "apply_row_version should only be used for row-backed objects"
        );

        if !row.parents.is_empty() {
            let branch = self
                .row_branch(object_id, &branch_name)
                .ok_or(Error::BranchNotFound(branch_name))?;
            for parent in &row.parents {
                if !branch.contains(*parent) {
                    return Err(Error::ParentNotFound(*parent));
                }
            }
        }

        let version_id = row.version_id();
        if self.row_version_exists(object_id, &branch_name, version_id) {
            return Ok(version_id);
        }

        let previous_row = self.visible_row(object_id, branch_name);
        let applied = self.remember_row_version(object_id, branch_name, row.clone());
        tracing::trace!(?version_id, "row version applied");
        self.notify_row_object_update(
            io,
            &object_metadata,
            previous_row,
            applied.current_visible,
            row,
            applied.visible_changed,
        );

        Ok(version_id)
    }

    fn persist_row_version<H: Storage>(io: &mut H, table: &str, row: &StoredRowVersion) {
        if let Err(error) = io.append_history_region_rows(table, std::slice::from_ref(row)) {
            tracing::warn!(
                table,
                branch = row.branch,
                row_id = %row.row_id,
                %error,
                "failed to append row-region history row"
            );
        }
    }

    fn upsert_visible_row<H: Storage>(io: &mut H, table: &str, entry: &VisibleRowEntry) {
        if let Err(error) = io.upsert_visible_region_rows(table, std::slice::from_ref(entry)) {
            tracing::warn!(
                table,
                branch = entry.current_row.branch,
                row_id = %entry.current_row.row_id,
                %error,
                "failed to upsert row-region visible row"
            );
        }
    }

    fn notify_row_object_update<H: Storage>(
        &mut self,
        io: &mut H,
        metadata: &HashMap<String, String>,
        previous_row: Option<StoredRowVersion>,
        current_row: Option<StoredRowVersion>,
        history_row: StoredRowVersion,
        visible_changed: bool,
    ) {
        let Some(table) = metadata.get(MetadataKey::Table.as_str()).cloned() else {
            return;
        };

        Self::persist_row_version(io, &table, &history_row);

        let Some(current_row) = current_row else {
            return;
        };

        if visible_changed {
            let branch_name = BranchName::new(current_row.branch.clone());
            if let Some(entry) = self.visible_row_entry(history_row.row_id, branch_name) {
                Self::upsert_visible_row(io, &table, &entry);
            }
        } else {
            return;
        }

        let is_new_object = previous_row.is_none();
        self.visible_row_updates.push(VisibleRowUpdate {
            object_id: current_row.row_id,
            metadata: metadata.clone(),
            row: current_row,
            previous_row,
            is_new_object,
        });
    }

    fn remember_row_version(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        row: StoredRowVersion,
    ) -> RowBranchApply {
        self.row_branches
            .entry((object_id, branch_name))
            .or_default()
            .remember(row)
    }

    pub fn remember_remote_row_version(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        row: StoredRowVersion,
    ) {
        let _ = self.remember_row_version(object_id, branch_name, row);
    }

    pub fn add_row_version<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        row: StoredRowVersion,
    ) -> Result<CommitId, Error> {
        self.apply_row_version(io, object_id, branch_name.into(), row)
    }

    pub fn visible_row(
        &self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
    ) -> Option<StoredRowVersion> {
        let branch_name = branch_name.into();
        self.row_branch(object_id, &branch_name)
            .and_then(RowBranch::current_visible_row)
    }

    pub fn visible_row_entry(
        &self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
    ) -> Option<VisibleRowEntry> {
        let branch_name = branch_name.into();
        self.row_branch(object_id, &branch_name)
            .and_then(RowBranch::visible_entry)
    }

    pub fn row_version_exists(
        &self,
        object_id: ObjectId,
        branch_name: &BranchName,
        version_id: CommitId,
    ) -> bool {
        self.row_branch(object_id, branch_name)
            .is_some_and(|branch| branch.contains(version_id))
    }

    pub fn patch_row_version_state(
        &mut self,
        object_id: ObjectId,
        branch_name: &BranchName,
        version_id: CommitId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Option<VisibleRowUpdate> {
        let metadata = self.get(object_id)?.clone();
        let applied = self.row_branch_mut(object_id, branch_name)?.patch_state(
            version_id,
            state,
            confirmed_tier,
        )?;

        if !applied.visible_changed {
            return None;
        }

        let row = applied.current_visible?;
        let is_new_object = applied.previous_visible.is_none();
        Some(VisibleRowUpdate {
            object_id,
            metadata,
            row,
            previous_row: applied.previous_visible,
            is_new_object,
        })
    }

    pub fn row_versions_for_sync(
        &self,
    ) -> Vec<(ObjectId, HashMap<String, String>, StoredRowVersion)> {
        let mut rows = Vec::new();
        for ((object_id, _branch_name), branch) in &self.row_branches {
            let Some(metadata) = self.metadata_by_id.get(object_id) else {
                continue;
            };
            for row in branch.versions.values() {
                rows.push((*object_id, metadata.clone(), row.clone()));
            }
        }
        rows.sort_by_key(|(object_id, _, row)| {
            (
                *object_id,
                row.branch.clone(),
                row.updated_at,
                row.version_id(),
            )
        });
        rows
    }

    /// Get an object by id.
    pub fn get(&self, id: ObjectId) -> Option<&HashMap<String, String>> {
        self.metadata_by_id.get(&id)
    }

    /// Get an object, loading from storage if not in memory (lazy cold-start load).
    pub fn get_or_load(
        &mut self,
        id: ObjectId,
        storage: &dyn Storage,
        branches: &[String],
    ) -> Option<&HashMap<String, String>> {
        let _span = tracing::trace_span!("OM::get_or_load", %id).entered();
        if let std::collections::hash_map::Entry::Vacant(entry) = self.metadata_by_id.entry(id) {
            // Load metadata
            let metadata = match storage.load_metadata(id) {
                Ok(Some(m)) => m,
                Ok(None) => {
                    tracing::trace!(%id, "get_or_load: no metadata in storage");
                    return None;
                }
                Err(e) => {
                    tracing::warn!(%id, error = ?e, "get_or_load: storage error");
                    return None;
                }
            };

            entry.insert(metadata);
        }

        let (metadata, loaded_row_versions) = {
            let metadata = self.metadata_by_id.get(&id)?;
            (metadata.clone(), self.row_branches.len())
        };

        if Self::is_row_metadata(&metadata) {
            let Some(table) = metadata.get(MetadataKey::Table.as_str()).cloned() else {
                return self.metadata_by_id.get(&id);
            };

            for branch_name in branches {
                let bn = BranchName::new(branch_name);
                if self.row_branches.contains_key(&(id, bn)) {
                    continue;
                }

                let history_rows = match storage.scan_history_region(
                    &table,
                    bn.as_str(),
                    HistoryScan::Row { row_id: id },
                ) {
                    Ok(rows) => rows,
                    Err(err) => {
                        tracing::warn!(
                            %id,
                            branch = %bn,
                            error = ?err,
                            "get_or_load: failed to hydrate row history"
                        );
                        Vec::new()
                    }
                };

                for row in history_rows {
                    self.remember_remote_row_version(id, bn, row);
                }

                if let Ok(Some(row)) = storage.load_visible_region_row(&table, bn.as_str(), id) {
                    self.remember_remote_row_version(id, bn, row);
                }
            }
        }

        let metadata = self.metadata_by_id.get(&id)?;
        let row_branch_count = self
            .row_branches
            .keys()
            .filter(|(object_id, _)| *object_id == id)
            .count();
        let row_version_count: usize = self
            .row_branches
            .iter()
            .filter(|((object_id, _), _)| *object_id == id)
            .map(|(_, branch)| branch.versions.len())
            .sum();
        tracing::trace!(
            %id,
            row_branch_count,
            row_version_count,
            previous_row_branch_count = loaded_row_versions,
            "get_or_load: loaded from storage"
        );
        Some(metadata)
    }

    /// Get tip IDs for a branch.
    pub fn get_tip_ids(
        &self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
    ) -> Result<&SmolSet<[CommitId; 2]>, Error> {
        let branch_name = branch_name.into();

        if let Some(branch) = self.row_branch(object_id, &branch_name) {
            return Ok(&branch.tips);
        }

        if self.get(object_id).is_none() {
            return Err(Error::ObjectNotFound(object_id));
        }

        Err(Error::BranchNotFound(branch_name))
    }

    /// Receive metadata from a remote source (with specified ID).
    ///
    /// Unlike `create`, this uses the provided ObjectId rather than generating a new one.
    /// Used by sync layer to receive objects from peers.
    /// Persists to storage via Storage synchronously.
    pub fn receive_metadata<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
    ) {
        // Sync storage - returns immediately
        let _ = io.put_metadata(object_id, metadata.clone());
        self.metadata_by_id.entry(object_id).or_insert(metadata);
    }

    /// Take all pending visible row updates.
    pub fn take_visible_row_updates(&mut self) -> Vec<VisibleRowUpdate> {
        std::mem::take(&mut self.visible_row_updates)
    }

    /// Take one pending visible row update for the given concrete row version.
    pub fn take_visible_row_update_for(
        &mut self,
        object_id: ObjectId,
        branch_name: &BranchName,
        version_id: CommitId,
    ) -> Option<VisibleRowUpdate> {
        let index = self.visible_row_updates.iter().position(|update| {
            update.object_id == object_id
                && update.row.branch == branch_name.as_str()
                && update.row.version_id() == version_id
        })?;
        Some(self.visible_row_updates.remove(index))
    }

    // ========================================================================
    // No-op storage driver (for tests)
    // ========================================================================
    // ========================================================================
    // Memory profiling
    // ========================================================================

    /// Calculate memory usage breakdown for profiling.
    ///
    /// Returns a tuple: (row_objects, index_objects, subscriptions, other, total)
    pub fn memory_size(&self) -> (usize, usize, usize, usize, usize) {
        let mut row_objects = 0usize;
        let mut index_objects = 0usize;

        for metadata in self.metadata_by_id.values() {
            let obj_size = self.estimate_object_size(metadata);
            let is_index = metadata
                .get(crate::metadata::MetadataKey::Type.as_str())
                .is_some_and(|t| t == crate::metadata::ObjectType::Index.as_str());
            // Add HashMap entry overhead: ~48 bytes per entry
            let entry_overhead = std::mem::size_of::<ObjectId>() + 48;
            if is_index {
                index_objects += obj_size + entry_overhead;
            } else {
                row_objects += obj_size + entry_overhead;
            }
        }

        let subscriptions = 0usize;
        let other = self.visible_row_updates.len() * 192;

        let total = row_objects + index_objects + subscriptions + other;
        (row_objects, index_objects, subscriptions, other, total)
    }

    /// Estimate memory size of an object's metadata map.
    fn estimate_object_size(&self, metadata: &HashMap<String, String>) -> usize {
        let mut size = std::mem::size_of::<HashMap<String, String>>();

        for (k, v) in metadata {
            size += k.len() + v.len() + 48; // String overhead + HashMap entry
        }

        size
    }
}

#[cfg(test)]
mod tests;
