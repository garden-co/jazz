use std::collections::HashMap;

#[cfg(test)]
use smolset::SmolSet;
use web_time::{SystemTime, UNIX_EPOCH};

use crate::commit::CommitId;
use crate::metadata::MetadataKey;
use crate::object::{BranchName, ObjectId};
use crate::row_regions::{RowState, StoredRowVersion, VisibleRowEntry};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddRowVersionResult {
    pub version_id: CommitId,
    pub visible_update: Option<VisibleRowUpdate>,
}

/// Errors that can occur when managing objects and row versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ObjectNotFound(ObjectId),
    BranchNotFound(BranchName),
    ParentNotFound(CommitId),
    StorageError(StorageError),
}

#[derive(Debug, Clone)]
struct RowVersionApply {
    version_id: CommitId,
    previous_visible: Option<StoredRowVersion>,
    current_visible: Option<StoredRowVersion>,
    is_new_object: bool,
    visible_changed: bool,
}

/// Manages object metadata and row-history helpers.
#[derive(Debug, Clone, Default)]
pub struct ObjectManager {
    pub metadata_by_id: HashMap<ObjectId, HashMap<String, String>>,
    #[cfg(test)]
    row_branch_tips: HashMap<(ObjectId, BranchName), SmolSet<[CommitId; 2]>>,
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
        let metadata = metadata.unwrap_or_default();
        let id = ObjectId::new();
        let _ = io.put_metadata(id, metadata.clone());
        self.metadata_by_id.insert(id, metadata);
        id
    }

    /// Create a metadata entry with a specific ObjectId (for deterministic IDs).
    pub fn create_with_id<H: Storage>(
        &mut self,
        io: &mut H,
        id: ObjectId,
        metadata: Option<HashMap<String, String>>,
    ) -> ObjectId {
        let metadata = metadata.unwrap_or_default();
        let _ = io.put_metadata(id, metadata.clone());
        self.metadata_by_id.insert(id, metadata);
        id
    }

    fn is_row_metadata(metadata: &HashMap<String, String>) -> bool {
        metadata.contains_key(MetadataKey::Table.as_str())
    }

    fn table_from_metadata(metadata: &HashMap<String, String>) -> Result<String, Error> {
        metadata
            .get(MetadataKey::Table.as_str())
            .cloned()
            .ok_or(Error::StorageError(StorageError::IoError(
                "row metadata missing table".to_string(),
            )))
    }

    fn load_metadata_from_storage<H: Storage>(
        &self,
        io: &H,
        object_id: ObjectId,
    ) -> Result<HashMap<String, String>, Error> {
        io.load_metadata(object_id)
            .map_err(Error::StorageError)?
            .ok_or(Error::ObjectNotFound(object_id))
    }

    #[cfg(test)]
    fn cache_tips(&mut self, object_id: ObjectId, branch_name: BranchName, tips: &[CommitId]) {
        self.row_branch_tips
            .insert((object_id, branch_name), tips.iter().copied().collect());
    }

    fn load_previous_visible_entry<H: Storage>(
        &self,
        io: &H,
        table: &str,
        object_id: ObjectId,
        branch_name: &BranchName,
    ) -> Result<Option<VisibleRowEntry>, Error> {
        io.load_visible_region_entry(table, branch_name.as_str(), object_id)
            .map_err(Error::StorageError)
    }

    fn load_branch_history<H: Storage>(
        &self,
        io: &H,
        table: &str,
        object_id: ObjectId,
        branch_name: &BranchName,
    ) -> Result<Vec<StoredRowVersion>, Error> {
        Ok(io
            .scan_history_row_versions(table, object_id)
            .map_err(Error::StorageError)?
            .into_iter()
            .filter(|row| row.branch == branch_name.as_str())
            .collect())
    }

    fn visible_row_from_history(rows: &[StoredRowVersion]) -> Option<StoredRowVersion> {
        rows.iter()
            .filter(|row| row.state.is_visible())
            .max_by_key(|row| (row.updated_at, row.version_id()))
            .cloned()
    }

    fn rebuild_visible_entry_from_history<H: Storage>(
        &self,
        io: &H,
        table: &str,
        object_id: ObjectId,
        branch_name: &BranchName,
    ) -> Result<Option<VisibleRowEntry>, Error> {
        let history_rows = self.load_branch_history(io, table, object_id, branch_name)?;
        let Some(current_row) = history_rows
            .iter()
            .filter(|row| row.state.is_visible())
            .max_by_key(|row| (row.updated_at, row.version_id()))
            .cloned()
        else {
            return Ok(None);
        };

        Ok(Some(VisibleRowEntry::rebuild(current_row, &history_rows)))
    }

    fn latest_row_wins(candidate: &StoredRowVersion, current: &StoredRowVersion) -> bool {
        (candidate.updated_at, candidate.version_id()) > (current.updated_at, current.version_id())
    }

    fn winner_after_tier_upgrade<H: Storage>(
        &self,
        io: &H,
        table: &str,
        entry: &VisibleRowEntry,
        current_row: &StoredRowVersion,
        patched_row: &StoredRowVersion,
        required_tier: DurabilityTier,
    ) -> Result<Option<CommitId>, Error> {
        let patched_version_id = patched_row.version_id();
        if !patched_row.state.is_visible()
            || patched_row
                .confirmed_tier
                .is_none_or(|tier| tier < required_tier)
        {
            return Ok(entry.version_id_for_tier(required_tier));
        }

        if current_row.version_id() == patched_version_id
            || current_row
                .confirmed_tier
                .is_some_and(|tier| tier >= required_tier)
        {
            return Ok(Some(current_row.version_id()));
        }

        let Some(previous_winner_id) = entry.version_id_for_tier(required_tier) else {
            return Ok(Some(patched_version_id));
        };

        if previous_winner_id == patched_version_id {
            return Ok(Some(patched_version_id));
        }

        let Some(previous_winner) = io
            .load_history_row_version(table, patched_row.row_id, previous_winner_id)
            .map_err(Error::StorageError)?
        else {
            return Err(Error::StorageError(StorageError::IoError(format!(
                "missing tier winner {previous_winner_id:?} for row {}",
                patched_row.row_id
            ))));
        };

        if Self::latest_row_wins(patched_row, &previous_winner) {
            Ok(Some(patched_version_id))
        } else {
            Ok(Some(previous_winner_id))
        }
    }

    fn visible_entry_after_tier_upgrade<H: Storage>(
        &self,
        io: &H,
        table: &str,
        entry: VisibleRowEntry,
        patched_row: &StoredRowVersion,
    ) -> Result<VisibleRowEntry, Error> {
        let current_row = if entry.current_row.version_id() == patched_row.version_id() {
            patched_row.clone()
        } else {
            entry.current_row.clone()
        };
        let current_version_id = current_row.version_id();

        let worker_version_id = self
            .winner_after_tier_upgrade(
                io,
                table,
                &entry,
                &current_row,
                patched_row,
                DurabilityTier::Worker,
            )?
            .filter(|version_id| *version_id != current_version_id);
        let edge_version_id = self
            .winner_after_tier_upgrade(
                io,
                table,
                &entry,
                &current_row,
                patched_row,
                DurabilityTier::EdgeServer,
            )?
            .filter(|version_id| *version_id != current_version_id);
        let global_version_id = self
            .winner_after_tier_upgrade(
                io,
                table,
                &entry,
                &current_row,
                patched_row,
                DurabilityTier::GlobalServer,
            )?
            .filter(|version_id| *version_id != current_version_id);

        Ok(VisibleRowEntry {
            current_row,
            worker_version_id,
            edge_version_id,
            global_version_id,
        })
    }

    #[cfg(test)]
    fn next_tips_after_append<H: Storage>(
        &self,
        io: &H,
        table: &str,
        object_id: ObjectId,
        branch_name: &BranchName,
        row: &StoredRowVersion,
    ) -> Result<SmolSet<[CommitId; 2]>, Error> {
        let mut tips = self
            .row_branch_tips
            .get(&(object_id, *branch_name))
            .cloned()
            .unwrap_or_else(|| {
                io.scan_row_branch_tip_ids(table, branch_name.as_str(), object_id)
                    .unwrap_or_default()
                    .into_iter()
                    .collect()
            });

        for parent in &row.parents {
            tips.remove(parent);
        }
        tips.insert(row.version_id());
        Ok(tips)
    }

    fn apply_row_version_internal<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: BranchName,
        row: StoredRowVersion,
    ) -> Result<RowVersionApply, Error> {
        let object_metadata = self.load_metadata_from_storage(io, object_id)?;

        debug_assert!(
            Self::is_row_metadata(&object_metadata),
            "apply_row_version_internal should only be used for row-backed objects"
        );

        let table = Self::table_from_metadata(&object_metadata)?;

        for parent in &row.parents {
            if io
                .load_history_row_version(&table, object_id, *parent)
                .map_err(Error::StorageError)?
                .is_none()
            {
                return Err(Error::ParentNotFound(*parent));
            }
        }

        let mut branch_history = self.load_branch_history(io, &table, object_id, &branch_name)?;
        let version_id = row.version_id();
        let previous_visible = Self::visible_row_from_history(&branch_history);
        if branch_history
            .iter()
            .any(|existing| existing.version_id() == version_id)
        {
            return Ok(RowVersionApply {
                version_id,
                current_visible: previous_visible.clone(),
                previous_visible,
                is_new_object: false,
                visible_changed: false,
            });
        }

        io.append_history_region_rows(&table, std::slice::from_ref(&row))
            .map_err(Error::StorageError)?;
        branch_history.push(row.clone());
        let current_visible = Self::visible_row_from_history(&branch_history);

        if let Some(current_row) = current_visible.clone() {
            let entry = VisibleRowEntry::rebuild(current_row, &branch_history);
            io.upsert_visible_region_rows(&table, std::slice::from_ref(&entry))
                .map_err(Error::StorageError)?;
        }

        #[cfg(test)]
        {
            let tips = self.next_tips_after_append(io, &table, object_id, &branch_name, &row)?;
            self.row_branch_tips
                .insert((object_id, branch_name), tips.clone());
        }

        Ok(RowVersionApply {
            version_id,
            previous_visible: previous_visible.clone(),
            current_visible: current_visible.clone(),
            is_new_object: previous_visible.is_none(),
            visible_changed: previous_visible != current_visible,
        })
    }

    fn visible_update_from_applied<H: Storage>(
        &self,
        io: &H,
        object_id: ObjectId,
        applied: RowVersionApply,
    ) -> Result<Option<VisibleRowUpdate>, Error> {
        if !applied.visible_changed {
            return Ok(None);
        }

        let metadata = self.load_metadata_from_storage(io, object_id)?;
        let Some(current_visible) = applied.current_visible else {
            return Ok(None);
        };

        Ok(Some(VisibleRowUpdate {
            object_id,
            metadata,
            row: current_visible,
            previous_row: applied.previous_visible,
            is_new_object: applied.is_new_object,
        }))
    }

    pub fn add_row_version_with_update<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        row: StoredRowVersion,
    ) -> Result<AddRowVersionResult, Error> {
        let branch_name = branch_name.into();
        let applied = self.apply_row_version_internal(io, object_id, branch_name, row)?;
        let version_id = applied.version_id;
        let visible_update = self.visible_update_from_applied(io, object_id, applied)?;
        Ok(AddRowVersionResult {
            version_id,
            visible_update,
        })
    }

    pub fn add_row_version<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        row: StoredRowVersion,
    ) -> Result<CommitId, Error> {
        Ok(self
            .add_row_version_with_update(io, object_id, branch_name, row)?
            .version_id)
    }

    pub fn remember_remote_row_version_with_storage<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: BranchName,
        row: StoredRowVersion,
    ) -> Result<Option<VisibleRowUpdate>, Error> {
        let applied = self.apply_row_version_internal(io, object_id, branch_name, row)?;
        self.visible_update_from_applied(io, object_id, applied)
    }

    pub fn patch_row_version_state_with_storage<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: &BranchName,
        version_id: CommitId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Option<VisibleRowUpdate> {
        let metadata = self.load_metadata_from_storage(io, object_id).ok()?;
        let table = Self::table_from_metadata(&metadata).ok()?;
        let previous_entry = self
            .load_previous_visible_entry(io, &table, object_id, branch_name)
            .ok()?;
        let previous_visible = previous_entry
            .as_ref()
            .map(|entry| entry.current_row.clone());

        let mut patched_row = io
            .load_history_row_version(&table, object_id, version_id)
            .ok()
            .flatten()?;
        if patched_row.branch != branch_name.as_str() {
            return None;
        }

        if let Some(state) = state {
            patched_row.state = state;
        }
        patched_row.confirmed_tier = match (patched_row.confirmed_tier, confirmed_tier) {
            (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
            (Some(existing), None) => Some(existing),
            (None, incoming) => incoming,
        };

        io.append_history_region_rows(&table, std::slice::from_ref(&patched_row))
            .ok()?;

        let patched_entry = match previous_entry {
            Some(entry) if state.is_none() => self
                .visible_entry_after_tier_upgrade(io, &table, entry, &patched_row)
                .ok()?,
            _ => self
                .rebuild_visible_entry_from_history(io, &table, object_id, branch_name)
                .ok()??,
        };

        io.upsert_visible_region_rows(&table, std::slice::from_ref(&patched_entry))
            .ok()?;

        let current_visible = patched_entry.current_row;
        if previous_visible.as_ref() == Some(&current_visible) {
            return None;
        }

        Some(VisibleRowUpdate {
            object_id,
            metadata,
            row: current_visible,
            previous_row: previous_visible.clone(),
            is_new_object: previous_visible.is_none(),
        })
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
        _branches: &[String],
    ) -> Option<&HashMap<String, String>> {
        if let std::collections::hash_map::Entry::Vacant(entry) = self.metadata_by_id.entry(id) {
            let metadata = match storage.load_metadata(id) {
                Ok(Some(metadata)) => metadata,
                Ok(None) => return None,
                Err(error) => {
                    tracing::warn!(%id, error = ?error, "get_or_load: storage error");
                    return None;
                }
            };
            entry.insert(metadata);
        }

        let metadata = self.metadata_by_id.get(&id)?.clone();
        if Self::is_row_metadata(&metadata) {
            let Some(_table) = metadata.get(MetadataKey::Table.as_str()).cloned() else {
                return self.metadata_by_id.get(&id);
            };

            #[cfg(test)]
            for branch_name in _branches {
                let branch_name = BranchName::new(branch_name);
                if let Ok(tips) = storage.scan_row_branch_tip_ids(&_table, branch_name.as_str(), id)
                {
                    self.cache_tips(id, branch_name, &tips);
                }
            }
        }

        self.metadata_by_id.get(&id)
    }

    /// Get current DAG tips for a row branch from storage-backed history.
    #[cfg(test)]
    pub fn get_tip_ids(
        &self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
    ) -> Result<&SmolSet<[CommitId; 2]>, Error> {
        let branch_name = branch_name.into();

        if self.get(object_id).is_none() {
            return Err(Error::ObjectNotFound(object_id));
        }

        self.row_branch_tips
            .get(&(object_id, branch_name))
            .ok_or(Error::BranchNotFound(branch_name))
    }

    /// Receive metadata from a remote source (with specified ID).
    pub fn receive_metadata<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
    ) {
        let _ = io.put_metadata(object_id, metadata.clone());
        self.metadata_by_id.entry(object_id).or_insert(metadata);
    }

    /// Calculate memory usage breakdown for profiling.
    pub fn memory_size(&self) -> (usize, usize, usize, usize, usize) {
        let mut row_objects = 0usize;
        let mut index_objects = 0usize;

        for metadata in self.metadata_by_id.values() {
            let obj_size = self.estimate_object_size(metadata);
            let is_index = metadata
                .get(crate::metadata::MetadataKey::Type.as_str())
                .is_some_and(|t| t == crate::metadata::ObjectType::Index.as_str());
            let entry_overhead = std::mem::size_of::<ObjectId>() + 48;
            if is_index {
                index_objects += obj_size + entry_overhead;
            } else {
                row_objects += obj_size + entry_overhead;
            }
        }

        let subscriptions = 0usize;
        let other = 0;

        let total = row_objects + index_objects + subscriptions + other;
        (row_objects, index_objects, subscriptions, other, total)
    }

    /// Estimate memory size of an object's metadata map.
    fn estimate_object_size(&self, metadata: &HashMap<String, String>) -> usize {
        let mut size = std::mem::size_of::<HashMap<String, String>>();

        for (key, value) in metadata {
            size += key.len() + value.len() + 48;
        }

        size
    }
}

#[cfg(test)]
mod tests;
