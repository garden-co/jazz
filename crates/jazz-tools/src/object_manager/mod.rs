#[cfg(test)]
use std::collections::HashMap;

#[cfg(test)]
use smolset::SmolSet;

#[cfg(test)]
use crate::commit::CommitId;
#[cfg(test)]
use crate::object::{BranchName, ObjectId};
#[cfg(test)]
use crate::row_histories::{
    AddRowVersionResult, RowHistoryError, RowState, StoredRowVersion, VisibleRowUpdate,
    apply_row_version, patch_row_version_state,
};
#[cfg(test)]
use crate::storage::{IndexMutation, RowLocator, Storage, StorageError};
#[cfg(test)]
use crate::sync_manager::DurabilityTier;

/// Errors that can occur when managing objects and row versions.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ObjectNotFound(ObjectId),
    BranchNotFound(BranchName),
    ParentNotFound(CommitId),
    StorageError(StorageError),
}

#[cfg(test)]
impl From<RowHistoryError> for Error {
    fn from(error: RowHistoryError) -> Self {
        match error {
            RowHistoryError::ObjectNotFound(object_id) => Self::ObjectNotFound(object_id),
            RowHistoryError::ParentNotFound(version_id) => Self::ParentNotFound(version_id),
            RowHistoryError::StorageError(error) => Self::StorageError(error),
        }
    }
}

/// Transitional test-oriented metadata and tip mirrors.
#[derive(Debug, Clone, Default)]
pub struct ObjectManager {
    #[cfg(test)]
    pub metadata_by_id: HashMap<ObjectId, HashMap<String, String>>,
    #[cfg(test)]
    row_branch_tips: HashMap<(ObjectId, BranchName), SmolSet<[CommitId; 2]>>,
}

impl ObjectManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new metadata entry with optional metadata, returning its id.
    /// Persists to storage via Storage synchronously.
    #[cfg(test)]
    pub fn create<H: Storage>(
        &mut self,
        io: &mut H,
        metadata: Option<HashMap<String, String>>,
    ) -> ObjectId {
        let metadata = metadata.unwrap_or_default();
        let id = ObjectId::new();
        let _ = io.put_metadata(id, metadata.clone());
        #[cfg(test)]
        self.metadata_by_id.insert(id, metadata);
        id
    }

    /// Create a metadata entry with a specific ObjectId (for deterministic IDs).
    #[cfg(test)]
    pub fn create_with_id<H: Storage>(
        &mut self,
        io: &mut H,
        id: ObjectId,
        metadata: Option<HashMap<String, String>>,
    ) -> ObjectId {
        let metadata = metadata.unwrap_or_default();
        let _ = io.put_metadata(id, metadata.clone());
        #[cfg(test)]
        self.metadata_by_id.insert(id, metadata);
        id
    }

    #[cfg(test)]
    pub fn create_row_with_id<H: Storage>(
        &mut self,
        io: &mut H,
        id: ObjectId,
        row_locator: RowLocator,
    ) -> ObjectId {
        let _ = io.put_row_locator(id, Some(&row_locator));
        #[cfg(test)]
        self.metadata_by_id
            .insert(id, crate::storage::metadata_from_row_locator(&row_locator));
        id
    }

    #[cfg(test)]
    pub fn cache_metadata_for_tests(
        &mut self,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
    ) {
        self.metadata_by_id.insert(object_id, metadata);
    }

    #[cfg(test)]
    pub(crate) fn refresh_row_branch_tips_for_tests<H: Storage>(
        &mut self,
        io: &H,
        table: &str,
        object_id: ObjectId,
        branch_name: BranchName,
    ) {
        if let Ok(tips) = io.scan_row_branch_tip_ids(table, branch_name.as_str(), object_id) {
            self.row_branch_tips
                .insert((object_id, branch_name), tips.into_iter().collect());
        }
    }

    #[cfg(test)]
    pub fn add_row_version_with_update<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        row: StoredRowVersion,
    ) -> Result<AddRowVersionResult, Error> {
        self.add_row_version_with_update_and_indices(io, object_id, branch_name, row, &[])
    }

    #[cfg(test)]
    pub fn add_row_version_with_update_and_indices<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        row: StoredRowVersion,
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<AddRowVersionResult, Error> {
        let branch_name = branch_name.into();
        let applied = apply_row_version(io, object_id, &branch_name, row, index_mutations)
            .map_err(Error::from)?;

        #[cfg(test)]
        self.refresh_row_branch_tips_for_tests(
            io,
            applied.row_locator.table.as_str(),
            object_id,
            branch_name,
        );

        Ok(applied)
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub fn remember_remote_row_version_with_storage<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: BranchName,
        row: StoredRowVersion,
    ) -> Result<Option<VisibleRowUpdate>, Error> {
        let applied =
            apply_row_version(io, object_id, &branch_name, row, &[]).map_err(Error::from)?;

        #[cfg(test)]
        self.refresh_row_branch_tips_for_tests(
            io,
            applied.row_locator.table.as_str(),
            object_id,
            branch_name,
        );

        Ok(applied.visible_update)
    }

    #[cfg(test)]
    pub fn patch_row_version_state_with_storage<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: &BranchName,
        version_id: CommitId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Option<VisibleRowUpdate> {
        patch_row_version_state(
            io,
            object_id,
            branch_name,
            version_id,
            state,
            confirmed_tier,
        )
        .ok()
        .flatten()
    }

    /// Get object metadata by id.
    #[cfg(test)]
    pub fn get(&self, id: ObjectId) -> Option<&HashMap<String, String>> {
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
    #[cfg(test)]
    pub fn receive_metadata<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
    ) {
        let _ = io.put_metadata(object_id, metadata.clone());
        #[cfg(test)]
        self.metadata_by_id.entry(object_id).or_insert(metadata);
    }

    /// Calculate memory usage breakdown for profiling.
    #[cfg(test)]
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
    #[cfg(test)]
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
