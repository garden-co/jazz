use crate::object::ObjectId;
use crate::storage::Storage;

use super::encoding::decode_column;
use super::manager::{QueryError, QueryManager};
use super::types::{RowDescriptor, Value};

impl QueryManager {
    /// Update indices when a row is inserted on a specific branch.
    pub(super) fn update_indices_for_insert_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Update "_id" index
        storage
            .index_insert(table, "_id", branch, &Value::Uuid(object_id), object_id)
            .map_err(|e| QueryError::IndexError(format!("{:?}", e)))?;

        // Update column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Ok(value) = decode_column(descriptor, data, col_idx)
                && value != Value::Null
            {
                storage
                    .index_insert(table, col.name.as_str(), branch, &value, object_id)
                    .map_err(|e| QueryError::IndexError(format!("{:?}", e)))?;
            }
        }

        Ok(())
    }

    /// Update indices when a row is inserted (on the default branch).
    pub(super) fn update_indices_for_insert(
        &self,
        storage: &mut dyn Storage,
        table: &str,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        Self::update_indices_for_insert_on_branch(
            storage,
            table,
            &self.current_branch(),
            object_id,
            data,
            descriptor,
        )
    }

    /// Update indices when a row is updated on a specific branch.
    pub(super) fn update_indices_for_update_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        old_data: &[u8],
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // "_id" index doesn't change on update

        // Update column indices (remove old value, add new value)
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            // Remove old value
            if let Ok(old_value) = decode_column(descriptor, old_data, col_idx)
                && old_value != Value::Null
            {
                let _ =
                    storage.index_remove(table, col.name.as_str(), branch, &old_value, object_id);
            }
            // Add new value
            if let Ok(new_value) = decode_column(descriptor, new_data, col_idx)
                && new_value != Value::Null
            {
                let _ =
                    storage.index_insert(table, col.name.as_str(), branch, &new_value, object_id);
            }
        }

        Ok(())
    }

    /// Update indices when a row is updated (on the default branch).
    pub(super) fn update_indices_for_update(
        &self,
        storage: &mut dyn Storage,
        table: &str,
        object_id: ObjectId,
        old_data: &[u8],
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        Self::update_indices_for_update_on_branch(
            storage,
            table,
            &self.current_branch(),
            object_id,
            old_data,
            new_data,
            descriptor,
        )
    }

    /// Update indices for soft delete on a specific branch.
    pub(super) fn update_indices_for_soft_delete_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        old_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Remove from "_id" index
        let _ = storage.index_remove(table, "_id", branch, &Value::Uuid(object_id), object_id);

        // Remove from all column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Ok(value) = decode_column(descriptor, old_data, col_idx)
                && value != Value::Null
            {
                let _ = storage.index_remove(table, col.name.as_str(), branch, &value, object_id);
            }
        }

        // Add to "_id_deleted" index
        storage
            .index_insert(
                table,
                "_id_deleted",
                branch,
                &Value::Uuid(object_id),
                object_id,
            )
            .map_err(|e| QueryError::IndexError(format!("{:?}", e)))?;

        Ok(())
    }

    /// Update indices for soft delete (on the default branch).
    pub(super) fn update_indices_for_soft_delete(
        &self,
        storage: &mut dyn Storage,
        table: &str,
        object_id: ObjectId,
        old_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        Self::update_indices_for_soft_delete_on_branch(
            storage,
            table,
            &self.current_branch(),
            object_id,
            old_data,
            descriptor,
        )
    }

    /// Update indices for hard delete on a specific branch.
    pub(super) fn update_indices_for_hard_delete_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        old_data: Option<&[u8]>,
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Remove from "_id" index (may not be present if already soft-deleted)
        let _ = storage.index_remove(table, "_id", branch, &Value::Uuid(object_id), object_id);

        // Remove from all column indices (if we have old data)
        if let Some(data) = old_data {
            for (col_idx, col) in descriptor.columns.iter().enumerate() {
                if let Ok(value) = decode_column(descriptor, data, col_idx)
                    && value != Value::Null
                {
                    let _ =
                        storage.index_remove(table, col.name.as_str(), branch, &value, object_id);
                }
            }
        }

        // Remove from "_id_deleted" index (handles soft→hard upgrade)
        let _ = storage.index_remove(
            table,
            "_id_deleted",
            branch,
            &Value::Uuid(object_id),
            object_id,
        );

        Ok(())
    }

    /// Update indices for hard delete (on the default branch).
    pub(super) fn update_indices_for_hard_delete(
        &self,
        storage: &mut dyn Storage,
        table: &str,
        object_id: ObjectId,
        old_data: Option<&[u8]>,
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        Self::update_indices_for_hard_delete_on_branch(
            storage,
            table,
            &self.current_branch(),
            object_id,
            old_data,
            descriptor,
        )
    }

    /// Update indices for undelete on a specific branch.
    pub(super) fn update_indices_for_undelete_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Remove from "_id_deleted" index
        let _ = storage.index_remove(
            table,
            "_id_deleted",
            branch,
            &Value::Uuid(object_id),
            object_id,
        );

        // Add to "_id" index
        storage
            .index_insert(table, "_id", branch, &Value::Uuid(object_id), object_id)
            .map_err(|e| QueryError::IndexError(format!("{:?}", e)))?;

        // Add to all column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Ok(value) = decode_column(descriptor, new_data, col_idx)
                && value != Value::Null
            {
                let _ = storage.index_insert(table, col.name.as_str(), branch, &value, object_id);
            }
        }

        Ok(())
    }

    /// Update indices for undelete (on the default branch).
    pub(super) fn update_indices_for_undelete(
        &self,
        storage: &mut dyn Storage,
        table: &str,
        object_id: ObjectId,
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        Self::update_indices_for_undelete_on_branch(
            storage,
            table,
            &self.current_branch(),
            object_id,
            new_data,
            descriptor,
        )
    }
}
