use crate::object::{BranchName, ObjectId};
use crate::storage::{Storage, StorageError, validate_index_value_size};

use super::encoding::decode_column;
use super::manager::{QueryError, QueryManager};
use super::types::{
    ColumnDescriptor, ColumnType, ComposedBranchName, RowDescriptor, TableName, Value,
};

impl QueryManager {
    fn map_index_storage_error(error: StorageError) -> QueryError {
        match error {
            StorageError::IndexKeyTooLarge {
                table,
                column,
                branch,
                key_bytes,
                max_key_bytes,
            } => QueryError::IndexValueTooLarge {
                table: TableName::new(table),
                column,
                branch,
                key_bytes,
                max_key_bytes,
            },
            other => QueryError::IndexError(other.to_string()),
        }
    }

    fn expand_index_values(column: &ColumnDescriptor, value: &Value) -> Vec<Value> {
        let mut values = vec![value.clone()];
        if column.references.is_some()
            && matches!(
                &column.column_type,
                ColumnType::Array { element: element_type } if matches!(element_type.as_ref(), ColumnType::Uuid)
            )
            && let Value::Array(elements) = value
        {
            values.extend(
                elements
                    .iter()
                    .filter(|element| matches!(element, Value::Uuid(_)))
                    .cloned(),
            );
        }
        values
    }

    fn validate_column_index_values(
        table: &str,
        column: &ColumnDescriptor,
        branch: &str,
        value: &Value,
    ) -> Result<(), QueryError> {
        for index_value in Self::expand_index_values(column, value) {
            validate_index_value_size(table, column.name.as_str(), branch, &index_value)
                .map_err(Self::map_index_storage_error)?;
        }
        Ok(())
    }

    pub(super) fn validate_write_index_values_on_branch(
        table: &str,
        branch: &str,
        values: &[Value],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        for (column, value) in descriptor.columns.iter().zip(values.iter()) {
            if *value != Value::Null {
                Self::validate_column_index_values(table, column, branch, value)?;
            }
        }
        Ok(())
    }

    fn insert_column_index_values(
        storage: &mut dyn Storage,
        table: &str,
        column: &ColumnDescriptor,
        branch: &str,
        value: &Value,
        object_id: ObjectId,
    ) -> Result<(), QueryError> {
        for index_value in Self::expand_index_values(column, value) {
            storage
                .index_insert(table, column.name.as_str(), branch, &index_value, object_id)
                .map_err(Self::map_index_storage_error)?;
        }
        Ok(())
    }

    fn register_table_prefix_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
    ) -> Result<(), QueryError> {
        let branch_name = BranchName::new(branch.to_string());
        let Some(composed_branch) = ComposedBranchName::parse(&branch_name) else {
            return Ok(());
        };
        storage
            .register_table_prefix_branch(
                table,
                &composed_branch.prefix().branch_prefix(),
                &branch_name,
            )
            .map_err(Self::map_index_storage_error)
    }

    fn remove_column_index_values(
        storage: &mut dyn Storage,
        table: &str,
        column: &ColumnDescriptor,
        branch: &str,
        value: &Value,
        object_id: ObjectId,
    ) -> Result<(), QueryError> {
        for index_value in Self::expand_index_values(column, value) {
            storage
                .index_remove(table, column.name.as_str(), branch, &index_value, object_id)
                .map_err(Self::map_index_storage_error)?;
        }
        Ok(())
    }

    /// Update indices when a row is inserted on a specific branch.
    pub(super) fn update_indices_for_insert_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        Self::register_table_prefix_branch(storage, table, branch)?;

        // Update "_id" index
        storage
            .index_insert(table, "_id", branch, &Value::Uuid(object_id), object_id)
            .map_err(Self::map_index_storage_error)?;

        // Update column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Ok(value) = decode_column(descriptor, data, col_idx)
                && value != Value::Null
            {
                Self::insert_column_index_values(storage, table, col, branch, &value, object_id)?;
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
        Self::register_table_prefix_branch(storage, table, branch)?;

        // "_id" index doesn't change on update

        // Update column indices (remove old value, add new value)
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            // Remove old value
            if let Ok(old_value) = decode_column(descriptor, old_data, col_idx)
                && old_value != Value::Null
            {
                Self::remove_column_index_values(
                    storage, table, col, branch, &old_value, object_id,
                )?;
            }
            // Add new value
            if let Ok(new_value) = decode_column(descriptor, new_data, col_idx)
                && new_value != Value::Null
            {
                Self::insert_column_index_values(
                    storage, table, col, branch, &new_value, object_id,
                )?;
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
        Self::register_table_prefix_branch(storage, table, branch)?;

        // Remove from "_id" index
        storage
            .index_remove(table, "_id", branch, &Value::Uuid(object_id), object_id)
            .map_err(Self::map_index_storage_error)?;

        // Remove from all column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Ok(value) = decode_column(descriptor, old_data, col_idx)
                && value != Value::Null
            {
                Self::remove_column_index_values(storage, table, col, branch, &value, object_id)?;
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
            .map_err(Self::map_index_storage_error)?;

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
        Self::register_table_prefix_branch(storage, table, branch)?;

        // Remove from "_id" index (may not be present if already soft-deleted)
        storage
            .index_remove(table, "_id", branch, &Value::Uuid(object_id), object_id)
            .map_err(Self::map_index_storage_error)?;

        // Remove from all column indices (if we have old data)
        if let Some(data) = old_data {
            for (col_idx, col) in descriptor.columns.iter().enumerate() {
                if let Ok(value) = decode_column(descriptor, data, col_idx)
                    && value != Value::Null
                {
                    Self::remove_column_index_values(
                        storage, table, col, branch, &value, object_id,
                    )?;
                }
            }
        }

        // Remove from "_id_deleted" index (handles soft→hard upgrade)
        storage
            .index_remove(
                table,
                "_id_deleted",
                branch,
                &Value::Uuid(object_id),
                object_id,
            )
            .map_err(Self::map_index_storage_error)?;

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
        Self::register_table_prefix_branch(storage, table, branch)?;

        // Remove from "_id_deleted" index
        storage
            .index_remove(
                table,
                "_id_deleted",
                branch,
                &Value::Uuid(object_id),
                object_id,
            )
            .map_err(Self::map_index_storage_error)?;

        // Add to "_id" index
        storage
            .index_insert(table, "_id", branch, &Value::Uuid(object_id), object_id)
            .map_err(Self::map_index_storage_error)?;

        // Add to all column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Ok(value) = decode_column(descriptor, new_data, col_idx)
                && value != Value::Null
            {
                Self::insert_column_index_values(storage, table, col, branch, &value, object_id)?;
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
