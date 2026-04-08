use crate::object::ObjectId;
use crate::storage::{Storage, StorageError, validate_index_value_size};

use super::encoding::decode_column;
use super::manager::{QueryError, QueryManager};
use super::types::{ColumnDescriptor, ColumnType, RowDescriptor, TableName, Value};

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
            let Ok(old_value) = decode_column(descriptor, old_data, col_idx) else {
                continue;
            };
            let Ok(new_value) = decode_column(descriptor, new_data, col_idx) else {
                continue;
            };

            if old_value == new_value {
                continue;
            }

            if old_value != Value::Null {
                Self::remove_column_index_values(
                    storage, table, col, branch, &old_value, object_id,
                )?;
            }
            if new_value != Value::Null {
                Self::insert_column_index_values(
                    storage, table, col, branch, &new_value, object_id,
                )?;
            }
        }

        Ok(())
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
}
