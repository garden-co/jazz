use crate::object::ObjectId;
use crate::row_histories::{RowState, VisibleRowEntry};
use crate::storage::{IndexMutation, Storage, StorageError, validate_index_value_size};

use crate::row_format::CompiledRowLayout;

use super::encoding::decode_column;
use super::manager::{QueryError, QueryManager};
use super::types::{ColumnDescriptor, ColumnName, ColumnType, RowDescriptor, TableName, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IndexUpdateError {
    pub column: String,
    pub source: QueryError,
}

impl std::fmt::Display for IndexUpdateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "index update failed for column {}: {}",
            self.column, self.source
        )
    }
}

impl std::error::Error for IndexUpdateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

pub(super) struct BranchIndexTarget<'a> {
    pub table: &'a str,
    pub branch: &'a str,
    pub descriptor: &'a RowDescriptor,
    pub indexed_columns: Option<&'a [ColumnName]>,
}

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
        indexed_columns: Option<&[ColumnName]>,
    ) -> Result<(), QueryError> {
        for (column, value) in descriptor.columns.iter().zip(values.iter()) {
            if !Self::should_index_column(indexed_columns, column) {
                continue;
            }
            if *value != Value::Null {
                Self::validate_column_index_values(table, column, branch, value)?;
            }
        }
        Ok(())
    }

    fn should_index_column(
        indexed_columns: Option<&[ColumnName]>,
        column: &ColumnDescriptor,
    ) -> bool {
        indexed_columns.is_none_or(|columns| columns.contains(&column.name))
    }

    fn push_insert_column_index_values<'a>(
        mutations: &mut Vec<IndexMutation<'a>>,
        table: &'a str,
        column: &'a ColumnDescriptor,
        branch: &'a str,
        value: &Value,
        object_id: ObjectId,
    ) {
        for index_value in Self::expand_index_values(column, value) {
            mutations.push(IndexMutation::Insert {
                table,
                column: column.name.as_str(),
                branch,
                value: index_value,
                row_id: object_id,
            });
        }
    }

    fn push_remove_column_index_values<'a>(
        mutations: &mut Vec<IndexMutation<'a>>,
        table: &'a str,
        column: &'a ColumnDescriptor,
        branch: &'a str,
        value: &Value,
        object_id: ObjectId,
    ) {
        for index_value in Self::expand_index_values(column, value) {
            mutations.push(IndexMutation::Remove {
                table,
                column: column.name.as_str(),
                branch,
                value: index_value,
                row_id: object_id,
            });
        }
    }

    pub(super) fn index_mutations_for_insert_on_branch<'a>(
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &'a RowDescriptor,
        indexed_columns: Option<&'a [ColumnName]>,
    ) -> Vec<IndexMutation<'a>> {
        let layout = crate::row_format::compiled_row_layout(descriptor);
        Self::index_mutations_for_insert_on_branch_with_layout(
            table,
            branch,
            object_id,
            data,
            descriptor,
            indexed_columns,
            &layout,
        )
    }

    pub(super) fn index_mutations_for_insert_on_branch_with_layout<'a>(
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &'a RowDescriptor,
        indexed_columns: Option<&'a [ColumnName]>,
        layout: &CompiledRowLayout,
    ) -> Vec<IndexMutation<'a>> {
        let mut mutations = vec![IndexMutation::Insert {
            table,
            column: "_id",
            branch,
            value: Value::Uuid(object_id),
            row_id: object_id,
        }];

        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if !Self::should_index_column(indexed_columns, col) {
                continue;
            }
            if let Ok(value) =
                crate::row_format::decode_column_with_layout(descriptor, layout, data, col_idx)
                && value != Value::Null
            {
                Self::push_insert_column_index_values(
                    &mut mutations,
                    table,
                    col,
                    branch,
                    &value,
                    object_id,
                );
            }
        }

        mutations
    }

    pub(super) fn index_mutations_for_update_on_branch<'a>(
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        old_data: &[u8],
        new_data: &[u8],
        descriptor: &'a RowDescriptor,
        indexed_columns: Option<&'a [ColumnName]>,
    ) -> Vec<IndexMutation<'a>> {
        let mut mutations = Vec::new();

        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if !Self::should_index_column(indexed_columns, col) {
                continue;
            }
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
                Self::push_remove_column_index_values(
                    &mut mutations,
                    table,
                    col,
                    branch,
                    &old_value,
                    object_id,
                );
            }
            if new_value != Value::Null {
                Self::push_insert_column_index_values(
                    &mut mutations,
                    table,
                    col,
                    branch,
                    &new_value,
                    object_id,
                );
            }
        }

        mutations
    }

    pub(super) fn index_mutations_for_soft_delete_on_branch<'a>(
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        old_data: &[u8],
        descriptor: &'a RowDescriptor,
        indexed_columns: Option<&'a [ColumnName]>,
    ) -> Vec<IndexMutation<'a>> {
        let mut mutations = vec![IndexMutation::Remove {
            table,
            column: "_id",
            branch,
            value: Value::Uuid(object_id),
            row_id: object_id,
        }];

        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if !Self::should_index_column(indexed_columns, col) {
                continue;
            }
            if let Ok(value) = decode_column(descriptor, old_data, col_idx)
                && value != Value::Null
            {
                Self::push_remove_column_index_values(
                    &mut mutations,
                    table,
                    col,
                    branch,
                    &value,
                    object_id,
                );
            }
        }

        mutations.push(IndexMutation::Insert {
            table,
            column: "_id_deleted",
            branch,
            value: Value::Uuid(object_id),
            row_id: object_id,
        });

        mutations
    }

    pub(super) fn index_mutations_for_hard_delete_on_branch<'a>(
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        old_data: Option<&[u8]>,
        descriptor: &'a RowDescriptor,
        indexed_columns: Option<&'a [ColumnName]>,
    ) -> Vec<IndexMutation<'a>> {
        let mut mutations = vec![IndexMutation::Remove {
            table,
            column: "_id",
            branch,
            value: Value::Uuid(object_id),
            row_id: object_id,
        }];

        if let Some(data) = old_data {
            for (col_idx, col) in descriptor.columns.iter().enumerate() {
                if !Self::should_index_column(indexed_columns, col) {
                    continue;
                }
                if let Ok(value) = decode_column(descriptor, data, col_idx)
                    && value != Value::Null
                {
                    Self::push_remove_column_index_values(
                        &mut mutations,
                        table,
                        col,
                        branch,
                        &value,
                        object_id,
                    );
                }
            }
        }

        mutations.push(IndexMutation::Remove {
            table,
            column: "_id_deleted",
            branch,
            value: Value::Uuid(object_id),
            row_id: object_id,
        });

        mutations
    }

    pub(super) fn index_mutations_for_undelete_on_branch<'a>(
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        new_data: &[u8],
        descriptor: &'a RowDescriptor,
        indexed_columns: Option<&'a [ColumnName]>,
    ) -> Vec<IndexMutation<'a>> {
        let mut mutations = vec![
            IndexMutation::Remove {
                table,
                column: "_id_deleted",
                branch,
                value: Value::Uuid(object_id),
                row_id: object_id,
            },
            IndexMutation::Insert {
                table,
                column: "_id",
                branch,
                value: Value::Uuid(object_id),
                row_id: object_id,
            },
        ];

        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if !Self::should_index_column(indexed_columns, col) {
                continue;
            }
            if let Ok(value) = decode_column(descriptor, new_data, col_idx)
                && value != Value::Null
            {
                Self::push_insert_column_index_values(
                    &mut mutations,
                    table,
                    col,
                    branch,
                    &value,
                    object_id,
                );
            }
        }

        mutations
    }

    /// Update indices when a row is inserted on a specific branch.
    pub(super) fn update_indices_for_insert_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &RowDescriptor,
        indexed_columns: Option<&[ColumnName]>,
    ) -> Result<(), IndexUpdateError> {
        let mutations = Self::index_mutations_for_insert_on_branch(
            table,
            branch,
            object_id,
            data,
            descriptor,
            indexed_columns,
        );
        for mutation in &mutations {
            if let Err(error) = storage.apply_index_mutations(std::slice::from_ref(mutation)) {
                let column = match mutation {
                    IndexMutation::Insert { column, .. } | IndexMutation::Remove { column, .. } => {
                        (*column).to_string()
                    }
                };
                return Err(IndexUpdateError {
                    column,
                    source: Self::map_index_storage_error(error),
                });
            }
        }
        Ok(())
    }

    /// Update indices when a row is updated on a specific branch.
    pub(super) fn update_indices_for_update_on_branch(
        storage: &mut dyn Storage,
        target: BranchIndexTarget<'_>,
        object_id: ObjectId,
        old_data: &[u8],
        new_data: &[u8],
    ) -> Result<(), QueryError> {
        let mutations = Self::index_mutations_for_update_on_branch(
            target.table,
            target.branch,
            object_id,
            old_data,
            new_data,
            target.descriptor,
            target.indexed_columns,
        );
        storage
            .apply_index_mutations(&mutations)
            .map_err(Self::map_index_storage_error)
    }

    /// Update indices for soft delete on a specific branch.
    pub(super) fn update_indices_for_soft_delete_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        old_data: &[u8],
        descriptor: &RowDescriptor,
        indexed_columns: Option<&[ColumnName]>,
    ) -> Result<(), QueryError> {
        let mutations = Self::index_mutations_for_soft_delete_on_branch(
            table,
            branch,
            object_id,
            old_data,
            descriptor,
            indexed_columns,
        );
        storage
            .apply_index_mutations(&mutations)
            .map_err(Self::map_index_storage_error)
    }

    /// Update indices for hard delete on a specific branch.
    pub(super) fn update_indices_for_hard_delete_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        old_data: Option<&[u8]>,
        descriptor: &RowDescriptor,
        indexed_columns: Option<&[ColumnName]>,
    ) -> Result<(), QueryError> {
        let mutations = Self::index_mutations_for_hard_delete_on_branch(
            table,
            branch,
            object_id,
            old_data,
            descriptor,
            indexed_columns,
        );
        storage
            .apply_index_mutations(&mutations)
            .map_err(Self::map_index_storage_error)
    }

    /// Update indices for undelete on a specific branch.
    pub(super) fn update_indices_for_undelete_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        new_data: &[u8],
        descriptor: &RowDescriptor,
        indexed_columns: Option<&[ColumnName]>,
    ) -> Result<(), QueryError> {
        let mutations = Self::index_mutations_for_undelete_on_branch(
            table,
            branch,
            object_id,
            new_data,
            descriptor,
            indexed_columns,
        );
        storage
            .apply_index_mutations(&mutations)
            .map_err(Self::map_index_storage_error)
    }

    pub(crate) fn retract_local_rejected_row(
        &mut self,
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        row_data: &[u8],
        was_visible: bool,
    ) {
        let table_name = TableName::new(table);
        let Some(table_schema) = self.schema.get(&table_name) else {
            if was_visible {
                self.pending_local_row_batches.remove(&row_id);
                self.mark_subscriptions_dirty_local(table);
                self.mark_local_row_deleted_in_subscriptions(table, row_id);
            } else {
                self.clear_local_pending_row_overlay(table, row_id);
            }
            return;
        };

        if let Err(error) = Self::update_indices_for_hard_delete_on_branch(
            storage,
            table,
            branch,
            row_id,
            Some(row_data),
            &table_schema.columns,
            table_schema.indexed_columns.as_deref(),
        ) {
            tracing::warn!(
                table,
                branch,
                object_id = %row_id,
                %error,
                "failed to retract local rejected row indices"
            );
        }

        if was_visible {
            self.pending_local_row_batches.remove(&row_id);
            self.mark_subscriptions_dirty_local(table);
            self.mark_local_row_deleted_in_subscriptions(table, row_id);
        } else {
            self.clear_local_pending_row_overlay(table, row_id);
        }
    }

    pub(crate) fn restore_local_rejected_delete_row(
        &mut self,
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        restored_data: &[u8],
    ) {
        if let Ok(history_rows) = storage.scan_history_row_batches(table, row_id)
            && let Some(mut restored_row) = history_rows
                .into_iter()
                .filter(|row| {
                    row.branch.as_str() == branch
                        && !matches!(row.state, RowState::Rejected)
                        && row.delete_kind.is_none()
                })
                .max_by_key(|row| row.updated_at)
        {
            restored_row.state = RowState::VisibleDirect;
            let visible_entry = VisibleRowEntry::new(restored_row.clone());
            if let Err(error) =
                storage.apply_row_mutation(table, &[restored_row], &[visible_entry], &[])
            {
                tracing::warn!(
                    table,
                    branch,
                    object_id = %row_id,
                    %error,
                    "failed to restore rejected delete visible row"
                );
            }
        }

        let table_name = TableName::new(table);
        if let Some(table_schema) = self.schema.get(&table_name)
            && let Err(error) = Self::update_indices_for_undelete_on_branch(
                storage,
                table,
                branch,
                row_id,
                restored_data,
                &table_schema.columns,
                table_schema.indexed_columns.as_deref(),
            )
        {
            tracing::warn!(
                table,
                branch,
                object_id = %row_id,
                %error,
                "failed to restore rejected delete indices"
            );
        }

        self.pending_local_row_batches.remove(&row_id);
        self.mark_subscriptions_dirty_local(table);
        self.mark_local_row_updated_in_subscriptions(table, row_id);
    }
}
