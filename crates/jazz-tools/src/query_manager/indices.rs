use std::collections::HashSet;
use std::ops::Bound;

use crate::metadata::RowProvenance;
use crate::object::ObjectId;
use crate::row_histories::{RowState, VisibleRowEntry};
use crate::storage::{IndexMutation, Storage, StorageError, validate_index_value_size};

use crate::row_format::CompiledRowLayout;

use super::encoding::decode_column;
use super::index::composite_index_value;
use super::magic_columns::{CREATED_AT_COLUMN_NAME, UPDATED_AT_COLUMN_NAME};
use super::manager::{QueryError, QueryManager};
use super::types::{
    ColumnDescriptor, ColumnName, ColumnType, ComposedBranchName, CompositeIndex, RowDescriptor,
    Schema, TableName, Value,
};

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
    pub composite_indexes: &'a [CompositeIndex],
}

const COMPOSITE_INDEX_BUILD_STATE_TABLE: &str = "__jazz_composite_index_build_state";
const COMPOSITE_INDEX_BACKFILL_CHUNK_SIZE: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompositeIndexBackfillBudget {
    pub rows_per_index: usize,
    pub indexes: usize,
}

impl Default for CompositeIndexBackfillBudget {
    fn default() -> Self {
        Self {
            rows_per_index: COMPOSITE_INDEX_BACKFILL_CHUNK_SIZE,
            indexes: usize::MAX,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompositeIndexBuildState {
    Building { cursor: Option<ObjectId> },
    Ready,
}

fn composite_index_build_key(
    table: &str,
    branch: &str,
    index_name: &str,
    index_signature: &str,
) -> String {
    format!(
        "t{}:{}:b{}:{}:i{}:{}:s{}:{}",
        table.len(),
        table,
        branch.len(),
        branch,
        index_name.len(),
        index_name,
        index_signature.len(),
        index_signature
    )
}

fn encode_composite_index_build_state(state: CompositeIndexBuildState) -> Vec<u8> {
    match state {
        CompositeIndexBuildState::Ready => vec![1],
        CompositeIndexBuildState::Building { cursor } => {
            let mut out = vec![0, u8::from(cursor.is_some())];
            if let Some(cursor) = cursor {
                out.extend_from_slice(cursor.uuid().as_bytes());
            }
            out
        }
    }
}

fn decode_composite_index_build_state(bytes: &[u8]) -> Option<CompositeIndexBuildState> {
    match bytes {
        [1] => Some(CompositeIndexBuildState::Ready),
        [0, 0] => Some(CompositeIndexBuildState::Building { cursor: None }),
        [0, 1, cursor @ ..] if cursor.len() == 16 => {
            let uuid = uuid::Uuid::from_slice(cursor).ok()?;
            Some(CompositeIndexBuildState::Building {
                cursor: Some(ObjectId::from_uuid(uuid)),
            })
        }
        _ => None,
    }
}

fn read_composite_index_build_key_part<'a>(
    raw: &'a str,
    offset: &mut usize,
    tag: u8,
) -> Option<&'a str> {
    let bytes = raw.as_bytes();
    if bytes.get(*offset).copied()? != tag {
        return None;
    }
    *offset += 1;
    let len_start = *offset;
    while bytes.get(*offset).copied()? != b':' {
        *offset += 1;
    }
    let len = raw.get(len_start..*offset)?.parse::<usize>().ok()?;
    *offset += 1;
    let value_start = *offset;
    let value_end = value_start.checked_add(len)?;
    let value = raw.get(value_start..value_end)?;
    *offset = value_end;
    if *offset < raw.len() {
        if bytes.get(*offset).copied()? != b':' {
            return None;
        }
        *offset += 1;
    }
    Some(value)
}

fn parse_composite_index_build_key(raw: &str) -> Option<(String, String, String, String)> {
    let mut offset = 0;
    let table = read_composite_index_build_key_part(raw, &mut offset, b't')?.to_string();
    let branch = read_composite_index_build_key_part(raw, &mut offset, b'b')?.to_string();
    let index = read_composite_index_build_key_part(raw, &mut offset, b'i')?.to_string();
    let signature = read_composite_index_build_key_part(raw, &mut offset, b's')?.to_string();
    (offset == raw.len()).then_some((table, branch, index, signature))
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

    pub(super) fn composite_index_projection(
        descriptor: &RowDescriptor,
        composite_indexes: &[CompositeIndex],
    ) -> Vec<(ColumnName, usize)> {
        let needed: HashSet<ColumnName> = composite_indexes
            .iter()
            .flat_map(|index| index.columns.iter().map(|part| part.name))
            .collect();

        if needed.is_empty() {
            return Vec::new();
        }

        descriptor
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| needed.contains(&column.name).then_some((column.name, idx)))
            .collect()
    }

    fn composite_index_values_by_projection(
        descriptor: &RowDescriptor,
        layout: &CompiledRowLayout,
        data: &[u8],
        projection: &[(ColumnName, usize)],
    ) -> Vec<(ColumnName, Value)> {
        projection
            .iter()
            .filter_map(|(name, idx)| {
                crate::row_format::decode_column_with_layout(descriptor, layout, data, *idx)
                    .ok()
                    .map(|value| (*name, value))
            })
            .collect()
    }

    fn push_insert_composite_index_values<'a>(
        mutations: &mut Vec<IndexMutation<'a>>,
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        values_by_column: &[(ColumnName, Value)],
        composite_indexes: &'a [CompositeIndex],
    ) {
        for index in composite_indexes {
            let Some(value) = composite_index_value(&index.columns, values_by_column) else {
                continue;
            };
            mutations.push(IndexMutation::Insert {
                table,
                column: index.name.as_str(),
                branch,
                value,
                row_id: object_id,
            });
        }
    }

    fn push_remove_composite_index_values<'a>(
        mutations: &mut Vec<IndexMutation<'a>>,
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        values_by_column: &[(ColumnName, Value)],
        composite_indexes: &'a [CompositeIndex],
    ) {
        for index in composite_indexes {
            let Some(value) = composite_index_value(&index.columns, values_by_column) else {
                continue;
            };
            mutations.push(IndexMutation::Remove {
                table,
                column: index.name.as_str(),
                branch,
                value,
                row_id: object_id,
            });
        }
    }

    fn push_insert_system_timestamp_index_values<'a>(
        mutations: &mut Vec<IndexMutation<'a>>,
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        provenance: &RowProvenance,
    ) {
        mutations.push(IndexMutation::Insert {
            table,
            column: CREATED_AT_COLUMN_NAME,
            branch,
            value: Value::Timestamp(provenance.created_at),
            row_id: object_id,
        });
        mutations.push(IndexMutation::Insert {
            table,
            column: UPDATED_AT_COLUMN_NAME,
            branch,
            value: Value::Timestamp(provenance.updated_at),
            row_id: object_id,
        });
    }

    fn push_remove_system_timestamp_index_values<'a>(
        mutations: &mut Vec<IndexMutation<'a>>,
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        provenance: &RowProvenance,
    ) {
        mutations.push(IndexMutation::Remove {
            table,
            column: CREATED_AT_COLUMN_NAME,
            branch,
            value: Value::Timestamp(provenance.created_at),
            row_id: object_id,
        });
        mutations.push(IndexMutation::Remove {
            table,
            column: UPDATED_AT_COLUMN_NAME,
            branch,
            value: Value::Timestamp(provenance.updated_at),
            row_id: object_id,
        });
    }

    fn push_update_system_timestamp_index_values<'a>(
        mutations: &mut Vec<IndexMutation<'a>>,
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        old_provenance: &RowProvenance,
        new_provenance: &RowProvenance,
    ) {
        if old_provenance.created_at != new_provenance.created_at {
            mutations.push(IndexMutation::Remove {
                table,
                column: CREATED_AT_COLUMN_NAME,
                branch,
                value: Value::Timestamp(old_provenance.created_at),
                row_id: object_id,
            });
            mutations.push(IndexMutation::Insert {
                table,
                column: CREATED_AT_COLUMN_NAME,
                branch,
                value: Value::Timestamp(new_provenance.created_at),
                row_id: object_id,
            });
        }
        if old_provenance.updated_at != new_provenance.updated_at {
            mutations.push(IndexMutation::Remove {
                table,
                column: UPDATED_AT_COLUMN_NAME,
                branch,
                value: Value::Timestamp(old_provenance.updated_at),
                row_id: object_id,
            });
            mutations.push(IndexMutation::Insert {
                table,
                column: UPDATED_AT_COLUMN_NAME,
                branch,
                value: Value::Timestamp(new_provenance.updated_at),
                row_id: object_id,
            });
        }
    }

    pub(super) fn index_mutations_for_insert_on_branch<'a>(
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        data: &[u8],
        provenance: &RowProvenance,
        descriptor: &'a RowDescriptor,
        indexed_columns: Option<&'a [ColumnName]>,
        composite_indexes: &'a [CompositeIndex],
    ) -> Vec<IndexMutation<'a>> {
        let layout = crate::row_format::compiled_row_layout(descriptor);
        Self::index_mutations_for_insert_on_branch_with_layout(
            table,
            branch,
            object_id,
            data,
            provenance,
            descriptor,
            indexed_columns,
            composite_indexes,
            &layout,
        )
    }

    pub(super) fn index_mutations_for_insert_on_branch_with_layout<'a>(
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        data: &[u8],
        provenance: &RowProvenance,
        descriptor: &'a RowDescriptor,
        indexed_columns: Option<&'a [ColumnName]>,
        composite_indexes: &'a [CompositeIndex],
        layout: &CompiledRowLayout,
    ) -> Vec<IndexMutation<'a>> {
        let projection = Self::composite_index_projection(descriptor, composite_indexes);
        Self::index_mutations_for_insert_on_branch_with_layout_and_projection(
            table,
            branch,
            object_id,
            data,
            provenance,
            descriptor,
            indexed_columns,
            composite_indexes,
            layout,
            &projection,
        )
    }

    pub(super) fn index_mutations_for_insert_on_branch_with_layout_and_projection<'a>(
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        data: &[u8],
        provenance: &RowProvenance,
        descriptor: &'a RowDescriptor,
        indexed_columns: Option<&'a [ColumnName]>,
        composite_indexes: &'a [CompositeIndex],
        layout: &CompiledRowLayout,
        composite_index_projection: &[(ColumnName, usize)],
    ) -> Vec<IndexMutation<'a>> {
        let mut mutations = vec![IndexMutation::Insert {
            table,
            column: "_id",
            branch,
            value: Value::Uuid(object_id),
            row_id: object_id,
        }];
        Self::push_insert_system_timestamp_index_values(
            &mut mutations,
            table,
            branch,
            object_id,
            provenance,
        );

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

        if !composite_indexes.is_empty() {
            let values_by_column = Self::composite_index_values_by_projection(
                descriptor,
                layout,
                data,
                composite_index_projection,
            );
            Self::push_insert_composite_index_values(
                &mut mutations,
                table,
                branch,
                object_id,
                &values_by_column,
                composite_indexes,
            );
        }

        mutations
    }

    pub(super) fn index_mutations_for_update_on_branch<'a>(
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        old_data: &[u8],
        new_data: &[u8],
        old_provenance: &RowProvenance,
        new_provenance: &RowProvenance,
        descriptor: &'a RowDescriptor,
        indexed_columns: Option<&'a [ColumnName]>,
        composite_indexes: &'a [CompositeIndex],
    ) -> Vec<IndexMutation<'a>> {
        let layout = crate::row_format::compiled_row_layout(descriptor);
        let projection = Self::composite_index_projection(descriptor, composite_indexes);
        Self::index_mutations_for_update_on_branch_with_layout_and_projection(
            table,
            branch,
            object_id,
            old_data,
            new_data,
            old_provenance,
            new_provenance,
            descriptor,
            indexed_columns,
            composite_indexes,
            layout.as_ref(),
            &projection,
        )
    }

    pub(super) fn index_mutations_for_update_on_branch_with_layout_and_projection<'a>(
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        old_data: &[u8],
        new_data: &[u8],
        old_provenance: &RowProvenance,
        new_provenance: &RowProvenance,
        descriptor: &'a RowDescriptor,
        indexed_columns: Option<&'a [ColumnName]>,
        composite_indexes: &'a [CompositeIndex],
        layout: &CompiledRowLayout,
        composite_index_projection: &[(ColumnName, usize)],
    ) -> Vec<IndexMutation<'a>> {
        let mut mutations = Vec::new();
        Self::push_update_system_timestamp_index_values(
            &mut mutations,
            table,
            branch,
            object_id,
            old_provenance,
            new_provenance,
        );

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

        if !composite_indexes.is_empty() {
            let old_values_by_column = Self::composite_index_values_by_projection(
                descriptor,
                layout,
                old_data,
                composite_index_projection,
            );
            let new_values_by_column = Self::composite_index_values_by_projection(
                descriptor,
                layout,
                new_data,
                composite_index_projection,
            );
            for index in composite_indexes {
                let old_value = composite_index_value(&index.columns, &old_values_by_column);
                let new_value = composite_index_value(&index.columns, &new_values_by_column);
                if old_value == new_value {
                    continue;
                }
                if let Some(value) = old_value {
                    mutations.push(IndexMutation::Remove {
                        table,
                        column: index.name.as_str(),
                        branch,
                        value,
                        row_id: object_id,
                    });
                }
                if let Some(value) = new_value {
                    mutations.push(IndexMutation::Insert {
                        table,
                        column: index.name.as_str(),
                        branch,
                        value,
                        row_id: object_id,
                    });
                }
            }
        }

        mutations
    }

    pub(super) fn index_mutations_for_soft_delete_on_branch<'a>(
        table: &'a str,
        branch: &'a str,
        object_id: ObjectId,
        old_data: &[u8],
        old_provenance: &RowProvenance,
        descriptor: &'a RowDescriptor,
        indexed_columns: Option<&'a [ColumnName]>,
        composite_indexes: &'a [CompositeIndex],
    ) -> Vec<IndexMutation<'a>> {
        let mut mutations = vec![IndexMutation::Remove {
            table,
            column: "_id",
            branch,
            value: Value::Uuid(object_id),
            row_id: object_id,
        }];
        Self::push_remove_system_timestamp_index_values(
            &mut mutations,
            table,
            branch,
            object_id,
            old_provenance,
        );

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

        if !composite_indexes.is_empty() {
            let layout = crate::row_format::compiled_row_layout(descriptor);
            let projection = Self::composite_index_projection(descriptor, composite_indexes);
            let values_by_column = Self::composite_index_values_by_projection(
                descriptor,
                layout.as_ref(),
                old_data,
                &projection,
            );
            Self::push_remove_composite_index_values(
                &mut mutations,
                table,
                branch,
                object_id,
                &values_by_column,
                composite_indexes,
            );
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
        old_provenance: Option<&RowProvenance>,
        descriptor: &'a RowDescriptor,
        indexed_columns: Option<&'a [ColumnName]>,
        composite_indexes: &'a [CompositeIndex],
    ) -> Vec<IndexMutation<'a>> {
        let mut mutations = vec![IndexMutation::Remove {
            table,
            column: "_id",
            branch,
            value: Value::Uuid(object_id),
            row_id: object_id,
        }];

        if let Some(old_provenance) = old_provenance {
            Self::push_remove_system_timestamp_index_values(
                &mut mutations,
                table,
                branch,
                object_id,
                old_provenance,
            );
        }

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

            if !composite_indexes.is_empty() {
                let layout = crate::row_format::compiled_row_layout(descriptor);
                let projection = Self::composite_index_projection(descriptor, composite_indexes);
                let values_by_column = Self::composite_index_values_by_projection(
                    descriptor,
                    layout.as_ref(),
                    data,
                    &projection,
                );
                Self::push_remove_composite_index_values(
                    &mut mutations,
                    table,
                    branch,
                    object_id,
                    &values_by_column,
                    composite_indexes,
                );
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
        new_provenance: &RowProvenance,
        descriptor: &'a RowDescriptor,
        indexed_columns: Option<&'a [ColumnName]>,
        composite_indexes: &'a [CompositeIndex],
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
        Self::push_insert_system_timestamp_index_values(
            &mut mutations,
            table,
            branch,
            object_id,
            new_provenance,
        );

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

        if !composite_indexes.is_empty() {
            let layout = crate::row_format::compiled_row_layout(descriptor);
            let projection = Self::composite_index_projection(descriptor, composite_indexes);
            let values_by_column = Self::composite_index_values_by_projection(
                descriptor,
                layout.as_ref(),
                new_data,
                &projection,
            );
            Self::push_insert_composite_index_values(
                &mut mutations,
                table,
                branch,
                object_id,
                &values_by_column,
                composite_indexes,
            );
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
        provenance: &RowProvenance,
        descriptor: &RowDescriptor,
        indexed_columns: Option<&[ColumnName]>,
        composite_indexes: &[CompositeIndex],
    ) -> Result<(), IndexUpdateError> {
        let mutations = Self::index_mutations_for_insert_on_branch(
            table,
            branch,
            object_id,
            data,
            provenance,
            descriptor,
            indexed_columns,
            composite_indexes,
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
        old_provenance: &RowProvenance,
        new_provenance: &RowProvenance,
    ) -> Result<(), QueryError> {
        let mutations = Self::index_mutations_for_update_on_branch(
            target.table,
            target.branch,
            object_id,
            old_data,
            new_data,
            old_provenance,
            new_provenance,
            target.descriptor,
            target.indexed_columns,
            target.composite_indexes,
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
        old_provenance: &RowProvenance,
        descriptor: &RowDescriptor,
        indexed_columns: Option<&[ColumnName]>,
        composite_indexes: &[CompositeIndex],
    ) -> Result<(), QueryError> {
        let mutations = Self::index_mutations_for_soft_delete_on_branch(
            table,
            branch,
            object_id,
            old_data,
            old_provenance,
            descriptor,
            indexed_columns,
            composite_indexes,
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
        old_provenance: Option<&RowProvenance>,
        descriptor: &RowDescriptor,
        indexed_columns: Option<&[ColumnName]>,
        composite_indexes: &[CompositeIndex],
    ) -> Result<(), QueryError> {
        let mutations = Self::index_mutations_for_hard_delete_on_branch(
            table,
            branch,
            object_id,
            old_data,
            old_provenance,
            descriptor,
            indexed_columns,
            composite_indexes,
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
        new_provenance: &RowProvenance,
        descriptor: &RowDescriptor,
        indexed_columns: Option<&[ColumnName]>,
        composite_indexes: &[CompositeIndex],
    ) -> Result<(), QueryError> {
        let mutations = Self::index_mutations_for_undelete_on_branch(
            table,
            branch,
            object_id,
            new_data,
            new_provenance,
            descriptor,
            indexed_columns,
            composite_indexes,
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
            None,
            &table_schema.columns,
            table_schema.indexed_columns.as_deref(),
            &table_schema.composite_indexes,
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
        let restored_provenance = storage
            .load_visible_region_row(table, branch, row_id)
            .ok()
            .flatten()
            .map(|row| row.row_provenance());
        if let (Some(table_schema), Some(restored_provenance)) =
            (self.schema.get(&table_name), restored_provenance.as_ref())
            && let Err(error) = Self::update_indices_for_undelete_on_branch(
                storage,
                table,
                branch,
                row_id,
                restored_data,
                restored_provenance,
                &table_schema.columns,
                table_schema.indexed_columns.as_deref(),
                &table_schema.composite_indexes,
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

    pub(crate) fn process_composite_index_backfills<H: Storage + ?Sized>(
        &mut self,
        storage: &mut H,
    ) -> Result<(), StorageError> {
        self.process_composite_index_backfills_with_budget(
            storage,
            CompositeIndexBackfillBudget::default(),
        )
    }

    pub(crate) fn process_composite_index_backfills_with_budget<H: Storage + ?Sized>(
        &mut self,
        storage: &mut H,
        budget: CompositeIndexBackfillBudget,
    ) -> Result<(), StorageError> {
        if !self.schema_context.is_initialized() {
            return Ok(());
        }

        let targets = self.composite_index_backfill_targets();
        let desired = Self::desired_composite_index_keys(&targets);
        self.cleanup_stale_composite_index_builds(storage, &desired)?;
        let mut became_ready = Vec::new();
        let mut processed_indexes = 0;
        let rows_per_index = budget.rows_per_index.max(1);

        for (branch, schema) in targets {
            for (table_name, table_schema) in schema {
                for index in &table_schema.composite_indexes {
                    let signature = index.signature();
                    let key = composite_index_build_key(
                        table_name.as_str(),
                        &branch,
                        index.name.as_str(),
                        &signature,
                    );
                    let state = storage
                        .raw_table_get(COMPOSITE_INDEX_BUILD_STATE_TABLE, &key)?
                        .and_then(|bytes| decode_composite_index_build_state(&bytes));

                    let state = match state {
                        Some(CompositeIndexBuildState::Ready) => {
                            self.ready_composite_indexes.insert((
                                table_name,
                                branch.clone(),
                                index.name,
                                signature.clone(),
                            ));
                            continue;
                        }
                        Some(state @ CompositeIndexBuildState::Building { .. }) => state,
                        None => {
                            let has_rows = !storage
                                .index_range_limited(
                                    table_name.as_str(),
                                    "_id",
                                    &branch,
                                    Bound::Unbounded,
                                    Bound::Unbounded,
                                    1,
                                )
                                .is_empty();
                            let initial = if has_rows {
                                CompositeIndexBuildState::Building { cursor: None }
                            } else {
                                CompositeIndexBuildState::Ready
                            };
                            storage.raw_table_put(
                                COMPOSITE_INDEX_BUILD_STATE_TABLE,
                                &key,
                                &encode_composite_index_build_state(initial),
                            )?;
                            if matches!(initial, CompositeIndexBuildState::Ready) {
                                self.ready_composite_indexes.insert((
                                    table_name,
                                    branch.clone(),
                                    index.name,
                                    signature.clone(),
                                ));
                                became_ready.push(table_name);
                                continue;
                            }
                            initial
                        }
                    };

                    let CompositeIndexBuildState::Building { cursor } = state else {
                        continue;
                    };
                    self.ready_composite_indexes.remove(&(
                        table_name,
                        branch.clone(),
                        index.name,
                        signature.clone(),
                    ));
                    if processed_indexes >= budget.indexes {
                        continue;
                    }
                    processed_indexes += 1;

                    let start_value = cursor.map(Value::Uuid);
                    let start = start_value
                        .as_ref()
                        .map_or(Bound::Unbounded, Bound::Excluded);
                    let row_ids = storage.index_range_limited(
                        table_name.as_str(),
                        "_id",
                        &branch,
                        start,
                        Bound::Unbounded,
                        rows_per_index,
                    );

                    let mut mutations = Vec::new();
                    let layout = crate::row_format::compiled_row_layout(&table_schema.columns);
                    let projection = Self::composite_index_projection(
                        &table_schema.columns,
                        std::slice::from_ref(index),
                    );
                    for row_id in &row_ids {
                        let Some(row) = storage.load_visible_region_row(
                            table_name.as_str(),
                            &branch,
                            *row_id,
                        )?
                        else {
                            continue;
                        };
                        if row.is_soft_deleted() || row.is_hard_deleted() {
                            continue;
                        }
                        let values_by_column = Self::composite_index_values_by_projection(
                            &table_schema.columns,
                            layout.as_ref(),
                            &row.data,
                            &projection,
                        );
                        Self::push_insert_composite_index_values(
                            &mut mutations,
                            table_name.as_str(),
                            &branch,
                            *row_id,
                            &values_by_column,
                            std::slice::from_ref(index),
                        );
                    }
                    storage.apply_index_mutations(&mutations)?;

                    let next_state = if row_ids.len() < rows_per_index {
                        CompositeIndexBuildState::Ready
                    } else {
                        CompositeIndexBuildState::Building {
                            cursor: row_ids.last().copied(),
                        }
                    };
                    storage.raw_table_put(
                        COMPOSITE_INDEX_BUILD_STATE_TABLE,
                        &key,
                        &encode_composite_index_build_state(next_state),
                    )?;

                    if matches!(next_state, CompositeIndexBuildState::Ready) {
                        self.ready_composite_indexes.insert((
                            table_name,
                            branch.clone(),
                            index.name,
                            signature,
                        ));
                        became_ready.push(table_name);
                    }
                }
            }
        }

        if !became_ready.is_empty() {
            self.mark_subscriptions_for_recompile();
        }
        Ok(())
    }

    fn composite_index_backfill_targets(&self) -> Vec<(String, Schema)> {
        let mut targets = Vec::new();
        let current_branch = self.schema_context.branch_name().as_str().to_string();
        targets.push((current_branch, self.schema.as_ref().clone()));

        for (schema_hash, schema) in &self.schema_context.live_schemas {
            let branch = ComposedBranchName::new(
                &self.schema_context.env,
                *schema_hash,
                &self.schema_context.user_branch,
            )
            .to_branch_name()
            .as_str()
            .to_string();
            targets.push((branch, schema.clone()));
        }

        targets
    }

    fn desired_composite_index_keys(
        targets: &[(String, Schema)],
    ) -> HashSet<(String, String, String, String)> {
        let mut desired = HashSet::new();
        for (branch, schema) in targets {
            for (table_name, table_schema) in schema {
                for index in &table_schema.composite_indexes {
                    desired.insert((
                        table_name.as_str().to_string(),
                        branch.clone(),
                        index.name.as_str().to_string(),
                        index.signature(),
                    ));
                }
            }
        }
        desired
    }

    fn cleanup_stale_composite_index_builds<H: Storage + ?Sized>(
        &mut self,
        storage: &mut H,
        desired: &HashSet<(String, String, String, String)>,
    ) -> Result<(), StorageError> {
        let keys = storage.raw_table_scan_prefix_keys(COMPOSITE_INDEX_BUILD_STATE_TABLE, "")?;
        let mut cleared_raw_indexes = HashSet::new();

        for key in keys {
            let Some((table, branch, index_name, signature)) =
                parse_composite_index_build_key(&key)
            else {
                continue;
            };
            if desired.contains(&(
                table.clone(),
                branch.clone(),
                index_name.clone(),
                signature.clone(),
            )) {
                continue;
            }

            storage.raw_table_delete(COMPOSITE_INDEX_BUILD_STATE_TABLE, &key)?;
            self.ready_composite_indexes.remove(&(
                TableName::new(table.clone()),
                branch.clone(),
                ColumnName::new(index_name.clone()),
                signature,
            ));

            if cleared_raw_indexes.insert((table.clone(), branch.clone(), index_name.clone())) {
                storage.clear_index(&table, &index_name, &branch)?;
            }
        }

        Ok(())
    }
}
