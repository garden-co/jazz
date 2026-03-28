use std::collections::HashMap;

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::storage::{Storage, StorageError, validate_index_value_size};

use super::encoding::decode_column;
use super::manager::{QueryError, QueryManager};
use super::types::{ColumnDescriptor, ColumnType, RowDescriptor, TableName, Value};

#[derive(Debug, Clone)]
struct IndexSourceState {
    branch: BranchName,
    data: Vec<u8>,
    is_deleted: bool,
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

    fn insert_live_row_indices_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        storage
            .index_insert(table, "_id", branch, &Value::Uuid(object_id), object_id)
            .map_err(Self::map_index_storage_error)?;

        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Ok(value) = decode_column(descriptor, data, col_idx)
                && value != Value::Null
            {
                Self::insert_column_index_values(storage, table, col, branch, &value, object_id)?;
            }
        }

        Ok(())
    }

    fn remove_live_row_indices_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        storage
            .index_remove(table, "_id", branch, &Value::Uuid(object_id), object_id)
            .map_err(Self::map_index_storage_error)?;

        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Ok(value) = decode_column(descriptor, data, col_idx)
                && value != Value::Null
            {
                Self::remove_column_index_values(storage, table, col, branch, &value, object_id)?;
            }
        }

        Ok(())
    }

    fn insert_deleted_row_index_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
    ) -> Result<(), QueryError> {
        storage
            .index_insert(
                table,
                "_id_deleted",
                branch,
                &Value::Uuid(object_id),
                object_id,
            )
            .map_err(Self::map_index_storage_error)
    }

    fn remove_deleted_row_index_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
    ) -> Result<(), QueryError> {
        storage
            .index_remove(
                table,
                "_id_deleted",
                branch,
                &Value::Uuid(object_id),
                object_id,
            )
            .map_err(Self::map_index_storage_error)
    }

    fn retire_source_indices(
        storage: &mut dyn Storage,
        table: &str,
        object_id: ObjectId,
        descriptor: &RowDescriptor,
        source_state: &IndexSourceState,
    ) -> Result<(), QueryError> {
        if source_state.is_deleted {
            Self::remove_deleted_row_index_on_branch(
                storage,
                table,
                source_state.branch.as_str(),
                object_id,
            )
        } else {
            Self::remove_live_row_indices_on_branch(
                storage,
                table,
                source_state.branch.as_str(),
                object_id,
                &source_state.data,
                descriptor,
            )
        }
    }

    pub(super) fn tip_commit_on_branch(
        &self,
        object_id: ObjectId,
        branch: &str,
    ) -> Option<(CommitId, Commit)> {
        let object = self.sync_manager.object_manager.get(object_id)?;
        let branch = object.branches.get(&BranchName::new(branch))?;
        let mut tips: Vec<_> = branch.tips.iter().copied().collect();
        tips.sort_by_key(|id| {
            (
                branch
                    .commits
                    .get(id)
                    .map(|commit| commit.timestamp)
                    .unwrap_or(0),
                *id,
            )
        });
        let tip_id = *tips.last()?;
        let commit = branch.commits.get(&tip_id)?.clone();
        Some((tip_id, commit))
    }

    fn commit_branch_for_object<H: Storage + ?Sized>(
        &self,
        storage: &H,
        object_id: ObjectId,
        commit_id: CommitId,
    ) -> Option<BranchName> {
        self.sync_manager
            .object_manager
            .get(object_id)
            .and_then(|object| object.commit_branches.get(&commit_id).copied())
            .or_else(|| {
                storage
                    .load_commit_branch(object_id, commit_id)
                    .ok()
                    .flatten()
            })
    }

    fn source_states_for_head_commit(
        &mut self,
        storage: &dyn Storage,
        object_id: ObjectId,
        branch: &str,
        head_commit_id: CommitId,
    ) -> Result<Vec<IndexSourceState>, QueryError> {
        let requested_branches = [branch.to_string()];
        self.sync_manager
            .object_manager
            .get_or_load(object_id, storage, &requested_branches)
            .ok_or(QueryError::ObjectNotFound(object_id))?;

        let branch_name = BranchName::new(branch);
        let head_commit = self
            .sync_manager
            .object_manager
            .get(object_id)
            .and_then(|object| object.branches.get(&branch_name))
            .and_then(|object_branch| object_branch.commits.get(&head_commit_id))
            .cloned()
            .ok_or(QueryError::ObjectNotFound(object_id))?;

        let mut states_by_branch = HashMap::new();
        for parent_id in &head_commit.parents {
            let Some(parent_branch) = self.commit_branch_for_object(storage, object_id, *parent_id)
            else {
                continue;
            };

            let parent_branch_request = [parent_branch.as_str().to_string()];
            let _ = self.sync_manager.object_manager.get_or_load_tips(
                object_id,
                storage,
                &parent_branch_request,
            );

            let Some(parent_commit) = self
                .sync_manager
                .object_manager
                .get(object_id)
                .and_then(|object| object.branches.get(&parent_branch))
                .and_then(|object_branch| object_branch.commits.get(parent_id))
                .cloned()
            else {
                continue;
            };

            if parent_commit.content.is_empty() && parent_commit.is_hard_deleted() {
                continue;
            }

            states_by_branch
                .entry(parent_branch)
                .or_insert_with(|| IndexSourceState {
                    branch: parent_branch,
                    is_deleted: parent_commit.is_soft_deleted(),
                    data: parent_commit.content,
                });
        }

        Ok(states_by_branch.into_values().collect())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn reconcile_indices_after_live_commit(
        &mut self,
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        head_commit_id: CommitId,
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        for source_state in
            self.source_states_for_head_commit(storage, object_id, branch, head_commit_id)?
        {
            Self::retire_source_indices(storage, table, object_id, descriptor, &source_state)?;
        }

        Self::insert_live_row_indices_on_branch(
            storage, table, branch, object_id, new_data, descriptor,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn reconcile_indices_after_soft_delete_commit(
        &mut self,
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        head_commit_id: CommitId,
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        for source_state in
            self.source_states_for_head_commit(storage, object_id, branch, head_commit_id)?
        {
            Self::retire_source_indices(storage, table, object_id, descriptor, &source_state)?;
        }

        Self::insert_deleted_row_index_on_branch(storage, table, branch, object_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn reconcile_indices_after_hard_delete_commit(
        &mut self,
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        head_commit_id: CommitId,
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        for source_state in
            self.source_states_for_head_commit(storage, object_id, branch, head_commit_id)?
        {
            Self::retire_source_indices(storage, table, object_id, descriptor, &source_state)?;
        }
        Ok(())
    }
}
