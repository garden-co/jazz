use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, Object, ObjectId, VisibleCommit, VisibleCommitState};
use crate::storage::{Storage, StorageError, validate_index_value_size};
use std::collections::{HashSet, VecDeque};

use super::encoding::decode_column;
use super::manager::{QueryError, QueryManager};
use super::types::{
    BatchBranchKey, ColumnDescriptor, ColumnType, QueryBranchRef, RowDescriptor, TableName, Value,
};

#[derive(Debug, Clone)]
struct VisibleIndexState {
    visible_commit: VisibleCommit,
    data: Vec<u8>,
    timestamp: u64,
}

impl QueryManager {
    fn commit_by_id(object: &Object, commit_id: CommitId) -> Option<&Commit> {
        let branch_key = object.commit_branches.get(&commit_id)?;
        object
            .branches
            .get_by_key(*branch_key)
            .and_then(|branch| branch.commits.get(&commit_id))
    }

    fn commit_is_descendant_of(
        object: &Object,
        commit_id: CommitId,
        ancestor_id: CommitId,
    ) -> bool {
        if commit_id == ancestor_id {
            return true;
        }

        let mut visited = HashSet::new();
        let mut queue = VecDeque::from([commit_id]);
        while let Some(current) = queue.pop_front() {
            if !visited.insert(current) {
                continue;
            }

            let Some(commit) = Self::commit_by_id(object, current) else {
                continue;
            };

            for parent_id in &commit.parents {
                if *parent_id == ancestor_id {
                    return true;
                }
                queue.push_back(*parent_id);
            }
        }

        false
    }

    fn branch_ref(branch: &BranchName) -> QueryBranchRef {
        QueryBranchRef::from_branch_name(*branch)
    }

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
        branch: &BranchName,
        value: &Value,
    ) -> Result<(), QueryError> {
        let branch_ref = Self::branch_ref(branch);
        for index_value in Self::expand_index_values(column, value) {
            validate_index_value_size(table, column.name.as_str(), &branch_ref, &index_value)
                .map_err(Self::map_index_storage_error)?;
        }
        Ok(())
    }

    pub(super) fn validate_write_index_values_on_branch(
        table: &str,
        branch: &BranchName,
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
        branch: &BranchName,
        value: &Value,
        object_id: ObjectId,
    ) -> Result<(), QueryError> {
        let branch_ref = Self::branch_ref(branch);
        for index_value in Self::expand_index_values(column, value) {
            storage
                .index_insert(
                    table,
                    column.name.as_str(),
                    &branch_ref,
                    &index_value,
                    object_id,
                )
                .map_err(Self::map_index_storage_error)?;
        }
        Ok(())
    }

    fn remove_column_index_values(
        storage: &mut dyn Storage,
        table: &str,
        column: &ColumnDescriptor,
        branch: &BranchName,
        value: &Value,
        object_id: ObjectId,
    ) -> Result<(), QueryError> {
        let branch_ref = Self::branch_ref(branch);
        for index_value in Self::expand_index_values(column, value) {
            storage
                .index_remove(
                    table,
                    column.name.as_str(),
                    &branch_ref,
                    &index_value,
                    object_id,
                )
                .map_err(Self::map_index_storage_error)?;
        }
        Ok(())
    }

    fn insert_live_row_indices_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &BranchName,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        let branch_ref = Self::branch_ref(branch);
        storage
            .index_insert(
                table,
                "_id",
                &branch_ref,
                &Value::Uuid(object_id),
                object_id,
            )
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
        branch: &BranchName,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        let branch_ref = Self::branch_ref(branch);
        storage
            .index_remove(
                table,
                "_id",
                &branch_ref,
                &Value::Uuid(object_id),
                object_id,
            )
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
        branch: &BranchName,
        object_id: ObjectId,
    ) -> Result<(), QueryError> {
        let branch_ref = Self::branch_ref(branch);
        storage
            .index_insert(
                table,
                "_id_deleted",
                &branch_ref,
                &Value::Uuid(object_id),
                object_id,
            )
            .map_err(Self::map_index_storage_error)
    }

    fn remove_deleted_row_index_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &BranchName,
        object_id: ObjectId,
    ) -> Result<(), QueryError> {
        let branch_ref = Self::branch_ref(branch);
        storage
            .index_remove(
                table,
                "_id_deleted",
                &branch_ref,
                &Value::Uuid(object_id),
                object_id,
            )
            .map_err(Self::map_index_storage_error)
    }

    pub(super) fn tip_commit_on_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Option<(CommitId, Commit)> {
        let object = self.sync_manager.object_manager.get(object_id)?;
        let branch = object.branches.get(branch)?;
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

    fn visible_state_kind(commit: &Commit) -> VisibleCommitState {
        if commit.content.is_empty() && commit.is_hard_deleted() {
            VisibleCommitState::HardDeleted
        } else if commit.is_soft_deleted() {
            VisibleCommitState::SoftDeleted
        } else {
            VisibleCommitState::Live
        }
    }

    fn visible_state_counts_toward_manifest(state: VisibleCommitState) -> bool {
        !matches!(state, VisibleCommitState::HardDeleted)
    }

    fn resolve_visible_state(
        &mut self,
        storage: &dyn Storage,
        object_id: ObjectId,
        prefix: BranchName,
    ) -> Result<Option<VisibleIndexState>, QueryError> {
        self.sync_manager
            .object_manager
            .get_or_load(object_id, storage, &[])
            .ok_or(QueryError::ObjectNotFound(object_id))?;

        let Some(visible_commit) = self
            .sync_manager
            .object_manager
            .get(object_id)
            .and_then(|object| object.visible_states.get(&prefix).copied())
        else {
            return Ok(None);
        };

        let branch_name = visible_commit.branch.branch_name();
        let requested_branches = [branch_name.as_str().to_string()];
        self.sync_manager
            .object_manager
            .get_or_load_tips(object_id, storage, &requested_branches)
            .ok_or(QueryError::ObjectNotFound(object_id))?;

        let object = self
            .sync_manager
            .object_manager
            .get(object_id)
            .ok_or(QueryError::ObjectNotFound(object_id))?;
        let commit = object
            .branches
            .get_by_key(visible_commit.branch)
            .and_then(|branch| branch.commits.get(&visible_commit.commit_id))
            .cloned()
            .ok_or(QueryError::ObjectNotFound(object_id))?;

        Ok(Some(VisibleIndexState {
            visible_commit,
            data: commit.content,
            timestamp: commit.timestamp,
        }))
    }

    fn retire_visible_indices(
        storage: &mut dyn Storage,
        table: &str,
        object_id: ObjectId,
        descriptor: &RowDescriptor,
        visible_state: &VisibleIndexState,
    ) -> Result<(), QueryError> {
        let branch_name = visible_state.visible_commit.branch.branch_name();
        match visible_state.visible_commit.state {
            VisibleCommitState::Live => Self::remove_live_row_indices_on_branch(
                storage,
                table,
                &branch_name,
                object_id,
                &visible_state.data,
                descriptor,
            ),
            VisibleCommitState::SoftDeleted => {
                Self::remove_deleted_row_index_on_branch(storage, table, &branch_name, object_id)
            }
            VisibleCommitState::HardDeleted => Ok(()),
        }
    }

    fn publish_visible_indices(
        storage: &mut dyn Storage,
        table: &str,
        object_id: ObjectId,
        descriptor: &RowDescriptor,
        visible_commit: VisibleCommit,
        data: &[u8],
    ) -> Result<(), QueryError> {
        let branch_name = visible_commit.branch.branch_name();
        match visible_commit.state {
            VisibleCommitState::Live => Self::insert_live_row_indices_on_branch(
                storage,
                table,
                &branch_name,
                object_id,
                data,
                descriptor,
            ),
            VisibleCommitState::SoftDeleted => {
                Self::insert_deleted_row_index_on_branch(storage, table, &branch_name, object_id)
            }
            VisibleCommitState::HardDeleted => Ok(()),
        }
    }

    fn update_cached_visible_commit(
        &mut self,
        object_id: ObjectId,
        prefix: BranchName,
        visible_commit: VisibleCommit,
    ) {
        if let Some(object) = self.sync_manager.object_manager.get_mut(object_id) {
            object.visible_states.set(prefix, visible_commit);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn reconcile_visible_state_after_commit(
        &mut self,
        storage: &mut dyn Storage,
        table: &str,
        branch: &BranchName,
        object_id: ObjectId,
        head_commit_id: CommitId,
        descriptor: &RowDescriptor,
    ) -> Result<bool, QueryError> {
        let branch_key = BatchBranchKey::from_branch_name(*branch);
        let prefix = branch_key.prefix_name();
        let old_visible = self.resolve_visible_state(storage, object_id, prefix)?;

        let object = self
            .sync_manager
            .object_manager
            .get(object_id)
            .ok_or(QueryError::ObjectNotFound(object_id))?;
        let head_commit = object
            .branches
            .get(branch)
            .and_then(|object_branch| object_branch.commits.get(&head_commit_id))
            .cloned()
            .ok_or(QueryError::ObjectNotFound(object_id))?;

        let new_visible_commit = VisibleCommit {
            branch: branch_key,
            commit_id: head_commit_id,
            state: Self::visible_state_kind(&head_commit),
        };

        if let Some(old_visible) = &old_visible {
            let old_commit_id = old_visible.visible_commit.commit_id;
            let new_beats_old =
                if Self::commit_is_descendant_of(object, head_commit_id, old_commit_id) {
                    true
                } else if Self::commit_is_descendant_of(object, old_commit_id, head_commit_id) {
                    false
                } else {
                    (head_commit.timestamp, head_commit_id) > (old_visible.timestamp, old_commit_id)
                };

            if !new_beats_old {
                return Ok(false);
            }
        }

        if let Some(old_visible) = &old_visible {
            Self::retire_visible_indices(storage, table, object_id, descriptor, old_visible)?;
        }
        Self::publish_visible_indices(
            storage,
            table,
            object_id,
            descriptor,
            new_visible_commit,
            &head_commit.content,
        )?;

        let old_manifest_batch = old_visible.as_ref().and_then(|visible| {
            Self::visible_state_counts_toward_manifest(visible.visible_commit.state)
                .then_some(visible.visible_commit.branch.batch_id())
        });
        let new_manifest_batch =
            Self::visible_state_counts_toward_manifest(new_visible_commit.state)
                .then_some(new_visible_commit.branch.batch_id());

        match (old_manifest_batch, new_manifest_batch) {
            (Some(old_batch), Some(new_batch)) if old_batch != new_batch => {
                storage
                    .adjust_table_prefix_batch_refcount(table, prefix, old_batch, -1)
                    .map_err(Self::map_index_storage_error)?;
                storage
                    .adjust_table_prefix_batch_refcount(table, prefix, new_batch, 1)
                    .map_err(Self::map_index_storage_error)?;
            }
            (Some(old_batch), None) => {
                storage
                    .adjust_table_prefix_batch_refcount(table, prefix, old_batch, -1)
                    .map_err(Self::map_index_storage_error)?;
            }
            (None, Some(new_batch)) => {
                storage
                    .adjust_table_prefix_batch_refcount(table, prefix, new_batch, 1)
                    .map_err(Self::map_index_storage_error)?;
            }
            _ => {}
        }

        storage
            .store_visible_commit(object_id, prefix, new_visible_commit)
            .map_err(Self::map_index_storage_error)?;
        self.update_cached_visible_commit(object_id, prefix, new_visible_commit);

        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn reconcile_indices_after_live_commit(
        &mut self,
        storage: &mut dyn Storage,
        table: &str,
        branch: &BranchName,
        object_id: ObjectId,
        head_commit_id: CommitId,
        _new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<bool, QueryError> {
        self.reconcile_visible_state_after_commit(
            storage,
            table,
            branch,
            object_id,
            head_commit_id,
            descriptor,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn reconcile_indices_after_soft_delete_commit(
        &mut self,
        storage: &mut dyn Storage,
        table: &str,
        branch: &BranchName,
        object_id: ObjectId,
        head_commit_id: CommitId,
        descriptor: &RowDescriptor,
    ) -> Result<bool, QueryError> {
        self.reconcile_visible_state_after_commit(
            storage,
            table,
            branch,
            object_id,
            head_commit_id,
            descriptor,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn reconcile_indices_after_hard_delete_commit(
        &mut self,
        storage: &mut dyn Storage,
        table: &str,
        branch: &BranchName,
        object_id: ObjectId,
        head_commit_id: CommitId,
        descriptor: &RowDescriptor,
    ) -> Result<bool, QueryError> {
        self.reconcile_visible_state_after_commit(
            storage,
            table,
            branch,
            object_id,
            head_commit_id,
            descriptor,
        )
    }
}
