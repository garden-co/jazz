//! apply_row_batch, patch_row_batch_state, and the visible-row computation helpers
//! they pull in.

use std::collections::HashMap;

use crate::metadata::DeleteKind;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::{
    ColumnDescriptor, ColumnMergeStrategy, RowDescriptor, SharedString, Value,
};
use crate::row_format::{EncodingError, encode_row};
use crate::storage::{IndexMutation, RowLocator, Storage, StorageError};
use crate::sync_manager::DurabilityTier;

use super::codecs::{flat_user_values, malformed, tier_satisfies};
use super::types::{
    ApplyRowBatchResult, BatchId, ComputedVisiblePreview, HistoryScan, RowHistoryError, RowState,
    RowVisibilityChange, StoredRowBatch, VisibleRowEntry,
};

pub(super) fn visible_rows_for_tier(
    history_rows: &[StoredRowBatch],
    required_tier: Option<DurabilityTier>,
) -> Vec<&StoredRowBatch> {
    history_rows
        .iter()
        .filter(|row| {
            row.state.is_visible()
                && required_tier
                    .map(|tier| tier_satisfies(row.confirmed_tier, tier))
                    .unwrap_or(true)
        })
        .collect()
}

pub(super) fn latest_common_ancestor<'a>(
    frontier: &[&'a StoredRowBatch],
    row_by_batch_id: &HashMap<BatchId, &'a StoredRowBatch>,
) -> Option<&'a StoredRowBatch> {
    let mut common_ancestors: Option<std::collections::HashSet<BatchId>> = None;

    for tip in frontier {
        let mut stack = vec![tip.batch_id()];
        let mut ancestors = std::collections::HashSet::new();
        while let Some(batch_id) = stack.pop() {
            if !ancestors.insert(batch_id) {
                continue;
            }
            if let Some(row) = row_by_batch_id.get(&batch_id) {
                stack.extend(row.parents.iter().copied());
            }
        }

        common_ancestors = Some(match common_ancestors {
            None => ancestors,
            Some(mut existing) => {
                existing.retain(|batch_id| ancestors.contains(batch_id));
                existing
            }
        });
    }

    common_ancestors?
        .into_iter()
        .filter_map(|batch_id| row_by_batch_id.get(&batch_id).copied())
        .max_by_key(|row| (row.updated_at, row.batch_id()))
}

pub(super) fn delete_winner<'a>(frontier: &[&'a StoredRowBatch]) -> Option<&'a StoredRowBatch> {
    frontier
        .iter()
        .copied()
        .filter(|row| row.delete_kind.is_some())
        .max_by(|left, right| {
            let left_rank = match left.delete_kind {
                Some(DeleteKind::Hard) => 2u8,
                Some(DeleteKind::Soft) => 1u8,
                None => 0u8,
            };
            let right_rank = match right.delete_kind {
                Some(DeleteKind::Hard) => 2u8,
                Some(DeleteKind::Soft) => 1u8,
                None => 0u8,
            };
            (left_rank, left.updated_at, left.batch_id()).cmp(&(
                right_rank,
                right.updated_at,
                right.batch_id(),
            ))
        })
}

pub(super) fn computed_visible_preview_matches(
    current: &ComputedVisiblePreview,
    candidate: &ComputedVisiblePreview,
) -> bool {
    current.row == candidate.row && current.winner_batch_ids == candidate.winner_batch_ids
}

pub(super) fn current_winner_batch_id(
    column_winner: Option<&StoredRowBatch>,
    fallback: &StoredRowBatch,
) -> BatchId {
    column_winner
        .map(StoredRowBatch::batch_id)
        .unwrap_or_else(|| fallback.batch_id())
}

#[derive(Clone, Copy)]
pub(super) struct ColumnContender<'a> {
    row: &'a StoredRowBatch,
    value: &'a Value,
}

pub(super) fn merge_column_with_strategy<'a>(
    column: &ColumnDescriptor,
    ancestor_value: &Value,
    contenders: &[ColumnContender<'a>],
) -> Result<(Value, Option<&'a StoredRowBatch>), EncodingError> {
    match column.merge_strategy {
        Some(ColumnMergeStrategy::Counter) => {
            let ancestor = match ancestor_value {
                Value::Integer(value) => *value,
                Value::Null => 0,
                other => {
                    return Err(malformed(format!(
                        "counter merge expected INTEGER ancestor for column '{}', got {:?}",
                        column.name_str(),
                        other
                    )));
                }
            };

            let mut delta_sum = 0i32;
            let mut latest_contributor: Option<&StoredRowBatch> = None;
            for contender in contenders {
                let contender_value = match contender.value {
                    Value::Integer(value) => *value,
                    other => {
                        return Err(malformed(format!(
                            "counter merge expected INTEGER contender for column '{}', got {:?}",
                            column.name_str(),
                            other
                        )));
                    }
                };
                let delta = contender_value.checked_sub(ancestor).ok_or_else(|| {
                    malformed(format!(
                        "counter merge delta overflow for column '{}'",
                        column.name_str()
                    ))
                })?;
                delta_sum = delta_sum.checked_add(delta).ok_or_else(|| {
                    malformed(format!(
                        "counter merge overflow for column '{}'",
                        column.name_str()
                    ))
                })?;
                if delta != 0
                    && latest_contributor
                        .map(|current| {
                            (contender.row.updated_at, contender.row.batch_id())
                                > (current.updated_at, current.batch_id())
                        })
                        .unwrap_or(true)
                {
                    latest_contributor = Some(contender.row);
                }
            }

            let merged = ancestor.checked_add(delta_sum).ok_or_else(|| {
                malformed(format!(
                    "counter merge overflow for column '{}'",
                    column.name_str()
                ))
            })?;

            Ok((Value::Integer(merged), latest_contributor))
        }
        None => {
            let mut latest_changed: Option<&StoredRowBatch> = None;
            let mut merged_value = ancestor_value.clone();

            for contender in contenders {
                if latest_changed
                    .map(|current| {
                        (contender.row.updated_at, contender.row.batch_id())
                            > (current.updated_at, current.batch_id())
                    })
                    .unwrap_or(true)
                {
                    latest_changed = Some(contender.row);
                    merged_value = contender.value.clone();
                }
            }

            Ok((merged_value, latest_changed))
        }
    }
}

pub(super) fn assign_winner_ordinals(
    winner_batch_ids: Option<&[BatchId]>,
    winner_batch_pool: &mut Vec<BatchId>,
    pool_ordinals: &mut HashMap<BatchId, u16>,
) -> Result<Option<Vec<u16>>, EncodingError> {
    let Some(winner_batch_ids) = winner_batch_ids else {
        return Ok(None);
    };

    let mut ordinals = Vec::with_capacity(winner_batch_ids.len());
    for batch_id in winner_batch_ids {
        let ordinal = if let Some(existing) = pool_ordinals.get(batch_id) {
            *existing
        } else {
            let ordinal = u16::try_from(winner_batch_pool.len())
                .map_err(|_| malformed("winner batch pool exceeds u16 ordinal capacity"))?;
            winner_batch_pool.push(*batch_id);
            pool_ordinals.insert(*batch_id, ordinal);
            ordinal
        };
        ordinals.push(ordinal);
    }

    Ok(Some(ordinals))
}

pub(super) fn preview_override_sidecar(
    current_preview: &ComputedVisiblePreview,
    candidate_preview: Option<&ComputedVisiblePreview>,
    winner_batch_pool: &mut Vec<BatchId>,
    pool_ordinals: &mut HashMap<BatchId, u16>,
) -> Result<(Option<BatchId>, Option<Vec<u16>>), EncodingError> {
    let Some(candidate_preview) = candidate_preview else {
        return Ok((None, None));
    };
    if computed_visible_preview_matches(current_preview, candidate_preview) {
        return Ok((None, None));
    }

    Ok((
        Some(candidate_preview.row.batch_id()),
        assign_winner_ordinals(
            candidate_preview.winner_batch_ids.as_deref(),
            winner_batch_pool,
            pool_ordinals,
        )?,
    ))
}

pub(super) fn build_computed_visible_preview(
    user_descriptor: &RowDescriptor,
    history_rows: &[StoredRowBatch],
    required_tier: Option<DurabilityTier>,
) -> Result<Option<ComputedVisiblePreview>, EncodingError> {
    let visible_rows = visible_rows_for_tier(history_rows, required_tier);
    if visible_rows.is_empty() {
        return Ok(None);
    }

    let mut non_tips = std::collections::BTreeSet::new();
    for row in &visible_rows {
        for parent in &row.parents {
            non_tips.insert(*parent);
        }
    }
    let mut frontier: Vec<_> = visible_rows
        .iter()
        .copied()
        .filter(|row| !non_tips.contains(&row.batch_id()))
        .collect();
    frontier.sort_by_key(|row| (row.updated_at, row.batch_id()));
    frontier.dedup_by_key(|row| row.batch_id());
    let Some(latest_tip) = frontier.last().copied() else {
        return Ok(None);
    };
    if frontier.len() == 1 {
        return Ok(Some(ComputedVisiblePreview {
            row: latest_tip.clone(),
            winner_batch_ids: None,
        }));
    }

    let row_by_batch_id = visible_rows
        .iter()
        .copied()
        .map(|row| (row.batch_id(), row))
        .collect::<HashMap<_, _>>();
    let ancestor = latest_common_ancestor(&frontier, &row_by_batch_id);

    let ancestor_values = match ancestor {
        Some(row) => flat_user_values(user_descriptor, &row.data)?,
        None => user_descriptor
            .columns
            .iter()
            .map(|_| Value::Null)
            .collect(),
    };
    let frontier_values = frontier
        .iter()
        .map(|row| flat_user_values(user_descriptor, &row.data))
        .collect::<Result<Vec<_>, _>>()?;

    let mut merged_values = Vec::with_capacity(user_descriptor.columns.len());
    let mut contributing_rows: Vec<&StoredRowBatch> = Vec::new();
    let mut winner_batch_ids = Vec::with_capacity(user_descriptor.columns.len());

    for column_index in 0..user_descriptor.columns.len() {
        let ancestor_value = ancestor_values[column_index].clone();
        let changed_contenders = frontier
            .iter()
            .zip(frontier_values.iter())
            .filter_map(|(row, row_values)| {
                let candidate_value = &row_values[column_index];
                (candidate_value != &ancestor_value).then_some(ColumnContender {
                    row,
                    value: candidate_value,
                })
            })
            .collect::<Vec<_>>();
        let (best_value, best_changed) = merge_column_with_strategy(
            &user_descriptor.columns[column_index],
            &ancestor_value,
            &changed_contenders,
        )?;

        merged_values.push(best_value);
        let winner_row = best_changed.or(ancestor).unwrap_or(latest_tip);
        winner_batch_ids.push(current_winner_batch_id(Some(winner_row), latest_tip));
        contributing_rows.push(winner_row);
    }

    let delete_winner = delete_winner(&frontier);
    let metadata_row = delete_winner.unwrap_or_else(|| {
        contributing_rows
            .iter()
            .copied()
            .max_by_key(|row| (row.updated_at, row.batch_id()))
            .unwrap_or(latest_tip)
    });

    let mut confirmed_tier: Option<DurabilityTier> = None;
    for tier in contributing_rows
        .iter()
        .copied()
        .chain(delete_winner)
        .map(|row| row.confirmed_tier)
    {
        let Some(tier) = tier else {
            confirmed_tier = None;
            break;
        };
        confirmed_tier = Some(match confirmed_tier {
            Some(existing) => existing.min(tier),
            None => tier,
        });
    }

    let data = match delete_winner.and_then(|row| row.delete_kind) {
        Some(DeleteKind::Hard) => Vec::new(),
        _ => encode_row(user_descriptor, &merged_values)?,
    };

    let row = StoredRowBatch {
        row_id: metadata_row.row_id,
        batch_id: metadata_row.batch_id,
        branch: metadata_row.branch.clone(),
        parents: metadata_row.parents.clone(),
        updated_at: metadata_row.updated_at,
        created_by: metadata_row.created_by.clone(),
        created_at: metadata_row.created_at,
        updated_by: metadata_row.updated_by.clone(),
        state: metadata_row.state,
        confirmed_tier,
        delete_kind: delete_winner.and_then(|row| row.delete_kind),
        is_deleted: delete_winner.is_some(),
        data: data.into(),
        metadata: metadata_row.metadata.clone(),
    };

    let winner_batch_ids = if winner_batch_ids
        .iter()
        .all(|batch_id| *batch_id == metadata_row.batch_id())
        && row.data == metadata_row.data
        && row.confirmed_tier == metadata_row.confirmed_tier
        && row.delete_kind == metadata_row.delete_kind
        && row.is_deleted == metadata_row.is_deleted
    {
        None
    } else {
        Some(winner_batch_ids)
    };

    Ok(Some(ComputedVisiblePreview {
        row,
        winner_batch_ids,
    }))
}

pub(crate) fn visible_row_preview_from_history_rows(
    user_descriptor: &RowDescriptor,
    history_rows: &[StoredRowBatch],
    required_tier: Option<DurabilityTier>,
) -> Result<Option<StoredRowBatch>, EncodingError> {
    Ok(
        build_computed_visible_preview(user_descriptor, history_rows, required_tier)?
            .map(|preview| preview.row),
    )
}

#[derive(Debug, Clone)]
pub(super) struct RowBatchApply {
    row_locator: RowLocator,
    previous_visible: Option<StoredRowBatch>,
    current_visible: Option<StoredRowBatch>,
    is_new_object: bool,
    visible_changed: bool,
}

pub(super) fn row_locator_from_storage<H: Storage>(
    io: &H,
    object_id: ObjectId,
) -> Result<RowLocator, RowHistoryError> {
    io.load_row_locator(object_id)
        .map_err(RowHistoryError::StorageError)?
        .ok_or(RowHistoryError::ObjectNotFound(object_id))
}

pub(super) fn load_branch_history<H: Storage>(
    io: &H,
    table: &str,
    object_id: ObjectId,
    branch_name: &SharedString,
) -> Result<Vec<StoredRowBatch>, RowHistoryError> {
    io.scan_history_region(
        table,
        branch_name.as_str(),
        HistoryScan::Row { row_id: object_id },
    )
    .map_err(RowHistoryError::StorageError)
}

pub(super) fn rebuild_visible_entry_from_history<H: Storage>(
    io: &H,
    table: &str,
    object_id: ObjectId,
    branch_name: &SharedString,
    user_descriptor: &RowDescriptor,
) -> Result<Option<VisibleRowEntry>, RowHistoryError> {
    let history_rows = load_branch_history(io, table, object_id, branch_name)?;
    visible_entry_from_history_rows(user_descriptor, &history_rows).map_err(|err| {
        RowHistoryError::StorageError(StorageError::IoError(format!(
            "rebuild visible entry: {err}"
        )))
    })
}

pub(super) fn visible_entry_from_history_rows(
    user_descriptor: &RowDescriptor,
    history_rows: &[StoredRowBatch],
) -> Result<Option<VisibleRowEntry>, EncodingError> {
    VisibleRowEntry::rebuild_with_descriptor(user_descriptor, history_rows)
}

pub(super) fn load_previous_visible_entry<H: Storage>(
    io: &H,
    table: &str,
    object_id: ObjectId,
    branch_name: &SharedString,
    user_descriptor: &RowDescriptor,
) -> Result<Option<VisibleRowEntry>, RowHistoryError> {
    match io.load_visible_region_entry(table, branch_name.as_str(), object_id) {
        Ok(Some(entry)) => Ok(Some(entry)),
        Ok(None) => {
            rebuild_visible_entry_from_history(io, table, object_id, branch_name, user_descriptor)
        }
        Err(_) => {
            rebuild_visible_entry_from_history(io, table, object_id, branch_name, user_descriptor)
        }
    }
}

pub(super) fn visibility_change_from_applied(
    object_id: ObjectId,
    applied: RowBatchApply,
) -> Option<RowVisibilityChange> {
    if !applied.visible_changed {
        return None;
    }

    let current_visible = applied.current_visible?;
    Some(RowVisibilityChange {
        object_id,
        row_locator: applied.row_locator,
        row: current_visible,
        previous_row: applied.previous_visible,
        is_new_object: applied.is_new_object,
    })
}

pub(super) fn supersede_older_staging_rows_for_batch<H: Storage>(
    io: &mut H,
    table: &str,
    object_id: ObjectId,
    branch_name: &BranchName,
    batch_id: BatchId,
) -> Result<(), RowHistoryError> {
    let branch = SharedString::from(branch_name.as_str().to_string());
    let history_rows = load_branch_history(io, table, object_id, &branch)?;
    let mut pending_rows = history_rows
        .into_iter()
        .filter(|row| row.batch_id == batch_id && matches!(row.state, RowState::StagingPending))
        .collect::<Vec<_>>();

    if pending_rows.len() <= 1 {
        return Ok(());
    }

    pending_rows.sort_by_key(|row| (row.updated_at, row.batch_id()));
    pending_rows.pop();

    for row in pending_rows {
        let _ = patch_row_batch_state(
            io,
            object_id,
            branch_name,
            row.batch_id(),
            Some(RowState::Superseded),
            None,
        )?;
    }

    Ok(())
}

pub fn apply_row_batch<H: Storage>(
    io: &mut H,
    object_id: ObjectId,
    branch_name: &BranchName,
    row: StoredRowBatch,
    index_mutations: &[IndexMutation<'_>],
) -> Result<ApplyRowBatchResult, RowHistoryError> {
    let row_locator = row_locator_from_storage(io, object_id)?;
    let table = row_locator.table.to_string();
    let batch_id = row.batch_id();
    let branch = SharedString::from(branch_name.as_str().to_string());
    let context = crate::storage::resolve_history_row_write_context(io, &table, &row)
        .map_err(RowHistoryError::StorageError)?;
    let previous_entry = load_previous_visible_entry(
        io,
        &table,
        object_id,
        &branch,
        context.user_descriptor.as_ref(),
    )?;
    let previous_visible = previous_entry
        .as_ref()
        .map(|entry| entry.current_row.clone());

    for parent in &row.parents {
        if io
            .load_history_row_batch(&table, branch_name.as_str(), object_id, *parent)
            .map_err(RowHistoryError::StorageError)?
            .is_none()
        {
            return Err(RowHistoryError::ParentNotFound(*parent));
        }
    }

    let mut patched_history = load_branch_history(io, &table, object_id, &branch)?;

    if let Some(existing_row) = io
        .load_history_row_batch(&table, branch_name.as_str(), object_id, batch_id)
        .map_err(RowHistoryError::StorageError)?
        && existing_row == row
    {
        return Ok(ApplyRowBatchResult {
            batch_id,
            row_locator,
            visibility_change: None,
        });
    }
    if let Some(existing) = patched_history
        .iter_mut()
        .find(|candidate| candidate.batch_id() == batch_id)
    {
        *existing = row.clone();
    } else {
        patched_history.push(row.clone());
    }
    let current_entry =
        visible_entry_from_history_rows(context.user_descriptor.as_ref(), &patched_history)
            .map_err(|err| {
                RowHistoryError::StorageError(StorageError::IoError(format!(
                    "rebuild visible entry after append: {err}"
                )))
            })?;
    let current_visible = current_entry
        .as_ref()
        .map(|entry| entry.current_row.clone());
    let visible_entry_changed = current_entry.as_ref() != previous_entry.as_ref();
    let visible_entries: &[VisibleRowEntry] = match (visible_entry_changed, current_entry.as_ref())
    {
        (true, Some(entry)) => std::slice::from_ref(entry),
        _ => &[],
    };
    let visible_changed = previous_visible != current_visible;
    let can_encode_visible_with_row_context = visible_entries.len() == 1
        && visible_entries[0].current_row.row_id == row.row_id
        && visible_entries[0].current_row.branch == row.branch
        && visible_entries[0].current_row.batch_id() == row.batch_id();

    if visible_entries.is_empty() || can_encode_visible_with_row_context {
        let encoded_history = crate::storage::encode_history_row_bytes_with_context(&context, &row)
            .map_err(RowHistoryError::StorageError)?;
        let encoded_visible = if let Some(entry) = visible_entries.first() {
            vec![
                crate::storage::encode_visible_row_bytes_with_context(&context, entry)
                    .map_err(RowHistoryError::StorageError)?,
            ]
        } else {
            Vec::new()
        };
        <H as Storage>::apply_prepared_row_mutation(
            io,
            &table,
            std::slice::from_ref(&row),
            visible_entries,
            std::slice::from_ref(&encoded_history),
            &encoded_visible,
            index_mutations,
        )
        .map_err(RowHistoryError::StorageError)?;
    } else {
        <H as Storage>::apply_row_mutation(
            io,
            &table,
            std::slice::from_ref(&row),
            visible_entries,
            index_mutations,
        )
        .map_err(RowHistoryError::StorageError)?;
    }

    if matches!(row.state, RowState::StagingPending) {
        supersede_older_staging_rows_for_batch(io, &table, object_id, branch_name, row.batch_id)?;
    }

    let applied = RowBatchApply {
        row_locator: row_locator.clone(),
        previous_visible: previous_visible.clone(),
        current_visible,
        is_new_object: previous_visible.is_none(),
        visible_changed,
    };

    Ok(ApplyRowBatchResult {
        batch_id,
        row_locator,
        visibility_change: visibility_change_from_applied(object_id, applied),
    })
}

pub fn patch_row_batch_state<H: Storage>(
    io: &mut H,
    object_id: ObjectId,
    branch_name: &BranchName,
    batch_id: BatchId,
    state: Option<RowState>,
    confirmed_tier: Option<DurabilityTier>,
) -> Result<Option<RowVisibilityChange>, RowHistoryError> {
    let row_locator = row_locator_from_storage(io, object_id)?;
    let table = row_locator.table.to_string();
    let branch = SharedString::from(branch_name.as_str().to_string());
    let mut patched_row = io
        .load_history_row_batch(&table, branch_name.as_str(), object_id, batch_id)
        .map_err(RowHistoryError::StorageError)?
        .ok_or(RowHistoryError::ObjectNotFound(object_id))?;
    if patched_row.branch.as_str() != branch_name.as_str() {
        return Ok(None);
    }
    let context = crate::storage::resolve_history_row_write_context(io, &table, &patched_row)
        .map_err(RowHistoryError::StorageError)?;
    let previous_entry = load_previous_visible_entry(
        io,
        &table,
        object_id,
        &branch,
        context.user_descriptor.as_ref(),
    )?;
    let previous_visible = previous_entry
        .as_ref()
        .map(|entry| entry.current_row.clone());

    if let Some(state) = state {
        patched_row.state = state;
    }
    patched_row.confirmed_tier = match (patched_row.confirmed_tier, confirmed_tier) {
        (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
        (Some(existing), None) => Some(existing),
        (None, incoming) => incoming,
    };

    let mut history_rows = load_branch_history(io, &table, object_id, &branch)?;
    let Some(existing) = history_rows
        .iter_mut()
        .find(|candidate| candidate.batch_id() == batch_id)
    else {
        return Err(RowHistoryError::ObjectNotFound(object_id));
    };
    *existing = patched_row.clone();
    let patched_entry =
        visible_entry_from_history_rows(context.user_descriptor.as_ref(), &history_rows).map_err(
            |err| {
                RowHistoryError::StorageError(StorageError::IoError(format!(
                    "rebuild visible entry after patch: {err}"
                )))
            },
        )?;
    let visible_entries: Vec<_> = patched_entry.iter().cloned().collect();
    if patched_entry.is_some() {
        io.apply_row_mutation(
            &table,
            std::slice::from_ref(&patched_row),
            &visible_entries,
            &[],
        )
        .map_err(RowHistoryError::StorageError)?;
    } else {
        io.append_history_region_rows(&table, std::slice::from_ref(&patched_row))
            .map_err(RowHistoryError::StorageError)?;
        io.delete_visible_region_row(&table, branch_name.as_str(), object_id)
            .map_err(RowHistoryError::StorageError)?;
    }

    let current_visible = patched_entry
        .as_ref()
        .map(|entry| entry.current_row.clone());
    if previous_visible == current_visible {
        return Ok(None);
    }

    let Some(current_visible) = current_visible else {
        return Ok(None);
    };

    Ok(Some(RowVisibilityChange {
        object_id,
        row_locator,
        row: current_visible,
        previous_row: previous_visible.clone(),
        is_new_object: previous_visible.is_none(),
    }))
}

pub(super) fn latest_visible_version_for_tier(
    history_rows: &[StoredRowBatch],
    required_tier: DurabilityTier,
) -> Option<BatchId> {
    history_rows
        .iter()
        .filter(|row| row.state.is_visible() && tier_satisfies(row.confirmed_tier, required_tier))
        .max_by_key(|row| (row.updated_at, row.batch_id()))
        .map(StoredRowBatch::batch_id)
}

pub(super) fn branch_frontier(history_rows: &[StoredRowBatch]) -> Vec<BatchId> {
    let mut non_tips = std::collections::BTreeSet::new();
    for row in history_rows.iter().filter(|row| row.state.is_visible()) {
        for parent in &row.parents {
            non_tips.insert(*parent);
        }
    }

    let mut tips: Vec<_> = history_rows
        .iter()
        .filter(|row| row.state.is_visible())
        .map(StoredRowBatch::batch_id)
        .filter(|batch_id| !non_tips.contains(batch_id))
        .collect();
    tips.sort();
    tips.dedup();
    tips
}
