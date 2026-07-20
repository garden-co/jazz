//! Visibility resolution: turn a set of history rows into the row a reader sees.
//!
//! Pure data transformations — nothing in this module reads or writes storage.
//! Mutations call into here after each write to recompute visibility, but the
//! algorithms themselves are independent and individually unit-testable.
//!
//! The model in one paragraph: a row's history is a DAG of `StoredRowBatch`
//! versions. The visible preview is computed from the *frontier* (tips with no
//! visible descendant) by walking down to the latest common ancestor and
//! merging column-by-column under the column's `ColumnMergeStrategy`. A
//! delete-winner check (hard > soft > none) overlays the merged values; the
//! resulting `StoredRowBatch` is what readers see.
//!
//! Key entry points:
//! - [`build_computed_visible_preview`] — full preview + per-column winner trail
//! - [`visible_row_preview_from_history_rows`] — preview only (drops the trail)
//! - [`visible_entry_from_history_rows`] — preview wrapped in `VisibleRowEntry`
//! - [`branch_frontier`], [`latest_visible_version_for_tier`] — frontier/version
//!   queries used by `VisibleRowEntry::rebuild_*`
//! - [`merge_column_with_strategy`], [`assign_winner_ordinals`],
//!   [`preview_override_sidecar`] — building blocks reused by `types.rs` when
//!   encoding the visible-row sidecar.

use std::collections::{BTreeMap, HashMap};

use crate::metadata::DeleteKind;
use crate::query_manager::types::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, RowDescriptor, Value,
};
use crate::row_format::{EncodingError, decode_row, encode_row, encode_value_with_type};
use crate::sync_manager::DurabilityTier;

use super::codecs::{flat_user_values, malformed, tier_satisfies};
use super::types::{BatchId, ComputedVisiblePreview, StoredRowBatch, VisibleRowEntry};

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

pub(crate) fn has_counter_merge_strategy(descriptor: &RowDescriptor) -> bool {
    descriptor
        .columns
        .iter()
        .any(|column| matches!(column.merge_strategy, Some(ColumnMergeStrategy::Counter)))
}

pub(crate) fn rebase_counter_data(
    descriptor: &RowDescriptor,
    source_base_data: &[u8],
    source_result_data: &[u8],
    target_base_data: &[u8],
) -> Result<Option<Vec<u8>>, EncodingError> {
    if !has_counter_merge_strategy(descriptor) {
        return Ok(None);
    }

    let source_base_values = decode_row(descriptor, source_base_data)?;
    let source_result_values = decode_row(descriptor, source_result_data)?;
    let target_base_values = decode_row(descriptor, target_base_data)?;
    let mut rebased_values = source_result_values.clone();

    let counter_value = |column: &ColumnDescriptor, value: &Value| match value {
        Value::Integer(value) => Ok(*value),
        Value::Null => Ok(0),
        other => Err(malformed(format!(
            "counter rebase expected INTEGER for column '{}', got {:?}",
            column.name_str(),
            other
        ))),
    };

    for (index, column) in descriptor.columns.iter().enumerate() {
        if !matches!(column.merge_strategy, Some(ColumnMergeStrategy::Counter)) {
            continue;
        }

        let source_base = counter_value(column, &source_base_values[index])?;
        let source_result = counter_value(column, &source_result_values[index])?;
        let target_base = counter_value(column, &target_base_values[index])?;
        let delta = source_result.checked_sub(source_base).ok_or_else(|| {
            malformed(format!(
                "counter rebase delta overflow for column '{}'",
                column.name_str()
            ))
        })?;
        let rebased = target_base.checked_add(delta).ok_or_else(|| {
            malformed(format!(
                "counter rebase overflow for column '{}'",
                column.name_str()
            ))
        })?;
        rebased_values[index] = Value::Integer(rebased);
    }

    if rebased_values == source_result_values {
        return Ok(None);
    }

    encode_row(descriptor, &rebased_values).map(Some)
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
        Some(ColumnMergeStrategy::GSet) => {
            let element_type = column.column_type.element_type().ok_or_else(|| {
                malformed(format!(
                    "g-set merge expected ARRAY column for '{}', got {:?}",
                    column.name_str(),
                    column.column_type
                ))
            })?;

            let mut elements: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
            collect_set_elements(column, element_type, ancestor_value, &mut elements)?;
            for contender in contenders {
                collect_set_elements(column, element_type, contender.value, &mut elements)?;
            }
            let merged = elements.into_values().collect();

            let latest_contributor = contenders
                .iter()
                .max_by_key(|contender| (contender.row.updated_at, contender.row.batch_id()))
                .map(|contender| contender.row);

            Ok((Value::Array(merged), latest_contributor))
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

fn collect_set_elements(
    column: &ColumnDescriptor,
    element_type: &ColumnType,
    value: &Value,
    out: &mut BTreeMap<Vec<u8>, Value>,
) -> Result<(), EncodingError> {
    match value {
        Value::Array(elements) => {
            for element in elements {
                out.entry(encode_value_with_type(element, element_type))
                    .or_insert_with(|| element.clone());
            }
            Ok(())
        }
        Value::Null => Ok(()),
        other => Err(malformed(format!(
            "g-set merge expected ARRAY value for column '{}', got {:?}",
            column.name_str(),
            other
        ))),
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

    let ancestor_values = ancestor
        .map(|row| flat_user_values(user_descriptor, &row.data))
        .transpose()?;
    let frontier_values = frontier
        .iter()
        .map(|row| flat_user_values(user_descriptor, &row.data))
        .collect::<Result<Vec<_>, _>>()?;

    let mut merged_values = Vec::with_capacity(user_descriptor.columns.len());
    let mut contributing_rows: Vec<&StoredRowBatch> = Vec::new();
    let mut winner_batch_ids = Vec::with_capacity(user_descriptor.columns.len());
    let null_ancestor = Value::Null;

    for column_index in 0..user_descriptor.columns.len() {
        let column = &user_descriptor.columns[column_index];
        let ancestor_value = ancestor_values
            .as_ref()
            .map(|values| &values[column_index])
            .unwrap_or(&null_ancestor);
        let changed_contenders = frontier
            .iter()
            .zip(frontier_values.iter())
            .filter_map(|(row, row_values)| {
                let candidate_value = &row_values[column_index];
                let changed_from_ancestor = ancestor_values
                    .as_ref()
                    .map(|values| candidate_value != &values[column_index])
                    .unwrap_or_else(|| {
                        // With no common ancestor, Null is an explicit value, not "unchanged from absence".
                        // The only exception is counters: for counter merge logic, we don’t want a
                        // missing/no-ancestor snapshot to look like a counter update of “null”.
                        !matches!(
                            (column.merge_strategy, candidate_value),
                            (Some(ColumnMergeStrategy::Counter), Value::Null)
                                | (Some(ColumnMergeStrategy::GSet), Value::Null)
                        )
                    });
                changed_from_ancestor.then_some(ColumnContender {
                    row,
                    value: candidate_value,
                })
            })
            .collect::<Vec<_>>();
        let (best_value, best_changed) =
            merge_column_with_strategy(column, ancestor_value, &changed_contenders)?;

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

pub(super) fn visible_entry_from_history_rows(
    user_descriptor: &RowDescriptor,
    history_rows: &[StoredRowBatch],
) -> Result<Option<VisibleRowEntry>, EncodingError> {
    VisibleRowEntry::rebuild_with_descriptor(user_descriptor, history_rows)
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
