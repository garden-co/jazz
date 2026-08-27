//! Subscription result-set and wire-update delivery helpers.
//!
//! These pure transformations sit below [`super::PeerState`]'s transport
//! lifecycle: they maintain the deduplicated result-member set and shape the
//! protocol message details emitted from it.

use super::*;
#[cfg(debug_assertions)]
use crate::ids::RowUuid;
#[cfg(debug_assertions)]
use crate::tools::OutputOccurrenceId;

pub(super) fn maintained_view_update_is_empty(
    result_member_adds: &[ResultMemberEntry],
    result_member_removes: &[ResultMemberEntry],
    terminal_operations: &[groove::ivm::TerminalOperation],
    program_fact_adds: &[ProgramFactEntry],
    program_fact_removes: &[ProgramFactEntry],
) -> bool {
    result_member_adds.is_empty()
        && result_member_removes.is_empty()
        && terminal_operations.is_empty()
        && program_fact_adds.is_empty()
        && program_fact_removes.is_empty()
}

fn member_row_key(member: &ResultMemberEntry) -> Option<RowKey> {
    let row = member.as_real_row()?;
    Some((
        member.output_occurrence_id()?,
        row.table.clone(),
        row.row_uuid,
    ))
}

fn member_index_key(member: &ResultMemberEntry) -> MemberIndexKey {
    if let Some(row) = member_row_key(member) {
        return MemberIndexKey::Row(row);
    }
    match member {
        ResultMemberEntry::Synthetic { table, row, .. } => MemberIndexKey::Synthetic {
            table: table.clone(),
            row: row.clone(),
        },
        _ => MemberIndexKey::Member(member.clone()),
    }
}

pub(super) fn result_member_replaces(
    replacement: &ResultMemberEntry,
    previous: &ResultMemberEntry,
) -> bool {
    if member_index_key(replacement) != member_index_key(previous) || replacement == previous {
        return false;
    }
    match (replacement.as_row(), previous.as_row()) {
        (
            Some((replacement_table, replacement_row, _)),
            Some((previous_table, previous_row, _)),
        ) => replacement_table == previous_table && replacement_row == previous_row,
        _ => matches!(replacement, ResultMemberEntry::Synthetic { .. }),
    }
}

pub(super) fn replacement_removals(
    previous: &PeerSubscriptionState,
    additions: &[ResultMemberEntry],
) -> Vec<ResultMemberEntry> {
    additions
        .iter()
        .filter_map(|added| {
            previous
                .member_index
                .get(&member_index_key(added))
                .map(|slot| &slot.member)
                .filter(|old| result_member_replaces(added, old))
        })
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(super) fn filter_program_facts_for_result_table(
    facts: Vec<ProgramFactEntry>,
    result_table_filter: Option<&str>,
    output_tables: &BTreeMap<String, TableSchema>,
) -> Vec<ProgramFactEntry> {
    facts
        .into_iter()
        .filter(|fact| match fact {
            ProgramFactEntry::ResultPayload(payload) => {
                let Some(table_name) = payload.member.table_name() else {
                    return false;
                };
                matches!(payload.member, ResultMemberEntry::Synthetic { .. })
                    || (result_table_filter.is_none_or(|table| table_name == table)
                        && output_tables.contains_key(table_name))
            }
            _ => true,
        })
        .collect()
}

pub(super) fn apply_contribution_add<'a>(
    state: &mut PeerSubscriptionState,
    contribution: impl IntoIterator<Item = &'a ResultMemberEntry>,
    result_member_adds: &mut Vec<ResultMemberEntry>,
    result_member_removes: &mut Vec<ResultMemberEntry>,
) {
    for member in contribution {
        let key = member_index_key(member);
        match state.member_index.get_mut(&key) {
            Some(slot) if slot.member == *member => slot.refcount += 1,
            Some(slot) if result_member_replaces(member, &slot.member) => {
                let old_member = slot.member.clone();
                result_member_removes.push(old_member.clone());
                result_member_adds.push(member.clone());
                state.result_member_set.remove(&old_member);
                state.result_member_set.insert(member.clone());
                slot.member = member.clone();
            }
            Some(slot) => slot.refcount += 1,
            None => {
                state.member_index.insert(
                    key,
                    MemberSlot {
                        member: member.clone(),
                        refcount: 1,
                    },
                );
                result_member_adds.push(member.clone());
                state.result_member_set.insert(member.clone());
            }
        }
    }
}

pub(super) fn apply_contribution_remove<'a>(
    state: &mut PeerSubscriptionState,
    contribution: impl IntoIterator<Item = &'a ResultMemberEntry>,
    result_member_removes: &mut Vec<ResultMemberEntry>,
) {
    for member in contribution {
        let key = member_index_key(member);
        let Some(slot) = state.member_index.get_mut(&key) else {
            continue;
        };
        if slot.refcount > 1 {
            slot.refcount -= 1;
        } else {
            let removed = slot.member.clone();
            state.member_index.remove(&key);
            result_member_removes.push(removed.clone());
            state.result_member_set.remove(&removed);
        }
    }
}

#[cfg(debug_assertions)]
pub(super) fn duplicate_physical_row_result_set(
    result_set: &BTreeSet<ResultMemberEntry>,
) -> Option<(
    (OutputOccurrenceId, groove::Intern<String>, RowUuid),
    TxId,
    TxId,
)> {
    let mut rows = BTreeMap::new();
    for member in result_set {
        let Some(row_key) = member_row_key(member) else {
            continue;
        };
        let Some((_, _, tx_id)) = member.as_row() else {
            continue;
        };
        if let Some(first) = rows.insert(row_key.clone(), tx_id) {
            return Some((row_key, first, tx_id));
        }
    }
    None
}

pub(super) fn bundle_contains_complete_tx_payload(bundle: &VersionBundle) -> bool {
    bundle.scope == crate::protocol::VersionBundleScope::CompleteTransaction
}

pub(super) fn view_update_singleton_bundles(
    version_carriers: &[VersionCarrier],
    version_bundles: &[VersionBundle],
) -> Vec<VersionBundle> {
    let mut bundles = version_bundles.to_vec();
    if let Ok(mut expanded) = expand_version_carriers(version_carriers) {
        bundles.append(&mut expanded);
    }
    bundles
}

pub(super) fn storage_read_metrics_buckets(metrics: &StorageReadMetrics) -> String {
    [
        ("history_rows", metrics.history_rows),
        ("history_indexes", metrics.history_indexes),
        ("ahead_current_rows", metrics.ahead_current_rows),
        ("global_current_rows", metrics.global_current_rows),
        ("global_current_indexes", metrics.global_current_indexes),
        (
            "register_global_current_rows",
            metrics.register_global_current_rows,
        ),
        ("global_changes_rows", metrics.global_changes_rows),
        ("global_changes_indexes", metrics.global_changes_indexes),
        ("transactions_rows", metrics.transactions_rows),
        ("transactions_indexes", metrics.transactions_indexes),
        ("other", metrics.other),
    ]
    .into_iter()
    .map(|(name, bucket)| storage_read_bucket_field(name, bucket))
    .collect::<Vec<_>>()
    .join(",")
}

fn storage_read_bucket_field(name: &str, bucket: StorageReadBucket) -> String {
    format!(
        "{name}.reads={}:{}.ranges={}",
        bucket.reads, name, bucket.ranges
    )
}

pub(super) fn view_update_reset_result_set(update: &mut SyncMessage) {
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set, ..
    }) = update
    else {
        return;
    };
    *reset_result_set = true;
}

pub(super) fn binding_values_in_param_order(
    shape: &ValidatedQuery,
    binding: &Binding,
) -> Vec<groove::records::Value> {
    shape
        .params()
        .keys()
        .map(|name| {
            binding
                .values()
                .get(name)
                .cloned()
                .expect("validated binding contains every shape param")
        })
        .collect()
}
