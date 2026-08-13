//! Subscription result-set and wire-update delivery helpers.
//!
//! These pure transformations sit below [`super::PeerState`]'s transport
//! lifecycle: they maintain the deduplicated result-member set and shape the
//! protocol message details emitted from it.

use super::*;

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
    member.output_occurrence_id()
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

fn member_content_tx(member: &ResultMemberEntry) -> Option<TxId> {
    member.as_row().map(|(_, _, tx_id)| tx_id)
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
            Some(slot)
                if member_content_tx(member)
                    .zip(member_content_tx(&slot.member))
                    .is_some_and(|(new_tx, old_tx)| new_tx > old_tx) =>
            {
                let old_member = slot.member.clone();
                result_member_removes.push(old_member.clone());
                result_member_adds.push(member.clone());
                state.result_member_set.remove(&old_member);
                state.result_member_set.insert(member.clone());
                slot.member = member.clone();
                slot.refcount += 1;
            }
            Some(slot)
                if slot.member != *member
                    && matches!(member, ResultMemberEntry::Synthetic { .. }) =>
            {
                let old_member = slot.member.clone();
                result_member_removes.push(old_member.clone());
                result_member_adds.push(member.clone());
                state.result_member_set.remove(&old_member);
                state.result_member_set.insert(member.clone());
                slot.member = member.clone();
                slot.refcount += 1;
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
pub(super) fn duplicate_output_occurrence_result_set(
    result_set: &BTreeSet<ResultMemberEntry>,
) -> Option<(OutputOccurrenceId, TxId, TxId)> {
    let mut rows = BTreeMap::new();
    for member in result_set {
        let Some(occurrence_id) = member.output_occurrence_id() else {
            continue;
        };
        let Some((_, _, tx_id)) = member.as_row() else {
            continue;
        };
        if let Some(first) = rows.insert(occurrence_id.clone(), tx_id) {
            return Some((occurrence_id, first, tx_id));
        }
    }
    None
}

pub(super) fn bundle_contains_complete_tx_payload(bundle: &VersionBundle) -> bool {
    usize::try_from(bundle.tx.n_total_writes).ok() == Some(bundle.versions.len())
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
    let SyncMessage::ViewUpdate {
        reset_result_set, ..
    } = update
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
