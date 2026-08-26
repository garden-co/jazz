//! Commit, fate, and sync-message ingestion for a storage-backed
//! node. This module owns mutation paths that validate incoming transactions,
//! apply authority fates, park/unpark causally blocked units, and write node
//! state into groove; read-only global derivations live in [`super::global_state`],
//! policy evaluation in [`super::policy`], and byte-level record construction in
//! [`super::codec`]. It is the node layer's write side below the `Db` facade and
//! protocol sync loop. Trusted catalogue snapshot activation lives in the
//! sibling [`super::catalogue_ingest`] module.

use super::*;
use crate::protocol::{CatalogueAck, LensOp, SchemaLineagePublication, VersionBundleRef};
use crate::protocol_limits::{
    commit_unit_limit_violation, validate_known_state_declaration, validate_shape_registration_size,
};
use crate::schema::{ColumnSchema, MERGE_HEADS_TABLE};
use groove::records::ValueType;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) const MAX_SCHEMA_LINEAGE_DECLARATIONS: usize = 4096;
pub(super) const MAX_SCHEMA_LINEAGE_NAME_BYTES: usize = 1024;
pub(super) const MAX_SCHEMA_LINEAGE_OPS: usize = 16_384;

#[cfg(test)]
static MERGE_HEAD_REACHABILITY_WALKS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub(super) fn reset_merge_head_reachability_walks_for_test() {
    MERGE_HEAD_REACHABILITY_WALKS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(super) fn merge_head_reachability_walks_for_test() -> usize {
    MERGE_HEAD_REACHABILITY_WALKS.load(Ordering::Relaxed)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CommitUnitParkMode {
    ingest_context: Option<CommitUnitIngestContext>,
    ingress_role: ParkedIngressRole,
}

impl Default for CommitUnitParkMode {
    fn default() -> Self {
        Self {
            ingest_context: None,
            ingress_role: ParkedIngressRole::Authority,
        }
    }
}

include!("ingest/catalogue.rs");
include!("ingest/commit_bundles.rs");
include!("ingest/fates.rs");
include!("ingest/view_updates.rs");
include!("ingest/validation.rs");

/// A sequence is the global-authority receipt. Peer payloads which pair it
/// with a weaker durability must be rejected before they can reach storage.
pub(super) fn validate_received_fate_update_global_time_durability(
    global_time: Option<GlobalTime>,
    durability: Option<DurabilityTier>,
) -> Result<(), Error> {
    if global_time.is_some() && durability != Some(DurabilityTier::Global) {
        return Err(Error::UnsupportedSyncMessage(
            "global timestamp requires Global durability",
        ));
    }
    Ok(())
}

/// View bundles are peer payloads too, including reset bundles eligible for
/// bulk persistence.
pub(super) fn validate_received_view_bundle_global_time_durability(
    global_time: Option<GlobalTime>,
    durability: DurabilityTier,
) -> Result<(), Error> {
    if global_time.is_some() && durability != DurabilityTier::Global {
        return Err(Error::MalformedViewUpdate(
            "global timestamp requires Global durability",
        ));
    }
    Ok(())
}

fn validate_transform_column(column: Option<&ColumnSchema>, transform: &str) -> Result<(), Error> {
    validate_registered_transform(transform)?;
    let Some(_) = column else {
        return Err(Error::InvalidCatalogueUpdate("transform column is unknown"));
    };
    Ok(())
}

fn fate_update_durability_claim(fate: &Fate, durability: DurabilityTier) -> Option<DurabilityTier> {
    match fate {
        Fate::Rejected(_) => None,
        Fate::Pending | Fate::Accepted => Some(durability),
    }
}

fn commit_unit_write_count_matches(tx: &Transaction, version_count: usize) -> bool {
    usize::try_from(tx.n_total_writes) == Ok(version_count)
}

fn view_version_key_for_ingest(
    version: &VersionRecord,
) -> (String, BranchKey, RowUuid, VersionLayer) {
    (
        version.table().to_owned(),
        version.branch_key().clone(),
        version.row_uuid(),
        VersionLayer::for_record(version),
    )
}

fn content_version_reaches_tx_in_staged_parents(
    start: TxId,
    target: TxId,
    parents_by_tx: &BTreeMap<TxId, Vec<TxId>>,
) -> Option<bool> {
    if !parents_by_tx.contains_key(&start) {
        return None;
    }
    let mut stack = vec![start];
    let mut seen = BTreeSet::new();
    while let Some(tx_id) = stack.pop() {
        if tx_id == target {
            return Some(true);
        }
        if !seen.insert(tx_id) {
            continue;
        }
        let Some(parents) = parents_by_tx.get(&tx_id) else {
            continue;
        };
        stack.extend(parents.iter().copied());
    }
    Some(false)
}

pub(super) fn counter_merge_value(
    table_schema: &TableSchema,
    column: &str,
    row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    tx_ids: &[TxId],
    memo: &mut BTreeMap<Vec<TxId>, i128>,
) -> Result<i128, Error> {
    let mut key = tx_ids.to_vec();
    key.sort();
    key.dedup();
    key = counter_head_tx_ids(row_versions_by_tx, &key);
    if key.is_empty() {
        return Ok(0);
    }
    if let Some(value) = memo.get(&key) {
        return Ok(*value);
    }

    let parent_union = key
        .iter()
        .map(|tx_id| {
            row_versions_by_tx
                .get(tx_id)
                .ok_or(Error::MissingTransaction(*tx_id))
        })
        .collect::<Result<Vec<_>, Error>>()?
        .into_iter()
        .flat_map(VersionRow::parents)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut merged = counter_merge_value(
        table_schema,
        column,
        row_versions_by_tx,
        &parent_union,
        memo,
    )?;

    for tx_id in &key {
        let version = row_versions_by_tx
            .get(tx_id)
            .ok_or(Error::MissingTransaction(*tx_id))?;
        let Some(value) = version.cell(table_schema, column)? else {
            continue;
        };
        let parent_value = counter_merge_value(
            table_schema,
            column,
            row_versions_by_tx,
            &version.parents(),
            memo,
        )?;
        merged += counter_value_to_i128(&value)? - parent_value;
    }
    memo.insert(key, merged);
    Ok(merged)
}

fn counter_head_tx_ids(
    row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    tx_ids: &[TxId],
) -> Vec<TxId> {
    let present = tx_ids.iter().copied().collect::<BTreeSet<_>>();
    let mut dominated = BTreeSet::new();
    for tx_id in tx_ids {
        let Some(version) = row_versions_by_tx.get(tx_id) else {
            continue;
        };
        let mut stack = version.parents();
        let mut seen = BTreeSet::new();
        while let Some(parent) = stack.pop() {
            if !seen.insert(parent) {
                continue;
            }
            if present.contains(&parent) {
                dominated.insert(parent);
            }
            if let Some(parent_version) = row_versions_by_tx.get(&parent) {
                stack.extend(parent_version.parents());
            }
        }
    }
    tx_ids
        .iter()
        .copied()
        .filter(|tx_id| !dominated.contains(tx_id))
        .collect()
}

/// Merge every array value reachable from the current heads.  A GSet is
/// deliberately history-based rather than last-write-based: omitting an
/// element in a later write cannot remove an element introduced by any parent.
/// Elements are keyed and ordered by Groove's deterministic record encoding;
/// this preserves distinct valid float bit patterns such as `+0.0` and `-0.0`.
pub(super) fn gset_merge_value(
    table_schema: &TableSchema,
    column: &str,
    row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    head_tx_ids: &[TxId],
) -> Result<Value, Error> {
    let column_schema = table_schema
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .ok_or(Error::InvalidStoredValue(
            "g-set column is missing from schema",
        ))?;
    let ValueType::Array(element_type) = &column_schema.column_type else {
        return Err(Error::InvalidStoredValue(
            "g-set merge strategy requires an array column",
        ));
    };
    let element_descriptor =
        records::RecordDescriptor::new([("element", element_type.as_ref().clone())]);

    let mut pending = head_tx_ids.to_vec();
    let mut visited = BTreeSet::new();
    let mut elements = BTreeMap::<Vec<u8>, Value>::new();
    while let Some(tx_id) = pending.pop() {
        if !visited.insert(tx_id) {
            continue;
        }
        let version = row_versions_by_tx
            .get(&tx_id)
            .ok_or(Error::MissingTransaction(tx_id))?;
        pending.extend(version.parents());
        let Some(Value::Array(values)) = version.cell(table_schema, column)? else {
            continue;
        };
        for value in values {
            let key = element_descriptor.create(std::slice::from_ref(&value))?;
            elements.entry(key).or_insert(value);
        }
    }
    Ok(Value::Array(elements.into_values().collect()))
}

/// A linear write is materialized only when its GSet cells differ from the
/// union of their ancestry. This prevents a no-op merge version from chaining
/// forever while making an attempted removal immediately restore prior values.
fn gset_cells_need_materialization(
    table_schema: &TableSchema,
    head: &VersionRow,
    merged_cells: &BTreeMap<String, Value>,
) -> Result<bool, Error> {
    for column in table_schema
        .columns
        .iter()
        .filter(|column| table_schema.merge_strategy(&column.name) == MergeStrategy::GSet)
    {
        let Some(current) = head.cell(table_schema, &column.name)? else {
            return Ok(true);
        };
        let Some(merged) = merged_cells.get(&column.name) else {
            return Ok(true);
        };
        let descriptor = records::RecordDescriptor::new([("cell", column.column_type.clone())]);
        if descriptor.create(std::slice::from_ref(&current))?
            != descriptor.create(std::slice::from_ref(merged))?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn raw_merge_head_tx_ids(
    row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    tx_ids: &[TxId],
) -> Result<Vec<TxId>, Error> {
    let mut raw = BTreeSet::new();
    let mut stack = tx_ids.to_vec();
    while let Some(tx_id) = stack.pop() {
        let version = row_versions_by_tx
            .get(&tx_id)
            .ok_or(Error::MissingTransaction(tx_id))?;
        let parents = version.parents();
        if parents.len() >= 2 {
            stack.extend(parents);
        } else {
            raw.insert(tx_id);
        }
    }
    Ok(counter_head_tx_ids(
        row_versions_by_tx,
        &raw.into_iter().collect::<Vec<_>>(),
    ))
}

pub(super) fn counter_value_to_i128(value: &Value) -> Result<i128, Error> {
    match value {
        Value::U8(value) => Ok(i128::from(*value)),
        Value::U16(value) => Ok(i128::from(*value)),
        Value::U32(value) => Ok(i128::from(*value)),
        Value::U64(value) => Ok(i128::from(*value)),
        Value::I32(value) => Ok(i128::from(*value)),
        Value::I64(value) => Ok(i128::from(*value)),
        _ => Err(Error::InvalidStoredValue("counter value must be integer")),
    }
}

pub(super) fn counter_value_from_i128(
    column_type: &groove::schema::ColumnType,
    value: i128,
) -> Result<Value, Error> {
    match column_type {
        groove::schema::ColumnType::U8 => u8::try_from(value)
            .map(Value::U8)
            .map_err(|_| Error::InvalidStoredValue("counter value out of range")),
        groove::schema::ColumnType::U16 => u16::try_from(value)
            .map(Value::U16)
            .map_err(|_| Error::InvalidStoredValue("counter value out of range")),
        groove::schema::ColumnType::U32 => u32::try_from(value)
            .map(Value::U32)
            .map_err(|_| Error::InvalidStoredValue("counter value out of range")),
        groove::schema::ColumnType::U64 => u64::try_from(value)
            .map(Value::U64)
            .map_err(|_| Error::InvalidStoredValue("counter value out of range")),
        groove::schema::ColumnType::I32 => i32::try_from(value)
            .map(Value::I32)
            .map_err(|_| Error::InvalidStoredValue("counter value out of range")),
        groove::schema::ColumnType::I64 => i64::try_from(value)
            .map(Value::I64)
            .map_err(|_| Error::InvalidStoredValue("counter value out of range")),
        _ => Err(Error::InvalidStoredValue(
            "counter strategy requires integer column",
        )),
    }
}
