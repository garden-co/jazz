use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use serde::{Serialize, de::DeserializeOwned};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::row_regions::{BatchId, HistoryScan, RowState, StoredRowVersion};
use crate::sync_manager::DurabilityTier;

use crate::query_manager::types::Value;

use super::key_codec::{
    ack_key, branch_tips_key, catalogue_manifest_op_key, catalogue_manifest_op_prefix, commit_key,
    commit_prefix, history_row_key, history_row_prefix, history_row_versions_prefix,
    history_table_prefix, index_entry_key, index_prefix, index_range_scan_bounds,
    index_value_prefix, obj_meta_key, parse_uuid_from_index_key, visible_row_key,
    visible_row_prefix, visible_table_prefix,
};
use super::{CatalogueManifest, CatalogueManifestOp, LoadedBranch, StorageError};

pub(super) fn encode_json<T: Serialize>(value: &T, label: &str) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(value).map_err(|e| StorageError::IoError(format!("serialize {label}: {e}")))
}

pub(super) fn decode_json<T: DeserializeOwned>(
    bytes: &[u8],
    label: &str,
) -> Result<T, StorageError> {
    serde_json::from_slice(bytes)
        .map_err(|e| StorageError::IoError(format!("deserialize {label}: {e}")))
}

pub(super) fn create_object_core(
    id: ObjectId,
    metadata: HashMap<String, String>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let key = obj_meta_key(id);
    let json = encode_json(&metadata, "metadata")?;
    set(&key, &json)
}

pub(super) fn load_object_metadata_core(
    id: ObjectId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<HashMap<String, String>>, StorageError> {
    let key = obj_meta_key(id);
    match get(&key)? {
        Some(data) => Ok(Some(decode_json(&data, "metadata")?)),
        None => Ok(None),
    }
}

pub(super) fn load_branch_core(
    object_id: ObjectId,
    branch: &BranchName,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut scan_prefix: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<Option<LoadedBranch>, StorageError> {
    let meta_key = obj_meta_key(object_id);
    if get(&meta_key)?.is_none() {
        return Ok(None);
    }

    let commit_prefix = commit_prefix(object_id, branch);
    let commit_entries = scan_prefix(&commit_prefix)?;

    if commit_entries.is_empty() {
        let tips_key = branch_tips_key(object_id, branch);
        if get(&tips_key)?.is_none() {
            return Ok(None);
        }
    }

    let mut commits = Vec::new();
    for (_key, data) in &commit_entries {
        let mut commit: Commit = decode_json(data, "commit")?;

        let ack_lookup_key = ack_key(commit.id());
        if let Some(ack_data) = get(&ack_lookup_key)? {
            let tiers: HashSet<DurabilityTier> = decode_json(&ack_data, "ack")?;
            commit.ack_state.confirmed_tiers = tiers;
        }

        commits.push(commit);
    }

    let tips_key = branch_tips_key(object_id, branch);
    let tails = match get(&tips_key)? {
        Some(data) => decode_json(&data, "tips")?,
        None => HashSet::new(),
    };

    Ok(Some(LoadedBranch { commits, tails }))
}

pub(super) fn append_commit_core(
    object_id: ObjectId,
    branch: &BranchName,
    commit: Commit,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let commit_id = commit.id();

    let commit_storage_key = commit_key(object_id, branch, commit_id);
    let commit_json = encode_json(&commit, "commit")?;
    set(&commit_storage_key, &commit_json)?;

    let tips_key = branch_tips_key(object_id, branch);
    let mut tips: HashSet<CommitId> = match get(&tips_key)? {
        Some(data) => decode_json(&data, "tips")?,
        None => HashSet::new(),
    };

    for parent in &commit.parents {
        tips.remove(parent);
    }
    tips.insert(commit_id);

    let tips_json = encode_json(&tips, "tips")?;
    set(&tips_key, &tips_json)
}

pub(super) fn delete_commit_core(
    object_id: ObjectId,
    branch: &BranchName,
    commit_id: CommitId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
    mut delete: impl FnMut(&str) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let commit_storage_key = commit_key(object_id, branch, commit_id);
    delete(&commit_storage_key)?;

    let tips_key = branch_tips_key(object_id, branch);
    if let Some(data) = get(&tips_key)? {
        let mut tips: HashSet<CommitId> = decode_json(&data, "tips")?;
        tips.remove(&commit_id);
        let tips_json = encode_json(&tips, "tips")?;
        set(&tips_key, &tips_json)?;
    }

    Ok(())
}

pub(super) fn set_branch_tails_core(
    object_id: ObjectId,
    branch: &BranchName,
    tails: Option<HashSet<CommitId>>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
    mut delete: impl FnMut(&str) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let tips_key = branch_tips_key(object_id, branch);
    match tails {
        Some(t) => {
            let json = encode_json(&t, "tails")?;
            set(&tips_key, &json)
        }
        None => delete(&tips_key),
    }
}

pub(super) fn store_ack_tier_core(
    commit_id: CommitId,
    tier: DurabilityTier,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let key = ack_key(commit_id);
    let mut tiers: HashSet<DurabilityTier> = match get(&key)? {
        Some(data) => decode_json(&data, "ack")?,
        None => HashSet::new(),
    };
    tiers.insert(tier);
    let json = encode_json(&tiers, "ack")?;
    set(&key, &json)
}

pub(super) fn append_catalogue_manifest_op_core(
    app_id: ObjectId,
    op: CatalogueManifestOp,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let key = catalogue_manifest_op_key(app_id, op.object_id());

    if let Some(existing) = get(&key)? {
        let existing_op: CatalogueManifestOp = decode_json(&existing, "catalogue manifest op")?;
        if existing_op == op {
            return Ok(());
        }
        return Err(StorageError::IoError(format!(
            "conflicting catalogue manifest op for object {}",
            op.object_id()
        )));
    }

    let json = encode_json(&op, "catalogue manifest op")?;
    set(&key, &json)
}

pub(super) fn append_catalogue_manifest_ops_core(
    app_id: ObjectId,
    ops: &[CatalogueManifestOp],
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    for op in ops {
        append_catalogue_manifest_op_core(app_id, op.clone(), &mut get, &mut set)?;
    }
    Ok(())
}

pub(super) fn load_catalogue_manifest_core(
    app_id: ObjectId,
    mut scan_prefix: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<Option<CatalogueManifest>, StorageError> {
    let prefix = catalogue_manifest_op_prefix(app_id);
    let entries = scan_prefix(&prefix)?;
    if entries.is_empty() {
        return Ok(None);
    }

    let mut manifest = CatalogueManifest::default();
    for (_key, data) in entries {
        let op: CatalogueManifestOp = decode_json(&data, "catalogue manifest op")?;
        manifest.apply(&op);
    }

    Ok(Some(manifest))
}

#[allow(dead_code)]
pub(super) fn append_history_region_rows_core(
    table: &str,
    rows: &[StoredRowVersion],
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    for row in rows {
        let key = history_row_key(table, &row.branch, row.row_id, row.updated_at);
        let json = encode_json(row, "stored row version")?;
        set(&key, &json)?;
    }
    Ok(())
}

#[allow(dead_code)]
pub(super) fn upsert_visible_region_rows_core(
    table: &str,
    rows: &[StoredRowVersion],
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    for row in rows {
        let key = visible_row_key(table, &row.branch, row.row_id);
        let json = encode_json(row, "stored row version")?;
        set(&key, &json)?;
    }
    Ok(())
}

#[allow(dead_code)]
pub(super) fn patch_row_region_rows_by_batch_core(
    table: &str,
    batch_id: BatchId,
    state: RowState,
    confirmed_tier: Option<DurabilityTier>,
    mut scan_prefix: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    for prefix in [history_table_prefix(table), visible_table_prefix(table)] {
        let entries = scan_prefix(&prefix)?;
        for (key, bytes) in entries {
            let mut row: StoredRowVersion = decode_json(&bytes, "stored row version")?;
            if row.batch_id != batch_id {
                continue;
            }

            row.state = state;
            row.confirmed_tier = match (row.confirmed_tier, confirmed_tier) {
                (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                (Some(existing), None) => Some(existing),
                (None, incoming) => incoming,
            };

            let json = encode_json(&row, "stored row version")?;
            set(&key, &json)?;
        }
    }

    Ok(())
}

#[allow(dead_code)]
pub(super) fn scan_visible_region_core(
    table: &str,
    branch: &str,
    mut scan_prefix: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<Vec<StoredRowVersion>, StorageError> {
    let prefix = visible_row_prefix(table, branch);
    let mut rows: Vec<StoredRowVersion> = scan_prefix(&prefix)?
        .into_iter()
        .map(|(_, bytes)| decode_json(&bytes, "stored row version"))
        .collect::<Result<_, _>>()?;
    rows.sort_by_key(|row| (row.branch.clone(), row.row_id));
    Ok(rows)
}

#[allow(dead_code)]
pub(super) fn load_visible_region_row_core(
    table: &str,
    branch: &str,
    row_id: ObjectId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<StoredRowVersion>, StorageError> {
    let key = visible_row_key(table, branch, row_id);
    match get(&key)? {
        Some(bytes) => Ok(Some(decode_json(&bytes, "stored row version")?)),
        None => Ok(None),
    }
}

#[allow(dead_code)]
pub(super) fn scan_history_region_core(
    table: &str,
    branch: &str,
    scan: HistoryScan,
    mut scan_prefix: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<Vec<StoredRowVersion>, StorageError> {
    let prefix = match scan {
        HistoryScan::Branch | HistoryScan::AsOf { .. } => history_row_prefix(table, branch),
        HistoryScan::Row { row_id } => history_row_versions_prefix(table, branch, row_id),
    };

    let scanned: Vec<StoredRowVersion> = scan_prefix(&prefix)?
        .into_iter()
        .map(|(_, bytes)| decode_json(&bytes, "stored row version"))
        .collect::<Result<_, _>>()?;

    let mut rows = match scan {
        HistoryScan::Branch | HistoryScan::Row { .. } => scanned,
        HistoryScan::AsOf { ts } => {
            let mut latest_per_row = HashMap::<ObjectId, StoredRowVersion>::new();
            for row in scanned {
                if row.updated_at > ts || !row.state.is_visible() {
                    continue;
                }

                match latest_per_row.get(&row.row_id) {
                    Some(existing) if existing.updated_at >= row.updated_at => {}
                    _ => {
                        latest_per_row.insert(row.row_id, row);
                    }
                }
            }
            latest_per_row.into_values().collect()
        }
    };

    rows.sort_by_key(|row| (row.branch.clone(), row.row_id, row.updated_at));
    Ok(rows)
}

pub(super) fn index_insert_core(
    table: &str,
    column: &str,
    branch: &str,
    value: &Value,
    row_id: ObjectId,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let key = index_entry_key(table, column, branch, value, row_id)?;
    set(&key, &[0x01])
}

pub(super) fn index_remove_core(
    table: &str,
    column: &str,
    branch: &str,
    value: &Value,
    row_id: ObjectId,
    mut delete: impl FnMut(&str) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let key = match index_entry_key(table, column, branch, value, row_id) {
        Ok(key) => key,
        Err(StorageError::IndexKeyTooLarge { .. }) => return Ok(()),
        Err(error) => return Err(error),
    };
    delete(&key)
}

pub(super) fn index_lookup_core(
    table: &str,
    column: &str,
    branch: &str,
    value: &Value,
    mut scan_prefix_keys: impl FnMut(&str) -> Result<Vec<String>, StorageError>,
) -> Vec<ObjectId> {
    // IEEE 754: -0.0 == 0.0, so scan both prefixes and merge.
    if super::is_double_zero(value) {
        let mut result = HashSet::new();
        for zero in &[Value::Double(0.0), Value::Double(-0.0)] {
            let Ok(prefix) = index_value_prefix(table, column, branch, zero) else {
                continue;
            };
            if let Ok(keys) = scan_prefix_keys(&prefix) {
                for key in &keys {
                    if let Some(id) = parse_uuid_from_index_key(key) {
                        result.insert(id);
                    }
                }
            }
        }
        return result.into_iter().collect();
    }

    let Ok(prefix) = index_value_prefix(table, column, branch, value) else {
        return Vec::new();
    };
    scan_prefix_keys(&prefix)
        .map(|keys| {
            keys.iter()
                .filter_map(|key| parse_uuid_from_index_key(key))
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn index_scan_all_core(
    table: &str,
    column: &str,
    branch: &str,
    mut scan_prefix_keys: impl FnMut(&str) -> Result<Vec<String>, StorageError>,
) -> Vec<ObjectId> {
    let prefix = index_prefix(table, column, branch);
    scan_prefix_keys(&prefix)
        .map(|keys| {
            keys.iter()
                .filter_map(|key| parse_uuid_from_index_key(key))
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn index_range_core(
    table: &str,
    column: &str,
    branch: &str,
    start: Bound<&Value>,
    end: Bound<&Value>,
    mut scan_key_range: impl FnMut(&str, &str) -> Result<Vec<String>, StorageError>,
) -> Vec<ObjectId> {
    let Some((start_key, end_key)) = index_range_scan_bounds(table, column, branch, start, end)
    else {
        return Vec::new();
    };

    scan_key_range(&start_key, &end_key)
        .map(|keys| {
            keys.iter()
                .filter_map(|key| parse_uuid_from_index_key(key))
                .collect()
        })
        .unwrap_or_default()
}
