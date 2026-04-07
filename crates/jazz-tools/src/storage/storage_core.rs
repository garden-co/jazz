use std::collections::HashMap;

use serde::{Serialize, de::DeserializeOwned};

use crate::object::ObjectId;
use crate::row_regions::{BatchId, HistoryScan, RowState, StoredRowVersion};
use crate::sync_manager::DurabilityTier;

use super::key_codec::{
    history_row_key, history_row_prefix, history_row_versions_prefix, history_table_prefix,
    increment_string, obj_meta_key, obj_meta_prefix, raw_table_entry_key, raw_table_prefix,
    raw_table_scan_prefix, strip_raw_table_key, visible_row_key, visible_row_prefix,
    visible_table_prefix,
};
use super::{ObjectMetadataRows, RawTableRows, StorageError};

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

#[allow(dead_code)]
pub(super) fn scan_object_metadata_core(
    mut scan_prefix: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<ObjectMetadataRows, StorageError> {
    let mut objects = Vec::new();
    for (key, data) in scan_prefix(obj_meta_prefix())? {
        let Some(hex_id) = key
            .strip_prefix("obj:")
            .and_then(|rest| rest.strip_suffix(":meta"))
        else {
            continue;
        };

        let bytes = hex::decode(hex_id).map_err(|err| {
            StorageError::IoError(format!("invalid object metadata key '{key}': {err}"))
        })?;
        let uuid = uuid::Uuid::from_slice(&bytes).map_err(|err| {
            StorageError::IoError(format!("invalid object metadata uuid '{key}': {err}"))
        })?;
        let metadata = decode_json::<HashMap<String, String>>(&data, "object metadata")?;
        objects.push((ObjectId::from_uuid(uuid), metadata));
    }
    objects.sort_by_key(|(object_id, _)| *object_id);
    Ok(objects)
}

pub(super) fn raw_table_put_core(
    table: &str,
    key: &str,
    value: &[u8],
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    set(&raw_table_entry_key(table, key), value)
}

pub(super) fn raw_table_delete_core(
    table: &str,
    key: &str,
    mut delete: impl FnMut(&str) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    delete(&raw_table_entry_key(table, key))
}

pub(super) fn raw_table_get_core(
    table: &str,
    key: &str,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<Vec<u8>>, StorageError> {
    get(&raw_table_entry_key(table, key))
}

pub(super) fn raw_table_scan_prefix_core(
    table: &str,
    prefix: &str,
    mut scan_prefix_entries: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<RawTableRows, StorageError> {
    let storage_prefix = raw_table_scan_prefix(table, prefix);
    Ok(scan_prefix_entries(&storage_prefix)?
        .into_iter()
        .filter_map(|(key, value)| {
            strip_raw_table_key(table, &key).map(|local_key| (local_key.to_string(), value))
        })
        .collect())
}

pub(super) fn raw_table_scan_range_core(
    table: &str,
    start: Option<&str>,
    end: Option<&str>,
    mut scan_range_entries: impl FnMut(&str, &str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<RawTableRows, StorageError> {
    let start_key = raw_table_entry_key(table, start.unwrap_or(""));
    let end_key = if let Some(end) = end {
        raw_table_entry_key(table, end)
    } else {
        let mut table_end = raw_table_prefix(table);
        increment_string(&mut table_end);
        table_end
    };

    Ok(scan_range_entries(&start_key, &end_key)?
        .into_iter()
        .filter_map(|(key, value)| {
            strip_raw_table_key(table, &key).map(|local_key| (local_key.to_string(), value))
        })
        .collect())
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
    state: Option<RowState>,
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

            if let Some(state) = state {
                row.state = state;
            }
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
pub(super) fn scan_visible_region_row_versions_core(
    table: &str,
    row_id: ObjectId,
    mut scan_prefix: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<Vec<StoredRowVersion>, StorageError> {
    let prefix = visible_table_prefix(table);
    let mut rows: Vec<StoredRowVersion> = scan_prefix(&prefix)?
        .into_iter()
        .map(|(_, bytes)| decode_json::<StoredRowVersion>(&bytes, "stored row version"))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|row| row.row_id == row_id)
        .collect();
    rows.sort_by_key(|row| row.branch.clone());
    Ok(rows)
}

#[allow(dead_code)]
pub(super) fn scan_history_row_versions_core(
    table: &str,
    row_id: ObjectId,
    mut scan_prefix: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<Vec<StoredRowVersion>, StorageError> {
    let prefix = history_table_prefix(table);
    let mut rows: Vec<StoredRowVersion> = scan_prefix(&prefix)?
        .into_iter()
        .map(|(_, bytes)| decode_json::<StoredRowVersion>(&bytes, "stored row version"))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|row| row.row_id == row_id)
        .collect();
    rows.sort_by_key(|row| (row.branch.clone(), row.updated_at));
    Ok(rows)
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
