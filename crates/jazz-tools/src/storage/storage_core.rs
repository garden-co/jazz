use std::collections::{HashMap, HashSet};

use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::row_regions::{
    BatchId, HistoryScan, RowState, StoredRowVersion, VisibleRowEntry, decode_stored_row_version,
    decode_visible_row_entry, encode_stored_row_version, encode_visible_row_entry,
};
use crate::sync_manager::DurabilityTier;

use super::key_codec::{
    history_row_key, history_row_prefix, history_row_versions_prefix, history_table_prefix,
    increment_string, raw_table_entry_key, raw_table_prefix, raw_table_scan_prefix,
    strip_raw_table_key, visible_row_key, visible_row_prefix, visible_table_prefix,
};
use super::{RawTableRows, StorageError};

fn storage_codec_error(action: &str, label: &str, err: impl std::fmt::Display) -> StorageError {
    StorageError::IoError(format!("{action} {label}: {err}"))
}

fn encode_history_row(row: &StoredRowVersion) -> Result<Vec<u8>, StorageError> {
    encode_stored_row_version(row)
        .map_err(|err| storage_codec_error("encode", "stored row version", err))
}

fn encode_visible_entry(entry: &VisibleRowEntry) -> Result<Vec<u8>, StorageError> {
    encode_visible_row_entry(entry)
        .map_err(|err| storage_codec_error("encode", "visible row entry", err))
}

fn decode_visible_entry(bytes: &[u8]) -> Result<VisibleRowEntry, StorageError> {
    decode_visible_row_entry(bytes)
        .map_err(|err| storage_codec_error("decode", "visible row entry", err))
}

fn decode_history_row(bytes: &[u8]) -> Result<StoredRowVersion, StorageError> {
    decode_stored_row_version(bytes)
        .map_err(|err| storage_codec_error("decode", "stored row version", err))
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
        let key = history_row_key(table, row.row_id, row.version_id());
        let encoded = encode_history_row(row)?;
        set(&key, &encoded)?;
    }
    Ok(())
}

#[allow(dead_code)]
pub(super) fn upsert_visible_region_rows_core(
    table: &str,
    entries: &[VisibleRowEntry],
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    for entry in entries {
        let key = visible_row_key(table, &entry.current_row.branch, entry.current_row.row_id);
        let encoded = encode_visible_entry(entry)?;
        set(&key, &encoded)?;
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
    let mut affected_visible_rows = HashSet::new();
    let history_entries = scan_prefix(&history_table_prefix(table))?;
    let mut history_by_visible_row = HashMap::<(String, ObjectId), Vec<StoredRowVersion>>::new();

    for (key, bytes) in history_entries {
        let mut row = decode_history_row(&bytes)?;
        if row.batch_id == batch_id {
            if let Some(state) = state {
                row.state = state;
            }
            row.confirmed_tier = match (row.confirmed_tier, confirmed_tier) {
                (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                (Some(existing), None) => Some(existing),
                (None, incoming) => incoming,
            };
            let encoded = encode_history_row(&row)?;
            set(&key, &encoded)?;
            affected_visible_rows.insert((row.branch.clone(), row.row_id));
        }
        history_by_visible_row
            .entry((row.branch.clone(), row.row_id))
            .or_default()
            .push(row);
    }

    let visible_entries = scan_prefix(&visible_table_prefix(table))?;
    let mut visible_by_key = HashMap::<(String, ObjectId), VisibleRowEntry>::new();
    for (_, bytes) in visible_entries {
        let mut entry = decode_visible_entry(&bytes)?;
        let row = &mut entry.current_row;
        if row.batch_id == batch_id {
            if let Some(state) = state {
                row.state = state;
            }
            row.confirmed_tier = match (row.confirmed_tier, confirmed_tier) {
                (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                (Some(existing), None) => Some(existing),
                (None, incoming) => incoming,
            };
            affected_visible_rows.insert((row.branch.clone(), row.row_id));
        }
        visible_by_key.insert((row.branch.clone(), row.row_id), entry);
    }

    for (branch, row_id) in affected_visible_rows {
        let Some(entry) = visible_by_key.get(&(branch.clone(), row_id)).cloned() else {
            continue;
        };
        let current_row = entry.current_row;
        let mut history_rows = history_by_visible_row
            .remove(&(branch.clone(), row_id))
            .unwrap_or_default();
        if !history_rows
            .iter()
            .any(|row| row.version_id() == current_row.version_id())
        {
            history_rows.push(current_row.clone());
        }
        let rebuilt = VisibleRowEntry::rebuild(current_row, &history_rows);
        let encoded = encode_visible_entry(&rebuilt)?;
        set(&visible_row_key(table, &branch, row_id), &encoded)?;
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
        .map(|(_, bytes)| decode_visible_entry(&bytes).map(|entry| entry.current_row))
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
        Some(bytes) => Ok(Some(decode_visible_entry(&bytes)?.current_row)),
        None => Ok(None),
    }
}

#[allow(dead_code)]
pub(super) fn load_visible_region_entry_core(
    table: &str,
    branch: &str,
    row_id: ObjectId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<VisibleRowEntry>, StorageError> {
    let key = visible_row_key(table, branch, row_id);
    match get(&key)? {
        Some(bytes) => Ok(Some(decode_visible_entry(&bytes)?)),
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
        .map(|(_, bytes)| decode_visible_entry(&bytes).map(|entry| entry.current_row))
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
    let prefix = history_row_versions_prefix(table, row_id);
    let mut rows: Vec<StoredRowVersion> = scan_prefix(&prefix)?
        .into_iter()
        .map(|(_, bytes)| decode_history_row(&bytes))
        .collect::<Result<_, _>>()?;
    rows.sort_by_key(|row| (row.branch.clone(), row.updated_at, row.version_id()));
    Ok(rows)
}

#[allow(dead_code)]
pub(super) fn load_history_row_version_core(
    table: &str,
    row_id: ObjectId,
    version_id: CommitId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<StoredRowVersion>, StorageError> {
    let key = history_row_key(table, row_id, version_id);
    match get(&key)? {
        Some(bytes) => Ok(Some(decode_history_row(&bytes)?)),
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
        HistoryScan::Branch | HistoryScan::AsOf { .. } => history_row_prefix(table),
        HistoryScan::Row { row_id } => history_row_versions_prefix(table, row_id),
    };

    let scanned: Vec<StoredRowVersion> = scan_prefix(&prefix)?
        .into_iter()
        .map(|(_, bytes)| decode_history_row(&bytes))
        .collect::<Result<_, _>>()?;

    let mut rows: Vec<StoredRowVersion> = match scan {
        HistoryScan::Branch | HistoryScan::Row { .. } => scanned
            .into_iter()
            .filter(|row| row.branch == branch)
            .collect(),
        HistoryScan::AsOf { ts } => {
            let mut latest_per_row = HashMap::<ObjectId, StoredRowVersion>::new();
            for row in scanned {
                if row.branch != branch || row.updated_at > ts || !row.state.is_visible() {
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

    rows.sort_by_key(|row| {
        (
            row.branch.clone(),
            row.row_id,
            row.updated_at,
            row.version_id(),
        )
    });
    Ok(rows)
}
