use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::row_histories::HistoryScan;

use super::key_codec::{
    history_row_key, history_row_prefix, history_row_versions_prefix, increment_string,
    raw_table_entry_key, raw_table_prefix, raw_table_scan_prefix, strip_raw_table_key,
    visible_row_key, visible_row_prefix, visible_row_versions_key, visible_row_versions_prefix,
};
use super::{HistoryRowBytes, RawTableKeys, RawTableRows, StorageError, VisibleRowBytes};

fn encode_commit_id(version_id: CommitId) -> [u8; 32] {
    version_id.0
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

pub(super) fn raw_table_scan_prefix_keys_core(
    table: &str,
    prefix: &str,
    mut scan_prefix_keys: impl FnMut(&str) -> Result<Vec<String>, StorageError>,
) -> Result<RawTableKeys, StorageError> {
    let storage_prefix = raw_table_scan_prefix(table, prefix);
    Ok(scan_prefix_keys(&storage_prefix)?
        .into_iter()
        .filter_map(|key| strip_raw_table_key(table, &key).map(str::to_string))
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

pub(super) fn raw_table_scan_range_keys_core(
    table: &str,
    start: Option<&str>,
    end: Option<&str>,
    mut scan_range_keys: impl FnMut(&str, &str) -> Result<Vec<String>, StorageError>,
) -> Result<RawTableKeys, StorageError> {
    let start_key = raw_table_entry_key(table, start.unwrap_or(""));
    let end_key = if let Some(end) = end {
        raw_table_entry_key(table, end)
    } else {
        let mut table_end = raw_table_prefix(table);
        increment_string(&mut table_end);
        table_end
    };

    Ok(scan_range_keys(&start_key, &end_key)?
        .into_iter()
        .filter_map(|key| strip_raw_table_key(table, &key).map(str::to_string))
        .collect())
}

#[allow(dead_code)]
pub(super) fn append_history_region_row_bytes_core(
    table: &str,
    rows: &[HistoryRowBytes<'_>],
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    for row in rows {
        let key = history_row_key(table, row.row_id, row.branch, row.version_id);
        set(&key, row.bytes)?;
    }
    Ok(())
}

#[allow(dead_code)]
pub(super) fn upsert_visible_region_row_bytes_core(
    table: &str,
    rows: &[VisibleRowBytes<'_>],
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    for row in rows {
        let key = visible_row_key(table, row.branch, row.row_id);
        set(&key, row.bytes)?;
        let row_versions_key = visible_row_versions_key(table, row.row_id, row.branch);
        let version_id = encode_commit_id(row.current_version_id);
        set(&row_versions_key, &version_id)?;
    }
    Ok(())
}

#[allow(dead_code)]
pub(super) fn load_history_row_version_bytes_core(
    table: &str,
    branch: &str,
    row_id: ObjectId,
    version_id: CommitId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<Vec<u8>>, StorageError> {
    let key = history_row_key(table, row_id, branch, version_id);
    get(&key)
}

#[allow(dead_code)]
pub(super) fn load_visible_region_row_bytes_core(
    table: &str,
    branch: &str,
    row_id: ObjectId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<Vec<u8>>, StorageError> {
    let key = visible_row_key(table, branch, row_id);
    get(&key)
}

#[allow(dead_code)]
pub(super) fn scan_history_region_bytes_core(
    table: &str,
    scan: HistoryScan,
    mut scan_prefix: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<Vec<Vec<u8>>, StorageError> {
    let prefix = match scan {
        HistoryScan::Branch | HistoryScan::AsOf { .. } => history_row_prefix(table),
        HistoryScan::Row { row_id } => history_row_versions_prefix(table, row_id),
    };

    Ok(scan_prefix(&prefix)?
        .into_iter()
        .map(|(_, bytes)| bytes)
        .collect())
}

#[allow(dead_code)]
pub(super) fn scan_visible_region_bytes_core(
    table: &str,
    branch: &str,
    mut scan_prefix: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<Vec<Vec<u8>>, StorageError> {
    let prefix = visible_row_prefix(table, branch);
    Ok(scan_prefix(&prefix)?
        .into_iter()
        .map(|(_, bytes)| bytes)
        .collect())
}

#[allow(dead_code)]
pub(super) fn scan_visible_region_row_version_branches_core(
    table: &str,
    row_id: ObjectId,
    mut scan_prefix_keys: impl FnMut(&str) -> Result<Vec<String>, StorageError>,
) -> Result<Vec<String>, StorageError> {
    let prefix = visible_row_versions_prefix(table, row_id);
    let mut branches = scan_prefix_keys(&prefix)?
        .into_iter()
        .filter_map(|key| key.strip_prefix(&prefix).map(str::to_string))
        .collect::<Vec<_>>();
    branches.sort();
    branches.dedup();
    Ok(branches)
}
