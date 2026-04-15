use super::key_codec::{
    history_namespace_row_key, increment_string, raw_table_entry_key, raw_table_prefix,
    raw_table_scan_prefix, strip_raw_table_key, visible_namespace_row_key,
};
use super::{HistoryRowBytes, RawTableKeys, RawTableRows, StorageError, VisibleRowBytes};

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
    _table: &str,
    rows: &[HistoryRowBytes<'_>],
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    for row in rows {
        let key = history_namespace_row_key(row.row_id, row.branch, row.batch_id);
        raw_table_put_core(row.namespace_raw_table, &key, row.bytes, &mut set)?;
    }
    Ok(())
}

#[allow(dead_code)]
pub(super) fn upsert_visible_region_row_bytes_core(
    _table: &str,
    rows: &[VisibleRowBytes<'_>],
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    for row in rows {
        let key = visible_namespace_row_key(row.branch, row.row_id);
        raw_table_put_core(row.namespace_raw_table, &key, row.bytes, &mut set)?;
    }
    Ok(())
}
