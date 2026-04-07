use std::ops::Bound;

use crate::object::ObjectId;
use crate::query_manager::types::Value;

use super::{StorageError, encode_value};

const INDEX_KEY_MAX_BYTES: usize = u16::MAX as usize;
const INDEX_ENTRY_UUID_HEX_BYTES: usize = 32;
const OVERFLOW_INDEX_VALUE_MARKER: char = '~';
const OVERFLOW_INDEX_VALUE_LEN_HEX_BYTES: usize = 16;
const OVERFLOW_INDEX_VALUE_HASH_HEX_BYTES: usize = blake3::OUT_LEN * 2;
const OVERFLOW_INDEX_VALUE_TRAILER_BYTES: usize =
    1 + OVERFLOW_INDEX_VALUE_LEN_HEX_BYTES + OVERFLOW_INDEX_VALUE_HASH_HEX_BYTES;
const RAW_TABLE_KEY_PREFIX: &str = "raw:";

fn raw_table_key_bytes(table: &str, key_len: usize) -> usize {
    RAW_TABLE_KEY_PREFIX.len() + table.len() + 1 + key_len
}

pub(super) fn raw_table_entry_key(table: &str, key: &str) -> String {
    format!("{RAW_TABLE_KEY_PREFIX}{table}:{key}")
}

pub(super) fn raw_table_prefix(table: &str) -> String {
    format!("{RAW_TABLE_KEY_PREFIX}{table}:")
}

pub(super) fn raw_table_scan_prefix(table: &str, prefix: &str) -> String {
    format!("{}{prefix}", raw_table_prefix(table))
}

pub(super) fn strip_raw_table_key<'a>(table: &str, storage_key: &'a str) -> Option<&'a str> {
    storage_key.strip_prefix(&raw_table_prefix(table))
}

pub(super) fn index_raw_table(table: &str, column: &str, branch: &str) -> String {
    format!("idx:{table}:{column}:{branch}")
}

fn index_entry_key_bytes(
    table: &str,
    column: &str,
    branch: &str,
    value_segment_len: usize,
) -> usize {
    let raw_table = index_raw_table(table, column, branch);
    raw_table_key_bytes(
        &raw_table,
        value_segment_len + 1 + INDEX_ENTRY_UUID_HEX_BYTES,
    )
}

fn index_value_prefix_bytes(
    table: &str,
    column: &str,
    branch: &str,
    value_segment_len: usize,
) -> usize {
    let raw_table = index_raw_table(table, column, branch);
    raw_table_key_bytes(&raw_table, value_segment_len + 1)
}

fn index_key_too_large_error(
    table: &str,
    column: &str,
    branch: &str,
    key_bytes: usize,
) -> StorageError {
    StorageError::IndexKeyTooLarge {
        table: table.to_string(),
        column: column.to_string(),
        branch: branch.to_string(),
        key_bytes,
        max_key_bytes: INDEX_KEY_MAX_BYTES,
    }
}

fn max_index_value_segment_len(table: &str, column: &str, branch: &str) -> Option<usize> {
    INDEX_KEY_MAX_BYTES.checked_sub(index_entry_key_bytes(table, column, branch, 0))
}

fn overflow_index_value_segment(
    encoded_value: &[u8],
    encoded_hex: &str,
    prefix_hex_len: usize,
) -> String {
    let hash = blake3::hash(encoded_value);
    let prefix = &encoded_hex[..prefix_hex_len];
    format!(
        "{}{}{:016x}{}",
        prefix,
        OVERFLOW_INDEX_VALUE_MARKER,
        encoded_value.len() as u64,
        hex::encode(hash.as_bytes())
    )
}

fn encode_index_value_segment(
    table: &str,
    column: &str,
    branch: &str,
    value: &Value,
) -> Result<String, StorageError> {
    let encoded_value = encode_value(value);
    let encoded_hex = hex::encode(&encoded_value);
    let Some(max_segment_len) = max_index_value_segment_len(table, column, branch) else {
        return Err(index_key_too_large_error(
            table,
            column,
            branch,
            index_entry_key_bytes(table, column, branch, 0),
        ));
    };

    if encoded_hex.len() <= max_segment_len {
        return Ok(encoded_hex);
    }

    let Some(prefix_hex_len) = max_segment_len.checked_sub(OVERFLOW_INDEX_VALUE_TRAILER_BYTES)
    else {
        return Err(index_key_too_large_error(
            table,
            column,
            branch,
            index_entry_key_bytes(table, column, branch, OVERFLOW_INDEX_VALUE_TRAILER_BYTES),
        ));
    };

    Ok(overflow_index_value_segment(
        &encoded_value,
        &encoded_hex,
        prefix_hex_len,
    ))
}

pub(super) fn validate_index_entry_size(
    table: &str,
    column: &str,
    branch: &str,
    value: &Value,
) -> Result<(), StorageError> {
    encode_index_value_segment(table, column, branch, value).map(|_| ())
}

/// Format an ObjectId as compact hex (no dashes).
pub(super) fn format_uuid(id: ObjectId) -> String {
    hex::encode(id.uuid().as_bytes())
}

pub(super) fn obj_meta_key(id: ObjectId) -> String {
    format!("obj:{}:meta", format_uuid(id))
}

pub(super) fn obj_meta_prefix() -> &'static str {
    "obj:"
}

#[allow(dead_code)]
pub(super) fn visible_row_key(table: &str, branch: &str, row_id: ObjectId) -> String {
    format!("row:{table}:0:{branch}:{}", format_uuid(row_id))
}

#[allow(dead_code)]
pub(super) fn visible_row_prefix(table: &str, branch: &str) -> String {
    format!("row:{table}:0:{branch}:")
}

#[allow(dead_code)]
pub(super) fn visible_table_prefix(table: &str) -> String {
    format!("row:{table}:0:")
}

#[allow(dead_code)]
pub(super) fn history_row_key(
    table: &str,
    branch: &str,
    row_id: ObjectId,
    updated_at: u64,
) -> String {
    format!(
        "row:{table}:1:{branch}:{}:{updated_at:016x}",
        format_uuid(row_id)
    )
}

#[allow(dead_code)]
pub(super) fn history_row_prefix(table: &str, branch: &str) -> String {
    format!("row:{table}:1:{branch}:")
}

#[allow(dead_code)]
pub(super) fn history_row_versions_prefix(table: &str, branch: &str, row_id: ObjectId) -> String {
    format!("row:{table}:1:{branch}:{}:", format_uuid(row_id))
}

#[allow(dead_code)]
pub(super) fn history_table_prefix(table: &str) -> String {
    format!("row:{table}:1:")
}

pub(super) fn catalogue_entry_key(object_id: ObjectId) -> String {
    format!("catrow:{}", format_uuid(object_id))
}

pub(super) fn catalogue_entry_prefix() -> &'static str {
    "catrow:"
}

pub(super) fn index_entry_key(
    table: &str,
    column: &str,
    branch: &str,
    value: &Value,
    row_id: ObjectId,
) -> Result<String, StorageError> {
    Ok(format!(
        "{}:{}",
        encode_index_value_segment(table, column, branch, value)?,
        format_uuid(row_id)
    ))
}

pub(super) fn index_value_prefix(
    table: &str,
    column: &str,
    branch: &str,
    value: &Value,
) -> Result<String, StorageError> {
    let value_segment = encode_index_value_segment(table, column, branch, value)?;
    let key_bytes = index_value_prefix_bytes(table, column, branch, value_segment.len());
    if key_bytes > INDEX_KEY_MAX_BYTES {
        return Err(index_key_too_large_error(table, column, branch, key_bytes));
    }
    Ok(format!("{value_segment}:"))
}

/// Compute lexicographic scan bounds for index range queries.
pub(super) fn index_range_scan_bounds(
    table: &str,
    column: &str,
    branch: &str,
    start: Bound<&Value>,
    end: Bound<&Value>,
) -> Option<(Option<String>, Option<String>)> {
    // IEEE 754: -0.0 == 0.0 but they encode differently. Adjust bounds
    // so both zeros are treated as the same point.
    let neg_zero = Value::Double(-0.0);
    let pos_zero = Value::Double(0.0);
    let neg_zero_segment = encode_index_value_segment(table, column, branch, &neg_zero).ok()?;
    let pos_zero_segment = encode_index_value_segment(table, column, branch, &pos_zero).ok()?;

    let start_key = match start {
        Bound::Included(v) if super::is_double_zero(v) => Some(neg_zero_segment.clone()),
        Bound::Excluded(v) if super::is_double_zero(v) => {
            let mut key = format!("{pos_zero_segment}:");
            increment_string(&mut key);
            Some(key)
        }
        Bound::Included(v) => {
            let segment = encode_index_value_segment(table, column, branch, v).ok()?;
            Some(segment)
        }
        Bound::Excluded(v) => {
            let segment = encode_index_value_segment(table, column, branch, v).ok()?;
            let mut key = format!("{segment}:");
            increment_string(&mut key);
            Some(key)
        }
        Bound::Unbounded => None,
    };

    let end_key = match end {
        Bound::Included(v) if super::is_double_zero(v) => {
            let mut key = format!("{pos_zero_segment}:");
            increment_string(&mut key);
            Some(key)
        }
        Bound::Excluded(v) if super::is_double_zero(v) => Some(neg_zero_segment.clone()),
        Bound::Included(v) => {
            let segment = encode_index_value_segment(table, column, branch, v).ok()?;
            let mut key = format!("{segment}:");
            increment_string(&mut key);
            Some(key)
        }
        Bound::Excluded(v) => {
            let segment = encode_index_value_segment(table, column, branch, v).ok()?;
            Some(segment)
        }
        Bound::Unbounded => None,
    };

    match (&start_key, &end_key) {
        (Some(start_key), Some(end_key)) if start_key >= end_key => None,
        _ => Some((start_key, end_key)),
    }
}

/// Parse a UUID from the last segment of an index key.
/// Key format: `idx:{table}:{col}:{branch}:{value_segment}:{uuid_hex}`
pub(super) fn parse_uuid_from_index_key(key: &str) -> Option<ObjectId> {
    let uuid_hex = key.rsplit(':').next()?;
    let bytes = hex::decode(uuid_hex).ok()?;
    if bytes.len() != 16 {
        return None;
    }
    let uuid = uuid::Uuid::from_bytes(bytes.try_into().ok()?);
    Some(ObjectId(internment::Intern::new(uuid)))
}

/// Increment the last byte for exclusive upper bounds.
pub(super) fn increment_bytes(bytes: &mut Vec<u8>) {
    for i in (0..bytes.len()).rev() {
        if bytes[i] < 0xFF {
            bytes[i] += 1;
            bytes.truncate(i + 1);
            return;
        }
    }
    bytes.push(0x00);
}

/// Increment a UTF-8 key string for exclusive upper bounds.
pub(super) fn increment_string(s: &mut String) {
    let mut bytes = std::mem::take(s).into_bytes();
    increment_bytes(&mut bytes);
    *s = String::from_utf8(bytes).unwrap_or_default();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_text_index_segments_stay_inline() {
        let segment =
            encode_index_value_segment("todos", "title", "main", &Value::Text("hello".into()))
                .expect("short text should fit inline");
        assert_eq!(
            segment,
            hex::encode(encode_value(&Value::Text("hello".into())))
        );
        assert!(!segment.contains(OVERFLOW_INDEX_VALUE_MARKER));
    }

    #[test]
    fn oversized_text_index_segments_preserve_real_prefix() {
        let value = Value::Text("x".repeat(40_000));
        let encoded_hex = hex::encode(encode_value(&value));
        let segment = encode_index_value_segment("todos", "title", "main", &value)
            .expect("oversized text should use overflow segment");
        let (prefix, suffix) = segment
            .split_once(OVERFLOW_INDEX_VALUE_MARKER)
            .expect("overflow segment should include marker");
        assert!(
            !prefix.is_empty(),
            "overflow segment should keep some real prefix"
        );
        assert_eq!(prefix, &encoded_hex[..prefix.len()]);
        assert_eq!(
            suffix.len(),
            OVERFLOW_INDEX_VALUE_LEN_HEX_BYTES + OVERFLOW_INDEX_VALUE_HASH_HEX_BYTES
        );
    }

    #[test]
    fn oversized_text_segments_sort_by_prefix() {
        let a =
            encode_index_value_segment("todos", "title", "main", &Value::Text("a".repeat(40_000)))
                .expect("a segment");
        let b =
            encode_index_value_segment("todos", "title", "main", &Value::Text("b".repeat(40_000)))
                .expect("b segment");
        assert!(a < b, "overflow segments should preserve prefix ordering");
    }

    #[test]
    fn range_bounds_support_oversized_text_values() {
        let min = Value::Text("a".repeat(40_000));
        let max = Value::Text("b".repeat(40_000));
        let bounds = index_range_scan_bounds(
            "todos",
            "title",
            "main",
            Bound::Included(&min),
            Bound::Included(&max),
        );
        assert!(
            bounds.is_some(),
            "overflow text values should still produce range bounds"
        );
    }
}
