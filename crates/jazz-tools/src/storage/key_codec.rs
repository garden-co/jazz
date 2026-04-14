use std::ops::Bound;

use crate::object::ObjectId;
use crate::query_manager::types::Value;
use crate::row_histories::BatchId;

use super::{StorageError, encode_value};

const INDEX_KEY_MAX_BYTES: usize = u16::MAX as usize;
const INDEX_ENTRY_UUID_HEX_BYTES: usize = 32;
const BATCH_ID_HEX_BYTES: usize = 32;
const OVERFLOW_INDEX_VALUE_MARKER: char = '~';
const OVERFLOW_INDEX_VALUE_LEN_HEX_BYTES: usize = 16;
const OVERFLOW_INDEX_VALUE_HASH_HEX_BYTES: usize = blake3::OUT_LEN * 2;
const OVERFLOW_INDEX_VALUE_TRAILER_BYTES: usize =
    1 + OVERFLOW_INDEX_VALUE_LEN_HEX_BYTES + OVERFLOW_INDEX_VALUE_HASH_HEX_BYTES;
const RAW_TABLE_KEY_PREFIX: &str = "raw:";
const INDEX_RAW_TABLE_PREFIX: &str = "idx:";
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

fn append_hex_bytes(dst: &mut String, bytes: &[u8]) {
    dst.reserve(bytes.len() * 2);
    for &byte in bytes {
        dst.push(HEX_DIGITS[(byte >> 4) as usize] as char);
        dst.push(HEX_DIGITS[(byte & 0x0f) as usize] as char);
    }
}

fn append_hex_prefix(dst: &mut String, bytes: &[u8], hex_len: usize) {
    let full_bytes = hex_len / 2;
    append_hex_bytes(dst, &bytes[..full_bytes]);
    if hex_len % 2 == 1 {
        dst.push(HEX_DIGITS[(bytes[full_bytes] >> 4) as usize] as char);
    }
}

fn append_fixed_u64_hex(dst: &mut String, value: u64) {
    dst.reserve(OVERFLOW_INDEX_VALUE_LEN_HEX_BYTES);
    for shift in (0..OVERFLOW_INDEX_VALUE_LEN_HEX_BYTES).rev() {
        let nibble = ((value >> (shift * 4)) & 0x0f) as usize;
        dst.push(HEX_DIGITS[nibble] as char);
    }
}

fn append_uuid_hex(dst: &mut String, id: ObjectId) {
    append_hex_bytes(dst, id.uuid().as_bytes());
}

fn append_batch_id_hex(dst: &mut String, batch_id: BatchId) {
    append_hex_bytes(dst, batch_id.as_bytes());
}

fn decode_hex_object_id(raw: &str, context: &str) -> Result<ObjectId, StorageError> {
    let bytes = hex::decode(raw)
        .map_err(|err| StorageError::IoError(format!("{context}: invalid row id hex: {err}")))?;
    let uuid = uuid::Uuid::from_slice(&bytes)
        .map_err(|err| StorageError::IoError(format!("{context}: invalid row id uuid: {err}")))?;
    Ok(ObjectId::from_uuid(uuid))
}

fn decode_hex_batch_id(raw: &str, context: &str) -> Result<BatchId, StorageError> {
    let bytes = hex::decode(raw)
        .map_err(|err| StorageError::IoError(format!("{context}: invalid batch id hex: {err}")))?;
    let bytes: [u8; 16] = bytes.as_slice().try_into().map_err(|_| {
        StorageError::IoError(format!(
            "{context}: expected 16 batch id bytes, got {}",
            bytes.len()
        ))
    })?;
    Ok(BatchId(bytes))
}

fn raw_table_key_bytes(table: &str, key_len: usize) -> usize {
    RAW_TABLE_KEY_PREFIX.len() + table.len() + 1 + key_len
}

fn raw_table_prefix_len(table: &str) -> usize {
    RAW_TABLE_KEY_PREFIX.len() + table.len() + 1
}

pub(super) fn raw_table_entry_key(table: &str, key: &str) -> String {
    let mut storage_key = String::with_capacity(raw_table_prefix_len(table) + key.len());
    storage_key.push_str(RAW_TABLE_KEY_PREFIX);
    storage_key.push_str(table);
    storage_key.push(':');
    storage_key.push_str(key);
    storage_key
}

pub(super) fn raw_table_prefix(table: &str) -> String {
    let mut prefix = String::with_capacity(raw_table_prefix_len(table));
    prefix.push_str(RAW_TABLE_KEY_PREFIX);
    prefix.push_str(table);
    prefix.push(':');
    prefix
}

pub(super) fn raw_table_scan_prefix(table: &str, prefix: &str) -> String {
    let mut storage_prefix = String::with_capacity(raw_table_prefix_len(table) + prefix.len());
    storage_prefix.push_str(RAW_TABLE_KEY_PREFIX);
    storage_prefix.push_str(table);
    storage_prefix.push(':');
    storage_prefix.push_str(prefix);
    storage_prefix
}

pub(super) fn strip_raw_table_key<'a>(table: &str, storage_key: &'a str) -> Option<&'a str> {
    storage_key
        .strip_prefix(RAW_TABLE_KEY_PREFIX)?
        .strip_prefix(table)?
        .strip_prefix(':')
}

fn index_raw_table_len(table: &str, column: &str, branch: &str) -> usize {
    INDEX_RAW_TABLE_PREFIX.len() + table.len() + 1 + column.len() + 1 + branch.len()
}

pub(super) fn index_raw_table(table: &str, column: &str, branch: &str) -> String {
    let mut raw_table = String::with_capacity(index_raw_table_len(table, column, branch));
    raw_table.push_str(INDEX_RAW_TABLE_PREFIX);
    raw_table.push_str(table);
    raw_table.push(':');
    raw_table.push_str(column);
    raw_table.push(':');
    raw_table.push_str(branch);
    raw_table
}

fn index_entry_key_bytes(
    table: &str,
    column: &str,
    branch: &str,
    value_segment_len: usize,
) -> usize {
    raw_table_key_bytes(
        "",
        index_raw_table_len(table, column, branch)
            + value_segment_len
            + 1
            + INDEX_ENTRY_UUID_HEX_BYTES,
    ) - RAW_TABLE_KEY_PREFIX.len()
        - 1
}

fn index_value_prefix_bytes(
    table: &str,
    column: &str,
    branch: &str,
    value_segment_len: usize,
) -> usize {
    raw_table_key_bytes(
        "",
        index_raw_table_len(table, column, branch) + value_segment_len + 1,
    ) - RAW_TABLE_KEY_PREFIX.len()
        - 1
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

fn overflow_index_value_segment(encoded_value: &[u8], prefix_hex_len: usize) -> String {
    let hash = blake3::hash(encoded_value);
    let mut segment = String::with_capacity(prefix_hex_len + OVERFLOW_INDEX_VALUE_TRAILER_BYTES);
    append_hex_prefix(&mut segment, encoded_value, prefix_hex_len);
    segment.push(OVERFLOW_INDEX_VALUE_MARKER);
    append_fixed_u64_hex(&mut segment, encoded_value.len() as u64);
    append_hex_bytes(&mut segment, hash.as_bytes());
    segment
}

fn encode_index_value_segment(
    table: &str,
    column: &str,
    branch: &str,
    value: &Value,
) -> Result<String, StorageError> {
    let encoded_value = encode_value(value);
    let Some(max_segment_len) = max_index_value_segment_len(table, column, branch) else {
        return Err(index_key_too_large_error(
            table,
            column,
            branch,
            index_entry_key_bytes(table, column, branch, 0),
        ));
    };
    let encoded_hex_len = encoded_value.len() * 2;

    if encoded_hex_len <= max_segment_len {
        let mut encoded_hex = String::with_capacity(encoded_hex_len);
        append_hex_bytes(&mut encoded_hex, &encoded_value);
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

    Ok(overflow_index_value_segment(&encoded_value, prefix_hex_len))
}

pub(super) fn validate_index_entry_size(
    table: &str,
    column: &str,
    branch: &str,
    value: &Value,
) -> Result<(), StorageError> {
    encode_index_value_segment(table, column, branch, value).map(|_| ())
}

#[allow(dead_code)]
pub(super) fn visible_row_key(table: &str, branch: &str, row_id: ObjectId) -> String {
    let mut key =
        String::with_capacity(4 + table.len() + 3 + branch.len() + INDEX_ENTRY_UUID_HEX_BYTES);
    key.push_str("row:");
    key.push_str(table);
    key.push_str(":0:");
    key.push_str(branch);
    key.push(':');
    append_uuid_hex(&mut key, row_id);
    key
}

#[allow(dead_code)]
pub(super) fn visible_row_prefix(table: &str, branch: &str) -> String {
    let mut prefix = String::with_capacity(4 + table.len() + 3 + branch.len() + 1);
    prefix.push_str("row:");
    prefix.push_str(table);
    prefix.push_str(":0:");
    prefix.push_str(branch);
    prefix.push(':');
    prefix
}

pub(super) fn decode_visible_row_key(
    table: &str,
    key: &str,
) -> Result<(String, ObjectId), StorageError> {
    let prefix = visible_table_prefix(table);
    let rest = key
        .strip_prefix(&prefix)
        .ok_or_else(|| StorageError::IoError(format!("invalid visible row key '{key}'")))?;
    let (branch, row_hex) = rest.rsplit_once(':').ok_or_else(|| {
        StorageError::IoError(format!("invalid visible row key '{key}': missing row id"))
    })?;
    Ok((
        branch.to_string(),
        decode_hex_object_id(row_hex, &format!("decode visible row key '{key}'"))?,
    ))
}

#[allow(dead_code)]
pub(super) fn visible_table_prefix(table: &str) -> String {
    let mut prefix = String::with_capacity(4 + table.len() + 3);
    prefix.push_str("row:");
    prefix.push_str(table);
    prefix.push_str(":0:");
    prefix
}

#[allow(dead_code)]
pub(super) fn history_row_key(
    table: &str,
    row_id: ObjectId,
    branch: &str,
    batch_id: BatchId,
) -> String {
    let mut key = String::with_capacity(
        4 + table.len()
            + 3
            + INDEX_ENTRY_UUID_HEX_BYTES
            + 1
            + branch.len()
            + 1
            + BATCH_ID_HEX_BYTES,
    );
    key.push_str("row:");
    key.push_str(table);
    key.push_str(":1:");
    append_uuid_hex(&mut key, row_id);
    key.push(':');
    key.push_str(branch);
    key.push(':');
    append_batch_id_hex(&mut key, batch_id);
    key
}

#[allow(dead_code)]
pub(super) fn history_row_prefix(table: &str) -> String {
    let mut prefix = String::with_capacity(4 + table.len() + 3);
    prefix.push_str("row:");
    prefix.push_str(table);
    prefix.push_str(":1:");
    prefix
}

pub(super) fn decode_history_row_key(
    table: &str,
    key: &str,
) -> Result<(ObjectId, String, BatchId), StorageError> {
    let prefix = history_row_prefix(table);
    let rest = key
        .strip_prefix(&prefix)
        .ok_or_else(|| StorageError::IoError(format!("invalid history row key '{key}'")))?;
    let (row_hex, branch_and_batch) = rest.split_once(':').ok_or_else(|| {
        StorageError::IoError(format!("invalid history row key '{key}': missing branch"))
    })?;
    let (branch, batch_hex) = branch_and_batch.rsplit_once(':').ok_or_else(|| {
        StorageError::IoError(format!("invalid history row key '{key}': missing batch id"))
    })?;
    Ok((
        decode_hex_object_id(row_hex, &format!("decode history row key '{key}'"))?,
        branch.to_string(),
        decode_hex_batch_id(batch_hex, &format!("decode history row key '{key}'"))?,
    ))
}

#[allow(dead_code)]
pub(super) fn history_row_batches_prefix(table: &str, row_id: ObjectId) -> String {
    let mut prefix = String::with_capacity(4 + table.len() + 3 + INDEX_ENTRY_UUID_HEX_BYTES + 1);
    prefix.push_str("row:");
    prefix.push_str(table);
    prefix.push_str(":1:");
    append_uuid_hex(&mut prefix, row_id);
    prefix.push(':');
    prefix
}

#[allow(dead_code)]
pub(super) fn history_table_prefix(table: &str) -> String {
    history_row_prefix(table)
}

pub(super) fn catalogue_entry_key(object_id: ObjectId) -> String {
    let mut key = String::with_capacity(7 + INDEX_ENTRY_UUID_HEX_BYTES);
    key.push_str("catrow:");
    append_uuid_hex(&mut key, object_id);
    key
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
    let value_segment = encode_index_value_segment(table, column, branch, value)?;
    let mut key = String::with_capacity(value_segment.len() + 1 + INDEX_ENTRY_UUID_HEX_BYTES);
    key.push_str(&value_segment);
    key.push(':');
    append_uuid_hex(&mut key, row_id);
    Ok(key)
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
    let mut prefix = String::with_capacity(value_segment.len() + 1);
    prefix.push_str(&value_segment);
    prefix.push(':');
    Ok(prefix)
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
    fn hex_nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let uuid_hex = key.rsplit(':').next()?.as_bytes();
    if uuid_hex.len() != INDEX_ENTRY_UUID_HEX_BYTES {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (i, chunk) in uuid_hex.chunks_exact(2).enumerate() {
        let high = hex_nibble(chunk[0])?;
        let low = hex_nibble(chunk[1])?;
        bytes[i] = (high << 4) | low;
    }
    Some(ObjectId::from_uuid(uuid::Uuid::from_bytes(bytes)))
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
