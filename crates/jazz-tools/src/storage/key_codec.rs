use std::ops::Bound;

use crate::commit::CommitId;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::{BatchId, ComposedBranchName, Value};

use super::{StorageError, encode_value};

const INDEX_KEY_MAX_BYTES: usize = u16::MAX as usize;
const INDEX_ENTRY_UUID_HEX_BYTES: usize = 32;
const OVERFLOW_INDEX_VALUE_MARKER: char = '~';
const OVERFLOW_INDEX_VALUE_LEN_HEX_BYTES: usize = 16;
const OVERFLOW_INDEX_VALUE_HASH_HEX_BYTES: usize = blake3::OUT_LEN * 2;
const OVERFLOW_INDEX_VALUE_TRAILER_BYTES: usize =
    1 + OVERFLOW_INDEX_VALUE_LEN_HEX_BYTES + OVERFLOW_INDEX_VALUE_HASH_HEX_BYTES;

fn index_entry_key_bytes(
    table: &str,
    column: &str,
    branch_key: &str,
    value_segment_len: usize,
) -> usize {
    "idx:".len()
        + table.len()
        + 1
        + column.len()
        + 1
        + branch_key.len()
        + 1
        + value_segment_len
        + 1
        + INDEX_ENTRY_UUID_HEX_BYTES
}

fn index_value_prefix_bytes(
    table: &str,
    column: &str,
    branch_key: &str,
    value_segment_len: usize,
) -> usize {
    "idx:".len() + table.len() + 1 + column.len() + 1 + branch_key.len() + 1 + value_segment_len + 1
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

pub(super) fn encode_index_branch_key(branch: &str) -> String {
    let branch_name = BranchName::new(branch.to_string());
    if let Some(composed_branch) = ComposedBranchName::parse(&branch_name) {
        format!("c{}", composed_branch.batch_id.branch_segment())
    } else {
        format!("r{}", hex::encode(branch.as_bytes()))
    }
}

fn encode_branch_prefix_storage_id(prefix: &str) -> String {
    hex::encode(uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, prefix.as_bytes()).as_bytes())
}

pub(super) fn encode_object_branch_key(branch: &BranchName) -> String {
    if let Some(composed_branch) = ComposedBranchName::parse(branch) {
        format!(
            "c{}:{}",
            encode_branch_prefix_storage_id(&composed_branch.prefix().branch_prefix()),
            composed_branch.batch_id.branch_segment()
        )
    } else {
        format!("r{}", hex::encode(branch.as_str().as_bytes()))
    }
}

fn max_index_value_segment_len(table: &str, column: &str, branch_key: &str) -> Option<usize> {
    INDEX_KEY_MAX_BYTES.checked_sub(index_entry_key_bytes(table, column, branch_key, 0))
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
    let branch_key = encode_index_branch_key(branch);
    let Some(max_segment_len) = max_index_value_segment_len(table, column, &branch_key) else {
        return Err(index_key_too_large_error(
            table,
            column,
            branch,
            index_entry_key_bytes(table, column, &branch_key, 0),
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
            index_entry_key_bytes(
                table,
                column,
                &branch_key,
                OVERFLOW_INDEX_VALUE_TRAILER_BYTES,
            ),
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

pub(super) fn branch_state_key(object_id: ObjectId, branch: &BranchName) -> String {
    format!(
        "obj:{}:br:{}:state",
        format_uuid(object_id),
        encode_object_branch_key(branch)
    )
}

pub(super) fn commit_branch_key(object_id: ObjectId, commit_id: CommitId) -> String {
    format!(
        "obj:{}:commit-branch:{}",
        format_uuid(object_id),
        hex::encode(commit_id.0)
    )
}

pub(super) fn prefix_leaf_batches_key(object_id: ObjectId, prefix: &str) -> String {
    format!(
        "obj:{}:prefix:{}:leaf-batches",
        format_uuid(object_id),
        prefix
    )
}

pub(super) fn prefix_batch_meta_key(
    object_id: ObjectId,
    prefix: &str,
    batch_id: BatchId,
) -> String {
    format!(
        "obj:{}:prefix:{}:batch:{}",
        format_uuid(object_id),
        prefix,
        batch_id.branch_segment()
    )
}

pub(super) fn prefix_batch_meta_prefix(object_id: ObjectId, prefix: &str) -> String {
    format!("obj:{}:prefix:{}:batch:", format_uuid(object_id), prefix)
}

pub(super) fn table_prefix_batch_key(table: &str, prefix: &str, batch_id: BatchId) -> String {
    format!(
        "tblpfx:{}:{}:batch:{}",
        table,
        prefix,
        batch_id.branch_segment()
    )
}

pub(super) fn table_prefix_batch_prefix(table: &str, prefix: &str) -> String {
    format!("tblpfx:{}:{}:batch:", table, prefix)
}

pub(super) fn parse_batch_id_from_table_prefix_key(
    key: &str,
    key_prefix: &str,
) -> Result<BatchId, StorageError> {
    let batch_segment = key.strip_prefix(key_prefix).ok_or_else(|| {
        StorageError::IoError(format!(
            "invalid table-prefix batch key `{key}` for prefix `{key_prefix}`"
        ))
    })?;
    BatchId::parse_segment(batch_segment).ok_or_else(|| {
        StorageError::IoError(format!(
            "invalid batch id `{batch_segment}` in table-prefix batch key `{key}`"
        ))
    })
}

pub(super) fn ack_key(commit_id: CommitId) -> String {
    format!("ack:{}", hex::encode(commit_id.0))
}

pub(super) fn catalogue_manifest_op_key(app_id: ObjectId, object_id: ObjectId) -> String {
    format!(
        "catman:{}:op:{}",
        format_uuid(app_id),
        format_uuid(object_id)
    )
}

pub(super) fn catalogue_manifest_op_prefix(app_id: ObjectId) -> String {
    format!("catman:{}:op:", format_uuid(app_id))
}

pub(super) fn index_entry_key(
    table: &str,
    column: &str,
    branch: &str,
    value: &Value,
    row_id: ObjectId,
) -> Result<String, StorageError> {
    let branch_key = encode_index_branch_key(branch);
    Ok(format!(
        "idx:{}:{}:{}:{}:{}",
        table,
        column,
        branch_key,
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
    let branch_key = encode_index_branch_key(branch);
    let key_bytes = index_value_prefix_bytes(table, column, &branch_key, value_segment.len());
    if key_bytes > INDEX_KEY_MAX_BYTES {
        return Err(index_key_too_large_error(table, column, branch, key_bytes));
    }
    Ok(format!(
        "idx:{}:{}:{}:{}:",
        table, column, branch_key, value_segment
    ))
}

pub(super) fn index_prefix(table: &str, column: &str, branch: &str) -> String {
    format!(
        "idx:{}:{}:{}:",
        table,
        column,
        encode_index_branch_key(branch)
    )
}

/// Compute lexicographic scan bounds for index range queries.
pub(super) fn index_range_scan_bounds(
    table: &str,
    column: &str,
    branch: &str,
    start: Bound<&Value>,
    end: Bound<&Value>,
) -> Option<(String, String)> {
    let base_prefix = index_prefix(table, column, branch);

    // IEEE 754: -0.0 == 0.0 but they encode differently. Adjust bounds
    // so both zeros are treated as the same point.
    let neg_zero = Value::Double(-0.0);
    let pos_zero = Value::Double(0.0);
    let neg_zero_segment = encode_index_value_segment(table, column, branch, &neg_zero).ok()?;
    let pos_zero_segment = encode_index_value_segment(table, column, branch, &pos_zero).ok()?;

    let start_key = match start {
        Bound::Included(v) if super::is_double_zero(v) => {
            format!("{}{}", base_prefix, neg_zero_segment)
        }
        Bound::Excluded(v) if super::is_double_zero(v) => {
            let mut key = format!("{}{}:", base_prefix, pos_zero_segment);
            increment_string(&mut key);
            key
        }
        Bound::Included(v) => {
            let segment = encode_index_value_segment(table, column, branch, v).ok()?;
            format!("{}{}", base_prefix, segment)
        }
        Bound::Excluded(v) => {
            let segment = encode_index_value_segment(table, column, branch, v).ok()?;
            let mut key = format!("{}{}:", base_prefix, segment);
            increment_string(&mut key);
            key
        }
        Bound::Unbounded => base_prefix.clone(),
    };

    let end_key = match end {
        Bound::Included(v) if super::is_double_zero(v) => {
            let mut key = format!("{}{}:", base_prefix, pos_zero_segment);
            increment_string(&mut key);
            key
        }
        Bound::Excluded(v) if super::is_double_zero(v) => {
            format!("{}{}", base_prefix, neg_zero_segment)
        }
        Bound::Included(v) => {
            let segment = encode_index_value_segment(table, column, branch, v).ok()?;
            let mut key = format!("{}{}:", base_prefix, segment);
            increment_string(&mut key);
            key
        }
        Bound::Excluded(v) => {
            let segment = encode_index_value_segment(table, column, branch, v).ok()?;
            format!("{}{}", base_prefix, segment)
        }
        Bound::Unbounded => {
            let mut end = base_prefix.clone();
            increment_string(&mut end);
            end
        }
    };

    (start_key < end_key).then_some((start_key, end_key))
}

/// Parse a UUID from the last segment of an index key.
/// Key format: `idx:{table}:{col}:{branch_key}:{value_segment}:{uuid_hex}`
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

    #[test]
    fn composed_branch_index_keys_only_store_batch_segment() {
        let branch = BranchName::new("dev-070707070707-main-b00000000000000000000000000000001");
        let key = index_entry_key(
            "users",
            "name",
            branch.as_str(),
            &Value::Text("Alice".into()),
            ObjectId::from_uuid(uuid::Uuid::nil()),
        )
        .expect("key should encode");

        assert!(key.contains(":cb00000000000000000000000000000001:"));
        assert!(!key.contains(branch.as_str()));
    }

    #[test]
    fn composed_object_branch_keys_store_prefix_id_and_batch_only() {
        let branch = BranchName::new("dev-070707070707-main-b00000000000000000000000000000001");
        let key = branch_state_key(ObjectId::from_uuid(uuid::Uuid::nil()), &branch);

        assert!(key.contains(":br:c"));
        assert!(key.contains(":b00000000000000000000000000000001:state"));
        assert!(!key.contains(branch.as_str()));
    }
}
