use crate::commit::CommitId;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::Value;

use super::encode_value;

/// Format an ObjectId as compact hex (no dashes).
pub(super) fn format_uuid(id: ObjectId) -> String {
    hex::encode(id.uuid().as_bytes())
}

pub(super) fn obj_meta_key(id: ObjectId) -> String {
    format!("obj:{}:meta", format_uuid(id))
}

pub(super) fn branch_tips_key(object_id: ObjectId, branch: &BranchName) -> String {
    format!("obj:{}:br:{}:tips", format_uuid(object_id), branch)
}

pub(super) fn commit_key(object_id: ObjectId, branch: &BranchName, commit_id: CommitId) -> String {
    format!(
        "obj:{}:br:{}:c:{}",
        format_uuid(object_id),
        branch,
        hex::encode(commit_id.0)
    )
}

pub(super) fn commit_prefix(object_id: ObjectId, branch: &BranchName) -> String {
    format!("obj:{}:br:{}:c:", format_uuid(object_id), branch)
}

pub(super) fn ack_key(commit_id: CommitId) -> String {
    format!("ack:{}", hex::encode(commit_id.0))
}

pub(super) fn index_entry_key(
    table: &str,
    column: &str,
    branch: &str,
    value: &Value,
    row_id: ObjectId,
) -> String {
    format!(
        "idx:{}:{}:{}:{}:{}",
        table,
        column,
        branch,
        hex::encode(encode_value(value)),
        format_uuid(row_id)
    )
}

pub(super) fn index_value_prefix(table: &str, column: &str, branch: &str, value: &Value) -> String {
    format!(
        "idx:{}:{}:{}:{}:",
        table,
        column,
        branch,
        hex::encode(encode_value(value))
    )
}

pub(super) fn index_prefix(table: &str, column: &str, branch: &str) -> String {
    format!("idx:{}:{}:{}:", table, column, branch)
}

/// Parse a UUID from the last segment of an index key.
/// Key format: `idx:{table}:{col}:{branch}:{hex_value}:{uuid_hex}`
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
