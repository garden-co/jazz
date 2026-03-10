use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use serde::{Serialize, de::DeserializeOwned};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::sync_manager::DurabilityTier;

use crate::query_manager::types::Value;

use super::key_codec::{
    ack_key, branch_tips_key, catalogue_manifest_op_key, catalogue_manifest_op_prefix, commit_key,
    commit_prefix, index_entry_key, index_prefix, index_range_scan_bounds, index_value_prefix,
    obj_meta_key, parse_ordered_cursor_from_index_key, parse_uuid_from_index_key,
};
use super::{
    CatalogueManifest, CatalogueManifestOp, IndexScanDirection, LoadedBranch, OrderedIndexCursor,
    OrderedIndexScan, StorageError,
};

fn encode_json<T: Serialize>(value: &T, label: &str) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(value).map_err(|e| StorageError::IoError(format!("serialize {label}: {e}")))
}

fn decode_json<T: DeserializeOwned>(bytes: &[u8], label: &str) -> Result<T, StorageError> {
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

pub(super) fn index_insert_core(
    table: &str,
    column: &str,
    branch: &str,
    value: &Value,
    row_id: ObjectId,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let key = index_entry_key(table, column, branch, value, row_id);
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
    let key = index_entry_key(table, column, branch, value, row_id);
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
            let prefix = index_value_prefix(table, column, branch, zero);
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

    let prefix = index_value_prefix(table, column, branch, value);
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

fn index_value_key_prefix(key: &str) -> &str {
    key.rsplit_once(':')
        .map(|(prefix, _)| prefix)
        .unwrap_or(key)
}

pub(super) fn ordered_index_scan_bounds(scan: OrderedIndexScan<'_>) -> Option<(String, String)> {
    index_range_scan_bounds(scan.table, scan.column, scan.branch, scan.start, scan.end)
}

pub(super) struct OrderedScanCollector {
    direction: IndexScanDirection,
    limit: Option<usize>,
    entries: Vec<OrderedIndexCursor>,
    current_group_prefix: Option<String>,
    current_group_entries: Vec<OrderedIndexCursor>,
    resume_after: Option<OrderedScanResume>,
}

#[derive(Debug)]
struct OrderedScanResume {
    value_prefix: String,
    row_id: ObjectId,
}

impl OrderedScanCollector {
    pub(super) fn new(direction: IndexScanDirection, take: Option<usize>) -> Self {
        Self {
            direction,
            limit: take,
            entries: Vec::with_capacity(take.unwrap_or_default()),
            current_group_prefix: None,
            current_group_entries: Vec::new(),
            resume_after: None,
        }
    }

    pub(super) fn with_cursor(
        direction: IndexScanDirection,
        take: Option<usize>,
        table: &str,
        column: &str,
        branch: &str,
        resume_after: Option<&OrderedIndexCursor>,
    ) -> Self {
        let mut collector = Self::new(direction, take);
        collector.resume_after = resume_after.map(|cursor| OrderedScanResume {
            value_prefix: index_value_prefix(table, column, branch, &cursor.value),
            row_id: cursor.row_id,
        });
        collector
    }

    fn is_after_cursor(&self, key: &str) -> bool {
        let Some(resume_after) = &self.resume_after else {
            return true;
        };

        let key_prefix = index_value_key_prefix(key);
        match self.direction {
            IndexScanDirection::Ascending => {
                if key_prefix < resume_after.value_prefix.as_str() {
                    return false;
                }
                if key_prefix > resume_after.value_prefix.as_str() {
                    return true;
                }
            }
            IndexScanDirection::Descending => {
                if key_prefix > resume_after.value_prefix.as_str() {
                    return false;
                }
                if key_prefix < resume_after.value_prefix.as_str() {
                    return true;
                }
            }
        }

        parse_uuid_from_index_key(key).is_some_and(|row_id| row_id > resume_after.row_id)
    }

    fn maybe_clear_cursor(&mut self, key: &str) {
        if self.is_after_cursor(key) {
            self.resume_after = None;
        }
    }

    pub(super) fn should_continue(&self) -> bool {
        self.remaining_slots() != Some(0)
    }

    pub(super) fn consume_key(&mut self, key: &str) -> bool {
        if !self.should_continue() {
            return false;
        }

        if !self.is_after_cursor(key) {
            return true;
        }
        self.maybe_clear_cursor(key);

        match self.direction {
            IndexScanDirection::Ascending => {
                if let Some(cursor) = parse_ordered_cursor_from_index_key(key) {
                    self.entries.push(cursor);
                }
                self.should_continue()
            }
            IndexScanDirection::Descending => {
                let group_prefix = index_value_key_prefix(key);
                if self.current_group_prefix.as_deref() != Some(group_prefix) {
                    if !self.flush_descending_group() {
                        return false;
                    }
                    self.current_group_prefix = Some(group_prefix.to_owned());
                }

                if let Some(cursor) = parse_ordered_cursor_from_index_key(key) {
                    self.current_group_entries.push(cursor);
                }

                true
            }
        }
    }

    pub(super) fn finish(mut self) -> Vec<OrderedIndexCursor> {
        if self.direction == IndexScanDirection::Descending {
            let _ = self.flush_descending_group();
        }
        self.entries
    }

    fn remaining_slots(&self) -> Option<usize> {
        self.limit
            .map(|limit| limit.saturating_sub(self.entries.len()))
    }

    fn flush_descending_group(&mut self) -> bool {
        if self.current_group_entries.is_empty() {
            self.current_group_prefix = None;
            return self.should_continue();
        }

        let take = self
            .remaining_slots()
            .unwrap_or(self.current_group_entries.len())
            .min(self.current_group_entries.len());
        self.entries
            .extend(self.current_group_entries.iter().rev().take(take).cloned());
        self.current_group_entries.clear();
        self.current_group_prefix = None;
        self.should_continue()
    }
}

#[cfg(test)]
pub(super) fn index_scan_ordered_core(
    scan: OrderedIndexScan<'_>,
    mut scan_key_range: impl FnMut(&str, &str) -> Result<Vec<String>, StorageError>,
) -> Vec<OrderedIndexCursor> {
    let Some((start_key, end_key)) = ordered_index_scan_bounds(scan) else {
        return Vec::new();
    };

    let Ok(keys) = scan_key_range(&start_key, &end_key) else {
        return Vec::new();
    };

    let mut collector = OrderedScanCollector::new(scan.direction, scan.take);
    match scan.direction {
        IndexScanDirection::Ascending => {
            for key in &keys {
                if !collector.consume_key(key) {
                    break;
                }
            }
        }
        IndexScanDirection::Descending => {
            for key in keys.iter().rev() {
                if !collector.consume_key(key) {
                    break;
                }
            }
        }
    }

    collector.finish()
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use crate::query_manager::types::Value;

    use super::*;

    #[test]
    fn descending_ordered_scan_keeps_id_tiebreaker_for_equal_values() {
        let row20 = ObjectId::new();
        let row25a = ObjectId::new();
        let row25b = ObjectId::new();
        let row30 = ObjectId::new();

        let mut keys = vec![
            index_entry_key("users", "age", "main", &Value::Integer(20), row20),
            index_entry_key("users", "age", "main", &Value::Integer(25), row25b),
            index_entry_key("users", "age", "main", &Value::Integer(25), row25a),
            index_entry_key("users", "age", "main", &Value::Integer(30), row30),
        ];
        keys.sort();

        let results = index_scan_ordered_core(
            OrderedIndexScan {
                table: "users",
                column: "age",
                branch: "main",
                start: Bound::Unbounded,
                end: Bound::Included(&Value::Integer(25)),
                direction: IndexScanDirection::Descending,
                take: Some(3),
                resume_after: None,
            },
            |start, end| {
                Ok(keys
                    .iter()
                    .filter(|key| key.as_str() >= start && key.as_str() < end)
                    .cloned()
                    .collect())
            },
        );

        assert_eq!(
            results
                .into_iter()
                .map(|cursor| cursor.row_id)
                .collect::<Vec<_>>(),
            vec![row25a, row25b, row20]
        );
    }

    #[test]
    fn ascending_ordered_scan_collector_stops_at_take() {
        let row20 = ObjectId::new();
        let row25 = ObjectId::new();
        let row30 = ObjectId::new();
        let keys = [
            index_entry_key("users", "age", "main", &Value::Integer(20), row20),
            index_entry_key("users", "age", "main", &Value::Integer(25), row25),
            index_entry_key("users", "age", "main", &Value::Integer(30), row30),
        ];

        let mut collector = OrderedScanCollector::new(IndexScanDirection::Ascending, Some(2));
        let mut visited = 0usize;

        for key in &keys {
            visited += 1;
            if !collector.consume_key(key) {
                break;
            }
        }

        assert_eq!(visited, 2);
        assert_eq!(
            collector
                .finish()
                .into_iter()
                .map(|cursor| cursor.row_id)
                .collect::<Vec<_>>(),
            vec![row20, row25]
        );
    }

    #[test]
    fn descending_ordered_scan_collector_stops_after_deciding_group() {
        let row10 = ObjectId::new();
        let row20a = ObjectId::new();
        let row20b = ObjectId::new();
        let row20c = ObjectId::new();
        let row30 = ObjectId::new();
        let row40 = ObjectId::new();

        let mut keys = vec![
            index_entry_key("users", "age", "main", &Value::Integer(10), row10),
            index_entry_key("users", "age", "main", &Value::Integer(20), row20c),
            index_entry_key("users", "age", "main", &Value::Integer(20), row20a),
            index_entry_key("users", "age", "main", &Value::Integer(20), row20b),
            index_entry_key("users", "age", "main", &Value::Integer(30), row30),
            index_entry_key("users", "age", "main", &Value::Integer(40), row40),
        ];
        keys.sort();

        let mut collector = OrderedScanCollector::new(IndexScanDirection::Descending, Some(3));
        let mut visited = 0usize;

        for key in keys.iter().rev() {
            visited += 1;
            if !collector.consume_key(key) {
                break;
            }
        }

        assert_eq!(visited, 6);
        assert_eq!(
            collector
                .finish()
                .into_iter()
                .map(|cursor| cursor.row_id)
                .collect::<Vec<_>>(),
            vec![row40, row30, row20a]
        );
    }
}
