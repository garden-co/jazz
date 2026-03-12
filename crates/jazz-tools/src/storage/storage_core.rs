use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use serde::{Serialize, de::DeserializeOwned};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::sync_manager::{DurabilityTier, MutationId, MutationOutcomeFilter, MutationRecord};

use crate::query_manager::types::Value;

use super::key_codec::{
    ack_key, branch_inactive_commits_key, branch_tips_key, catalogue_manifest_op_key,
    catalogue_manifest_op_prefix, commit_key, commit_prefix, index_entry_key, index_prefix,
    index_range_scan_bounds, index_value_prefix, mutation_commit_index_key, mutation_record_key,
    mutation_record_prefix, obj_meta_key, parse_uuid_from_index_key,
};
use super::{CatalogueManifest, CatalogueManifestOp, LoadedBranch, StorageError};

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

    let inactive_key = branch_inactive_commits_key(object_id, branch);
    let inactive_commits = match get(&inactive_key)? {
        Some(data) => decode_json(&data, "inactive commits")?,
        None => HashSet::new(),
    };

    Ok(Some(LoadedBranch {
        commits,
        inactive_commits,
        tails,
    }))
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

    let inactive_key = branch_inactive_commits_key(object_id, branch);
    if let Some(data) = get(&inactive_key)? {
        let mut inactive: HashSet<CommitId> = decode_json(&data, "inactive commits")?;
        if inactive.remove(&commit_id) {
            if inactive.is_empty() {
                delete(&inactive_key)?;
            } else {
                let inactive_json = encode_json(&inactive, "inactive commits")?;
                set(&inactive_key, &inactive_json)?;
            }
        }
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

pub(super) fn set_branch_inactive_commits_core(
    object_id: ObjectId,
    branch: &BranchName,
    inactive_commits: Option<HashSet<CommitId>>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
    mut delete: impl FnMut(&str) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let inactive_key = branch_inactive_commits_key(object_id, branch);
    match inactive_commits {
        Some(commits) if !commits.is_empty() => {
            let json = encode_json(&commits, "inactive commits")?;
            set(&inactive_key, &json)
        }
        _ => delete(&inactive_key),
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

pub(super) fn put_mutation_record_core(
    record: MutationRecord,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
    mut delete: impl FnMut(&str) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let record_key = mutation_record_key(record.id);

    if let Some(existing) = get(&record_key)? {
        let existing_record: MutationRecord = decode_json(&existing, "mutation record")?;
        for commit_id in existing_record.commit_ids {
            if !record.commit_ids.contains(&commit_id) {
                delete(&mutation_commit_index_key(commit_id))?;
            }
        }
    }

    let record_json = encode_json(&record, "mutation record")?;
    set(&record_key, &record_json)?;

    let mutation_id_json = encode_json(&record.id, "mutation id")?;
    for &commit_id in &record.commit_ids {
        let key = mutation_commit_index_key(commit_id);
        set(&key, &mutation_id_json)?;
    }

    Ok(())
}

pub(super) fn load_mutation_record_core(
    mutation_id: MutationId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<MutationRecord>, StorageError> {
    let key = mutation_record_key(mutation_id);
    match get(&key)? {
        Some(data) => Ok(Some(decode_json(&data, "mutation record")?)),
        None => Ok(None),
    }
}

pub(super) fn load_mutation_record_by_commit_core(
    commit_id: CommitId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<MutationRecord>, StorageError> {
    let commit_key = mutation_commit_index_key(commit_id);
    let Some(mutation_id_bytes) = get(&commit_key)? else {
        return Ok(None);
    };
    let mutation_id: MutationId = decode_json(&mutation_id_bytes, "mutation id")?;
    load_mutation_record_core(mutation_id, get)
}

pub(super) fn delete_mutation_record_core(
    mutation_id: MutationId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut delete: impl FnMut(&str) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    if let Some(record) = load_mutation_record_core(mutation_id, &mut get)? {
        for commit_id in record.commit_ids {
            delete(&mutation_commit_index_key(commit_id))?;
        }
        delete(&mutation_record_key(mutation_id))?;
    }

    Ok(())
}

pub(super) fn list_mutation_records_by_outcome_core(
    outcome: MutationOutcomeFilter,
    mut scan_prefix: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<Vec<MutationRecord>, StorageError> {
    let entries = scan_prefix(mutation_record_prefix())?;
    let mut records = Vec::new();

    for (_key, data) in entries {
        let record: MutationRecord = decode_json(&data, "mutation record")?;
        if record.matches_filter(outcome) {
            records.push(record);
        }
    }

    Ok(records)
}

pub(super) fn list_mutation_records_for_object_core(
    object_id: ObjectId,
    mut scan_prefix: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<Vec<MutationRecord>, StorageError> {
    let entries = scan_prefix(mutation_record_prefix())?;
    let mut records = Vec::new();

    for (_key, data) in entries {
        let record: MutationRecord = decode_json(&data, "mutation record")?;
        if record.object_id == object_id {
            records.push(record);
        }
    }

    Ok(records)
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
