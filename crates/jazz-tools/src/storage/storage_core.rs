use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use serde::{Serialize, de::DeserializeOwned};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId, PrefixBatchCatalog, PrefixBatchMeta};
use crate::sync_manager::DurabilityTier;

use crate::query_manager::types::{BatchId, Value};

use super::key_codec::{
    ack_key, branch_tips_key, catalogue_manifest_op_key, catalogue_manifest_op_prefix,
    commit_branch_key, commit_key, commit_prefix, index_entry_key, index_prefix,
    index_range_scan_bounds, index_value_prefix, obj_meta_key, parse_branch_from_table_prefix_key,
    parse_uuid_from_index_key, prefix_batch_meta_key, prefix_batch_meta_prefix,
    prefix_leaf_batches_key, table_prefix_branch_key, table_prefix_branch_prefix,
};
use super::{
    CatalogueManifest, CatalogueManifestOp, LoadedBranch, PrefixBatchUpdate, StorageError,
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

pub(super) fn load_commit_branch_core(
    object_id: ObjectId,
    commit_id: CommitId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<BranchName>, StorageError> {
    let key = commit_branch_key(object_id, commit_id);
    match get(&key)? {
        Some(data) => Ok(Some(decode_json(&data, "commit branch")?)),
        None => Ok(None),
    }
}

pub(super) fn load_prefix_batch_catalog_core(
    object_id: ObjectId,
    prefix: &str,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut scan_prefix: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<Option<PrefixBatchCatalog>, StorageError> {
    let leaf_key = prefix_leaf_batches_key(object_id, prefix);
    let meta_prefix = prefix_batch_meta_prefix(object_id, prefix);

    let leaf_batches: HashSet<BatchId> = match get(&leaf_key)? {
        Some(data) => decode_json(&data, "prefix leaf batches")?,
        None => HashSet::new(),
    };
    let entries = scan_prefix(&meta_prefix)?;
    if leaf_batches.is_empty() && entries.is_empty() {
        return Ok(None);
    }

    let mut catalog = PrefixBatchCatalog::default();
    for (_key, data) in entries {
        let meta: PrefixBatchMeta = decode_json(&data, "prefix batch meta")?;
        catalog.batches.insert(meta.batch_id, meta);
    }
    catalog.leaf_batches = leaf_batches.into_iter().collect();
    Ok(Some(catalog))
}

fn load_prefix_batch_meta(
    object_id: ObjectId,
    prefix: &str,
    batch_id: BatchId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<PrefixBatchMeta>, StorageError> {
    let key = prefix_batch_meta_key(object_id, prefix, batch_id);
    match get(&key)? {
        Some(data) => Ok(Some(decode_json(&data, "prefix batch meta")?)),
        None => Ok(None),
    }
}

pub(super) fn register_table_prefix_branch_core(
    table: &str,
    prefix: &str,
    branch: &BranchName,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let key = table_prefix_branch_key(table, prefix, branch);
    set(&key, &[])
}

pub(super) fn load_table_prefix_branches_core(
    table: &str,
    prefix: &str,
    mut scan_prefix: impl FnMut(&str) -> Result<Vec<(String, Vec<u8>)>, StorageError>,
) -> Result<HashSet<BranchName>, StorageError> {
    let key_prefix = table_prefix_branch_prefix(table, prefix);
    let entries = scan_prefix(&key_prefix)?;
    let mut branches = HashSet::with_capacity(entries.len());
    for (key, _value) in entries {
        branches.insert(parse_branch_from_table_prefix_key(&key, &key_prefix)?);
    }
    Ok(branches)
}

pub(super) fn append_commit_core(
    object_id: ObjectId,
    branch: &BranchName,
    commit: Commit,
    prefix_batch_update: Option<PrefixBatchUpdate>,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let commit_id = commit.id();

    let commit_storage_key = commit_key(object_id, branch, commit_id);
    let commit_json = encode_json(&commit, "commit")?;
    set(&commit_storage_key, &commit_json)?;

    let commit_branch_lookup_key = commit_branch_key(object_id, commit_id);
    let commit_branch_json = encode_json(branch, "commit branch")?;
    set(&commit_branch_lookup_key, &commit_branch_json)?;

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
    set(&tips_key, &tips_json)?;

    if let Some(update) = prefix_batch_update {
        for parent_batch_id in &update.increment_parent_child_counts {
            if let Some(mut parent_meta) =
                load_prefix_batch_meta(object_id, &update.prefix, *parent_batch_id, |key| get(key))?
            {
                parent_meta.child_count = parent_meta.child_count.saturating_add(1);
                let parent_key = prefix_batch_meta_key(object_id, &update.prefix, *parent_batch_id);
                let parent_json = encode_json(&parent_meta, "prefix batch meta")?;
                set(&parent_key, &parent_json)?;
            }
        }

        let leaf_key = prefix_leaf_batches_key(object_id, &update.prefix);
        let mut leaf_batches: HashSet<BatchId> = match get(&leaf_key)? {
            Some(data) => decode_json(&data, "prefix leaf batches")?,
            None => HashSet::new(),
        };
        for removed_batch in update.remove_leaf_batches {
            leaf_batches.remove(&removed_batch);
        }
        leaf_batches.insert(update.batch_meta.batch_id);
        let leaf_json = encode_json(&leaf_batches, "prefix leaf batches")?;
        set(&leaf_key, &leaf_json)?;

        let batch_key =
            prefix_batch_meta_key(object_id, &update.prefix, update.batch_meta.batch_id);
        let batch_json = encode_json(&update.batch_meta, "prefix batch meta")?;
        set(&batch_key, &batch_json)?;
    }

    Ok(())
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

    let commit_branch_lookup_key = commit_branch_key(object_id, commit_id);
    delete(&commit_branch_lookup_key)?;

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
    let key = index_entry_key(table, column, branch, value, row_id)?;
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
    let key = match index_entry_key(table, column, branch, value, row_id) {
        Ok(key) => key,
        Err(StorageError::IndexKeyTooLarge { .. }) => return Ok(()),
        Err(error) => return Err(error),
    };
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
            let Ok(prefix) = index_value_prefix(table, column, branch, zero) else {
                continue;
            };
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

    let Ok(prefix) = index_value_prefix(table, column, branch, value) else {
        return Vec::new();
    };
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
