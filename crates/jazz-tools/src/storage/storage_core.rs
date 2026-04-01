use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId, PrefixBatchCatalog, PrefixBatchMeta};
use crate::sync_manager::DurabilityTier;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use smallvec::SmallVec;

use crate::query_manager::types::{BatchBranchKey, BatchId, BatchOrd, QueryBranchRef, Value};

use super::key_codec::{
    ack_key, branch_manifest_key, branch_segment_key, catalogue_manifest_op_key,
    catalogue_manifest_op_prefix, commit_branch_key, index_entry_key, index_prefix,
    index_range_scan_bounds, index_value_prefix, obj_meta_key, parse_uuid_from_index_key,
    prefix_batch_catalog_key, table_prefix_batches_key,
};
use super::{
    CatalogueManifest, CatalogueManifestOp, LoadedBranch, LoadedBranchTips, PrefixBatchUpdate,
    StorageError, TablePrefixBatchManifest,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredBranchRef {
    prefix: String,
    batch_id: BatchId,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedBranchManifest {
    segment_ids: Vec<u32>,
    tails: HashSet<CommitId>,
    tip_commits: Vec<Commit>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedBranchSegment {
    commits: Vec<Commit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedPrefixBatchCatalogEntry {
    batch_id: BatchId,
    head_commit_id: CommitId,
    last_timestamp: u64,
    child_count: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedPrefixBatchCatalog {
    batches: Vec<PersistedPrefixBatchCatalogEntry>,
}

const MAX_COMMITS_PER_BRANCH_SEGMENT: usize = 32;
const STORAGE_BINARY_V1: u8 = 1;

impl StoredBranchRef {
    fn from_branch_ref(branch: &QueryBranchRef) -> Self {
        Self {
            prefix: branch.prefix_name().as_str().to_string(),
            batch_id: branch.batch_id(),
        }
    }

    fn to_branch_ref(&self) -> QueryBranchRef {
        QueryBranchRef::from_prefix_name_and_batch(
            BranchName::new(self.prefix.clone()),
            self.batch_id,
        )
    }
}

fn encode_json<T: Serialize>(value: &T, label: &str) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(value).map_err(|e| StorageError::IoError(format!("serialize {label}: {e}")))
}

fn decode_json<T: DeserializeOwned>(bytes: &[u8], label: &str) -> Result<T, StorageError> {
    serde_json::from_slice(bytes)
        .map_err(|e| StorageError::IoError(format!("deserialize {label}: {e}")))
}

fn codec_error(label: &str, message: impl Into<String>) -> StorageError {
    StorageError::IoError(format!("{label}: {}", message.into()))
}

#[derive(Clone, Copy)]
struct BinaryCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BinaryCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, label: &str, len: usize) -> Result<&'a [u8], StorageError> {
        let end = self.offset.saturating_add(len);
        let Some(slice) = self.bytes.get(self.offset..end) else {
            return Err(codec_error(label, "unexpected end of payload"));
        };
        self.offset = end;
        Ok(slice)
    }

    fn read_u8(&mut self, label: &str) -> Result<u8, StorageError> {
        Ok(self.read_exact(label, 1)?[0])
    }

    fn read_u32(&mut self, label: &str) -> Result<u32, StorageError> {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(self.read_exact(label, 4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self, label: &str) -> Result<u64, StorageError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.read_exact(label, 8)?);
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_fixed<const N: usize>(&mut self, label: &str) -> Result<[u8; N], StorageError> {
        let mut bytes = [0u8; N];
        bytes.copy_from_slice(self.read_exact(label, N)?);
        Ok(bytes)
    }

    fn read_len_prefixed_bytes(&mut self, label: &str) -> Result<Vec<u8>, StorageError> {
        let len = self.read_u32(label)? as usize;
        Ok(self.read_exact(label, len)?.to_vec())
    }

    fn read_string(&mut self, label: &str) -> Result<String, StorageError> {
        String::from_utf8(self.read_len_prefixed_bytes(label)?)
            .map_err(|e| codec_error(label, format!("invalid utf-8: {e}")))
    }

    fn expect_end(&self, label: &str) -> Result<(), StorageError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(codec_error(label, "trailing bytes"))
        }
    }
}

fn encode_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn encode_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn encode_len(out: &mut Vec<u8>, label: &str, len: usize) -> Result<(), StorageError> {
    let len = u32::try_from(len).map_err(|_| codec_error(label, "length exceeds u32"))?;
    encode_u32(out, len);
    Ok(())
}

fn encode_bytes(out: &mut Vec<u8>, label: &str, bytes: &[u8]) -> Result<(), StorageError> {
    encode_len(out, label, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn encode_string(out: &mut Vec<u8>, label: &str, value: &str) -> Result<(), StorageError> {
    encode_bytes(out, label, value.as_bytes())
}

fn decode_binary_payload<'a>(
    bytes: &'a [u8],
    label: &str,
) -> Result<BinaryCursor<'a>, StorageError> {
    let mut cursor = BinaryCursor::new(bytes);
    let version = cursor.read_u8(label)?;
    if version != STORAGE_BINARY_V1 {
        return Err(codec_error(label, format!("unknown version {version}")));
    }
    Ok(cursor)
}

fn encode_branch_ref(branch_ref: &StoredBranchRef) -> Result<Vec<u8>, StorageError> {
    let mut out = Vec::new();
    out.push(STORAGE_BINARY_V1);
    encode_string(&mut out, "commit branch", &branch_ref.prefix)?;
    out.extend_from_slice(branch_ref.batch_id.as_bytes());
    Ok(out)
}

fn decode_branch_ref(bytes: &[u8]) -> Result<StoredBranchRef, StorageError> {
    let mut cursor = decode_binary_payload(bytes, "commit branch")?;
    let prefix = cursor.read_string("commit branch")?;
    let batch_id = BatchId(cursor.read_fixed::<16>("commit branch")?);
    cursor.expect_end("commit branch")?;
    Ok(StoredBranchRef { prefix, batch_id })
}

fn encode_commit(out: &mut Vec<u8>, commit: &Commit, label: &str) -> Result<(), StorageError> {
    encode_len(out, label, commit.parents.len())?;
    for parent in &commit.parents {
        out.extend_from_slice(&parent.0);
    }
    encode_bytes(out, label, &commit.content)?;
    encode_u64(out, commit.timestamp);
    encode_string(out, label, &commit.author)?;
    match &commit.metadata {
        Some(metadata) => {
            out.push(1);
            encode_len(out, label, metadata.len())?;
            for (key, value) in metadata {
                encode_string(out, label, key)?;
                encode_string(out, label, value)?;
            }
        }
        None => out.push(0),
    }
    Ok(())
}

fn decode_commit(cursor: &mut BinaryCursor<'_>, label: &str) -> Result<Commit, StorageError> {
    let parent_count = cursor.read_u32(label)? as usize;
    let mut parents = SmallVec::with_capacity(parent_count);
    for _ in 0..parent_count {
        parents.push(CommitId(cursor.read_fixed::<32>(label)?));
    }
    let content = cursor.read_len_prefixed_bytes(label)?;
    let timestamp = cursor.read_u64(label)?;
    let author = cursor.read_string(label)?;
    let metadata = match cursor.read_u8(label)? {
        0 => None,
        1 => {
            let entry_count = cursor.read_u32(label)? as usize;
            let mut metadata = std::collections::BTreeMap::new();
            for _ in 0..entry_count {
                let key = cursor.read_string(label)?;
                let value = cursor.read_string(label)?;
                metadata.insert(key, value);
            }
            Some(metadata)
        }
        other => return Err(codec_error(label, format!("unknown metadata flag {other}"))),
    };

    Ok(Commit {
        parents,
        content,
        timestamp,
        author,
        metadata,
        stored_state: Default::default(),
        ack_state: Default::default(),
    })
}

fn encode_branch_manifest(manifest: &PersistedBranchManifest) -> Result<Vec<u8>, StorageError> {
    let mut out = Vec::new();
    out.push(STORAGE_BINARY_V1);
    encode_len(&mut out, "branch manifest", manifest.segment_ids.len())?;
    for segment_id in &manifest.segment_ids {
        encode_u32(&mut out, *segment_id);
    }

    let mut tails: Vec<_> = manifest.tails.iter().copied().collect();
    tails.sort_unstable();
    encode_len(&mut out, "branch manifest", tails.len())?;
    for tail in &tails {
        out.extend_from_slice(&tail.0);
    }

    encode_len(&mut out, "branch manifest", manifest.tip_commits.len())?;
    for commit in &manifest.tip_commits {
        encode_commit(&mut out, commit, "branch manifest")?;
    }
    Ok(out)
}

fn decode_branch_manifest(bytes: &[u8]) -> Result<PersistedBranchManifest, StorageError> {
    let mut cursor = decode_binary_payload(bytes, "branch manifest")?;
    let segment_count = cursor.read_u32("branch manifest")? as usize;
    let mut segment_ids = Vec::with_capacity(segment_count);
    for _ in 0..segment_count {
        segment_ids.push(cursor.read_u32("branch manifest")?);
    }

    let tail_count = cursor.read_u32("branch manifest")? as usize;
    let mut tails = HashSet::with_capacity(tail_count);
    for _ in 0..tail_count {
        tails.insert(CommitId(cursor.read_fixed::<32>("branch manifest")?));
    }

    let tip_count = cursor.read_u32("branch manifest")? as usize;
    let mut tip_commits = Vec::with_capacity(tip_count);
    for _ in 0..tip_count {
        tip_commits.push(decode_commit(&mut cursor, "branch manifest")?);
    }
    cursor.expect_end("branch manifest")?;

    Ok(PersistedBranchManifest {
        segment_ids,
        tails,
        tip_commits,
    })
}

fn encode_branch_segment(segment: &PersistedBranchSegment) -> Result<Vec<u8>, StorageError> {
    let mut out = Vec::new();
    out.push(STORAGE_BINARY_V1);
    encode_len(&mut out, "branch segment", segment.commits.len())?;
    for commit in &segment.commits {
        encode_commit(&mut out, commit, "branch segment")?;
    }
    Ok(out)
}

fn decode_branch_segment(bytes: &[u8]) -> Result<PersistedBranchSegment, StorageError> {
    let mut cursor = decode_binary_payload(bytes, "branch segment")?;
    let commit_count = cursor.read_u32("branch segment")? as usize;
    let mut commits = Vec::with_capacity(commit_count);
    for _ in 0..commit_count {
        commits.push(decode_commit(&mut cursor, "branch segment")?);
    }
    cursor.expect_end("branch segment")?;
    Ok(PersistedBranchSegment { commits })
}

fn encode_prefix_batch_catalog(
    catalog: &PersistedPrefixBatchCatalog,
) -> Result<Vec<u8>, StorageError> {
    let mut out = Vec::new();
    out.push(STORAGE_BINARY_V1);
    encode_len(&mut out, "prefix batch catalog", catalog.batches.len())?;
    for batch in &catalog.batches {
        out.extend_from_slice(batch.batch_id.as_bytes());
        out.extend_from_slice(&batch.head_commit_id.0);
        encode_u64(&mut out, batch.last_timestamp);
        encode_u32(&mut out, batch.child_count);
    }
    Ok(out)
}

fn decode_prefix_batch_catalog(bytes: &[u8]) -> Result<PersistedPrefixBatchCatalog, StorageError> {
    let mut cursor = decode_binary_payload(bytes, "prefix batch catalog")?;
    let batch_count = cursor.read_u32("prefix batch catalog")? as usize;
    let mut batches = Vec::with_capacity(batch_count);
    for _ in 0..batch_count {
        let batch_id = BatchId(cursor.read_fixed::<16>("prefix batch catalog")?);
        let head_commit_id = CommitId(cursor.read_fixed::<32>("prefix batch catalog")?);
        let last_timestamp = cursor.read_u64("prefix batch catalog")?;
        let child_count = cursor.read_u32("prefix batch catalog")?;
        batches.push(PersistedPrefixBatchCatalogEntry {
            batch_id,
            head_commit_id,
            last_timestamp,
            child_count,
        });
    }
    cursor.expect_end("prefix batch catalog")?;

    Ok(PersistedPrefixBatchCatalog { batches })
}

fn encode_table_prefix_batch_manifest(
    manifest: &TablePrefixBatchManifest,
) -> Result<Vec<u8>, StorageError> {
    let mut out = Vec::new();
    out.push(STORAGE_BINARY_V1);
    encode_len(
        &mut out,
        "table prefix active batches",
        manifest.entries_by_ord.len(),
    )?;
    for entry in &manifest.entries_by_ord {
        out.extend_from_slice(entry.batch_id.as_bytes());
        encode_u64(&mut out, entry.ref_count);
    }
    Ok(out)
}

fn decode_table_prefix_batch_manifest(
    bytes: &[u8],
) -> Result<TablePrefixBatchManifest, StorageError> {
    let mut cursor = decode_binary_payload(bytes, "table prefix active batches")?;
    let entry_count = cursor.read_u32("table prefix active batches")? as usize;
    let mut entries_by_ord = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        entries_by_ord.push(super::TablePrefixBatchEntry {
            batch_id: BatchId(cursor.read_fixed::<16>("table prefix active batches")?),
            ref_count: cursor.read_u64("table prefix active batches")?,
        });
    }
    cursor.expect_end("table prefix active batches")?;

    let mut manifest = TablePrefixBatchManifest {
        entries_by_ord,
        ..Default::default()
    };
    manifest.rebuild_lookup();
    Ok(manifest)
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

fn load_branch_manifest(
    object_id: ObjectId,
    branch: &QueryBranchRef,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<PersistedBranchManifest>, StorageError> {
    let key = branch_manifest_key(object_id, branch);
    match get(&key)? {
        Some(data) => Ok(Some(decode_branch_manifest(&data)?)),
        None => Ok(None),
    }
}

fn load_branch_segment(
    object_id: ObjectId,
    branch: &QueryBranchRef,
    segment_id: u32,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<PersistedBranchSegment>, StorageError> {
    let key = branch_segment_key(object_id, branch, segment_id);
    match get(&key)? {
        Some(data) => Ok(Some(decode_branch_segment(&data)?)),
        None => Ok(None),
    }
}

impl PersistedPrefixBatchCatalog {
    fn from_catalog(catalog: &PrefixBatchCatalog) -> Self {
        Self {
            batches: catalog
                .batch_metas()
                .map(|batch| PersistedPrefixBatchCatalogEntry {
                    batch_id: batch.batch_id,
                    head_commit_id: batch.head_commit_id,
                    last_timestamp: batch.last_timestamp,
                    child_count: batch.child_count,
                })
                .collect(),
        }
    }

    fn into_catalog(self) -> PrefixBatchCatalog {
        let mut leaf_batch_ords = Vec::new();
        let batches_by_ord = self
            .batches
            .into_iter()
            .enumerate()
            .map(|(index, batch)| {
                let batch_ord = BatchOrd(index as u32);
                if batch.child_count == 0 {
                    leaf_batch_ords.push(batch_ord);
                }
                PrefixBatchMeta {
                    batch_id: batch.batch_id,
                    batch_ord,
                    root_commit_id: batch.head_commit_id,
                    head_commit_id: batch.head_commit_id,
                    first_timestamp: batch.last_timestamp,
                    last_timestamp: batch.last_timestamp,
                    parent_batch_ords: Vec::new(),
                    child_count: batch.child_count,
                }
            })
            .collect();
        PrefixBatchCatalog::from_persisted_parts(batches_by_ord, leaf_batch_ords)
    }
}

fn persist_branch_manifest(
    object_id: ObjectId,
    branch: &QueryBranchRef,
    manifest: &PersistedBranchManifest,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let key = branch_manifest_key(object_id, branch);
    let data = encode_branch_manifest(manifest)?;
    set(&key, &data)
}

fn persist_branch_segment(
    object_id: ObjectId,
    branch: &QueryBranchRef,
    segment_id: u32,
    segment: &PersistedBranchSegment,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let key = branch_segment_key(object_id, branch, segment_id);
    let data = encode_branch_segment(segment)?;
    set(&key, &data)
}

fn tip_commits_for_branch(commits: &[Commit]) -> Vec<Commit> {
    let mut parent_ids = HashSet::new();
    for commit in commits {
        for parent in &commit.parents {
            parent_ids.insert(*parent);
        }
    }

    commits
        .iter()
        .filter(|commit| !parent_ids.contains(&commit.id()))
        .cloned()
        .collect()
}

pub(super) fn load_branch_core(
    object_id: ObjectId,
    branch: &QueryBranchRef,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<LoadedBranch>, StorageError> {
    let meta_key = obj_meta_key(object_id);
    if get(&meta_key)?.is_none() {
        return Ok(None);
    }

    let Some(manifest) = load_branch_manifest(object_id, branch, |key| get(key))? else {
        return Ok(None);
    };

    let mut commits = Vec::new();
    for segment_id in manifest.segment_ids {
        let Some(segment) = load_branch_segment(object_id, branch, segment_id, |key| get(key))?
        else {
            continue;
        };

        for mut commit in segment.commits {
            let ack_lookup_key = ack_key(commit.id());
            if let Some(ack_data) = get(&ack_lookup_key)? {
                let tiers: HashSet<DurabilityTier> = decode_json(&ack_data, "ack")?;
                commit.ack_state.confirmed_tiers = tiers;
            }

            commits.push(commit);
        }
    }

    Ok(Some(LoadedBranch {
        commits,
        tails: manifest.tails,
    }))
}

pub(super) fn load_branch_tips_core(
    object_id: ObjectId,
    branch: &QueryBranchRef,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<LoadedBranchTips>, StorageError> {
    let meta_key = obj_meta_key(object_id);
    if get(&meta_key)?.is_none() {
        return Ok(None);
    }

    let Some(manifest) = load_branch_manifest(object_id, branch, |key| get(key))? else {
        return Ok(None);
    };

    let mut tips = Vec::with_capacity(manifest.tip_commits.len());
    for mut commit in manifest.tip_commits {
        let ack_lookup_key = ack_key(commit.id());
        if let Some(ack_data) = get(&ack_lookup_key)? {
            let tiers: HashSet<DurabilityTier> = decode_json(&ack_data, "ack")?;
            commit.ack_state.confirmed_tiers = tiers;
        }
        tips.push(commit);
    }

    Ok(Some(LoadedBranchTips { tips }))
}

pub(super) fn load_commit_branch_core(
    object_id: ObjectId,
    commit_id: CommitId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<QueryBranchRef>, StorageError> {
    let key = commit_branch_key(object_id, commit_id);
    match get(&key)? {
        Some(data) => {
            let branch_ref = decode_branch_ref(&data)?;
            Ok(Some(branch_ref.to_branch_ref()))
        }
        None => Ok(None),
    }
}

pub(super) fn load_prefix_batch_catalog_core(
    object_id: ObjectId,
    prefix: &str,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<PrefixBatchCatalog>, StorageError> {
    let key = prefix_batch_catalog_key(object_id, prefix);
    match get(&key)? {
        Some(data) => {
            let persisted = decode_prefix_batch_catalog(&data)?;
            Ok(Some(persisted.into_catalog()))
        }
        None => Ok(None),
    }
}

fn persist_prefix_batch_catalog(
    object_id: ObjectId,
    prefix: &str,
    catalog: &PrefixBatchCatalog,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let key = prefix_batch_catalog_key(object_id, prefix);
    let persisted = PersistedPrefixBatchCatalog::from_catalog(catalog);
    let data = encode_prefix_batch_catalog(&persisted)?;
    set(&key, &data)
}

pub(super) fn load_table_prefix_batch_keys_core(
    table: &str,
    prefix: BranchName,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Vec<BatchBranchKey>, StorageError> {
    let key = table_prefix_batches_key(table, prefix.as_str());
    let manifest: TablePrefixBatchManifest = match get(&key)? {
        Some(data) => decode_table_prefix_batch_manifest(&data)?,
        None => TablePrefixBatchManifest::default(),
    };
    Ok(manifest.branch_keys(prefix))
}

pub(super) fn adjust_table_prefix_batch_refcount_core(
    table: &str,
    branch: &QueryBranchRef,
    delta: i64,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
    mut delete: impl FnMut(&str) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let key = table_prefix_batches_key(table, branch.prefix_name().as_str());
    let mut manifest: TablePrefixBatchManifest = match get(&key)? {
        Some(data) => decode_table_prefix_batch_manifest(&data)?,
        None => TablePrefixBatchManifest::default(),
    };
    manifest.adjust_refcount(branch.batch_id(), delta);

    if manifest.is_empty() {
        delete(&key)
    } else {
        let data = encode_table_prefix_batch_manifest(&manifest)?;
        set(&key, &data)
    }
}

pub(super) fn append_commit_core(
    object_id: ObjectId,
    branch: &QueryBranchRef,
    commit: Commit,
    prefix_batch_update: Option<PrefixBatchUpdate>,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let commit_id = commit.id();
    let mut manifest = load_branch_manifest(object_id, branch, |key| get(key))?.unwrap_or_default();
    let mut current_segment_id = manifest.segment_ids.last().copied().unwrap_or(0);
    let mut current_segment = if manifest.segment_ids.is_empty() {
        manifest.segment_ids.push(current_segment_id);
        PersistedBranchSegment::default()
    } else {
        load_branch_segment(object_id, branch, current_segment_id, |key| get(key))?
            .unwrap_or_default()
    };

    if current_segment.commits.len() >= MAX_COMMITS_PER_BRANCH_SEGMENT {
        current_segment_id = current_segment_id.saturating_add(1);
        manifest.segment_ids.push(current_segment_id);
        current_segment = PersistedBranchSegment::default();
    }

    let commit_branch_lookup_key = commit_branch_key(object_id, commit_id);
    let commit_branch_bytes = encode_branch_ref(&StoredBranchRef::from_branch_ref(branch))?;
    set(&commit_branch_lookup_key, &commit_branch_bytes)?;

    for parent in &commit.parents {
        manifest.tails.remove(parent);
    }
    manifest.tails.insert(commit_id);
    manifest
        .tip_commits
        .retain(|tip| !commit.parents.contains(&tip.id()));
    manifest.tip_commits.push(commit.clone());
    current_segment.commits.push(commit);
    persist_branch_segment(
        object_id,
        branch,
        current_segment_id,
        &current_segment,
        |key, value| set(key, value),
    )?;
    persist_branch_manifest(object_id, branch, &manifest, |key, value| set(key, value))?;

    if let Some(update) = prefix_batch_update {
        let mut catalog =
            load_prefix_batch_catalog_core(object_id, &update.prefix, |key| get(key))?
                .unwrap_or_default();

        for parent_batch_ord in &update.increment_parent_child_counts {
            if let Some(parent_meta) = catalog.batch_meta_by_ord_mut(*parent_batch_ord) {
                parent_meta.child_count = parent_meta.child_count.saturating_add(1);
            }
        }
        for removed_batch_ord in update.remove_leaf_batch_ords {
            catalog.remove_leaf_batch_ord(removed_batch_ord);
        }
        catalog.insert_batch_meta(update.batch_meta.clone());
        catalog.insert_leaf_batch_ord(update.batch_meta.batch_ord);
        persist_prefix_batch_catalog(object_id, &update.prefix, &catalog, |key, value| {
            set(key, value)
        })?;
    }

    Ok(())
}

pub(super) fn replace_branch_core(
    object_id: ObjectId,
    branch: &QueryBranchRef,
    commits: Vec<Commit>,
    tails: HashSet<CommitId>,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
    mut delete: impl FnMut(&str) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let old_manifest = load_branch_manifest(object_id, branch, |key| get(key))?.unwrap_or_default();
    let mut old_commit_ids = HashSet::new();
    let old_segment_ids: HashSet<u32> = old_manifest.segment_ids.iter().copied().collect();
    for segment_id in &old_manifest.segment_ids {
        if let Some(segment) = load_branch_segment(object_id, branch, *segment_id, |key| get(key))?
        {
            old_commit_ids.extend(segment.commits.into_iter().map(|commit| commit.id()));
        }
    }

    let mut segment_ids = Vec::new();
    for (segment_id, commit_chunk) in commits.chunks(MAX_COMMITS_PER_BRANCH_SEGMENT).enumerate() {
        let segment_id = segment_id as u32;
        let segment = PersistedBranchSegment {
            commits: commit_chunk.to_vec(),
        };
        persist_branch_segment(object_id, branch, segment_id, &segment, |key, value| {
            set(key, value)
        })?;
        segment_ids.push(segment_id);
    }

    for old_segment_id in old_segment_ids {
        if !segment_ids.contains(&old_segment_id) {
            let key = branch_segment_key(object_id, branch, old_segment_id);
            delete(&key)?;
        }
    }

    let new_manifest = PersistedBranchManifest {
        segment_ids,
        tails,
        tip_commits: tip_commits_for_branch(&commits),
    };
    persist_branch_manifest(object_id, branch, &new_manifest, |key, value| {
        set(key, value)
    })?;

    let new_commit_ids: HashSet<CommitId> = commits.iter().map(Commit::id).collect();
    for removed_commit_id in old_commit_ids.difference(&new_commit_ids) {
        let key = commit_branch_key(object_id, *removed_commit_id);
        delete(&key)?;
    }
    for commit in &commits {
        let key = commit_branch_key(object_id, commit.id());
        let value = encode_branch_ref(&StoredBranchRef::from_branch_ref(branch))?;
        set(&key, &value)?;
    }

    Ok(())
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
    branch: &QueryBranchRef,
    value: &Value,
    row_id: ObjectId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<bool, StorageError> {
    let key = index_entry_key(table, column, branch, value, row_id)?;
    if get(&key)?.is_some() {
        return Ok(false);
    }
    set(&key, &[0x01])?;
    Ok(true)
}

pub(super) fn index_remove_core(
    table: &str,
    column: &str,
    branch: &QueryBranchRef,
    value: &Value,
    row_id: ObjectId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut delete: impl FnMut(&str) -> Result<(), StorageError>,
) -> Result<bool, StorageError> {
    let key = match index_entry_key(table, column, branch, value, row_id) {
        Ok(key) => key,
        Err(StorageError::IndexKeyTooLarge { .. }) => return Ok(false),
        Err(error) => return Err(error),
    };
    if get(&key)?.is_none() {
        return Ok(false);
    }
    delete(&key)?;
    Ok(true)
}

pub(super) fn index_lookup_core(
    table: &str,
    column: &str,
    branch: &QueryBranchRef,
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
    branch: &QueryBranchRef,
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
    branch: &QueryBranchRef,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    #[test]
    fn branch_ref_binary_codec_roundtrips() {
        let branch_ref = StoredBranchRef {
            prefix: "dev-schema-main".to_string(),
            batch_id: BatchId([7; 16]),
        };

        let encoded = encode_branch_ref(&branch_ref).unwrap();

        assert_eq!(encoded.first().copied(), Some(STORAGE_BINARY_V1));
        assert_eq!(decode_branch_ref(&encoded).unwrap(), branch_ref);
    }

    #[test]
    fn branch_manifest_binary_codec_roundtrips() {
        let commit = Commit {
            parents: vec![CommitId([1; 32]), CommitId([2; 32])].into(),
            content: vec![3; 64],
            timestamp: 42,
            author: ObjectId::from_uuid(Uuid::nil()).to_string(),
            metadata: Some(BTreeMap::from([("kind".to_string(), "merge".to_string())])),
            stored_state: Default::default(),
            ack_state: Default::default(),
        };
        let manifest = PersistedBranchManifest {
            segment_ids: vec![0, 1, 4],
            tails: [CommitId([9; 32]), CommitId([8; 32])].into_iter().collect(),
            tip_commits: vec![commit],
        };

        let encoded = encode_branch_manifest(&manifest).unwrap();

        assert_eq!(decode_branch_manifest(&encoded).unwrap(), manifest);
    }

    #[test]
    fn branch_segment_binary_codec_roundtrips() {
        let segment = PersistedBranchSegment {
            commits: vec![
                Commit {
                    parents: vec![CommitId([1; 32])].into(),
                    content: vec![2; 128],
                    timestamp: 11,
                    author: ObjectId::from_uuid(Uuid::nil()).to_string(),
                    metadata: None,
                    stored_state: Default::default(),
                    ack_state: Default::default(),
                },
                Commit {
                    parents: vec![CommitId([3; 32]), CommitId([4; 32])].into(),
                    content: vec![5; 256],
                    timestamp: 12,
                    author: ObjectId::from_uuid(Uuid::from_bytes([6; 16])).to_string(),
                    metadata: Some(BTreeMap::from([
                        ("a".to_string(), "b".to_string()),
                        ("c".to_string(), "d".to_string()),
                    ])),
                    stored_state: Default::default(),
                    ack_state: Default::default(),
                },
            ],
        };

        let encoded = encode_branch_segment(&segment).unwrap();

        assert_eq!(decode_branch_segment(&encoded).unwrap(), segment);
    }

    #[test]
    fn prefix_batch_catalog_binary_codec_roundtrips() {
        let persisted = PersistedPrefixBatchCatalog {
            batches: vec![
                PersistedPrefixBatchCatalogEntry {
                    batch_id: BatchId([1; 16]),
                    head_commit_id: CommitId([3; 32]),
                    last_timestamp: 11,
                    child_count: 1,
                },
                PersistedPrefixBatchCatalogEntry {
                    batch_id: BatchId([4; 16]),
                    head_commit_id: CommitId([6; 32]),
                    last_timestamp: 13,
                    child_count: 0,
                },
            ],
        };

        let encoded = encode_prefix_batch_catalog(&persisted).unwrap();

        assert_eq!(decode_prefix_batch_catalog(&encoded).unwrap(), persisted);
    }

    #[test]
    fn table_prefix_batch_manifest_binary_codec_roundtrips() {
        let mut manifest = TablePrefixBatchManifest::default();
        manifest.adjust_refcount(BatchId([1; 16]), 2);
        manifest.adjust_refcount(BatchId([2; 16]), 5);

        let encoded = encode_table_prefix_batch_manifest(&manifest).unwrap();
        let decoded = decode_table_prefix_batch_manifest(&encoded).unwrap();

        assert_eq!(decoded, manifest);
    }
}
