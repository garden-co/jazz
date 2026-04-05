use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId, PrefixBatchCatalog, PrefixBatchMeta};
use crate::sync_manager::DurabilityTier;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use smallvec::SmallVec;

use crate::query_manager::types::PrefixId;
use crate::query_manager::types::{BatchBranchKey, BatchId, BatchOrd, QueryBranchRef, Value};

use super::key_codec::{
    ack_key, branch_manifest_key, branch_segment_key, catalogue_manifest_op_key,
    catalogue_manifest_op_prefix, commit_branch_key, index_entry_key, index_prefix,
    index_range_scan_bounds, index_value_prefix, obj_meta_key, parse_uuid_from_index_key,
    prefix_batch_catalog_key, prefix_batch_catalog_key_for_id, table_prefix_batches_key,
};
use super::{
    CatalogueManifest, CatalogueManifestOp, LoadedBranch, LoadedBranchTips, PrefixBatchUpdate,
    StorageError, TablePrefixBatchManifest,
};

#[derive(Debug, Clone, Default)]
struct CommitEncodingTables {
    authors: Vec<String>,
    author_lookup: HashMap<String, u32>,
    metadata_keys: Vec<String>,
    metadata_key_lookup: HashMap<String, u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredBranchRef {
    prefix_id: PrefixId,
    batch_ord: BatchOrd,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedBranchManifest {
    segment_ids: SmallVec<[u32; 2]>,
    inline_commits: Vec<Commit>,
    tails: HashSet<CommitId>,
    tip_commits: Vec<Commit>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedBranchSegment {
    commits: Vec<Commit>,
}

#[derive(Debug, Clone, PartialEq)]
struct DecodedPrefixBatchCatalog {
    prefix_name: BranchName,
    catalog: PrefixBatchCatalog,
}

const MAX_COMMITS_PER_BRANCH_SEGMENT: usize = 32;
const MAX_INLINE_COMMITS_PER_BRANCH: usize = 8;
const STORAGE_BINARY_V1: u8 = 1;
const BRANCH_TAILS_DERIVE_FROM_TIPS: u8 = 0;
const BRANCH_TAILS_EXPLICIT: u8 = 1;

impl StoredBranchRef {
    fn from_prefix_batch(prefix_id: PrefixId, batch_ord: BatchOrd) -> Self {
        Self {
            prefix_id,
            batch_ord,
        }
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

    fn read_var_u32(&mut self, label: &str) -> Result<u32, StorageError> {
        let value = self.read_var_u64(label)?;
        u32::try_from(value).map_err(|_| codec_error(label, "u32 varint overflow"))
    }

    fn read_var_u64(&mut self, label: &str) -> Result<u64, StorageError> {
        let mut shift = 0_u32;
        let mut value = 0_u64;

        loop {
            let byte = self.read_u8(label)?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }

            shift += 7;
            if shift >= 64 {
                return Err(codec_error(label, "u64 varint overflow"));
            }
        }
    }

    fn read_fixed<const N: usize>(&mut self, label: &str) -> Result<[u8; N], StorageError> {
        let mut bytes = [0u8; N];
        bytes.copy_from_slice(self.read_exact(label, N)?);
        Ok(bytes)
    }

    fn read_len_prefixed_bytes(&mut self, label: &str) -> Result<Vec<u8>, StorageError> {
        Ok(self.read_len_prefixed_slice(label)?.to_vec())
    }

    fn read_len_prefixed_slice(&mut self, label: &str) -> Result<&'a [u8], StorageError> {
        let len = self.read_var_u32(label)? as usize;
        self.read_exact(label, len)
    }

    fn skip_len_prefixed_bytes(&mut self, label: &str) -> Result<(), StorageError> {
        let len = self.read_var_u32(label)? as usize;
        let _ = self.read_exact(label, len)?;
        Ok(())
    }

    fn read_string(&mut self, label: &str) -> Result<String, StorageError> {
        String::from_utf8(self.read_len_prefixed_bytes(label)?)
            .map_err(|e| codec_error(label, format!("invalid utf-8: {e}")))
    }

    fn skip_string(&mut self, label: &str) -> Result<(), StorageError> {
        self.skip_len_prefixed_bytes(label)
    }

    fn expect_end(&self, label: &str) -> Result<(), StorageError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(codec_error(label, "trailing bytes"))
        }
    }
}

fn encode_var_u32(out: &mut Vec<u8>, value: u32) {
    encode_var_u64(out, u64::from(value));
}

fn encode_var_u64(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn encode_var_i64(out: &mut Vec<u8>, value: i64) {
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    encode_var_u64(out, zigzag);
}

fn decode_var_i64(cursor: &mut BinaryCursor<'_>, label: &str) -> Result<i64, StorageError> {
    let zigzag = cursor.read_var_u64(label)?;
    let value = ((zigzag >> 1) as i64) ^ (-((zigzag & 1) as i64));
    Ok(value)
}

fn encode_len(out: &mut Vec<u8>, label: &str, len: usize) -> Result<(), StorageError> {
    let len = u32::try_from(len).map_err(|_| codec_error(label, "length exceeds u32"))?;
    encode_var_u32(out, len);
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
    out.extend_from_slice(branch_ref.prefix_id.as_bytes());
    encode_var_u32(&mut out, branch_ref.batch_ord.0);
    Ok(out)
}

fn decode_branch_ref(bytes: &[u8]) -> Result<StoredBranchRef, StorageError> {
    let mut cursor = decode_binary_payload(bytes, "commit branch")?;
    let prefix_id = PrefixId(cursor.read_fixed::<16>("commit branch")?);
    let batch_ord = BatchOrd(cursor.read_var_u32("commit branch")?);
    cursor.expect_end("commit branch")?;
    Ok(StoredBranchRef {
        prefix_id,
        batch_ord,
    })
}

fn collect_commit_encoding_tables<'a>(
    commits: impl IntoIterator<Item = &'a Commit>,
) -> CommitEncodingTables {
    let mut tables = CommitEncodingTables::default();
    for commit in commits {
        if !tables.author_lookup.contains_key(&commit.author) {
            let index = tables.authors.len() as u32;
            tables.author_lookup.insert(commit.author.clone(), index);
            tables.authors.push(commit.author.clone());
        }

        if let Some(metadata) = &commit.metadata {
            for key in metadata.keys() {
                if !tables.metadata_key_lookup.contains_key(key) {
                    let index = tables.metadata_keys.len() as u32;
                    tables.metadata_key_lookup.insert(key.clone(), index);
                    tables.metadata_keys.push(key.clone());
                }
            }
        }
    }
    tables
}

fn encode_commit_tables(
    out: &mut Vec<u8>,
    label: &str,
    tables: &CommitEncodingTables,
) -> Result<(), StorageError> {
    encode_len(out, label, tables.authors.len())?;
    for author in &tables.authors {
        if let Ok(uuid) = uuid::Uuid::parse_str(author)
            && uuid.to_string() == *author
        {
            out.push(1);
            out.extend_from_slice(uuid.as_bytes());
            continue;
        }
        out.push(0);
        encode_string(out, label, author)?;
    }

    encode_len(out, label, tables.metadata_keys.len())?;
    for key in &tables.metadata_keys {
        encode_string(out, label, key)?;
    }
    Ok(())
}

fn decode_commit_tables(
    cursor: &mut BinaryCursor<'_>,
    label: &str,
) -> Result<(Vec<String>, Vec<String>), StorageError> {
    let author_count = cursor.read_var_u32(label)? as usize;
    let mut authors = Vec::with_capacity(author_count);
    for _ in 0..author_count {
        match cursor.read_u8(label)? {
            0 => authors.push(cursor.read_string(label)?),
            1 => authors.push(uuid::Uuid::from_bytes(cursor.read_fixed::<16>(label)?).to_string()),
            other => {
                return Err(codec_error(
                    label,
                    format!("unknown author encoding mode {other}"),
                ));
            }
        }
    }

    let metadata_key_count = cursor.read_var_u32(label)? as usize;
    let mut metadata_keys = Vec::with_capacity(metadata_key_count);
    for _ in 0..metadata_key_count {
        metadata_keys.push(cursor.read_string(label)?);
    }

    Ok((authors, metadata_keys))
}

fn encode_commit(
    out: &mut Vec<u8>,
    commit: &Commit,
    label: &str,
    tables: &CommitEncodingTables,
    previous_commit_id: Option<CommitId>,
    previous_timestamp: Option<u64>,
) -> Result<(), StorageError> {
    let explicit_parent_count = commit.parents.len();
    if explicit_parent_count == 0 {
        out.push(0);
    } else if explicit_parent_count == 1 && previous_commit_id == Some(commit.parents[0]) {
        out.push(1);
    } else {
        out.push(2);
        encode_len(out, label, explicit_parent_count)?;
        for parent in &commit.parents {
            out.extend_from_slice(&parent.0);
        }
    }
    encode_bytes(out, label, &commit.content)?;
    if let Some(previous_timestamp) = previous_timestamp {
        let delta = i128::from(commit.timestamp) - i128::from(previous_timestamp);
        if let Ok(delta) = i64::try_from(delta) {
            out.push(1);
            encode_var_i64(out, delta);
        } else {
            out.push(0);
            encode_var_u64(out, commit.timestamp);
        }
    } else {
        out.push(0);
        encode_var_u64(out, commit.timestamp);
    }
    let author_index = tables
        .author_lookup
        .get(&commit.author)
        .copied()
        .ok_or_else(|| codec_error(label, "missing commit author in string table"))?;
    encode_var_u32(out, author_index);
    match &commit.metadata {
        Some(metadata) => {
            out.push(1);
            encode_len(out, label, metadata.len())?;
            for (key, value) in metadata {
                let key_index = tables
                    .metadata_key_lookup
                    .get(key)
                    .copied()
                    .ok_or_else(|| codec_error(label, "missing metadata key in string table"))?;
                encode_var_u32(out, key_index);
                encode_string(out, label, value)?;
            }
        }
        None => out.push(0),
    }
    Ok(())
}

fn decode_commit(
    cursor: &mut BinaryCursor<'_>,
    label: &str,
    authors: &[String],
    metadata_keys: &[String],
    previous_commit_id: Option<CommitId>,
    previous_timestamp: Option<u64>,
) -> Result<Commit, StorageError> {
    let parent_mode = cursor.read_u8(label)?;
    let mut parents = match parent_mode {
        0 => SmallVec::new(),
        1 => {
            let previous_commit_id =
                previous_commit_id.ok_or_else(|| codec_error(label, "missing previous commit"))?;
            smallvec::smallvec![previous_commit_id]
        }
        2 => {
            let parent_count = cursor.read_var_u32(label)? as usize;
            let mut parents = SmallVec::with_capacity(parent_count);
            for _ in 0..parent_count {
                parents.push(CommitId(cursor.read_fixed::<32>(label)?));
            }
            parents
        }
        other => return Err(codec_error(label, format!("unknown parent mode {other}"))),
    };
    let content = cursor.read_len_prefixed_bytes(label)?;
    let timestamp = match cursor.read_u8(label)? {
        0 => cursor.read_var_u64(label)?,
        1 => {
            let previous_timestamp = previous_timestamp
                .ok_or_else(|| codec_error(label, "missing previous timestamp"))?;
            let delta = i128::from(decode_var_i64(cursor, label)?);
            let timestamp = i128::from(previous_timestamp) + delta;
            u64::try_from(timestamp).map_err(|_| codec_error(label, "timestamp underflow"))?
        }
        other => {
            return Err(codec_error(
                label,
                format!("unknown timestamp mode {other}"),
            ));
        }
    };
    let author_index = cursor.read_var_u32(label)? as usize;
    let author = authors
        .get(author_index)
        .cloned()
        .ok_or_else(|| codec_error(label, "invalid author index"))?;
    let metadata = match cursor.read_u8(label)? {
        0 => None,
        1 => {
            let entry_count = cursor.read_var_u32(label)? as usize;
            let mut metadata = std::collections::BTreeMap::new();
            for _ in 0..entry_count {
                let key_index = cursor.read_var_u32(label)? as usize;
                let key = metadata_keys
                    .get(key_index)
                    .cloned()
                    .ok_or_else(|| codec_error(label, "invalid metadata key index"))?;
                let value = cursor.read_string(label)?;
                metadata.insert(key, value);
            }
            Some(metadata)
        }
        other => return Err(codec_error(label, format!("unknown metadata flag {other}"))),
    };

    Ok(Commit {
        parents: {
            parents.shrink_to_fit();
            parents
        },
        content,
        timestamp,
        author,
        metadata,
        stored_state: Default::default(),
        ack_state: Default::default(),
    })
}

fn skip_commit(
    cursor: &mut BinaryCursor<'_>,
    label: &str,
    author_count: usize,
    metadata_key_count: usize,
    previous_commit_id: Option<CommitId>,
    previous_timestamp: Option<u64>,
) -> Result<(), StorageError> {
    match cursor.read_u8(label)? {
        0 => {}
        1 => {
            previous_commit_id.ok_or_else(|| codec_error(label, "missing previous commit"))?;
        }
        2 => {
            let parent_count = cursor.read_var_u32(label)? as usize;
            let _ = cursor.read_exact(label, parent_count * 32)?;
        }
        other => return Err(codec_error(label, format!("unknown parent mode {other}"))),
    }

    cursor.skip_len_prefixed_bytes(label)?;

    match cursor.read_u8(label)? {
        0 => {
            let _ = cursor.read_var_u64(label)?;
        }
        1 => {
            previous_timestamp.ok_or_else(|| codec_error(label, "missing previous timestamp"))?;
            let _ = decode_var_i64(cursor, label)?;
        }
        other => {
            return Err(codec_error(
                label,
                format!("unknown timestamp mode {other}"),
            ));
        }
    }

    let author_index = cursor.read_var_u32(label)? as usize;
    if author_index >= author_count {
        return Err(codec_error(label, "invalid author index"));
    }

    match cursor.read_u8(label)? {
        0 => {}
        1 => {
            let entry_count = cursor.read_var_u32(label)? as usize;
            for _ in 0..entry_count {
                let key_index = cursor.read_var_u32(label)? as usize;
                if key_index >= metadata_key_count {
                    return Err(codec_error(label, "invalid metadata key index"));
                }
                cursor.skip_string(label)?;
            }
        }
        other => return Err(codec_error(label, format!("unknown metadata flag {other}"))),
    }

    Ok(())
}

fn encode_branch_manifest(manifest: &PersistedBranchManifest) -> Result<Vec<u8>, StorageError> {
    let mut out = Vec::new();
    out.push(STORAGE_BINARY_V1);
    let persisted_tip_commits: &[Commit] = if manifest.inline_commits.is_empty() {
        &manifest.tip_commits
    } else {
        &[]
    };
    let tables = collect_commit_encoding_tables(
        manifest
            .inline_commits
            .iter()
            .chain(persisted_tip_commits.iter()),
    );
    encode_commit_tables(&mut out, "branch manifest", &tables)?;
    encode_len(&mut out, "branch manifest", manifest.segment_ids.len())?;

    encode_len(&mut out, "branch manifest", manifest.inline_commits.len())?;
    let mut previous_commit_id = None;
    let mut previous_timestamp = None;
    for commit in &manifest.inline_commits {
        encode_commit(
            &mut out,
            commit,
            "branch manifest",
            &tables,
            previous_commit_id,
            previous_timestamp,
        )?;
        previous_commit_id = Some(commit.id());
        previous_timestamp = Some(commit.timestamp);
    }

    let derived_tail_ids: HashSet<CommitId> = if manifest.inline_commits.is_empty() {
        persisted_tip_commits.iter().map(Commit::id).collect()
    } else {
        tip_commits_for_branch(&manifest.inline_commits)
            .iter()
            .map(Commit::id)
            .collect()
    };
    if manifest.tails == derived_tail_ids {
        out.push(BRANCH_TAILS_DERIVE_FROM_TIPS);
    } else {
        out.push(BRANCH_TAILS_EXPLICIT);
        let mut tails: Vec<CommitId> = manifest.tails.iter().copied().collect();
        tails.sort_unstable();
        encode_len(&mut out, "branch manifest", tails.len())?;
        for tail in &tails {
            out.extend_from_slice(&tail.0);
        }
    }

    encode_len(&mut out, "branch manifest", persisted_tip_commits.len())?;
    for commit in persisted_tip_commits {
        encode_commit(&mut out, commit, "branch manifest", &tables, None, None)?;
    }
    Ok(out)
}

fn decode_branch_manifest_with_tips(
    bytes: &[u8],
    decode_tip_commits: bool,
) -> Result<PersistedBranchManifest, StorageError> {
    let mut cursor = decode_binary_payload(bytes, "branch manifest")?;
    let (authors, metadata_keys) = decode_commit_tables(&mut cursor, "branch manifest")?;
    let segment_count = cursor.read_var_u32("branch manifest")? as usize;
    let segment_ids = (0..segment_count as u32).collect::<SmallVec<[u32; 2]>>();

    let inline_commit_count = cursor.read_var_u32("branch manifest")? as usize;
    let mut inline_commits = Vec::with_capacity(inline_commit_count);
    let mut previous_commit_id = None;
    let mut previous_timestamp = None;
    for _ in 0..inline_commit_count {
        let commit = decode_commit(
            &mut cursor,
            "branch manifest",
            &authors,
            &metadata_keys,
            previous_commit_id,
            previous_timestamp,
        )?;
        previous_commit_id = Some(commit.id());
        previous_timestamp = Some(commit.timestamp);
        inline_commits.push(commit);
    }

    let tail_mode = cursor.read_u8("branch manifest")?;
    let explicit_tails = match tail_mode {
        BRANCH_TAILS_DERIVE_FROM_TIPS => None,
        BRANCH_TAILS_EXPLICIT => {
            let tail_count = cursor.read_var_u32("branch manifest")? as usize;
            let mut tails = HashSet::with_capacity(tail_count);
            for _ in 0..tail_count {
                tails.insert(CommitId(cursor.read_fixed::<32>("branch manifest")?));
            }
            Some(tails)
        }
        other => {
            return Err(codec_error(
                "branch manifest",
                format!("unknown tail mode {other}"),
            ));
        }
    };

    let tip_count = cursor.read_var_u32("branch manifest")? as usize;
    let mut tip_commits = Vec::new();
    if decode_tip_commits {
        tip_commits.reserve(tip_count);
        for _ in 0..tip_count {
            tip_commits.push(decode_commit(
                &mut cursor,
                "branch manifest",
                &authors,
                &metadata_keys,
                None,
                None,
            )?);
        }
    } else {
        for _ in 0..tip_count {
            skip_commit(
                &mut cursor,
                "branch manifest",
                authors.len(),
                metadata_keys.len(),
                None,
                None,
            )?;
        }
    }
    cursor.expect_end("branch manifest")?;

    if !inline_commits.is_empty() {
        tip_commits = tip_commits_for_branch(&inline_commits);
    }
    let tails = explicit_tails.unwrap_or_else(|| tip_commits.iter().map(Commit::id).collect());
    Ok(PersistedBranchManifest {
        segment_ids,
        inline_commits,
        tails,
        tip_commits,
    })
}

fn decode_branch_manifest(bytes: &[u8]) -> Result<PersistedBranchManifest, StorageError> {
    decode_branch_manifest_with_tips(bytes, true)
}

fn encode_branch_segment(segment: &PersistedBranchSegment) -> Result<Vec<u8>, StorageError> {
    let mut out = Vec::new();
    out.push(STORAGE_BINARY_V1);
    let tables = collect_commit_encoding_tables(segment.commits.iter());
    encode_commit_tables(&mut out, "branch segment", &tables)?;
    encode_len(&mut out, "branch segment", segment.commits.len())?;
    let mut previous_commit_id = None;
    let mut previous_timestamp = None;
    for commit in &segment.commits {
        encode_commit(
            &mut out,
            commit,
            "branch segment",
            &tables,
            previous_commit_id,
            previous_timestamp,
        )?;
        previous_commit_id = Some(commit.id());
        previous_timestamp = Some(commit.timestamp);
    }
    Ok(out)
}

fn decode_branch_segment(bytes: &[u8]) -> Result<PersistedBranchSegment, StorageError> {
    let mut cursor = decode_binary_payload(bytes, "branch segment")?;
    let (authors, metadata_keys) = decode_commit_tables(&mut cursor, "branch segment")?;
    let commit_count = cursor.read_var_u32("branch segment")? as usize;
    let mut commits = Vec::with_capacity(commit_count);
    let mut previous_commit_id = None;
    let mut previous_timestamp = None;
    for _ in 0..commit_count {
        let commit = decode_commit(
            &mut cursor,
            "branch segment",
            &authors,
            &metadata_keys,
            previous_commit_id,
            previous_timestamp,
        )?;
        previous_commit_id = Some(commit.id());
        previous_timestamp = Some(commit.timestamp);
        commits.push(commit);
    }
    cursor.expect_end("branch segment")?;
    Ok(PersistedBranchSegment { commits })
}

fn encode_prefix_batch_catalog(
    prefix_name: &BranchName,
    catalog: &PrefixBatchCatalog,
) -> Result<Vec<u8>, StorageError> {
    let mut out = Vec::new();
    out.push(STORAGE_BINARY_V1);
    encode_string(&mut out, "prefix batch catalog", prefix_name.as_str())?;
    encode_len(&mut out, "prefix batch catalog", catalog.batch_count())?;
    for batch in catalog.batch_metas() {
        let mut flags = 0_u8;
        if batch.root_commit_id != batch.head_commit_id {
            flags |= 0b001;
        }
        if batch.first_timestamp != batch.last_timestamp {
            flags |= 0b010;
        }
        if !batch.parent_batch_ords.is_empty() {
            flags |= 0b100;
        }
        out.push(flags);
        out.extend_from_slice(batch.batch_id.as_bytes());
        if flags & 0b001 != 0 {
            out.extend_from_slice(&batch.root_commit_id.0);
        }
        out.extend_from_slice(&batch.head_commit_id.0);
        if flags & 0b010 != 0 {
            encode_var_u64(&mut out, batch.first_timestamp);
        }
        encode_var_u64(&mut out, batch.last_timestamp);
        if flags & 0b100 != 0 {
            encode_len(
                &mut out,
                "prefix batch catalog",
                batch.parent_batch_ords.len(),
            )?;
            for parent_batch_ord in &batch.parent_batch_ords {
                encode_var_u32(&mut out, parent_batch_ord.0);
            }
        }
        encode_var_u32(&mut out, batch.child_count);
    }
    Ok(out)
}

fn decode_prefix_batch_catalog(bytes: &[u8]) -> Result<DecodedPrefixBatchCatalog, StorageError> {
    let mut cursor = decode_binary_payload(bytes, "prefix batch catalog")?;
    let prefix_name = BranchName::new(cursor.read_string("prefix batch catalog")?);
    let batch_count = cursor.read_var_u32("prefix batch catalog")? as usize;
    let mut batches = Vec::with_capacity(batch_count);
    let mut leaf_batch_ords = Vec::new();
    for batch_index in 0..batch_count {
        let flags = cursor.read_u8("prefix batch catalog")?;
        let batch_id = BatchId(cursor.read_fixed::<16>("prefix batch catalog")?);
        let root_commit_id = if flags & 0b001 != 0 {
            CommitId(cursor.read_fixed::<32>("prefix batch catalog")?)
        } else {
            CommitId([0; 32])
        };
        let head_commit_id = CommitId(cursor.read_fixed::<32>("prefix batch catalog")?);
        let root_commit_id = if flags & 0b001 != 0 {
            root_commit_id
        } else {
            head_commit_id
        };
        let first_timestamp = if flags & 0b010 != 0 {
            cursor.read_var_u64("prefix batch catalog")?
        } else {
            0
        };
        let last_timestamp = cursor.read_var_u64("prefix batch catalog")?;
        let first_timestamp = if flags & 0b010 != 0 {
            first_timestamp
        } else {
            last_timestamp
        };
        let parent_batch_ords = if flags & 0b100 != 0 {
            let parent_count = cursor.read_var_u32("prefix batch catalog")? as usize;
            let mut parent_batch_ords = SmallVec::<[BatchOrd; 4]>::with_capacity(parent_count);
            for _ in 0..parent_count {
                parent_batch_ords.push(BatchOrd(cursor.read_var_u32("prefix batch catalog")?));
            }
            parent_batch_ords
        } else {
            SmallVec::new()
        };
        let child_count = cursor.read_var_u32("prefix batch catalog")?;
        let batch_ord = BatchOrd(batch_index as u32);
        if child_count == 0 {
            leaf_batch_ords.push(batch_ord);
        }
        batches.push(PrefixBatchMeta {
            batch_id,
            batch_ord,
            root_commit_id,
            head_commit_id,
            first_timestamp,
            last_timestamp,
            parent_batch_ords,
            child_count,
        });
    }
    cursor.expect_end("prefix batch catalog")?;

    Ok(DecodedPrefixBatchCatalog {
        prefix_name,
        catalog: PrefixBatchCatalog::from_persisted_parts(batches, leaf_batch_ords),
    })
}

fn decode_prefix_head_entries(
    bytes: &[u8],
    expected_prefix: &str,
    leaf_only: bool,
) -> Result<Vec<(BatchId, CommitId)>, StorageError> {
    let mut cursor = decode_binary_payload(bytes, "prefix batch catalog")?;
    let persisted_prefix = cursor.read_len_prefixed_slice("prefix batch catalog")?;
    if persisted_prefix != expected_prefix.as_bytes() {
        return Err(codec_error(
            "prefix batch catalog",
            format!(
                "catalog key/value prefix mismatch: key='{expected_prefix}', value='{}'",
                String::from_utf8_lossy(persisted_prefix)
            ),
        ));
    }

    let batch_count = cursor.read_var_u32("prefix batch catalog")? as usize;
    let mut heads = Vec::with_capacity(batch_count);
    for _ in 0..batch_count {
        let flags = cursor.read_u8("prefix batch catalog")?;
        let batch_id = BatchId(cursor.read_fixed::<16>("prefix batch catalog")?);
        if flags & 0b001 != 0 {
            let _ = cursor.read_fixed::<32>("prefix batch catalog")?;
        }
        let head_commit_id = CommitId(cursor.read_fixed::<32>("prefix batch catalog")?);
        if flags & 0b010 != 0 {
            let _ = cursor.read_var_u64("prefix batch catalog")?;
        }
        let _ = cursor.read_var_u64("prefix batch catalog")?;
        if flags & 0b100 != 0 {
            let parent_count = cursor.read_var_u32("prefix batch catalog")? as usize;
            for _ in 0..parent_count {
                let _ = cursor.read_var_u32("prefix batch catalog")?;
            }
        }
        let child_count = cursor.read_var_u32("prefix batch catalog")?;
        if !leaf_only || child_count == 0 {
            heads.push((batch_id, head_commit_id));
        }
    }
    cursor.expect_end("prefix batch catalog")?;
    Ok(heads)
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
        encode_var_u64(&mut out, entry.ref_count);
    }
    Ok(out)
}

fn decode_table_prefix_batch_manifest(
    bytes: &[u8],
) -> Result<TablePrefixBatchManifest, StorageError> {
    let mut cursor = decode_binary_payload(bytes, "table prefix active batches")?;
    let entry_count = cursor.read_var_u32("table prefix active batches")? as usize;
    let mut entries_by_ord = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        entries_by_ord.push(super::TablePrefixBatchEntry {
            batch_id: BatchId(cursor.read_fixed::<16>("table prefix active batches")?),
            ref_count: cursor.read_var_u64("table prefix active batches")?,
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

fn load_branch_manifest_for_full_load(
    object_id: ObjectId,
    branch: &QueryBranchRef,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<PersistedBranchManifest>, StorageError> {
    let key = branch_manifest_key(object_id, branch);
    match get(&key)? {
        Some(data) => Ok(Some(decode_branch_manifest_with_tips(&data, false)?)),
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

fn stored_branch_ref_for_batch(
    object_id: ObjectId,
    branch: &QueryBranchRef,
    batch_ord_hint: Option<BatchOrd>,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<StoredBranchRef, StorageError> {
    let batch_ord = if let Some(batch_ord) = batch_ord_hint {
        batch_ord
    } else {
        let catalog =
            load_prefix_batch_catalog_core(object_id, branch.prefix_name().as_str(), |key| {
                get(key)
            })?
            .ok_or_else(|| codec_error("commit branch", "missing prefix batch catalog"))?;
        catalog
            .batch_ord(&branch.batch_id())
            .ok_or_else(|| codec_error("commit branch", "missing batch ord in prefix catalog"))?
    };

    Ok(StoredBranchRef::from_prefix_batch(
        branch.batch_branch_key().prefix_id(),
        batch_ord,
    ))
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

fn tip_ids_for_branch(commits: &[Commit]) -> SmallVec<[CommitId; 2]> {
    let mut parent_ids = HashSet::new();
    for commit in commits {
        for parent in &commit.parents {
            parent_ids.insert(*parent);
        }
    }

    commits
        .iter()
        .filter_map(|commit| {
            let commit_id = commit.id();
            (!parent_ids.contains(&commit_id)).then_some(commit_id)
        })
        .collect()
}

fn append_commit_to_manifest_state(manifest: &mut PersistedBranchManifest, commit: &Commit) {
    for parent in &commit.parents {
        manifest.tails.remove(parent);
    }
    manifest.tails.insert(commit.id());
    manifest
        .tip_commits
        .retain(|tip| !commit.parents.contains(&tip.id()));
    manifest.tip_commits.push(commit.clone());
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

    load_branch_core_existing_object(object_id, branch, get)
}

pub(super) fn load_branch_core_existing_object(
    object_id: ObjectId,
    branch: &QueryBranchRef,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<LoadedBranch>, StorageError> {
    let Some(manifest) = load_branch_manifest_for_full_load(object_id, branch, |key| get(key))?
    else {
        return Ok(None);
    };

    let mut tip_ids = manifest
        .tip_commits
        .iter()
        .map(Commit::id)
        .collect::<SmallVec<[CommitId; 2]>>();
    let mut commits = Vec::new();
    let mut tails = manifest
        .tails
        .into_iter()
        .collect::<SmallVec<[CommitId; 2]>>();
    if manifest.segment_ids.is_empty() {
        for mut commit in manifest.inline_commits {
            let ack_lookup_key = ack_key(commit.id());
            if let Some(ack_data) = get(&ack_lookup_key)? {
                let tiers: HashSet<DurabilityTier> = decode_json(&ack_data, "ack")?;
                commit.ack_state.confirmed_tiers = tiers;
            }
            commits.push(commit);
        }
    } else {
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
    }

    if tip_ids.is_empty() && !commits.is_empty() {
        tip_ids = tip_ids_for_branch(&commits);
    }
    tip_ids.sort_unstable();
    if tails.is_empty() && !commits.is_empty() {
        tails = tip_ids.clone();
    }
    tails.sort_unstable();

    Ok(Some(LoadedBranch {
        commits,
        tips: tip_ids,
        tails,
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

    load_branch_tips_core_existing_object(object_id, branch, get)
}

pub(super) fn load_branch_tips_core_existing_object(
    object_id: ObjectId,
    branch: &QueryBranchRef,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<LoadedBranchTips>, StorageError> {
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
            let Some(prefix_catalog) = load_persisted_prefix_batch_catalog_by_id(
                object_id,
                branch_ref.prefix_id,
                |key| get(key),
            )?
            else {
                return Ok(None);
            };
            let Some(batch_entry) = prefix_catalog
                .catalog
                .batch_meta_by_ord(branch_ref.batch_ord)
            else {
                return Ok(None);
            };
            Ok(Some(QueryBranchRef::from_prefix_name_and_batch(
                prefix_catalog.prefix_name,
                batch_entry.batch_id,
            )))
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
            let decoded = decode_prefix_batch_catalog(&data)?;
            if decoded.prefix_name.as_str() != prefix {
                return Err(codec_error(
                    "prefix batch catalog",
                    format!(
                        "catalog key/value prefix mismatch: key='{prefix}', value='{}'",
                        decoded.prefix_name.as_str()
                    ),
                ));
            }
            Ok(Some(decoded.catalog))
        }
        None => Ok(None),
    }
}

pub(super) fn load_prefix_head_entries_core(
    object_id: ObjectId,
    prefix: &str,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Vec<(BatchId, CommitId)>, StorageError> {
    let key = prefix_batch_catalog_key(object_id, prefix);
    match get(&key)? {
        Some(data) => decode_prefix_head_entries(&data, prefix, false),
        None => Ok(Vec::new()),
    }
}

pub(super) fn load_prefix_leaf_head_entries_core(
    object_id: ObjectId,
    prefix: &str,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Vec<(BatchId, CommitId)>, StorageError> {
    let key = prefix_batch_catalog_key(object_id, prefix);
    match get(&key)? {
        Some(data) => decode_prefix_head_entries(&data, prefix, true),
        None => Ok(Vec::new()),
    }
}

fn persist_prefix_batch_catalog(
    object_id: ObjectId,
    prefix: &str,
    catalog: &PrefixBatchCatalog,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let key = prefix_batch_catalog_key(object_id, prefix);
    let data = encode_prefix_batch_catalog(&BranchName::new(prefix), catalog)?;
    set(&key, &data)
}

fn load_persisted_prefix_batch_catalog_by_id(
    object_id: ObjectId,
    prefix_id: PrefixId,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
) -> Result<Option<DecodedPrefixBatchCatalog>, StorageError> {
    let key = prefix_batch_catalog_key_for_id(object_id, prefix_id);
    match get(&key)? {
        Some(data) => Ok(Some(decode_prefix_batch_catalog(&data)?)),
        None => Ok(None),
    }
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
    let commit_timestamp = commit.timestamp;
    let mut manifest = load_branch_manifest(object_id, branch, |key| get(key))?.unwrap_or_default();

    append_commit_to_manifest_state(&mut manifest, &commit);

    if manifest.segment_ids.is_empty()
        && manifest.inline_commits.len() < MAX_INLINE_COMMITS_PER_BRANCH
    {
        manifest.inline_commits.push(commit);
    } else if manifest.segment_ids.is_empty() {
        let mut commits = std::mem::take(&mut manifest.inline_commits);
        commits.push(commit);
        manifest.segment_ids.clear();
        for (segment_id, commit_chunk) in commits.chunks(MAX_COMMITS_PER_BRANCH_SEGMENT).enumerate()
        {
            let segment_id = segment_id as u32;
            manifest.segment_ids.push(segment_id);
            persist_branch_segment(
                object_id,
                branch,
                segment_id,
                &PersistedBranchSegment {
                    commits: commit_chunk.to_vec(),
                },
                |key, value| set(key, value),
            )?;
        }
    } else {
        let mut current_segment_id = manifest.segment_ids.last().copied().unwrap_or(0);
        let mut current_segment =
            load_branch_segment(object_id, branch, current_segment_id, |key| get(key))?
                .unwrap_or_default();

        if current_segment.commits.len() >= MAX_COMMITS_PER_BRANCH_SEGMENT {
            current_segment_id = current_segment_id.saturating_add(1);
            manifest.segment_ids.push(current_segment_id);
            current_segment = PersistedBranchSegment::default();
        }

        current_segment.commits.push(commit);
        persist_branch_segment(
            object_id,
            branch,
            current_segment_id,
            &current_segment,
            |key, value| set(key, value),
        )?;
    }

    persist_branch_manifest(object_id, branch, &manifest, |key, value| set(key, value))?;

    if let Some(ref update) = prefix_batch_update {
        let mut catalog =
            load_prefix_batch_catalog_core(object_id, update.prefix.as_str(), |key| get(key))?
                .unwrap_or_default();

        for parent_batch_ord in &update.increment_parent_child_counts {
            if let Some(parent_meta) = catalog.batch_meta_by_ord_mut(*parent_batch_ord) {
                parent_meta.child_count = parent_meta.child_count.saturating_add(1);
            }
        }
        for removed_batch_ord in &update.remove_leaf_batch_ords {
            catalog.remove_leaf_batch_ord(*removed_batch_ord);
        }
        catalog.insert_batch_meta(update.batch_meta.clone());
        catalog.insert_leaf_batch_ord(update.batch_meta.batch_ord);
        persist_prefix_batch_catalog(object_id, update.prefix.as_str(), &catalog, |key, value| {
            set(key, value)
        })?;
    } else {
        let prefix_name = branch.prefix_name();
        let mut catalog =
            load_prefix_batch_catalog_core(object_id, prefix_name.as_str(), |key| get(key))?
                .unwrap_or_default();
        let batch_meta = if let Some(mut existing) = catalog.batch_meta(&branch.batch_id()).cloned()
        {
            existing.head_commit_id = commit_id;
            existing.last_timestamp = commit_timestamp;
            existing
        } else {
            PrefixBatchMeta {
                batch_id: branch.batch_id(),
                batch_ord: catalog.next_batch_ord(),
                root_commit_id: commit_id,
                head_commit_id: commit_id,
                first_timestamp: commit_timestamp,
                last_timestamp: commit_timestamp,
                parent_batch_ords: SmallVec::new(),
                child_count: 0,
            }
        };
        catalog.insert_batch_meta(batch_meta.clone());
        catalog.insert_leaf_batch_ord(batch_meta.batch_ord);
        persist_prefix_batch_catalog(object_id, prefix_name.as_str(), &catalog, |key, value| {
            set(key, value)
        })?;
    }

    let stored_branch_ref = stored_branch_ref_for_batch(
        object_id,
        branch,
        prefix_batch_update
            .as_ref()
            .map(|update| update.batch_meta.batch_ord),
        |key| get(key),
    )?;
    let commit_branch_lookup_key = commit_branch_key(object_id, commit_id);
    let commit_branch_bytes = encode_branch_ref(&stored_branch_ref)?;
    set(&commit_branch_lookup_key, &commit_branch_bytes)?;

    Ok(())
}

pub(super) fn replace_branch_core(
    object_id: ObjectId,
    branch: &QueryBranchRef,
    commits: Vec<Commit>,
    tails: smolset::SmolSet<[CommitId; 2]>,
    mut get: impl FnMut(&str) -> Result<Option<Vec<u8>>, StorageError>,
    mut set: impl FnMut(&str, &[u8]) -> Result<(), StorageError>,
    mut delete: impl FnMut(&str) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let old_manifest = load_branch_manifest(object_id, branch, |key| get(key))?.unwrap_or_default();
    let stored_branch_ref = stored_branch_ref_for_batch(object_id, branch, None, |key| get(key))?;
    let mut old_commit_ids: HashSet<CommitId> =
        old_manifest.inline_commits.iter().map(Commit::id).collect();
    let old_segment_ids: HashSet<u32> = old_manifest.segment_ids.iter().copied().collect();
    for segment_id in &old_manifest.segment_ids {
        if let Some(segment) = load_branch_segment(object_id, branch, *segment_id, |key| get(key))?
        {
            old_commit_ids.extend(segment.commits.into_iter().map(|commit| commit.id()));
        }
    }

    let mut segment_ids = SmallVec::<[u32; 2]>::new();
    let inline_commits = if commits.len() <= MAX_INLINE_COMMITS_PER_BRANCH {
        commits.clone()
    } else {
        for (segment_id, commit_chunk) in commits.chunks(MAX_COMMITS_PER_BRANCH_SEGMENT).enumerate()
        {
            let segment_id = segment_id as u32;
            let segment = PersistedBranchSegment {
                commits: commit_chunk.to_vec(),
            };
            persist_branch_segment(object_id, branch, segment_id, &segment, |key, value| {
                set(key, value)
            })?;
            segment_ids.push(segment_id);
        }
        Vec::new()
    };

    for old_segment_id in old_segment_ids {
        if !segment_ids.contains(&old_segment_id) {
            let key = branch_segment_key(object_id, branch, old_segment_id);
            delete(&key)?;
        }
    }

    let new_manifest = PersistedBranchManifest {
        segment_ids,
        inline_commits,
        tails: tails.into_iter().collect(),
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
        let value = encode_branch_ref(&stored_branch_ref)?;
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
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    #[test]
    fn branch_ref_binary_codec_roundtrips() {
        let branch_ref = StoredBranchRef {
            prefix_id: PrefixId::from_prefix_str("dev-schema-main"),
            batch_ord: BatchOrd(7),
        };

        let encoded = encode_branch_ref(&branch_ref).unwrap();

        assert_eq!(encoded.first().copied(), Some(STORAGE_BINARY_V1));
        assert_eq!(decode_branch_ref(&encoded).unwrap(), branch_ref);
    }

    #[test]
    fn branch_ref_binary_codec_stays_compact_for_long_prefixes() {
        let long_prefix = format!("dev-{}-feature", "a".repeat(64));
        let branch_ref = StoredBranchRef {
            prefix_id: PrefixId::from_prefix_str(&long_prefix),
            batch_ord: BatchOrd(7),
        };

        let encoded = encode_branch_ref(&branch_ref).unwrap();

        assert!(
            encoded.len() < 24,
            "expected long-prefix commit refs to stay compact, got {} bytes",
            encoded.len()
        );
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
            segment_ids: smallvec::smallvec![0, 1, 2],
            inline_commits: Vec::new(),
            tails: [commit.id()].into_iter().collect(),
            tip_commits: vec![commit.clone()],
        };

        let encoded = encode_branch_manifest(&manifest).unwrap();

        assert_eq!(decode_branch_manifest(&encoded).unwrap(), manifest);
    }

    #[test]
    fn branch_manifest_binary_codec_stays_compact_for_inline_linear_chain() {
        let author = ObjectId::from_uuid(Uuid::nil()).to_string();
        let mut inline_commits = Vec::new();
        let mut previous_commit_id = None;
        for index in 0..6 {
            let commit = Commit {
                parents: previous_commit_id.into_iter().collect(),
                content: vec![index as u8],
                timestamp: 1_744_000_000 + index,
                author: author.clone(),
                metadata: None,
                stored_state: Default::default(),
                ack_state: Default::default(),
            };
            previous_commit_id = Some(commit.id());
            inline_commits.push(commit);
        }
        let tip_commits = tip_commits_for_branch(&inline_commits);
        let manifest = PersistedBranchManifest {
            segment_ids: SmallVec::new(),
            inline_commits,
            tails: tip_commits.iter().map(Commit::id).collect(),
            tip_commits,
        };

        let encoded = encode_branch_manifest(&manifest).unwrap();

        assert!(
            encoded.len() < 100,
            "expected inline manifests to derive frontier state compactly, got {} bytes",
            encoded.len()
        );
        assert_eq!(decode_branch_manifest(&encoded).unwrap(), manifest);
    }

    #[test]
    fn branch_manifest_binary_codec_stays_compact_for_segment_frontier() {
        let tip = Commit {
            parents: vec![CommitId([1; 32])].into(),
            content: vec![9],
            timestamp: 99,
            author: ObjectId::from_uuid(Uuid::nil()).to_string(),
            metadata: None,
            stored_state: Default::default(),
            ack_state: Default::default(),
        };
        let manifest = PersistedBranchManifest {
            segment_ids: smallvec::smallvec![0, 1],
            inline_commits: Vec::new(),
            tails: [tip.id()].into_iter().collect(),
            tip_commits: vec![tip],
        };

        let encoded = encode_branch_manifest(&manifest).unwrap();

        assert!(
            encoded.len() < 72,
            "expected segment manifests to derive tail ids from tip commits, got {} bytes",
            encoded.len()
        );
        assert_eq!(decode_branch_manifest(&encoded).unwrap(), manifest);
    }

    #[test]
    fn branch_manifest_binary_codec_stays_compact_for_dense_segment_ids() {
        let tip = Commit {
            parents: vec![CommitId([1; 32])].into(),
            content: vec![9],
            timestamp: 99,
            author: ObjectId::from_uuid(Uuid::nil()).to_string(),
            metadata: None,
            stored_state: Default::default(),
            ack_state: Default::default(),
        };
        let manifest = PersistedBranchManifest {
            segment_ids: (0..10).collect::<SmallVec<[u32; 2]>>(),
            inline_commits: Vec::new(),
            tails: [tip.id()].into_iter().collect(),
            tip_commits: vec![tip],
        };

        let encoded = encode_branch_manifest(&manifest).unwrap();

        assert!(
            encoded.len() < 70,
            "expected dense segment ids to encode as a count, got {} bytes",
            encoded.len()
        );
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
    fn branch_segment_binary_codec_deduplicates_authors_and_metadata_keys() {
        let author = ObjectId::from_uuid(Uuid::nil()).to_string();
        let segment = PersistedBranchSegment {
            commits: (0..8)
                .map(|index| Commit {
                    parents: SmallVec::new(),
                    content: vec![index as u8; 32],
                    timestamp: 10 + index,
                    author: author.clone(),
                    metadata: Some(BTreeMap::from([
                        ("table".to_string(), "messages".to_string()),
                        ("kind".to_string(), "append".to_string()),
                    ])),
                    stored_state: Default::default(),
                    ack_state: Default::default(),
                })
                .collect(),
        };

        let encoded = encode_branch_segment(&segment).unwrap();

        assert!(
            encoded.len() < 700,
            "expected author/key tables to keep payload compact, got {} bytes",
            encoded.len()
        );
        assert_eq!(decode_branch_segment(&encoded).unwrap(), segment);
    }

    #[test]
    fn branch_segment_binary_codec_stays_compact_for_linear_commit_chain() {
        let author = ObjectId::from_uuid(Uuid::nil()).to_string();
        let mut commits = Vec::new();
        let mut previous_commit_id = None;
        for index in 0..6 {
            let parents = previous_commit_id.into_iter().collect();
            let commit = Commit {
                parents,
                content: vec![index as u8],
                timestamp: 100 + index,
                author: author.clone(),
                metadata: None,
                stored_state: Default::default(),
                ack_state: Default::default(),
            };
            previous_commit_id = Some(commit.id());
            commits.push(commit);
        }

        let segment = PersistedBranchSegment { commits };
        let encoded = encode_branch_segment(&segment).unwrap();

        assert!(
            encoded.len() < 120,
            "expected implicit parent encoding for linear chains, got {} bytes",
            encoded.len()
        );
        assert_eq!(decode_branch_segment(&encoded).unwrap(), segment);
    }

    #[test]
    fn branch_segment_binary_codec_delta_encodes_large_linear_timestamps() {
        let author = ObjectId::from_uuid(Uuid::nil()).to_string();
        let mut commits = Vec::new();
        let mut previous_commit_id = None;
        let base_timestamp = 1_744_000_000_000_000_u64;
        for index in 0..6 {
            let parents = previous_commit_id.into_iter().collect();
            let commit = Commit {
                parents,
                content: vec![index as u8],
                timestamp: base_timestamp + index,
                author: author.clone(),
                metadata: None,
                stored_state: Default::default(),
                ack_state: Default::default(),
            };
            previous_commit_id = Some(commit.id());
            commits.push(commit);
        }

        let segment = PersistedBranchSegment { commits };
        let encoded = encode_branch_segment(&segment).unwrap();

        assert!(
            encoded.len() < 100,
            "expected delta timestamp encoding for linear chains, got {} bytes",
            encoded.len()
        );
        assert_eq!(decode_branch_segment(&encoded).unwrap(), segment);
    }

    #[test]
    fn prefix_batch_catalog_binary_codec_roundtrips() {
        let prefix_name = BranchName::new("dev-schema-main");
        let catalog = PrefixBatchCatalog::from_persisted_parts(
            vec![
                PrefixBatchMeta {
                    batch_id: BatchId([1; 16]),
                    batch_ord: BatchOrd(0),
                    root_commit_id: CommitId([3; 32]),
                    head_commit_id: CommitId([3; 32]),
                    first_timestamp: 11,
                    last_timestamp: 11,
                    parent_batch_ords: SmallVec::new(),
                    child_count: 1,
                },
                PrefixBatchMeta {
                    batch_id: BatchId([4; 16]),
                    batch_ord: BatchOrd(1),
                    root_commit_id: CommitId([6; 32]),
                    head_commit_id: CommitId([6; 32]),
                    first_timestamp: 13,
                    last_timestamp: 13,
                    parent_batch_ords: SmallVec::new(),
                    child_count: 0,
                },
            ],
            [BatchOrd(1)],
        );

        let encoded = encode_prefix_batch_catalog(&prefix_name, &catalog).unwrap();
        let decoded = decode_prefix_batch_catalog(&encoded).unwrap();

        assert_eq!(decoded.prefix_name, prefix_name);
        assert_eq!(
            decoded.catalog.batch_metas().cloned().collect::<Vec<_>>(),
            catalog.batch_metas().cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            decoded.catalog.leaf_batch_ords().collect::<Vec<_>>(),
            catalog.leaf_batch_ords().collect::<Vec<_>>()
        );
    }

    #[test]
    fn prefix_batch_catalog_binary_codec_stays_compact_for_small_values() {
        let prefix_name = BranchName::new("dev-schema-main");
        let catalog = PrefixBatchCatalog::from_persisted_parts(
            vec![
                PrefixBatchMeta {
                    batch_id: BatchId([1; 16]),
                    batch_ord: BatchOrd(0),
                    root_commit_id: CommitId([3; 32]),
                    head_commit_id: CommitId([3; 32]),
                    first_timestamp: 11,
                    last_timestamp: 11,
                    parent_batch_ords: SmallVec::new(),
                    child_count: 1,
                },
                PrefixBatchMeta {
                    batch_id: BatchId([4; 16]),
                    batch_ord: BatchOrd(1),
                    root_commit_id: CommitId([6; 32]),
                    head_commit_id: CommitId([6; 32]),
                    first_timestamp: 13,
                    last_timestamp: 13,
                    parent_batch_ords: SmallVec::new(),
                    child_count: 0,
                },
            ],
            [BatchOrd(1)],
        );

        let encoded = encode_prefix_batch_catalog(&prefix_name, &catalog).unwrap();

        assert!(
            encoded.len() < 121,
            "expected a denser encoding, got {} bytes",
            encoded.len()
        );
    }

    #[test]
    fn persisted_prefix_batch_catalog_preserves_full_batch_meta() {
        let catalog = PrefixBatchCatalog::from_persisted_parts(
            vec![
                PrefixBatchMeta {
                    batch_id: BatchId([1; 16]),
                    batch_ord: BatchOrd(0),
                    root_commit_id: CommitId([2; 32]),
                    head_commit_id: CommitId([3; 32]),
                    first_timestamp: 11,
                    last_timestamp: 19,
                    parent_batch_ords: SmallVec::new(),
                    child_count: 1,
                },
                PrefixBatchMeta {
                    batch_id: BatchId([4; 16]),
                    batch_ord: BatchOrd(1),
                    root_commit_id: CommitId([5; 32]),
                    head_commit_id: CommitId([6; 32]),
                    first_timestamp: 23,
                    last_timestamp: 29,
                    parent_batch_ords: smallvec::smallvec![BatchOrd(0)],
                    child_count: 0,
                },
            ],
            [BatchOrd(1)],
        );

        let prefix_name = BranchName::new("dev-schema-main");
        let encoded = encode_prefix_batch_catalog(&prefix_name, &catalog).unwrap();
        let decoded = decode_prefix_batch_catalog(&encoded).unwrap();

        assert_eq!(decoded.prefix_name, prefix_name);
        let decoded_entries: Vec<_> = decoded.catalog.batch_metas().cloned().collect();
        let original_entries: Vec<_> = catalog.batch_metas().cloned().collect();
        assert_eq!(decoded_entries, original_entries);
        assert_eq!(
            decoded.catalog.leaf_batch_ords().collect::<Vec<_>>(),
            catalog.leaf_batch_ords().collect::<Vec<_>>()
        );
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

    #[test]
    fn table_prefix_batch_manifest_binary_codec_stays_compact_for_small_values() {
        let mut manifest = TablePrefixBatchManifest::default();
        manifest.adjust_refcount(BatchId([1; 16]), 2);
        manifest.adjust_refcount(BatchId([2; 16]), 5);

        let encoded = encode_table_prefix_batch_manifest(&manifest).unwrap();

        assert!(
            encoded.len() < 53,
            "expected a denser encoding, got {} bytes",
            encoded.len()
        );
    }

    #[test]
    fn short_branch_stays_inline_before_spilling_to_segment_storage() {
        let object_id = ObjectId::from_uuid(Uuid::from_bytes([1; 16]));
        let prefix = BranchName::new("dev-schema-main");
        let branch = QueryBranchRef::from_prefix_name_and_batch(prefix, BatchId([7; 16]));
        let author = ObjectId::from_uuid(Uuid::nil()).to_string();
        let store = RefCell::new(HashMap::<String, Vec<u8>>::new());

        create_object_core(object_id, HashMap::new(), |key, value| {
            store.borrow_mut().insert(key.to_string(), value.to_vec());
            Ok(())
        })
        .unwrap();

        let mut previous_commit_id = None;
        let mut appended_commits = Vec::new();
        for index in 0..MAX_COMMITS_PER_BRANCH_SEGMENT.min(8) {
            let commit = Commit {
                parents: previous_commit_id.into_iter().collect(),
                content: vec![index as u8],
                timestamp: 1_744_000_000 + index as u64,
                author: author.clone(),
                metadata: None,
                stored_state: Default::default(),
                ack_state: Default::default(),
            };
            previous_commit_id = Some(commit.id());
            appended_commits.push(commit.clone());
            append_commit_core(
                object_id,
                &branch,
                commit,
                None,
                |key| Ok(store.borrow().get(key).cloned()),
                |key, value| {
                    store.borrow_mut().insert(key.to_string(), value.to_vec());
                    Ok(())
                },
            )
            .unwrap();

            assert!(
                !store
                    .borrow()
                    .contains_key(&branch_segment_key(object_id, &branch, 0)),
                "short branch should stay inline before spill"
            );
        }

        let loaded = load_branch_core(object_id, &branch, |key| {
            Ok(store.borrow().get(key).cloned())
        })
        .unwrap()
        .unwrap();
        assert_eq!(loaded.commits, appended_commits);
        assert_eq!(
            loaded.tails,
            smallvec::SmallVec::<[CommitId; 2]>::from_iter([previous_commit_id.unwrap()])
        );

        let spill_commit = Commit {
            parents: previous_commit_id.into_iter().collect(),
            content: vec![99],
            timestamp: 1_744_000_999,
            author,
            metadata: Some(BTreeMap::from([("kind".to_string(), "spill".to_string())])),
            stored_state: Default::default(),
            ack_state: Default::default(),
        };
        appended_commits.push(spill_commit.clone());
        append_commit_core(
            object_id,
            &branch,
            spill_commit,
            None,
            |key| Ok(store.borrow().get(key).cloned()),
            |key, value| {
                store.borrow_mut().insert(key.to_string(), value.to_vec());
                Ok(())
            },
        )
        .unwrap();

        assert!(
            store
                .borrow()
                .contains_key(&branch_segment_key(object_id, &branch, 0)),
            "branch should spill to segment storage once it grows past the inline threshold"
        );

        let loaded = load_branch_core(object_id, &branch, |key| {
            Ok(store.borrow().get(key).cloned())
        })
        .unwrap()
        .unwrap();
        assert_eq!(loaded.commits, appended_commits);
        assert_eq!(
            loaded.tails,
            smallvec::SmallVec::<[CommitId; 2]>::from_iter([appended_commits
                .last()
                .expect("spilled branch should have a tip")
                .id()])
        );
    }
}
