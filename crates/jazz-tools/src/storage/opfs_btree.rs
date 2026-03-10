//! opfs-btree-backed Storage implementation.
//!
//! Uses a single opfs-btree instance with key-encoded namespaces for all data:
//! objects, commits, ack tiers, catalogue manifest ops, and indices.
//!
//! Key encoding scheme (all keys are UTF-8 strings with hex-encoded binary parts):
//!
//! ```text
//! "obj:{uuid}:meta"                                       → JSON metadata
//! "obj:{uuid}:br:{branch}:tips"                           → JSON HashSet<CommitId>
//! "obj:{uuid}:br:{branch}:c:{commit_uuid}"                → JSON Commit
//! "ack:{commit_hex}"                                      → JSON HashSet<DurabilityTier>
//! "catman:{app_uuid}:op:{object_uuid}"                    → JSON CatalogueManifestOp
//! "idx:{table}:{col}:{branch}:{hex_encoded_value}:{uuid}" → empty (existence is the signal)
//! ```

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};

#[cfg(target_arch = "wasm32")]
use opfs_btree::OpfsFile;
#[cfg(not(target_arch = "wasm32"))]
use opfs_btree::StdFile;
use opfs_btree::{BTreeError, BTreeOptions, MemoryFile, OpfsBTree, SyncFile};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::Value;
use crate::sync_manager::DurabilityTier;

use super::{
    CatalogueManifest, CatalogueManifestOp, LoadedBranch, Storage, StorageError,
    key_codec::increment_bytes,
    storage_core::{
        append_catalogue_manifest_op_core, append_catalogue_manifest_ops_core, append_commit_core,
        create_object_core, delete_commit_core, index_insert_core, index_lookup_core,
        index_range_core, index_remove_core, index_scan_all_core, load_branch_core,
        load_catalogue_manifest_core, load_object_metadata_core, set_branch_tails_core,
        store_ack_tier_core,
    },
};

const MIN_CACHE_SIZE_BYTES: usize = 4 * 1024 * 1024;
const FULL_SCAN_END_KEY: [u8; 1] = [u8::MAX];
const MANIFEST_MAGIC: [u8; 8] = *b"JAZZWAL1";
const MANIFEST_VERSION: u32 = 1;
const MANIFEST_SLOT_BYTES: usize = 128;
const MANIFEST_CHECKSUM_BYTES: usize = 8;
const WAL_BATCH_MAGIC: [u8; 8] = *b"JAZZWLB1";
const WAL_BATCH_HEADER_BYTES: usize = 8 + 4 + MANIFEST_CHECKSUM_BYTES;
type EncodedKv = (Vec<u8>, Vec<u8>);

#[derive(Clone, Debug)]
enum AnyFile {
    Memory(MemoryFile),
    #[cfg(not(target_arch = "wasm32"))]
    Std(StdFile),
    #[cfg(target_arch = "wasm32")]
    Opfs(OpfsFile),
}

impl SyncFile for AnyFile {
    fn len(&self) -> Result<u64, BTreeError> {
        match self {
            Self::Memory(file) => file.len(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.len(),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.len(),
        }
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError> {
        match self {
            Self::Memory(file) => file.read_exact_at(offset, buf),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.read_exact_at(offset, buf),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.read_exact_at(offset, buf),
        }
    }

    fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError> {
        match self {
            Self::Memory(file) => file.write_all_at(offset, buf),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.write_all_at(offset, buf),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.write_all_at(offset, buf),
        }
    }

    fn truncate(&self, len: u64) -> Result<(), BTreeError> {
        match self {
            Self::Memory(file) => file.truncate(len),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.truncate(len),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.truncate(len),
        }
    }

    fn flush(&self) -> Result<(), BTreeError> {
        match self {
            Self::Memory(file) => file.flush(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.flush(),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.flush(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicaSlot {
    A,
    B,
}

impl ReplicaSlot {
    fn inactive(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    fn encode(self) -> u8 {
        match self {
            Self::A => 0,
            Self::B => 1,
        }
    }

    fn decode(raw: u8) -> Result<Self, StorageError> {
        match raw {
            0 => Ok(Self::A),
            1 => Ok(Self::B),
            other => Err(StorageError::IoError(format!(
                "opfs-btree durable manifest: invalid slot {}",
                other
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManifestSlot {
    A,
    B,
}

impl ManifestSlot {
    fn inactive(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    fn byte_offset(self) -> u64 {
        match self {
            Self::A => 0,
            Self::B => MANIFEST_SLOT_BYTES as u64,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DurableManifest {
    generation: u64,
    active_snapshot: ReplicaSlot,
    active_wal: ReplicaSlot,
    applied_wal_seq: u64,
}

impl DurableManifest {
    fn bootstrap() -> Self {
        Self {
            generation: 1,
            active_snapshot: ReplicaSlot::A,
            active_wal: ReplicaSlot::A,
            applied_wal_seq: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WalEntry {
    Put {
        seq: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        seq: u64,
        key: Vec<u8>,
    },
}

impl WalEntry {
    fn seq(&self) -> u64 {
        match self {
            Self::Put { seq, .. } | Self::Delete { seq, .. } => *seq,
        }
    }
}

#[derive(Clone, Debug)]
struct DurableFiles {
    snapshot_a: AnyFile,
    snapshot_b: AnyFile,
    wal_a: AnyFile,
    wal_b: AnyFile,
    manifest: AnyFile,
}

impl DurableFiles {
    fn memory() -> Self {
        Self {
            snapshot_a: AnyFile::Memory(MemoryFile::new()),
            snapshot_b: AnyFile::Memory(MemoryFile::new()),
            wal_a: AnyFile::Memory(MemoryFile::new()),
            wal_b: AnyFile::Memory(MemoryFile::new()),
            manifest: AnyFile::Memory(MemoryFile::new()),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        Ok(Self {
            snapshot_a: AnyFile::Std(StdFile::open(path).map_err(map_storage_err)?),
            snapshot_b: AnyFile::Std(
                StdFile::open(sidecar_path(path, ".snapshot-b")).map_err(map_storage_err)?,
            ),
            wal_a: AnyFile::Std(
                StdFile::open(sidecar_path(path, ".wal-a")).map_err(map_storage_err)?,
            ),
            wal_b: AnyFile::Std(
                StdFile::open(sidecar_path(path, ".wal-b")).map_err(map_storage_err)?,
            ),
            manifest: AnyFile::Std(
                StdFile::open(sidecar_path(path, ".manifest")).map_err(map_storage_err)?,
            ),
        })
    }

    #[cfg(target_arch = "wasm32")]
    async fn open_opfs(namespace: &str) -> Result<Self, StorageError> {
        Ok(Self {
            snapshot_a: AnyFile::Opfs(OpfsFile::open(namespace).await.map_err(map_storage_err)?),
            snapshot_b: AnyFile::Opfs(
                OpfsFile::open(&opfs_sidecar_name(namespace, "snapshot-b"))
                    .await
                    .map_err(map_storage_err)?,
            ),
            wal_a: AnyFile::Opfs(
                OpfsFile::open(&opfs_sidecar_name(namespace, "wal-a"))
                    .await
                    .map_err(map_storage_err)?,
            ),
            wal_b: AnyFile::Opfs(
                OpfsFile::open(&opfs_sidecar_name(namespace, "wal-b"))
                    .await
                    .map_err(map_storage_err)?,
            ),
            manifest: AnyFile::Opfs(
                OpfsFile::open(&opfs_sidecar_name(namespace, "manifest"))
                    .await
                    .map_err(map_storage_err)?,
            ),
        })
    }

    fn snapshot(&self, slot: ReplicaSlot) -> AnyFile {
        match slot {
            ReplicaSlot::A => self.snapshot_a.clone(),
            ReplicaSlot::B => self.snapshot_b.clone(),
        }
    }

    fn wal(&self, slot: ReplicaSlot) -> AnyFile {
        match slot {
            ReplicaSlot::A => self.wal_a.clone(),
            ReplicaSlot::B => self.wal_b.clone(),
        }
    }
}

#[derive(Debug)]
struct StorageState {
    files: DurableFiles,
    options: BTreeOptions,
    tree: OpfsBTree<AnyFile>,
    manifest_slot: ManifestSlot,
    manifest: DurableManifest,
    pending_wal: Vec<WalEntry>,
    wal_flushed_seq: u64,
    next_wal_seq: u64,
}

#[cfg(test)]
mod test_failpoints {
    use std::cell::RefCell;

    use super::StorageError;

    #[derive(Default)]
    struct State {
        armed_step: Option<usize>,
        next_step: usize,
        hit_sites: Vec<&'static str>,
    }

    std::thread_local! {
        static STATE: RefCell<State> = RefCell::new(State::default());
    }

    pub(crate) fn arm(step: usize) {
        STATE.with(|cell| {
            let mut state = cell.borrow_mut();
            state.armed_step = Some(step);
            state.next_step = 0;
            state.hit_sites.clear();
        });
    }

    pub(crate) fn clear() {
        STATE.with(|cell| {
            let mut state = cell.borrow_mut();
            state.armed_step = None;
            state.next_step = 0;
            state.hit_sites.clear();
        });
    }

    pub(crate) fn step_count() -> usize {
        STATE.with(|cell| cell.borrow().next_step)
    }

    pub(crate) fn hit_sites() -> Vec<&'static str> {
        STATE.with(|cell| cell.borrow().hit_sites.clone())
    }

    pub(crate) fn hit(site: &'static str) -> Result<(), StorageError> {
        STATE.with(|cell| {
            let mut state = cell.borrow_mut();
            state.next_step = state.next_step.saturating_add(1);
            let step = state.next_step;
            state.hit_sites.push(site);
            if state.armed_step == Some(step) {
                return Err(StorageError::IoError(format!(
                    "opfs-btree durable failpoint at step {} ({})",
                    step, site
                )));
            }
            Ok(())
        })
    }
}

#[cfg(test)]
macro_rules! durable_failpoint {
    ($site:literal) => {
        test_failpoints::hit($site)?;
    };
}

#[cfg(not(test))]
macro_rules! durable_failpoint {
    ($site:literal) => {};
}

pub struct OpfsBTreeStorage {
    state: RefCell<StorageState>,
}

impl OpfsBTreeStorage {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(path: impl AsRef<Path>, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let files = DurableFiles::open(path)?;
        Self::open_with_files(files, cache_size_bytes)
    }

    pub fn memory(cache_size_bytes: usize) -> Result<Self, StorageError> {
        Self::open_with_files(DurableFiles::memory(), cache_size_bytes)
    }

    #[cfg(target_arch = "wasm32")]
    pub fn with_opfs(_file: OpfsFile, _cache_size_bytes: usize) -> Result<Self, StorageError> {
        Err(StorageError::IoError(
            "OpfsBTreeStorage::with_opfs is unsupported for durable mode; use open_opfs"
                .to_string(),
        ))
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn open_opfs(namespace: &str, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let files = DurableFiles::open_opfs(namespace).await?;
        Self::open_with_files(files, cache_size_bytes)
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn destroy_opfs(namespace: &str) -> Result<(), StorageError> {
        OpfsFile::destroy(namespace)
            .await
            .map_err(map_storage_err)?;
        let _ = OpfsFile::destroy(&opfs_sidecar_name(namespace, "snapshot-b")).await;
        let _ = OpfsFile::destroy(&opfs_sidecar_name(namespace, "wal-a")).await;
        let _ = OpfsFile::destroy(&opfs_sidecar_name(namespace, "wal-b")).await;
        let _ = OpfsFile::destroy(&opfs_sidecar_name(namespace, "manifest")).await;
        Ok(())
    }

    fn open_with_files(files: DurableFiles, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let options = Self::options(cache_size_bytes);
        let (manifest_slot, manifest) = read_manifest(&files.manifest)?
            .unwrap_or((ManifestSlot::A, DurableManifest::bootstrap()));
        durable_failpoint!("open:after-read-manifest");

        let mut tree = OpfsBTree::open(files.snapshot(manifest.active_snapshot), options)
            .map_err(map_storage_err)?;
        durable_failpoint!("open:after-open-snapshot");

        let wal_entries = read_wal_entries(&files.wal(manifest.active_wal))?;
        let mut wal_flushed_seq = manifest.applied_wal_seq;
        for entry in wal_entries {
            wal_flushed_seq = wal_flushed_seq.max(entry.seq());
            if entry.seq() > manifest.applied_wal_seq {
                apply_wal_entry(&mut tree, &entry)?;
            }
        }
        durable_failpoint!("open:after-replay-wal");

        Ok(Self {
            state: RefCell::new(StorageState {
                files,
                options,
                tree,
                manifest_slot,
                manifest,
                pending_wal: Vec::new(),
                wal_flushed_seq,
                next_wal_seq: wal_flushed_seq.saturating_add(1),
            }),
        })
    }

    fn options(cache_size_bytes: usize) -> BTreeOptions {
        BTreeOptions {
            cache_bytes: cache_size_bytes.max(MIN_CACHE_SIZE_BYTES),
            pin_internal_pages: true,
            read_coalesce_pages: 4,
            ..Default::default()
        }
    }

    fn with_state_mut<R>(
        &self,
        f: impl FnOnce(&mut StorageState) -> Result<R, StorageError>,
    ) -> Result<R, StorageError> {
        let mut state = self
            .state
            .try_borrow_mut()
            .map_err(|_| StorageError::IoError("opfs-btree already borrowed".to_string()))?;
        f(&mut state)
    }

    fn tree_insert(&self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.with_state_mut(|state| state.put(key.as_bytes(), value))
    }

    fn tree_read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.with_state_mut(|state| state.tree.get(key.as_bytes()).map_err(map_storage_err))
    }

    fn tree_delete(&self, key: &str) -> Result<(), StorageError> {
        self.with_state_mut(|state| state.delete(key.as_bytes()))
    }

    fn tree_scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let start = prefix.as_bytes();
        let mut end = start.to_vec();
        increment_bytes(&mut end);
        self.tree_scan_range_bytes(start, &end)
    }

    fn tree_scan_keys(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        Ok(self
            .tree_scan_prefix(prefix)?
            .into_iter()
            .map(|(key, _)| key)
            .collect())
    }

    fn tree_scan_key_range(&self, start: &str, end: &str) -> Result<Vec<String>, StorageError> {
        Ok(self
            .tree_scan_range_bytes(start.as_bytes(), end.as_bytes())?
            .into_iter()
            .map(|(key, _)| key)
            .collect())
    }

    fn tree_scan_range_bytes(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        if start >= end {
            return Ok(Vec::new());
        }

        self.with_state_mut(|state| {
            let entries = state
                .tree
                .range(start, end, usize::MAX)
                .map_err(map_storage_err)?;

            entries
                .into_iter()
                .map(|(key, value)| {
                    let key = String::from_utf8(key)
                        .map_err(|e| StorageError::IoError(format!("invalid key utf8: {}", e)))?;
                    Ok((key, value))
                })
                .collect()
        })
    }
}

impl StorageState {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.tree.put(key, value).map_err(map_storage_err)?;
        let seq = self.alloc_seq();
        self.pending_wal.push(WalEntry::Put {
            seq,
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), StorageError> {
        self.tree.delete(key).map_err(map_storage_err)?;
        let seq = self.alloc_seq();
        self.pending_wal.push(WalEntry::Delete {
            seq,
            key: key.to_vec(),
        });
        Ok(())
    }

    fn alloc_seq(&mut self) -> u64 {
        let seq = self.next_wal_seq;
        self.next_wal_seq = self.next_wal_seq.saturating_add(1);
        seq
    }

    fn flush_wal_inner(&mut self) -> Result<(), StorageError> {
        if self.pending_wal.is_empty() {
            return Ok(());
        }

        let active_wal = self.files.wal(self.manifest.active_wal);
        let batch = encode_wal_batch(&self.pending_wal)?;
        let offset = active_wal.len().map_err(map_storage_err)?;
        durable_failpoint!("flush_wal:before-write");
        active_wal
            .write_all_at(offset, &batch)
            .map_err(map_storage_err)?;
        durable_failpoint!("flush_wal:after-write");
        active_wal.flush().map_err(map_storage_err)?;
        durable_failpoint!("flush_wal:after-flush");

        self.wal_flushed_seq = self
            .pending_wal
            .last()
            .map(WalEntry::seq)
            .unwrap_or(self.wal_flushed_seq);
        self.pending_wal.clear();
        Ok(())
    }

    fn checkpoint_inner(&mut self) -> Result<(), StorageError> {
        self.flush_wal_inner()?;
        durable_failpoint!("checkpoint:after-flush-wal");

        if self.wal_flushed_seq == self.manifest.applied_wal_seq {
            return Ok(());
        }

        let target_snapshot = self.manifest.active_snapshot.inactive();
        let target_wal = self.manifest.active_wal.inactive();

        let snapshot_file = self.files.snapshot(target_snapshot);
        snapshot_file.truncate(0).map_err(map_storage_err)?;
        durable_failpoint!("checkpoint:after-truncate-snapshot");

        let mut snapshot_tree =
            OpfsBTree::open(snapshot_file, self.options).map_err(map_storage_err)?;
        durable_failpoint!("checkpoint:after-open-target-snapshot");

        for (key, value) in self.full_scan()? {
            snapshot_tree.put(&key, &value).map_err(map_storage_err)?;
        }
        durable_failpoint!("checkpoint:after-build-snapshot");
        snapshot_tree.checkpoint().map_err(map_storage_err)?;
        durable_failpoint!("checkpoint:after-snapshot-checkpoint");

        let next_wal_file = self.files.wal(target_wal);
        next_wal_file.truncate(0).map_err(map_storage_err)?;
        next_wal_file.flush().map_err(map_storage_err)?;
        durable_failpoint!("checkpoint:after-prepare-inactive-wal");

        let next_manifest = DurableManifest {
            generation: self.manifest.generation.saturating_add(1),
            active_snapshot: target_snapshot,
            active_wal: target_wal,
            applied_wal_seq: self.wal_flushed_seq,
        };
        let next_manifest_slot = self.manifest_slot.inactive();
        write_manifest(&self.files.manifest, next_manifest_slot, next_manifest)?;
        durable_failpoint!("checkpoint:after-manifest-write");
        self.files.manifest.flush().map_err(map_storage_err)?;
        durable_failpoint!("checkpoint:after-manifest-flush");

        self.tree = snapshot_tree;
        self.manifest = next_manifest;
        self.manifest_slot = next_manifest_slot;
        self.pending_wal.clear();
        durable_failpoint!("checkpoint:after-tree-swap");
        Ok(())
    }

    fn full_scan(&mut self) -> Result<Vec<EncodedKv>, StorageError> {
        self.tree
            .range(b"", &FULL_SCAN_END_KEY, usize::MAX)
            .map_err(map_storage_err)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| "jazz.opfsbtree".to_string());
    let mut out = path.to_path_buf();
    out.set_file_name(format!("{}{}", file_name, suffix));
    out
}

#[cfg(target_arch = "wasm32")]
fn opfs_sidecar_name(namespace: &str, suffix: &str) -> String {
    format!("{namespace}__{suffix}")
}

fn apply_wal_entry(tree: &mut OpfsBTree<AnyFile>, entry: &WalEntry) -> Result<(), StorageError> {
    match entry {
        WalEntry::Put { key, value, .. } => tree.put(key, value).map_err(map_storage_err),
        WalEntry::Delete { key, .. } => tree.delete(key).map_err(map_storage_err),
    }
}

fn manifest_checksum(bytes: &[u8]) -> [u8; MANIFEST_CHECKSUM_BYTES] {
    let hash = blake3::hash(bytes);
    let mut out = [0u8; MANIFEST_CHECKSUM_BYTES];
    out.copy_from_slice(&hash.as_bytes()[..MANIFEST_CHECKSUM_BYTES]);
    out
}

fn encode_manifest(manifest: DurableManifest) -> [u8; MANIFEST_SLOT_BYTES] {
    let mut slot = [0u8; MANIFEST_SLOT_BYTES];
    let mut payload = Vec::with_capacity(32);
    payload.extend_from_slice(&MANIFEST_MAGIC);
    payload.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    payload.extend_from_slice(&manifest.generation.to_le_bytes());
    payload.push(manifest.active_snapshot.encode());
    payload.push(manifest.active_wal.encode());
    payload.extend_from_slice(&manifest.applied_wal_seq.to_le_bytes());
    let checksum = manifest_checksum(&payload);
    let payload_len = payload.len();
    slot[..payload_len].copy_from_slice(&payload);
    slot[payload_len..payload_len + MANIFEST_CHECKSUM_BYTES].copy_from_slice(&checksum);
    slot
}

fn decode_manifest(slot: &[u8]) -> Result<DurableManifest, StorageError> {
    if slot.len() < MANIFEST_SLOT_BYTES {
        return Err(StorageError::IoError(
            "opfs-btree durable manifest slot too small".to_string(),
        ));
    }
    if slot[..MANIFEST_MAGIC.len()] != MANIFEST_MAGIC {
        return Err(StorageError::IoError(
            "opfs-btree durable manifest magic mismatch".to_string(),
        ));
    }

    let checksum_offset = 8 + 4 + 8 + 1 + 1 + 8;
    let expected_checksum = manifest_checksum(&slot[..checksum_offset]);
    let actual_checksum = &slot[checksum_offset..checksum_offset + MANIFEST_CHECKSUM_BYTES];
    if actual_checksum != expected_checksum {
        return Err(StorageError::IoError(
            "opfs-btree durable manifest checksum mismatch".to_string(),
        ));
    }

    let version = u32::from_le_bytes(
        slot[8..12]
            .try_into()
            .expect("manifest version slice must fit"),
    );
    if version != MANIFEST_VERSION {
        return Err(StorageError::IoError(format!(
            "opfs-btree durable manifest version {} is unsupported",
            version
        )));
    }

    let generation = u64::from_le_bytes(
        slot[12..20]
            .try_into()
            .expect("manifest generation slice must fit"),
    );
    let active_snapshot = ReplicaSlot::decode(slot[20])?;
    let active_wal = ReplicaSlot::decode(slot[21])?;
    let applied_wal_seq = u64::from_le_bytes(
        slot[22..30]
            .try_into()
            .expect("manifest wal seq slice must fit"),
    );

    Ok(DurableManifest {
        generation,
        active_snapshot,
        active_wal,
        applied_wal_seq,
    })
}

fn read_manifest(file: &AnyFile) -> Result<Option<(ManifestSlot, DurableManifest)>, StorageError> {
    if file.len().map_err(map_storage_err)? < MANIFEST_SLOT_BYTES as u64 {
        return Ok(None);
    }

    let mut best: Option<(ManifestSlot, DurableManifest)> = None;
    for slot_id in [ManifestSlot::A, ManifestSlot::B] {
        let needed = slot_id.byte_offset() + MANIFEST_SLOT_BYTES as u64;
        if file.len().map_err(map_storage_err)? < needed {
            continue;
        }

        let mut raw = [0u8; MANIFEST_SLOT_BYTES];
        file.read_exact_at(slot_id.byte_offset(), &mut raw)
            .map_err(map_storage_err)?;
        let Ok(manifest) = decode_manifest(&raw) else {
            continue;
        };
        match best {
            Some((_, current)) if current.generation >= manifest.generation => {}
            _ => best = Some((slot_id, manifest)),
        }
    }

    Ok(best)
}

fn write_manifest(
    file: &AnyFile,
    slot: ManifestSlot,
    manifest: DurableManifest,
) -> Result<(), StorageError> {
    let raw = encode_manifest(manifest);
    file.write_all_at(slot.byte_offset(), &raw)
        .map_err(map_storage_err)
}

fn wal_checksum(bytes: &[u8]) -> [u8; MANIFEST_CHECKSUM_BYTES] {
    manifest_checksum(bytes)
}

fn encode_wal_batch(entries: &[WalEntry]) -> Result<Vec<u8>, StorageError> {
    let mut body = Vec::new();
    let start_seq = entries
        .first()
        .map(WalEntry::seq)
        .ok_or_else(|| StorageError::IoError("empty WAL batch".to_string()))?;
    body.extend_from_slice(&start_seq.to_le_bytes());
    body.extend_from_slice(
        &(u32::try_from(entries.len())
            .map_err(|_| StorageError::IoError("WAL batch too large".to_string()))?)
        .to_le_bytes(),
    );

    for entry in entries {
        match entry {
            WalEntry::Put { key, value, .. } => {
                body.push(1);
                body.extend_from_slice(
                    &(u32::try_from(key.len())
                        .map_err(|_| StorageError::IoError("WAL key too large".to_string()))?)
                    .to_le_bytes(),
                );
                body.extend_from_slice(
                    &(u32::try_from(value.len())
                        .map_err(|_| StorageError::IoError("WAL value too large".to_string()))?)
                    .to_le_bytes(),
                );
                body.extend_from_slice(key);
                body.extend_from_slice(value);
            }
            WalEntry::Delete { key, .. } => {
                body.push(2);
                body.extend_from_slice(
                    &(u32::try_from(key.len())
                        .map_err(|_| StorageError::IoError("WAL key too large".to_string()))?)
                    .to_le_bytes(),
                );
                body.extend_from_slice(&0u32.to_le_bytes());
                body.extend_from_slice(key);
            }
        }
    }

    let checksum = wal_checksum(&body);
    let mut out = Vec::with_capacity(WAL_BATCH_HEADER_BYTES + body.len());
    out.extend_from_slice(&WAL_BATCH_MAGIC);
    out.extend_from_slice(
        &(u32::try_from(body.len())
            .map_err(|_| StorageError::IoError("WAL batch body too large".to_string()))?)
        .to_le_bytes(),
    );
    out.extend_from_slice(&checksum);
    out.extend_from_slice(&body);
    Ok(out)
}

fn decode_wal_batch(bytes: &[u8]) -> Result<Vec<WalEntry>, StorageError> {
    if bytes.len() < 12 {
        return Err(StorageError::IoError(
            "WAL batch body too small".to_string(),
        ));
    }

    let start_seq = u64::from_le_bytes(
        bytes[..8]
            .try_into()
            .expect("WAL batch start seq slice must fit"),
    );
    let entry_count = u32::from_le_bytes(
        bytes[8..12]
            .try_into()
            .expect("WAL batch count slice must fit"),
    ) as usize;

    let mut entries = Vec::with_capacity(entry_count);
    let mut offset = 12usize;
    for index in 0..entry_count {
        if offset + 9 > bytes.len() {
            return Err(StorageError::IoError(
                "WAL batch entry header truncated".to_string(),
            ));
        }
        let op = bytes[offset];
        offset += 1;
        let key_len = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .expect("WAL key length slice must fit"),
        ) as usize;
        offset += 4;
        let value_len = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .expect("WAL value length slice must fit"),
        ) as usize;
        offset += 4;
        if offset + key_len > bytes.len() {
            return Err(StorageError::IoError("WAL key truncated".to_string()));
        }
        let key = bytes[offset..offset + key_len].to_vec();
        offset += key_len;
        let seq = start_seq.saturating_add(index as u64);
        match op {
            1 => {
                if offset + value_len > bytes.len() {
                    return Err(StorageError::IoError("WAL value truncated".to_string()));
                }
                let value = bytes[offset..offset + value_len].to_vec();
                offset += value_len;
                entries.push(WalEntry::Put { seq, key, value });
            }
            2 => entries.push(WalEntry::Delete { seq, key }),
            other => {
                return Err(StorageError::IoError(format!(
                    "WAL operation {} is invalid",
                    other
                )));
            }
        }
    }

    if offset != bytes.len() {
        return Err(StorageError::IoError(
            "WAL batch trailing bytes are invalid".to_string(),
        ));
    }
    Ok(entries)
}

fn read_wal_entries(file: &AnyFile) -> Result<Vec<WalEntry>, StorageError> {
    let len = usize::try_from(file.len().map_err(map_storage_err)?)
        .map_err(|_| StorageError::IoError("WAL length does not fit in usize".to_string()))?;
    if len == 0 {
        return Ok(Vec::new());
    }

    let mut raw = vec![0u8; len];
    file.read_exact_at(0, &mut raw).map_err(map_storage_err)?;

    let mut entries = Vec::new();
    let mut offset = 0usize;
    while offset + WAL_BATCH_HEADER_BYTES <= raw.len() {
        if raw[offset..offset + 8] != WAL_BATCH_MAGIC {
            break;
        }
        let body_len = u32::from_le_bytes(
            raw[offset + 8..offset + 12]
                .try_into()
                .expect("WAL batch length slice must fit"),
        ) as usize;
        let batch_end = offset + WAL_BATCH_HEADER_BYTES + body_len;
        if batch_end > raw.len() {
            break;
        }

        let expected_checksum = wal_checksum(&raw[offset + WAL_BATCH_HEADER_BYTES..batch_end]);
        let checksum = &raw[offset + 12..offset + 12 + MANIFEST_CHECKSUM_BYTES];
        if checksum != expected_checksum {
            break;
        }

        let Ok(batch) = decode_wal_batch(&raw[offset + WAL_BATCH_HEADER_BYTES..batch_end]) else {
            break;
        };
        entries.extend(batch);
        offset = batch_end;
    }

    Ok(entries)
}

impl Storage for OpfsBTreeStorage {
    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        create_object_core(id, metadata, |key, value| self.tree_insert(key, value))
    }

    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        load_object_metadata_core(id, |key| self.tree_read(key))
    }

    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        load_branch_core(
            object_id,
            branch,
            |key| self.tree_read(key),
            |prefix| self.tree_scan_prefix(prefix),
        )
    }

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
    ) -> Result<(), StorageError> {
        append_commit_core(
            object_id,
            branch,
            commit,
            |key| self.tree_read(key),
            |key, value| self.tree_insert(key, value),
        )
    }

    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        delete_commit_core(
            object_id,
            branch,
            commit_id,
            |key| self.tree_read(key),
            |key, value| self.tree_insert(key, value),
            |key| self.tree_delete(key),
        )
    }

    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        set_branch_tails_core(
            object_id,
            branch,
            tails,
            |key, value| self.tree_insert(key, value),
            |key| self.tree_delete(key),
        )
    }

    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: DurabilityTier,
    ) -> Result<(), StorageError> {
        store_ack_tier_core(
            commit_id,
            tier,
            |key| self.tree_read(key),
            |key, value| self.tree_insert(key, value),
        )
    }

    fn append_catalogue_manifest_op(
        &mut self,
        app_id: ObjectId,
        op: CatalogueManifestOp,
    ) -> Result<(), StorageError> {
        append_catalogue_manifest_op_core(
            app_id,
            op,
            |key| self.tree_read(key),
            |key, value| self.tree_insert(key, value),
        )
    }

    fn append_catalogue_manifest_ops(
        &mut self,
        app_id: ObjectId,
        ops: &[CatalogueManifestOp],
    ) -> Result<(), StorageError> {
        append_catalogue_manifest_ops_core(
            app_id,
            ops,
            |key| self.tree_read(key),
            |key, value| self.tree_insert(key, value),
        )
    }

    fn load_catalogue_manifest(
        &self,
        app_id: ObjectId,
    ) -> Result<Option<CatalogueManifest>, StorageError> {
        load_catalogue_manifest_core(app_id, |prefix| self.tree_scan_prefix(prefix))
    }

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        tracing::trace!(table, column, branch, ?row_id, "index_insert");
        index_insert_core(table, column, branch, value, row_id, |key, bytes| {
            self.tree_insert(key, bytes)
        })
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        tracing::trace!(table, column, branch, ?row_id, "index_remove");
        index_remove_core(table, column, branch, value, row_id, |key| {
            self.tree_delete(key)
        })
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        tracing::trace!(table, column, branch, "index_lookup");
        index_lookup_core(table, column, branch, value, |prefix| {
            self.tree_scan_keys(prefix)
        })
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        index_range_core(table, column, branch, start, end, |start_key, end_key| {
            self.tree_scan_key_range(start_key, end_key)
        })
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        index_scan_all_core(table, column, branch, |prefix| self.tree_scan_keys(prefix))
    }

    fn flush(&self) {
        let _span = tracing::debug_span!("OpfsBTreeStorage::flush").entered();
        if let Err(error) = self.with_state_mut(|state| state.checkpoint_inner()) {
            tracing::warn!(?error, "OpfsBTreeStorage flush failed");
        }
    }

    fn flush_wal(&self) {
        let _span = tracing::debug_span!("OpfsBTreeStorage::flush_wal").entered();
        if let Err(error) = self.with_state_mut(|state| state.flush_wal_inner()) {
            tracing::warn!(?error, "OpfsBTreeStorage WAL flush failed");
        }
    }
}

fn map_storage_err(error: BTreeError) -> StorageError {
    StorageError::IoError(format!("opfs-btree: {}", error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;
    use std::collections::BTreeMap;

    fn make_commit(content: &[u8]) -> Commit {
        Commit {
            parents: smallvec![],
            content: content.to_vec(),
            timestamp: 12345,
            author: ObjectId::new(),
            metadata: None,
            stored_state: Default::default(),
            ack_state: Default::default(),
        }
    }

    fn test_storage() -> OpfsBTreeStorage {
        OpfsBTreeStorage::memory(4 * 1024 * 1024).unwrap()
    }

    fn durable_fixture() -> (DurableFiles, usize) {
        (DurableFiles::memory(), 4 * 1024 * 1024)
    }

    fn open_fixture(files: DurableFiles, cache_size_bytes: usize) -> OpfsBTreeStorage {
        OpfsBTreeStorage::open_with_files(files, cache_size_bytes).expect("open fixture storage")
    }

    fn apply_baseline(storage: &OpfsBTreeStorage) {
        storage
            .tree_insert("kv:stable", b"old")
            .expect("insert stable");
        storage
            .tree_insert("kv:delete-me", b"remove-later")
            .expect("insert delete-me");
        storage
            .with_state_mut(|state| state.checkpoint_inner())
            .expect("checkpoint baseline");
    }

    fn apply_update(storage: &OpfsBTreeStorage) {
        storage
            .tree_insert("kv:stable", b"new")
            .expect("update stable");
        storage.tree_insert("kv:new", b"fresh").expect("insert new");
        storage.tree_delete("kv:delete-me").expect("delete key");
    }

    fn collect_tree(storage: &OpfsBTreeStorage) -> BTreeMap<String, Vec<u8>> {
        storage
            .tree_scan_range_bytes(b"", &FULL_SCAN_END_KEY)
            .expect("scan full tree")
            .into_iter()
            .collect()
    }

    fn baseline_tree() -> BTreeMap<String, Vec<u8>> {
        BTreeMap::from([
            ("kv:delete-me".to_string(), b"remove-later".to_vec()),
            ("kv:stable".to_string(), b"old".to_vec()),
        ])
    }

    fn updated_tree() -> BTreeMap<String, Vec<u8>> {
        BTreeMap::from([
            ("kv:new".to_string(), b"fresh".to_vec()),
            ("kv:stable".to_string(), b"new".to_vec()),
        ])
    }

    #[test]
    fn opfs_btree_object_roundtrip() {
        let mut storage = test_storage();

        let id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        );
        metadata.insert("app".to_string(), "test".to_string());

        storage.create_object(id, metadata.clone()).unwrap();

        let loaded = storage.load_object_metadata(id).unwrap();
        assert_eq!(loaded, Some(metadata));

        let other = ObjectId::new();
        assert_eq!(storage.load_object_metadata(other).unwrap(), None);
    }

    #[test]
    fn opfs_btree_commit_roundtrip() {
        let mut storage = test_storage();

        let id = ObjectId::new();
        let branch = BranchName::new("main");
        storage.create_object(id, HashMap::new()).unwrap();

        assert_eq!(storage.load_branch(id, &branch).unwrap(), None);

        let commit = make_commit(b"first");
        let commit_id = commit.id();
        storage.append_commit(id, &branch, commit).unwrap();

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert!(loaded.tails.contains(&commit_id));
        assert_eq!(loaded.commits[0].content, b"first");

        let mut commit2 = make_commit(b"second");
        commit2.parents = smallvec![commit_id];
        let commit2_id = commit2.id();
        storage.append_commit(id, &branch, commit2).unwrap();

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 2);
        assert!(!loaded.tails.contains(&commit_id));
        assert!(loaded.tails.contains(&commit2_id));

        storage.delete_commit(id, &branch, commit_id).unwrap();
        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert_eq!(loaded.commits[0].content, b"second");
    }

    #[test]
    fn opfs_btree_index_ops() {
        let mut storage = test_storage();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();
        let row4 = ObjectId::new();

        storage
            .index_insert("users", "age", "main", &Value::Integer(20), row1)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row2)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row3)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(30), row4)
            .unwrap();

        let results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));

        let results = storage.index_lookup("users", "age", "main", &Value::Integer(99));
        assert!(results.is_empty());

        let results = storage.index_range(
            "users",
            "age",
            "main",
            Bound::Included(&Value::Integer(25)),
            Bound::Excluded(&Value::Integer(30)),
        );
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));

        let results = storage.index_range(
            "users",
            "age",
            "main",
            Bound::Unbounded,
            Bound::Excluded(&Value::Integer(26)),
        );
        assert_eq!(results.len(), 3);
        assert!(results.contains(&row1));
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));

        let results = storage.index_range(
            "users",
            "age",
            "main",
            Bound::Included(&Value::Integer(30)),
            Bound::Unbounded,
        );
        assert_eq!(results.len(), 1);
        assert!(results.contains(&row4));

        let results = storage.index_scan_all("users", "age", "main");
        assert_eq!(results.len(), 4);

        storage
            .index_remove("users", "age", "main", &Value::Integer(25), row2)
            .unwrap();
        let results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(results.len(), 1);
        assert!(results.contains(&row3));
    }

    #[test]
    fn opfs_btree_index_branch_isolation() {
        let mut storage = test_storage();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row1)
            .unwrap();
        storage
            .index_insert("users", "age", "feature", &Value::Integer(25), row2)
            .unwrap();

        let main_results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(main_results.len(), 1);
        assert!(main_results.contains(&row1));

        let feature_results = storage.index_lookup("users", "age", "feature", &Value::Integer(25));
        assert_eq!(feature_results.len(), 1);
        assert!(feature_results.contains(&row2));
    }

    #[test]
    fn opfs_btree_ack_tier_roundtrip() {
        let mut storage = test_storage();

        let commit_id = CommitId([99u8; 32]);

        storage
            .store_ack_tier(commit_id, DurabilityTier::Worker)
            .unwrap();
        storage
            .store_ack_tier(commit_id, DurabilityTier::EdgeServer)
            .unwrap();

        let key = super::super::key_codec::ack_key(commit_id);
        let data = storage.tree_read(&key).unwrap().unwrap();
        let tiers: HashSet<DurabilityTier> = serde_json::from_slice(&data).unwrap();
        assert!(tiers.contains(&DurabilityTier::Worker));
        assert!(tiers.contains(&DurabilityTier::EdgeServer));
    }

    #[test]
    fn opfs_btree_persistence() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.opfsbtree");

        let id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        );

        let commit_content = b"persistent data";
        let branch = BranchName::new("main");

        {
            let mut storage = OpfsBTreeStorage::open(&db_path, 4 * 1024 * 1024).unwrap();
            storage.create_object(id, metadata.clone()).unwrap();

            let commit = make_commit(commit_content);
            storage.append_commit(id, &branch, commit).unwrap();

            storage
                .index_insert(
                    "users",
                    "name",
                    "main",
                    &Value::Text("Alice".to_string()),
                    id,
                )
                .unwrap();

            storage.flush();
        }

        {
            let storage = OpfsBTreeStorage::open(&db_path, 4 * 1024 * 1024).unwrap();

            let loaded_meta = storage.load_object_metadata(id).unwrap();
            assert_eq!(loaded_meta, Some(metadata));

            let loaded_branch = storage.load_branch(id, &branch).unwrap().unwrap();
            assert_eq!(loaded_branch.commits.len(), 1);
            assert_eq!(loaded_branch.commits[0].content, commit_content);

            let results =
                storage.index_lookup("users", "name", "main", &Value::Text("Alice".to_string()));
            assert_eq!(results.len(), 1);
            assert!(results.contains(&id));
        }
    }

    #[test]
    fn opfs_btree_flush_wal_persists_without_snapshot() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("wal-only.opfsbtree");

        {
            let storage = OpfsBTreeStorage::open(&db_path, 4 * 1024 * 1024).unwrap();
            storage.tree_insert("kv:stable", b"wal-value").unwrap();
            storage.flush_wal();
        }

        {
            let storage = OpfsBTreeStorage::open(&db_path, 4 * 1024 * 1024).unwrap();
            assert_eq!(
                storage.tree_read("kv:stable").unwrap(),
                Some(b"wal-value".to_vec())
            );
        }
    }

    #[test]
    fn durable_failpoint_sweep_requires_clean_recovery_for_every_step() {
        test_failpoints::clear();
        let cache_size_bytes = 4 * 1024 * 1024;
        let (probe_files, _) = durable_fixture();

        let max_step = {
            let storage = open_fixture(probe_files.clone(), cache_size_bytes);
            apply_baseline(&storage);
            apply_update(&storage);
            test_failpoints::clear();
            storage
                .with_state_mut(|state| state.checkpoint_inner())
                .expect("baseline durable checkpoint");
            test_failpoints::step_count()
        };
        assert!(max_step > 0, "expected at least one durable failpoint");
        test_failpoints::clear();

        let baseline = baseline_tree();
        let updated = updated_tree();
        let mut failures = Vec::new();

        for step in 1..=max_step {
            let (files, _) = durable_fixture();
            let storage = open_fixture(files.clone(), cache_size_bytes);
            apply_baseline(&storage);
            drop(storage);

            let storage = open_fixture(files.clone(), cache_size_bytes);
            apply_update(&storage);
            test_failpoints::arm(step);
            let result = storage.with_state_mut(|state| state.checkpoint_inner());
            let hit_sites = test_failpoints::hit_sites();
            let site = hit_sites
                .get(step.saturating_sub(1))
                .copied()
                .or_else(|| hit_sites.last().copied())
                .unwrap_or("<no-site-recorded>");
            match result {
                Ok(()) => failures.push(format!(
                    "step {step}: {site} -> armed failpoint did not abort durable checkpoint"
                )),
                Err(StorageError::IoError(message)) if message.contains("durable failpoint") => {}
                Err(error) => failures.push(format!(
                    "step {step}: {site} -> unexpected error during checkpoint: {:?}",
                    error
                )),
            }
            drop(storage);
            test_failpoints::clear();

            let reopened = open_fixture(files.clone(), cache_size_bytes);
            let recovered = collect_tree(&reopened);
            if recovered != baseline && recovered != updated {
                failures.push(format!(
                    "step {step}: {site} -> mixed durable recovery state: {:?}",
                    recovered
                ));
            }
        }

        if !failures.is_empty() {
            panic!(
                "durable failpoint sweep found {} failing step(s)\n{}",
                failures.len(),
                failures.join("\n")
            );
        }
    }

    #[test]
    fn opfs_btree_catalogue_manifest_roundtrip() {
        let mut storage = test_storage();
        let app_id = ObjectId::new();
        let schema_object_id = ObjectId::new();
        let lens_object_id = ObjectId::new();
        let schema_hash = crate::query_manager::types::SchemaHash::from_bytes([0x11; 32]);
        let source_hash = crate::query_manager::types::SchemaHash::from_bytes([0x22; 32]);
        let target_hash = crate::query_manager::types::SchemaHash::from_bytes([0x33; 32]);

        storage
            .append_catalogue_manifest_op(
                app_id,
                crate::storage::CatalogueManifestOp::SchemaSeen {
                    object_id: schema_object_id,
                    schema_hash,
                },
            )
            .unwrap();
        storage
            .append_catalogue_manifest_op(
                app_id,
                crate::storage::CatalogueManifestOp::LensSeen {
                    object_id: lens_object_id,
                    source_hash,
                    target_hash,
                },
            )
            .unwrap();
        storage
            .append_catalogue_manifest_op(
                app_id,
                crate::storage::CatalogueManifestOp::SchemaSeen {
                    object_id: schema_object_id,
                    schema_hash,
                },
            )
            .unwrap();

        let manifest = storage.load_catalogue_manifest(app_id).unwrap().unwrap();
        assert_eq!(
            manifest.schema_seen.get(&schema_object_id),
            Some(&schema_hash)
        );
        assert_eq!(
            manifest.lens_seen.get(&lens_object_id),
            Some(&crate::storage::CatalogueLensSeen {
                source_hash,
                target_hash,
            })
        );
    }
}
