use crate::BTreeError;
use crate::db::{BTreeOptions, OpfsBTree};
use crate::file::{MemoryFile, SyncFile};

#[cfg(target_arch = "wasm32")]
use crate::file::OpfsFile;
#[cfg(not(target_arch = "wasm32"))]
use crate::file::StdFile;

#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};

const FULL_SCAN_END_KEY: [u8; 1] = [u8::MAX];
const MANIFEST_MAGIC: [u8; 8] = *b"JAZZWAL1";
const MANIFEST_VERSION: u32 = 1;
const MANIFEST_SLOT_BYTES: usize = 128;
const CHECKSUM_BYTES: usize = 4;
const WAL_BATCH_MAGIC: [u8; 8] = *b"JAZZWLB1";
const WAL_BATCH_HEADER_BYTES: usize = 8 + 4 + CHECKSUM_BYTES;

type EncodedKv = (Vec<u8>, Vec<u8>);

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

    fn decode(raw: u8) -> Result<Self, BTreeError> {
        match raw {
            0 => Ok(Self::A),
            1 => Ok(Self::B),
            other => Err(BTreeError::Io(format!(
                "durable manifest has invalid replica slot {}",
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
pub struct DurableBTreeFiles<F: Clone> {
    snapshot_a: F,
    snapshot_b: F,
    wal_a: F,
    wal_b: F,
    manifest: F,
}

impl<F: Clone> DurableBTreeFiles<F> {
    pub fn new(snapshot_a: F, snapshot_b: F, wal_a: F, wal_b: F, manifest: F) -> Self {
        Self {
            snapshot_a,
            snapshot_b,
            wal_a,
            wal_b,
            manifest,
        }
    }

    pub fn map<G: Clone>(self, mut f: impl FnMut(F) -> G) -> DurableBTreeFiles<G> {
        DurableBTreeFiles {
            snapshot_a: f(self.snapshot_a),
            snapshot_b: f(self.snapshot_b),
            wal_a: f(self.wal_a),
            wal_b: f(self.wal_b),
            manifest: f(self.manifest),
        }
    }

    fn snapshot(&self, slot: ReplicaSlot) -> F {
        match slot {
            ReplicaSlot::A => self.snapshot_a.clone(),
            ReplicaSlot::B => self.snapshot_b.clone(),
        }
    }

    fn wal(&self, slot: ReplicaSlot) -> F {
        match slot {
            ReplicaSlot::A => self.wal_a.clone(),
            ReplicaSlot::B => self.wal_b.clone(),
        }
    }

    fn manifest(&self) -> F {
        self.manifest.clone()
    }
}

impl DurableBTreeFiles<MemoryFile> {
    pub fn memory() -> Self {
        Self::new(
            MemoryFile::new(),
            MemoryFile::new(),
            MemoryFile::new(),
            MemoryFile::new(),
            MemoryFile::new(),
        )
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl DurableBTreeFiles<StdFile> {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, BTreeError> {
        let path = path.as_ref();
        Ok(Self::new(
            StdFile::open(path)?,
            StdFile::open(sidecar_path(path, ".snapshot-b"))?,
            StdFile::open(sidecar_path(path, ".wal-a"))?,
            StdFile::open(sidecar_path(path, ".wal-b"))?,
            StdFile::open(sidecar_path(path, ".manifest"))?,
        ))
    }
}

#[cfg(target_arch = "wasm32")]
impl DurableBTreeFiles<OpfsFile> {
    pub async fn open_opfs(namespace: &str) -> Result<Self, BTreeError> {
        Ok(Self::new(
            OpfsFile::open(namespace).await?,
            OpfsFile::open(&opfs_sidecar_name(namespace, "snapshot-b")).await?,
            OpfsFile::open(&opfs_sidecar_name(namespace, "wal-a")).await?,
            OpfsFile::open(&opfs_sidecar_name(namespace, "wal-b")).await?,
            OpfsFile::open(&opfs_sidecar_name(namespace, "manifest")).await?,
        ))
    }

    pub async fn destroy_opfs(namespace: &str) -> Result<(), BTreeError> {
        OpfsFile::destroy(namespace).await?;
        let _ = OpfsFile::destroy(&opfs_sidecar_name(namespace, "snapshot-b")).await;
        let _ = OpfsFile::destroy(&opfs_sidecar_name(namespace, "wal-a")).await;
        let _ = OpfsFile::destroy(&opfs_sidecar_name(namespace, "wal-b")).await;
        let _ = OpfsFile::destroy(&opfs_sidecar_name(namespace, "manifest")).await;
        Ok(())
    }
}

#[derive(Debug)]
pub struct DurableOpfsBTree<F: SyncFile + Clone> {
    files: DurableBTreeFiles<F>,
    options: BTreeOptions,
    tree: OpfsBTree<F>,
    manifest_slot: ManifestSlot,
    manifest: DurableManifest,
    pending_wal: Vec<WalEntry>,
    wal_flushed_seq: u64,
    next_wal_seq: u64,
}

#[cfg(test)]
mod test_failpoints {
    use std::cell::RefCell;

    use crate::BTreeError;

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

    pub(crate) fn hit(site: &'static str) -> Result<(), BTreeError> {
        STATE.with(|cell| {
            let mut state = cell.borrow_mut();
            state.next_step = state.next_step.saturating_add(1);
            let step = state.next_step;
            state.hit_sites.push(site);
            if state.armed_step == Some(step) {
                return Err(BTreeError::Io(format!(
                    "simulated durable failpoint at step {} ({})",
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

impl<F: SyncFile + Clone> DurableOpfsBTree<F> {
    pub fn open(files: DurableBTreeFiles<F>, options: BTreeOptions) -> Result<Self, BTreeError> {
        let manifest_file = files.manifest();
        let (manifest_slot, manifest) = read_manifest(&manifest_file)?
            .unwrap_or((ManifestSlot::A, DurableManifest::bootstrap()));
        durable_failpoint!("open:after-read-manifest");

        let mut tree = OpfsBTree::open(files.snapshot(manifest.active_snapshot), options)?;
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
            files,
            options,
            tree,
            manifest_slot,
            manifest,
            pending_wal: Vec::new(),
            wal_flushed_seq,
            next_wal_seq: wal_flushed_seq.saturating_add(1),
        })
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, BTreeError> {
        self.tree.get(key)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), BTreeError> {
        self.tree.put(key, value)?;
        let seq = self.alloc_seq();
        self.pending_wal.push(WalEntry::Put {
            seq,
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), BTreeError> {
        self.tree.delete(key)?;
        let seq = self.alloc_seq();
        self.pending_wal.push(WalEntry::Delete {
            seq,
            key: key.to_vec(),
        });
        Ok(())
    }

    pub fn range(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<EncodedKv>, BTreeError> {
        self.tree.range(start, end, limit)
    }

    pub fn flush_wal(&mut self) -> Result<(), BTreeError> {
        if self.pending_wal.is_empty() {
            return Ok(());
        }

        let active_wal = self.files.wal(self.manifest.active_wal);
        let batch = encode_wal_batch(&self.pending_wal)?;
        let offset = active_wal.len()?;
        durable_failpoint!("flush_wal:before-write");
        active_wal.write_all_at(offset, &batch)?;
        durable_failpoint!("flush_wal:after-write");
        active_wal.flush()?;
        durable_failpoint!("flush_wal:after-flush");

        self.wal_flushed_seq = self
            .pending_wal
            .last()
            .map(WalEntry::seq)
            .unwrap_or(self.wal_flushed_seq);
        self.pending_wal.clear();
        Ok(())
    }

    pub fn checkpoint(&mut self) -> Result<(), BTreeError> {
        self.flush_wal()?;
        durable_failpoint!("checkpoint:after-flush-wal");

        if self.wal_flushed_seq == self.manifest.applied_wal_seq {
            return Ok(());
        }

        let target_snapshot = self.manifest.active_snapshot.inactive();
        let target_wal = self.manifest.active_wal.inactive();

        let snapshot_file = self.files.snapshot(target_snapshot);
        snapshot_file.truncate(0)?;
        durable_failpoint!("checkpoint:after-truncate-snapshot");

        let mut snapshot_tree = OpfsBTree::open(snapshot_file, self.options)?;
        durable_failpoint!("checkpoint:after-open-target-snapshot");

        for (key, value) in self.full_scan()? {
            snapshot_tree.put(&key, &value)?;
        }
        durable_failpoint!("checkpoint:after-build-snapshot");
        snapshot_tree.checkpoint()?;
        durable_failpoint!("checkpoint:after-snapshot-checkpoint");

        let next_wal_file = self.files.wal(target_wal);
        next_wal_file.truncate(0)?;
        next_wal_file.flush()?;
        durable_failpoint!("checkpoint:after-prepare-inactive-wal");

        let next_manifest = DurableManifest {
            generation: self.manifest.generation.saturating_add(1),
            active_snapshot: target_snapshot,
            active_wal: target_wal,
            applied_wal_seq: self.wal_flushed_seq,
        };
        let next_manifest_slot = self.manifest_slot.inactive();
        write_manifest(&self.files.manifest(), next_manifest_slot, next_manifest)?;
        durable_failpoint!("checkpoint:after-manifest-write");
        self.files.manifest().flush()?;
        durable_failpoint!("checkpoint:after-manifest-flush");

        self.tree = snapshot_tree;
        self.manifest = next_manifest;
        self.manifest_slot = next_manifest_slot;
        self.pending_wal.clear();
        durable_failpoint!("checkpoint:after-tree-swap");
        Ok(())
    }

    pub fn into_files(self) -> DurableBTreeFiles<F> {
        self.files
    }

    fn alloc_seq(&mut self) -> u64 {
        let seq = self.next_wal_seq;
        self.next_wal_seq = self.next_wal_seq.saturating_add(1);
        seq
    }

    fn full_scan(&mut self) -> Result<Vec<EncodedKv>, BTreeError> {
        self.tree.range(b"", &FULL_SCAN_END_KEY, usize::MAX)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| "opfs-btree".to_string());
    let mut out = path.to_path_buf();
    out.set_file_name(format!("{}{}", file_name, suffix));
    out
}

#[cfg(target_arch = "wasm32")]
fn opfs_sidecar_name(namespace: &str, suffix: &str) -> String {
    format!("{namespace}__{suffix}")
}

fn apply_wal_entry<F: SyncFile + Clone>(
    tree: &mut OpfsBTree<F>,
    entry: &WalEntry,
) -> Result<(), BTreeError> {
    match entry {
        WalEntry::Put { key, value, .. } => tree.put(key, value),
        WalEntry::Delete { key, .. } => tree.delete(key),
    }
}

fn checksum(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

fn encode_manifest(manifest: DurableManifest) -> [u8; MANIFEST_SLOT_BYTES] {
    let mut slot = [0u8; MANIFEST_SLOT_BYTES];
    let mut payload = Vec::with_capacity(30);
    payload.extend_from_slice(&MANIFEST_MAGIC);
    payload.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    payload.extend_from_slice(&manifest.generation.to_le_bytes());
    payload.push(manifest.active_snapshot.encode());
    payload.push(manifest.active_wal.encode());
    payload.extend_from_slice(&manifest.applied_wal_seq.to_le_bytes());
    let payload_len = payload.len();
    slot[..payload_len].copy_from_slice(&payload);
    slot[payload_len..payload_len + CHECKSUM_BYTES]
        .copy_from_slice(&checksum(&payload).to_le_bytes());
    slot
}

fn decode_manifest(slot: &[u8]) -> Result<DurableManifest, BTreeError> {
    if slot.len() < MANIFEST_SLOT_BYTES {
        return Err(BTreeError::Io(
            "durable manifest slot too small".to_string(),
        ));
    }
    if slot[..MANIFEST_MAGIC.len()] != MANIFEST_MAGIC {
        return Err(BTreeError::Io(
            "durable manifest magic mismatch".to_string(),
        ));
    }

    let checksum_offset = 8 + 4 + 8 + 1 + 1 + 8;
    let expected_checksum = checksum(&slot[..checksum_offset]);
    let actual_checksum = u32::from_le_bytes(
        slot[checksum_offset..checksum_offset + CHECKSUM_BYTES]
            .try_into()
            .expect("manifest checksum slice"),
    );
    if actual_checksum != expected_checksum {
        return Err(BTreeError::Io(
            "durable manifest checksum mismatch".to_string(),
        ));
    }

    let version = u32::from_le_bytes(slot[8..12].try_into().expect("manifest version slice"));
    if version != MANIFEST_VERSION {
        return Err(BTreeError::Io(format!(
            "durable manifest version {} is unsupported",
            version
        )));
    }

    let generation =
        u64::from_le_bytes(slot[12..20].try_into().expect("manifest generation slice"));
    let active_snapshot = ReplicaSlot::decode(slot[20])?;
    let active_wal = ReplicaSlot::decode(slot[21])?;
    let applied_wal_seq = u64::from_le_bytes(slot[22..30].try_into().expect("manifest seq slice"));

    Ok(DurableManifest {
        generation,
        active_snapshot,
        active_wal,
        applied_wal_seq,
    })
}

fn read_manifest<F: SyncFile>(
    file: &F,
) -> Result<Option<(ManifestSlot, DurableManifest)>, BTreeError> {
    if file.len()? < MANIFEST_SLOT_BYTES as u64 {
        return Ok(None);
    }

    let mut best: Option<(ManifestSlot, DurableManifest)> = None;
    for slot_id in [ManifestSlot::A, ManifestSlot::B] {
        let needed = slot_id.byte_offset() + MANIFEST_SLOT_BYTES as u64;
        if file.len()? < needed {
            continue;
        }

        let mut raw = [0u8; MANIFEST_SLOT_BYTES];
        file.read_exact_at(slot_id.byte_offset(), &mut raw)?;
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

fn write_manifest<F: SyncFile>(
    file: &F,
    slot: ManifestSlot,
    manifest: DurableManifest,
) -> Result<(), BTreeError> {
    let raw = encode_manifest(manifest);
    file.write_all_at(slot.byte_offset(), &raw)
}

fn encode_wal_batch(entries: &[WalEntry]) -> Result<Vec<u8>, BTreeError> {
    let mut body = Vec::new();
    let start_seq = entries
        .first()
        .map(WalEntry::seq)
        .ok_or_else(|| BTreeError::Io("empty WAL batch".to_string()))?;
    body.extend_from_slice(&start_seq.to_le_bytes());
    body.extend_from_slice(
        &(u32::try_from(entries.len())
            .map_err(|_| BTreeError::Io("WAL batch too large".to_string()))?)
        .to_le_bytes(),
    );

    for entry in entries {
        match entry {
            WalEntry::Put { key, value, .. } => {
                body.push(1);
                body.extend_from_slice(
                    &(u32::try_from(key.len())
                        .map_err(|_| BTreeError::Io("WAL key too large".to_string()))?)
                    .to_le_bytes(),
                );
                body.extend_from_slice(
                    &(u32::try_from(value.len())
                        .map_err(|_| BTreeError::Io("WAL value too large".to_string()))?)
                    .to_le_bytes(),
                );
                body.extend_from_slice(key);
                body.extend_from_slice(value);
            }
            WalEntry::Delete { key, .. } => {
                body.push(2);
                body.extend_from_slice(
                    &(u32::try_from(key.len())
                        .map_err(|_| BTreeError::Io("WAL key too large".to_string()))?)
                    .to_le_bytes(),
                );
                body.extend_from_slice(&0u32.to_le_bytes());
                body.extend_from_slice(key);
            }
        }
    }

    let mut out = Vec::with_capacity(WAL_BATCH_HEADER_BYTES + body.len());
    out.extend_from_slice(&WAL_BATCH_MAGIC);
    out.extend_from_slice(
        &(u32::try_from(body.len())
            .map_err(|_| BTreeError::Io("WAL body too large".to_string()))?)
        .to_le_bytes(),
    );
    out.extend_from_slice(&checksum(&body).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

fn decode_wal_batch(bytes: &[u8]) -> Result<Vec<WalEntry>, BTreeError> {
    if bytes.len() < 12 {
        return Err(BTreeError::Io("WAL batch body too small".to_string()));
    }

    let start_seq = u64::from_le_bytes(bytes[..8].try_into().expect("WAL start seq slice"));
    let entry_count =
        u32::from_le_bytes(bytes[8..12].try_into().expect("WAL count slice")) as usize;

    let mut entries = Vec::with_capacity(entry_count);
    let mut offset = 12usize;
    for index in 0..entry_count {
        if offset + 9 > bytes.len() {
            return Err(BTreeError::Io("WAL entry header truncated".to_string()));
        }

        let op = bytes[offset];
        offset += 1;
        let key_len = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .expect("WAL key len slice"),
        ) as usize;
        offset += 4;
        let value_len = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .expect("WAL value len slice"),
        ) as usize;
        offset += 4;

        if offset + key_len > bytes.len() {
            return Err(BTreeError::Io("WAL key truncated".to_string()));
        }
        let key = bytes[offset..offset + key_len].to_vec();
        offset += key_len;
        let seq = start_seq.saturating_add(index as u64);

        match op {
            1 => {
                if offset + value_len > bytes.len() {
                    return Err(BTreeError::Io("WAL value truncated".to_string()));
                }
                let value = bytes[offset..offset + value_len].to_vec();
                offset += value_len;
                entries.push(WalEntry::Put { seq, key, value });
            }
            2 => entries.push(WalEntry::Delete { seq, key }),
            other => {
                return Err(BTreeError::Io(format!("WAL op {} is invalid", other)));
            }
        }
    }

    if offset != bytes.len() {
        return Err(BTreeError::Io(
            "WAL batch trailing bytes are invalid".to_string(),
        ));
    }
    Ok(entries)
}

fn read_wal_entries<F: SyncFile>(file: &F) -> Result<Vec<WalEntry>, BTreeError> {
    let len = usize::try_from(file.len()?)
        .map_err(|_| BTreeError::Io("WAL length does not fit in usize".to_string()))?;
    if len == 0 {
        return Ok(Vec::new());
    }

    let mut raw = vec![0u8; len];
    file.read_exact_at(0, &mut raw)?;

    let mut entries = Vec::new();
    let mut offset = 0usize;
    while offset + WAL_BATCH_HEADER_BYTES <= raw.len() {
        if raw[offset..offset + 8] != WAL_BATCH_MAGIC {
            break;
        }

        let body_len = u32::from_le_bytes(
            raw[offset + 8..offset + 12]
                .try_into()
                .expect("WAL body len slice"),
        ) as usize;
        let batch_end = offset + WAL_BATCH_HEADER_BYTES + body_len;
        if batch_end > raw.len() {
            break;
        }

        let actual_checksum = u32::from_le_bytes(
            raw[offset + 12..offset + 12 + CHECKSUM_BYTES]
                .try_into()
                .expect("WAL checksum slice"),
        );
        let expected_checksum = checksum(&raw[offset + WAL_BATCH_HEADER_BYTES..batch_end]);
        if actual_checksum != expected_checksum {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn durable_fixture() -> DurableBTreeFiles<MemoryFile> {
        DurableBTreeFiles::memory()
    }

    fn open_fixture(files: DurableBTreeFiles<MemoryFile>) -> DurableOpfsBTree<MemoryFile> {
        DurableOpfsBTree::open(files, BTreeOptions::default()).expect("open durable fixture")
    }

    fn apply_baseline(tree: &mut DurableOpfsBTree<MemoryFile>) {
        tree.put(b"kv:stable", b"old").expect("put stable");
        tree.put(b"kv:delete-me", b"remove-later")
            .expect("put delete-me");
        tree.checkpoint().expect("checkpoint baseline");
    }

    fn apply_update(tree: &mut DurableOpfsBTree<MemoryFile>) {
        tree.put(b"kv:stable", b"new").expect("update stable");
        tree.put(b"kv:new", b"fresh").expect("put new");
        tree.delete(b"kv:delete-me").expect("delete old key");
    }

    fn collect_tree(tree: &mut DurableOpfsBTree<MemoryFile>) -> Vec<(Vec<u8>, Vec<u8>)> {
        tree.range(b"", &FULL_SCAN_END_KEY, usize::MAX)
            .expect("scan full tree")
    }

    fn baseline_tree() -> Vec<(Vec<u8>, Vec<u8>)> {
        vec![
            (b"kv:delete-me".to_vec(), b"remove-later".to_vec()),
            (b"kv:stable".to_vec(), b"old".to_vec()),
        ]
    }

    fn updated_tree() -> Vec<(Vec<u8>, Vec<u8>)> {
        vec![
            (b"kv:new".to_vec(), b"fresh".to_vec()),
            (b"kv:stable".to_vec(), b"new".to_vec()),
        ]
    }

    #[test]
    fn flush_wal_persists_without_checkpoint() {
        let files = durable_fixture();
        {
            let mut tree = open_fixture(files.clone());
            tree.put(b"kv:stable", b"wal-value").expect("put");
            tree.flush_wal().expect("flush wal");
        }

        let mut reopened = open_fixture(files);
        assert_eq!(
            reopened.get(b"kv:stable").expect("get reopened"),
            Some(b"wal-value".to_vec())
        );
    }

    #[test]
    fn durable_failpoint_sweep_requires_clean_recovery_for_every_step() {
        test_failpoints::clear();
        let probe_files = durable_fixture();

        let max_step = {
            let mut tree = open_fixture(probe_files.clone());
            apply_baseline(&mut tree);
            apply_update(&mut tree);
            test_failpoints::clear();
            tree.checkpoint().expect("probe checkpoint");
            test_failpoints::step_count()
        };
        assert!(max_step > 0, "expected at least one durable failpoint");
        test_failpoints::clear();

        let baseline = baseline_tree();
        let updated = updated_tree();
        let mut failures = Vec::new();

        for step in 1..=max_step {
            let files = durable_fixture();
            {
                let mut tree = open_fixture(files.clone());
                apply_baseline(&mut tree);
            }

            let mut tree = open_fixture(files.clone());
            apply_update(&mut tree);
            test_failpoints::arm(step);
            let result = tree.checkpoint();
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
                Err(err) if err.to_string().contains("simulated durable failpoint") => {}
                Err(err) => failures.push(format!(
                    "step {step}: {site} -> unexpected checkpoint error: {err}"
                )),
            }
            drop(tree);
            test_failpoints::clear();

            let mut reopened = open_fixture(files);
            let recovered = collect_tree(&mut reopened);
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
}
