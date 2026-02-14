use std::collections::BTreeMap;

use crate::BTreeError;
use crate::file::SyncFile;
use crate::superblock::{Superblock, SuperblockSlot};

const MIN_PAGE_SIZE: usize = 4 * 1024;
const DEFAULT_PAGE_SIZE: usize = 16 * 1024;
const DEFAULT_CACHE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_OVERFLOW_THRESHOLD: usize = 8 * 1024;
const BOOTSTRAP_GENERATION: u64 = 1;

const SNAPSHOT_MAGIC: [u8; 8] = *b"OBSNAP01";
const SNAPSHOT_HEADER_BYTES: usize = 16;

#[derive(Debug, Clone, Copy)]
pub struct BTreeOptions {
    pub page_size: usize,
    pub cache_bytes: usize,
    pub overflow_threshold: usize,
}

impl Default for BTreeOptions {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            cache_bytes: DEFAULT_CACHE_BYTES,
            overflow_threshold: DEFAULT_OVERFLOW_THRESHOLD,
        }
    }
}

impl BTreeOptions {
    fn validate(self) -> Result<(), BTreeError> {
        if self.page_size < MIN_PAGE_SIZE {
            return Err(BTreeError::InvalidOptions(format!(
                "page_size must be >= {}",
                MIN_PAGE_SIZE
            )));
        }
        if !self.page_size.is_power_of_two() {
            return Err(BTreeError::InvalidOptions(
                "page_size must be a power of two".to_string(),
            ));
        }
        if self.cache_bytes < self.page_size {
            return Err(BTreeError::InvalidOptions(
                "cache_bytes must be >= page_size".to_string(),
            ));
        }
        if self.overflow_threshold == 0 {
            return Err(BTreeError::InvalidOptions(
                "overflow_threshold must be > 0".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointState {
    pub active_slot: char,
    pub generation: u64,
    pub root_page_id: u64,
    pub freelist_head_page_id: u64,
    pub total_pages: u64,
}

#[derive(Debug)]
pub struct OpfsBTree<F: SyncFile> {
    file: F,
    options: BTreeOptions,
    active_slot: SuperblockSlot,
    active: Superblock,
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl<F: SyncFile> OpfsBTree<F> {
    pub fn open(file: F, options: BTreeOptions) -> Result<Self, BTreeError> {
        options.validate()?;

        let a = read_slot(&file, SuperblockSlot::A, options.page_size)?;
        let b = read_slot(&file, SuperblockSlot::B, options.page_size)?;
        let file_len = file.len()?;

        let (active_slot, active) = choose_active(a, b).unwrap_or((
            SuperblockSlot::A,
            Superblock::new(options.page_size as u32, 0, 0, 0, 0),
        ));

        let mut tree = Self {
            file,
            options,
            active_slot,
            active,
            data: BTreeMap::new(),
        };

        if tree.active.generation == 0 {
            if file_len != 0 {
                return Err(BTreeError::Corrupt(
                    "no valid superblock found in non-empty file".to_string(),
                ));
            }
            let bootstrap =
                Superblock::new(options.page_size as u32, BOOTSTRAP_GENERATION, 0, 0, 2);
            write_slot(&tree.file, SuperblockSlot::A, options.page_size, bootstrap)?;
            write_slot(&tree.file, SuperblockSlot::B, options.page_size, bootstrap)?;
            tree.file.flush()?;
            tree.active_slot = SuperblockSlot::A;
            tree.active = bootstrap;
            return Ok(tree);
        }

        tree.data = load_snapshot(&tree.file, tree.options.page_size, tree.active)?;
        Ok(tree)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BTreeError> {
        Ok(self.data.get(key).cloned())
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), BTreeError> {
        self.data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), BTreeError> {
        self.data.remove(key);
        Ok(())
    }

    pub fn range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, BTreeError> {
        if limit == 0 || start >= end {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for (k, v) in &self.data {
            let key = k.as_slice();
            if key < start {
                continue;
            }
            if key >= end {
                break;
            }
            out.push((k.clone(), v.clone()));
            if out.len() == limit {
                break;
            }
        }
        Ok(out)
    }

    pub fn checkpoint(&mut self) -> Result<(), BTreeError> {
        let (root_page_id, total_pages) = self.write_snapshot()?;
        self.checkpoint_superblock(root_page_id, 0, total_pages)
    }

    pub fn checkpoint_state(&self) -> CheckpointState {
        CheckpointState {
            active_slot: slot_char(self.active_slot),
            generation: self.active.generation,
            root_page_id: self.active.root_page_id,
            freelist_head_page_id: self.active.freelist_head_page_id,
            total_pages: self.active.total_pages,
        }
    }

    pub fn into_file(self) -> F {
        self.file
    }

    fn write_snapshot(&self) -> Result<(u64, u64), BTreeError> {
        let snapshot = serialize_snapshot(&self.data)?;
        let page_size = self.options.page_size;
        let page_count = div_ceil(snapshot.len(), page_size)? as u64;
        let start_page_id = self.active.total_pages.max(2);
        let total_pages = start_page_id
            .checked_add(page_count)
            .ok_or_else(|| BTreeError::Io("page count overflow".to_string()))?;

        let required_file_len = total_pages
            .checked_mul(page_size as u64)
            .ok_or_else(|| BTreeError::Io("file length overflow".to_string()))?;

        if self.file.len()? < required_file_len {
            self.file.truncate(required_file_len)?;
        }

        let mut page = vec![0u8; page_size];
        for i in 0..page_count {
            page.fill(0);
            let src_start = (i as usize)
                .checked_mul(page_size)
                .ok_or_else(|| BTreeError::Io("snapshot offset overflow".to_string()))?;
            let src_end = src_start.saturating_add(page_size).min(snapshot.len());
            if src_end > src_start {
                page[..(src_end - src_start)].copy_from_slice(&snapshot[src_start..src_end]);
            }
            let offset = (start_page_id + i)
                .checked_mul(page_size as u64)
                .ok_or_else(|| BTreeError::Io("page write offset overflow".to_string()))?;
            self.file.write_all_at(offset, &page)?;
        }

        self.file.flush()?;
        Ok((start_page_id, total_pages))
    }

    fn checkpoint_superblock(
        &mut self,
        root_page_id: u64,
        freelist_head_page_id: u64,
        total_pages: u64,
    ) -> Result<(), BTreeError> {
        if total_pages < 2 {
            return Err(BTreeError::InvalidOptions(
                "total_pages must be >= 2".to_string(),
            ));
        }

        let next = Superblock::new(
            self.options.page_size as u32,
            self.active.generation.saturating_add(1),
            root_page_id,
            freelist_head_page_id,
            total_pages,
        );

        let target_slot = self.active_slot.inactive();
        write_slot(&self.file, target_slot, self.options.page_size, next)?;
        self.file.flush()?;

        self.active_slot = target_slot;
        self.active = next;
        Ok(())
    }
}

fn div_ceil(len: usize, step: usize) -> Result<usize, BTreeError> {
    if step == 0 {
        return Err(BTreeError::InvalidOptions("step must be > 0".to_string()));
    }
    let len_minus_one = len
        .checked_sub(1)
        .ok_or_else(|| BTreeError::Io("length underflow".to_string()))?;
    let pages = (len_minus_one / step)
        .checked_add(1)
        .ok_or_else(|| BTreeError::Io("page count overflow".to_string()))?;
    Ok(pages)
}

fn load_snapshot<F: SyncFile>(
    file: &F,
    page_size: usize,
    active: Superblock,
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, BTreeError> {
    if active.root_page_id == 0 {
        return Ok(BTreeMap::new());
    }

    if active.root_page_id < 2 || active.root_page_id >= active.total_pages {
        return Err(BTreeError::Corrupt(format!(
            "invalid root page id {} for total_pages {}",
            active.root_page_id, active.total_pages
        )));
    }

    let root_offset = active
        .root_page_id
        .checked_mul(page_size as u64)
        .ok_or_else(|| BTreeError::Corrupt("root offset overflow".to_string()))?;

    let mut first_page = vec![0u8; page_size];
    file.read_exact_at(root_offset, &mut first_page)?;

    if first_page.len() < SNAPSHOT_HEADER_BYTES {
        return Err(BTreeError::Corrupt("snapshot header too small".to_string()));
    }
    if first_page[..8] != SNAPSHOT_MAGIC {
        return Err(BTreeError::Corrupt("snapshot magic mismatch".to_string()));
    }

    let payload_len = u64::from_le_bytes(
        first_page[8..16]
            .try_into()
            .expect("snapshot payload length slice"),
    );

    let total_len_u64 = (SNAPSHOT_HEADER_BYTES as u64)
        .checked_add(payload_len)
        .ok_or_else(|| BTreeError::Corrupt("snapshot length overflow".to_string()))?;
    let total_len = usize::try_from(total_len_u64)
        .map_err(|_| BTreeError::Corrupt("snapshot too large for platform".to_string()))?;

    let page_count = div_ceil(total_len, page_size)? as u64;
    let end_page = active
        .root_page_id
        .checked_add(page_count)
        .ok_or_else(|| BTreeError::Corrupt("snapshot page range overflow".to_string()))?;
    if end_page > active.total_pages {
        return Err(BTreeError::Corrupt(
            "snapshot extends beyond total_pages".to_string(),
        ));
    }

    let mut raw = vec![0u8; (page_count as usize) * page_size];
    for i in 0..page_count {
        let offset = (active.root_page_id + i)
            .checked_mul(page_size as u64)
            .ok_or_else(|| BTreeError::Corrupt("snapshot read offset overflow".to_string()))?;
        let start = (i as usize)
            .checked_mul(page_size)
            .ok_or_else(|| BTreeError::Corrupt("snapshot read start overflow".to_string()))?;
        file.read_exact_at(offset, &mut raw[start..start + page_size])?;
    }

    raw.truncate(total_len);
    deserialize_snapshot(&raw)
}

fn serialize_snapshot(data: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<Vec<u8>, BTreeError> {
    let mut payload = Vec::new();
    let count = u32::try_from(data.len())
        .map_err(|_| BTreeError::Io("too many entries for snapshot format".to_string()))?;
    payload.extend_from_slice(&count.to_le_bytes());

    for (key, value) in data {
        let key_len = u32::try_from(key.len())
            .map_err(|_| BTreeError::Io("key too large for snapshot format".to_string()))?;
        let value_len = u32::try_from(value.len())
            .map_err(|_| BTreeError::Io("value too large for snapshot format".to_string()))?;
        payload.extend_from_slice(&key_len.to_le_bytes());
        payload.extend_from_slice(&value_len.to_le_bytes());
        payload.extend_from_slice(key);
        payload.extend_from_slice(value);
    }

    let payload_len = u64::try_from(payload.len())
        .map_err(|_| BTreeError::Io("snapshot payload too large".to_string()))?;

    let mut out = Vec::with_capacity(SNAPSHOT_HEADER_BYTES + payload.len());
    out.extend_from_slice(&SNAPSHOT_MAGIC);
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

fn deserialize_snapshot(raw: &[u8]) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, BTreeError> {
    if raw.len() < SNAPSHOT_HEADER_BYTES {
        return Err(BTreeError::Corrupt("snapshot buffer too small".to_string()));
    }
    if raw[..8] != SNAPSHOT_MAGIC {
        return Err(BTreeError::Corrupt("snapshot magic mismatch".to_string()));
    }

    let payload_len = u64::from_le_bytes(
        raw[8..16]
            .try_into()
            .expect("snapshot payload length slice"),
    );
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| BTreeError::Corrupt("snapshot payload too large".to_string()))?;

    let expected_total = SNAPSHOT_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| BTreeError::Corrupt("snapshot length overflow".to_string()))?;
    if raw.len() != expected_total {
        return Err(BTreeError::Corrupt(format!(
            "snapshot length mismatch: expected {}, found {}",
            expected_total,
            raw.len()
        )));
    }

    let payload = &raw[SNAPSHOT_HEADER_BYTES..];
    if payload.len() < 4 {
        return Err(BTreeError::Corrupt(
            "snapshot payload missing entry count".to_string(),
        ));
    }

    let entry_count =
        u32::from_le_bytes(payload[..4].try_into().expect("snapshot entry count slice")) as usize;

    let mut cursor = 4usize;
    let mut out = BTreeMap::new();
    for _ in 0..entry_count {
        let lens_end = cursor
            .checked_add(8)
            .ok_or_else(|| BTreeError::Corrupt("snapshot cursor overflow".to_string()))?;
        if lens_end > payload.len() {
            return Err(BTreeError::Corrupt(
                "snapshot truncated while reading key/value lengths".to_string(),
            ));
        }

        let key_len = u32::from_le_bytes(
            payload[cursor..cursor + 4]
                .try_into()
                .expect("snapshot key len slice"),
        ) as usize;
        let value_len = u32::from_le_bytes(
            payload[cursor + 4..cursor + 8]
                .try_into()
                .expect("snapshot value len slice"),
        ) as usize;
        cursor = lens_end;

        let key_end = cursor
            .checked_add(key_len)
            .ok_or_else(|| BTreeError::Corrupt("snapshot key overflow".to_string()))?;
        let value_end = key_end
            .checked_add(value_len)
            .ok_or_else(|| BTreeError::Corrupt("snapshot value overflow".to_string()))?;
        if value_end > payload.len() {
            return Err(BTreeError::Corrupt(
                "snapshot truncated while reading key/value bytes".to_string(),
            ));
        }

        let key = payload[cursor..key_end].to_vec();
        let value = payload[key_end..value_end].to_vec();
        out.insert(key, value);
        cursor = value_end;
    }

    if cursor != payload.len() {
        return Err(BTreeError::Corrupt(
            "snapshot trailing bytes after entries".to_string(),
        ));
    }

    Ok(out)
}

fn choose_active(
    a: Option<Superblock>,
    b: Option<Superblock>,
) -> Option<(SuperblockSlot, Superblock)> {
    match (a, b) {
        (Some(a), Some(b)) => {
            if b.generation > a.generation {
                Some((SuperblockSlot::B, b))
            } else {
                Some((SuperblockSlot::A, a))
            }
        }
        (Some(a), None) => Some((SuperblockSlot::A, a)),
        (None, Some(b)) => Some((SuperblockSlot::B, b)),
        (None, None) => None,
    }
}

fn slot_char(slot: SuperblockSlot) -> char {
    match slot {
        SuperblockSlot::A => 'A',
        SuperblockSlot::B => 'B',
    }
}

fn read_slot<F: SyncFile>(
    file: &F,
    slot: SuperblockSlot,
    page_size: usize,
) -> Result<Option<Superblock>, BTreeError> {
    let offset = slot.byte_offset(page_size);
    let needed = offset.saturating_add(page_size as u64);
    if file.len()? < needed {
        return Ok(None);
    }

    let mut page = vec![0u8; page_size];
    file.read_exact_at(offset, &mut page)?;
    if page.iter().all(|b| *b == 0) {
        return Ok(None);
    }

    match Superblock::decode_from_page(&page, page_size) {
        Ok(sb) => Ok(Some(sb)),
        Err(_) => Ok(None),
    }
}

fn write_slot<F: SyncFile>(
    file: &F,
    slot: SuperblockSlot,
    page_size: usize,
    sb: Superblock,
) -> Result<(), BTreeError> {
    let required_file_len = (2 * page_size) as u64;
    if file.len()? < required_file_len {
        file.truncate(required_file_len)?;
    }

    let mut page = vec![0u8; page_size];
    sb.encode_into_page(&mut page)?;
    file.write_all_at(slot.byte_offset(page_size), &page)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::{MemoryFile, SyncFile};

    fn corrupt_slot(file: &MemoryFile, slot: SuperblockSlot, page_size: usize) {
        let offset = slot.byte_offset(page_size) + 8;
        file.write_all_at(offset, &[0xFF, 0x00, 0xAA, 0x55])
            .expect("corrupt slot bytes");
    }

    #[test]
    fn bootstrap_creates_checkpoint_state() {
        let file = MemoryFile::new();
        let tree = OpfsBTree::open(file, BTreeOptions::default()).expect("open tree");
        let state = tree.checkpoint_state();
        assert_eq!(state.generation, 1);
        assert_eq!(state.total_pages, 2);
    }

    #[test]
    fn checkpoint_swaps_slots_and_increments_generation() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, BTreeOptions::default()).expect("open tree");

        let before = tree.checkpoint_state();
        tree.checkpoint_superblock(7, 11, 42)
            .expect("first checkpoint");
        let after = tree.checkpoint_state();

        assert_eq!(after.generation, before.generation + 1);
        assert_ne!(after.active_slot, before.active_slot);
        assert_eq!(after.root_page_id, 7);
        assert_eq!(after.freelist_head_page_id, 11);
        assert_eq!(after.total_pages, 42);
    }

    #[test]
    fn reopen_picks_latest_valid_superblock() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file.clone(), BTreeOptions::default()).expect("open tree");
        tree.checkpoint_superblock(0, 0, 10).expect("checkpoint 1");
        tree.checkpoint_superblock(0, 0, 20).expect("checkpoint 2");

        let reopened = OpfsBTree::open(file, BTreeOptions::default()).expect("reopen tree");
        let state = reopened.checkpoint_state();
        assert_eq!(state.root_page_id, 0);
        assert_eq!(state.total_pages, 20);
        assert_eq!(state.generation, 3);
    }

    #[test]
    fn reopen_falls_back_when_latest_slot_corrupt() {
        let page_size = BTreeOptions::default().page_size;
        let file = MemoryFile::new();

        let mut tree = OpfsBTree::open(file.clone(), BTreeOptions::default()).expect("open tree");
        let initial = tree.checkpoint_state();
        tree.checkpoint_superblock(99, 0, 33)
            .expect("checkpoint to inactive slot");
        let latest = tree.checkpoint_state();
        assert!(latest.generation > initial.generation);

        let latest_slot = if latest.active_slot == 'A' {
            SuperblockSlot::A
        } else {
            SuperblockSlot::B
        };
        corrupt_slot(&file, latest_slot, page_size);

        let reopened = OpfsBTree::open(file, BTreeOptions::default()).expect("reopen tree");
        let state = reopened.checkpoint_state();
        assert_eq!(state.generation, initial.generation);
        assert_eq!(state.root_page_id, initial.root_page_id);
    }

    #[test]
    fn open_nonempty_without_valid_superblock_errors() {
        let file = MemoryFile::new();
        file.write_all_at(0, &[1, 2, 3, 4, 5, 6, 7, 8])
            .expect("seed invalid bytes");

        let err = OpfsBTree::open(file, BTreeOptions::default()).expect_err("must error");
        assert!(matches!(err, BTreeError::Corrupt(_)));
    }

    #[test]
    fn put_get_delete_and_range_work_in_memory() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, BTreeOptions::default()).expect("open tree");

        tree.put(b"a", b"1").expect("put a");
        tree.put(b"b", b"2").expect("put b");
        tree.put(b"c", b"3").expect("put c");
        tree.put(b"b", b"20").expect("update b");

        assert_eq!(tree.get(b"a").expect("get a"), Some(b"1".to_vec()));
        assert_eq!(tree.get(b"b").expect("get b"), Some(b"20".to_vec()));
        assert_eq!(tree.get(b"z").expect("get z"), None);

        let range = tree.range(b"a", b"d", 10).expect("range a..d");
        assert_eq!(
            range,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"20".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
            ]
        );

        tree.delete(b"b").expect("delete b");
        assert_eq!(tree.get(b"b").expect("get b after delete"), None);
    }

    #[test]
    fn checkpoint_persists_data_across_reopen() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file.clone(), BTreeOptions::default()).expect("open tree");

        tree.put(b"k1", b"value1").expect("put k1");
        tree.put(b"k2", b"value2").expect("put k2");
        tree.checkpoint().expect("checkpoint");

        let reopened = OpfsBTree::open(file, BTreeOptions::default()).expect("reopen tree");
        assert_eq!(
            reopened.get(b"k1").expect("get k1"),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            reopened.get(b"k2").expect("get k2"),
            Some(b"value2".to_vec())
        );

        let state = reopened.checkpoint_state();
        assert!(state.generation >= 2);
        assert!(state.root_page_id >= 2);
        assert!(state.total_pages > 2);
    }

    #[test]
    fn latest_checkpoint_wins_across_reopen() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file.clone(), BTreeOptions::default()).expect("open tree");

        tree.put(b"k", b"v1").expect("put v1");
        tree.checkpoint().expect("checkpoint v1");

        tree.put(b"k", b"v2").expect("put v2");
        tree.put(b"k2", b"v3").expect("put v3");
        tree.checkpoint().expect("checkpoint v2");

        let reopened = OpfsBTree::open(file, BTreeOptions::default()).expect("reopen tree");
        assert_eq!(reopened.get(b"k").expect("get k"), Some(b"v2".to_vec()));
        assert_eq!(reopened.get(b"k2").expect("get k2"), Some(b"v3".to_vec()));
    }

    #[test]
    fn latest_valid_superblock_recovers_after_second_crash_pattern() {
        let file = MemoryFile::new();

        {
            let mut tree =
                OpfsBTree::open(file.clone(), BTreeOptions::default()).expect("open tree");
            tree.put(b"persist", b"v1").expect("put persist");
            tree.checkpoint().expect("checkpoint persist");
        }

        {
            let mut recovered = OpfsBTree::open(file.clone(), BTreeOptions::default())
                .expect("reopen after first crash");
            assert_eq!(
                recovered.get(b"persist").expect("get persisted"),
                Some(b"v1".to_vec())
            );
            recovered.put(b"ephemeral", b"temp").expect("put ephemeral");
            // Simulate crash before checkpoint: drop without checkpoint.
        }

        let reopened =
            OpfsBTree::open(file, BTreeOptions::default()).expect("reopen after second crash");
        assert_eq!(
            reopened
                .get(b"persist")
                .expect("get persisted after second crash"),
            Some(b"v1".to_vec())
        );
        assert_eq!(reopened.get(b"ephemeral").expect("get ephemeral"), None);
    }
}
