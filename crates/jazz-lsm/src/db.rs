use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::error::LsmError;
use crate::format::{OpKind, VersionedRecord, decode_records_into, encode_record_into};
use crate::fs::{FsError, SyncFs};
use crate::manifest::{Manifest, SstMeta};

const MANIFEST_FILE: &str = "MANIFEST.json";
const WAL_FILE: &str = "active.wal";
const SST_PREFIX: &str = "sst-";
const WAL_APPEND_BATCH_BYTES: usize = 32 * 1024;
const SST_V2_BLOCK_TARGET_BYTES: usize = 16 * 1024;
const SST_V2_FOOTER_MAGIC: [u8; 8] = *b"JLSM2IDX";
const SST_V2_VERSION: u32 = 1;
const SST_V2_FOOTER_SIZE: usize = 8 + 4 + 4 + 8 + 8;

#[derive(Debug, Clone)]
struct SstV2BlockIndex {
    offset: u64,
    len: u32,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
}

pub type MergeFn = Box<dyn Fn(Option<&[u8]>, &[u8]) -> Vec<u8> + 'static>;

pub struct MergeOperator {
    pub id: u32,
    pub name: String,
    pub apply: MergeFn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyPrefixMode {
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueCompression {
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteDurability {
    Buffered,
    SyncEveryWrite,
}

#[derive(Debug, Clone)]
pub struct LsmOptions {
    pub max_memtable_bytes: usize,
    pub max_wal_bytes: u64,
    pub level0_file_limit: usize,
    pub level_fanout: usize,
    pub max_levels: usize,
    pub write_durability: WriteDurability,
    pub key_prefix_mode: KeyPrefixMode,
    pub value_compression: ValueCompression,
}

impl Default for LsmOptions {
    fn default() -> Self {
        Self {
            max_memtable_bytes: 4 * 1024 * 1024,
            max_wal_bytes: 64 * 1024 * 1024,
            level0_file_limit: 4,
            level_fanout: 4,
            max_levels: 4,
            write_durability: WriteDurability::Buffered,
            key_prefix_mode: KeyPrefixMode::Disabled,
            value_compression: ValueCompression::None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct DebugState {
    pub wal_bytes: u64,
    pub level_file_counts: Vec<usize>,
    pub deepest_tombstones: usize,
}

pub struct LsmTree<F: SyncFs> {
    fs: F,
    options: LsmOptions,
    manifest: Manifest,
    merge_ops: HashMap<u32, MergeFn>,
    required_merge_ops: BTreeSet<u32>,
    memtable: BTreeMap<Vec<u8>, Vec<VersionedRecord>>,
    memtable_bytes: usize,
    wal_bytes: Cell<u64>,
    wal_buffer: RefCell<Vec<u8>>,
}

impl<F: SyncFs> LsmTree<F> {
    pub fn open(
        fs: F,
        options: LsmOptions,
        merge_ops: Vec<MergeOperator>,
    ) -> Result<Self, LsmError> {
        validate_options(&options)?;

        let mut merge_map = HashMap::new();
        for op in merge_ops {
            if merge_map.insert(op.id, op.apply).is_some() {
                return Err(LsmError::InvalidOptions(format!(
                    "duplicate merge operator id {}",
                    op.id
                )));
            }
        }

        let mut manifest = load_manifest(&fs, options.max_levels)?;
        if manifest.levels.len() < options.max_levels {
            manifest.levels.resize_with(options.max_levels, Vec::new);
        }

        let mut required_merge_ops: BTreeSet<u32> =
            manifest.required_merge_ops.iter().copied().collect();
        for op_id in &required_merge_ops {
            if !merge_map.contains_key(op_id) {
                return Err(LsmError::UnknownMergeOperator(*op_id));
            }
        }

        let mut tree = Self {
            fs,
            options,
            manifest,
            merge_ops: merge_map,
            required_merge_ops: std::mem::take(&mut required_merge_ops),
            memtable: BTreeMap::new(),
            memtable_bytes: 0,
            wal_bytes: Cell::new(0),
            wal_buffer: RefCell::new(Vec::with_capacity(WAL_APPEND_BATCH_BYTES)),
        };

        tree.replay_wal()?;

        // Keep replay bounded over time by checkpointing if WAL/memtable grew too much.
        if tree.wal_bytes.get() > tree.options.max_wal_bytes
            || tree.memtable_bytes > tree.options.max_memtable_bytes
        {
            tree.flush()?;
        }

        Ok(tree)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), LsmError> {
        let seq = self.next_seq();
        let record = VersionedRecord::put(key.to_vec(), seq, value.to_vec());
        self.append_wal(&record)?;
        self.apply_to_memtable(record);
        self.after_write()
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), LsmError> {
        let seq = self.next_seq();
        let record = VersionedRecord::delete(key.to_vec(), seq);
        self.append_wal(&record)?;
        self.apply_to_memtable(record);
        self.after_write()
    }

    pub fn merge(&mut self, key: &[u8], merge_op_id: u32, operand: &[u8]) -> Result<(), LsmError> {
        if !self.merge_ops.contains_key(&merge_op_id) {
            return Err(LsmError::UnknownMergeOperator(merge_op_id));
        }

        self.required_merge_ops.insert(merge_op_id);

        let seq = self.next_seq();
        let record = VersionedRecord::merge(key.to_vec(), seq, merge_op_id, operand.to_vec());
        self.append_wal(&record)?;
        self.apply_to_memtable(record);
        self.after_write()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, LsmError> {
        let versions = self.collect_versions_for_key(key)?;
        self.resolve_versions(&versions)
    }

    pub fn scan_range(
        &self,
        start_inclusive: Option<&[u8]>,
        end_exclusive: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, LsmError> {
        let mut keys = BTreeSet::new();
        let mut sst_records = Vec::new();

        for key in self.memtable.keys() {
            if key_in_range(key, start_inclusive, end_exclusive) {
                keys.insert(key.clone());
            }
        }

        for level in &self.manifest.levels {
            for meta in level {
                if !meta_overlaps_range(meta, start_inclusive, end_exclusive) {
                    continue;
                }
                self.read_sst_records_for_range_into(
                    meta,
                    start_inclusive,
                    end_exclusive,
                    &mut sst_records,
                )?;
                for record in &sst_records {
                    if key_in_range(&record.key, start_inclusive, end_exclusive) {
                        keys.insert(record.key.clone());
                    }
                }
            }
        }

        let mut out = Vec::new();
        for key in keys {
            if let Some(value) = self.get(&key)? {
                out.push((key, value));
            }
        }

        Ok(out)
    }

    pub fn flush_wal(&self) -> Result<(), LsmError> {
        self.flush_wal_buffer()?;
        self.fs.sync_file(WAL_FILE)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), LsmError> {
        self.flush_memtable_to_sst()?;
        self.fs.sync_dir()?;
        Ok(())
    }

    pub fn compact_step(&mut self) -> Result<bool, LsmError> {
        self.compact_step_internal()
    }

    pub fn debug_state(&self) -> Result<DebugState, LsmError> {
        let wal_bytes = self.wal_bytes.get();
        let level_file_counts = self
            .manifest
            .levels
            .iter()
            .map(std::vec::Vec::len)
            .collect::<Vec<_>>();

        let deepest = self.manifest.levels.len().saturating_sub(1);
        let mut deepest_tombstones = 0usize;
        let mut sst_records = Vec::new();
        if let Some(level) = self.manifest.levels.get(deepest) {
            for meta in level {
                self.read_sst_records_into(meta, &mut sst_records)?;
                deepest_tombstones += sst_records
                    .iter()
                    .filter(|r| r.kind == OpKind::Delete)
                    .count();
            }
        }

        Ok(DebugState {
            wal_bytes,
            level_file_counts,
            deepest_tombstones,
        })
    }

    fn after_write(&mut self) -> Result<(), LsmError> {
        if self.options.write_durability == WriteDurability::SyncEveryWrite {
            self.flush_wal()?;
        }

        if self.memtable_bytes >= self.options.max_memtable_bytes
            || self.wal_bytes.get() >= self.options.max_wal_bytes
        {
            self.flush()?;
        }

        Ok(())
    }

    fn next_seq(&mut self) -> u64 {
        let seq = self.manifest.next_seq;
        self.manifest.next_seq += 1;
        seq
    }

    fn append_wal(&self, record: &VersionedRecord) -> Result<(), LsmError> {
        let should_flush = {
            let mut buffer = self.wal_buffer.borrow_mut();
            let before_len = buffer.len();
            encode_record_into(record, &mut buffer);
            let appended = (buffer.len() - before_len) as u64;
            self.wal_bytes
                .set(self.wal_bytes.get().saturating_add(appended));
            buffer.len() >= WAL_APPEND_BATCH_BYTES
        };
        if should_flush {
            self.flush_wal_buffer()?;
        }
        Ok(())
    }

    fn flush_wal_buffer(&self) -> Result<(), LsmError> {
        let mut buffer = self.wal_buffer.borrow_mut();
        if buffer.is_empty() {
            return Ok(());
        }

        self.fs.append(WAL_FILE, &buffer)?;
        buffer.clear();
        Ok(())
    }

    fn apply_to_memtable(&mut self, record: VersionedRecord) {
        self.memtable_bytes += record.key.len() + record.value.len() + 24;
        self.memtable
            .entry(record.key.clone())
            .or_default()
            .push(record);
    }

    fn replay_wal(&mut self) -> Result<(), LsmError> {
        let data = match self.fs.read_all(WAL_FILE) {
            Ok(data) => data,
            Err(FsError::NotFound(_)) => {
                self.wal_bytes.set(0);
                return Ok(());
            }
            Err(e) => return Err(LsmError::Fs(e)),
        };
        self.wal_bytes.set(data.len() as u64);
        self.wal_buffer.borrow_mut().clear();

        let mut records = Vec::new();
        decode_records_into(&data, WAL_FILE, true, &mut records)?;
        for record in records {
            if record.kind == OpKind::Merge && !self.merge_ops.contains_key(&record.merge_op_id) {
                return Err(LsmError::UnknownMergeOperator(record.merge_op_id));
            }
            if record.kind == OpKind::Merge {
                self.required_merge_ops.insert(record.merge_op_id);
            }
            self.manifest.next_seq = self.manifest.next_seq.max(record.seq + 1);
            self.apply_to_memtable(record);
        }

        Ok(())
    }

    fn flush_memtable_to_sst(&mut self) -> Result<(), LsmError> {
        // Make sure any buffered WAL bytes are persisted before checkpoint/truncate.
        self.flush_wal_buffer()?;

        if self.memtable.is_empty() {
            return Ok(());
        }

        let mut records = Vec::new();
        for ops in self.memtable.values_mut() {
            ops.sort_by(|a, b| b.seq.cmp(&a.seq));
            records.extend(ops.iter().cloned());
        }
        records.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| b.seq.cmp(&a.seq)));

        let file_id = self.manifest.next_file_id;
        self.manifest.next_file_id += 1;

        let path = sst_path(file_id);
        let bytes = encode_sst_v2(&records)?;

        self.fs.write_all(&path, &bytes)?;
        self.fs.sync_file(&path)?;

        let min_key = records.first().map(|r| r.key.clone()).unwrap_or_default();
        let max_key = records.last().map(|r| r.key.clone()).unwrap_or_default();

        let meta = SstMeta {
            id: file_id,
            level: 0,
            path: path.clone(),
            min_key,
            max_key,
            bytes: bytes.len() as u64,
            records: records.len() as u64,
        };

        self.manifest.levels[0].push(meta);

        self.memtable.clear();
        self.memtable_bytes = 0;

        self.persist_manifest()?;

        // WAL can be reset after manifest references the new SST.
        self.fs.truncate(WAL_FILE, 0)?;
        self.fs.sync_file(WAL_FILE)?;
        self.wal_bytes.set(0);
        self.wal_buffer.borrow_mut().clear();

        let _ = self.compact_step_internal()?;
        Ok(())
    }

    fn persist_manifest(&mut self) -> Result<(), LsmError> {
        self.manifest.required_merge_ops = self.required_merge_ops.iter().copied().collect();
        let bytes = serde_json::to_vec(&self.manifest)
            .map_err(|e| LsmError::ManifestParse(e.to_string()))?;
        self.fs.write_atomic(MANIFEST_FILE, &bytes)?;
        self.fs.sync_file(MANIFEST_FILE)?;
        self.fs.sync_dir()?;
        Ok(())
    }

    fn read_sst_records_into(
        &self,
        meta: &SstMeta,
        out: &mut Vec<VersionedRecord>,
    ) -> Result<(), LsmError> {
        out.clear();
        let index = self.read_sst_v2_index(meta)?;
        let mut block_records = Vec::new();
        for block in &index {
            self.read_sst_v2_block_records(meta, block, &mut block_records)?;
            out.append(&mut block_records);
        }
        Ok(())
    }

    fn read_sst_records_for_key_into(
        &self,
        meta: &SstMeta,
        key: &[u8],
        out: &mut Vec<VersionedRecord>,
    ) -> Result<(), LsmError> {
        out.clear();
        let index = self.read_sst_v2_index(meta)?;
        let mut block_records = Vec::new();
        for block in &index {
            if key < block.min_key.as_slice() || key > block.max_key.as_slice() {
                continue;
            }
            self.read_sst_v2_block_records(meta, block, &mut block_records)?;
            for record in block_records.drain(..) {
                if record.key == key {
                    out.push(record);
                }
            }
        }
        Ok(())
    }

    fn read_sst_records_for_range_into(
        &self,
        meta: &SstMeta,
        start_inclusive: Option<&[u8]>,
        end_exclusive: Option<&[u8]>,
        out: &mut Vec<VersionedRecord>,
    ) -> Result<(), LsmError> {
        out.clear();
        let index = self.read_sst_v2_index(meta)?;
        let mut block_records = Vec::new();
        for block in &index {
            if let Some(start) = start_inclusive
                && block.max_key.as_slice() < start
            {
                continue;
            }
            if let Some(end) = end_exclusive
                && block.min_key.as_slice() >= end
            {
                continue;
            }
            self.read_sst_v2_block_records(meta, block, &mut block_records)?;
            for record in block_records.drain(..) {
                if key_in_range(&record.key, start_inclusive, end_exclusive) {
                    out.push(record);
                }
            }
        }
        Ok(())
    }

    fn read_sst_v2_index(&self, meta: &SstMeta) -> Result<Vec<SstV2BlockIndex>, LsmError> {
        let file_len = self.fs.file_len(&meta.path)?;
        if file_len < SST_V2_FOOTER_SIZE as u64 {
            return Err(corrupt_sst(&meta.path, 0));
        }

        let footer_offset = file_len - SST_V2_FOOTER_SIZE as u64;
        let footer = self
            .fs
            .read_range(&meta.path, footer_offset, SST_V2_FOOTER_SIZE)?;
        if footer.len() != SST_V2_FOOTER_SIZE {
            return Err(corrupt_sst(&meta.path, footer_offset));
        }

        if footer[0..8] != SST_V2_FOOTER_MAGIC {
            return Err(corrupt_sst(&meta.path, footer_offset));
        }

        let version = u32::from_le_bytes(footer[8..12].try_into().expect("footer version bytes"));
        if version != SST_V2_VERSION {
            return Err(corrupt_sst(&meta.path, footer_offset));
        }

        let block_count =
            u32::from_le_bytes(footer[12..16].try_into().expect("footer block count bytes"));
        let index_offset = u64::from_le_bytes(
            footer[16..24]
                .try_into()
                .expect("footer index offset bytes"),
        );
        let index_len =
            u64::from_le_bytes(footer[24..32].try_into().expect("footer index len bytes"));

        if index_offset >= footer_offset
            || index_len == 0
            || index_offset.saturating_add(index_len) != footer_offset
        {
            return Err(corrupt_sst(&meta.path, footer_offset));
        }

        if index_len > usize::MAX as u64 {
            return Err(corrupt_sst(&meta.path, index_offset));
        }

        let index_bytes = self
            .fs
            .read_range(&meta.path, index_offset, index_len as usize)?;
        if index_bytes.len() != index_len as usize {
            return Err(corrupt_sst(&meta.path, index_offset));
        }

        let blocks = parse_sst_v2_index(&index_bytes, &meta.path, index_offset, block_count)?;
        let mut prev_end = 0u64;
        for block in &blocks {
            if block.offset < prev_end {
                return Err(corrupt_sst(&meta.path, block.offset));
            }
            let end = block.offset.saturating_add(block.len as u64);
            if end > index_offset {
                return Err(corrupt_sst(&meta.path, block.offset));
            }
            prev_end = end;
        }
        Ok(blocks)
    }

    fn read_sst_v2_block_records(
        &self,
        meta: &SstMeta,
        block: &SstV2BlockIndex,
        out: &mut Vec<VersionedRecord>,
    ) -> Result<(), LsmError> {
        let data = self
            .fs
            .read_range(&meta.path, block.offset, block.len as usize)?;
        if data.len() != block.len as usize {
            return Err(corrupt_sst(&meta.path, block.offset));
        }
        decode_records_into(&data, &meta.path, false, out)?;
        self.validate_merge_ops(out)
    }

    fn validate_merge_ops(&self, records: &[VersionedRecord]) -> Result<(), LsmError> {
        for record in records {
            if record.kind == OpKind::Merge && !self.merge_ops.contains_key(&record.merge_op_id) {
                return Err(LsmError::UnknownMergeOperator(record.merge_op_id));
            }
        }
        Ok(())
    }

    fn collect_versions_for_key(&self, key: &[u8]) -> Result<Vec<VersionedRecord>, LsmError> {
        let mut versions = Vec::new();
        let mut sst_records = Vec::new();

        if let Some(ops) = self.memtable.get(key) {
            versions.extend(ops.iter().cloned());
        }

        for level in &self.manifest.levels {
            for meta in level {
                if key < meta.min_key.as_slice() || key > meta.max_key.as_slice() {
                    continue;
                }
                self.read_sst_records_for_key_into(meta, key, &mut sst_records)?;
                versions.extend(sst_records.iter().cloned());
            }
        }

        versions.sort_by(|a, b| b.seq.cmp(&a.seq));
        Ok(versions)
    }

    fn resolve_versions(&self, versions: &[VersionedRecord]) -> Result<Option<Vec<u8>>, LsmError> {
        let mut pending_merges: Vec<(u32, Vec<u8>)> = Vec::new();

        for version in versions {
            match version.kind {
                OpKind::Merge => pending_merges.push((version.merge_op_id, version.value.clone())),
                OpKind::Put => {
                    let mut current = Some(version.value.clone());
                    for (merge_op_id, operand) in pending_merges.iter().rev() {
                        let merge = self
                            .merge_ops
                            .get(merge_op_id)
                            .ok_or(LsmError::UnknownMergeOperator(*merge_op_id))?;
                        current = Some((merge)(current.as_deref(), operand));
                    }
                    return Ok(current);
                }
                OpKind::Delete => {
                    // Delete dominates older history and any merges above it.
                    return Ok(None);
                }
            }
        }

        if pending_merges.is_empty() {
            return Ok(None);
        }

        let mut current: Option<Vec<u8>> = None;
        for (merge_op_id, operand) in pending_merges.iter().rev() {
            let merge = self
                .merge_ops
                .get(merge_op_id)
                .ok_or(LsmError::UnknownMergeOperator(*merge_op_id))?;
            current = Some((merge)(current.as_deref(), operand));
        }

        Ok(current)
    }

    fn compact_step_internal(&mut self) -> Result<bool, LsmError> {
        if self.manifest.levels.is_empty() {
            return Ok(false);
        }

        let deepest = self.manifest.levels.len() - 1;
        let mut selected_level = None;

        for i in 0..deepest {
            let limit = if i == 0 {
                self.options.level0_file_limit
            } else {
                self.options.level_fanout
            };
            if self.manifest.levels[i].len() >= limit {
                selected_level = Some(i);
                break;
            }
        }

        if selected_level.is_none()
            && self.manifest.levels[deepest].len() >= self.options.level_fanout
        {
            selected_level = Some(deepest);
        }

        let level = match selected_level {
            Some(level) => level,
            None => return Ok(false),
        };

        let output_level = if level == deepest { deepest } else { level + 1 };
        let drop_tombstones = level == deepest;

        let input_files = std::mem::take(&mut self.manifest.levels[level]);
        if input_files.is_empty() {
            return Ok(false);
        }

        let mut by_key: BTreeMap<Vec<u8>, Vec<VersionedRecord>> = BTreeMap::new();
        let mut sst_records = Vec::new();
        for meta in &input_files {
            self.read_sst_records_into(meta, &mut sst_records)?;
            for record in &sst_records {
                by_key
                    .entry(record.key.clone())
                    .or_default()
                    .push(record.clone());
            }
        }

        let mut output_records = Vec::new();
        for (key, mut records) in by_key {
            records.sort_by(|a, b| b.seq.cmp(&a.seq));

            if drop_tombstones {
                if let Some(value) = self.resolve_versions(&records)? {
                    let seq = records.first().map(|r| r.seq).unwrap_or(0);
                    output_records.push(VersionedRecord::put(key, seq, value));
                }
            } else {
                output_records.extend(records);
            }
        }

        output_records.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| b.seq.cmp(&a.seq)));

        for meta in input_files {
            self.fs.remove_file(&meta.path)?;
        }

        if !output_records.is_empty() {
            let file_id = self.manifest.next_file_id;
            self.manifest.next_file_id += 1;

            let path = sst_path(file_id);
            let bytes = encode_sst_v2(&output_records)?;

            self.fs.write_all(&path, &bytes)?;
            self.fs.sync_file(&path)?;

            let min_key = output_records
                .first()
                .map(|r| r.key.clone())
                .unwrap_or_default();
            let max_key = output_records
                .last()
                .map(|r| r.key.clone())
                .unwrap_or_default();

            let meta = SstMeta {
                id: file_id,
                level: output_level,
                path,
                min_key,
                max_key,
                bytes: bytes.len() as u64,
                records: output_records.len() as u64,
            };

            self.manifest.levels[output_level].push(meta);
        }

        self.persist_manifest()?;
        Ok(true)
    }
}

fn encode_sst_v2(records: &[VersionedRecord]) -> Result<Vec<u8>, LsmError> {
    let mut file_bytes = Vec::with_capacity(
        records
            .iter()
            .map(|r| r.key.len() + r.value.len() + 32)
            .sum::<usize>()
            .saturating_add(1024),
    );
    let mut blocks = Vec::new();
    let mut block_bytes = Vec::with_capacity(SST_V2_BLOCK_TARGET_BYTES + 256);
    let mut block_min_key: Option<Vec<u8>> = None;
    let mut block_max_key: Option<Vec<u8>> = None;

    for record in records {
        if block_min_key.is_none() {
            block_min_key = Some(record.key.clone());
        }
        block_max_key = Some(record.key.clone());
        encode_record_into(record, &mut block_bytes);

        if block_bytes.len() >= SST_V2_BLOCK_TARGET_BYTES {
            finish_sst_v2_block(
                &mut file_bytes,
                &mut blocks,
                &mut block_bytes,
                &mut block_min_key,
                &mut block_max_key,
            )?;
        }
    }

    finish_sst_v2_block(
        &mut file_bytes,
        &mut blocks,
        &mut block_bytes,
        &mut block_min_key,
        &mut block_max_key,
    )?;

    let index_offset = file_bytes.len() as u64;
    let index_bytes = encode_sst_v2_index(&blocks)?;
    let index_len = index_bytes.len() as u64;
    file_bytes.extend_from_slice(&index_bytes);

    let block_count = u32::try_from(blocks.len())
        .map_err(|_| LsmError::InvalidOptions("too many SST blocks".to_string()))?;
    file_bytes.extend_from_slice(&SST_V2_FOOTER_MAGIC);
    file_bytes.extend_from_slice(&SST_V2_VERSION.to_le_bytes());
    file_bytes.extend_from_slice(&block_count.to_le_bytes());
    file_bytes.extend_from_slice(&index_offset.to_le_bytes());
    file_bytes.extend_from_slice(&index_len.to_le_bytes());

    Ok(file_bytes)
}

fn finish_sst_v2_block(
    file_bytes: &mut Vec<u8>,
    blocks: &mut Vec<SstV2BlockIndex>,
    block_bytes: &mut Vec<u8>,
    block_min_key: &mut Option<Vec<u8>>,
    block_max_key: &mut Option<Vec<u8>>,
) -> Result<(), LsmError> {
    if block_bytes.is_empty() {
        return Ok(());
    }

    let len = u32::try_from(block_bytes.len())
        .map_err(|_| LsmError::InvalidOptions("SST block length overflow".to_string()))?;
    let min_key = block_min_key
        .take()
        .ok_or_else(|| LsmError::InvalidOptions("missing SST block min key".to_string()))?;
    let max_key = block_max_key
        .take()
        .ok_or_else(|| LsmError::InvalidOptions("missing SST block max key".to_string()))?;

    let offset = file_bytes.len() as u64;
    file_bytes.extend_from_slice(block_bytes);
    block_bytes.clear();

    blocks.push(SstV2BlockIndex {
        offset,
        len,
        min_key,
        max_key,
    });

    Ok(())
}

fn encode_sst_v2_index(blocks: &[SstV2BlockIndex]) -> Result<Vec<u8>, LsmError> {
    let mut out = Vec::with_capacity(
        4 + blocks
            .iter()
            .map(|b| 8 + 4 + 4 + 4 + b.min_key.len() + b.max_key.len())
            .sum::<usize>(),
    );

    let count = u32::try_from(blocks.len())
        .map_err(|_| LsmError::InvalidOptions("too many SST index entries".to_string()))?;
    out.extend_from_slice(&count.to_le_bytes());

    for block in blocks {
        let min_len = u32::try_from(block.min_key.len())
            .map_err(|_| LsmError::InvalidOptions("SST min key too large".to_string()))?;
        let max_len = u32::try_from(block.max_key.len())
            .map_err(|_| LsmError::InvalidOptions("SST max key too large".to_string()))?;
        out.extend_from_slice(&block.offset.to_le_bytes());
        out.extend_from_slice(&block.len.to_le_bytes());
        out.extend_from_slice(&min_len.to_le_bytes());
        out.extend_from_slice(&max_len.to_le_bytes());
        out.extend_from_slice(&block.min_key);
        out.extend_from_slice(&block.max_key);
    }

    Ok(out)
}

fn parse_sst_v2_index(
    data: &[u8],
    path: &str,
    index_offset: u64,
    expected_block_count: u32,
) -> Result<Vec<SstV2BlockIndex>, LsmError> {
    if data.len() < 4 {
        return Err(corrupt_sst(path, index_offset));
    }

    let mut cursor = 0usize;
    let count = u32::from_le_bytes(
        data[cursor..cursor + 4]
            .try_into()
            .expect("SST index count bytes"),
    );
    cursor += 4;
    if count != expected_block_count {
        return Err(corrupt_sst(path, index_offset));
    }

    let mut blocks = Vec::with_capacity(count as usize);
    for _ in 0..count {
        if cursor + 8 + 4 + 4 + 4 > data.len() {
            return Err(corrupt_sst(path, index_offset + cursor as u64));
        }

        let offset = u64::from_le_bytes(
            data[cursor..cursor + 8]
                .try_into()
                .expect("SST index block offset bytes"),
        );
        cursor += 8;
        let len = u32::from_le_bytes(
            data[cursor..cursor + 4]
                .try_into()
                .expect("SST index block len bytes"),
        );
        cursor += 4;
        let min_len = u32::from_le_bytes(
            data[cursor..cursor + 4]
                .try_into()
                .expect("SST index min key len bytes"),
        ) as usize;
        cursor += 4;
        let max_len = u32::from_le_bytes(
            data[cursor..cursor + 4]
                .try_into()
                .expect("SST index max key len bytes"),
        ) as usize;
        cursor += 4;

        if len == 0 {
            return Err(corrupt_sst(path, index_offset + cursor as u64));
        }
        if cursor + min_len + max_len > data.len() {
            return Err(corrupt_sst(path, index_offset + cursor as u64));
        }

        let min_key = data[cursor..cursor + min_len].to_vec();
        cursor += min_len;
        let max_key = data[cursor..cursor + max_len].to_vec();
        cursor += max_len;

        if min_key > max_key {
            return Err(corrupt_sst(path, index_offset + cursor as u64));
        }

        blocks.push(SstV2BlockIndex {
            offset,
            len,
            min_key,
            max_key,
        });
    }

    if cursor != data.len() {
        return Err(corrupt_sst(path, index_offset + cursor as u64));
    }

    Ok(blocks)
}

fn corrupt_sst(path: &str, offset: u64) -> LsmError {
    LsmError::CorruptRecord {
        path: path.to_string(),
        offset,
    }
}

fn load_manifest<F: SyncFs>(fs: &F, num_levels: usize) -> Result<Manifest, LsmError> {
    match fs.read_all(MANIFEST_FILE) {
        Ok(bytes) => {
            let mut manifest: Manifest = serde_json::from_slice(&bytes)
                .map_err(|e| LsmError::ManifestParse(e.to_string()))?;
            if manifest.levels.len() < num_levels {
                manifest.levels.resize_with(num_levels, Vec::new);
            }
            Ok(manifest)
        }
        Err(FsError::NotFound(_)) => Ok(Manifest::new(num_levels)),
        Err(e) => Err(LsmError::Fs(e)),
    }
}

fn validate_options(options: &LsmOptions) -> Result<(), LsmError> {
    if options.max_levels == 0 {
        return Err(LsmError::InvalidOptions(
            "max_levels must be >= 1".to_string(),
        ));
    }
    if options.level0_file_limit == 0 {
        return Err(LsmError::InvalidOptions(
            "level0_file_limit must be >= 1".to_string(),
        ));
    }
    if options.level_fanout == 0 {
        return Err(LsmError::InvalidOptions(
            "level_fanout must be >= 1".to_string(),
        ));
    }
    if options.max_memtable_bytes == 0 {
        return Err(LsmError::InvalidOptions(
            "max_memtable_bytes must be >= 1".to_string(),
        ));
    }
    if options.max_wal_bytes == 0 {
        return Err(LsmError::InvalidOptions(
            "max_wal_bytes must be >= 1".to_string(),
        ));
    }
    Ok(())
}

fn key_in_range(key: &[u8], start_inclusive: Option<&[u8]>, end_exclusive: Option<&[u8]>) -> bool {
    if let Some(start) = start_inclusive
        && key < start
    {
        return false;
    }
    if let Some(end) = end_exclusive
        && key >= end
    {
        return false;
    }
    true
}

fn meta_overlaps_range(
    meta: &SstMeta,
    start_inclusive: Option<&[u8]>,
    end_exclusive: Option<&[u8]>,
) -> bool {
    if let Some(start) = start_inclusive
        && meta.max_key.as_slice() < start
    {
        return false;
    }
    if let Some(end) = end_exclusive
        && meta.min_key.as_slice() >= end
    {
        return false;
    }
    true
}

fn sst_path(file_id: u64) -> String {
    format!("{}{:020}.sst", SST_PREFIX, file_id)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::*;
    use crate::fs::MemoryFs;

    #[derive(Clone, Default)]
    struct TrackingFs {
        inner: MemoryFs,
        read_all_calls: Rc<Cell<u64>>,
        read_range_calls: Rc<Cell<u64>>,
    }

    impl TrackingFs {
        fn reset_read_counters(&self) {
            self.read_all_calls.set(0);
            self.read_range_calls.set(0);
        }

        fn read_counts(&self) -> (u64, u64) {
            (self.read_all_calls.get(), self.read_range_calls.get())
        }
    }

    impl SyncFs for TrackingFs {
        fn read_all(&self, path: &str) -> Result<Vec<u8>, FsError> {
            self.read_all_calls.set(self.read_all_calls.get() + 1);
            self.inner.read_all(path)
        }

        fn read_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, FsError> {
            self.read_range_calls.set(self.read_range_calls.get() + 1);
            self.inner.read_range(path, offset, len)
        }

        fn write_all(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
            self.inner.write_all(path, data)
        }

        fn write_atomic(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
            self.inner.write_atomic(path, data)
        }

        fn append(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
            self.inner.append(path, data)
        }

        fn file_len(&self, path: &str) -> Result<u64, FsError> {
            self.inner.file_len(path)
        }

        fn truncate(&self, path: &str, len: u64) -> Result<(), FsError> {
            self.inner.truncate(path, len)
        }

        fn remove_file(&self, path: &str) -> Result<(), FsError> {
            self.inner.remove_file(path)
        }

        fn list_files(&self, prefix: &str) -> Result<Vec<String>, FsError> {
            self.inner.list_files(prefix)
        }

        fn sync_file(&self, path: &str) -> Result<(), FsError> {
            self.inner.sync_file(path)
        }

        fn sync_dir(&self) -> Result<(), FsError> {
            self.inner.sync_dir()
        }
    }

    #[test]
    fn point_reads_use_positional_sst_blocks() {
        let fs = TrackingFs::default();
        let mut db = LsmTree::open(
            fs.clone(),
            LsmOptions {
                max_memtable_bytes: 8 * 1024 * 1024,
                max_wal_bytes: 64 * 1024 * 1024,
                level0_file_limit: 8,
                level_fanout: 4,
                max_levels: 2,
                ..Default::default()
            },
            vec![],
        )
        .expect("open");

        for i in 0..2000 {
            let key = format!("k{i:06}").into_bytes();
            let value = format!("value-{i:06}-{}", "x".repeat(64)).into_bytes();
            db.put(&key, &value).expect("put");
        }
        db.flush().expect("flush");

        fs.reset_read_counters();

        let target_key = b"k001337".to_vec();
        let expected_value = format!("value-001337-{}", "x".repeat(64)).into_bytes();
        let got = db.get(&target_key).expect("get");
        assert_eq!(got, Some(expected_value));

        let (read_all_calls, read_range_calls) = fs.read_counts();
        assert_eq!(
            read_all_calls, 0,
            "point lookups should avoid full-file SST reads"
        );
        assert!(
            read_range_calls >= 3,
            "expected footer/index/block reads via read_range"
        );
    }
}
