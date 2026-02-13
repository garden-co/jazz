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
                self.read_sst_records_into(meta, &mut sst_records)?;
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
        let mut bytes = Vec::with_capacity(
            records
                .iter()
                .map(|r| r.key.len() + r.value.len() + 32)
                .sum::<usize>(),
        );
        for record in &records {
            encode_record_into(record, &mut bytes);
        }

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
        let data = self.fs.read_all(&meta.path)?;
        out.clear();
        decode_records_into(&data, &meta.path, false, out)?;
        for record in out.iter() {
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
                self.read_sst_records_into(meta, &mut sst_records)?;
                for record in &sst_records {
                    if record.key == key {
                        versions.push(record.clone());
                    }
                }
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
            let mut bytes = Vec::with_capacity(
                output_records
                    .iter()
                    .map(|r| r.key.len() + r.value.len() + 32)
                    .sum::<usize>(),
            );
            for record in &output_records {
                encode_record_into(record, &mut bytes);
            }

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
