#![cfg_attr(not(any(target_arch = "wasm32", test)), allow(dead_code))]

use std::cell::RefCell;
use std::collections::BTreeMap;
#[cfg(not(target_arch = "wasm32"))]
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use thiserror::Error;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::JsCast;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::JsFuture;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum FsError {
    #[error("file not found: {0}")]
    NotFound(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("unsupported operation: {0}")]
    Unsupported(String),
}

pub trait SyncFs: Clone {
    fn read_all(&self, path: &str) -> Result<Vec<u8>, FsError>;
    fn read_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, FsError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let data = self.read_all(path)?;
        if offset > usize::MAX as u64 {
            return Ok(Vec::new());
        }
        let start = offset as usize;
        if start >= data.len() {
            return Ok(Vec::new());
        }
        let end = start.saturating_add(len).min(data.len());
        Ok(data[start..end].to_vec())
    }
    fn write_all(&self, path: &str, data: &[u8]) -> Result<(), FsError>;
    fn write_atomic(&self, path: &str, data: &[u8]) -> Result<(), FsError>;
    fn append(&self, path: &str, data: &[u8]) -> Result<(), FsError>;
    fn file_len(&self, path: &str) -> Result<u64, FsError>;
    fn truncate(&self, path: &str, len: u64) -> Result<(), FsError>;
    fn remove_file(&self, path: &str) -> Result<(), FsError>;
    fn list_files(&self, prefix: &str) -> Result<Vec<String>, FsError>;
    fn sync_file(&self, path: &str) -> Result<(), FsError>;
    fn sync_dir(&self) -> Result<(), FsError>;
}

#[derive(Clone, Default)]
pub struct MemoryFs {
    files: Rc<RefCell<BTreeMap<String, Vec<u8>>>>,
}

impl MemoryFs {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SyncFs for MemoryFs {
    fn read_all(&self, path: &str) -> Result<Vec<u8>, FsError> {
        self.files
            .borrow()
            .get(path)
            .cloned()
            .ok_or_else(|| FsError::NotFound(path.to_string()))
    }

    fn read_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, FsError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let files = self.files.borrow();
        let data = files
            .get(path)
            .ok_or_else(|| FsError::NotFound(path.to_string()))?;
        if offset > usize::MAX as u64 {
            return Ok(Vec::new());
        }
        let start = offset as usize;
        if start >= data.len() {
            return Ok(Vec::new());
        }
        let end = start.saturating_add(len).min(data.len());
        Ok(data[start..end].to_vec())
    }

    fn write_all(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
        self.files
            .borrow_mut()
            .insert(path.to_string(), data.to_vec());
        Ok(())
    }

    fn write_atomic(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
        self.write_all(path, data)
    }

    fn append(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
        let mut files = self.files.borrow_mut();
        let buf = files.entry(path.to_string()).or_default();
        buf.extend_from_slice(data);
        Ok(())
    }

    fn file_len(&self, path: &str) -> Result<u64, FsError> {
        match self.files.borrow().get(path) {
            Some(data) => Ok(data.len() as u64),
            None => Ok(0),
        }
    }

    fn truncate(&self, path: &str, len: u64) -> Result<(), FsError> {
        let mut files = self.files.borrow_mut();
        let buf = files.entry(path.to_string()).or_default();
        if (len as usize) >= buf.len() {
            buf.resize(len as usize, 0);
        } else {
            buf.truncate(len as usize);
        }
        Ok(())
    }

    fn remove_file(&self, path: &str) -> Result<(), FsError> {
        self.files.borrow_mut().remove(path);
        Ok(())
    }

    fn list_files(&self, prefix: &str) -> Result<Vec<String>, FsError> {
        let mut out: Vec<String> = self
            .files
            .borrow()
            .keys()
            .filter(|name| name.starts_with(prefix))
            .cloned()
            .collect();
        out.sort();
        Ok(out)
    }

    fn sync_file(&self, _path: &str) -> Result<(), FsError> {
        Ok(())
    }

    fn sync_dir(&self) -> Result<(), FsError> {
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
pub struct StdFs {
    root: std::path::PathBuf,
    read_handle_cache: Rc<RefCell<StdReadHandleCache>>,
}

#[cfg(not(target_arch = "wasm32"))]
const STD_FS_READ_HANDLE_CACHE_CAPACITY: usize = 64;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
struct StdReadHandleCache {
    capacity: usize,
    entries: HashMap<String, std::fs::File>,
    lru: VecDeque<String>,
}

#[cfg(not(target_arch = "wasm32"))]
impl StdReadHandleCache {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    fn get_or_open(
        &mut self,
        path: &str,
        full_path: &std::path::Path,
    ) -> Result<&mut std::fs::File, FsError> {
        if self.entries.contains_key(path) {
            self.touch(path);
            return self
                .entries
                .get_mut(path)
                .ok_or_else(|| FsError::Io(format!("read handle cache miss after touch: {path}")));
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(full_path)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    FsError::NotFound(path.to_string())
                } else {
                    FsError::Io(e.to_string())
                }
            })?;

        self.entries.insert(path.to_string(), file);
        self.touch(path);
        self.evict_if_needed();

        self.entries
            .get_mut(path)
            .ok_or_else(|| FsError::Io(format!("read handle cache insert failed: {path}")))
    }

    fn remove(&mut self, path: &str) {
        self.entries.remove(path);
        self.lru.retain(|cached| cached != path);
    }

    fn touch(&mut self, path: &str) {
        self.lru.retain(|cached| cached != path);
        self.lru.push_back(path.to_string());
    }

    fn evict_if_needed(&mut self) {
        while self.entries.len() > self.capacity {
            let Some(oldest) = self.lru.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl StdFs {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Result<Self, FsError> {
        Self::with_read_handle_cache_capacity(root, STD_FS_READ_HANDLE_CACHE_CAPACITY)
    }

    pub fn with_read_handle_cache_capacity(
        root: impl Into<std::path::PathBuf>,
        capacity: usize,
    ) -> Result<Self, FsError> {
        if capacity == 0 {
            return Err(FsError::Unsupported(
                "StdFs read handle cache capacity must be >= 1".to_string(),
            ));
        }
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(|e| FsError::Io(e.to_string()))?;
        Ok(Self {
            root,
            read_handle_cache: Rc::new(RefCell::new(StdReadHandleCache::with_capacity(capacity))),
        })
    }

    fn full_path(&self, relative: &str) -> std::path::PathBuf {
        self.root.join(relative)
    }

    fn invalidate_read_handle(&self, path: &str) {
        self.read_handle_cache.borrow_mut().remove(path);
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl SyncFs for StdFs {
    fn read_all(&self, path: &str) -> Result<Vec<u8>, FsError> {
        std::fs::read(self.full_path(path)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                FsError::NotFound(path.to_string())
            } else {
                FsError::Io(e.to_string())
            }
        })
    }

    fn read_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, FsError> {
        use std::io::{Read, Seek, SeekFrom};

        if len == 0 {
            return Ok(Vec::new());
        }

        let full_path = self.full_path(path);
        let mut cache = self.read_handle_cache.borrow_mut();
        let file = cache.get_or_open(path, &full_path)?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| FsError::Io(e.to_string()))?;

        let mut out = vec![0u8; len];
        let read = file
            .read(&mut out)
            .map_err(|e| FsError::Io(e.to_string()))?;
        out.truncate(read);
        Ok(out)
    }

    fn write_all(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
        self.invalidate_read_handle(path);
        std::fs::write(self.full_path(path), data).map_err(|e| FsError::Io(e.to_string()))
    }

    fn write_atomic(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
        let tmp_name = format!("{}.tmp", path);
        let tmp = self.full_path(&tmp_name);
        let dst = self.full_path(path);

        self.invalidate_read_handle(path);
        self.invalidate_read_handle(&tmp_name);
        std::fs::write(&tmp, data).map_err(|e| FsError::Io(e.to_string()))?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&tmp)
            .map_err(|e| FsError::Io(e.to_string()))?;
        file.sync_all().map_err(|e| FsError::Io(e.to_string()))?;

        std::fs::rename(&tmp, &dst).map_err(|e| FsError::Io(e.to_string()))?;
        self.sync_dir()?;
        Ok(())
    }

    fn append(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.full_path(path))
            .map_err(|e| FsError::Io(e.to_string()))?;
        file.write_all(data).map_err(|e| FsError::Io(e.to_string()))
    }

    fn file_len(&self, path: &str) -> Result<u64, FsError> {
        match std::fs::metadata(self.full_path(path)) {
            Ok(meta) => Ok(meta.len()),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(0)
                } else {
                    Err(FsError::Io(e.to_string()))
                }
            }
        }
    }

    fn truncate(&self, path: &str, len: u64) -> Result<(), FsError> {
        self.invalidate_read_handle(path);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(self.full_path(path))
            .map_err(|e| FsError::Io(e.to_string()))?;
        file.set_len(len).map_err(|e| FsError::Io(e.to_string()))
    }

    fn remove_file(&self, path: &str) -> Result<(), FsError> {
        self.invalidate_read_handle(path);
        match std::fs::remove_file(self.full_path(path)) {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(FsError::Io(e.to_string()))
                }
            }
        }
    }

    fn list_files(&self, prefix: &str) -> Result<Vec<String>, FsError> {
        let mut out = Vec::new();
        let entries = std::fs::read_dir(&self.root).map_err(|e| FsError::Io(e.to_string()))?;
        for entry in entries {
            let entry = entry.map_err(|e| FsError::Io(e.to_string()))?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(prefix) {
                out.push(name);
            }
        }
        out.sort();
        Ok(out)
    }

    fn sync_file(&self, path: &str) -> Result<(), FsError> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(self.full_path(path))
            .map_err(|e| FsError::Io(e.to_string()))?;
        file.sync_all().map_err(|e| FsError::Io(e.to_string()))
    }

    fn sync_dir(&self) -> Result<(), FsError> {
        let dir = std::fs::File::open(&self.root).map_err(|e| FsError::Io(e.to_string()))?;
        dir.sync_all().map_err(|e| FsError::Io(e.to_string()))
    }
}

// ============================================================================
// Single-container virtual filesystem mapping
// ============================================================================

const CONTAINER_MAGIC: [u8; 8] = *b"JLSMFS01";
const CONTAINER_VERSION: u32 = 1;
const META_SLOT_SIZE: usize = 256 * 1024;
const META_HEADER_SIZE: usize = 8 + 4 + 8 + 4 + 4;
const DATA_START: u64 = (META_SLOT_SIZE as u64) * 2;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
struct Extent {
    offset: u64,
    len: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FileMapEntry {
    size: u64,
    extents: Vec<Extent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedMeta {
    next_offset: u64,
    files: BTreeMap<String, FileMapEntry>,
    free: Vec<Extent>,
}

#[derive(Debug, Clone)]
struct ContainerMeta {
    generation: u64,
    next_offset: u64,
    files: BTreeMap<String, FileMapEntry>,
    free: Vec<Extent>,
}

impl Default for ContainerMeta {
    fn default() -> Self {
        Self {
            generation: 0,
            next_offset: DATA_START,
            files: BTreeMap::new(),
            free: Vec::new(),
        }
    }
}

impl From<ContainerMeta> for PersistedMeta {
    fn from(meta: ContainerMeta) -> Self {
        Self {
            next_offset: meta.next_offset,
            files: meta.files,
            free: meta.free,
        }
    }
}

impl PersistedMeta {
    fn into_runtime(self, generation: u64) -> ContainerMeta {
        ContainerMeta {
            generation,
            next_offset: self.next_offset,
            files: self.files,
            free: self.free,
        }
    }
}

trait ContainerIo {
    fn len(&self) -> Result<u64, FsError>;
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize, FsError>;
    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize, FsError>;
    fn truncate(&self, len: u64) -> Result<(), FsError>;
    fn flush(&self) -> Result<(), FsError>;
}

struct ContainerState<I: ContainerIo> {
    io: I,
    meta: ContainerMeta,
    active_slot: usize,
    dirty: bool,
}

struct ContainerFs<I: ContainerIo> {
    inner: Rc<RefCell<ContainerState<I>>>,
}

impl<I: ContainerIo> Clone for ContainerFs<I> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<I: ContainerIo> ContainerFs<I> {
    fn open(io: I) -> Result<Self, FsError> {
        let (mut meta, active_slot) = load_container_meta(&io)?;
        if meta.next_offset < DATA_START {
            meta.next_offset = DATA_START;
        }
        let state = ContainerState {
            io,
            meta,
            active_slot,
            dirty: false,
        };
        ensure_min_len(&state.io, DATA_START)?;
        state.validate_invariants()?;
        Ok(Self {
            inner: Rc::new(RefCell::new(state)),
        })
    }

    fn read_all_inner(&self, path: &str) -> Result<Vec<u8>, FsError> {
        let state = self.inner.borrow();
        let entry = state
            .meta
            .files
            .get(path)
            .ok_or_else(|| FsError::NotFound(path.to_string()))?;

        let mut out = vec![0u8; entry.size as usize];
        let mut cursor = 0usize;
        for extent in &entry.extents {
            let want = extent.len as usize;
            read_exact_at(&state.io, extent.offset, &mut out[cursor..cursor + want])?;
            cursor += want;
        }

        Ok(out)
    }

    fn read_range_inner(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, FsError> {
        if len == 0 {
            return Ok(Vec::new());
        }

        let state = self.inner.borrow();
        let entry = state
            .meta
            .files
            .get(path)
            .ok_or_else(|| FsError::NotFound(path.to_string()))?;

        if offset >= entry.size {
            return Ok(Vec::new());
        }

        let want = (entry.size - offset).min(len as u64) as usize;
        let mut out = vec![0u8; want];
        let mut out_cursor = 0usize;
        let mut logical_cursor = 0u64;
        let mut remaining = want as u64;

        for extent in &entry.extents {
            let extent_start = logical_cursor;
            let extent_end = logical_cursor + extent.len;
            logical_cursor = extent_end;

            if extent_end <= offset {
                continue;
            }
            if remaining == 0 {
                break;
            }

            let read_start_in_extent = offset.saturating_sub(extent_start);
            if read_start_in_extent >= extent.len {
                continue;
            }

            let available = extent.len - read_start_in_extent;
            let take = available.min(remaining);
            let start = out_cursor;
            let end = out_cursor + take as usize;
            read_exact_at(
                &state.io,
                extent.offset + read_start_in_extent,
                &mut out[start..end],
            )?;

            out_cursor = end;
            remaining -= take;
        }

        if out_cursor != out.len() {
            return Err(FsError::Io(format!(
                "short range read for {path} at offset {offset}: expected {} bytes, got {}",
                out.len(),
                out_cursor
            )));
        }

        Ok(out)
    }

    fn write_all_inner(&self, path: &str, data: &[u8], atomic: bool) -> Result<(), FsError> {
        let mut state = self.inner.borrow_mut();

        let old = state.meta.files.remove(path);
        if let Some(old_entry) = old {
            state.free_extents(old_entry.extents);
        }

        let extents = state.allocate_extents(data.len() as u64);
        write_data_segments(&state.io, &extents, data)?;

        let entry = FileMapEntry {
            size: data.len() as u64,
            extents,
        };
        state.meta.files.insert(path.to_string(), entry);
        state.dirty = true;
        state.validate_invariants()?;

        if atomic {
            state.persist_metadata()?;
        }

        Ok(())
    }

    fn append_inner(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
        if data.is_empty() {
            return Ok(());
        }

        let mut state = self.inner.borrow_mut();

        let extents = state.allocate_extents(data.len() as u64);
        write_data_segments(&state.io, &extents, data)?;

        let entry = state
            .meta
            .files
            .entry(path.to_string())
            .or_insert_with(|| FileMapEntry {
                size: 0,
                extents: Vec::new(),
            });

        entry.size += data.len() as u64;
        for extent in extents {
            push_extent(&mut entry.extents, extent);
        }

        state.dirty = true;
        state.validate_invariants()?;
        Ok(())
    }

    fn truncate_inner(&self, path: &str, len: u64) -> Result<(), FsError> {
        let mut state = self.inner.borrow_mut();

        if !state.meta.files.contains_key(path) {
            state.meta.files.insert(
                path.to_string(),
                FileMapEntry {
                    size: 0,
                    extents: Vec::new(),
                },
            );
        }

        let entry = state
            .meta
            .files
            .get(path)
            .cloned()
            .ok_or_else(|| FsError::Io("internal file lookup failure".to_string()))?;

        if len == entry.size {
            return Ok(());
        }

        if len < entry.size {
            let mut kept = Vec::new();
            let mut freed = Vec::new();
            let mut covered = 0u64;

            for extent in entry.extents {
                let next = covered + extent.len;
                if covered >= len {
                    freed.push(extent);
                } else if next <= len {
                    kept.push(extent);
                } else {
                    let keep_len = len - covered;
                    if keep_len > 0 {
                        kept.push(Extent {
                            offset: extent.offset,
                            len: keep_len,
                        });
                    }
                    let tail_len = extent.len - keep_len;
                    if tail_len > 0 {
                        freed.push(Extent {
                            offset: extent.offset + keep_len,
                            len: tail_len,
                        });
                    }
                }
                covered = next;
            }

            state.free_extents(freed);
            state.meta.files.insert(
                path.to_string(),
                FileMapEntry {
                    size: len,
                    extents: kept,
                },
            );
        } else {
            let grow_by = len - entry.size;
            let extents = state.allocate_extents(grow_by);
            write_zero_segments(&state.io, &extents)?;

            let mut new_entry = entry;
            new_entry.size = len;
            for extent in extents {
                push_extent(&mut new_entry.extents, extent);
            }

            state.meta.files.insert(path.to_string(), new_entry);
        }

        state.dirty = true;
        state.validate_invariants()?;
        Ok(())
    }

    fn remove_file_inner(&self, path: &str) -> Result<(), FsError> {
        let mut state = self.inner.borrow_mut();
        if let Some(entry) = state.meta.files.remove(path) {
            state.free_extents(entry.extents);
            state.dirty = true;
            state.validate_invariants()?;
        }
        Ok(())
    }

    fn list_files_inner(&self, prefix: &str) -> Vec<String> {
        let state = self.inner.borrow();
        let mut out: Vec<String> = state
            .meta
            .files
            .keys()
            .filter(|name| name.starts_with(prefix))
            .cloned()
            .collect();
        out.sort();
        out
    }

    fn sync_inner(&self) -> Result<(), FsError> {
        let mut state = self.inner.borrow_mut();
        state.persist_metadata()
    }

    #[cfg(test)]
    fn assert_valid(&self) -> Result<(), FsError> {
        self.inner.borrow().validate_invariants()
    }
}

impl<I: ContainerIo> SyncFs for ContainerFs<I> {
    fn read_all(&self, path: &str) -> Result<Vec<u8>, FsError> {
        self.read_all_inner(path)
    }

    fn read_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, FsError> {
        self.read_range_inner(path, offset, len)
    }

    fn write_all(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
        self.write_all_inner(path, data, false)
    }

    fn write_atomic(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
        self.write_all_inner(path, data, true)
    }

    fn append(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
        self.append_inner(path, data)
    }

    fn file_len(&self, path: &str) -> Result<u64, FsError> {
        let state = self.inner.borrow();
        Ok(state.meta.files.get(path).map(|f| f.size).unwrap_or(0))
    }

    fn truncate(&self, path: &str, len: u64) -> Result<(), FsError> {
        self.truncate_inner(path, len)
    }

    fn remove_file(&self, path: &str) -> Result<(), FsError> {
        self.remove_file_inner(path)
    }

    fn list_files(&self, prefix: &str) -> Result<Vec<String>, FsError> {
        Ok(self.list_files_inner(prefix))
    }

    fn sync_file(&self, _path: &str) -> Result<(), FsError> {
        self.sync_inner()
    }

    fn sync_dir(&self) -> Result<(), FsError> {
        self.sync_inner()
    }
}

impl<I: ContainerIo> ContainerState<I> {
    fn allocate_extents(&mut self, len: u64) -> Vec<Extent> {
        if len == 0 {
            return Vec::new();
        }

        self.normalize_free_list();

        let mut remaining = len;
        let mut out = Vec::new();

        let mut idx = 0usize;
        while remaining > 0 && idx < self.meta.free.len() {
            let avail = self.meta.free[idx].len;
            if avail == 0 {
                self.meta.free.remove(idx);
                continue;
            }

            let take = avail.min(remaining);
            let extent = Extent {
                offset: self.meta.free[idx].offset,
                len: take,
            };
            out.push(extent);

            self.meta.free[idx].offset += take;
            self.meta.free[idx].len -= take;
            remaining -= take;

            if self.meta.free[idx].len == 0 {
                self.meta.free.remove(idx);
            } else {
                idx += 1;
            }
        }

        if remaining > 0 {
            let extent = Extent {
                offset: self.meta.next_offset,
                len: remaining,
            };
            self.meta.next_offset += remaining;
            out.push(extent);
        }

        out
    }

    fn free_extents(&mut self, extents: Vec<Extent>) {
        for extent in extents {
            if extent.len > 0 {
                self.meta.free.push(extent);
            }
        }
        self.normalize_free_list();
    }

    fn normalize_free_list(&mut self) {
        self.meta.free.retain(|e| e.len > 0);
        self.meta.free.sort_by_key(|e| e.offset);

        let mut merged: Vec<Extent> = Vec::with_capacity(self.meta.free.len());
        for extent in &self.meta.free {
            if let Some(last) = merged.last_mut()
                && last.offset + last.len >= extent.offset
            {
                let new_end = (last.offset + last.len).max(extent.offset + extent.len);
                last.len = new_end - last.offset;
            } else {
                merged.push(*extent);
            }
        }

        self.meta.free = merged;

        // Reclaim trailing free extents at the high-water mark.
        loop {
            let Some(last) = self.meta.free.last().copied() else {
                break;
            };
            if last.offset + last.len == self.meta.next_offset {
                self.meta.next_offset = last.offset;
                self.meta.free.pop();
            } else {
                break;
            }
        }

        if self.meta.next_offset < DATA_START {
            self.meta.next_offset = DATA_START;
        }
    }

    fn persist_metadata(&mut self) -> Result<(), FsError> {
        if !self.dirty {
            self.io.flush()?;
            return Ok(());
        }

        self.normalize_free_list();
        self.validate_invariants()?;

        let generation = self.meta.generation + 1;
        let payload = PersistedMeta::from(self.meta.clone());
        write_meta_slot(
            &self.io,
            1 - self.active_slot,
            generation,
            &payload,
            self.meta.next_offset.max(DATA_START),
        )?;

        self.meta.generation = generation;
        self.active_slot = 1 - self.active_slot;
        self.dirty = false;
        Ok(())
    }

    fn validate_invariants(&self) -> Result<(), FsError> {
        if self.meta.next_offset < DATA_START {
            return Err(FsError::Io(format!(
                "invariant violation: next_offset {} < DATA_START {}",
                self.meta.next_offset, DATA_START
            )));
        }

        let mut occupied: Vec<Extent> = Vec::new();

        for (name, file) in &self.meta.files {
            let mut sum = 0u64;
            for extent in &file.extents {
                validate_extent_bounds(*extent, self.meta.next_offset, name)?;
                sum += extent.len;
                occupied.push(*extent);
            }
            if sum != file.size {
                return Err(FsError::Io(format!(
                    "invariant violation: file {name} size {} != extent sum {}",
                    file.size, sum
                )));
            }
        }

        // Free list must be sorted, non-overlapping, and non-adjacent after normalization.
        for (idx, extent) in self.meta.free.iter().enumerate() {
            validate_extent_bounds(*extent, self.meta.next_offset, "free")?;
            occupied.push(*extent);
            if idx > 0 {
                let prev = self.meta.free[idx - 1];
                if prev.offset + prev.len >= extent.offset {
                    return Err(FsError::Io(
                        "invariant violation: free list extents overlap or touch".to_string(),
                    ));
                }
            }
        }

        occupied.sort_by_key(|e| e.offset);
        for idx in 1..occupied.len() {
            let prev = occupied[idx - 1];
            let curr = occupied[idx];
            if prev.offset + prev.len > curr.offset {
                return Err(FsError::Io(
                    "invariant violation: extents overlap across files/free list".to_string(),
                ));
            }
        }

        Ok(())
    }
}

fn validate_extent_bounds(extent: Extent, max_offset: u64, context: &str) -> Result<(), FsError> {
    if extent.len == 0 {
        return Err(FsError::Io(format!(
            "invariant violation: zero-length extent in {context}"
        )));
    }
    if extent.offset < DATA_START {
        return Err(FsError::Io(format!(
            "invariant violation: extent below DATA_START in {context}"
        )));
    }
    let end = extent
        .offset
        .checked_add(extent.len)
        .ok_or_else(|| FsError::Io(format!("invariant violation: extent overflow in {context}")))?;
    if end > max_offset {
        return Err(FsError::Io(format!(
            "invariant violation: extent end {} > next_offset {} in {context}",
            end, max_offset
        )));
    }
    Ok(())
}

fn push_extent(extents: &mut Vec<Extent>, next: Extent) {
    if next.len == 0 {
        return;
    }
    if let Some(last) = extents.last_mut()
        && last.offset + last.len == next.offset
    {
        last.len += next.len;
        return;
    }
    extents.push(next);
}

fn write_data_segments<I: ContainerIo>(
    io: &I,
    extents: &[Extent],
    data: &[u8],
) -> Result<(), FsError> {
    let mut cursor = 0usize;
    for extent in extents {
        let len = extent.len as usize;
        let chunk = &data[cursor..cursor + len];
        write_exact_at(io, extent.offset, chunk)?;
        cursor += len;
    }
    Ok(())
}

fn write_zero_segments<I: ContainerIo>(io: &I, extents: &[Extent]) -> Result<(), FsError> {
    let zeros = vec![0u8; 8192];
    for extent in extents {
        let mut written = 0u64;
        while written < extent.len {
            let step = (extent.len - written).min(zeros.len() as u64) as usize;
            write_exact_at(io, extent.offset + written, &zeros[..step])?;
            written += step as u64;
        }
    }
    Ok(())
}

fn read_exact_at<I: ContainerIo>(io: &I, offset: u64, out: &mut [u8]) -> Result<(), FsError> {
    let mut cursor = 0usize;
    while cursor < out.len() {
        let n = io.read_at(offset + cursor as u64, &mut out[cursor..])?;
        if n == 0 {
            return Err(FsError::Io(format!(
                "short read at offset {} (wanted {}, got {})",
                offset,
                out.len(),
                cursor
            )));
        }
        cursor += n;
    }
    Ok(())
}

fn write_exact_at<I: ContainerIo>(io: &I, offset: u64, data: &[u8]) -> Result<(), FsError> {
    if data.is_empty() {
        return Ok(());
    }

    let end = offset
        .checked_add(data.len() as u64)
        .ok_or_else(|| FsError::Io("write offset overflow".to_string()))?;
    ensure_min_len(io, end)?;

    let mut cursor = 0usize;
    while cursor < data.len() {
        let n = io.write_at(offset + cursor as u64, &data[cursor..])?;
        if n == 0 {
            return Err(FsError::Io(format!(
                "short write at offset {} (wanted {}, wrote {})",
                offset,
                data.len(),
                cursor
            )));
        }
        cursor += n;
    }
    Ok(())
}

fn ensure_min_len<I: ContainerIo>(io: &I, min_len: u64) -> Result<(), FsError> {
    if io.len()? < min_len {
        io.truncate(min_len)?;
    }
    Ok(())
}

fn load_container_meta<I: ContainerIo>(io: &I) -> Result<(ContainerMeta, usize), FsError> {
    let slot0 = read_meta_slot(io, 0)?;
    let slot1 = read_meta_slot(io, 1)?;

    match (slot0, slot1) {
        (Some((g0, m0)), Some((g1, m1))) => {
            if g1 >= g0 {
                Ok((m1.into_runtime(g1), 1))
            } else {
                Ok((m0.into_runtime(g0), 0))
            }
        }
        (Some((g0, m0)), None) => Ok((m0.into_runtime(g0), 0)),
        (None, Some((g1, m1))) => Ok((m1.into_runtime(g1), 1)),
        (None, None) => Ok((ContainerMeta::default(), 0)),
    }
}

fn read_meta_slot<I: ContainerIo>(
    io: &I,
    slot: usize,
) -> Result<Option<(u64, PersistedMeta)>, FsError> {
    let slot_offset = (slot as u64) * (META_SLOT_SIZE as u64);
    let mut header = [0u8; META_HEADER_SIZE];

    let header_read = io.read_at(slot_offset, &mut header)?;
    if header_read < META_HEADER_SIZE {
        return Ok(None);
    }

    if header[0..8] != CONTAINER_MAGIC {
        return Ok(None);
    }

    let version = u32::from_le_bytes(header[8..12].try_into().expect("header version slice"));
    if version != CONTAINER_VERSION {
        return Ok(None);
    }

    let generation = u64::from_le_bytes(header[12..20].try_into().expect("header gen slice"));
    let payload_len =
        u32::from_le_bytes(header[20..24].try_into().expect("header len slice")) as usize;
    let expected_crc = u32::from_le_bytes(header[24..28].try_into().expect("header crc slice"));

    if payload_len == 0 || payload_len > META_SLOT_SIZE - META_HEADER_SIZE {
        return Ok(None);
    }

    let mut payload = vec![0u8; payload_len];
    read_exact_at(io, slot_offset + META_HEADER_SIZE as u64, &mut payload)?;

    if crc32fast::hash(&payload) != expected_crc {
        return Ok(None);
    }

    let decoded: PersistedMeta = serde_json::from_slice(&payload)
        .map_err(|e| FsError::Io(format!("failed to decode metadata slot {slot}: {e}")))?;
    Ok(Some((generation, decoded)))
}

fn write_meta_slot<I: ContainerIo>(
    io: &I,
    slot: usize,
    generation: u64,
    payload: &PersistedMeta,
    min_len_after_commit: u64,
) -> Result<(), FsError> {
    let payload_bytes = serde_json::to_vec(payload)
        .map_err(|e| FsError::Io(format!("failed to encode metadata: {e}")))?;
    if payload_bytes.len() > META_SLOT_SIZE - META_HEADER_SIZE {
        return Err(FsError::Io(format!(
            "metadata too large: {} bytes (max {})",
            payload_bytes.len(),
            META_SLOT_SIZE - META_HEADER_SIZE
        )));
    }

    let mut slot_bytes = vec![0u8; META_SLOT_SIZE];
    slot_bytes[0..8].copy_from_slice(&CONTAINER_MAGIC);
    slot_bytes[8..12].copy_from_slice(&CONTAINER_VERSION.to_le_bytes());
    slot_bytes[12..20].copy_from_slice(&generation.to_le_bytes());
    slot_bytes[20..24].copy_from_slice(&(payload_bytes.len() as u32).to_le_bytes());
    slot_bytes[24..28].copy_from_slice(&crc32fast::hash(&payload_bytes).to_le_bytes());
    let start = META_HEADER_SIZE;
    let end = start + payload_bytes.len();
    slot_bytes[start..end].copy_from_slice(&payload_bytes);

    let slot_offset = (slot as u64) * (META_SLOT_SIZE as u64);
    write_exact_at(io, slot_offset, &slot_bytes)?;
    io.flush()?;

    ensure_min_len(io, min_len_after_commit.max(DATA_START))?;
    io.truncate(min_len_after_commit.max(DATA_START))?;
    io.flush()?;

    Ok(())
}

// ============================================================================
// OPFS-backed single-container SyncFs
// ============================================================================

#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
pub struct OpfsFs {
    inner: ContainerFs<OpfsContainerIo>,
}

#[cfg(target_arch = "wasm32")]
impl OpfsFs {
    pub async fn open(namespace: &str) -> Result<Self, FsError> {
        let global: web_sys::WorkerGlobalScope = js_sys::global()
            .dyn_into()
            .map_err(|_| FsError::Io("OpfsFs::open must run in a dedicated worker".to_string()))?;
        let storage = global.navigator().storage();

        let root: web_sys::FileSystemDirectoryHandle = JsFuture::from(storage.get_directory())
            .await
            .map_err(map_js_error)?
            .dyn_into()
            .map_err(|_| FsError::Io("failed to cast OPFS root".to_string()))?;

        let opts = web_sys::FileSystemGetFileOptions::new();
        opts.set_create(true);
        let file: web_sys::FileSystemFileHandle = JsFuture::from(
            root.get_file_handle_with_options(&Self::container_name(namespace), &opts),
        )
        .await
        .map_err(map_js_error)?
        .dyn_into()
        .map_err(|_| FsError::Io("failed to cast OPFS file handle".to_string()))?;

        let handle: web_sys::FileSystemSyncAccessHandle =
            JsFuture::from(file.create_sync_access_handle())
                .await
                .map_err(map_js_error)?
                .dyn_into()
                .map_err(|_| FsError::Io("failed to cast OPFS sync access handle".to_string()))?;

        let inner = ContainerFs::open(OpfsContainerIo { handle })?;
        Ok(Self { inner })
    }

    pub async fn destroy(namespace: &str) -> Result<(), FsError> {
        let global: web_sys::WorkerGlobalScope = js_sys::global().dyn_into().map_err(|_| {
            FsError::Io("OpfsFs::destroy must run in a dedicated worker".to_string())
        })?;
        let storage = global.navigator().storage();
        let root: web_sys::FileSystemDirectoryHandle = JsFuture::from(storage.get_directory())
            .await
            .map_err(map_js_error)?
            .dyn_into()
            .map_err(|_| FsError::Io("failed to cast OPFS root".to_string()))?;

        let name = Self::container_name(namespace);
        let remove_fn = js_sys::Reflect::get(&root, &"removeEntry".into()).map_err(map_js_error)?;
        let remove_fn: js_sys::Function = remove_fn
            .dyn_into()
            .map_err(|_| FsError::Io("OPFS removeEntry is unavailable".to_string()))?;
        let opts = js_sys::Object::new();
        let _ = js_sys::Reflect::set(&opts, &"recursive".into(), &false.into());
        let promise = remove_fn.call2(&root, &name.into(), &opts.into());
        if let Ok(promise) = promise {
            let promise: js_sys::Promise = promise
                .dyn_into()
                .map_err(|_| FsError::Io("failed to cast removeEntry promise".to_string()))?;
            let _ = JsFuture::from(promise).await;
        }

        Ok(())
    }

    fn container_name(namespace: &str) -> String {
        format!("{}.jazzlsmfs", namespace)
    }
}

#[cfg(target_arch = "wasm32")]
impl SyncFs for OpfsFs {
    fn read_all(&self, path: &str) -> Result<Vec<u8>, FsError> {
        self.inner.read_all(path)
    }

    fn read_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, FsError> {
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

#[cfg(target_arch = "wasm32")]
struct OpfsContainerIo {
    handle: web_sys::FileSystemSyncAccessHandle,
}

#[cfg(target_arch = "wasm32")]
impl Drop for OpfsContainerIo {
    fn drop(&mut self) {
        self.handle.close();
    }
}

#[cfg(target_arch = "wasm32")]
impl ContainerIo for OpfsContainerIo {
    fn len(&self) -> Result<u64, FsError> {
        Ok(self.handle.get_size().map_err(map_js_error)? as u64)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize, FsError> {
        let opts = web_sys::FileSystemReadWriteOptions::new();
        opts.set_at(offset as f64);
        let read = self
            .handle
            .read_with_u8_array_and_options(buf, &opts)
            .map_err(map_js_error)?;
        Ok(read as usize)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize, FsError> {
        let opts = web_sys::FileSystemReadWriteOptions::new();
        opts.set_at(offset as f64);
        let written = self
            .handle
            .write_with_u8_array_and_options(buf, &opts)
            .map_err(map_js_error)?;
        Ok(written as usize)
    }

    fn truncate(&self, len: u64) -> Result<(), FsError> {
        truncate_handle(&self.handle, len)
    }

    fn flush(&self) -> Result<(), FsError> {
        self.handle.flush().map_err(map_js_error)
    }
}

#[cfg(target_arch = "wasm32")]
fn truncate_handle(handle: &web_sys::FileSystemSyncAccessHandle, len: u64) -> Result<(), FsError> {
    let truncate = js_sys::Reflect::get(handle, &"truncate".into()).map_err(map_js_error)?;
    let truncate: js_sys::Function = truncate
        .dyn_into()
        .map_err(|_| FsError::Io("OPFS truncate is unavailable".to_string()))?;
    truncate
        .call1(handle, &wasm_bindgen::JsValue::from_f64(len as f64))
        .map_err(map_js_error)?;
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn map_js_error(value: wasm_bindgen::JsValue) -> FsError {
    if let Some(s) = value.as_string() {
        FsError::Io(s)
    } else {
        FsError::Io(format!("{value:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Default)]
    struct MemContainerIo {
        data: Rc<RefCell<Vec<u8>>>,
        flushes: Rc<RefCell<u64>>,
    }

    impl ContainerIo for MemContainerIo {
        fn len(&self) -> Result<u64, FsError> {
            Ok(self.data.borrow().len() as u64)
        }

        fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize, FsError> {
            let data = self.data.borrow();
            let offset = offset as usize;
            if offset >= data.len() {
                return Ok(0);
            }
            let end = (offset + buf.len()).min(data.len());
            let len = end - offset;
            buf[..len].copy_from_slice(&data[offset..end]);
            Ok(len)
        }

        fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize, FsError> {
            let mut data = self.data.borrow_mut();
            let offset = offset as usize;
            let end = offset + buf.len();
            if end > data.len() {
                data.resize(end, 0);
            }
            data[offset..end].copy_from_slice(buf);
            Ok(buf.len())
        }

        fn truncate(&self, len: u64) -> Result<(), FsError> {
            self.data.borrow_mut().resize(len as usize, 0);
            Ok(())
        }

        fn flush(&self) -> Result<(), FsError> {
            *self.flushes.borrow_mut() += 1;
            Ok(())
        }
    }

    fn open_mem_fs(io: MemContainerIo) -> ContainerFs<MemContainerIo> {
        ContainerFs::open(io).expect("open container fs")
    }

    #[test]
    fn mapping_round_trip_read_write() {
        let io = MemContainerIo::default();
        let fs = open_mem_fs(io);

        fs.write_all("a", b"hello").unwrap();
        fs.write_all("b", b"world").unwrap();
        fs.assert_valid().unwrap();

        assert_eq!(fs.read_all("a").unwrap(), b"hello".to_vec());
        assert_eq!(fs.read_all("b").unwrap(), b"world".to_vec());
        assert_eq!(fs.file_len("a").unwrap(), 5);
        assert_eq!(fs.file_len("missing").unwrap(), 0);

        let files = fs.list_files("").unwrap();
        assert_eq!(files, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn mapping_append_and_truncate_invariants() {
        let io = MemContainerIo::default();
        let fs = open_mem_fs(io);

        fs.append("wal", b"abc").unwrap();
        fs.append("wal", b"def").unwrap();
        fs.assert_valid().unwrap();
        assert_eq!(fs.read_all("wal").unwrap(), b"abcdef".to_vec());

        fs.truncate("wal", 4).unwrap();
        fs.assert_valid().unwrap();
        assert_eq!(fs.read_all("wal").unwrap(), b"abcd".to_vec());

        fs.truncate("wal", 7).unwrap();
        fs.assert_valid().unwrap();
        assert_eq!(fs.read_all("wal").unwrap(), b"abcd\0\0\0".to_vec());
    }

    #[test]
    fn mapping_read_range_spans_multiple_extents() {
        let io = MemContainerIo::default();
        let fs = open_mem_fs(io);

        fs.append("wal", b"abcdefghij").unwrap();
        fs.write_all("gap", b"XXXXXXXXXXXXXXXXXXXX").unwrap();
        fs.append("wal", b"klmnopqrst").unwrap();
        fs.assert_valid().unwrap();

        assert_eq!(fs.read_range("wal", 0, 4).unwrap(), b"abcd".to_vec());
        assert_eq!(fs.read_range("wal", 8, 6).unwrap(), b"ijklmn".to_vec());
        assert_eq!(fs.read_range("wal", 18, 10).unwrap(), b"st".to_vec());
        assert_eq!(fs.read_range("wal", 999, 8).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn mapping_remove_reuses_space_without_overlap() {
        let io = MemContainerIo::default();
        let fs = open_mem_fs(io);

        fs.write_all("f1", &[1u8; 100]).unwrap();
        fs.write_all("f2", &[2u8; 100]).unwrap();
        fs.sync_dir().unwrap();

        let before = {
            let state = fs.inner.borrow();
            state.meta.next_offset
        };

        fs.remove_file("f1").unwrap();
        fs.write_all("f3", &[3u8; 60]).unwrap();
        fs.assert_valid().unwrap();

        let after = {
            let state = fs.inner.borrow();
            state.meta.next_offset
        };

        // The allocator should have reused freed space; high-water mark should not increase.
        assert_eq!(before, after);
    }

    #[test]
    fn mapping_persists_and_reopens_from_metadata_slots() {
        let io = MemContainerIo::default();
        let fs = open_mem_fs(io.clone());

        fs.write_all("manifest", b"m1").unwrap();
        fs.write_all("sst-001", b"payload").unwrap();
        fs.append("wal", b"abc").unwrap();
        fs.sync_dir().unwrap();
        fs.assert_valid().unwrap();

        let reopened = open_mem_fs(io);
        reopened.assert_valid().unwrap();

        assert_eq!(reopened.read_all("manifest").unwrap(), b"m1".to_vec());
        assert_eq!(reopened.read_all("sst-001").unwrap(), b"payload".to_vec());
        assert_eq!(reopened.read_all("wal").unwrap(), b"abc".to_vec());
    }

    #[test]
    fn mapping_write_atomic_commits_metadata_immediately() {
        let io = MemContainerIo::default();
        let fs = open_mem_fs(io.clone());

        fs.write_atomic("CURRENT", b"MANIFEST-00001").unwrap();
        fs.assert_valid().unwrap();

        let reopened = open_mem_fs(io);
        assert_eq!(
            reopened.read_all("CURRENT").unwrap(),
            b"MANIFEST-00001".to_vec()
        );
    }

    #[test]
    fn mapping_free_list_remains_normalized() {
        let io = MemContainerIo::default();
        let fs = open_mem_fs(io);

        fs.write_all("a", &[1u8; 64]).unwrap();
        fs.write_all("b", &[2u8; 64]).unwrap();
        fs.write_all("c", &[3u8; 64]).unwrap();

        fs.remove_file("b").unwrap();
        fs.remove_file("a").unwrap();
        fs.assert_valid().unwrap();

        let state = fs.inner.borrow();
        // Adjacent free extents should be merged by normalization.
        for i in 1..state.meta.free.len() {
            let prev = state.meta.free[i - 1];
            let curr = state.meta.free[i];
            assert!(prev.offset + prev.len < curr.offset);
        }
    }

    #[test]
    fn mapping_missing_file_errors_on_read() {
        let io = MemContainerIo::default();
        let fs = open_mem_fs(io);
        match fs.read_all("missing") {
            Err(FsError::NotFound(path)) => assert_eq!(path, "missing"),
            other => panic!("unexpected read result: {other:?}"),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn std_fs_read_handle_cache_invalidation_on_write_all() {
        let dir = tempfile::tempdir().expect("tempdir");
        let fs = StdFs::with_read_handle_cache_capacity(dir.path(), 2).expect("open std fs");

        fs.write_all("data", b"first").unwrap();
        assert_eq!(fs.read_range("data", 0, 5).unwrap(), b"first".to_vec());

        fs.write_all("data", b"second").unwrap();
        assert_eq!(fs.read_range("data", 0, 6).unwrap(), b"second".to_vec());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn std_fs_read_handle_cache_invalidation_on_write_atomic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let fs = StdFs::with_read_handle_cache_capacity(dir.path(), 2).expect("open std fs");

        fs.write_all("manifest", b"old").unwrap();
        assert_eq!(fs.read_range("manifest", 0, 3).unwrap(), b"old".to_vec());

        fs.write_atomic("manifest", b"new-value").unwrap();
        assert_eq!(
            fs.read_range("manifest", 0, 9).unwrap(),
            b"new-value".to_vec()
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn std_fs_read_handle_cache_invalidation_on_remove_and_recreate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let fs = StdFs::with_read_handle_cache_capacity(dir.path(), 2).expect("open std fs");

        fs.write_all("sst", b"old").unwrap();
        assert_eq!(fs.read_range("sst", 0, 3).unwrap(), b"old".to_vec());

        fs.remove_file("sst").unwrap();
        fs.write_all("sst", b"new").unwrap();
        assert_eq!(fs.read_range("sst", 0, 3).unwrap(), b"new".to_vec());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn std_fs_read_handle_cache_eviction_is_lru() {
        let dir = tempfile::tempdir().expect("tempdir");
        let fs = StdFs::with_read_handle_cache_capacity(dir.path(), 1).expect("open std fs");

        fs.write_all("a", b"aaaa").unwrap();
        fs.write_all("b", b"bbbb").unwrap();
        assert_eq!(fs.read_range("a", 0, 4).unwrap(), b"aaaa".to_vec());
        assert_eq!(fs.read_range("b", 0, 4).unwrap(), b"bbbb".to_vec());

        let cache = fs.read_handle_cache.borrow();
        assert_eq!(cache.entries.len(), 1);
        assert!(cache.entries.contains_key("b"));
        assert!(!cache.entries.contains_key("a"));
    }
}
