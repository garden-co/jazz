use rustc_hash::{FxHashMap, FxHashSet};
use std::borrow::Cow;
use std::collections::BinaryHeap;

use crate::BTreeError;
use crate::file::SyncFile;
use crate::page::{
    OverflowRef, Page, PageId, PageKind, RawDescendStep, RawLeafDeleteResult, RawLeafSpan,
    RawLeafUpsertResult, ValueCell, ValueCellRef, decode_page, encode_page, freelist_ids_per_page,
    page_fits, raw_descend_step, raw_freelist_page, raw_leaf_covers_key, raw_leaf_delete_in_place,
    raw_leaf_find_value, raw_leaf_scan, raw_leaf_span, raw_leaf_upsert_in_place, raw_page_kind,
    refresh_page_checksum, validate_page,
};
use crate::superblock::{Superblock, SuperblockSlot};
use crate::wal::{self, WalFrame, WalFrameRef, WalHeader};

const MIN_PAGE_SIZE: usize = 4 * 1024;
const DEFAULT_PAGE_SIZE: usize = 16 * 1024;
const DEFAULT_CACHE_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_OVERFLOW_THRESHOLD: usize = 4 * 1024;
const OVERFLOW_REUSE_MIN_BYTES: usize = 128 * 1024;
const OVERFLOW_DIRECT_READ_MIN_BYTES: usize = 128 * 1024;
const BOOTSTRAP_GENERATION: u64 = 1;
const ALLOC_NEAR_WINDOW: u64 = 32;
// Recently used leaf pages, most recent first; page id 0 marks an empty slot
// (ids below 2 are superblocks and never tree pages). Multiple slots let
// workloads that interleave several key regions (separate tables in one tree)
// keep a hint per region; the could_cover prefilter keeps non-front slots to
// a couple of byte comparisons on the miss path.
const LEAF_HINT_SLOTS: usize = 4;

type OpfsMap<K, V> = FxHashMap<K, V>;
type OpfsSet<T> = FxHashSet<T>;

struct PageWrite<'a> {
    page_id: PageId,
    is_blob: bool,
    is_freelist: bool,
    raw: Cow<'a, [u8]>,
}

#[derive(Clone, Debug, Default)]
struct LeafHint {
    page_id: PageId,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    is_tail: bool,
}

impl LeafHint {
    /// Overwrites this slot in place, reusing its key buffers so descents
    /// don't allocate two fresh Vecs each time a hint is remembered.
    fn set(&mut self, page_id: PageId, span: &RawLeafSpan<'_>) {
        self.page_id = page_id;
        self.first_key.clear();
        self.first_key.extend_from_slice(span.first_key);
        self.last_key.clear();
        self.last_key.extend_from_slice(span.last_key);
        self.is_tail = span.next_page_id.is_none();
    }

    fn is_empty(&self) -> bool {
        self.page_id == 0
    }

    /// Prefilter only: false positives are re-checked by
    /// `raw_leaf_covers_key` on the live page bytes; `is_tail` can only go
    /// stale in the safe direction (a tail becomes non-tail on split, never
    /// the reverse, because leaves are never merged).
    fn could_cover(&self, key: &[u8]) -> bool {
        !self.is_empty()
            && self.first_key.as_slice() <= key
            && (key <= self.last_key.as_slice() || self.is_tail)
    }

    fn clear(&mut self) {
        self.page_id = 0;
        self.first_key.clear();
        self.last_key.clear();
        self.is_tail = false;
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BTreeOptions {
    pub page_size: usize,
    pub cache_bytes: usize,
    pub overflow_threshold: usize,
    pub pin_internal_pages: bool,
    pub read_coalesce_pages: usize,
}

impl Default for BTreeOptions {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            cache_bytes: DEFAULT_CACHE_BYTES,
            overflow_threshold: DEFAULT_OVERFLOW_THRESHOLD,
            pin_internal_pages: false,
            read_coalesce_pages: 1,
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
        if self.read_coalesce_pages == 0 {
            return Err(BTreeError::InvalidOptions(
                "read_coalesce_pages must be > 0".to_string(),
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
    root_page_id: Option<PageId>,
    total_pages: u64,
    // Pages >= active.total_pages are WAL tail pages, not home locations.
    // Page ids in wal_pages have newer bytes only in the cache/WAL and must
    // not be evicted until a checkpoint writes them to their home locations.
    persisted_pages: u64,
    pages: OpfsMap<PageId, Vec<u8>>,
    blob_pages: OpfsSet<PageId>,
    page_access_epoch: OpfsMap<PageId, u64>,
    access_epoch: u64,
    dirty_pages: OpfsSet<PageId>,
    wal_pages: OpfsSet<PageId>,
    freelist_dirty: bool,
    free_pages: Vec<PageId>,
    free_set: OpfsSet<PageId>,
    freelist_meta_pages: Vec<PageId>,
    leaf_hints: [LeafHint; LEAF_HINT_SLOTS],
}

#[derive(Debug)]
struct SplitResult {
    separator: Vec<u8>,
    right_page_id: PageId,
}

type KvPair = (Vec<u8>, Vec<u8>);

enum StagedValue {
    Inline(Vec<u8>),
    Overflow {
        head_page_id: PageId,
        total_len: usize,
    },
}

impl<F: SyncFile> OpfsBTree<F> {
    pub fn open(file: F, options: BTreeOptions) -> Result<Self, BTreeError> {
        options.validate()?;

        let a = read_slot(&file, SuperblockSlot::A, options.page_size)?;
        let b = read_slot(&file, SuperblockSlot::B, options.page_size)?;
        let file_len = file.len()?;

        let persisted_pages = file_len / options.page_size as u64;
        let (active_slot, active) = choose_active(a, b).unwrap_or((
            SuperblockSlot::A,
            Superblock::new(options.page_size as u32, 0, 0, 0, 0),
        ));

        let mut tree = Self {
            file,
            options,
            active_slot,
            active,
            root_page_id: None,
            total_pages: 2,
            persisted_pages,
            pages: OpfsMap::default(),
            blob_pages: OpfsSet::default(),
            page_access_epoch: OpfsMap::default(),
            access_epoch: 0,
            dirty_pages: OpfsSet::default(),
            wal_pages: OpfsSet::default(),
            freelist_dirty: false,
            free_pages: Vec::new(),
            free_set: OpfsSet::default(),
            freelist_meta_pages: Vec::new(),
            leaf_hints: std::array::from_fn(|_| LeafHint::default()),
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
            tree.total_pages = 2;
            tree.persisted_pages = 2;
            return Ok(tree);
        }

        tree.total_pages = tree.active.total_pages.max(2);
        if tree.active.root_page_id != 0 {
            tree.root_page_id = Some(tree.active.root_page_id);
            tree.ensure_page_loaded(tree.active.root_page_id)?;
            match tree.page_kind(tree.active.root_page_id)? {
                PageKind::Leaf | PageKind::Internal => {}
                PageKind::Overflow | PageKind::Freelist => {
                    return Err(BTreeError::Corrupt(format!(
                        "root page {} has invalid kind",
                        tree.active.root_page_id
                    )));
                }
            }
        }
        if tree.active.freelist_head_page_id != 0 {
            tree.load_freelist_from_disk(tree.active.freelist_head_page_id)?;
        }
        tree.sanitize_free_pages();
        // Loading the persisted freelist is not a modification; only replayed
        // WAL commits or later alloc/free calls should mark it dirty.
        tree.freelist_dirty = false;
        tree.replay_wal()?;

        Ok(tree)
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, BTreeError> {
        let _span = tracing::trace_span!("OpfsBTree::get", key_len = key.len()).entered();
        let leaf_page_id = match self.leaf_for_key(key)? {
            Some(id) => id,
            None => return Ok(None),
        };

        let page_size = self.options.page_size;
        let raw = self.ensure_page_loaded(leaf_page_id)?;
        let value_cell = raw_leaf_find_value(raw, page_size, key)?;
        match value_cell {
            None => Ok(None),
            Some(ValueCellRef::Inline(value)) => Ok(Some(value.to_vec())),
            Some(ValueCellRef::Overflow {
                head_page_id,
                total_len,
            }) => self
                .read_overflow_value(head_page_id, total_len as usize)
                .map(Some),
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), BTreeError> {
        let _span = tracing::trace_span!(
            "OpfsBTree::put",
            key_len = key.len(),
            value_len = value.len()
        )
        .entered();
        if self.root_page_id.is_none() {
            let root_page_id = self.alloc_page();
            let value_cell = self.build_value_cell(value)?;
            let leaf = Page::Leaf {
                entries: vec![(key.to_vec(), value_cell)],
                next: None,
            };
            ensure_page_fits(&leaf, self.options.page_size, "initial leaf")?;
            self.set_dirty_page(root_page_id, leaf)?;
            self.root_page_id = Some(root_page_id);
            return Ok(());
        }

        // Fast path: when a hinted leaf covers the key and the value is
        // inline-sized, upsert in place without descending. Splits still need
        // the parent path, so NeedSplit falls through to the recursive insert.
        if value.len() <= self.options.overflow_threshold
            && let Some(page_id) = self.hinted_leaf_for_key(key, true)?
        {
            let new_value = ValueCell::Inline(value.to_vec());
            let upsert = {
                let raw = self.pages.get_mut(&page_id).ok_or_else(|| {
                    BTreeError::Corrupt(format!("page {} missing during insert", page_id))
                })?;
                raw_leaf_upsert_in_place(raw, self.options.page_size, key, &new_value)?
            };
            match upsert {
                RawLeafUpsertResult::Inserted => {
                    self.mark_dirty_loaded_page(page_id);
                    self.remember_leaf_hint(page_id)?;
                    return Ok(());
                }
                RawLeafUpsertResult::Updated { old_overflow } => {
                    self.mark_dirty_loaded_page(page_id);
                    self.remember_leaf_hint(page_id)?;
                    if let Some(old_overflow) = old_overflow {
                        self.free_overflow_extent(
                            old_overflow.head_page_id,
                            old_overflow.total_len as usize,
                        )?;
                    }
                    return Ok(());
                }
                RawLeafUpsertResult::NeedSplit => {}
            }
        }

        let root_page_id = self.root_page_id.expect("root must exist");
        if let Some(split) = self.insert_recursive(root_page_id, key, value)? {
            let new_root_page_id = self.alloc_page();
            let new_root = Page::Internal {
                keys: vec![split.separator],
                children: vec![root_page_id, split.right_page_id],
            };
            ensure_page_fits(&new_root, self.options.page_size, "new root")?;
            self.set_dirty_page(new_root_page_id, new_root)?;
            self.root_page_id = Some(new_root_page_id);
        }

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), BTreeError> {
        let _span = tracing::trace_span!("OpfsBTree::delete", key_len = key.len()).entered();
        let root_page_id = match self.root_page_id {
            Some(id) => id,
            None => return Ok(()),
        };

        let removed = self.delete_recursive(root_page_id, key)?;
        if !removed {
            return Ok(());
        }

        let root_page_id = match self.root_page_id {
            Some(id) => id,
            None => return Ok(()),
        };

        let root_page_raw = self.pages.get(&root_page_id).ok_or_else(|| {
            BTreeError::Corrupt(format!("root page {} missing after delete", root_page_id))
        })?;
        let root_page = decode_page(root_page_raw, self.options.page_size)?;

        if let Page::Leaf { entries, .. } = &root_page
            && entries.is_empty()
        {
            self.remove_page(root_page_id);
            self.add_free_page(root_page_id);
            self.root_page_id = None;
        }

        Ok(())
    }

    pub fn range(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<KvPair>, BTreeError> {
        let _span = tracing::trace_span!("OpfsBTree::range", limit).entered();
        if limit == 0 || start >= end {
            return Ok(Vec::new());
        }

        let page_size = self.options.page_size;
        let mut out = Vec::with_capacity(limit.min(1024));
        let mut current = self.leaf_for_range_start(start)?;
        let mut visited = OpfsSet::default();
        let mut staged: Vec<(Vec<u8>, StagedValue)> = Vec::new();
        let mut last_scanned = None;

        while let Some(page_id) = current {
            if !visited.insert(page_id) {
                return Err(BTreeError::Corrupt(
                    "leaf chain contains a cycle".to_string(),
                ));
            }

            let remaining = limit.saturating_sub(out.len());
            let raw = self.ensure_page_loaded(page_id)?;
            staged.clear();
            let next = raw_leaf_scan(raw, page_size, start, end, remaining, |key, value| {
                let staged_value = match value {
                    ValueCellRef::Inline(value) => StagedValue::Inline(value.to_vec()),
                    ValueCellRef::Overflow {
                        head_page_id,
                        total_len,
                    } => StagedValue::Overflow {
                        head_page_id,
                        total_len: total_len as usize,
                    },
                };
                staged.push((key.to_vec(), staged_value));
                Ok(())
            })?;

            for (key, value) in staged.drain(..) {
                let value = match value {
                    StagedValue::Inline(value) => value,
                    StagedValue::Overflow {
                        head_page_id,
                        total_len,
                    } => self.read_overflow_value(head_page_id, total_len)?,
                };
                out.push((key, value));
                if out.len() == limit {
                    self.remember_leaf_hint_if_not_mru(page_id)?;
                    return Ok(out);
                }
            }

            last_scanned = Some(page_id);
            current = next;
        }

        if let Some(page_id) = last_scanned {
            self.remember_leaf_hint_if_not_mru(page_id)?;
        }
        Ok(out)
    }

    pub fn checkpoint(&mut self) -> Result<(), BTreeError> {
        let _span = tracing::debug_span!(
            "OpfsBTree::checkpoint",
            dirty_pages = self.dirty_pages.len(),
            total_pages = self.total_pages
        )
        .entered();
        let mut dirty_page_ids: Vec<PageId> =
            self.dirty_pages.union(&self.wal_pages).copied().collect();
        dirty_page_ids.sort_unstable();
        let (freelist_head_page_id, freelist_pages) = self.build_freelist_pages()?;
        self.write_pages_to_disk(&dirty_page_ids, &freelist_pages)?;
        self.file.flush()?;
        self.checkpoint_superblock(
            self.root_page_id.unwrap_or(0),
            freelist_head_page_id,
            self.total_pages,
        )?;
        self.truncate_wal_tail()?;
        self.dirty_pages.clear();
        self.wal_pages.clear();
        self.freelist_dirty = false;
        self.evict_pages_if_needed(None);
        Ok(())
    }

    pub fn flush_wal(&mut self) -> Result<(), BTreeError> {
        let _span = tracing::debug_span!(
            "OpfsBTree::flush_wal",
            dirty_pages = self.dirty_pages.len(),
            total_pages = self.total_pages
        )
        .entered();
        let root_page_id = self.root_page_id.unwrap_or(0);
        if self.dirty_pages.is_empty()
            && !self.freelist_dirty
            && root_page_id == self.active.root_page_id
            && self.total_pages == self.active.total_pages
        {
            return Ok(());
        }

        let mut dirty_page_ids: Vec<PageId> = self.dirty_pages.iter().copied().collect();
        dirty_page_ids.sort_unstable();
        self.refresh_dirty_page_checksums(&dirty_page_ids)?;
        let (freelist_head_page_id, freelist_pages) = self.build_freelist_pages()?;
        let generation = self.active.generation.saturating_add(1);
        let persisted_pages = {
            let frames = self.collect_pages_for_write(&dirty_page_ids, &freelist_pages)?;
            if frames.is_empty() {
                return Ok(());
            }

            let wal_frames: Vec<WalFrameRef<'_>> = frames
                .iter()
                .map(|frame| WalFrameRef {
                    page_id: frame.page_id,
                    is_blob: frame.is_blob,
                    is_freelist: frame.is_freelist,
                    raw: frame.raw.as_ref(),
                })
                .collect();
            let start_page_id = self.persisted_pages.max(2);
            let pages_written = wal::append_commit(
                &self.file,
                self.options.page_size,
                start_page_id,
                WalHeader::new(
                    generation,
                    root_page_id,
                    freelist_head_page_id,
                    self.total_pages,
                ),
                &wal_frames,
            )?;
            start_page_id
                .checked_add(pages_written)
                .ok_or_else(|| BTreeError::Io("WAL persisted pages overflow".to_string()))?
        };
        self.persisted_pages = persisted_pages;
        self.file.flush()?;

        self.wal_pages.extend(dirty_page_ids);
        self.dirty_pages.clear();
        self.active = Superblock::new(
            self.options.page_size as u32,
            generation,
            root_page_id,
            freelist_head_page_id,
            self.total_pages,
        );
        self.freelist_dirty = false;
        Ok(())
    }

    pub fn checkpoint_state(&self) -> CheckpointState {
        // Reports the latest durable logical state: checkpoint metadata plus
        // any replayed or newly flushed WAL commits. `active_slot` still names
        // the checkpoint superblock slot; WAL commits do not rotate slots.
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

    fn insert_recursive(
        &mut self,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<SplitResult>, BTreeError> {
        let page_size = self.options.page_size;
        let step = {
            let raw = self.ensure_page_loaded(page_id)?;
            raw_descend_step(raw, page_size, key)?
        };

        match step {
            RawDescendStep::Leaf => {
                let (new_value, reused_overflow_head) =
                    if value.len() <= self.options.overflow_threshold {
                        (ValueCell::Inline(value.to_vec()), None)
                    } else if value.len() < OVERFLOW_REUSE_MIN_BYTES {
                        (self.build_value_cell(value)?, None)
                    } else {
                        let existing_overflow = {
                            let raw = self.raw_page_bytes(page_id)?;
                            match raw_leaf_find_value(raw, self.options.page_size, key)? {
                                Some(ValueCellRef::Overflow {
                                    head_page_id,
                                    total_len,
                                }) => Some(OverflowRef {
                                    head_page_id,
                                    total_len,
                                }),
                                _ => None,
                            }
                        };
                        self.build_value_cell_for_existing(existing_overflow, value)?
                    };
                let upsert = {
                    let raw = self.pages.get_mut(&page_id).ok_or_else(|| {
                        BTreeError::Corrupt(format!("page {} missing during insert", page_id))
                    })?;
                    raw_leaf_upsert_in_place(raw, self.options.page_size, key, &new_value)?
                };

                match upsert {
                    RawLeafUpsertResult::Inserted => {
                        self.mark_dirty_loaded_page(page_id);
                        self.remember_tail_leaf_hint(page_id)?;
                        return Ok(None);
                    }
                    RawLeafUpsertResult::Updated { old_overflow } => {
                        self.mark_dirty_loaded_page(page_id);
                        self.remember_tail_leaf_hint(page_id)?;
                        if let Some(old_overflow) = old_overflow
                            && Some(old_overflow.head_page_id) != reused_overflow_head
                        {
                            self.free_overflow_extent(
                                old_overflow.head_page_id,
                                old_overflow.total_len as usize,
                            )?;
                        }
                        return Ok(None);
                    }
                    RawLeafUpsertResult::NeedSplit => {}
                }

                let page_raw = self.pages.get(&page_id).cloned().ok_or_else(|| {
                    BTreeError::Corrupt(format!("page {} missing during insert", page_id))
                })?;
                let page = decode_page(&page_raw, self.options.page_size)?;
                let Page::Leaf { mut entries, next } = page else {
                    return Err(BTreeError::Corrupt(format!(
                        "expected leaf page {}, found non-leaf during split fallback",
                        page_id
                    )));
                };

                match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                    Ok(idx) => {
                        let old_value = std::mem::replace(&mut entries[idx].1, new_value);
                        match old_value {
                            ValueCell::Overflow { head_page_id, .. }
                                if Some(head_page_id) == reused_overflow_head => {}
                            other => self.free_value_cell(other)?,
                        }
                    }
                    Err(idx) => entries.insert(idx, (key.to_vec(), new_value)),
                }

                let candidate = Page::Leaf { entries, next };
                if page_fits(&candidate, self.options.page_size)? {
                    self.set_dirty_page(page_id, candidate)?;
                    return Ok(None);
                }
                let Page::Leaf { entries, next } = candidate else {
                    unreachable!("candidate is constructed as a leaf above")
                };

                if entries.len() < 2 {
                    return Err(BTreeError::InvalidOptions(
                        "single leaf entry exceeds page size".to_string(),
                    ));
                }

                let (split_key, mut left_page, right_page) =
                    choose_leaf_split(entries, next, self.options.page_size)?;
                let right_page_id = self.alloc_page_near(page_id);
                if let Page::Leaf { next, .. } = &mut left_page {
                    *next = Some(right_page_id);
                }

                self.set_dirty_page(page_id, left_page)?;
                self.set_dirty_page(right_page_id, right_page)?;
                Ok(Some(SplitResult {
                    separator: split_key,
                    right_page_id,
                }))
            }
            RawDescendStep::Child(child_page_id) => {
                let split = self.insert_recursive(child_page_id, key, value)?;
                let Some(split) = split else {
                    return Ok(None);
                };

                // Split path: recursion may have evicted this clean parent, so
                // reload before decoding for the structural update.
                let page = {
                    let raw = self.ensure_page_loaded(page_id)?;
                    decode_page(raw, page_size)?
                };
                let Page::Internal {
                    mut keys,
                    mut children,
                } = page
                else {
                    return Err(BTreeError::Corrupt(format!(
                        "expected internal page {}, found non-internal during insert",
                        page_id
                    )));
                };
                let child_idx = child_index(&keys, key);

                keys.insert(child_idx, split.separator);
                children.insert(child_idx + 1, split.right_page_id);

                let candidate = Page::Internal { keys, children };
                if page_fits(&candidate, self.options.page_size)? {
                    self.set_dirty_page(page_id, candidate)?;
                    return Ok(None);
                }
                let Page::Internal { keys, children } = candidate else {
                    unreachable!("candidate is constructed as an internal page above")
                };

                if keys.len() < 2 {
                    return Err(BTreeError::InvalidOptions(
                        "internal split requires at least two keys".to_string(),
                    ));
                }

                let (promoted, left_page, right_page) =
                    choose_internal_split(keys, children, self.options.page_size)?;

                let right_page_id = self.alloc_page_near(page_id);
                self.set_dirty_page(page_id, left_page)?;
                self.set_dirty_page(right_page_id, right_page)?;

                Ok(Some(SplitResult {
                    separator: promoted,
                    right_page_id,
                }))
            }
            RawDescendStep::Other(kind) => Err(BTreeError::Corrupt(format!(
                "insert reached {:?} page {}",
                kind, page_id
            ))),
        }
    }

    fn delete_recursive(&mut self, page_id: PageId, key: &[u8]) -> Result<bool, BTreeError> {
        let page_size = self.options.page_size;
        let step = {
            let raw = self.ensure_page_loaded(page_id)?;
            raw_descend_step(raw, page_size, key)?
        };

        match step {
            RawDescendStep::Leaf => {
                let deleted = {
                    let raw = self.pages.get_mut(&page_id).ok_or_else(|| {
                        BTreeError::Corrupt(format!("page {} missing during delete", page_id))
                    })?;
                    raw_leaf_delete_in_place(raw, self.options.page_size, key)?
                };
                match deleted {
                    RawLeafDeleteResult::NotFound => Ok(false),
                    RawLeafDeleteResult::Deleted { old_overflow, .. } => {
                        self.mark_dirty_loaded_page(page_id);
                        if let Some(old_overflow) = old_overflow {
                            self.free_overflow_extent(
                                old_overflow.head_page_id,
                                old_overflow.total_len as usize,
                            )?;
                        }
                        Ok(true)
                    }
                }
            }
            RawDescendStep::Child(child_page_id) => self.delete_recursive(child_page_id, key),
            RawDescendStep::Other(kind) => Err(BTreeError::Corrupt(format!(
                "delete reached {:?} page {}",
                kind, page_id
            ))),
        }
    }

    fn read_overflow_value(
        &mut self,
        head_page_id: PageId,
        expected_len: usize,
    ) -> Result<Vec<u8>, BTreeError> {
        let max_chunk = self.options.page_size;
        let page_count = overflow_pages_for_len(expected_len, max_chunk);

        if expected_len >= OVERFLOW_DIRECT_READ_MIN_BYTES
            && !self.overflow_extent_has_dirty_pages(head_page_id, page_count)?
        {
            return self.read_overflow_extent_direct(head_page_id, expected_len, page_count);
        }

        let mut out = Vec::with_capacity(expected_len);
        self.ensure_overflow_extent_loaded(head_page_id, page_count)?;

        for idx in 0..page_count {
            let page_id = head_page_id.checked_add(idx as u64).ok_or_else(|| {
                BTreeError::Corrupt("overflow extent page id overflow".to_string())
            })?;
            let page_raw = self.pages.get(&page_id).ok_or_else(|| {
                BTreeError::Corrupt(format!("overflow page {} missing in memory", page_id))
            })?;
            if page_raw.len() != self.options.page_size {
                return Err(BTreeError::Corrupt(format!(
                    "overflow page {} has invalid length {}",
                    page_id,
                    page_raw.len()
                )));
            }
            let remaining = expected_len.saturating_sub(out.len());
            if remaining == 0 {
                break;
            }
            let take = remaining.min(self.options.page_size);
            out.extend_from_slice(&page_raw[..take]);
        }

        if out.len() != expected_len {
            return Err(BTreeError::Corrupt(format!(
                "overflow payload truncated: expected {}, found {}",
                expected_len,
                out.len()
            )));
        }
        Ok(out)
    }

    fn read_overflow_extent_direct(
        &self,
        head_page_id: PageId,
        expected_len: usize,
        page_count: usize,
    ) -> Result<Vec<u8>, BTreeError> {
        if page_count == 0 {
            return Ok(Vec::new());
        }
        if head_page_id < 2 {
            return Err(BTreeError::Corrupt(format!(
                "overflow page id {} out of bounds",
                head_page_id
            )));
        }
        let last_page_id = head_page_id
            .checked_add((page_count - 1) as u64)
            .ok_or_else(|| BTreeError::Corrupt("overflow extent page id overflow".to_string()))?;
        if last_page_id >= self.total_pages {
            return Err(BTreeError::Corrupt(format!(
                "overflow extent [{}..={}] exceeds total_pages {}",
                head_page_id, last_page_id, self.total_pages
            )));
        }

        let run_bytes = page_count
            .checked_mul(self.options.page_size)
            .ok_or_else(|| BTreeError::Io("overflow extent read size overflow".to_string()))?;
        let offset = head_page_id
            .checked_mul(self.options.page_size as u64)
            .ok_or_else(|| BTreeError::Corrupt("page offset overflow".to_string()))?;

        let mut raw = vec![0u8; run_bytes];
        self.file.read_exact_at(offset, &mut raw)?;

        let mut out = Vec::with_capacity(expected_len);
        for i in 0..page_count {
            let start = i * self.options.page_size;
            let remaining = expected_len.saturating_sub(out.len());
            if remaining == 0 {
                break;
            }
            let take = remaining.min(self.options.page_size);
            out.extend_from_slice(&raw[start..start + take]);
        }

        if out.len() != expected_len {
            return Err(BTreeError::Corrupt(format!(
                "overflow payload truncated: expected {}, found {}",
                expected_len,
                out.len()
            )));
        }
        Ok(out)
    }

    fn overflow_extent_has_dirty_pages(
        &self,
        head_page_id: PageId,
        page_count: usize,
    ) -> Result<bool, BTreeError> {
        for idx in 0..page_count {
            let page_id = head_page_id.checked_add(idx as u64).ok_or_else(|| {
                BTreeError::Corrupt("overflow extent page id overflow".to_string())
            })?;
            if self.dirty_pages.contains(&page_id) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn free_value_cell(&mut self, value: ValueCell) -> Result<(), BTreeError> {
        if let ValueCell::Overflow {
            head_page_id,
            total_len,
        } = value
        {
            self.free_overflow_extent(head_page_id, total_len as usize)?;
        }
        Ok(())
    }

    fn free_overflow_extent(
        &mut self,
        head_page_id: PageId,
        total_len: usize,
    ) -> Result<(), BTreeError> {
        let page_count = overflow_pages_for_len(total_len, self.options.page_size);

        for idx in 0..page_count {
            let page_id = head_page_id.checked_add(idx as u64).ok_or_else(|| {
                BTreeError::Corrupt("overflow extent page id overflow".to_string())
            })?;
            self.remove_page(page_id);
            self.add_free_page(page_id);
        }

        Ok(())
    }

    fn ensure_overflow_extent_loaded(
        &mut self,
        head_page_id: PageId,
        page_count: usize,
    ) -> Result<(), BTreeError> {
        if page_count == 0 {
            return Ok(());
        }

        let mut first_missing_idx = None;
        for idx in 0..page_count {
            let page_id = head_page_id.checked_add(idx as u64).ok_or_else(|| {
                BTreeError::Corrupt("overflow extent page id overflow".to_string())
            })?;
            if self.pages.contains_key(&page_id) {
                self.touch_page(page_id);
            } else {
                first_missing_idx = Some(idx);
                break;
            }
        }

        let Some(start_idx) = first_missing_idx else {
            return Ok(());
        };
        let start_page_id = head_page_id
            .checked_add(start_idx as u64)
            .ok_or_else(|| BTreeError::Corrupt("overflow extent page id overflow".to_string()))?;
        let load_pages = page_count.saturating_sub(start_idx);
        let run_bytes = load_pages
            .checked_mul(self.options.page_size)
            .ok_or_else(|| BTreeError::Io("overflow extent read size overflow".to_string()))?;
        let offset = start_page_id
            .checked_mul(self.options.page_size as u64)
            .ok_or_else(|| BTreeError::Corrupt("page offset overflow".to_string()))?;

        let mut raw = vec![0u8; run_bytes];
        self.file.read_exact_at(offset, &mut raw)?;

        for i in 0..load_pages {
            let page_id = start_page_id.checked_add(i as u64).ok_or_else(|| {
                BTreeError::Corrupt("overflow extent page id overflow".to_string())
            })?;
            if self.pages.contains_key(&page_id) {
                self.touch_page(page_id);
                continue;
            }

            let start = i * self.options.page_size;
            let end = start + self.options.page_size;
            let page_raw = &raw[start..end];
            self.pages.insert(page_id, page_raw.to_vec());
            self.blob_pages.insert(page_id);
            self.touch_page(page_id);
        }
        self.evict_pages_if_needed(Some(head_page_id));
        Ok(())
    }

    fn build_value_cell(&mut self, value: &[u8]) -> Result<ValueCell, BTreeError> {
        if value.len() <= self.options.overflow_threshold {
            return Ok(ValueCell::Inline(value.to_vec()));
        }

        let max_chunk = self.options.page_size;

        let total_len = u32::try_from(value.len())
            .map_err(|_| BTreeError::InvalidOptions("value too large".to_string()))?;
        let page_count = overflow_pages_for_len(value.len(), max_chunk);
        let head_page_id = self.alloc_extent_pages(page_count)?;

        let mut remaining = value;
        for idx in 0..page_count {
            let consume = remaining.len().min(max_chunk);
            let chunk = &remaining[..consume];
            remaining = &remaining[consume..];

            let page_id = head_page_id.checked_add(idx as u64).ok_or_else(|| {
                BTreeError::Corrupt("overflow extent page id overflow".to_string())
            })?;
            let raw = build_blob_page(chunk, self.options.page_size)?;
            self.set_dirty_blob_page(page_id, raw);
        }

        Ok(ValueCell::Overflow {
            head_page_id,
            total_len,
        })
    }

    fn build_value_cell_for_existing(
        &mut self,
        existing_overflow: Option<OverflowRef>,
        value: &[u8],
    ) -> Result<(ValueCell, Option<PageId>), BTreeError> {
        if value.len() <= self.options.overflow_threshold {
            return Ok((ValueCell::Inline(value.to_vec()), None));
        }
        if let Some(existing) = existing_overflow {
            let max_chunk = self.options.page_size;
            let existing_pages = overflow_pages_for_len(existing.total_len as usize, max_chunk);
            let needed_pages = overflow_pages_for_len(value.len(), max_chunk);
            if needed_pages <= existing_pages {
                let cell =
                    self.rewrite_overflow_extent(existing.head_page_id, existing_pages, value)?;
                return Ok((cell, Some(existing.head_page_id)));
            }
        }
        Ok((self.build_value_cell(value)?, None))
    }

    fn rewrite_overflow_extent(
        &mut self,
        head_page_id: PageId,
        existing_pages: usize,
        value: &[u8],
    ) -> Result<ValueCell, BTreeError> {
        let max_chunk = self.options.page_size;

        let total_len = u32::try_from(value.len())
            .map_err(|_| BTreeError::InvalidOptions("value too large".to_string()))?;
        let needed_pages = value.len().div_ceil(max_chunk).max(1);
        if needed_pages > existing_pages {
            return Err(BTreeError::Corrupt(
                "overflow extent rewrite requires additional pages".to_string(),
            ));
        }

        let mut remaining = value;
        for idx in 0..needed_pages {
            let consume = remaining.len().min(max_chunk);
            let chunk = &remaining[..consume];
            remaining = &remaining[consume..];
            let page_id = head_page_id.checked_add(idx as u64).ok_or_else(|| {
                BTreeError::Corrupt("overflow extent page id overflow".to_string())
            })?;
            let raw = build_blob_page(chunk, self.options.page_size)?;
            self.set_dirty_blob_page(page_id, raw);
        }

        for idx in needed_pages..existing_pages {
            let page_id = head_page_id.checked_add(idx as u64).ok_or_else(|| {
                BTreeError::Corrupt("overflow extent page id overflow".to_string())
            })?;
            self.remove_page(page_id);
            self.add_free_page(page_id);
        }

        Ok(ValueCell::Overflow {
            head_page_id,
            total_len,
        })
    }

    fn find_leaf_page_id(&mut self, key: &[u8]) -> Result<Option<PageId>, BTreeError> {
        let mut current = match self.root_page_id {
            Some(id) => id,
            None => return Ok(None),
        };

        let page_size = self.options.page_size;
        loop {
            let raw = self.ensure_page_loaded(current)?;
            match raw_descend_step(raw, page_size, key)? {
                RawDescendStep::Leaf => return Ok(Some(current)),
                RawDescendStep::Child(child) => current = child,
                RawDescendStep::Other(kind) => {
                    return Err(BTreeError::Corrupt(format!(
                        "unexpected {:?} page {} in tree path",
                        kind, current
                    )));
                }
            }
        }
    }

    /// Checks the hint slots for a cached leaf whose current key span covers
    /// `key`. Safe across mutations because `raw_leaf_covers_key` re-validates
    /// the page's current bytes on every use; `remove_page` drops freed pages
    /// from the slots and WAL replay clears them all.
    ///
    /// `prefilter_mru` extends the cached-span prefilter to slot 0: latency
    /// -sensitive callers (the put fast path, range starts) skip the page
    /// lookup when even the most recent hint cannot cover `key`, while plain
    /// point lookups give slot 0 the authoritative check directly because it
    /// usually hits.
    fn hinted_leaf_for_key(
        &mut self,
        key: &[u8],
        prefilter_mru: bool,
    ) -> Result<Option<PageId>, BTreeError> {
        for idx in 0..LEAF_HINT_SLOTS {
            if self.leaf_hints[idx].is_empty()
                || ((prefilter_mru || idx > 0) && !self.leaf_hints[idx].could_cover(key))
            {
                continue;
            }
            let page_id = self.leaf_hints[idx].page_id;
            let covers = match self.pages.get(&page_id) {
                Some(raw) => raw_leaf_covers_key(raw, self.options.page_size, key)?,
                None => false,
            };
            if covers {
                self.leaf_hints[..=idx].rotate_right(1);
                self.touch_page(page_id);
                return Ok(Some(page_id));
            }
        }
        Ok(None)
    }

    /// Resolves the leaf for `key`, trying the hint slots before paying for a
    /// full root-to-leaf descent.
    fn leaf_for_key(&mut self, key: &[u8]) -> Result<Option<PageId>, BTreeError> {
        if let Some(page_id) = self.hinted_leaf_for_key(key, false)? {
            return Ok(Some(page_id));
        }
        let leaf = self.find_leaf_page_id(key)?;
        if let Some(page_id) = leaf {
            self.remember_leaf_hint(page_id)?;
        }
        Ok(leaf)
    }

    /// Resolves the leaf where a range scan for `start` should begin. Unlike
    /// point lookups, a range only needs the leaf where keys >= `start` begin,
    /// so when `start` falls in the empty gap between a hinted leaf and its
    /// cached successor, the successor is provably the right starting leaf and
    /// the descent can be skipped.
    fn leaf_for_range_start(&mut self, start: &[u8]) -> Result<Option<PageId>, BTreeError> {
        if let Some(page_id) = self.hinted_leaf_for_key(start, true)? {
            return Ok(Some(page_id));
        }
        for idx in 0..LEAF_HINT_SLOTS {
            if self.leaf_hints[idx].is_empty() || self.leaf_hints[idx].first_key.as_slice() > start
            {
                continue;
            }
            let page_id = self.leaf_hints[idx].page_id;
            let Some(raw) = self.pages.get(&page_id) else {
                continue;
            };
            let Some(span) = raw_leaf_span(raw, self.options.page_size)? else {
                continue;
            };
            if span.first_key > start || start <= span.last_key {
                // Not a floor for `start` (covers would have caught the
                // in-span case above).
                continue;
            }
            // `start` is past this leaf; if the successor is cached and its
            // first key is past `start` too, the gap in between is empty and
            // the successor is where keys >= start begin. A tail leaf is
            // already handled by the covers check above.
            let Some(next_id) = span.next_page_id else {
                continue;
            };
            let Some(next_raw) = self.pages.get(&next_id) else {
                continue;
            };
            let Some(next_span) = raw_leaf_span(next_raw, self.options.page_size)? else {
                continue;
            };
            if next_span.first_key > start {
                self.touch_page(next_id);
                return Ok(Some(next_id));
            }
        }
        let leaf = self.find_leaf_page_id(start)?;
        if let Some(page_id) = leaf {
            self.remember_leaf_hint(page_id)?;
        }
        Ok(leaf)
    }

    fn remember_leaf_hint_if_not_mru(&mut self, page_id: PageId) -> Result<(), BTreeError> {
        if self.leaf_hints[0].page_id == page_id {
            return Ok(());
        }
        self.remember_leaf_hint(page_id)
    }

    fn remember_leaf_hint(&mut self, page_id: PageId) -> Result<(), BTreeError> {
        let Some(raw) = self.pages.get(&page_id) else {
            return Ok(());
        };
        let Some(span) = raw_leaf_span(raw, self.options.page_size)? else {
            return Ok(());
        };
        Self::remember_leaf_span(&mut self.leaf_hints, page_id, &span);
        Ok(())
    }

    fn remember_tail_leaf_hint(&mut self, page_id: PageId) -> Result<(), BTreeError> {
        let Some(raw) = self.pages.get(&page_id) else {
            return Ok(());
        };
        let Some(span) = raw_leaf_span(raw, self.options.page_size)? else {
            return Ok(());
        };
        if span.next_page_id.is_some() {
            return Ok(());
        }
        Self::remember_leaf_span(&mut self.leaf_hints, page_id, &span);
        Ok(())
    }

    fn remember_leaf_span(
        leaf_hints: &mut [LeafHint; LEAF_HINT_SLOTS],
        page_id: PageId,
        span: &RawLeafSpan<'_>,
    ) {
        if let Some(pos) = leaf_hints.iter().position(|entry| entry.page_id == page_id) {
            leaf_hints[pos].set(page_id, span);
            leaf_hints[..=pos].rotate_right(1);
        } else {
            leaf_hints.rotate_right(1);
            leaf_hints[0].set(page_id, span);
        }
    }

    fn forget_leaf_hint(&mut self, page_id: PageId) {
        for slot in &mut self.leaf_hints {
            if slot.page_id == page_id {
                slot.clear();
            }
        }
    }

    fn clear_leaf_hints(&mut self) {
        for slot in &mut self.leaf_hints {
            slot.clear();
        }
    }

    fn ensure_page_loaded(&mut self, page_id: PageId) -> Result<&[u8], BTreeError> {
        if self.pages.contains_key(&page_id) {
            self.touch_page(page_id);
        } else {
            self.read_page_run_from_disk(page_id)?;
        }
        match self.pages.get(&page_id) {
            Some(raw) => Ok(raw),
            None => Err(BTreeError::Corrupt(format!(
                "page {} missing after disk load",
                page_id
            ))),
        }
    }

    fn page_kind(&self, page_id: PageId) -> Result<PageKind, BTreeError> {
        let raw = self
            .pages
            .get(&page_id)
            .ok_or_else(|| BTreeError::Corrupt(format!("page {} missing", page_id)))?;
        raw_page_kind(raw, self.options.page_size)
    }

    fn raw_page_bytes(&self, page_id: PageId) -> Result<&[u8], BTreeError> {
        match self.pages.get(&page_id) {
            Some(raw) => Ok(raw),
            None => Err(BTreeError::Corrupt(format!("page {} missing", page_id))),
        }
    }

    fn load_freelist_from_disk(&mut self, head_page_id: PageId) -> Result<(), BTreeError> {
        let mut current = head_page_id;
        let mut seen = OpfsSet::default();

        while current != 0 {
            if !seen.insert(current) {
                return Err(BTreeError::Corrupt(
                    "freelist pages contain a cycle".to_string(),
                ));
            }

            let raw = self.read_page_raw_from_disk(current)?;
            let (ids, next) = raw_freelist_page(&raw, self.options.page_size)?;
            self.freelist_meta_pages.push(current);
            for id in ids {
                self.add_free_page(id);
            }
            current = next.unwrap_or(0);
        }

        Ok(())
    }

    fn read_page_raw_from_disk(&self, page_id: PageId) -> Result<Vec<u8>, BTreeError> {
        if page_id < 2 || page_id >= self.total_pages {
            return Err(BTreeError::Corrupt(format!(
                "page id {} out of bounds for total_pages {}",
                page_id, self.total_pages
            )));
        }

        let offset = page_id
            .checked_mul(self.options.page_size as u64)
            .ok_or_else(|| BTreeError::Corrupt("page offset overflow".to_string()))?;

        let mut raw = vec![0u8; self.options.page_size];
        self.file.read_exact_at(offset, &mut raw)?;
        let _ = validate_page(&raw, self.options.page_size)?;
        Ok(raw)
    }

    fn read_page_run_from_disk(&mut self, page_id: PageId) -> Result<(), BTreeError> {
        if self.options.read_coalesce_pages <= 1 {
            let raw = self.read_page_raw_from_disk(page_id)?;
            self.cache_loaded_raw_page(page_id, raw);
            return Ok(());
        }

        if page_id < 2 || page_id >= self.total_pages {
            return Err(BTreeError::Corrupt(format!(
                "page id {} out of bounds for total_pages {}",
                page_id, self.total_pages
            )));
        }

        let readable_pages = self.total_pages.min(self.persisted_pages);
        if page_id >= readable_pages {
            return Err(BTreeError::Corrupt(format!(
                "page id {} out of bounds for persisted pages {}",
                page_id, readable_pages
            )));
        }

        let run_pages =
            (readable_pages - page_id).min(self.options.read_coalesce_pages as u64) as usize;
        let run_bytes = run_pages
            .checked_mul(self.options.page_size)
            .ok_or_else(|| BTreeError::Io("read run size overflow".to_string()))?;
        let offset = page_id
            .checked_mul(self.options.page_size as u64)
            .ok_or_else(|| BTreeError::Corrupt("page offset overflow".to_string()))?;

        let mut raw = vec![0u8; run_bytes];
        self.file.read_exact_at(offset, &mut raw)?;
        let allowance = self.max_cached_pages() / 4;

        for i in 0..run_pages {
            let current_page_id = page_id + i as u64;
            if self.pages.contains_key(&current_page_id) {
                continue;
            }

            let start = i * self.options.page_size;
            let end = start + self.options.page_size;
            let page_raw = &raw[start..end];

            let validate = validate_page(page_raw, self.options.page_size);
            if i == 0 {
                validate?;
            } else if validate.is_err() {
                break;
            };
            self.pages.insert(current_page_id, page_raw.to_vec());
            self.touch_page(current_page_id);
            self.evict_pages_if_needed_with_allowance(Some(page_id), allowance);
        }
        self.evict_pages_if_needed(Some(page_id));

        Ok(())
    }

    // Checkpoint path: copy the latest dirty/WAL-pinned page bytes back to
    // their home locations, so the checkpointed region becomes current.
    fn write_pages_to_disk(
        &mut self,
        dirty_page_ids: &[PageId],
        freelist_pages: &[(PageId, Page)],
    ) -> Result<(), BTreeError> {
        self.refresh_dirty_page_checksums(dirty_page_ids)?;

        let mut max_page_id = 1u64;
        if let Some(max_live) = self.pages.keys().max().copied() {
            max_page_id = max_page_id.max(max_live);
        }
        if let Some(max_free) = self.free_set.iter().max().copied() {
            max_page_id = max_page_id.max(max_free);
        }
        if let Some(max_freelist) = freelist_pages.iter().map(|(id, _)| *id).max() {
            max_page_id = max_page_id.max(max_freelist);
        }

        self.total_pages = self.total_pages.max(max_page_id.saturating_add(1)).max(2);

        let required_len = self
            .total_pages
            .checked_mul(self.options.page_size as u64)
            .ok_or_else(|| BTreeError::Io("file size overflow".to_string()))?;
        if self.persisted_pages < self.total_pages {
            self.file.truncate(required_len)?;
            self.persisted_pages = self.total_pages;
        }

        let checkpoint_pages = self.collect_pages_for_write(dirty_page_ids, freelist_pages)?;
        if checkpoint_pages.is_empty() {
            return Ok(());
        }

        let page_size = self.options.page_size;
        let mut idx = 0usize;
        while idx < checkpoint_pages.len() {
            let start_page_id = checkpoint_pages[idx].page_id;
            let mut end = idx + 1;
            while end < checkpoint_pages.len() {
                let prev_page_id = checkpoint_pages[end - 1].page_id;
                if prev_page_id.checked_add(1) != Some(checkpoint_pages[end].page_id) {
                    break;
                }
                end += 1;
            }

            let run_len = end - idx;
            let run_capacity = run_len
                .checked_mul(page_size)
                .ok_or_else(|| BTreeError::Io("checkpoint run buffer size overflow".to_string()))?;
            let mut run = Vec::with_capacity(run_capacity);
            for write in &checkpoint_pages[idx..end] {
                run.extend_from_slice(&write.raw);
            }

            let offset = start_page_id.checked_mul(page_size as u64).ok_or_else(|| {
                BTreeError::Io("checkpoint run write offset overflow".to_string())
            })?;
            self.file.write_all_at(offset, &run)?;

            idx = end;
        }

        Ok(())
    }

    fn refresh_dirty_page_checksums(&mut self, page_ids: &[PageId]) -> Result<(), BTreeError> {
        for page_id in page_ids {
            // Only pages mutated since the last flush can carry a deferred
            // checksum; WAL-pinned pages in a checkpoint batch were already
            // refreshed by the flush that wrote them.
            if !self.dirty_pages.contains(page_id) || self.blob_pages.contains(page_id) {
                continue;
            }
            if let Some(raw) = self.pages.get_mut(page_id) {
                refresh_page_checksum(raw, self.options.page_size)?;
            }
        }
        Ok(())
    }

    fn collect_pages_for_write<'a>(
        &'a self,
        dirty_page_ids: &[PageId],
        freelist_pages: &'a [(PageId, Page)],
    ) -> Result<Vec<PageWrite<'a>>, BTreeError> {
        let mut writes: Vec<PageWrite<'a>> =
            Vec::with_capacity(dirty_page_ids.len() + freelist_pages.len());
        for page_id in dirty_page_ids {
            if let Some(raw) = self.pages.get(page_id) {
                self.validate_writable_page_id(*page_id)?;
                let is_blob = self.blob_pages.contains(page_id);
                if is_blob {
                    if raw.len() != self.options.page_size {
                        return Err(BTreeError::Corrupt(format!(
                            "blob page {} has invalid length {}",
                            page_id,
                            raw.len()
                        )));
                    }
                } else {
                    let _ = validate_page(raw, self.options.page_size)?;
                }
                writes.push(PageWrite {
                    page_id: *page_id,
                    is_blob,
                    is_freelist: false,
                    raw: Cow::Borrowed(raw.as_slice()),
                });
            }
        }
        for (page_id, page) in freelist_pages {
            self.validate_writable_page_id(*page_id)?;
            writes.push(PageWrite {
                page_id: *page_id,
                is_blob: false,
                is_freelist: true,
                raw: Cow::Owned(encode_page(page, self.options.page_size)?),
            });
        }

        writes.sort_unstable_by_key(|write| write.page_id);
        for pair in writes.windows(2) {
            if pair[0].page_id == pair[1].page_id {
                return Err(BTreeError::Corrupt(format!(
                    "duplicate page {} in write batch",
                    pair[0].page_id
                )));
            }
        }
        Ok(writes)
    }

    fn truncate_wal_tail(&mut self) -> Result<(), BTreeError> {
        if self.persisted_pages <= self.total_pages {
            return Ok(());
        }
        let required_len = self
            .total_pages
            .checked_mul(self.options.page_size as u64)
            .ok_or_else(|| BTreeError::Io("checkpoint truncate length overflow".to_string()))?;
        self.file.truncate(required_len)?;
        self.file.flush()?;
        self.persisted_pages = self.total_pages;
        Ok(())
    }

    fn replay_wal(&mut self) -> Result<(), BTreeError> {
        let mut cursor = self.active.total_pages.max(2);
        while cursor < self.persisted_pages {
            let Some((header, frames, next_cursor)) = wal::read_commit(
                &self.file,
                self.options.page_size,
                cursor,
                self.persisted_pages,
            )?
            else {
                self.persisted_pages = cursor;
                break;
            };
            self.apply_wal_commit(header, frames)?;
            cursor = next_cursor;
        }
        Ok(())
    }

    fn apply_wal_commit(
        &mut self,
        header: WalHeader,
        frames: Vec<WalFrame>,
    ) -> Result<(), BTreeError> {
        self.clear_leaf_hints();
        if header.total_pages < 2 {
            return Err(BTreeError::Corrupt(
                "WAL total_pages must be >= 2".to_string(),
            ));
        }
        self.total_pages = header.total_pages;
        self.root_page_id = (header.root_page_id != 0).then_some(header.root_page_id);
        self.free_pages.clear();
        self.free_set.clear();
        self.freelist_meta_pages.clear();

        for frame in frames {
            self.validate_writable_page_id(frame.page_id)?;
            if frame.raw.len() != self.options.page_size {
                return Err(BTreeError::Corrupt(format!(
                    "WAL frame page {} has invalid length {}",
                    frame.page_id,
                    frame.raw.len()
                )));
            }
            if frame.is_blob {
                self.blob_pages.insert(frame.page_id);
            } else {
                let _ = validate_page(&frame.raw, self.options.page_size)?;
                self.blob_pages.remove(&frame.page_id);
            }
            self.pages.insert(frame.page_id, frame.raw);
            if frame.is_freelist {
                self.dirty_pages.remove(&frame.page_id);
                self.wal_pages.remove(&frame.page_id);
            } else {
                self.wal_pages.insert(frame.page_id);
            }
            self.touch_page(frame.page_id);
        }

        if header.freelist_head_page_id != 0 {
            self.load_freelist_from_cached_pages(header.freelist_head_page_id)?;
        }
        self.sanitize_free_pages();
        self.active = Superblock::new(
            self.options.page_size as u32,
            header.generation,
            header.root_page_id,
            header.freelist_head_page_id,
            header.total_pages,
        );
        self.freelist_dirty = false;
        // Replay inserts frames into the cache like any other load path, so it
        // owes the same eviction check as the other insertion sites. This runs
        // after the freelist load because freelist frames are clean and
        // evictable the moment they are applied.
        self.evict_pages_if_needed(None);
        Ok(())
    }

    fn load_freelist_from_cached_pages(&mut self, head_page_id: PageId) -> Result<(), BTreeError> {
        let mut current = head_page_id;
        let mut seen = OpfsSet::default();

        while current != 0 {
            if !seen.insert(current) {
                return Err(BTreeError::Corrupt(
                    "freelist pages contain a cycle".to_string(),
                ));
            }

            let raw = self.pages.get(&current).ok_or_else(|| {
                BTreeError::Corrupt(format!("WAL freelist page {} missing", current))
            })?;
            let (ids, next) = raw_freelist_page(raw, self.options.page_size)?;
            self.freelist_meta_pages.push(current);
            for id in ids {
                self.add_free_page(id);
            }
            current = next.unwrap_or(0);
        }

        Ok(())
    }

    fn validate_writable_page_id(&self, page_id: PageId) -> Result<(), BTreeError> {
        if page_id < 2 {
            return Err(BTreeError::Corrupt(format!(
                "attempt to write reserved page {}",
                page_id
            )));
        }
        if page_id >= self.total_pages {
            return Err(BTreeError::Corrupt(format!(
                "attempt to write page {} beyond total_pages {}",
                page_id, self.total_pages
            )));
        }
        Ok(())
    }

    fn set_dirty_page(&mut self, page_id: PageId, page: Page) -> Result<(), BTreeError> {
        let raw = encode_page(&page, self.options.page_size)?;
        self.pages.insert(page_id, raw);
        self.mark_dirty_loaded_page(page_id);
        self.evict_pages_if_needed(Some(page_id));
        Ok(())
    }

    fn set_dirty_blob_page(&mut self, page_id: PageId, raw: Vec<u8>) {
        self.pages.insert(page_id, raw);
        self.blob_pages.insert(page_id);
        self.mark_dirty_loaded_page(page_id);
        self.evict_pages_if_needed(Some(page_id));
    }

    // Marking an already-cached page dirty cannot grow the cache, so it must
    // not trigger an eviction scan; only the insertion paths do that.
    fn mark_dirty_loaded_page(&mut self, page_id: PageId) {
        self.dirty_pages.insert(page_id);
        self.touch_page(page_id);
    }

    fn remove_page(&mut self, page_id: PageId) -> Option<Vec<u8>> {
        self.forget_leaf_hint(page_id);
        self.dirty_pages.remove(&page_id);
        self.wal_pages.remove(&page_id);
        self.page_access_epoch.remove(&page_id);
        self.blob_pages.remove(&page_id);
        self.pages.remove(&page_id)
    }

    fn cache_loaded_raw_page(&mut self, page_id: PageId, raw: Vec<u8>) {
        self.pages.insert(page_id, raw);
        self.touch_page(page_id);
        self.evict_pages_if_needed(Some(page_id));
    }

    fn build_freelist_pages(&mut self) -> Result<(PageId, Vec<(PageId, Page)>), BTreeError> {
        let old_meta_pages = std::mem::take(&mut self.freelist_meta_pages);
        for page_id in old_meta_pages {
            self.add_free_page(page_id);
        }

        self.sanitize_free_pages();

        let mut free_ids: Vec<PageId> = self.free_set.iter().copied().collect();
        free_ids.sort_unstable();

        let capacity = freelist_ids_per_page(self.options.page_size)?;
        if capacity == 0 && !free_ids.is_empty() {
            return Err(BTreeError::InvalidOptions(
                "page size too small for freelist pages".to_string(),
            ));
        }

        let mut meta_count = 0usize;
        while meta_count
            .checked_mul(capacity)
            .ok_or_else(|| BTreeError::Io("freelist capacity overflow".to_string()))?
            < free_ids.len().saturating_sub(meta_count)
        {
            meta_count += 1;
        }

        let mut meta_page_ids = Vec::with_capacity(meta_count);
        for _ in 0..meta_count {
            let page_id = free_ids.pop().ok_or_else(|| {
                BTreeError::Corrupt("freelist meta page allocation underflow".to_string())
            })?;
            meta_page_ids.push(page_id);
        }
        meta_page_ids.sort_unstable();

        let remaining_free = free_ids;
        self.free_set.clear();
        self.free_pages.clear();
        for page_id in &remaining_free {
            self.free_set.insert(*page_id);
            self.free_pages.push(*page_id);
        }

        self.freelist_meta_pages = meta_page_ids.clone();
        let head_page_id = *meta_page_ids.first().unwrap_or(&0);

        let mut freelist_pages = Vec::with_capacity(meta_page_ids.len());
        for (idx, page_id) in meta_page_ids.iter().enumerate() {
            let start = idx
                .checked_mul(capacity)
                .ok_or_else(|| BTreeError::Io("freelist chunk start overflow".to_string()))?;
            let end = ((idx + 1)
                .checked_mul(capacity)
                .ok_or_else(|| BTreeError::Io("freelist chunk end overflow".to_string()))?)
            .min(remaining_free.len());
            let ids = if start < end {
                remaining_free[start..end].to_vec()
            } else {
                Vec::new()
            };
            let next = meta_page_ids.get(idx + 1).copied();
            freelist_pages.push((*page_id, Page::Freelist { ids, next }));
        }

        Ok((head_page_id, freelist_pages))
    }

    fn sanitize_free_pages(&mut self) {
        let live_page_ids: OpfsSet<PageId> = self.pages.keys().copied().collect();
        self.free_set.retain(|page_id| {
            *page_id >= 2 && *page_id < self.total_pages && !live_page_ids.contains(page_id)
        });

        self.free_pages.clear();
        self.free_pages.extend(self.free_set.iter().copied());
        self.free_pages.sort_unstable();
    }

    fn alloc_page(&mut self) -> PageId {
        while let Some(page_id) = self.free_pages.pop() {
            if self.claim_free_page(page_id) {
                tracing::trace!(page_id, reused = true, "alloc_page");
                return page_id;
            }
        }

        let page_id = self.total_pages;
        self.total_pages = self.total_pages.saturating_add(1);
        tracing::trace!(page_id, reused = false, "alloc_page");
        page_id
    }

    fn alloc_extent_pages(&mut self, page_count: usize) -> Result<PageId, BTreeError> {
        if page_count == 0 {
            return Err(BTreeError::InvalidOptions(
                "extent page_count must be > 0".to_string(),
            ));
        }
        if page_count == 1 {
            return Ok(self.alloc_page());
        }

        let mut free_ids: Vec<PageId> = self.free_set.iter().copied().collect();
        free_ids.sort_unstable();
        let mut run_start = 0u64;
        let mut run_len = 0usize;
        let mut prev = 0u64;
        for id in free_ids {
            if run_len == 0 {
                run_start = id;
                run_len = 1;
            } else if id == prev.saturating_add(1) {
                run_len = run_len.saturating_add(1);
            } else {
                run_start = id;
                run_len = 1;
            }
            prev = id;

            if run_len >= page_count {
                for i in 0..page_count {
                    let page_id = run_start.checked_add(i as u64).ok_or_else(|| {
                        BTreeError::Corrupt("extent free-run page id overflow".to_string())
                    })?;
                    self.claim_free_page(page_id);
                }
                tracing::trace!(
                    head_page_id = run_start,
                    page_count,
                    reused = true,
                    "alloc_extent_pages"
                );
                return Ok(run_start);
            }
        }

        let start = self.total_pages;
        self.total_pages = self
            .total_pages
            .checked_add(page_count as u64)
            .ok_or_else(|| {
                BTreeError::Io("total_pages overflow while allocating extent".to_string())
            })?;
        tracing::trace!(
            head_page_id = start,
            page_count,
            reused = false,
            "alloc_extent_pages"
        );
        Ok(start)
    }

    fn alloc_page_near(&mut self, preferred: PageId) -> PageId {
        for delta in 1..=ALLOC_NEAR_WINDOW {
            if let Some(hi) = preferred.checked_add(delta)
                && self.claim_free_page(hi)
            {
                return hi;
            }
            if let Some(lo) = preferred.checked_sub(delta)
                && lo >= 2
                && self.claim_free_page(lo)
            {
                return lo;
            }
        }

        if preferred.saturating_add(1) == self.total_pages {
            let page_id = self.total_pages;
            self.total_pages = self.total_pages.saturating_add(1);
            return page_id;
        }

        self.alloc_page()
    }

    fn add_free_page(&mut self, page_id: PageId) {
        if page_id < 2 {
            return;
        }
        if self.free_set.insert(page_id) {
            self.free_pages.push(page_id);
            self.freelist_dirty = true;
        }
    }

    fn claim_free_page(&mut self, page_id: PageId) -> bool {
        if self.free_set.remove(&page_id) {
            self.freelist_dirty = true;
            true
        } else {
            false
        }
    }

    fn max_cached_pages(&self) -> usize {
        (self.options.cache_bytes / self.options.page_size).max(1)
    }

    fn touch_page(&mut self, page_id: PageId) {
        self.access_epoch = self.access_epoch.wrapping_add(1);
        if self.access_epoch == 0 {
            self.access_epoch = 1;
            self.page_access_epoch.clear();
        }
        self.page_access_epoch.insert(page_id, self.access_epoch);
    }

    fn evict_pages_if_needed(&mut self, protected_page: Option<PageId>) {
        self.evict_pages_if_needed_with_allowance(protected_page, 0);
    }

    fn evict_pages_if_needed_with_allowance(
        &mut self,
        protected_page: Option<PageId>,
        over_budget_allowance: usize,
    ) {
        let max_cached_pages = self.max_cached_pages();
        // Dirty and WAL-pinned pages cannot be evicted; they raise both the
        // floor the scan can reach and the level at which it re-arms. Without
        // this, a cache pinned above the trigger rescans on every call. The
        // sum overcounts pages in both sets, which only makes the resting
        // level conservative; those pages are unevictable either way.
        let pinned = self.dirty_pages.len() + self.wal_pages.len();
        let steady_cached_pages = max_cached_pages.saturating_sub(max_cached_pages / 4).max(1);
        let resting_pages = steady_cached_pages.max(pinned.saturating_add(max_cached_pages / 8));
        let trigger = max_cached_pages
            .max(resting_pages.saturating_add(max_cached_pages / 4))
            .saturating_add(over_budget_allowance);
        if self.pages.len() <= trigger {
            return;
        }

        let root_page_id = self.root_page_id;
        let target = self.pages.len().saturating_sub(resting_pages);
        if target == 0 {
            return;
        }

        // One pass top-k selection:
        // Keep only the k worst eviction candidates in a bounded max heap.
        // Candidate ordering is (priority, access_epoch, page_id), where smaller
        // values are better eviction victims.
        let mut victims: BinaryHeap<(u8, u64, PageId)> = BinaryHeap::with_capacity(target);
        for (page_id, page) in &self.pages {
            if Some(*page_id) == protected_page
                || Some(*page_id) == root_page_id
                || self.dirty_pages.contains(page_id)
                || self.wal_pages.contains(page_id)
            {
                continue;
            }

            let priority = match raw_page_kind(page, self.options.page_size) {
                Ok(kind) => {
                    if self.options.pin_internal_pages && kind == PageKind::Internal {
                        continue;
                    }
                    eviction_priority(kind)
                }
                Err(_) => 0, // blob/raw pages are always evictable when clean
            };

            let candidate = (
                priority,
                *self.page_access_epoch.get(page_id).unwrap_or(&0),
                *page_id,
            );

            if victims.len() < target {
                victims.push(candidate);
                continue;
            }

            if let Some(worst_kept) = victims.peek()
                && candidate < *worst_kept
            {
                let _ = victims.pop();
                victims.push(candidate);
            }
        }

        for (_, _, page_id) in victims {
            self.pages.remove(&page_id);
            self.page_access_epoch.remove(&page_id);
        }
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
        write_slot_unchecked(&self.file, target_slot, self.options.page_size, next)?;
        self.file.flush()?;

        self.active_slot = target_slot;
        self.active = next;
        Ok(())
    }
}

fn child_index(keys: &[Vec<u8>], key: &[u8]) -> usize {
    keys.partition_point(|separator| separator.as_slice() <= key)
}

fn overflow_pages_for_len(total_len: usize, max_chunk: usize) -> usize {
    total_len.div_ceil(max_chunk).max(1)
}

fn build_blob_page(chunk: &[u8], page_size: usize) -> Result<Vec<u8>, BTreeError> {
    if chunk.len() > page_size {
        return Err(BTreeError::Corrupt(format!(
            "blob chunk {} exceeds page size {}",
            chunk.len(),
            page_size
        )));
    }
    let mut raw = vec![0u8; page_size];
    raw[..chunk.len()].copy_from_slice(chunk);
    Ok(raw)
}

fn ensure_page_fits(page: &Page, page_size: usize, context: &str) -> Result<(), BTreeError> {
    if page_fits(page, page_size)? {
        Ok(())
    } else {
        Err(BTreeError::InvalidOptions(format!(
            "{} does not fit in page",
            context
        )))
    }
}

fn centered_split_candidates(preferred: usize, min: usize, max: usize) -> Vec<usize> {
    debug_assert!(min <= max);

    let preferred = preferred.clamp(min, max);
    let mut out = Vec::with_capacity(max - min + 1);
    out.push(preferred);

    for offset in 1..=max - min {
        if let Some(left) = preferred.checked_sub(offset)
            && left >= min
        {
            out.push(left);
        }

        let right = preferred + offset;
        if right <= max {
            out.push(right);
        }
    }

    out
}

fn choose_leaf_split(
    entries: Vec<(Vec<u8>, ValueCell)>,
    next: Option<PageId>,
    page_size: usize,
) -> Result<(Vec<u8>, Page, Page), BTreeError> {
    debug_assert!(entries.len() >= 2);

    for split_at in centered_split_candidates(entries.len() / 2, 1, entries.len() - 1) {
        let left_page = Page::Leaf {
            entries: entries[..split_at].to_vec(),
            next: Some(1),
        };
        let right_page = Page::Leaf {
            entries: entries[split_at..].to_vec(),
            next,
        };

        if page_fits(&left_page, page_size)? && page_fits(&right_page, page_size)? {
            let split_key = match &right_page {
                Page::Leaf { entries, .. } => entries
                    .first()
                    .map(|(key, _)| key.clone())
                    .ok_or_else(|| BTreeError::Corrupt("right leaf split empty".to_string()))?,
                _ => unreachable!("constructed leaf page must remain a leaf"),
            };
            return Ok((split_key, left_page, right_page));
        }
    }

    Err(BTreeError::InvalidOptions(
        "no valid leaf split fits in page".to_string(),
    ))
}

fn choose_internal_split(
    keys: Vec<Vec<u8>>,
    children: Vec<PageId>,
    page_size: usize,
) -> Result<(Vec<u8>, Page, Page), BTreeError> {
    debug_assert!(keys.len() >= 2);
    debug_assert_eq!(children.len(), keys.len() + 1);

    for mid in centered_split_candidates(keys.len() / 2, 1, keys.len() - 1) {
        let promoted = keys[mid].clone();
        let left_page = Page::Internal {
            keys: keys[..mid].to_vec(),
            children: children[..mid + 1].to_vec(),
        };
        let right_page = Page::Internal {
            keys: keys[mid + 1..].to_vec(),
            children: children[mid + 1..].to_vec(),
        };

        if page_fits(&left_page, page_size)? && page_fits(&right_page, page_size)? {
            return Ok((promoted, left_page, right_page));
        }
    }

    Err(BTreeError::InvalidOptions(
        "no valid internal split fits in page".to_string(),
    ))
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

fn eviction_priority(kind: PageKind) -> u8 {
    match kind {
        PageKind::Overflow | PageKind::Freelist => 0,
        PageKind::Leaf => 1,
        PageKind::Internal => 2,
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

    write_slot_unchecked(file, slot, page_size, sb)
}

fn write_slot_unchecked<F: SyncFile>(
    file: &F,
    slot: SuperblockSlot,
    page_size: usize,
    sb: Superblock,
) -> Result<(), BTreeError> {
    let mut page = vec![0u8; page_size];
    sb.encode_into_page(&mut page)?;
    file.write_all_at(slot.byte_offset(page_size), &page)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::file::{MemoryFile, SyncFile};

    fn small_options() -> BTreeOptions {
        BTreeOptions {
            page_size: 4 * 1024,
            cache_bytes: 4 * 1024 * 8,
            overflow_threshold: 128,
            pin_internal_pages: false,
            read_coalesce_pages: 1,
        }
    }

    fn tiny_cache_options() -> BTreeOptions {
        BTreeOptions {
            page_size: 4 * 1024,
            cache_bytes: 4 * 1024 * 2,
            overflow_threshold: 128,
            pin_internal_pages: false,
            read_coalesce_pages: 1,
        }
    }

    fn tiny_cache_pinned_options() -> BTreeOptions {
        let mut options = tiny_cache_options();
        options.pin_internal_pages = true;
        options
    }

    #[derive(Clone, Debug)]
    struct CountingFile {
        inner: MemoryFile,
        writes: Rc<RefCell<Vec<(u64, usize)>>>,
        reads: Rc<RefCell<Vec<(u64, usize)>>>,
    }

    impl CountingFile {
        fn new() -> Self {
            Self {
                inner: MemoryFile::new(),
                writes: Rc::new(RefCell::new(Vec::new())),
                reads: Rc::new(RefCell::new(Vec::new())),
            }
        }

        fn data_write_segments(&self, page_size: usize) -> Vec<(u64, usize)> {
            let min_data_offset = (2 * page_size) as u64;
            self.writes
                .borrow()
                .iter()
                .copied()
                .filter(|(offset, _)| *offset >= min_data_offset)
                .collect()
        }

        fn data_page_write_count(&self, page_size: usize) -> usize {
            self.data_write_segments(page_size).len()
        }

        fn max_data_write_len(&self, page_size: usize) -> usize {
            self.data_write_segments(page_size)
                .into_iter()
                .map(|(_, len)| len)
                .max()
                .unwrap_or(0)
        }

        fn data_page_read_count(&self, page_size: usize) -> usize {
            let min_data_offset = (2 * page_size) as u64;
            self.reads
                .borrow()
                .iter()
                .filter(|(offset, _)| *offset >= min_data_offset)
                .count()
        }

        fn reset_io_stats(&self) {
            self.reads.borrow_mut().clear();
            self.writes.borrow_mut().clear();
        }
    }

    impl SyncFile for CountingFile {
        fn len(&self) -> Result<u64, BTreeError> {
            self.inner.len()
        }

        fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError> {
            self.reads.borrow_mut().push((offset, buf.len()));
            self.inner.read_exact_at(offset, buf)
        }

        fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError> {
            self.writes.borrow_mut().push((offset, buf.len()));
            self.inner.write_all_at(offset, buf)
        }

        fn truncate(&self, len: u64) -> Result<(), BTreeError> {
            self.inner.truncate(len)
        }

        fn flush(&self) -> Result<(), BTreeError> {
            self.inner.flush()
        }
    }

    fn corrupt_slot(file: &MemoryFile, slot: SuperblockSlot, page_size: usize) {
        let offset = slot.byte_offset(page_size) + 8;
        file.write_all_at(offset, &[0xFF, 0x00, 0xAA, 0x55])
            .expect("corrupt slot bytes");
    }

    #[test]
    fn interleaved_ops_match_btreemap_model() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, small_options()).expect("open");
        let mut model = std::collections::BTreeMap::<Vec<u8>, Vec<u8>>::new();

        // Deterministic LCG so failures reproduce.
        let mut rng_state = 0x12345678u64;
        let mut rng = move || {
            rng_state = rng_state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (rng_state >> 33) as usize
        };

        for op in 0..5_000 {
            let k = format!("key/{:06}", rng() % 800).into_bytes();
            match rng() % 10 {
                0..=4 => {
                    let v = format!("value-{}", op).into_bytes();
                    tree.put(&k, &v).expect("put");
                    model.insert(k, v);
                }
                5 => {
                    tree.delete(&k).expect("delete");
                    model.remove(&k);
                }
                6..=8 => {
                    assert_eq!(
                        tree.get(&k).expect("get"),
                        model.get(&k).cloned(),
                        "op {}",
                        op
                    );
                }
                _ => {
                    let hi = format!("key/{:06}", rng() % 800).into_bytes();
                    let (start, end) = if k <= hi {
                        (k.clone(), hi)
                    } else {
                        (hi, k.clone())
                    };
                    let got = tree.range(&start, &end, 50).expect("range");
                    let want: Vec<(Vec<u8>, Vec<u8>)> = model
                        .range(start..end)
                        .take(50)
                        .map(|(a, b)| (a.clone(), b.clone()))
                        .collect();
                    assert_eq!(got, want, "op {}", op);
                }
            }
        }
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
    fn put_get_delete_and_range_work() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, small_options()).expect("open tree");

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
    fn range_handles_gap_and_boundary_starts() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, tiny_cache_options()).expect("open");
        for i in 0..600u32 {
            tree.put(
                format!("k{:04}", i).as_bytes(),
                format!("v-{}", i).as_bytes(),
            )
            .expect("put");
        }
        // "k0042~" sorts after "k0042" and before "k0043", so this sweeps a
        // start through every inter-key gap, including gaps between leaves.
        for i in 0..599u32 {
            let start = format!("k{:04}~", i);
            let end = format!("k{:04}", i + 4);
            let got = tree
                .range(start.as_bytes(), end.as_bytes(), 10)
                .expect("range");
            let expected: Vec<(Vec<u8>, Vec<u8>)> = (i + 1..(i + 4).min(600))
                .map(|j| {
                    (
                        format!("k{:04}", j).into_bytes(),
                        format!("v-{}", j).into_bytes(),
                    )
                })
                .collect();
            assert_eq!(got, expected, "start {}", start);
        }
    }

    #[test]
    fn paginated_ranges_stitch_into_full_scan() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, tiny_cache_options()).expect("open");
        for i in 0..600u32 {
            tree.put(
                format!("k{:04}", i).as_bytes(),
                format!("v-{}", i).as_bytes(),
            )
            .expect("put");
        }
        let mut seen: Vec<Vec<u8>> = Vec::new();
        let mut cursor = b"k".to_vec();
        loop {
            let batch = tree.range(&cursor, b"l", 37).expect("range");
            let Some((last_key, _)) = batch.last() else {
                break;
            };
            // Next page starts just past the last returned key.
            cursor = last_key.clone();
            cursor.push(0);
            seen.extend(batch.into_iter().map(|(k, _)| k));
        }
        assert_eq!(seen.len(), 600);
        assert_eq!(
            seen.first().map(|k| k.as_slice()),
            Some(b"k0000".as_slice())
        );
        assert_eq!(seen.last().map(|k| k.as_slice()), Some(b"k0599".as_slice()));
        assert!(seen.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn overflow_values_round_trip() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file.clone(), small_options()).expect("open tree");

        let big = vec![7u8; 25_000];
        tree.put(b"big", &big).expect("put big");
        tree.checkpoint().expect("checkpoint");

        let mut reopened = OpfsBTree::open(file, small_options()).expect("reopen tree");
        assert_eq!(reopened.get(b"big").expect("get big"), Some(big.clone()));

        reopened.delete(b"big").expect("delete big");
        reopened.checkpoint().expect("checkpoint delete");
        assert_eq!(reopened.get(b"big").expect("get big after delete"), None);
    }

    #[test]
    fn overflow_large_patterned_value_round_trip() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file.clone(), small_options()).expect("open tree");

        let len = 1_048_576 + 513;
        let big: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        tree.put(b"big-pattern", &big).expect("put big patterned");
        assert_eq!(
            tree.get(b"big-pattern").expect("get big patterned"),
            Some(big.clone())
        );

        tree.checkpoint().expect("checkpoint");
        let mut reopened = OpfsBTree::open(file, small_options()).expect("reopen tree");
        assert_eq!(
            reopened
                .get(b"big-pattern")
                .expect("get patterned after reopen"),
            Some(big)
        );
    }

    #[test]
    fn overflow_update_reuses_existing_extent_when_page_count_matches() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, small_options()).expect("open tree");

        // Keep both values within the same 4KiB-page extent footprint.
        let value_v1 = vec![1u8; 179_000];
        let value_v2 = vec![2u8; 180_000];

        tree.put(b"k", &value_v1).expect("put v1");
        let total_pages_before = tree.total_pages;

        tree.put(b"k", &value_v2).expect("put v2");
        assert_eq!(tree.get(b"k").expect("get k"), Some(value_v2));
        assert_eq!(
            tree.total_pages, total_pages_before,
            "same-page-count overflow update should reuse existing extent pages"
        );
        assert!(
            tree.free_set.is_empty(),
            "reuse path should not free overflow pages when page count is unchanged"
        );
    }

    #[test]
    fn overflow_update_shrink_frees_tail_pages() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, small_options()).expect("open tree");

        let value_large = vec![3u8; 180_000];
        let value_small = vec![4u8; 170_000];

        tree.put(b"k", &value_large).expect("put large");
        let total_pages_before = tree.total_pages;

        tree.put(b"k", &value_small).expect("put small");
        assert_eq!(tree.get(b"k").expect("get k"), Some(value_small));
        assert_eq!(tree.total_pages, total_pages_before);
        assert!(
            !tree.free_set.is_empty(),
            "shrinking overflow value should return tail pages to free list"
        );
    }

    #[test]
    fn many_inserts_create_multiple_levels() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file.clone(), small_options()).expect("open tree");

        for i in 0..2_000u32 {
            let key = format!("k{:05}", i);
            let value = format!("v{:05}", i);
            tree.put(key.as_bytes(), value.as_bytes()).expect("put key");
        }

        assert_eq!(
            tree.get(b"k00000").expect("get first"),
            Some(b"v00000".to_vec())
        );
        assert_eq!(
            tree.get(b"k01000").expect("get middle"),
            Some(b"v01000".to_vec())
        );
        assert_eq!(
            tree.get(b"k01999").expect("get last"),
            Some(b"v01999".to_vec())
        );

        let slice = tree
            .range(b"k00500", b"k00600", 500)
            .expect("range 500..600");
        assert_eq!(slice.len(), 100);

        tree.checkpoint().expect("checkpoint");
        let mut reopened = OpfsBTree::open(file, small_options()).expect("reopen tree");
        assert_eq!(
            reopened.get(b"k01999").expect("reopen get last"),
            Some(b"v01999".to_vec())
        );
    }

    #[test]
    fn checkpoint_persists_data_across_reopen() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file.clone(), small_options()).expect("open tree");

        tree.put(b"k1", b"value1").expect("put k1");
        tree.put(b"k2", b"value2").expect("put k2");
        tree.checkpoint().expect("checkpoint");

        let mut reopened = OpfsBTree::open(file, small_options()).expect("reopen tree");
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
    fn interleaved_puts_and_reads_stay_consistent() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, tiny_cache_options()).expect("open");
        for i in 0..2_000u32 {
            let a = format!("a{:05}", i);
            let b = format!("b{:05}", i);
            tree.put(a.as_bytes(), format!("va-{}", i).as_bytes())
                .expect("put a");
            tree.put(b.as_bytes(), format!("vb-{}", i).as_bytes())
                .expect("put b");
            assert_eq!(
                tree.get(a.as_bytes()).expect("get a"),
                Some(format!("va-{}", i).into_bytes())
            );
            assert_eq!(
                tree.get(b.as_bytes()).expect("get b"),
                Some(format!("vb-{}", i).into_bytes())
            );
            if i % 3 == 0 {
                tree.delete(a.as_bytes()).expect("delete a");
                assert_eq!(tree.get(a.as_bytes()).expect("get deleted"), None);
            }
            if i % 500 == 499 {
                tree.flush_wal().expect("flush");
            }
        }
        let got = tree.range(b"a", b"b", usize::MAX).expect("range a-region");
        let expected_live = (0..2_000u32).filter(|i| i % 3 != 0).count();
        assert_eq!(got.len(), expected_live);
    }

    #[test]
    fn in_place_mutations_survive_flush_and_reopen() {
        for checkpoint in [true, false] {
            let file = MemoryFile::new();
            {
                let mut tree = OpfsBTree::open(file.clone(), tiny_cache_options()).expect("open");
                for i in 0..500u32 {
                    tree.put(
                        format!("k{:05}", i).as_bytes(),
                        format!("v-{}", i).as_bytes(),
                    )
                    .expect("put");
                }
                tree.flush_wal().expect("first flush");

                for i in 0..500u32 {
                    tree.put(
                        format!("k{:05}", i).as_bytes(),
                        format!("w-{}", i).as_bytes(),
                    )
                    .expect("overwrite");
                }
                tree.delete(b"k00000").expect("delete");
                tree.flush_wal().expect("second flush");
                if checkpoint {
                    tree.checkpoint().expect("checkpoint");
                }
            }

            let mut tree = OpfsBTree::open(file, tiny_cache_options()).expect("reopen");
            assert_eq!(tree.get(b"k00000").expect("get deleted"), None);
            for i in 1..500u32 {
                assert_eq!(
                    tree.get(format!("k{:05}", i).as_bytes()).expect("get"),
                    Some(format!("w-{}", i).into_bytes()),
                    "key {} (checkpoint={})",
                    i,
                    checkpoint
                );
            }
        }
    }

    #[test]
    fn checkpoint_writes_only_dirty_data_pages() {
        let options = small_options();
        let file = CountingFile::new();
        let mut tree = OpfsBTree::open(file.clone(), options).expect("open tree");

        tree.put(b"k", b"v1").expect("put v1");
        tree.checkpoint().expect("checkpoint v1");
        let writes_after_v1 = file.data_page_write_count(options.page_size);
        assert_eq!(writes_after_v1, 1);

        tree.checkpoint().expect("checkpoint without changes");
        let writes_after_noop = file.data_page_write_count(options.page_size);
        assert_eq!(writes_after_noop, writes_after_v1);

        tree.put(b"k", b"v2").expect("put v2");
        tree.checkpoint().expect("checkpoint v2");
        let writes_after_v2 = file.data_page_write_count(options.page_size);
        assert_eq!(writes_after_v2, writes_after_noop + 1);
    }

    #[test]
    fn checkpoint_coalesces_contiguous_data_page_writes() {
        let options = small_options();
        let file = CountingFile::new();
        let mut tree = OpfsBTree::open(file.clone(), options).expect("open tree");

        for i in 0..2_000u32 {
            let key = format!("k{:05}", i);
            let value = format!("value-{}", i);
            tree.put(key.as_bytes(), value.as_bytes()).expect("put");
        }
        tree.checkpoint().expect("checkpoint");

        let max_data_write_len = file.max_data_write_len(options.page_size);
        assert!(
            max_data_write_len > options.page_size,
            "expected coalesced write larger than one page, got {} bytes",
            max_data_write_len
        );
    }

    #[test]
    fn read_coalescing_reduces_data_page_read_calls() {
        let file = CountingFile::new();
        let mut build_options = small_options();
        build_options.read_coalesce_pages = 1;
        let mut tree = OpfsBTree::open(file.clone(), build_options).expect("open tree");

        for i in 0..4_000u32 {
            let key = format!("k{:05}", i);
            let value = format!("value-{}", i);
            tree.put(key.as_bytes(), value.as_bytes()).expect("put");
        }
        tree.checkpoint().expect("checkpoint");
        drop(tree);

        let mut baseline_options = small_options();
        baseline_options.read_coalesce_pages = 1;
        let mut baseline = OpfsBTree::open(file.clone(), baseline_options).expect("open baseline");
        file.reset_io_stats();
        for i in (0..4_000usize).step_by(17) {
            let key = format!("k{:05}", i);
            let _ = baseline.get(key.as_bytes()).expect("baseline get");
        }
        let baseline_reads = file.data_page_read_count(small_options().page_size);
        drop(baseline);

        let mut coalesced_options = small_options();
        coalesced_options.read_coalesce_pages = 8;
        let mut coalesced =
            OpfsBTree::open(file.clone(), coalesced_options).expect("open coalesced");
        file.reset_io_stats();
        for i in (0..4_000usize).step_by(17) {
            let key = format!("k{:05}", i);
            let _ = coalesced.get(key.as_bytes()).expect("coalesced get");
        }
        let coalesced_reads = file.data_page_read_count(small_options().page_size);

        assert!(
            baseline_reads > 0,
            "expected baseline data page reads to be non-zero"
        );
        assert!(
            coalesced_reads < baseline_reads,
            "expected coalesced reads ({}) to be < baseline ({})",
            coalesced_reads,
            baseline_reads
        );
    }

    #[test]
    fn coalesced_read_stops_at_file_len_after_uncheckpointed_growth() {
        let file = MemoryFile::new();
        let mut options = small_options();
        options.read_coalesce_pages = 4;
        let mut tree = OpfsBTree::open(file.clone(), options).expect("open tree");

        tree.put(b"alice", b"first").expect("put");
        tree.checkpoint().expect("checkpoint");

        let root_page_id = tree.root_page_id.expect("root page");
        assert_eq!(
            file.len().expect("file len") / options.page_size as u64,
            tree.total_pages,
            "checkpoint should extend the file through total_pages"
        );

        for _ in 0..3 {
            let _ = tree.alloc_page();
        }
        tree.remove_page(root_page_id);

        tree.ensure_page_loaded(root_page_id)
            .expect("coalesced read should not cross the persisted file length");
        assert!(tree.pages.contains_key(&root_page_id));
    }

    #[test]
    fn latest_checkpoint_wins_across_reopen() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file.clone(), small_options()).expect("open tree");

        tree.put(b"k", b"v1").expect("put v1");
        tree.checkpoint().expect("checkpoint v1");

        tree.put(b"k", b"v2").expect("put v2");
        tree.put(b"k2", b"v3").expect("put v3");
        tree.checkpoint().expect("checkpoint v2");

        let mut reopened = OpfsBTree::open(file, small_options()).expect("reopen tree");
        assert_eq!(reopened.get(b"k").expect("get k"), Some(b"v2".to_vec()));
        assert_eq!(reopened.get(b"k2").expect("get k2"), Some(b"v3".to_vec()));
    }

    #[test]
    fn latest_valid_superblock_recovers_after_second_crash_pattern() {
        let file = MemoryFile::new();

        {
            let mut tree = OpfsBTree::open(file.clone(), small_options()).expect("open tree");
            tree.put(b"persist", b"v1").expect("put persist");
            tree.checkpoint().expect("checkpoint persist");
        }

        {
            let mut recovered =
                OpfsBTree::open(file.clone(), small_options()).expect("reopen after first crash");
            assert_eq!(
                recovered.get(b"persist").expect("get persisted"),
                Some(b"v1".to_vec())
            );
            recovered.put(b"ephemeral", b"temp").expect("put ephemeral");
        }

        let mut reopened =
            OpfsBTree::open(file, small_options()).expect("reopen after second crash");
        assert_eq!(
            reopened
                .get(b"persist")
                .expect("get persisted after second crash"),
            Some(b"v1".to_vec())
        );
        assert_eq!(reopened.get(b"ephemeral").expect("get ephemeral"), None);
    }

    #[test]
    fn cache_eviction_keeps_root_and_budget_after_checkpoint() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, tiny_cache_options()).expect("open tree");

        for i in 0..20_000u32 {
            let key = format!("k{:05}", i);
            let value = format!("value-{}", i);
            tree.put(key.as_bytes(), value.as_bytes()).expect("put");
        }

        tree.checkpoint().expect("checkpoint");
        let max_cached = tree.max_cached_pages();
        let root = tree.root_page_id.expect("root exists");
        assert!(tree.pages.len() <= max_cached);
        assert!(tree.pages.contains_key(&root));

        for page_id in 2..tree.total_pages {
            tree.ensure_page_loaded(page_id).expect("load page");
            assert!(tree.pages.len() <= max_cached);
            assert!(tree.pages.contains_key(&root));
        }

        assert_eq!(
            tree.get(b"k00000").expect("get first"),
            Some(b"value-0".to_vec())
        );
        assert_eq!(
            tree.get(b"k01999").expect("get last"),
            Some(b"value-1999".to_vec())
        );
        let range = tree.range(b"k01000", b"k01010", 32).expect("range");
        assert_eq!(range.len(), 10);
    }

    #[test]
    fn cache_eviction_allowance_defers_until_threshold() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, tiny_cache_options()).expect("open tree");
        let max_cached = tree.max_cached_pages();
        let steady_cached = max_cached.saturating_sub(max_cached / 4).max(1);
        let allowance = 4usize;
        let keep_len = max_cached + allowance;
        let page_size = tree.options.page_size;

        for idx in 0..keep_len {
            let page_id = 10_000 + idx as u64;
            tree.pages.insert(page_id, vec![0u8; page_size]);
            tree.touch_page(page_id);
        }

        tree.evict_pages_if_needed_with_allowance(None, allowance);
        assert_eq!(
            tree.pages.len(),
            keep_len,
            "eviction should not run while within allowance"
        );

        let overflow_page_id = 10_000 + keep_len as u64;
        tree.pages.insert(overflow_page_id, vec![0u8; page_size]);
        tree.touch_page(overflow_page_id);

        tree.evict_pages_if_needed_with_allowance(None, allowance);
        assert!(
            tree.pages.len() <= steady_cached,
            "eviction should reduce cache to steady-state budget"
        );
    }

    #[test]
    fn cache_eviction_keeps_budget_for_coalesced_reads() {
        let file = MemoryFile::new();
        let mut options = tiny_cache_options();
        options.read_coalesce_pages = 8;
        let mut tree = OpfsBTree::open(file, options).expect("open tree");

        for i in 0..8_000u32 {
            let key = format!("k{:05}", i);
            let value = format!("value-{}", i);
            tree.put(key.as_bytes(), value.as_bytes()).expect("put");
        }
        tree.checkpoint().expect("checkpoint");

        let max_cached = tree.max_cached_pages();
        let root = tree.root_page_id.expect("root exists");

        for page_id in 2..tree.total_pages {
            tree.ensure_page_loaded(page_id).expect("load page");
            assert!(
                tree.pages.len() <= max_cached,
                "cache exceeded budget after loading page {}",
                page_id
            );
            assert!(tree.pages.contains_key(&root));
        }
    }

    #[test]
    fn cache_eviction_never_drops_dirty_pages() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, tiny_cache_options()).expect("open tree");
        let big = vec![9u8; 9_000];

        for i in 0..8u32 {
            let key = format!("k{:03}", i);
            tree.put(key.as_bytes(), &big).expect("put big");
        }

        assert!(tree.pages.len() > tree.max_cached_pages());
        let dirty_ids: Vec<PageId> = tree.dirty_pages.iter().copied().collect();
        tree.evict_pages_if_needed(None);

        for page_id in dirty_ids {
            assert!(
                tree.pages.contains_key(&page_id),
                "dirty page {} was evicted",
                page_id
            );
        }
    }

    #[test]
    fn eviction_with_pinned_cache_keeps_warm_evictable_pages() {
        let file = MemoryFile::new();
        let options = BTreeOptions {
            page_size: 4 * 1024,
            cache_bytes: 4 * 1024 * 64,
            overflow_threshold: 128,
            pin_internal_pages: false,
            read_coalesce_pages: 1,
        };
        let mut tree = OpfsBTree::open(file, options).expect("open tree");
        let max_cached = tree.max_cached_pages();
        let page_size = tree.options.page_size;

        let pinned = max_cached * 2;
        for idx in 0..pinned {
            let page_id = 10_000 + idx as u64;
            tree.pages.insert(page_id, vec![0u8; page_size]);
            tree.dirty_pages.insert(page_id);
            tree.touch_page(page_id);
        }

        let warm = max_cached / 8;
        for idx in 0..warm {
            let page_id = 20_000 + idx as u64;
            tree.pages.insert(page_id, vec![0u8; page_size]);
            tree.touch_page(page_id);
        }
        tree.evict_pages_if_needed(None);
        assert_eq!(
            tree.pages.len(),
            pinned + warm,
            "eviction must not wipe warm pages while the cache is pinned over budget"
        );

        let burst = max_cached / 4 + max_cached / 8;
        for idx in 0..burst {
            let page_id = 30_000 + idx as u64;
            tree.pages.insert(page_id, vec![0u8; page_size]);
            tree.touch_page(page_id);
        }
        tree.evict_pages_if_needed(None);
        assert_eq!(
            tree.pages.len(),
            pinned + max_cached / 8,
            "eviction should trim to the pinned-aware resting level"
        );
        let surviving_dirty = (0..pinned)
            .filter(|idx| tree.pages.contains_key(&(10_000 + *idx as u64)))
            .count();
        assert_eq!(surviving_dirty, pinned, "dirty pages must never be evicted");
    }

    #[test]
    fn cache_eviction_preserves_loaded_internal_pages_when_pinned() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, tiny_cache_pinned_options()).expect("open tree");
        let key_suffix = "x".repeat(256);

        for i in 0..20_000u32 {
            let key = format!("k{:05}-{}", i, key_suffix);
            let value = format!("value-{}", i);
            tree.put(key.as_bytes(), value.as_bytes()).expect("put");
        }
        tree.checkpoint().expect("checkpoint");

        let mut internal_ids = Vec::new();
        for page_id in 2..tree.total_pages {
            tree.ensure_page_loaded(page_id).expect("load page");
            if tree.page_kind(page_id).expect("page kind") == PageKind::Internal {
                internal_ids.push(page_id);
            }
        }
        assert!(
            internal_ids.len() >= 2,
            "expected at least two internal pages"
        );

        tree.evict_pages_if_needed(None);

        for page_id in internal_ids {
            assert!(
                tree.pages.contains_key(&page_id),
                "internal page {} was evicted despite pinning",
                page_id
            );
        }
    }

    #[test]
    fn alloc_page_near_prefers_adjacent_free_page() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, small_options()).expect("open tree");

        tree.total_pages = 32;
        tree.add_free_page(7);
        tree.add_free_page(10);
        tree.add_free_page(11);
        tree.add_free_page(14);

        let allocated = tree.alloc_page_near(10);
        assert_eq!(allocated, 11, "expected +1 neighbor to be selected first");
        assert!(!tree.free_set.contains(&11));
    }

    #[test]
    fn alloc_page_near_appends_when_preferred_at_tail() {
        let file = MemoryFile::new();
        let mut tree = OpfsBTree::open(file, small_options()).expect("open tree");

        tree.total_pages = 18;
        let allocated = tree.alloc_page_near(17);
        assert_eq!(allocated, 18);
        assert_eq!(tree.total_pages, 19);
    }
}
