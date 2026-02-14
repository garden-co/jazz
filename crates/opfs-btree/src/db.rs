use std::collections::{HashMap, HashSet};

use crate::BTreeError;
use crate::file::SyncFile;
use crate::page::{
    Page, PageId, ValueCell, decode_page, encode_page, freelist_ids_per_page,
    overflow_chunk_capacity, page_fits,
};
use crate::superblock::{Superblock, SuperblockSlot};

const MIN_PAGE_SIZE: usize = 4 * 1024;
const DEFAULT_PAGE_SIZE: usize = 16 * 1024;
const DEFAULT_CACHE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_OVERFLOW_THRESHOLD: usize = 8 * 1024;
const BOOTSTRAP_GENERATION: u64 = 1;

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
    root_page_id: Option<PageId>,
    total_pages: u64,
    pages: HashMap<PageId, Page>,
    page_access_epoch: HashMap<PageId, u64>,
    access_epoch: u64,
    dirty_pages: HashSet<PageId>,
    free_pages: Vec<PageId>,
    free_set: HashSet<PageId>,
    freelist_meta_pages: Vec<PageId>,
}

#[derive(Debug)]
struct SplitResult {
    separator: Vec<u8>,
    right_page_id: PageId,
}

type KvPair = (Vec<u8>, Vec<u8>);
type LeafEntry = (Vec<u8>, ValueCell);

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
            root_page_id: None,
            total_pages: 2,
            pages: HashMap::new(),
            page_access_epoch: HashMap::new(),
            access_epoch: 0,
            dirty_pages: HashSet::new(),
            free_pages: Vec::new(),
            free_set: HashSet::new(),
            freelist_meta_pages: Vec::new(),
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
            return Ok(tree);
        }

        tree.total_pages = tree.active.total_pages.max(2);
        if tree.active.root_page_id != 0 {
            tree.root_page_id = Some(tree.active.root_page_id);
            tree.ensure_page_loaded(tree.active.root_page_id)?;
            let root_page = tree.pages.get(&tree.active.root_page_id).ok_or_else(|| {
                BTreeError::Corrupt(format!(
                    "root page {} missing after load",
                    tree.active.root_page_id
                ))
            })?;
            match root_page {
                Page::Leaf { .. } | Page::Internal { .. } => {}
                Page::Overflow { .. } | Page::Freelist { .. } => {
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

        Ok(tree)
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, BTreeError> {
        let leaf_page_id = match self.find_leaf_page_id(key)? {
            Some(id) => id,
            None => return Ok(None),
        };

        self.ensure_page_loaded(leaf_page_id)?;
        let leaf = self.pages.get(&leaf_page_id).ok_or_else(|| {
            BTreeError::Corrupt(format!("leaf page {} missing in memory", leaf_page_id))
        })?;
        let (entries, _) = expect_leaf(leaf)?;

        let value_cell = match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
            Ok(idx) => Some(entries[idx].1.clone()),
            Err(_) => None,
        };
        value_cell
            .map(|cell| self.materialize_value(&cell))
            .transpose()
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), BTreeError> {
        if self.root_page_id.is_none() {
            let root_page_id = self.alloc_page();
            let value_cell = self.build_value_cell(value)?;
            let leaf = Page::Leaf {
                entries: vec![(key.to_vec(), value_cell)],
                next: None,
            };
            ensure_page_fits(&leaf, self.options.page_size, "initial leaf")?;
            self.set_dirty_page(root_page_id, leaf);
            self.root_page_id = Some(root_page_id);
            return Ok(());
        }

        let root_page_id = self.root_page_id.expect("root must exist");
        if let Some(split) = self.insert_recursive(root_page_id, key, value)? {
            let new_root_page_id = self.alloc_page();
            let new_root = Page::Internal {
                keys: vec![split.separator],
                children: vec![root_page_id, split.right_page_id],
            };
            ensure_page_fits(&new_root, self.options.page_size, "new root")?;
            self.set_dirty_page(new_root_page_id, new_root);
            self.root_page_id = Some(new_root_page_id);
        }

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), BTreeError> {
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

        self.ensure_page_loaded(root_page_id)?;
        let root_page = self.pages.get(&root_page_id).ok_or_else(|| {
            BTreeError::Corrupt(format!("root page {} missing after delete", root_page_id))
        })?;

        if let Page::Leaf { entries, .. } = root_page
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
        if limit == 0 || start >= end {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        let mut current = self.find_leaf_page_id(start)?;
        let mut visited = HashSet::new();

        while let Some(page_id) = current {
            if !visited.insert(page_id) {
                return Err(BTreeError::Corrupt(
                    "leaf chain contains a cycle".to_string(),
                ));
            }

            self.ensure_page_loaded(page_id)?;
            let (next, staged_entries) = {
                let page = self.pages.get(&page_id).ok_or_else(|| {
                    BTreeError::Corrupt(format!("leaf page {} missing during range", page_id))
                })?;
                let (entries, next) = expect_leaf(page)?;

                let remaining = limit.saturating_sub(out.len());
                let mut staged = Vec::with_capacity(remaining.min(entries.len()));
                for (key, value) in entries {
                    if key.as_slice() >= end {
                        break;
                    }
                    if key.as_slice() >= start {
                        staged.push((key.clone(), value.clone()));
                        if staged.len() == remaining {
                            break;
                        }
                    }
                }

                (*next, staged)
            };

            for (key, value) in staged_entries {
                out.push((key, self.materialize_value(&value)?));
                if out.len() == limit {
                    return Ok(out);
                }
            }

            current = next;
        }

        Ok(out)
    }

    pub fn checkpoint(&mut self) -> Result<(), BTreeError> {
        let mut dirty_page_ids: Vec<PageId> = self.dirty_pages.iter().copied().collect();
        dirty_page_ids.sort_unstable();
        let (freelist_head_page_id, freelist_pages) = self.build_freelist_pages()?;
        self.write_pages_to_disk(&dirty_page_ids, &freelist_pages)?;
        self.file.flush()?;
        self.checkpoint_superblock(
            self.root_page_id.unwrap_or(0),
            freelist_head_page_id,
            self.total_pages,
        )?;
        self.dirty_pages.clear();
        self.evict_pages_if_needed(None);
        Ok(())
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

    fn insert_recursive(
        &mut self,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<SplitResult>, BTreeError> {
        self.ensure_page_loaded(page_id)?;
        let page = self.pages.get(&page_id).cloned().ok_or_else(|| {
            BTreeError::Corrupt(format!("page {} missing during insert", page_id))
        })?;

        match page {
            Page::Leaf { mut entries, next } => {
                let new_value = self.build_value_cell(value)?;
                match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                    Ok(idx) => {
                        let old_value = std::mem::replace(&mut entries[idx].1, new_value);
                        self.free_value_cell(old_value)?;
                    }
                    Err(idx) => entries.insert(idx, (key.to_vec(), new_value)),
                }

                let candidate = Page::Leaf {
                    entries: entries.clone(),
                    next,
                };
                if page_fits(&candidate, self.options.page_size)? {
                    self.set_dirty_page(page_id, candidate);
                    return Ok(None);
                }

                if entries.len() < 2 {
                    return Err(BTreeError::InvalidOptions(
                        "single leaf entry exceeds page size".to_string(),
                    ));
                }

                let split_at = entries.len() / 2;
                let right_entries = entries.split_off(split_at);
                let split_key = right_entries
                    .first()
                    .map(|(k, _)| k.clone())
                    .ok_or_else(|| BTreeError::Corrupt("right leaf split empty".to_string()))?;

                let right_page_id = self.alloc_page();
                let left_page = Page::Leaf {
                    entries,
                    next: Some(right_page_id),
                };
                let right_page = Page::Leaf {
                    entries: right_entries,
                    next,
                };

                ensure_page_fits(&left_page, self.options.page_size, "left leaf split")?;
                ensure_page_fits(&right_page, self.options.page_size, "right leaf split")?;

                self.set_dirty_page(page_id, left_page);
                self.set_dirty_page(right_page_id, right_page);
                Ok(Some(SplitResult {
                    separator: split_key,
                    right_page_id,
                }))
            }
            Page::Internal {
                mut keys,
                mut children,
            } => {
                let child_idx = child_index(&keys, key);
                let child_page_id = *children.get(child_idx).ok_or_else(|| {
                    BTreeError::Corrupt(format!(
                        "internal page {} missing child {}",
                        page_id, child_idx
                    ))
                })?;

                let split = self.insert_recursive(child_page_id, key, value)?;
                let Some(split) = split else {
                    return Ok(None);
                };

                keys.insert(child_idx, split.separator);
                children.insert(child_idx + 1, split.right_page_id);

                let candidate = Page::Internal {
                    keys: keys.clone(),
                    children: children.clone(),
                };
                if page_fits(&candidate, self.options.page_size)? {
                    self.set_dirty_page(page_id, candidate);
                    return Ok(None);
                }

                if keys.len() < 2 {
                    return Err(BTreeError::InvalidOptions(
                        "internal split requires at least two keys".to_string(),
                    ));
                }

                let mid = keys.len() / 2;
                let promoted = keys[mid].clone();

                let right_keys = keys.split_off(mid + 1);
                let _ = keys.pop();
                let right_children = children.split_off(mid + 1);

                let left_page = Page::Internal { keys, children };
                let right_page = Page::Internal {
                    keys: right_keys,
                    children: right_children,
                };

                ensure_page_fits(&left_page, self.options.page_size, "left internal split")?;
                ensure_page_fits(&right_page, self.options.page_size, "right internal split")?;

                let right_page_id = self.alloc_page();
                self.set_dirty_page(page_id, left_page);
                self.set_dirty_page(right_page_id, right_page);

                Ok(Some(SplitResult {
                    separator: promoted,
                    right_page_id,
                }))
            }
            Page::Overflow { .. } => Err(BTreeError::Corrupt(format!(
                "insert reached overflow page {}",
                page_id
            ))),
            Page::Freelist { .. } => Err(BTreeError::Corrupt(format!(
                "insert reached freelist page {}",
                page_id
            ))),
        }
    }

    fn delete_recursive(&mut self, page_id: PageId, key: &[u8]) -> Result<bool, BTreeError> {
        self.ensure_page_loaded(page_id)?;
        let page = self.pages.get(&page_id).cloned().ok_or_else(|| {
            BTreeError::Corrupt(format!("page {} missing during delete", page_id))
        })?;

        match page {
            Page::Leaf { mut entries, next } => {
                let idx = match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                    Ok(idx) => idx,
                    Err(_) => return Ok(false),
                };
                let (_, value) = entries.remove(idx);
                self.free_value_cell(value)?;
                self.set_dirty_page(page_id, Page::Leaf { entries, next });
                Ok(true)
            }
            Page::Internal { keys, children } => {
                let child_idx = child_index(&keys, key);
                let child_page_id = *children.get(child_idx).ok_or_else(|| {
                    BTreeError::Corrupt(format!(
                        "internal page {} missing child {}",
                        page_id, child_idx
                    ))
                })?;
                self.delete_recursive(child_page_id, key)
            }
            Page::Overflow { .. } => Err(BTreeError::Corrupt(format!(
                "delete reached overflow page {}",
                page_id
            ))),
            Page::Freelist { .. } => Err(BTreeError::Corrupt(format!(
                "delete reached freelist page {}",
                page_id
            ))),
        }
    }

    fn materialize_value(&mut self, value: &ValueCell) -> Result<Vec<u8>, BTreeError> {
        match value {
            ValueCell::Inline(value) => Ok(value.clone()),
            ValueCell::Overflow {
                head_page_id,
                total_len,
            } => self.read_overflow_value(*head_page_id, *total_len as usize),
        }
    }

    fn read_overflow_value(
        &mut self,
        head_page_id: PageId,
        expected_len: usize,
    ) -> Result<Vec<u8>, BTreeError> {
        let mut out = Vec::with_capacity(expected_len);
        let mut current = head_page_id;
        let mut seen = HashSet::new();

        while current != 0 && out.len() < expected_len {
            if !seen.insert(current) {
                return Err(BTreeError::Corrupt(
                    "overflow chain contains a cycle".to_string(),
                ));
            }

            self.ensure_page_loaded(current)?;
            let page = self.pages.get(&current).ok_or_else(|| {
                BTreeError::Corrupt(format!("overflow page {} missing in memory", current))
            })?;
            let (chunk, next) = expect_overflow(page)?;
            out.extend_from_slice(chunk);
            current = next.unwrap_or(0);
        }

        if out.len() < expected_len {
            return Err(BTreeError::Corrupt(format!(
                "overflow payload truncated: expected {}, found {}",
                expected_len,
                out.len()
            )));
        }

        out.truncate(expected_len);
        Ok(out)
    }

    fn free_value_cell(&mut self, value: ValueCell) -> Result<(), BTreeError> {
        if let ValueCell::Overflow { head_page_id, .. } = value {
            self.free_overflow_chain(head_page_id)?;
        }
        Ok(())
    }

    fn free_overflow_chain(&mut self, head_page_id: PageId) -> Result<(), BTreeError> {
        let mut current = head_page_id;
        let mut seen = HashSet::new();

        while current != 0 {
            if !seen.insert(current) {
                return Err(BTreeError::Corrupt(
                    "overflow free encountered a cycle".to_string(),
                ));
            }

            self.ensure_page_loaded(current)?;
            let page = self.remove_page(current).ok_or_else(|| {
                BTreeError::Corrupt(format!("overflow page {} missing while freeing", current))
            })?;
            let (_, next) = expect_overflow(&page)?;
            self.add_free_page(current);
            current = next.unwrap_or(0);
        }

        Ok(())
    }

    fn build_value_cell(&mut self, value: &[u8]) -> Result<ValueCell, BTreeError> {
        if value.len() <= self.options.overflow_threshold {
            return Ok(ValueCell::Inline(value.to_vec()));
        }

        let max_chunk = overflow_chunk_capacity(self.options.page_size)?;
        if max_chunk == 0 {
            return Err(BTreeError::InvalidOptions(
                "page size too small for overflow pages".to_string(),
            ));
        }

        let total_len = u32::try_from(value.len())
            .map_err(|_| BTreeError::InvalidOptions("value too large".to_string()))?;

        let mut remaining = value;
        let mut page_ids = Vec::new();
        while !remaining.is_empty() {
            page_ids.push(self.alloc_page());
            let consume = remaining.len().min(max_chunk);
            remaining = &remaining[consume..];
        }

        remaining = value;
        for (idx, page_id) in page_ids.iter().enumerate() {
            let consume = remaining.len().min(max_chunk);
            let chunk = remaining[..consume].to_vec();
            remaining = &remaining[consume..];

            let next = page_ids.get(idx + 1).copied();
            self.set_dirty_page(*page_id, Page::Overflow { data: chunk, next });
        }

        let head_page_id = *page_ids
            .first()
            .ok_or_else(|| BTreeError::Corrupt("overflow page id allocation failed".to_string()))?;

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

        loop {
            self.ensure_page_loaded(current)?;
            let page = self.pages.get(&current).ok_or_else(|| {
                BTreeError::Corrupt(format!("page {} missing while descending", current))
            })?;
            match page {
                Page::Leaf { .. } => return Ok(Some(current)),
                Page::Internal { keys, children } => {
                    let idx = child_index(keys, key);
                    current = *children.get(idx).ok_or_else(|| {
                        BTreeError::Corrupt(format!(
                            "internal page {} missing child {}",
                            current, idx
                        ))
                    })?;
                }
                Page::Overflow { .. } => {
                    return Err(BTreeError::Corrupt(format!(
                        "unexpected overflow page {} in tree path",
                        current
                    )));
                }
                Page::Freelist { .. } => {
                    return Err(BTreeError::Corrupt(format!(
                        "unexpected freelist page {} in tree path",
                        current
                    )));
                }
            }
        }
    }

    fn ensure_page_loaded(&mut self, page_id: PageId) -> Result<(), BTreeError> {
        if self.pages.contains_key(&page_id) {
            self.touch_page(page_id);
            return Ok(());
        }
        let page = self.read_page_from_disk(page_id)?;
        self.cache_loaded_page(page_id, page);
        Ok(())
    }

    fn load_freelist_from_disk(&mut self, head_page_id: PageId) -> Result<(), BTreeError> {
        let mut current = head_page_id;
        let mut seen = HashSet::new();

        while current != 0 {
            if !seen.insert(current) {
                return Err(BTreeError::Corrupt(
                    "freelist pages contain a cycle".to_string(),
                ));
            }

            let page = self.read_page_from_disk(current)?;
            let (ids, next) = expect_freelist(&page)?;
            self.freelist_meta_pages.push(current);
            for id in ids {
                self.add_free_page(*id);
            }
            current = next.unwrap_or(0);
        }

        Ok(())
    }

    fn read_page_from_disk(&self, page_id: PageId) -> Result<Page, BTreeError> {
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
        decode_page(&raw, self.options.page_size)
    }

    fn write_pages_to_disk(
        &mut self,
        dirty_page_ids: &[PageId],
        freelist_pages: &[(PageId, Page)],
    ) -> Result<(), BTreeError> {
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
        if self.file.len()? < required_len {
            self.file.truncate(required_len)?;
        }

        for page_id in dirty_page_ids {
            if let Some(page) = self.pages.get(page_id) {
                self.write_single_page(*page_id, page)?;
            }
        }
        for (page_id, page) in freelist_pages {
            self.write_single_page(*page_id, page)?;
        }

        Ok(())
    }

    fn write_single_page(&self, page_id: PageId, page: &Page) -> Result<(), BTreeError> {
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

        let raw = encode_page(page, self.options.page_size)?;
        let offset = page_id
            .checked_mul(self.options.page_size as u64)
            .ok_or_else(|| BTreeError::Io("page write offset overflow".to_string()))?;
        self.file.write_all_at(offset, &raw)
    }

    fn set_dirty_page(&mut self, page_id: PageId, page: Page) {
        self.pages.insert(page_id, page);
        self.dirty_pages.insert(page_id);
        self.touch_page(page_id);
        self.evict_pages_if_needed(Some(page_id));
    }

    fn remove_page(&mut self, page_id: PageId) -> Option<Page> {
        self.dirty_pages.remove(&page_id);
        self.page_access_epoch.remove(&page_id);
        self.pages.remove(&page_id)
    }

    fn cache_loaded_page(&mut self, page_id: PageId, page: Page) {
        self.pages.insert(page_id, page);
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
        let live_page_ids: HashSet<PageId> = self.pages.keys().copied().collect();
        self.free_set.retain(|page_id| {
            *page_id >= 2 && *page_id < self.total_pages && !live_page_ids.contains(page_id)
        });

        self.free_pages.clear();
        self.free_pages.extend(self.free_set.iter().copied());
        self.free_pages.sort_unstable();
    }

    fn alloc_page(&mut self) -> PageId {
        while let Some(page_id) = self.free_pages.pop() {
            if self.free_set.remove(&page_id) {
                return page_id;
            }
        }

        let page_id = self.total_pages;
        self.total_pages = self.total_pages.saturating_add(1);
        page_id
    }

    fn add_free_page(&mut self, page_id: PageId) {
        if page_id < 2 {
            return;
        }
        if self.free_set.insert(page_id) {
            self.free_pages.push(page_id);
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
        let max_cached_pages = self.max_cached_pages();
        if self.pages.len() <= max_cached_pages {
            return;
        }

        let root_page_id = self.root_page_id;
        let mut candidates: Vec<(u8, u64, PageId)> = self
            .pages
            .iter()
            .filter_map(|(page_id, page)| {
                if Some(*page_id) == protected_page
                    || Some(*page_id) == root_page_id
                    || self.dirty_pages.contains(page_id)
                {
                    return None;
                }

                Some((
                    eviction_priority(page),
                    *self.page_access_epoch.get(page_id).unwrap_or(&0),
                    *page_id,
                ))
            })
            .collect();
        candidates.sort_unstable();

        let mut target = self.pages.len().saturating_sub(max_cached_pages);
        for (_, _, page_id) in candidates {
            if target == 0 {
                break;
            }
            self.pages.remove(&page_id);
            self.page_access_epoch.remove(&page_id);
            target -= 1;
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
        write_slot(&self.file, target_slot, self.options.page_size, next)?;
        self.file.flush()?;

        self.active_slot = target_slot;
        self.active = next;
        Ok(())
    }
}

fn child_index(keys: &[Vec<u8>], key: &[u8]) -> usize {
    keys.partition_point(|separator| separator.as_slice() <= key)
}

fn expect_leaf(page: &Page) -> Result<(&[LeafEntry], &Option<PageId>), BTreeError> {
    match page {
        Page::Leaf { entries, next } => Ok((entries, next)),
        _ => Err(BTreeError::Corrupt("expected leaf page".to_string())),
    }
}

fn expect_overflow(page: &Page) -> Result<(&Vec<u8>, &Option<PageId>), BTreeError> {
    match page {
        Page::Overflow { data, next } => Ok((data, next)),
        _ => Err(BTreeError::Corrupt("expected overflow page".to_string())),
    }
}

fn expect_freelist(page: &Page) -> Result<(&Vec<PageId>, &Option<PageId>), BTreeError> {
    match page {
        Page::Freelist { ids, next } => Ok((ids, next)),
        _ => Err(BTreeError::Corrupt("expected freelist page".to_string())),
    }
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

fn eviction_priority(page: &Page) -> u8 {
    match page {
        Page::Overflow { .. } | Page::Freelist { .. } => 0,
        Page::Leaf { .. } => 1,
        Page::Internal { .. } => 2,
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
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::file::{MemoryFile, SyncFile};

    fn small_options() -> BTreeOptions {
        BTreeOptions {
            page_size: 4 * 1024,
            cache_bytes: 4 * 1024 * 8,
            overflow_threshold: 128,
        }
    }

    fn tiny_cache_options() -> BTreeOptions {
        BTreeOptions {
            page_size: 4 * 1024,
            cache_bytes: 4 * 1024 * 2,
            overflow_threshold: 128,
        }
    }

    #[derive(Clone, Debug)]
    struct CountingFile {
        inner: MemoryFile,
        writes: Rc<RefCell<Vec<(u64, usize)>>>,
    }

    impl CountingFile {
        fn new() -> Self {
            Self {
                inner: MemoryFile::new(),
                writes: Rc::new(RefCell::new(Vec::new())),
            }
        }

        fn data_page_write_count(&self, page_size: usize) -> usize {
            let min_data_offset = (2 * page_size) as u64;
            self.writes
                .borrow()
                .iter()
                .filter(|(offset, _)| *offset >= min_data_offset)
                .count()
        }
    }

    impl SyncFile for CountingFile {
        fn len(&self) -> Result<u64, BTreeError> {
            self.inner.len()
        }

        fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError> {
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

        for i in 0..2_000u32 {
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
}
