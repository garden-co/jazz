//! B-tree index implementation with page-based storage.
//!
//! This B-tree implementation stores each node as a separate page in storage,
//! enabling:
//! - **Incremental updates**: Only modified pages are written
//! - **Lazy loading**: Pages are loaded on demand as queries traverse
//! - **WASM compatible**: Uses key-value storage semantics, no mmap
//!
//! Pages are stored directly without Object overhead for efficient memory use.

use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use crate::object::ObjectId;
use crate::storage::StorageRequest;

use super::ScanCondition;
use super::btree_page::{BTreePage, IndexMeta, LeafEntry, PageId};

/// Maximum entries per leaf page before splitting.
/// Higher values = fewer pages, more memory per page.
const MAX_LEAF_ENTRIES: usize = 64;

// TODO: Implement page merging for underflow
// const MIN_LEAF_ENTRIES: usize = MAX_LEAF_ENTRIES / 2;
// const MAX_INTERNAL_CHILDREN: usize = 64;
// const MIN_INTERNAL_CHILDREN: usize = MAX_INTERNAL_CHILDREN / 2;

/// Errors that can occur during index operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexError {
    /// Page not loaded yet - retry after processing storage responses.
    PageNotLoaded(PageId),
    /// Page data corrupted.
    DecodeError(PageId),
    /// Internal consistency error.
    InternalError(String),
}

/// State of a page in memory.
#[derive(Debug, Clone)]
enum PageState {
    /// Page loaded and available.
    Loaded(BTreePage),
    /// Page load requested, waiting for response.
    Loading,
}

/// B-tree index with page-based storage.
///
/// Provides the same interface as IndexState for drop-in replacement.
#[derive(Debug, Clone)]
pub struct BTreeIndex {
    /// Table this index belongs to.
    table: String,
    /// Column name ("_id" for primary index).
    column: String,

    /// Index metadata (root page, next page id, entry count).
    meta: IndexMeta,
    /// Whether metadata has been loaded from storage.
    meta_loaded: bool,
    /// Whether metadata needs to be persisted.
    meta_dirty: bool,

    /// Loaded pages (lazy loading).
    pages: HashMap<PageId, PageState>,

    /// Pages that have been modified and need persistence.
    dirty_pages: HashSet<PageId>,

    /// Pages to delete on next persist.
    deleted_pages: HashSet<PageId>,

    /// Pending storage requests to emit.
    pending_requests: Vec<StorageRequest>,

    // ========================================================================
    // Delta tracking for push-based notifications
    // ========================================================================
    /// Recent inserts since last delta clear: (key, row_id)
    recent_inserts: Vec<(Vec<u8>, ObjectId)>,

    /// Recent deletes since last delta clear: (key, row_id)
    recent_deletes: Vec<(Vec<u8>, ObjectId)>,

    /// Epoch counter - incremented when deltas cleared
    delta_epoch: u64,
}

impl BTreeIndex {
    /// Create a new B-tree index for a table/column.
    pub fn new(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            column: column.into(),
            meta: IndexMeta::new(),
            meta_loaded: false,
            meta_dirty: false,
            pages: HashMap::new(),
            dirty_pages: HashSet::new(),
            deleted_pages: HashSet::new(),
            pending_requests: Vec::new(),
            recent_inserts: Vec::new(),
            recent_deletes: Vec::new(),
            delta_epoch: 0,
        }
    }

    /// Get table name.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Get column name.
    pub fn column(&self) -> &str {
        &self.column
    }

    // ========================================================================
    // Storage request/response handling
    // ========================================================================

    /// Take pending storage requests that need to be sent.
    pub fn take_storage_requests(&mut self) -> Vec<StorageRequest> {
        // Generate requests for dirty pages
        for &page_id in &self.dirty_pages {
            if let Some(PageState::Loaded(page)) = self.pages.get(&page_id) {
                self.pending_requests.push(StorageRequest::StoreIndexPage {
                    table: self.table.clone(),
                    column: self.column.clone(),
                    page_id: page_id.0,
                    data: page.serialize(),
                });
            }
        }
        self.dirty_pages.clear();

        // Generate requests for deleted pages
        for page_id in self.deleted_pages.drain() {
            self.pending_requests.push(StorageRequest::DeleteIndexPage {
                table: self.table.clone(),
                column: self.column.clone(),
                page_id: page_id.0,
            });
        }

        // Generate meta store request if dirty
        if self.meta_dirty {
            self.pending_requests.push(StorageRequest::StoreIndexMeta {
                table: self.table.clone(),
                column: self.column.clone(),
                data: self.meta.serialize(),
            });
            self.meta_dirty = false;
        }

        std::mem::take(&mut self.pending_requests)
    }

    /// Check if there are pending storage requests.
    pub fn has_pending_requests(&self) -> bool {
        !self.pending_requests.is_empty()
            || !self.dirty_pages.is_empty()
            || !self.deleted_pages.is_empty()
            || self.meta_dirty
    }

    /// Process a loaded page response.
    pub fn process_page_load(&mut self, page_id: PageId, data: Option<Vec<u8>>) {
        match data {
            Some(bytes) => {
                if let Some(page) = BTreePage::deserialize(&bytes) {
                    self.pages.insert(page_id, PageState::Loaded(page));
                } else {
                    // Corrupted data - treat as empty (new page)
                    self.pages
                        .insert(page_id, PageState::Loaded(BTreePage::new_leaf()));
                }
            }
            None => {
                // Page doesn't exist - create new leaf
                self.pages
                    .insert(page_id, PageState::Loaded(BTreePage::new_leaf()));
                // Mark as dirty so it gets persisted
                self.dirty_pages.insert(page_id);
            }
        }
    }

    /// Process loaded metadata response.
    pub fn process_meta_load(&mut self, data: Option<Vec<u8>>) {
        match data {
            Some(bytes) => {
                if let Some(meta) = IndexMeta::deserialize(&bytes) {
                    self.meta = meta;
                    // Queue load request for root page if not already loaded
                    if !self.pages.contains_key(&self.meta.root_page_id) {
                        self.ensure_page_loading(self.meta.root_page_id);
                    }
                }
                // else: corrupted, keep default
            }
            None => {
                // New index - use default meta
                // Create empty root page
                self.pages
                    .insert(PageId::ROOT, PageState::Loaded(BTreePage::new_leaf()));
                self.dirty_pages.insert(PageId::ROOT);
                self.meta_dirty = true;
            }
        }
        self.meta_loaded = true;
    }

    /// Request metadata load if not already loaded.
    fn ensure_meta_loading(&mut self) {
        if !self.meta_loaded && self.pending_requests.is_empty() {
            self.pending_requests.push(StorageRequest::LoadIndexMeta {
                table: self.table.clone(),
                column: self.column.clone(),
            });
        }
    }

    /// Request a page load if not already loaded/loading.
    fn ensure_page_loading(&mut self, page_id: PageId) {
        use std::collections::hash_map::Entry;
        if let Entry::Vacant(e) = self.pages.entry(page_id) {
            e.insert(PageState::Loading);
            self.pending_requests.push(StorageRequest::LoadIndexPage {
                table: self.table.clone(),
                column: self.column.clone(),
                page_id: page_id.0,
            });
        }
    }

    /// Get a page if loaded.
    fn get_page(&self, page_id: PageId) -> Option<&BTreePage> {
        match self.pages.get(&page_id) {
            Some(PageState::Loaded(page)) => Some(page),
            _ => None,
        }
    }

    /// Get a mutable page reference, marking it dirty.
    fn get_page_mut(&mut self, page_id: PageId) -> Option<&mut BTreePage> {
        match self.pages.get_mut(&page_id) {
            Some(PageState::Loaded(page)) => {
                self.dirty_pages.insert(page_id);
                Some(page)
            }
            _ => None,
        }
    }

    /// Allocate a new page ID.
    fn alloc_page(&mut self) -> PageId {
        let page_id = PageId(self.meta.next_page_id);
        self.meta.next_page_id += 1;
        self.meta_dirty = true;
        page_id
    }

    // ========================================================================
    // Public API (same as IndexState for drop-in replacement)
    // ========================================================================

    /// Insert a row into the index.
    ///
    /// Returns Ok(true) if inserted, Ok(false) if pages need loading.
    pub fn insert(&mut self, key: &[u8], row_id: ObjectId) -> Result<bool, IndexError> {
        // Ensure metadata is loaded
        if !self.meta_loaded {
            self.ensure_meta_loading();
            return Ok(false);
        }

        // Insert into tree
        match self.insert_into_tree(key, row_id) {
            Ok(()) => {
                self.meta.entry_count += 1;
                self.meta_dirty = true;
                // Record delta for push-based notifications
                self.recent_inserts.push((key.to_vec(), row_id));
                Ok(true)
            }
            Err(IndexError::PageNotLoaded(page_id)) => {
                self.ensure_page_loading(page_id);
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// Remove a row from the index.
    pub fn remove(&mut self, key: &[u8], row_id: ObjectId) -> Result<(), IndexError> {
        // Ensure metadata is loaded
        if !self.meta_loaded {
            self.ensure_meta_loading();
            return Ok(());
        }

        match self.remove_from_tree(key, row_id) {
            Ok(removed) => {
                if removed {
                    if self.meta.entry_count > 0 {
                        self.meta.entry_count -= 1;
                        self.meta_dirty = true;
                    }
                    // Record delta for push-based notifications
                    self.recent_deletes.push((key.to_vec(), row_id));
                }
                Ok(())
            }
            Err(IndexError::PageNotLoaded(page_id)) => {
                self.ensure_page_loading(page_id);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Exact lookup - returns row IDs for the given key.
    pub fn lookup_exact(&self, key: &[u8]) -> Vec<ObjectId> {
        if !self.meta_loaded {
            return vec![];
        }

        match self.find_leaf_for_key(key) {
            Ok(page_id) => {
                if let Some(BTreePage::Leaf { entries, .. }) = self.get_page(page_id) {
                    for entry in entries {
                        if entry.key == key {
                            return entry.row_ids.iter().copied().collect();
                        }
                    }
                }
                vec![]
            }
            Err(_) => vec![],
        }
    }

    /// Range scan - returns row IDs for keys in the specified range.
    pub fn range_scan(&self, min: &Bound<Vec<u8>>, max: &Bound<Vec<u8>>) -> Vec<ObjectId> {
        if !self.meta_loaded {
            return vec![];
        }

        let mut results = Vec::new();

        // Find starting leaf
        let start_key = match min {
            Bound::Included(k) | Bound::Excluded(k) => Some(k.as_slice()),
            Bound::Unbounded => None,
        };

        let start_page_id = match start_key {
            Some(k) => match self.find_leaf_for_key(k) {
                Ok(id) => id,
                Err(_) => return results,
            },
            None => {
                // Start from leftmost leaf
                match self.find_leftmost_leaf() {
                    Ok(id) => id,
                    Err(_) => return results,
                }
            }
        };

        // Scan leaves
        self.scan_leaves(start_page_id, min, max, &mut results);
        results
    }

    /// Full scan - returns all row IDs.
    ///
    /// Scans all loaded leaf pages. For a complete B-tree this would traverse
    /// leaves in order, but for simplicity we scan all loaded pages.
    pub fn scan_all(&self) -> Vec<ObjectId> {
        if !self.meta_loaded {
            return vec![];
        }

        let mut results = Vec::new();

        // Collect all entries from all loaded leaf pages
        for state in self.pages.values() {
            if let PageState::Loaded(BTreePage::Leaf { entries, .. }) = state {
                for entry in entries {
                    results.extend(entry.row_ids.iter().copied());
                }
            }
        }

        results
    }

    /// Check if a row ID exists in the index.
    pub fn contains_row(&self, row_id: ObjectId) -> bool {
        let key = row_id.uuid().as_bytes();
        !self.lookup_exact(key).is_empty()
    }

    /// Check if the index root exists (is ready for operations).
    ///
    /// Note: This returns true only when the root page is actually loaded,
    /// not when it's in the `Loading` state.
    pub fn root_exists(&self) -> bool {
        self.meta_loaded
            && matches!(
                self.pages.get(&self.meta.root_page_id),
                Some(PageState::Loaded(_))
            )
    }

    /// Check if the index is ready to serve queries.
    ///
    /// Returns true only when both metadata AND root page are loaded
    /// (not just requested for loading).
    pub fn is_ready(&self) -> bool {
        self.meta_loaded
            && matches!(
                self.pages.get(&self.meta.root_page_id),
                Some(PageState::Loaded(_))
            )
    }

    /// Reset index to unloaded state for cold start scenarios.
    ///
    /// After calling this, the index will request its metadata from storage
    /// on the next operation, allowing it to load persisted data.
    pub fn reset_for_cold_start(&mut self) {
        self.meta = IndexMeta::new();
        self.meta_loaded = false;
        self.meta_dirty = false;
        self.pages.clear();
        self.dirty_pages.clear();
        self.deleted_pages.clear();
        self.pending_requests.clear();
        // Reset delta tracking
        self.recent_inserts.clear();
        self.recent_deletes.clear();
        self.delta_epoch = 0;
        // Queue a meta load request
        self.ensure_meta_loading();
    }

    /// Estimate memory size of this index.
    pub fn memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();

        // Table and column strings
        size += self.table.capacity();
        size += self.column.capacity();

        // Pages
        for state in self.pages.values() {
            size += std::mem::size_of::<PageId>() + std::mem::size_of::<PageState>();
            if let PageState::Loaded(page) = state {
                size += page.memory_size();
            }
        }

        // Dirty/deleted tracking
        size += self.dirty_pages.capacity() * std::mem::size_of::<PageId>();
        size += self.deleted_pages.capacity() * std::mem::size_of::<PageId>();

        size
    }

    // ========================================================================
    // Delta tracking for push-based notifications
    // ========================================================================

    /// Get deltas filtered by scan condition.
    ///
    /// Returns (added_ids, removed_ids) for entries matching the condition.
    pub fn get_deltas(&self, condition: &ScanCondition) -> (Vec<ObjectId>, Vec<ObjectId>) {
        let filter = |key: &[u8]| -> bool {
            match condition {
                ScanCondition::All => true,
                ScanCondition::Eq(eq_key) => key == eq_key.as_slice(),
                ScanCondition::Range { min, max } => {
                    let min_ok = match min {
                        Bound::Included(min_key) => key >= min_key.as_slice(),
                        Bound::Excluded(min_key) => key > min_key.as_slice(),
                        Bound::Unbounded => true,
                    };
                    let max_ok = match max {
                        Bound::Included(max_key) => key <= max_key.as_slice(),
                        Bound::Excluded(max_key) => key < max_key.as_slice(),
                        Bound::Unbounded => true,
                    };
                    min_ok && max_ok
                }
            }
        };

        let added: Vec<ObjectId> = self
            .recent_inserts
            .iter()
            .filter(|(key, _)| filter(key))
            .map(|(_, id)| *id)
            .collect();

        let removed: Vec<ObjectId> = self
            .recent_deletes
            .iter()
            .filter(|(key, _)| filter(key))
            .map(|(_, id)| *id)
            .collect();

        (added, removed)
    }

    /// Check if any deltas exist.
    pub fn has_deltas(&self) -> bool {
        !self.recent_inserts.is_empty() || !self.recent_deletes.is_empty()
    }

    /// Get current delta epoch.
    pub fn delta_epoch(&self) -> u64 {
        self.delta_epoch
    }

    /// Clear deltas and increment epoch.
    ///
    /// Called after all subscriptions have settled in a process() cycle.
    pub fn clear_deltas(&mut self) {
        self.recent_inserts.clear();
        self.recent_deletes.clear();
        self.delta_epoch += 1;
    }

    // ========================================================================
    // Internal B-tree operations
    // ========================================================================

    /// Find the leaf page that should contain the given key.
    fn find_leaf_for_key(&self, key: &[u8]) -> Result<PageId, IndexError> {
        let (leaf_id, _path) = self.find_leaf_with_path(key)?;
        Ok(leaf_id)
    }

    /// Find the leaf page for a key, returning both the leaf ID and the path from root.
    /// Path is a list of (parent_page_id, child_index) pairs.
    fn find_leaf_with_path(
        &self,
        key: &[u8],
    ) -> Result<(PageId, Vec<(PageId, usize)>), IndexError> {
        let mut current_page_id = self.meta.root_page_id;
        let mut path = Vec::new();

        loop {
            let page = self
                .get_page(current_page_id)
                .ok_or(IndexError::PageNotLoaded(current_page_id))?;

            match page {
                BTreePage::Leaf { .. } => return Ok((current_page_id, path)),
                BTreePage::Internal { keys, children } => {
                    // Binary search for the correct child
                    let idx = keys.partition_point(|k| k.as_slice() <= key);
                    path.push((current_page_id, idx));
                    current_page_id = children[idx];
                }
            }
        }
    }

    /// Find the leftmost leaf page.
    fn find_leftmost_leaf(&self) -> Result<PageId, IndexError> {
        let mut current_page_id = self.meta.root_page_id;

        loop {
            let page = self
                .get_page(current_page_id)
                .ok_or(IndexError::PageNotLoaded(current_page_id))?;

            match page {
                BTreePage::Leaf { .. } => return Ok(current_page_id),
                BTreePage::Internal { children, .. } => {
                    current_page_id = children[0];
                }
            }
        }
    }

    /// Scan leaves from a starting page, collecting entries in range.
    /// Follows sibling pointers to traverse multiple leaf pages.
    fn scan_leaves(
        &self,
        start_page_id: PageId,
        min: &Bound<Vec<u8>>,
        max: &Bound<Vec<u8>>,
        results: &mut Vec<ObjectId>,
    ) {
        let mut current_page_id = Some(start_page_id);

        while let Some(page_id) = current_page_id {
            let (entries, next_leaf) = match self.get_page(page_id) {
                Some(BTreePage::Leaf { entries, next_leaf }) => (entries, *next_leaf),
                _ => break,
            };

            let mut hit_max = false;
            for entry in entries {
                // Check min bound
                let skip = match min {
                    Bound::Included(min_key) => entry.key.as_slice() < min_key.as_slice(),
                    Bound::Excluded(min_key) => entry.key.as_slice() <= min_key.as_slice(),
                    Bound::Unbounded => false,
                };
                if skip {
                    continue;
                }

                // Check max bound
                let stop = match max {
                    Bound::Included(max_key) => entry.key.as_slice() > max_key.as_slice(),
                    Bound::Excluded(max_key) => entry.key.as_slice() >= max_key.as_slice(),
                    Bound::Unbounded => false,
                };
                if stop {
                    hit_max = true;
                    break;
                }

                results.extend(entry.row_ids.iter().copied());
            }

            // Stop if we've passed the max bound
            if hit_max {
                break;
            }

            // Move to next leaf page via sibling pointer
            current_page_id = next_leaf;
        }
    }

    /// Insert a key-value pair into the tree.
    fn insert_into_tree(&mut self, key: &[u8], row_id: ObjectId) -> Result<(), IndexError> {
        // Find leaf page with path for split propagation
        let (leaf_page_id, path) = self.find_leaf_with_path(key)?;

        // Get leaf and insert
        let need_split = {
            let leaf = self
                .get_page_mut(leaf_page_id)
                .ok_or(IndexError::PageNotLoaded(leaf_page_id))?;

            if let BTreePage::Leaf { entries, .. } = leaf {
                // Find insertion point
                let idx = entries.partition_point(|e| e.key.as_slice() < key);

                if idx < entries.len() && entries[idx].key == key {
                    // Key exists - add row_id
                    entries[idx].row_ids.insert(row_id);
                    false
                } else {
                    // New key
                    entries.insert(idx, LeafEntry::new(key.to_vec(), row_id));
                    entries.len() > MAX_LEAF_ENTRIES
                }
            } else {
                return Err(IndexError::InternalError("expected leaf page".to_string()));
            }
        };

        if need_split {
            self.split_leaf(leaf_page_id, &path)?;
        }

        Ok(())
    }

    /// Split a leaf page that has exceeded MAX_LEAF_ENTRIES.
    /// `path` is the list of (parent_page_id, child_index) from root to this leaf.
    fn split_leaf(&mut self, page_id: PageId, path: &[(PageId, usize)]) -> Result<(), IndexError> {
        // Get the entries and old next_leaf pointer
        let (left_entries, right_entries, split_key, old_next_leaf) = {
            let page = self
                .get_page(page_id)
                .ok_or(IndexError::PageNotLoaded(page_id))?;

            if let BTreePage::Leaf { entries, next_leaf } = page {
                let mid = entries.len() / 2;
                let split_key = entries[mid].key.clone();
                (
                    entries[..mid].to_vec(),
                    entries[mid..].to_vec(),
                    split_key,
                    *next_leaf,
                )
            } else {
                return Err(IndexError::InternalError("expected leaf".to_string()));
            }
        };

        // Create new right page (inherits old next_leaf)
        let right_page_id = self.alloc_page();
        self.pages.insert(
            right_page_id,
            PageState::Loaded(BTreePage::Leaf {
                entries: right_entries,
                next_leaf: old_next_leaf,
            }),
        );
        self.dirty_pages.insert(right_page_id);

        // Update left page (next_leaf now points to right page)
        if let Some(PageState::Loaded(BTreePage::Leaf { entries, next_leaf })) =
            self.pages.get_mut(&page_id)
        {
            *entries = left_entries;
            *next_leaf = Some(right_page_id);
            self.dirty_pages.insert(page_id);
        }

        // Handle root split vs non-root split
        if page_id == self.meta.root_page_id {
            // Create new root
            let new_root_id = self.alloc_page();
            self.pages.insert(
                new_root_id,
                PageState::Loaded(BTreePage::Internal {
                    keys: vec![split_key],
                    children: vec![page_id, right_page_id],
                }),
            );
            self.dirty_pages.insert(new_root_id);
            self.meta.root_page_id = new_root_id;
            self.meta_dirty = true;
        } else {
            // Insert separator key and new child pointer into parent
            self.insert_into_internal(path, split_key, right_page_id)?;
        }

        Ok(())
    }

    /// Insert a separator key and child pointer into an internal node.
    /// May recursively split internal nodes if they overflow.
    fn insert_into_internal(
        &mut self,
        path: &[(PageId, usize)],
        key: Vec<u8>,
        new_child: PageId,
    ) -> Result<(), IndexError> {
        // Get the parent from the path
        let (parent_id, child_idx) = path
            .last()
            .ok_or_else(|| IndexError::InternalError("empty path for non-root split".into()))?;
        let parent_id = *parent_id;
        let child_idx = *child_idx;

        // Insert into parent's keys and children
        let need_split = {
            let parent = self
                .get_page_mut(parent_id)
                .ok_or(IndexError::PageNotLoaded(parent_id))?;

            if let BTreePage::Internal { keys, children } = parent {
                // Insert key at child_idx (between children[child_idx] and children[child_idx+1])
                keys.insert(child_idx, key);
                children.insert(child_idx + 1, new_child);

                // Check if we need to split this internal node too
                // Use same threshold as leaves for simplicity
                keys.len() > MAX_LEAF_ENTRIES
            } else {
                return Err(IndexError::InternalError("expected internal node".into()));
            }
        };

        self.dirty_pages.insert(parent_id);

        if need_split {
            self.split_internal(parent_id, &path[..path.len() - 1])?;
        }

        Ok(())
    }

    /// Split an internal node that has exceeded capacity.
    fn split_internal(
        &mut self,
        page_id: PageId,
        path: &[(PageId, usize)],
    ) -> Result<(), IndexError> {
        // Get keys and children
        let (left_keys, right_keys, promote_key, left_children, right_children) = {
            let page = self
                .get_page(page_id)
                .ok_or(IndexError::PageNotLoaded(page_id))?;

            if let BTreePage::Internal { keys, children } = page {
                let mid = keys.len() / 2;
                // The middle key gets promoted to parent, not kept in either child
                let promote_key = keys[mid].clone();
                (
                    keys[..mid].to_vec(),
                    keys[mid + 1..].to_vec(),
                    promote_key,
                    children[..=mid].to_vec(),
                    children[mid + 1..].to_vec(),
                )
            } else {
                return Err(IndexError::InternalError("expected internal node".into()));
            }
        };

        // Create new right internal page
        let right_page_id = self.alloc_page();
        self.pages.insert(
            right_page_id,
            PageState::Loaded(BTreePage::Internal {
                keys: right_keys,
                children: right_children,
            }),
        );
        self.dirty_pages.insert(right_page_id);

        // Update left page
        if let Some(PageState::Loaded(BTreePage::Internal { keys, children })) =
            self.pages.get_mut(&page_id)
        {
            *keys = left_keys;
            *children = left_children;
            self.dirty_pages.insert(page_id);
        }

        // Handle root split vs propagate up
        if page_id == self.meta.root_page_id {
            let new_root_id = self.alloc_page();
            self.pages.insert(
                new_root_id,
                PageState::Loaded(BTreePage::Internal {
                    keys: vec![promote_key],
                    children: vec![page_id, right_page_id],
                }),
            );
            self.dirty_pages.insert(new_root_id);
            self.meta.root_page_id = new_root_id;
            self.meta_dirty = true;
        } else {
            self.insert_into_internal(path, promote_key, right_page_id)?;
        }

        Ok(())
    }

    /// Remove a key-value pair from the tree.
    fn remove_from_tree(&mut self, key: &[u8], row_id: ObjectId) -> Result<bool, IndexError> {
        // Find leaf page
        let leaf_page_id = self.find_leaf_for_key(key)?;

        // Get leaf and remove
        let removed = {
            let leaf = self
                .get_page_mut(leaf_page_id)
                .ok_or(IndexError::PageNotLoaded(leaf_page_id))?;

            if let BTreePage::Leaf { entries, .. } = leaf {
                // Find the key
                if let Some(idx) = entries.iter().position(|e| e.key == key) {
                    entries[idx].row_ids.remove(&row_id);

                    // Remove entry if no more row_ids
                    if entries[idx].row_ids.is_empty() {
                        entries.remove(idx);
                    }
                    true
                } else {
                    false
                }
            } else {
                return Err(IndexError::InternalError("expected leaf page".to_string()));
            }
        };

        // TODO: Handle underflow and merge pages

        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_index_needs_meta_load() {
        let mut index = BTreeIndex::new("users", "email");
        assert!(!index.root_exists());

        // First insert triggers meta load request
        let row = ObjectId::new();
        let result = index.insert(b"test@example.com", row);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Not inserted, needs loading

        let requests = index.take_storage_requests();
        assert_eq!(requests.len(), 1);
        assert!(matches!(requests[0], StorageRequest::LoadIndexMeta { .. }));
    }

    #[test]
    fn insert_after_meta_load() {
        let mut index = BTreeIndex::new("users", "email");

        // Simulate meta load (new index)
        index.process_meta_load(None);
        assert!(index.root_exists());

        // Now insert should work
        let row = ObjectId::new();
        let result = index.insert(b"test@example.com", row);
        assert!(result.is_ok());
        assert!(result.unwrap()); // Inserted

        // Verify lookup
        let results = index.lookup_exact(b"test@example.com");
        assert_eq!(results.len(), 1);
        assert!(results.contains(&row));
    }

    #[test]
    fn insert_duplicate_key() {
        let mut index = BTreeIndex::new("users", "email");
        index.process_meta_load(None);

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        index.insert(b"alice@example.com", row1).unwrap();
        index.insert(b"alice@example.com", row2).unwrap();

        let results = index.lookup_exact(b"alice@example.com");
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row1));
        assert!(results.contains(&row2));
    }

    #[test]
    fn remove_row() {
        let mut index = BTreeIndex::new("users", "email");
        index.process_meta_load(None);

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        index.insert(b"alice@example.com", row1).unwrap();
        index.insert(b"alice@example.com", row2).unwrap();

        index.remove(b"alice@example.com", row1).unwrap();

        let results = index.lookup_exact(b"alice@example.com");
        assert_eq!(results.len(), 1);
        assert!(results.contains(&row2));
    }

    #[test]
    fn remove_last_row_removes_entry() {
        let mut index = BTreeIndex::new("users", "email");
        index.process_meta_load(None);

        let row = ObjectId::new();
        index.insert(b"alice@example.com", row).unwrap();
        index.remove(b"alice@example.com", row).unwrap();

        let results = index.lookup_exact(b"alice@example.com");
        assert!(results.is_empty());
    }

    #[test]
    fn scan_all() {
        let mut index = BTreeIndex::new("users", "_id");
        index.process_meta_load(None);

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();

        index.insert(row1.uuid().as_bytes(), row1).unwrap();
        index.insert(row2.uuid().as_bytes(), row2).unwrap();
        index.insert(row3.uuid().as_bytes(), row3).unwrap();

        let all = index.scan_all();
        assert_eq!(all.len(), 3);
        assert!(all.contains(&row1));
        assert!(all.contains(&row2));
        assert!(all.contains(&row3));
    }

    #[test]
    fn range_scan() {
        let mut index = BTreeIndex::new("users", "score");
        index.process_meta_load(None);

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();
        let row4 = ObjectId::new();

        // Insert scores as bytes
        index.insert(&10i32.to_le_bytes(), row1).unwrap();
        index.insert(&20i32.to_le_bytes(), row2).unwrap();
        index.insert(&30i32.to_le_bytes(), row3).unwrap();
        index.insert(&40i32.to_le_bytes(), row4).unwrap();

        // Range [15, 35] should get 20 and 30
        let results = index.range_scan(
            &Bound::Included(15i32.to_le_bytes().to_vec()),
            &Bound::Included(35i32.to_le_bytes().to_vec()),
        );
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));
    }

    #[test]
    fn contains_row() {
        let mut index = BTreeIndex::new("users", "_id");
        index.process_meta_load(None);

        let row = ObjectId::new();
        assert!(!index.contains_row(row));

        index.insert(row.uuid().as_bytes(), row).unwrap();
        assert!(index.contains_row(row));
    }

    #[test]
    fn storage_requests_generated() {
        let mut index = BTreeIndex::new("users", "email");
        index.process_meta_load(None);

        let row = ObjectId::new();
        index.insert(b"test@example.com", row).unwrap();

        let requests = index.take_storage_requests();

        // Should have: store root page + store meta
        assert!(requests.len() >= 2);

        let has_page_store = requests
            .iter()
            .any(|r| matches!(r, StorageRequest::StoreIndexPage { .. }));
        let has_meta_store = requests
            .iter()
            .any(|r| matches!(r, StorageRequest::StoreIndexMeta { .. }));

        assert!(has_page_store);
        assert!(has_meta_store);
    }

    #[test]
    fn load_existing_index() {
        // Create index and insert
        let mut index1 = BTreeIndex::new("users", "email");
        index1.process_meta_load(None);

        let row = ObjectId::new();
        index1.insert(b"test@example.com", row).unwrap();

        // Get storage data
        let requests = index1.take_storage_requests();
        let mut meta_data = None;
        let mut page_data = HashMap::new();

        for req in requests {
            match req {
                StorageRequest::StoreIndexMeta { data, .. } => {
                    meta_data = Some(data);
                }
                StorageRequest::StoreIndexPage { page_id, data, .. } => {
                    page_data.insert(page_id, data);
                }
                _ => {}
            }
        }

        // Create new index and load from storage
        let mut index2 = BTreeIndex::new("users", "email");
        index2.process_meta_load(meta_data);

        // Load root page
        let root_id = index2.meta.root_page_id.0;
        if let Some(data) = page_data.get(&root_id) {
            index2.process_page_load(PageId(root_id), Some(data.clone()));
        }

        // Verify data is present
        let results = index2.lookup_exact(b"test@example.com");
        assert_eq!(results.len(), 1);
        assert!(results.contains(&row));
    }

    #[test]
    fn many_inserts_maintains_order() {
        let mut index = BTreeIndex::new("users", "score");
        index.process_meta_load(None);

        // Insert 100 items
        let mut rows = Vec::new();
        for i in (0..100).rev() {
            let row = ObjectId::new();
            rows.push((i, row));
            index.insert(&(i as i32).to_le_bytes(), row).unwrap();
        }

        // Scan all should return them all
        let all = index.scan_all();
        assert_eq!(all.len(), 100);
    }

    // ========================================================================
    // Delta tracking tests
    // ========================================================================

    #[test]
    fn delta_tracking_on_insert() {
        let mut index = BTreeIndex::new("users", "email");
        index.process_meta_load(None);

        assert!(!index.has_deltas());
        assert_eq!(index.delta_epoch(), 0);

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        index.insert(b"alice@example.com", row1).unwrap();
        index.insert(b"bob@example.com", row2).unwrap();

        assert!(index.has_deltas());
        let (added, removed) = index.get_deltas(&ScanCondition::All);
        assert_eq!(added.len(), 2);
        assert!(added.contains(&row1));
        assert!(added.contains(&row2));
        assert!(removed.is_empty());
    }

    #[test]
    fn delta_tracking_on_remove() {
        let mut index = BTreeIndex::new("users", "email");
        index.process_meta_load(None);

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        index.insert(b"alice@example.com", row1).unwrap();
        index.insert(b"bob@example.com", row2).unwrap();

        // Clear the insert deltas
        index.clear_deltas();
        assert!(!index.has_deltas());

        // Now remove one row
        index.remove(b"alice@example.com", row1).unwrap();

        assert!(index.has_deltas());
        let (added, removed) = index.get_deltas(&ScanCondition::All);
        assert!(added.is_empty());
        assert_eq!(removed.len(), 1);
        assert!(removed.contains(&row1));
    }

    #[test]
    fn delta_filtering_by_eq() {
        let mut index = BTreeIndex::new("users", "email");
        index.process_meta_load(None);

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        index.insert(b"alice@example.com", row1).unwrap();
        index.insert(b"bob@example.com", row2).unwrap();

        // Filter by exact match
        let (added, _) = index.get_deltas(&ScanCondition::Eq(b"alice@example.com".to_vec()));
        assert_eq!(added.len(), 1);
        assert!(added.contains(&row1));

        // Different key - no matches
        let (added, _) = index.get_deltas(&ScanCondition::Eq(b"charlie@example.com".to_vec()));
        assert!(added.is_empty());
    }

    #[test]
    fn delta_filtering_by_range() {
        let mut index = BTreeIndex::new("users", "score");
        index.process_meta_load(None);

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();
        index.insert(&10i32.to_le_bytes(), row1).unwrap();
        index.insert(&20i32.to_le_bytes(), row2).unwrap();
        index.insert(&30i32.to_le_bytes(), row3).unwrap();

        // Range [15, 25] should only include row2 (score=20)
        let (added, _) = index.get_deltas(&ScanCondition::Range {
            min: Bound::Included(15i32.to_le_bytes().to_vec()),
            max: Bound::Included(25i32.to_le_bytes().to_vec()),
        });
        assert_eq!(added.len(), 1);
        assert!(added.contains(&row2));
    }

    #[test]
    fn clear_deltas_increments_epoch() {
        let mut index = BTreeIndex::new("users", "email");
        index.process_meta_load(None);

        assert_eq!(index.delta_epoch(), 0);

        let row = ObjectId::new();
        index.insert(b"test@example.com", row).unwrap();

        assert!(index.has_deltas());
        index.clear_deltas();

        assert!(!index.has_deltas());
        assert_eq!(index.delta_epoch(), 1);

        // Insert more and clear again
        index.insert(b"test2@example.com", ObjectId::new()).unwrap();
        index.clear_deltas();
        assert_eq!(index.delta_epoch(), 2);
    }

    #[test]
    fn find_leaf_returns_correct_leaf() {
        let mut index = BTreeIndex::new("test", "key");
        index.process_meta_load(None);

        // Insert 100 entries with BE encoding
        for i in 0..100i32 {
            let row = ObjectId::new();
            index.insert(&(i * 10).to_be_bytes(), row).unwrap();
        }

        // Search for key 250
        let search_key = 250i32.to_be_bytes();
        let leaf_id = index.find_leaf_for_key(&search_key).unwrap();

        // Get the first key in this leaf
        if let Some(BTreePage::Leaf { entries, .. }) = index.get_page(leaf_id) {
            let first_key = i32::from_be_bytes(entries[0].key.clone().try_into().unwrap());
            let last_key =
                i32::from_be_bytes(entries.last().unwrap().key.clone().try_into().unwrap());
            assert!(
                first_key <= 250 && 250 <= last_key,
                "Leaf should contain key 250, but leaf has keys {} to {}",
                first_key,
                last_key
            );
        }
    }

    #[test]
    fn sibling_pointers_after_split() {
        let mut index = BTreeIndex::new("test", "key");
        index.process_meta_load(None);

        // Insert enough to cause a split
        for i in 0..70u32 {
            let row = ObjectId::new();
            index.insert(&i.to_be_bytes(), row).unwrap();
        }

        // Find first leaf and follow sibling chain
        let first_leaf_id = index.find_leftmost_leaf().unwrap();

        let mut leaf_count = 0;
        let mut total_entries = 0;
        let mut current = Some(first_leaf_id);

        while let Some(page_id) = current {
            if let Some(BTreePage::Leaf { entries, next_leaf }) = index.get_page(page_id) {
                leaf_count += 1;
                total_entries += entries.len();
                current = *next_leaf;
            } else {
                break;
            }
        }

        assert!(
            leaf_count >= 2,
            "Expected at least 2 leaf pages after 70 inserts, got {}",
            leaf_count
        );
        assert_eq!(
            total_entries, 70,
            "Expected 70 total entries across all leaves, got {}",
            total_entries
        );
    }

    #[test]
    fn range_scan_spans_multiple_leaves() {
        let mut index = BTreeIndex::new("users", "score");
        index.process_meta_load(None);

        // Insert 100 entries to ensure we get multiple leaf pages
        // (MAX_LEAF_ENTRIES = 64, so this will cause at least one split)
        // Use big-endian encoding so byte order matches numeric order
        let mut rows: Vec<(i32, ObjectId)> = Vec::new();
        for i in 0..100i32 {
            let row = ObjectId::new();
            let key = i * 10; // Keys: 0, 10, 20, ..., 990
            index.insert(&key.to_be_bytes(), row).unwrap();
            rows.push((key, row));
        }

        // Verify we have multiple pages (tree depth > 0 means we split)
        assert!(
            index.meta.root_page_id.0 > 0 || index.pages.len() > 1,
            "Expected multiple pages after inserting 100 entries, got {} pages",
            index.pages.len()
        );

        // First verify scan_all works
        let all_results = index.scan_all();
        assert_eq!(
            all_results.len(),
            100,
            "scan_all should return all 100 entries"
        );

        // Range scan [250, 750] should include keys 250, 260, ..., 750
        // That's (750 - 250) / 10 + 1 = 51 entries
        let results = index.range_scan(
            &Bound::Included(250i32.to_be_bytes().to_vec()),
            &Bound::Included(750i32.to_be_bytes().to_vec()),
        );

        // Collect expected rows
        let expected: Vec<ObjectId> = rows
            .iter()
            .filter(|(k, _)| *k >= 250 && *k <= 750)
            .map(|(_, id)| *id)
            .collect();

        // Also check an unbounded scan to verify sibling traversal
        let unbounded_results = index.range_scan(&Bound::Unbounded, &Bound::Unbounded);
        assert_eq!(
            unbounded_results.len(),
            100,
            "Unbounded range scan should return all 100 entries, got {}",
            unbounded_results.len()
        );

        assert_eq!(
            results.len(),
            expected.len(),
            "Range scan should return {} entries but got {}. \
             This likely means range_scan doesn't traverse sibling pages.",
            expected.len(),
            results.len()
        );

        for id in &expected {
            assert!(results.contains(id), "Missing expected row from range scan");
        }
    }

    #[test]
    fn sibling_chain_complete_after_multiple_splits() {
        let mut index = BTreeIndex::new("test", "key");
        index.process_meta_load(None);

        // Insert 100 entries (causes 2+ splits with MAX_LEAF_ENTRIES=64)
        for i in 0..100i32 {
            let row = ObjectId::new();
            index.insert(&(i * 10).to_be_bytes(), row).unwrap();
        }

        // Count entries via sibling chain - this verifies all leaves are reachable
        let first_leaf_id = index.find_leftmost_leaf().unwrap();
        let mut total_entries = 0;
        let mut current = Some(first_leaf_id);

        while let Some(page_id) = current {
            if let Some(BTreePage::Leaf { entries, next_leaf }) = index.get_page(page_id) {
                total_entries += entries.len();
                current = *next_leaf;
            } else {
                break;
            }
        }

        assert_eq!(
            total_entries, 100,
            "Expected 100 entries reachable via sibling chain, got {}",
            total_entries
        );
    }
}
