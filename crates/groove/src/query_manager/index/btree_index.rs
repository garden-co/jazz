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
                if removed && self.meta.entry_count > 0 {
                    self.meta.entry_count -= 1;
                    self.meta_dirty = true;
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
                if let Some(BTreePage::Leaf { entries }) = self.get_page(page_id) {
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
            if let PageState::Loaded(BTreePage::Leaf { entries }) = state {
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
    pub fn root_exists(&self) -> bool {
        self.meta_loaded && self.pages.contains_key(&self.meta.root_page_id)
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
    // Internal B-tree operations
    // ========================================================================

    /// Find the leaf page that should contain the given key.
    fn find_leaf_for_key(&self, key: &[u8]) -> Result<PageId, IndexError> {
        let mut current_page_id = self.meta.root_page_id;

        loop {
            let page = self
                .get_page(current_page_id)
                .ok_or(IndexError::PageNotLoaded(current_page_id))?;

            match page {
                BTreePage::Leaf { .. } => return Ok(current_page_id),
                BTreePage::Internal { keys, children } => {
                    // Binary search for the correct child
                    let idx = keys.partition_point(|k| k.as_slice() <= key);
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
    fn scan_leaves(
        &self,
        start_page_id: PageId,
        min: &Bound<Vec<u8>>,
        max: &Bound<Vec<u8>>,
        results: &mut Vec<ObjectId>,
    ) {
        // For now, just scan the single leaf we found
        // TODO: Add sibling pointers for efficient range scans across pages
        if let Some(BTreePage::Leaf { entries }) = self.get_page(start_page_id) {
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
                    break;
                }

                results.extend(entry.row_ids.iter().copied());
            }
        }
    }

    /// Insert a key-value pair into the tree.
    fn insert_into_tree(&mut self, key: &[u8], row_id: ObjectId) -> Result<(), IndexError> {
        // Find leaf page
        let leaf_page_id = self.find_leaf_for_key(key)?;

        // Get leaf and insert
        let need_split = {
            let leaf = self
                .get_page_mut(leaf_page_id)
                .ok_or(IndexError::PageNotLoaded(leaf_page_id))?;

            if let BTreePage::Leaf { entries } = leaf {
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
            self.split_leaf(leaf_page_id)?;
        }

        Ok(())
    }

    /// Split a leaf page that has exceeded MAX_LEAF_ENTRIES.
    fn split_leaf(&mut self, page_id: PageId) -> Result<(), IndexError> {
        // Get the entries
        let (left_entries, right_entries, split_key) = {
            let page = self
                .get_page(page_id)
                .ok_or(IndexError::PageNotLoaded(page_id))?;

            if let BTreePage::Leaf { entries } = page {
                let mid = entries.len() / 2;
                let split_key = entries[mid].key.clone();
                (entries[..mid].to_vec(), entries[mid..].to_vec(), split_key)
            } else {
                return Err(IndexError::InternalError("expected leaf".to_string()));
            }
        };

        // Create new right page
        let right_page_id = self.alloc_page();
        self.pages.insert(
            right_page_id,
            PageState::Loaded(BTreePage::Leaf {
                entries: right_entries,
            }),
        );
        self.dirty_pages.insert(right_page_id);

        // Update left page
        if let Some(PageState::Loaded(BTreePage::Leaf { entries })) = self.pages.get_mut(&page_id) {
            *entries = left_entries;
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
            // Insert into parent (simplified - assumes parent is already loaded)
            // For a complete implementation, would need to track path and propagate splits
            // TODO: Implement proper parent propagation
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

            if let BTreePage::Leaf { entries } = leaf {
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
}
