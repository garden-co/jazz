//! An asynchronous B-tree designed for IndexedDB page persistence.
//!
//! The tree owns structure, residency, and dirty generations. Its backing
//! [`PageStore`] is intentionally dumb: it reads opaque pages and atomically
//! replaces a set of pages together with the current root metadata. IndexedDB
//! provides that atomic commit, so this engine has no WAL or checkpoint phase.

mod page;
mod store;
#[cfg(target_arch = "wasm32")]
mod web;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::rc::Rc;

pub use store::{BoxFuture, Commit, MemoryPageStore, Metadata, PageReclamationToken, PageStore};
#[cfg(target_arch = "wasm32")]
pub use web::IndexedDbPageStore;

use page::{Page, PageId, ValueCell, decode_page, encode_page};

const DEFAULT_PAGE_SIZE: usize = 16 * 1024;
const MIN_PAGE_SIZE: usize = 1024;
const MAX_JS_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

pub type KeyValue = (Vec<u8>, Vec<u8>);
type LeafEntry = (Vec<u8>, ValueCell);
/// A resident root-to-leaf walk. `visited` belongs to the whole logical
/// operation, rather than merely to the structural descent: an overflow chain
/// must not alias a structural page, and a caller which goes on to inspect a
/// value must continue using this same ownership set.
type Descent = (
    PageId,
    Vec<LeafEntry>,
    Vec<(PageId, usize)>,
    HashSet<PageId>,
);

enum PageReplacement {
    One(PageId),
    Split {
        left: PageId,
        separator: Vec<u8>,
        right: PageId,
    },
}

pub enum WriteOperation {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid IDBTree options: {0}")]
    InvalidOptions(String),
    #[error("IDBTree page {0} is missing")]
    MissingPage(PageId),
    #[error("invalid IDBTree page: {0}")]
    InvalidPage(String),
    #[error("IDBTree page {page_id} exceeds the configured {page_size}-byte page size")]
    PageTooLarge { page_id: PageId, page_size: usize },
    #[error("IDBTree store error: {0}")]
    Store(String),
    #[error("IDBTree generation conflict: {0}")]
    GenerationConflict(String),
    #[error("an IDBTree commit is already in flight")]
    CommitInFlight,
}

#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub page_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
        }
    }
}

impl Options {
    fn validate(self) -> Result<Self, Error> {
        if self.page_size < MIN_PAGE_SIZE
            || !self.page_size.is_power_of_two()
            || self.page_size > u32::MAX as usize
        {
            return Err(Error::InvalidOptions(format!(
                "page_size must be a power of two between {MIN_PAGE_SIZE} and {}",
                u32::MAX
            )));
        }
        Ok(self)
    }
}

/// The tree core. Cached pages make resident reads complete on their first
/// poll; a cache miss awaits exactly the required page from the page store.
struct TreeCore<S> {
    store: S,
    options: Options,
    metadata: Metadata,
    pages: HashMap<PageId, Page>,
    dirty: BTreeMap<PageId, Page>,
    /// Page ids retired by COW replacement in the current unpublished state.
    /// At commit preparation these are split into fresh ids, which were never
    /// durable, and durable ids sent through Commit only while the backing
    /// store currently proves exclusive ownership.
    retired: BTreeSet<PageId>,
    commit_in_flight: bool,
}

/// A write only appends fresh COW page ids and advances root/allocation
/// metadata. Remember that small frontier instead of cloning the resident page
/// cache for every operation.
struct WriteCheckpoint {
    metadata: Metadata,
    retired: BTreeSet<PageId>,
}

#[derive(Debug)]
pub struct PreparedCommit {
    commit: Commit,
    /// Retired pages allocated in the dirty generation. They were never
    /// durable, so they are intentionally absent from commit writes/deletions.
    fresh_retired_page_ids: Vec<PageId>,
}

enum Attempt<T> {
    Ready(T),
    Missing(PageId),
}

impl<T> Attempt<T> {
    fn map<U>(self, map: impl FnOnce(T) -> U) -> Attempt<U> {
        match self {
            Self::Ready(value) => Attempt::Ready(map(value)),
            Self::Missing(page_id) => Attempt::Missing(page_id),
        }
    }
}

/// Cloneable, single-threaded handle used by Groove. No `RefCell` borrow is
/// held across page I/O: operations attempt synchronously against resident
/// pages, hydrate one precise miss, then retry.
#[derive(Clone)]
pub struct IdbTree<S> {
    inner: Rc<RefCell<TreeCore<S>>>,
}

impl<S: PageStore + Clone> IdbTree<S> {
    pub async fn open(store: S, options: Options) -> Result<Self, Error> {
        Ok(Self {
            inner: Rc::new(RefCell::new(TreeCore::open(store, options).await?)),
        })
    }

    pub fn metadata(&self) -> Metadata {
        self.inner.borrow().metadata.clone()
    }

    pub fn dirty_page_count(&self) -> usize {
        self.inner.borrow().dirty.len()
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        loop {
            let (attempt, generation) = {
                let tree = self.inner.borrow();
                (tree.try_get(key)?, tree.metadata.generation)
            };
            match attempt {
                Attempt::Ready(value) => return Ok(value),
                Attempt::Missing(page_id) => self.hydrate(page_id, generation).await?,
            }
        }
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        loop {
            let (attempt, generation) = {
                let mut tree = self.inner.borrow_mut();
                let attempt =
                    tree.with_write_attempt_checkpoint(|tree| tree.try_put(&key, &value))?;
                (attempt, tree.metadata.generation)
            };
            match attempt {
                Attempt::Ready(()) => return Ok(()),
                Attempt::Missing(page_id) => self.hydrate(page_id, generation).await?,
            }
        }
    }

    pub async fn delete(&self, key: &[u8]) -> Result<bool, Error> {
        loop {
            let (attempt, generation) = {
                let mut tree = self.inner.borrow_mut();
                let attempt = tree.with_write_attempt_checkpoint(|tree| tree.try_delete(key))?;
                (attempt, tree.metadata.generation)
            };
            match attempt {
                Attempt::Ready(deleted) => return Ok(deleted),
                Attempt::Missing(page_id) => self.hydrate(page_id, generation).await?,
            }
        }
    }

    pub async fn write_many(&self, operations: Vec<WriteOperation>) -> Result<(), Error> {
        for operation in &operations {
            let key = match operation {
                WriteOperation::Set { key, .. } | WriteOperation::Delete { key } => key,
            };
            loop {
                let (attempt, generation) = {
                    let tree = self.inner.borrow();
                    (tree.write_path_resident(key)?, tree.metadata.generation)
                };
                match attempt {
                    Attempt::Ready(()) => break,
                    Attempt::Missing(page_id) => self.hydrate(page_id, generation).await?,
                }
            }
        }

        self.inner
            .borrow_mut()
            .with_write_checkpoint(|tree| tree.apply_write_many(operations))
    }

    pub async fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<KeyValue>, Error> {
        self.range_limit(start, end, usize::MAX).await
    }

    /// Return at most `limit` rows in canonical forward order without walking
    /// or hydrating pages after the bound is satisfied.
    pub async fn range_limit(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<KeyValue>, Error> {
        loop {
            let (attempt, generation) = {
                let tree = self.inner.borrow();
                (tree.try_range(start, end, limit)?, tree.metadata.generation)
            };
            match attempt {
                Attempt::Ready(rows) => return Ok(rows),
                Attempt::Missing(page_id) => self.hydrate(page_id, generation).await?,
            }
        }
    }

    pub async fn range_reverse(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<KeyValue>, Error> {
        loop {
            let (attempt, generation) = {
                let tree = self.inner.borrow();
                (
                    tree.try_range_reverse(start, end, limit)?,
                    tree.metadata.generation,
                )
            };
            match attempt {
                Attempt::Ready(rows) => return Ok(rows),
                Attempt::Missing(page_id) => self.hydrate(page_id, generation).await?,
            }
        }
    }

    pub async fn flush(&self) -> Result<(), Error> {
        let (store, prepared) = {
            let mut tree = self.inner.borrow_mut();
            (tree.store.clone(), tree.prepare_commit()?)
        };
        let Some(mut prepared) = prepared else {
            return Ok(());
        };
        // Capability lifetime can end after preparation but before the async
        // store submission. Never send path-local deletions after ownership
        // has been lost; TreeCore retains those durable retirement intents.
        if store.reclamation_token().is_none() {
            prepared.commit.deleted_page_ids.clear();
        }
        let outcome = store.commit(prepared.commit()).await;
        self.inner.borrow_mut().complete_commit(prepared, outcome)
    }

    async fn hydrate(&self, page_id: PageId, expected_generation: u64) -> Result<(), Error> {
        let store = {
            let tree = self.inner.borrow();
            if tree.pages.contains_key(&page_id) {
                return Ok(());
            }
            tree.store.clone()
        };
        let bytes = store.read_page(page_id).await.map_err(Error::Store)?;
        let (generation, page_size) = {
            let tree = self.inner.borrow();
            (tree.metadata.generation, tree.options.page_size)
        };
        if generation != expected_generation {
            // A newer publication invalidates this miss. The old read may
            // return either the retired page or no page after deletion; in
            // both cases discard it and retry from the current root.
            return Ok(());
        }
        let bytes = bytes.ok_or(Error::MissingPage(page_id))?;
        if bytes.len() > page_size {
            return Err(Error::PageTooLarge { page_id, page_size });
        }
        let page = decode_page(&bytes).map_err(Error::InvalidPage)?;
        self.inner.borrow_mut().pages.entry(page_id).or_insert(page);
        Ok(())
    }
}

impl PreparedCommit {
    pub fn commit(&self) -> &Commit {
        &self.commit
    }
}

impl<S: PageStore> TreeCore<S> {
    fn apply_write_many(&mut self, operations: Vec<WriteOperation>) -> Result<(), Error> {
        for operation in operations {
            let attempt = match operation {
                WriteOperation::Set { key, value } => self.try_put(&key, &value)?,
                WriteOperation::Delete { key } => self.try_delete(&key)?.map(|_| ()),
            };
            if let Attempt::Missing(page_id) = attempt {
                return Err(Error::InvalidPage(format!(
                    "prepared batch unexpectedly missed page {page_id}"
                )));
            }
        }
        Ok(())
    }

    fn write_checkpoint(&self) -> WriteCheckpoint {
        WriteCheckpoint {
            metadata: self.metadata.clone(),
            retired: self.retired.clone(),
        }
    }

    fn with_write_checkpoint<T>(
        &mut self,
        write: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let checkpoint = self.write_checkpoint();
        match write(self) {
            Ok(value) => Ok(value),
            Err(error) => {
                self.rollback_write(checkpoint);
                Err(error)
            }
        }
    }

    fn with_write_attempt_checkpoint<T>(
        &mut self,
        write: impl FnOnce(&mut Self) -> Result<Attempt<T>, Error>,
    ) -> Result<Attempt<T>, Error> {
        let checkpoint = self.write_checkpoint();
        match write(self) {
            Ok(Attempt::Ready(value)) => Ok(Attempt::Ready(value)),
            Ok(Attempt::Missing(page_id)) => {
                self.rollback_write(checkpoint);
                Ok(Attempt::Missing(page_id))
            }
            Err(error) => {
                self.rollback_write(checkpoint);
                Err(error)
            }
        }
    }

    fn rollback_write(&mut self, checkpoint: WriteCheckpoint) {
        let allocated_start = checkpoint.metadata.next_page_id;
        let allocated_end = self.metadata.next_page_id;
        debug_assert!(allocated_end >= allocated_start);
        for page_id in allocated_start..allocated_end {
            self.pages.remove(&page_id);
            self.dirty.remove(&page_id);
        }
        self.retired = checkpoint.retired;
        self.metadata = checkpoint.metadata;
    }

    pub async fn open(store: S, options: Options) -> Result<Self, Error> {
        let options = options.validate()?;
        let metadata = store.load_metadata().await.map_err(Error::Store)?;
        let mut tree = Self {
            store,
            options,
            metadata: metadata.unwrap_or_else(|| Metadata::empty(options.page_size)),
            pages: HashMap::new(),
            dirty: BTreeMap::new(),
            retired: BTreeSet::new(),
            commit_in_flight: false,
        };
        if tree.metadata.page_size != options.page_size {
            return Err(Error::InvalidOptions(format!(
                "store uses {}-byte pages, requested {}",
                tree.metadata.page_size, options.page_size
            )));
        }
        if tree.metadata.root_page_id.is_none() {
            let root = tree.allocate_page(Page::leaf())?;
            tree.metadata.root_page_id = Some(root);
        }
        Ok(tree)
    }

    fn try_get(&self, key: &[u8]) -> Result<Attempt<Option<Vec<u8>>>, Error> {
        let Some((_, entries, _, mut visited)) = self.resident_descent(key)? else {
            return Ok(Attempt::Missing(self.missing_page_for_key(key)?));
        };
        let value = entries
            .binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
            .ok()
            .map(|index| entries[index].1.clone());
        match value {
            Some(value) => self
                .read_value_resident(&value, &mut visited)
                .map(|attempt| match attempt {
                    Attempt::Ready(value) => Attempt::Ready(Some(value)),
                    Attempt::Missing(page_id) => Attempt::Missing(page_id),
                }),
            None => Ok(Attempt::Ready(None)),
        }
    }

    fn try_put(&mut self, key: &[u8], value: &[u8]) -> Result<Attempt<()>, Error> {
        let Some((page_id, entries, path, mut visited)) = self.resident_descent(key)? else {
            return Ok(Attempt::Missing(self.missing_page_for_key(key)?));
        };
        // A write copies this complete leaf into a new immutable page. Verify
        // every retained value edge under the same ownership set before doing
        // so; otherwise a point update could silently perpetuate a malformed
        // sibling overflow graph.
        if let Attempt::Missing(page_id) = self.leaf_values_resident(&entries, &mut visited)? {
            return Ok(Attempt::Missing(page_id));
        }
        let mut entries = entries;
        let new_value = self.build_value(value.to_vec())?;
        match entries.binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key)) {
            Ok(index) => {
                let old_value = entries[index].1.clone();
                self.retire_value(&old_value)?;
                entries[index].1 = new_value;
            }
            Err(index) => entries.insert(index, (key.to_vec(), new_value)),
        }
        self.finish_leaf_write(page_id, entries, path)?;
        Ok(Attempt::Ready(()))
    }

    fn try_delete(&mut self, key: &[u8]) -> Result<Attempt<bool>, Error> {
        let Some((page_id, entries, path, mut visited)) = self.resident_descent(key)? else {
            return Ok(Attempt::Missing(self.missing_page_for_key(key)?));
        };
        let Ok(index) = entries.binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
        else {
            return Ok(Attempt::Ready(false));
        };
        // Deletion also republishes all surviving cells in this leaf.
        if let Attempt::Missing(page_id) = self.leaf_values_resident(&entries, &mut visited)? {
            return Ok(Attempt::Missing(page_id));
        }
        let mut entries = entries;
        let old_value = entries[index].1.clone();
        self.retire_value(&old_value)?;
        entries.remove(index);
        self.finish_leaf_write(page_id, entries, path)?;
        Ok(Attempt::Ready(true))
    }

    fn write_path_resident(&self, key: &[u8]) -> Result<Attempt<()>, Error> {
        let Some((_, entries, _, mut visited)) = self.resident_descent(key)? else {
            return Ok(Attempt::Missing(self.missing_page_for_key(key)?));
        };
        self.leaf_values_resident(&entries, &mut visited)
    }

    fn try_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Attempt<Vec<KeyValue>>, Error> {
        let mut cells = Vec::new();
        let mut visited = HashSet::new();
        if let Some(page_id) = self.collect_range_resident(
            self.root_page_id(),
            start,
            end,
            limit,
            &mut cells,
            &mut visited,
        )? {
            return Ok(Attempt::Missing(page_id));
        }
        let mut rows = Vec::with_capacity(cells.len());
        for (key, value) in cells {
            match self.read_value_resident(&value, &mut visited)? {
                Attempt::Ready(value) => rows.push((key, value)),
                Attempt::Missing(page_id) => return Ok(Attempt::Missing(page_id)),
            }
        }
        Ok(Attempt::Ready(rows))
    }

    fn try_range_reverse(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Attempt<Vec<KeyValue>>, Error> {
        let mut cells = Vec::new();
        let mut visited = HashSet::new();
        if let Some(page_id) = self.collect_range_reverse_resident(
            self.root_page_id(),
            start,
            end,
            limit,
            &mut cells,
            &mut visited,
        )? {
            return Ok(Attempt::Missing(page_id));
        }
        let mut rows = Vec::with_capacity(cells.len());
        for (key, value) in cells {
            match self.read_value_resident(&value, &mut visited)? {
                Attempt::Ready(value) => rows.push((key, value)),
                Attempt::Missing(page_id) => return Ok(Attempt::Missing(page_id)),
            }
        }
        Ok(Attempt::Ready(rows))
    }
    pub fn prepare_commit(&mut self) -> Result<Option<PreparedCommit>, Error> {
        if self.commit_in_flight {
            return Err(Error::CommitInFlight);
        }
        if self.dirty.is_empty() && self.retired.is_empty() {
            return Ok(None);
        }

        // A retired page in the active dirty map was allocated and replaced
        // before it ever became durable. Do not write or delete it: only the
        // surviving dirty closure and durable retirement intent cross the
        // persistence seam.
        let dirty_page_ids: BTreeSet<_> = self.dirty.keys().copied().collect();
        let fresh_retired: BTreeSet<_> = self
            .retired
            .intersection(&dirty_page_ids)
            .copied()
            .collect();
        let durable_retired: BTreeSet<_> =
            self.retired.difference(&dirty_page_ids).copied().collect();
        let reclamation_enabled = self.store.reclamation_token().is_some();
        let retired_for_commit: Vec<_> = if reclamation_enabled {
            durable_retired.iter().copied().collect()
        } else {
            Vec::new()
        };
        let fresh_retired_page_ids: Vec<_> = fresh_retired.iter().copied().collect();
        let mut pages = Vec::with_capacity(self.dirty.len() - fresh_retired.len());
        for (&page_id, page) in &self.dirty {
            if fresh_retired.contains(&page_id) {
                continue;
            }
            let bytes = encode_page(page).map_err(Error::InvalidPage)?;
            if bytes.len() > self.options.page_size {
                return Err(Error::PageTooLarge {
                    page_id,
                    page_size: self.options.page_size,
                });
            }
            pages.push((page_id, bytes));
        }
        if pages
            .iter()
            .any(|(page_id, _)| durable_retired.contains(page_id))
        {
            return Err(Error::InvalidPage(
                "IDBTree commit writes and deletes the same page".to_owned(),
            ));
        }

        self.commit_in_flight = true;
        let commit = Commit {
            expected_generation: self.metadata.generation,
            metadata: self.metadata.clone(),
            pages,
            deleted_page_ids: retired_for_commit,
        };
        // Fresh retired pages are unreachable and were never durable. Drop
        // them at the generation swap rather than retaining phantom cache
        // entries; a failed commit restores only durable commit pages.
        for page_id in &fresh_retired_page_ids {
            self.pages.remove(page_id);
        }
        self.retired = durable_retired;
        self.dirty.clear();
        Ok(Some(PreparedCommit {
            commit,
            fresh_retired_page_ids,
        }))
    }

    /// Reconcile an atomic commit result with writes made after
    /// [`prepare_commit`](Self::prepare_commit). On failure, pages unchanged
    /// since the swap are marked dirty again and durable retirement intent is
    /// restored; newer dirty versions win.
    pub fn complete_commit(
        &mut self,
        prepared: PreparedCommit,
        outcome: Result<Metadata, String>,
    ) -> Result<(), Error> {
        if !self.commit_in_flight {
            return Err(Error::InvalidPage(
                "completed an IDBTree commit that was not in flight".to_owned(),
            ));
        }
        self.commit_in_flight = false;
        let PreparedCommit {
            commit,
            fresh_retired_page_ids,
        } = prepared;
        debug_assert!(fresh_retired_page_ids.iter().all(|page_id| {
            !commit.pages.iter().any(|(written, _)| written == page_id)
                && commit.deleted_page_ids.binary_search(page_id).is_err()
        }));

        match outcome {
            Ok(committed) if committed.generation == commit.expected_generation + 1 => {
                // Root and allocation metadata may already describe writes in
                // the next dirty generation. Only advance its durable base.
                self.metadata.generation = committed.generation;
                // A successful reclaiming commit consumes only the retirement
                // ids it actually submitted. Intents omitted after capability
                // loss remain queued for a later reclaiming commit.
                for page_id in &commit.deleted_page_ids {
                    self.retired.remove(page_id);
                }
                // A newer dirty generation may retain a page id only under a
                // malformed alias. Never evict such a page while it is dirty.
                for page_id in &commit.deleted_page_ids {
                    if !self.dirty.contains_key(page_id) {
                        self.pages.remove(page_id);
                    }
                }
                Ok(())
            }
            Ok(committed) => {
                let error = format!(
                    "commit returned generation {}, expected {}",
                    committed.generation,
                    commit.expected_generation + 1
                );
                self.restore_failed_commit(&commit);
                Err(Error::Store(error))
            }
            Err(error) => {
                self.restore_failed_commit(&commit);
                if error.contains("generation changed") {
                    Err(Error::GenerationConflict(error))
                } else {
                    Err(Error::Store(error))
                }
            }
        }
    }

    fn restore_failed_commit(&mut self, commit: &Commit) {
        for (page_id, _) in &commit.pages {
            if self.dirty.contains_key(page_id) {
                continue;
            }
            if let Some(page) = self.pages.get(page_id) {
                self.dirty.insert(*page_id, page.clone());
            }
        }
        for page_id in &commit.deleted_page_ids {
            self.retire_page(*page_id);
        }
    }

    fn root_page_id(&self) -> PageId {
        self.metadata
            .root_page_id
            .expect("open always installs a root page")
    }

    fn resident_descent(&self, key: &[u8]) -> Result<Option<Descent>, Error> {
        let mut page_id = self.root_page_id();
        let mut path = Vec::new();
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(page_id) {
                return Err(Error::InvalidPage(
                    "tree child graph contains a cycle or shared page".to_owned(),
                ));
            }
            let Some(page) = self.pages.get(&page_id) else {
                return Ok(None);
            };
            match page {
                Page::Leaf { entries } => {
                    return Ok(Some((page_id, entries.clone(), path, visited)));
                }
                Page::Internal { keys, children } => {
                    let child_index = keys.partition_point(|separator| separator.as_slice() <= key);
                    path.push((page_id, child_index));
                    page_id = children[child_index];
                }
                Page::Overflow { .. } => {
                    return Err(Error::InvalidPage(
                        "overflow page reached during tree descent".to_owned(),
                    ));
                }
            }
        }
    }

    fn missing_page_for_key(&self, key: &[u8]) -> Result<PageId, Error> {
        let mut page_id = self.root_page_id();
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(page_id) {
                return Err(Error::InvalidPage(
                    "tree child graph contains a cycle or shared page".to_owned(),
                ));
            }
            let Some(page) = self.pages.get(&page_id) else {
                return Ok(page_id);
            };
            match page {
                Page::Leaf { .. } => {
                    return Err(Error::InvalidPage(
                        "requested a missing page for a resident descent".to_owned(),
                    ));
                }
                Page::Internal { keys, children } => {
                    page_id =
                        children[keys.partition_point(|separator| separator.as_slice() <= key)];
                }
                Page::Overflow { .. } => {
                    return Err(Error::InvalidPage(
                        "overflow page reached during tree descent".to_owned(),
                    ));
                }
            }
        }
    }

    fn collect_range_resident(
        &self,
        page_id: PageId,
        start: &[u8],
        end: &[u8],
        limit: usize,
        output: &mut Vec<(Vec<u8>, ValueCell)>,
        visited: &mut HashSet<PageId>,
    ) -> Result<Option<PageId>, Error> {
        if output.len() == limit {
            return Ok(None);
        }
        if !visited.insert(page_id) {
            return Err(Error::InvalidPage(
                "tree child graph contains a cycle or shared page".to_owned(),
            ));
        }
        let Some(page) = self.pages.get(&page_id) else {
            return Ok(Some(page_id));
        };
        match page {
            Page::Leaf { entries } => output.extend(
                entries
                    .iter()
                    .filter(|(key, _)| key.as_slice() >= start && key.as_slice() < end)
                    .take(limit - output.len())
                    .cloned(),
            ),
            Page::Internal { keys, children } => {
                for (index, child) in children.iter().copied().enumerate() {
                    let below_end = index == 0 || keys[index - 1].as_slice() < end;
                    let above_start = index == keys.len() || keys[index].as_slice() > start;
                    if below_end
                        && above_start
                        && let Some(missing) =
                            self.collect_range_resident(child, start, end, limit, output, visited)?
                    {
                        return Ok(Some(missing));
                    }
                    if output.len() == limit {
                        break;
                    }
                }
            }
            Page::Overflow { .. } => {
                return Err(Error::InvalidPage(
                    "overflow page reached during range traversal".to_owned(),
                ));
            }
        }
        Ok(None)
    }

    fn collect_range_reverse_resident(
        &self,
        page_id: PageId,
        start: &[u8],
        end: &[u8],
        limit: usize,
        output: &mut Vec<LeafEntry>,
        visited: &mut HashSet<PageId>,
    ) -> Result<Option<PageId>, Error> {
        if output.len() == limit {
            return Ok(None);
        }
        if !visited.insert(page_id) {
            return Err(Error::InvalidPage(
                "tree child graph contains a cycle or shared page".to_owned(),
            ));
        }
        let Some(page) = self.pages.get(&page_id) else {
            return Ok(Some(page_id));
        };
        match page {
            Page::Leaf { entries } => output.extend(
                entries
                    .iter()
                    .rev()
                    .filter(|(key, _)| key.as_slice() >= start && key.as_slice() < end)
                    .take(limit - output.len())
                    .cloned(),
            ),
            Page::Internal { keys, children } => {
                for index in (0..children.len()).rev() {
                    let below_end = index == 0 || keys[index - 1].as_slice() < end;
                    let above_start = index == keys.len() || keys[index].as_slice() > start;
                    if below_end
                        && above_start
                        && let Some(missing) = self.collect_range_reverse_resident(
                            children[index],
                            start,
                            end,
                            limit,
                            output,
                            visited,
                        )?
                    {
                        return Ok(Some(missing));
                    }
                    if output.len() == limit {
                        break;
                    }
                }
            }
            Page::Overflow { .. } => {
                return Err(Error::InvalidPage(
                    "overflow page reached during reverse range traversal".to_owned(),
                ));
            }
        }
        Ok(None)
    }

    fn allocate_page(&mut self, page: Page) -> Result<PageId, Error> {
        let page_id = self.metadata.next_page_id;
        if page_id >= MAX_JS_SAFE_INTEGER {
            return Err(Error::InvalidPage(
                "IDBTree page id space exceeds JavaScript's safe integer range".to_owned(),
            ));
        }
        if self.pages.contains_key(&page_id) || self.dirty.contains_key(&page_id) {
            return Err(Error::InvalidPage(
                "IDBTree next page id already exists".to_owned(),
            ));
        }
        if encode_page(&page).map_err(Error::InvalidPage)?.len() > self.options.page_size {
            return Err(Error::PageTooLarge {
                page_id,
                page_size: self.options.page_size,
            });
        }
        self.metadata.next_page_id = page_id + 1;
        self.pages.insert(page_id, page.clone());
        self.dirty.insert(page_id, page);
        Ok(page_id)
    }

    fn finish_leaf_write(
        &mut self,
        page_id: PageId,
        entries: Vec<(Vec<u8>, ValueCell)>,
        path: Vec<(PageId, usize)>,
    ) -> Result<(), Error> {
        // The old leaf is no longer reachable from the replacement root.
        // Checkpoint rollback restores this intent if allocation fails.
        self.retire_page(page_id);
        let page = Page::Leaf { entries };
        let replacement = if self.page_fits(&page)? {
            PageReplacement::One(self.allocate_page(page)?)
        } else {
            let Page::Leaf { mut entries } = page else {
                unreachable!()
            };
            if entries.len() < 2 {
                return Err(Error::PageTooLarge {
                    page_id,
                    page_size: self.options.page_size,
                });
            }
            let right_entries = entries.split_off(entries.len() / 2);
            let separator = right_entries[0].0.clone();
            PageReplacement::Split {
                left: self.allocate_page(Page::Leaf { entries })?,
                separator,
                right: self.allocate_page(Page::Leaf {
                    entries: right_entries,
                })?,
            }
        };
        self.publish_replacement(replacement, path)
    }

    fn retire_page(&mut self, page_id: PageId) {
        self.retired.insert(page_id);
    }

    fn retire_value(&mut self, value: &ValueCell) -> Result<(), Error> {
        let ValueCell::Overflow { head, .. } = value else {
            return Ok(());
        };
        let mut current = Some(*head);
        let mut visited = HashSet::new();
        while let Some(page_id) = current {
            if !visited.insert(page_id) {
                return Err(Error::InvalidPage(
                    "value overflow chain contains a cycle".to_owned(),
                ));
            }
            let Some(Page::Overflow { next, .. }) = self.pages.get(&page_id) else {
                return Err(Error::MissingPage(page_id));
            };
            current = *next;
            self.retire_page(page_id);
        }
        Ok(())
    }

    /// Rebuild every changed ancestor under fresh page ids. A committed root
    /// therefore names a complete immutable closure. Under the production
    /// single-owner epoch, each replaced ancestor is retired in the same
    /// commit as its replacement. Historical roots/readers are outside this
    /// scoped guarantee; a separate historical collector may be added later.
    fn publish_replacement(
        &mut self,
        mut replacement: PageReplacement,
        path: Vec<(PageId, usize)>,
    ) -> Result<(), Error> {
        for (parent_id, child_index) in path.into_iter().rev() {
            let Page::Internal {
                mut keys,
                mut children,
            } = self
                .pages
                .get(&parent_id)
                .cloned()
                .expect("descent path remains resident")
            else {
                return Err(Error::InvalidPage(
                    "descent parent is not internal".to_owned(),
                ));
            };
            self.retire_page(parent_id);
            replacement = match replacement {
                PageReplacement::One(page_id) => {
                    children[child_index] = page_id;
                    PageReplacement::One(self.allocate_page(Page::Internal { keys, children })?)
                }
                PageReplacement::Split {
                    left,
                    separator,
                    right,
                } => {
                    children[child_index] = left;
                    keys.insert(child_index, separator);
                    children.insert(child_index + 1, right);
                    let page = Page::Internal { keys, children };
                    if self.page_fits(&page)? {
                        PageReplacement::One(self.allocate_page(page)?)
                    } else {
                        let Page::Internal {
                            mut keys,
                            mut children,
                        } = page
                        else {
                            unreachable!()
                        };
                        let middle = keys.len() / 2;
                        let separator = keys.remove(middle);
                        let right_keys = keys.split_off(middle);
                        let right_children = children.split_off(middle + 1);
                        PageReplacement::Split {
                            left: self.allocate_page(Page::Internal { keys, children })?,
                            separator,
                            right: self.allocate_page(Page::Internal {
                                keys: right_keys,
                                children: right_children,
                            })?,
                        }
                    }
                }
            };
        }

        let root = match replacement {
            PageReplacement::One(root) => root,
            PageReplacement::Split {
                left,
                separator,
                right,
            } => self.allocate_page(Page::Internal {
                keys: vec![separator],
                children: vec![left, right],
            })?,
        };
        self.metadata.root_page_id = Some(root);
        // The replaced closure is retired in this commit under the
        // single-owner browser-worker epoch. Historical readers are outside
        // this guarantee; historical mark/sweep cleanup is separate work.
        Ok(())
    }

    fn page_fits(&self, page: &Page) -> Result<bool, Error> {
        Ok(encode_page(page).map_err(Error::InvalidPage)?.len() <= self.options.page_size)
    }

    fn build_value(&mut self, value: Vec<u8>) -> Result<ValueCell, Error> {
        // Keep leaves dense and make the worst-case inline insertion bounded.
        // The exact cutoff is intentionally a tree policy, not a page-store
        // concern, and can be tuned from receipts later.
        if value.len() <= self.options.page_size / 4 {
            return Ok(ValueCell::Inline(value));
        }

        let len = u64::try_from(value.len())
            .map_err(|_| Error::InvalidPage("overflow value length exceeds u64".to_owned()))?;
        let mut next = None;
        let chunk_size = self.options.page_size.saturating_sub(64);
        for chunk in value.rchunks(chunk_size) {
            let page = Page::Overflow {
                next,
                bytes: chunk.to_vec(),
            };
            if !self.page_fits(&page)? {
                return Err(Error::InvalidPage(
                    "overflow chunk does not fit configured page".to_owned(),
                ));
            }
            next = Some(self.allocate_page(page)?);
        }
        Ok(ValueCell::Overflow {
            head: next.expect("large values have at least one chunk"),
            len,
        })
    }

    fn value_resident(
        &self,
        value: &ValueCell,
        visited: &mut HashSet<PageId>,
    ) -> Result<Attempt<()>, Error> {
        let ValueCell::Overflow { head, .. } = value else {
            return Ok(Attempt::Ready(()));
        };
        let mut current = Some(*head);
        while let Some(page_id) = current {
            if !visited.insert(page_id) {
                return Err(Error::InvalidPage(
                    "tree graph contains a cycle or shared page".to_owned(),
                ));
            }
            let Some(page) = self.pages.get(&page_id) else {
                return Ok(Attempt::Missing(page_id));
            };
            let Page::Overflow { next, .. } = page else {
                return Err(Error::InvalidPage(
                    "value references a non-overflow page".to_owned(),
                ));
            };
            current = *next;
        }
        Ok(Attempt::Ready(()))
    }

    /// Validate all value edges retained by a copied leaf. `visited` already
    /// owns the root-to-leaf structural path, so this detects aliases both
    /// between sibling cells and between a value chain and that structure.
    fn leaf_values_resident(
        &self,
        entries: &[LeafEntry],
        visited: &mut HashSet<PageId>,
    ) -> Result<Attempt<()>, Error> {
        for (_, value) in entries {
            if let Attempt::Missing(page_id) = self.value_resident(value, visited)? {
                return Ok(Attempt::Missing(page_id));
            }
        }
        Ok(Attempt::Ready(()))
    }

    fn read_value_resident(
        &self,
        value: &ValueCell,
        visited: &mut HashSet<PageId>,
    ) -> Result<Attempt<Vec<u8>>, Error> {
        match value {
            ValueCell::Inline(value) => Ok(Attempt::Ready(value.clone())),
            ValueCell::Overflow { head, len } => {
                // The persisted logical length is u64 so page decoding stays
                // architecture-independent. Do not turn an untrusted durable
                // length into a host-sized allocation: grow only for actual
                // overflow page bytes as they are validated and materialized.
                let mut output = Vec::new();
                let mut current = Some(*head);
                while let Some(page_id) = current {
                    if !visited.insert(page_id) {
                        return Err(Error::InvalidPage(
                            "tree graph contains a cycle or shared page".to_owned(),
                        ));
                    }
                    let Some(page) = self.pages.get(&page_id) else {
                        return Ok(Attempt::Missing(page_id));
                    };
                    let Page::Overflow { next, bytes } = page else {
                        return Err(Error::InvalidPage(
                            "value references a non-overflow page".to_owned(),
                        ));
                    };
                    output.extend_from_slice(bytes);
                    current = *next;
                }
                if u64::try_from(output.len()).ok() != Some(*len) {
                    return Err(Error::InvalidPage(format!(
                        "overflow value length is {}, expected {len}",
                        output.len()
                    )));
                }
                Ok(Attempt::Ready(output))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::Poll;

    use futures::FutureExt;

    use super::*;

    // These are intentionally engine-level contract tests: page splitting,
    // reopen, and residency are not observably attributable through Jazz's
    // public query API, while every backend must preserve them.
    #[test]
    fn inserts_split_reopen_and_scan_in_key_order() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            let options = Options { page_size: 1024 };
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            for ordinal in (0..200).rev() {
                tree.put(
                    format!("key-{ordinal:04}").into_bytes(),
                    vec![ordinal as u8; 24],
                )
                .await
                .unwrap();
            }
            assert!(tree.metadata().root_page_id.unwrap() > 0);
            tree.flush().await.unwrap();
            drop(tree);

            let reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(reopened.get(b"key-0042").await.unwrap(), Some(vec![42; 24]));
            let rows = reopened.range(b"key-0030", b"key-0040").await.unwrap();
            assert_eq!(rows.len(), 10);
            assert_eq!(rows[0].0, b"key-0030");
            assert_eq!(rows[9].0, b"key-0039");
        });
    }

    #[test]
    fn delete_is_visible_and_durable() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            let options = Options::default();
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            tree.put(b"a".to_vec(), b"one".to_vec()).await.unwrap();
            assert!(tree.delete(b"a").await.unwrap());
            assert_eq!(tree.get(b"a").await.unwrap(), None);
            tree.flush().await.unwrap();

            let reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(reopened.get(b"a").await.unwrap(), None);
        });
    }

    #[test]
    fn overflow_values_survive_split_reopen_and_replacement() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            let options = Options { page_size: 1024 };
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            let first = vec![7; 10_000];
            tree.put(b"large".to_vec(), first.clone()).await.unwrap();
            for ordinal in 0..100 {
                tree.put(
                    format!("small-{ordinal:03}").into_bytes(),
                    vec![ordinal; 20],
                )
                .await
                .unwrap();
            }
            assert_eq!(tree.get(b"large").await.unwrap(), Some(first));
            tree.flush().await.unwrap();

            let reopened = IdbTree::open(store.clone(), options).await.unwrap();
            let second = vec![9; 7_000];
            reopened
                .put(b"large".to_vec(), second.clone())
                .await
                .unwrap();
            reopened.flush().await.unwrap();

            let reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(reopened.get(b"large").await.unwrap(), Some(second));
        });
    }

    #[test]
    fn overflow_replacement_reclaims_only_the_replaced_value_chain() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::reclaiming();
            let options = Options { page_size: 1024 };
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            tree.put(b"a".to_vec(), vec![1; 5_000]).await.unwrap();
            tree.put(b"z".to_vec(), vec![2; 5_000]).await.unwrap();
            tree.flush().await.unwrap();
            let old_a_chain = overflow_ids(&tree, b"a");
            let old_z_chain = overflow_ids(&tree, b"z");

            tree.put(b"a".to_vec(), vec![3; 4_000]).await.unwrap();
            let prepared = tree.inner.borrow_mut().prepare_commit().unwrap().unwrap();
            for page_id in &old_a_chain {
                assert!(prepared.commit().deleted_page_ids.contains(page_id));
            }
            for page_id in &old_z_chain {
                assert!(!prepared.commit().deleted_page_ids.contains(page_id));
            }
            let outcome = store.commit(prepared.commit()).await.unwrap();
            tree.inner
                .borrow_mut()
                .complete_commit(prepared, Ok(outcome))
                .unwrap();

            for page_id in old_a_chain {
                assert_eq!(store.read_page(page_id).await.unwrap(), None);
            }
            for page_id in old_z_chain {
                assert!(store.read_page(page_id).await.unwrap().is_some());
            }
            let reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(reopened.get(b"a").await.unwrap(), Some(vec![3; 4_000]));
            assert_eq!(reopened.get(b"z").await.unwrap(), Some(vec![2; 5_000]));
        });
    }

    #[test]
    fn point_updates_reclaim_the_old_closure_after_publication() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::reclaiming();
            let options = Options { page_size: 1024 };
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            tree.put(b"key".to_vec(), b"before".to_vec()).await.unwrap();
            tree.flush().await.unwrap();
            let old_root = tree.metadata().root_page_id.unwrap();

            tree.put(b"key".to_vec(), b"after".to_vec()).await.unwrap();
            let new_root = tree.metadata().root_page_id.unwrap();
            assert_ne!(new_root, old_root);
            let prepared = tree.inner.borrow_mut().prepare_commit().unwrap().unwrap();
            assert!(
                prepared
                    .commit()
                    .pages
                    .iter()
                    .any(|(id, _)| *id == new_root)
            );
            assert!(prepared.commit().deleted_page_ids.contains(&old_root));
            assert!(
                prepared
                    .commit()
                    .pages
                    .iter()
                    .all(|(id, _)| { !prepared.commit().deleted_page_ids.contains(id) })
            );

            // Until root publication, an independently opened reader still
            // sees the old immutable closure. Post-publication stale-reader
            // usability is intentionally outside the single-owner contract.
            let before_publish = IdbTree::open(store.clone(), options).await.unwrap();
            assert_eq!(
                before_publish.get(b"key").await.unwrap(),
                Some(b"before".to_vec())
            );
            let outcome = store.commit(prepared.commit()).await;
            tree.inner
                .borrow_mut()
                .complete_commit(prepared, outcome)
                .unwrap();
            let after_publish = IdbTree::open(store, options).await.unwrap();
            assert_eq!(
                after_publish.get(b"key").await.unwrap(),
                Some(b"after".to_vec())
            );
        });
    }

    #[test]
    fn non_reclaiming_store_retains_durable_old_closure() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            let options = Options { page_size: 1024 };
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            tree.put(b"key".to_vec(), b"before".to_vec()).await.unwrap();
            tree.flush().await.unwrap();
            let old_root = tree.metadata().root_page_id.unwrap();

            tree.put(b"key".to_vec(), b"after".to_vec()).await.unwrap();
            let prepared = tree.inner.borrow_mut().prepare_commit().unwrap().unwrap();
            assert!(prepared.commit().deleted_page_ids.is_empty());
            let outcome = store.commit(prepared.commit()).await.unwrap();
            tree.inner
                .borrow_mut()
                .complete_commit(prepared, Ok(outcome))
                .unwrap();

            assert!(store.read_page(old_root).await.unwrap().is_some());
            let reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(reopened.get(b"key").await.unwrap(), Some(b"after".to_vec()));
        });
    }

    #[test]
    fn capability_loss_defers_durable_retirement_until_reenabled() {
        futures::executor::block_on(async {
            let durable = MemoryPageStore::default();
            let store = DynamicCapabilityPageStore::new(durable.clone());
            let options = Options { page_size: 1024 };
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            tree.put(b"key".to_vec(), b"before".to_vec()).await.unwrap();
            tree.flush().await.unwrap();
            let old_root = tree.metadata().root_page_id.unwrap();

            tree.put(b"key".to_vec(), b"after".to_vec()).await.unwrap();
            store.set_reclamation_enabled(false);
            tree.flush().await.unwrap();
            assert!(durable.read_page(old_root).await.unwrap().is_some());
            assert_eq!(store.deleted_commits()[1], Vec::<PageId>::new());

            store.set_reclamation_enabled(true);
            tree.flush().await.unwrap();
            assert_eq!(store.deleted_commits()[2], vec![old_root]);
            assert_eq!(durable.read_page(old_root).await.unwrap(), None);
        });
    }

    #[test]
    fn fresh_retired_pages_are_not_written_or_deleted() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            let options = Options { page_size: 1024 };
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            tree.put(b"key".to_vec(), b"first".to_vec()).await.unwrap();
            tree.put(b"key".to_vec(), b"second".to_vec()).await.unwrap();
            let current_root = tree.metadata().root_page_id.unwrap();

            let prepared = tree.inner.borrow_mut().prepare_commit().unwrap().unwrap();
            assert_eq!(
                prepared
                    .commit()
                    .pages
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>(),
                vec![current_root]
            );
            assert!(prepared.commit().deleted_page_ids.is_empty());
            {
                let tree = tree.inner.borrow();
                for page_id in 0..current_root {
                    assert!(!tree.pages.contains_key(&page_id));
                }
            }
            let outcome = store.commit(prepared.commit()).await.unwrap();
            tree.inner
                .borrow_mut()
                .complete_commit(prepared, Ok(outcome))
                .unwrap();

            let reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(
                reopened.get(b"key").await.unwrap(),
                Some(b"second".to_vec())
            );
        });
    }

    #[test]
    fn write_many_reclaims_disjoint_replacements_atomically() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::reclaiming();
            let options = Options { page_size: 1024 };
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            for ordinal in 0..100 {
                tree.put(
                    format!("key-{ordinal:03}").into_bytes(),
                    vec![ordinal as u8; 20],
                )
                .await
                .unwrap();
            }
            tree.flush().await.unwrap();

            tree.write_many(vec![
                WriteOperation::Set {
                    key: b"key-001".to_vec(),
                    value: b"updated".to_vec(),
                },
                WriteOperation::Delete {
                    key: b"key-098".to_vec(),
                },
                WriteOperation::Set {
                    key: b"key-050".to_vec(),
                    value: b"also-updated".to_vec(),
                },
            ])
            .await
            .unwrap();
            let prepared = tree.inner.borrow_mut().prepare_commit().unwrap().unwrap();
            assert!(!prepared.commit().deleted_page_ids.is_empty());
            assert!(
                prepared
                    .commit()
                    .pages
                    .iter()
                    .all(|(id, _)| { !prepared.commit().deleted_page_ids.contains(id) })
            );
            let outcome = store.commit(prepared.commit()).await.unwrap();
            tree.inner
                .borrow_mut()
                .complete_commit(prepared, Ok(outcome))
                .unwrap();

            let reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(
                reopened.get(b"key-001").await.unwrap(),
                Some(b"updated".to_vec())
            );
            assert_eq!(reopened.get(b"key-098").await.unwrap(), None);
            assert_eq!(
                reopened.get(b"key-050").await.unwrap(),
                Some(b"also-updated".to_vec())
            );
            assert_eq!(reopened.get(b"key-049").await.unwrap(), Some(vec![49; 20]));
        });
    }

    #[test]
    fn failed_oversized_put_leaves_no_local_or_durable_orphans() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            let options = Options { page_size: 1024 };
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            tree.flush().await.unwrap();
            let before = tree.metadata();
            let dirty_before = tree.dirty_page_count();
            let key = vec![b'k'; 1100];

            assert!(matches!(
                tree.put(key.clone(), vec![7; 2000]).await,
                Err(Error::PageTooLarge { .. })
            ));
            assert_eq!(tree.metadata(), before);
            assert_eq!(tree.dirty_page_count(), dirty_before);

            tree.flush().await.unwrap();
            drop(tree);
            let reopened = IdbTree::open(store.clone(), options).await.unwrap();
            assert_eq!(reopened.metadata(), before);
            assert_eq!(reopened.get(&key).await.unwrap(), None);
            for page_id in before.next_page_id..before.next_page_id + 8 {
                assert_eq!(store.read_page(page_id).await.unwrap(), None);
            }
        });
    }

    #[test]
    fn failed_write_many_rolls_back_earlier_operations_and_allocations() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            let options = Options { page_size: 1024 };
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            tree.flush().await.unwrap();
            let before = tree.metadata();
            let oversized_key = vec![b'z'; 1100];

            assert!(matches!(
                tree.write_many(vec![
                    WriteOperation::Set {
                        key: b"first".to_vec(),
                        value: b"must roll back".to_vec(),
                    },
                    WriteOperation::Set {
                        key: oversized_key.clone(),
                        value: vec![9; 2000],
                    },
                ])
                .await,
                Err(Error::PageTooLarge { .. })
            ));
            assert_eq!(tree.metadata(), before);
            assert_eq!(tree.dirty_page_count(), 0);

            tree.flush().await.unwrap();
            drop(tree);
            let reopened = IdbTree::open(store.clone(), options).await.unwrap();
            assert_eq!(reopened.get(b"first").await.unwrap(), None);
            assert_eq!(reopened.get(&oversized_key).await.unwrap(), None);
            for page_id in before.next_page_id..before.next_page_id + 8 {
                assert_eq!(store.read_page(page_id).await.unwrap(), None);
            }
        });
    }

    #[test]
    fn published_root_with_a_missing_page_fails_closed_after_reopen() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            let metadata = Metadata {
                page_size: 1024,
                generation: 0,
                root_page_id: Some(41),
                next_page_id: 42,
            };
            store
                .commit(&Commit {
                    expected_generation: 0,
                    metadata,
                    pages: Vec::new(),
                    deleted_page_ids: Vec::new(),
                })
                .await
                .unwrap();
            let reopened = IdbTree::open(store, Options { page_size: 1024 })
                .await
                .unwrap();
            assert!(matches!(
                reopened.get(b"anything").await,
                Err(Error::MissingPage(41))
            ));
        });
    }

    #[test]
    fn corrupt_overflow_cycle_fails_instead_of_spinning() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            let metadata = Metadata {
                page_size: 1024,
                generation: 0,
                root_page_id: Some(1),
                next_page_id: 3,
            };
            store
                .commit(&Commit {
                    expected_generation: 0,
                    metadata,
                    pages: vec![
                        (
                            1,
                            encode_page(&Page::Leaf {
                                entries: vec![(
                                    b"cycle".to_vec(),
                                    ValueCell::Overflow { head: 2, len: 1 },
                                )],
                            })
                            .unwrap(),
                        ),
                        (
                            2,
                            encode_page(&Page::Overflow {
                                next: Some(2),
                                bytes: vec![1],
                            })
                            .unwrap(),
                        ),
                    ],
                    deleted_page_ids: Vec::new(),
                })
                .await
                .unwrap();
            let tree = IdbTree::open(store, Options { page_size: 1024 })
                .await
                .unwrap();
            assert!(matches!(
                tree.get(b"cycle").await,
                Err(Error::InvalidPage(message)) if message.contains("cycle")
            ));
        });
    }

    /// These pages have valid page framing and checksums. Their failure is a
    /// graph-integrity failure discovered only while the persisted closure is
    /// traversed, not a decoder shortcut for a malformed byte body.
    #[test]
    fn checksum_valid_leaf_cells_cannot_share_an_overflow_page() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            persist_malformed_pages(
                &store,
                Some(1),
                3,
                vec![
                    (
                        1,
                        Page::Leaf {
                            entries: vec![
                                (b"a".to_vec(), ValueCell::Overflow { head: 2, len: 1 }),
                                (b"b".to_vec(), ValueCell::Overflow { head: 2, len: 1 }),
                            ],
                        },
                    ),
                    (
                        2,
                        Page::Overflow {
                            next: None,
                            bytes: vec![7],
                        },
                    ),
                ],
            )
            .await;

            let tree = IdbTree::open(store, Options { page_size: 1024 })
                .await
                .unwrap();
            let root_before = tree.metadata().root_page_id;
            assert_shared_graph(tree.range(b"", b"z").await);
            // A point write would otherwise copy and republish this leaf;
            // prove the same operation-wide ownership check rejects it before
            // allocating a new root or making a dirty page.
            assert_shared_error(tree.put(b"c".to_vec(), b"new".to_vec()).await);
            assert_eq!(tree.metadata().root_page_id, root_before);
            assert_eq!(tree.dirty_page_count(), 0);
        });
    }

    #[test]
    fn checksum_valid_cross_branch_leaves_cannot_share_an_overflow_page() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            persist_malformed_pages(
                &store,
                Some(1),
                5,
                vec![
                    (
                        1,
                        Page::Internal {
                            keys: vec![b"m".to_vec()],
                            children: vec![2, 3],
                        },
                    ),
                    (
                        2,
                        Page::Leaf {
                            entries: vec![(b"a".to_vec(), ValueCell::Overflow { head: 4, len: 1 })],
                        },
                    ),
                    (
                        3,
                        Page::Leaf {
                            entries: vec![(b"n".to_vec(), ValueCell::Overflow { head: 4, len: 1 })],
                        },
                    ),
                    (
                        4,
                        Page::Overflow {
                            next: None,
                            bytes: vec![7],
                        },
                    ),
                ],
            )
            .await;

            let tree = IdbTree::open(store, Options { page_size: 1024 })
                .await
                .unwrap();
            assert_shared_graph(tree.range(b"", b"z").await);
        });
    }

    #[test]
    fn corrupt_internal_cycle_or_shared_child_fails_instead_of_looping_or_duplication() {
        futures::executor::block_on(async {
            let cycle_store = MemoryPageStore::default();
            cycle_store
                .commit(&Commit {
                    expected_generation: 0,
                    metadata: Metadata {
                        page_size: 1024,
                        generation: 0,
                        root_page_id: Some(1),
                        next_page_id: 2,
                    },
                    pages: vec![(
                        1,
                        encode_page(&Page::Internal {
                            keys: vec![],
                            children: vec![1],
                        })
                        .unwrap(),
                    )],
                    deleted_page_ids: Vec::new(),
                })
                .await
                .unwrap();
            let cycle = IdbTree::open(cycle_store, Options { page_size: 1024 })
                .await
                .unwrap();
            assert!(matches!(
                cycle.get(b"loop").await,
                Err(Error::InvalidPage(message)) if message.contains("cycle or shared")
            ));

            let shared_store = MemoryPageStore::default();
            shared_store
                .commit(&Commit {
                    expected_generation: 0,
                    metadata: Metadata {
                        page_size: 1024,
                        generation: 0,
                        root_page_id: Some(1),
                        next_page_id: 5,
                    },
                    pages: vec![
                        (
                            1,
                            encode_page(&Page::Internal {
                                keys: vec![b"middle".to_vec()],
                                children: vec![2, 3],
                            })
                            .unwrap(),
                        ),
                        (
                            2,
                            encode_page(&Page::Internal {
                                keys: vec![],
                                children: vec![4],
                            })
                            .unwrap(),
                        ),
                        (
                            3,
                            encode_page(&Page::Internal {
                                keys: vec![],
                                children: vec![4],
                            })
                            .unwrap(),
                        ),
                        (
                            4,
                            encode_page(&Page::Leaf {
                                entries: vec![(
                                    b"key".to_vec(),
                                    ValueCell::Inline(b"value".to_vec()),
                                )],
                            })
                            .unwrap(),
                        ),
                    ],
                    deleted_page_ids: Vec::new(),
                })
                .await
                .unwrap();
            let shared = IdbTree::open(shared_store, Options { page_size: 1024 })
                .await
                .unwrap();
            assert!(matches!(
                shared.range(b"", b"z").await,
                Err(Error::InvalidPage(message)) if message.contains("shared")
            ));
        });
    }

    async fn persist_malformed_pages(
        store: &MemoryPageStore,
        root_page_id: Option<PageId>,
        next_page_id: PageId,
        pages: Vec<(PageId, Page)>,
    ) {
        store
            .commit(&Commit {
                expected_generation: 0,
                metadata: Metadata {
                    page_size: 1024,
                    generation: 0,
                    root_page_id,
                    next_page_id,
                },
                pages: pages
                    .into_iter()
                    .map(|(page_id, page)| (page_id, encode_page(&page).unwrap()))
                    .collect(),
                deleted_page_ids: Vec::new(),
            })
            .await
            .unwrap();
    }

    fn assert_shared_graph(result: Result<Vec<KeyValue>, Error>) {
        assert_shared_error(result);
    }

    fn assert_shared_error<T>(result: Result<T, Error>) {
        assert!(matches!(
            result,
            Err(Error::InvalidPage(message)) if message.contains("cycle or shared")
        ));
    }

    #[test]
    fn oversized_persisted_page_is_rejected_before_decode() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            store
                .commit(&Commit {
                    expected_generation: 0,
                    metadata: Metadata {
                        page_size: 1024,
                        generation: 0,
                        root_page_id: Some(1),
                        next_page_id: 2,
                    },
                    pages: vec![(1, vec![0; 1025])],
                    deleted_page_ids: Vec::new(),
                })
                .await
                .unwrap();
            let tree = IdbTree::open(store, Options { page_size: 1024 })
                .await
                .unwrap();
            assert!(matches!(
                tree.get(b"anything").await,
                Err(Error::PageTooLarge {
                    page_id: 1,
                    page_size: 1024
                })
            ));
        });
    }

    #[test]
    fn resident_lookup_is_ready_on_first_poll_but_cold_lookup_yields() {
        futures::executor::block_on(async {
            let durable = MemoryPageStore::default();
            let options = Options::default();
            let tree = IdbTree::open(durable.clone(), options).await.unwrap();
            tree.put(b"resident".to_vec(), b"yes".to_vec())
                .await
                .unwrap();

            {
                let resident = tree.get(b"resident");
                futures::pin_mut!(resident);
                assert_eq!(
                    resident.now_or_never().unwrap().unwrap(),
                    Some(b"yes".to_vec())
                );
            }
            tree.flush().await.unwrap();
            drop(tree);

            let delayed = YieldingPageStore::new(durable);
            let reopened = IdbTree::open(delayed.clone(), options).await.unwrap();
            let mut cold = Box::pin(reopened.get(b"resident"));
            assert!(matches!(poll_once(cold.as_mut()), Poll::Pending));
            assert_eq!(cold.await.unwrap(), Some(b"yes".to_vec()));
            assert_eq!(delayed.page_reads.get(), 1);
        });
    }

    #[test]
    fn writes_continue_in_a_new_generation_while_commit_is_in_flight() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            let options = Options::default();
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            tree.put(b"before".to_vec(), b"one".to_vec()).await.unwrap();
            let prepared = tree.inner.borrow_mut().prepare_commit().unwrap().unwrap();

            tree.put(b"during".to_vec(), b"two".to_vec()).await.unwrap();
            assert!(tree.dirty_page_count() > 0);
            let outcome = store.commit(prepared.commit()).await;
            tree.inner
                .borrow_mut()
                .complete_commit(prepared, outcome)
                .unwrap();
            assert!(tree.dirty_page_count() > 0);
            tree.flush().await.unwrap();

            let reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(
                reopened.get(b"before").await.unwrap(),
                Some(b"one".to_vec())
            );
            assert_eq!(
                reopened.get(b"during").await.unwrap(),
                Some(b"two".to_vec())
            );
        });
    }

    #[test]
    fn failed_commit_restores_unchanged_pages_to_the_dirty_generation() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            let options = Options::default();
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            tree.put(b"retry".to_vec(), b"me".to_vec()).await.unwrap();
            let prepared = tree.inner.borrow_mut().prepare_commit().unwrap().unwrap();
            assert_eq!(tree.dirty_page_count(), 0);
            assert!(
                tree.inner
                    .borrow_mut()
                    .complete_commit(prepared, Err("injected failure".to_owned()))
                    .is_err()
            );
            assert!(tree.dirty_page_count() > 0);

            tree.flush().await.unwrap();
            let reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(reopened.get(b"retry").await.unwrap(), Some(b"me".to_vec()));
        });
    }

    #[test]
    fn failed_commit_with_a_resident_next_generation_restores_deletions_for_retry() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::reclaiming();
            let options = Options::default();
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            tree.put(b"base".to_vec(), b"old".to_vec()).await.unwrap();
            tree.flush().await.unwrap();
            let old_root = tree.metadata().root_page_id.unwrap();

            tree.put(b"base".to_vec(), b"new".to_vec()).await.unwrap();
            let first_commit = tree.inner.borrow_mut().prepare_commit().unwrap().unwrap();
            assert!(first_commit.commit().deleted_page_ids.contains(&old_root));
            tree.put(b"during".to_vec(), b"value".to_vec())
                .await
                .unwrap();

            assert!(
                tree.inner
                    .borrow_mut()
                    .complete_commit(first_commit, Err("injected failure".to_owned()))
                    .is_err()
            );
            let retry = tree.inner.borrow_mut().prepare_commit().unwrap().unwrap();
            assert!(retry.commit().deleted_page_ids.contains(&old_root));
            assert!(
                retry
                    .commit()
                    .pages
                    .iter()
                    .all(|(id, _)| { !retry.commit().deleted_page_ids.contains(id) })
            );
            let outcome = store.commit(retry.commit()).await.unwrap();
            tree.inner
                .borrow_mut()
                .complete_commit(retry, Ok(outcome))
                .unwrap();

            let reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(reopened.get(b"base").await.unwrap(), Some(b"new".to_vec()));
            assert_eq!(
                reopened.get(b"during").await.unwrap(),
                Some(b"value".to_vec())
            );
        });
    }

    #[test]
    fn delayed_public_get_retries_after_reclaiming_write() {
        futures::executor::block_on(async {
            let durable = MemoryPageStore::reclaiming();
            let options = Options { page_size: 1024 };
            let seed = IdbTree::open(durable.clone(), options).await.unwrap();
            seed.put(b"warm".to_vec(), vec![1; 16]).await.unwrap();
            seed.put(b"target".to_vec(), vec![2; 5_000]).await.unwrap();
            seed.flush().await.unwrap();
            drop(seed);

            let delayed = YieldingPageStore::new(durable);
            let tree = IdbTree::open(delayed, options).await.unwrap();
            // Hydrate only the structural page and inline value. The target's
            // overflow chain remains a cold, durable read.
            assert_eq!(tree.get(b"warm").await.unwrap(), Some(vec![1; 16]));
            let mut cold = Box::pin(tree.get(b"target"));
            assert!(matches!(poll_once(cold.as_mut()), Poll::Pending));

            tree.put(b"target".to_vec(), b"new".to_vec()).await.unwrap();
            tree.flush().await.unwrap();

            assert_eq!(cold.await.unwrap(), Some(b"new".to_vec()));
        });
    }

    #[test]
    fn cold_read_does_not_block_a_resident_write() {
        futures::executor::block_on(async {
            let durable = MemoryPageStore::default();
            let options = Options { page_size: 1024 };
            let seed = IdbTree::open(durable.clone(), options).await.unwrap();
            for ordinal in 0..200 {
                seed.put(
                    format!("key-{ordinal:04}").into_bytes(),
                    vec![ordinal as u8; 24],
                )
                .await
                .unwrap();
            }
            seed.flush().await.unwrap();
            drop(seed);

            let delayed = YieldingPageStore::new(durable);
            let tree = IdbTree::open(delayed, options).await.unwrap();
            assert_eq!(tree.get(b"key-0001").await.unwrap(), Some(vec![1; 24]));

            let mut cold = Box::pin(tree.get(b"key-0199"));
            assert!(matches!(poll_once(cold.as_mut()), Poll::Pending));
            {
                let resident_write = tree.put(b"key-0001".to_vec(), b"updated".to_vec());
                futures::pin_mut!(resident_write);
                assert!(resident_write.now_or_never().unwrap().is_ok());
            }
            assert_eq!(cold.await.unwrap(), Some(vec![199; 24]));
            assert_eq!(
                tree.get(b"key-0001").await.unwrap(),
                Some(b"updated".to_vec())
            );
        });
    }

    #[test]
    fn in_flight_flush_does_not_block_the_next_resident_write_generation() {
        futures::executor::block_on(async {
            let durable = MemoryPageStore::default();
            let delayed = YieldingPageStore::new(durable.clone());
            let tree = IdbTree::open(delayed, Options::default()).await.unwrap();
            tree.put(b"before".to_vec(), b"one".to_vec()).await.unwrap();

            let mut flush = Box::pin(tree.flush());
            assert!(matches!(poll_once(flush.as_mut()), Poll::Pending));
            {
                let resident_write = tree.put(b"during".to_vec(), b"two".to_vec());
                futures::pin_mut!(resident_write);
                assert!(resident_write.now_or_never().unwrap().is_ok());
            }
            flush.await.unwrap();
            assert!(tree.dirty_page_count() > 0);
            tree.flush().await.unwrap();

            let reopened = IdbTree::open(durable, Options::default()).await.unwrap();
            assert_eq!(
                reopened.get(b"before").await.unwrap(),
                Some(b"one".to_vec())
            );
            assert_eq!(
                reopened.get(b"during").await.unwrap(),
                Some(b"two".to_vec())
            );
        });
    }

    fn overflow_ids<S: PageStore>(tree: &IdbTree<S>, key: &[u8]) -> Vec<PageId> {
        let tree = tree.inner.borrow();
        let (_, entries, _, _) = tree.resident_descent(key).unwrap().unwrap();
        let (_, value) = entries
            .into_iter()
            .find(|(candidate, _)| candidate.as_slice() == key)
            .unwrap();
        let ValueCell::Overflow { head, .. } = value else {
            panic!("expected overflow value");
        };
        let mut ids = Vec::new();
        let mut current = Some(head);
        while let Some(page_id) = current {
            ids.push(page_id);
            let Page::Overflow { next, .. } = tree.pages.get(&page_id).unwrap() else {
                panic!("expected overflow page");
            };
            current = *next;
        }
        ids
    }

    #[derive(Clone)]
    struct DynamicCapabilityPageStore {
        inner: MemoryPageStore,
        reclamation_enabled: Rc<Cell<bool>>,
        deleted_commits: Rc<RefCell<Vec<Vec<PageId>>>>,
    }

    impl DynamicCapabilityPageStore {
        fn new(inner: MemoryPageStore) -> Self {
            Self {
                inner,
                reclamation_enabled: Rc::new(Cell::new(true)),
                deleted_commits: Rc::new(RefCell::new(Vec::new())),
            }
        }

        fn set_reclamation_enabled(&self, enabled: bool) {
            self.reclamation_enabled.set(enabled);
        }

        fn deleted_commits(&self) -> Vec<Vec<PageId>> {
            self.deleted_commits.borrow().clone()
        }
    }

    impl PageStore for DynamicCapabilityPageStore {
        fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
            self.inner.load_metadata()
        }

        fn read_page(&self, page_id: PageId) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
            self.inner.read_page(page_id)
        }

        fn reclamation_token(&self) -> Option<PageReclamationToken> {
            self.reclamation_enabled
                .get()
                .then(PageReclamationToken::new)
        }

        fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
            let deleted = commit.deleted_page_ids.clone();
            let deleted_commits = self.deleted_commits.clone();
            Box::pin(async move {
                deleted_commits.borrow_mut().push(deleted);
                self.inner.commit(commit).await
            })
        }
    }

    #[derive(Clone)]
    struct YieldingPageStore {
        inner: MemoryPageStore,
        page_reads: Rc<Cell<usize>>,
    }

    impl YieldingPageStore {
        fn new(inner: MemoryPageStore) -> Self {
            Self {
                inner,
                page_reads: Rc::new(Cell::new(0)),
            }
        }
    }

    impl PageStore for YieldingPageStore {
        fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
            self.inner.load_metadata()
        }

        fn read_page(&self, page_id: PageId) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
            self.page_reads.set(self.page_reads.get() + 1);
            Box::pin(async move {
                YieldOnce(false).await;
                self.inner.read_page(page_id).await
            })
        }
        fn reclamation_token(&self) -> Option<PageReclamationToken> {
            self.inner.reclamation_token()
        }

        fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
            Box::pin(async move {
                YieldOnce(false).await;
                self.inner.commit(commit).await
            })
        }
    }

    struct YieldOnce(bool);

    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    fn poll_once<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
        let waker = futures::task::noop_waker();
        let mut context = std::task::Context::from_waker(&waker);
        future.poll(&mut context)
    }
}
