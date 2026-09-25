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

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::rc::Rc;

pub use store::{BoxFuture, Commit, MemoryPageStore, Metadata, PageStore, TreeOwnership};
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
type Descent<'a> = (
    PageId,
    &'a [LeafEntry],
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

/// Choose the most byte-balanced boundary for which both encoded pages fit.
/// Leaf boundaries retain every entry; internal boundaries promote one key
/// out of the two children. A count midpoint is unsafe for variable-size cells.
fn byte_balanced_split(
    sizes: impl ExactSizeIterator<Item = usize> + Clone,
    base_len: usize,
    page_size: usize,
    promote_separator: bool,
) -> Option<usize> {
    let count = sizes.len();
    let mut left = base_len;
    let mut right = base_len + sizes.clone().sum::<usize>();
    let mut best = None;
    for (index, size) in sizes.enumerate() {
        right -= size;
        if !promote_separator {
            left += size;
        }
        if (promote_separator || index + 1 < count) && left <= page_size && right <= page_size {
            let boundary = if promote_separator { index } else { index + 1 };
            // Preserve the count midpoint on equal-byte choices, particularly
            // when wide separators permit only one key per internal page.
            let imbalance = (left.abs_diff(right), boundary.abs_diff(count / 2));
            if best.is_none_or(|(_, previous)| imbalance < previous) {
                best = Some((boundary, imbalance));
            }
        }
        if promote_separator {
            left += size;
        }
    }
    best.map(|(index, _)| index)
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
    #[error("IDBTree ownership has expired")]
    OwnershipExpired,
    #[error("an IDBTree commit is already in flight")]
    CommitInFlight,
    /// A flush was cancelled mid-commit. Every operation except reads through
    /// a read-committed view fails until [`IdbTree::reload`] resolves the
    /// commit's outcome from the store.
    #[error("an IDBTree commit was abandoned; reload before using the tree")]
    CommitAbandoned,
    /// A read-committed view needed a page that is not resident. It never
    /// hydrates: the caller retries through the ordinary tree instead.
    #[error("IDBTree page {0} is not resident for a read-committed view")]
    NotResident(PageId),
    #[error("a read-committed IDBTree view cannot write")]
    ReadOnlyView,
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
    /// Root of the last durable generation: the store's root at open, then
    /// each successfully committed root. Its closure stays resident until a
    /// later commit retires it, so read-committed views can serve it while
    /// newer writes are staged or committing.
    durable_root: Option<PageId>,
    pages: HashMap<PageId, Page>,
    /// Pages written in the active generation; their images live in `pages`.
    dirty: BTreeSet<PageId>,
    /// Pages allocated at or after this id belong to the write currently
    /// under a checkpoint. Nothing else can reference them yet, and rollback
    /// discards them wholesale, so that write may change them in place
    /// instead of copying them again. `PageId::MAX` outside a write.
    write_floor: PageId,
    deleted: BTreeSet<PageId>,
    retirement_undo: Vec<PageId>,
    commit_in_flight: bool,
    /// A flush was dropped while its commit was in flight, so whether that
    /// commit landed is unknown, and the live root may name pages that never
    /// became durable. Only [`IdbTree::reload`] recovers: the store is the
    /// sole authority on the outcome.
    commit_abandoned: bool,
}

struct AbandonOnDrop<'a, S>(Option<&'a RefCell<TreeCore<S>>>);

impl<S> Drop for AbandonOnDrop<'_, S> {
    fn drop(&mut self) {
        if let Some(inner) = self.0.take() {
            let mut tree = inner.borrow_mut();
            tree.commit_in_flight = false;
            tree.commit_abandoned = true;
        }
    }
}

/// A write only appends fresh COW page ids and advances root/allocation
/// metadata. Remember that small frontier instead of cloning the resident page
/// cache for every operation.
struct WriteCheckpoint {
    metadata: Metadata,
}

#[derive(Debug)]
pub struct PreparedCommit {
    commit: Commit,
    retired: BTreeSet<PageId>,
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

/// A range scan reports every missing page it can prove it needs, so one
/// batched read replaces a restart from the root per cold page.
enum ScanAttempt<T> {
    Ready(T),
    Missing(Vec<PageId>),
}

/// Cloneable, single-threaded handle used by Groove. No `RefCell` borrow is
/// held across page I/O: operations attempt synchronously against resident
/// pages, hydrate one precise miss, then retry.
#[derive(Clone)]
pub struct IdbTree<S> {
    inner: Rc<RefCell<TreeCore<S>>>,
    _ownership: Rc<TreeOwnership>,
    reload_epoch: Rc<Cell<u64>>,
    /// Reads see only the last committed generation, never staged or
    /// committing writes, and never hydrate. See [`IdbTree::read_committed`].
    read_committed: bool,
}

impl<S: PageStore + Clone> IdbTree<S> {
    pub async fn open(store: S, options: Options) -> Result<Self, Error> {
        let ownership = store.claim_tree_ownership().map_err(Error::Store)?;
        let tree = TreeCore::open(store, options).await?;
        if !ownership.is_live() {
            return Err(Error::OwnershipExpired);
        }
        Ok(Self {
            inner: Rc::new(RefCell::new(tree)),
            _ownership: Rc::new(ownership),
            reload_epoch: Rc::new(Cell::new(0)),
            read_committed: false,
        })
    }

    /// A read-only view of the same tree that answers from the last committed
    /// generation only. Writes staged since, including a commit still in
    /// flight, are invisible to it. It never performs page I/O: a read that
    /// needs a non-resident page fails with [`Error::NotResident`] on its
    /// first poll, and every write method fails with [`Error::ReadOnlyView`].
    pub fn read_committed(&self) -> Self {
        Self {
            read_committed: true,
            ..self.clone()
        }
    }

    fn ensure_writable(&self) -> Result<(), Error> {
        if self.read_committed {
            Err(Error::ReadOnlyView)
        } else {
            Ok(())
        }
    }

    /// The root a read starts from, or None for a view of an empty store.
    fn read_root(&self, tree: &TreeCore<S>) -> Option<PageId> {
        if self.read_committed {
            tree.durable_root
        } else {
            Some(tree.root_page_id())
        }
    }

    /// Hydrate a missing page, or fail a read-committed view without I/O.
    async fn hydrate_for_read(&self, page_id: PageId) -> Result<(), Error> {
        if self.read_committed {
            return Err(Error::NotResident(page_id));
        }
        self.hydrate(page_id).await
    }

    /// Hydrate a batch of missing pages, or fail a read-committed view without
    /// I/O.
    async fn hydrate_many_for_read(&self, page_ids: Vec<PageId>) -> Result<(), Error> {
        if self.read_committed {
            return Err(Error::NotResident(page_ids[0]));
        }
        self.hydrate_many(page_ids).await
    }

    fn ensure_live(&self) -> Result<(), Error> {
        if !self._ownership.is_live() {
            return Err(Error::OwnershipExpired);
        }
        // The durable root is still a committed generation after an abandoned
        // commit, so the read-committed view keeps serving it.
        if !self.read_committed && self.inner.borrow().commit_abandoned {
            return Err(Error::CommitAbandoned);
        }
        Ok(())
    }

    /// Discard staged writes and reload the durable root while retaining this
    /// handle's ownership. Callers must serialize this with writes/flushes.
    pub async fn reload(&self) -> Result<(), Error> {
        if !self._ownership.is_live() {
            return Err(Error::OwnershipExpired);
        }
        self.ensure_writable()?;
        let (store, options) = {
            let tree = self.inner.borrow();
            if tree.commit_in_flight {
                return Err(Error::CommitInFlight);
            }
            (tree.store.clone(), tree.options)
        };
        let fresh = TreeCore::open(store, options).await?;
        if !self._ownership.is_live() {
            return Err(Error::OwnershipExpired);
        }
        *self.inner.borrow_mut() = fresh;
        self.reload_epoch
            .set(self.reload_epoch.get().wrapping_add(1));
        Ok(())
    }

    pub fn metadata(&self) -> Metadata {
        self.inner.borrow().metadata.clone()
    }

    pub fn dirty_page_count(&self) -> usize {
        self.inner.borrow().dirty.len()
    }

    /// Compare an existing value in place; None denotes an absent key.
    pub async fn value_equals(&self, key: &[u8], expected: &[u8]) -> Result<Option<bool>, Error> {
        loop {
            self.ensure_live()?;
            let attempt = {
                let tree = self.inner.borrow();
                match self.read_root(&tree) {
                    Some(root) => tree.try_value_equals(root, key, expected)?,
                    None => Attempt::Ready(None),
                }
            };
            match attempt {
                Attempt::Ready(value) => return Ok(value),
                Attempt::Missing(page_id) => self.hydrate_for_read(page_id).await?,
            }
        }
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        loop {
            self.ensure_live()?;
            let attempt = {
                let tree = self.inner.borrow();
                match self.read_root(&tree) {
                    Some(root) => tree.try_get(root, key)?,
                    None => Attempt::Ready(None),
                }
            };
            match attempt {
                Attempt::Ready(value) => return Ok(value),
                Attempt::Missing(page_id) => self.hydrate_for_read(page_id).await?,
            }
        }
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        self.ensure_writable()?;
        loop {
            self.ensure_live()?;
            let attempt = self
                .inner
                .borrow_mut()
                .with_write_attempt_checkpoint(|tree| tree.try_put(&key, &value))?;
            match attempt {
                Attempt::Ready(()) => return Ok(()),
                Attempt::Missing(page_id) => self.hydrate(page_id).await?,
            }
        }
    }

    pub async fn delete(&self, key: &[u8]) -> Result<bool, Error> {
        self.ensure_writable()?;
        loop {
            self.ensure_live()?;
            let attempt = self
                .inner
                .borrow_mut()
                .with_write_attempt_checkpoint(|tree| tree.try_delete(key))?;
            match attempt {
                Attempt::Ready(deleted) => return Ok(deleted),
                Attempt::Missing(page_id) => self.hydrate(page_id).await?,
            }
        }
    }

    pub async fn write_many(&self, operations: Vec<WriteOperation>) -> Result<(), Error> {
        self.ensure_live()?;
        self.ensure_writable()?;
        for operation in &operations {
            let key = match operation {
                WriteOperation::Set { key, .. } | WriteOperation::Delete { key } => key,
            };
            loop {
                self.ensure_live()?;
                let attempt = self.inner.borrow().write_path_resident(key)?;
                match attempt {
                    Attempt::Ready(()) => break,
                    Attempt::Missing(page_id) => self.hydrate(page_id).await?,
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
            self.ensure_live()?;
            let attempt = {
                let tree = self.inner.borrow();
                match self.read_root(&tree) {
                    Some(root) => tree.try_range(root, start, end, limit)?,
                    None => ScanAttempt::Ready(Vec::new()),
                }
            };
            match attempt {
                ScanAttempt::Ready(rows) => return Ok(rows),
                ScanAttempt::Missing(page_ids) => self.hydrate_many_for_read(page_ids).await?,
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
            self.ensure_live()?;
            let attempt = {
                let tree = self.inner.borrow();
                match self.read_root(&tree) {
                    Some(root) => tree.try_range_reverse(root, start, end, limit)?,
                    None => ScanAttempt::Ready(Vec::new()),
                }
            };
            match attempt {
                ScanAttempt::Ready(rows) => return Ok(rows),
                ScanAttempt::Missing(page_ids) => self.hydrate_many_for_read(page_ids).await?,
            }
        }
    }

    pub async fn flush(&self) -> Result<(), Error> {
        self.ensure_live()?;
        self.ensure_writable()?;
        let (store, prepared) = {
            let mut tree = self.inner.borrow_mut();
            (tree.store.clone(), tree.prepare_commit()?)
        };
        let Some(prepared) = prepared else {
            return Ok(());
        };
        // If this future is dropped while the store commit is pending, nothing
        // would ever complete it: the tree would refuse every later commit and
        // keep serving the uncommitted root. Mark the commit abandoned instead.
        let mut abandon = AbandonOnDrop(Some(&self.inner));
        let outcome = store.commit(prepared.commit()).await;
        abandon.0 = None;
        self.inner.borrow_mut().complete_commit(prepared, outcome)
    }

    /// Hydrate a scan's whole missing frontier with one store read. The same
    /// publication/reset fence as [`hydrate`](Self::hydrate) applies to the
    /// batch as a unit: a stale completion imports none of its pages.
    async fn hydrate_many(&self, page_ids: Vec<PageId>) -> Result<(), Error> {
        let (store, page_ids) = {
            let tree = self.inner.borrow();
            let page_ids: Vec<PageId> = page_ids
                .into_iter()
                .filter(|page_id| !tree.pages.contains_key(page_id))
                .collect();
            (tree.store.clone(), page_ids)
        };
        match page_ids.as_slice() {
            [] => return Ok(()),
            [page_id] => return self.hydrate(*page_id).await,
            _ => {}
        }
        let root_before = self.inner.borrow().metadata.root_page_id;
        let reload_before = self.reload_epoch.get();
        let result = store.read_pages(&page_ids).await;
        self.ensure_live()?;
        if self.reload_epoch.get() != reload_before
            || self.inner.borrow().metadata.root_page_id != root_before
        {
            return Ok(());
        }
        let pages = result.map_err(Error::Store)?;
        if pages.len() != page_ids.len() {
            return Err(Error::Store(format!(
                "read {} pages for {} requested ids",
                pages.len(),
                page_ids.len()
            )));
        }
        let page_size = self.inner.borrow().options.page_size;
        let mut decoded = Vec::with_capacity(pages.len());
        for (page_id, bytes) in page_ids.into_iter().zip(pages) {
            let bytes = bytes.ok_or(Error::MissingPage(page_id))?;
            if bytes.len() > page_size {
                return Err(Error::PageTooLarge { page_id, page_size });
            }
            decoded.push((page_id, decode_page(&bytes).map_err(Error::InvalidPage)?));
        }
        let mut tree = self.inner.borrow_mut();
        for (page_id, page) in decoded {
            tree.pages.entry(page_id).or_insert(page);
        }
        Ok(())
    }

    async fn hydrate(&self, page_id: PageId) -> Result<(), Error> {
        let store = {
            let tree = self.inner.borrow();
            if tree.pages.contains_key(&page_id) {
                return Ok(());
            }
            tree.store.clone()
        };
        let root_before = self.inner.borrow().metadata.root_page_id;
        let reload_before = self.reload_epoch.get();
        let result = store.read_page(page_id).await;
        self.ensure_live()?;
        // The operation retries from the current root, so never import an old
        // completion (including an error) across publication or failure reset.
        // Reset can reuse fresh page IDs, making a cache insert unsafe even if
        // the old read returned successfully.
        if self.reload_epoch.get() != reload_before
            || self.inner.borrow().metadata.root_page_id != root_before
        {
            return Ok(());
        }
        let bytes = result
            .map_err(Error::Store)?
            .ok_or(Error::MissingPage(page_id))?;
        let page_size = self.inner.borrow().options.page_size;
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

    fn write_checkpoint(&mut self) -> WriteCheckpoint {
        debug_assert_eq!(
            self.write_floor,
            PageId::MAX,
            "write checkpoints do not nest"
        );
        self.write_floor = self.metadata.next_page_id;
        WriteCheckpoint {
            metadata: self.metadata.clone(),
        }
    }

    fn is_fresh(&self, page_id: PageId) -> bool {
        page_id >= self.write_floor
    }

    fn with_write_checkpoint<T>(
        &mut self,
        write: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let checkpoint = self.write_checkpoint();
        let outcome = write(self);
        self.write_floor = PageId::MAX;
        match outcome {
            Ok(value) => {
                self.retirement_undo.clear();
                Ok(value)
            }
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
        let outcome = write(self);
        self.write_floor = PageId::MAX;
        match outcome {
            Ok(Attempt::Ready(value)) => {
                self.retirement_undo.clear();
                Ok(Attempt::Ready(value))
            }
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
        for page_id in self.retirement_undo.drain(..) {
            self.deleted.remove(&page_id);
        }
        self.metadata = checkpoint.metadata;
    }

    pub async fn open(store: S, options: Options) -> Result<Self, Error> {
        let options = options.validate()?;
        let metadata = store.load_metadata().await.map_err(Error::Store)?;
        let metadata = metadata.unwrap_or_else(|| Metadata::empty(options.page_size));
        let mut tree = Self {
            store,
            options,
            durable_root: metadata.root_page_id,
            metadata,
            pages: HashMap::new(),
            dirty: BTreeSet::new(),
            write_floor: PageId::MAX,
            deleted: BTreeSet::new(),
            retirement_undo: Vec::new(),
            commit_in_flight: false,
            commit_abandoned: false,
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

    fn try_value_equals(
        &self,
        root: PageId,
        key: &[u8],
        expected: &[u8],
    ) -> Result<Attempt<Option<bool>>, Error> {
        let Some((_, entries, _, mut visited)) = self.resident_descent_from(root, key)? else {
            return Ok(Attempt::Missing(self.missing_page_for_key_from(root, key)?));
        };
        let Ok(index) = entries.binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
        else {
            return Ok(Attempt::Ready(None));
        };
        match &entries[index].1 {
            ValueCell::Inline(value) => Ok(Attempt::Ready(Some(value.as_slice() == expected))),
            ValueCell::Overflow { head, len } => {
                if u64::try_from(expected.len()).ok() != Some(*len) {
                    return Ok(Attempt::Ready(Some(false)));
                }
                let mut current = Some(*head);
                let mut offset = 0usize;
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
                    let end = offset.checked_add(bytes.len()).ok_or_else(|| {
                        Error::InvalidPage("overflow value length overflow".to_owned())
                    })?;
                    if expected.get(offset..end) != Some(bytes.as_slice()) {
                        return Ok(Attempt::Ready(Some(false)));
                    }
                    offset = end;
                    current = *next;
                }
                Ok(Attempt::Ready(Some(offset == expected.len())))
            }
        }
    }

    fn try_get(&self, root: PageId, key: &[u8]) -> Result<Attempt<Option<Vec<u8>>>, Error> {
        let Some((_, entries, _, mut visited)) = self.resident_descent_from(root, key)? else {
            return Ok(Attempt::Missing(self.missing_page_for_key_from(root, key)?));
        };
        let value = entries
            .binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
            .ok()
            .map(|index| &entries[index].1);
        match value {
            Some(value) => {
                self.read_value_resident(value, &mut visited)
                    .map(|attempt| match attempt {
                        Attempt::Ready(value) => Attempt::Ready(Some(value)),
                        Attempt::Missing(page_id) => Attempt::Missing(page_id),
                    })
            }
            None => Ok(Attempt::Ready(None)),
        }
    }

    fn try_put(&mut self, key: &[u8], value: &[u8]) -> Result<Attempt<()>, Error> {
        let Some((page_id, entries, path, mut visited)) = self.resident_descent(key)? else {
            return Ok(Attempt::Missing(self.missing_page_for_key(key)?));
        };
        if self.is_fresh(page_id) {
            // This write already copied (and validated) or built every value
            // in this leaf, so change it in place rather than copy it again.
            let new_value = self.build_value(value.to_vec())?;
            let mut entries = self.take_fresh_leaf(page_id);
            match entries.binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key)) {
                Ok(index) => {
                    let old = std::mem::replace(&mut entries[index].1, new_value);
                    self.retire_value(&old);
                }
                Err(index) => entries.insert(index, (key.to_vec(), new_value)),
            }
            self.finish_fresh_leaf_write(page_id, entries, path)?;
            return Ok(Attempt::Ready(()));
        }
        // A write copies this complete leaf into a new immutable page. Verify
        // every retained value edge under the same ownership set before doing
        // so; otherwise a point update could silently perpetuate a malformed
        // sibling overflow graph.
        if let Attempt::Missing(page_id) = self.leaf_values_resident(entries, &mut visited)? {
            return Ok(Attempt::Missing(page_id));
        }
        let mut entries = entries.to_vec();
        let new_value = self.build_value(value.to_vec())?;
        match entries.binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key)) {
            Ok(index) => {
                self.retire_value(&entries[index].1);
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
        if self.is_fresh(page_id) {
            let mut entries = self.take_fresh_leaf(page_id);
            let (_, old) = entries.remove(index);
            self.retire_value(&old);
            self.finish_fresh_leaf_write(page_id, entries, path)?;
            return Ok(Attempt::Ready(true));
        }
        // Deletion also republishes all surviving cells in this leaf.
        if let Attempt::Missing(page_id) = self.leaf_values_resident(entries, &mut visited)? {
            return Ok(Attempt::Missing(page_id));
        }
        let mut entries = entries.to_vec();
        self.retire_value(&entries[index].1);
        entries.remove(index);
        self.finish_leaf_write(page_id, entries, path)?;
        Ok(Attempt::Ready(true))
    }

    fn write_path_resident(&self, key: &[u8]) -> Result<Attempt<()>, Error> {
        let Some((_, entries, _, mut visited)) = self.resident_descent(key)? else {
            return Ok(Attempt::Missing(self.missing_page_for_key(key)?));
        };
        self.leaf_values_resident(entries, &mut visited)
    }

    fn try_range(
        &self,
        root: PageId,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<ScanAttempt<Vec<KeyValue>>, Error> {
        let mut cells = Vec::new();
        let mut missing = Vec::new();
        let mut visited = HashSet::new();
        self.collect_range_resident(
            root,
            start,
            end,
            limit,
            &mut cells,
            &mut missing,
            &mut visited,
        )?;
        if !missing.is_empty() {
            return Ok(ScanAttempt::Missing(missing));
        }
        self.materialize_range_values(cells, visited)
    }

    fn try_range_reverse(
        &self,
        root: PageId,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<ScanAttempt<Vec<KeyValue>>, Error> {
        let mut cells = Vec::new();
        let mut missing = Vec::new();
        let mut visited = HashSet::new();
        self.collect_range_reverse_resident(
            root,
            start,
            end,
            limit,
            &mut cells,
            &mut missing,
            &mut visited,
        )?;
        if !missing.is_empty() {
            return Ok(ScanAttempt::Missing(missing));
        }
        self.materialize_range_values(cells, visited)
    }

    /// Swap the active dirty generation without awaiting persistence. New
    /// writes can immediately begin populating a fresh generation while the
    /// returned immutable page images are committed by the caller.
    pub fn prepare_commit(&mut self) -> Result<Option<PreparedCommit>, Error> {
        if self.commit_in_flight {
            return Err(Error::CommitInFlight);
        }
        if self.dirty.is_empty() && self.deleted.is_empty() {
            return Ok(None);
        }
        let mut pages = Vec::with_capacity(self.dirty.len());
        for &page_id in &self.dirty {
            if self.deleted.contains(&page_id) {
                continue;
            }
            let page = self
                .pages
                .get(&page_id)
                .expect("dirty pages remain resident");
            let bytes = encode_page(page).map_err(Error::InvalidPage)?;
            if bytes.len() > self.options.page_size {
                return Err(Error::PageTooLarge {
                    page_id,
                    page_size: self.options.page_size,
                });
            }
            pages.push((page_id, bytes));
        }
        self.commit_in_flight = true;
        let commit = Commit {
            expected_generation: self.metadata.generation,
            metadata: self.metadata.clone(),
            pages,
            deleted_page_ids: if self.store.can_reclaim_obsolete_pages() {
                self.deleted
                    .iter()
                    .filter(|id| !self.dirty.contains(id))
                    .copied()
                    .collect()
            } else {
                Vec::new()
            },
        };
        self.dirty.clear();
        Ok(Some(PreparedCommit {
            commit,
            retired: std::mem::take(&mut self.deleted),
        }))
    }

    /// Reconcile an atomic commit result with writes made after
    /// [`prepare_commit`](Self::prepare_commit). On failure, pages unchanged
    /// since the swap are marked dirty again; newer dirty versions win.
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
        match outcome {
            Ok(committed) => {
                if committed.generation != prepared.commit.expected_generation + 1 {
                    return Err(Error::Store(format!(
                        "commit returned generation {}, expected {}",
                        committed.generation,
                        prepared.commit.expected_generation + 1
                    )));
                }
                // Root and allocation metadata may already describe writes in
                // the next dirty generation. Only advance its durable base.
                for page_id in prepared.retired {
                    self.pages.remove(&page_id);
                }
                self.durable_root = prepared.commit.metadata.root_page_id;
                self.metadata.generation = committed.generation;
                Ok(())
            }
            Err(error) => {
                for (page_id, _) in prepared.commit.pages {
                    if self.pages.contains_key(&page_id) {
                        self.dirty.insert(page_id);
                    }
                }
                self.deleted.extend(prepared.retired);
                if error.contains("generation changed") {
                    Err(Error::GenerationConflict(error))
                } else {
                    Err(Error::Store(error))
                }
            }
        }
    }

    fn root_page_id(&self) -> PageId {
        self.metadata
            .root_page_id
            .expect("open always installs a root page")
    }

    fn resident_descent(&self, key: &[u8]) -> Result<Option<Descent<'_>>, Error> {
        self.resident_descent_from(self.root_page_id(), key)
    }

    fn resident_descent_from(
        &self,
        root: PageId,
        key: &[u8],
    ) -> Result<Option<Descent<'_>>, Error> {
        let mut page_id = root;
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
                    return Ok(Some((page_id, entries, path, visited)));
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
        self.missing_page_for_key_from(self.root_page_id(), key)
    }

    fn missing_page_for_key_from(&self, root: PageId, key: &[u8]) -> Result<PageId, Error> {
        let mut page_id = root;
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

    /// Resolve the scanned cells' values. Every overflow chain which reaches a
    /// cold page contributes that page to one batched miss rather than
    /// restarting the scan once per chain.
    fn materialize_range_values(
        &self,
        cells: Vec<LeafEntry>,
        mut visited: HashSet<PageId>,
    ) -> Result<ScanAttempt<Vec<KeyValue>>, Error> {
        let mut rows = Vec::with_capacity(cells.len());
        let mut missing = Vec::new();
        for (key, value) in cells {
            match self.read_value_resident(&value, &mut visited)? {
                Attempt::Ready(value) if missing.is_empty() => rows.push((key, value)),
                Attempt::Ready(_) => {}
                Attempt::Missing(page_id) => missing.push(page_id),
            }
        }
        if missing.is_empty() {
            Ok(ScanAttempt::Ready(rows))
        } else {
            Ok(ScanAttempt::Missing(missing))
        }
    }

    /// Whether a scan should stop descending after `missing` became non-empty.
    /// An unbounded scan needs every page in range, so it keeps walking the
    /// resident structure to gather the whole missing frontier. A bounded scan
    /// cannot know how many rows a cold subtree holds, so it stops at the
    /// first miss rather than hydrate pages past its limit.
    fn scan_stops_at_miss(limit: usize, missing: &[PageId]) -> bool {
        limit != usize::MAX && !missing.is_empty()
    }

    // Once a page is known missing the retry recollects every row, so leaves
    // visited afterwards are only walked for further misses, not cloned.
    #[allow(clippy::too_many_arguments)]
    fn collect_range_resident(
        &self,
        page_id: PageId,
        start: &[u8],
        end: &[u8],
        limit: usize,
        output: &mut Vec<LeafEntry>,
        missing: &mut Vec<PageId>,
        visited: &mut HashSet<PageId>,
    ) -> Result<(), Error> {
        if output.len() == limit || Self::scan_stops_at_miss(limit, missing) {
            return Ok(());
        }
        if !visited.insert(page_id) {
            return Err(Error::InvalidPage(
                "tree child graph contains a cycle or shared page".to_owned(),
            ));
        }
        let Some(page) = self.pages.get(&page_id) else {
            missing.push(page_id);
            return Ok(());
        };
        match page {
            Page::Leaf { .. } if !missing.is_empty() => {}
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
                    if below_end && above_start {
                        self.collect_range_resident(
                            child, start, end, limit, output, missing, visited,
                        )?;
                    }
                    if output.len() == limit || Self::scan_stops_at_miss(limit, missing) {
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
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_range_reverse_resident(
        &self,
        page_id: PageId,
        start: &[u8],
        end: &[u8],
        limit: usize,
        output: &mut Vec<LeafEntry>,
        missing: &mut Vec<PageId>,
        visited: &mut HashSet<PageId>,
    ) -> Result<(), Error> {
        if output.len() == limit || Self::scan_stops_at_miss(limit, missing) {
            return Ok(());
        }
        if !visited.insert(page_id) {
            return Err(Error::InvalidPage(
                "tree child graph contains a cycle or shared page".to_owned(),
            ));
        }
        let Some(page) = self.pages.get(&page_id) else {
            missing.push(page_id);
            return Ok(());
        };
        match page {
            Page::Leaf { .. } if !missing.is_empty() => {}
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
                    if below_end && above_start {
                        self.collect_range_reverse_resident(
                            children[index],
                            start,
                            end,
                            limit,
                            output,
                            missing,
                            visited,
                        )?;
                    }
                    if output.len() == limit || Self::scan_stops_at_miss(limit, missing) {
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
        Ok(())
    }

    fn allocate_page(&mut self, page: Page) -> Result<PageId, Error> {
        let page_id = self.metadata.next_page_id;
        if page_id >= MAX_JS_SAFE_INTEGER {
            return Err(Error::InvalidPage(
                "IDBTree page id space exceeds JavaScript's safe integer range".to_owned(),
            ));
        }
        if self.pages.contains_key(&page_id) || self.dirty.contains(&page_id) {
            return Err(Error::InvalidPage(
                "IDBTree next page id already exists".to_owned(),
            ));
        }
        if !self.page_fits(&page) {
            return Err(Error::PageTooLarge {
                page_id,
                page_size: self.options.page_size,
            });
        }
        self.metadata.next_page_id = page_id + 1;
        self.pages.insert(page_id, page);
        self.dirty.insert(page_id);
        Ok(page_id)
    }

    fn take_fresh_leaf(&mut self, page_id: PageId) -> Vec<LeafEntry> {
        debug_assert!(self.is_fresh(page_id));
        match self.pages.get_mut(&page_id) {
            Some(Page::Leaf { entries }) => std::mem::take(entries),
            _ => unreachable!("descent ended at a resident leaf"),
        }
    }

    /// Finish a write to a leaf this write already owns. It keeps its id when
    /// it fits (so no ancestor changes), and keeps the left half when it splits.
    fn finish_fresh_leaf_write(
        &mut self,
        page_id: PageId,
        mut entries: Vec<LeafEntry>,
        path: Vec<(PageId, usize)>,
    ) -> Result<(), Error> {
        let fits = page::LEAF_BASE_LEN
            + entries
                .iter()
                .map(|(key, value)| page::leaf_entry_len(key, value))
                .sum::<usize>()
            <= self.options.page_size;
        if fits {
            self.pages.insert(page_id, Page::Leaf { entries });
            return Ok(());
        }
        let right_entries = self.split_leaf_entries(page_id, &mut entries)?;
        let separator = right_entries[0].0.clone();
        let right = self.allocate_page(Page::Leaf {
            entries: right_entries,
        })?;
        self.pages.insert(page_id, Page::Leaf { entries });
        self.publish_replacement(
            PageReplacement::Split {
                left: page_id,
                separator,
                right,
            },
            path,
        )
    }

    fn split_leaf_entries(
        &self,
        page_id: PageId,
        entries: &mut Vec<LeafEntry>,
    ) -> Result<Vec<LeafEntry>, Error> {
        if entries.len() < 2 {
            return Err(Error::PageTooLarge {
                page_id,
                page_size: self.options.page_size,
            });
        }
        let split = byte_balanced_split(
            entries
                .iter()
                .map(|(key, value)| page::leaf_entry_len(key, value)),
            page::LEAF_BASE_LEN,
            self.options.page_size,
            false,
        )
        .ok_or(Error::PageTooLarge {
            page_id,
            page_size: self.options.page_size,
        })?;
        Ok(entries.split_off(split))
    }

    fn finish_leaf_write(
        &mut self,
        page_id: PageId,
        entries: Vec<(Vec<u8>, ValueCell)>,
        path: Vec<(PageId, usize)>,
    ) -> Result<(), Error> {
        let page = Page::Leaf { entries };
        let replacement = if self.page_fits(&page) {
            PageReplacement::One(self.allocate_page(page)?)
        } else {
            let Page::Leaf { mut entries } = page else {
                unreachable!()
            };
            let right_entries = self.split_leaf_entries(page_id, &mut entries)?;
            let separator = right_entries[0].0.clone();
            PageReplacement::Split {
                left: self.allocate_page(Page::Leaf { entries })?,
                separator,
                right: self.allocate_page(Page::Leaf {
                    entries: right_entries,
                })?,
            }
        };
        self.retire_page(page_id);
        self.publish_replacement(replacement, path)
    }

    /// Rebuild every changed ancestor under fresh page ids. A committed root
    /// therefore names a complete immutable closure. Retire the replaced path
    /// atomically with publication, only when the store proves exclusive ownership.
    fn publish_replacement(
        &mut self,
        mut replacement: PageReplacement,
        path: Vec<(PageId, usize)>,
    ) -> Result<(), Error> {
        for (parent_id, child_index) in path.into_iter().rev() {
            let fresh = self.is_fresh(parent_id);
            let parent = if fresh {
                // Owned by this write: edit it and keep its id.
                self.pages
                    .get_mut(&parent_id)
                    .map(|page| std::mem::replace(page, Page::leaf()))
            } else {
                self.pages.get(&parent_id).cloned()
            };
            let Page::Internal {
                mut keys,
                mut children,
            } = parent.expect("descent path remains resident")
            else {
                return Err(Error::InvalidPage(
                    "descent parent is not internal".to_owned(),
                ));
            };
            if fresh {
                match replacement {
                    PageReplacement::One(page_id) => {
                        children[child_index] = page_id;
                        self.pages
                            .insert(parent_id, Page::Internal { keys, children });
                        // Every ancestor of a fresh page is fresh and already
                        // points at it, so the root is unchanged.
                        return Ok(());
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
                        if self.page_fits(&page) {
                            self.pages.insert(parent_id, page);
                            return Ok(());
                        }
                        let (page, separator, right) = self.split_internal(parent_id, page)?;
                        self.pages.insert(parent_id, page);
                        replacement = PageReplacement::Split {
                            left: parent_id,
                            separator,
                            right: self.allocate_page(right)?,
                        };
                        continue;
                    }
                }
            }
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
                    if self.page_fits(&page) {
                        PageReplacement::One(self.allocate_page(page)?)
                    } else {
                        let (left, separator, right) = self.split_internal(parent_id, page)?;
                        PageReplacement::Split {
                            left: self.allocate_page(left)?,
                            separator,
                            right: self.allocate_page(right)?,
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

        Ok(())
    }

    // Track only new retirements in the current attempt. A failed operation
    // removes these IDs; successful writes retain the set and discard the log.
    // Cloning the whole growing set at every checkpoint made staged writes O(n²).
    fn retire_page(&mut self, page_id: PageId) {
        if self.deleted.insert(page_id) {
            self.retirement_undo.push(page_id);
        }
    }

    // The complete leaf ownership graph has already been validated and hydrated.
    // Only the replaced value owns this chain; surviving values retain theirs.
    fn retire_value(&mut self, value: &ValueCell) {
        let ValueCell::Overflow { head, .. } = value else {
            return;
        };
        let mut current = Some(*head);
        while let Some(id) = current {
            let Page::Overflow { next, .. } =
                self.pages.get(&id).expect("validated overflow is resident")
            else {
                unreachable!()
            };
            current = *next;
            self.retire_page(id);
        }
    }

    fn page_fits(&self, page: &Page) -> bool {
        page::encoded_len(page) <= self.options.page_size
    }

    /// Split an over-full internal page, promoting one separator key.
    fn split_internal(&self, page_id: PageId, page: Page) -> Result<(Page, Vec<u8>, Page), Error> {
        let Page::Internal {
            mut keys,
            mut children,
        } = page
        else {
            unreachable!("only internal pages are split here")
        };
        let middle = byte_balanced_split(
            keys.iter().map(|key| page::internal_key_len(key)),
            page::INTERNAL_BASE_LEN,
            self.options.page_size,
            true,
        )
        .ok_or(Error::PageTooLarge {
            page_id,
            page_size: self.options.page_size,
        })?;
        let separator = keys.remove(middle);
        let right_keys = keys.split_off(middle);
        let right_children = children.split_off(middle + 1);
        Ok((
            Page::Internal { keys, children },
            separator,
            Page::Internal {
                keys: right_keys,
                children: right_children,
            },
        ))
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
            if !self.page_fits(&page) {
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
    use std::cell::Cell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::Poll;

    use futures::FutureExt;
    use std::collections::BTreeMap;

    use super::*;

    /// A tree's logical page structure with page ids erased, so trees built
    /// by different write paths can be compared page for page.
    #[derive(Debug, PartialEq)]
    enum Shape {
        Leaf(Vec<(Vec<u8>, ShapeValue)>),
        Internal(Vec<Vec<u8>>, Vec<Shape>),
    }

    #[derive(Debug, PartialEq)]
    enum ShapeValue {
        Inline(Vec<u8>),
        Overflow { len: u64, chunks: Vec<Vec<u8>> },
    }

    fn stored_page(pages: &BTreeMap<PageId, Vec<u8>>, page_id: PageId) -> Page {
        decode_page(pages.get(&page_id).expect("reachable page is stored")).unwrap()
    }

    fn shape_of(
        pages: &BTreeMap<PageId, Vec<u8>>,
        page_id: PageId,
        reachable: &mut BTreeSet<PageId>,
    ) -> Shape {
        assert!(reachable.insert(page_id), "page {page_id} reached twice");
        match stored_page(pages, page_id) {
            Page::Leaf { entries } => Shape::Leaf(
                entries
                    .into_iter()
                    .map(|(key, value)| {
                        let value = match value {
                            ValueCell::Inline(bytes) => ShapeValue::Inline(bytes),
                            ValueCell::Overflow { head, len } => {
                                let mut chunks = Vec::new();
                                let mut next = Some(head);
                                while let Some(page_id) = next {
                                    assert!(reachable.insert(page_id));
                                    let Page::Overflow { next: after, bytes } =
                                        stored_page(pages, page_id)
                                    else {
                                        panic!("overflow chain reaches a non-overflow page");
                                    };
                                    chunks.push(bytes);
                                    next = after;
                                }
                                ShapeValue::Overflow { len, chunks }
                            }
                        };
                        (key, value)
                    })
                    .collect(),
            ),
            Page::Internal { keys, children } => Shape::Internal(
                keys,
                children
                    .into_iter()
                    .map(|child| shape_of(pages, child, reachable))
                    .collect(),
            ),
            Page::Overflow { .. } => panic!("tree descends into an overflow page"),
        }
    }

    /// A sole-owner store that deletes the pages each commit retires.
    #[derive(Clone, Default)]
    struct ReclaimingStore(MemoryPageStore);

    impl PageStore for ReclaimingStore {
        fn can_reclaim_obsolete_pages(&self) -> bool {
            true
        }

        fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
            self.0.load_metadata()
        }

        fn read_page(&self, page_id: PageId) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
            self.0.read_page(page_id)
        }

        fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
            self.0.commit(commit)
        }
    }

    /// The durable tree's shape, after checking that the store holds exactly
    /// the pages reachable from the durable root: nothing leaked, nothing lost.
    fn durable_shape(store: &ReclaimingStore) -> Option<Shape> {
        let (root, pages) = store.0.stored();
        let mut reachable = BTreeSet::new();
        let shape = root.map(|root| shape_of(&pages, root, &mut reachable));
        let stored: BTreeSet<PageId> = pages.keys().copied().collect();
        assert_eq!(
            stored, reachable,
            "stored pages differ from reachable pages"
        );
        shape
    }

    struct Xorshift(u64);

    impl Xorshift {
        fn below(&mut self, bound: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % bound
        }
    }

    fn random_operation(rng: &mut Xorshift) -> WriteOperation {
        let mut key = rng.below(800).to_be_bytes().to_vec();
        if rng.below(10) == 0 {
            key.resize(120, b'k');
        }
        if rng.below(4) == 0 {
            return WriteOperation::Delete { key };
        }
        let len = match rng.below(12) {
            0 => 700 + rng.below(3_000) as usize,
            1..=3 => 100 + rng.below(150) as usize,
            _ => rng.below(24) as usize,
        };
        let byte = rng.below(256) as u8;
        WriteOperation::Set {
            key,
            value: vec![byte; len],
        }
    }

    // A batch must build page for page the tree that the same writes build one
    // at a time, and every commit must leave the store holding exactly the
    // reachable pages. Keys and values alone cannot catch a batch that splits
    // at a different point, or one that strands a replaced overflow chain.
    #[test]
    fn a_batch_builds_the_same_pages_as_single_writes_and_leaks_none() {
        futures::executor::block_on(async {
            let options = Options { page_size: 1024 };
            for seed in 1..=6u64 {
                let batched_store = ReclaimingStore::default();
                let single_store = ReclaimingStore::default();
                let batched = IdbTree::open(batched_store.clone(), options).await.unwrap();
                let single = IdbTree::open(single_store.clone(), options).await.unwrap();
                let mut rng = Xorshift(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
                for round in 0..6 {
                    let batch: Vec<_> = (0..40 + 120 * (round % 3))
                        .map(|_| random_operation(&mut rng))
                        .collect();
                    for operation in &batch {
                        match operation {
                            WriteOperation::Set { key, value } => {
                                single.put(key.clone(), value.clone()).await.unwrap()
                            }
                            WriteOperation::Delete { key } => {
                                single.delete(key).await.unwrap();
                            }
                        }
                    }
                    batched.write_many(batch).await.unwrap();
                    batched.flush().await.unwrap();
                    single.flush().await.unwrap();
                    let single_shape = durable_shape(&single_store);
                    assert_eq!(
                        durable_shape(&batched_store),
                        single_shape,
                        "seed {seed} round {round}"
                    );
                }
            }
        });
    }

    // These are intentionally engine-level contract tests: page splitting,
    // reopen, and residency are not observably attributable through Jazz's
    // public query API, while every backend must preserve them.
    #[test]
    fn exact_value_comparison_handles_inline_overflow_and_cold_reopen() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            let options = Options { page_size: 1024 };
            let tree = IdbTree::open(store.clone(), options.clone()).await.unwrap();
            tree.put(b"small".to_vec(), b"hello".to_vec())
                .await
                .unwrap();
            let large = vec![7; 10000];
            tree.put(b"large".to_vec(), large.clone()).await.unwrap();
            tree.flush().await.unwrap();
            drop(tree);
            let tree = IdbTree::open(store, options).await.unwrap();
            assert_eq!(tree.value_equals(b"missing", b"hello").await.unwrap(), None);
            assert_eq!(
                tree.value_equals(b"small", b"hello").await.unwrap(),
                Some(true)
            );
            assert_eq!(
                tree.value_equals(b"small", b"world").await.unwrap(),
                Some(false)
            );
            assert_eq!(
                tree.value_equals(b"large", &large).await.unwrap(),
                Some(true)
            );
            let mut changed = large.clone();
            changed[9999] = 8;
            assert_eq!(
                tree.value_equals(b"large", &changed).await.unwrap(),
                Some(false)
            );
            assert_eq!(
                tree.value_equals(b"large", &large[..9999]).await.unwrap(),
                Some(false)
            );
            assert_eq!(tree.get(b"large").await.unwrap(), Some(large));
        });
    }

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
    fn point_updates_publish_a_fresh_root_without_deleting_the_old_closure() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
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
            assert!(prepared.commit().deleted_page_ids.is_empty());

            // Until root publication, a separate opener still sees the old
            // immutable closure.
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
