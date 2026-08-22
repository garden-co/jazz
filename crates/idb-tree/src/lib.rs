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

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;

pub use store::{BoxFuture, Commit, MemoryPageStore, Metadata, PageStore};
#[cfg(target_arch = "wasm32")]
pub use web::IndexedDbPageStore;

use page::{Page, PageId, ValueCell, decode_page, encode_page};

const DEFAULT_PAGE_SIZE: usize = 16 * 1024;
const MIN_PAGE_SIZE: usize = 1024;

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
        if self.page_size < MIN_PAGE_SIZE || !self.page_size.is_power_of_two() {
            return Err(Error::InvalidOptions(format!(
                "page_size must be a power of two and at least {MIN_PAGE_SIZE}"
            )));
        }
        Ok(self)
    }
}

/// The tree core. Cached pages make resident reads complete on their first
/// poll; a cache miss awaits exactly the required page from the page store.
pub struct IdbTree<S> {
    store: S,
    options: Options,
    metadata: Metadata,
    pages: HashMap<PageId, Page>,
    dirty: BTreeMap<PageId, Page>,
    deleted: Vec<PageId>,
    commit_in_flight: bool,
}

#[derive(Debug)]
pub struct PreparedCommit {
    commit: Commit,
}

impl PreparedCommit {
    pub fn commit(&self) -> &Commit {
        &self.commit
    }
}

impl<S: PageStore> IdbTree<S> {
    pub async fn open(store: S, options: Options) -> Result<Self, Error> {
        let options = options.validate()?;
        let metadata = store.load_metadata().await.map_err(Error::Store)?;
        let mut tree = Self {
            store,
            options,
            metadata: metadata.unwrap_or_else(|| Metadata::empty(options.page_size)),
            pages: HashMap::new(),
            dirty: BTreeMap::new(),
            deleted: Vec::new(),
            commit_in_flight: false,
        };
        if tree.metadata.page_size != options.page_size {
            return Err(Error::InvalidOptions(format!(
                "store uses {}-byte pages, requested {}",
                tree.metadata.page_size, options.page_size
            )));
        }
        if tree.metadata.root_page_id.is_none() {
            let root = tree.allocate_page(Page::leaf());
            tree.metadata.root_page_id = Some(root);
        }
        Ok(tree)
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn is_resident(&self, page_id: PageId) -> bool {
        self.pages.contains_key(&page_id)
    }

    pub fn dirty_page_count(&self) -> usize {
        self.dirty.len()
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let mut page_id = self.root_page_id();
        loop {
            match self.load_page(page_id).await?.clone() {
                Page::Leaf { entries } => {
                    let value = entries
                        .binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
                        .ok()
                        .map(|index| entries[index].1.clone());
                    return match value {
                        Some(value) => self.read_value(value).await.map(Some),
                        None => Ok(None),
                    };
                }
                Page::Internal { keys, children } => {
                    let child = keys.partition_point(|separator| separator.as_slice() <= key);
                    page_id = children[child];
                }
                Page::Overflow { .. } => {
                    return Err(Error::InvalidPage(
                        "overflow page reached during tree descent".to_owned(),
                    ));
                }
            }
        }
    }

    pub async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        let mut page_id = self.root_page_id();
        let mut path = Vec::new();
        loop {
            match self.load_page(page_id).await?.clone() {
                Page::Leaf { mut entries } => {
                    let value = self.build_value(value)?;
                    match entries.binary_search_by(|(candidate, _)| candidate.cmp(&key)) {
                        Ok(index) => {
                            self.retire_value(&entries[index].1).await?;
                            entries[index].1 = value;
                        }
                        Err(index) => entries.insert(index, (key, value)),
                    }
                    self.finish_leaf_write(page_id, entries, path)?;
                    return Ok(());
                }
                Page::Internal { keys, children } => {
                    let child_index = keys.partition_point(|separator| separator <= &key);
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

    pub async fn delete(&mut self, key: &[u8]) -> Result<bool, Error> {
        let mut page_id = self.root_page_id();
        loop {
            match self.load_page(page_id).await?.clone() {
                Page::Leaf { mut entries } => {
                    let Ok(index) =
                        entries.binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
                    else {
                        return Ok(false);
                    };
                    let (_, value) = entries.remove(index);
                    self.retire_value(&value).await?;
                    self.replace_page(page_id, Page::Leaf { entries });
                    // Sparse leaves remain valid search targets. Compaction is
                    // deliberately separate from correctness and can retire
                    // underfull pages without complicating foreground deletes.
                    return Ok(true);
                }
                Page::Internal { keys, children } => {
                    let child = keys.partition_point(|separator| separator.as_slice() <= key);
                    page_id = children[child];
                }
                Page::Overflow { .. } => {
                    return Err(Error::InvalidPage(
                        "overflow page reached during tree descent".to_owned(),
                    ));
                }
            }
        }
    }

    pub async fn range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let mut cells = Vec::new();
        self.collect_range(self.root_page_id(), start, end, &mut cells)
            .await?;
        let mut output = Vec::with_capacity(cells.len());
        for (key, value) in cells {
            output.push((key, self.read_value(value).await?));
        }
        Ok(output)
    }

    /// Persist the current dirty generation atomically. A later concurrent
    /// wrapper may swap generations before awaiting this call; the core keeps
    /// this explicit boundary small and deterministic.
    pub async fn flush(&mut self) -> Result<(), Error> {
        let Some(prepared) = self.prepare_commit()? else {
            return Ok(());
        };
        let outcome = self.store.commit(prepared.commit()).await;
        self.complete_commit(prepared, outcome)
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
        for (&page_id, page) in &self.dirty {
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
            deleted_page_ids: std::mem::take(&mut self.deleted),
        };
        self.dirty.clear();
        Ok(Some(PreparedCommit { commit }))
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
                self.metadata.generation = committed.generation;
                Ok(())
            }
            Err(error) => {
                for (page_id, _) in prepared.commit.pages {
                    if self.dirty.contains_key(&page_id) {
                        continue;
                    }
                    if let Some(page) = self.pages.get(&page_id) {
                        self.dirty.insert(page_id, page.clone());
                    }
                }
                for page_id in prepared.commit.deleted_page_ids {
                    if !self.pages.contains_key(&page_id) && !self.deleted.contains(&page_id) {
                        self.deleted.push(page_id);
                    }
                }
                Err(Error::Store(error))
            }
        }
    }

    async fn load_page(&mut self, page_id: PageId) -> Result<&Page, Error> {
        if !self.pages.contains_key(&page_id) {
            let bytes = self
                .store
                .read_page(page_id)
                .await
                .map_err(Error::Store)?
                .ok_or(Error::MissingPage(page_id))?;
            let page = decode_page(&bytes).map_err(Error::InvalidPage)?;
            self.pages.insert(page_id, page);
        }
        Ok(self
            .pages
            .get(&page_id)
            .expect("page was just made resident"))
    }

    fn root_page_id(&self) -> PageId {
        self.metadata
            .root_page_id
            .expect("open always installs a root page")
    }

    fn allocate_page(&mut self, page: Page) -> PageId {
        let page_id = self.metadata.next_page_id;
        self.metadata.next_page_id += 1;
        self.pages.insert(page_id, page.clone());
        self.dirty.insert(page_id, page);
        page_id
    }

    fn finish_leaf_write(
        &mut self,
        page_id: PageId,
        entries: Vec<(Vec<u8>, ValueCell)>,
        mut path: Vec<(PageId, usize)>,
    ) -> Result<(), Error> {
        let page = Page::Leaf { entries };
        if self.page_fits(&page)? {
            self.replace_page(page_id, page);
            return Ok(());
        }

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
        self.replace_page(page_id, Page::Leaf { entries });
        let right_page_id = self.allocate_page(Page::Leaf {
            entries: right_entries,
        });
        self.propagate_split(page_id, separator, right_page_id, &mut path)
    }

    fn propagate_split(
        &mut self,
        left_page_id: PageId,
        mut separator: Vec<u8>,
        mut right_page_id: PageId,
        path: &mut Vec<(PageId, usize)>,
    ) -> Result<(), Error> {
        let mut left_page_id = left_page_id;
        while let Some((parent_id, child_index)) = path.pop() {
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
            debug_assert_eq!(children[child_index], left_page_id);
            keys.insert(child_index, separator);
            children.insert(child_index + 1, right_page_id);
            let page = Page::Internal { keys, children };
            if self.page_fits(&page)? {
                self.replace_page(parent_id, page);
                return Ok(());
            }

            let Page::Internal {
                mut keys,
                mut children,
            } = page
            else {
                unreachable!()
            };
            let middle = keys.len() / 2;
            separator = keys.remove(middle);
            let right_keys = keys.split_off(middle);
            let right_children = children.split_off(middle + 1);
            self.replace_page(parent_id, Page::Internal { keys, children });
            right_page_id = self.allocate_page(Page::Internal {
                keys: right_keys,
                children: right_children,
            });
            left_page_id = parent_id;
        }

        let root = self.allocate_page(Page::Internal {
            keys: vec![separator],
            children: vec![left_page_id, right_page_id],
        });
        self.metadata.root_page_id = Some(root);
        Ok(())
    }

    fn collect_range<'a>(
        &'a mut self,
        page_id: PageId,
        start: &'a [u8],
        end: &'a [u8],
        output: &'a mut Vec<(Vec<u8>, ValueCell)>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'a>> {
        Box::pin(async move {
            match self.load_page(page_id).await?.clone() {
                Page::Leaf { entries } => {
                    output.extend(
                        entries
                            .into_iter()
                            .filter(|(key, _)| key.as_slice() >= start && key.as_slice() < end),
                    );
                }
                Page::Internal { keys, children } => {
                    for (index, child) in children.into_iter().enumerate() {
                        let below_end = index == 0 || keys[index - 1].as_slice() < end;
                        let above_start = index == keys.len() || keys[index].as_slice() > start;
                        if below_end && above_start {
                            self.collect_range(child, start, end, output).await?;
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
        })
    }

    fn replace_page(&mut self, page_id: PageId, page: Page) {
        self.pages.insert(page_id, page.clone());
        self.dirty.insert(page_id, page);
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

        let len = value.len();
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
            next = Some(self.allocate_page(page));
        }
        Ok(ValueCell::Overflow {
            head: next.expect("large values have at least one chunk"),
            len,
        })
    }

    async fn read_value(&mut self, value: ValueCell) -> Result<Vec<u8>, Error> {
        match value {
            ValueCell::Inline(value) => Ok(value),
            ValueCell::Overflow { head, len } => {
                let mut output = Vec::with_capacity(len);
                let mut current = Some(head);
                while let Some(page_id) = current {
                    let Page::Overflow { next, bytes } = self.load_page(page_id).await?.clone()
                    else {
                        return Err(Error::InvalidPage(
                            "value references a non-overflow page".to_owned(),
                        ));
                    };
                    output.extend_from_slice(&bytes);
                    current = next;
                }
                if output.len() != len {
                    return Err(Error::InvalidPage(format!(
                        "overflow value length is {}, expected {len}",
                        output.len()
                    )));
                }
                Ok(output)
            }
        }
    }

    async fn retire_value(&mut self, value: &ValueCell) -> Result<(), Error> {
        let ValueCell::Overflow { head, .. } = value else {
            return Ok(());
        };
        let mut current = Some(*head);
        while let Some(page_id) = current {
            let Page::Overflow { next, .. } = self.load_page(page_id).await?.clone() else {
                return Err(Error::InvalidPage(
                    "value references a non-overflow page".to_owned(),
                ));
            };
            self.pages.remove(&page_id);
            self.dirty.remove(&page_id);
            self.deleted.push(page_id);
            current = next;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
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
            let mut tree = IdbTree::open(store.clone(), options).await.unwrap();
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

            let mut reopened = IdbTree::open(store, options).await.unwrap();
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
            let mut tree = IdbTree::open(store.clone(), options).await.unwrap();
            tree.put(b"a".to_vec(), b"one".to_vec()).await.unwrap();
            assert!(tree.delete(b"a").await.unwrap());
            assert_eq!(tree.get(b"a").await.unwrap(), None);
            tree.flush().await.unwrap();

            let mut reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(reopened.get(b"a").await.unwrap(), None);
        });
    }

    #[test]
    fn overflow_values_survive_split_reopen_and_replacement() {
        futures::executor::block_on(async {
            let store = MemoryPageStore::default();
            let options = Options { page_size: 1024 };
            let mut tree = IdbTree::open(store.clone(), options).await.unwrap();
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

            let mut reopened = IdbTree::open(store.clone(), options).await.unwrap();
            let second = vec![9; 7_000];
            reopened
                .put(b"large".to_vec(), second.clone())
                .await
                .unwrap();
            reopened.flush().await.unwrap();

            let mut reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(reopened.get(b"large").await.unwrap(), Some(second));
        });
    }

    #[test]
    fn resident_lookup_is_ready_on_first_poll_but_cold_lookup_yields() {
        futures::executor::block_on(async {
            let durable = MemoryPageStore::default();
            let options = Options::default();
            let mut tree = IdbTree::open(durable.clone(), options).await.unwrap();
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
            let mut reopened = IdbTree::open(delayed.clone(), options).await.unwrap();
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
            let mut tree = IdbTree::open(store.clone(), options).await.unwrap();
            tree.put(b"before".to_vec(), b"one".to_vec()).await.unwrap();
            let prepared = tree.prepare_commit().unwrap().unwrap();

            tree.put(b"during".to_vec(), b"two".to_vec()).await.unwrap();
            assert!(tree.dirty_page_count() > 0);
            let outcome = store.commit(prepared.commit()).await;
            tree.complete_commit(prepared, outcome).unwrap();
            assert!(tree.dirty_page_count() > 0);
            tree.flush().await.unwrap();

            let mut reopened = IdbTree::open(store, options).await.unwrap();
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
            let mut tree = IdbTree::open(store.clone(), options).await.unwrap();
            tree.put(b"retry".to_vec(), b"me".to_vec()).await.unwrap();
            let prepared = tree.prepare_commit().unwrap().unwrap();
            assert_eq!(tree.dirty_page_count(), 0);
            assert!(
                tree.complete_commit(prepared, Err("injected failure".to_owned()))
                    .is_err()
            );
            assert!(tree.dirty_page_count() > 0);

            tree.flush().await.unwrap();
            let mut reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(reopened.get(b"retry").await.unwrap(), Some(b"me".to_vec()));
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
            self.inner.commit(commit)
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
