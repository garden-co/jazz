use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

pub type PageId = u64;
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub page_size: usize,
    pub generation: u64,
    pub root_page_id: Option<PageId>,
    pub next_page_id: PageId,
}

impl Metadata {
    pub fn empty(page_size: usize) -> Self {
        Self {
            page_size,
            generation: 0,
            root_page_id: None,
            next_page_id: 0,
        }
    }
}

#[derive(Debug)]
pub struct Commit {
    pub expected_generation: u64,
    pub metadata: Metadata,
    pub pages: Vec<(PageId, Vec<u8>)>,
    pub deleted_page_ids: Vec<PageId>,
}

pub trait PageStore {
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>>;
    fn read_page(&self, page_id: PageId) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>>;
    fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>>;
}

/// Deterministic store used by the engine contract tests. Async/failure
/// injection belongs here rather than in IDBTree so the same tree exercises
/// resident and genuinely pending I/O.
#[derive(Clone, Default)]
pub struct MemoryPageStore {
    inner: Rc<RefCell<MemoryPageStoreState>>,
}

#[derive(Default)]
struct MemoryPageStoreState {
    metadata: Option<Metadata>,
    pages: BTreeMap<PageId, Vec<u8>>,
}

impl PageStore for MemoryPageStore {
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
        Box::pin(async { Ok(self.inner.borrow().metadata.clone()) })
    }

    fn read_page(&self, page_id: PageId) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
        Box::pin(async move { Ok(self.inner.borrow().pages.get(&page_id).cloned()) })
    }

    fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
        Box::pin(async move {
            let mut state = self.inner.borrow_mut();
            let generation = state
                .metadata
                .as_ref()
                .map_or(0, |metadata| metadata.generation);
            if generation != commit.expected_generation {
                return Err(format!(
                    "generation changed: expected {}, found {generation}",
                    commit.expected_generation
                ));
            }
            for (page_id, page) in &commit.pages {
                state.pages.insert(*page_id, page.clone());
            }
            for page_id in &commit.deleted_page_ids {
                state.pages.remove(page_id);
            }
            let mut metadata = commit.metadata.clone();
            metadata.generation = generation + 1;
            state.metadata = Some(metadata.clone());
            Ok(metadata)
        })
    }
}
