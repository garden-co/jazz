use futures::{FutureExt, executor::block_on};
use idb_tree::{
    BoxFuture, Commit, IdbTree, MemoryPageStore, Metadata, Options, PageStore, TreeOwnership,
    WriteOperation,
};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeSet,
    rc::Rc,
};

#[derive(Clone, Default)]
struct Store {
    memory: MemoryPageStore,
    exclusive: Rc<Cell<bool>>,
    claimed: Rc<Cell<bool>>,
    fail: Rc<Cell<bool>>,
    fail_metadata: Rc<Cell<bool>>,
    metadata_reads: Rc<Cell<usize>>,
    live: Rc<RefCell<BTreeSet<u64>>>,
    deleted: Rc<RefCell<BTreeSet<u64>>>,
    pause: Rc<Cell<bool>>,
    commit_resume: Rc<RefCell<Option<futures::channel::oneshot::Receiver<()>>>>,
    read_resume: Rc<RefCell<Option<futures::channel::oneshot::Receiver<()>>>>,
}
impl Store {
    fn exclusive() -> Self {
        let store = Self::default();
        store.exclusive.set(true);
        store
    }
}
impl PageStore for Store {
    fn claim_tree_ownership(&self) -> Result<TreeOwnership, String> {
        if self.exclusive.get() && self.claimed.replace(true) {
            return Err("owner already has a tree".into());
        }
        let claimed = self.claimed.clone();
        Ok(TreeOwnership::new(move || claimed.set(false)))
    }
    fn can_reclaim_obsolete_pages(&self) -> bool {
        self.exclusive.get()
    }
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
        self.metadata_reads.set(self.metadata_reads.get() + 1);
        if self.fail_metadata.replace(false) {
            return Box::pin(async { Err("metadata unavailable".into()) });
        }
        self.memory.load_metadata()
    }
    fn read_page(&self, id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
        Box::pin(async move {
            let resume = if self.pause.replace(false) {
                self.read_resume.borrow_mut().take()
            } else {
                None
            };
            if let Some(resume) = resume {
                resume.await.map_err(|e| e.to_string())?;
            }
            self.memory.read_page(id).await
        })
    }
    fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
        Box::pin(async move {
            let resume = self.commit_resume.borrow_mut().take();
            if let Some(resume) = resume {
                resume.await.map_err(|e| e.to_string())?;
            }
            if self.fail.replace(false) {
                return Err("injected failure".into());
            }
            if !commit.deleted_page_ids.is_empty() && !self.exclusive.get() {
                return Err("ownership expired".into());
            }
            let metadata = self.memory.commit(commit).await?;
            let mut live = self.live.borrow_mut();
            live.extend(commit.pages.iter().map(|(id, _)| *id));
            for id in &commit.deleted_page_ids {
                live.remove(id);
                self.deleted.borrow_mut().insert(*id);
            }
            Ok(metadata)
        })
    }
}
fn options() -> Options {
    Options { page_size: 1024 }
}

#[test]
fn exclusive_replacement_bounds_live_pages_and_preserves_sibling_overflow_on_reopen() {
    block_on(async {
        let store = Store::exclusive();
        let tree = IdbTree::open(store.clone(), options()).await.unwrap();
        let sibling = vec![9; 5000];
        tree.put(b"sibling".to_vec(), sibling.clone())
            .await
            .unwrap();
        for n in 0..80 {
            tree.write_many(vec![
                WriteOperation::Set {
                    key: b"changing".to_vec(),
                    value: vec![n; 4000],
                },
                WriteOperation::Set {
                    key: b"temporary".to_vec(),
                    value: vec![n; 3000],
                },
                WriteOperation::Delete {
                    key: b"temporary".to_vec(),
                },
            ])
            .await
            .unwrap();
            tree.flush().await.unwrap();
            assert!(
                store.live.borrow().len() <= 12,
                "historical COW pages leaked"
            );
            assert_eq!(tree.get(b"sibling").await.unwrap(), Some(sibling.clone()));
        }
        tree.delete(b"changing").await.unwrap();
        tree.flush().await.unwrap();
        assert!(store.live.borrow().len() <= 7);
        drop(tree);
        let reopened = IdbTree::open(store.clone(), options()).await.unwrap();
        assert_eq!(reopened.get(b"sibling").await.unwrap(), Some(sibling));
        assert_eq!(reopened.get(b"changing").await.unwrap(), None);
        assert!(!store.deleted.borrow().is_empty());
    });
}

#[test]
fn generic_independent_cold_handle_keeps_its_complete_old_closure() {
    block_on(async {
        let store = Store::default();
        let writer = IdbTree::open(store.clone(), options()).await.unwrap();
        writer.put(b"key".to_vec(), vec![1; 4000]).await.unwrap();
        writer.flush().await.unwrap();
        let cold = IdbTree::open(store.clone(), options()).await.unwrap();
        writer.put(b"key".to_vec(), vec![2; 4000]).await.unwrap();
        writer.flush().await.unwrap();
        assert_eq!(cold.get(b"key").await.unwrap(), Some(vec![1; 4000]));
        cold.put(b"other".to_vec(), vec![3]).await.unwrap();
        assert!(matches!(
            cold.flush().await,
            Err(idb_tree::Error::GenerationConflict(_))
        ));
        assert!(store.deleted.borrow().is_empty());
        let reopened = IdbTree::open(store, options()).await.unwrap();
        assert_eq!(reopened.get(b"key").await.unwrap(), Some(vec![2; 4000]));
    });
}

#[test]
fn failed_publication_with_newer_writes_retries_complete_closure() {
    block_on(async {
        let store = Store::exclusive();
        let tree = IdbTree::open(store.clone(), options()).await.unwrap();
        tree.put(b"key".to_vec(), vec![1; 4000]).await.unwrap();
        tree.flush().await.unwrap();
        tree.put(b"key".to_vec(), vec![2; 4000]).await.unwrap();
        let (resume, receiver) = futures::channel::oneshot::channel();
        *store.commit_resume.borrow_mut() = Some(receiver);
        store.fail.set(true);
        let mut flush = Box::pin(tree.flush());
        assert!(flush.as_mut().now_or_never().is_none());
        tree.put(b"key".to_vec(), vec![3; 4000]).await.unwrap();
        resume.send(()).unwrap();
        flush.await.unwrap_err();
        tree.flush().await.unwrap();
        drop(tree);
        let reopened = IdbTree::open(store.clone(), options()).await.unwrap();
        assert_eq!(reopened.get(b"key").await.unwrap(), Some(vec![3; 4000]));
        assert!(store.live.borrow().len() <= 6);
    });
}

#[test]
fn same_handle_pending_cold_read_retries_after_old_page_is_reclaimed() {
    block_on(async {
        let store = Store::exclusive();
        let initial = IdbTree::open(store.clone(), options()).await.unwrap();
        initial.put(b"key".to_vec(), vec![1; 4000]).await.unwrap();
        initial.flush().await.unwrap();
        drop(initial);
        let tree = IdbTree::open(store.clone(), options()).await.unwrap();
        let (resume, receiver) = futures::channel::oneshot::channel();
        *store.read_resume.borrow_mut() = Some(receiver);
        store.pause.set(true);
        let mut read = Box::pin(tree.get(b"key"));
        assert!(read.as_mut().now_or_never().is_none());
        tree.put(b"key".to_vec(), vec![2; 4000]).await.unwrap();
        tree.flush().await.unwrap();
        resume.send(()).unwrap();
        assert_eq!(read.await.unwrap(), Some(vec![2; 4000]));
    });
}

#[test]
fn exclusive_admission_precedes_metadata_io_and_failed_open_releases_it() {
    block_on(async {
        let store = Store::exclusive();
        let initial = IdbTree::open(store.clone(), options()).await.unwrap();
        initial.put(b"key".to_vec(), vec![1; 4000]).await.unwrap();
        initial.flush().await.unwrap();
        let reads = store.metadata_reads.get();
        assert!(IdbTree::open(store.clone(), options()).await.is_err());
        assert_eq!(
            store.metadata_reads.get(),
            reads,
            "competing open must not observe a stale root before admission"
        );
        initial.put(b"key".to_vec(), vec![2; 4000]).await.unwrap();
        initial.flush().await.unwrap();
        let retained = initial.clone();
        drop(initial);
        assert!(IdbTree::open(store.clone(), options()).await.is_err());
        drop(retained);
        store.fail_metadata.set(true);
        assert!(IdbTree::open(store.clone(), options()).await.is_err());
        assert!(
            IdbTree::open(store.clone(), Options { page_size: 2048 })
                .await
                .is_err()
        );
        let reopened = IdbTree::open(store.clone(), options()).await.unwrap();
        assert_eq!(reopened.get(b"key").await.unwrap(), Some(vec![2; 4000]));
    });
}

#[test]
fn reload_retains_exclusive_admission_and_discards_failed_staging() {
    block_on(async {
        let store = Store::exclusive();
        let tree = IdbTree::open(store.clone(), options()).await.unwrap();
        tree.put(b"key".to_vec(), vec![1; 4000]).await.unwrap();
        tree.flush().await.unwrap();
        tree.put(b"key".to_vec(), vec![2; 4000]).await.unwrap();
        store.fail.set(true);
        tree.flush().await.unwrap_err();
        let clone = tree.clone();
        tree.reload().await.unwrap();
        assert!(IdbTree::open(store.clone(), options()).await.is_err());
        assert_eq!(clone.get(b"key").await.unwrap(), Some(vec![1; 4000]));
        tree.put(b"key".to_vec(), vec![3; 4000]).await.unwrap();
        tree.flush().await.unwrap();
        assert!(store.live.borrow().len() <= 6);
    });
}
