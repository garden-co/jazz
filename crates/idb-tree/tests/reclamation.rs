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
    ownership_generation: Rc<Cell<u64>>,
    metadata_resume: Rc<RefCell<Option<futures::channel::oneshot::Receiver<()>>>>,
    fail: Rc<Cell<bool>>,
    fail_metadata: Rc<Cell<bool>>,
    metadata_reads: Rc<Cell<usize>>,
    live: Rc<RefCell<BTreeSet<u64>>>,
    deleted: Rc<RefCell<BTreeSet<u64>>>,
    pause: Rc<Cell<bool>>,
    fail_paused_read: Rc<Cell<bool>>,
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
        if !self.exclusive.get() {
            return Ok(TreeOwnership::default());
        }
        if self.claimed.replace(true) {
            return Err("owner already has a tree".into());
        }
        let token = self.ownership_generation.get() + 1;
        self.ownership_generation.set(token);
        let live = self.claimed.clone();
        let generation = self.ownership_generation.clone();
        let release_live = live.clone();
        let release_generation = generation.clone();
        Ok(TreeOwnership::revocable(
            move || live.get() && generation.get() == token,
            move || {
                if release_generation.get() == token {
                    release_live.set(false);
                }
            },
        ))
    }

    fn can_reclaim_obsolete_pages(&self) -> bool {
        self.exclusive.get()
    }
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
        self.metadata_reads.set(self.metadata_reads.get() + 1);
        if self.fail_metadata.replace(false) {
            return Box::pin(async { Err("metadata unavailable".into()) });
        }
        Box::pin(async move {
            let metadata = self.memory.load_metadata().await?;
            let resume = self.metadata_resume.borrow_mut().take();
            if let Some(resume) = resume {
                resume.await.map_err(|e| e.to_string())?;
            }
            Ok(metadata)
        })
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
                if self.fail_paused_read.replace(false) {
                    return Err("old read failed".into());
                }
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

#[test]
fn pending_read_error_from_before_reload_retries_even_when_root_id_is_unchanged() {
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
        store.fail_paused_read.set(true);
        let mut read = Box::pin(tree.get(b"key"));
        assert!(read.as_mut().now_or_never().is_none());
        let root = tree.metadata().root_page_id;
        tree.reload().await.unwrap();
        assert_eq!(tree.metadata().root_page_id, root);
        resume.send(()).unwrap();
        assert_eq!(read.await.unwrap(), Some(vec![1; 4000]));
    });
}

#[test]
fn failed_batch_restores_retirement_and_multilevel_replacements_stay_bounded() {
    block_on(async {
        let store = Store::exclusive();
        let tree = IdbTree::open(store.clone(), options()).await.unwrap();
        for n in 0u32..1000 {
            tree.put(n.to_be_bytes().to_vec(), vec![1; 80])
                .await
                .unwrap();
        }
        tree.flush().await.unwrap();
        let initial_pages = store.live.borrow().len();
        assert!(initial_pages > 100, "fixture must span multiple leaves");
        for generation in 2..6 {
            let operations = (0u32..1000)
                .map(|n| WriteOperation::Set {
                    key: n.to_be_bytes().to_vec(),
                    value: vec![generation; 80],
                })
                .collect();
            tree.write_many(operations).await.unwrap();
            tree.flush().await.unwrap();
            assert!(
                store.live.borrow().len() <= initial_pages,
                "replaced ancestors leaked"
            );
        }
        let failed = tree
            .write_many(vec![
                WriteOperation::Set {
                    key: 0u32.to_be_bytes().to_vec(),
                    value: vec![7; 5000],
                },
                WriteOperation::Set {
                    key: vec![255; 2000],
                    value: vec![8; 5000],
                },
            ])
            .await;
        assert!(failed.is_err());
        tree.put(999u32.to_be_bytes().to_vec(), vec![9; 80])
            .await
            .unwrap();
        tree.flush().await.unwrap();
        drop(tree);
        let reopened = IdbTree::open(store.clone(), options()).await.unwrap();
        assert_eq!(
            reopened.get(&0u32.to_be_bytes()).await.unwrap(),
            Some(vec![5; 80])
        );
        assert_eq!(
            reopened.get(&999u32.to_be_bytes()).await.unwrap(),
            Some(vec![9; 80])
        );
        assert!(store.live.borrow().len() <= initial_pages);
    });
}

#[test]
fn successful_publication_keeps_newer_staged_generation_and_reclaims_it_on_next_flush() {
    block_on(async {
        let store = Store::exclusive();
        let tree = IdbTree::open(store.clone(), options()).await.unwrap();
        tree.put(b"key".to_vec(), vec![1; 4000]).await.unwrap();
        tree.flush().await.unwrap();
        tree.put(b"key".to_vec(), vec![2; 4000]).await.unwrap();
        let (resume, receiver) = futures::channel::oneshot::channel();
        *store.commit_resume.borrow_mut() = Some(receiver);
        let mut flush = Box::pin(tree.flush());
        assert!(flush.as_mut().now_or_never().is_none());
        tree.put(b"key".to_vec(), vec![3; 4000]).await.unwrap();
        resume.send(()).unwrap();
        flush.await.unwrap();
        assert_eq!(tree.get(b"key").await.unwrap(), Some(vec![3; 4000]));
        tree.flush().await.unwrap();
        assert!(store.live.borrow().len() <= 6);
        drop(tree);
        let reopened = IdbTree::open(store, options()).await.unwrap();
        assert_eq!(reopened.get(b"key").await.unwrap(), Some(vec![3; 4000]));
    });
}

#[test]
fn revoked_cached_and_pending_handles_cannot_access_or_release_successor() {
    block_on(async {
        let store = Store::exclusive();
        let old = IdbTree::open(store.clone(), options()).await.unwrap();
        old.put(b"key".to_vec(), vec![1; 4000]).await.unwrap();
        old.flush().await.unwrap();
        assert_eq!(old.get(b"key").await.unwrap(), Some(vec![1; 4000]));
        let retained = old.clone();
        store.claimed.set(false); // synchronous runtime retirement
        let successor = IdbTree::open(store.clone(), options()).await.unwrap();
        assert!(matches!(
            old.get(b"key").await,
            Err(idb_tree::Error::OwnershipExpired)
        ));
        assert!(matches!(
            old.put(b"key".to_vec(), vec![2]).await,
            Err(idb_tree::Error::OwnershipExpired)
        ));
        assert!(matches!(
            old.delete(b"key").await,
            Err(idb_tree::Error::OwnershipExpired)
        ));
        assert!(matches!(
            old.write_many(vec![]).await,
            Err(idb_tree::Error::OwnershipExpired)
        ));
        assert!(matches!(
            old.range(b"", b"z").await,
            Err(idb_tree::Error::OwnershipExpired)
        ));
        assert!(matches!(
            old.range_limit(b"", b"z", 0).await,
            Err(idb_tree::Error::OwnershipExpired)
        ));
        assert!(matches!(
            old.range_reverse(b"", b"z", 0).await,
            Err(idb_tree::Error::OwnershipExpired)
        ));
        assert!(matches!(
            old.range_reverse(b"", b"z", 1).await,
            Err(idb_tree::Error::OwnershipExpired)
        ));
        assert!(matches!(
            old.flush().await,
            Err(idb_tree::Error::OwnershipExpired)
        ));
        assert!(matches!(
            old.reload().await,
            Err(idb_tree::Error::OwnershipExpired)
        ));
        drop(old);
        drop(retained);
        assert!(IdbTree::open(store.clone(), options()).await.is_err());
        successor.put(b"key".to_vec(), vec![3; 4000]).await.unwrap();
        successor.flush().await.unwrap();
        successor.reload().await.unwrap();
        let (resume, receiver) = futures::channel::oneshot::channel();
        *store.read_resume.borrow_mut() = Some(receiver);
        store.pause.set(true);
        let mut read = Box::pin(successor.get(b"key"));
        assert!(read.as_mut().now_or_never().is_none());
        store.claimed.set(false);
        let final_owner = IdbTree::open(store.clone(), options()).await.unwrap();
        final_owner
            .put(b"key".to_vec(), vec![4; 4000])
            .await
            .unwrap();
        final_owner.flush().await.unwrap();
        resume.send(()).unwrap();
        assert!(matches!(read.await, Err(idb_tree::Error::OwnershipExpired)));
        assert_eq!(final_owner.get(b"key").await.unwrap(), Some(vec![4; 4000]));
    });
}

#[test]
fn revocation_during_open_rejects_old_metadata_and_preserves_new_guard() {
    block_on(async {
        let store = Store::exclusive();
        let (resume, receiver) = futures::channel::oneshot::channel();
        *store.metadata_resume.borrow_mut() = Some(receiver);
        let mut old_open = Box::pin(IdbTree::open(store.clone(), options()));
        assert!(old_open.as_mut().now_or_never().is_none());
        store.claimed.set(false);
        let new = IdbTree::open(store.clone(), options()).await.unwrap();
        new.put(b"key".to_vec(), vec![7]).await.unwrap();
        new.flush().await.unwrap();
        resume.send(()).unwrap();
        assert!(matches!(
            old_open.await,
            Err(idb_tree::Error::OwnershipExpired)
        ));
        assert!(IdbTree::open(store.clone(), options()).await.is_err());
        assert_eq!(new.get(b"key").await.unwrap(), Some(vec![7]));
    });
}
