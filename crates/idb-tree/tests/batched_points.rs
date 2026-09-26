//! Batch page-count and suspension contracts are exercised at the public
//! IdbTree/PageStore boundary; ordinary query equality cannot prove them.
use futures::{FutureExt, channel::oneshot, executor::block_on, future::join_all};
use idb_tree::{BoxFuture, Commit, Error, IdbTree, MemoryPageStore, Metadata, Options, PageStore};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeSet,
    rc::Rc,
};

#[derive(Clone, Default)]
struct Store {
    memory: MemoryPageStore,
    reads: Rc<Cell<usize>>,
    trips: Rc<Cell<usize>>,
    ids: Rc<RefCell<BTreeSet<u64>>>,
    pause: Rc<RefCell<Option<oneshot::Receiver<()>>>>,
    fail_paused: Rc<Cell<bool>>,
}
impl Store {
    fn reset_counts(&self) {
        self.reads.set(0);
        self.trips.set(0);
        self.ids.borrow_mut().clear();
    }
    fn load(&self, id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
        self.reads.set(self.reads.get() + 1);
        self.ids.borrow_mut().insert(id);
        Box::pin(async move {
            let value = self.memory.read_page(id).await?;
            let pause = self.pause.borrow_mut().take();
            if let Some(pause) = pause {
                pause.await.map_err(|error| error.to_string())?;
                if self.fail_paused.replace(false) {
                    return Err("injected paused-read failure".into());
                }
            }
            Ok(value)
        })
    }
    fn pause_next(&self) -> oneshot::Sender<()> {
        let (send, recv) = oneshot::channel();
        *self.pause.borrow_mut() = Some(recv);
        send
    }
}
impl PageStore for Store {
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
        self.memory.load_metadata()
    }
    fn read_page(&self, id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
        self.trips.set(self.trips.get() + 1);
        self.load(id)
    }
    fn read_pages<'a>(
        &'a self,
        ids: &'a [u64],
    ) -> BoxFuture<'a, Result<Vec<Option<Vec<u8>>>, String>> {
        self.trips.set(self.trips.get() + 1);
        Box::pin(async move {
            join_all(ids.iter().map(|id| self.load(*id)))
                .await
                .into_iter()
                .collect()
        })
    }
    fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
        self.memory.commit(commit)
    }
}
fn key(id: u32) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}
fn value(id: u32, len: usize) -> Vec<u8> {
    vec![id as u8; len]
}
async fn fixture(rows: u32, len: usize) -> (Store, IdbTree<Store>) {
    let store = Store::default();
    let options = Options { page_size: 1024 };
    let tree = IdbTree::open(store.clone(), options).await.unwrap();
    for id in 0..rows {
        tree.put(key(id), value(id, len)).await.unwrap();
    }
    tree.flush().await.unwrap();
    drop(tree);
    let tree = IdbTree::open(store.clone(), options).await.unwrap();
    store.reset_counts();
    (store, tree)
}

#[test]
fn cold_required_points_share_pages_and_keep_order_including_overflow() {
    block_on(async {
        for len in [80, 4500] {
            let (store, tree) = fixture(500, len).await;
            let ids: Vec<_> = (0..300).map(|i| (i * 73) % 500).collect();
            let keys: Vec<_> = ids.iter().map(|id| key(*id)).collect();
            let expected: Vec<_> = ids.iter().map(|id| value(*id, len)).collect();
            assert_eq!(
                tree.get_many_required(&keys).await.unwrap(),
                Some(expected.clone())
            );
            assert!(store.reads.get() > 20, "fixture must span cold pages");
            assert_eq!(
                store.reads.get(),
                store.ids.borrow().len(),
                "a shared page is read once"
            );
            assert!(
                store.trips.get() <= 10,
                "one trip per missing frontier, including overflow: {}",
                store.trips.get()
            );
            store.reset_counts();
            assert_eq!(tree.get_many_required(&keys).await.unwrap(), Some(expected));
            assert_eq!(store.reads.get(), 0, "warm point batches need no store I/O");
        }
    });
}

#[test]
fn required_points_preserve_duplicates_absence_and_empty_groups() {
    block_on(async {
        let (_, tree) = fixture(3, 20).await;
        assert_eq!(tree.get_many_required(&[]).await.unwrap(), Some(vec![]));
        assert_eq!(
            tree.get_many_required(&[key(2), key(0), key(2)])
                .await
                .unwrap(),
            Some(vec![value(2, 20), value(0, 20), value(2, 20)])
        );
        assert_eq!(
            tree.get_many_required(&[key(0), key(99), key(2)])
                .await
                .unwrap(),
            None
        );
        tree.delete(&key(0)).await.unwrap();
        assert_eq!(tree.get_many_required(&[key(0)]).await.unwrap(), None);
    });
}

#[test]
fn parked_batch_does_not_block_a_writer_and_retries_the_new_root() {
    block_on(async {
        let (store, tree) = fixture(200, 80).await;
        let keys = vec![key(1), key(150)];
        let resume = store.pause_next();
        let mut read = Box::pin(tree.get_many_required(&keys));
        assert!(read.as_mut().now_or_never().is_none());
        tree.put(key(1), vec![7; 80])
            .now_or_never()
            .expect("writer must be independently driveable")
            .unwrap();
        tree.put(key(150), vec![8; 80]).await.unwrap();
        tree.flush().await.unwrap();
        resume.send(()).unwrap();
        assert_eq!(read.await.unwrap(), Some(vec![vec![7; 80], vec![8; 80]]));
    });
}

#[test]
fn stale_batch_error_after_reload_is_discarded_and_retried() {
    block_on(async {
        let (store, tree) = fixture(200, 80).await;
        let keys = vec![key(1), key(150)];
        let resume = store.pause_next();
        store.fail_paused.set(true);
        let mut read = Box::pin(tree.get_many_required(&keys));
        assert!(read.as_mut().now_or_never().is_none());
        tree.reload().await.unwrap();
        resume.send(()).unwrap();
        assert_eq!(
            read.await.unwrap(),
            Some(vec![value(1, 80), value(150, 80)])
        );
    });
}

#[test]
fn cancelled_batch_leaves_later_reads_free_to_complete() {
    block_on(async {
        let (store, tree) = fixture(200, 80).await;
        let keys = vec![key(1), key(150)];
        let resume = store.pause_next();
        let mut read = Box::pin(tree.get_many_required(&keys));
        assert!(read.as_mut().now_or_never().is_none());
        drop(read);
        assert!(
            resume.send(()).is_err(),
            "cancellation releases the pending store read"
        );
        assert_eq!(
            tree.get_many_required(&keys).await.unwrap(),
            Some(vec![value(1, 80), value(150, 80)])
        );
    });
}

#[test]
fn required_point_batches_preserve_read_committed_visibility() {
    block_on(async {
        let (store, tree) = fixture(200, 80).await;
        let keys = vec![key(1), key(150)];
        assert!(matches!(
            tree.read_committed().get_many_required(&keys).await,
            Err(Error::NotResident(_))
        ));
        assert_eq!(
            store.reads.get(),
            0,
            "a read-committed miss must not start I/O"
        );
        tree.get_many_required(&keys).await.unwrap();
        tree.put(key(1), vec![9; 80]).await.unwrap();
        assert_eq!(
            tree.read_committed()
                .get_many_required(&keys)
                .await
                .unwrap(),
            Some(vec![value(1, 80), value(150, 80)])
        );
        tree.flush().await.unwrap();
        assert_eq!(
            tree.read_committed()
                .get_many_required(&keys)
                .await
                .unwrap(),
            Some(vec![vec![9; 80], value(150, 80)])
        );
    });
}
