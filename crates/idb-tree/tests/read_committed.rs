//! The read-committed view of an `IdbTree`: it answers from the last durable
//! generation, never sees staged or committing writes, and never does I/O.
//! This is a storage-engine contract, below anything a Jazz client observes.

use futures::{FutureExt, executor::block_on};
use idb_tree::{BoxFuture, Commit, Error, IdbTree, MemoryPageStore, Metadata, Options, PageStore};
use std::{cell::RefCell, rc::Rc};

type Pause = Rc<RefCell<Option<futures::channel::oneshot::Receiver<Result<(), String>>>>>;

#[derive(Clone, Default)]
struct PausingStore {
    memory: MemoryPageStore,
    pause_next_commit: Pause,
}

impl PageStore for PausingStore {
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
        self.memory.load_metadata()
    }

    fn read_page(&self, page_id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
        self.memory.read_page(page_id)
    }

    fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
        let pause = self.pause_next_commit.borrow_mut().take();
        Box::pin(async move {
            if let Some(pause) = pause {
                pause.await.map_err(|_| "cancelled".to_owned())??;
            }
            self.memory.commit(commit).await
        })
    }
}

fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    future
        .now_or_never()
        .expect("read-committed reads complete on their first poll")
}

#[test]
fn staged_writes_become_visible_only_once_committed() {
    block_on(async {
        let tree = IdbTree::open(PausingStore::default(), Options::default())
            .await
            .unwrap();
        let committed = tree.read_committed();
        tree.put(b"a".to_vec(), b"1".to_vec()).await.unwrap();

        // Nothing is durable yet: the view is empty.
        assert_eq!(ready(committed.get(b"a")).unwrap(), None);
        assert_eq!(ready(committed.range(b"", b"\xff")).unwrap(), vec![]);

        tree.flush().await.unwrap();
        tree.put(b"a".to_vec(), b"2".to_vec()).await.unwrap();
        tree.put(b"b".to_vec(), b"3".to_vec()).await.unwrap();
        assert_eq!(ready(committed.get(b"a")).unwrap(), Some(b"1".to_vec()));
        assert_eq!(
            ready(committed.value_equals(b"a", b"1")).unwrap(),
            Some(true)
        );
        assert_eq!(
            ready(committed.range_reverse(b"", b"\xff", 5)).unwrap(),
            vec![(b"a".to_vec(), b"1".to_vec())]
        );
        assert_eq!(tree.get(b"a").await.unwrap(), Some(b"2".to_vec()));

        tree.flush().await.unwrap();
        assert_eq!(
            ready(committed.range(b"", b"\xff")).unwrap(),
            vec![
                (b"a".to_vec(), b"2".to_vec()),
                (b"b".to_vec(), b"3".to_vec())
            ]
        );
    });
}

#[test]
fn a_commit_in_flight_is_invisible_until_it_lands_and_a_failed_one_never_appears() {
    block_on(async {
        for succeeds in [true, false] {
            let store = PausingStore::default();
            let tree = IdbTree::open(store.clone(), Options { page_size: 1024 })
                .await
                .unwrap();
            for index in 0u32..300 {
                tree.put(index.to_be_bytes().to_vec(), b"old".to_vec())
                    .await
                    .unwrap();
            }
            tree.flush().await.unwrap();
            for index in 0u32..300 {
                tree.put(index.to_be_bytes().to_vec(), b"new".to_vec())
                    .await
                    .unwrap();
            }
            let committed = tree.read_committed();

            let (release, paused) = futures::channel::oneshot::channel();
            *store.pause_next_commit.borrow_mut() = Some(paused);
            let mut flush = Box::pin(tree.flush());
            assert!(futures::poll!(flush.as_mut()).is_pending());
            let rows = ready(committed.range(b"", b"\xff")).unwrap();
            assert_eq!(rows.len(), 300);
            assert!(rows.iter().all(|(_, value)| value == b"old"));

            release
                .send(if succeeds { Ok(()) } else { Err("disk".into()) })
                .unwrap();
            assert_eq!(flush.await.is_ok(), succeeds);
            let expected: &[u8] = if succeeds { b"new" } else { b"old" };
            let rows = ready(committed.range(b"", b"\xff")).unwrap();
            assert!(rows.iter().all(|(_, value)| value == expected));
        }
    });
}

#[test]
fn the_view_never_hydrates_and_cannot_write() {
    block_on(async {
        let store = PausingStore::default();
        let tree = IdbTree::open(store.clone(), Options::default())
            .await
            .unwrap();
        tree.put(b"a".to_vec(), b"1".to_vec()).await.unwrap();
        tree.flush().await.unwrap();
        drop(tree);

        let cold = IdbTree::open(store, Options::default()).await.unwrap();
        let committed = cold.read_committed();
        assert!(matches!(
            ready(committed.get(b"a")),
            Err(Error::NotResident(_))
        ));
        assert!(matches!(
            ready(committed.range(b"", b"\xff")),
            Err(Error::NotResident(_))
        ));
        assert!(matches!(
            ready(committed.put(b"b".to_vec(), b"2".to_vec())),
            Err(Error::ReadOnlyView)
        ));
        assert!(matches!(ready(committed.flush()), Err(Error::ReadOnlyView)));

        // Once the ordinary tree hydrates the page, the view serves it.
        assert_eq!(cold.get(b"a").await.unwrap(), Some(b"1".to_vec()));
        assert_eq!(ready(committed.get(b"a")).unwrap(), Some(b"1".to_vec()));
    });
}
