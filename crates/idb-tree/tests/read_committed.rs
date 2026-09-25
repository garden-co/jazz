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
fn a_generation_staged_over_an_in_flight_commit_stays_invisible_after_it_lands() {
    block_on(async {
        for succeeds in [true, false] {
            let store = PausingStore::default();
            let options = Options { page_size: 1024 };
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            let key = |index: u32| index.to_be_bytes().to_vec();
            let large = |byte: u8| vec![byte; 3_000];
            // Generation N-1: durable.
            for index in 0u32..200 {
                tree.put(key(index), b"n-1".to_vec()).await.unwrap();
            }
            tree.put(b"big".to_vec(), large(1)).await.unwrap();
            tree.flush().await.unwrap();
            let committed = tree.read_committed();

            // Generation N: its commit is paused.
            for index in 0u32..200 {
                tree.put(key(index), b"n".to_vec()).await.unwrap();
            }
            tree.put(b"big".to_vec(), large(2)).await.unwrap();
            let (release, paused) = futures::channel::oneshot::channel();
            *store.pause_next_commit.borrow_mut() = Some(paused);
            let mut flush = Box::pin(tree.flush());
            assert!(futures::poll!(flush.as_mut()).is_pending());

            // Generation N+1: staged on top of the in-flight commit.
            for index in (0u32..200).step_by(3) {
                tree.put(key(index), b"n+1".to_vec()).await.unwrap();
            }
            assert!(tree.delete(&key(1)).await.unwrap());
            tree.put(b"big".to_vec(), large(3)).await.unwrap();
            tree.put(key(500), large(4)).await.unwrap();

            let expect = |generation: &[u8], big: u8| {
                let mut rows: Vec<_> = (0u32..200)
                    .map(|index| (key(index), generation.to_vec()))
                    .collect();
                rows.push((b"big".to_vec(), large(big)));
                rows.sort();
                rows
            };
            assert_eq!(
                ready(committed.range(b"", b"\xff\xff\xff\xff\xff")).unwrap(),
                expect(b"n-1", 1)
            );

            release
                .send(if succeeds { Ok(()) } else { Err("disk".into()) })
                .unwrap();
            assert_eq!(flush.await.is_ok(), succeeds);
            // Exactly N (or still N-1): never any of N+1's staged writes.
            let (generation, big): (&[u8], u8) = if succeeds { (b"n", 2) } else { (b"n-1", 1) };
            assert_eq!(
                ready(committed.range(b"", b"\xff\xff\xff\xff\xff")).unwrap(),
                expect(generation, big)
            );
            assert_eq!(ready(committed.get(&key(500))).unwrap(), None);

            // The next flush lands everything, and disk agrees with the view.
            tree.flush().await.unwrap();
            let mut latest: Vec<_> = (0u32..200)
                .filter(|index| *index != 1)
                .map(|index| {
                    let value: &[u8] = if index % 3 == 0 { b"n+1" } else { b"n" };
                    (key(index), value.to_vec())
                })
                .collect();
            latest.push((b"big".to_vec(), large(3)));
            latest.push((key(500), large(4)));
            latest.sort();
            let everything = |tree: &IdbTree<PausingStore>| {
                ready(tree.read_committed().range(b"", b"\xff\xff\xff\xff\xff")).unwrap()
            };
            assert_eq!(everything(&tree), latest);
            drop(committed);
            drop(tree);
            let reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(
                reopened.range(b"", b"\xff\xff\xff\xff\xff").await.unwrap(),
                latest
            );
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

#[test]
fn a_flush_dropped_mid_commit_forces_a_reload_and_never_serves_its_writes() {
    block_on(async {
        let store = PausingStore::default();
        let tree = IdbTree::open(store.clone(), Options::default())
            .await
            .unwrap();
        tree.put(b"a".to_vec(), b"1".to_vec()).await.unwrap();
        tree.flush().await.unwrap();
        let committed = tree.read_committed();

        tree.put(b"a".to_vec(), b"2".to_vec()).await.unwrap();
        let (_hold, paused) = futures::channel::oneshot::channel();
        *store.pause_next_commit.borrow_mut() = Some(paused);
        let mut flush = Box::pin(tree.flush());
        assert!(futures::poll!(flush.as_mut()).is_pending());
        drop(flush);

        // The outcome is unknown, so the live tree refuses everything, while
        // the view keeps serving the last committed generation.
        assert!(matches!(tree.get(b"a").await, Err(Error::CommitAbandoned)));
        assert!(matches!(
            tree.put(b"b".to_vec(), b"3".to_vec()).await,
            Err(Error::CommitAbandoned)
        ));
        assert!(matches!(tree.flush().await, Err(Error::CommitAbandoned)));
        assert_eq!(ready(committed.get(b"a")).unwrap(), Some(b"1".to_vec()));

        // The store never committed it: a reload returns to generation 1 and
        // the tree is writable again.
        tree.reload().await.unwrap();
        assert_eq!(tree.get(b"a").await.unwrap(), Some(b"1".to_vec()));
        tree.put(b"b".to_vec(), b"3".to_vec()).await.unwrap();
        tree.flush().await.unwrap();
        drop(committed);
        drop(tree);
        let reopened = IdbTree::open(store, Options::default()).await.unwrap();
        assert_eq!(
            reopened.range(b"", b"\xff").await.unwrap(),
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"3".to_vec())
            ]
        );
    });
}
