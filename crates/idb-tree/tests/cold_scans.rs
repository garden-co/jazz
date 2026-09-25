//! Cold range scans against a store that counts round trips.
//!
//! This stays at the `IdbTree` + `PageStore` boundary on purpose: the
//! property under test is how many store reads a cold scan issues, which no
//! Jazz client API can observe.

use futures::executor::block_on;
use idb_tree::{BoxFuture, Commit, IdbTree, MemoryPageStore, Metadata, Options, PageStore};
use std::{cell::Cell, rc::Rc};

#[derive(Clone, Default)]
struct CountingStore {
    memory: MemoryPageStore,
    round_trips: Rc<Cell<usize>>,
    pages_read: Rc<Cell<usize>>,
}

impl CountingStore {
    fn reset(&self) {
        self.round_trips.set(0);
        self.pages_read.set(0);
    }
}

impl PageStore for CountingStore {
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
        self.memory.load_metadata()
    }

    fn read_page(&self, page_id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
        self.round_trips.set(self.round_trips.get() + 1);
        self.pages_read.set(self.pages_read.get() + 1);
        self.memory.read_page(page_id)
    }

    fn read_pages<'a>(
        &'a self,
        page_ids: &'a [u64],
    ) -> BoxFuture<'a, Result<Vec<Option<Vec<u8>>>, String>> {
        self.round_trips.set(self.round_trips.get() + 1);
        self.pages_read.set(self.pages_read.get() + page_ids.len());
        Box::pin(async move {
            let mut pages = Vec::with_capacity(page_ids.len());
            for &page_id in page_ids {
                pages.push(self.memory.read_page(page_id).await?);
            }
            Ok(pages)
        })
    }

    fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
        self.memory.commit(commit)
    }
}

const SMALL_PAGES: Options = Options { page_size: 1024 };

fn key(index: u32) -> Vec<u8> {
    index.to_be_bytes().to_vec()
}

fn value(index: u32, len: usize) -> Vec<u8> {
    (0..len)
        .map(|offset| (index as usize + offset) as u8)
        .collect()
}

/// Persist `rows` rows, then reopen with an empty page cache.
async fn cold_tree(rows: u32, value_len: usize) -> (CountingStore, IdbTree<CountingStore>) {
    let store = CountingStore::default();
    let tree = IdbTree::open(store.clone(), SMALL_PAGES).await.unwrap();
    for index in 0..rows {
        tree.put(key(index), value(index, value_len)).await.unwrap();
        if index % 500 == 0 {
            tree.flush().await.unwrap();
        }
    }
    tree.flush().await.unwrap();
    drop(tree);
    let tree = IdbTree::open(store.clone(), SMALL_PAGES).await.unwrap();
    store.reset();
    (store, tree)
}

fn expected(range: std::ops::Range<u32>, value_len: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    range
        .map(|index| (key(index), value(index, value_len)))
        .collect()
}

#[test]
fn cold_unbounded_scan_reads_each_tree_level_in_one_round_trip() {
    block_on(async {
        let (store, tree) = cold_tree(4_000, 40).await;

        let rows = tree.range(&key(0), &key(u32::MAX)).await.unwrap();
        assert_eq!(rows, expected(0..4_000, 40));
        let cold_pages = store.pages_read.get();
        assert!(
            cold_pages > 100,
            "fixture should span many pages: {cold_pages}"
        );
        // One round trip per tree level. A per-page restart would need one
        // round trip for every page read.
        assert!(
            store.round_trips.get() <= 5,
            "{} round trips for {cold_pages} pages",
            store.round_trips.get()
        );

        store.reset();
        assert_eq!(tree.range(&key(0), &key(u32::MAX)).await.unwrap(), rows);
        assert_eq!(store.round_trips.get(), 0, "a warm scan reads nothing");
    });
}

#[test]
fn cold_unbounded_reverse_scan_matches_forward_and_batches_reads() {
    block_on(async {
        let (store, tree) = cold_tree(4_000, 40).await;

        let rows = tree
            .range_reverse(&key(1_000), &key(3_000), usize::MAX)
            .await
            .unwrap();
        let mut forward = expected(1_000..3_000, 40);
        forward.reverse();
        assert_eq!(rows, forward);
        assert!(
            store.round_trips.get() <= 5,
            "{} round trips for {} pages",
            store.round_trips.get(),
            store.pages_read.get()
        );
    });
}

#[test]
fn cold_scan_batches_overflow_chains_across_rows() {
    block_on(async {
        // Each value spans a chain of several overflow pages.
        let (store, tree) = cold_tree(200, 3_000).await;

        let rows = tree.range(&key(0), &key(u32::MAX)).await.unwrap();
        assert_eq!(rows, expected(0..200, 3_000));
        // Chains are followed one hop per round trip, but every row's chain
        // advances in the same batch rather than one row at a time.
        assert!(
            store.round_trips.get() <= 12,
            "{} round trips for {} pages",
            store.round_trips.get(),
            store.pages_read.get()
        );
    });
}

#[test]
fn cold_bounded_scans_do_not_hydrate_pages_past_their_limit() {
    block_on(async {
        let (store, tree) = cold_tree(4_000, 40).await;
        let height_bound = 5;

        let first = tree.range_limit(&key(0), &key(u32::MAX), 3).await.unwrap();
        assert_eq!(first, expected(0..3, 40));
        assert!(
            store.pages_read.get() <= height_bound,
            "bounded forward scan read {} pages",
            store.pages_read.get()
        );

        store.reset();
        let last = tree
            .range_reverse(&key(0), &key(u32::MAX), 3)
            .await
            .unwrap();
        let mut tail = expected(3_997..4_000, 40);
        tail.reverse();
        assert_eq!(last, tail);
        assert!(
            store.pages_read.get() <= height_bound,
            "bounded reverse scan read {} pages",
            store.pages_read.get()
        );
    });
}

#[test]
fn cold_scan_sees_writes_staged_before_the_scan() {
    block_on(async {
        let (store, tree) = cold_tree(2_000, 40).await;
        tree.delete(&key(10)).await.unwrap();
        tree.put(key(5_000), value(5_000, 40)).await.unwrap();
        store.reset();

        let rows = tree.range(&key(0), &key(u32::MAX)).await.unwrap();
        let mut want = expected(0..2_000, 40);
        want.remove(10);
        want.push((key(5_000), value(5_000, 40)));
        assert_eq!(rows, want);
        assert!(store.round_trips.get() <= 5);
    });
}
