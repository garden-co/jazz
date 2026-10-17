//! Batched writes against the public `IdbTree` API, checked against an
//! in-memory model through flushes and cold reopens.
//!
//! These stay at the `IdbTree` + `PageStore` boundary: how many pages a batch
//! stages, and rollback of a failed batch, are not observable through any
//! Jazz client API.

use futures::executor::block_on;
use idb_tree::{
    BoxFuture, Commit, Error, IdbTree, MemoryPageStore, Metadata, Options, PageStore,
    WriteOperation,
};
use std::{cell::Cell, collections::BTreeMap, rc::Rc};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

const SMALL_PAGES: Options = Options { page_size: 1024 };

/// Deterministic xorshift so failures replay exactly.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

fn random_key(rng: &mut Rng) -> Vec<u8> {
    let mut key = rng.below(3_000).to_be_bytes().to_vec();
    // A few wide keys skew both leaves and internal separators.
    if rng.below(10) == 0 {
        key.resize(120, b'k');
    }
    key
}

fn random_value(rng: &mut Rng) -> Vec<u8> {
    let len = match rng.below(20) {
        0 => 700 + rng.below(3_000) as usize, // overflow chain at 1 KiB pages
        1..=3 => 100 + rng.below(150) as usize,
        _ => rng.below(24) as usize,
    };
    let byte = rng.next() as u8;
    vec![byte; len]
}

fn random_batch(rng: &mut Rng, model: &mut Model, len: usize) -> Vec<WriteOperation> {
    (0..len)
        .map(|_| {
            let key = random_key(rng);
            if rng.below(4) == 0 {
                model.remove(&key);
                WriteOperation::Delete { key }
            } else {
                let value = random_value(rng);
                model.insert(key.clone(), value.clone());
                WriteOperation::Set { key, value }
            }
        })
        .collect()
}

async fn assert_matches<S: PageStore + Clone>(tree: &IdbTree<S>, model: &Model) {
    let expected: Vec<_> = model.clone().into_iter().collect();
    assert_eq!(tree.range(&[], &[0xff; 8]).await.unwrap(), expected);
    let mut reversed = expected.clone();
    reversed.reverse();
    assert_eq!(
        tree.range_reverse(&[], &[0xff; 8], usize::MAX)
            .await
            .unwrap(),
        reversed
    );
    for (key, value) in model.iter().step_by(37) {
        assert_eq!(tree.get(key).await.unwrap().as_ref(), Some(value));
    }
}

#[test]
fn batched_writes_match_a_model_through_flushes_and_reopens() {
    block_on(async {
        let store = MemoryPageStore::default();
        let mut tree = IdbTree::open(store.clone(), SMALL_PAGES).await.unwrap();
        let mut model = Model::new();
        let mut rng = Rng(0x5eed_1dbb_7ee0_0001);
        for round in 0..12 {
            let batch = random_batch(&mut rng, &mut model, 50 + 250 * (round % 4));
            tree.write_many(batch).await.unwrap();
            assert_matches(&tree, &model).await;
            if round % 3 == 2 {
                tree.put(b"single".to_vec(), vec![round as u8; 5])
                    .await
                    .unwrap();
                model.insert(b"single".to_vec(), vec![round as u8; 5]);
            }
            tree.flush().await.unwrap();
            if round % 2 == 1 {
                drop(tree);
                tree = IdbTree::open(store.clone(), SMALL_PAGES).await.unwrap();
            }
            assert_matches(&tree, &model).await;
        }
    });
}

#[test]
fn a_bulk_batch_stages_each_page_once_rather_than_once_per_key() {
    block_on(async {
        let store = MemoryPageStore::default();
        let tree = IdbTree::open(store.clone(), Options::default())
            .await
            .unwrap();
        let rows = 20_000u32;
        tree.write_many(
            (0..rows)
                .map(|index| WriteOperation::Set {
                    key: index.to_be_bytes().to_vec(),
                    value: vec![7; 100],
                })
                .collect(),
        )
        .await
        .unwrap();
        // About 145 leaves hold these rows. Copying the root-to-leaf path for
        // every key would stage tens of thousands of superseded pages.
        let staged = tree.dirty_page_count();
        assert!(staged < 400, "staged {staged} pages for {rows} rows");

        tree.flush().await.unwrap();
        drop(tree);
        let reopened = IdbTree::open(store, Options::default()).await.unwrap();
        let expected: Vec<_> = (0..rows)
            .map(|index| (index.to_be_bytes().to_vec(), vec![7; 100]))
            .collect();
        assert_eq!(reopened.range(&[], &[0xff; 5]).await.unwrap(), expected);
    });
}

#[test]
fn a_failed_batch_discards_its_in_place_edits() {
    block_on(async {
        let store = MemoryPageStore::default();
        let tree = IdbTree::open(store.clone(), SMALL_PAGES).await.unwrap();
        let mut model = Model::new();
        let mut rng = Rng(0x5eed_1dbb_7ee0_0002);
        let batch = random_batch(&mut rng, &mut model, 800);
        tree.write_many(batch).await.unwrap();
        tree.flush().await.unwrap();
        let durable = tree.metadata();

        // Enough writes to edit fresh leaves and parents in place and split
        // them, followed by an operation that cannot fit any page.
        let mut doomed = random_batch(&mut rng, &mut model.clone(), 600);
        doomed.push(WriteOperation::Set {
            key: vec![b'z'; 1_100],
            value: vec![1],
        });
        assert!(matches!(
            tree.write_many(doomed).await,
            Err(Error::PageTooLarge { .. })
        ));
        assert_eq!(tree.metadata(), durable);
        assert_eq!(tree.dirty_page_count(), 0);
        assert_matches(&tree, &model).await;

        // The tree remains writable after the rollback.
        let batch = random_batch(&mut rng, &mut model, 300);
        tree.write_many(batch).await.unwrap();
        tree.flush().await.unwrap();
        drop(tree);
        let reopened = IdbTree::open(store, SMALL_PAGES).await.unwrap();
        assert_matches(&reopened, &model).await;
    });
}

#[derive(Clone, Default)]
struct FlakyStore {
    memory: MemoryPageStore,
    fail_next_commit: Rc<Cell<bool>>,
}

impl PageStore for FlakyStore {
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
        self.memory.load_metadata()
    }

    fn read_page(&self, page_id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
        self.memory.read_page(page_id)
    }

    fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
        if self.fail_next_commit.replace(false) {
            return Box::pin(async { Err("disk unavailable".to_owned()) });
        }
        self.memory.commit(commit)
    }
}

#[test]
fn a_batch_after_a_failed_commit_is_durable_with_the_retried_generation() {
    block_on(async {
        let store = FlakyStore::default();
        let tree = IdbTree::open(store.clone(), SMALL_PAGES).await.unwrap();
        let mut model = Model::new();
        let mut rng = Rng(0x5eed_1dbb_7ee0_0003);
        let batch = random_batch(&mut rng, &mut model, 500);
        tree.write_many(batch).await.unwrap();

        store.fail_next_commit.set(true);
        assert!(tree.flush().await.is_err());

        // The failed generation's pages are dirty again but predate this
        // batch, so they must be copied rather than edited in place.
        let batch = random_batch(&mut rng, &mut model, 500);
        tree.write_many(batch).await.unwrap();
        assert_matches(&tree, &model).await;
        tree.flush().await.unwrap();
        drop(tree);
        let reopened = IdbTree::open(store, SMALL_PAGES).await.unwrap();
        assert_matches(&reopened, &model).await;
    });
}
