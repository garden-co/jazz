//! Public range results across split pages, cold opens, mutations and limits.
//!
//! These tests use the idb-tree public API because byte-key bounds, reverse
//! limits and page-size selection are storage contracts not exposed by a
//! Jazz query. No private page structure or traversal implementation is used.
use futures::executor::block_on;
use idb_tree::{IdbTree, KeyValue, MemoryPageStore, Options, WriteOperation};
use std::collections::BTreeMap;

fn key(index: u32) -> Vec<u8> {
    let mut key = b"records/".to_vec();
    key.extend_from_slice(&(index * 4).to_be_bytes());
    if index.is_multiple_of(5) {
        key.extend_from_slice(b"/suffix");
    }
    key
}

fn value(index: u32) -> Vec<u8> {
    vec![index as u8; if index.is_multiple_of(71) { 2400 } else { 24 }]
}

async fn seed() -> (MemoryPageStore, BTreeMap<Vec<u8>, Vec<u8>>) {
    let expected: BTreeMap<_, _> = (0..256).map(|index| (key(index), value(index))).collect();
    let store = MemoryPageStore::default();
    let tree = IdbTree::open(store.clone(), Options { page_size: 1024 })
        .await
        .unwrap();
    tree.write_many(
        expected
            .iter()
            .map(|(key, value)| WriteOperation::Set {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
    )
    .await
    .unwrap();
    tree.flush().await.unwrap();
    (store, expected)
}

async fn check_range(
    tree: &IdbTree<MemoryPageStore>,
    expected: &BTreeMap<Vec<u8>, Vec<u8>>,
    start: &[u8],
    end: &[u8],
) {
    // Filtering also defines the contract for reversed bounds without asking
    // BTreeMap::range to accept a range that its own API rejects.
    let rows: Vec<KeyValue> = expected
        .iter()
        .filter(|(key, _)| key.as_slice() >= start && key.as_slice() < end)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(tree.range(start, end).await.unwrap(), rows);
    for limit in [0, 1, 7, usize::MAX] {
        assert_eq!(
            tree.range_limit(start, end, limit).await.unwrap(),
            rows.iter().take(limit).cloned().collect::<Vec<_>>(),
            "forward: {start:?}..{end:?}, limit={limit}"
        );
        assert_eq!(
            tree.range_reverse(start, end, limit).await.unwrap(),
            rows.iter().rev().take(limit).cloned().collect::<Vec<_>>(),
            "reverse: {start:?}..{end:?}, limit={limit}"
        );
    }
}

#[test]
fn selective_ranges_match_ordered_map_at_every_stored_key_and_gap() {
    block_on(async {
        let (store, expected) = seed().await;
        for index in 0..256 {
            // Starting each case cold covers whichever boundaries became
            // separators as pages split; no private split positions are used.
            let tree = IdbTree::open(store.clone(), Options { page_size: 1024 })
                .await
                .unwrap();
            let start = key(index);
            let end = key(index + 1);
            check_range(&tree, &expected, &start, &end).await;
            let mut gap_start = start.clone();
            gap_start.push(1);
            let mut gap_end = start.clone();
            gap_end.push(2);
            check_range(&tree, &expected, &gap_start, &gap_end).await;
            check_range(&tree, &expected, &start, &start).await;
            check_range(&tree, &expected, &end, &start).await;
        }
    });
}

#[test]
fn range_limits_match_after_staged_deletes_updates_and_reopen() {
    block_on(async {
        let (store, mut expected) = seed().await;
        let tree = IdbTree::open(store.clone(), Options { page_size: 1024 })
            .await
            .unwrap();
        for index in (0..256).step_by(3) {
            tree.delete(&key(index)).await.unwrap();
            expected.remove(&key(index));
        }
        for index in [2, 70, 142, 256, 300] {
            let value = vec![42; 3200];
            tree.put(key(index), value.clone()).await.unwrap();
            expected.insert(key(index), value);
        }
        let ranges = [
            (vec![], vec![255]),
            (b"records/".to_vec(), b"records0".to_vec()),
            (key(0), key(1)),
            (key(63), key(178)),
            (key(255), key(301)),
            (key(301), key(302)),
            (key(178), key(63)),
        ];
        for (start, end) in &ranges {
            check_range(&tree, &expected, start, end).await;
        }
        tree.flush().await.unwrap();
        drop(tree);
        for (start, end) in &ranges {
            let reopened = IdbTree::open(store.clone(), Options { page_size: 1024 })
                .await
                .unwrap();
            // Exercise a reverse bounded call before a forward hydration.
            let reverse: Vec<_> = expected
                .iter()
                .rev()
                .filter(|(key, _)| {
                    key.as_slice() >= start.as_slice() && key.as_slice() < end.as_slice()
                })
                .take(7)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            assert_eq!(
                reopened.range_reverse(start, end, 7).await.unwrap(),
                reverse
            );
            check_range(&reopened, &expected, start, end).await;
        }
    });
}

#[test]
fn empty_tree_ranges_and_zero_limits_return_no_rows() {
    block_on(async {
        let tree = IdbTree::open(MemoryPageStore::default(), Options::default())
            .await
            .unwrap();
        check_range(&tree, &BTreeMap::new(), &[], &[255]).await;
        check_range(&tree, &BTreeMap::new(), &[255], &[]).await;
    });
}
