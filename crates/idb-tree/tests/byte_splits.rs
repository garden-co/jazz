use std::collections::BTreeMap;

use idb_tree::{IdbTree, MemoryPageStore, Options, WriteOperation};

#[test]
fn skewed_inline_values_split_and_reopen_in_both_directions() {
    futures::executor::block_on(async {
        for reverse in [false, true] {
            let store = MemoryPageStore::default();
            let options = Options::default();
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            let mut expected = BTreeMap::new();
            for ordinal in 0u32..320 {
                let key = if reverse { 320 - ordinal } else { ordinal }
                    .to_be_bytes()
                    .to_vec();
                let value = vec![ordinal as u8; if ordinal < 300 { 1 } else { 4000 }];
                tree.put(key.clone(), value.clone()).await.unwrap();
                expected.insert(key, value);
            }
            tree.flush().await.unwrap();
            drop(tree);
            let reopened = IdbTree::open(store, options).await.unwrap();
            let expected = expected.into_iter().collect::<Vec<_>>();
            assert_eq!(reopened.range(&[], &[255; 5]).await.unwrap(), expected);
            for (key, value) in &expected {
                assert_eq!(reopened.get(key).await.unwrap().as_ref(), Some(value));
            }
            assert_eq!(
                reopened.range_reverse(&[], &[255; 5], 320).await.unwrap(),
                expected.into_iter().rev().collect::<Vec<_>>()
            );
        }
    });
}

#[test]
fn skewed_key_sizes_and_value_replacements_survive_multiple_levels() {
    futures::executor::block_on(async {
        for reverse in [false, true] {
            let store = MemoryPageStore::default();
            let options = Options { page_size: 1024 };
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            let mut expected = BTreeMap::new();
            let mut operations = Vec::new();
            for ordinal in 0u32..1200 {
                let rank = if reverse { 1199 - ordinal } else { ordinal };
                let mut key = rank.to_be_bytes().to_vec();
                // Clusters of large keys skew both leaves and parent separators.
                key.resize(if rank % 100 < 70 { 5 } else { 220 }, b'k');
                let value = vec![(rank % 251) as u8; 1];
                expected.insert(key.clone(), value.clone());
                operations.push(WriteOperation::Set { key, value });
            }
            tree.write_many(operations).await.unwrap();
            tree.flush().await.unwrap();
            for (index, (key, value)) in expected.iter_mut().enumerate() {
                if index % 7 == 0 {
                    *value = vec![42; 250];
                    tree.put(key.clone(), value.clone()).await.unwrap();
                }
            }
            tree.flush().await.unwrap();
            drop(tree);
            let reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(
                reopened.range(&[], &[255; 5]).await.unwrap(),
                expected.into_iter().collect::<Vec<_>>()
            );
        }
    });
}

#[test]
fn wide_separators_fit_after_skewed_parent_split() {
    futures::executor::block_on(async {
        for reverse in [false, true] {
            let store = MemoryPageStore::default();
            let options = Options { page_size: 1024 };
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            let mut expected = BTreeMap::new();
            let mut writes = Vec::new();
            for ordinal in 0u32..2804 {
                let rank = if reverse { 2803 - ordinal } else { ordinal };
                let mut key = rank.to_be_bytes().to_vec();
                key.resize(if rank < 2800 { 5 } else { 900 }, b'k');
                let value = vec![(rank % 251) as u8];
                expected.insert(key.clone(), value.clone());
                writes.push(WriteOperation::Set { key, value });
            }
            // Keeping only the leaf fix is insufficient for this fixture:
            // count-based parent splitting still produces an oversized page.
            tree.write_many(writes).await.unwrap();
            tree.flush().await.unwrap();
            drop(tree);
            let reopened = IdbTree::open(store, options).await.unwrap();
            assert_eq!(
                reopened.range(&[], &[255; 5]).await.unwrap(),
                expected.into_iter().collect::<Vec<_>>()
            );
        }
    });
}

/// A leaf created earlier in one batch must choose a split whose promoted
/// separator fits the new root, just like a previously persisted leaf.
#[test]
fn fresh_leaf_split_uses_a_separator_that_fits_the_new_root() {
    futures::executor::block_on(async {
        let store = MemoryPageStore::default();
        let options = Options { page_size: 1024 };
        let tree = IdbTree::open(store.clone(), options).await.unwrap();
        let first = vec![0];
        let middle = vec![1; 980];
        let last = vec![2];
        // Insert the wide middle key last: the two narrow entries establish a
        // fresh leaf first. Both leaf partitions fit; only the narrow separator
        // can be promoted into the root.
        tree.write_many(vec![
            WriteOperation::Set {
                key: first.clone(),
                value: vec![10],
            },
            WriteOperation::Set {
                key: last.clone(),
                value: vec![30],
            },
            WriteOperation::Set {
                key: middle.clone(),
                value: vec![20],
            },
        ])
        .await
        .unwrap();
        tree.flush().await.unwrap();
        drop(tree);
        let reopened = IdbTree::open(store, options).await.unwrap();
        assert_eq!(
            reopened.range(&[], &[255]).await.unwrap(),
            vec![(first, vec![10]), (middle, vec![20]), (last, vec![30])],
        );
    });
}
