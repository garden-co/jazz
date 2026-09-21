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
