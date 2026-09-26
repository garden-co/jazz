//! Storage-level checks are needed because query results cannot expose class
//! namespace forwarding or transaction-local encoded read-your-writes behavior.
use futures::executor::block_on;
use groove::storage::{IdbStorage, LayoutStorage, OrderedKvStorage, StorageLayout};
use idb_tree::MemoryPageStore;

#[test]
fn required_batches_keep_class_namespaces_and_transaction_local_values() {
    block_on(async {
        let first = "jazz_alpha_global_current";
        let second = "jazz_beta_global_current";
        let layout = StorageLayout::jazz_class_v1();
        let families = layout.physical_column_families([first, second]);
        let names: Vec<_> = families.iter().map(String::as_str).collect();
        let backend = IdbStorage::open(MemoryPageStore::default(), &names)
            .await
            .unwrap();
        let storage = LayoutStorage::new(backend, layout).await.unwrap();
        storage.set(first.into(), vec![0], vec![10]).await.unwrap();
        storage.set(first.into(), vec![1], vec![11]).await.unwrap();
        storage.set(second.into(), vec![0], vec![20]).await.unwrap();
        assert_eq!(
            storage
                .get_many_required(first.into(), vec![vec![1], vec![0], vec![1]])
                .await
                .unwrap(),
            Some(vec![vec![11], vec![10], vec![11]])
        );
        assert_eq!(
            storage
                .get_many_required(second.into(), vec![vec![0]])
                .await
                .unwrap(),
            Some(vec![vec![20]])
        );
        let transaction = storage.begin_txn();
        transaction
            .set(first.into(), vec![0], vec![30])
            .await
            .unwrap();
        transaction
            .set(first.into(), vec![2], vec![32])
            .await
            .unwrap();
        transaction.delete(first.into(), vec![1]).await.unwrap();
        assert_eq!(
            transaction
                .get_many_required(first.into(), vec![vec![2], vec![0], vec![2]])
                .await
                .unwrap(),
            Some(vec![vec![32], vec![30], vec![32]])
        );
        assert_eq!(
            transaction
                .get_many_required(first.into(), vec![vec![0], vec![1]])
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            transaction
                .get_many_required(first.into(), vec![vec![0], vec![9]])
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            transaction
                .get_many_required(second.into(), vec![vec![0]])
                .await
                .unwrap(),
            Some(vec![vec![20]])
        );
        assert_eq!(
            storage
                .get_many_required(first.into(), vec![vec![0], vec![1]])
                .await
                .unwrap(),
            Some(vec![vec![10], vec![11]])
        );
        // A future begins reading when polled, after intervening staged edits.
        let pending = transaction.get_many_required(first.into(), vec![vec![0]]);
        transaction
            .set(first.into(), vec![0], vec![40])
            .await
            .unwrap();
        assert_eq!(pending.await.unwrap(), Some(vec![vec![40]]));
        transaction.commit().await.unwrap();
        assert_eq!(
            storage
                .get_many_required(first.into(), vec![vec![2], vec![0]])
                .await
                .unwrap(),
            Some(vec![vec![32], vec![40]])
        );
        assert_eq!(
            storage
                .get_many_required(first.into(), vec![vec![1]])
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            storage
                .get_many_required(second.into(), vec![vec![0]])
                .await
                .unwrap(),
            Some(vec![vec![20]])
        );
        assert_eq!(
            storage
                .get_many_required("no-such-family".into(), vec![])
                .await
                .unwrap(),
            Some(vec![])
        );
    });
}
