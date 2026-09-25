use groove::storage::{
    Error, KeyValue, MemoryStorage, OrderedKvStorage, ScanRequest, StorageTransaction, collect_scan,
};

fn key(id: u16) -> Vec<u8> {
    format!("a/{id:04}").into_bytes()
}

fn row(id: u16, value: u16) -> KeyValue {
    (key(id), value.to_be_bytes().to_vec())
}

async fn seeded_storage(count: u16) -> MemoryStorage {
    let storage = MemoryStorage::new(&["rows", "other"]).unwrap();
    for id in 0..count {
        storage
            .set("rows".into(), key(id), id.to_be_bytes().to_vec())
            .await
            .unwrap();
    }
    storage
}

#[futures_test::test]
async fn empty_in_range_overlays_preserve_bounds_order_limits_and_missing_family_errors() {
    let storage = seeded_storage(600).await;
    for unrelated_writes in [false, true] {
        let transaction = StorageTransaction::new(&storage);
        if unrelated_writes {
            transaction
                .set("rows".into(), b"z/0000".to_vec(), vec![1])
                .await
                .unwrap();
            transaction.delete("other".into(), key(100)).await.unwrap();
        }
        let cases = [
            (ScanRequest::prefix("rows".into(), b"a/".to_vec()), 0..600),
            (
                ScanRequest::range("rows".into(), key(100), key(500)),
                100..500,
            ),
            (ScanRequest::range("rows".into(), key(200), key(200)), 0..0),
            (ScanRequest::range("rows".into(), key(400), key(200)), 0..0),
            (
                ScanRequest::prefix("rows".into(), b"missing".to_vec()),
                0..0,
            ),
        ];
        for (request, ids) in cases {
            for reverse in [false, true] {
                for limit in [None, Some(0), Some(1), Some(257), Some(700)] {
                    let mut request = request.clone();
                    let mut expected = ids.clone().map(|id| row(id, id)).collect::<Vec<_>>();
                    if reverse {
                        request = request.reversed();
                        expected.reverse();
                    }
                    if let Some(limit) = limit {
                        request = request.with_max_items(limit);
                        expected.truncate(limit);
                    }
                    let actual = collect_scan(transaction.scan(request).await.unwrap())
                        .await
                        .unwrap();
                    assert_eq!(actual, expected);
                }
            }
        }
        for limit in [0, 1] {
            assert!(matches!(
                transaction
                    .scan(ScanRequest::prefix("missing".into(), vec![]).with_max_items(limit))
                    .await,
                Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
            ));
        }
    }
}

#[futures_test::test]
async fn transaction_scan_keeps_its_overlay_snapshot_from_before_first_poll() {
    let storage = seeded_storage(4).await;
    for initially_nonempty in [false, true] {
        let transaction = StorageTransaction::new(&storage);
        if initially_nonempty {
            transaction
                .set("rows".into(), key(1), 99_u16.to_be_bytes().to_vec())
                .await
                .unwrap();
        }
        let request = ScanRequest::prefix("rows".into(), b"a/".to_vec());
        let pending_scan = transaction.scan(request.clone());
        transaction
            .set("rows".into(), key(1), 101_u16.to_be_bytes().to_vec())
            .await
            .unwrap();
        transaction.delete("rows".into(), key(2)).await.unwrap();
        transaction
            .set("rows".into(), key(4), 4_u16.to_be_bytes().to_vec())
            .await
            .unwrap();
        let cursor = pending_scan.await.unwrap();
        transaction
            .set("rows".into(), key(0), 200_u16.to_be_bytes().to_vec())
            .await
            .unwrap();
        assert_eq!(
            collect_scan(cursor).await.unwrap(),
            vec![
                row(0, 0),
                row(1, if initially_nonempty { 99 } else { 1 }),
                row(2, 2),
                row(3, 3)
            ]
        );
        assert_eq!(
            collect_scan(transaction.scan(request).await.unwrap())
                .await
                .unwrap(),
            vec![row(0, 200), row(1, 101), row(3, 3), row(4, 4)]
        );
    }
}

#[futures_test::test]
async fn nested_transaction_opens_its_base_when_the_outer_scan_is_polled() {
    let storage = seeded_storage(3).await;
    let inner = StorageTransaction::new(&storage);
    let outer = StorageTransaction::new(&inner);
    let request = ScanRequest::prefix("rows".into(), b"a/".to_vec());
    let pending_scan = outer.scan(request.clone());
    inner
        .set("rows".into(), key(1), 101_u16.to_be_bytes().to_vec())
        .await
        .unwrap();
    outer
        .set("rows".into(), key(2), 102_u16.to_be_bytes().to_vec())
        .await
        .unwrap();
    assert_eq!(
        collect_scan(pending_scan.await.unwrap()).await.unwrap(),
        vec![row(0, 0), row(1, 101), row(2, 2)]
    );
    assert_eq!(
        collect_scan(outer.scan(request).await.unwrap())
            .await
            .unwrap(),
        vec![row(0, 0), row(1, 101), row(2, 102)]
    );
}

#[cfg(feature = "test")]
#[test]
fn suspended_and_cancelled_empty_overlay_scans_preserve_read_your_writes() {
    use futures::{executor::block_on, task::noop_waker};
    use groove::storage::{TestStorage, TestStorageOperation};
    use std::task::Context;

    let (storage, control) = TestStorage::controlled(&["rows"]);
    block_on(storage.set("rows".into(), key(0), 0_u16.to_be_bytes().to_vec())).unwrap();
    let transaction = StorageTransaction::new(&storage);
    let request = ScanRequest::prefix("rows".into(), b"a/".to_vec());
    control.pause_on(TestStorageOperation::ScanOpen);
    let mut scan = transaction.scan(request.clone());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(scan.as_mut().poll(&mut context).is_pending());
    drop(scan);

    let mut scan = transaction.scan(request.clone());
    assert!(scan.as_mut().poll(&mut context).is_pending());
    block_on(transaction.set("rows".into(), key(1), 1_u16.to_be_bytes().to_vec())).unwrap();
    control.resume_operation(TestStorageOperation::ScanOpen);
    let cursor = block_on(scan).unwrap();
    assert_eq!(block_on(collect_scan(cursor)).unwrap(), vec![row(0, 0)]);
    assert_eq!(
        block_on(async { collect_scan(transaction.scan(request).await.unwrap()).await }).unwrap(),
        vec![row(0, 0), row(1, 1)]
    );
}
