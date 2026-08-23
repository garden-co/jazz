#![cfg(feature = "test")]

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::noop_waker;
use groove::storage::{OrderedKvStorage, OwnedWriteOperation, TestStorage};

fn first_poll<F: Future>(future: &mut Pin<Box<F>>) -> Poll<F::Output> {
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    future.as_mut().poll(&mut context)
}

async fn collect_scan(
    mut scan: groove::storage::StorageScan<'_>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, groove::storage::Error> {
    let mut rows = Vec::new();
    while let Some(batch) = scan.next_batch().await? {
        rows.extend(batch);
    }
    Ok(rows)
}

#[test]
fn completed_reads_are_immediately_resident_until_evicted() {
    let storage = TestStorage::new(&["rows"]);
    block_on(storage.set("rows".into(), b"a/1".to_vec(), b"one".to_vec())).unwrap();
    storage.evict_all();

    let mut cold = Box::pin(storage.get("rows".into(), b"a/1".to_vec()));
    assert!(matches!(first_poll(&mut cold), Poll::Pending));
    assert_eq!(block_on(cold).unwrap(), Some(b"one".to_vec()));

    let mut resident = Box::pin(storage.get("rows".into(), b"a/1".to_vec()));
    assert!(matches!(first_poll(&mut resident), Poll::Ready(Ok(Some(value))) if value == b"one"));

    storage.evict_all();
    let mut evicted = Box::pin(storage.get("rows".into(), b"a/1".to_vec()));
    assert!(matches!(first_poll(&mut evicted), Poll::Pending));
}

#[test]
fn complete_scan_makes_present_and_absent_points_and_nested_scans_resident() {
    let storage = TestStorage::new(&["rows"]);
    block_on(storage.set("rows".into(), b"a/1".to_vec(), b"one".to_vec())).unwrap();
    block_on(storage.set("rows".into(), b"a/2".to_vec(), b"two".to_vec())).unwrap();
    storage.evict_all();

    let mut cold_scan = Box::pin(storage.scan(groove::storage::ScanRequest::prefix(
        "rows".into(),
        b"a/".to_vec(),
    )));
    assert!(matches!(first_poll(&mut cold_scan), Poll::Pending));
    let scan = block_on(cold_scan).unwrap();
    assert_eq!(block_on(collect_scan(scan)).unwrap().len(), 2);

    for (key, expected) in [
        (b"a/1".to_vec(), Some(b"one".to_vec())),
        (b"a/missing".to_vec(), None),
    ] {
        let mut read = Box::pin(storage.get("rows".into(), key));
        assert!(matches!(first_poll(&mut read), Poll::Ready(Ok(value)) if value == expected));
    }

    let mut nested = Box::pin(storage.scan(groove::storage::ScanRequest::prefix(
        "rows".into(),
        b"a/1".to_vec(),
    )));
    assert!(matches!(first_poll(&mut nested), Poll::Ready(Ok(_))));
}

#[test]
fn writes_keep_resident_points_coherent() {
    let storage = TestStorage::new(&["rows"]);
    block_on(storage.get("rows".into(), b"key".to_vec())).unwrap();

    block_on(storage.set("rows".into(), b"key".to_vec(), b"value".to_vec())).unwrap();
    let mut after_set = Box::pin(storage.get("rows".into(), b"key".to_vec()));
    assert!(
        matches!(first_poll(&mut after_set), Poll::Ready(Ok(Some(value))) if value == b"value")
    );

    block_on(storage.delete("rows".into(), b"key".to_vec())).unwrap();
    let mut after_delete = Box::pin(storage.get("rows".into(), b"key".to_vec()));
    assert!(matches!(
        first_poll(&mut after_delete),
        Poll::Ready(Ok(None))
    ));

    block_on(storage.write_many(vec![OwnedWriteOperation::Set {
        cf: "rows".into(),
        key: b"key".to_vec(),
        value: b"batched".to_vec(),
    }]))
    .unwrap();
    let mut after_batch = Box::pin(storage.get("rows".into(), b"key".to_vec()));
    assert!(
        matches!(first_poll(&mut after_batch), Poll::Ready(Ok(Some(value))) if value == b"batched")
    );
}
