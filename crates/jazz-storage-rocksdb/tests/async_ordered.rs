use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use groove::storage::OwnedWriteOperation;
use groove::storage::async_ordered::{
    ImmediateStorage, OrderedKvStorage, OwnedStorageOperation, OwnedStorageRequest,
    OwnedStorageResponse,
};
use jazz_storage_rocksdb::RocksDbStorage;

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn poll_request(
    storage: &mut dyn OrderedKvStorage,
    request: &OwnedStorageRequest,
) -> Poll<Result<OwnedStorageResponse, groove::storage::Error>> {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    storage.poll_request(request, &mut context)
}

#[test]
fn rocksdb_inherits_first_poll_commit_readiness() {
    let directory = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(directory.path(), &["rows"]).unwrap();
    let mut storage = ImmediateStorage::new(storage);

    assert!(matches!(
        poll_request(
            &mut storage,
            &OwnedStorageRequest::new(OwnedStorageOperation::Commit(vec![
                OwnedWriteOperation::Set {
                    cf: "rows".to_owned(),
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                }
            ]))
        ),
        Poll::Ready(Ok(OwnedStorageResponse::Committed))
    ));
    let request = OwnedStorageRequest::new(OwnedStorageOperation::Get {
        column_family: "rows".to_owned(),
        key: b"key".to_vec(),
    });
    let Poll::Ready(Ok(OwnedStorageResponse::Value(value))) = poll_request(&mut storage, &request)
    else {
        panic!("RocksDB get must complete successfully on its first poll");
    };
    assert_eq!(value, Some(b"value".to_vec()));

    assert!(matches!(
        poll_request(
            &mut storage,
            &OwnedStorageRequest::new(OwnedStorageOperation::EnsureColumnFamilies(vec![
                "rows".to_owned(),
                "later_rows".to_owned(),
            ])),
        ),
        Poll::Ready(Ok(OwnedStorageResponse::ColumnFamiliesReady))
    ));
    assert!(matches!(
        poll_request(
            &mut storage,
            &OwnedStorageRequest::new(OwnedStorageOperation::Commit(vec![
                OwnedWriteOperation::Set {
                    cf: "later_rows".to_owned(),
                    key: b"later".to_vec(),
                    value: b"ready".to_vec(),
                },
            ])),
        ),
        Poll::Ready(Ok(OwnedStorageResponse::Committed))
    ));
}
