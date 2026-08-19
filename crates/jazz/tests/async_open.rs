use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::noop_waker;
use jazz::db::{Db, DbConfig, DbIdentity};
use jazz::groove::schema::ColumnType;
use jazz::groove::storage::{TestStorage, TestStorageOperation};
use jazz::ids::{AuthorId, NodeUuid};
use jazz::schema::{ColumnSchema, JazzSchema, Policy, TableSchema};

fn schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
    ])
}

fn config(storage: TestStorage) -> DbConfig<TestStorage> {
    DbConfig::new(
        schema(),
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x31; 16]),
            author: AuthorId::from_bytes([0x41; 16]),
        },
    )
}

#[test]
fn open_suspends_on_storage_and_can_be_cancelled_cleanly() {
    let schema = schema();
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&refs);

    control.pause();
    let mut open = Box::pin(Db::open(config(storage.clone())));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut open).poll(&mut context),
        Poll::Pending
    ));
    assert!(
        control.observed().iter().any(|operation| matches!(
            operation,
            TestStorageOperation::ScanOpen | TestStorageOperation::Get
        )),
        "opening must reach the actual asynchronous storage path"
    );

    drop(open);
    control.resume();
    block_on(Db::open(config(storage))).expect("cancelled cold open remains recoverable");
}
