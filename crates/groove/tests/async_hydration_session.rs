use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::noop_waker;
use groove::db::{Database, GraphBuilder};
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{TestStorage, TestStorageOperation};

fn schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

#[test]
fn cancelled_hydration_publishes_no_subscription_or_partial_session() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    block_on(database.commit_batch(batch)).unwrap();

    control.take_observed();
    control.pause();
    let mut install = Box::pin(database.subscribe_one_sink(GraphBuilder::table("albums")));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut install).poll(&mut context),
        Poll::Pending
    ));
    assert!(control.observed().contains(&TestStorageOperation::ScanOpen));
    drop(install);

    assert_eq!(database.runtime_stats().active_subscriptions, 0);
    control.resume();
    let subscription =
        block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 1);
}
