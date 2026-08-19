use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::noop_waker;
use groove::db::{Database, GraphBuilder};
use groove::ivm::ProjectField;
use groove::records::{RecordDescriptor, Value};
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
    let before = database.runtime_stats();

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

    let after = database.runtime_stats();
    assert_eq!(after.graph_nodes, before.graph_nodes);
    assert_eq!(after.active_subscriptions, before.active_subscriptions);
    control.resume();
    let subscription =
        block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 1);
}

#[test]
fn cancelled_prepared_bind_discards_binding_tick_and_subscription_state() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    block_on(database.commit_batch(batch)).unwrap();

    let binding = RecordDescriptor::new([("id", ColumnType::U64)]);
    let graph = GraphBuilder::join(
        GraphBuilder::binding_source("album_by_id", binding),
        GraphBuilder::table("albums"),
        ["id"],
        ["id"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
    ]);
    let shape = block_on(database.prepare_one_sink(graph, "album_by_id", binding, ["id"])).unwrap();

    control.take_observed();
    control.pause();
    let mut install = Box::pin(database.bind_shape_one_sink(shape.id(), &[Value::U64(1)]));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut install).poll(&mut context),
        Poll::Pending
    ));
    drop(install);

    assert_eq!(database.runtime_stats().active_subscriptions, 0);
    control.resume();
    let subscription =
        block_on(database.bind_shape_one_sink(shape.id(), &[Value::U64(1)])).unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 1);
}

#[test]
fn cancelled_one_shot_query_discards_ephemeral_graph_and_hydration_state() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    block_on(database.commit_batch(batch)).unwrap();
    let before = database.runtime_stats();

    control.take_observed();
    control.pause();
    let mut query = Box::pin(database.query_graph(GraphBuilder::table("albums")));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut query).poll(&mut context),
        Poll::Pending
    ));
    drop(query);

    let after = database.runtime_stats();
    assert_eq!(after.graph_nodes, before.graph_nodes);
    assert_eq!(after.active_subscriptions, before.active_subscriptions);
    control.resume();
    let records = block_on(database.query_graph(GraphBuilder::table("albums"))).unwrap();
    assert_eq!(records.deltas.len(), 1);
}
