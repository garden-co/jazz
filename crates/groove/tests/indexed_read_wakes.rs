#![cfg(feature = "test")]
//! Public indexed subscriptions must resume from storage readiness. Row equality
//! alone cannot detect a loop re-polling every parked storage future, so this
//! integration test also counts polls at the controlled storage boundary.
use futures::{executor::block_on, task::noop_waker};
use groove::{
    db::{Database, Error as DatabaseError, GraphBuilder},
    ivm::{LiteralValue, ProjectField, StaticScanSpec},
    records::{RecordDescriptor, Value, ValueType, VariantRecord},
    schema::{
        ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey,
        TableSchema,
    },
    storage::{TestStorage, TestStorageControl, TestStorageOperation},
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

fn fixture() -> (Database, TestStorageControl) {
    let schema = DatabaseSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("group", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("items_by_group", ["group"]))
    .with_variant(1, ["group", "id"])]);
    let (storage, control) = TestStorage::controlled(&["items", "indices"]);
    let mut database = block_on(Database::new(schema, storage.clone())).unwrap();
    let physical = RecordDescriptor::new([("group", ValueType::String), ("id", ValueType::U64)]);
    database
        .define_variant_projection(
            "items",
            "logical-item",
            RecordDescriptor::new([("id", ValueType::U64), ("group", ValueType::String)]),
        )
        .unwrap();
    database
        .register_variant_projection_case(
            "items",
            "logical-item",
            1,
            [ProjectField::named("id"), ProjectField::named("group")],
        )
        .unwrap();
    let mut batch = database.open_batch();
    for id in 0..64 {
        batch.insert(
            "items",
            VariantRecord::create(
                1,
                physical,
                &[Value::String("shared".into()), Value::U64(id)],
            )
            .unwrap(),
        );
    }
    block_on(database.commit_batch(batch)).unwrap();
    storage.evict_all();
    control.take_observed();
    control.pause_on(TestStorageOperation::Get);
    (database, control)
}

fn indexed_items() -> GraphBuilder {
    GraphBuilder::variant_index_scan(
        "items",
        "items_by_group",
        "logical-item",
        StaticScanSpec::Prefix(vec![LiteralValue::String("shared".into())]),
    )
}

fn park_reads(mut progress: Pin<&mut impl Future>, control: &TestStorageControl) {
    let before = control.poll_count(TestStorageOperation::Get);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..160 {
        assert!(progress.as_mut().poll(&mut cx).is_pending());
        if control.poll_count(TestStorageOperation::Get) >= before + 128 {
            break;
        }
    }
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::Get)
            .count(),
        64,
        "all indexed row reads must start before any is allowed to finish"
    );
    for _ in 0..2 {
        assert!(progress.as_mut().poll(&mut cx).is_pending());
    }
}

#[test]
fn parked_indexed_rows_are_polled_only_after_a_storage_wake() {
    let (mut database, control) = fixture();
    let subscription = block_on(database.subscribe_one_sink(indexed_items())).unwrap();
    let mut progress = Box::pin(database.drive_progress());
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    park_reads(progress.as_mut(), &control);
    let parked = control.poll_count(TestStorageOperation::Get);
    for _ in 0..16 {
        assert!(progress.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(
        control.poll_count(TestStorageOperation::Get),
        parked,
        "unrelated polls must not revisit parked rows"
    );
    control.resume_operation(TestStorageOperation::Get);
    block_on(progress).unwrap();
    let rows = subscription.recv().unwrap();
    assert_eq!(
        rows.to_values().unwrap(),
        (0..64)
            .map(|id| (vec![Value::U64(id), Value::String("shared".into())], 1))
            .collect::<Vec<_>>()
    );
}

#[test]
fn indexed_row_failure_does_not_wait_for_the_other_parked_rows() {
    let (mut database, control) = fixture();
    let subscription = block_on(database.subscribe_one_sink(indexed_items())).unwrap();
    let mut delivery = Box::pin(database.next_subscription(&subscription));
    park_reads(delivery.as_mut(), &control);
    control.fail_next(TestStorageOperation::Get);
    control.release_one();
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..160 {
        if let Poll::Ready(result) = delivery.as_mut().poll(&mut cx) {
            assert!(matches!(result, Err(DatabaseError::SubscriptionFailed(_))));
            return;
        }
    }
    panic!("a ready indexed-row error was blocked behind unrelated parked reads");
}

#[test]
fn cancelling_indexed_hydration_releases_all_parked_rows() {
    let (mut database, control) = fixture();
    let before = database.runtime_stats();
    let subscription = block_on(database.subscribe_one_sink(indexed_items())).unwrap();
    let mut progress = Box::pin(database.drive_progress());
    park_reads(progress.as_mut(), &control);
    drop(progress);
    assert!(database.unsubscribe(subscription.id()));
    drop(subscription);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut progress = Box::pin(database.drive_progress());
    assert!(matches!(
        progress.as_mut().poll(&mut cx),
        Poll::Ready(Ok(()))
    ));
    drop(progress);
    assert_eq!(
        database.runtime_stats().active_subscriptions,
        before.active_subscriptions
    );
    assert_eq!(database.runtime_stats().graph_nodes, before.graph_nodes);
}
