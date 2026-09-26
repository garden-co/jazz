#![cfg(feature = "test")]

//! Queued subscriptions must share completed initial work and retain the
//! physical aggregate state needed for subsequent insertions and retractions.

use futures::executor::block_on;
use futures::task::noop_waker;
use groove::db::{Database, GraphBuilder, PrimaryKeyValue};
use groove::ivm::{AggregateExpr, AggregateFunction, PlanExpr};
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{TestStorage, TestStorageOperation};

fn aggregate() -> GraphBuilder {
    GraphBuilder::aggregate(
        GraphBuilder::table("measurements"),
        ["bucket"],
        [AggregateExpr {
            function: AggregateFunction::Sum,
            expression: Some(PlanExpr::Field("score".to_owned())),
            distinct: false,
            output_name: Some("total".to_owned()),
            output_identity: None,
        }],
    )
}

fn row(bucket: u64, total: u64) -> Vec<Value> {
    vec![
        Value::U64(bucket),
        Value::Nullable(Some(Box::new(Value::U64(total)))),
    ]
}

fn queued_aggregates(subscribers: usize) -> u64 {
    let schema = DatabaseSchema::new([TableSchema::new(
        "measurements",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("bucket", ColumnType::U64),
            ColumnSchema::new("score", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let (storage, control) = TestStorage::controlled(&["measurements"]);
    let mut database = block_on(Database::new(schema, storage.clone())).unwrap();
    let mut seed = database.open_batch();
    for (id, bucket, score) in [(1, 0, 10), (2, 0, 20), (3, 1, 7)] {
        seed.insert(
            "measurements",
            vec![Value::U64(id), Value::U64(bucket), Value::U64(score)],
        );
    }
    block_on(database.commit_batch(seed)).unwrap();
    storage.evict_column_family("measurements");
    control.pause_on(TestStorageOperation::ScanOpen);
    let before = database.runtime_stats();
    let waker = noop_waker();
    let subscriptions = (0..subscribers)
        .map(|_| {
            database
                .subscribe_with_waker([("totals", aggregate())], Some(&waker))
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(
        subscriptions
            .iter()
            .all(|subscription| subscription.try_recv().is_err())
    );
    control.resume_operation(TestStorageOperation::ScanOpen);
    block_on(database.drive_progress()).unwrap();
    for subscription in &subscriptions {
        let initial = block_on(database.next_multisink_subscription(subscription)).unwrap();
        let rows = initial.sinks["totals"].to_values().unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&(row(0, 30), 1)));
        assert!(rows.contains(&(row(1, 7), 1)));
    }
    // Public result equality cannot prove that each queued subscriber avoided
    // recomputing the same initial graph. Runtime work counters provide that
    // mechanism check, separately from the exact values asserted above/below.
    let computes =
        database.runtime_stats().hydration_memo_computes - before.hydration_memo_computes;
    let mut change = database.open_batch();
    change.delete("measurements", PrimaryKeyValue::U64(1));
    change.insert(
        "measurements",
        vec![Value::U64(4), Value::U64(0), Value::U64(5)],
    );
    block_on(database.commit_batch(change)).unwrap();
    block_on(database.drive_progress()).unwrap();
    for subscription in &subscriptions {
        let changed = block_on(database.next_multisink_subscription(subscription)).unwrap();
        let rows = changed.sinks["totals"].to_values().unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&(row(0, 30), -1)));
        assert!(rows.contains(&(row(0, 25), 1)));
    }
    let mut clear_group = database.open_batch();
    clear_group.delete("measurements", PrimaryKeyValue::U64(2));
    clear_group.delete("measurements", PrimaryKeyValue::U64(4));
    block_on(database.commit_batch(clear_group)).unwrap();
    block_on(database.drive_progress()).unwrap();
    for subscription in &subscriptions {
        let changed = block_on(database.next_multisink_subscription(subscription)).unwrap();
        assert_eq!(
            changed.sinks["totals"].to_values().unwrap(),
            vec![(row(0, 25), -1)]
        );
    }
    computes
}

#[test]
fn queued_aggregate_hydrations_reuse_completed_state_and_keep_later_deltas() {
    let single = queued_aggregates(1);
    assert!(single > 0);
    assert_eq!(
        queued_aggregates(8),
        single,
        "shared initial work grows with queued subscriptions"
    );
}

#[test]
fn write_queued_behind_shared_hydrations_is_delivered_exactly_once() {
    use std::future::Future;
    use std::task::{Context, Poll};

    let schema = DatabaseSchema::new([TableSchema::new(
        "measurements",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("bucket", ColumnType::U64),
            ColumnSchema::new("score", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let (storage, control) = TestStorage::controlled(&["measurements"]);
    let mut database = block_on(Database::new(schema, storage.clone())).unwrap();
    let mut seed = database.open_batch();
    seed.insert(
        "measurements",
        vec![Value::U64(1), Value::U64(0), Value::U64(10)],
    );
    block_on(database.commit_batch(seed)).unwrap();
    storage.evict_column_family("measurements");
    control.pause_on(TestStorageOperation::ScanOpen);
    let waker = noop_waker();
    let subscriptions = (0..4)
        .map(|_| {
            database
                .subscribe_with_waker([("totals", aggregate())], Some(&waker))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut write = database.open_batch();
    write.insert(
        "measurements",
        vec![Value::U64(2), Value::U64(0), Value::U64(5)],
    );
    let mut apply = Box::pin(database.apply_batch(write));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(apply.as_mut().poll(&mut context), Poll::Pending));
    control.resume_operation(TestStorageOperation::ScanOpen);
    let publication = block_on(apply).unwrap();
    let persisted = block_on(publication.persist());
    database.finish_persistence(persisted).unwrap();
    block_on(database.drive_progress()).unwrap();
    for subscription in &subscriptions {
        let first = block_on(database.next_multisink_subscription(subscription)).unwrap();
        let mut updates = vec![first];
        while let Ok(update) = subscription.try_recv() {
            updates.push(update);
        }
        let mut weighted = Vec::<(Vec<Value>, i64)>::new();
        for update in updates {
            for (values, weight) in update.sinks["totals"].to_values().unwrap() {
                if let Some((_, total)) = weighted
                    .iter_mut()
                    .find(|(previous, _)| *previous == values)
                {
                    *total += weight;
                } else {
                    weighted.push((values, weight));
                }
            }
        }
        weighted.retain(|(_, weight)| *weight != 0);
        assert_eq!(weighted, vec![(row(0, 15), 1)]);
    }
}
