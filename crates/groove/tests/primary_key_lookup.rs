#![cfg(feature = "test")]

use std::collections::BTreeMap;
use std::future::Future;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::noop_waker;
use groove::db::{Database, GraphBuilder, PrimaryKeyValue};
use groove::ivm::{PredicateExpr, ProjectField, Subscription};
use groove::records::{RecordDescriptor, Value, VariantRecord};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{MemoryStorage, TestStorage, TestStorageOperation};

fn schema() -> DatabaseSchema {
    DatabaseSchema::new(["refs", "targets"].map(|name| {
        TableSchema::new(
            name,
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("value", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    }))
}

fn lookup() -> GraphBuilder {
    GraphBuilder::table_lookup(GraphBuilder::table("refs"), "targets", ["value"])
}

fn drain(subscription: &Subscription, rows: &mut BTreeMap<String, i64>) {
    while let Ok(deltas) = subscription.try_recv() {
        for (values, weight) in deltas.to_values().unwrap() {
            *rows.entry(format!("{values:?}")).or_default() += weight;
        }
    }
    rows.retain(|_, count| *count != 0);
    assert!(rows.values().all(|count| *count == 1), "{rows:?}");
}

/// Alice's lookup must agree with a relational semijoin while Bob changes
/// references and targets, including absent targets and same-commit changes.
#[futures_test::test]
async fn lookup_tracks_live_keys_and_target_updates() {
    let mut db = Database::new(schema(), MemoryStorage::new(&["refs", "targets"]).unwrap())
        .await
        .unwrap();
    let mut batch = db.open_batch();
    batch.insert("refs", vec![Value::U64(1), Value::U64(10)]);
    batch.insert("refs", vec![Value::U64(2), Value::U64(10)]);
    batch.insert("refs", vec![Value::U64(3), Value::U64(20)]);
    batch.insert("targets", vec![Value::U64(10), Value::U64(100)]);
    batch.insert("targets", vec![Value::U64(30), Value::U64(300)]);
    db.commit_batch(batch).await.unwrap();
    let actual = db.subscribe_one_sink(lookup()).await.unwrap();
    let oracle = db
        .subscribe_one_sink(GraphBuilder::semi_join(
            GraphBuilder::table("targets"),
            GraphBuilder::table("refs"),
            ["id"],
            ["value"],
        ))
        .await
        .unwrap();
    let mut actual_rows = BTreeMap::new();
    let mut oracle_rows = BTreeMap::new();
    for step in 0..10 {
        db.drive_progress().await.unwrap();
        drain(&actual, &mut actual_rows);
        drain(&oracle, &mut oracle_rows);
        assert_eq!(actual_rows, oracle_rows, "step {step}");
        let mut batch = db.open_batch();
        match step {
            0 => batch.delete("refs", PrimaryKeyValue::U64(1)),
            1 => batch.update("targets", vec![Value::U64(10), Value::U64(101)]),
            2 => batch.insert("targets", vec![Value::U64(20), Value::U64(200)]),
            3 => batch.update("refs", vec![Value::U64(2), Value::U64(30)]),
            4 => batch.delete("targets", PrimaryKeyValue::U64(30)),
            5 => {
                batch.insert("targets", vec![Value::U64(30), Value::U64(301)]);
                batch.update("refs", vec![Value::U64(3), Value::U64(30)]);
            }
            6 => {
                batch.update("refs", vec![Value::U64(2), Value::U64(10)]);
                batch.update("targets", vec![Value::U64(10), Value::U64(102)]);
            }
            7 => {
                batch.update("refs", vec![Value::U64(3), Value::U64(20)]);
                batch.delete("targets", PrimaryKeyValue::U64(20));
            }
            8 => batch.insert("targets", vec![Value::U64(20), Value::U64(201)]),
            _ => break,
        }
        db.commit_batch(batch).await.unwrap();
    }
    let second = db.subscribe_one_sink(lookup()).await.unwrap();
    db.drive_progress().await.unwrap();
    let mut second_rows = BTreeMap::new();
    drain(&second, &mut second_rows);
    assert_eq!(
        second_rows, actual_rows,
        "shared lookup hydrates a second reader"
    );
    let mut batch = db.open_batch();
    batch.update("targets", vec![Value::U64(10), Value::U64(103)]);
    db.commit_batch(batch).await.unwrap();
    db.drive_progress().await.unwrap();
    drain(&actual, &mut actual_rows);
    drain(&second, &mut second_rows);
    drain(&oracle, &mut oracle_rows);
    assert_eq!(actual_rows, oracle_rows);
    assert_eq!(second_rows, oracle_rows);
}

/// Alice opens three cold keys (one absent) while Bob has 100 unrelated rows.
/// Nonzero schema-version tags use the homogeneous table descriptor.
/// Storage controls verify bounded, concurrent discovery and that suspension
/// never publishes a partial page; this requires the public storage-test seam.
#[test]
fn cold_lookup_batches_only_requested_keys_and_resumes_atomically() {
    let (storage, control) = TestStorage::controlled(&["refs", "targets"]);
    let mut db = block_on(Database::new(schema(), storage.clone())).unwrap();
    let mut batch = db.open_batch();
    for id in 0..100 {
        batch.insert(
            "targets",
            VariantRecord::create(
                7,
                RecordDescriptor::new([("id", ColumnType::U64), ("value", ColumnType::U64)]),
                &[Value::U64(id), Value::U64(id + 1000)],
            )
            .unwrap(),
        );
    }
    block_on(db.commit_batch(batch)).unwrap();
    storage.evict_column_family("targets");
    control.take_observed();
    let before = control.point_read_count();
    control.pause_on(TestStorageOperation::Get);
    let keys = GraphBuilder::values(
        RecordDescriptor::new([("key", ColumnType::U64)]),
        [
            [Value::U64(1)],
            [Value::U64(2)],
            [Value::U64(101)],
            [Value::U64(1)],
        ],
    )
    .unwrap();
    let sub = block_on(db.subscribe_one_sink(GraphBuilder::table_lookup(keys, "targets", ["key"])))
        .unwrap();
    let mut progress = Box::pin(db.drive_progress());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    for _ in 0..8 {
        assert!(matches!(
            progress.as_mut().poll(&mut context),
            Poll::Pending
        ));
        if control
            .observed()
            .iter()
            .filter(|op| **op == TestStorageOperation::Get)
            .count()
            == 3
        {
            break;
        }
    }
    assert!(sub.try_recv().is_err());
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|op| **op == TestStorageOperation::Get)
            .count(),
        3
    );
    assert!(!control.observed().contains(&TestStorageOperation::ScanOpen));
    control.resume_operation(TestStorageOperation::Get);
    block_on(progress).unwrap();
    assert_eq!(control.point_read_count() - before, 3);
    let mut rows = BTreeMap::new();
    drain(&sub, &mut rows);
    assert_eq!(
        rows,
        BTreeMap::from([
            (format!("{:?}", [Value::U64(1), Value::U64(1001)]), 1),
            (format!("{:?}", [Value::U64(2), Value::U64(1002)]), 1),
        ])
    );
}

/// Alice follows Bob's linked rows recursively. Target insertions and deletions
/// must update the closure, including after recursive snapshot recomputation.
#[futures_test::test]
async fn recursive_lookup_tracks_insertions_and_retractions() {
    let mut db = Database::new(schema(), MemoryStorage::new(&["refs", "targets"]).unwrap())
        .await
        .unwrap();
    let mut batch = db.open_batch();
    batch.insert("refs", vec![Value::U64(0), Value::U64(1)]);
    batch.insert("targets", vec![Value::U64(1), Value::U64(2)]);
    batch.insert("targets", vec![Value::U64(2), Value::U64(3)]);
    db.commit_batch(batch).await.unwrap();
    let output = RecordDescriptor::new([("id", ColumnType::U64)]);
    let seed = GraphBuilder::table("refs").project_fields([ProjectField::renamed("value", "id")]);
    let step = GraphBuilder::table_lookup(
        GraphBuilder::frontier_source("frontier", output),
        "targets",
        ["id"],
    )
    .project_fields([ProjectField::renamed("value", "id")]);
    let sub = db
        .subscribe_one_sink(GraphBuilder::recursive(seed, step, "frontier", 16))
        .await
        .unwrap();
    let mut rows = BTreeMap::new();
    for (phase, expected) in [&[1, 2, 3][..], &[1, 2], &[1, 2, 3], &[1, 2, 3, 4]]
        .into_iter()
        .enumerate()
    {
        db.drive_progress().await.unwrap();
        drain(&sub, &mut rows);
        assert_eq!(
            rows,
            expected
                .iter()
                .map(|id| (format!("{:?}", [Value::U64(*id)]), 1))
                .collect(),
            "phase {phase}"
        );
        let mut batch = db.open_batch();
        match phase {
            0 => batch.delete("targets", PrimaryKeyValue::U64(2)),
            1 => batch.insert("targets", vec![Value::U64(2), Value::U64(3)]),
            2 => batch.insert("targets", vec![Value::U64(3), Value::U64(4)]),
            _ => break,
        }
        db.commit_batch(batch).await.unwrap();
    }
}

/// Alice's filtered page excludes Bob's deleted rows. A row that enters the
/// filter while deleted must appear when Bob subsequently restores it.
#[futures_test::test]
async fn lookup_anti_join_restores_a_key_that_entered_after_hydration() {
    let mut db = Database::new(schema(), MemoryStorage::new(&["refs", "targets"]).unwrap())
        .await
        .unwrap();
    let mut batch = db.open_batch();
    batch.insert("refs", vec![Value::U64(1), Value::U64(1)]);
    batch.insert("refs", vec![Value::U64(2), Value::U64(0)]);
    batch.insert("targets", vec![Value::U64(2), Value::U64(0)]);
    db.commit_batch(batch).await.unwrap();
    let candidates = GraphBuilder::table("refs").filter(PredicateExpr::eq("value", Value::U64(1)));
    let deleted = GraphBuilder::table_lookup(candidates.clone(), "targets", ["id"])
        .filter(PredicateExpr::eq("value", Value::U64(0)));
    let sub = db
        .subscribe_one_sink(GraphBuilder::anti_join(candidates, deleted, ["id"], ["id"]))
        .await
        .unwrap();
    let mut rows = BTreeMap::new();
    for (phase, expected) in [&[1][..], &[1], &[1, 2]].into_iter().enumerate() {
        db.drive_progress().await.unwrap();
        drain(&sub, &mut rows);
        assert_eq!(
            rows,
            expected
                .iter()
                .map(|id| (format!("{:?}", [Value::U64(*id), Value::U64(1)]), 1))
                .collect(),
            "phase {phase}"
        );
        let mut batch = db.open_batch();
        match phase {
            0 => batch.update("refs", vec![Value::U64(2), Value::U64(1)]),
            1 => batch.update("targets", vec![Value::U64(2), Value::U64(1)]),
            _ => break,
        }
        db.commit_batch(batch).await.unwrap();
    }
}

/// Alice cancels a cold lookup before its targets arrive. Bob's later lookup
/// must hydrate normally, without inheriting partial references or blocking.
/// Storage controls are necessary to exercise cancellation at the read boundary.
#[test]
fn cancelled_cold_lookup_leaves_no_partial_state() {
    let (storage, control) = TestStorage::controlled(&["refs", "targets"]);
    let mut db = block_on(Database::new(schema(), storage.clone())).unwrap();
    let mut batch = db.open_batch();
    batch.insert("targets", vec![Value::U64(1), Value::U64(100)]);
    block_on(db.commit_batch(batch)).unwrap();
    let keys = GraphBuilder::values(
        RecordDescriptor::new([("key", ColumnType::U64)]),
        [[Value::U64(1)]],
    )
    .unwrap();
    let graph = GraphBuilder::table_lookup(keys, "targets", ["key"]);
    storage.evict_column_family("targets");
    control.pause_on(TestStorageOperation::Get);
    let sub = block_on(db.subscribe_one_sink(graph.clone())).unwrap();
    let mut progress = Box::pin(db.drive_progress());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        progress.as_mut().poll(&mut context),
        Poll::Pending
    ));
    assert!(sub.try_recv().is_err());
    drop(progress);
    assert!(db.unsubscribe(sub.id()));
    drop(sub);
    control.resume_operation(TestStorageOperation::Get);
    block_on(db.drive_progress()).unwrap();
    let sub = block_on(db.subscribe_one_sink(graph)).unwrap();
    let rows = block_on(db.next_subscription(&sub))
        .unwrap()
        .to_values()
        .unwrap();
    assert_eq!(rows, vec![(vec![Value::U64(1), Value::U64(100)], 1)]);
}

/// Alice requests many keys before Bob has any targets. One bounded emptiness
/// probe replaces absent point reads, but Bob's later insert must still appear.
/// Storage controls expose request counts and suspend the emptiness proof.
#[test]
fn large_lookup_of_empty_table_stays_live_without_absent_point_reads() {
    let (storage, control) = TestStorage::controlled(&["refs", "targets"]);
    let mut db = block_on(Database::new(schema(), storage.clone())).unwrap();
    storage.evict_column_family("targets");
    control.take_observed();
    let before = control.point_read_count();
    control.pause_on(TestStorageOperation::ScanOpen);
    let keys = GraphBuilder::values(
        RecordDescriptor::new([("key", ColumnType::U64)]),
        (0..128).map(|key| [Value::U64(key)]),
    )
    .unwrap();
    let sub = block_on(db.subscribe_one_sink(GraphBuilder::table_lookup(keys, "targets", ["key"])))
        .unwrap();
    let mut progress = Box::pin(db.drive_progress());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        progress.as_mut().poll(&mut context),
        Poll::Pending
    ));
    assert!(sub.try_recv().is_err());
    control.resume_operation(TestStorageOperation::ScanOpen);
    block_on(progress).unwrap();
    assert!(sub.recv().unwrap().is_empty());
    assert_eq!(control.point_read_count(), before);
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|op| **op == TestStorageOperation::ScanOpen)
            .count(),
        1
    );

    let mut batch = db.open_batch();
    batch.insert("targets", vec![Value::U64(7), Value::U64(70)]);
    batch.insert("targets", vec![Value::U64(900), Value::U64(9000)]);
    block_on(db.commit_batch(batch)).unwrap();
    block_on(db.drive_progress()).unwrap();
    let mut rows = BTreeMap::new();
    drain(&sub, &mut rows);
    assert_eq!(
        rows,
        BTreeMap::from([(format!("{:?}", [Value::U64(7), Value::U64(70)]), 1)])
    );
}
