//! Public Groove API checks for first-result join execution. Groove exposes
//! multiset weights and array-key expansion that Jazz's row API normalizes.

use std::rc::Rc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::task::noop_waker;
use groove::chunks::{ChunkRequest, TestChunkProvider};
use groove::db::{Database, GraphBuilder, PrimaryKeyValue, SubscriptionLifetime};
use groove::ivm::ProjectField;
use groove::large_values::{LargeValueKind, prepare};
use groove::records::{RecordDescriptor, Value, ValueType};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

async fn empty_db() -> Database {
    Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]).unwrap())
        .await
        .unwrap()
}

fn sorted(mut rows: Vec<(Vec<Value>, i64)>) -> Vec<(Vec<Value>, i64)> {
    rows.sort_by_key(|row| format!("{row:?}"));
    rows
}

async fn first(db: &mut Database, graph: GraphBuilder) -> Vec<(Vec<Value>, i64)> {
    let sub = db
        .subscribe_with_lifetime([("rows", graph)], SubscriptionLifetime::FirstResult, None)
        .unwrap();
    let result = db.next_multisink_subscription(&sub).await.unwrap();
    sorted(result.get("rows").unwrap().to_values().unwrap())
}

fn values(name: &str, ty: ValueType, rows: impl IntoIterator<Item = Value>) -> GraphBuilder {
    GraphBuilder::values(
        RecordDescriptor::new([(name, ty)]),
        rows.into_iter().map(|v| vec![v]),
    )
    .unwrap()
}

#[futures_test::test]
async fn first_result_joins_preserve_duplicate_and_array_key_multiplicities() {
    let mut db = empty_db().await;
    let array = |keys: &[u64]| Value::Array(keys.iter().copied().map(Value::U64).collect());
    let left = GraphBuilder::values(
        RecordDescriptor::new([
            ("id", ValueType::U64),
            ("keys", ValueType::Array(Box::new(ValueType::U64))),
        ]),
        [
            vec![Value::U64(1), array(&[7, 7, 8])],
            vec![Value::U64(1), array(&[7, 7, 8])],
            vec![Value::U64(2), array(&[])],
            vec![Value::U64(3), array(&[9])],
        ],
    )
    .unwrap();
    let right = GraphBuilder::values(
        RecordDescriptor::new([("tag", ValueType::U64), ("key", ValueType::U64)]),
        [
            vec![Value::U64(10), Value::U64(7)],
            vec![Value::U64(10), Value::U64(7)],
            vec![Value::U64(11), Value::U64(8)],
        ],
    )
    .unwrap();
    let inner =
        GraphBuilder::join(left.clone(), right.clone(), ["keys"], ["key"]).project_fields([
            ProjectField::renamed("left.id", "id"),
            ProjectField::renamed("right.tag", "tag"),
        ]);
    assert_eq!(
        first(&mut db, inner).await,
        sorted(vec![
            (vec![Value::U64(1), Value::U64(10)], 4),
            (vec![Value::U64(1), Value::U64(11)], 2),
        ])
    );
    // Each distinct matching key contributes once per left occurrence. An
    // empty array has no key in either the semi or anti relation.
    let semi =
        GraphBuilder::semi_join(left.clone(), right.clone(), ["keys"], ["key"]).project(["id"]);
    assert_eq!(first(&mut db, semi).await, vec![(vec![Value::U64(1)], 4)]);
    let anti = GraphBuilder::anti_join(left, right, ["keys"], ["key"]).project(["id"]);
    assert_eq!(first(&mut db, anti).await, vec![(vec![Value::U64(3)], 1)]);
}

#[futures_test::test]
async fn first_result_joins_preserve_nullable_and_policy_numeric_comparison() {
    let mut db = empty_db().await;
    let nullable = ValueType::Nullable(Box::new(ValueType::U64));
    let null = Value::Nullable(None);
    let some = Value::Nullable(Some(Box::new(Value::U64(7))));
    let left = values("key", nullable.clone(), [null.clone(), some.clone()]);
    let right = values("key", nullable, [null.clone()]);
    assert_eq!(
        first(
            &mut db,
            GraphBuilder::semi_join(left.clone(), right.clone(), ["key"], ["key"])
        )
        .await,
        vec![(vec![null], 1)]
    );
    assert_eq!(
        first(
            &mut db,
            GraphBuilder::anti_join(left, right, ["key"], ["key"])
        )
        .await,
        vec![(vec![some], 1)]
    );
    let left = values("key", ValueType::U64, [Value::U64(7), Value::U64(u64::MAX)]);
    let right = values("key", ValueType::I64, [Value::I64(7), Value::I64(-1)]);
    let join = GraphBuilder::policy_join(left, right, ["key"], ["key"])
        .project_fields([ProjectField::renamed("left.key", "key")]);
    assert_eq!(first(&mut db, join).await, vec![(vec![Value::U64(7)], 1)]);
}

#[futures_test::test]
async fn first_result_self_join_keeps_the_shared_lookup_arrangement() {
    let mut db = empty_db().await;
    let source = values(
        "key",
        ValueType::U64,
        [Value::U64(1), Value::U64(1), Value::U64(2)],
    );
    let join = GraphBuilder::join(source.clone(), source, ["key"], ["key"])
        .project_fields([ProjectField::renamed("left.key", "key")]);
    assert_eq!(
        first(&mut db, join).await,
        sorted(vec![(vec![Value::U64(1)], 4), (vec![Value::U64(2)], 1)])
    );
}

#[futures_test::test]
async fn first_result_threshold_reads_leave_live_join_updates_intact() {
    let schema = DatabaseSchema::new(["items", "matches"].map(|name| {
        TableSchema::new(
            name,
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("key", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    }));
    let mut db = Database::new(schema, MemoryStorage::new(&["items", "matches"]).unwrap())
        .await
        .unwrap();
    let mut batch = db.open_batch();
    batch.insert("items", vec![Value::U64(1), Value::U64(7)]);
    batch.insert("matches", vec![Value::U64(10), Value::U64(7)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();
    let left = GraphBuilder::table("items");
    let right = GraphBuilder::table("matches");
    let semi =
        GraphBuilder::semi_join(left.clone(), right.clone(), ["key"], ["key"]).project(["id"]);
    let anti = GraphBuilder::anti_join(left, right, ["key"], ["key"]).project(["id"]);
    let retained = db
        .subscribe([("semi", semi.clone()), ("anti", anti.clone())])
        .unwrap();
    let initial = db.next_multisink_subscription(&retained).await.unwrap();
    assert_eq!(
        initial.get("semi").unwrap().to_values().unwrap(),
        vec![(vec![Value::U64(1)], 1)]
    );
    assert!(initial.get("anti").unwrap().is_empty());
    assert_eq!(
        first(&mut db, semi.clone()).await,
        vec![(vec![Value::U64(1)], 1)]
    );
    assert!(first(&mut db, anti.clone()).await.is_empty());

    let mut batch = db.open_batch();
    batch.delete("matches", PrimaryKeyValue::U64(10));
    batch.insert("items", vec![Value::U64(2), Value::U64(7)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();
    let delta = db.next_multisink_subscription(&retained).await.unwrap();
    assert_eq!(
        delta.get("semi").unwrap().to_values().unwrap(),
        vec![(vec![Value::U64(1)], -1)]
    );
    assert_eq!(
        sorted(delta.get("anti").unwrap().to_values().unwrap()),
        sorted(vec![(vec![Value::U64(1)], 1), (vec![Value::U64(2)], 1)])
    );
    assert!(first(&mut db, semi).await.is_empty());
    assert_eq!(
        first(&mut db, anti).await,
        sorted(vec![(vec![Value::U64(1)], 1), (vec![Value::U64(2)], 1)])
    );
}

#[futures_test::test]
async fn first_result_join_key_can_suspend_cancel_and_resume() {
    let mut db = empty_db().await;
    let logical = "large join key/".repeat(10_000);
    let prepared = prepare(LargeValueKind::String, logical.as_bytes()).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    control.pause();
    db.set_chunk_provider(Rc::new(provider));
    let left = values(
        "key",
        ValueType::String,
        [Value::Large(Box::new(prepared.value_ref))],
    );
    let right = values("key", ValueType::String, [Value::String(logical.clone())]);
    let graph = GraphBuilder::join(left, right, ["key"], ["key"])
        .project_fields([ProjectField::renamed("left.key", "key")]);
    let cancelled = db
        .subscribe_with_lifetime(
            [("rows", graph.clone())],
            SubscriptionLifetime::FirstResult,
            None,
        )
        .unwrap();
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        db.poll_multisink_subscription(&cancelled, &mut cx),
        Poll::Pending
    ));
    assert!(!control.observed().is_empty());
    drop(cancelled);
    db.prune_dropped_subscriptions().await.unwrap();
    control.resume();
    assert_eq!(
        first(&mut db, graph).await,
        vec![(vec![Value::String(logical)], 1)]
    );
}
