#![cfg(feature = "test")]

use std::collections::BTreeMap;
use std::future::Future;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::noop_waker;
use groove::db::{Database, GraphBuilder, PrimaryKeyValue};
use groove::ivm::{
    IndexWindow, IndexWindowExclusion, LiteralValue, PredicateExpr, ProjectField, StaticScanSpec,
    Subscription, TopByLimit, TopByOrder,
};
use groove::records::{RecordDescriptor, Value, VariantRecord};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{MemoryStorage, TestStorage, TestStorageOperation};

fn schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "items",
            ["id", "bucket", "rank"].map(|name| ColumnSchema::new(name, ColumnType::U64)),
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
        .with_index(IndexSchema::new("by_bucket_rank", ["bucket", "rank"]))
        .with_variant(1, ["id", "bucket", "rank"])
        .with_variant(2, ["id", "bucket", "rank"]),
        TableSchema::new(
            "exclusions",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("deleted", ColumnType::Bool),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn projection(db: &mut Database) {
    let output = schema().table("items").unwrap().record_schema();
    db.define_variant_projection("items", "visible", output)
        .unwrap();
    db.register_variant_projection_case(
        "items",
        "visible",
        1,
        [
            ProjectField::named("id"),
            ProjectField::named("bucket"),
            ProjectField::named("rank"),
        ],
    )
    .unwrap();
    db.register_variant_projection_ignore_case("items", "visible", 2)
        .unwrap();
}

fn item(id: u64, bucket: u64, rank: u64, version: u32) -> VariantRecord {
    let descriptor = schema()
        .table("items")
        .unwrap()
        .record_schema_for_variant(version)
        .unwrap();
    VariantRecord::create(
        version,
        descriptor,
        &[Value::U64(id), Value::U64(bucket), Value::U64(rank)],
    )
    .unwrap()
}

fn page(reverse: bool, bounded: bool) -> GraphBuilder {
    let input = if bounded {
        let prefix = vec![LiteralValue::U64(1)];
        GraphBuilder::variant_index_window(
            "items",
            "by_bucket_rank",
            "visible",
            if reverse {
                StaticScanSpec::ReversePrefixLimit {
                    prefix,
                    max_items: 4,
                }
            } else {
                StaticScanSpec::PrefixLimit {
                    prefix,
                    max_items: 4,
                }
            },
            IndexWindow {
                limit: 3,
                order_field: "rank".into(),
                exclusion: Some(IndexWindowExclusion {
                    table: "exclusions".into(),
                    key_prefix: Vec::new(),
                    key_fields: vec!["id".into()],
                    predicate: PredicateExpr::eq("deleted", Value::Bool(true)),
                }),
            },
        )
    } else {
        GraphBuilder::anti_join(
            GraphBuilder::variant_source("items", "visible")
                .filter(PredicateExpr::eq("bucket", Value::U64(1))),
            GraphBuilder::table("exclusions")
                .filter(PredicateExpr::eq("deleted", Value::Bool(true))),
            ["id"],
            ["id"],
        )
    };
    GraphBuilder::top_by(
        input,
        Vec::<String>::new(),
        [if reverse {
            TopByOrder::desc("rank")
        } else {
            TopByOrder::asc("rank")
        }],
        ["id"],
        0,
        TopByLimit::Finite(3),
    )
}

fn drain(subscription: &Subscription, rows: &mut BTreeMap<String, i64>) {
    while let Ok(deltas) = subscription.try_recv() {
        for (values, weight) in deltas.to_values().unwrap() {
            *rows.entry(format!("{values:?}")).or_default() += weight;
        }
    }
    rows.retain(|_, weight| *weight != 0);
    assert!(rows.values().all(|weight| *weight == 1), "{rows:?}");
}

/// Bob changes index keys and exclusion records while Alice retains ascending
/// and descending pages. Both pages must match an unrestricted relational
/// oracle through empty/short pages, boundary ties, and ignored schema cases.
/// This lower layer exposes arbitrary projection cases and atomic index writes.
/// bob --one commit--> index + exclusions --refilled page--> alice
#[futures_test::test]
async fn ordered_window_matches_live_relational_pages() {
    for reverse in [false, true] {
        let schema = schema();
        let mut db = Database::new(
            schema.clone(),
            MemoryStorage::new(&schema.column_families()).unwrap(),
        )
        .await
        .unwrap();
        projection(&mut db);
        let actual = db.subscribe_one_sink(page(reverse, true)).await.unwrap();
        let oracle = db.subscribe_one_sink(page(reverse, false)).await.unwrap();
        let mut actual_rows = BTreeMap::new();
        let mut oracle_rows = BTreeMap::new();
        for step in 0..11 {
            db.drive_progress().await.unwrap();
            drain(&actual, &mut actual_rows);
            drain(&oracle, &mut oracle_rows);
            assert_eq!(actual_rows, oracle_rows, "step {step}, reverse {reverse}");
            let mut batch = db.open_batch();
            match step {
                0 => batch.insert("items", item(200, 1, 200, 1)),
                1 => {
                    for id in 0..96 {
                        batch.insert("items", item(id, 1, id / 8, if id < 8 { 2 } else { 1 }));
                    }
                }
                2 => {
                    batch.update("items", item(200, 1, 0, 1));
                    batch.insert("exclusions", vec![Value::U64(200), Value::Bool(true)]);
                }
                3 => batch.update("exclusions", vec![Value::U64(200), Value::Bool(false)]),
                4 => batch.update("items", item(200, 2, 0, 1)),
                5 => {
                    // Remove each end's boundary group in one commit. The
                    // replacement page must come from beyond the old cap.
                    for id in (8..16).chain(88..96) {
                        batch.insert("exclusions", vec![Value::U64(id), Value::Bool(true)]);
                    }
                }
                6 => {
                    batch.update("items", item(95, 1, 0, 1));
                    batch.delete("exclusions", PrimaryKeyValue::U64(95));
                    batch.update("items", item(0, 1, 300, 1));
                }
                7 => batch.delete("items", PrimaryKeyValue::U64(95)),
                8 => {
                    let duplicate = db.subscribe_one_sink(page(reverse, true)).await.unwrap();
                    db.drive_progress().await.unwrap();
                    let mut rows = BTreeMap::new();
                    drain(&duplicate, &mut rows);
                    assert_eq!(
                        rows, oracle_rows,
                        "second subscriber shares a complete page"
                    );
                    assert!(db.unsubscribe(duplicate.id()));
                    batch.update("items", item(0, 1, 300, 2));
                }
                9 => {
                    for id in 0..96 {
                        if id != 95 {
                            batch.delete("items", PrimaryKeyValue::U64(id));
                        }
                    }
                }
                _ => break,
            }
            db.commit_batch(batch).await.unwrap();
        }
    }
}

/// Alice cancels a page while Bob's storage is cold. A subsequent page must
/// hydrate from scratch without publishing partial rows or duplicate weights.
/// Controlled storage is needed to suspend precisely between scan and rows.
#[test]
fn cancelled_cold_ordered_window_leaves_no_partial_state() {
    let schema = schema();
    let (storage, control) = TestStorage::controlled(&schema.column_families());
    let mut db = block_on(Database::new(schema, storage.clone())).unwrap();
    projection(&mut db);
    let mut batch = db.open_batch();
    for id in 0..100 {
        batch.insert("items", item(id, 1, id, 1));
    }
    block_on(db.commit_batch(batch)).unwrap();
    storage.evict_column_family("items");
    storage.evict_column_family("indices");
    storage.evict_column_family("exclusions");
    control.take_observed();
    control.pause_on(TestStorageOperation::Get);
    let sub = block_on(db.subscribe_one_sink(page(false, true))).unwrap();
    let mut progress = Box::pin(db.drive_progress());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    for _ in 0..4 {
        assert!(matches!(
            progress.as_mut().poll(&mut context),
            Poll::Pending
        ));
        assert!(sub.try_recv().is_err());
    }
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|op| **op == TestStorageOperation::Get)
            .count(),
        4
    );
    drop(progress);
    assert!(db.unsubscribe(sub.id()));
    control.resume_operation(TestStorageOperation::Get);
    block_on(db.drive_progress()).unwrap();
    let sub = block_on(db.subscribe_one_sink(page(false, true))).unwrap();
    block_on(db.drive_progress()).unwrap();
    let mut actual = BTreeMap::new();
    drain(&sub, &mut actual);
    assert_eq!(actual.len(), 3);
    assert_eq!(
        actual,
        (0..3)
            .map(|id| (
                format!("{:?}", [Value::U64(id), Value::U64(1), Value::U64(id)]),
                1
            ))
            .collect()
    );
}

/// Alice must not receive a falsely complete page when Bob's projection
/// changes the field used to prove index order. This is a runtime contract
/// because application queries do not expose arbitrary projection expressions.
#[futures_test::test]
async fn ordered_window_rejects_a_different_projected_order() {
    let schema = schema();
    let mut db = Database::new(
        schema.clone(),
        MemoryStorage::new(&schema.column_families()).unwrap(),
    )
    .await
    .unwrap();
    let output: RecordDescriptor = schema.table("items").unwrap().record_schema();
    db.define_variant_projection("items", "visible", output)
        .unwrap();
    db.register_variant_projection_case(
        "items",
        "visible",
        1,
        [
            ProjectField::named("id"),
            ProjectField::named("bucket"),
            ProjectField::renamed("id", "rank"),
        ],
    )
    .unwrap();
    let error = db.subscribe_one_sink(page(false, true)).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("ordering projection that differs from the index key"),
        "{error}"
    );
}

/// Bob's unbounded boundary tie cannot be proven within Alice's candidate
/// budget. Fail the page explicitly instead of silently returning an arbitrary
/// subset or falling back to a full scan. The runtime seam exposes this error.
#[futures_test::test]
async fn ordered_window_rejects_a_boundary_tie_beyond_its_budget() {
    let schema = schema();
    let mut db = Database::new(
        schema.clone(),
        MemoryStorage::new(&schema.column_families()).unwrap(),
    )
    .await
    .unwrap();
    projection(&mut db);
    let mut batch = db.open_batch();
    for id in 0..4100 {
        batch.insert("items", item(id, 1, 0, 1));
    }
    db.commit_batch(batch).await.unwrap();
    let sub = db.subscribe_one_sink(page(false, true)).await.unwrap();
    let error = db.next_subscription(&sub).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("sparse prefix or boundary tie beyond the bounded candidate budget"),
        "{error}"
    );
    assert!(sub.try_recv().is_err(), "no partial page may publish");
}

/// Bob's distinct rows may project to identical values. Alice's source still
/// needs their full multiplicity as rows enter and leave its retained prefix.
/// This runtime seam exposes projection multiplicities hidden by Jazz row IDs.
#[futures_test::test]
async fn ordered_window_preserves_projected_row_multiplicity() {
    let schema = schema();
    let mut db = Database::new(
        schema.clone(),
        MemoryStorage::new(&schema.column_families()).unwrap(),
    )
    .await
    .unwrap();
    let output = schema.table("items").unwrap().record_schema();
    db.define_variant_projection("items", "visible", output)
        .unwrap();
    db.register_variant_projection_case(
        "items",
        "visible",
        1,
        [
            ProjectField::renamed("rank", "id"),
            ProjectField::named("bucket"),
            ProjectField::named("rank"),
        ],
    )
    .unwrap();
    let mut batch = db.open_batch();
    for id in 0..8 {
        batch.insert("items", item(id, 1, id / 2, 1));
    }
    db.commit_batch(batch).await.unwrap();
    let GraphBuilder::TopBy { input, .. } = page(false, true) else {
        unreachable!()
    };
    let sub = db.subscribe_one_sink((*input).clone()).await.unwrap();
    let first = db
        .next_subscription(&sub)
        .await
        .unwrap()
        .to_values()
        .unwrap();
    assert_eq!(first.len(), 4);
    assert!(first.iter().all(|(_, weight)| *weight == 2), "{first:?}");
    let mut batch = db.open_batch();
    batch.delete("items", PrimaryKeyValue::U64(7));
    db.commit_batch(batch).await.unwrap();
    assert_eq!(
        db.next_subscription(&sub)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(3), Value::U64(1), Value::U64(3)], -1)]
    );
}
