//! Keyed lookup (`GraphBuilder::lookup`): a stored table probed by the keys
//! of another relation.
//!
//! Every maintained case is checked against an oracle: the same relation
//! computed one-shot as an ordinary semi-join of the whole (prefix-scanned)
//! table. The storage read counter is the only way to tell a lookup from
//! that semi-join, so the cost tests read it directly.

use std::collections::BTreeMap;

use groove::db::{Database, GraphBuilder, PredicateExpr, PrimaryKeyValue};
use groove::ivm::{LiteralValue, StaticScanSpec};
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, PrimaryKeyColumn,
    TableSchema,
};
use groove::storage::MemoryStorage;

/// `items(id, bucket)` supplies keys; `refs(id, target)` supplies duplicate
/// keys; `marks(branch, row, seq, note)` is the probed table, keyed
/// `(branch, row, seq)` so one row id can have several marks and a branch is
/// a static prefix.
fn schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "items",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("bucket", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64).user_supplied()),
        TableSchema::new(
            "refs",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("target", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64).user_supplied()),
        TableSchema::new(
            "marks",
            [
                ColumnSchema::new("branch", ColumnType::U64),
                ColumnSchema::new("row", ColumnType::U64),
                ColumnSchema::new("seq", ColumnType::U64),
                ColumnSchema::new("note", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::integer("branch", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("row", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("seq", IntegerKeyType::U64),
        ])),
    ])
}

/// Oracles here are the whole-table plans, so joins are not keyed
/// automatically (`keyed_join.rs` covers that).
async fn open_db() -> Database {
    let storage = MemoryStorage::new(&["items", "refs", "marks"]).expect("valid families");
    let mut db = Database::new(schema(), storage).await.unwrap();
    db.set_keyed_joins_enabled(false);
    db
}

fn marks_of_branch(branch: u64) -> GraphBuilder {
    GraphBuilder::table_scan(
        "marks",
        StaticScanSpec::Prefix(vec![LiteralValue::from(Value::U64(branch))]),
    )
}

/// Ids of the items in bucket 1.
fn bucket_keys() -> GraphBuilder {
    GraphBuilder::table("items")
        .filter(PredicateExpr::eq("bucket", Value::U64(1)))
        .project(["id"])
}

/// Targets of every ref: a key relation with duplicates.
fn ref_keys() -> GraphBuilder {
    GraphBuilder::table("refs").project(["target"])
}

fn lookup(keys: GraphBuilder, field: &str) -> GraphBuilder {
    GraphBuilder::lookup(marks_of_branch(1), keys, [field]).expect("marks is a table source")
}

/// The same relation as an ordinary semi-join of the whole branch.
fn oracle(keys: GraphBuilder, field: &str) -> GraphBuilder {
    GraphBuilder::semi_join(marks_of_branch(1), keys, ["row"], [field])
}

fn mark(branch: u64, row: u64, seq: u64, note: &str) -> Vec<Value> {
    vec![
        Value::U64(branch),
        Value::U64(row),
        Value::U64(seq),
        Value::String(note.into()),
    ]
}

fn mark_key(branch: u64, row: u64, seq: u64) -> PrimaryKeyValue {
    PrimaryKeyValue::Composite(vec![
        PrimaryKeyValue::U64(branch),
        PrimaryKeyValue::U64(row),
        PrimaryKeyValue::U64(seq),
    ])
}

type Bag = BTreeMap<String, i64>;

/// One batch of writes in a scripted sequence.
type Edit = Box<dyn FnOnce(&mut groove::db::DatabaseBatch)>;

fn add(bag: &mut Bag, rows: Vec<(Vec<Value>, i64)>) {
    for (values, weight) in rows {
        *bag.entry(format!("{values:?}")).or_default() += weight;
    }
    bag.retain(|_, weight| *weight != 0);
}

async fn snapshot(db: &mut Database, graph: GraphBuilder) -> Bag {
    let mut bag = Bag::new();
    add(
        &mut bag,
        db.query_graph(graph).await.unwrap().to_values().unwrap(),
    );
    bag
}

async fn apply(db: &mut Database, edit: impl FnOnce(&mut groove::db::DatabaseBatch)) {
    let mut batch = db.open_batch();
    edit(&mut batch);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();
}

/// One-shot: only the marks of live keys, under the static prefix, each
/// once however often its key occurs.
#[futures_test::test]
async fn one_shot_lookup_reads_the_marks_of_its_keys() {
    let mut db = open_db().await;
    apply(&mut db, |batch| {
        batch.insert("items", vec![Value::U64(1), Value::U64(1)]);
        batch.insert("items", vec![Value::U64(2), Value::U64(1)]);
        batch.insert("items", vec![Value::U64(3), Value::U64(9)]);
        batch.insert("refs", vec![Value::U64(10), Value::U64(2)]);
        batch.insert("refs", vec![Value::U64(11), Value::U64(2)]);
        batch.insert("marks", mark(1, 1, 1, "a"));
        batch.insert("marks", mark(1, 1, 2, "b"));
        batch.insert("marks", mark(1, 2, 1, "c"));
        batch.insert("marks", mark(1, 3, 1, "not a key"));
        batch.insert("marks", mark(2, 1, 1, "other branch"));
    })
    .await;

    let bucket = snapshot(&mut db, lookup(bucket_keys(), "id")).await;
    assert_eq!(bucket, snapshot(&mut db, oracle(bucket_keys(), "id")).await);
    assert_eq!(bucket.len(), 3, "{bucket:?}");

    let refs = snapshot(&mut db, lookup(ref_keys(), "target")).await;
    assert_eq!(refs, snapshot(&mut db, oracle(ref_keys(), "target")).await);
    assert_eq!(
        refs,
        Bag::from([(format!("{:?}", mark(1, 2, 1, "c")), 1)]),
        "a duplicated key selects its rows once"
    );
}

/// A maintained lookup follows every change on either side: rows of a live
/// key inserted, updated and deleted; rows of other keys or branches
/// ignored; keys entering and leaving (alone or in the same batch as their
/// rows' changes); duplicate keys counted.
#[futures_test::test]
async fn maintained_lookup_follows_both_sides() {
    let mut db = open_db().await;
    apply(&mut db, |batch| {
        batch.insert("items", vec![Value::U64(1), Value::U64(1)]);
        batch.insert("items", vec![Value::U64(2), Value::U64(9)]);
        batch.insert("marks", mark(1, 1, 1, "a"));
        batch.insert("marks", mark(1, 2, 1, "b"));
    })
    .await;

    for (keys, field) in [(bucket_keys(), "id"), (ref_keys(), "target")] {
        let sub = db
            .subscribe_one_sink(lookup(keys.clone(), field))
            .await
            .unwrap();
        let mut live = Bag::new();
        add(&mut live, sub.recv().unwrap().to_values().unwrap());
        let edits: Vec<Edit> = vec![
            // A row of a live key.
            Box::new(|b| b.insert("marks", mark(1, 1, 2, "a2"))),
            // Rows of a key that is not live, and of another branch.
            Box::new(|b| {
                b.insert("marks", mark(1, 7, 1, "x"));
                b.insert("marks", mark(2, 1, 9, "y"));
            }),
            // Update and delete rows of a live key.
            Box::new(|b| b.update("marks", mark(1, 1, 1, "a-updated"))),
            Box::new(|b| b.delete("marks", mark_key(1, 1, 2))),
            // A key enters: its existing rows appear.
            Box::new(|b| b.update("items", vec![Value::U64(2), Value::U64(1)])),
            Box::new(|b| b.insert("refs", vec![Value::U64(10), Value::U64(2)])),
            // A duplicate key, then one of the duplicates leaves.
            Box::new(|b| b.insert("refs", vec![Value::U64(11), Value::U64(2)])),
            Box::new(|b| b.delete("refs", PrimaryKeyValue::U64(10))),
            // A key enters in the same batch as a new row of it.
            Box::new(|b| {
                b.insert("items", vec![Value::U64(7), Value::U64(1)]);
                b.insert("refs", vec![Value::U64(12), Value::U64(7)]);
                b.insert("marks", mark(1, 7, 2, "x2"));
            }),
            // A key leaves in the same batch as a change of its rows.
            Box::new(|b| {
                b.update("items", vec![Value::U64(2), Value::U64(9)]);
                b.delete("refs", PrimaryKeyValue::U64(11));
                b.update("marks", mark(1, 2, 1, "b-updated"));
            }),
            // A deleted key, then re-inserted with a row it gained meanwhile.
            Box::new(|b| {
                b.delete("items", PrimaryKeyValue::U64(7));
                b.delete("refs", PrimaryKeyValue::U64(12));
            }),
            Box::new(|b| b.insert("marks", mark(1, 7, 3, "x3"))),
            Box::new(|b| {
                b.insert("items", vec![Value::U64(7), Value::U64(1)]);
                b.insert("refs", vec![Value::U64(12), Value::U64(7)]);
            }),
            // Delete a row and its key together.
            Box::new(|b| {
                b.delete("marks", mark_key(1, 7, 1));
                b.delete("items", PrimaryKeyValue::U64(7));
                b.delete("refs", PrimaryKeyValue::U64(12));
            }),
        ];
        for (step, edit) in edits.into_iter().enumerate() {
            apply(&mut db, edit).await;
            while let Ok(deltas) = sub.try_recv() {
                add(&mut live, deltas.to_values().unwrap());
            }
            assert!(
                live.values().all(|weight| *weight == 1),
                "step {step}: {live:?}"
            );
            assert_eq!(
                live,
                snapshot(&mut db, oracle(keys.clone(), field)).await,
                "{field} step {step}"
            );
        }
        drop(sub);
        // Reset the data both key relations share for the next pass.
        apply(&mut db, |b| {
            b.update("items", vec![Value::U64(2), Value::U64(9)]);
            b.update("marks", mark(1, 1, 1, "a"));
            b.update("marks", mark(1, 2, 1, "b"));
            b.delete("marks", mark_key(1, 7, 2));
            b.delete("marks", mark_key(1, 7, 3));
            b.delete("marks", mark_key(2, 1, 9));
        })
        .await;
    }
}

/// The keyed lookup semi- and anti-joins are plain compositions over the
/// lookup, and stay maintained.
#[futures_test::test]
async fn lookup_semi_and_anti_joins_stay_maintained() {
    let mut db = open_db().await;
    apply(&mut db, |batch| {
        batch.insert("items", vec![Value::U64(1), Value::U64(1)]);
        batch.insert("items", vec![Value::U64(2), Value::U64(1)]);
        batch.insert("marks", mark(1, 1, 1, "a"));
    })
    .await;
    let items = GraphBuilder::table("items");
    let marked = lookup(GraphBuilder::table("items").project(["id"]), "id");
    let semi = GraphBuilder::semi_join(items.clone(), marked.clone(), ["id"], ["row"]);
    let anti = GraphBuilder::anti_join(items.clone(), marked, ["id"], ["row"]);
    let whole_semi = GraphBuilder::semi_join(items.clone(), marks_of_branch(1), ["id"], ["row"]);
    let whole_anti = GraphBuilder::anti_join(items, marks_of_branch(1), ["id"], ["row"]);
    let semi_sub = db.subscribe_one_sink(semi).await.unwrap();
    let anti_sub = db.subscribe_one_sink(anti).await.unwrap();
    let mut semi_live = Bag::new();
    let mut anti_live = Bag::new();
    add(
        &mut semi_live,
        semi_sub.recv().unwrap().to_values().unwrap(),
    );
    add(
        &mut anti_live,
        anti_sub.recv().unwrap().to_values().unwrap(),
    );
    let edits: Vec<Edit> = vec![
        Box::new(|b| b.insert("marks", mark(1, 2, 1, "b"))),
        Box::new(|b| b.delete("marks", mark_key(1, 1, 1))),
        Box::new(|b| b.insert("items", vec![Value::U64(3), Value::U64(1)])),
        Box::new(|b| b.insert("marks", mark(1, 3, 1, "c"))),
        Box::new(|b| b.delete("items", PrimaryKeyValue::U64(2))),
    ];
    for (step, edit) in edits.into_iter().enumerate() {
        apply(&mut db, edit).await;
        while let Ok(deltas) = semi_sub.try_recv() {
            add(&mut semi_live, deltas.to_values().unwrap());
        }
        while let Ok(deltas) = anti_sub.try_recv() {
            add(&mut anti_live, deltas.to_values().unwrap());
        }
        assert_eq!(
            semi_live,
            snapshot(&mut db, whole_semi.clone()).await,
            "{step}"
        );
        assert_eq!(
            anti_live,
            snapshot(&mut db, whole_anti.clone()).await,
            "{step}"
        );
    }
}

async fn populate(db: &mut Database, rows: u64) {
    apply(db, |batch| {
        for row in 1..=rows {
            batch.insert("marks", mark(1, row, 1, "m"));
        }
        for id in 1..=3 {
            batch.insert("items", vec![Value::U64(id), Value::U64(1)]);
        }
    })
    .await;
}

/// The lookup's storage work scales with its keys, not with the probed
/// table: hydrating three keys reads the same number of `marks` rows at
/// 100 and 2,000 rows, while the whole-table semi-join reads them all.
#[futures_test::test]
async fn lookup_storage_reads_scale_with_keys_not_table_size() {
    let mut reads = Vec::new();
    for rows in [100, 2_000] {
        let mut db = open_db().await;
        populate(&mut db, rows).await;
        db.reset_storage_read_metrics();
        let result = snapshot(&mut db, lookup(bucket_keys(), "id")).await;
        assert_eq!(result.len(), 3);
        let lookup_reads = db.take_storage_read_metrics().total.reads;
        let _ = snapshot(&mut db, oracle(bucket_keys(), "id")).await;
        let whole_reads = db.take_storage_read_metrics().total.reads;
        assert!(
            whole_reads as u64 >= rows,
            "the oracle reads the whole table: {whole_reads}"
        );
        reads.push(lookup_reads);
    }
    assert_eq!(
        reads[0], reads[1],
        "lookup reads grew with the table: {reads:?}"
    );
    // Three items and three marks.
    assert!(reads[0] <= 6, "{reads:?}");
}

/// A maintained lookup reads storage only for keys that become live. A
/// write to an unrelated row of the probed table, or a new row of a live
/// key, reads nothing; a key entering reads only its own rows.
#[futures_test::test]
async fn maintained_lookup_reads_only_entering_keys() {
    let mut db = open_db().await;
    populate(&mut db, 2_000).await;
    db.reset_storage_read_metrics();
    let sub = db
        .subscribe_one_sink(lookup(bucket_keys(), "id"))
        .await
        .unwrap();
    let mut live = Bag::new();
    add(&mut live, sub.recv().unwrap().to_values().unwrap());
    assert_eq!(live.len(), 3);
    let hydration = db.take_storage_read_metrics().total.reads;
    assert!(hydration <= 6, "hydration read {hydration} rows");

    // Writes read their own previous images; measure them against a write
    // that the lookup does not see.
    apply(&mut db, |b| {
        b.update("marks", mark(1, 1_500, 1, "unrelated"))
    })
    .await;
    let unrelated = db.take_storage_read_metrics().total.reads;
    apply(&mut db, |b| b.insert("marks", mark(1, 2, 2, "live key"))).await;
    let live_row = db.take_storage_read_metrics().total.reads;
    assert_eq!(live_row, unrelated, "a live key's new row needs no probe");

    apply(&mut db, |b| {
        b.insert("items", vec![Value::U64(900), Value::U64(1)])
    })
    .await;
    let entering = db.take_storage_read_metrics().total.reads;
    assert!(
        entering <= unrelated + 1,
        "an entering key reads its own one row: {entering} vs {unrelated}"
    );
    while let Ok(deltas) = sub.try_recv() {
        add(&mut live, deltas.to_values().unwrap());
    }
    assert_eq!(live, snapshot(&mut db, oracle(bucket_keys(), "id")).await);
    assert_eq!(live.len(), 5);
}

/// A lookup names a table source, and its keys must match the type of the
/// primary-key columns they continue.
#[futures_test::test]
async fn lookup_rejects_shapes_it_cannot_probe() {
    let mut db = open_db().await;
    assert!(GraphBuilder::lookup(bucket_keys(), bucket_keys(), ["id"]).is_none());
    let wrong_type = GraphBuilder::lookup(
        marks_of_branch(1),
        GraphBuilder::table("marks").project(["note"]),
        ["note"],
    )
    .unwrap();
    assert!(db.query_graph(wrong_type).await.is_err());
    let too_long = GraphBuilder::lookup(
        marks_of_branch(1),
        GraphBuilder::table("items").project(["id", "bucket"]),
        ["id", "bucket", "id"],
    )
    .unwrap();
    assert!(db.query_graph(too_long).await.is_err());
}

/// A probe that finds its rows evicted parks the tick on a cold storage
/// read; the tick resumes with the probed rows and later ticks stay exact.
#[cfg(feature = "test")]
mod cold_probe {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures::executor::block_on;
    use futures::task::noop_waker;
    use groove::storage::{TestStorage, TestStorageOperation};

    use super::*;

    /// Poll `future` to completion, resuming paused storage whenever it parks.
    fn run_resuming<T>(
        control: &groove::storage::TestStorageControl,
        future: impl Future<Output = T>,
    ) -> (T, usize) {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        let mut parked = 0;
        loop {
            match Pin::new(&mut future).poll(&mut cx) {
                Poll::Ready(value) => return (value, parked),
                Poll::Pending => {
                    parked += 1;
                    control.resume_operation(TestStorageOperation::ScanOpen);
                    control.resume_operation(TestStorageOperation::Get);
                }
            }
        }
    }

    #[test]
    fn an_entering_key_probed_from_cold_storage_converges() {
        let (storage, control) = TestStorage::controlled(&["items", "refs", "marks"]);
        let mut db = block_on(Database::new(schema(), storage.clone())).unwrap();
        db.set_keyed_joins_enabled(false);
        block_on(apply(&mut db, |b| {
            b.insert("items", vec![Value::U64(1), Value::U64(1)]);
            b.insert("items", vec![Value::U64(2), Value::U64(9)]);
            b.insert("marks", mark(1, 1, 1, "a"));
            b.insert("marks", mark(1, 2, 1, "b"));
            b.insert("marks", mark(1, 2, 2, "b2"));
        }));
        let sub = block_on(db.subscribe_one_sink(lookup(bucket_keys(), "id"))).unwrap();
        block_on(db.drive_progress()).unwrap();
        let mut live = Bag::new();
        while let Ok(deltas) = sub.try_recv() {
            add(&mut live, deltas.to_values().unwrap());
        }
        assert_eq!(live.len(), 1, "{live:?}");

        storage.evict_all();
        control.pause_on(TestStorageOperation::ScanOpen);
        control.pause_on(TestStorageOperation::Get);
        let mut batch = db.open_batch();
        // Item 2 enters bucket 1: its marks are only in cold storage.
        batch.update("items", vec![Value::U64(2), Value::U64(1)]);
        let (applied, parked) = run_resuming(&control, db.apply_batch(batch));
        assert!(parked > 0, "the write should park on cold storage");
        let persisted = block_on(applied.unwrap().persist());
        db.finish_persistence(persisted).unwrap();
        let (progress, _) = run_resuming(&control, db.drive_progress());
        progress.unwrap();
        while let Ok(deltas) = sub.try_recv() {
            add(&mut live, deltas.to_values().unwrap());
        }
        assert_eq!(live.len(), 3, "{live:?}");

        // Later changes of the probed key apply exactly once.
        block_on(apply(&mut db, |b| {
            b.update("marks", mark(1, 2, 1, "b-updated"));
            b.delete("marks", mark_key(1, 2, 2));
        }));
        block_on(db.drive_progress()).unwrap();
        while let Ok(deltas) = sub.try_recv() {
            add(&mut live, deltas.to_values().unwrap());
        }
        assert!(live.values().all(|weight| *weight == 1), "{live:?}");
        assert_eq!(
            live,
            block_on(snapshot(&mut db, oracle(bucket_keys(), "id")))
        );
    }
}
