//! Cyclic graphs are valid inputs to monotone set recursion: transitive
//! closure over a cycle is the canonical recursive query. The incremental
//! path already converges via frontier dedup (`accept_positive`); the
//! recompute path must apply the same dedup instead of circulating the
//! cycle until the iteration limit.

use groove::db::{Database, GraphBuilder};
use groove::ivm::ProjectField;
use groove::records::{RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

fn edges_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "edges",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("src", ColumnType::U64),
            ColumnSchema::new("dst", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn reachability_graph() -> GraphBuilder {
    let frontier = GraphBuilder::frontier_source(
        "frontier",
        RecordDescriptor::new([
            ("src", ColumnType::U64.clone()),
            ("dst", ColumnType::U64.clone()),
        ]),
    );
    let edge_pairs = GraphBuilder::table("edges").project(["src", "dst"]);
    let step = GraphBuilder::join(frontier, edge_pairs, ["dst"], ["src"]).project_fields([
        ProjectField::renamed("left.src", "src"),
        ProjectField::renamed("right.dst", "dst"),
    ]);
    GraphBuilder::recursive(
        GraphBuilder::table("edges").project(["src", "dst"]),
        step,
        "frontier",
        64,
    )
}

fn full_two_cycle_closure() -> Vec<(Vec<Value>, i64)> {
    vec![
        (vec![Value::U64(1), Value::U64(1)], 1),
        (vec![Value::U64(1), Value::U64(2)], 1),
        (vec![Value::U64(2), Value::U64(1)], 1),
        (vec![Value::U64(2), Value::U64(2)], 1),
    ]
}

fn sorted(mut values: Vec<(Vec<Value>, i64)>) -> Vec<(Vec<Value>, i64)> {
    values.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    values
}

#[futures_test::test]
async fn incremental_ticks_converge_on_cycles() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut db = Database::new(edges_schema(), storage).await.unwrap();
    let sub = db.subscribe_one_sink(reachability_graph()).await.unwrap();
    let _initial = sub.recv().unwrap();

    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();
    let _t1 = sub.recv().unwrap();

    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(1)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

    assert_eq!(
        sorted(sub.recv().unwrap().to_values().unwrap()),
        [
            (vec![Value::U64(1), Value::U64(1)], 1),
            (vec![Value::U64(2), Value::U64(1)], 1),
            (vec![Value::U64(2), Value::U64(2)], 1),
        ]
    );
}

#[futures_test::test]
async fn recompute_converges_on_cycles_at_subscribe() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut db = Database::new(edges_schema(), storage).await.unwrap();

    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    batch.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(1)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

    let sub = db
        .subscribe_one_sink(reachability_graph())
        .await
        .expect("subscribing over a cyclic graph must not hit the iteration limit");
    assert_eq!(
        sorted(sub.recv().unwrap().to_values().unwrap()),
        full_two_cycle_closure()
    );
}

#[futures_test::test]
async fn retraction_recompute_converges_while_a_cycle_exists() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut db = Database::new(edges_schema(), storage).await.unwrap();
    let sub = db.subscribe_one_sink(reachability_graph()).await.unwrap();
    let _initial = sub.recv().unwrap();

    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    batch.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(1)]);
    batch.insert("edges", vec![Value::U64(3), Value::U64(2), Value::U64(3)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();
    let _t1 = sub.recv().unwrap();

    // Deleting the unrelated edge triggers a retraction recompute while the
    // 1 <-> 2 cycle is still present in the base table.
    let mut batch = db.open_batch();
    batch.delete("edges", groove::db::PrimaryKeyValue::U64(3));
    let applied = db
        .apply_batch(batch)
        .await
        .expect("retraction ticks must not fail while the base data contains a cycle");
    let persisted = applied.persist().await;
    db.finish_persistence(persisted)
        .expect("retraction persistence must not fail");

    assert_eq!(
        sorted(sub.recv().unwrap().to_values().unwrap()),
        [
            (vec![Value::U64(1), Value::U64(3)], -1),
            (vec![Value::U64(2), Value::U64(3)], -1),
        ]
    );
}
