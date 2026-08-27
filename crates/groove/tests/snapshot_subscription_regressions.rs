//! Regression tests for initial subscription messages.

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

fn reach_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("src", ColumnType::U64.clone()),
        ("dst", ColumnType::U64.clone()),
    ])
}

fn edge_pairs() -> GraphBuilder {
    GraphBuilder::table("edges").project(["src", "dst"])
}

fn reachability_graph() -> GraphBuilder {
    let frontier = GraphBuilder::frontier_source("frontier", reach_descriptor());
    let step = GraphBuilder::join(frontier, edge_pairs(), ["dst"], ["src"]).project_fields([
        ProjectField::renamed("left.src", "src"),
        ProjectField::renamed("right.dst", "dst"),
    ]);
    GraphBuilder::recursive(edge_pairs(), step, "frontier", 16)
}

#[futures_test::test]
async fn second_subscriber_to_prepared_recursive_graph_gets_full_initial_message() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut db = Database::new(edges_schema(), storage).await.unwrap();

    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    batch.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

    // First subscriber: fresh recursive state, recompute path, full initial.
    let first = db.subscribe_one_sink(reachability_graph()).await.unwrap();
    db.flush().await.unwrap();
    assert_eq!(
        first.recv().unwrap().to_values().unwrap().len(),
        3,
        "first subscriber sees 1->2, 2->3, 1->3"
    );

    // Second subscriber: identical graph dedups to the same prepared node.
    let second = db.subscribe_one_sink(reachability_graph()).await.unwrap();
    db.flush().await.unwrap();
    let initial = second.recv().unwrap().to_values().unwrap();
    assert_eq!(
        initial.len(),
        3,
        "second subscriber must see the same current result, got {initial:?}"
    );
}

#[futures_test::test]
async fn hydrating_a_new_subscriber_must_not_steal_tick_deltas_from_existing_recursive_subscribers()
{
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut db = Database::new(edges_schema(), storage).await.unwrap();

    // Subscriber A: prepared by its first two commits.
    let a = db.subscribe_one_sink(reachability_graph()).await.unwrap();
    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();
    let _empty_initial = a.recv().unwrap();
    let _tick_one = a.recv().unwrap();
    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();
    let _tick_two = a.recv().unwrap();

    // Subscriber B shares the same recursive node and gets an immediate
    // snapshot. That read must not steal A's later tick deltas.
    let b = db.subscribe_one_sink(reachability_graph()).await.unwrap();
    assert!(!b.recv().unwrap().is_empty());

    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(3), Value::U64(3), Value::U64(4)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

    // A must still receive this tick's derived deltas: 3->4, 2->4, 1->4.
    let mut values = a
        .recv()
        .expect("existing subscriber should be notified of this tick's changes")
        .to_values()
        .unwrap();
    values.sort_by(|x, y| format!("{x:?}").cmp(&format!("{y:?}")));
    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(4)], 1),
            (vec![Value::U64(2), Value::U64(4)], 1),
            (vec![Value::U64(3), Value::U64(4)], 1),
        ]
    );
}

#[futures_test::test]
async fn one_shot_queries_do_not_perturb_subscription_streams() {
    use groove::queries::{Query, Select, SelectItem, TableRef};

    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut db = Database::new(edges_schema(), storage).await.unwrap();

    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

    let s = db
        .subscribe_one_sink(GraphBuilder::table("edges"))
        .await
        .unwrap();
    assert!(!s.recv().unwrap().is_empty());
    let query = Query::Select(Box::new(
        Select::new([SelectItem::Wildcard]).from([TableRef::named("edges")]),
    ));
    db.query(query).await.unwrap();
    assert!(s.try_recv().is_err());
}

#[futures_test::test]
async fn new_subscriber_uses_current_state_not_stale_hydrated_accumulated() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut db = Database::new(edges_schema(), storage).await.unwrap();

    // S1 hydrates and prepares the shared recursive state.
    let s1 = db.subscribe_one_sink(reachability_graph()).await.unwrap();
    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();
    let _empty_initial = s1.recv().unwrap();
    let _s1_tick_one = s1.recv().unwrap();

    db.unsubscribe(s1.id());

    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

    // S2's immediate initial snapshot must reflect storage as of subscription.
    let s2 = db.subscribe_one_sink(reachability_graph()).await.unwrap();
    let mut values = s2.recv().unwrap().to_values().unwrap();
    values.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
            (vec![Value::U64(2), Value::U64(3)], 1),
        ],
        "hydration must include this tick's edge and its derived reachability"
    );
}
