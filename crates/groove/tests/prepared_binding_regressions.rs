//! Prepared shapes must keep the binding relation, the
//! shared arrangements, and the refcount map in lockstep:
//! - a shape subscription cleaned up via the storage-less path (dropped
//!   receiver detected mid-tick) must still retract its binding from shared
//!   arrangements before the same binding can be re-added,
//! - creating a second identical shape must not rehydrate the shared graph
//!   in a way that wipes the bindings already registered by the first.

use groove::db::{Database, GraphBuilder};
use groove::ivm::ProjectField;
use groove::records::{RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{Durability, RocksDbStorage};

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

fn binding_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("src", ColumnType::U64.value_type())])
}

fn edges_by_src_shape_graph() -> GraphBuilder {
    let bindings = GraphBuilder::binding_source("edges_by_src", binding_descriptor());
    let edges = GraphBuilder::table("edges").project(["src", "dst"]);
    GraphBuilder::join(bindings, edges, ["src"], ["src"]).project_fields([
        ProjectField::renamed("right.src", "src"),
        ProjectField::renamed("right.dst", "dst"),
    ])
}

fn open_db() -> (tempfile::TempDir, Database<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage =
        RocksDbStorage::open_with_durability(temp_dir.path(), &["edges"], Durability::WalNoSync)
            .unwrap();
    let db = Database::new(edges_schema(), storage).unwrap();
    (temp_dir, db)
}

fn insert_edge(db: &mut Database<RocksDbStorage>, id: u64, src: u64, dst: u64) {
    let mut batch = db.open_batch();
    batch.insert(
        "edges",
        vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
    );
    db.commit_batch(batch).unwrap();
}

#[test]
fn dropped_shape_receiver_cleanup_retracts_binding_before_rebind() {
    let (_dir, mut db) = open_db();
    let shape = db
        .prepare_one_sink(
            edges_by_src_shape_graph(),
            "edges_by_src",
            binding_descriptor(),
            ["src"],
        )
        .unwrap();

    // Subscriber for src=7, whose receiver is dropped without unsubscribe.
    let sub1 = db
        .bind_shape_one_sink(shape.id(), &[Value::U64(7)])
        .unwrap();
    let _initial = sub1.recv().unwrap();
    drop(sub1);

    // This commit produces a delta for src=7, the send fails, and the runtime
    // auto-unsubscribes through the storage-less path.
    insert_edge(&mut db, 1, 7, 100);

    // Re-subscribing the same binding must start from weight 1, not 2.
    let sub2 = db
        .bind_shape_one_sink(shape.id(), &[Value::U64(7)])
        .unwrap();
    let _initial = sub2.recv().unwrap();

    insert_edge(&mut db, 2, 7, 200);
    let deltas = sub2.recv().unwrap();
    let weights: Vec<i64> = deltas.iter().map(|(_, weight)| weight).collect();
    assert_eq!(
        weights,
        vec![1],
        "edge 7->200 must be delivered exactly once after binding re-subscribe"
    );
}

#[test]
fn second_identical_shape_does_not_wipe_existing_bindings() {
    let (_dir, mut db) = open_db();
    let shape_a = db
        .prepare_one_sink(
            edges_by_src_shape_graph(),
            "edges_by_src",
            binding_descriptor(),
            ["src"],
        )
        .unwrap();
    let sub_a = db
        .bind_shape_one_sink(shape_a.id(), &[Value::U64(7)])
        .unwrap();
    let _initial = sub_a.recv().unwrap();

    insert_edge(&mut db, 1, 7, 100);
    assert_eq!(
        sub_a.recv().unwrap().iter().count(),
        1,
        "sub_a receives deltas before the second shape exists"
    );

    // An identical shape interns to the same shared graph nodes.
    let _shape_b = db
        .prepare_one_sink(
            edges_by_src_shape_graph(),
            "edges_by_src",
            binding_descriptor(),
            ["src"],
        )
        .unwrap();

    insert_edge(&mut db, 2, 7, 200);
    let deltas = sub_a
        .try_recv()
        .expect("sub_a must still receive deltas after a second shape is created");
    assert_eq!(deltas.iter().count(), 1);
}

#[test]
fn pending_retraction_does_not_corrupt_freshly_hydrated_sibling_shape() {
    let (_dir, mut db) = open_db();
    // Shape A: src/dst projection.
    let shape_a = db
        .prepare_one_sink(
            edges_by_src_shape_graph(),
            "edges_by_src",
            binding_descriptor(),
            ["src"],
        )
        .unwrap();
    let sub_a = db
        .bind_shape_one_sink(shape_a.id(), &[Value::U64(7)])
        .unwrap();
    let _initial = sub_a.recv().unwrap();
    drop(sub_a);

    // Tick detects the dropped receiver and queues the binding-7 retraction.
    insert_edge(&mut db, 1, 7, 100);

    // Shape B: different shape (dst only), same binding source. Its fresh
    // arrangement is hydrated from a snapshot that already excludes binding 7,
    // and the queued -1 must not drive that binding to weight -1.
    let bindings = GraphBuilder::binding_source("edges_by_src", binding_descriptor());
    let edges = GraphBuilder::table("edges").project(["src", "dst"]);
    let shape_b = GraphBuilder::join(bindings, edges, ["src"], ["src"]).project_fields([
        ProjectField::renamed("right.src", "src"),
        ProjectField::renamed("left.src", "binding_src"),
        ProjectField::renamed("right.dst", "dst"),
    ]);
    let shape_b = db
        .prepare_one_sink(
            shape_b,
            "edges_by_src",
            binding_descriptor(),
            ["binding_src"],
        )
        .unwrap();

    let sub_b = db
        .bind_shape_one_sink(shape_b.id(), &[Value::U64(7)])
        .unwrap();
    let _initial = sub_b.recv().unwrap();

    insert_edge(&mut db, 2, 7, 200);
    let deltas = sub_b
        .try_recv()
        .expect("shape B subscriber must receive deltas for binding 7");
    let weights: Vec<i64> = deltas.iter().map(|(_, weight)| weight).collect();
    assert_eq!(weights, vec![1]);
}
