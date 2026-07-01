//! Regression tests for the shared-arrangement maintenance protocol.
//!
//! These guard against arrangements being updated once per consuming join
//! evaluation instead of once per logical time. See review notes:
//! - sibling joins sharing one arrangement side each apply the same tick's
//!   deltas to it,
//! - inside a recursive fixpoint, context-independent inputs re-emit the
//!   tick's deltas at every sub_tick and the step join re-applies them,
//! - a root-scope arrangement shared between a non-recursive join (advanced
//!   at sub_tick 0) and a recursive step join (advanced at sub_tick 1) is
//!   advanced once per distinct logical time instead of once per tick.

use groove::db::{Database, GraphBuilder, PredicateExpr};
use groove::ivm::ProjectField;
use groove::records::{RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::RocksDbStorage;

fn albums_artists_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("artist_id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "artists",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("name", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

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
        ("src", ColumnType::U64.value_type()),
        ("dst", ColumnType::U64.value_type()),
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

#[test]
fn sibling_joins_sharing_an_arrangement_do_not_double_count() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut db = Database::new(albums_artists_schema(), storage).unwrap();

    let join1 = db
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .unwrap();
    // A second, similar join that shares the artists-by-id arrangement.
    let _join2 = db
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("albums").filter(PredicateExpr::gt("id", Value::U64(0))),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .unwrap();
    let _empty_initial = join1.recv().unwrap();

    let mut batch = db.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::from("John Coltrane")],
    );
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::U64(11), Value::from("Blue Train")],
    );
    db.commit_batch(batch).unwrap();
    let _tick_one = join1.recv().unwrap();

    let mut batch = db.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::U64(11), Value::from("Giant Steps")],
    );
    db.commit_batch(batch).unwrap();

    let deltas = join1.recv().unwrap();
    let weights: Vec<i64> = deltas.iter().map(|(_, weight)| weight).collect();
    assert_eq!(
        weights,
        vec![1],
        "second tick should emit the new album joined exactly once"
    );
}

#[test]
fn recursive_incremental_ticks_do_not_inflate_shared_edge_arrangements() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut db = Database::new(edges_schema(), storage).unwrap();
    let sub = db.subscribe_one_sink(reachability_graph()).unwrap();
    let _empty_initial = sub.recv().unwrap();

    // Tick 1: edge 1 -> 2 (recompute + arrangement preparation).
    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    db.commit_batch(batch).unwrap();
    let _initial = sub.recv().unwrap();

    // Tick 2: edge 2 -> 3 (incremental). The fixpoint loop re-applies this
    // tick's edge delta to the shared arrangement once per sub_tick.
    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    db.commit_batch(batch).unwrap();
    let _tick_two = sub.recv().unwrap();

    // Tick 3: edge 0 -> 1 (incremental). Probes the inflated 2->3 entry.
    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(3), Value::U64(0), Value::U64(1)]);
    db.commit_batch(batch).unwrap();

    let mut rows = sub.recv().unwrap().to_values().unwrap();
    rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    let expected: Vec<(Vec<Value>, i64)> = vec![
        (vec![Value::U64(0), Value::U64(1)], 1),
        (vec![Value::U64(0), Value::U64(2)], 1),
        (vec![Value::U64(0), Value::U64(3)], 1),
    ];
    assert_eq!(
        rows, expected,
        "all newly reached pairs should have weight 1"
    );
}

#[test]
fn arrangement_shared_across_sub_ticks_is_applied_once_per_tick() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut db = Database::new(edges_schema(), storage).unwrap();

    // The recursive subscription advances the shared edges-by-src arrangement
    // at sub_tick 1; the plain two-hop join advances it at sub_tick 0. Each
    // tick's edge delta must be incorporated exactly once regardless.
    let _reach = db.subscribe_one_sink(reachability_graph()).unwrap();
    let two_hop = db
        .subscribe_one_sink(GraphBuilder::join(
            edge_pairs(),
            edge_pairs(),
            ["dst"],
            ["src"],
        ))
        .unwrap();

    // Tick 1: 1 -> 2 (recursive recompute + arrangement preparation).
    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    db.commit_batch(batch).unwrap();
    let _initial = two_hop.recv().unwrap();

    // Tick 2: 2 -> 3 (incremental recursion). Both consumers advance the
    // shared arrangement with this delta at different sub_ticks.
    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    db.commit_batch(batch).unwrap();
    let _tick_two = two_hop.recv().unwrap();

    // Tick 3: 9 -> 2. The two-hop join probes the 2 -> 3 entry.
    let mut batch = db.open_batch();
    batch.insert("edges", vec![Value::U64(3), Value::U64(9), Value::U64(2)]);
    db.commit_batch(batch).unwrap();

    let deltas = two_hop.recv().unwrap();
    let weights: Vec<i64> = deltas.iter().map(|(_, weight)| weight).collect();
    assert_eq!(
        weights,
        vec![1],
        "the new two-hop pair (9->2->3) should be emitted exactly once"
    );
}
