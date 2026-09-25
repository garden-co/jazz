//! Generic root positions (insert indices and moves) for plain ordered
//! outputs are collected only for an output that can apply them (#2086).
//! For an unbounded TopBy, collecting them ranks only the rows a write
//! changed (#3505), so a one-row write costs the same at any result size. An
//! unbounded TopBy whose outputs take no positions records no ranks at all.

use groove::db::{Database, GraphBuilder};
use groove::ivm::runtime::TerminalEdit;
use groove::ivm::{TopByLimit, TopByOrder};
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

const SHAPES: u64 = 200;

async fn database() -> Database {
    let schema = DatabaseSchema::new([
        TableSchema::new(
            "shapes",
            [
                ColumnSchema::new("canvas", ColumnType::U64),
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("z", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new("canvases", [ColumnSchema::new("id", ColumnType::U64)])
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ]);
    let mut db = Database::new(schema, MemoryStorage::new(&["shapes", "canvases"]).unwrap())
        .await
        .unwrap();
    let mut batch = db.open_batch();
    batch.insert("canvases", vec![Value::U64(1)]);
    for id in 0..SHAPES {
        batch.insert(
            "shapes",
            vec![Value::U64(1), Value::U64(id), Value::U64(id)],
        );
    }
    let persistence = db.apply_batch(batch).await.unwrap().persist().await;
    db.finish_persistence(persistence).unwrap();
    db
}

/// Every shape of the canvas in z order: an unbounded, zero-offset window.
fn ordered_shapes() -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::table("shapes"),
        Vec::<String>::new(),
        [TopByOrder::asc("z")],
        ["id"],
        0,
        TopByLimit::Unbounded,
    )
}

async fn add_shape(db: &mut Database, id: u64) {
    let mut batch = db.open_batch();
    batch.insert(
        "shapes",
        vec![Value::U64(1), Value::U64(id), Value::U64(id)],
    );
    let persistence = db.apply_batch(batch).await.unwrap().persist().await;
    db.finish_persistence(persistence).unwrap();
}

/// Positions a TopBy collected for the last write, and records its delta-only
/// path handled.
fn last_tick_position_work(db: &Database) -> (usize, usize) {
    let metrics = db.last_tick_metrics().expect("the write ran a tick");
    (
        metrics.root_ordering_position_records,
        metrics.top_by_delta_membership_records,
    )
}

#[futures_test::test]
async fn an_output_carrying_the_top_by_identity_still_receives_root_positions() {
    // Positive control: this output applies positions, so the write ranks the
    // inserted shape and it lands at its ordered index.
    let mut db = database().await;
    let subscription = db.subscribe([("shapes", ordered_shapes())]).unwrap();
    subscription.try_recv().unwrap();

    add_shape(&mut db, SHAPES).await;

    let (positions, _) = last_tick_position_work(&db);
    // Only the inserted shape is ranked, not the other SHAPES rows.
    assert_eq!(positions, 1);
    let tick = subscription.try_recv().unwrap();
    let operations = &tick.terminal_sinks["shapes"].operations;
    assert!(
        operations.iter().any(|operation| matches!(
            operation.edit,
            TerminalEdit::Insert { index, .. } if index == SHAPES as usize
        )),
        "{operations:?}"
    );
}

#[futures_test::test]
async fn a_row_that_only_changes_rank_is_reordered_by_moves_alone() {
    // The output keeps only the identity, so moving a shape to the back
    // changes nothing it shows except positions. Moves alone carry the
    // change (no insert, update or removal), and applying them in order puts
    // the shape last.
    let mut db = database().await;
    let subscription = db
        .subscribe([("shapes", ordered_shapes().project(["id"]))])
        .unwrap();
    subscription.try_recv().unwrap();

    let mut batch = db.open_batch();
    batch.update(
        "shapes",
        vec![Value::U64(1), Value::U64(0), Value::U64(SHAPES)],
    );
    let persistence = db.apply_batch(batch).await.unwrap().persist().await;
    db.finish_persistence(persistence).unwrap();

    let tick = subscription.try_recv().unwrap();
    let operations = &tick.terminal_sinks["shapes"].operations;
    // Every other shape keeps its relative order, so one move of the changed
    // shape suffices (#3505), not one per shifted shape.
    assert_eq!(operations.len(), 1, "{operations:?}");
    let mut order: Vec<u64> = (0..SHAPES).collect();
    for operation in operations {
        let TerminalEdit::Move { key, index } = &operation.edit else {
            panic!("expected only moves, got {operations:?}");
        };
        let id = u64::from_be_bytes(key[1..].try_into().unwrap());
        let from = order.iter().position(|shape| *shape == id).unwrap();
        order.remove(from);
        order.insert(*index, id);
    }
    let expected: Vec<u64> = (1..SHAPES).chain([0]).collect();
    assert_eq!(order, expected);
}

#[futures_test::test]
async fn an_output_without_the_top_by_identity_does_not_collect_root_positions() {
    // The semi join between the TopBy and the output means the output's
    // terminal keys are its own fields, not the TopBy row identity that
    // positions are keyed by, so no position could address them.
    let mut db = database().await;
    let visible_shapes = GraphBuilder::semi_join(
        ordered_shapes(),
        GraphBuilder::table("canvases"),
        ["canvas"],
        ["id"],
    );
    let subscription = db.subscribe([("shapes", visible_shapes)]).unwrap();
    subscription.try_recv().unwrap();

    add_shape(&mut db, SHAPES).await;

    assert_eq!(last_tick_position_work(&db), (0, 1));
    let tick = subscription.try_recv().unwrap();
    assert_eq!(tick.sinks["shapes"].deltas.len(), 1);
    assert!(
        tick.terminal_sinks["shapes"]
            .operations
            .iter()
            .all(|operation| !matches!(operation.edit, TerminalEdit::Move { .. })),
        "{:?}",
        tick.terminal_sinks["shapes"].operations
    );
}

#[futures_test::test]
async fn a_consumer_that_declines_root_positions_keeps_the_delta_only_path() {
    let mut db = database().await;
    db.set_plain_output_root_positions_enabled(false);
    let subscription = db.subscribe([("shapes", ordered_shapes())]).unwrap();
    subscription.try_recv().unwrap();

    add_shape(&mut db, SHAPES).await;

    assert_eq!(last_tick_position_work(&db), (0, 1));
    let tick = subscription.try_recv().unwrap();
    assert_eq!(tick.sinks["shapes"].deltas.len(), 1);
}
