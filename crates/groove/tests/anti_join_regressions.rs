//! Anti-join same-tick cross-term regressions: when a left row and its
//! matching right row arrive in the same commit, the left row arrives
//! suppressed — nothing must be emitted, and especially not a retraction
//! of a row that was never inserted downstream. Symmetrically, a left row
//! arriving in the same tick as the retraction of its last blocker must be
//! emitted exactly once.

use std::collections::BTreeMap;

use groove::db::{Database, GraphBuilder, MultisinkDeltas, PredicateExpr, PrimaryKeyValue};
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{Durability, RocksDbStorage};

fn schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("artist_id", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "blockers",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("artist_id", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn open_db() -> (tempfile::TempDir, Database<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open_with_durability(
        temp_dir.path(),
        &["albums", "blockers"],
        Durability::WalNoSync,
    )
    .unwrap();
    let db = Database::new(schema(), storage).unwrap();
    (temp_dir, db)
}

fn anti_join() -> GraphBuilder {
    GraphBuilder::anti_join(
        GraphBuilder::table("albums"),
        GraphBuilder::table("blockers"),
        ["artist_id"],
        ["artist_id"],
    )
}

fn assert_sink_values(deltas: &MultisinkDeltas, sinks: [&str; 2], expected: &[(Vec<Value>, i64)]) {
    for sink in sinks {
        assert_eq!(
            deltas.get(sink).unwrap().to_values().unwrap(),
            expected,
            "unexpected delta for {sink}"
        );
    }
}

const SHARED_LEFT_SINKS: [&str; 2] = ["a_id_filter", "z_artist_filter"];

fn shared_left_sinks(semi: bool) -> [(&'static str, GraphBuilder); 2] {
    let left = GraphBuilder::table("albums");
    let join = |left, right| {
        if semi {
            GraphBuilder::semi_join(left, right, ["artist_id"], ["artist_id"])
        } else {
            GraphBuilder::anti_join(left, right, ["artist_id"], ["artist_id"])
        }
    };
    [
        (
            SHARED_LEFT_SINKS[0],
            join(
                left.clone(),
                GraphBuilder::table("blockers").filter(PredicateExpr::gt("id", Value::U64(0))),
            ),
        ),
        (
            SHARED_LEFT_SINKS[1],
            join(
                left,
                GraphBuilder::table("blockers")
                    .filter(PredicateExpr::gt("artist_id", Value::U64(0))),
            ),
        ),
    ]
}

#[test]
fn same_tick_left_and_blocking_right_emit_nothing() {
    let (_dir, mut db) = open_db();
    let sub = db.subscribe_one_sink(anti_join()).unwrap();
    let _initial = sub.recv().unwrap();

    // Album and its blocker arrive in one commit: the album is suppressed
    // from the start and must never appear, positively or negatively.
    let mut batch = db.open_batch();
    batch.insert("albums", vec![Value::U64(1), Value::U64(11)]);
    batch.insert("blockers", vec![Value::U64(1), Value::U64(11)]);
    db.commit_batch(batch).unwrap();

    let mut materialized = BTreeMap::<String, i64>::new();
    while let Ok(deltas) = sub.try_recv() {
        for (values, weight) in deltas.to_values().unwrap() {
            *materialized.entry(format!("{values:?}")).or_default() += weight;
        }
    }
    materialized.retain(|_, weight| *weight != 0);
    assert!(
        materialized.is_empty(),
        "suppressed-on-arrival album must produce no net deltas, got {materialized:?}"
    );
}

#[test]
fn same_tick_left_insert_and_last_blocker_retraction_emit_once() {
    let (_dir, mut db) = open_db();
    let sub = db.subscribe_one_sink(anti_join()).unwrap();
    let _initial = sub.recv().unwrap();

    // Pre-existing blocked album.
    let mut batch = db.open_batch();
    batch.insert("albums", vec![Value::U64(1), Value::U64(11)]);
    batch.insert("blockers", vec![Value::U64(1), Value::U64(11)]);
    db.commit_batch(batch).unwrap();
    while sub.try_recv().is_ok() {}

    // One commit: a second album for the artist arrives while the artist's
    // last blocker is deleted. Both albums become visible exactly once.
    let mut batch = db.open_batch();
    batch.insert("albums", vec![Value::U64(2), Value::U64(11)]);
    batch.delete("blockers", PrimaryKeyValue::U64(1));
    db.commit_batch(batch).unwrap();

    let mut materialized = BTreeMap::<String, i64>::new();
    while let Ok(deltas) = sub.try_recv() {
        for (values, weight) in deltas.to_values().unwrap() {
            *materialized.entry(format!("{values:?}")).or_default() += weight;
        }
    }
    assert_eq!(
        materialized,
        BTreeMap::from([
            (format!("{:?}", vec![Value::U64(1), Value::U64(11)]), 1),
            (format!("{:?}", vec![Value::U64(2), Value::U64(11)]), 1),
        ]),
        "both albums must surface with weight exactly 1"
    );
}

#[test]
fn shared_right_arrangement_retracts_every_anti_join_consumer() {
    let (_dir, mut db) = open_db();
    let right = GraphBuilder::table("blockers");
    let sub = db
        .subscribe([
            (
                "a_projected",
                GraphBuilder::anti_join(
                    GraphBuilder::table("albums").project(["id", "artist_id"]),
                    right.clone(),
                    ["artist_id"],
                    ["artist_id"],
                ),
            ),
            (
                "z_filtered",
                GraphBuilder::anti_join(
                    GraphBuilder::table("albums").filter(PredicateExpr::gt("id", Value::U64(0))),
                    right,
                    ["artist_id"],
                    ["artist_id"],
                ),
            ),
        ])
        .unwrap();
    let _initial = sub.recv().unwrap();

    let mut batch = db.open_batch();
    batch.insert("albums", vec![Value::U64(1), Value::U64(11)]);
    db.commit_batch(batch).unwrap();
    let inserted = sub.recv().unwrap();
    assert_eq!(
        inserted.get("a_projected").unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(11)], 1)]
    );
    assert_eq!(
        inserted.get("z_filtered").unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(11)], 1)]
    );

    let mut batch = db.open_batch();
    batch.insert("blockers", vec![Value::U64(1), Value::U64(11)]);
    db.commit_batch(batch).unwrap();
    let blocked = sub.recv().unwrap();
    for sink in ["a_projected", "z_filtered"] {
        assert_eq!(
            blocked.get(sink).unwrap().to_values().unwrap(),
            [(vec![Value::U64(1), Value::U64(11)], -1)],
            "every anti-join consumer sharing the blocker arrangement must retract"
        );
    }

    let mut batch = db.open_batch();
    batch.delete("blockers", PrimaryKeyValue::U64(1));
    db.commit_batch(batch).unwrap();
    let restored = sub.recv().unwrap();
    for sink in ["a_projected", "z_filtered"] {
        assert_eq!(
            restored.get(sink).unwrap().to_values().unwrap(),
            [(vec![Value::U64(1), Value::U64(11)], 1)],
            "every anti-join consumer sharing the blocker arrangement must restore"
        );
    }
}

#[test]
fn shared_right_arrangement_updates_every_semi_join_consumer() {
    let (_dir, mut db) = open_db();
    let mut batch = db.open_batch();
    batch.insert("albums", vec![Value::U64(1), Value::U64(11)]);
    db.commit_batch(batch).unwrap();

    let right = GraphBuilder::table("blockers");
    let sub = db
        .subscribe([
            (
                "a_projected",
                GraphBuilder::semi_join(
                    GraphBuilder::table("albums").project(["id", "artist_id"]),
                    right.clone(),
                    ["artist_id"],
                    ["artist_id"],
                ),
            ),
            (
                "z_filtered",
                GraphBuilder::semi_join(
                    GraphBuilder::table("albums").filter(PredicateExpr::gt("id", Value::U64(0))),
                    right,
                    ["artist_id"],
                    ["artist_id"],
                ),
            ),
        ])
        .unwrap();
    let initial = sub.recv().unwrap();
    assert!(initial.get("a_projected").unwrap().is_empty());
    assert!(initial.get("z_filtered").unwrap().is_empty());

    let mut batch = db.open_batch();
    batch.insert("blockers", vec![Value::U64(1), Value::U64(11)]);
    db.commit_batch(batch).unwrap();
    let unblocked = sub.recv().unwrap();
    for sink in ["a_projected", "z_filtered"] {
        assert_eq!(
            unblocked.get(sink).unwrap().to_values().unwrap(),
            [(vec![Value::U64(1), Value::U64(11)], 1)],
            "every semi-join consumer sharing the blocker arrangement must update"
        );
    }

    let mut batch = db.open_batch();
    batch.delete("blockers", PrimaryKeyValue::U64(1));
    db.commit_batch(batch).unwrap();
    let blocked = sub.recv().unwrap();
    for sink in ["a_projected", "z_filtered"] {
        assert_eq!(
            blocked.get(sink).unwrap().to_values().unwrap(),
            [(vec![Value::U64(1), Value::U64(11)], -1)],
            "every semi-join consumer sharing the blocker arrangement must retract"
        );
    }
}

#[test]
fn shared_left_arrangement_updates_every_anti_join_consumer() {
    let (_dir, mut db) = open_db();
    let sub = db.subscribe(shared_left_sinks(false)).unwrap();
    let _initial = sub.recv().unwrap();

    let mut batch = db.open_batch();
    batch.insert("albums", vec![Value::U64(1), Value::U64(11)]);
    db.commit_batch(batch).unwrap();
    let inserted = sub.recv().unwrap();
    assert_sink_values(
        &inserted,
        SHARED_LEFT_SINKS,
        &[(vec![Value::U64(1), Value::U64(11)], 1)],
    );

    // The new album is blocked on arrival, while the previously visible album
    // retracts. A consumer must not reconstruct the shared left side as if the
    // new album had been visible before this tick.
    let mut batch = db.open_batch();
    batch.insert("albums", vec![Value::U64(2), Value::U64(11)]);
    batch.insert("blockers", vec![Value::U64(1), Value::U64(11)]);
    db.commit_batch(batch).unwrap();
    let blocked = sub.recv().unwrap();
    assert_sink_values(
        &blocked,
        SHARED_LEFT_SINKS,
        &[(vec![Value::U64(1), Value::U64(11)], -1)],
    );

    let mut batch = db.open_batch();
    batch.delete("blockers", PrimaryKeyValue::U64(1));
    batch.delete("albums", PrimaryKeyValue::U64(2));
    db.commit_batch(batch).unwrap();
    let restored = sub.recv().unwrap();
    assert_sink_values(
        &restored,
        SHARED_LEFT_SINKS,
        &[(vec![Value::U64(1), Value::U64(11)], 1)],
    );
}

#[test]
fn shared_left_arrangement_updates_every_semi_join_consumer() {
    let (_dir, mut db) = open_db();
    let mut batch = db.open_batch();
    batch.insert("blockers", vec![Value::U64(1), Value::U64(11)]);
    db.commit_batch(batch).unwrap();

    let sub = db.subscribe(shared_left_sinks(true)).unwrap();
    let _initial = sub.recv().unwrap();

    let mut batch = db.open_batch();
    batch.insert("albums", vec![Value::U64(1), Value::U64(11)]);
    db.commit_batch(batch).unwrap();
    let inserted = sub.recv().unwrap();
    assert_sink_values(
        &inserted,
        SHARED_LEFT_SINKS,
        &[(vec![Value::U64(1), Value::U64(11)], 1)],
    );

    // Removing the match while another left row arrives retracts only the row
    // that was previously visible; the new row never enters the semi-join.
    let mut batch = db.open_batch();
    batch.insert("albums", vec![Value::U64(2), Value::U64(11)]);
    batch.delete("blockers", PrimaryKeyValue::U64(1));
    db.commit_batch(batch).unwrap();
    let unmatched = sub.recv().unwrap();
    assert_sink_values(
        &unmatched,
        SHARED_LEFT_SINKS,
        &[(vec![Value::U64(1), Value::U64(11)], -1)],
    );

    let mut batch = db.open_batch();
    batch.insert("blockers", vec![Value::U64(1), Value::U64(11)]);
    batch.delete("albums", PrimaryKeyValue::U64(2));
    db.commit_batch(batch).unwrap();
    let matched = sub.recv().unwrap();
    assert_sink_values(
        &matched,
        SHARED_LEFT_SINKS,
        &[(vec![Value::U64(1), Value::U64(11)], 1)],
    );
}
