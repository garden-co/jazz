//! Anti-join same-tick cross-term regressions: when a left row and its
//! matching right row arrive in the same commit, the left row arrives
//! suppressed — nothing must be emitted, and especially not a retraction
//! of a row that was never inserted downstream. Symmetrically, a left row
//! arriving in the same tick as the retraction of its last blocker must be
//! emitted exactly once.

use std::collections::BTreeMap;

use groove::db::{Database, GraphBuilder, PrimaryKeyValue};
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

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

async fn open_db() -> Database {
    let storage =
        MemoryStorage::new(&["albums", "blockers"]).expect("valid memory storage families");
    let db = Database::new(schema(), storage).await.unwrap();
    db
}

fn anti_join() -> GraphBuilder {
    GraphBuilder::anti_join(
        GraphBuilder::table("albums"),
        GraphBuilder::table("blockers"),
        ["artist_id"],
        ["artist_id"],
    )
}

#[futures_test::test]
async fn same_tick_left_and_blocking_right_emit_nothing() {
    let mut db = open_db().await;
    let sub = db.subscribe_one_sink(anti_join()).await.unwrap();
    let _initial = sub.recv().unwrap();

    // Album and its blocker arrive in one commit: the album is suppressed
    // from the start and must never appear, positively or negatively.
    let mut batch = db.open_batch();
    batch.insert("albums", vec![Value::U64(1), Value::U64(11)]);
    batch.insert("blockers", vec![Value::U64(1), Value::U64(11)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

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

#[futures_test::test]
async fn same_tick_left_insert_and_last_blocker_retraction_emit_once() {
    let mut db = open_db().await;
    let sub = db.subscribe_one_sink(anti_join()).await.unwrap();
    let _initial = sub.recv().unwrap();

    // Pre-existing blocked album.
    let mut batch = db.open_batch();
    batch.insert("albums", vec![Value::U64(1), Value::U64(11)]);
    batch.insert("blockers", vec![Value::U64(1), Value::U64(11)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();
    while sub.try_recv().is_ok() {}

    // One commit: a second album for the artist arrives while the artist's
    // last blocker is deleted. Both albums become visible exactly once.
    let mut batch = db.open_batch();
    batch.insert("albums", vec![Value::U64(2), Value::U64(11)]);
    batch.delete("blockers", PrimaryKeyValue::U64(1));
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

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
