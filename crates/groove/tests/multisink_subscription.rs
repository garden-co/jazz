//! Integration tests for direct multisink graph subscriptions.
//!
//! This is a lower-level Groove test because multisink delivery is executor
//! behavior that is not yet exposed through the Jazz public API.

use std::sync::mpsc::TryRecvError;

use groove::db::{Database, GraphBuilder};
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

fn albums_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("year", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn database() -> Database<MemoryStorage> {
    Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap()
}

fn insert_album(db: &mut Database<MemoryStorage>, id: u64, title: &str, year: u64) {
    let mut batch = db.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(id),
            Value::String(title.to_owned()),
            Value::U64(year),
        ],
    );
    db.commit_batch(batch).unwrap();
}

#[test]
fn multisink_subscription_delivers_initial_and_tick_deltas_for_all_sinks() {
    let mut db = database();
    insert_album(&mut db, 1, "Kind of Blue", 1959);

    let subscription = db
        .subscribe_multisink([
            ("rows", GraphBuilder::table("albums")),
            (
                "years",
                GraphBuilder::table("albums").project(["id", "year"]),
            ),
        ])
        .unwrap();

    let initial = subscription.recv().unwrap();
    assert_eq!(
        initial.get("rows").unwrap().to_values().unwrap(),
        [(
            vec![
                Value::U64(1),
                Value::String("Kind of Blue".to_owned()),
                Value::U64(1959),
            ],
            1,
        )]
    );
    assert_eq!(
        initial.get("years").unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(1959)], 1)]
    );

    insert_album(&mut db, 2, "Blue Train", 1957);

    let tick = subscription.recv().unwrap();
    assert_eq!(tick.sinks.len(), 2);
    assert_eq!(
        tick.get("rows").unwrap().to_values().unwrap(),
        [(
            vec![
                Value::U64(2),
                Value::String("Blue Train".to_owned()),
                Value::U64(1957),
            ],
            1,
        )]
    );
    assert_eq!(
        tick.get("years").unwrap().to_values().unwrap(),
        [(vec![Value::U64(2), Value::U64(1957)], 1)]
    );
}

#[test]
fn unsubscribing_multisink_subscription_closes_the_whole_stream() {
    let mut db = database();
    let subscription = db
        .subscribe_multisink([
            ("rows", GraphBuilder::table("albums")),
            (
                "years",
                GraphBuilder::table("albums").project(["id", "year"]),
            ),
        ])
        .unwrap();
    let initial = subscription.recv().unwrap();
    assert!(initial.get("rows").unwrap().is_empty());
    assert!(initial.get("years").unwrap().is_empty());

    assert!(db.unsubscribe(subscription.id()));
    insert_album(&mut db, 1, "Kind of Blue", 1959);

    assert!(matches!(
        subscription.try_recv(),
        Err(TryRecvError::Disconnected)
    ));
}
