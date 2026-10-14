//! Grouped operator state (arg-max/min, top-by, collect) is counted in
//! `RuntimeStats` alongside arrangements, so memory accounting sees it.

use groove::db::{Database, GraphBuilder, PrimaryKeyValue};
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, PrimaryKeyColumn,
    TableSchema,
};
use groove::storage::MemoryStorage;

fn schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("artist_id", ColumnType::U64),
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("artist_id", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("id", IntegerKeyType::U64),
    ]))])
}

fn album(id: u64, artist: u64) -> Vec<Value> {
    vec![
        Value::U64(artist),
        Value::U64(id),
        Value::String(format!("album {id}")),
    ]
}

fn key(artist: u64, id: u64) -> PrimaryKeyValue {
    PrimaryKeyValue::Composite(vec![PrimaryKeyValue::U64(artist), PrimaryKeyValue::U64(id)])
}

async fn apply(db: &mut Database, edit: impl FnOnce(&mut groove::db::DatabaseBatch)) {
    let mut batch = db.open_batch();
    edit(&mut batch);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();
}

#[futures_test::test]
async fn grouped_operator_state_is_counted() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid families");
    let mut db = Database::new(schema(), storage).await.unwrap();
    apply(&mut db, |b| {
        b.insert("albums", album(1, 10));
        b.insert("albums", album(2, 10));
        b.insert("albums", album(3, 20));
    })
    .await;
    let empty = db.runtime_stats();
    assert_eq!(empty.operator_state_count, 0);

    let latest = GraphBuilder::arg_max_by(GraphBuilder::table("albums"), ["artist_id"], ["id"]);
    let sub = db.subscribe_one_sink(latest).await.unwrap();
    sub.recv().unwrap();
    let held = db.runtime_stats();
    assert_eq!(held.operator_state_count, 1);
    assert_eq!(held.operator_state_groups, 2);
    assert_eq!(held.operator_state_rows, 3);
    assert!(held.operator_state_encoded_bytes > 0);

    apply(&mut db, |b| b.delete("albums", key(10, 1))).await;
    let after = db.runtime_stats();
    assert_eq!(after.operator_state_groups, 2);
    assert_eq!(after.operator_state_rows, 2);
    assert!(after.operator_state_encoded_bytes < held.operator_state_encoded_bytes);

    apply(&mut db, |b| b.delete("albums", key(20, 3))).await;
    assert_eq!(db.runtime_stats().operator_state_groups, 1);
}
