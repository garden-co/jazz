//! Public behavior of batch-local preparation across reads, writes and owners.
use groove::db::{Database, DatabaseBatch, EnsureExactOutcome, GraphBuilder, PrimaryKeyValue};
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

fn table(name: &str, payload: ColumnType) -> TableSchema {
    TableSchema::new(
        name,
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", payload),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
}

async fn database(payload: ColumnType) -> Database {
    Database::new(
        DatabaseSchema::new([table("rows", payload)]),
        MemoryStorage::new(&["rows", "extra"]).unwrap(),
    )
    .await
    .unwrap()
}

async fn persist(database: &mut Database, batch: DatabaseBatch) {
    let applied = database.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    database.finish_persistence(persisted).unwrap();
}

#[futures_test::test]
async fn prepared_prefix_observes_overwrites_deletes_and_independent_clones() {
    let mut db = database(ColumnType::String).await;
    let mut batch = db.open_batch();
    batch.insert("rows", vec![Value::U64(1), Value::String("first".into())]);
    assert_eq!(
        db.primary_key_get_raw_in_batch(&batch, "rows", &[Value::U64(1)])
            .await
            .unwrap()
            .unwrap()
            .record()
            .get("payload")
            .unwrap(),
        Value::String("first".into())
    );
    let mut sibling = batch.clone();
    batch.update("rows", vec![Value::U64(1), Value::String("second".into())]);
    batch.insert("rows", vec![Value::U64(2), Value::String("deleted".into())]);
    assert_eq!(
        db.primary_key_scan_raw_in_batch(&batch, "rows", &[])
            .await
            .unwrap()
            .len(),
        2
    );
    batch.delete("rows", PrimaryKeyValue::U64(2));
    assert!(
        db.primary_key_get_raw_in_batch(&batch, "rows", &[Value::U64(2)])
            .await
            .unwrap()
            .is_none()
    );
    persist(&mut db, batch).await;
    assert_eq!(
        db.primary_key_scan("rows", &[]).await.unwrap()[0]
            .get("payload")
            .unwrap(),
        Value::String("second".into())
    );
    // A prepared record is not a proof that the old row is absent at commit.
    sibling.update("rows", vec![Value::U64(1), Value::String("sibling".into())]);
    assert!(db.apply_batch(sibling.clone()).await.is_err());
    let rows = db.primary_key_scan("rows", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("payload").unwrap(),
        Value::String("second".into())
    );
    let mut other = database(ColumnType::String).await;
    persist(&mut other, sibling).await;
    assert_eq!(
        other.primary_key_scan("rows", &[]).await.unwrap()[0]
            .get("payload")
            .unwrap(),
        Value::String("sibling".into())
    );
}

#[futures_test::test]
async fn preparation_cannot_cross_database_schema_owners() {
    let db = database(ColumnType::String).await;
    let mut other = database(ColumnType::U64).await;
    let mut batch = db.open_batch();
    batch.insert(
        "rows",
        vec![Value::U64(1), Value::String("wrong schema".into())],
    );
    db.primary_key_get_raw_in_batch(&batch, "rows", &[Value::U64(1)])
        .await
        .unwrap();
    // Check commit directly, before an overlay read has an opportunity to
    // discard the foreign preparation.
    assert!(other.apply_batch(batch.clone()).await.is_err());
    assert!(
        other
            .primary_key_get_raw_in_batch(&batch, "rows", &[Value::U64(1)])
            .await
            .is_err()
    );
    assert!(other.apply_batch(batch).await.is_err());
    assert!(
        other
            .primary_key_scan("rows", &[])
            .await
            .unwrap()
            .is_empty()
    );
}

#[futures_test::test]
async fn prepared_update_uses_commit_time_predecessor_for_subscription_deltas() {
    let mut db = database(ColumnType::String).await;
    let mut seed = db.open_batch();
    seed.insert("rows", vec![Value::U64(1), Value::String("old".into())]);
    persist(&mut db, seed).await;
    let subscription = db
        .subscribe_one_sink(GraphBuilder::table("rows"))
        .await
        .unwrap();
    db.next_subscription(&subscription).await.unwrap();
    let mut later = db.open_batch();
    later.update("rows", vec![Value::U64(1), Value::String("last".into())]);
    db.primary_key_get_raw_in_batch(&later, "rows", &[Value::U64(1)])
        .await
        .unwrap();
    let mut intervening = db.open_batch();
    intervening.update("rows", vec![Value::U64(1), Value::String("middle".into())]);
    persist(&mut db, intervening).await;
    db.next_subscription(&subscription).await.unwrap();
    persist(&mut db, later).await;
    let delta = db
        .next_subscription(&subscription)
        .await
        .unwrap()
        .to_values()
        .unwrap();
    assert_eq!(delta.len(), 2);
    assert!(delta.contains(&(vec![Value::U64(1), Value::String("middle".into())], -1)));
    assert!(delta.contains(&(vec![Value::U64(1), Value::String("last".into())], 1)));
}

#[futures_test::test]
async fn schema_rollback_rejects_prepared_records_from_removed_tables() {
    let mut db = database(ColumnType::String).await;
    let checkpoint = db.runtime_registry_checkpoint();
    db.register_table(table("extra", ColumnType::String))
        .unwrap();
    let mut batch = db.open_batch();
    batch.insert(
        "extra",
        vec![Value::U64(1), Value::String("removed".into())],
    );
    db.primary_key_get_raw_in_batch(&batch, "extra", &[Value::U64(1)])
        .await
        .unwrap();
    db.restore_runtime_registry(checkpoint);
    assert!(db.apply_batch(batch).await.is_err());
    db.register_table(table("extra", ColumnType::String))
        .unwrap();
    assert!(db.primary_key_scan("extra", &[]).await.unwrap().is_empty());
}

#[futures_test::test]
async fn prepared_exact_insert_still_rejects_later_conflicting_write() {
    let mut db = database(ColumnType::String).await;
    let descriptor = db.table_schema("rows").unwrap().record_schema();
    let raw = descriptor
        .create(&[Value::U64(1), Value::String("immutable".into())])
        .unwrap();
    let mut batch = db.open_batch();
    assert_eq!(
        batch
            .ensure_exact(&db, "rows", PrimaryKeyValue::U64(1), raw.clone())
            .await
            .unwrap(),
        EnsureExactOutcome::Inserted
    );
    assert_eq!(
        batch
            .ensure_exact(&db, "rows", PrimaryKeyValue::U64(1), raw)
            .await
            .unwrap(),
        EnsureExactOutcome::AlreadyIdentical
    );
    batch.update(
        "rows",
        vec![Value::U64(1), Value::String("conflict".into())],
    );
    assert!(db.apply_batch(batch).await.is_err());
    assert!(db.primary_key_scan("rows", &[]).await.unwrap().is_empty());
}
