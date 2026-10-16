//! Contract: a stored row holds no copy of its primary-key columns. The
//! storage key already holds them, so the stored value is the record without
//! those fields, and every read (point, scan, index and subscription, cold or
//! warm, before and after reopen) restores them from the key.

use groove::db::{Database, Error, GraphBuilder, PrimaryKeyValue};
use groove::records::{RecordDescriptor, Value, ValueType, VariantRecord};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey,
    PrimaryKeyColumn, TableSchema,
};
use groove::storage::{MemoryStorage, OrderedKvStorage};

const BRANCH: [u8; 5] = [1, 0, 0, 0, 0];

fn schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "rows",
        [
            // Declaration order interleaves key and value columns.
            ColumnSchema::new("label", ColumnType::String),
            ColumnSchema::new("branch", ColumnType::Bytes),
            ColumnSchema::new("row", ColumnType::Uuid),
            ColumnSchema::new("count", ColumnType::U64),
            ColumnSchema::new("seq", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::bytes("branch"),
        PrimaryKeyColumn::uuid("row"),
        PrimaryKeyColumn::integer("seq", IntegerKeyType::U64),
    ]))
    .with_index(IndexSchema::new("rows_by_label", ["label"]))])
}

fn row(label: &str, row: u128, count: u64, seq: u64) -> Vec<Value> {
    vec![
        Value::String(label.to_owned()),
        Value::Bytes(BRANCH.to_vec()),
        Value::Uuid(uuid::Uuid::from_u128(row)),
        Value::U64(count),
        Value::U64(seq),
    ]
}

fn key(row: u128, seq: u64) -> Vec<u8> {
    PrimaryKeyValue::Composite(vec![
        PrimaryKeyValue::Bytes(BRANCH.to_vec()),
        PrimaryKeyValue::Uuid(uuid::Uuid::from_u128(row)),
        PrimaryKeyValue::U64(seq),
    ])
    .into_bytes()
}

async fn commit(database: &mut Database, rows: Vec<Vec<Value>>, update: bool) {
    let mut batch = database.open_batch();
    for values in rows {
        if update {
            batch.update("rows", values);
        } else {
            batch.insert("rows", values);
        }
    }
    let applied = database.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    database.finish_persistence(persisted).unwrap();
}

fn values(records: Vec<VariantRecord>) -> Vec<Vec<Value>> {
    records
        .into_iter()
        .map(|record| record.record().to_values().unwrap())
        .collect()
}

#[futures_test::test]
async fn stored_rows_leave_out_primary_key_columns_and_reads_restore_them()
-> Result<(), Box<dyn std::error::Error>> {
    let schema = schema();
    let table = schema.table("rows").unwrap();
    let stored_descriptor = table.storage_record_schema();
    assert_eq!(
        stored_descriptor
            .fields()
            .iter()
            .map(|field| field.name.as_deref().unwrap())
            .collect::<Vec<_>>(),
        ["label", "count"],
        "the stored descriptor is the record without its key columns"
    );
    let storage = MemoryStorage::new(&schema.column_families())?;
    let mut database = Database::new(schema.clone(), storage).await?;
    let warm = database
        .subscribe_one_sink(GraphBuilder::table("rows"))
        .await?;
    assert!(warm.recv()?.is_empty());

    commit(
        &mut database,
        vec![row("alpha", 1, 10, 7), row("beta", 2, 20, 8)],
        false,
    )
    .await;
    let mut inserted = warm.recv()?.to_values()?;
    inserted.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(
        inserted,
        vec![(row("alpha", 1, 10, 7), 1), (row("beta", 2, 20, 8), 1)]
    );

    // An update retracts the old row, read back from storage, in full.
    commit(&mut database, vec![row("gamma", 1, 11, 7)], true).await;
    let mut updated = warm.recv()?.to_values()?;
    updated.sort_by_key(|(_, weight)| *weight);
    assert_eq!(
        updated,
        vec![(row("alpha", 1, 10, 7), -1), (row("gamma", 1, 11, 7), 1)]
    );

    let storage = database.into_storage();
    let stored = storage.get("rows".to_owned(), key(1, 7)).await?.unwrap();
    assert_eq!(
        stored,
        groove::records::encode_variant_record(
            0,
            &stored_descriptor.create(&[Value::String("gamma".to_owned()), Value::U64(11)])?
        ),
        "the stored value holds exactly the non-key columns"
    );

    let mut database = Database::new(schema, storage).await?;
    assert_eq!(
        database
            .primary_key_get(
                "rows",
                &[
                    Value::Bytes(BRANCH.to_vec()),
                    Value::Uuid(uuid::Uuid::from_u128(2)),
                    Value::U64(8),
                ],
            )
            .await?
            .map(|record| record.record().to_values().unwrap()),
        Some(row("beta", 2, 20, 8))
    );
    assert_eq!(
        values(
            database
                .primary_key_scan("rows", &[Value::Bytes(BRANCH.to_vec())])
                .await?
        ),
        vec![row("gamma", 1, 11, 7), row("beta", 2, 20, 8)]
    );
    assert_eq!(
        values(
            database
                .index_get(
                    "rows",
                    "rows_by_label",
                    &[Value::String("gamma".to_owned())]
                )
                .await?
        ),
        vec![row("gamma", 1, 11, 7)]
    );
    let mut cold = database
        .subscribe_one_sink(GraphBuilder::table("rows"))
        .await?
        .recv()?
        .to_values()?;
    cold.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(
        cold,
        vec![(row("beta", 2, 20, 8), 1), (row("gamma", 1, 11, 7), 1)]
    );
    Ok(())
}

/// A table variant names its key column at its own position; its stored
/// payload leaves it out all the same.
#[futures_test::test]
async fn variant_rows_leave_out_their_key_field() -> Result<(), Box<dyn std::error::Error>> {
    let schema = DatabaseSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_variant(1, ["title", "id"])]);
    let table = schema.table("items").unwrap();
    assert_eq!(table.primary_key_fields_for_variant(1), Some(vec![1]));
    let stored_descriptor = table.storage_record_schema_for_variant(1).unwrap();
    let storage = MemoryStorage::new(&schema.column_families())?;
    let mut database = Database::new(schema.clone(), storage).await?;
    let v1 = RecordDescriptor::new([("title", ValueType::String), ("id", ValueType::U64)]);
    let mut batch = database.open_batch();
    batch.insert(
        "items",
        VariantRecord::create(1, v1, &[Value::String("first".into()), Value::U64(1)])?,
    );
    let applied = database.apply_batch(batch).await?;
    let persisted = applied.persist().await;
    database.finish_persistence(persisted)?;
    drop(applied);

    let storage = database.into_storage();
    assert_eq!(
        storage
            .get("items".to_owned(), PrimaryKeyValue::U64(1).into_bytes())
            .await?
            .unwrap(),
        groove::records::encode_variant_record(
            1,
            &stored_descriptor.create(&[Value::String("first".into())])?
        )
    );
    let database = Database::new(schema, storage).await?;
    let read = database
        .primary_key_get("items", &[Value::U64(1)])
        .await?
        .unwrap();
    assert_eq!(read.variant_tag(), 1);
    assert_eq!(
        read.record().to_values()?,
        vec![Value::String("first".into()), Value::U64(1)]
    );
    Ok(())
}

/// A stored row whose key does not decode as the table's primary key is
/// refused on read rather than given made-up key columns.
#[futures_test::test]
async fn a_stored_row_under_a_malformed_key_is_refused() -> Result<(), Box<dyn std::error::Error>> {
    let schema = schema();
    let storage = MemoryStorage::new(&schema.column_families())?;
    let stored_descriptor = schema.table("rows").unwrap().storage_record_schema();
    let mut malformed = key(1, 7);
    malformed.truncate(malformed.len() - 1);
    storage
        .set(
            "rows".to_owned(),
            malformed,
            groove::records::encode_variant_record(
                0,
                &stored_descriptor.create(&[Value::String("x".to_owned()), Value::U64(1)])?,
            ),
        )
        .await?;
    let database = Database::new(schema, storage).await?;
    assert!(matches!(
        database
            .primary_key_scan("rows", &[Value::Bytes(BRANCH.to_vec())])
            .await,
        Err(Error::InvalidDirectRecordStoreKey(_))
    ));
    Ok(())
}
