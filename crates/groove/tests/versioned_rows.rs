use groove::db::{Database, Error};
use groove::records::{Value, VersionedRecord};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{MemoryStorage, OrderedKvStorage};

fn versioned_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("completed", ColumnType::Bool),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    // A version's logical field order is independent from catalogue order.
    .with_schema_version(1, ["title", "id"])
    .with_schema_version(2, ["id", "title", "completed"])])
}

fn open_database() -> Result<Database<MemoryStorage>, Error> {
    let schema = versioned_schema();
    let storage = MemoryStorage::new(&schema.column_families());
    Database::new(schema, storage)
}

fn row(version: u64, values: &[Value]) -> VersionedRecord {
    let schema = versioned_schema();
    let descriptor = schema
        .table("items")
        .unwrap()
        .record_schema_for_version(version)
        .expect("registered test version");
    VersionedRecord::create(version, descriptor, values).expect("valid test row")
}

#[test]
fn mixed_schema_versions_survive_replacement_and_reopen() -> Result<(), Box<dyn std::error::Error>>
{
    let mut database = open_database()?;
    let mut batch = database.open_batch();
    batch.insert(
        "items",
        row(1, &[Value::String("first".into()), Value::U64(1)]),
    );
    batch.insert(
        "items",
        row(
            2,
            &[
                Value::U64(2),
                Value::String("second".into()),
                Value::Bool(false),
            ],
        ),
    );
    database.commit_batch(batch)?;

    let rows = database.primary_key_scan("items", &[])?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].schema_version(), 1);
    assert_eq!(rows[0].get("title")?, Value::String("first".into()));
    assert!(rows[0].get("completed").is_err());
    assert_eq!(rows[1].schema_version(), 2);
    assert_eq!(rows[1].get("completed")?, Value::Bool(false));

    let mut batch = database.open_batch();
    batch.update(
        "items",
        row(
            2,
            &[
                Value::U64(1),
                Value::String("first, revised".into()),
                Value::Bool(true),
            ],
        ),
    );
    database.commit_batch(batch)?;
    let replaced = database
        .primary_key_get("items", &[Value::U64(1)])?
        .expect("updated row exists");
    assert_eq!(replaced.schema_version(), 2);
    assert_eq!(
        replaced.get("title")?,
        Value::String("first, revised".into())
    );
    assert_eq!(replaced.get("completed")?, Value::Bool(true));

    let storage = database.into_storage();
    let reopened = Database::new(versioned_schema(), storage)?;
    let rows = reopened.primary_key_scan("items", &[])?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].schema_version(), 2);
    assert_eq!(rows[1].schema_version(), 2);
    assert_eq!(rows[1].get("completed")?, Value::Bool(false));
    Ok(())
}

#[test]
fn version_header_is_fixed_width_and_validated_on_read() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = open_database()?;
    let mut batch = database.open_batch();
    batch.insert(
        "items",
        row(1, &[Value::String("first".into()), Value::U64(1)]),
    );
    database.commit_batch(batch)?;

    let storage = database.into_storage();
    let entries = storage.prefix("items", b"")?;
    let (key, stored) = entries.first().expect("stored row");
    assert_eq!(&stored[..8], &1_u64.to_le_bytes());

    storage.set("items", key, &[1, 2, 3])?;
    let reopened = Database::new(versioned_schema(), storage)?;
    assert!(matches!(
        reopened.primary_key_get("items", &[Value::U64(1)]),
        Err(Error::RecordEncoding(
            groove::records::Error::InvalidSchemaVersionHeader
        ))
    ));
    Ok(())
}

#[test]
fn writes_reject_unregistered_schema_versions() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = open_database()?;
    let mut batch = database.open_batch();
    batch.insert(
        "items",
        VersionedRecord::new(
            3,
            groove::records::OwnedRecord::new(
                Vec::new(),
                versioned_schema().table("items").unwrap().record_schema(),
            ),
        ),
    );
    assert!(matches!(
        database.commit_batch(batch),
        Err(Error::UnknownTableSchemaVersion { table, version })
            if table == "items" && version == 3
    ));
    Ok(())
}

#[test]
fn explicit_registries_cannot_claim_reserved_version_zero() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "items",
        [ColumnSchema::new("id", ColumnType::U64)],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_schema_version(0, ["id"])]);
    let storage = MemoryStorage::new(&schema.column_families());
    assert!(matches!(
        Database::new(schema, storage),
        Err(Error::ReservedTableSchemaVersion(table)) if table == "items"
    ));
}

#[test]
fn homogeneous_layouts_retain_nonzero_versions_through_windows()
-> Result<(), Box<dyn std::error::Error>> {
    let table = TableSchema::new(
        "jazz_items_history",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64));
    let descriptor = table.record_schema();
    let schema = DatabaseSchema::new([table]);
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema, storage)?;

    let mut batch = database.open_batch();
    for id in 1..=4 {
        batch.insert(
            "jazz_items_history",
            VersionedRecord::create(
                7,
                descriptor,
                &[Value::U64(id), Value::String(format!("item {id}"))],
            )?,
        );
    }
    database.commit_batch(batch)?;
    database.consolidate_table_windows("jazz_items_history", 2)?;

    let rows = database.primary_key_scan("jazz_items_history", &[])?;
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(|row| row.schema_version() == 7));
    assert_eq!(rows[2].get("title")?, Value::String("item 3".into()));
    Ok(())
}

#[test]
fn heterogeneous_layouts_survive_window_consolidation() -> Result<(), Box<dyn std::error::Error>> {
    let mut table = versioned_schema().table("items").unwrap().clone();
    table.name = "jazz_items_history".into();
    let schema = DatabaseSchema::new([table]);
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema, storage)?;

    let mut batch = database.open_batch();
    batch.insert(
        "jazz_items_history",
        row(1, &[Value::String("first".into()), Value::U64(1)]),
    );
    batch.insert(
        "jazz_items_history",
        row(
            2,
            &[
                Value::U64(2),
                Value::String("second".into()),
                Value::Bool(true),
            ],
        ),
    );
    database.commit_batch(batch)?;
    database.consolidate_table_windows("jazz_items_history", 2)?;

    let rows = database.primary_key_scan("jazz_items_history", &[])?;
    assert_eq!(rows[0].schema_version(), 1);
    assert!(rows[0].get("completed").is_err());
    assert_eq!(rows[1].schema_version(), 2);
    assert_eq!(rows[1].get("completed")?, Value::Bool(true));
    Ok(())
}
