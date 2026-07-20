use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity};
use jazz::groove::records::Value;
use jazz::groove::schema::ColumnType;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::schema::{ColumnSchema, JazzSchema, Policy, TableSchema};

const BIG_DEFAULT: i64 = 9_007_199_254_740_993;

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "events",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("count", ColumnType::I64).with_default(Value::I64(BIG_DEFAULT)),
            ColumnSchema::new("status", ColumnType::String)
                .with_default(Value::String("queued".to_owned())),
            ColumnSchema::new("note", ColumnType::String.nullable())
                .with_default(Value::String("default note".to_owned())),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn open_db() -> Db<MemoryStorage> {
    let schema = schema();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    jazz::db::block_on(Db::open(DbConfig {
        schema,
        storage: MemoryStorage::new(&refs),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorId::from_bytes([0xa1; 16]),
        },
        id_source: None,
        large_value_checkpoint_op_interval: 1024,
    }))
    .expect("open db")
}

fn cells(values: impl IntoIterator<Item = (&'static str, Value)>) -> BTreeMap<String, Value> {
    values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn stored_row(db: &Db<MemoryStorage>, row_id: RowUuid) -> BTreeMap<String, Value> {
    let table = schema()
        .tables
        .into_iter()
        .find(|table| table.name == "events")
        .expect("events table");
    let prepared = db
        .prepare_query(&db.table("events"))
        .expect("prepare query");
    let rows = db.read(&prepared).expect("read rows");
    let row = rows
        .iter()
        .find(|row| row.row_uuid() == row_id)
        .expect("stored row");
    table
        .columns
        .iter()
        .filter_map(|column| {
            row.cell(&table, &column.name)
                .map(|value| (column.name.clone(), value))
        })
        .collect()
}

#[test]
fn core_insert_applies_literal_defaults_for_omitted_columns() {
    let db = open_db();

    db.insert_with_id(
        "events",
        row(1),
        cells([("title", Value::String("created".to_owned()))]),
    )
    .expect("insert row");

    let stored = stored_row(&db, row(1));
    assert_eq!(
        stored.get("title"),
        Some(&Value::String("created".to_owned()))
    );
    assert_eq!(stored.get("count"), Some(&Value::I64(BIG_DEFAULT)));
    assert_eq!(
        stored.get("status"),
        Some(&Value::String("queued".to_owned()))
    );
    assert_eq!(
        stored.get("note"),
        Some(&Value::Nullable(Some(Box::new(Value::String(
            "default note".to_owned()
        )))))
    );
}

#[test]
fn core_insert_preserves_explicit_null_instead_of_using_default() {
    let db = open_db();

    db.insert_with_id(
        "events",
        row(2),
        cells([
            ("title", Value::String("created".to_owned())),
            ("note", Value::Nullable(None)),
        ]),
    )
    .expect("insert row");

    let stored = stored_row(&db, row(2));
    assert_eq!(stored.get("note"), Some(&Value::Nullable(None)));
    assert_eq!(stored.get("count"), Some(&Value::I64(BIG_DEFAULT)));
}

#[test]
fn core_insert_keeps_explicit_values_for_defaulted_columns() {
    let db = open_db();

    db.insert_with_id(
        "events",
        row(3),
        cells([
            ("title", Value::String("created".to_owned())),
            ("count", Value::I64(7)),
            ("status", Value::String("done".to_owned())),
            (
                "note",
                Value::Nullable(Some(Box::new(Value::String("explicit note".to_owned())))),
            ),
        ]),
    )
    .expect("insert row");

    let stored = stored_row(&db, row(3));
    assert_eq!(stored.get("count"), Some(&Value::I64(7)));
    assert_eq!(
        stored.get("status"),
        Some(&Value::String("done".to_owned()))
    );
    assert_eq!(
        stored.get("note"),
        Some(&Value::Nullable(Some(Box::new(Value::String(
            "explicit note".to_owned()
        )))))
    );
}
