use std::collections::BTreeMap;

mod common;

use jazz::db::{Db, DbConfig, DbIdentity};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnDescriptor, ColumnType, RowDescriptor, Schema, TableName, TableSchema,
    Value as PublicValue,
};

use common::{allow_all_policies, compile_schema};

const BIG_DEFAULT: i64 = 9_007_199_254_740_993;

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn schema() -> JazzSchema {
    let columns = RowDescriptor::new(vec![
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("count", ColumnType::BigInt)
            .default(PublicValue::BigInt(BIG_DEFAULT)),
        ColumnDescriptor::new("status", ColumnType::Text)
            .default(PublicValue::Text("queued".to_owned())),
        ColumnDescriptor::new("note", ColumnType::Text)
            .nullable()
            .default(PublicValue::Text("default note".to_owned())),
        ColumnDescriptor::new("nullable_title", ColumnType::Text)
            .nullable()
            .default(PublicValue::Null),
        ColumnDescriptor::new("nullable_count", ColumnType::Integer)
            .nullable()
            .default(PublicValue::Null),
    ]);
    let source = Schema::from([(
        TableName::new("events"),
        TableSchema::with_policies(columns, allow_all_policies()),
    )]);
    compile_schema(&source)
}

fn open_db() -> Db<TestStorage> {
    let schema = schema();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    jazz::db::block_on(Db::open(DbConfig {
        schema,
        storage: TestStorage::new(&refs),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorSubject::for_test_bytes([0xa1; 16]),
        },
        id_source: None,
    }))
    .expect("open db")
}

fn cells(values: impl IntoIterator<Item = (&'static str, Value)>) -> BTreeMap<String, Value> {
    values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn stored_row(db: &Db<TestStorage>, row_id: RowUuid) -> BTreeMap<String, Value> {
    let table = schema()
        .tables()
        .iter()
        .find(|table| table.name == "events")
        .expect("events table")
        .clone();
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
            row.cell(&table, column.name())
                .map(|value| (column.name().to_owned(), value))
        })
        .collect()
}
fn required_null_default_diagnostic(column_name: &'static str, column_type: ColumnType) -> String {
    let source = Schema::from([(
        TableName::new("events"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new(column_name, column_type).default(PublicValue::Null),
            ]),
            allow_all_policies(),
        ),
    )]);
    JazzSchema::new(&source)
        .expect_err("a required column cannot use a top-level null default")
        .to_string()
}

#[test]
fn required_text_null_default_is_rejected_during_schema_compilation() {
    let diagnostic = required_null_default_diagnostic("title", ColumnType::Text);

    assert!(
        diagnostic.contains("$.events.title"),
        "diagnostic must identify the invalid column: {diagnostic}"
    );
    assert!(
        diagnostic.to_ascii_lowercase().contains("null"),
        "diagnostic must identify the invalid null default: {diagnostic}"
    );
}

#[test]
fn required_integer_null_default_is_rejected_during_schema_compilation() {
    let diagnostic = required_null_default_diagnostic("count", ColumnType::Integer);

    assert!(
        diagnostic.contains("$.events.count"),
        "diagnostic must identify the invalid column: {diagnostic}"
    );
    assert!(
        diagnostic.to_ascii_lowercase().contains("null"),
        "diagnostic must identify the invalid null default: {diagnostic}"
    );
}

#[test]
fn nullable_null_defaults_materialize_as_null_for_omitted_columns() {
    let db = open_db();

    jazz::block_on(db.insert(
        "events",
        cells([("title", Value::String("created".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row(4)),
            ..Default::default()
        },
    ))
    .expect("insert row");

    let stored = stored_row(&db, row(4));
    assert_eq!(stored.get("nullable_title"), Some(&Value::Nullable(None)));
    assert_eq!(stored.get("nullable_count"), Some(&Value::Nullable(None)));
}

#[test]
fn core_insert_applies_literal_defaults_for_omitted_columns() {
    let db = open_db();

    jazz::block_on(db.insert(
        "events",
        cells([("title", Value::String("created".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
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

    jazz::block_on(db.insert(
        "events",
        cells([
            ("title", Value::String("created".to_owned())),
            ("note", Value::Nullable(None)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row(2)),
            ..Default::default()
        },
    ))
    .expect("insert row");

    let stored = stored_row(&db, row(2));
    assert_eq!(stored.get("note"), Some(&Value::Nullable(None)));
    assert_eq!(stored.get("count"), Some(&Value::I64(BIG_DEFAULT)));
}

#[test]
fn core_insert_keeps_explicit_values_for_defaulted_columns() {
    let db = open_db();

    jazz::block_on(db.insert(
        "events",
        cells([
            ("title", Value::String("created".to_owned())),
            ("count", Value::I64(7)),
            ("status", Value::String("done".to_owned())),
            (
                "note",
                Value::Nullable(Some(Box::new(Value::String("explicit note".to_owned())))),
            ),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row(3)),
            ..Default::default()
        },
    ))
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
