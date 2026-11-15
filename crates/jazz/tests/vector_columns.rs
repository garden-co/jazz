//! `vector(n)` columns (SPEC 20.1): `n` binary32 values carried as their
//! `f32le/1` bytes, read back exactly, refused at admission when the length
//! is wrong or an element is NaN or infinite.
use std::collections::BTreeMap;

use crate::common;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnDescriptor, ColumnType, RowDescriptor, Schema, TableName, TableSchema};

use common::{allow_all_policies, compile_schema};

fn open_db() -> Db {
    let columns = RowDescriptor::new(vec![
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("embedding", ColumnType::Vector { dimensions: 3 }),
        ColumnDescriptor::new("draft", ColumnType::Vector { dimensions: 2 }).nullable(),
    ]);
    let schema = compile_schema(&Schema::from([(
        TableName::new("docs"),
        TableSchema::with_policies(columns, allow_all_policies()),
    )]));
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorSubject::for_test_bytes([0xa1; 16]),
        },
    )))
    .unwrap()
}

fn f32le(values: &[f32]) -> Value {
    Value::Bytes(values.iter().flat_map(|x| x.to_le_bytes()).collect())
}

fn insert(db: &Db, byte: u8, embedding: Value, draft: Value) -> Result<(), String> {
    let cells: BTreeMap<String, Value> = [
        ("title".to_owned(), Value::String("doc".to_owned())),
        ("embedding".to_owned(), embedding),
        ("draft".to_owned(), draft),
    ]
    .into();
    block_on(db.insert(
        "docs",
        cells,
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([byte; 16])),
            ..Default::default()
        },
    ))
    .map(|_| ())
    .map_err(|error| error.to_string())
}

/// alice writes a vector and reads back the same bits, extremes included
/// (subnormals, `-0.0`, `f32::MAX`); a null nullable vector stays null.
#[test]
fn vector_cells_round_trip_bit_exactly() {
    let db = open_db();
    let embedding = f32le(&[1e-45, -0.0, f32::MAX]);
    let draft = Value::Nullable(Some(Box::new(f32le(&[0.25, -3.5]))));
    insert(&db, 1, embedding.clone(), draft.clone()).unwrap();
    insert(&db, 2, f32le(&[1.0, 2.0, 3.0]), Value::Nullable(None)).unwrap();

    let prepared = db.prepare_query(&Query::from("docs")).unwrap();
    let mut rows = db.read(&prepared).unwrap();
    rows.sort_by_key(|row| row.row_uuid().as_bytes()[0]);
    let cell = |row: usize, name: &str| {
        rows[row]
            .cells()
            .find(|(n, _)| *n == name)
            .unwrap()
            .1
            .cloned()
    };
    assert_eq!(cell(0, "embedding"), Some(embedding));
    assert_eq!(cell(0, "draft"), Some(draft));
    assert_eq!(cell(1, "draft"), Some(Value::Nullable(None)));
}

/// mallory's writes with the wrong number of elements, a NaN or an infinity
/// are refused at admission and leave nothing behind.
#[test]
fn vector_admission_refuses_wrong_length_and_non_finite_values() {
    let db = open_db();
    let null = || Value::Nullable(None);
    for (embedding, why) in [
        (f32le(&[1.0, 2.0]), "too short"),
        (f32le(&[1.0, 2.0, 3.0, 4.0]), "too long"),
        (Value::Bytes(vec![0; 11]), "not a multiple of four bytes"),
        (f32le(&[1.0, f32::NAN, 3.0]), "NaN"),
        (f32le(&[f32::INFINITY, 0.0, 0.0]), "infinity"),
        (Value::String("abc".to_owned()), "not bytes"),
    ] {
        assert!(insert(&db, 3, embedding, null()).is_err(), "{why}");
    }
    assert!(
        insert(
            &db,
            4,
            f32le(&[0.0; 3]),
            Value::Nullable(Some(Box::new(f32le(&[f32::NEG_INFINITY, 0.0]))))
        )
        .is_err(),
        "nullable vectors are checked too"
    );
    let prepared = db.prepare_query(&Query::from("docs")).unwrap();
    assert!(db.read(&prepared).unwrap().is_empty());
}

/// Outside a vector index search (SPEC 20.4), a filter compares vectors as
/// whole values, bit for bit, as for `bytes`: `-0.0` and `0.0` differ.
#[test]
fn vector_filters_compare_whole_values_bit_for_bit() {
    use jazz::query::{Operand, Predicate};
    let db = open_db();
    let null = || Value::Nullable(None);
    insert(&db, 1, f32le(&[1.0, 2.0, 3.0]), null()).unwrap();
    insert(&db, 2, f32le(&[0.0, 2.0, 3.0]), null()).unwrap();
    insert(&db, 3, f32le(&[-0.0, 2.0, 3.0]), null()).unwrap();
    let ids = |literal: Value| {
        let query = Query::from("docs").filter(Predicate::Eq(
            Operand::Column("embedding".to_owned()),
            Operand::Literal(literal),
        ));
        let prepared = db.prepare_query(&query).unwrap();
        let mut ids: Vec<u8> = db
            .read(&prepared)
            .unwrap()
            .iter()
            .map(|row| row.row_uuid().as_bytes()[0])
            .collect();
        ids.sort();
        ids
    };
    assert_eq!(ids(f32le(&[1.0, 2.0, 3.0])), [1]);
    assert_eq!(ids(f32le(&[0.0, 2.0, 3.0])), [2]);
    assert_eq!(ids(f32le(&[-0.0, 2.0, 3.0])), [3]);
}
