//! Array columns on SQLite: stored as JSON text, read back by their
//! declared element type, compared whole and searched by element.
use std::collections::BTreeMap;

mod common;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Operand, Predicate, Query};
use jazz::records::Value;
use jazz::tools::{ColumnDescriptor, ColumnType, RowDescriptor, Schema, TableName, TableSchema};

use common::{allow_all_policies, compile_schema};

fn array(element: ColumnType) -> ColumnType {
    ColumnType::Array {
        element: Box::new(element),
    }
}

fn open_db() -> Db {
    let columns = RowDescriptor::new(vec![
        ColumnDescriptor::new("tags", array(ColumnType::Text)),
        ColumnDescriptor::new("owners", array(ColumnType::Uuid)),
        ColumnDescriptor::new("grid", array(array(ColumnType::Integer))),
        ColumnDescriptor::new("scores", array(ColumnType::Double)).nullable(),
    ]);
    let schema = compile_schema(&Schema::from([(
        TableName::new("posts"),
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

fn strings(items: &[&str]) -> Value {
    Value::Array(
        items
            .iter()
            .map(|s| Value::String((*s).to_owned()))
            .collect(),
    )
}

fn insert(db: &Db, byte: u8, cells: Vec<(&str, Value)>) {
    let cells: BTreeMap<String, Value> = cells
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
    block_on(db.insert(
        "posts",
        cells,
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([byte; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn ids(db: &Db, query: Query) -> Vec<u8> {
    let prepared = db.prepare_query(&query).unwrap();
    let mut ids: Vec<u8> = db
        .read(&prepared)
        .unwrap()
        .iter()
        .map(|row| row.row_uuid().as_bytes()[0])
        .collect();
    ids.sort();
    ids
}

fn column(name: &str) -> Operand {
    Operand::Column(name.to_owned())
}

#[test]
fn array_cells_round_trip_by_their_declared_element_types() {
    let db = open_db();
    let owner = uuid::Uuid::from_bytes([7; 16]);
    let grid = Value::Array(vec![
        Value::Array(vec![Value::I32(1), Value::I32(2)]),
        Value::Array(vec![]),
    ]);
    insert(
        &db,
        1,
        vec![
            ("tags", strings(&["a", "b \"quoted\""])),
            ("owners", Value::Array(vec![Value::Uuid(owner)])),
            ("grid", grid.clone()),
            (
                "scores",
                Value::Nullable(Some(Box::new(Value::Array(vec![
                    Value::F64(1.0),
                    Value::F64(2.5),
                ])))),
            ),
        ],
    );
    let prepared = db.prepare_query(&Query::from("posts")).unwrap();
    let rows = db.read(&prepared).unwrap();
    let cell = |name: &str| {
        rows[0]
            .cells()
            .find(|(n, _)| *n == name)
            .unwrap()
            .1
            .cloned()
    };
    assert_eq!(cell("tags"), Some(strings(&["a", "b \"quoted\""])));
    assert_eq!(cell("owners"), Some(Value::Array(vec![Value::Uuid(owner)])));
    assert_eq!(cell("grid"), Some(grid));
    assert_eq!(
        cell("scores"),
        Some(Value::Nullable(Some(Box::new(Value::Array(vec![
            Value::F64(1.0),
            Value::F64(2.5)
        ])))))
    );
}

#[test]
fn array_filters_compare_whole_arrays_and_search_elements() {
    let db = open_db();
    let owner = uuid::Uuid::from_bytes([7; 16]);
    let empty = Value::Array(vec![]);
    insert(
        &db,
        1,
        vec![
            ("tags", strings(&["red", "blue"])),
            ("owners", Value::Array(vec![Value::Uuid(owner)])),
            ("grid", empty.clone()),
        ],
    );
    insert(
        &db,
        2,
        vec![
            ("tags", strings(&[])),
            ("owners", empty.clone()),
            ("grid", empty.clone()),
        ],
    );

    let filtered = |predicate| ids(&db, Query::from("posts").filter(predicate));
    let literal = |value: Value| Operand::Literal(value);
    assert_eq!(
        filtered(Predicate::Contains(
            column("tags"),
            literal(Value::String("blue".into()))
        )),
        [1]
    );
    assert_eq!(
        filtered(Predicate::Contains(
            column("tags"),
            literal(Value::String("blu".into()))
        )),
        Vec::<u8>::new()
    );
    assert_eq!(
        filtered(Predicate::Contains(
            column("owners"),
            literal(Value::Uuid(owner))
        )),
        [1]
    );
    assert_eq!(
        filtered(Predicate::Eq(column("tags"), literal(strings(&[])))),
        [2]
    );
    assert_eq!(
        filtered(Predicate::Eq(
            column("tags"),
            literal(strings(&["red", "blue"]))
        )),
        [1]
    );
}
