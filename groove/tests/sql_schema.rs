//! Integration tests for SQL schema.

use groove::sql::{ColumnDef, ColumnType, TableSchema};

#[test]
fn column_type_fixed_sizes() {
    assert_eq!(ColumnType::Bool.fixed_size(), Some(1));
    assert_eq!(ColumnType::I64.fixed_size(), Some(8));
    assert_eq!(ColumnType::F64.fixed_size(), Some(8));
    assert_eq!(ColumnType::Ref("users".into()).fixed_size(), Some(16));
    assert_eq!(ColumnType::String.fixed_size(), None);
    assert_eq!(ColumnType::Bytes.fixed_size(), None);
}

#[test]
fn column_def_constructors() {
    let required = ColumnDef::required("name", ColumnType::String);
    assert!(!required.nullable);

    let optional = ColumnDef::optional("email", ColumnType::String);
    assert!(optional.nullable);
}

#[test]
fn schema_column_lookup() {
    let schema = TableSchema::new(
        "users",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::optional("age", ColumnType::I64),
        ],
    );

    assert!(schema.column("name").is_some());
    assert!(schema.column("age").is_some());
    assert!(schema.column("unknown").is_none());

    assert_eq!(schema.column_index("name"), Some(0));
    assert_eq!(schema.column_index("age"), Some(1));
}

#[test]
fn schema_roundtrip() {
    let schema = TableSchema::new(
        "posts",
        vec![
            ColumnDef::required("author", ColumnType::Ref("users".into())),
            ColumnDef::required("title", ColumnType::String),
            ColumnDef::optional("body", ColumnType::String),
            ColumnDef::optional("views", ColumnType::I64),
            ColumnDef::required("published", ColumnType::Bool),
        ],
    );

    let bytes = schema.to_bytes();
    let parsed = TableSchema::from_bytes(&bytes).unwrap();

    assert_eq!(schema, parsed);
}

#[test]
fn variable_column_count() {
    let schema = TableSchema::new(
        "test",
        vec![
            ColumnDef::required("id", ColumnType::Ref("other".into())), // fixed
            ColumnDef::required("name", ColumnType::String),            // variable
            ColumnDef::optional("count", ColumnType::I64),              // fixed
            ColumnDef::optional("data", ColumnType::Bytes),             // variable
        ],
    );

    assert_eq!(schema.variable_column_count(), 2);
}
