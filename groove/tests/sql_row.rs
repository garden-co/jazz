//! Integration tests for SQL row encoding/decoding.

use groove::sql::{decode_row, encode_row, ColumnDef, ColumnType, RowError, TableSchema, Value};

#[test]
fn encode_decode_simple_row() {
    let schema = TableSchema::new(
        "users",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("age", ColumnType::I64),
            ColumnDef::required("active", ColumnType::Bool),
        ],
    );

    let values = vec![
        Value::String("Alice".into()),
        Value::I64(30),
        Value::Bool(true),
    ];

    let encoded = encode_row(&values, &schema).unwrap();
    let decoded = decode_row(&encoded, &schema).unwrap();

    assert_eq!(values, decoded);
}

#[test]
fn encode_decode_with_nulls() {
    let schema = TableSchema::new(
        "users",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::optional("email", ColumnType::String),
            ColumnDef::optional("age", ColumnType::I64),
        ],
    );

    let values = vec![
        Value::String("Bob".into()),
        Value::Null,
        Value::I64(25),
    ];

    let encoded = encode_row(&values, &schema).unwrap();
    let decoded = decode_row(&encoded, &schema).unwrap();

    assert_eq!(values, decoded);
}

#[test]
fn encode_decode_all_nulls() {
    let schema = TableSchema::new(
        "test",
        vec![
            ColumnDef::optional("a", ColumnType::String),
            ColumnDef::optional("b", ColumnType::I64),
            ColumnDef::optional("c", ColumnType::Bool),
        ],
    );

    let values = vec![Value::Null, Value::Null, Value::Null];

    let encoded = encode_row(&values, &schema).unwrap();
    let decoded = decode_row(&encoded, &schema).unwrap();

    assert_eq!(values, decoded);
}

#[test]
fn encode_decode_with_refs() {
    let schema = TableSchema::new(
        "posts",
        vec![
            ColumnDef::required("author", ColumnType::Ref("users".into())),
            ColumnDef::required("title", ColumnType::String),
        ],
    );

    let author_id: u128 = 0x0192_abcd_1234_5678_9abc_def0_1234_5678;
    let values = vec![
        Value::Ref(author_id),
        Value::String("Hello World".into()),
    ];

    let encoded = encode_row(&values, &schema).unwrap();
    let decoded = decode_row(&encoded, &schema).unwrap();

    assert_eq!(values, decoded);
}

#[test]
fn encode_decode_f64() {
    let schema = TableSchema::new(
        "data",
        vec![ColumnDef::required("value", ColumnType::F64)],
    );

    let values = vec![Value::F64(3.14159265358979)];

    let encoded = encode_row(&values, &schema).unwrap();
    let decoded = decode_row(&encoded, &schema).unwrap();

    assert_eq!(values, decoded);
}

#[test]
fn encode_decode_bytes() {
    let schema = TableSchema::new(
        "files",
        vec![ColumnDef::required("data", ColumnType::Bytes)],
    );

    let values = vec![Value::Bytes(vec![0x00, 0xff, 0x42, 0x13])];

    let encoded = encode_row(&values, &schema).unwrap();
    let decoded = decode_row(&encoded, &schema).unwrap();

    assert_eq!(values, decoded);
}

#[test]
fn null_in_non_nullable_fails() {
    let schema = TableSchema::new(
        "users",
        vec![ColumnDef::required("name", ColumnType::String)],
    );

    let values = vec![Value::Null];

    let result = encode_row(&values, &schema);
    assert!(result.is_err());
}

#[test]
fn column_count_mismatch_fails() {
    let schema = TableSchema::new(
        "users",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("age", ColumnType::I64),
        ],
    );

    let values = vec![Value::String("Alice".into())];

    let result = encode_row(&values, &schema);
    assert!(matches!(result, Err(RowError::ColumnCountMismatch { .. })));
}

#[test]
fn type_mismatch_fails() {
    let schema = TableSchema::new(
        "users",
        vec![ColumnDef::required("age", ColumnType::I64)],
    );

    let values = vec![Value::String("not a number".into())];

    let result = encode_row(&values, &schema);
    assert!(matches!(result, Err(RowError::TypeMismatch { .. })));
}
