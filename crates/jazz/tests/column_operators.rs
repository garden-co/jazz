//! What each column type supports in a query, as `prepare` checks it: a
//! JSON or bytes column takes no range comparison and can't order a query,
//! and JSON takes no `contains`; every other type keeps what the engine
//! supports. One
//! checker for every client (#5927 R13): TS, the FFI and the inspector
//! leave it to the engine.

use crate::common;

use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::{self as q, ArraySubquery, OrderDirection, Query};
use jazz::records::Value;
use jazz::tools::{ColumnDescriptor, ColumnType, RowDescriptor, Schema, TableName, TableSchema};

use common::{allow_all_policies, compile_schema};

fn open_db() -> Db {
    let things = RowDescriptor::new(vec![
        ColumnDescriptor::new("label", ColumnType::Text),
        ColumnDescriptor::new("flag", ColumnType::Boolean),
        ColumnDescriptor::new("meta", ColumnType::Json { schema: None }),
        ColumnDescriptor::new("blob", ColumnType::Bytea),
        ColumnDescriptor::new(
            "tags",
            ColumnType::Array {
                element: Box::new(ColumnType::Text),
            },
        ),
    ]);
    let parts = RowDescriptor::new(vec![
        ColumnDescriptor::new("thing", ColumnType::Uuid),
        ColumnDescriptor::new("meta", ColumnType::Json { schema: None }),
    ]);
    let schema = compile_schema(&Schema::from([
        (
            TableName::new("things"),
            TableSchema::with_policies(things, allow_all_policies()),
        ),
        (
            TableName::new("parts"),
            TableSchema::with_policies(parts, allow_all_policies()),
        ),
    ]));
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x12; 16]),
            author: AuthorSubject::for_test_bytes([0xa2; 16]),
        },
    )))
    .unwrap()
}

fn text(value: &str) -> q::Operand {
    q::lit(Value::String(value.to_owned()))
}

fn refusal(db: &Db, query: Query) -> String {
    match db.prepare_query(&query) {
        Err(error) => {
            assert_eq!(error.code, ErrorCode::Query, "{error}");
            error.to_string()
        }
        Ok(_) => panic!("expected {query:?} to be refused at prepare"),
    }
}

/// A JSON or bytes column refuses range comparisons and `orderBy`, and
/// JSON refuses `contains`, at the root and in a relation, with the column
/// and operator named.
#[test]
fn json_and_bytes_columns_refuse_range_contains_and_order() {
    let db = open_db();
    let things = || Query::from("things");
    let message = refusal(&db, things().filter(q::gt(q::col("meta"), text("a"))));
    assert!(
        message.contains("JSON column \"meta\" does not support \"gt\""),
        "{message}"
    );
    refusal(&db, things().filter(q::contains(q::col("meta"), text("a"))));
    refusal(
        &db,
        things().filter(q::lte(q::col("blob"), q::lit(Value::Bytes(vec![1])))),
    );
    let message = refusal(&db, things().order_by("blob", OrderDirection::Asc));
    assert!(
        message.contains("bytes column \"blob\" cannot be used in orderBy"),
        "{message}"
    );
    refusal(&db, things().order_by("meta", OrderDirection::Desc));
    // In an optional relation, which reads compile only per parent row.
    refusal(
        &db,
        things().array_subquery(
            ArraySubquery::new("parts", "parts", "thing", "id")
                .order_by("meta", OrderDirection::Asc),
        ),
    );
}

/// Every other type keeps what the engine supports: text, boolean and
/// arrays order, text compares by range, and JSON still takes equality.
#[test]
fn other_columns_keep_what_the_engine_supports() {
    let db = open_db();
    let things = || Query::from("things");
    for query in [
        things().filter(q::gt(q::col("label"), text("a"))),
        things().order_by("flag", OrderDirection::Asc),
        things().order_by("tags", OrderDirection::Asc),
        things().order_by("label", OrderDirection::Desc),
        things().filter(q::eq(q::col("meta"), text("{}"))),
        things().filter(q::contains(q::col("tags"), text("a"))),
        things().filter(q::contains(q::col("blob"), q::lit(Value::Bytes(vec![1])))),
    ] {
        db.prepare_query(&query)
            .unwrap_or_else(|error| panic!("{query:?}: {error}"));
    }
}
