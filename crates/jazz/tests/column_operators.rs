//! What each column type supports in a query, as `prepare` checks it: a
//! JSON or bytes column takes no range comparison and can't order a query,
//! and JSON takes no `contains`; every other type keeps what the engine
//! supports. One
//! checker for every client (#5927 R13): TS, the FFI and the inspector
//! leave it to the engine.

use crate::common;

use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::{
    self as q, ArraySubquery, OrderDirection, Query, RelationOrderBy, RelationQuery,
};
use jazz::records::Value;
use jazz::tools::public_schema::{
    RelationCmpOp, RelationColumnRef, RelationExpr, RelationPredicate, RelationProjectColumn,
    RelationProjectExpr, RelationRowIdRef, RelationUnionArm, RelationValueRef,
};
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
        ColumnDescriptor::new("pmeta", ColumnType::Json { schema: None }),
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

fn things_column(column: &str) -> RelationColumnRef {
    RelationColumnRef {
        scope: Some("things".to_owned()),
        column: column.to_owned(),
    }
}

/// `things.where({ column: { op: value } })`, as a union arm.
fn arm(label: &str, column: &str, op: RelationCmpOp) -> RelationUnionArm {
    RelationUnionArm {
        label: label.to_owned(),
        input: RelationExpr::Project {
            input: Box::new(RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: "things".to_owned(),
                    alias: None,
                }),
                predicate: RelationPredicate::Cmp {
                    left: things_column(column),
                    op,
                    right: RelationValueRef::literal(jazz::tools::Value::Text("a".to_owned())),
                },
            }),
            columns: ["id", "label", "flag", "meta", "blob", "tags"]
                .iter()
                .map(|alias| RelationProjectColumn {
                    alias: (*alias).to_owned(),
                    expr: if *alias == "id" {
                        RelationProjectExpr::RowId(RelationRowIdRef::Current)
                    } else {
                        RelationProjectExpr::Column(things_column(alias))
                    },
                })
                .collect(),
        },
    }
}

fn union(arms: Vec<RelationUnionArm>, order: Option<&str>) -> Query {
    let mut rel = RelationExpr::Union { inputs: arms };
    if let Some(column) = order {
        rel = RelationExpr::OrderBy {
            input: Box::new(rel),
            terms: vec![RelationOrderBy {
                column: things_column(column),
                direction: OrderDirection::Asc,
            }],
        };
    }
    let mut query = Query::from("things");
    query.relation = Some(RelationQuery { rel });
    query
}

/// The check reaches every place a query names columns: union arms and
/// the union's order, hop seeds (joins), and history reads, which compile
/// against their source table.
#[test]
fn unions_joins_and_history_are_checked_too() {
    let db = open_db();
    let fine = || arm("fine", "label", RelationCmpOp::Eq);
    let refused = |query: Query, expected: &str| {
        let message = refusal(&db, query);
        assert!(message.contains(expected), "{message}");
    };
    refused(
        union(vec![fine(), arm("json", "meta", RelationCmpOp::Gt)], None),
        "JSON column \"meta\" does not support \"gt\"",
    );
    refused(
        union(vec![fine(), arm("bytes", "blob", RelationCmpOp::Lt)], None),
        "bytes column \"blob\" does not support \"lt\"",
    );
    refused(
        union(vec![fine()], Some("meta")),
        "JSON column \"meta\" cannot be used in orderBy",
    );
    refused(
        union(vec![fine()], Some("blob")),
        "bytes column \"blob\" cannot be used in orderBy",
    );
    db.prepare_query(&union(
        vec![fine(), arm("text", "label", RelationCmpOp::Gt)],
        Some("label"),
    ))
    .expect("a union of supported operators");

    refused(
        Query::from("things").join_via("parts", "thing", [q::gt(q::col("meta"), text("a"))]),
        "JSON column \"meta\" does not support \"gt\"",
    );

    let message = refusal(
        &db,
        Query::from("history.things")
            .filter(q::eq(q::col("id"), q::lit(Value::Uuid(uuid::Uuid::nil()))))
            .filter(q::gt(q::col("meta"), text("a"))),
    );
    assert!(message.contains("JSON column \"meta\""), "{message}");
    db.prepare_query(
        &Query::from("history.things")
            .filter(q::eq(q::col("id"), q::lit(Value::Uuid(uuid::Uuid::nil()))))
            .filter(q::gt(q::col("label"), text("a"))),
    )
    .expect("a supported history read");
}

/// Gather (reachability) filters on its access, edge and seed tables, flat
/// joins (a joined column by `alias.column`, or by a bare name only the
/// joined table has), and joins nested in a join are checked too.
#[test]
fn gathers_flat_joins_and_nested_joins_are_checked_too() {
    let db = open_db();
    let refused = |query: Query, expected: &str| {
        let message = refusal(&db, query);
        assert!(message.contains(expected), "{message}");
    };
    let json_gt = || q::gt(q::col("meta"), text("a"));
    let gather = |access: Vec<q::Predicate>, edge: Vec<q::Predicate>| {
        Query::from("things").reachable_via_with_access_filters(
            "parts",
            "thing",
            "thing",
            q::col("id"),
            access,
            "parts",
            "thing",
            "thing",
            edge,
        )
    };
    refused(
        gather(vec![json_gt()], vec![]),
        "JSON column \"meta\" does not support \"gt\"",
    );
    refused(
        gather(vec![], vec![json_gt()]),
        "JSON column \"meta\" does not support \"gt\"",
    );
    let mut seeded = gather(vec![], vec![]).seeded_by("parts", "thing", "sub", "thing");
    seeded.reachable[0]
        .seed
        .as_mut()
        .expect("a seed")
        .filters
        .push(json_gt());
    refused(seeded, "JSON column \"meta\" does not support \"gt\"");

    let flat = || Query::from("things").flat_join("parts", "things.id", "parts.thing");
    refused(
        flat().filter(q::gt(q::col("parts.meta"), text("a"))),
        "JSON column \"parts.meta\" does not support \"gt\"",
    );
    refused(
        flat().filter(q::contains(q::col("pmeta"), text("a"))),
        "JSON column \"pmeta\" does not support \"contains\"",
    );
    db.prepare_query(&flat().filter(q::gt(q::col("things.label"), text("a"))))
        .expect("a flat join on supported operators");

    let mut nested = Query::from("things").join_via("parts", "thing", []);
    let inner = Query::from("things")
        .join_via("parts", "thing", [json_gt()])
        .joins
        .remove(0);
    nested.joins[0].nested_joins.push(inner);
    refused(nested, "JSON column \"meta\" does not support \"gt\"");
}
