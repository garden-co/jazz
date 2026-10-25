//! Relation facade queries on SQLite: a client's hopTo, gather or match read
//! arrives as relation IR ending in a full-row projection, and is read as
//! the ordinary query it stands for. Projections that rename or narrow the
//! output are refused; full-row unions preserve their arm occurrences.

mod common;
use common::compile_schema;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, RelationQuery};
use jazz::records::Value as CoreValue;
use jazz::tools::public_schema::{
    RelationCmpOp, RelationColumnRef, RelationExpr, RelationPredicate, RelationProjectColumn,
    RelationProjectExpr, RelationRowIdRef, RelationUnionArm, RelationValueRef,
};
use jazz::tools::{
    ColumnType, PolicyExpr, Schema, SchemaBuilder, TablePolicies, TableSchema, Value,
};
use std::collections::BTreeMap;

fn schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean)
                .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
        )
        .build()
}

fn open() -> Db {
    block_on(Db::open_history_complete(DbConfig::new(
        compile_schema(&schema()),
        DbIdentity {
            node: NodeUuid::from_bytes([0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([1, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

fn column(alias: &str) -> RelationProjectColumn {
    RelationProjectColumn {
        alias: alias.to_owned(),
        expr: if alias == "id" {
            RelationProjectExpr::RowId(RelationRowIdRef::Current)
        } else {
            RelationProjectExpr::Column(RelationColumnRef {
                scope: Some("todos".to_owned()),
                column: alias.to_owned(),
            })
        },
    }
}

fn done_todos(columns: &[&str]) -> RelationExpr {
    RelationExpr::Project {
        input: Box::new(RelationExpr::Filter {
            input: Box::new(RelationExpr::TableScan {
                table: "todos".to_owned(),
                alias: None,
            }),
            predicate: RelationPredicate::Cmp {
                left: RelationColumnRef {
                    scope: Some("todos".to_owned()),
                    column: "done".to_owned(),
                },
                op: RelationCmpOp::Eq,
                right: RelationValueRef::literal(Value::Boolean(true)),
            },
        }),
        columns: columns.iter().map(|alias| column(alias)).collect(),
    }
}

fn relation_query(rel: RelationExpr) -> Query {
    let mut query = Query::from("todos");
    query.relation = Some(RelationQuery { rel });
    query
}

#[test]
fn a_full_row_relation_projection_reads_as_its_ordinary_query() {
    let db = open();
    for (n, done) in [(1, true), (2, false), (3, true)] {
        let cells = BTreeMap::from([
            ("title".to_owned(), CoreValue::String(format!("todo {n}"))),
            ("done".to_owned(), CoreValue::Bool(done)),
        ]);
        db.seed_settled_mergeable_for_bootstrap("todos", id(n), AuthorSubject::SYSTEM, cells)
            .unwrap();
    }
    let prepared = db
        .prepare_query(&relation_query(done_todos(&["id", "title", "done"])))
        .unwrap();
    let mut ids: Vec<RowUuid> = block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect();
    ids.sort();
    assert_eq!(ids, [id(1), id(3)]);
}

#[test]
fn narrowing_projections_are_refused_and_full_row_unions_are_supported() {
    let db = open();
    let narrowed = db
        .prepare_query(&relation_query(done_todos(&["id", "title"])))
        .expect_err("a narrowing projection is refused");
    assert!(
        narrowed.to_string().contains("not supported yet"),
        "{narrowed}"
    );
    let union = db
        .prepare_query(&relation_query(RelationExpr::Union {
            inputs: vec![RelationUnionArm {
                label: "done".to_owned(),
                input: done_todos(&["id", "title", "done"]),
            }],
        }))
        .expect("a full-row union is supported");
    assert!(
        block_on(db.all(&union, ReadOpts::default()))
            .unwrap()
            .is_empty()
    );
}
