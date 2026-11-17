//! A relation-policy literal compares as the column's type, whether it is
//! written plainly (`code = '<uuid>'`, as the policy compiler writes a hop
//! condition) or as a typed value: UUID-shaped text compared with a text
//! column is text, and a UUID column's literal is a UUID in any spelling the
//! compile accepts (hyphenated, simple, braced, uppercase). Otherwise `=`
//! never matches, and `!=` matches every row: the policy would grant more
//! than it says.
//!
//! ```text
//! members: one row, user_text = "<alice's uuid>", user_uuid = <alice's uuid>
//! anyone ──read / insert──► docs_<n>
//!                             └── exists members where <column> <op> <literal>
//! ```

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, WriteIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::public_schema::{
    RelationCmpOp, RelationColumnRef, RelationExpr, RelationPredicate, RelationValueRef,
};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use serde_json::json;

use common::compile_schema;

fn alice_uuid() -> uuid::Uuid {
    uuid::Uuid::from_bytes([0xa1; 16])
}

/// One policy: `exists members where <column> <op> <literal>`, and whether
/// a doc under it is visible (the one member matches `=`, never `!=`).
struct Case {
    label: String,
    column: &'static str,
    op: RelationCmpOp,
    literal: serde_json::Value,
    visible: bool,
}

fn cases() -> Vec<Case> {
    let uuid = alice_uuid();
    let hyphenated = uuid.hyphenated().to_string();
    let spellings: [(&str, &str, serde_json::Value); 6] = [
        ("text", "plain", json!(hyphenated)),
        (
            "text",
            "typed",
            json!({"type": "Text", "value": hyphenated}),
        ),
        ("uuid", "hyphenated", json!(hyphenated)),
        ("uuid", "uppercase", json!(hyphenated.to_uppercase())),
        ("uuid", "simple", json!(uuid.simple().to_string())),
        ("uuid", "braced", json!(uuid.braced().to_string())),
    ];
    let mut cases = Vec::new();
    for (column, spelling, literal) in spellings {
        for (op_name, op, visible) in [
            ("eq", RelationCmpOp::Eq, true),
            ("ne", RelationCmpOp::Ne, false),
        ] {
            cases.push(Case {
                label: format!("{column} {op_name} {spelling}"),
                column: if column == "text" {
                    "user_text"
                } else {
                    "user_uuid"
                },
                op,
                literal: literal.clone(),
                visible,
            });
        }
    }
    cases
}

fn table(index: usize) -> String {
    format!("docs_{index}")
}

fn policy(case: &Case) -> PolicyExpr {
    PolicyExpr::ExistsRel {
        rel: RelationExpr::Filter {
            input: Box::new(RelationExpr::TableScan {
                table: "members".into(),
                alias: None,
            }),
            predicate: RelationPredicate::Cmp {
                left: RelationColumnRef {
                    scope: Some("members".to_owned()),
                    column: case.column.to_owned(),
                },
                op: case.op.clone(),
                right: RelationValueRef::Literal(case.literal.clone()),
            },
        },
    }
}

fn doc() -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), Value::String("plan".to_owned()))])
}

fn open(cases: &[Case]) -> Db {
    let mut schema = SchemaBuilder::new().table(
        TableSchemaBuilder::new("members")
            .column("user_text", ColumnType::Text)
            .column("user_uuid", ColumnType::Uuid)
            .policies(TablePolicies::new().with_select(PolicyExpr::True)),
    );
    for (index, case) in cases.iter().enumerate() {
        let policy = policy(case);
        schema = schema.table(
            TableSchemaBuilder::new(&table(index))
                .column("title", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(policy.clone())
                        .with_insert(policy),
                ),
        );
    }
    let db = block_on(Db::open(DbConfig::new(
        compile_schema(&schema.build()),
        DbIdentity {
            node: NodeUuid::from_bytes([0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    block_on(db.insert(
        "members",
        BTreeMap::from([
            (
                "user_text".to_owned(),
                Value::String(alice_uuid().to_string()),
            ),
            ("user_uuid".to_owned(), Value::Uuid(alice_uuid())),
        ]),
        Default::default(),
    ))
    .unwrap();
    for index in 0..cases.len() {
        block_on(db.insert(&table(index), doc(), Default::default())).unwrap();
    }
    db
}

/// bob, a session with no claims, reads and inserts docs under each policy.
#[test]
fn a_relation_literal_compares_as_its_columns_type() {
    let cases = cases();
    let db = open(&cases);
    let bob = AuthorSubject::for_test_bytes([0x60; 16]);
    db.set_identity_claims(bob, BTreeMap::new());
    let mut wrong = Vec::new();
    for (index, case) in cases.iter().enumerate() {
        let table = table(index);
        let prepared = db.prepare_query(&Query::from(table.clone())).unwrap();
        let read = block_on(db.all_for_identity(&prepared, ReadOpts::default(), bob))
            .unwrap()
            .len();
        let inserted = match block_on(db.insert(
            &table,
            doc(),
            InsertOptions {
                identity: WriteIdentity::Session(bob),
                ..Default::default()
            },
        )) {
            Ok(_) => true,
            Err(error) if error.is_authorization_denied() => false,
            Err(error) => panic!("{}: {}", case.label, error.message),
        };
        if (read == 1) != case.visible || inserted != case.visible {
            wrong.push(format!(
                "{}: read {read}, insert allowed {inserted}, expected visible {}",
                case.label, case.visible
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
