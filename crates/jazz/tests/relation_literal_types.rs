//! A relation-policy literal compares as the column's type, whether it is
//! written plainly (`code = '<uuid>'`, as the policy compiler writes a hop
//! condition) or as a typed value: UUID-shaped text compared with a text
//! column is text. Otherwise `=` never matches, and `!=` matches every row:
//! the policy would grant more than it says.
//!
//! ```text
//! members: one row, user_text = "<alice's uuid>" (text)
//! anyone ──read / insert──► docs_<op>_<spelling>
//!                             └── exists members where user_text <op> '<alice's uuid>'
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

/// The comparisons, with whether a doc is visible (there is a member whose
/// text equals alice's uuid, and none that differs).
const OPS: [(&str, RelationCmpOp, bool); 2] = [
    ("eq", RelationCmpOp::Eq, true),
    ("ne", RelationCmpOp::Ne, false),
];

/// How the literal is written.
fn spellings() -> [(&'static str, serde_json::Value); 2] {
    let text = alice_uuid().to_string();
    [
        ("plain", json!(text)),
        ("typed", json!({"type": "Text", "value": text})),
    ]
}

fn table(op: &str, spelling: &str) -> String {
    format!("docs_{op}_{spelling}")
}

fn policy(op: RelationCmpOp, literal: serde_json::Value) -> PolicyExpr {
    PolicyExpr::ExistsRel {
        rel: RelationExpr::Filter {
            input: Box::new(RelationExpr::TableScan {
                table: "members".into(),
                alias: None,
            }),
            predicate: RelationPredicate::Cmp {
                left: RelationColumnRef {
                    scope: Some("members".to_owned()),
                    column: "user_text".to_owned(),
                },
                op,
                right: RelationValueRef::Literal(literal),
            },
        },
    }
}

fn open() -> Db {
    let mut schema = SchemaBuilder::new().table(
        TableSchemaBuilder::new("members")
            .column("user_text", ColumnType::Text)
            .policies(TablePolicies::new().with_select(PolicyExpr::True)),
    );
    for (op_name, op, _) in OPS {
        for (spelling, literal) in spellings() {
            let policy = policy(op.clone(), literal);
            schema = schema.table(
                TableSchemaBuilder::new(&table(op_name, spelling))
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(policy.clone())
                            .with_insert(policy),
                    ),
            );
        }
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
        BTreeMap::from([(
            "user_text".to_owned(),
            Value::String(alice_uuid().to_string()),
        )]),
        Default::default(),
    ))
    .unwrap();
    for (op_name, _, _) in OPS {
        for (spelling, _) in spellings() {
            block_on(db.insert(&table(op_name, spelling), doc(), Default::default())).unwrap();
        }
    }
    db
}

fn doc() -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), Value::String("plan".to_owned()))])
}

/// bob, a session with no claims, reads and inserts docs under each policy.
#[test]
fn a_uuid_shaped_literal_compares_with_a_text_column_as_text() {
    let db = open();
    let bob = AuthorSubject::for_test_bytes([0x60; 16]);
    db.set_identity_claims(bob, BTreeMap::new());
    let mut wrong = Vec::new();
    for (op_name, _, visible) in OPS {
        for (spelling, _) in spellings() {
            let table = table(op_name, spelling);
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
                Err(error) => panic!("{table}: {}", error.message),
            };
            if (read == 1) != visible || inserted != visible {
                wrong.push(format!(
                    "{table}: read {read}, insert allowed {inserted}, expected visible {visible}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
