//! The Inspector's `explain`: for rows of a table, which clauses of its read
//! policy (the arms of its top-level `Or`) hold for a session, so a person can
//! see why a user may or may not read each row.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::records::Value;
use jazz::tools::{
    CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};

use common::compile_schema;

fn owned_by_session() -> PolicyExpr {
    PolicyExpr::Cmp {
        column: "owner".to_owned(),
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(vec!["identity".to_owned(), "subject".to_owned()]),
    }
}

fn manager_claim() -> PolicyExpr {
    PolicyExpr::SessionCmp {
        path: vec!["claims".to_owned(), "role".to_owned()],
        op: CmpOp::Eq,
        value: jazz::tools::Value::Text("manager".to_owned()),
    }
}

fn server() -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::Or(vec![owned_by_session(), manager_claim()])),
                    ),
            )
            .table(
                TableSchemaBuilder::new("owned")
                    .column("owner", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(owned_by_session())),
            )
            .table(TableSchemaBuilder::new("closed").column("title", ColumnType::Text))
            .build(),
    );
    block_on(Db::open_history_complete(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn insert(db: &Db, table: &str, id: u8, cells: &[(&str, Value)]) -> RowUuid {
    let row = RowUuid::from_bytes([id; 16]);
    let cells: BTreeMap<String, Value> = cells
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect();
    block_on(db.insert(
        table,
        cells,
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap();
    row
}

fn subject(author: AuthorSubject) -> Value {
    Value::String(author.principal_parts().1)
}

fn sorted(mut rows: Vec<(RowUuid, Vec<bool>)>) -> Vec<(RowUuid, Vec<bool>)> {
    rows.sort_by_key(|(row, _)| row.0);
    rows
}

#[test]
fn explain_says_which_clause_of_the_read_policy_holds_for_each_row() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let db = server();
    let alices = insert(
        &db,
        "notes",
        1,
        &[
            ("owner", subject(alice)),
            ("title", Value::String("a".into())),
        ],
    );
    let bobs = insert(
        &db,
        "notes",
        2,
        &[
            ("owner", subject(bob)),
            ("title", Value::String("b".into())),
        ],
    );
    let rows = [alices, bobs];

    // Alice, no claims: her own row by clause 0, bob's by none.
    assert_eq!(
        sorted(
            db.explain_read_clauses("notes", &rows, alice, BTreeMap::new())
                .unwrap()
        ),
        vec![(alices, vec![true, false]), (bobs, vec![false, false])],
    );
    // As a manager: both by clause 1, and her own by clause 0 too. (Claims
    // as a binding passes them: canonical, bound to the author.)
    let manager =
        jazz::binding_args::policy_claims(&alice, serde_json::json!({ "role": "manager" }))
            .unwrap();
    assert_eq!(
        sorted(
            db.explain_read_clauses("notes", &rows, alice, manager)
                .unwrap()
        ),
        vec![(alices, vec![true, true]), (bobs, vec![false, true])],
    );

    // A policy that isn't an `Or` is one clause.
    let owned = insert(&db, "owned", 3, &[("owner", subject(bob))]);
    assert_eq!(
        db.explain_read_clauses("owned", &[owned], bob, BTreeMap::new())
            .unwrap(),
        vec![(owned, vec![true])],
    );
    // No read policy: no clauses, every row denied.
    let closed = insert(&db, "closed", 4, &[("title", Value::String("c".into()))]);
    assert_eq!(
        db.explain_read_clauses("closed", &[closed], bob, BTreeMap::new())
            .unwrap(),
        vec![(closed, vec![])],
    );
    // Rows that aren't there are left out; a missing table is an error.
    assert_eq!(
        db.explain_read_clauses(
            "notes",
            &[RowUuid::from_bytes([9; 16])],
            alice,
            BTreeMap::new()
        )
        .unwrap(),
        vec![],
    );
    assert!(
        db.explain_read_clauses("nope", &rows, alice, BTreeMap::new())
            .is_err()
    );
}
