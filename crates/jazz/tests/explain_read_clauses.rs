//! The Inspector's `explain`: for rows of a table, which clauses of its read
//! policy (the arms of its top-level `Or`) hold for a session, so a person can
//! see why a user may or may not read each row.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, DeleteOptions, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::records::Value;
use jazz::tools::{
    CmpOp, ColumnType, Operation, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies,
    TableSchemaBuilder,
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
            // A comment is readable through its note, or when it's pinned.
            .table(
                TableSchemaBuilder::new("comments")
                    .fk_column("note", "notes")
                    .column("pinned", ColumnType::Boolean)
                    .policies(TablePolicies::new().with_select(PolicyExpr::Or(vec![
                        PolicyExpr::Inherits {
                            operation: Operation::Select,
                            via_column: "note".into(),
                            max_depth: None,
                        },
                        PolicyExpr::Cmp {
                            column: "pinned".to_owned(),
                            op: CmpOp::Eq,
                            value: PolicyValue::Literal(jazz::tools::Value::Boolean(true)),
                        },
                    ]))),
            )
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

fn text(value: &str) -> Value {
    Value::String(value.to_owned())
}

type Explained = (usize, Vec<(RowUuid, Vec<bool>)>);

/// The clause count, and the rows sorted by id.
fn sorted((clauses, mut rows): Explained) -> Explained {
    rows.sort_by_key(|(row, _)| row.0);
    (clauses, rows)
}

#[test]
fn explain_says_which_clause_of_the_read_policy_holds_for_each_row() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let none = BTreeMap::new;
    let db = server();
    let alices = insert(
        &db,
        "notes",
        1,
        &[("owner", subject(alice)), ("title", text("a"))],
    );
    let bobs = insert(
        &db,
        "notes",
        2,
        &[("owner", subject(bob)), ("title", text("b"))],
    );
    let rows = [alices, bobs];
    let explain = |table: &str, rows: &[RowUuid], author, claims| {
        sorted(
            db.explain_read_clauses(table, rows, author, claims)
                .unwrap(),
        )
    };

    // Alice, no claims: her own row by clause 0; bob's by none of the two.
    assert_eq!(
        explain("notes", &rows, alice, none()),
        (
            2,
            vec![(alices, vec![true, false]), (bobs, vec![false, false])]
        ),
    );
    // As a manager: both by clause 1, and her own by clause 0 too. (Claims
    // as a binding passes them: canonical, bound to the author.)
    let manager =
        jazz::binding_args::policy_claims(&alice, serde_json::json!({ "role": "manager" }))
            .unwrap();
    assert_eq!(
        explain("notes", &rows, alice, manager),
        (
            2,
            vec![(alices, vec![true, true]), (bobs, vec![false, true])]
        ),
    );

    // A policy that isn't an `Or` is one clause.
    let owned = insert(&db, "owned", 3, &[("owner", subject(bob))]);
    assert_eq!(
        explain("owned", &[owned], bob, none()),
        (1, vec![(owned, vec![true])])
    );
    // No read policy: no clauses, every row denied.
    let closed = insert(&db, "closed", 4, &[("title", text("c"))]);
    assert_eq!(
        explain("closed", &[closed], bob, none()),
        (0, vec![(closed, vec![])])
    );
    // Rows that aren't there are left out; a missing table is an error.
    assert_eq!(
        explain("notes", &[RowUuid::from_bytes([9; 16])], alice, none()),
        (2, vec![]),
    );
    assert!(
        db.explain_read_clauses("nope", &rows, alice, none())
            .is_err()
    );

    // An inherited clause holds when the parent row's own policy does.
    let comment = |id, note: RowUuid, pinned| {
        insert(
            &db,
            "comments",
            id,
            &[
                ("note", Value::Uuid(note.0)),
                ("pinned", Value::Bool(pinned)),
            ],
        )
    };
    let on_alices = comment(5, alices, false);
    let on_bobs = comment(6, bobs, false);
    let pinned = comment(7, bobs, true);
    assert_eq!(
        explain("comments", &[on_alices, on_bobs, pinned], alice, none()),
        (
            2,
            vec![
                (on_alices, vec![true, false]),
                (on_bobs, vec![false, false]),
                (pinned, vec![false, true]),
            ]
        ),
    );

    // A deleted row is not explained.
    block_on(db.delete("notes", bobs, DeleteOptions::default())).unwrap();
    assert_eq!(
        explain("notes", &rows, alice, none()),
        (2, vec![(alices, vec![true, false])]),
    );
}
