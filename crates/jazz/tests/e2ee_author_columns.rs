//! Protocol R2: a space record's author column (`creatorAccountId`,
//! `authorAccountId`, `senderAccountId`) equals both the session account
//! and the row's `$createdBy.account`; so does a commitment's `accountId`.
//!
//! Inspected in the derived policies rather than through a write: every
//! write a policy checks is created by its own session, so no write can
//! show the two apart, and the clause guards against a later writer kind
//! that could.

use std::collections::BTreeMap;

use jazz::tools::deployment::artifact::system_tables::{
    COMMITMENTS, SPACE_DELIVERIES, SPACE_SUCCESSORS, SPACES, derive_e2ee_policies,
    with_e2ee_fragment,
};
use jazz::tools::public_schema::{
    ColumnType, EncryptionEntry, PolicyExpr, PolicyValue, SchemaBuilder, TableName,
    TableSchemaBuilder,
};

fn conjuncts(expr: &PolicyExpr) -> Vec<&PolicyExpr> {
    match expr {
        PolicyExpr::And(items) => items.iter().flat_map(conjuncts).collect(),
        other => vec![other],
    }
}

fn session_eq(column: &str) -> PolicyExpr {
    PolicyExpr::eq_session(column, vec!["user".to_owned()])
}

fn created_by_session() -> PolicyExpr {
    PolicyExpr::Cmp {
        column: "$createdBy.account".to_owned(),
        op: jazz::tools::CmpOp::Eq,
        value: PolicyValue::SessionRef(vec!["user".to_owned()]),
    }
}

fn policies() -> jazz::catalogue::Permissions {
    let schema = with_e2ee_fragment(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("projects").column("name", ColumnType::Text))
            .build(),
    );
    let entries = BTreeMap::from([(
        "projects".to_owned(),
        EncryptionEntry {
            space: "id".to_owned(),
            scope: "projects".to_owned(),
            columns: BTreeMap::new(),
            equality: Vec::new(),
        },
    )]);
    derive_e2ee_policies(&schema, &entries, &Default::default()).unwrap()
}

#[test]
fn space_records_bind_their_author_to_the_session_and_the_creator() {
    let policies = policies();
    for (table, author) in [
        (SPACES, "creatorAccountId"),
        (SPACE_SUCCESSORS, "authorAccountId"),
        (SPACE_DELIVERIES, "senderAccountId"),
    ] {
        let insert = policies[&TableName::from(table)]
            .insert_policy()
            .expect("an insert policy");
        let all = conjuncts(insert);
        assert!(all.contains(&&session_eq(author)), "{table}: {insert:?}");
        assert!(all.contains(&&created_by_session()), "{table}: {insert:?}");
    }
}

/// A commitment's `accountId` equals both the session account and the row's
/// `$createdBy.account`.
#[test]
fn commitments_bind_their_account_to_the_session_and_the_creator() {
    let policies = policies();
    let insert = policies[&TableName::from(COMMITMENTS)]
        .insert_policy()
        .expect("an insert policy");
    let all = conjuncts(insert);
    assert!(all.contains(&&session_eq("accountId")), "{insert:?}");
    assert!(all.contains(&&created_by_session()), "{insert:?}");
}
