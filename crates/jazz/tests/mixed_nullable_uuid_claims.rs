use std::collections::{BTreeMap, BTreeSet};

mod common;

use common::{allow_all_policies, exists, outer_eq, read_and_allow_all_writes, session_eq};
use jazz::block_on;
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

fn row(seed: u8) -> RowUuid {
    RowUuid::from_bytes([seed; 16])
}

fn ownership() -> PolicyExpr {
    session_eq("owner", &["user", "account"])
}

fn membership() -> PolicyExpr {
    exists(
        "memberships",
        vec![
            outer_eq("organization", "organization"),
            session_eq("account", &["user", "account"]),
        ],
    )
}

fn read_case(nullable_owner: bool, policy: PolicyExpr) -> Result<BTreeSet<RowUuid>, String> {
    let documents = TableSchemaBuilder::new("documents").fk_column("organization", "organizations");
    let documents = if nullable_owner {
        documents.nullable_column("owner", ColumnType::Uuid)
    } else {
        documents.column("owner", ColumnType::Uuid)
    };
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(documents.policies(read_and_allow_all_writes(policy)))
            .table(
                TableSchemaBuilder::new("organizations")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("organization", "organizations")
                    .column("account", ColumnType::Uuid)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
    .map_err(|error| format!("compile schema: {error}"))?;
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let alice_author = AuthorSubject::for_test_uuid(uuid::Uuid::from_bytes([0xa1; 16]));
    let alice = alice_author.account_id().unwrap().0;
    let mallory = AuthorSubject::for_test_uuid(uuid::Uuid::from_bytes([0xb2; 16]))
        .account_id()
        .unwrap()
        .0;
    // Db::all reads received local data without serving RLS. Exercise the
    // public identity-scoped node read instead, to observe actual policy rows.
    // session.user.account comes from Alice's admitted author, not metadata.
    let mut node = block_on(NodeState::new_with_shared_test_catalogue(
        NodeUuid::from_bytes([0xc3; 16]),
        schema.clone(),
        TestStorage::new(&family_refs),
    ))
    .map_err(|error| format!("open node: {error}"))?;

    for organization in [row(0x11), row(0x22), row(0x33)] {
        block_on(async {
            let published = node
                .commit_mergeable(
                    MergeableCommit::new("organizations", organization, 1)
                        .made_by(alice_author)
                        .cells(BTreeMap::from([(
                            "name".to_owned(),
                            Value::String("synthetic organization".to_owned()),
                        )])),
                )
                .await?;
            node.persist_and_settle_transaction(published).await
        })
        .map_err(|error| format!("insert organization: {error}"))?;
    }
    for (id, organization, owner) in [
        (row(1), row(0x11), Some(alice)),
        (row(2), row(0x22), None),
        (row(3), row(0x33), Some(mallory)),
    ] {
        let owner = if nullable_owner {
            Value::Nullable(owner.map(|owner| Box::new(Value::Uuid(owner))))
        } else {
            Value::Uuid(owner.unwrap_or(mallory))
        };
        block_on(async {
            let published = node
                .commit_mergeable(
                    MergeableCommit::new("documents", id, id.0.as_bytes()[0] as u64)
                        .made_by(alice_author)
                        .cells(BTreeMap::from([
                            ("organization".to_owned(), Value::Uuid(organization.0)),
                            ("owner".to_owned(), owner),
                        ])),
                )
                .await?;
            node.persist_and_settle_transaction(published).await
        })
        .map_err(|error| format!("insert document {id:?}: {error}"))?;
    }
    // Alice belongs only to the second document's organization. Mallory's
    // membership in the third organization must not grant Alice access.
    for (id, organization, account) in [(row(4), row(0x22), alice), (row(5), row(0x33), mallory)] {
        block_on(async {
            let published = node
                .commit_mergeable(
                    MergeableCommit::new("memberships", id, id.0.as_bytes()[0] as u64)
                        .made_by(alice_author)
                        .cells(BTreeMap::from([
                            ("organization".to_owned(), Value::Uuid(organization.0)),
                            ("account".to_owned(), Value::Uuid(account)),
                        ])),
                )
                .await?;
            node.persist_and_settle_transaction(published).await
        })
        .map_err(|error| format!("insert membership: {error}"))?;
    }
    let membership_shape = Query::from("memberships")
        .validate(&schema)
        .map_err(|error| format!("validate membership query: {error}"))?;
    let membership_binding = membership_shape
        .bind(BTreeMap::new())
        .map_err(|error| format!("bind membership query: {error}"))?;
    let witnesses = block_on(node.query_rows_for_link(
        &membership_shape,
        &membership_binding,
        DurabilityTier::Local,
        alice_author,
    ))
    .map_err(|error| format!("read membership witnesses: {error}"))?;
    let witness_ids = witnesses
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    if witness_ids != BTreeSet::from([row(4), row(5)]) {
        return Err(format!("membership witnesses missing: {witness_ids:?}"));
    }
    let shape = Query::from("documents")
        .validate(&schema)
        .map_err(|error| format!("validate query: {error}"))?;
    let binding = shape
        .bind(BTreeMap::new())
        .map_err(|error| format!("bind query: {error}"))?;
    block_on(node.query_rows_for_link(&shape, &binding, DurabilityTier::Local, alice_author))
        .map(|rows| rows.into_iter().map(|row| row.row_uuid()).collect())
        .map_err(|error| error.to_string())
}

/// Alice can read personally owned documents or documents in her organization;
/// Mallory's unrelated document remains hidden. Nullable and required UUID
/// comparisons must bind the same admitted session account without type clashes.
#[test]
fn mixed_nullable_and_required_account_claims() {
    let cases = [
        ("nullable_only", true, ownership(), BTreeSet::from([row(1)])),
        (
            "required_only",
            true,
            membership(),
            BTreeSet::from([row(2)]),
        ),
        (
            "same_required_types",
            false,
            PolicyExpr::or(vec![ownership(), membership()]),
            BTreeSet::from([row(1), row(2)]),
        ),
        (
            "mixed_owner_first",
            true,
            PolicyExpr::or(vec![ownership(), membership()]),
            BTreeSet::from([row(1), row(2)]),
        ),
        (
            "mixed_membership_first",
            true,
            PolicyExpr::or(vec![membership(), ownership()]),
            BTreeSet::from([row(1), row(2)]),
        ),
    ];
    let mut failures = Vec::new();
    for (label, nullable_owner, policy, expected) in cases {
        let actual = read_case(nullable_owner, policy);
        if actual.as_ref() != Ok(&expected) {
            failures.push(format!("{label}: expected={expected:?}; actual={actual:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
