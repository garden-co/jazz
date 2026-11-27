//! The stored form of the address policy nodes (`InheritsFrom`,
//! `InsertedByThisTransaction`, `table_id('T')`): an additive extension of
//! the permissions bundle encoding `jazz-bundle-v1`, whose definition is the
//! canonical JSON of the policies (sorted keys, no whitespace) and whose ID
//! hashes it. Pinned byte for byte, with the SHA-256 of each canonical form;
//! a bundle holding none of them keeps its ID. The lowered operand of a
//! table identity is never part of a query: one that carries it is refused.

use std::collections::BTreeMap;

use jazz::catalogue::{Permissions, bundle_id, canonical_json};
use jazz::db::{Db, DbConfig, DbIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::{Operand, Predicate, Query};
use jazz::tools::{
    CmpOp, ColumnType, Operation, PolicyExpr, PolicyValue, SchemaBuilder, SchemaHash, TableName,
    TablePolicies, TableSchemaBuilder,
};

fn sha256_hex(bytes: &[u8]) -> String {
    jazz_types::sha256::hash(bytes)
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Each node's canonical bundle form, its length and its SHA-256.
#[test]
fn the_canonical_bundle_form_of_each_address_node_is_pinned() {
    for (expr, canonical, length, digest) in [
        (
            PolicyExpr::InheritsFrom {
                operation: Operation::Select,
                table_column: "scopeId".into(),
                id_column: "identifier".into(),
                tables: vec!["projects".into()],
            },
            r#"{"id_column":"identifier","operation":"Select","table_column":"scopeId","tables":["projects"],"type":"InheritsFrom"}"#,
            116,
            "c2aead25ca360dafc60aed57ce264c42ec8ae17e8f52ee70e5a01dc17c05bd4b",
        ),
        (
            PolicyExpr::InsertedByThisTransaction,
            r#"{"type":"InsertedByThisTransaction"}"#,
            36,
            "2427d9e1459a05c4bf72e4ebd14aecc6f3350f33e824d9c6ba4402f07653b7c8",
        ),
        (
            PolicyExpr::Cmp {
                column: "scopeId".into(),
                op: CmpOp::Eq,
                value: PolicyValue::TableIdentity {
                    table: "projects".into(),
                },
            },
            r#"{"column":"scopeId","op":"Eq","type":"Cmp","value":{"table":"projects","type":"TableIdentity"}}"#,
            95,
            "66ae7328d728bb12d49b1893fee39eb1946da9cf3ed2786561ba0cf99a11a9af",
        ),
    ] {
        let stored = canonical_json(&expr).unwrap();
        assert_eq!(stored, canonical);
        assert_eq!(stored.len(), length, "{canonical}");
        assert_eq!(sha256_hex(stored.as_bytes()), digest, "{canonical}");
    }
}

/// A bundle without the new nodes is the bundle it was: same definition,
/// same ID. The ID is SHA-256 of `jazz-bundle-v1\0`, the schema hash and
/// the canonical definition
/// `{"todos":{"delete":{"using":null,"with_check":null},"insert":{"using":null,"with_check":{"type":"True"}},"select":{"using":{"column":"owner","op":"Eq","type":"Cmp","value":{"path":["user"],"type":"SessionRef"}},"with_check":null},"update":{"using":null,"with_check":null}}}`.
#[test]
fn a_bundle_without_address_nodes_keeps_its_id() {
    let permissions: Permissions = BTreeMap::from([(
        TableName::from("todos"),
        TablePolicies::new()
            .with_select(PolicyExpr::eq_session("owner", vec!["user".into()]))
            .with_insert(PolicyExpr::True),
    )]);
    let id = bundle_id(&SchemaHash::from_bytes([7; 32]), &permissions).unwrap();
    assert_eq!(
        id.to_hex(),
        "6b4e0d78391af9b552f8799503b6eb2e9b822cf81072233edd2c23635e2e5d29"
    );
}

/// The lowering's table identity operand names a table in a policy only:
/// a query that carries it, from an application or a peer, is refused.
#[test]
fn a_query_carrying_a_table_identity_is_refused() {
    let schema = jazz::schema::JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("owner", ColumnType::Uuid)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build(),
    )
    .unwrap();
    let db = block_on(Db::open_history_complete(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let query = Query::from("todos").filter(Predicate::Eq(
        Operand::Column("owner".into()),
        Operand::TableIdentity("todos".into()),
    ));
    let error = db.prepare_query(&query).unwrap_err();
    assert!(
        error
            .message
            .contains("a table identity is not supported in a query"),
        "{error:?}"
    );

    block_on(db.set_authority()).unwrap();
    let (mut peer, endpoint) = jazz::db::duplex();
    block_on(db.accept_subscriber(endpoint, AuthorSubject::for_test_bytes([2; 16]))).unwrap();
    peer.send(jazz::db::SyncMessage::Read {
        id: 1,
        query: Box::new(query),
        binding: Default::default(),
        include_deleted: false,
        tier: jazz::db::ReadTier::Remote,
        wait: None,
        view: Box::new(jazz::protocol::ReadViewSourceSpec::Current),
        since: None,
        span: jazz::db::ReadSpan::Once { at: None },
    })
    .unwrap();
    block_on(db.tick()).unwrap();
    let reply = std::iter::from_fn(|| peer.try_recv())
        .find_map(|message| match message {
            jazz::db::SyncMessage::Refused { id: 1, message, .. } => Some(message),
            _ => None,
        })
        .expect("the authority refuses the peer's read");
    assert!(
        reply.contains("a table identity is not supported in a query"),
        "{reply}"
    );
}
