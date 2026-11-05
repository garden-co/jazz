//! E2EE permission correlations can share an account reference or a scope UUID.
use jazz::db::{Db, DbConfig, DbIdentity, Error, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use std::collections::BTreeMap;

fn author(name: &str) -> AuthorSubject {
    AuthorSubject::for_test_bytes([if name == "alice" { 1 } else { 2 }; 16])
}
fn row(n: u128) -> RowUuid {
    RowUuid(uuid::Uuid::from_u128(n))
}
fn outer(column: &str, parent: &str) -> PolicyExpr {
    PolicyExpr::eq_session(column, vec!["__jazz_outer_row".into(), parent.into()])
}
fn own() -> PolicyExpr {
    PolicyExpr::eq_session("ownerId", vec!["user".into(), "account".into()])
}
fn open(key: &str, indexed: bool, include_created: bool) -> Db {
    let condition = PolicyExpr::and(vec![outer(key, "identifier"), outer("ownerId", "ownerId")]);
    let evidence = if include_created {
        PolicyExpr::exists_including_created("sources", condition)
    } else {
        PolicyExpr::Exists {
            table: "sources".into(),
            condition: Box::new(condition),
        }
    };
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("sources")
                    .column("scopeId", ColumnType::Uuid)
                    .fk_column("ownerId", "accounts")
                    .index_only(if indexed { vec!["scopeId"] } else { vec![] })
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(own())
                            .with_delete(own()),
                    ),
            )
            .table(
                TableSchemaBuilder::new("spaces")
                    .column("identifier", ColumnType::Uuid)
                    .fk_column("ownerId", "accounts")
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::and(vec![own(), evidence])),
                    ),
            )
            // A sibling's created-source policy selects candidate authorization
            // for the schema. Ordinary EXISTS must still use accepted evidence.
            .table(
                TableSchemaBuilder::new("markers")
                    .fk_column("sourceId", "sources")
                    .policies(TablePolicies::new().with_insert(
                        PolicyExpr::exists_including_created("sources", outer("id", "sourceId")),
                    )),
            )
            .table(TableSchemaBuilder::new("accounts"))
            .build(),
    )
    .unwrap();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x73; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    db.set_authority();
    db.initialize_authority_catalogue().unwrap();
    db
}
fn source() -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("scopeId".into(), Value::Uuid(row(10).0)),
        (
            "ownerId".into(),
            Value::Uuid(author("alice").account_id().unwrap().0),
        ),
    ])
}
fn count(db: &Db, table: &str) -> usize {
    let query = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.all(&query, ReadOpts::default())).unwrap().len()
}
fn attempt(
    db: &Db,
    who: &str,
    scope: u128,
    create: bool,
    serializable: bool,
    remove: bool,
) -> Result<(), Error> {
    let id = OpenTransactionId::new();
    let identity = author(who);
    if serializable {
        block_on(db.begin_serializable_for_identity(id, identity)).unwrap();
    } else {
        block_on(db.begin_mergeable_for_identity(id, identity)).unwrap();
    }
    block_on(db.tx_ref(id).insert(
        "spaces",
        BTreeMap::from([
            ("identifier".into(), Value::Uuid(row(scope).0)),
            (
                "ownerId".into(),
                Value::Uuid(identity.account_id().unwrap().0),
            ),
        ]),
        Default::default(),
    ))
    .unwrap();
    if create {
        // The source follows the dependent row: admission must prove it first.
        block_on(db.tx_ref(id).insert(
            "sources",
            source(),
            InsertOptions {
                row_id: Some(row(10)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    if remove {
        block_on(db.tx_ref(id).delete("sources", row(10), Default::default())).unwrap();
    }
    if serializable {
        block_on(db.commit_serializable_handle(id)).map(|_| ())
    } else {
        block_on(db.commit_mergeable_handle(id)).map(|_| ())
    }
}
fn assert_admission(result: Result<(), Error>, allowed: bool) {
    if allowed {
        result.unwrap();
    } else {
        let error = result.unwrap_err();
        assert!(
            error.is_authorization_denied(),
            "expected an authority permission rejection, got {error}"
        );
    }
}
fn seed(db: &Db) {
    block_on(db.insert(
        "sources",
        source(),
        InsertOptions {
            row_id: Some(row(10)),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Alice can name an accepted source by primary key or an indexed generic UUID.
/// Mallory's own owner field and a different scope cannot borrow Alice's grant.
#[test]
fn accepted_uuid_sources_keep_subject_and_scope_checks() {
    for key in ["id", "scopeId"] {
        for (who, scope, allowed) in [
            ("alice", 10, true),
            ("alice", 99, false),
            ("mallory", 10, false),
        ] {
            let db = open(key, true, true);
            seed(&db);
            assert_admission(attempt(&db, who, scope, false, true, false), allowed);
            assert_eq!(count(&db, "spaces"), usize::from(allowed));
            assert_eq!(count(&db, "sources"), 1);
        }
    }
}

/// Alice cannot borrow a sibling policy's created-source marker for ordinary
/// EXISTS. A new generic UUID witness is not accepted evidence in either kind
/// of unit, even when its independent insert permission passes.
#[test]
fn ordinary_uuid_sources_cannot_borrow_created_markers() {
    for key in ["id", "scopeId"] {
        for (serializable, scope, allowed) in
            [(true, 10, false), (false, 10, false), (true, 99, false)]
        {
            let db = open(key, true, false);
            assert_admission(
                attempt(&db, "alice", scope, true, serializable, false),
                allowed,
            );
            assert_eq!(count(&db, "spaces"), usize::from(allowed));
            assert_eq!(count(&db, "sources"), usize::from(allowed));
        }
    }
}

/// Alice cannot publish a space while deleting its authorizing source in the
/// same unit. Both the space and deletion roll back after post-state validation.
#[test]
fn uuid_source_removal_revokes_same_unit_proof() {
    for key in ["id", "scopeId"] {
        let db = open(key, true, true);
        seed(&db);
        assert_admission(attempt(&db, "alice", 10, false, true, true), false);
        assert_eq!(count(&db, "spaces"), 0);
        assert_eq!(count(&db, "sources"), 1);
    }
}

/// Alice's generic scope equality may be unindexed when another mandatory
/// equality has a declared UUID index. All predicates still constrain the proof.
#[test]
fn later_indexed_correlation_bounds_an_unindexed_scope_equality() {
    let db = open("scopeId", false, true);
    seed(&db);
    attempt(&db, "alice", 10, false, true, false).unwrap();
    assert_admission(attempt(&db, "alice", 99, false, true, false), false);
    assert_eq!(count(&db, "spaces"), 1);
    assert_eq!(count(&db, "sources"), 1);
}

/// Alice can use a newly created, independently authorized source through a
/// marked occurrence. Mergeable units cannot turn that source into evidence.
#[test]
fn shared_account_references_qualify_created_uuid_sources() {
    for key in ["id", "scopeId"] {
        for serializable in [false, true] {
            let db = open(key, true, true);
            assert_admission(
                attempt(&db, "alice", 10, true, serializable, false),
                serializable,
            );
            assert_eq!(count(&db, "spaces"), usize::from(serializable));
            assert_eq!(count(&db, "sources"), usize::from(serializable));
        }
    }
}
