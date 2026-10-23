//! Draft-restack boundaries: unported E2EE contracts must fail explicitly.

mod common;

use std::collections::BTreeMap;

use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, ReadOpts, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};

/// Alice may insert through an ordinary accepted-parent EXISTS. The candidate-create
/// variant must fail explicitly until SQLite implements its independent authorization.
/// alice -> existing parent -> ordinary insert succeeds / candidate-source insert rejected
#[test]
fn candidate_create_permissions_are_explicitly_unsupported() {
    let alice = AuthorSubject::for_test_bytes([0x61; 16]);
    let parent = RowUuid::from_bytes([0x62; 16]);
    for include_created in [false, true] {
        let correlation = PolicyExpr::eq_session(
            "id",
            vec!["__jazz_outer_row".to_owned(), "parent".to_owned()],
        );
        let policy = if include_created {
            PolicyExpr::exists_including_created("parents", correlation)
        } else {
            PolicyExpr::Exists {
                table: "parents".to_owned(),
                condition: Box::new(correlation),
            }
        };
        let schema = common::compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("parents")
                        .column("name", ColumnType::Text)
                        .policies(common::allow_all_policies()),
                )
                .table(
                    TableSchemaBuilder::new("children")
                        .fk_column("parent", "parents")
                        .policies(TablePolicies::new().with_insert(policy)),
                )
                .build(),
        );
        let db = block_on(Db::open(DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x63; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        block_on(db.insert(
            "parents",
            BTreeMap::from([("name".to_owned(), Value::String("parent".to_owned()))]),
            InsertOptions {
                row_id: Some(parent),
                ..Default::default()
            },
        ))
        .unwrap();
        let result = block_on(db.insert(
            "children",
            BTreeMap::from([("parent".to_owned(), Value::Uuid(parent.0))]),
            InsertOptions {
                identity: WriteIdentity::Session(alice),
                ..Default::default()
            },
        ));
        if include_created {
            let error = result.expect_err("candidate-created sources must not use ordinary EXISTS");
            assert_eq!(error.code, ErrorCode::Query);
            assert!(error.message.contains("ExistsIncludingCreated"), "{error}");
        } else {
            assert!(result.is_ok(), "ordinary EXISTS remains supported");
        }
        let query = db.prepare_query(&Query::from("children")).unwrap();
        let rows = block_on(db.all(&query, ReadOpts::default())).unwrap();
        assert_eq!(rows.len(), usize::from(!include_created));
    }
}

/// Mallory cannot replace accepted catalogue metadata with a matching session claim.
/// Both the policy intrinsic and its lowered operand fail before returning any rows.
#[test]
fn catalogue_identity_intrinsics_cannot_be_spoofed_as_claims() {
    let mallory = AuthorSubject::for_test_bytes([0x71; 16]);
    let scope = uuid::Uuid::from_bytes([0x72; 16]);
    let key = format!("{}documents", jazz::query::TABLE_IDENTITY_OPERAND_PREFIX);
    for lowered_operand in [false, true] {
        let policy = if lowered_operand {
            PolicyExpr::True
        } else {
            PolicyExpr::eq_session(
                "scopeId",
                vec![
                    "__jazz_table_identity_v1".to_owned(),
                    "documents".to_owned(),
                ],
            )
        };
        let schema = common::compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("documents")
                        .column("scopeId", ColumnType::Uuid)
                        .policies(TablePolicies::new().with_select(policy)),
                )
                .build(),
        );
        let db = block_on(Db::open(DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x73; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        block_on(db.insert(
            "documents",
            BTreeMap::from([("scopeId".to_owned(), Value::Uuid(scope))]),
            Default::default(),
        ))
        .unwrap();
        db.set_identity_claims(mallory, BTreeMap::from([(key.clone(), Value::Uuid(scope))]));
        let query = if lowered_operand {
            Query::from("documents").filter(jazz::query::eq(
                jazz::query::col("scopeId"),
                jazz::query::claim(&key),
            ))
        } else {
            Query::from("documents")
        };
        let error = db
            .prepare_query(&query)
            .and_then(|query| block_on(db.all_for_identity(&query, ReadOpts::default(), mallory)))
            .expect_err("catalogue identity must not resolve through session claims");
        assert_eq!(error.code, ErrorCode::Query);
        assert!(
            error.message.contains("CatalogueTableIdentityV1"),
            "{error}"
        );
    }
}

/// Alice may read ordinary nested EXISTS sources, but neither an outer nor an
/// inner join may silently treat candidate-created evidence as accepted rows.
#[test]
fn nested_candidate_sources_remain_explicitly_unsupported() {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("parents").policies(common::allow_all_policies()))
            .table(
                TableSchemaBuilder::new("children")
                    .fk_column("parent", "parents")
                    .policies(common::allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("leaves")
                    .fk_column("child", "children")
                    .policies(common::allow_all_policies()),
            )
            .build(),
    );
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x78; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let leaf = Query::from("children")
        .join_via("leaves", "child", [])
        .joins
        .remove(0);
    let ordinary =
        Query::from("parents").join_via_with_nested_joins("children", "parent", [], [leaf]);
    let prepared = db.prepare_query(&ordinary).unwrap();
    assert!(
        block_on(db.all(&prepared, ReadOpts::default()))
            .unwrap()
            .is_empty()
    );
    for nested in [false, true] {
        let mut candidate = ordinary.clone();
        let join = if nested {
            &mut candidate.joins[0].nested_joins[0]
        } else {
            &mut candidate.joins[0]
        };
        join.source_mode = jazz::query::CandidateSourceMode::IncludeAuthorizedCreatedV1;
        let error = db
            .prepare_query(&candidate)
            .and_then(|prepared| block_on(db.all(&prepared, ReadOpts::default())))
            .expect_err("candidate-created joins need their independent authorization port");
        assert_eq!(error.code, ErrorCode::Query);
        assert!(error.message.contains("candidate-created"), "{error}");
    }
}

/// Mallory cannot make absent catalogue metadata authorize reads through a
/// session-null predicate; reserved paths fail before ordinary claim semantics.
#[test]
fn catalogue_identity_null_predicates_cannot_authorize_rows() {
    let path = vec![
        "__jazz_table_identity_v1".to_owned(),
        "documents".to_owned(),
    ];
    for policy in [
        PolicyExpr::SessionIsNull { path: path.clone() },
        PolicyExpr::Not(Box::new(PolicyExpr::SessionIsNotNull { path })),
    ] {
        let schema = common::compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("documents")
                        .column("body", ColumnType::Text)
                        .policies(TablePolicies::new().with_select(policy)),
                )
                .build(),
        );
        let db = block_on(Db::open(DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x79; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        block_on(db.insert(
            "documents",
            BTreeMap::from([(
                "body".to_owned(),
                Value::String("Alice's private note".to_owned()),
            )]),
            Default::default(),
        ))
        .unwrap();
        let prepared = db.prepare_query(&Query::from("documents")).unwrap();
        let error = block_on(db.all_for_identity(
            &prepared,
            ReadOpts::default(),
            AuthorSubject::for_test_bytes([0x7a; 16]),
        ))
        .expect_err("an unavailable catalogue intrinsic must not act like a null claim");
        assert_eq!(error.code, ErrorCode::Query);
        assert!(
            error.message.contains("CatalogueTableIdentityV1"),
            "{error}"
        );
    }
}
