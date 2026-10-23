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

/// Alice may use an already accepted parent through either EXISTS form.
/// Same-unit creation still requires the independent proof tested separately.
#[test]
fn candidate_create_permissions_keep_accepted_parent_evidence() {
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
        assert!(result.is_ok(), "accepted parent evidence remains supported");
        let query = db.prepare_query(&Query::from("children")).unwrap();
        let rows = block_on(db.all(&query, ReadOpts::default())).unwrap();
        assert_eq!(rows.len(), 1);
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
