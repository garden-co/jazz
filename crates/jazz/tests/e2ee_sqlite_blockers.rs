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
            let error = result
                .err()
                .expect("candidate-created sources must not use ordinary EXISTS");
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
