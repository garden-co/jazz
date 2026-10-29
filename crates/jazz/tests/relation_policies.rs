//! Relation-backed policies: a gather whose step walks an edge
//! table and joins back to the seed table reaches the joined rows, so an
//! access edge on an ancestor team grants.

use crate::common;
use common::compile_schema;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value as CoreValue;
use jazz::tools::public_schema::{
    RecursionBound, RelationCmpOp, RelationColumnRef, RelationExpr, RelationJoinCondition,
    RelationJoinKind, RelationKeyRef, RelationPredicate, RelationRowIdRef, RelationValueRef,
    TablePolicies,
};
use jazz::tools::{ColumnType, PolicyExpr, Schema, SchemaBuilder, TableSchema, Value};
use std::collections::BTreeMap;
fn policy_graph_policy_schema() -> Schema {
    schema_with_resource_policy(resource_access_policy())
}

fn schema_with_resource_policy(select: PolicyExpr) -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("resources")
                .column("label", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(select)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("teams")
                .column("identity_key", ColumnType::Text)
                .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
        )
        .table(
            TableSchema::builder("team_team_edges")
                .fk_column("child_team", "teams")
                .fk_column("parent_team", "teams")
                .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
        )
        .table(
            TableSchema::builder("resource_access_edges")
                .nullable_fk_column("resource", "resources")
                .fk_column("team", "teams")
                .column("grant_role", ColumnType::Text)
                .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
        )
        .build()
}

fn resource_access_policy() -> PolicyExpr {
    PolicyExpr::ExistsRel {
        rel: RelationExpr::Filter {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::Gather {
                    seed: Box::new(RelationExpr::Filter {
                        input: Box::new(RelationExpr::TableScan {
                            table: "teams".into(),
                            alias: None,
                        }),
                        predicate: RelationPredicate::Cmp {
                            left: RelationColumnRef {
                                scope: Some("teams".to_owned()),
                                column: "identity_key".to_owned(),
                            },
                            op: RelationCmpOp::Eq,
                            right: RelationValueRef::SessionRef(vec![
                                "claims".to_owned(),
                                "sub".to_owned(),
                            ]),
                        },
                    }),
                    step: Box::new(RelationExpr::Project {
                        input: Box::new(RelationExpr::Join {
                            left: Box::new(RelationExpr::Filter {
                                input: Box::new(RelationExpr::TableScan {
                                    table: "team_team_edges".into(),
                                    alias: None,
                                }),
                                predicate: RelationPredicate::Cmp {
                                    left: RelationColumnRef {
                                        scope: Some("team_team_edges".to_owned()),
                                        column: "child_team".to_owned(),
                                    },
                                    op: RelationCmpOp::Eq,
                                    right: RelationValueRef::RowId(RelationRowIdRef::Frontier),
                                },
                            }),
                            right: Box::new(RelationExpr::TableScan {
                                table: "teams".into(),
                                alias: Some("__recursive_hop_0".to_owned()),
                            }),
                            on: vec![RelationJoinCondition {
                                left: RelationColumnRef {
                                    scope: Some("team_team_edges".to_owned()),
                                    column: "parent_team".to_owned(),
                                },
                                right: RelationColumnRef {
                                    scope: Some("__recursive_hop_0".to_owned()),
                                    column: "id".to_owned(),
                                },
                            }],
                            join_kind: RelationJoinKind::Inner,
                        }),
                        columns: Vec::new(),
                    }),
                    frontier_key: RelationKeyRef::RowId(RelationRowIdRef::Current),
                    bound: RecursionBound::MaxDepth(8),
                    dedupe_key: vec![RelationKeyRef::RowId(RelationRowIdRef::Current)],
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "resource_access_edges".into(),
                    alias: Some("access".to_owned()),
                }),
                on: vec![RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: None,
                        column: "id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("access".to_owned()),
                        column: "team".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            predicate: RelationPredicate::And(vec![
                RelationPredicate::Cmp {
                    left: RelationColumnRef {
                        scope: Some("access".to_owned()),
                        column: "resource".to_owned(),
                    },
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::RowId(RelationRowIdRef::Outer),
                },
                RelationPredicate::Cmp {
                    left: RelationColumnRef {
                        scope: Some("access".to_owned()),
                        column: "grant_role".to_owned(),
                    },
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::literal(Value::Text("viewer".to_owned())),
                },
            ]),
        },
    }
}

fn id(kind: u8, n: u8) -> RowUuid {
    RowUuid::from_bytes([kind, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

fn seed(db: &Db, table: &str, row: RowUuid, pairs: &[(&str, CoreValue)]) {
    let cells = pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect::<BTreeMap<_, _>>();
    db.seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, cells)
        .unwrap();
}

fn read_as(db: &Db, sub: &str) -> usize {
    let author = AuthorSubject::for_test_bytes([sub.len() as u8; 16]);
    let prepared = db
        .prepare_query(&Query::from("resources"))
        .unwrap()
        .with_identity_claims(
            author,
            BTreeMap::from([(
                jazz::query::provider_claim_key("sub"),
                CoreValue::String(sub.to_owned()),
            )]),
        );
    block_on(db.all_for_identity(&prepared, ReadOpts::default(), author))
        .unwrap()
        .len()
}

fn open(schema: &Schema) -> Db {
    block_on(Db::open_history_complete(DbConfig::new(
        compile_schema(schema),
        DbIdentity {
            node: NodeUuid::from_bytes([0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

/// A viewer grant on the row itself: `access.resource = outer row id`, the
/// relation's only reference to the row it guards.
fn granted_policy() -> PolicyExpr {
    PolicyExpr::ExistsRel {
        rel: RelationExpr::Filter {
            input: Box::new(RelationExpr::TableScan {
                table: "resource_access_edges".into(),
                alias: Some("access".to_owned()),
            }),
            predicate: RelationPredicate::And(vec![
                RelationPredicate::Cmp {
                    left: RelationColumnRef {
                        scope: Some("access".to_owned()),
                        column: "resource".to_owned(),
                    },
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::RowId(RelationRowIdRef::Outer),
                },
                RelationPredicate::Cmp {
                    left: RelationColumnRef {
                        scope: Some("access".to_owned()),
                        column: "grant_role".to_owned(),
                    },
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::literal(Value::Text("viewer".to_owned())),
                },
            ]),
        },
    }
}

/// A relation keyed only by the guarded row's id runs once for all rows
/// rather than once per row, with the same answer: only a matching edge
/// that names the row grants it; an edge naming no resource grants none.
///
/// ```
/// edges: (granted, viewer) (NULL, viewer) (other, editor)
/// member ──"has a viewer grant"──► granted   (not open, not other)
/// ```
#[test]
fn a_relation_keyed_by_the_row_id_grants_only_the_rows_it_names() {
    let s = |v: &str| CoreValue::String(v.to_owned());
    let u = |r: RowUuid| CoreValue::Uuid(r.0);
    let seed_rows = |db: &Db| {
        seed(db, "resources", id(3, 1), &[("label", s("granted"))]);
        seed(db, "resources", id(3, 2), &[("label", s("open"))]);
        seed(db, "resources", id(3, 3), &[("label", s("other"))]);
        seed(db, "teams", id(1, 1), &[("identity_key", s("member"))]);
        seed(
            db,
            "resource_access_edges",
            id(4, 1),
            &[
                ("resource", u(id(3, 1))),
                ("team", u(id(1, 1))),
                ("grant_role", s("viewer")),
            ],
        );
        seed(
            db,
            "resource_access_edges",
            id(4, 2),
            &[
                ("resource", CoreValue::Nullable(None)),
                ("team", u(id(1, 1))),
                ("grant_role", s("viewer")),
            ],
        );
        seed(
            db,
            "resource_access_edges",
            id(4, 3),
            &[
                ("resource", u(id(3, 3))),
                ("team", u(id(1, 1))),
                ("grant_role", s("editor")),
            ],
        );
    };
    let db = open(&schema_with_resource_policy(granted_policy()));
    seed_rows(&db);
    assert_eq!(read_as(&db, "member"), 1, "only the granted resource");
}

#[test]
fn a_gather_through_an_edge_table_reaches_the_ancestor_team() {
    let db = block_on(Db::open_history_complete(DbConfig::new(
        compile_schema(&policy_graph_policy_schema()),
        DbIdentity {
            node: NodeUuid::from_bytes([0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let s = |v: &str| CoreValue::String(v.to_owned());
    let u = |r: RowUuid| CoreValue::Uuid(r.0);
    seed(&db, "teams", id(1, 1), &[("identity_key", s("member"))]);
    seed(&db, "teams", id(1, 2), &[("identity_key", s("other-sub"))]);
    seed(
        &db,
        "team_team_edges",
        id(2, 1),
        &[("child_team", u(id(1, 1))), ("parent_team", u(id(1, 2)))],
    );
    seed(&db, "resources", id(3, 1), &[("label", s("visible"))]);
    seed(
        &db,
        "resource_access_edges",
        id(4, 1),
        &[
            ("resource", u(id(3, 1))),
            ("team", u(id(1, 2))),
            ("grant_role", s("viewer")),
        ],
    );
    assert_eq!(read_as(&db, "member"), 1, "granted through the parent team");
    assert_eq!(read_as(&db, "spy"), 0);
}
