//! Relation-backed policies: a gather whose step walks an edge
//! table and joins back to the seed table reaches the joined rows, so an
//! access edge on an ancestor team grants.

use crate::common;
use common::compile_schema;
use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, WriteIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Operand, Predicate, Query};
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
    schema_with_resource_policies(
        TablePolicies::new()
            .with_select(select)
            .with_insert(PolicyExpr::True),
    )
}

fn schema_with_resource_policies(policies: TablePolicies) -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("resources")
                .column("label", ColumnType::Text)
                .policies(policies),
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

fn access(column: &str) -> RelationColumnRef {
    RelationColumnRef {
        scope: Some("access".to_owned()),
        column: column.to_owned(),
    }
}

fn eq(left: RelationColumnRef, right: RelationValueRef) -> RelationPredicate {
    RelationPredicate::Cmp {
        left,
        op: RelationCmpOp::Eq,
        right,
    }
}

fn outer_id() -> RelationValueRef {
    RelationValueRef::RowId(RelationRowIdRef::Outer)
}

fn text(value: &str) -> RelationValueRef {
    RelationValueRef::literal(Value::Text(value.to_owned()))
}

/// Access edges meeting `predicate` exist.
fn edges_where(predicate: RelationPredicate) -> PolicyExpr {
    PolicyExpr::ExistsRel {
        rel: RelationExpr::Filter {
            input: Box::new(RelationExpr::TableScan {
                table: "resource_access_edges".into(),
                alias: Some("access".to_owned()),
            }),
            predicate,
        },
    }
}

/// A viewer grant on the row itself: `access.resource = outer row id`, the
/// relation's only reference to the row it guards.
fn granted_policy() -> PolicyExpr {
    edges_where(RelationPredicate::And(vec![
        eq(access("resource"), outer_id()),
        eq(access("grant_role"), text("viewer")),
    ]))
}

/// The compiled read of `resources` for the reader `sub` (as [`read_as`]
/// reads it) and SQLite's plan for it.
fn read_plan(db: &Db, sub: &str) -> (String, String) {
    let prepared = db.prepare_query(&Query::from("resources")).unwrap();
    db.explain_query_plan(
        &prepared,
        Some(AuthorSubject::for_test_bytes([sub.len() as u8; 16])),
    )
    .unwrap()
}

/// Whether the read's policy runs once, as an uncorrelated `IN` list,
/// rather than once per row as a correlated `EXISTS`. White-box on
/// purpose: the rows a policy grants are the same either way, so only the
/// compiled shape shows which one ran.
fn lifted(db: &Db, sub: &str) -> bool {
    let (sql, plan) = read_plan(db, sub);
    let lifted = sql.contains("t.id IN (");
    assert_eq!(
        lifted,
        !sql.contains("EXISTS ("),
        "one form or the other: {sql}"
    );
    assert_eq!(
        lifted,
        !plan.contains("CORRELATED"),
        "SQLite runs the lifted form once: {plan}"
    );
    lifted
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
    assert!(lifted(&db, "member"));
    assert_eq!(read_as(&db, "member"), 1, "only the granted resource");
    // A read of listed rows, as a recheck of changed rows is, seeks each
    // row's grants instead.
    let picked = Query::from("resources").filter(Predicate::In(
        Operand::Column("id".to_owned()),
        vec![Operand::Literal(CoreValue::Uuid(id(3, 1).0))],
    ));
    let prepared = db.prepare_query(&picked).unwrap();
    let (sql, plan) = db
        .explain_query_plan(&prepared, Some(AuthorSubject::for_test_bytes([6; 16])))
        .unwrap();
    assert!(
        sql.contains("EXISTS (") && !sql.contains("t.id IN (SELECT"),
        "{sql}"
    );
    assert!(
        plan.contains("resource_access_edges__ix(resource)"),
        "{plan}"
    );
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
    // The edge names the resource outside the gather, so the recursive
    // team closure runs once per read rather than once per resource.
    assert!(lifted(&db, "member"));
    let (_, plan) = read_plan(&db, "member");
    assert!(plan.contains("LIST SUBQUERY"), "{plan}");
}

fn seed_three_resources(db: &Db, labels: [&str; 3]) {
    for (n, label) in labels.into_iter().enumerate() {
        seed(
            db,
            "resources",
            id(3, n as u8 + 1),
            &[("label", CoreValue::String(label.to_owned()))],
        );
    }
}

fn seed_edge(db: &Db, n: u8, resource: Option<RowUuid>, role: &str) {
    let resource = match resource {
        Some(row) => CoreValue::Uuid(row.0),
        None => CoreValue::Nullable(None),
    };
    seed(
        db,
        "resource_access_edges",
        id(4, n),
        &[
            ("resource", resource),
            ("team", CoreValue::Uuid(id(1, 1).0)),
            ("grant_role", CoreValue::String(role.to_owned())),
        ],
    );
}

/// A row-id key under an OR is not a key the schema takes yet: the other
/// arm could grant every row. The compiler's own precondition refuses to
/// lift it too (`compile::tests`).
#[test]
fn a_row_id_key_under_an_or_is_refused() {
    let refused = refusal(edges_where(RelationPredicate::Or(vec![
        eq(access("resource"), outer_id()),
        eq(access("grant_role"), text("public")),
    ])));
    assert!(refused.contains("under Or"), "{refused}");
}

/// Why a schema with this `resources` read policy does not compile.
fn refusal(select: PolicyExpr) -> String {
    let refused = jazz::schema::JazzSchema::new(&schema_with_resource_policy(select))
        .err()
        .expect("the policy is refused");
    format!("{refused:?}")
}

/// A relation that also compares a second column of the guarded row is
/// correlated beyond its id, so it stays a per-row EXISTS.
///
/// ```
/// resources: r1 "viewer"  r2 "editor"  r3 "viewer"
/// edges:     (r1, viewer) (r2, viewer)
/// "edge names the row and its role is the row's label" ──► r1
/// ```
#[test]
fn a_row_id_key_with_a_second_outer_column_stays_correlated() {
    let db = open(&schema_with_resource_policy(edges_where(
        RelationPredicate::And(vec![
            eq(access("resource"), outer_id()),
            eq(
                access("grant_role"),
                RelationValueRef::OuterColumn(RelationColumnRef {
                    scope: None,
                    column: "label".to_owned(),
                }),
            ),
        ]),
    )));
    seed_three_resources(&db, ["viewer", "editor", "viewer"]);
    seed_edge(&db, 1, Some(id(3, 1)), "viewer");
    seed_edge(&db, 2, Some(id(3, 2)), "viewer");
    assert!(!lifted(&db, "member"));
    assert_eq!(
        read_as(&db, "member"),
        1,
        "only r1's label matches its edge"
    );
}

/// A row-id key inside a gather's seed would start the walk from the
/// guarded row itself, a closure per row; the schema refuses it, and the
/// compiler's precondition would not lift it (`compile::tests`).
#[test]
fn a_row_id_key_inside_a_gather_seed_is_refused() {
    let seeded_from_row = PolicyExpr::ExistsRel {
        rel: RelationExpr::Join {
            left: Box::new(RelationExpr::Gather {
                seed: Box::new(RelationExpr::Filter {
                    input: Box::new(RelationExpr::TableScan {
                        table: "teams".into(),
                        alias: None,
                    }),
                    predicate: eq(
                        RelationColumnRef {
                            scope: Some("teams".to_owned()),
                            column: "id".to_owned(),
                        },
                        outer_id(),
                    ),
                }),
                step: team_step(),
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
                right: access("team"),
            }],
            join_kind: RelationJoinKind::Inner,
        },
    };
    let refused = refusal(seeded_from_row);
    assert!(refused.contains("Gather seed"), "{refused}");
}

/// A deny policy, a negated relation, is not a form the schema takes yet:
/// it is refused when the schema compiles, not compiled either way.
#[test]
fn a_negated_relation_policy_is_refused() {
    let refused = refusal(PolicyExpr::Not(Box::new(granted_policy())));
    assert!(refused.contains("Not"), "{refused}");
}

/// An insert check keyed by the new row's id checks one row: it keeps the
/// per-row EXISTS, which seeks the grants naming that row by index rather
/// than listing every row a grant names, and admits exactly those rows.
///
/// ```
/// edges: (r1, viewer)
/// alice ──insert r1──► ✓     alice ──insert r2──► ✗
/// ```
#[test]
fn an_insert_check_keyed_by_the_row_id_admits_the_rows_it_names() {
    let db = open(&schema_with_resource_policies(
        TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(granted_policy()),
    ));
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    seed_edge(&db, 1, Some(id(3, 1)), "viewer");
    let plan = db
        .explain_write_check_plan("resources", id(3, 1), alice, &[])
        .unwrap();
    assert!(
        plan.contains("resource_access_edges__ix(resource)") && !plan.contains("LIST SUBQUERY"),
        "{plan}"
    );
    let insert = |row: RowUuid| {
        block_on(db.insert(
            "resources",
            BTreeMap::from([("label".to_owned(), CoreValue::String("new".to_owned()))]),
            InsertOptions {
                row_id: Some(row),
                identity: WriteIdentity::Session(alice),
                ..Default::default()
            },
        ))
    };
    insert(id(3, 1)).expect("a granted row is admitted");
    insert(id(3, 2)).expect_err("an ungranted row is refused");
}

fn team_step() -> Box<RelationExpr> {
    let RelationExpr::Filter { input, .. } = resource_access_policy_rel() else {
        unreachable!("the access policy filters its join")
    };
    let RelationExpr::Join { left, .. } = *input else {
        unreachable!("the access policy joins the gather")
    };
    let RelationExpr::Gather { step, .. } = *left else {
        unreachable!("the join's left side is the gather")
    };
    step
}

fn resource_access_policy_rel() -> RelationExpr {
    let PolicyExpr::ExistsRel { rel } = resource_access_policy() else {
        unreachable!("the access policy is a relation")
    };
    rel
}
