//! A relation `Gather` means the same thing whether it appears in a query or in
//! a relation-backed policy (`PolicyExpr::ExistsRel`). Each case lowers one
//! gather through both front ends and requires the same recursive edge, or the
//! same rejection.

use crate::model::public_schema::{
    ColumnType, PolicyExpr, RecursionBound, RelationCmpOp, RelationColumnRef, RelationExpr,
    RelationJoinCondition, RelationJoinKind, RelationKeyRef, RelationPredicate,
    RelationProjectColumn, RelationProjectExpr, RelationRowIdRef, RelationValueRef, SchemaBuilder,
    TablePolicies, TableSchemaBuilder,
};
use crate::model::public_schema_convert::convert_public_schema;
use crate::query::{JoinCorrelation, Predicate, RelationQuery, relation_query_to_query};

#[derive(Debug, PartialEq)]
struct GatherEdge {
    edge_table: String,
    edge_member_column: String,
    edge_parent_column: String,
    edge_filters: Vec<Predicate>,
    bound: RecursionBound,
}

#[derive(Debug)]
struct Lowerings {
    query: Result<GatherEdge, String>,
    policy: Result<GatherEdge, String>,
}

fn column(scope: &str, column: &str) -> RelationColumnRef {
    RelationColumnRef {
        scope: Some(scope.to_owned()),
        column: column.to_owned(),
    }
}

fn equals(scope: &str, column_name: &str, right: RelationValueRef) -> RelationPredicate {
    RelationPredicate::Cmp {
        left: column(scope, column_name),
        op: RelationCmpOp::Eq,
        right,
    }
}

fn scan(table: &str, alias: Option<&str>) -> RelationExpr {
    RelationExpr::TableScan {
        table: table.into(),
        alias: alias.map(str::to_owned),
    }
}

fn filter(input: RelationExpr, predicate: RelationPredicate) -> RelationExpr {
    RelationExpr::Filter {
        input: Box::new(input),
        predicate,
    }
}

/// Teams the session owns.
fn owned_teams_seed() -> RelationExpr {
    filter(
        scan("teams", None),
        equals(
            "teams",
            "owner_id",
            RelationValueRef::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
        ),
    )
}

/// The frontier team's own row, filtered to enabled teams.
fn frontier_team_rows() -> RelationExpr {
    filter(
        scan("teams", None),
        RelationPredicate::And(vec![
            equals(
                "teams",
                "id",
                RelationValueRef::RowId(RelationRowIdRef::Frontier),
            ),
            equals(
                "teams",
                "enabled",
                RelationValueRef::literal(crate::model::public_schema::Value::Boolean(true)),
            ),
        ]),
    )
}

/// Step form emitted by the TypeScript query builder: hop the parent FK back
/// to a `teams` row and project that row.
fn joined_parent_step(target_column: &str) -> RelationExpr {
    joined_parent_step_projecting(target_column, "id")
}

fn joined_parent_step_projecting(target_column: &str, projected_column: &str) -> RelationExpr {
    RelationExpr::Project {
        input: Box::new(RelationExpr::Join {
            left: Box::new(frontier_team_rows()),
            right: Box::new(scan("teams", Some("__recursive_hop_0"))),
            on: vec![RelationJoinCondition {
                left: column("teams", "parent"),
                right: column("__recursive_hop_0", target_column),
            }],
            join_kind: RelationJoinKind::Inner,
        }),
        columns: vec![RelationProjectColumn {
            alias: "id".to_owned(),
            expr: RelationProjectExpr::Column(column("__recursive_hop_0", projected_column)),
        }],
    }
}

/// Step form emitted by the TypeScript policy builder: carry only the parent
/// key across the frontier.
fn scalar_parent_step() -> RelationExpr {
    RelationExpr::Project {
        input: Box::new(frontier_team_rows()),
        columns: vec![RelationProjectColumn {
            alias: "id".to_owned(),
            expr: RelationProjectExpr::Column(column("teams", "parent")),
        }],
    }
}

fn gather(step: RelationExpr) -> RelationExpr {
    RelationExpr::Gather {
        seed: Box::new(owned_teams_seed()),
        step: Box::new(step),
        frontier_key: RelationKeyRef::RowId(RelationRowIdRef::Current),
        bound: RecursionBound::MaxDepth(4),
        dedupe_key: vec![RelationKeyRef::RowId(RelationRowIdRef::Current)],
    }
}

fn with_gather_fields(
    mut rel: RelationExpr,
    edit: impl FnOnce(&mut RelationKeyRef, &mut RecursionBound, &mut Vec<RelationKeyRef>),
) -> RelationExpr {
    let RelationExpr::Gather {
        frontier_key,
        bound,
        dedupe_key,
        ..
    } = &mut rel
    else {
        unreachable!("helper only edits gathers");
    };
    edit(frontier_key, bound, dedupe_key);
    rel
}

fn query_lowering(rel: &RelationExpr) -> Result<GatherEdge, String> {
    let query = relation_query_to_query(&RelationQuery { rel: rel.clone() })
        .map_err(|err| err.to_string())?;
    let [reachable] = query.reachable.as_slice() else {
        return Err(format!("expected one reachable, got {query:?}"));
    };
    Ok(GatherEdge {
        edge_table: reachable.edge_table.clone(),
        edge_member_column: reachable.edge_member_column.clone(),
        edge_parent_column: reachable.edge_parent_column.clone(),
        edge_filters: reachable.edge_filters.clone(),
        bound: reachable.bound.clone(),
    })
}

fn policy_lowering(rel: &RelationExpr) -> Result<GatherEdge, String> {
    let policy = PolicyExpr::ExistsRel {
        rel: filter(
            RelationExpr::Join {
                left: Box::new(rel.clone()),
                right: Box::new(scan("doc_access", Some("access"))),
                on: vec![RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: None,
                        column: "id".to_owned(),
                    },
                    right: column("access", "team"),
                }],
                join_kind: RelationJoinKind::Inner,
            },
            equals(
                "access",
                "doc",
                RelationValueRef::RowId(RelationRowIdRef::Outer),
            ),
        ),
    };
    let read_policy = docs_read_policy(policy)?;
    let [reachable] = read_policy.reachable.as_slice() else {
        return Err(format!("expected one reachable, got {read_policy:?}"));
    };
    Ok(GatherEdge {
        edge_table: reachable.edge_table.clone(),
        edge_member_column: reachable.edge_member_column.clone(),
        edge_parent_column: reachable.edge_parent_column.clone(),
        edge_filters: reachable.edge_filters.clone(),
        bound: reachable.bound.clone(),
    })
}

/// Compile `policy` as the docs read policy and return its core query.
fn docs_read_policy(policy: PolicyExpr) -> Result<crate::query::Query, String> {
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("docs")
                .column("title", ColumnType::Text)
                .column("workspace", ColumnType::Uuid)
                .fk_column("team", "teams")
                .fk_column("team2", "teams")
                .policies(TablePolicies::new().with_select(policy)),
        )
        .table(
            TableSchemaBuilder::new("teams")
                .column("owner_id", ColumnType::Uuid)
                .column("enabled", ColumnType::Boolean)
                .nullable_column("archived_at", ColumnType::Timestamp)
                .fk_column("parent", "teams"),
        )
        .table(
            TableSchemaBuilder::new("doc_access")
                .fk_column("doc", "docs")
                .fk_column("team", "teams")
                .column("workspace", ColumnType::Uuid),
        )
        .build();
    let converted = convert_public_schema(&schema).map_err(|err| err.to_string())?;
    let docs = converted
        .tables()
        .iter()
        .find(|table| table.name == "docs")
        .expect("docs table converts");
    Ok(docs.read_policy.clone().expect("docs read policy"))
}

fn lower_both(rel: &RelationExpr) -> Lowerings {
    Lowerings {
        query: query_lowering(rel),
        policy: policy_lowering(rel),
    }
}

fn assert_both_accept(rel: &RelationExpr) -> GatherEdge {
    let lowered = lower_both(rel);
    let (Ok(query), Ok(policy)) = (&lowered.query, &lowered.policy) else {
        panic!("both front ends must accept this gather: {lowered:#?}");
    };
    assert_eq!(
        query, policy,
        "query and policy lower the gather differently"
    );
    lowered.query.unwrap()
}

fn assert_both_reject(rel: &RelationExpr) {
    let lowered = lower_both(rel);
    assert!(
        lowered.query.is_err() && lowered.policy.is_err(),
        "both front ends must reject this gather: {lowered:#?}"
    );
}

fn expected_edge(bound: RecursionBound) -> GatherEdge {
    GatherEdge {
        edge_table: "teams".to_owned(),
        edge_member_column: "id".to_owned(),
        edge_parent_column: "parent".to_owned(),
        edge_filters: vec![Predicate::Eq(
            crate::query::Operand::Column("enabled".to_owned()),
            crate::query::Operand::Literal(jazz_types::records::Value::Bool(true)),
        )],
        bound,
    }
}

#[test]
fn joined_parent_step_lowers_identically() {
    let edge = assert_both_accept(&gather(joined_parent_step("id")));
    assert_eq!(edge, expected_edge(RecursionBound::MaxDepth(4)));
}

#[test]
fn scalar_parent_step_lowers_identically() {
    let edge = assert_both_accept(&gather(scalar_parent_step()));
    assert_eq!(edge, expected_edge(RecursionBound::MaxDepth(4)));
}

#[test]
fn fixpoint_bound_lowers_identically() {
    for step in [joined_parent_step("id"), scalar_parent_step()] {
        let rel = with_gather_fields(gather(step), |_, bound, _| {
            *bound = RecursionBound::Fixpoint;
        });
        let edge = assert_both_accept(&rel);
        assert_eq!(edge, expected_edge(RecursionBound::Fixpoint));
    }
}

#[test]
fn joined_step_without_an_id_projection_keeps_the_target_row_id() {
    // An empty projection passes the hop target row through, so its row id is
    // still the next frontier.
    let RelationExpr::Project { input, .. } = joined_parent_step("id") else {
        unreachable!("joined_parent_step returns a projection");
    };
    let step = RelationExpr::Project {
        input,
        columns: Vec::new(),
    };
    let edge = assert_both_accept(&gather(step));
    assert_eq!(edge, expected_edge(RecursionBound::MaxDepth(4)));
}

#[test]
fn step_joining_a_non_row_id_target_column_is_rejected() {
    // `teams.parent = hop.owner_id` does not name the parent row, so it cannot
    // be lowered as a parent-id reachability edge.
    assert_both_reject(&gather(joined_parent_step("owner_id")));
}

#[test]
fn column_frontier_key_is_rejected() {
    let rel = with_gather_fields(gather(scalar_parent_step()), |frontier_key, _, _| {
        *frontier_key = RelationKeyRef::Column(column("teams", "parent"));
    });
    assert_both_reject(&rel);
}

#[test]
fn non_row_id_dedupe_key_is_rejected() {
    let rel = with_gather_fields(gather(scalar_parent_step()), |_, _, dedupe_key| {
        *dedupe_key = vec![RelationKeyRef::Column(column("teams", "owner_id"))];
    });
    assert_both_reject(&rel);
}

#[test]
fn step_projecting_another_target_column_as_id_is_rejected() {
    // Projecting `hop.parent` makes the next frontier the grandparent, which
    // a one-hop parent edge cannot express.
    assert_both_reject(&gather(joined_parent_step_projecting("id", "parent")));
}

#[test]
fn joined_step_projecting_ambiguous_or_edge_columns_is_rejected() {
    // In a joined step both the edge and the output row are in scope, so an
    // unscoped column could name either one. Only output columns may be projected.
    let projections = [
        (
            "id",
            RelationProjectExpr::Column(RelationColumnRef::unscoped("id")),
        ),
        (
            "parent",
            RelationProjectExpr::Column(column("teams", "parent")),
        ),
        (
            "x",
            RelationProjectExpr::Column(RelationColumnRef::unscoped("parent")),
        ),
        ("x", RelationProjectExpr::RowId(RelationRowIdRef::Frontier)),
    ];
    for (alias, expr) in projections {
        let RelationExpr::Project { input, .. } = joined_parent_step("id") else {
            unreachable!("joined_parent_step returns a projection");
        };
        let step = RelationExpr::Project {
            input,
            columns: vec![RelationProjectColumn {
                alias: alias.to_owned(),
                expr,
            }],
        };
        assert_both_reject(&gather(step));
    }
}

/// Gather from a seed whose rows hop to `teams` before starting the frontier.
fn gather_from_seed(seed: RelationExpr) -> RelationExpr {
    let RelationExpr::Gather {
        step,
        frontier_key,
        bound,
        dedupe_key,
        ..
    } = gather(scalar_parent_step())
    else {
        unreachable!("gather builds a Gather");
    };
    RelationExpr::Gather {
        seed: Box::new(seed),
        step,
        frontier_key,
        bound,
        dedupe_key,
    }
}

fn hopped_seed(target_column: &str, join_kind: RelationJoinKind) -> RelationExpr {
    RelationExpr::Project {
        input: Box::new(RelationExpr::Join {
            left: Box::new(owned_teams_seed()),
            right: Box::new(scan("teams", Some("hop"))),
            on: vec![RelationJoinCondition {
                left: column("teams", "parent"),
                right: column("hop", target_column),
            }],
            join_kind,
        }),
        columns: vec![RelationProjectColumn {
            alias: "id".to_owned(),
            expr: RelationProjectExpr::Column(column("hop", "id")),
        }],
    }
}

#[test]
fn seed_hop_onto_the_target_id_lowers_identically() {
    let edge = assert_both_accept(&gather_from_seed(hopped_seed(
        "id",
        RelationJoinKind::Inner,
    )));
    assert_eq!(edge, expected_edge(RecursionBound::MaxDepth(4)));
}

#[test]
fn seed_hop_onto_a_non_id_target_column_is_rejected() {
    assert_both_reject(&gather_from_seed(hopped_seed(
        "owner_id",
        RelationJoinKind::Inner,
    )));
}

#[test]
fn left_seed_hop_is_rejected() {
    assert_both_reject(&gather_from_seed(hopped_seed("id", RelationJoinKind::Left)));
}

/// After the hop both the seed and the target have an `id`, so an unscoped
/// `id` does not say which row the walk starts from.
#[test]
fn seed_hop_projecting_an_unscoped_id_is_rejected() {
    let RelationExpr::Project { input, .. } = hopped_seed("id", RelationJoinKind::Inner) else {
        unreachable!("hopped_seed builds a Project");
    };
    assert_both_reject(&gather_from_seed(RelationExpr::Project {
        input,
        columns: vec![RelationProjectColumn {
            alias: "id".to_owned(),
            expr: RelationProjectExpr::Column(RelationColumnRef::unscoped("id")),
        }],
    }));
}

/// Only policies key their seed by a session claim; a query keeps every seed
/// filter, so this one is policy-specific.
#[test]
fn policy_seed_claim_inequality_does_not_key_the_seed() {
    // `owner_id != me` must not become the claim-keyed seed `owner_id = me`.
    let seed = filter(
        scan("teams", None),
        RelationPredicate::Cmp {
            left: column("teams", "owner_id"),
            op: RelationCmpOp::Ne,
            right: RelationValueRef::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
        },
    );
    let lowered = policy_lowering(&gather_from_seed(seed));
    assert!(lowered.is_err(), "{lowered:#?}");
}

/// A scalar step whose edge rows must also satisfy `edge_filter`.
fn gather_with_edge_filter(edge_filter: RelationPredicate) -> RelationExpr {
    gather(RelationExpr::Project {
        input: Box::new(filter(
            scan("teams", None),
            RelationPredicate::And(vec![
                equals(
                    "teams",
                    "id",
                    RelationValueRef::RowId(RelationRowIdRef::Frontier),
                ),
                edge_filter,
            ]),
        )),
        columns: vec![RelationProjectColumn {
            alias: "id".to_owned(),
            expr: RelationProjectExpr::Column(column("teams", "parent")),
        }],
    })
}

fn compare(column_name: &str, op: RelationCmpOp, right: RelationValueRef) -> RelationPredicate {
    RelationPredicate::Cmp {
        left: column("teams", column_name),
        op,
        right,
    }
}

#[test]
fn bare_and_typed_literals_lower_to_the_same_value() {
    let bare = assert_both_accept(&gather_with_edge_filter(compare(
        "enabled",
        RelationCmpOp::Eq,
        RelationValueRef::Literal(serde_json::json!(true)),
    )));
    let typed = assert_both_accept(&gather_with_edge_filter(compare(
        "enabled",
        RelationCmpOp::Eq,
        RelationValueRef::literal(crate::model::public_schema::Value::Boolean(true)),
    )));
    assert_eq!(bare, typed);
    assert_eq!(bare, expected_edge(RecursionBound::MaxDepth(4)));
}

#[test]
fn null_comparisons_lower_to_null_tests() {
    let null = || RelationValueRef::literal(crate::model::public_schema::Value::Null);
    let owner = || crate::query::Operand::Column("archived_at".to_owned());
    let is_null = assert_both_accept(&gather_with_edge_filter(compare(
        "archived_at",
        RelationCmpOp::Eq,
        null(),
    )));
    assert_eq!(is_null.edge_filters, vec![Predicate::IsNull(owner())]);
    let not_null = assert_both_accept(&gather_with_edge_filter(compare(
        "archived_at",
        RelationCmpOp::Ne,
        RelationValueRef::Literal(serde_json::Value::Null),
    )));
    assert_eq!(
        not_null.edge_filters,
        vec![Predicate::Not(Box::new(Predicate::IsNull(owner())))]
    );
    assert_both_reject(&gather_with_edge_filter(compare(
        "archived_at",
        RelationCmpOp::Lt,
        null(),
    )));
}

#[test]
fn unscoped_edge_filters_lower_identically() {
    let scoped = assert_both_accept(&gather_with_edge_filter(compare(
        "enabled",
        RelationCmpOp::Eq,
        RelationValueRef::Literal(serde_json::json!(true)),
    )));
    let unscoped = assert_both_accept(&gather_with_edge_filter(RelationPredicate::Cmp {
        left: RelationColumnRef::unscoped("enabled"),
        op: RelationCmpOp::Eq,
        right: RelationValueRef::Literal(serde_json::json!(true)),
    }));
    assert_eq!(scoped, unscoped);
}

/// Compile a docs read policy over access rows filtered by `predicate`, and
/// return the conversion error.
fn policy_access_error(predicate: RelationPredicate) -> String {
    let access_rel = filter(scan("doc_access", Some("access")), predicate);
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("docs")
                .column("title", ColumnType::Text)
                .policies(
                    TablePolicies::new().with_select(PolicyExpr::ExistsRel { rel: access_rel }),
                ),
        )
        .table(TableSchemaBuilder::new("teams").column("enabled", ColumnType::Boolean))
        .table(
            TableSchemaBuilder::new("doc_access")
                .fk_column("doc", "docs")
                .fk_column("team", "teams"),
        )
        .build();
    convert_public_schema(&schema)
        .expect_err("the policy must not compile")
        .to_string()
}

fn outer_doc() -> RelationPredicate {
    equals(
        "access",
        "doc",
        RelationValueRef::RowId(RelationRowIdRef::Outer),
    )
}

/// Under `Or`, `Not` or an enum payload, a row correlation cannot become a
/// join. Dropping it as `true` would let every access row authorize the
/// document.
#[test]
fn policy_row_correlation_under_or_not_or_enum_payload_is_rejected() {
    let cases = [
        (
            "Or",
            RelationPredicate::Or(vec![outer_doc(), RelationPredicate::False]),
        ),
        ("Not", RelationPredicate::Not(Box::new(outer_doc()))),
        (
            "EnumMatch",
            RelationPredicate::EnumMatch {
                column: column("access", "team"),
                case: "member".to_owned(),
                payload: Box::new(RelationPredicate::Cmp {
                    left: RelationColumnRef::unscoped("doc"),
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::RowId(RelationRowIdRef::Outer),
                }),
            },
        ),
    ];
    for (under, predicate) in cases {
        let error = policy_access_error(predicate);
        assert!(
            error.contains(&format!(
                "Row-id correlations under {under} are not supported in policies yet"
            )),
            "{under}: {error}"
        );
    }
}

/// A walk's seed and edge rows cannot be joined to the protected row: as a
/// `true` placeholder, the correlation would drop a restriction such as "the
/// edge belongs to this document's workspace" and widen the policy.
#[test]
fn edge_filters_correlated_with_the_outer_row_are_rejected() {
    for correlation in [
        RelationValueRef::RowId(RelationRowIdRef::Outer),
        RelationValueRef::OuterColumn(RelationColumnRef::unscoped("title")),
    ] {
        let rel = gather_with_edge_filter(equals("teams", "owner_id", correlation));
        assert_both_reject(&rel);
        let error = policy_lowering(&rel).unwrap_err();
        assert!(
            error.contains("Gather edge filters correlated with the protected row"),
            "{error}"
        );
    }
}

#[test]
fn seed_filters_correlated_with_the_outer_row_are_rejected() {
    let seed = filter(
        owned_teams_seed(),
        equals(
            "teams",
            "owner_id",
            RelationValueRef::OuterColumn(RelationColumnRef::unscoped("title")),
        ),
    );
    let rel = gather_from_seed(seed);
    assert_both_reject(&rel);
    let error = policy_lowering(&rel).unwrap_err();
    assert!(
        error.contains("Gather seed filters correlated with the protected row"),
        "{error}"
    );
}

/// Only a gather step's one frontier equality binds to the walk.
#[test]
fn frontier_references_outside_a_step_are_rejected() {
    let frontier = || RelationValueRef::RowId(RelationRowIdRef::Frontier);
    let seed = filter(owned_teams_seed(), equals("teams", "parent", frontier()));
    assert_both_reject(&gather_from_seed(seed));

    let error = policy_access_error(RelationPredicate::And(vec![
        outer_doc(),
        equals("access", "team", frontier()),
    ]));
    assert!(
        error.contains("Frontier row-id references outside a gather step"),
        "{error}"
    );
    let query = relation_query_to_query(&RelationQuery {
        rel: filter(
            scan("doc_access", Some("access")),
            equals("access", "team", frontier()),
        ),
    });
    assert!(query.is_err(), "{query:?}");
}

/// `Join(gather, access)` on the access row's team, as the TypeScript policy
/// builder emits it.
fn gather_access_join(access: RelationExpr) -> RelationExpr {
    RelationExpr::Join {
        left: Box::new(gather(scalar_parent_step())),
        right: Box::new(access),
        on: vec![RelationJoinCondition {
            left: RelationColumnRef::unscoped("id"),
            right: column("access", "team"),
        }],
        join_kind: RelationJoinKind::Inner,
    }
}

fn outer_workspace(scope: &str) -> RelationPredicate {
    equals(
        scope,
        "workspace",
        RelationValueRef::OuterColumn(RelationColumnRef::unscoped("workspace")),
    )
}

fn access_policy_error(rel: RelationExpr) -> String {
    match docs_read_policy(PolicyExpr::ExistsRel { rel }) {
        Ok(policy) => panic!("the policy must not compile, got {policy:?}"),
        Err(error) => error,
    }
}

/// A walk's access rows reach the protected row through one key column. A
/// second correlation such as "the access row is in the document's
/// workspace", or a join off the access rows, has nowhere to go in the
/// lowered walk; dropping it would let the access row authorize documents
/// in other workspaces, so each is rejected.
#[test]
fn gather_access_rows_keep_every_restriction_or_are_rejected() {
    let access = || scan("doc_access", Some("access"));
    let keyed_on_doc = || outer_doc();

    // The supported shape: one correlation above the access join.
    let accepted = docs_read_policy(PolicyExpr::ExistsRel {
        rel: filter(gather_access_join(access()), keyed_on_doc()),
    })
    .expect("one access correlation compiles");
    assert_eq!(accepted.reachable.len(), 1);

    // A second correlation above the access join.
    let error = access_policy_error(filter(
        gather_access_join(access()),
        RelationPredicate::And(vec![keyed_on_doc(), outer_workspace("access")]),
    ));
    assert!(
        error.contains("correlated with the protected row more than once"),
        "{error}"
    );

    // The same correlation on the access rows, below the join.
    let error = access_policy_error(filter(
        gather_access_join(filter(access(), outer_workspace("access"))),
        keyed_on_doc(),
    ));
    assert!(error.contains("below their join"), "{error}");

    // A join off the access rows, with its own filter.
    let error = access_policy_error(filter(
        gather_access_join(RelationExpr::Join {
            left: Box::new(access()),
            right: Box::new(filter(
                scan("teams", Some("t2")),
                equals(
                    "t2",
                    "enabled",
                    RelationValueRef::literal(crate::model::public_schema::Value::Boolean(true)),
                ),
            )),
            on: vec![RelationJoinCondition {
                left: column("access", "team"),
                right: column("t2", "id"),
            }],
            join_kind: RelationJoinKind::Inner,
        }),
        keyed_on_doc(),
    ));
    assert!(error.contains("joined to further tables"), "{error}");
}

/// A scalar gather can itself be the access rows, keyed on one column of the
/// protected row; a second key would be dropped, so it is rejected.
#[test]
fn scalar_gather_keyed_twice_is_rejected() {
    let keyed_on = |source: &str| {
        equals(
            "teams",
            "id",
            RelationValueRef::OuterColumn(RelationColumnRef::unscoped(source)),
        )
    };
    let once = docs_read_policy(PolicyExpr::ExistsRel {
        rel: filter(gather(scalar_parent_step()), keyed_on("team")),
    })
    .expect("one key compiles");
    assert_eq!(once.reachable.len(), 1);

    let error = access_policy_error(filter(
        gather(scalar_parent_step()),
        RelationPredicate::And(vec![keyed_on("team"), keyed_on("team2")]),
    ));
    assert!(
        error.contains("correlated with the protected row more than once"),
        "{error}"
    );
}

/// A scalar gather keyed on a column of the protected row has no access table
/// of its own, so a filter meant for the reached team, such as "the team is
/// enabled", would be evaluated against the protected row instead.
#[test]
fn filters_on_scalar_gather_rows_are_rejected() {
    let error = access_policy_error(filter(
        gather(scalar_parent_step()),
        RelationPredicate::And(vec![
            equals(
                "teams",
                "id",
                RelationValueRef::OuterColumn(RelationColumnRef::unscoped("team")),
            ),
            equals(
                "teams",
                "enabled",
                RelationValueRef::literal(crate::model::public_schema::Value::Boolean(true)),
            ),
        ]),
    ));
    assert!(
        error.contains("Filters on the rows a scalar Gather reaches"),
        "{error}"
    );
}

/// Access rows joined to their team on two equalities, both bound to the same
/// team row.
fn access_joined_on_two_columns() -> RelationExpr {
    filter(
        RelationExpr::Join {
            left: Box::new(scan("doc_access", Some("access"))),
            right: Box::new(scan("teams", Some("t"))),
            on: vec![
                RelationJoinCondition {
                    left: column("access", "team"),
                    right: column("t", "id"),
                },
                RelationJoinCondition {
                    left: column("access", "team"),
                    right: column("t", "parent"),
                },
            ],
            join_kind: RelationJoinKind::Inner,
        },
        outer_doc(),
    )
}

fn compound_team_join(joins: &[crate::query::JoinVia]) -> Option<&crate::query::JoinVia> {
    for join in joins {
        if join.table == "teams"
            && join.correlated_filters
                == [JoinCorrelation {
                    join_column: "parent".to_owned(),
                    source_column: "team".to_owned(),
                }]
        {
            return Some(join);
        }
        if let Some(found) = compound_team_join(&join.nested_joins) {
            return Some(found);
        }
    }
    None
}

/// Compound join conditions remain together in direct and legacy Exists
/// policy contexts.
#[test]
fn joins_on_more_than_one_equality_preserve_the_same_witness() {
    let direct = docs_read_policy(PolicyExpr::ExistsRel {
        rel: access_joined_on_two_columns(),
    })
    .expect("compound ExistsRel join compiles");
    let direct_team = compound_team_join(&direct.joins)
        .expect("the second equality remains on the joined team row");
    assert_eq!(direct_team.on_column, "id");
    assert_eq!(direct_team.source_column.as_deref(), Some("team"));

    let nested = docs_read_policy(PolicyExpr::Exists {
        table: "doc_access".to_owned(),
        condition: Box::new(PolicyExpr::ExistsRel {
            rel: access_joined_on_two_columns(),
        }),
    })
    .expect("nested compound ExistsRel join compiles");
    assert!(
        compound_team_join(&nested.joins).is_some(),
        "nested legacy Exists must retain both conditions on one witness: {nested:?}"
    );
}

/// A correlation more than one join below the correlated root would be bound
/// to its parent join's row rather than the protected row.
#[test]
fn correlations_deeper_than_one_nested_join_are_rejected() {
    let hop = |input: RelationExpr, from: (&str, &str), to: &str| RelationExpr::Join {
        left: Box::new(input),
        right: Box::new(scan("teams", Some(to))),
        on: vec![RelationJoinCondition {
            left: column(from.0, from.1),
            right: column(to, "id"),
        }],
        join_kind: RelationJoinKind::Inner,
    };
    let chain = |correlated: &str| {
        let joins = hop(
            hop(
                hop(scan("doc_access", Some("access")), ("access", "team"), "t"),
                ("t", "parent"),
                "u",
            ),
            ("u", "parent"),
            "v",
        );
        filter(
            joins,
            RelationPredicate::And(vec![
                outer_doc(),
                equals(
                    correlated,
                    "owner_id",
                    RelationValueRef::OuterColumn(RelationColumnRef::unscoped("workspace")),
                ),
            ]),
        )
    };
    for correlated in ["u", "v"] {
        let error = access_policy_error(chain(correlated));
        assert!(
            error.contains("beyond one nested join"),
            "{correlated}: {error}"
        );
    }
}
