//! A relation `Gather` means the same thing whether it appears in a query or in
//! a relation-backed policy (`PolicyExpr::ExistsRel`). Each case lowers one
//! gather through both front ends and requires the same recursive edge, or the
//! same rejection.

use crate::model::public_schema::{
    ColumnType, PolicyExpr, RelColumnRef, RelExpr, RelJoinCondition, RelJoinKind, RelKeyRef,
    RelPredicateCmpOp, RelPredicateExpr, RelProjectColumn, RelProjectExpr, RelRecursionBound,
    RelValueRef, RowIdRef, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use crate::model::public_schema_convert::convert_public_schema;
use crate::query::{
    Predicate, RecursionBound, RelationExpr, RelationQuery, relation_query_to_query,
};

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

fn column(scope: &str, column: &str) -> RelColumnRef {
    RelColumnRef {
        scope: Some(scope.to_owned()),
        column: column.to_owned(),
    }
}

fn equals(scope: &str, column_name: &str, right: RelValueRef) -> RelPredicateExpr {
    RelPredicateExpr::Cmp {
        left: column(scope, column_name),
        op: RelPredicateCmpOp::Eq,
        right,
    }
}

fn scan(table: &str, alias: Option<&str>) -> RelExpr {
    RelExpr::TableScan {
        table: table.into(),
        alias: alias.map(str::to_owned),
    }
}

fn filter(input: RelExpr, predicate: RelPredicateExpr) -> RelExpr {
    RelExpr::Filter {
        input: Box::new(input),
        predicate,
    }
}

/// Teams the session owns.
fn owned_teams_seed() -> RelExpr {
    filter(
        scan("teams", None),
        equals(
            "teams",
            "owner_id",
            RelValueRef::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
        ),
    )
}

/// The frontier team's own row, filtered to enabled teams.
fn frontier_team_rows() -> RelExpr {
    filter(
        scan("teams", None),
        RelPredicateExpr::And(vec![
            equals("teams", "id", RelValueRef::RowId(RowIdRef::Frontier)),
            equals(
                "teams",
                "enabled",
                RelValueRef::Literal(crate::model::public_schema::Value::Boolean(true)),
            ),
        ]),
    )
}

/// Step form emitted by the TypeScript query builder: hop the parent FK back
/// to a `teams` row and project that row.
fn joined_parent_step(target_column: &str) -> RelExpr {
    joined_parent_step_projecting(target_column, "id")
}

fn joined_parent_step_projecting(target_column: &str, projected_column: &str) -> RelExpr {
    RelExpr::Project {
        input: Box::new(RelExpr::Join {
            left: Box::new(frontier_team_rows()),
            right: Box::new(scan("teams", Some("__recursive_hop_0"))),
            on: vec![RelJoinCondition {
                left: column("teams", "parent"),
                right: column("__recursive_hop_0", target_column),
            }],
            join_kind: RelJoinKind::Inner,
        }),
        columns: vec![RelProjectColumn {
            alias: "id".to_owned(),
            expr: RelProjectExpr::Column(column("__recursive_hop_0", projected_column)),
        }],
    }
}

/// Step form emitted by the TypeScript policy builder: carry only the parent
/// key across the frontier.
fn scalar_parent_step() -> RelExpr {
    RelExpr::Project {
        input: Box::new(frontier_team_rows()),
        columns: vec![RelProjectColumn {
            alias: "id".to_owned(),
            expr: RelProjectExpr::Column(column("teams", "parent")),
        }],
    }
}

fn gather(step: RelExpr) -> RelExpr {
    RelExpr::Gather {
        seed: Box::new(owned_teams_seed()),
        step: Box::new(step),
        frontier_key: RelKeyRef::RowId(RowIdRef::Current),
        bound: RelRecursionBound::MaxDepth(4),
        dedupe_key: vec![RelKeyRef::RowId(RowIdRef::Current)],
    }
}

fn with_gather_fields(
    mut rel: RelExpr,
    edit: impl FnOnce(&mut RelKeyRef, &mut RelRecursionBound, &mut Vec<RelKeyRef>),
) -> RelExpr {
    let RelExpr::Gather {
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

fn query_lowering(rel: &RelExpr) -> Result<GatherEdge, String> {
    // Both IRs share their JSON names, so the policy tree is also a query tree.
    let rel: RelationExpr =
        serde_json::from_value(serde_json::to_value(rel).expect("serialize policy relation"))
            .expect("policy relation JSON is a query relation");
    let query = relation_query_to_query(&RelationQuery { rel }).map_err(|err| err.to_string())?;
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

fn policy_lowering(rel: &RelExpr) -> Result<GatherEdge, String> {
    let policy = PolicyExpr::ExistsRel {
        rel: filter(
            RelExpr::Join {
                left: Box::new(rel.clone()),
                right: Box::new(scan("doc_access", Some("access"))),
                on: vec![RelJoinCondition {
                    left: RelColumnRef {
                        scope: None,
                        column: "id".to_owned(),
                    },
                    right: column("access", "team"),
                }],
                join_kind: RelJoinKind::Inner,
            },
            equals("access", "doc", RelValueRef::RowId(RowIdRef::Outer)),
        ),
    };
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("docs")
                .column("title", ColumnType::Text)
                .policies(TablePolicies::new().with_select(policy)),
        )
        .table(
            TableSchemaBuilder::new("teams")
                .column("owner_id", ColumnType::Uuid)
                .column("enabled", ColumnType::Boolean)
                .fk_column("parent", "teams"),
        )
        .table(
            TableSchemaBuilder::new("doc_access")
                .fk_column("doc", "docs")
                .fk_column("team", "teams"),
        )
        .build();
    let converted = convert_public_schema(&schema).map_err(|err| err.to_string())?;
    let docs = converted
        .tables()
        .iter()
        .find(|table| table.name == "docs")
        .expect("docs table converts");
    let read_policy = docs.read_policy.as_ref().expect("docs read policy");
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

fn lower_both(rel: &RelExpr) -> Lowerings {
    Lowerings {
        query: query_lowering(rel),
        policy: policy_lowering(rel),
    }
}

fn assert_both_accept(rel: &RelExpr) -> GatherEdge {
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

fn assert_both_reject(rel: &RelExpr) {
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
            crate::query::Operand::Literal(groove::records::Value::Bool(true)),
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
fn joined_step_without_an_id_projection_keeps_the_target_row_id() {
    // An empty projection passes the hop target row through, so its row id is
    // still the next frontier.
    let RelExpr::Project { input, .. } = joined_parent_step("id") else {
        unreachable!("joined_parent_step returns a projection");
    };
    let step = RelExpr::Project {
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
        *frontier_key = RelKeyRef::Column(column("teams", "parent"));
    });
    assert_both_reject(&rel);
}

#[test]
fn non_row_id_dedupe_key_is_rejected() {
    let rel = with_gather_fields(gather(scalar_parent_step()), |_, _, dedupe_key| {
        *dedupe_key = vec![RelKeyRef::Column(column("teams", "owner_id"))];
    });
    assert_both_reject(&rel);
}

#[test]
fn step_projecting_another_target_column_as_id_is_rejected() {
    // Projecting `hop.parent` makes the next frontier the grandparent, which
    // a one-hop parent edge cannot express.
    assert_both_reject(&gather(joined_parent_step_projecting("id", "parent")));
}

/// Gather from a seed whose rows hop to `teams` before starting the frontier.
fn gather_from_seed(seed: RelExpr) -> RelExpr {
    let RelExpr::Gather {
        step,
        frontier_key,
        bound,
        dedupe_key,
        ..
    } = gather(scalar_parent_step())
    else {
        unreachable!("gather builds a Gather");
    };
    RelExpr::Gather {
        seed: Box::new(seed),
        step,
        frontier_key,
        bound,
        dedupe_key,
    }
}

fn hopped_seed(target_column: &str, join_kind: RelJoinKind) -> RelExpr {
    RelExpr::Project {
        input: Box::new(RelExpr::Join {
            left: Box::new(owned_teams_seed()),
            right: Box::new(scan("teams", Some("hop"))),
            on: vec![RelJoinCondition {
                left: column("teams", "parent"),
                right: column("hop", target_column),
            }],
            join_kind,
        }),
        columns: vec![RelProjectColumn {
            alias: "id".to_owned(),
            expr: RelProjectExpr::Column(column("hop", "id")),
        }],
    }
}

#[test]
fn policy_seed_hop_onto_the_target_id_starts_from_the_hop() {
    let edge = policy_lowering(&gather_from_seed(hopped_seed("id", RelJoinKind::Inner)))
        .expect("an inner seed hop onto the target id lowers");
    assert_eq!(edge, expected_edge(RecursionBound::MaxDepth(4)));
}

#[test]
fn policy_seed_hop_onto_a_non_id_target_column_is_rejected() {
    let lowered = policy_lowering(&gather_from_seed(hopped_seed(
        "owner_id",
        RelJoinKind::Inner,
    )));
    assert!(lowered.is_err(), "{lowered:#?}");
}

#[test]
fn policy_left_seed_hop_is_rejected() {
    let lowered = policy_lowering(&gather_from_seed(hopped_seed("id", RelJoinKind::Left)));
    assert!(lowered.is_err(), "{lowered:#?}");
}

#[test]
fn policy_seed_claim_inequality_does_not_key_the_seed() {
    // `owner_id != me` must not become the claim-keyed seed `owner_id = me`.
    let seed = filter(
        scan("teams", None),
        RelPredicateExpr::Cmp {
            left: column("teams", "owner_id"),
            op: RelPredicateCmpOp::Ne,
            right: RelValueRef::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
        },
    );
    let lowered = policy_lowering(&gather_from_seed(seed));
    assert!(lowered.is_err(), "{lowered:#?}");
}
