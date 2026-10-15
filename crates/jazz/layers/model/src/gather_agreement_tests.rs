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
use crate::query::{Predicate, RelationQuery, relation_query_to_query};

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
            expr: RelationProjectExpr::Column(column("__recursive_hop_0", "id")),
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
