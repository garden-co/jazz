//! Public-schema helpers shared by simulation fixtures and benchmark scenarios.

use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{
    PolicyExpr, RecursionBound, RelationCmpOp, RelationColumnRef, RelationExpr,
    RelationJoinCondition, RelationJoinKind, RelationKeyRef, RelationPredicate,
    RelationProjectColumn, RelationProjectExpr, RelationRowIdRef, RelationValueRef, Schema,
    TablePolicies, Value,
};

/// Compile a developer-authored schema while retaining its persistable source.
pub fn compile_public_schema(source: Schema) -> JazzSchema {
    jazz::schema::JazzSchema::new(&source).expect("simulation public schema compiles")
}

/// Apply the same policy expression to every table operation.
pub fn all_operation_policies(policy: PolicyExpr) -> TablePolicies {
    TablePolicies::new()
        .with_select(policy.clone())
        .with_insert(policy.clone())
        .with_update(Some(policy.clone()), policy.clone())
        .with_delete(policy)
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

/// Build the recursive access-edge policy used by simulation scenarios.
#[allow(clippy::too_many_arguments)]
pub fn seeded_recursive_access_policy(
    access_table: &str,
    access_row_column: &str,
    access_team_column: &str,
    access_filters: &[(&str, Value)],
    team_table: &str,
    edge_table: &str,
    edge_member_column: &str,
    edge_parent_column: &str,
    edge_filters: &[(&str, Value)],
    seed_table: &str,
    seed_user_column: &str,
    seed_claim_path: &[&str],
    seed_team_column: &str,
) -> PolicyExpr {
    let seed_alias = "seed";
    let edge_alias = "recursive_edge";
    let target_alias = "recursive_target";
    let access_alias = "access";
    let seed = RelationExpr::Project {
        input: Box::new(RelationExpr::Filter {
            input: Box::new(RelationExpr::TableScan {
                table: seed_table.into(),
                alias: Some(seed_alias.to_owned()),
            }),
            predicate: equals(
                seed_alias,
                seed_user_column,
                RelationValueRef::SessionRef(
                    seed_claim_path
                        .iter()
                        .map(|segment| (*segment).to_owned())
                        .collect(),
                ),
            ),
        }),
        columns: vec![RelationProjectColumn {
            alias: "id".to_owned(),
            expr: RelationProjectExpr::Column(column(seed_alias, seed_team_column)),
        }],
    };
    let mut edge_predicates = vec![equals(
        edge_alias,
        edge_member_column,
        RelationValueRef::RowId(RelationRowIdRef::Frontier),
    )];
    edge_predicates.extend(edge_filters.iter().map(|(column_name, value)| {
        equals(
            edge_alias,
            column_name,
            RelationValueRef::literal(value.clone()),
        )
    }));
    let step = RelationExpr::Project {
        input: Box::new(RelationExpr::Join {
            left: Box::new(RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: edge_table.into(),
                    alias: Some(edge_alias.to_owned()),
                }),
                predicate: RelationPredicate::And(edge_predicates),
            }),
            right: Box::new(RelationExpr::TableScan {
                table: team_table.into(),
                alias: Some(target_alias.to_owned()),
            }),
            on: vec![RelationJoinCondition {
                left: column(edge_alias, edge_parent_column),
                right: column(target_alias, "id"),
            }],
            join_kind: RelationJoinKind::Inner,
        }),
        columns: vec![RelationProjectColumn {
            alias: "id".to_owned(),
            expr: RelationProjectExpr::Column(column(target_alias, "id")),
        }],
    };
    let reachable = RelationExpr::Gather {
        seed: Box::new(seed),
        step: Box::new(step),
        frontier_key: RelationKeyRef::RowId(RelationRowIdRef::Current),
        bound: RecursionBound::MaxDepth(8),
        dedupe_key: vec![RelationKeyRef::RowId(RelationRowIdRef::Current)],
    };
    let mut access_predicates = vec![equals(
        access_alias,
        access_row_column,
        RelationValueRef::RowId(RelationRowIdRef::Outer),
    )];
    access_predicates.extend(access_filters.iter().map(|(column_name, value)| {
        equals(
            access_alias,
            column_name,
            RelationValueRef::literal(value.clone()),
        )
    }));
    PolicyExpr::ExistsRel {
        rel: RelationExpr::Filter {
            input: Box::new(RelationExpr::Join {
                left: Box::new(reachable),
                right: Box::new(RelationExpr::TableScan {
                    table: access_table.into(),
                    alias: Some(access_alias.to_owned()),
                }),
                on: vec![RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: None,
                        column: "id".to_owned(),
                    },
                    right: column(access_alias, access_team_column),
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            predicate: RelationPredicate::And(access_predicates),
        },
    }
}
