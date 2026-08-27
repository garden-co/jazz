#![allow(dead_code)]

use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{
    CmpOp, PolicyExpr, PolicyValue, RelColumnRef, RelExpr, RelJoinCondition, RelJoinKind,
    RelKeyRef, RelPredicateCmpOp, RelPredicateExpr, RelProjectColumn, RelProjectExpr,
    RelRecursionBound, RelValueRef, RowIdRef,
};
use jazz::tools::{SchemaBuilder, TablePolicies};

pub fn compile(builder: SchemaBuilder) -> JazzSchema {
    JazzSchema::new(&builder.build()).expect("benchmark public schema compiles")
}

/// Compare a text column with the authenticated session identity.
///
/// Public schemas expose that identity as `session.user`; JWT `sub` is an
/// authentication transport detail, not a policy-session field.
pub fn session_user_id_column(column: &str) -> PolicyExpr {
    PolicyExpr::Cmp {
        column: column.to_owned(),
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(vec!["user".to_owned()]),
    }
}

pub fn all_operations(policy: PolicyExpr) -> TablePolicies {
    TablePolicies::new()
        .with_select(policy.clone())
        .with_insert(policy.clone())
        .with_update(Some(policy.clone()), policy.clone())
        .with_delete(policy)
}

pub fn write_operations(policy: PolicyExpr) -> TablePolicies {
    TablePolicies::new()
        .with_insert(policy.clone())
        .with_update(Some(policy.clone()), policy.clone())
        .with_delete(policy)
}

#[allow(clippy::too_many_arguments)]
pub fn reachable_access(
    access_table: &str,
    access_row_column: &str,
    access_team_column: &str,
    team_table: &str,
    edge_table: &str,
    edge_member_column: &str,
    edge_parent_column: &str,
    seed_value: RelValueRef,
) -> PolicyExpr {
    let column = |scope: &str, column: &str| RelColumnRef {
        scope: Some(scope.to_owned()),
        column: column.to_owned(),
    };
    let seed_alias = "seed";
    let edge_alias = "recursive_edge";
    let target_alias = "recursive_target";
    let access_alias = "access";
    let seed = RelExpr::Project {
        input: Box::new(RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: team_table.into(),
                alias: Some(seed_alias.to_owned()),
            }),
            predicate: RelPredicateExpr::Cmp {
                left: column(seed_alias, "id"),
                op: RelPredicateCmpOp::Eq,
                right: seed_value,
            },
        }),
        columns: vec![RelProjectColumn {
            alias: "id".to_owned(),
            expr: RelProjectExpr::Column(column(seed_alias, "id")),
        }],
    };
    let step = RelExpr::Project {
        input: Box::new(RelExpr::Join {
            left: Box::new(RelExpr::Filter {
                input: Box::new(RelExpr::TableScan {
                    table: edge_table.into(),
                    alias: Some(edge_alias.to_owned()),
                }),
                predicate: RelPredicateExpr::Cmp {
                    left: column(edge_alias, edge_member_column),
                    op: RelPredicateCmpOp::Eq,
                    right: RelValueRef::RowId(RowIdRef::Frontier),
                },
            }),
            right: Box::new(RelExpr::TableScan {
                table: team_table.into(),
                alias: Some(target_alias.to_owned()),
            }),
            on: vec![RelJoinCondition {
                left: column(edge_alias, edge_parent_column),
                right: column(target_alias, "id"),
            }],
            join_kind: RelJoinKind::Inner,
        }),
        columns: vec![RelProjectColumn {
            alias: "id".to_owned(),
            expr: RelProjectExpr::Column(column(target_alias, "id")),
        }],
    };
    PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::Gather {
                    seed: Box::new(seed),
                    step: Box::new(step),
                    frontier_key: RelKeyRef::RowId(RowIdRef::Current),
                    bound: RelRecursionBound::MaxDepth(8),
                    dedupe_key: vec![RelKeyRef::RowId(RowIdRef::Current)],
                }),
                right: Box::new(RelExpr::TableScan {
                    table: access_table.into(),
                    alias: Some(access_alias.to_owned()),
                }),
                on: vec![RelJoinCondition {
                    left: RelColumnRef {
                        scope: None,
                        column: "id".to_owned(),
                    },
                    right: column(access_alias, access_team_column),
                }],
                join_kind: RelJoinKind::Inner,
            }),
            predicate: RelPredicateExpr::Cmp {
                left: column(access_alias, access_row_column),
                op: RelPredicateCmpOp::Eq,
                right: RelValueRef::RowId(RowIdRef::Outer),
            },
        },
    }
}
