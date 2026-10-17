#![allow(dead_code)]

use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{
    CmpOp, PolicyExpr, PolicyValue, RecursionBound, RelationCmpOp, RelationColumnRef, RelationExpr,
    RelationJoinCondition, RelationJoinKind, RelationKeyRef, RelationPredicate,
    RelationProjectColumn, RelationProjectExpr, RelationRowIdRef, RelationValueRef,
};
use jazz::tools::{SchemaBuilder, TablePolicies};

pub fn compile(builder: SchemaBuilder) -> JazzSchema {
    JazzSchema::new(&builder.build()).expect("benchmark public schema compiles")
}

/// UUID ownership in account-aware benchmark fixtures.
pub fn session_user_id_column(column: &str) -> PolicyExpr {
    PolicyExpr::eq_session(column, vec!["user".into(), "account".into()])
}

/// Exact subject ownership for benchmark actors sharing the fixed test issuer.
pub fn session_subject_column(column: &str) -> PolicyExpr {
    PolicyExpr::eq_session(
        column,
        vec!["user".into(), "identity".into(), "subject".into()],
    )
}

/// Fixture-only stand-in for an already-admitted account in direct-Db
/// benchmarks. UUID ownership values stay unchanged.
pub fn account_author_uuid(id: uuid::Uuid) -> jazz::ids::AuthorSubject {
    jazz::ids::AuthorSubject::for_test_uuid(id).with_account(jazz::account_registry::AccountId(id))
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
    seed_value: RelationValueRef,
) -> PolicyExpr {
    let column = |scope: &str, column: &str| RelationColumnRef {
        scope: Some(scope.to_owned()),
        column: column.to_owned(),
    };
    let seed_alias = "seed";
    let edge_alias = "recursive_edge";
    let target_alias = "recursive_target";
    let access_alias = "access";
    let seed = RelationExpr::Project {
        input: Box::new(RelationExpr::Filter {
            input: Box::new(RelationExpr::TableScan {
                table: team_table.into(),
                alias: Some(seed_alias.to_owned()),
            }),
            predicate: RelationPredicate::Cmp {
                left: column(seed_alias, "id"),
                op: RelationCmpOp::Eq,
                right: seed_value,
            },
        }),
        columns: vec![RelationProjectColumn {
            alias: "id".to_owned(),
            expr: RelationProjectExpr::Column(column(seed_alias, "id")),
        }],
    };
    let step = RelationExpr::Project {
        input: Box::new(RelationExpr::Join {
            left: Box::new(RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: edge_table.into(),
                    alias: Some(edge_alias.to_owned()),
                }),
                predicate: RelationPredicate::Cmp {
                    left: column(edge_alias, edge_member_column),
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::RowId(RelationRowIdRef::Frontier),
                },
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
    PolicyExpr::ExistsRel {
        rel: RelationExpr::Filter {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::Gather {
                    seed: Box::new(seed),
                    step: Box::new(step),
                    frontier_key: RelationKeyRef::RowId(RelationRowIdRef::Current),
                    bound: RecursionBound::MaxDepth(8),
                    dedupe_key: vec![RelationKeyRef::RowId(RelationRowIdRef::Current)],
                }),
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
            predicate: RelationPredicate::Cmp {
                left: column(access_alias, access_row_column),
                op: RelationCmpOp::Eq,
                right: RelationValueRef::RowId(RelationRowIdRef::Outer),
            },
        },
    }
}
