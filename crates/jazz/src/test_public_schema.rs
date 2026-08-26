use crate::tools::public_schema::{
    PolicyExpr, RelColumnRef, RelExpr, RelJoinCondition, RelJoinKind, RelKeyRef, RelPredicateCmpOp,
    RelPredicateExpr, RelProjectColumn, RelProjectExpr, RelRecursionBound, RelValueRef, RowIdRef,
    Value,
};

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

#[allow(clippy::too_many_arguments)]
pub(crate) fn seeded_recursive_access_policy(
    access_table: &str,
    access_row_column: &str,
    access_team_column: &str,
    access_filters: &[(&str, Value)],
    access_in_filters: &[(&str, Vec<Value>)],
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
    let seed = RelExpr::Project {
        input: Box::new(RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: seed_table.into(),
                alias: Some(seed_alias.to_owned()),
            }),
            predicate: equals(
                seed_alias,
                seed_user_column,
                RelValueRef::SessionRef(
                    seed_claim_path
                        .iter()
                        .map(|segment| (*segment).to_owned())
                        .collect(),
                ),
            ),
        }),
        columns: vec![RelProjectColumn {
            alias: "id".to_owned(),
            expr: RelProjectExpr::Column(column(seed_alias, seed_team_column)),
        }],
    };
    let mut edge_predicates = vec![equals(
        edge_alias,
        edge_member_column,
        RelValueRef::RowId(RowIdRef::Frontier),
    )];
    edge_predicates.extend(edge_filters.iter().map(|(column_name, value)| {
        equals(edge_alias, column_name, RelValueRef::Literal(value.clone()))
    }));
    let step = RelExpr::Project {
        input: Box::new(RelExpr::Join {
            left: Box::new(RelExpr::Filter {
                input: Box::new(RelExpr::TableScan {
                    table: edge_table.into(),
                    alias: Some(edge_alias.to_owned()),
                }),
                predicate: RelPredicateExpr::And(edge_predicates),
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
    let reachable = RelExpr::Gather {
        seed: Box::new(seed),
        step: Box::new(step),
        frontier_key: RelKeyRef::RowId(RowIdRef::Current),
        bound: RelRecursionBound::MaxDepth(8),
        dedupe_key: vec![RelKeyRef::RowId(RowIdRef::Current)],
    };
    let mut access_predicates = vec![equals(
        access_alias,
        access_row_column,
        RelValueRef::RowId(RowIdRef::Outer),
    )];
    access_predicates.extend(access_filters.iter().map(|(column_name, value)| {
        equals(
            access_alias,
            column_name,
            RelValueRef::Literal(value.clone()),
        )
    }));
    access_predicates.extend(access_in_filters.iter().map(|(column_name, values)| {
        RelPredicateExpr::In {
            left: column(access_alias, column_name),
            values: values.iter().cloned().map(RelValueRef::Literal).collect(),
        }
    }));
    PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::Join {
                left: Box::new(reachable),
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
            predicate: RelPredicateExpr::And(access_predicates),
        },
    }
}
