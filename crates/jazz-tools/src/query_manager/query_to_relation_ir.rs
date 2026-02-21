use super::graph_nodes::sort::SortDirection;
use super::query::{Condition, Conjunction, Query};
use super::relation_ir::{
    ColumnRef, JoinCondition, JoinKind, OrderByExpr, OrderDirection, PredicateCmpOp, PredicateExpr,
    ProjectColumn, ProjectExpr, RelExpr, ValueRef,
};

/// Convert legacy query fields into relation IR when shape-compatible.
///
/// Returns `None` for query constructs that don't yet have a faithful relation-IR lowering.
pub(crate) fn normalize_query_to_rel_expr(query: &Query) -> Option<RelExpr> {
    if query.has_relation_ir() || query.has_recursive() {
        return None;
    }

    let mut relation = RelExpr::TableScan { table: query.table };
    let mut current_scope = query.effective_name().to_string();

    for join in &query.joins {
        let (left_raw, right_raw) = join.on.as_ref()?;
        let right_scope = join.effective_name().to_string();
        relation = RelExpr::Join {
            left: Box::new(relation),
            right: Box::new(RelExpr::TableScan { table: join.table }),
            on: vec![JoinCondition {
                left: parse_join_column(left_raw, &current_scope)?,
                right: parse_join_column(right_raw, &right_scope)?,
            }],
            join_kind: JoinKind::Inner,
        };
        current_scope = right_scope;
    }

    let predicate = normalize_disjuncts(&query.disjuncts)?;
    if !matches!(predicate, PredicateExpr::True) {
        relation = RelExpr::Filter {
            input: Box::new(relation),
            predicate,
        };
    }

    if let Some(index) = query.result_element_index {
        if query.joins.is_empty() || index != query.joins.len() {
            return None;
        }
        let projected_scope = query
            .joins
            .last()
            .map(|join| join.effective_name().to_string())
            .unwrap_or_else(|| query.effective_name().to_string());
        relation = RelExpr::Project {
            input: Box::new(relation),
            columns: vec![ProjectColumn {
                alias: "id".to_string(),
                expr: ProjectExpr::Column(ColumnRef::scoped(projected_scope, "id")),
            }],
        };
    }

    if !query.order_by.is_empty() {
        relation = RelExpr::OrderBy {
            input: Box::new(relation),
            terms: query
                .order_by
                .iter()
                .map(|(column, direction)| OrderByExpr {
                    column: ColumnRef::unscoped(column.clone()),
                    direction: match direction {
                        SortDirection::Ascending => OrderDirection::Asc,
                        SortDirection::Descending => OrderDirection::Desc,
                    },
                })
                .collect(),
        };
    }

    if query.offset > 0 {
        relation = RelExpr::Offset {
            input: Box::new(relation),
            offset: query.offset,
        };
    }

    if let Some(limit) = query.limit {
        relation = RelExpr::Limit {
            input: Box::new(relation),
            limit,
        };
    }

    Some(relation)
}

fn normalize_disjuncts(disjuncts: &[Conjunction]) -> Option<PredicateExpr> {
    if disjuncts.is_empty() {
        return Some(PredicateExpr::True);
    }

    let mut predicates = Vec::new();
    for conjunction in disjuncts {
        if conjunction.conditions.is_empty() {
            return Some(PredicateExpr::True);
        }
        predicates.push(normalize_conjunction(conjunction)?);
    }

    if predicates.is_empty() {
        Some(PredicateExpr::True)
    } else if predicates.len() == 1 {
        predicates.into_iter().next()
    } else {
        Some(PredicateExpr::Or(predicates))
    }
}

fn normalize_conjunction(conjunction: &Conjunction) -> Option<PredicateExpr> {
    let mut terms = Vec::new();
    for condition in &conjunction.conditions {
        match normalize_condition(condition)? {
            PredicateExpr::And(inner) => terms.extend(inner),
            PredicateExpr::True => {}
            other => terms.push(other),
        }
    }

    if terms.is_empty() {
        Some(PredicateExpr::True)
    } else if terms.len() == 1 {
        terms.into_iter().next()
    } else {
        Some(PredicateExpr::And(terms))
    }
}

fn normalize_condition(condition: &Condition) -> Option<PredicateExpr> {
    match condition {
        Condition::Eq { column, value } => Some(PredicateExpr::Cmp {
            left: ColumnRef::unscoped(column.clone()),
            op: PredicateCmpOp::Eq,
            right: ValueRef::Literal(value.clone()),
        }),
        Condition::Ne { column, value } => Some(PredicateExpr::Cmp {
            left: ColumnRef::unscoped(column.clone()),
            op: PredicateCmpOp::Ne,
            right: ValueRef::Literal(value.clone()),
        }),
        Condition::Lt { column, value } => Some(PredicateExpr::Cmp {
            left: ColumnRef::unscoped(column.clone()),
            op: PredicateCmpOp::Lt,
            right: ValueRef::Literal(value.clone()),
        }),
        Condition::Le { column, value } => Some(PredicateExpr::Cmp {
            left: ColumnRef::unscoped(column.clone()),
            op: PredicateCmpOp::Le,
            right: ValueRef::Literal(value.clone()),
        }),
        Condition::Gt { column, value } => Some(PredicateExpr::Cmp {
            left: ColumnRef::unscoped(column.clone()),
            op: PredicateCmpOp::Gt,
            right: ValueRef::Literal(value.clone()),
        }),
        Condition::Ge { column, value } => Some(PredicateExpr::Cmp {
            left: ColumnRef::unscoped(column.clone()),
            op: PredicateCmpOp::Ge,
            right: ValueRef::Literal(value.clone()),
        }),
        Condition::Between { column, min, max } => Some(PredicateExpr::And(vec![
            PredicateExpr::Cmp {
                left: ColumnRef::unscoped(column.clone()),
                op: PredicateCmpOp::Ge,
                right: ValueRef::Literal(min.clone()),
            },
            PredicateExpr::Cmp {
                left: ColumnRef::unscoped(column.clone()),
                op: PredicateCmpOp::Le,
                right: ValueRef::Literal(max.clone()),
            },
        ])),
        Condition::IsNull { column } => Some(PredicateExpr::IsNull {
            column: ColumnRef::unscoped(column.clone()),
        }),
        Condition::IsNotNull { column } => Some(PredicateExpr::IsNotNull {
            column: ColumnRef::unscoped(column.clone()),
        }),
        Condition::Contains { .. } => None,
    }
}

fn parse_join_column(raw: &str, default_scope: &str) -> Option<ColumnRef> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some((scope, column)) = trimmed.rsplit_once('.') {
        let scope = scope.trim();
        let column = column.trim();
        if !scope.is_empty() && !column.is_empty() {
            return Some(ColumnRef::scoped(scope, column));
        }
    }
    Some(ColumnRef::scoped(default_scope, trimmed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::query::QueryBuilder;
    use crate::query_manager::types::{TableName, Value};

    #[test]
    fn normalize_query_with_or_predicate_order_offset_limit() {
        let query = QueryBuilder::new("users")
            .filter_eq("name", Value::Text("alice".to_string()))
            .or()
            .filter_eq("name", Value::Text("bob".to_string()))
            .order_by_desc("name")
            .offset(3)
            .limit(5)
            .build();

        let relation =
            normalize_query_to_rel_expr(&query).expect("legacy query should normalize to relation");
        assert_eq!(
            relation,
            RelExpr::Limit {
                input: Box::new(RelExpr::Offset {
                    input: Box::new(RelExpr::OrderBy {
                        input: Box::new(RelExpr::Filter {
                            input: Box::new(RelExpr::TableScan {
                                table: TableName::new("users"),
                            }),
                            predicate: PredicateExpr::Or(vec![
                                PredicateExpr::Cmp {
                                    left: ColumnRef::unscoped("name"),
                                    op: PredicateCmpOp::Eq,
                                    right: ValueRef::Literal(Value::Text("alice".to_string())),
                                },
                                PredicateExpr::Cmp {
                                    left: ColumnRef::unscoped("name"),
                                    op: PredicateCmpOp::Eq,
                                    right: ValueRef::Literal(Value::Text("bob".to_string())),
                                },
                            ]),
                        }),
                        terms: vec![OrderByExpr {
                            column: ColumnRef::unscoped("name"),
                            direction: OrderDirection::Desc,
                        }],
                    }),
                    offset: 3,
                }),
                limit: 5,
            }
        );
    }

    #[test]
    fn normalize_join_query_with_result_element_index_projects_last_join_alias() {
        let mut query = QueryBuilder::new("users")
            .alias("u")
            .join("posts")
            .alias("p")
            .on("u.id", "p.author_id")
            .build();
        query.result_element_index = Some(1);

        let relation =
            normalize_query_to_rel_expr(&query).expect("join query should normalize to relation");
        assert_eq!(
            relation,
            RelExpr::Project {
                input: Box::new(RelExpr::Join {
                    left: Box::new(RelExpr::TableScan {
                        table: TableName::new("users"),
                    }),
                    right: Box::new(RelExpr::TableScan {
                        table: TableName::new("posts"),
                    }),
                    on: vec![JoinCondition {
                        left: ColumnRef::scoped("u", "id"),
                        right: ColumnRef::scoped("p", "author_id"),
                    }],
                    join_kind: JoinKind::Inner,
                }),
                columns: vec![ProjectColumn {
                    alias: "id".to_string(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("p", "id")),
                }],
            }
        );
    }

    #[test]
    fn normalize_query_with_contains_filter_is_unsupported() {
        let query = QueryBuilder::new("users")
            .filter_contains("tags", Value::Text("admin".to_string()))
            .build();

        assert!(
            normalize_query_to_rel_expr(&query).is_none(),
            "contains predicates should fall back to legacy compilation"
        );
    }

    #[test]
    fn normalize_query_with_recursive_spec_is_unsupported() {
        let query = QueryBuilder::new("teams")
            .with_recursive(|r| r.from("team_edges").correlate("child_team", "_id"))
            .build();

        assert!(
            normalize_query_to_rel_expr(&query).is_none(),
            "recursive legacy query field should fall back during cutover"
        );
    }

    #[test]
    fn normalize_query_with_non_terminal_result_element_index_is_unsupported() {
        let mut query = QueryBuilder::new("users")
            .join("posts")
            .on("users.id", "posts.author_id")
            .build();
        query.result_element_index = Some(0);

        assert!(
            normalize_query_to_rel_expr(&query).is_none(),
            "only projection to the terminal join element is currently representable"
        );
    }
}
