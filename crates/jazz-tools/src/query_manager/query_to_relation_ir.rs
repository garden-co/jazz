use super::graph_nodes::sort::SortDirection;
use super::query::{Condition, Conjunction, Query, RecursiveSpec};
use super::relation_ir::{
    ColumnRef, JoinCondition, JoinKind, KeyRef, OrderByExpr, OrderDirection, PredicateCmpOp,
    PredicateExpr, ProjectColumn, ProjectExpr, RelExpr, RowIdRef, ValueRef,
};

/// Convert legacy query fields into relation IR when shape-compatible.
///
/// Returns `None` for query constructs that don't yet have a faithful relation-IR lowering.
pub(crate) fn normalize_query_to_rel_expr(query: &Query) -> Option<RelExpr> {
    let mut relation = RelExpr::TableScan { table: query.table };
    let mut current_scope = query.effective_name().to_string();
    let mut scope_order = vec![current_scope.clone()];
    let predicate = normalize_disjuncts(&query.disjuncts)?;
    if let Some(recursive) = query.recursive.as_ref() {
        if query.joins.is_empty() {
            if !matches!(predicate, PredicateExpr::True) {
                relation = RelExpr::Filter {
                    input: Box::new(relation),
                    predicate,
                };
            }
            relation = normalize_recursive_spec(recursive, relation)?;
        } else {
            relation = normalize_recursive_spec(recursive, relation)?;
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
                scope_order.push(current_scope.clone());
            }
            if !matches!(predicate, PredicateExpr::True) {
                relation = RelExpr::Filter {
                    input: Box::new(relation),
                    predicate,
                };
            }
        }
    } else {
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
            scope_order.push(current_scope.clone());
        }
        if !matches!(predicate, PredicateExpr::True) {
            relation = RelExpr::Filter {
                input: Box::new(relation),
                predicate,
            };
        }
    }

    if let Some(index) = query.result_element_index {
        let projected_scope = scope_order.get(index)?.clone();
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

fn normalize_recursive_spec(spec: &RecursiveSpec, seed: RelExpr) -> Option<RelExpr> {
    let outer_column = spec
        .outer_column
        .split('.')
        .next_back()
        .unwrap_or(&spec.outer_column)
        .to_string();
    let frontier_value = match outer_column.as_str() {
        "id" | "_id" => ValueRef::RowId(RowIdRef::Frontier),
        _ => ValueRef::FrontierColumn(ColumnRef::unscoped(outer_column.clone())),
    };
    let frontier_key = match outer_column.as_str() {
        "id" | "_id" => KeyRef::RowId(RowIdRef::Current),
        _ => KeyRef::Column(ColumnRef::unscoped(outer_column.clone())),
    };

    let step_scope = spec.table.as_str().to_string();
    let mut step_terms = vec![PredicateExpr::Cmp {
        left: ColumnRef::scoped(step_scope.clone(), spec.inner_column.clone()),
        op: PredicateCmpOp::Eq,
        right: frontier_value,
    }];
    for condition in &spec.filters {
        step_terms.push(normalize_condition(condition)?);
    }
    let step_predicate = if step_terms.len() == 1 {
        step_terms.into_iter().next()?
    } else {
        PredicateExpr::And(step_terms)
    };
    let step_left = RelExpr::Filter {
        input: Box::new(RelExpr::TableScan { table: spec.table }),
        predicate: step_predicate,
    };

    let step = if let Some(hop) = spec.hop.as_ref() {
        if !spec.joins.is_empty() || spec.result_element_index.is_some() || hop.table == spec.table
        {
            return None;
        }

        let step_source = if let Some(select_columns) = spec.select_columns.as_ref() {
            RelExpr::Project {
                input: Box::new(step_left),
                columns: select_columns
                    .iter()
                    .map(|column| ProjectColumn {
                        alias: column.clone(),
                        expr: ProjectExpr::Column(ColumnRef::scoped(
                            spec.table.as_str(),
                            column.clone(),
                        )),
                    })
                    .collect(),
            }
        } else {
            step_left
        };

        let hop_scope = "__recursive_hop_0".to_string();
        RelExpr::Project {
            input: Box::new(RelExpr::Join {
                left: Box::new(step_source),
                right: Box::new(RelExpr::TableScan { table: hop.table }),
                on: vec![JoinCondition {
                    left: ColumnRef::scoped(step_scope, hop.via_column.clone()),
                    right: ColumnRef::scoped(hop_scope.clone(), "id"),
                }],
                join_kind: JoinKind::Inner,
            }),
            columns: vec![ProjectColumn {
                alias: "id".to_string(),
                expr: ProjectExpr::Column(ColumnRef::scoped(hop_scope, "id")),
            }],
        }
    } else if spec.joins.len() == 1 && spec.result_element_index == Some(1) {
        if spec.select_columns.is_some() {
            return None;
        }
        let join_spec = spec.joins.first()?;
        let (left_raw, right_raw) = join_spec.on.as_ref()?;
        let hop_scope = join_spec.effective_name().to_string();
        if join_spec.table == spec.table {
            return None;
        }
        RelExpr::Project {
            input: Box::new(RelExpr::Join {
                left: Box::new(step_left),
                right: Box::new(RelExpr::TableScan {
                    table: join_spec.table,
                }),
                on: vec![JoinCondition {
                    left: parse_join_column(left_raw, &step_scope)?,
                    right: parse_join_column(right_raw, &hop_scope)?,
                }],
                join_kind: JoinKind::Inner,
            }),
            columns: vec![ProjectColumn {
                alias: "id".to_string(),
                expr: ProjectExpr::Column(ColumnRef::scoped(hop_scope, "id")),
            }],
        }
    } else {
        if !spec.joins.is_empty() || spec.result_element_index.is_some() {
            return None;
        }

        if let Some(select_columns) = spec.select_columns.as_ref() {
            RelExpr::Project {
                input: Box::new(step_left),
                columns: select_columns
                    .iter()
                    .map(|column| ProjectColumn {
                        alias: column.clone(),
                        expr: ProjectExpr::Column(ColumnRef::scoped(
                            spec.table.as_str(),
                            column.clone(),
                        )),
                    })
                    .collect(),
            }
        } else {
            step_left
        }
    };

    Some(RelExpr::Gather {
        seed: Box::new(seed),
        step: Box::new(step),
        frontier_key: frontier_key.clone(),
        max_depth: spec.max_depth,
        dedupe_key: vec![frontier_key],
    })
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
        Condition::Contains { column, value } => Some(PredicateExpr::Contains {
            left: ColumnRef::unscoped(column.clone()),
            right: ValueRef::Literal(value.clone()),
        }),
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
        let query = QueryBuilder::new("users")
            .alias("u")
            .join("posts")
            .alias("p")
            .on("u.id", "p.author_id")
            .result_element_index(1)
            .build();

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
    fn normalize_query_with_contains_filter_is_supported() {
        let query = QueryBuilder::new("users")
            .filter_contains("tags", Value::Text("admin".to_string()))
            .build();
        let relation = normalize_query_to_rel_expr(&query)
            .expect("contains filter should normalize to relation predicate");
        assert_eq!(
            relation,
            RelExpr::Filter {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("users"),
                }),
                predicate: PredicateExpr::Contains {
                    left: ColumnRef::unscoped("tags"),
                    right: ValueRef::Literal(Value::Text("admin".to_string())),
                },
            }
        );
    }

    #[test]
    fn normalize_query_with_recursive_hop_spec_produces_gather() {
        let query = QueryBuilder::new("teams")
            .with_recursive(|r| {
                r.from("team_edges")
                    .correlate("child_team", "_id")
                    .select(&["parent_team"])
                    .hop("teams", "parent_team")
                    .max_depth(8)
            })
            .build();
        let relation =
            normalize_query_to_rel_expr(&query).expect("recursive hop should normalize to gather");
        let RelExpr::Gather {
            seed,
            step,
            frontier_key,
            max_depth,
            dedupe_key,
        } = relation
        else {
            panic!("expected gather relation");
        };
        assert_eq!(
            *seed,
            RelExpr::TableScan {
                table: TableName::new("teams"),
            }
        );
        assert_eq!(frontier_key, KeyRef::RowId(RowIdRef::Current));
        assert_eq!(max_depth, 8);
        assert_eq!(dedupe_key, vec![KeyRef::RowId(RowIdRef::Current)]);

        let RelExpr::Project { input, columns } = *step else {
            panic!("expected project step");
        };
        assert_eq!(
            columns,
            vec![ProjectColumn {
                alias: "id".to_string(),
                expr: ProjectExpr::Column(ColumnRef::scoped("__recursive_hop_0", "id")),
            }]
        );
        let RelExpr::Join {
            left,
            right,
            on,
            join_kind,
        } = *input
        else {
            panic!("expected join step");
        };
        assert_eq!(join_kind, JoinKind::Inner);
        assert_eq!(
            *right,
            RelExpr::TableScan {
                table: TableName::new("teams"),
            }
        );
        assert_eq!(
            on,
            vec![JoinCondition {
                left: ColumnRef::scoped("team_edges", "parent_team"),
                right: ColumnRef::scoped("__recursive_hop_0", "id"),
            }]
        );
        let RelExpr::Project {
            input: step_filter,
            columns: step_columns,
        } = *left
        else {
            panic!("expected projected step source");
        };
        assert_eq!(
            step_columns,
            vec![ProjectColumn {
                alias: "parent_team".to_string(),
                expr: ProjectExpr::Column(ColumnRef::scoped("team_edges", "parent_team")),
            }]
        );
        assert_eq!(
            *step_filter,
            RelExpr::Filter {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("team_edges"),
                }),
                predicate: PredicateExpr::Cmp {
                    left: ColumnRef::scoped("team_edges", "child_team"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::RowId(RowIdRef::Frontier),
                },
            }
        );
    }

    #[test]
    fn normalize_query_with_recursive_spec_without_hop_produces_gather() {
        let query = QueryBuilder::new("teams")
            .with_recursive(|r| {
                r.from("team_edges")
                    .correlate("child_team", "_id")
                    .select(&["parent_team"])
                    .max_depth(4)
            })
            .build();
        let relation = normalize_query_to_rel_expr(&query)
            .expect("recursive no-hop spec should normalize to gather");

        let RelExpr::Gather {
            max_depth, step, ..
        } = relation
        else {
            panic!("expected gather relation");
        };
        assert_eq!(max_depth, 4);

        let RelExpr::Project { input, columns } = *step else {
            panic!("expected project step");
        };
        assert_eq!(
            columns,
            vec![ProjectColumn {
                alias: "parent_team".to_string(),
                expr: ProjectExpr::Column(ColumnRef::scoped("team_edges", "parent_team")),
            }]
        );
        assert_eq!(
            *input,
            RelExpr::Filter {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("team_edges"),
                }),
                predicate: PredicateExpr::Cmp {
                    left: ColumnRef::scoped("team_edges", "child_team"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::RowId(RowIdRef::Frontier),
                },
            }
        );
    }

    #[test]
    fn normalize_query_with_recursive_spec_column_frontier_produces_gather() {
        let query = QueryBuilder::new("teams")
            .with_recursive(|r| {
                r.from("team_edges")
                    .correlate("child_team", "team_id")
                    .select(&["parent_team"])
                    .max_depth(4)
            })
            .build();
        let relation = normalize_query_to_rel_expr(&query)
            .expect("recursive column-frontier spec should normalize to gather");

        let RelExpr::Gather {
            frontier_key,
            dedupe_key,
            step,
            ..
        } = relation
        else {
            panic!("expected gather relation")
        };
        assert_eq!(frontier_key, KeyRef::Column(ColumnRef::unscoped("team_id")));
        assert_eq!(
            dedupe_key,
            vec![KeyRef::Column(ColumnRef::unscoped("team_id"))]
        );

        let RelExpr::Project { input, columns } = *step else {
            panic!("expected project step");
        };
        assert_eq!(
            columns,
            vec![ProjectColumn {
                alias: "parent_team".to_string(),
                expr: ProjectExpr::Column(ColumnRef::scoped("team_edges", "parent_team")),
            }]
        );
        assert_eq!(
            *input,
            RelExpr::Filter {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("team_edges"),
                }),
                predicate: PredicateExpr::Cmp {
                    left: ColumnRef::scoped("team_edges", "child_team"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::FrontierColumn(ColumnRef::unscoped("team_id")),
                },
            }
        );
    }

    #[test]
    fn normalize_query_with_recursive_join_projection_spec_produces_gather() {
        let query = QueryBuilder::new("teams")
            .with_recursive(|r| {
                r.from("team_edges")
                    .correlate("child_team", "_id")
                    .join("teams")
                    .alias("__recursive_hop_0")
                    .on("team_edges.parent_team", "__recursive_hop_0.id")
                    .result_element_index(1)
                    .max_depth(6)
            })
            .build();

        let relation = normalize_query_to_rel_expr(&query)
            .expect("recursive join-projection spec should normalize to gather");
        let RelExpr::Gather {
            max_depth,
            step,
            frontier_key,
            dedupe_key,
            ..
        } = relation
        else {
            panic!("expected gather relation");
        };
        assert_eq!(max_depth, 6);
        assert_eq!(frontier_key, KeyRef::RowId(RowIdRef::Current));
        assert_eq!(dedupe_key, vec![KeyRef::RowId(RowIdRef::Current)]);

        let RelExpr::Project { input, .. } = *step else {
            panic!("expected project step");
        };
        let RelExpr::Join { on, .. } = *input else {
            panic!("expected join step");
        };
        assert_eq!(
            on,
            vec![JoinCondition {
                left: ColumnRef::scoped("team_edges", "parent_team"),
                right: ColumnRef::scoped("__recursive_hop_0", "id"),
            }]
        );
    }

    #[test]
    fn normalize_query_with_recursive_and_top_level_join_keeps_gather_on_left() {
        let query = QueryBuilder::new("teams")
            .with_recursive(|r| {
                r.from("team_edges")
                    .correlate("child_team", "_id")
                    .hop("teams", "parent_team")
            })
            .join("team_edges")
            .on("teams.id", "team_edges.parent_team")
            .filter_eq("name", Value::Text("seed".to_string()))
            .build();

        let relation = normalize_query_to_rel_expr(&query)
            .expect("recursive query with top-level join should normalize");
        let RelExpr::Filter { input, .. } = relation else {
            panic!("expected filter wrapper after recursive join chain");
        };
        let RelExpr::Join {
            left, right, on, ..
        } = *input
        else {
            panic!("expected top-level join");
        };
        assert!(matches!(*left, RelExpr::Gather { .. }));
        assert_eq!(
            *right,
            RelExpr::TableScan {
                table: TableName::new("team_edges"),
            }
        );
        assert_eq!(
            on,
            vec![JoinCondition {
                left: ColumnRef::scoped("teams", "id"),
                right: ColumnRef::scoped("team_edges", "parent_team"),
            }]
        );
    }

    #[test]
    fn normalize_query_with_non_terminal_result_element_index_is_supported() {
        let query = QueryBuilder::new("users")
            .join("posts")
            .on("users.id", "posts.author_id")
            .result_element_index(0)
            .build();

        let relation = normalize_query_to_rel_expr(&query)
            .expect("non-terminal result element index should normalize to project");
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
                        left: ColumnRef::scoped("users", "id"),
                        right: ColumnRef::scoped("posts", "author_id"),
                    }],
                    join_kind: JoinKind::Inner,
                }),
                columns: vec![ProjectColumn {
                    alias: "id".to_string(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("users", "id")),
                }],
            }
        );
    }
}
