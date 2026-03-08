use super::graph_nodes::sort::SortDirection;
use super::query::{
    ArraySubquerySpec, Condition, Conjunction, JoinSpec, RecursiveHopSpec, RecursiveSpec,
};
use super::relation_ir::{
    ColumnRef, JoinKind, OrderDirection, PredicateCmpOp, PredicateExpr, ProjectColumn, ProjectExpr,
    RelExpr, RowIdRef, ValueRef,
};
use super::types::TableName;

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryEnvelope<'a> {
    core: &'a RelExpr,
    order_by: Vec<(String, SortDirection)>,
    offset: usize,
    limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LinearJoinInfo {
    base_table: TableName,
    current_scope: String,
    scope_order: Vec<String>,
    disjuncts: Vec<Conjunction>,
    joins: Vec<JoinSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeCorePlan {
    table: TableName,
    base_scope: String,
    disjuncts: Vec<Conjunction>,
    joins: Vec<JoinSpec>,
    result_element_index: Option<usize>,
    recursive: Option<RecursiveSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionQueryPlan {
    pub table: TableName,
    pub base_scope: String,
    pub branches: Vec<String>,
    pub disjuncts: Vec<Conjunction>,
    pub joins: Vec<JoinSpec>,
    pub recursive: Option<RecursiveSpec>,
    pub result_element_index: Option<usize>,
    pub order_by: Vec<(String, SortDirection)>,
    pub offset: usize,
    pub limit: Option<usize>,
    pub include_deleted: bool,
    pub array_subqueries: Vec<ArraySubquerySpec>,
    pub select_columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GatherJoinInfo {
    plan: RuntimeCorePlan,
    current_scope: String,
}

fn to_runtime_column(column: &str) -> String {
    if column == "id" {
        "_id".to_string()
    } else {
        column.to_string()
    }
}

fn to_scoped_runtime_column(column_ref: &ColumnRef) -> String {
    let column = to_runtime_column(&column_ref.column);
    match column_ref.scope.as_deref() {
        Some(scope) => format!("{scope}.{column}"),
        None => column,
    }
}

fn predicate_single_scope(predicate: &PredicateExpr) -> Option<String> {
    fn collect(predicate: &PredicateExpr, scopes: &mut Vec<String>) {
        match predicate {
            PredicateExpr::Cmp { left, .. }
            | PredicateExpr::Contains { left, .. }
            | PredicateExpr::In { left, .. } => {
                if let Some(scope) = &left.scope {
                    scopes.push(scope.clone());
                }
            }
            PredicateExpr::IsNull { column } | PredicateExpr::IsNotNull { column } => {
                if let Some(scope) = &column.scope {
                    scopes.push(scope.clone());
                }
            }
            PredicateExpr::And(exprs) | PredicateExpr::Or(exprs) => {
                for expr in exprs {
                    collect(expr, scopes);
                }
            }
            PredicateExpr::Not(inner) => collect(inner, scopes),
            PredicateExpr::True | PredicateExpr::False => {}
        }
    }

    let mut scopes = Vec::new();
    collect(predicate, &mut scopes);
    let first = scopes.first()?.clone();
    scopes
        .into_iter()
        .all(|scope| scope == first)
        .then_some(first)
}

fn flatten_predicate_terms<'a>(predicate: &'a PredicateExpr, out: &mut Vec<&'a PredicateExpr>) {
    match predicate {
        PredicateExpr::And(exprs) => {
            for expr in exprs {
                flatten_predicate_terms(expr, out);
            }
        }
        _ => out.push(predicate),
    }
}

fn predicate_term_to_condition(predicate: &PredicateExpr) -> Option<Condition> {
    match predicate {
        PredicateExpr::Cmp {
            left,
            op,
            right: ValueRef::Literal(value),
        } => {
            let column = to_scoped_runtime_column(left);
            Some(match op {
                PredicateCmpOp::Eq => Condition::Eq {
                    column,
                    value: value.clone(),
                },
                PredicateCmpOp::Ne => Condition::Ne {
                    column,
                    value: value.clone(),
                },
                PredicateCmpOp::Lt => Condition::Lt {
                    column,
                    value: value.clone(),
                },
                PredicateCmpOp::Le => Condition::Le {
                    column,
                    value: value.clone(),
                },
                PredicateCmpOp::Gt => Condition::Gt {
                    column,
                    value: value.clone(),
                },
                PredicateCmpOp::Ge => Condition::Ge {
                    column,
                    value: value.clone(),
                },
            })
        }
        PredicateExpr::Contains {
            left,
            right: ValueRef::Literal(value),
        } => Some(Condition::Contains {
            column: to_scoped_runtime_column(left),
            value: value.clone(),
        }),
        PredicateExpr::IsNull { column } => Some(Condition::IsNull {
            column: to_scoped_runtime_column(column),
        }),
        PredicateExpr::IsNotNull { column } => Some(Condition::IsNotNull {
            column: to_scoped_runtime_column(column),
        }),
        PredicateExpr::True => None,
        _ => None,
    }
}

fn dnf_true() -> Vec<Conjunction> {
    vec![Conjunction::new()]
}

fn and_disjuncts(lhs: Vec<Conjunction>, rhs: Vec<Conjunction>) -> Vec<Conjunction> {
    let mut out = Vec::new();
    for left in lhs {
        for right in &rhs {
            let mut merged = left.clone();
            merged.conditions.extend(right.conditions.clone());
            out.push(merged);
        }
    }
    out
}

fn relation_predicate_to_disjuncts(predicate: &PredicateExpr) -> Option<Vec<Conjunction>> {
    match predicate {
        PredicateExpr::True => Some(dnf_true()),
        PredicateExpr::Cmp { .. }
        | PredicateExpr::Contains { .. }
        | PredicateExpr::IsNull { .. }
        | PredicateExpr::IsNotNull { .. } => {
            let condition = predicate_term_to_condition(predicate)?;
            Some(vec![Conjunction {
                conditions: vec![condition],
            }])
        }
        PredicateExpr::In { left, values } => {
            if values.is_empty() {
                return None;
            }
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                let literal = match value {
                    ValueRef::Literal(v) => v.clone(),
                    _ => return None,
                };
                out.push(Conjunction {
                    conditions: vec![Condition::Eq {
                        column: to_scoped_runtime_column(left),
                        value: literal,
                    }],
                });
            }
            Some(out)
        }
        PredicateExpr::And(exprs) => {
            let mut current = dnf_true();
            for expr in exprs {
                let rhs = relation_predicate_to_disjuncts(expr)?;
                current = and_disjuncts(current, rhs);
                if current.is_empty() {
                    return None;
                }
            }
            Some(current)
        }
        PredicateExpr::Or(exprs) => {
            let mut out = Vec::new();
            for expr in exprs {
                out.extend(relation_predicate_to_disjuncts(expr)?);
            }
            if out.is_empty() {
                return None;
            }
            Some(out)
        }
        PredicateExpr::False | PredicateExpr::Not(_) => None,
    }
}

fn extract_linear_join_info(expr: &RelExpr) -> Option<LinearJoinInfo> {
    match expr {
        RelExpr::TableScan { table } => Some(LinearJoinInfo {
            base_table: *table,
            current_scope: table.as_str().to_string(),
            scope_order: vec![table.as_str().to_string()],
            disjuncts: dnf_true(),
            joins: Vec::new(),
        }),
        RelExpr::Filter { input, predicate } => {
            let mut inner = extract_linear_join_info(input)?;
            if inner.joins.is_empty()
                && let Some(scope) = predicate_single_scope(predicate)
            {
                if let Some(base_scope) = inner.scope_order.first_mut() {
                    *base_scope = scope.clone();
                }
                inner.current_scope = scope;
            }
            let filter_disjuncts = relation_predicate_to_disjuncts(predicate)?;
            inner.disjuncts = and_disjuncts(inner.disjuncts, filter_disjuncts);
            if inner.disjuncts.is_empty() {
                return None;
            }
            Some(inner)
        }
        RelExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => {
            if !matches!(join_kind, JoinKind::Inner) {
                return None;
            }
            let right_table = match right.as_ref() {
                RelExpr::TableScan { table } => *table,
                _ => return None,
            };
            let mut left_info = extract_linear_join_info(left)?;
            let first_join = on.first()?;

            let left_scope = first_join
                .left
                .scope
                .clone()
                .unwrap_or_else(|| left_info.current_scope.clone());
            let right_scope = first_join
                .right
                .scope
                .clone()
                .unwrap_or_else(|| right_table.as_str().to_string());

            if let Some(last_scope) = left_info.scope_order.last_mut() {
                *last_scope = left_scope.clone();
            }

            left_info.joins.push(JoinSpec {
                table: right_table,
                alias: (right_scope != right_table.as_str()).then_some(right_scope.clone()),
                on: Some((
                    format!("{left_scope}.{}", first_join.left.column),
                    format!("{right_scope}.{}", first_join.right.column),
                )),
            });
            left_info.current_scope = right_scope.clone();
            left_info.scope_order.push(right_scope);
            Some(left_info)
        }
        _ => None,
    }
}

fn extract_step_scan(
    expr: &RelExpr,
    predicates: &mut Vec<PredicateExpr>,
    select_columns: &mut Option<Vec<String>>,
) -> Option<TableName> {
    match expr {
        RelExpr::TableScan { table } => Some(*table),
        RelExpr::Filter { input, predicate } => {
            predicates.push(predicate.clone());
            extract_step_scan(input, predicates, select_columns)
        }
        RelExpr::Project { input, columns } => {
            if select_columns.is_some() {
                return None;
            }
            *select_columns = Some(project_columns_to_select(columns)?);
            extract_step_scan(input, predicates, select_columns)
        }
        _ => None,
    }
}

fn parse_frontier_and_filters(
    step_predicates: &[PredicateExpr],
) -> Option<(String, String, Vec<Condition>)> {
    let mut frontier_inner_column: Option<String> = None;
    let mut frontier_outer_column: Option<String> = None;
    let mut step_filters = Vec::new();
    for predicate in step_predicates {
        let mut terms = Vec::new();
        flatten_predicate_terms(predicate, &mut terms);
        for term in terms {
            let frontier_outer = match &term {
                PredicateExpr::Cmp {
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::RowId(RowIdRef::Frontier),
                    ..
                } => Some("_id".to_string()),
                PredicateExpr::Cmp {
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::FrontierColumn(column),
                    ..
                } => Some(to_runtime_column(&column.column)),
                _ => None,
            };
            if let Some(candidate_outer) = frontier_outer {
                let PredicateExpr::Cmp { left, .. } = term else {
                    continue;
                };
                let candidate_inner = to_runtime_column(&left.column);
                if let Some(existing) = &frontier_inner_column
                    && existing != &candidate_inner
                {
                    return None;
                }
                if let Some(existing) = &frontier_outer_column
                    && existing != &candidate_outer
                {
                    return None;
                }
                frontier_inner_column = Some(candidate_inner);
                frontier_outer_column = Some(candidate_outer);
                continue;
            }
            let condition = predicate_term_to_condition(term)?;
            step_filters.push(condition);
        }
    }
    Some((frontier_inner_column?, frontier_outer_column?, step_filters))
}

fn project_columns_to_select(columns: &[ProjectColumn]) -> Option<Vec<String>> {
    let mut select_columns = Vec::with_capacity(columns.len());
    for column in columns {
        let ProjectExpr::Column(column_ref) = &column.expr else {
            return None;
        };
        select_columns.push(to_runtime_column(&column_ref.column));
    }
    Some(select_columns)
}

fn parse_gather_core(seed: &RelExpr, step: &RelExpr, max_depth: usize) -> Option<RuntimeCorePlan> {
    let seed_info = extract_linear_join_info(seed)?;
    if !seed_info.joins.is_empty() {
        return None;
    }

    let (step_core, step_projection) = match step {
        RelExpr::Project { input, columns } => (input.as_ref(), Some(columns.as_slice())),
        _ => (step, None),
    };

    let (step_left, step_right, step_on, step_join_kind) = match step_core {
        RelExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => (left, right, on, join_kind),
        _ => {
            let mut step_predicates = Vec::new();
            let mut select_columns = if let Some(columns) = step_projection {
                Some(project_columns_to_select(columns)?)
            } else {
                None
            };
            let step_scan_table =
                extract_step_scan(step_core, &mut step_predicates, &mut select_columns)?;
            let (inner_column, outer_column, step_filters) =
                parse_frontier_and_filters(&step_predicates)?;

            let recursive = RecursiveSpec {
                table: step_scan_table,
                inner_column,
                outer_column,
                select_columns,
                filters: step_filters,
                joins: Vec::new(),
                result_element_index: None,
                hop: None,
                max_depth,
            };

            return Some(RuntimeCorePlan {
                table: seed_info.base_table,
                base_scope: seed_info.scope_order[0].clone(),
                disjuncts: seed_info.disjuncts,
                joins: Vec::new(),
                result_element_index: None,
                recursive: Some(recursive),
            });
        }
    };
    if !matches!(step_join_kind, JoinKind::Inner) {
        return None;
    }

    let step_hop_table = match step_right.as_ref() {
        RelExpr::TableScan { table } => *table,
        _ => return None,
    };

    let mut step_predicates = Vec::new();
    let mut step_select_columns = None;
    let step_scan_table =
        extract_step_scan(step_left, &mut step_predicates, &mut step_select_columns)?;
    let (inner_column, outer_column, step_filters) = parse_frontier_and_filters(&step_predicates)?;
    let first_join = step_on.first()?;
    let left_scope = first_join
        .left
        .scope
        .clone()
        .unwrap_or_else(|| step_scan_table.as_str().to_string());
    let right_scope = first_join
        .right
        .scope
        .clone()
        .unwrap_or_else(|| step_hop_table.as_str().to_string());

    let right_join_column = to_runtime_column(&first_join.right.column);
    let recursive = if right_join_column == "_id" {
        RecursiveSpec {
            table: step_scan_table,
            inner_column,
            outer_column: outer_column.clone(),
            select_columns: step_select_columns,
            filters: step_filters,
            joins: Vec::new(),
            result_element_index: None,
            hop: Some(RecursiveHopSpec {
                table: step_hop_table,
                via_column: to_runtime_column(&first_join.left.column),
            }),
            max_depth,
        }
    } else {
        if step_select_columns.is_some() {
            return None;
        }
        RecursiveSpec {
            table: step_scan_table,
            inner_column,
            outer_column,
            select_columns: None,
            filters: step_filters,
            joins: vec![JoinSpec {
                table: step_hop_table,
                alias: (right_scope != step_hop_table.as_str()).then_some(right_scope.clone()),
                on: Some((
                    format!("{left_scope}.{}", first_join.left.column),
                    format!("{right_scope}.{}", first_join.right.column),
                )),
            }],
            result_element_index: Some(1),
            hop: None,
            max_depth,
        }
    };

    Some(RuntimeCorePlan {
        table: seed_info.base_table,
        base_scope: seed_info.scope_order[0].clone(),
        disjuncts: seed_info.disjuncts,
        joins: Vec::new(),
        result_element_index: None,
        recursive: Some(recursive),
    })
}

fn parse_gather_join_info(expr: &RelExpr) -> Option<GatherJoinInfo> {
    match expr {
        RelExpr::Gather {
            seed,
            step,
            max_depth,
            ..
        } => {
            let plan = parse_gather_core(seed, step, *max_depth)?;
            Some(GatherJoinInfo {
                current_scope: plan.base_scope.clone(),
                plan,
            })
        }
        RelExpr::Filter { input, predicate } => {
            let mut inner = parse_gather_join_info(input)?;
            if inner.plan.joins.is_empty()
                && let Some(scope) = predicate_single_scope(predicate)
            {
                inner.current_scope = scope.clone();
                inner.plan.base_scope = scope;
            }
            let filter_disjuncts = relation_predicate_to_disjuncts(predicate)?;
            inner.plan.disjuncts = and_disjuncts(inner.plan.disjuncts, filter_disjuncts);
            if inner.plan.disjuncts.is_empty() {
                return None;
            }
            Some(inner)
        }
        RelExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => {
            if !matches!(join_kind, JoinKind::Inner) {
                return None;
            }
            let mut left_info = parse_gather_join_info(left)?;
            let right_table = match right.as_ref() {
                RelExpr::TableScan { table } => *table,
                _ => return None,
            };
            let first_join = on.first()?;
            let left_scope = first_join
                .left
                .scope
                .clone()
                .unwrap_or_else(|| left_info.current_scope.clone());
            let right_scope = first_join
                .right
                .scope
                .clone()
                .unwrap_or_else(|| right_table.as_str().to_string());

            left_info.plan.joins.push(JoinSpec {
                table: right_table,
                alias: (right_scope != right_table.as_str()).then_some(right_scope.clone()),
                on: Some((
                    format!("{left_scope}.{}", first_join.left.column),
                    format!("{right_scope}.{}", first_join.right.column),
                )),
            });
            left_info.current_scope = right_scope;
            Some(left_info)
        }
        _ => None,
    }
}

fn parse_runtime_core_plan(core: &RelExpr) -> Option<RuntimeCorePlan> {
    fn projected_result_element_index(
        scope_order: &[String],
        columns: &[ProjectColumn],
    ) -> Option<usize> {
        let projected_column = match columns {
            [
                ProjectColumn {
                    expr: ProjectExpr::Column(column),
                    ..
                },
            ] => column,
            _ => return None,
        };
        if to_runtime_column(&projected_column.column) != "_id" {
            return None;
        }
        match projected_column.scope.as_deref() {
            Some(scope) => scope_order.iter().position(|candidate| candidate == scope),
            None if scope_order.len() == 1 => Some(0),
            None => None,
        }
    }

    match core {
        RelExpr::Gather {
            seed,
            step,
            max_depth,
            ..
        } => parse_gather_core(seed, step, *max_depth),
        RelExpr::Project { input, columns } => {
            if let Some(mut gather_info) = parse_gather_join_info(input) {
                let mut scope_order = vec![gather_info.plan.base_scope.clone()];
                scope_order.extend(
                    gather_info
                        .plan
                        .joins
                        .iter()
                        .map(|join| join.effective_name().to_string()),
                );
                if let Some(index) = projected_result_element_index(&scope_order, columns) {
                    gather_info.plan.result_element_index = Some(index);
                } else if !gather_info.plan.joins.is_empty() {
                    gather_info.plan.result_element_index = Some(gather_info.plan.joins.len());
                }
                return Some(gather_info.plan);
            }

            let linear = extract_linear_join_info(input)?;
            let result_element_index =
                if let Some(index) = projected_result_element_index(&linear.scope_order, columns) {
                    Some(index)
                } else if !linear.joins.is_empty() {
                    Some(linear.joins.len())
                } else {
                    None
                };
            Some(RuntimeCorePlan {
                table: linear.base_table,
                base_scope: linear.scope_order[0].clone(),
                disjuncts: linear.disjuncts,
                joins: linear.joins.clone(),
                result_element_index,
                recursive: None,
            })
        }
        _ => {
            if let Some(gather_info) = parse_gather_join_info(core) {
                return Some(gather_info.plan);
            }

            let linear = extract_linear_join_info(core)?;
            Some(RuntimeCorePlan {
                table: linear.base_table,
                base_scope: linear.scope_order[0].clone(),
                disjuncts: linear.disjuncts,
                joins: linear.joins,
                result_element_index: None,
                recursive: None,
            })
        }
    }
}

fn unwrap_query_envelope(expr: &RelExpr) -> QueryEnvelope<'_> {
    let mut current = expr;
    let mut order_by = Vec::new();
    let mut offset = 0;
    let mut limit = None;

    loop {
        match current {
            RelExpr::OrderBy { input, terms } => {
                if order_by.is_empty() {
                    order_by = terms
                        .iter()
                        .map(|term| {
                            (
                                term.column.column.clone(),
                                match term.direction {
                                    OrderDirection::Asc => SortDirection::Ascending,
                                    OrderDirection::Desc => SortDirection::Descending,
                                },
                            )
                        })
                        .collect();
                }
                current = input;
            }
            RelExpr::Offset { input, offset: n } => {
                offset = *n;
                current = input;
            }
            RelExpr::Limit { input, limit: n } => {
                limit = Some(*n);
                current = input;
            }
            _ => {
                return QueryEnvelope {
                    core: current,
                    order_by,
                    offset,
                    limit,
                };
            }
        }
    }
}

pub(crate) fn lower_relation_to_execution_plan(
    relation: &RelExpr,
    branches: &[String],
    include_deleted: bool,
    array_subqueries: Vec<ArraySubquerySpec>,
    select_columns: Option<Vec<String>>,
) -> Option<ExecutionQueryPlan> {
    let envelope = unwrap_query_envelope(relation);
    let core_plan = parse_runtime_core_plan(envelope.core)?;
    if core_plan.disjuncts.is_empty() {
        return None;
    }

    Some(ExecutionQueryPlan {
        table: core_plan.table,
        base_scope: core_plan.base_scope,
        branches: branches.to_vec(),
        disjuncts: core_plan.disjuncts,
        joins: core_plan.joins,
        recursive: core_plan.recursive,
        result_element_index: core_plan.result_element_index,
        order_by: envelope.order_by,
        offset: envelope.offset,
        limit: envelope.limit,
        include_deleted,
        array_subqueries,
        select_columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::relation_ir::{ColumnRef, JoinCondition, PredicateCmpOp};
    use crate::query_manager::types::Value;

    #[test]
    fn lower_relation_to_execution_plan_preserves_scoped_join_filters() {
        let relation = RelExpr::Filter {
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
            predicate: PredicateExpr::And(vec![
                PredicateExpr::Cmp {
                    left: ColumnRef::scoped("u", "name"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::Literal(Value::Text("Bob".to_string())),
                },
                PredicateExpr::Cmp {
                    left: ColumnRef::scoped("p", "title"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::Literal(Value::Text("Learning Rust".to_string())),
                },
            ]),
        };
        let branches = vec!["main".to_string()];

        let plan = lower_relation_to_execution_plan(&relation, &branches, false, Vec::new(), None)
            .expect("scoped join filters should lower");

        assert_eq!(plan.base_scope, "u");
        assert_eq!(plan.joins.len(), 1);
        assert_eq!(plan.joins[0].alias.as_deref(), Some("p"));
        assert_eq!(plan.disjuncts.len(), 1);
        assert_eq!(
            plan.disjuncts[0].conditions,
            vec![
                Condition::Eq {
                    column: "u.name".to_string(),
                    value: Value::Text("Bob".to_string()),
                },
                Condition::Eq {
                    column: "p.title".to_string(),
                    value: Value::Text("Learning Rust".to_string()),
                },
            ]
        );
    }

    #[test]
    fn lower_relation_to_execution_plan_tracks_single_table_alias_scope() {
        let relation = RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("users"),
            }),
            predicate: PredicateExpr::Cmp {
                left: ColumnRef::scoped("u", "name"),
                op: PredicateCmpOp::Eq,
                right: ValueRef::Literal(Value::Text("Bob".to_string())),
            },
        };
        let branches = vec!["main".to_string()];

        let plan = lower_relation_to_execution_plan(&relation, &branches, false, Vec::new(), None)
            .expect("single-table alias filter should lower");

        assert_eq!(plan.base_scope, "u");
        assert_eq!(
            plan.disjuncts[0].conditions,
            vec![Condition::Eq {
                column: "u.name".to_string(),
                value: Value::Text("Bob".to_string()),
            }]
        );
    }
}
