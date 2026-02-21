use super::graph_nodes::sort::SortDirection;
use super::query::{ArraySubquerySpec, Condition, Conjunction, JoinSpec, Query, RecursiveSpec};
use super::relation_ir::{
    JoinKind, OrderDirection, PredicateCmpOp, PredicateExpr, RelExpr, RowIdRef, ValueRef,
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
    base_scope: String,
    disjuncts: Vec<Conjunction>,
    joins: Vec<JoinSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeCorePlan {
    table: TableName,
    disjuncts: Vec<Conjunction>,
    joins: Vec<JoinSpec>,
    result_element_index: Option<usize>,
    recursive: Option<RecursiveSpec>,
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
            let column = to_runtime_column(&left.column);
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
        PredicateExpr::IsNull { column } => Some(Condition::IsNull {
            column: to_runtime_column(&column.column),
        }),
        PredicateExpr::IsNotNull { column } => Some(Condition::IsNotNull {
            column: to_runtime_column(&column.column),
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
                        column: to_runtime_column(&left.column),
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
            base_scope: table.as_str().to_string(),
            disjuncts: dnf_true(),
            joins: Vec::new(),
        }),
        RelExpr::Filter { input, predicate } => {
            let mut inner = extract_linear_join_info(input)?;
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
                .unwrap_or_else(|| left_info.base_scope.clone());
            let right_scope = first_join
                .right
                .scope
                .clone()
                .unwrap_or_else(|| right_table.as_str().to_string());

            left_info.joins.push(JoinSpec {
                table: right_table,
                alias: (right_scope != right_table.as_str()).then_some(right_scope.clone()),
                on: Some((
                    format!("{left_scope}.{}", first_join.left.column),
                    format!("{right_scope}.{}", first_join.right.column),
                )),
            });
            left_info.base_scope = right_scope;
            Some(left_info)
        }
        _ => None,
    }
}

fn extract_step_scan(expr: &RelExpr, predicates: &mut Vec<PredicateExpr>) -> Option<TableName> {
    match expr {
        RelExpr::TableScan { table } => Some(*table),
        RelExpr::Filter { input, predicate } => {
            predicates.push(predicate.clone());
            extract_step_scan(input, predicates)
        }
        _ => None,
    }
}

fn parse_gather_core(seed: &RelExpr, step: &RelExpr, max_depth: usize) -> Option<RuntimeCorePlan> {
    let seed_info = extract_linear_join_info(seed)?;
    if !seed_info.joins.is_empty() {
        return None;
    }

    let (step_join, _step_columns) = match step {
        RelExpr::Project { input, columns } => (input, columns),
        _ => return None,
    };
    let (step_left, step_right, step_on, step_join_kind) = match step_join.as_ref() {
        RelExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => (left, right, on, join_kind),
        _ => return None,
    };
    if !matches!(step_join_kind, JoinKind::Inner) {
        return None;
    }

    let step_hop_table = match step_right.as_ref() {
        RelExpr::TableScan { table } => *table,
        _ => return None,
    };

    let mut step_predicates = Vec::new();
    let step_scan_table = extract_step_scan(step_left, &mut step_predicates)?;

    let mut frontier_column: Option<String> = None;
    let mut step_filters = Vec::new();
    for predicate in &step_predicates {
        let mut terms = Vec::new();
        flatten_predicate_terms(predicate, &mut terms);
        for term in terms {
            match term {
                PredicateExpr::Cmp {
                    left,
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::RowId(RowIdRef::Frontier),
                } => {
                    let candidate = to_runtime_column(&left.column);
                    if let Some(existing) = &frontier_column
                        && existing != &candidate
                    {
                        return None;
                    }
                    frontier_column = Some(candidate);
                }
                _ => {
                    let condition = predicate_term_to_condition(term)?;
                    step_filters.push(condition);
                }
            }
        }
    }

    let inner_column = frontier_column?;
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

    let recursive = RecursiveSpec {
        table: step_scan_table,
        inner_column,
        outer_column: "_id".to_string(),
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
    };

    Some(RuntimeCorePlan {
        table: seed_info.base_table,
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
                current_scope: plan.table.as_str().to_string(),
                plan,
            })
        }
        RelExpr::Filter { input, predicate } => {
            let mut inner = parse_gather_join_info(input)?;
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
    match core {
        RelExpr::Gather {
            seed,
            step,
            max_depth,
            ..
        } => parse_gather_core(seed, step, *max_depth),
        RelExpr::Project { input, .. } => {
            if let Some(mut gather_info) = parse_gather_join_info(input) {
                if !gather_info.plan.joins.is_empty() {
                    gather_info.plan.result_element_index = Some(gather_info.plan.joins.len());
                }
                return Some(gather_info.plan);
            }

            let linear = extract_linear_join_info(input)?;
            Some(RuntimeCorePlan {
                table: linear.base_table,
                disjuncts: linear.disjuncts,
                joins: linear.joins.clone(),
                result_element_index: Some(linear.joins.len()),
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

pub(crate) fn lower_relation_to_execution_query(
    relation: &RelExpr,
    branches: &[String],
    include_deleted: bool,
    array_subqueries: Vec<ArraySubquerySpec>,
) -> Option<Query> {
    let envelope = unwrap_query_envelope(relation);
    let core_plan = parse_runtime_core_plan(envelope.core)?;

    let mut lowered = Query::new(core_plan.table);
    lowered.table = core_plan.table;
    lowered.alias = None;
    lowered.branches = branches.to_vec();
    if core_plan.disjuncts.is_empty() {
        return None;
    }
    lowered.disjuncts = core_plan.disjuncts;
    lowered.joins = core_plan.joins;
    lowered.order_by = envelope.order_by;
    lowered.offset = envelope.offset;
    lowered.limit = envelope.limit;
    lowered.result_element_index = core_plan.result_element_index;
    lowered.recursive = core_plan.recursive;
    lowered.include_deleted = include_deleted;
    lowered.array_subqueries = array_subqueries;
    lowered.select_columns = None;
    lowered.relation_ir = None;
    Some(lowered)
}
