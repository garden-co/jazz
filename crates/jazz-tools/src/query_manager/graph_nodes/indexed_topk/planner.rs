use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

use crate::object::BranchName;
use crate::query_manager::graph_nodes::filter::{FilterNode, Predicate};
use crate::query_manager::graph_nodes::policy_filter::PolicyFilterNode;
use crate::query_manager::policy::PolicyExpr;
use crate::query_manager::query::{Condition, Conjunction};
use crate::query_manager::relation_ir_query_plan::ExecutionQueryPlan;
use crate::query_manager::session::Session;
use crate::query_manager::types::{
    RowDescriptor, Schema, SchemaHash, TableName, TupleDescriptor, Value,
};
use crate::schema_manager::{SchemaContext, translate_column_for_index};
use crate::storage::IndexScanDirection;

use super::plan::{
    ExactMatchProbe, IndexedTopKGraphPlan, JoinLookupSpec, MergeOrderedSpec,
    OrderedDriverSourcePlan, OrderedDriverSourceSpec, ProbeJoinSpec, ResolvedRowKey,
    ResolvedSortKey, ResolvedSortTarget, ScopedPolicySpec, TieSortSpec,
};

const DIRECT_REQUIRED_IDS_MIN: usize = 256;
const DIRECT_REQUIRED_IDS_PREFIX_MULTIPLIER: usize = 4;

#[derive(Debug, Clone)]
struct DriverPlan {
    scope_index: usize,
    table: TableName,
    key: ResolvedRowKey,
    direction: IndexScanDirection,
    sort_keys: Vec<ResolvedSortKey>,
}

#[derive(Debug, Clone)]
struct DriverPolicyTemplate {
    table: TableName,
    descriptor: RowDescriptor,
    policy: PolicyExpr,
    session: Session,
    schema: Schema,
    dependency_tables: Vec<TableName>,
}

pub(crate) fn compile_indexed_topk_graph_plan(
    plan: &ExecutionQueryPlan,
    schema: &Schema,
    branches: &[String],
    session: Option<Session>,
    schema_context: &SchemaContext,
    branch_schema_map: &HashMap<String, SchemaHash>,
) -> Option<IndexedTopKGraphPlan> {
    if plan.include_deleted || plan.recursive.is_some() {
        return None;
    }
    if plan.limit.is_none() && plan.offset == 0 {
        return None;
    }
    if !plan.joins.is_empty() && !plan.array_subqueries.is_empty() {
        return None;
    }

    let base_table_schema = schema.get(&plan.table)?;
    let base_descriptor = base_table_schema.columns.clone();

    let mut scope_names = vec![plan.base_scope.clone()];
    let mut table_names = vec![plan.table];
    let mut table_descriptors = vec![base_descriptor.clone()];
    let mut seen_tables = HashSet::new();
    seen_tables.insert(plan.table.as_str().to_string());

    for join_spec in &plan.joins {
        let join_table_name = join_spec.table.as_str().to_string();
        if seen_tables.contains(&join_table_name) {
            return None;
        }

        let joined_schema = schema.get(&join_spec.table)?;
        scope_names.push(join_spec.effective_name().to_string());
        table_names.push(join_spec.table);
        table_descriptors.push(joined_schema.columns.clone());
        seen_tables.insert(join_table_name);
    }

    let tuple_descriptor = TupleDescriptor::from_tables(
        &scope_names
            .iter()
            .cloned()
            .zip(table_descriptors.iter().cloned())
            .collect::<Vec<_>>(),
    )
    .with_all_materialized();
    let combined_descriptor = RowDescriptor::combine(&table_descriptors);
    let desired_prefix_len = plan.limit.map(|limit| plan.offset.saturating_add(limit));
    let max_direct_required_ids = desired_prefix_len
        .map(|prefix_len| {
            prefix_len
                .saturating_mul(DIRECT_REQUIRED_IDS_PREFIX_MULTIPLIER)
                .max(DIRECT_REQUIRED_IDS_MIN)
        })
        .unwrap_or(DIRECT_REQUIRED_IDS_MIN);

    let (driver_scope_index, driver_key, sort_keys) = if plan.order_by.is_empty() {
        let driver_key = ResolvedRowKey::from_descriptor(&base_descriptor, "_id")?;
        (
            0,
            driver_key,
            vec![ResolvedSortKey {
                target: ResolvedSortTarget::RowId { element_index: 0 },
                direction: crate::query_manager::graph_nodes::sort::SortDirection::Ascending,
            }],
        )
    } else {
        let mut resolved_sort_keys = Vec::with_capacity(plan.order_by.len());
        let mut driver_scope_index = None;
        let mut driver_key = None;

        for (column, direction) in &plan.order_by {
            let (scope_index, key, sort_key) =
                resolve_sort_key(&tuple_descriptor, column, *direction)?;
            if driver_scope_index.is_none() {
                driver_scope_index = Some(scope_index);
                driver_key = Some(key.clone());
            }
            resolved_sort_keys.push(sort_key);
        }

        (driver_scope_index?, driver_key?, resolved_sort_keys)
    };

    let driver = DriverPlan {
        scope_index: driver_scope_index,
        table: table_names[driver_scope_index],
        key: driver_key.clone(),
        direction: match sort_keys.first()?.direction {
            crate::query_manager::graph_nodes::sort::SortDirection::Ascending => {
                IndexScanDirection::Ascending
            }
            crate::query_manager::graph_nodes::sort::SortDirection::Descending => {
                IndexScanDirection::Descending
            }
        },
        sort_keys: sort_keys.clone(),
    };

    let topk_branch_schema_map: HashMap<BranchName, SchemaHash> = branches
        .iter()
        .filter_map(|branch| {
            branch_schema_map
                .get(branch)
                .copied()
                .map(|hash| (BranchName::new(branch), hash))
        })
        .collect();

    let join_edges = resolve_join_edges(
        plan,
        &tuple_descriptor,
        &table_names,
        schema_context,
        &topk_branch_schema_map,
        branches,
    )?;
    let probe_join_lookup_columns = probe_join_lookup_columns(&join_edges);

    let residual_filter = match disjuncts_to_predicate(&plan.disjuncts, &tuple_descriptor) {
        Predicate::True => None,
        predicate => Some(FilterNode::with_tuple_descriptor(
            tuple_descriptor.clone(),
            predicate,
        )),
    };

    let (driver_policy_template, joined_policy_specs) = build_policy_specs(
        driver.scope_index,
        session,
        schema,
        branches,
        &table_names,
        &table_descriptors,
    )?;
    let driver_policy_dependencies = driver_policy_template
        .as_ref()
        .map(|template| template.dependency_tables.clone())
        .unwrap_or_default();
    let probe_join_policy_dependencies = joined_policy_specs
        .iter()
        .flat_map(|policy| policy.dependency_tables.iter().copied())
        .collect();

    let source_plans = build_source_plans(
        branches,
        &topk_branch_schema_map,
        schema_context,
        &tuple_descriptor,
        &driver,
        &plan.disjuncts,
        desired_prefix_len,
        max_direct_required_ids,
        join_edges.is_empty() && residual_filter.is_none(),
        driver_policy_template.as_ref(),
        &driver_policy_dependencies,
    )?;

    let merge_spec = Arc::new(MergeOrderedSpec {
        direction: driver.direction,
        driver_descriptor: table_descriptors[driver.scope_index].clone(),
        driver_key: driver.key.clone(),
    });
    let probe_join_spec = Arc::new(ProbeJoinSpec {
        driver_scope_index: driver.scope_index,
        table_descriptors: table_descriptors.clone(),
        join_edges,
        residual_filter,
        policies: joined_policy_specs,
    });
    let tie_sort_spec = Arc::new(TieSortSpec {
        driver_scope_index: driver.scope_index,
        driver_descriptor: table_descriptors[driver.scope_index].clone(),
        driver_key,
        sort_keys: driver.sort_keys,
        desired_prefix_len,
    });

    Some(IndexedTopKGraphPlan {
        base_descriptor,
        combined_descriptor,
        table_descriptors,
        tuple_descriptor,
        source_plans,
        merge_spec,
        probe_join_spec,
        probe_join_lookup_columns,
        probe_join_policy_dependencies,
        tie_sort_spec,
        limit: plan.limit,
        offset: plan.offset,
    })
}

fn resolve_join_edges(
    plan: &ExecutionQueryPlan,
    tuple_descriptor: &TupleDescriptor,
    table_names: &[TableName],
    schema_context: &SchemaContext,
    branch_schema_map: &HashMap<BranchName, SchemaHash>,
    branches: &[String],
) -> Option<Vec<JoinLookupSpec>> {
    let mut join_edges = Vec::with_capacity(plan.joins.len());
    for (join_index, join_spec) in plan.joins.iter().enumerate() {
        let (left_col, right_col) = join_spec.on.as_ref()?;
        let left_candidates = (0..=join_index).collect::<Vec<_>>();
        let right_scope_index = join_index + 1;
        let (left_scope_index, left_key) =
            resolve_row_key_in_scopes(tuple_descriptor, left_col, &left_candidates)?;
        let (resolved_right_scope_index, right_key) =
            resolve_row_key_in_scopes(tuple_descriptor, right_col, &[right_scope_index])?;
        if resolved_right_scope_index != right_scope_index {
            return None;
        }

        join_edges.push(JoinLookupSpec {
            left_scope_index,
            right_scope_index,
            left_table: table_names[left_scope_index],
            right_table: table_names[right_scope_index],
            left_key: left_key.clone(),
            right_key: right_key.clone(),
            left_translated_columns_by_branch: translated_columns_by_branch(
                branches,
                branch_schema_map,
                schema_context,
                table_names[left_scope_index],
                left_key.index_column(),
            ),
            right_translated_columns_by_branch: translated_columns_by_branch(
                branches,
                branch_schema_map,
                schema_context,
                table_names[right_scope_index],
                right_key.index_column(),
            ),
        });
    }

    Some(join_edges)
}

fn build_policy_specs(
    driver_scope_index: usize,
    session: Option<Session>,
    schema: &Schema,
    branches: &[String],
    table_names: &[TableName],
    table_descriptors: &[RowDescriptor],
) -> Option<(Option<DriverPolicyTemplate>, Vec<ScopedPolicySpec>)> {
    let Some(session) = session else {
        return Some((None, Vec::new()));
    };

    let mut driver_policy_template = None;
    let mut joined_policy_specs = Vec::new();

    for (scope_index, table) in table_names.iter().enumerate() {
        let table_schema = schema.get(table)?;
        let Some(policy) = table_schema.policies.select.using.clone() else {
            continue;
        };
        if policy == PolicyExpr::True {
            continue;
        }

        let evaluator = PolicyFilterNode::new_with_branch(
            table_descriptors[scope_index].clone(),
            policy.clone(),
            session.clone(),
            schema.clone(),
            table.as_str(),
            branches.first().map(String::as_str).unwrap_or("main"),
        );
        let dependency_tables = evaluator
            .inherits_tables()
            .iter()
            .map(|table| TableName::new(table.as_str()))
            .collect::<Vec<_>>();

        if scope_index == driver_scope_index {
            driver_policy_template = Some(DriverPolicyTemplate {
                table: *table,
                descriptor: table_descriptors[scope_index].clone(),
                policy,
                session: session.clone(),
                schema: schema.clone(),
                dependency_tables,
            });
            continue;
        }

        let mut evaluators_by_branch = HashMap::with_capacity(branches.len());
        for branch in branches {
            let branch_name = BranchName::new(branch);
            evaluators_by_branch.insert(
                branch_name,
                PolicyFilterNode::new_with_branch(
                    table_descriptors[scope_index].clone(),
                    policy.clone(),
                    session.clone(),
                    schema.clone(),
                    table.as_str(),
                    branch,
                ),
            );
        }
        joined_policy_specs.push(ScopedPolicySpec {
            scope_index,
            dependency_tables,
            evaluators_by_branch,
        });
    }

    Some((driver_policy_template, joined_policy_specs))
}

#[allow(clippy::too_many_arguments)]
fn build_source_plans(
    branches: &[String],
    branch_schema_map: &HashMap<BranchName, SchemaHash>,
    schema_context: &SchemaContext,
    tuple_descriptor: &TupleDescriptor,
    driver: &DriverPlan,
    disjuncts: &[Conjunction],
    desired_prefix_len: Option<usize>,
    max_direct_required_ids: usize,
    enable_prefix_short_circuit: bool,
    driver_policy_template: Option<&DriverPolicyTemplate>,
    driver_policy_dependencies: &[TableName],
) -> Option<Vec<OrderedDriverSourcePlan>> {
    let driver_element = tuple_descriptor.element(driver.scope_index)?;
    let mut source_plans = Vec::with_capacity(branches.len().saturating_mul(disjuncts.len()));

    for branch in branches {
        let branch_name = BranchName::new(branch);
        let translated_driver_column = translate_index_column_for_branch(
            schema_context,
            branch_schema_map.get(&branch_name).copied(),
            driver.table,
            driver.key.index_column(),
        );

        for disjunct in disjuncts {
            let mut driver_conditions = Vec::new();
            let mut exact_match_probe_columns = Vec::new();
            let mut required_probes = Vec::new();

            for condition in &disjunct.conditions {
                let Some(scope_index) = resolve_condition_scope(tuple_descriptor, condition) else {
                    continue;
                };
                if scope_index != driver.scope_index {
                    continue;
                }

                if driver
                    .key
                    .matches_selector(&driver_element.descriptor, condition.column())
                {
                    if condition.is_index_scannable() {
                        driver_conditions.push(condition.clone());
                    }
                    continue;
                }

                let Condition::Eq { column, value } = condition else {
                    continue;
                };
                let logical_column = column.split('.').next_back().unwrap_or(column).to_string();
                if !exact_match_probe_columns.contains(&logical_column) {
                    exact_match_probe_columns.push(logical_column.clone());
                }
                required_probes.push(ExactMatchProbe {
                    translated_column: translate_index_column_for_branch(
                        schema_context,
                        branch_schema_map.get(&branch_name).copied(),
                        driver.table,
                        &logical_column,
                    ),
                    value: value.clone(),
                });
            }

            let (start, end) = if driver_conditions.is_empty() {
                (Bound::Unbounded, Bound::Unbounded)
            } else {
                bounds_for_driver_conditions(&Conjunction {
                    conditions: driver_conditions,
                })
                .unwrap_or((Bound::Unbounded, Bound::Unbounded))
            };

            let policy_evaluator = driver_policy_template.map(|template| {
                PolicyFilterNode::new_with_branch(
                    template.descriptor.clone(),
                    template.policy.clone(),
                    template.session.clone(),
                    template.schema.clone(),
                    template.table.as_str(),
                    branch,
                )
            });

            source_plans.push(OrderedDriverSourcePlan {
                spec: Arc::new(OrderedDriverSourceSpec {
                    branch: branch_name,
                    table: driver.table,
                    driver_descriptor: driver_element.descriptor.clone(),
                    driver_key: driver.key.clone(),
                    direction: driver.direction,
                    translated_driver_column: translated_driver_column.clone(),
                    start,
                    end,
                    required_probes,
                    policy_evaluator,
                    desired_prefix_len,
                    enable_prefix_short_circuit,
                    max_direct_required_ids,
                }),
                ordered_scan_column: driver.key.index_column().to_string(),
                exact_match_probe_columns,
                policy_dependencies: driver_policy_dependencies.to_vec(),
            });
        }
    }

    Some(source_plans)
}

fn translated_columns_by_branch(
    branches: &[String],
    branch_schema_map: &HashMap<BranchName, SchemaHash>,
    schema_context: &SchemaContext,
    table: TableName,
    column: &str,
) -> HashMap<BranchName, String> {
    branches
        .iter()
        .map(|branch| {
            let branch_name = BranchName::new(branch);
            (
                branch_name,
                translate_index_column_for_branch(
                    schema_context,
                    branch_schema_map.get(&branch_name).copied(),
                    table,
                    column,
                ),
            )
        })
        .collect()
}

fn translate_index_column_for_branch(
    schema_context: &SchemaContext,
    branch_schema_hash: Option<SchemaHash>,
    table: TableName,
    column: &str,
) -> String {
    let logical_column = column.split('.').next_back().unwrap_or(column);
    if let Some(target_hash) = branch_schema_hash
        && target_hash != schema_context.current_hash
    {
        return translate_column_for_index(
            schema_context,
            table.as_str(),
            logical_column,
            &target_hash,
        )
        .unwrap_or_else(|| logical_column.to_string());
    }
    logical_column.to_string()
}

fn probe_join_lookup_columns(join_edges: &[JoinLookupSpec]) -> Vec<(TableName, String)> {
    let mut seen = HashSet::new();
    let mut columns = Vec::new();

    for edge in join_edges {
        let left = (edge.left_table, edge.left_key.index_column().to_string());
        if seen.insert(left.clone()) {
            columns.push(left);
        }

        let right = (edge.right_table, edge.right_key.index_column().to_string());
        if seen.insert(right.clone()) {
            columns.push(right);
        }
    }

    columns
}

fn resolve_row_key_in_scopes(
    tuple_descriptor: &TupleDescriptor,
    raw: &str,
    candidate_scopes: &[usize],
) -> Option<(usize, ResolvedRowKey)> {
    if let Some((scope, column)) = raw.rsplit_once('.') {
        let scope_index = candidate_scopes.iter().copied().find(|index| {
            tuple_descriptor
                .element(*index)
                .is_some_and(|element| element.table == scope)
        })?;
        let descriptor = &tuple_descriptor.element(scope_index)?.descriptor;
        let key = ResolvedRowKey::from_descriptor(descriptor, column.trim())?;
        return Some((scope_index, key));
    }

    let mut matches = candidate_scopes
        .iter()
        .copied()
        .filter_map(|index| {
            let descriptor = &tuple_descriptor.element(index)?.descriptor;
            let key = ResolvedRowKey::from_descriptor(descriptor, raw)?;
            Some((index, key))
        })
        .collect::<Vec<_>>();

    (matches.len() == 1).then(|| matches.remove(0))
}

fn resolve_sort_key(
    tuple_descriptor: &TupleDescriptor,
    column: &str,
    direction: crate::query_manager::graph_nodes::sort::SortDirection,
) -> Option<(usize, ResolvedRowKey, ResolvedSortKey)> {
    let candidate_scopes = (0..tuple_descriptor.element_count()).collect::<Vec<_>>();
    let (scope_index, key) =
        resolve_row_key_in_scopes(tuple_descriptor, column, &candidate_scopes)?;
    let target = if key.use_row_id {
        ResolvedSortTarget::RowId {
            element_index: scope_index,
        }
    } else {
        ResolvedSortTarget::Column {
            element_index: scope_index,
            descriptor: tuple_descriptor.element(scope_index)?.descriptor.clone(),
            local_col_index: key.local_col_index?,
        }
    };

    Some((scope_index, key, ResolvedSortKey { target, direction }))
}

fn disjuncts_to_predicate(
    disjuncts: &[Conjunction],
    tuple_descriptor: &TupleDescriptor,
) -> Predicate {
    if disjuncts.is_empty() {
        return Predicate::True;
    }

    let non_empty: Vec<_> = disjuncts
        .iter()
        .filter(|d| !d.conditions.is_empty())
        .collect();
    if non_empty.is_empty() {
        return Predicate::True;
    }
    if non_empty.len() == 1 {
        return non_empty[0].to_tuple_predicate(tuple_descriptor);
    }

    Predicate::Or(
        non_empty
            .iter()
            .map(|d| d.to_tuple_predicate(tuple_descriptor))
            .collect(),
    )
}

fn resolve_condition_scope(
    tuple_descriptor: &TupleDescriptor,
    condition: &Condition,
) -> Option<usize> {
    let raw_column = condition.raw_column();
    let column = raw_column.split('.').next_back().unwrap_or(raw_column);

    if let Some((scope, _)) = raw_column.rsplit_once('.') {
        let scope = scope.trim();
        return (0..tuple_descriptor.element_count()).find(|index| {
            tuple_descriptor
                .element(*index)
                .is_some_and(|element| element.table == scope)
        });
    }

    let matches: Vec<_> = (0..tuple_descriptor.element_count())
        .filter(|index| {
            tuple_descriptor
                .element(*index)
                .is_some_and(|element| element.descriptor.column_index(column).is_some())
        })
        .collect();
    if matches.len() == 1 {
        return matches.into_iter().next();
    }

    if (column == "id" || column == "_id") && tuple_descriptor.element_count() == 1 {
        let element = tuple_descriptor.element(0)?;
        if element.descriptor.column_index(column).is_none() {
            return Some(0);
        }
    }

    None
}

fn bounds_for_driver_conditions(conjunction: &Conjunction) -> Option<(Bound<Value>, Bound<Value>)> {
    let mut lower: Option<Bound<Value>> = None;
    let mut upper: Option<Bound<Value>> = None;
    let mut exact: Option<Value> = None;

    for condition in &conjunction.conditions {
        match condition {
            Condition::Eq { value, .. } => {
                if lower.is_some() || upper.is_some() || exact.is_some() {
                    return None;
                }
                exact = Some(value.clone());
            }
            Condition::Gt { value, .. } => {
                if lower.is_some() || exact.is_some() {
                    return None;
                }
                lower = Some(Bound::Excluded(value.clone()));
            }
            Condition::Ge { value, .. } => {
                if lower.is_some() || exact.is_some() {
                    return None;
                }
                lower = Some(Bound::Included(value.clone()));
            }
            Condition::Lt { value, .. } => {
                if upper.is_some() || exact.is_some() {
                    return None;
                }
                upper = Some(Bound::Excluded(value.clone()));
            }
            Condition::Le { value, .. } => {
                if upper.is_some() || exact.is_some() {
                    return None;
                }
                upper = Some(Bound::Included(value.clone()));
            }
            Condition::Between { min, max, .. } => {
                if lower.is_some() || upper.is_some() || exact.is_some() {
                    return None;
                }
                lower = Some(Bound::Included(min.clone()));
                upper = Some(Bound::Included(max.clone()));
            }
            _ => return None,
        }
    }

    if let Some(value) = exact {
        return Some((Bound::Included(value.clone()), Bound::Included(value)));
    }

    Some((
        lower.unwrap_or(Bound::Unbounded),
        upper.unwrap_or(Bound::Unbounded),
    ))
}
