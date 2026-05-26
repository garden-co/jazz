use std::collections::HashSet;

use crate::object::ObjectId;
use crate::query_manager::permission_routing::{
    BranchBackingResolution, PermissionRoute, ResolvedBranchPolicyBacking, branch_policy_scope,
    resolve_permission_route_with_backing_loader,
};
use crate::query_manager::policy::{
    BranchPolicyContext, Operation, PolicyExpr, bind_branch_refs, bind_outer_row_refs,
    bind_relation_refs, evaluate_expr_recursive_with_context, normalize_recursive_max_depth,
};
use crate::query_manager::policy_graph::PolicyGraph;
use crate::query_manager::relation_ir::RelExpr;
use crate::query_manager::session::Session;
use crate::query_manager::settlement_eval_cache::{RefAccessSubexprKey, SettlementEvalCache};
use crate::query_manager::types::{
    ColumnType, LoadedRow, PermissionPhase, Row, RowDescriptor, RowPolicyMode, Schema, TableName,
    Value,
};
use crate::storage::Storage;

use super::super::encoding::{column_is_null, decode_column};

pub(crate) struct PolicyContextEvaluator<'a> {
    schema: &'a Schema,
    session: &'a Session,
    branch: &'a str,
    row_policy_mode: RowPolicyMode,
    branch_context: Option<&'a BranchPolicyContext<'a>>,
    settlement_eval_cache: Option<&'a mut SettlementEvalCache>,
}

impl<'a> PolicyContextEvaluator<'a> {
    pub(crate) fn new(
        schema: &'a Schema,
        session: &'a Session,
        branch: &'a str,
        row_policy_mode: RowPolicyMode,
    ) -> Self {
        Self {
            schema,
            session,
            branch,
            row_policy_mode,
            branch_context: None,
            settlement_eval_cache: None,
        }
    }

    pub(crate) fn with_branch_context(
        mut self,
        branch_context: &'a BranchPolicyContext<'a>,
    ) -> Self {
        self.branch_context = Some(branch_context);
        self
    }

    pub(crate) fn with_settlement_eval_cache(
        mut self,
        settlement_eval_cache: Option<&'a mut SettlementEvalCache>,
    ) -> Self {
        self.settlement_eval_cache = settlement_eval_cache;
        self
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn evaluate_row_access(
        &mut self,
        operation: Operation,
        row: &Row,
        descriptor: &RowDescriptor,
        table_name: &str,
        local_policy_override: Option<&PolicyExpr>,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
        depth: usize,
        visited_referencing: &mut HashSet<(TableName, ObjectId, Operation)>,
    ) -> bool {
        if depth > crate::query_manager::policy::RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
            return false;
        }

        let table = TableName::new(table_name);
        let key = (table, row.id, operation);
        if !visited_referencing.insert(key) {
            return false;
        }

        crate::query_manager::policy_counters::increment(
            "row_access_eval",
            format!("table={} op={:?} depth={}", table_name, operation, depth),
        );
        let local_policy = local_policy_override
            .cloned()
            .or_else(|| self.policy_for_operation(table, operation).cloned());
        let local_allow = match local_policy {
            Some(policy) => {
                let mut visited_inherits = HashSet::new();
                self.evaluate_expr_with_context(
                    &policy,
                    operation,
                    row,
                    descriptor,
                    table_name,
                    io,
                    row_loader,
                    depth,
                    &mut visited_inherits,
                    visited_referencing,
                )
            }
            None => !self.row_policy_mode.denies_missing_explicit_policy(),
        };

        visited_referencing.remove(&(table, row.id, operation));
        local_allow
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn evaluate_row_access_with_branch_route(
        &mut self,
        operation: Operation,
        row: &Row,
        descriptor: &RowDescriptor,
        table_name: &str,
        policy_branch: crate::object::BranchName,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
        depth: usize,
        visited_referencing: &mut HashSet<(TableName, ObjectId, Operation)>,
    ) -> bool {
        let Some(route) =
            self.resolve_permission_route(io, policy_branch, TableName::new(table_name))
        else {
            return self.evaluate_row_access(
                operation,
                row,
                descriptor,
                table_name,
                None,
                io,
                row_loader,
                depth,
                visited_referencing,
            );
        };
        if route.is_denied() {
            return false;
        }

        let Some(policy) = route.policy_for_operation(operation, PermissionPhase::Using) else {
            return route.allows_missing_policy(operation, self.row_policy_mode);
        };

        let branch_context = route.branch_context();
        let mut evaluator = PolicyContextEvaluator::new(
            self.schema,
            self.session,
            policy_branch.as_str(),
            self.row_policy_mode,
        );
        if let Some(branch_context) = branch_context.as_ref() {
            evaluator = evaluator.with_branch_context(branch_context);
        }
        evaluator.evaluate_row_access(
            operation,
            row,
            descriptor,
            table_name,
            Some(policy),
            io,
            row_loader,
            depth,
            visited_referencing,
        )
    }

    fn resolve_permission_route(
        &self,
        io: &dyn Storage,
        policy_branch: crate::object::BranchName,
        target_table: TableName,
    ) -> Option<PermissionRoute<'_>> {
        branch_policy_scope(&policy_branch)?;
        Some(resolve_permission_route_with_backing_loader(
            policy_branch,
            target_table,
            self.schema,
            self.row_policy_mode,
            |backing_table, backing_schema, branch_object_id, current_branch| {
                self.resolve_branch_backing(
                    io,
                    backing_table,
                    backing_schema,
                    branch_object_id,
                    current_branch,
                )
            },
        ))
    }

    fn resolve_branch_backing(
        &self,
        io: &dyn Storage,
        backing_table: &TableName,
        backing_schema: &crate::query_manager::types::TableSchema,
        branch_object_id: ObjectId,
        current_branch: crate::object::BranchName,
    ) -> BranchBackingResolution {
        let Ok(Some(backing_row)) = io.load_visible_region_row(
            backing_table.as_str(),
            current_branch.as_str(),
            branch_object_id,
        ) else {
            return BranchBackingResolution::NotFound;
        };
        if backing_row.is_hard_deleted() {
            return BranchBackingResolution::Denied;
        }

        let backing_provenance = backing_row.row_provenance();
        let backing_content = backing_row.data.clone();
        let backing_policy = backing_schema.policies.select_policy();
        let backing_allowed = if let Some(policy) = backing_policy {
            let backing_row_for_policy = Row::new(
                branch_object_id,
                backing_content.clone(),
                backing_row.batch_id,
                backing_provenance.clone(),
            );
            let mut evaluator = PolicyContextEvaluator::new(
                self.schema,
                self.session,
                current_branch.as_str(),
                self.row_policy_mode,
            );
            let mut visited_referencing = HashSet::new();
            let mut backing_dependency_loader = |id: ObjectId,
                                                 table_hint: Option<TableName>|
             -> Option<LoadedRow> {
                let table_hint = table_hint?;
                let Ok(Some(row)) =
                    io.load_visible_region_row(table_hint.as_str(), current_branch.as_str(), id)
                else {
                    return None;
                };
                if row.is_hard_deleted() {
                    return None;
                }
                let row_provenance = row.row_provenance();
                let row_branch = crate::object::BranchName::new(row.branch.as_str());
                Some(LoadedRow::new(
                    row.data,
                    row_provenance,
                    [(id, row_branch)].into_iter().collect(),
                    row.batch_id,
                ))
            };
            evaluator.evaluate_row_access(
                Operation::Select,
                &backing_row_for_policy,
                &backing_schema.columns,
                backing_table.as_str(),
                Some(policy),
                io,
                &mut backing_dependency_loader,
                0,
                &mut visited_referencing,
            )
        } else {
            !self.row_policy_mode.denies_missing_explicit_policy()
        };
        if !backing_allowed {
            return BranchBackingResolution::Denied;
        }

        BranchBackingResolution::Found(ResolvedBranchPolicyBacking {
            backing_table: *backing_table,
            row_id: branch_object_id,
            descriptor: backing_schema.columns.clone(),
            content: backing_content.to_vec(),
            provenance: backing_provenance,
        })
    }

    fn policy_for_operation(
        &mut self,
        table_name: TableName,
        operation: Operation,
    ) -> Option<&PolicyExpr> {
        let table_schema = self.schema.get(&table_name)?;
        match operation {
            Operation::Select => table_schema.policies.select_policy(),
            Operation::Insert => table_schema.policies.insert_policy(),
            Operation::Update => table_schema.policies.update_using_policy(),
            Operation::Delete => table_schema.policies.effective_delete_using(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_inherits_referencing_with_context(
        &mut self,
        operation: Operation,
        source_table: &str,
        via_column: &str,
        max_depth: Option<usize>,
        row: &Row,
        target_table_name: &str,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
        depth: usize,
        visited_referencing: &mut HashSet<(TableName, ObjectId, Operation)>,
    ) -> bool {
        let Some(effective_max_depth) = normalize_recursive_max_depth(max_depth) else {
            return false;
        };
        if depth >= effective_max_depth {
            return false;
        }

        let source_table_name = TableName::new(source_table);
        let Some(source_schema) = self.schema.get(&source_table_name) else {
            return false;
        };
        let source_descriptor = &source_schema.columns;

        let Some(col_idx) = source_descriptor.column_index(via_column) else {
            return false;
        };
        let col = &source_descriptor.columns[col_idx];
        if col.references != Some(TableName::new(target_table_name)) {
            return false;
        }

        let candidate_ids = match &col.column_type {
            ColumnType::Uuid => io.index_lookup(
                source_table_name.as_str(),
                col.name.as_str(),
                self.branch,
                &Value::Uuid(row.id),
            ),
            ColumnType::Array { element } if **element == ColumnType::Uuid => {
                io.index_scan_all(source_table_name.as_str(), col.name.as_str(), self.branch)
            }
            _ => return false,
        };

        for source_row_id in candidate_ids {
            let Some(source_row) = row_loader(source_row_id, Some(source_table_name)) else {
                continue;
            };

            if !referencing_edge_matches_target(
                source_descriptor,
                &source_row.data,
                col_idx,
                row.id,
            ) {
                continue;
            }

            let source_row = Row::new(
                source_row_id,
                source_row.data,
                source_row.batch_id,
                source_row.row_provenance,
            );
            if self.evaluate_row_access(
                operation,
                &source_row,
                source_descriptor,
                source_table_name.as_str(),
                None,
                io,
                row_loader,
                depth + 1,
                visited_referencing,
            ) {
                return true;
            }
        }

        false
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_expr_with_context(
        &mut self,
        expr: &PolicyExpr,
        operation: Operation,
        row: &Row,
        descriptor: &RowDescriptor,
        table_name: &str,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
        depth: usize,
        visited: &mut HashSet<ObjectId>,
        visited_referencing: &mut HashSet<(TableName, ObjectId, Operation)>,
    ) -> bool {
        if depth > crate::query_manager::policy::RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
            return false;
        }

        match expr {
            PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            } => self.evaluate_inherits_with_context(
                *operation,
                via_column,
                *max_depth,
                row,
                descriptor,
                table_name,
                io,
                row_loader,
                depth,
                visited,
                visited_referencing,
            ),
            PolicyExpr::InheritsReferencing {
                operation,
                source_table,
                via_column,
                max_depth,
            } => self.evaluate_inherits_referencing_with_context(
                *operation,
                source_table,
                via_column,
                *max_depth,
                row,
                table_name,
                io,
                row_loader,
                depth,
                visited_referencing,
            ),
            PolicyExpr::Exists { table, condition } => self.evaluate_exists_with_context(
                table, condition, operation, row, descriptor, io, row_loader, depth,
            ),
            PolicyExpr::ExistsRel { rel } => self.evaluate_exists_rel_with_context(
                rel,
                operation == Operation::Select,
                row,
                descriptor,
                table_name,
                io,
                row_loader,
                depth,
            ),
            PolicyExpr::And(exprs) => exprs.iter().all(|e| {
                self.evaluate_expr_with_context(
                    e,
                    operation,
                    row,
                    descriptor,
                    table_name,
                    io,
                    row_loader,
                    depth,
                    visited,
                    visited_referencing,
                )
            }),
            PolicyExpr::Or(exprs) => exprs.iter().any(|e| {
                self.evaluate_expr_with_context(
                    e,
                    operation,
                    row,
                    descriptor,
                    table_name,
                    io,
                    row_loader,
                    depth,
                    visited,
                    visited_referencing,
                )
            }),
            PolicyExpr::Not(inner) => !self.evaluate_expr_with_context(
                inner,
                operation,
                row,
                descriptor,
                table_name,
                io,
                row_loader,
                depth,
                visited,
                visited_referencing,
            ),
            _ => evaluate_expr_recursive_with_context(
                expr,
                &row.data,
                &row.provenance,
                descriptor,
                self.session,
                Some(row.id),
                self.branch_context,
                depth,
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_inherits_with_context(
        &mut self,
        operation: Operation,
        via_column: &str,
        max_depth: Option<usize>,
        row: &Row,
        descriptor: &RowDescriptor,
        _table_name: &str,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
        depth: usize,
        visited: &mut HashSet<ObjectId>,
        visited_referencing: &mut HashSet<(TableName, ObjectId, Operation)>,
    ) -> bool {
        let Some(effective_max_depth) = normalize_recursive_max_depth(max_depth) else {
            return false;
        };
        if depth >= effective_max_depth {
            return false;
        }

        let col_index = match descriptor.column_index(via_column) {
            Some(idx) => idx,
            None => return false,
        };

        if column_is_null(descriptor, &row.data, col_index).unwrap_or(false) {
            return true;
        }

        let col_desc = &descriptor.columns[col_index];
        let parent_table = match &col_desc.references {
            Some(table) => table,
            None => return false,
        };

        let parent_id = match decode_column(descriptor, &row.data, col_index) {
            Ok(Value::Uuid(id)) => id,
            _ => return false,
        };

        let cache_key = if depth == 0 && visited.is_empty() {
            Some(RefAccessSubexprKey {
                branch: self.branch.to_string(),
                table: *parent_table,
                id: parent_id,
                operation,
            })
        } else {
            None
        };

        if let Some(cache_key) = &cache_key {
            if let Some(result) = self
                .settlement_eval_cache
                .as_ref()
                .and_then(|cache| cache.ref_access_get(cache_key))
            {
                crate::query_manager::policy_counters::increment(
                    "ref_access_subexpr_cache",
                    format!("hit table={} op={:?}", cache_key.table.as_str(), operation),
                );
                return result;
            }

            crate::query_manager::policy_counters::increment(
                "ref_access_subexpr_cache",
                format!("miss table={} op={:?}", cache_key.table.as_str(), operation),
            );
        }

        if visited.contains(&parent_id) {
            return false;
        }
        visited.insert(parent_id);

        let parent_table_name = *parent_table;
        let parent_row = match row_loader(parent_id, Some(parent_table_name)) {
            Some(content) => content,
            None => return false,
        };

        let parent_schema = match self.schema.get(&parent_table_name) {
            Some(schema) => schema,
            None => return false,
        };

        let parent_row = Row::new(
            parent_id,
            parent_row.data,
            parent_row.batch_id,
            parent_row.row_provenance,
        );
        let policy_branch = crate::object::BranchName::new(self.branch);
        if branch_policy_scope(&policy_branch).is_some() {
            let mut evaluator = PolicyContextEvaluator::new(
                self.schema,
                self.session,
                self.branch,
                self.row_policy_mode,
            );
            let result = evaluator.evaluate_row_access_with_branch_route(
                operation,
                &parent_row,
                &parent_schema.columns,
                parent_table_name.as_str(),
                policy_branch,
                io,
                row_loader,
                depth + 1,
                visited_referencing,
            );
            if let Some(cache_key) = cache_key
                && let Some(cache) = self.settlement_eval_cache.as_mut()
            {
                cache.ref_access_insert(cache_key, result);
            }
            return result;
        }

        let parent_policy = match operation {
            Operation::Select => parent_schema.policies.select_policy(),
            Operation::Insert => parent_schema.policies.insert_policy(),
            Operation::Update => parent_schema.policies.update_using_policy(),
            Operation::Delete => parent_schema.policies.effective_delete_using(),
        };

        let parent_policy = match parent_policy {
            Some(p) => p,
            None => return false,
        };

        let result = self.evaluate_expr_with_context(
            parent_policy,
            operation,
            &parent_row,
            &parent_schema.columns,
            parent_table_name.as_str(),
            io,
            row_loader,
            depth + 1,
            visited,
            visited_referencing,
        );
        if let Some(cache_key) = cache_key
            && let Some(cache) = self.settlement_eval_cache.as_mut()
        {
            cache.ref_access_insert(cache_key, result);
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_exists_with_context(
        &mut self,
        table: &str,
        condition: &PolicyExpr,
        operation: Operation,
        row: &Row,
        descriptor: &RowDescriptor,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
        depth: usize,
    ) -> bool {
        if depth >= crate::query_manager::policy::RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
            return false;
        }

        let bound_condition =
            match bind_outer_row_refs(condition, &row.data, descriptor, Some(row.id)) {
                Some(expr) => expr,
                None => return false,
            };
        let bound_condition = if let Some(branch_context) = self.branch_context {
            match bind_branch_refs(&bound_condition, branch_context) {
                Some(expr) => expr,
                None => return false,
            }
        } else {
            bound_condition
        };

        let table_name = TableName::new(table);
        let mut graph = match PolicyGraph::for_exists(
            &table_name,
            &bound_condition,
            self.session,
            self.schema,
            self.branch,
            operation,
            self.row_policy_mode,
        ) {
            Some(g) => g,
            None => return false,
        };

        for _ in 0..100 {
            if graph.settle_with_settlement_eval_cache(
                io,
                self.settlement_eval_cache.as_deref_mut(),
                row_loader,
            ) {
                break;
            }
        }

        graph.result()
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_exists_rel_with_context(
        &mut self,
        rel: &RelExpr,
        structural_scans: bool,
        row: &Row,
        descriptor: &RowDescriptor,
        table_name: &str,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
        depth: usize,
    ) -> bool {
        if depth >= crate::query_manager::policy::RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
            return false;
        }

        let bound_rel = match bind_relation_refs(
            rel,
            &row.data,
            descriptor,
            self.session,
            Some(row.id),
            self.branch_context,
        ) {
            Some(expr) => expr,
            None => return false,
        };

        let current_table = TableName::new(table_name);
        let mut graph = match PolicyGraph::for_exists_rel(
            &bound_rel,
            self.schema,
            self.branch,
            Some(self.session.clone()),
            self.row_policy_mode,
            Some(&current_table),
            structural_scans,
        ) {
            Some(g) => g,
            None => return false,
        };

        for _ in 0..100 {
            if graph.settle_with_settlement_eval_cache(
                io,
                self.settlement_eval_cache.as_deref_mut(),
                row_loader,
            ) {
                break;
            }
        }

        graph.result()
    }
}

fn referencing_edge_matches_target(
    descriptor: &RowDescriptor,
    row_content: &[u8],
    column_index: usize,
    target_row_id: ObjectId,
) -> bool {
    match decode_column(descriptor, row_content, column_index) {
        Ok(Value::Uuid(id)) => id == target_row_id,
        Ok(Value::Array(values)) => values
            .iter()
            .any(|value| matches!(value, Value::Uuid(id) if *id == target_row_id)),
        _ => false,
    }
}

pub(crate) fn collect_policy_dependency_tables(
    policy: &PolicyExpr,
    descriptor: &RowDescriptor,
) -> HashSet<String> {
    let mut tables = HashSet::new();
    collect_policy_dependency_tables_recursive(policy, descriptor, &mut tables);
    tables
}

fn collect_policy_dependency_tables_recursive(
    policy: &PolicyExpr,
    descriptor: &RowDescriptor,
    tables: &mut HashSet<String>,
) {
    match policy {
        PolicyExpr::Inherits { via_column, .. } => {
            let Some(col_index) = descriptor.column_index(via_column) else {
                return;
            };
            if let Some(ref references) = descriptor.columns[col_index].references {
                tables.insert(references.as_str().to_string());
            }
        }
        PolicyExpr::InheritsReferencing { source_table, .. } => {
            tables.insert(source_table.clone());
        }
        PolicyExpr::And(exprs) | PolicyExpr::Or(exprs) => {
            for expr in exprs {
                collect_policy_dependency_tables_recursive(expr, descriptor, tables);
            }
        }
        PolicyExpr::Exists { table, condition } => {
            tables.insert(table.clone());
            collect_policy_dependency_tables_recursive(condition, descriptor, tables);
        }
        PolicyExpr::ExistsRel { rel } => {
            collect_relation_tables(rel, tables);
        }
        PolicyExpr::Not(inner) => {
            collect_policy_dependency_tables_recursive(inner, descriptor, tables);
        }
        _ => {}
    }
}

fn collect_relation_tables(rel: &RelExpr, tables: &mut HashSet<String>) {
    match rel {
        RelExpr::TableScan { table } => {
            tables.insert(table.as_str().to_string());
        }
        RelExpr::Union { inputs } => {
            for input in inputs {
                collect_relation_tables(input, tables);
            }
        }
        RelExpr::Filter { input, .. }
        | RelExpr::Branch { input, .. }
        | RelExpr::Project { input, .. }
        | RelExpr::Distinct { input, .. }
        | RelExpr::OrderBy { input, .. }
        | RelExpr::Offset { input, .. }
        | RelExpr::Limit { input, .. } => collect_relation_tables(input, tables),
        RelExpr::Join { left, right, .. } => {
            collect_relation_tables(left, tables);
            collect_relation_tables(right, tables);
        }
        RelExpr::Gather { seed, step, .. } => {
            collect_relation_tables(seed, tables);
            collect_relation_tables(step, tables);
        }
    }
}
