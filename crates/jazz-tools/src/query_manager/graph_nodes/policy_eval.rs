use std::collections::HashSet;

use crate::object::{BranchName, ObjectId};
use crate::query_manager::policy::{
    Operation, PolicyExpr, bind_outer_row_refs, bind_relation_refs,
    evaluate_expr_recursive_with_row_id, normalize_recursive_max_depth,
};
use crate::query_manager::policy_graph::PolicyGraph;
use crate::query_manager::relation_ir::RelExpr;
use crate::query_manager::session::Session;
use crate::query_manager::types::{
    ColumnType, LoadedRow, Row, RowDescriptor, RowPolicyMode, Schema, TableName, Value,
};
use crate::storage::Storage;

use super::super::encoding::{column_is_null, decode_column};

type WitnessScope = HashSet<(ObjectId, BranchName)>;

#[derive(Default)]
pub(crate) struct ScopedPolicyResult {
    pub(crate) allowed: bool,
    pub(crate) scope: WitnessScope,
}

pub(crate) struct PolicyContextEvaluator<'a> {
    schema: &'a Schema,
    session: &'a Session,
    branch: &'a str,
    row_policy_mode: RowPolicyMode,
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
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn evaluate_row_access(
        &self,
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
        self.evaluate_row_access_with_witness_scope(
            operation,
            row,
            descriptor,
            table_name,
            local_policy_override,
            io,
            row_loader,
            depth,
            visited_referencing,
        )
        .allowed
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn evaluate_row_access_with_witness_scope(
        &self,
        operation: Operation,
        row: &Row,
        descriptor: &RowDescriptor,
        table_name: &str,
        local_policy_override: Option<&PolicyExpr>,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
        depth: usize,
        visited_referencing: &mut HashSet<(TableName, ObjectId, Operation)>,
    ) -> ScopedPolicyResult {
        if depth > crate::query_manager::policy::RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
            return ScopedPolicyResult::default();
        }

        let table = TableName::new(table_name);
        let key = (table, row.id, operation);
        if !visited_referencing.insert(key) {
            return ScopedPolicyResult::default();
        }

        let result = local_policy_override
            .or_else(|| self.policy_for_operation(table, operation))
            .map(|policy| {
                let mut visited_inherits = HashSet::new();
                self.evaluate_expr_with_witness_scope(
                    policy,
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
            })
            .unwrap_or(ScopedPolicyResult {
                allowed: !self.row_policy_mode.denies_missing_explicit_policy(),
                scope: WitnessScope::new(),
            });

        visited_referencing.remove(&(table, row.id, operation));
        result
    }

    fn policy_for_operation(
        &self,
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
    fn evaluate_inherits_referencing_with_witness_scope(
        &self,
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
    ) -> ScopedPolicyResult {
        let Some(effective_max_depth) = normalize_recursive_max_depth(max_depth) else {
            return ScopedPolicyResult::default();
        };
        if depth >= effective_max_depth {
            return ScopedPolicyResult::default();
        }

        let source_table_name = TableName::new(source_table);
        let Some(source_schema) = self.schema.get(&source_table_name) else {
            return ScopedPolicyResult::default();
        };
        let source_descriptor = &source_schema.columns;

        let Some(col_idx) = source_descriptor.column_index(via_column) else {
            return ScopedPolicyResult::default();
        };
        let col = &source_descriptor.columns[col_idx];
        if col.references != Some(TableName::new(target_table_name)) {
            return ScopedPolicyResult::default();
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
            _ => return ScopedPolicyResult::default(),
        };

        for source_row_id in candidate_ids {
            let Some(source_row) = row_loader(source_row_id, Some(source_table_name)) else {
                continue;
            };
            let source_scope = source_row.provenance.clone();

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
            let mut result = self.evaluate_row_access_with_witness_scope(
                operation,
                &source_row,
                source_descriptor,
                source_table_name.as_str(),
                None,
                io,
                row_loader,
                depth + 1,
                visited_referencing,
            );
            if result.allowed {
                result.scope.extend(source_scope);
                return result;
            }
        }

        ScopedPolicyResult::default()
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_expr_with_witness_scope(
        &self,
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
    ) -> ScopedPolicyResult {
        if depth > crate::query_manager::policy::RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
            return ScopedPolicyResult::default();
        }

        match expr {
            PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            } => self.evaluate_inherits_with_witness_scope(
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
            } => self.evaluate_inherits_referencing_with_witness_scope(
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
            PolicyExpr::Exists { table, condition } => self.evaluate_exists_with_witness_scope(
                table, condition, operation, row, descriptor, io, row_loader, depth,
            ),
            PolicyExpr::ExistsRel { rel } => self.evaluate_exists_rel_with_witness_scope(
                rel,
                operation == Operation::Select,
                row,
                descriptor,
                table_name,
                io,
                row_loader,
                depth,
            ),
            PolicyExpr::And(exprs) => {
                let mut scope = WitnessScope::new();
                for expr in exprs {
                    let result = self.evaluate_expr_with_witness_scope(
                        expr,
                        operation,
                        row,
                        descriptor,
                        table_name,
                        io,
                        row_loader,
                        depth,
                        visited,
                        visited_referencing,
                    );
                    if !result.allowed {
                        return ScopedPolicyResult::default();
                    }
                    scope.extend(result.scope);
                }
                ScopedPolicyResult {
                    allowed: true,
                    scope,
                }
            }
            PolicyExpr::Or(exprs) => {
                for expr in exprs {
                    let result = self.evaluate_expr_with_witness_scope(
                        expr,
                        operation,
                        row,
                        descriptor,
                        table_name,
                        io,
                        row_loader,
                        depth,
                        visited,
                        visited_referencing,
                    );
                    if result.allowed {
                        return result;
                    }
                }
                ScopedPolicyResult::default()
            }
            PolicyExpr::Not(inner) => {
                let result = self.evaluate_expr_with_witness_scope(
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
                );
                ScopedPolicyResult {
                    allowed: !result.allowed,
                    scope: WitnessScope::new(),
                }
            }
            _ => ScopedPolicyResult {
                allowed: evaluate_expr_recursive_with_row_id(
                    expr,
                    &row.data,
                    &row.provenance,
                    descriptor,
                    self.session,
                    Some(row.id),
                    depth,
                ),
                scope: WitnessScope::new(),
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_inherits_with_witness_scope(
        &self,
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
    ) -> ScopedPolicyResult {
        let Some(effective_max_depth) = normalize_recursive_max_depth(max_depth) else {
            return ScopedPolicyResult::default();
        };
        if depth >= effective_max_depth {
            return ScopedPolicyResult::default();
        }

        let col_index = match descriptor.column_index(via_column) {
            Some(idx) => idx,
            None => return ScopedPolicyResult::default(),
        };

        if column_is_null(descriptor, &row.data, col_index).unwrap_or(false) {
            return ScopedPolicyResult {
                allowed: true,
                scope: WitnessScope::new(),
            };
        }

        let col_desc = &descriptor.columns[col_index];
        let parent_table = match &col_desc.references {
            Some(table) => table,
            None => return ScopedPolicyResult::default(),
        };

        let parent_id = match decode_column(descriptor, &row.data, col_index) {
            Ok(Value::Uuid(id)) => id,
            _ => return ScopedPolicyResult::default(),
        };

        if visited.contains(&parent_id) {
            return ScopedPolicyResult::default();
        }
        visited.insert(parent_id);

        let parent_table_name = *parent_table;
        let parent_row = match row_loader(parent_id, Some(parent_table_name)) {
            Some(content) => content,
            None => return ScopedPolicyResult::default(),
        };
        let parent_scope = parent_row.provenance.clone();

        let parent_schema = match self.schema.get(&parent_table_name) {
            Some(schema) => schema,
            None => return ScopedPolicyResult::default(),
        };

        let parent_policy = match operation {
            Operation::Select => parent_schema.policies.select_policy(),
            Operation::Insert => parent_schema.policies.insert_policy(),
            Operation::Update => parent_schema.policies.update_using_policy(),
            Operation::Delete => parent_schema.policies.effective_delete_using(),
        };

        let parent_policy = match parent_policy {
            Some(p) => p,
            None => return ScopedPolicyResult::default(),
        };

        let parent_row = Row::new(
            parent_id,
            parent_row.data,
            parent_row.batch_id,
            parent_row.row_provenance,
        );
        let mut result = self.evaluate_expr_with_witness_scope(
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
        if result.allowed {
            result.scope.extend(parent_scope);
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_exists_with_witness_scope(
        &self,
        table: &str,
        condition: &PolicyExpr,
        operation: Operation,
        row: &Row,
        descriptor: &RowDescriptor,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
        depth: usize,
    ) -> ScopedPolicyResult {
        if depth >= crate::query_manager::policy::RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
            return ScopedPolicyResult::default();
        }

        let bound_condition =
            match bind_outer_row_refs(condition, &row.data, descriptor, Some(row.id)) {
                Some(expr) => expr,
                None => return ScopedPolicyResult::default(),
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
            None => return ScopedPolicyResult::default(),
        };

        for _ in 0..100 {
            if graph.settle(io, row_loader) {
                break;
            }
        }

        ScopedPolicyResult {
            allowed: graph.result(),
            scope: graph.matching_scope_object_ids(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_exists_rel_with_witness_scope(
        &self,
        rel: &RelExpr,
        structural_scans: bool,
        row: &Row,
        descriptor: &RowDescriptor,
        table_name: &str,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
        depth: usize,
    ) -> ScopedPolicyResult {
        if depth >= crate::query_manager::policy::RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
            return ScopedPolicyResult::default();
        }

        let bound_rel =
            match bind_relation_refs(rel, &row.data, descriptor, self.session, Some(row.id)) {
                Some(expr) => expr,
                None => return ScopedPolicyResult::default(),
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
            None => return ScopedPolicyResult::default(),
        };

        for _ in 0..100 {
            if graph.settle(io, row_loader) {
                break;
            }
        }

        ScopedPolicyResult {
            allowed: graph.result(),
            scope: graph.matching_scope_object_ids(),
        }
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
