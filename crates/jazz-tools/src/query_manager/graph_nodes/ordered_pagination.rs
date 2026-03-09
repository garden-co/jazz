use ahash::AHashSet;
use std::ops::Bound;

use crate::object::BranchName;
use crate::query_manager::graph_nodes::policy_filter::PolicyFilterNode;
use crate::query_manager::policy::PolicyExpr;
use crate::query_manager::session::Session;
use crate::query_manager::types::{
    ColumnName, LoadedRow, RowDescriptor, Schema, TableName, Tuple, TupleDelta, TupleElement, Value,
};
use crate::storage::{IndexScanDirection, OrderedIndexScan};

use super::{SourceContext, SourceNode, tuple_delta::compute_tuple_delta};

#[derive(Debug, Clone)]
struct OrderedPaginationPolicy {
    expr: PolicyExpr,
    session: Session,
    schema: Schema,
    dependency_tables: Vec<TableName>,
}

#[derive(Debug)]
pub(crate) struct OrderedPaginationNodeConfig {
    pub table: TableName,
    pub column: ColumnName,
    pub branch: String,
    pub start: Bound<Value>,
    pub end: Bound<Value>,
    pub direction: IndexScanDirection,
    pub limit: Option<usize>,
    pub offset: usize,
    pub descriptor: RowDescriptor,
}

/// Source node that reads one index directly in order and applies offset/limit.
#[derive(Debug)]
pub struct OrderedPaginationNode {
    table: TableName,
    column: ColumnName,
    branch: String,
    start: Bound<Value>,
    end: Bound<Value>,
    direction: IndexScanDirection,
    limit: Option<usize>,
    offset: usize,
    descriptor: RowDescriptor,
    policy: Option<OrderedPaginationPolicy>,
    ordered_prefix_tuples: Vec<Tuple>,
    windowed_tuples: Vec<Tuple>,
    current_tuples: AHashSet<Tuple>,
    dirty: bool,
}

impl OrderedPaginationNode {
    pub(crate) fn new(config: OrderedPaginationNodeConfig) -> Self {
        let OrderedPaginationNodeConfig {
            table,
            column,
            branch,
            start,
            end,
            direction,
            limit,
            offset,
            descriptor,
        } = config;
        Self {
            table,
            column,
            branch,
            start,
            end,
            direction,
            limit,
            offset,
            descriptor,
            policy: None,
            ordered_prefix_tuples: Vec::new(),
            windowed_tuples: Vec::new(),
            current_tuples: AHashSet::new(),
            dirty: true,
        }
    }

    pub fn with_policy(mut self, expr: PolicyExpr, session: Session, schema: Schema) -> Self {
        let filter = PolicyFilterNode::new_with_branch(
            self.descriptor.clone(),
            expr.clone(),
            session.clone(),
            schema.clone(),
            self.table.as_str(),
            &self.branch,
        );
        let dependency_tables = filter
            .inherits_tables()
            .iter()
            .map(TableName::new)
            .collect();
        self.policy = Some(OrderedPaginationPolicy {
            expr,
            session,
            schema,
            dependency_tables,
        });
        self
    }

    pub fn windowed_tuples(&self) -> &[Tuple] {
        &self.windowed_tuples
    }

    pub fn sync_input_tuples(&self) -> &[Tuple] {
        &self.ordered_prefix_tuples
    }

    fn take_count(&self) -> Option<usize> {
        self.limit.map(|limit| self.offset.saturating_add(limit))
    }

    pub fn policy_dependency_tables(&self) -> &[TableName] {
        self.policy
            .as_ref()
            .map(|policy| policy.dependency_tables.as_slice())
            .unwrap_or(&[])
    }

    pub fn has_policy(&self) -> bool {
        self.policy.is_some()
    }

    fn sync_scope_provenance(&self) -> crate::query_manager::types::TupleProvenance {
        self.sync_input_tuples()
            .iter()
            .flat_map(|tuple| tuple.provenance().iter().copied())
            .collect()
    }

    fn rebuild_window(&mut self, visible_prefix: Vec<Tuple>) -> TupleDelta {
        let old_window = std::mem::take(&mut self.windowed_tuples);
        self.ordered_prefix_tuples = visible_prefix;

        let start = self.offset.min(self.ordered_prefix_tuples.len());
        let end = match self.limit {
            Some(limit) => start
                .saturating_add(limit)
                .min(self.ordered_prefix_tuples.len()),
            None => self.ordered_prefix_tuples.len(),
        };
        let sync_scope = self.sync_scope_provenance();
        self.windowed_tuples = self.ordered_prefix_tuples[start..end]
            .iter()
            .cloned()
            .map(|mut tuple| {
                tuple.merge_provenance(&sync_scope);
                tuple
            })
            .collect();
        self.current_tuples = self.windowed_tuples.iter().cloned().collect();
        self.dirty = false;

        compute_tuple_delta(&old_window, &self.windowed_tuples)
    }

    fn scan_without_policy(&mut self, ctx: &SourceContext) -> TupleDelta {
        let branch = BranchName::new(&self.branch);
        let ordered_ids = ctx.storage.index_scan_ordered(OrderedIndexScan {
            table: self.table.as_str(),
            column: self.column.as_str(),
            branch: &self.branch,
            start: self.start.as_ref(),
            end: self.end.as_ref(),
            direction: self.direction,
            take: self.take_count(),
        });

        self.rebuild_window(
            ordered_ids
                .into_iter()
                .map(|id| Tuple::from_scoped_id(id, branch))
                .collect(),
        )
    }

    pub fn scan_with_context<F>(&mut self, ctx: &SourceContext, row_loader: &mut F) -> TupleDelta
    where
        F: FnMut(crate::object::ObjectId) -> Option<LoadedRow>,
    {
        let Some(policy) = &self.policy else {
            return self.scan_without_policy(ctx);
        };

        let desired_visible = self.take_count();
        let mut take = desired_visible;

        loop {
            let ordered_ids = ctx.storage.index_scan_ordered(OrderedIndexScan {
                table: self.table.as_str(),
                column: self.column.as_str(),
                branch: &self.branch,
                start: self.start.as_ref(),
                end: self.end.as_ref(),
                direction: self.direction,
                take,
            });

            let mut policy_filter = PolicyFilterNode::new_with_branch(
                self.descriptor.clone(),
                policy.expr.clone(),
                policy.session.clone(),
                policy.schema.clone(),
                self.table.as_str(),
                &self.branch,
            );
            let mut visible_prefix = Vec::new();

            for row_id in &ordered_ids {
                let Some(loaded) = row_loader(*row_id) else {
                    continue;
                };
                let tuple = Tuple::new_with_provenance(
                    vec![TupleElement::Row {
                        id: *row_id,
                        content: loaded.data,
                        commit_id: loaded.commit_id,
                    }],
                    loaded.provenance,
                );
                let delta = policy_filter.process_with_context(
                    TupleDelta {
                        added: vec![tuple.clone()],
                        removed: vec![],
                        moved: vec![],
                        updated: vec![],
                    },
                    ctx.storage,
                    &mut *row_loader,
                );
                if !delta.added.is_empty() {
                    visible_prefix.push(tuple);
                }
            }

            let reached_end = take.is_none() || take.is_some_and(|limit| ordered_ids.len() < limit);
            let enough_visible =
                desired_visible.is_some_and(|needed| visible_prefix.len() >= needed);
            if (desired_visible.is_some() && (enough_visible || reached_end))
                || (desired_visible.is_none() && reached_end)
            {
                return self.rebuild_window(visible_prefix);
            }

            take = take.map(|limit| limit.saturating_mul(2).max(1));
        }
    }
}

impl SourceNode for OrderedPaginationNode {
    fn scan(&mut self, ctx: &SourceContext) -> TupleDelta {
        self.scan_without_policy(ctx)
    }

    fn current_tuples(&self) -> &AHashSet<Tuple> {
        &self.current_tuples
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }
}
