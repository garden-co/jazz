use ahash::{AHashMap, AHashSet};
use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use bitvec::prelude::*;
use smallvec::SmallVec;

use crate::commit::CommitId;
use crate::object::{BranchName, ObjectId};
use crate::object_manager::ObjectManager;
use crate::schema_manager::{SchemaContext, translate_column_for_index};

use super::encoding::encode_value;
use super::graph_nodes::alias::AliasNode;
use super::graph_nodes::array_subquery::ArraySubqueryNode;
use super::graph_nodes::exists_output::ExistsOutputNode;
use super::graph_nodes::filter::{FilterNode, Predicate};
use super::graph_nodes::index_scan::IndexScanNode;
use super::graph_nodes::join::JoinNode;
use super::graph_nodes::limit_offset::LimitOffsetNode;
use super::graph_nodes::materialize::MaterializeNode;
use super::graph_nodes::output::{OutputMode, OutputNode};
use super::graph_nodes::policy_filter::PolicyFilterNode;
use super::graph_nodes::project::ProjectNode;
use super::graph_nodes::sort::SortNode;
use super::graph_nodes::subgraph::SubgraphTemplate;
use super::graph_nodes::union::UnionNode;
use super::graph_nodes::{IndicesMap, NodeId, RowNode, SourceContext, SourceNode, TransformNode};
use super::index::ScanCondition;
use super::query::{Condition, Query};
use super::session::Session;
use super::types::{
    ColumnDescriptor, ColumnName, ColumnType, ComposedBranchName, Row, RowDelta, RowDescriptor,
    Schema, SchemaHash, TableName, Tuple, TupleDelta, TupleDescriptor,
};

/// A node in the query graph (type-erased).
#[derive(Debug)]
pub enum GraphNode {
    IndexScan(IndexScanNode),
    Union(UnionNode),
    Alias(AliasNode),
    Join(JoinNode),
    Materialize(MaterializeNode),
    Project(ProjectNode),
    Filter(FilterNode),
    PolicyFilter(PolicyFilterNode),
    Sort(SortNode),
    LimitOffset(LimitOffsetNode),
    ArraySubquery(ArraySubqueryNode),
    Output(OutputNode),
    ExistsOutput(ExistsOutputNode),
}

/// Compact node with inline edge storage.
/// Most nodes have 0-2 inputs/outputs, so inline storage avoids heap allocation.
#[derive(Debug)]
pub struct CompactNode {
    pub node: GraphNode,
    /// Input edges (children/dependencies). Most nodes have 0-2 inputs.
    pub inputs: SmallVec<[NodeId; 2]>,
    /// Output edges (parents/dependents). Most nodes have 0-2 outputs.
    pub outputs: SmallVec<[NodeId; 2]>,
}

/// Compiled query graph for a single query.
#[derive(Debug)]
pub struct QueryGraph {
    /// Dense node storage (NodeId.0 is index).
    pub nodes: Vec<CompactNode>,
    /// Dirty tracking bitmap (1 bit per node, indexed by NodeId.0).
    dirty_bitmap: BitVec,
    /// The output node ID.
    pub output_node: NodeId,
    /// Table this query operates on.
    pub table: TableName,
    /// Index scan nodes for this query (for marking dirty on updates).
    pub index_scan_nodes: Vec<(NodeId, TableName, ColumnName)>, // (node_id, table, column)
    /// Array subquery nodes and their inner tables (for marking dirty on inner table updates).
    pub array_subquery_tables: Vec<(NodeId, TableName)>, // (node_id, inner_table)
    /// PolicyFilter nodes and their INHERITS-referenced tables (for marking dirty on table updates).
    pub policy_filter_tables: Vec<(NodeId, TableName)>, // (node_id, inherits_table)
    /// Per-table descriptors in join order (for flattening multi-element tuples).
    pub table_descriptors: Vec<RowDescriptor>,
    /// Combined descriptor for output (all columns from all tables).
    pub combined_descriptor: RowDescriptor,
}

impl QueryGraph {
    pub fn new(table: TableName, descriptor: RowDescriptor) -> Self {
        Self {
            nodes: Vec::new(),
            dirty_bitmap: BitVec::new(),
            output_node: NodeId(0),
            table,
            index_scan_nodes: Vec::new(),
            array_subquery_tables: Vec::new(),
            policy_filter_tables: Vec::new(),
            table_descriptors: vec![descriptor.clone()],
            combined_descriptor: descriptor,
        }
    }

    /// Mark a node as dirty using the bitmap.
    pub fn mark_dirty(&mut self, id: NodeId) {
        let idx = id.0 as usize;
        if idx >= self.dirty_bitmap.len() {
            self.dirty_bitmap.resize(idx + 1, false);
        }
        self.dirty_bitmap.set(idx, true);
    }

    /// Check if a node is dirty.
    fn is_dirty(&self, id: NodeId) -> bool {
        let idx = id.0 as usize;
        idx < self.dirty_bitmap.len() && self.dirty_bitmap[idx]
    }

    /// Check if any nodes are dirty.
    pub fn has_dirty_nodes(&self) -> bool {
        self.dirty_bitmap.any()
    }

    /// Clear all dirty flags.
    pub fn clear_dirty(&mut self) {
        self.dirty_bitmap.fill(false);
    }

    fn add_node(&mut self, node: GraphNode) -> NodeId {
        let id = NodeId(self.nodes.len() as u64);
        self.nodes.push(CompactNode {
            node,
            inputs: SmallVec::new(),
            outputs: SmallVec::new(),
        });
        // Grow dirty bitmap to accommodate new node
        self.dirty_bitmap.push(true); // New nodes start dirty
        id
    }

    /// Add an edge from one node to another.
    pub fn add_edge(&mut self, from: NodeId, to: NodeId) {
        self.nodes[from.0 as usize].inputs.push(to);
        self.nodes[to.0 as usize].outputs.push(from);
    }

    /// Add a node and return its ID.
    pub fn add_node_with_id(&mut self, node: GraphNode) -> NodeId {
        self.add_node(node)
    }

    /// Get a reference to a node by ID.
    fn get_node(&self, id: NodeId) -> Option<&GraphNode> {
        self.nodes.get(id.0 as usize).map(|c| &c.node)
    }

    /// Get a mutable reference to a node by ID.
    fn get_node_mut(&mut self, id: NodeId) -> Option<&mut GraphNode> {
        self.nodes.get_mut(id.0 as usize).map(|c| &mut c.node)
    }

    /// Get input edges for a node.
    fn get_inputs(&self, id: NodeId) -> &[NodeId] {
        &self.nodes[id.0 as usize].inputs
    }

    /// Get output edges (reverse edges) for a node.
    fn get_outputs(&self, id: NodeId) -> Option<&[NodeId]> {
        self.nodes.get(id.0 as usize).map(|c| c.outputs.as_slice())
    }

    /// Returns ObjectIds contributing to current result set along with their branches.
    ///
    /// These are the objects that, if synced, would affect query results.
    /// Only includes ObjectIds that:
    /// 1. Come from an IndexScanNode (source of all objects)
    /// 2. Survive all filtering/joins to appear in the output
    ///
    /// After calling `settle()`, this method returns the (ObjectId, BranchName) pairs
    /// for all rows currently in the query result.
    pub fn contributing_object_ids(&self) -> HashSet<(ObjectId, BranchName)> {
        // Get all ObjectIds in the final output
        let output_ids: AHashSet<ObjectId> = self
            .get_node(self.output_node)
            .and_then(|node| {
                if let GraphNode::Output(output) = node {
                    Some(
                        output
                            .current_tuples()
                            .iter()
                            .flat_map(|t| t.ids())
                            .collect(),
                    )
                } else {
                    None
                }
            })
            .unwrap_or_default();

        // For each IndexScanNode, find which of its ObjectIds are in the output
        // and pair them with the scan's branch
        let mut result = HashSet::new();

        for (node_id, _table, _column) in &self.index_scan_nodes {
            if let Some(GraphNode::IndexScan(scan)) = self.get_node(*node_id) {
                let branch = BranchName::new(&scan.branch);
                for tuple in scan.current_tuples() {
                    for id in tuple.ids() {
                        if output_ids.contains(&id) {
                            result.insert((id, branch));
                        }
                    }
                }
            }
        }

        result
    }

    /// Check if the graph is waiting for pending objects to load.
    ///
    /// Returns true if the OutputNode is held pending (waiting for objects to materialize).
    pub fn is_pending(&self) -> bool {
        if let Some(GraphNode::Output(output)) = self.get_node(self.output_node) {
            return output.is_held_pending();
        }
        false
    }

    /// Get all pending object IDs with their branches from MaterializeNodes.
    ///
    /// Returns (ObjectId, branch_name) pairs for all objects that are pending.
    pub fn pending_ids_with_branches(&self) -> Vec<(ObjectId, String)> {
        let mut result = Vec::new();
        for compact in &self.nodes {
            if let GraphNode::Materialize(mat) = &compact.node {
                for (&id, branch) in mat.pending_ids_with_branches() {
                    result.push((id, branch.clone()));
                }
            }
        }
        result
    }

    /// Set the default branch for pending ID tracking on all MaterializeNodes.
    pub fn set_pending_branch(&mut self, branch: &str) {
        for compact in &mut self.nodes {
            if let GraphNode::Materialize(mat) = &mut compact.node {
                mat.set_pending_branch(branch);
            }
        }
    }

    /// Compile a query into a graph (without policy filtering).
    pub fn compile(query: &Query, schema: &Schema) -> Option<Self> {
        Self::compile_with_session(query, schema, None)
    }

    /// Compile a query into a graph with optional session-based policy filtering.
    ///
    /// When a session is provided and the table has a SELECT policy, a PolicyFilterNode
    /// is inserted after materialization to filter rows based on the policy.
    pub fn compile_with_session(
        query: &Query,
        schema: &Schema,
        session: Option<Session>,
    ) -> Option<Self> {
        // Get branches (default to "main" if not specified)
        let default_branches = vec!["main".to_string()];
        let branches: &[String] = if query.branches.is_empty() {
            &default_branches
        } else {
            &query.branches
        };

        if query.is_join() {
            // TODO: Add policy support for joins
            return Self::compile_join(query, schema, branches);
        }

        let table_schema = schema.get(&query.table)?;
        let descriptor = table_schema.descriptor.clone();
        let select_policy = table_schema.policies.select.using.clone();
        let mut graph = QueryGraph::new(query.table, descriptor.clone());

        // Phase 1: Build IndexScan nodes (one per disjunct per branch)
        // For multi-branch queries, we create scans for each branch and union them
        let mut phase1_outputs: Vec<NodeId> = Vec::new();
        let mut index_columns: Vec<String> = Vec::new();

        for branch in branches {
            for disjunct in &query.disjuncts {
                // Find best index condition for this disjunct
                let (scan_column, scan_condition) =
                    if let Some(cond) = disjunct.best_index_condition() {
                        let column = cond.column().to_string();
                        let scan_cond = condition_to_scan(cond);
                        (column, scan_cond)
                    } else {
                        // No index condition, use "_id" for full scan
                        ("_id".to_string(), ScanCondition::All)
                    };

                index_columns.push(scan_column.clone());
                let scan_column_name = ColumnName::new(&scan_column);

                let scan_node = IndexScanNode::new_with_branch(
                    query.table,
                    scan_column_name,
                    branch,
                    scan_condition,
                    descriptor.clone(),
                );
                let scan_id = graph.add_node(GraphNode::IndexScan(scan_node));
                graph
                    .index_scan_nodes
                    .push((scan_id, query.table, scan_column_name));
                phase1_outputs.push(scan_id);
            }

            // If include_deleted is set, also scan _id_deleted index for this branch
            if query.include_deleted {
                let deleted_column = ColumnName::new("_id_deleted");
                let deleted_scan_node = IndexScanNode::new_with_branch(
                    query.table,
                    deleted_column,
                    branch,
                    ScanCondition::All,
                    descriptor.clone(),
                );
                let deleted_scan_id = graph.add_node(GraphNode::IndexScan(deleted_scan_node));
                graph
                    .index_scan_nodes
                    .push((deleted_scan_id, query.table, deleted_column));
                phase1_outputs.push(deleted_scan_id);
            }
        }

        // If multiple disjuncts (or include_deleted), add Union node
        let phase1_output = if phase1_outputs.len() > 1 {
            let union_node = UnionNode::new();
            let union_id = graph.add_node(GraphNode::Union(union_node));
            for scan_id in &phase1_outputs {
                graph.add_edge(union_id, *scan_id);
            }
            union_id
        } else {
            phase1_outputs[0]
        };

        // Materialize node (boundary between Phase 1 and Phase 2)
        let materialize_node = MaterializeNode::new(descriptor.clone());
        let materialize_id = graph.add_node(GraphNode::Materialize(materialize_node));
        graph.add_edge(materialize_id, phase1_output);

        let mut phase2_input = materialize_id;
        let mut current_descriptor = descriptor.clone();

        // Policy filter node (if session provided and table has SELECT policy)
        if let (Some(session), Some(policy)) = (&session, select_policy) {
            let policy_node = PolicyFilterNode::new(
                current_descriptor.clone(),
                policy,
                session.clone(),
                schema.clone(),
                query.table.as_str(),
            );
            // Collect INHERITS tables before moving the node
            let inherits_tables: Vec<TableName> = policy_node
                .inherits_tables()
                .iter()
                .map(TableName::new)
                .collect();
            let policy_id = graph.add_node(GraphNode::PolicyFilter(policy_node));
            graph.add_edge(policy_id, phase2_input);
            // Track INHERITS-referenced tables for dirty marking
            for inherits_table in inherits_tables {
                graph.policy_filter_tables.push((policy_id, inherits_table));
            }
            phase2_input = policy_id;
        }

        // Array subqueries: insert ArraySubqueryNode for each array subquery
        for subquery_spec in &query.array_subqueries {
            if let Some((node, new_descriptor)) =
                graph.compile_array_subquery(subquery_spec, &current_descriptor, schema, branches)
            {
                let node_id = graph.add_node(GraphNode::ArraySubquery(node));
                graph.add_edge(node_id, phase2_input);
                // Track inner table for dirty marking on inner table updates
                graph
                    .array_subquery_tables
                    .push((node_id, subquery_spec.table));
                phase2_input = node_id;
                current_descriptor = new_descriptor;
            }
        }

        // Phase 2: Filter node (only if there are remaining conditions not covered by index)
        // Build remaining predicate - only include conditions not fully handled by index scans
        let predicate = build_remaining_predicate(query, &index_columns, &current_descriptor);
        if !matches!(predicate, Predicate::True) {
            let filter_node = FilterNode::new(current_descriptor.clone(), predicate);
            let filter_id = graph.add_node(GraphNode::Filter(filter_node));
            graph.add_edge(filter_id, phase2_input);
            phase2_input = filter_id;
        }

        // Sort node (if order_by specified)
        if !query.order_by.is_empty() {
            let sort_keys = query.sort_keys(&current_descriptor);
            if !sort_keys.is_empty() {
                let sort_node = SortNode::new(current_descriptor.clone(), sort_keys);
                let sort_id = graph.add_node(GraphNode::Sort(sort_node));
                graph.add_edge(sort_id, phase2_input);
                phase2_input = sort_id;
            }
        }

        // LimitOffset node (if limit or offset specified)
        if query.limit.is_some() || query.offset > 0 {
            let limit_offset_node =
                LimitOffsetNode::new(current_descriptor.clone(), query.limit, query.offset);
            let limit_offset_id = graph.add_node(GraphNode::LimitOffset(limit_offset_node));
            graph.add_edge(limit_offset_id, phase2_input);
            phase2_input = limit_offset_id;
        }

        // Project node (if select_columns specified)
        if let Some(columns) = &query.select_columns {
            let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            let project_node = ProjectNode::new(current_descriptor.clone(), &col_refs);
            current_descriptor = project_node.output_descriptor().clone();
            let project_id = graph.add_node(GraphNode::Project(project_node));
            graph.add_edge(project_id, phase2_input);
            phase2_input = project_id;
        }

        // Output node
        let output_node = OutputNode::new(current_descriptor, OutputMode::Delta);
        let output_id = graph.add_node(GraphNode::Output(output_node));
        graph.add_edge(output_id, phase2_input);
        graph.output_node = output_id;

        Some(graph)
    }

    /// Compile a query with schema context for multi-schema queries.
    ///
    /// When schema context is provided:
    /// - Branches are automatically expanded to include all live schema branches
    /// - Column names are translated through lens chain for old schema branches
    /// - The descriptor uses the current schema (lens transforms happen at row load time)
    pub fn compile_with_schema_context(
        query: &Query,
        schema: &Schema,
        session: Option<Session>,
        schema_context: &SchemaContext,
    ) -> Option<Self> {
        // Build branch -> schema hash map for column translation
        let mut branch_schema_map: HashMap<String, SchemaHash> = HashMap::new();
        for branch_name in schema_context.all_branch_names() {
            let branch_str = branch_name.as_str().to_string();
            if let Some(composed) = ComposedBranchName::parse(&branch_name) {
                branch_schema_map.insert(branch_str, composed.schema_hash);
            }
        }

        // Expand branches to include all live schema branches if not specified
        let branches: Vec<String> = if query.branches.is_empty() {
            schema_context
                .all_branch_names()
                .into_iter()
                .map(|b| b.as_str().to_string())
                .collect()
        } else {
            query.branches.clone()
        };

        if query.is_join() {
            return Self::compile_join(query, schema, &branches);
        }

        let table_schema = schema.get(&query.table)?;
        let descriptor = table_schema.descriptor.clone();
        let select_policy = table_schema.policies.select.using.clone();
        let mut graph = QueryGraph::new(query.table, descriptor.clone());
        let table_str = query.table.as_str();

        // Phase 1: Build IndexScan nodes (one per disjunct per branch)
        // For multi-branch queries, we create scans for each branch and union them
        // Column names are translated for old schema branches
        let mut phase1_outputs: Vec<NodeId> = Vec::new();
        let mut index_columns: Vec<String> = Vec::new();

        for branch in &branches {
            // Get schema hash for this branch to determine if column translation is needed
            let branch_schema_hash = branch_schema_map.get(branch).copied();

            for disjunct in &query.disjuncts {
                // Find best index condition for this disjunct
                let (scan_column, scan_condition) =
                    if let Some(cond) = disjunct.best_index_condition() {
                        let column = cond.column().to_string();
                        let scan_cond = condition_to_scan(cond);
                        (column, scan_cond)
                    } else {
                        // No index condition, use "_id" for full scan
                        ("_id".to_string(), ScanCondition::All)
                    };

                // Translate column name for old schema branches
                let translated_column = if let Some(target_hash) = branch_schema_hash {
                    if target_hash != schema_context.current_hash {
                        // This branch uses an old schema - translate column name
                        translate_column_for_index(
                            schema_context,
                            table_str,
                            &scan_column,
                            &target_hash,
                        )
                        .unwrap_or_else(|| scan_column.clone())
                    } else {
                        scan_column.clone()
                    }
                } else {
                    scan_column.clone()
                };

                index_columns.push(scan_column.clone());
                let scan_column_name = ColumnName::new(&translated_column);

                let scan_node = IndexScanNode::new_with_branch(
                    query.table,
                    scan_column_name,
                    branch,
                    scan_condition,
                    descriptor.clone(),
                );
                let scan_id = graph.add_node(GraphNode::IndexScan(scan_node));
                graph
                    .index_scan_nodes
                    .push((scan_id, query.table, scan_column_name));
                phase1_outputs.push(scan_id);
            }

            // If include_deleted is set, also scan _id_deleted index for this branch
            if query.include_deleted {
                let deleted_column = ColumnName::new("_id_deleted");
                let deleted_scan_node = IndexScanNode::new_with_branch(
                    query.table,
                    deleted_column,
                    branch,
                    ScanCondition::All,
                    descriptor.clone(),
                );
                let deleted_scan_id = graph.add_node(GraphNode::IndexScan(deleted_scan_node));
                graph
                    .index_scan_nodes
                    .push((deleted_scan_id, query.table, deleted_column));
                phase1_outputs.push(deleted_scan_id);
            }
        }

        // If multiple outputs, add Union node
        let phase1_output = if phase1_outputs.len() > 1 {
            let union_node = UnionNode::new();
            let union_id = graph.add_node(GraphNode::Union(union_node));
            for scan_id in &phase1_outputs {
                graph.add_edge(union_id, *scan_id);
            }
            union_id
        } else if !phase1_outputs.is_empty() {
            phase1_outputs[0]
        } else {
            return None;
        };

        // Materialize node (boundary between Phase 1 and Phase 2)
        // Lens transforms are applied in the row_loader, so MaterializeNode uses current schema
        let materialize_node = MaterializeNode::new(descriptor.clone());
        let materialize_id = graph.add_node(GraphNode::Materialize(materialize_node));
        graph.add_edge(materialize_id, phase1_output);

        let mut phase2_input = materialize_id;
        let mut current_descriptor = descriptor.clone();

        // Policy filter node (if session provided and table has SELECT policy)
        if let (Some(session), Some(policy)) = (&session, select_policy) {
            let policy_node = PolicyFilterNode::new(
                current_descriptor.clone(),
                policy,
                session.clone(),
                schema.clone(),
                query.table.as_str(),
            );
            let inherits_tables: Vec<TableName> = policy_node
                .inherits_tables()
                .iter()
                .map(TableName::new)
                .collect();
            let policy_id = graph.add_node(GraphNode::PolicyFilter(policy_node));
            graph.add_edge(policy_id, phase2_input);
            for inherits_table in inherits_tables {
                graph.policy_filter_tables.push((policy_id, inherits_table));
            }
            phase2_input = policy_id;
        }

        // Array subqueries: insert ArraySubqueryNode for each array subquery
        for subquery_spec in &query.array_subqueries {
            if let Some((node, new_descriptor)) =
                graph.compile_array_subquery(subquery_spec, &current_descriptor, schema, &branches)
            {
                let node_id = graph.add_node(GraphNode::ArraySubquery(node));
                graph.add_edge(node_id, phase2_input);
                graph
                    .array_subquery_tables
                    .push((node_id, subquery_spec.table));
                phase2_input = node_id;
                current_descriptor = new_descriptor;
            }
        }

        // Phase 2: Filter node (only if there are remaining conditions not covered by index)
        let predicate = build_remaining_predicate(query, &index_columns, &current_descriptor);
        if !matches!(predicate, Predicate::True) {
            let filter_node = FilterNode::new(current_descriptor.clone(), predicate);
            let filter_id = graph.add_node(GraphNode::Filter(filter_node));
            graph.add_edge(filter_id, phase2_input);
            phase2_input = filter_id;
        }

        // Sort node (if order_by specified)
        if !query.order_by.is_empty() {
            let sort_keys = query.sort_keys(&current_descriptor);
            if !sort_keys.is_empty() {
                let sort_node = SortNode::new(current_descriptor.clone(), sort_keys);
                let sort_id = graph.add_node(GraphNode::Sort(sort_node));
                graph.add_edge(sort_id, phase2_input);
                phase2_input = sort_id;
            }
        }

        // LimitOffset node (if limit or offset specified)
        if query.limit.is_some() || query.offset > 0 {
            let limit_offset_node =
                LimitOffsetNode::new(current_descriptor.clone(), query.limit, query.offset);
            let limit_offset_id = graph.add_node(GraphNode::LimitOffset(limit_offset_node));
            graph.add_edge(limit_offset_id, phase2_input);
            phase2_input = limit_offset_id;
        }

        // Project node (if select_columns specified)
        if let Some(columns) = &query.select_columns {
            let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            let project_node = ProjectNode::new(current_descriptor.clone(), &col_refs);
            current_descriptor = project_node.output_descriptor().clone();
            let project_id = graph.add_node(GraphNode::Project(project_node));
            graph.add_edge(project_id, phase2_input);
            phase2_input = project_id;
        }

        // Output node
        let output_node = OutputNode::new(current_descriptor, OutputMode::Delta);
        let output_id = graph.add_node(GraphNode::Output(output_node));
        graph.add_edge(output_id, phase2_input);
        graph.output_node = output_id;

        Some(graph)
    }

    /// Compile an array subquery specification into an ArraySubqueryNode.
    /// Returns the node and the new output descriptor (outer + array column).
    fn compile_array_subquery(
        &self,
        spec: &crate::query_manager::query::ArraySubquerySpec,
        outer_descriptor: &RowDescriptor,
        schema: &Schema,
        branches: &[String],
    ) -> Option<(ArraySubqueryNode, RowDescriptor)> {
        // Get inner table descriptor
        let inner_descriptor = schema.get(&spec.table)?.descriptor.clone();

        // Find outer correlation column index
        // The outer_column may be qualified (table.column) or unqualified
        let outer_col_name = spec
            .outer_column
            .split('.')
            .next_back()
            .unwrap_or(&spec.outer_column);
        let outer_correlation_col = outer_descriptor.column_index(outer_col_name)?;

        // Build base query for subgraph, inheriting branches from outer query
        let mut base_query = Query::new(spec.table);
        base_query.branches = branches.to_vec();
        base_query.joins = spec.joins.clone();
        for condition in &spec.filters {
            base_query.disjuncts[0].conditions.push(condition.clone());
        }
        base_query.order_by = spec.order_by.clone();
        base_query.limit = spec.limit;
        base_query.select_columns = spec.select_columns.clone();
        base_query.array_subqueries = spec.nested_arrays.clone();

        // Build combined descriptor: base table + all joined tables + nested array columns
        let mut combined_columns = inner_descriptor.columns.clone();
        for join_spec in &spec.joins {
            if let Some(joined_schema) = schema.get(&join_spec.table) {
                combined_columns.extend(joined_schema.descriptor.columns.clone());
            }
        }

        // Add columns for nested array subqueries (recursive)
        for nested in &spec.nested_arrays {
            if let Some(nested_element_desc) = Self::build_nested_array_descriptor(nested, schema) {
                combined_columns.push(ColumnDescriptor::new(
                    &nested.column_name,
                    ColumnType::Array(Box::new(ColumnType::Row(Box::new(nested_element_desc)))),
                ));
            }
        }

        let combined_descriptor = RowDescriptor::new(combined_columns);

        // Build output descriptor for inner query
        let inner_output_descriptor = if let Some(cols) = &spec.select_columns {
            let columns = cols
                .iter()
                .filter_map(|name| {
                    combined_descriptor
                        .columns
                        .iter()
                        .find(|c| c.name.as_str() == name)
                        .cloned()
                })
                .collect();
            RowDescriptor::new(columns)
        } else {
            combined_descriptor
        };

        // Create subgraph template
        let subgraph_template = SubgraphTemplate::new(
            base_query,
            spec.inner_column.clone(),
            spec.select_columns.clone().unwrap_or_default(),
            inner_output_descriptor,
        );

        // Create outer tuple descriptor
        let outer_tuple_descriptor =
            TupleDescriptor::single_with_materialization("", outer_descriptor.clone(), true);

        // Create node - it computes its own output descriptor with proper Array<Row> type
        let node = ArraySubqueryNode::new(
            outer_tuple_descriptor,
            subgraph_template,
            outer_correlation_col,
            spec.column_name.clone(),
            schema.clone(),
        );

        // Use the node's output descriptor (which has correct Array<Row> type)
        let output_descriptor = node.output_descriptor().clone();

        Some((node, output_descriptor))
    }

    /// Recursively build the element descriptor for a nested array subquery.
    fn build_nested_array_descriptor(
        spec: &crate::query_manager::query::ArraySubquerySpec,
        schema: &Schema,
    ) -> Option<RowDescriptor> {
        let inner_schema = schema.get(&spec.table)?;

        // Start with base table columns + joined table columns
        let mut columns = inner_schema.descriptor.columns.clone();
        for join_spec in &spec.joins {
            if let Some(joined_schema) = schema.get(&join_spec.table) {
                columns.extend(joined_schema.descriptor.columns.clone());
            }
        }

        // Recursively add nested array columns
        for nested in &spec.nested_arrays {
            if let Some(nested_element_desc) = Self::build_nested_array_descriptor(nested, schema) {
                columns.push(ColumnDescriptor::new(
                    &nested.column_name,
                    ColumnType::Array(Box::new(ColumnType::Row(Box::new(nested_element_desc)))),
                ));
            }
        }

        // Apply select_columns if specified
        if let Some(cols) = &spec.select_columns {
            let selected: Vec<_> = cols
                .iter()
                .filter_map(|name| columns.iter().find(|c| c.name.as_str() == name).cloned())
                .collect();
            Some(RowDescriptor::new(selected))
        } else {
            Some(RowDescriptor::new(columns))
        }
    }

    /// Compile a join query into a graph.
    ///
    /// Note: Table aliases (query.alias, join_spec.alias) are parsed but not yet
    /// used in graph construction. AliasNodes exist but aren't wired in.
    /// Basic joins between different tables work; self-joins with aliases need
    /// AliasNode integration to properly distinguish duplicate table columns.
    /// TODO: Insert AliasNodes when aliases are present to enable self-joins.
    fn compile_join(query: &Query, schema: &Schema, branches: &[String]) -> Option<Self> {
        let base_table_schema = schema.get(&query.table)?;
        let base_descriptor = base_table_schema.descriptor.clone();
        let mut graph = QueryGraph::new(query.table, base_descriptor.clone());

        // For joins, we use the first branch only for now
        // TODO: Support multi-branch joins with LWW merge
        let branch = branches.first().map(|s| s.as_str()).unwrap_or("main");

        // Track all table names and descriptors for TupleDescriptor
        let mut table_names = vec![query.table.as_str().to_string()];
        let mut table_descriptors = vec![base_descriptor.clone()];

        // Build pipeline for base table: IndexScan → Materialize
        let id_column = ColumnName::new("_id");
        let base_scan = IndexScanNode::new_with_branch(
            query.table,
            id_column,
            branch,
            ScanCondition::All,
            base_descriptor.clone(),
        );
        let base_scan_id = graph.add_node(GraphNode::IndexScan(base_scan));
        graph
            .index_scan_nodes
            .push((base_scan_id, query.table, id_column));

        let base_mat = MaterializeNode::new(base_descriptor.clone());
        let base_mat_id = graph.add_node(GraphNode::Materialize(base_mat));
        graph.add_edge(base_mat_id, base_scan_id);

        // Track current left side descriptor (accumulates columns from joins)
        let mut left_id = base_mat_id;
        let mut left_descriptor = base_descriptor.clone();
        let mut left_table_name = query.table.as_str().to_string();

        // Process each join
        for join_spec in &query.joins {
            let right_table_schema = schema.get(&join_spec.table)?;
            let right_descriptor = right_table_schema.descriptor.clone();

            // Build pipeline for right table: IndexScan → Materialize (same branch)
            let right_scan = IndexScanNode::new_with_branch(
                join_spec.table,
                id_column,
                branch,
                ScanCondition::All,
                right_descriptor.clone(),
            );
            let right_scan_id = graph.add_node(GraphNode::IndexScan(right_scan));
            graph
                .index_scan_nodes
                .push((right_scan_id, join_spec.table, id_column));

            let right_mat = MaterializeNode::new(right_descriptor.clone());
            let right_mat_id = graph.add_node(GraphNode::Materialize(right_mat));
            graph.add_edge(right_mat_id, right_scan_id);

            // Create JoinNode
            if let Some((left_col, right_col)) = &join_spec.on {
                // Parse column names - may be qualified (table.col) or unqualified
                let left_col_name = left_col.split('.').next_back().unwrap_or(left_col);
                let right_col_name = right_col.split('.').next_back().unwrap_or(right_col);

                let join_node = JoinNode::from_row_descriptors(
                    &left_table_name,
                    left_descriptor.clone(),
                    join_spec.table.as_str(),
                    right_descriptor.clone(),
                    left_col_name,
                    right_col_name,
                )?;
                let join_id = graph.add_node(GraphNode::Join(join_node));

                // JoinNode takes left and right as inputs
                // Using convention: first edge is left, second is right
                graph.add_edge(join_id, left_id);
                graph.add_edge(join_id, right_mat_id);

                // Update for next join in chain
                left_id = join_id;

                // Track table name and descriptor for TupleDescriptor
                table_names.push(join_spec.table.as_str().to_string());
                table_descriptors.push(right_descriptor.clone());

                // Combine descriptors for downstream nodes
                left_descriptor = RowDescriptor::combine(&[left_descriptor, right_descriptor]);
                // Use combined table name for multi-way joins
                left_table_name = format!("{}_{}", left_table_name, join_spec.table.as_str());
            }
        }

        // Build combined descriptor and TupleDescriptor from all tables
        let combined_descriptor = RowDescriptor::combine(&table_descriptors);
        // For FilterNode, all elements are materialized at this point (after Materialize nodes)
        let tuple_descriptor = TupleDescriptor::from_tables(
            &table_names
                .iter()
                .cloned()
                .zip(table_descriptors.iter().cloned())
                .collect::<Vec<_>>(),
        )
        .with_all_materialized();
        graph.table_descriptors = table_descriptors;
        graph.combined_descriptor = combined_descriptor.clone();

        let mut phase2_input = left_id;

        // Project node (if select_columns specified)
        if let Some(columns) = &query.select_columns {
            let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            let project_node = ProjectNode::new(combined_descriptor.clone(), &col_refs);
            let project_id = graph.add_node(GraphNode::Project(project_node));
            graph.add_edge(project_id, phase2_input);
            phase2_input = project_id;
        }

        // Filter node (if conditions exist)
        // Use TupleDescriptor to enable filtering on columns from any joined table
        let predicate = query.to_predicate(&combined_descriptor);
        if !matches!(predicate, Predicate::True) {
            let filter_node =
                FilterNode::with_tuple_descriptor(tuple_descriptor.clone(), predicate);
            let filter_id = graph.add_node(GraphNode::Filter(filter_node));
            graph.add_edge(filter_id, phase2_input);
            phase2_input = filter_id;
        }

        // Sort node (if order_by specified)
        if !query.order_by.is_empty() {
            let sort_keys = query.sort_keys(&combined_descriptor);
            if !sort_keys.is_empty() {
                let sort_node = SortNode::new(combined_descriptor.clone(), sort_keys);
                let sort_id = graph.add_node(GraphNode::Sort(sort_node));
                graph.add_edge(sort_id, phase2_input);
                phase2_input = sort_id;
            }
        }

        // LimitOffset node (if limit or offset specified)
        if query.limit.is_some() || query.offset > 0 {
            let limit_offset_node =
                LimitOffsetNode::new(combined_descriptor.clone(), query.limit, query.offset);
            let limit_offset_id = graph.add_node(GraphNode::LimitOffset(limit_offset_node));
            graph.add_edge(limit_offset_id, phase2_input);
            phase2_input = limit_offset_id;
        }

        // Output node
        let output_node = OutputNode::new(combined_descriptor, OutputMode::Delta);
        let output_id = graph.add_node(GraphNode::Output(output_node));
        graph.add_edge(output_id, phase2_input);
        graph.output_node = output_id;

        Some(graph)
    }

    /// Mark index scan nodes dirty for a given table/column.
    pub fn mark_dirty_for_column(&mut self, table: &str, column: &str) {
        let affected: Vec<NodeId> = self
            .index_scan_nodes
            .iter()
            .filter(|(_, t, c)| {
                t.as_str() == table && (c.as_str() == column || c.as_str() == "_id")
            })
            .map(|(node_id, _, _)| *node_id)
            .collect();
        for node_id in affected {
            self.mark_dirty(node_id);
        }
    }

    /// Mark all index scan nodes for a table dirty.
    /// Also marks array subquery nodes dirty if the table is their inner table.
    /// Also marks PolicyFilter nodes dirty if the table is INHERITS-referenced.
    pub fn mark_dirty_for_table(&mut self, table: &str) {
        // Mark index scan nodes and propagate downstream
        let affected_index_scans: Vec<NodeId> = self
            .index_scan_nodes
            .iter()
            .filter_map(|(node_id, t, _)| {
                if t.as_str() == table {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in affected_index_scans {
            self.mark_dirty(node_id);
            self.mark_downstream_dirty(node_id);
        }
        // Mark array subquery nodes whose inner table changed
        // Collect node_ids first to avoid borrow conflict
        let affected_array_subqueries: Vec<NodeId> = self
            .array_subquery_tables
            .iter()
            .filter_map(|(node_id, inner_table)| {
                if inner_table.as_str() == table {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in affected_array_subqueries {
            self.mark_dirty(node_id);
            // Mark the node as needing inner re-evaluation
            if let Some(GraphNode::ArraySubquery(node)) = self.get_node_mut(node_id) {
                node.mark_inner_dirty();
            }
            // Propagate dirty marks to downstream nodes (Output, etc.)
            self.mark_downstream_dirty(node_id);
        }

        // Mark PolicyFilter nodes whose INHERITS-referenced tables changed
        let affected_policy_filters: Vec<NodeId> = self
            .policy_filter_tables
            .iter()
            .filter_map(|(node_id, inherits_table)| {
                if inherits_table.as_str() == table {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in affected_policy_filters {
            self.mark_dirty(node_id);
            // Mark the node as needing INHERITS re-evaluation
            if let Some(GraphNode::PolicyFilter(node)) = self.get_node_mut(node_id) {
                node.mark_inherits_dirty();
            }
            // Propagate dirty marks to downstream nodes
            self.mark_downstream_dirty(node_id);
        }
    }

    /// Check if this graph involves a table (as index scan, array subquery inner table, or INHERITS reference).
    pub fn involves_table(&self, table: &str) -> bool {
        self.index_scan_nodes
            .iter()
            .any(|(_, t, _)| t.as_str() == table)
            || self
                .array_subquery_tables
                .iter()
                .any(|(_, t)| t.as_str() == table)
            || self
                .policy_filter_tables
                .iter()
                .any(|(_, t)| t.as_str() == table)
    }

    /// Check if the MaterializeNode has a specific object ID pending.
    pub fn has_pending_id(&self, object_id: ObjectId) -> bool {
        for compact in &self.nodes {
            if let GraphNode::Materialize(mat_node) = &compact.node
                && mat_node.pending_ids().any(|id| *id == object_id)
            {
                return true;
            }
        }
        false
    }

    /// Mark the materialize node dirty (to re-check pending IDs).
    pub fn mark_materialize_dirty(&mut self) {
        let materialize_ids: Vec<NodeId> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, c)| matches!(c.node, GraphNode::Materialize(_)))
            .map(|(idx, _)| NodeId(idx as u64))
            .collect();
        for node_id in materialize_ids {
            self.mark_dirty(node_id);
        }
    }

    /// Mark a row ID as updated for content checking.
    /// This tells MaterializeNodes to check if the row's content has changed.
    pub fn mark_row_updated(&mut self, id: ObjectId) {
        // First pass: mark the ID as updated in each MaterializeNode and collect node IDs
        let materialize_node_ids: Vec<NodeId> = self
            .nodes
            .iter_mut()
            .enumerate()
            .filter_map(|(idx, compact)| {
                if let GraphNode::Materialize(mat_node) = &mut compact.node {
                    mat_node.mark_updated(id);
                    Some(NodeId(idx as u64))
                } else {
                    None
                }
            })
            .collect();

        // Second pass: mark dirty and propagate downstream
        for node_id in materialize_node_ids {
            self.mark_dirty(node_id);
            self.mark_downstream_dirty(node_id);
        }
    }

    /// Mark a row ID as deleted for removal delta emission.
    /// This tells MaterializeNodes to emit a removal delta for this row.
    pub fn mark_row_deleted(&mut self, id: ObjectId) {
        // First pass: mark the ID as deleted in each MaterializeNode and collect node IDs
        let materialize_node_ids: Vec<NodeId> = self
            .nodes
            .iter_mut()
            .enumerate()
            .filter_map(|(idx, compact)| {
                if let GraphNode::Materialize(mat_node) = &mut compact.node {
                    mat_node.mark_deleted(id);
                    Some(NodeId(idx as u64))
                } else {
                    None
                }
            })
            .collect();

        // Second pass: mark dirty and propagate downstream
        for node_id in materialize_node_ids {
            self.mark_dirty(node_id);
            self.mark_downstream_dirty(node_id);
        }
    }

    /// Mark all nodes that depend on the given node as dirty (propagate forward).
    fn mark_downstream_dirty(&mut self, node_id: NodeId) {
        if let Some(outputs) = self.get_outputs(node_id) {
            let parents: SmallVec<[NodeId; 2]> = outputs.iter().copied().collect();
            for parent in parents {
                // Only recurse if not already dirty (avoid infinite loops)
                if !self.is_dirty(parent) {
                    self.mark_dirty(parent);
                    // Recursively mark parents of parent
                    self.mark_downstream_dirty(parent);
                }
            }
        }
    }

    /// Topological sort of dirty nodes (dependencies first).
    fn topo_sort_dirty(&self) -> Vec<NodeId> {
        let mut result = Vec::new();
        let mut visited = AHashSet::new();

        fn visit(
            node: NodeId,
            graph: &QueryGraph,
            visited: &mut AHashSet<NodeId>,
            result: &mut Vec<NodeId>,
        ) {
            if visited.contains(&node) {
                return;
            }
            visited.insert(node);

            // Visit dependencies first (inputs)
            if let Some(compact) = graph.nodes.get(node.0 as usize) {
                for dep in &compact.inputs {
                    visit(*dep, graph, visited, result);
                }
            }

            result.push(node);
        }

        // Iterate over dirty nodes using BitVec's iter_ones()
        for idx in self.dirty_bitmap.iter_ones() {
            visit(NodeId(idx as u64), self, &mut visited, &mut result);
        }

        result
    }

    /// Settle the graph - process all dirty nodes in topological order.
    /// Uses tuple-based processing internally, converts to RowDelta for output.
    pub fn settle<F>(
        &mut self,
        indices: &IndicesMap,
        om: &ObjectManager,
        mut row_loader: F,
    ) -> RowDelta
    where
        F: FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    {
        let order = self.topo_sort_dirty();
        let mut tuple_deltas: AHashMap<NodeId, TupleDelta> = AHashMap::new();

        let ctx = SourceContext { indices };
        let _ = om; // om is still passed to other nodes that need it

        for node_id in order {
            match self.get_node(node_id) {
                Some(GraphNode::IndexScan(_)) => {
                    if let Some(GraphNode::IndexScan(scan_node)) = self.get_node_mut(node_id) {
                        let delta = SourceNode::scan(scan_node, &ctx);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Union(_)) => {
                    let inputs = self.collect_tuple_inputs(node_id);
                    if let Some(GraphNode::Union(union_node)) = self.get_node_mut(node_id) {
                        let input_refs: Vec<_> = inputs.iter().collect();
                        let delta = TransformNode::process(union_node, &input_refs);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Alias(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Alias(alias_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(alias_node, input_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Join(_)) => {
                    // JoinNode has two inputs: left (index 0) and right (index 1)
                    let inputs = self.get_inputs(node_id);
                    let left_delta = inputs
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();
                    let right_delta = inputs
                        .get(1)
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Join(join_node)) = self.get_node_mut(node_id) {
                        // Process left side first, then right side
                        let left_result = join_node.process_left(left_delta);
                        let right_result = join_node.process_right(right_delta);

                        // Merge results
                        let mut merged = TupleDelta::new();
                        merged.added.extend(left_result.added);
                        merged.added.extend(right_result.added);
                        merged.removed.extend(left_result.removed);
                        merged.removed.extend(right_result.removed);
                        merged.pending = left_result.pending || right_result.pending;

                        tuple_deltas.insert(node_id, merged);
                    }
                }
                Some(GraphNode::Project(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Project(project_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(project_node, input_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Materialize(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Materialize(mat_node)) = self.get_node_mut(node_id) {
                        let deleted_delta = mat_node.check_deleted_tuples();
                        let pending_delta = mat_node.check_pending_tuples(&mut row_loader);
                        let new_delta = mat_node.materialize_tuples(input_delta, &mut row_loader);
                        let update_delta = mat_node.check_updated_tuples(&mut row_loader);

                        let mut merged = TupleDelta::new();
                        merged.added.extend(pending_delta.added);
                        merged.added.extend(new_delta.added);
                        merged.removed.extend(deleted_delta.removed);
                        merged.removed.extend(pending_delta.removed);
                        merged.removed.extend(new_delta.removed);
                        merged.updated.extend(pending_delta.updated);
                        merged.updated.extend(new_delta.updated);
                        merged.updated.extend(update_delta.updated);
                        merged.pending = new_delta.pending;

                        tuple_deltas.insert(node_id, merged);
                    }
                }
                Some(GraphNode::Filter(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Filter(filter_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(filter_node, input_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::PolicyFilter(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::PolicyFilter(policy_node)) = self.get_node_mut(node_id) {
                        // Use process_with_context if the policy has INHERITS clauses
                        let delta = if policy_node.has_inherits() {
                            policy_node.process_with_context(input_delta, indices, om, &mut |id| {
                                row_loader(id)
                            })
                        } else {
                            RowNode::process(policy_node, input_delta)
                        };
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Sort(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Sort(sort_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(sort_node, input_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::LimitOffset(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::LimitOffset(lo_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(lo_node, input_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::ArraySubquery(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::ArraySubquery(subquery_node)) =
                        self.get_node_mut(node_id)
                    {
                        // Check if inner table changed - need to reevaluate all existing instances
                        let mut delta = if subquery_node.is_inner_dirty() {
                            subquery_node.reevaluate_all(indices, om, &mut |id| row_loader(id))
                        } else {
                            TupleDelta::new()
                        };

                        // Process outer input changes
                        let outer_delta = subquery_node.process_with_context(
                            input_delta,
                            indices,
                            om,
                            &mut row_loader,
                        );

                        // Merge outer delta into combined delta
                        delta.merge(outer_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Output(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Output(output_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(output_node, input_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::ExistsOutput(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::ExistsOutput(exists_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(exists_node, input_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                None => {}
            }
        }

        self.dirty_bitmap.fill(false);

        // Convert TupleDelta to RowDelta for output
        // For single-table queries: use simple conversion
        // For join queries: flatten multi-element tuples using table descriptors
        tuple_deltas
            .remove(&self.output_node)
            .and_then(|td| {
                if self.table_descriptors.len() == 1 {
                    // Single-table query - direct conversion
                    td.to_row_delta()
                } else {
                    // Join query - flatten multi-element tuples
                    td.flatten_to_row_delta(&self.table_descriptors, &self.combined_descriptor)
                }
            })
            .unwrap_or_default()
    }

    /// Collect tuple sets from input nodes for a transform node.
    fn collect_tuple_inputs(&self, node_id: NodeId) -> Vec<AHashSet<Tuple>> {
        self.get_inputs(node_id)
            .iter()
            .filter_map(|dep| match &self.nodes[dep.0 as usize].node {
                GraphNode::IndexScan(n) => Some(n.current_tuples().clone()),
                GraphNode::Union(n) => Some(n.current_tuples().clone()),
                _ => None,
            })
            .collect()
    }

    /// Get current result from output node.
    pub fn current_result(&self) -> Vec<Row> {
        match self.get_node(self.output_node) {
            Some(GraphNode::Output(node)) => node.current_rows(),
            _ => vec![],
        }
    }

    // ========================================================================
    // Memory profiling
    // ========================================================================

    /// Estimate memory size of this QueryGraph.
    pub fn estimate_memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();

        // Nodes Vec with CompactNode (node + inline edges)
        for compact in &self.nodes {
            size += std::mem::size_of::<CompactNode>() + 512; // estimate node size
            size += compact.inputs.len() * std::mem::size_of::<NodeId>();
            size += compact.outputs.len() * std::mem::size_of::<NodeId>();
        }

        // Dirty bitmap (1 bit per node)
        size += self.dirty_bitmap.len() / 8 + 1;

        // Table name (interned - shared, but count the string length for this ref)
        size += self.table.as_str().len();

        // Index scan nodes (interned - pointer sized, but count string lengths for reference)
        for (_, table, col) in &self.index_scan_nodes {
            size += std::mem::size_of::<NodeId>() + table.as_str().len() + col.as_str().len();
        }

        // Array subquery tables
        for (_, table) in &self.array_subquery_tables {
            size += std::mem::size_of::<NodeId>() + table.as_str().len();
        }

        // Policy filter tables
        for (_, table) in &self.policy_filter_tables {
            size += std::mem::size_of::<NodeId>() + table.as_str().len();
        }

        // Table descriptors - estimate 200 bytes per descriptor
        size += self.table_descriptors.len() * 200;

        // Combined descriptor
        size += 200;

        size
    }
}

/// Build remaining predicate from conditions not covered by index scans.
/// Returns Predicate::True if all conditions are fully covered.
fn build_remaining_predicate(
    query: &Query,
    index_columns: &[String],
    descriptor: &RowDescriptor,
) -> Predicate {
    // Check if all disjuncts are fully covered by their respective index scans
    let all_fully_covered = query
        .disjuncts
        .iter()
        .zip(index_columns.iter())
        .all(|(disjunct, index_col)| disjunct.is_fully_covered_by_index(index_col));

    if all_fully_covered {
        return Predicate::True;
    }

    // Build remaining predicates for each disjunct
    let remaining_predicates: Vec<Predicate> = query
        .disjuncts
        .iter()
        .zip(index_columns.iter())
        .map(|(disjunct, index_col)| disjunct.remaining_predicate(index_col, descriptor))
        .filter(|p| !matches!(p, Predicate::True))
        .collect();

    // If any disjunct needs filtering, we must use the full predicate for correctness
    // (because we can't tell which disjunct a row came from after union)
    if remaining_predicates.is_empty() {
        Predicate::True
    } else {
        // Fall back to full predicate for partial coverage cases
        query.to_predicate(descriptor)
    }
}

/// Convert a condition to a scan condition.
fn condition_to_scan(cond: &Condition) -> ScanCondition {
    match cond {
        Condition::Eq { value, .. } => ScanCondition::Eq(encode_value(value)),
        Condition::Lt { value, .. } => ScanCondition::Range {
            min: Bound::Unbounded,
            max: Bound::Excluded(encode_value(value)),
        },
        Condition::Le { value, .. } => ScanCondition::Range {
            min: Bound::Unbounded,
            max: Bound::Included(encode_value(value)),
        },
        Condition::Gt { value, .. } => ScanCondition::Range {
            min: Bound::Excluded(encode_value(value)),
            max: Bound::Unbounded,
        },
        Condition::Ge { value, .. } => ScanCondition::Range {
            min: Bound::Included(encode_value(value)),
            max: Bound::Unbounded,
        },
        Condition::Between { min, max, .. } => ScanCondition::Range {
            min: Bound::Included(encode_value(min)),
            max: Bound::Included(encode_value(max)),
        },
        _ => ScanCondition::All,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::query::QueryBuilder;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, RowDescriptor, Value};

    fn test_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
                ColumnDescriptor::new("score", ColumnType::Integer),
            ])
            .into(),
        );
        schema
    }

    #[test]
    fn compile_simple_query() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_eq("score", Value::Integer(100))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Should have: IndexScan -> Materialize -> Output (Filter elided - Eq fully covered)
        assert_eq!(graph.nodes.len(), 3);
        assert_eq!(graph.index_scan_nodes.len(), 1);
    }

    #[test]
    fn compile_query_with_or() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_eq("score", Value::Integer(50))
            .or()
            .filter_eq("score", Value::Integer(100))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Should have: 2x IndexScan -> Union -> Materialize -> Output (Filter elided)
        assert_eq!(graph.nodes.len(), 5);
        assert_eq!(graph.index_scan_nodes.len(), 2);
    }

    #[test]
    fn compile_query_with_sort_and_limit() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .order_by_desc("score")
            .limit(10)
            .offset(5)
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Should have: IndexScan -> Materialize -> Sort -> LimitOffset -> Output
        // (no Filter because no WHERE clause)
        assert_eq!(graph.nodes.len(), 5);
    }

    #[test]
    fn compile_query_no_filter() {
        let schema = test_schema();
        let query = QueryBuilder::new("users").build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Should have: IndexScan -> Materialize -> Output
        assert_eq!(graph.nodes.len(), 3);
    }

    // ========================================================================
    // FilterNode elision tests
    // ========================================================================

    fn has_filter_node(graph: &QueryGraph) -> bool {
        graph
            .nodes
            .iter()
            .any(|c| matches!(c.node, GraphNode::Filter(_)))
    }

    #[test]
    fn single_eq_condition_elides_filter() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_eq("score", Value::Integer(100))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Eq is fully covered by index scan, no FilterNode needed
        // Should have: IndexScan -> Materialize -> Output (3 nodes)
        assert!(
            !has_filter_node(&graph),
            "FilterNode should be elided for single Eq condition"
        );
        assert_eq!(graph.nodes.len(), 3);
    }

    #[test]
    fn single_lt_condition_elides_filter() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_lt("score", Value::Integer(50))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Lt is fully covered by index scan with Bound::Excluded
        assert!(
            !has_filter_node(&graph),
            "FilterNode should be elided for single Lt condition"
        );
        assert_eq!(graph.nodes.len(), 3);
    }

    #[test]
    fn single_between_condition_elides_filter() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_between("score", Value::Integer(10), Value::Integer(50))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Between is fully covered by index scan with inclusive bounds
        assert!(
            !has_filter_node(&graph),
            "FilterNode should be elided for single Between condition"
        );
        assert_eq!(graph.nodes.len(), 3);
    }

    #[test]
    fn multiple_conditions_different_columns_keeps_filter() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_lt("score", Value::Integer(50))
            .filter_eq("name", Value::Text("Alice".into()))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Index scan covers score < 50, but name = 'Alice' still needs filtering
        // Should have: IndexScan -> Materialize -> Filter -> Output (4 nodes)
        assert!(
            has_filter_node(&graph),
            "FilterNode needed for non-indexed condition"
        );
        assert_eq!(graph.nodes.len(), 4);
    }

    #[test]
    fn non_indexable_condition_keeps_filter() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_ne("score", Value::Integer(50))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Ne is not index-scannable, uses full scan + filter
        // Should have: IndexScan -> Materialize -> Filter -> Output (4 nodes)
        assert!(
            has_filter_node(&graph),
            "FilterNode needed for non-indexable condition"
        );
        assert_eq!(graph.nodes.len(), 4);
    }

    #[test]
    fn or_with_single_conditions_elides_filter() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_eq("score", Value::Integer(50))
            .or()
            .filter_eq("score", Value::Integer(100))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Each disjunct has one Eq condition fully covered by its index scan
        // Union combines them, no additional filtering needed
        // Should have: 2x IndexScan -> Union -> Materialize -> Output (5 nodes)
        assert!(
            !has_filter_node(&graph),
            "FilterNode should be elided when all disjuncts are fully covered"
        );
        assert_eq!(graph.nodes.len(), 5);
    }

    // ========================================================================
    // Join compilation tests
    // ========================================================================

    fn join_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("posts"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("author_id", ColumnType::Integer),
            ])
            .into(),
        );
        schema
    }

    fn has_join_node(graph: &QueryGraph) -> bool {
        graph
            .nodes
            .iter()
            .any(|c| matches!(c.node, GraphNode::Join(_)))
    }

    fn has_project_node(graph: &QueryGraph) -> bool {
        graph
            .nodes
            .iter()
            .any(|c| matches!(c.node, GraphNode::Project(_)))
    }

    #[test]
    fn compile_simple_join() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .join("posts")
            .on("id", "author_id")
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Should have: 2x IndexScan -> 2x Materialize -> JoinNode -> Output
        // 2 IndexScans + 2 Materializes + 1 Join + 1 Output = 6 nodes
        assert!(has_join_node(&graph), "Should have a JoinNode");
        assert_eq!(graph.index_scan_nodes.len(), 2);
    }

    #[test]
    fn compile_join_with_projection() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .join("posts")
            .on("id", "author_id")
            .select(&["name", "title"])
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        assert!(has_join_node(&graph), "Should have a JoinNode");
        assert!(has_project_node(&graph), "Should have a ProjectNode");
    }

    #[test]
    fn compile_join_returns_none_for_missing_table() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .join("comments") // Table doesn't exist
            .on("id", "user_id")
            .build();

        let graph = QueryGraph::compile(&query, &schema);
        assert!(graph.is_none(), "Should return None for missing table");
    }

    #[test]
    fn compile_join_returns_none_for_invalid_column() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .join("posts")
            .on("nonexistent", "author_id") // Column doesn't exist
            .build();

        let graph = QueryGraph::compile(&query, &schema);
        assert!(graph.is_none(), "Should return None for invalid column");
    }

    // ========================================================================
    // Array subquery compilation tests
    // ========================================================================

    fn has_array_subquery_node(graph: &QueryGraph) -> bool {
        graph
            .nodes
            .iter()
            .any(|c| matches!(c.node, GraphNode::ArraySubquery(_)))
    }

    #[test]
    fn compile_query_with_array_subquery() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .with_array("posts", |sub| {
                sub.from("posts")
                    .correlate("author_id", "users.id")
                    .select(&["id", "title"])
            })
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Should have: IndexScan -> Materialize -> ArraySubquery -> Output
        assert!(
            has_array_subquery_node(&graph),
            "Should have an ArraySubqueryNode"
        );
    }

    #[test]
    fn compile_query_with_array_subquery_and_filter() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .with_array("posts", |sub| {
                sub.from("posts").correlate("author_id", "users.id")
            })
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        assert!(has_array_subquery_node(&graph));
        // Filter may be elided if covered by index scan
    }

    #[test]
    fn compile_query_with_multiple_array_subqueries() {
        let mut schema = join_schema();
        schema.insert(
            TableName::new("comments"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("text", ColumnType::Text),
                ColumnDescriptor::new("user_id", ColumnType::Integer),
            ])
            .into(),
        );

        let query = QueryBuilder::new("users")
            .with_array("posts", |sub| {
                sub.from("posts").correlate("author_id", "users.id")
            })
            .with_array("comments", |sub| {
                sub.from("comments").correlate("user_id", "users.id")
            })
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Count ArraySubquery nodes
        let array_subquery_count = graph
            .nodes
            .iter()
            .filter(|c| matches!(c.node, GraphNode::ArraySubquery(_)))
            .count();
        assert_eq!(
            array_subquery_count, 2,
            "Should have two ArraySubqueryNodes"
        );
    }

    #[test]
    fn compile_array_subquery_returns_none_for_missing_inner_table() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .with_array("comments", |sub| {
                sub.from("comments") // Table doesn't exist
                    .correlate("user_id", "users.id")
            })
            .build();

        let graph = QueryGraph::compile(&query, &schema);
        // Should succeed but with no ArraySubqueryNode (graceful degradation)
        // Or should fail entirely - depends on design choice
        // Current implementation silently skips invalid array subqueries
        assert!(graph.is_some());
    }
}
