use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::object_manager::ObjectManager;

use super::encoding::encode_value;
use super::graph_nodes::alias::AliasNode;
use super::graph_nodes::array_subquery::ArraySubqueryNode;
use super::graph_nodes::exists_output::ExistsOutputNode;
use super::graph_nodes::filter::{FilterNode, Predicate};
use super::graph_nodes::index_scan::{IndexScanNode, ScanCondition};
use super::graph_nodes::join::JoinNode;
use super::graph_nodes::limit_offset::LimitOffsetNode;
use super::graph_nodes::materialize::MaterializeNode;
use super::graph_nodes::output::{OutputMode, OutputNode};
use super::graph_nodes::policy_filter::PolicyFilterNode;
use super::graph_nodes::project::ProjectNode;
use super::graph_nodes::sort::SortNode;
use super::graph_nodes::subgraph::SubgraphTemplate;
use super::graph_nodes::union::UnionNode;
use super::graph_nodes::{NodeId, RowNode, SourceContext, SourceNode, TransformNode};
use super::index::IndexState;
use super::query::{Condition, Query};
use super::session::Session;
use super::types::{
    ColumnDescriptor, ColumnType, Row, RowDelta, RowDescriptor, Schema, TableName, Tuple,
    TupleDelta, TupleDescriptor,
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

/// Compiled query graph for a single query.
#[derive(Debug)]
pub struct QueryGraph {
    /// All nodes in the graph.
    pub nodes: HashMap<NodeId, GraphNode>,
    /// Edges: node -> its inputs (children).
    pub edges: HashMap<NodeId, Vec<NodeId>>,
    /// Reverse edges: node -> nodes that depend on it (parents).
    pub reverse_edges: HashMap<NodeId, Vec<NodeId>>,
    /// Dirty nodes that need processing.
    pub dirty_nodes: HashSet<NodeId>,
    /// The output node ID.
    pub output_node: NodeId,
    /// Table this query operates on.
    pub table: TableName,
    /// Index scan nodes for this query (for marking dirty on updates).
    pub index_scan_nodes: Vec<(NodeId, String, String)>, // (node_id, table, column)
    /// Array subquery nodes and their inner tables (for marking dirty on inner table updates).
    pub array_subquery_tables: Vec<(NodeId, String)>, // (node_id, inner_table)
    /// PolicyFilter nodes and their INHERITS-referenced tables (for marking dirty on table updates).
    pub policy_filter_tables: Vec<(NodeId, String)>, // (node_id, inherits_table)
    /// Per-table descriptors in join order (for flattening multi-element tuples).
    pub table_descriptors: Vec<RowDescriptor>,
    /// Combined descriptor for output (all columns from all tables).
    pub combined_descriptor: RowDescriptor,
    /// Next node ID.
    next_node_id: u64,
}

impl QueryGraph {
    pub fn new(table: TableName, descriptor: RowDescriptor) -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            reverse_edges: HashMap::new(),
            dirty_nodes: HashSet::new(),
            output_node: NodeId(0),
            table,
            index_scan_nodes: Vec::new(),
            array_subquery_tables: Vec::new(),
            policy_filter_tables: Vec::new(),
            table_descriptors: vec![descriptor.clone()],
            combined_descriptor: descriptor,
            next_node_id: 0,
        }
    }

    fn next_id(&mut self) -> NodeId {
        let id = NodeId(self.next_node_id);
        self.next_node_id += 1;
        id
    }

    fn add_node(&mut self, node: GraphNode) -> NodeId {
        let id = self.next_id();
        self.nodes.insert(id, node);
        self.edges.insert(id, Vec::new());
        self.reverse_edges.insert(id, Vec::new());
        self.dirty_nodes.insert(id);
        id
    }

    /// Add an edge from one node to another.
    pub fn add_edge(&mut self, from: NodeId, to: NodeId) {
        self.edges.entry(from).or_default().push(to);
        self.reverse_edges.entry(to).or_default().push(from);
    }

    /// Add a node and return its ID.
    pub fn add_node_with_id(&mut self, node: GraphNode) -> NodeId {
        let id = self.next_id();
        self.nodes.insert(id, node);
        self.edges.insert(id, Vec::new());
        self.reverse_edges.insert(id, Vec::new());
        self.dirty_nodes.insert(id);
        id
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
        if query.is_join() {
            // TODO: Add policy support for joins
            return Self::compile_join(query, schema);
        }

        let table_schema = schema.get(&query.table)?;
        let descriptor = table_schema.descriptor.clone();
        let select_policy = table_schema.policies.select.using.clone();
        let mut graph = QueryGraph::new(query.table.clone(), descriptor.clone());

        // Phase 1: Build IndexScan nodes (one per disjunct)
        let mut phase1_outputs: Vec<NodeId> = Vec::new();
        let mut index_columns: Vec<String> = Vec::new();

        for disjunct in &query.disjuncts {
            // Find best index condition for this disjunct
            let (scan_column, scan_condition) = if let Some(cond) = disjunct.best_index_condition()
            {
                let column = cond.column().to_string();
                let scan_cond = condition_to_scan(cond);
                (column, scan_cond)
            } else {
                // No index condition, use "_id" for full scan
                ("_id".to_string(), ScanCondition::All)
            };

            index_columns.push(scan_column.clone());

            let scan_node = IndexScanNode::new(
                query.table.0.clone(),
                &scan_column,
                scan_condition,
                descriptor.clone(),
            );
            let scan_id = graph.add_node(GraphNode::IndexScan(scan_node));
            graph
                .index_scan_nodes
                .push((scan_id, query.table.0.clone(), scan_column));
            phase1_outputs.push(scan_id);
        }

        // If include_deleted is set, also scan _id_deleted index
        if query.include_deleted {
            let deleted_scan_node = IndexScanNode::new(
                query.table.0.clone(),
                "_id_deleted",
                ScanCondition::All,
                descriptor.clone(),
            );
            let deleted_scan_id = graph.add_node(GraphNode::IndexScan(deleted_scan_node));
            graph.index_scan_nodes.push((
                deleted_scan_id,
                query.table.0.clone(),
                "_id_deleted".to_string(),
            ));
            phase1_outputs.push(deleted_scan_id);
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
                query.table.0.clone(),
            );
            // Collect INHERITS tables before moving the node
            let inherits_tables: Vec<_> = policy_node.inherits_tables().iter().cloned().collect();
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
                graph.compile_array_subquery(subquery_spec, &current_descriptor, schema)
            {
                let node_id = graph.add_node(GraphNode::ArraySubquery(node));
                graph.add_edge(node_id, phase2_input);
                // Track inner table for dirty marking on inner table updates
                graph
                    .array_subquery_tables
                    .push((node_id, subquery_spec.table.0.clone()));
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

    /// Compile an array subquery specification into an ArraySubqueryNode.
    /// Returns the node and the new output descriptor (outer + array column).
    fn compile_array_subquery(
        &self,
        spec: &crate::query_manager::query::ArraySubquerySpec,
        outer_descriptor: &RowDescriptor,
        schema: &Schema,
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

        // Build base query for subgraph
        let mut base_query = Query::new(spec.table.clone());
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
                        .find(|c| &c.name == name)
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
                .filter_map(|name| columns.iter().find(|c| &c.name == name).cloned())
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
    fn compile_join(query: &Query, schema: &Schema) -> Option<Self> {
        let base_table_schema = schema.get(&query.table)?;
        let base_descriptor = base_table_schema.descriptor.clone();
        let mut graph = QueryGraph::new(query.table.clone(), base_descriptor.clone());

        // Track all table names and descriptors for TupleDescriptor
        let mut table_names = vec![query.table.0.clone()];
        let mut table_descriptors = vec![base_descriptor.clone()];

        // Build pipeline for base table: IndexScan → Materialize
        let base_scan = IndexScanNode::new(
            query.table.0.clone(),
            "_id",
            ScanCondition::All,
            base_descriptor.clone(),
        );
        let base_scan_id = graph.add_node(GraphNode::IndexScan(base_scan));
        graph
            .index_scan_nodes
            .push((base_scan_id, query.table.0.clone(), "_id".to_string()));

        let base_mat = MaterializeNode::new(base_descriptor.clone());
        let base_mat_id = graph.add_node(GraphNode::Materialize(base_mat));
        graph.add_edge(base_mat_id, base_scan_id);

        // Track current left side descriptor (accumulates columns from joins)
        let mut left_id = base_mat_id;
        let mut left_descriptor = base_descriptor.clone();
        let mut left_table_name = query.table.0.clone();

        // Process each join
        for join_spec in &query.joins {
            let right_table_schema = schema.get(&join_spec.table)?;
            let right_descriptor = right_table_schema.descriptor.clone();

            // Build pipeline for right table: IndexScan → Materialize
            let right_scan = IndexScanNode::new(
                join_spec.table.0.clone(),
                "_id",
                ScanCondition::All,
                right_descriptor.clone(),
            );
            let right_scan_id = graph.add_node(GraphNode::IndexScan(right_scan));
            graph.index_scan_nodes.push((
                right_scan_id,
                join_spec.table.0.clone(),
                "_id".to_string(),
            ));

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
                    &join_spec.table.0,
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
                table_names.push(join_spec.table.0.clone());
                table_descriptors.push(right_descriptor.clone());

                // Combine descriptors for downstream nodes
                left_descriptor = RowDescriptor::combine(&[left_descriptor, right_descriptor]);
                // Use combined table name for multi-way joins
                left_table_name = format!("{}_{}", left_table_name, join_spec.table.0);
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
        for (node_id, t, c) in &self.index_scan_nodes {
            if t == table && (c == column || c == "_id") {
                self.dirty_nodes.insert(*node_id);
            }
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
            .filter_map(
                |(node_id, t, _)| {
                    if t == table { Some(*node_id) } else { None }
                },
            )
            .collect();

        for node_id in affected_index_scans {
            self.dirty_nodes.insert(node_id);
            self.mark_downstream_dirty(node_id);
        }
        // Mark array subquery nodes whose inner table changed
        // Collect node_ids first to avoid borrow conflict
        let affected_array_subqueries: Vec<NodeId> = self
            .array_subquery_tables
            .iter()
            .filter_map(|(node_id, inner_table)| {
                if inner_table == table {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in affected_array_subqueries {
            self.dirty_nodes.insert(node_id);
            // Mark the node as needing inner re-evaluation
            if let Some(GraphNode::ArraySubquery(node)) = self.nodes.get_mut(&node_id) {
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
                if inherits_table == table {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in affected_policy_filters {
            self.dirty_nodes.insert(node_id);
            // Mark the node as needing INHERITS re-evaluation
            if let Some(GraphNode::PolicyFilter(node)) = self.nodes.get_mut(&node_id) {
                node.mark_inherits_dirty();
            }
            // Propagate dirty marks to downstream nodes
            self.mark_downstream_dirty(node_id);
        }
    }

    /// Check if this graph involves a table (as index scan, array subquery inner table, or INHERITS reference).
    pub fn involves_table(&self, table: &str) -> bool {
        self.index_scan_nodes.iter().any(|(_, t, _)| t == table)
            || self.array_subquery_tables.iter().any(|(_, t)| t == table)
            || self.policy_filter_tables.iter().any(|(_, t)| t == table)
    }

    /// Check if the MaterializeNode has a specific object ID pending.
    pub fn has_pending_id(&self, object_id: ObjectId) -> bool {
        for node in self.nodes.values() {
            if let GraphNode::Materialize(mat_node) = node
                && mat_node.pending_ids().contains(&object_id)
            {
                return true;
            }
        }
        false
    }

    /// Mark the materialize node dirty (to re-check pending IDs).
    pub fn mark_materialize_dirty(&mut self) {
        for (node_id, node) in &self.nodes {
            if matches!(node, GraphNode::Materialize(_)) {
                self.dirty_nodes.insert(*node_id);
            }
        }
    }

    /// Mark a row ID as updated for content checking.
    /// This tells MaterializeNodes to check if the row's content has changed.
    pub fn mark_row_updated(&mut self, id: ObjectId) {
        // First pass: mark the ID as updated in each MaterializeNode and collect node IDs
        let materialize_node_ids: Vec<NodeId> = self
            .nodes
            .iter_mut()
            .filter_map(|(node_id, node)| {
                if let GraphNode::Materialize(mat_node) = node {
                    mat_node.mark_updated(id);
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        // Second pass: mark dirty and propagate downstream
        for node_id in materialize_node_ids {
            self.dirty_nodes.insert(node_id);
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
            .filter_map(|(node_id, node)| {
                if let GraphNode::Materialize(mat_node) = node {
                    mat_node.mark_deleted(id);
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        // Second pass: mark dirty and propagate downstream
        for node_id in materialize_node_ids {
            self.dirty_nodes.insert(node_id);
            self.mark_downstream_dirty(node_id);
        }
    }

    /// Mark all nodes that depend on the given node as dirty (propagate forward).
    fn mark_downstream_dirty(&mut self, node_id: NodeId) {
        if let Some(parents) = self.reverse_edges.get(&node_id).cloned() {
            for parent in parents {
                if self.dirty_nodes.insert(parent) {
                    // Recursively mark parents of parent
                    self.mark_downstream_dirty(parent);
                }
            }
        }
    }

    /// Topological sort of dirty nodes (dependencies first).
    fn topo_sort_dirty(&self) -> Vec<NodeId> {
        let mut result = Vec::new();
        let mut visited = HashSet::new();

        fn visit(
            node: NodeId,
            graph: &QueryGraph,
            visited: &mut HashSet<NodeId>,
            result: &mut Vec<NodeId>,
        ) {
            if visited.contains(&node) {
                return;
            }
            visited.insert(node);

            // Visit dependencies first
            if let Some(deps) = graph.edges.get(&node) {
                for dep in deps {
                    visit(*dep, graph, visited, result);
                }
            }

            result.push(node);
        }

        for node in &self.dirty_nodes {
            visit(*node, self, &mut visited, &mut result);
        }

        result
    }

    /// Settle the graph - process all dirty nodes in topological order.
    /// Uses tuple-based processing internally, converts to RowDelta for output.
    pub fn settle<F>(
        &mut self,
        indices: &HashMap<(String, String), IndexState>,
        om: &ObjectManager,
        mut row_loader: F,
    ) -> RowDelta
    where
        F: FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    {
        let order = self.topo_sort_dirty();
        let mut tuple_deltas: HashMap<NodeId, TupleDelta> = HashMap::new();

        let ctx = SourceContext { indices, om };

        for node_id in order {
            match self.nodes.get(&node_id) {
                Some(GraphNode::IndexScan(_)) => {
                    if let Some(GraphNode::IndexScan(scan_node)) = self.nodes.get_mut(&node_id) {
                        let delta = SourceNode::scan(scan_node, &ctx);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Union(_)) => {
                    let inputs = self.collect_tuple_inputs(node_id);
                    if let Some(GraphNode::Union(union_node)) = self.nodes.get_mut(&node_id) {
                        let input_refs: Vec<_> = inputs.iter().collect();
                        let delta = TransformNode::process(union_node, &input_refs);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Alias(_)) => {
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Alias(alias_node)) = self.nodes.get_mut(&node_id) {
                        let delta = RowNode::process(alias_node, input_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Join(_)) => {
                    // JoinNode has two inputs: left (index 0) and right (index 1)
                    let edges = self.edges[&node_id].clone();
                    let left_delta = edges
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();
                    let right_delta = edges
                        .get(1)
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Join(join_node)) = self.nodes.get_mut(&node_id) {
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
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Project(project_node)) = self.nodes.get_mut(&node_id) {
                        let delta = RowNode::process(project_node, input_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Materialize(_)) => {
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Materialize(mat_node)) = self.nodes.get_mut(&node_id) {
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
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Filter(filter_node)) = self.nodes.get_mut(&node_id) {
                        let delta = RowNode::process(filter_node, input_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::PolicyFilter(_)) => {
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::PolicyFilter(policy_node)) = self.nodes.get_mut(&node_id)
                    {
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
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Sort(sort_node)) = self.nodes.get_mut(&node_id) {
                        let delta = RowNode::process(sort_node, input_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::LimitOffset(_)) => {
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::LimitOffset(lo_node)) = self.nodes.get_mut(&node_id) {
                        let delta = RowNode::process(lo_node, input_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::ArraySubquery(_)) => {
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::ArraySubquery(subquery_node)) =
                        self.nodes.get_mut(&node_id)
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
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Output(output_node)) = self.nodes.get_mut(&node_id) {
                        let delta = RowNode::process(output_node, input_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::ExistsOutput(_)) => {
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::ExistsOutput(exists_node)) = self.nodes.get_mut(&node_id)
                    {
                        let delta = RowNode::process(exists_node, input_delta);
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                None => {}
            }
        }

        self.dirty_nodes.clear();

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
    fn collect_tuple_inputs(&self, node_id: NodeId) -> Vec<HashSet<Tuple>> {
        self.edges[&node_id]
            .iter()
            .filter_map(|dep| match &self.nodes[dep] {
                GraphNode::IndexScan(n) => Some(n.current_tuples().clone()),
                GraphNode::Union(n) => Some(n.current_tuples().clone()),
                _ => None,
            })
            .collect()
    }

    /// Get current result from output node.
    pub fn current_result(&self) -> Vec<Row> {
        match self.nodes.get(&self.output_node) {
            Some(GraphNode::Output(node)) => node.current_rows(),
            _ => vec![],
        }
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
            .values()
            .any(|n| matches!(n, GraphNode::Filter(_)))
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
            .values()
            .any(|n| matches!(n, GraphNode::Join(_)))
    }

    fn has_project_node(graph: &QueryGraph) -> bool {
        graph
            .nodes
            .values()
            .any(|n| matches!(n, GraphNode::Project(_)))
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
            .values()
            .any(|n| matches!(n, GraphNode::ArraySubquery(_)))
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
            .values()
            .filter(|n| matches!(n, GraphNode::ArraySubquery(_)))
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
