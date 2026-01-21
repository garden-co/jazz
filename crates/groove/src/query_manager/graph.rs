use std::collections::{HashMap, HashSet};

use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::object_manager::ObjectManager;

use super::encoding::encode_value;
use super::graph_nodes::filter::{FilterNode, Predicate};
use super::graph_nodes::index_scan::{IndexScanNode, ScanCondition};
use super::graph_nodes::limit_offset::LimitOffsetNode;
use super::graph_nodes::materialize::MaterializeNode;
use super::graph_nodes::output::{OutputMode, OutputNode};
use super::graph_nodes::sort::SortNode;
use super::graph_nodes::union::UnionNode;
use super::graph_nodes::{IdNode, NodeId, RowNode};
use super::index::IndexState;
use super::query::{Condition, Query};
use super::types::{IdDelta, Row, RowDelta, Schema, TableName};

/// A node in the query graph (type-erased).
#[derive(Debug)]
pub enum GraphNode {
    IndexScan(IndexScanNode),
    Union(UnionNode),
    Materialize(MaterializeNode),
    Filter(FilterNode),
    Sort(SortNode),
    LimitOffset(LimitOffsetNode),
    Output(OutputNode),
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
    /// Next node ID.
    next_node_id: u64,
}

impl QueryGraph {
    pub fn new(table: TableName) -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            reverse_edges: HashMap::new(),
            dirty_nodes: HashSet::new(),
            output_node: NodeId(0),
            table,
            index_scan_nodes: Vec::new(),
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

    fn add_edge(&mut self, from: NodeId, to: NodeId) {
        self.edges.entry(from).or_default().push(to);
        self.reverse_edges.entry(to).or_default().push(from);
    }

    /// Compile a query into a graph.
    pub fn compile(query: &Query, schema: &Schema) -> Option<Self> {
        let descriptor = schema.get(&query.table)?.clone();
        let mut graph = QueryGraph::new(query.table.clone());

        // Phase 1: Build IndexScan nodes (one per disjunct)
        let mut phase1_outputs: Vec<NodeId> = Vec::new();

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

            let scan_node = IndexScanNode::new(query.table.0.clone(), &scan_column, scan_condition);
            let scan_id = graph.add_node(GraphNode::IndexScan(scan_node));
            graph
                .index_scan_nodes
                .push((scan_id, query.table.0.clone(), scan_column));
            phase1_outputs.push(scan_id);
        }

        // If multiple disjuncts, add Union node
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

        // Phase 2: Filter node (full predicate)
        let predicate = query.to_predicate(&descriptor);
        if !matches!(predicate, Predicate::True) {
            let filter_node = FilterNode::new(descriptor.clone(), predicate);
            let filter_id = graph.add_node(GraphNode::Filter(filter_node));
            graph.add_edge(filter_id, phase2_input);
            phase2_input = filter_id;
        }

        // Sort node (if order_by specified)
        if !query.order_by.is_empty() {
            let sort_keys = query.sort_keys(&descriptor);
            if !sort_keys.is_empty() {
                let sort_node = SortNode::new(descriptor.clone(), sort_keys);
                let sort_id = graph.add_node(GraphNode::Sort(sort_node));
                graph.add_edge(sort_id, phase2_input);
                phase2_input = sort_id;
            }
        }

        // LimitOffset node (if limit or offset specified)
        if query.limit.is_some() || query.offset > 0 {
            let limit_offset_node =
                LimitOffsetNode::new(descriptor.clone(), query.limit, query.offset);
            let limit_offset_id = graph.add_node(GraphNode::LimitOffset(limit_offset_node));
            graph.add_edge(limit_offset_id, phase2_input);
            phase2_input = limit_offset_id;
        }

        // Output node
        let output_node = OutputNode::new(descriptor, OutputMode::Delta);
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
    pub fn mark_dirty_for_table(&mut self, table: &str) {
        for (node_id, t, _) in &self.index_scan_nodes {
            if t == table {
                self.dirty_nodes.insert(*node_id);
            }
        }
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
    /// Returns the output delta.
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
        let mut id_deltas: HashMap<NodeId, IdDelta> = HashMap::new();
        let mut per_node_deltas: HashMap<NodeId, RowDelta> = HashMap::new();

        for node_id in order {
            // First, gather any needed inputs before mutating
            let node_type = self.nodes.get(&node_id).map(|n| match n {
                GraphNode::IndexScan(_) => "index_scan",
                GraphNode::Union(_) => "union",
                GraphNode::Materialize(_) => "materialize",
                GraphNode::Filter(_) => "filter",
                GraphNode::Sort(_) => "sort",
                GraphNode::LimitOffset(_) => "limit_offset",
                GraphNode::Output(_) => "output",
            });

            match node_type {
                Some("index_scan") => {
                    let node = self.nodes.get_mut(&node_id).unwrap();
                    if let GraphNode::IndexScan(scan_node) = node {
                        let key = (scan_node.table.clone(), scan_node.column.clone());
                        if let Some(index) = indices.get(&key) {
                            let delta = scan_node.scan(index, om);
                            id_deltas.insert(node_id, delta);
                        } else {
                            id_deltas.insert(node_id, IdDelta::new());
                        }
                    }
                }
                Some("union") => {
                    // Collect inputs first (immutable borrows)
                    let deps = self.edges[&node_id].clone();
                    let inputs: Vec<_> = deps
                        .iter()
                        .filter_map(|dep| match &self.nodes[dep] {
                            GraphNode::IndexScan(n) => Some(n.current_ids().clone()),
                            GraphNode::Union(n) => Some(n.current_ids().clone()),
                            _ => None,
                        })
                        .collect();

                    // Now mutate
                    let node = self.nodes.get_mut(&node_id).unwrap();
                    if let GraphNode::Union(union_node) = node {
                        let input_refs: Vec<_> = inputs.iter().collect();
                        let delta = union_node.process_inputs(&input_refs);
                        id_deltas.insert(node_id, delta);
                    }
                }
                Some("materialize") => {
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| id_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    let node = self.nodes.get_mut(&node_id).unwrap();
                    if let GraphNode::Materialize(mat_node) = node {
                        // First, check if any previously-pending rows are now available
                        let pending_delta = mat_node.check_pending(&mut row_loader);

                        // Then materialize the new IdDelta
                        let new_delta = mat_node.materialize(input_delta, &mut row_loader);

                        // Check updated IDs for content changes
                        let update_delta = mat_node.check_updated_ids(&mut row_loader);

                        // Merge all three deltas
                        let mut merged = RowDelta::new();
                        merged.added.extend(pending_delta.added);
                        merged.added.extend(new_delta.added);
                        merged.removed.extend(pending_delta.removed);
                        merged.removed.extend(new_delta.removed);
                        merged.updated.extend(pending_delta.updated);
                        merged.updated.extend(new_delta.updated);
                        merged.updated.extend(update_delta.updated);
                        // pending flag is set based on whether we still have pending IDs
                        merged.pending = new_delta.pending;

                        per_node_deltas.insert(node_id, merged);
                    }
                }
                Some("filter") => {
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| per_node_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    let node = self.nodes.get_mut(&node_id).unwrap();
                    if let GraphNode::Filter(filter_node) = node {
                        let delta = filter_node.process(input_delta);
                        per_node_deltas.insert(node_id, delta);
                    }
                }
                Some("sort") => {
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| per_node_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    let node = self.nodes.get_mut(&node_id).unwrap();
                    if let GraphNode::Sort(sort_node) = node {
                        let delta = sort_node.process(input_delta);
                        per_node_deltas.insert(node_id, delta);
                    }
                }
                Some("limit_offset") => {
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| per_node_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    let node = self.nodes.get_mut(&node_id).unwrap();
                    if let GraphNode::LimitOffset(lo_node) = node {
                        let delta = lo_node.process(input_delta);
                        per_node_deltas.insert(node_id, delta);
                    }
                }
                Some("output") => {
                    let input_delta = self.edges[&node_id]
                        .first()
                        .and_then(|dep| per_node_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    let node = self.nodes.get_mut(&node_id).unwrap();
                    if let GraphNode::Output(output_node) = node {
                        let delta = output_node.process(input_delta);
                        per_node_deltas.insert(node_id, delta);
                    }
                }
                _ => {}
            }
        }

        self.dirty_nodes.clear();
        per_node_deltas
            .remove(&self.output_node)
            .unwrap_or_default()
    }

    /// Get current result from output node.
    pub fn current_result(&self) -> &[Row] {
        match self.nodes.get(&self.output_node) {
            Some(GraphNode::Output(node)) => node.current_result(),
            _ => &[],
        }
    }
}

/// Convert a condition to a scan condition.
fn condition_to_scan(cond: &Condition) -> ScanCondition {
    match cond {
        Condition::Eq { value, .. } => ScanCondition::Eq(encode_value(value)),
        Condition::Lt { value, .. } => ScanCondition::Range {
            min: None,
            max: Some(encode_value(value)),
        },
        Condition::Le { value, .. } => ScanCondition::Range {
            min: None,
            max: Some(encode_value(value)),
        },
        Condition::Gt { value, .. } => ScanCondition::Range {
            min: Some(encode_value(value)),
            max: None,
        },
        Condition::Ge { value, .. } => ScanCondition::Range {
            min: Some(encode_value(value)),
            max: None,
        },
        Condition::Between { min, max, .. } => ScanCondition::Range {
            min: Some(encode_value(min)),
            max: Some(encode_value(max)),
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
            ]),
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

        // Should have: IndexScan -> Materialize -> Filter -> Output
        assert_eq!(graph.nodes.len(), 4);
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

        // Should have: 2x IndexScan -> Union -> Materialize -> Filter -> Output
        assert_eq!(graph.nodes.len(), 6);
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
}
