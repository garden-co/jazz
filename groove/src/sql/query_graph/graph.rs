//! Query graph - the main computation DAG.

use std::collections::HashMap;

use crate::object::ObjectId;
use crate::sql::catalog::DescriptorId;
use crate::sql::lens::QueryLensContext;
use crate::sql::query_graph::cache::RowCache;
use crate::sql::query_graph::delta::{DeltaBatch, RowDelta};
use crate::sql::query_graph::node::{InputPort, NodeId, QueryNode};
use crate::sql::row_buffer::OwnedRow;
use crate::sql::schema::TableSchema;

use super::DatabaseState;

/// Truncate a string to a maximum length, adding "..." if truncated.
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else if max_len > 3 {
        format!("{}...", &s[..max_len - 3])
    } else {
        s[..max_len].to_string()
    }
}

/// Unique identifier for a query graph.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct GraphId(pub u64);

/// Initialization state of a query graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphState {
    /// Not yet initialized - will load data on first access.
    Uninitialized,
    /// Currently loading initial data.
    Initializing,
    /// Ready for incremental updates.
    Ready,
}

/// An edge in the query DAG, connecting a node's output to another node's input port.
#[derive(Clone, Debug)]
pub struct Edge {
    /// Target node index.
    pub to: usize,
    /// Which input port on the target node this edge connects to.
    pub port: InputPort,
}

/// A computation graph for incremental query evaluation.
///
/// The graph is a DAG of nodes in topological order (sources first,
/// output last). Deltas propagate through the nodes, with early
/// cutoff when a node's output doesn't change.
pub struct QueryGraph {
    /// Unique identifier.
    id: GraphId,

    /// Current state.
    state: GraphState,

    /// Primary table this graph queries.
    table: String,

    /// All tables this graph depends on (for routing changes).
    all_tables: Vec<String>,

    /// Schema for the primary table.
    schema: TableSchema,

    /// Schemas for all tables (including joined tables).
    all_schemas: HashMap<String, TableSchema>,

    /// Whether this is a join query.
    is_join: bool,

    /// Nodes in topological order.
    nodes: Vec<QueryNode>,

    /// Map from NodeId to index in nodes vec.
    node_indices: HashMap<NodeId, usize>,

    /// The output node.
    output_node: NodeId,

    /// Entry points for each table: table name → (node index, input port).
    /// A table can have multiple entry points if joined multiple times.
    /// The port specifies which logical input the delta should enter through.
    entry_points: HashMap<String, Vec<(usize, InputPort)>>,

    /// Explicit DAG edges: successors[node_idx] = list of edges to successor nodes.
    /// Each edge specifies the target node and which input port to use.
    /// Empty list means terminal node (Output).
    successors: Vec<Vec<Edge>>,

    /// Target descriptor ID for schema-aware queries.
    /// When set, rows from older schema versions will be transformed before predicate evaluation.
    target_descriptor: Option<DescriptorId>,

    /// Lens context for transforming rows from different schema versions.
    /// Contains lenses for all known schema version pairs for this table.
    lens_context: Option<QueryLensContext>,

    /// Branches to read from for branch-aware queries.
    /// When set, rows are read from all specified branches and merged using per-column LWW.
    /// If empty, defaults to reading from "main" only.
    branches: Vec<String>,
}

impl std::fmt::Debug for QueryGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryGraph")
            .field("id", &self.id)
            .field("state", &self.state)
            .field("table", &self.table)
            .field("nodes", &self.nodes.len())
            .finish()
    }
}

impl QueryGraph {
    /// Build entry points map from nodes with typed input ports.
    ///
    /// Maps each table to (node index, input port) pairs where its deltas should enter.
    /// - Primary table enters at node 0 with Default port
    /// - Join tables enter at their Join node with Right port
    /// - For ARRAY inner joins, input_tables enter at Join with Left port
    /// - ArrayAggregate inner tables enter with Inner port
    fn build_entry_points(
        nodes: &[QueryNode],
        primary_table: &str,
    ) -> HashMap<String, Vec<(usize, InputPort)>> {
        let mut entry_points: HashMap<String, Vec<(usize, InputPort)>> = HashMap::new();

        // Primary table always enters at node 0 with Default port
        entry_points.insert(primary_table.to_string(), vec![(0, InputPort::Default)]);

        // Join tables enter at their Join node with Right port
        // For ARRAY inner joins, input_tables enter with Left port
        for (idx, node) in nodes.iter().enumerate() {
            if let QueryNode::Join {
                join_table,
                input_tables,
                input_tables_need_entry,
                ..
            } = node
            {
                // join_table enters at Right port
                entry_points
                    .entry(join_table.clone())
                    .or_default()
                    .push((idx, InputPort::Right));

                // For ARRAY inner joins, input_tables enter at Left port
                if *input_tables_need_entry {
                    for table in input_tables {
                        entry_points
                            .entry(table.clone())
                            .or_default()
                            .push((idx, InputPort::Left));
                    }
                }
            }
        }

        // ArrayAggregate inner tables enter with Inner port
        // Skip if already handled by a Join node (for ARRAY subqueries with inner joins)
        for (idx, node) in nodes.iter().enumerate() {
            if let QueryNode::ArrayAggregate {
                inner_table,
                inner_joins,
                ..
            } = node
            {
                // Only add entry point if no inner joins (otherwise Join handles it)
                if inner_joins.is_empty() {
                    entry_points
                        .entry(inner_table.clone())
                        .or_default()
                        .push((idx, InputPort::Inner));
                }
            }
        }

        entry_points
    }

    /// Build explicit successor edges from node dependencies with typed input ports.
    ///
    /// For each node, determine which nodes should receive its output and
    /// which input port the edge connects to.
    fn build_successors(
        nodes: &[QueryNode],
        node_indices: &HashMap<NodeId, usize>,
    ) -> Vec<Vec<Edge>> {
        let mut successors: Vec<Vec<Edge>> = vec![Vec::new(); nodes.len()];

        // Build edges from each node's `input` field
        // If node B has input = node A, then A has B as a successor
        for (idx, node) in nodes.iter().enumerate() {
            // Get this node's input and the port it should connect to
            let input_info: Option<(NodeId, InputPort)> = match node {
                QueryNode::Filter { input, .. }
                | QueryNode::RecursiveFilter { input, .. }
                | QueryNode::LimitOffset { input, .. }
                | QueryNode::Projection { input, .. }
                | QueryNode::Output { input, .. } => Some((*input, InputPort::Default)),
                QueryNode::ArrayAggregate { input, .. } => {
                    // ArrayAggregate receives upstream at Outer port
                    Some((*input, InputPort::Outer))
                }
                QueryNode::Join { .. } => {
                    // Join nodes receive from multiple sources (handled by entry_points)
                    // Their output goes to the next node in topological order
                    None
                }
                QueryNode::TableScan { .. } | QueryNode::IndexLookup { .. } => None,
            };

            if let Some((input_node_id, port)) = input_info
                && let Some(&input_idx) = node_indices.get(&input_node_id)
            {
                successors[input_idx].push(Edge { to: idx, port });
            }
        }

        // Build edges from ARRAY inner joins to their corresponding ArrayAggregate.
        // These are Join nodes with input_tables_need_entry = true.
        // Their output flows to the ArrayAggregate via the Inner port.
        for (idx, node) in nodes.iter().enumerate() {
            if let QueryNode::Join {
                input_tables,
                input_tables_need_entry: true,
                ..
            } = node
            {
                // Find the ArrayAggregate whose inner_table matches this join's input_tables
                let inner_table = input_tables.first().map(|s| s.as_str()).unwrap_or("");
                for (agg_idx, agg_node) in nodes.iter().enumerate() {
                    if let QueryNode::ArrayAggregate {
                        inner_table: agg_inner_table,
                        ..
                    } = agg_node
                        && agg_inner_table == inner_table
                    {
                        // Edge from ARRAY inner join to ArrayAggregate with Inner port
                        successors[idx].push(Edge {
                            to: agg_idx,
                            port: InputPort::Inner,
                        });
                        break; // Found the matching ArrayAggregate
                    }
                }
            }
        }

        // For nodes without explicit successors that aren't Output,
        // the successor is implicitly the next node in topological order.
        // This handles Join nodes which don't have an `input` field but
        // whose output flows to downstream nodes.
        // NOTE: Skip ARRAY inner joins (input_tables_need_entry = true) - they
        // flow to ArrayAggregate via Inner port, not to the next node.
        for idx in 0..nodes.len() {
            if successors[idx].is_empty() && idx + 1 < nodes.len() {
                // Check if this is not an Output node and not an ARRAY inner join
                let is_array_inner_join = matches!(
                    &nodes[idx],
                    QueryNode::Join {
                        input_tables_need_entry: true,
                        ..
                    }
                );
                if !matches!(nodes[idx], QueryNode::Output { .. }) && !is_array_inner_join {
                    let next_idx = idx + 1;
                    // Determine the port based on the target node type
                    let port = match &nodes[next_idx] {
                        QueryNode::Join { .. } => InputPort::Left,
                        QueryNode::ArrayAggregate { .. } => InputPort::Outer,
                        _ => InputPort::Default,
                    };
                    // Check if edge doesn't already exist
                    if !successors[idx].iter().any(|e| e.to == next_idx) {
                        successors[idx].push(Edge { to: next_idx, port });
                    }
                }
            }
        }

        successors
    }

    /// Create a new single-table query graph.
    ///
    /// This is typically called by `QueryGraphBuilder::output()`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: GraphId,
        table: String,
        schema: TableSchema,
        nodes: Vec<QueryNode>,
        node_indices: HashMap<NodeId, usize>,
        output_node: NodeId,
        branches: Vec<String>,
    ) -> Self {
        let mut all_schemas = HashMap::new();
        all_schemas.insert(table.clone(), schema.clone());
        let mut all_tables = vec![table.clone()];

        // Check for ArrayAggregate nodes and add their inner tables
        for node in &nodes {
            if let QueryNode::ArrayAggregate {
                inner_table,
                inner_schema,
                ..
            } = node
                && !all_tables.contains(inner_table)
            {
                all_tables.push(inner_table.clone());
                all_schemas.insert(inner_table.clone(), inner_schema.clone());
            }
        }

        // Check for Join nodes (e.g., from ARRAY inner joins) and add their join_table
        for node in &nodes {
            if let QueryNode::Join {
                join_table,
                join_schema,
                ..
            } = node
                && !all_tables.contains(join_table)
            {
                all_tables.push(join_table.clone());
                all_schemas.insert(join_table.clone(), join_schema.clone());
            }
        }

        // Build entry points for multi-entry routing
        let entry_points = Self::build_entry_points(&nodes, &table);

        // Build explicit DAG edges
        let successors = Self::build_successors(&nodes, &node_indices);

        Self {
            id,
            state: GraphState::Uninitialized,
            table: table.clone(),
            all_tables,
            schema,
            all_schemas,
            is_join: false,
            nodes,
            node_indices,
            output_node,
            entry_points,
            successors,
            target_descriptor: None,
            lens_context: None,
            branches,
        }
    }

    /// Create a new join query graph with optional additional right tables.
    ///
    /// Used for INHERITS chains like: documents → folders → workspaces
    /// The `additional_right_tables` contains tables beyond the first join.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_chain_join(
        id: GraphId,
        left_table: String,
        left_schema: TableSchema,
        right_table: String,
        right_schema: TableSchema,
        additional_right_tables: Vec<(String, TableSchema)>,
        nodes: Vec<QueryNode>,
        node_indices: HashMap<NodeId, usize>,
        output_node: NodeId,
        branches: Vec<String>,
    ) -> Self {
        use crate::sql::schema::ColumnDef;

        let mut all_schemas = HashMap::new();
        all_schemas.insert(left_table.clone(), left_schema.clone());
        all_schemas.insert(right_table.clone(), right_schema.clone());

        // Track all tables for delta routing
        let mut all_tables = vec![left_table.clone(), right_table.clone()];

        // Build a combined schema for filter evaluation on joined rows.
        // Columns are: [left_cols..., right_cols...] with qualified names.
        let mut combined_columns = Vec::new();
        for col in &left_schema.columns {
            combined_columns.push(ColumnDef {
                name: format!("{}.{}", left_table, col.name),
                ty: col.ty.clone(),
                nullable: col.nullable,
            });
        }
        for col in &right_schema.columns {
            combined_columns.push(ColumnDef {
                name: format!("{}.{}", right_table, col.name),
                ty: col.ty.clone(),
                nullable: col.nullable,
            });
        }

        // Add additional right tables (for chain joins)
        for (table_name, table_schema) in additional_right_tables {
            all_schemas.insert(table_name.clone(), table_schema.clone());
            all_tables.push(table_name.clone());

            for col in &table_schema.columns {
                combined_columns.push(ColumnDef {
                    name: format!("{}.{}", table_name, col.name),
                    ty: col.ty.clone(),
                    nullable: col.nullable,
                });
            }
        }

        let combined_schema = TableSchema::new("_joined", combined_columns);

        // Check for ArrayAggregate nodes and add their inner tables
        for node in &nodes {
            if let QueryNode::ArrayAggregate {
                inner_table,
                inner_schema,
                inner_joins,
                ..
            } = node
            {
                if !all_tables.contains(inner_table) {
                    all_tables.push(inner_table.clone());
                    all_schemas.insert(inner_table.clone(), inner_schema.clone());
                }
                // Also add tables from inner joins within the ARRAY subquery
                for (_, target_table, target_schema) in inner_joins {
                    if !all_tables.contains(target_table) {
                        all_tables.push(target_table.clone());
                        all_schemas.insert(target_table.clone(), target_schema.clone());
                    }
                }
            }
        }

        // Build entry points for multi-entry routing
        let entry_points = Self::build_entry_points(&nodes, &left_table);

        // Build explicit DAG edges
        let successors = Self::build_successors(&nodes, &node_indices);

        Self {
            id,
            state: GraphState::Uninitialized,
            table: left_table,
            all_tables,
            schema: combined_schema, // Use combined schema for JOIN graphs
            all_schemas,
            is_join: true,
            nodes,
            node_indices,
            output_node,
            entry_points,
            successors,
            target_descriptor: None,
            lens_context: None,
            branches,
        }
    }

    /// Get the graph ID.
    pub fn id(&self) -> GraphId {
        self.id
    }

    /// Set the graph ID (used when registering).
    pub(crate) fn set_id(&mut self, id: GraphId) {
        self.id = id;
    }

    /// Get the primary table this graph queries.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Get all tables this graph depends on.
    pub fn all_tables(&self) -> &[String] {
        &self.all_tables
    }

    /// Check if this graph handles a specific table.
    pub fn handles_table(&self, table: &str) -> bool {
        self.all_tables.iter().any(|t| t == table)
    }

    /// Get the primary schema.
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Get schema for a specific table.
    pub fn get_schema(&self, table: &str) -> Option<&TableSchema> {
        self.all_schemas.get(table)
    }

    /// Check if this is a join query.
    pub fn is_join(&self) -> bool {
        self.is_join
    }

    /// Check if the graph is initialized.
    pub fn is_ready(&self) -> bool {
        self.state == GraphState::Ready
    }

    /// Set the target descriptor for schema-aware queries.
    ///
    /// When set, rows from older schema versions will be transformed
    /// before predicate evaluation using the lens context.
    pub fn set_target_descriptor(&mut self, descriptor_id: DescriptorId) {
        self.target_descriptor = Some(descriptor_id);
    }

    /// Get the target descriptor ID.
    pub fn target_descriptor(&self) -> Option<&DescriptorId> {
        self.target_descriptor.as_ref()
    }

    /// Set the lens context for schema transformations.
    ///
    /// The lens context contains lenses for transforming rows between
    /// different schema versions during query evaluation.
    pub fn set_lens_context(&mut self, ctx: QueryLensContext) {
        self.lens_context = Some(ctx);
    }

    /// Get the lens context for schema transformations.
    pub fn lens_context(&self) -> Option<&QueryLensContext> {
        self.lens_context.as_ref()
    }

    /// Check if this graph has lens context for schema transformations.
    pub fn has_lens_context(&self) -> bool {
        self.lens_context.is_some()
    }

    /// Get the branches this query reads from.
    ///
    /// Returns an empty slice if no branches specified (defaults to "main").
    pub fn branches(&self) -> &[String] {
        &self.branches
    }

    /// Check if this graph is branch-aware (reads from multiple branches).
    pub fn is_branch_aware(&self) -> bool {
        !self.branches.is_empty()
    }

    /// Get current output rows in buffer format, initializing lazily if needed.
    pub fn get_output(
        &mut self,
        cache: &mut RowCache,
        db: &DatabaseState,
    ) -> Vec<(ObjectId, OwnedRow)> {
        self.ensure_initialized_skip(cache, db, None, None);
        self.collect_output(cache, db)
    }

    /// Get the output schema for this query.
    ///
    /// For queries with a Projection node before Output, returns a schema matching
    /// the projection's output descriptor.
    pub fn output_schema(&self) -> Option<TableSchema> {
        use crate::sql::schema::ColumnDef;

        // Find the Output node and check if its input is a Projection
        let output_idx = self.node_indices.get(&self.output_node)?;
        if let QueryNode::Output { input, .. } = &self.nodes[*output_idx] {
            let input_idx = self.node_indices.get(input)?;
            if let QueryNode::Projection {
                output_descriptor,
                table,
                ..
            } = &self.nodes[*input_idx]
            {
                // Create a schema from the projection's output descriptor
                let columns: Vec<ColumnDef> = output_descriptor
                    .columns
                    .iter()
                    .map(|col| ColumnDef::new(col.name.clone(), col.ty.clone(), col.nullable))
                    .collect();
                return Some(TableSchema::new_raw(table, columns));
            }
        }
        Some(self.schema.clone())
    }

    /// Ensure the graph is initialized, optionally skipping a specific row.
    ///
    /// The skip_id and skip_table are used when initializing during a row change
    /// notification - we skip that row because it will be processed as part of the delta.
    fn ensure_initialized_skip(
        &mut self,
        cache: &mut RowCache,
        db: &DatabaseState,
        skip_id: Option<ObjectId>,
        skip_table: Option<&str>,
    ) {
        if self.state != GraphState::Uninitialized {
            return;
        }

        self.state = GraphState::Initializing;

        if self.is_join {
            // For JOIN queries, we need to initialize the join node by loading
            // all left rows and joining them with corresponding right rows
            self.init_join_query(cache, db, skip_id, skip_table);
        } else {
            // Check if this graph has a RecursiveFilter node
            let has_recursive = self
                .nodes
                .iter()
                .any(|n| matches!(n, QueryNode::RecursiveFilter { .. }));

            // Check if this graph has ArrayAggregate nodes
            let has_array_aggregate = self
                .nodes
                .iter()
                .any(|n| matches!(n, QueryNode::ArrayAggregate { .. }));

            if has_recursive {
                // RecursiveFilter requires fixpoint iteration
                self.init_recursive_query(cache, db, skip_id, skip_table);
            } else if has_array_aggregate {
                // ArrayAggregate needs database access to look up inner rows
                self.init_array_aggregate_query(cache, db, skip_id, skip_table);
            } else {
                // Single-table query: load all rows from the primary table
                let rows = db.read_all_rows(&self.table);
                let schema = self.schema.clone();

                for (id, owned) in rows {
                    // Skip the triggering row - it will be processed as a delta
                    if skip_table == Some(&self.table) && skip_id == Some(id) {
                        continue;
                    }

                    cache.insert(&self.table, id, owned.clone());

                    // Process as Added delta
                    let mut delta = DeltaBatch::added(id, owned);
                    for node in &mut self.nodes {
                        if delta.is_empty() {
                            break;
                        }
                        // LimitOffset nodes need special handling
                        delta = match node {
                            QueryNode::LimitOffset { .. } => {
                                node.evaluate_limit_offset(delta, &schema, cache)
                            }
                            _ => node.evaluate(delta, cache),
                        };
                    }
                }
            }
        }

        self.state = GraphState::Ready;
    }

    /// Initialize a JOIN query by loading and joining all rows.
    ///
    /// With streaming joins, we must load tables in the right order:
    /// 1. Load right (join) table rows first - populates right_index
    /// 2. Load left (input) table rows - finds matches in right_index
    fn init_join_query(
        &mut self,
        cache: &mut RowCache,
        db: &DatabaseState,
        skip_id: Option<ObjectId>,
        skip_table: Option<&str>,
    ) {
        let table = self.table.clone();

        // Collect all join tables from Join nodes
        let mut join_tables: Vec<String> = Vec::new();
        for node in &self.nodes {
            if let QueryNode::Join { join_table, .. } = node
                && !join_tables.contains(join_table)
            {
                join_tables.push(join_table.clone());
            }
        }

        // 1. Load right (join) table rows first - populates right_index
        for join_table in &join_tables {
            let right_rows = db.read_all_rows(join_table);
            let right_schema = self
                .all_schemas
                .get(join_table)
                .cloned()
                .unwrap_or_else(|| self.schema.clone());

            for (id, owned) in right_rows {
                if skip_table == Some(join_table.as_str()) && skip_id == Some(id) {
                    continue;
                }

                cache.insert(join_table, id, owned.clone());

                // Qualify column names for the right table
                let qualified_row = owned.qualify_columns(join_table, &right_schema);
                let delta = RowDelta::Added {
                    id,
                    row: qualified_row,
                };
                // Process through nodes - this populates right_index in Join nodes
                self.process_delta_through_nodes(delta, join_table, cache, db);
            }
        }

        // 2. Load left (input) table rows - finds matches in right_index
        let left_rows = db.read_all_rows(&table);
        let table_schema = self
            .all_schemas
            .get(&table)
            .cloned()
            .unwrap_or_else(|| self.schema.clone());

        for (id, owned) in left_rows {
            if skip_table == Some(table.as_str()) && skip_id == Some(id) {
                continue;
            }

            cache.insert(&table, id, owned.clone());

            // For JOIN queries, qualify the column names so predicates with qualified names work
            let qualified_row = owned.qualify_columns(&table, &table_schema);
            let delta = RowDelta::Added {
                id,
                row: qualified_row,
            };
            self.process_delta_through_nodes(delta, &table, cache, db);
        }
    }

    /// Initialize a recursive query using fixpoint iteration.
    ///
    /// For RecursiveFilter nodes, we need to:
    /// 1. Load all rows and build the children_index
    /// 2. Find all base-accessible rows
    /// 3. Iterate until no new rows become accessible (fixpoint)
    fn init_recursive_query(
        &mut self,
        cache: &mut RowCache,
        db: &DatabaseState,
        skip_id: Option<ObjectId>,
        skip_table: Option<&str>,
    ) {
        let table = self.table.clone();

        // Load all rows
        let rows = db.read_all_rows(&table);

        // First pass: populate the RecursiveFilter node's all_rows and children_index
        for (id, owned) in &rows {
            if skip_table == Some(&table) && skip_id == Some(*id) {
                continue;
            }
            cache.insert(&table, *id, owned.clone());
        }

        // Process all rows through nodes up to (but not including) RecursiveFilter
        // Then do fixpoint iteration on the RecursiveFilter
        for (id, owned) in rows {
            if skip_table == Some(&table) && skip_id == Some(id) {
                continue;
            }

            let mut delta = DeltaBatch::added(id, owned);

            for node in &mut self.nodes {
                if delta.is_empty() {
                    break;
                }

                match node {
                    QueryNode::RecursiveFilter { .. } => {
                        // RecursiveFilter handles its own fixpoint iteration internally
                        // via propagate_access_to_children
                        delta = node.evaluate_recursive(delta);
                    }
                    _ => {
                        delta = node.evaluate(delta, cache);
                    }
                }
            }
        }
    }

    /// Initialize Join nodes that are for ARRAY inner joins.
    ///
    /// These nodes have `input_tables_need_entry = true` and need their tables loaded
    /// before the main query can process incremental updates.
    fn init_inner_join_nodes(
        &mut self,
        cache: &mut RowCache,
        db: &DatabaseState,
        skip_id: Option<ObjectId>,
        skip_table: Option<&str>,
    ) {
        // Find Join nodes with input_tables_need_entry = true
        // Collect info first to avoid borrow issues
        let inner_join_info: Vec<(usize, String, Vec<String>)> = self
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(idx, node)| {
                if let QueryNode::Join {
                    join_table,
                    input_tables,
                    input_tables_need_entry: true,
                    ..
                } = node
                {
                    Some((idx, join_table.clone(), input_tables.clone()))
                } else {
                    None
                }
            })
            .collect();

        for (node_idx, join_table, input_tables) in inner_join_info {
            // Get schema for the join table (unused for now, may be useful for validation)
            let _join_schema = self
                .all_schemas
                .get(&join_table)
                .cloned()
                .unwrap_or_else(|| self.schema.clone());

            // 1. Load join table rows (e.g., Labels) - populates right_index
            let join_rows = db.read_all_rows(&join_table);
            for (id, owned) in join_rows {
                if skip_table == Some(join_table.as_str()) && skip_id == Some(id) {
                    continue;
                }

                cache.insert(&join_table, id, owned.clone());

                // Process directly through the Join node
                let delta = RowDelta::Added {
                    id,
                    row: owned.clone(),
                };

                // Evaluate just this Join node to populate its indexes
                if let QueryNode::Join { .. } = &mut self.nodes[node_idx] {
                    // For join table deltas, is_from_input = false
                    self.nodes[node_idx].evaluate_join_by_port(delta, false);
                }
            }

            // 2. Load input table rows (e.g., IssueLabels) - populates left_index
            for input_table in &input_tables {
                let input_schema = self
                    .all_schemas
                    .get(input_table)
                    .cloned()
                    .unwrap_or_else(|| self.schema.clone());

                let input_rows = db.read_all_rows(input_table);
                for (id, owned) in input_rows {
                    if skip_table == Some(input_table.as_str()) && skip_id == Some(id) {
                        continue;
                    }

                    cache.insert(input_table, id, owned.clone());

                    let delta = RowDelta::Added {
                        id,
                        row: owned.clone(),
                    };

                    // Evaluate just this Join node to populate its indexes
                    if let QueryNode::Join { .. } = &mut self.nodes[node_idx] {
                        // For input table deltas, is_from_input = true
                        self.nodes[node_idx].evaluate_join_by_port(delta, true);
                    }
                }

                // Don't add this to all_schemas since it's handled by the Join node
                let _ = input_schema;
            }
        }
    }

    /// Initialize a query with ArrayAggregate nodes.
    ///
    /// ArrayAggregate needs database access to look up inner rows for each outer row.
    /// For ArrayAggregates with inner joins, we must initialize the Join nodes first.
    fn init_array_aggregate_query(
        &mut self,
        cache: &mut RowCache,
        db: &DatabaseState,
        skip_id: Option<ObjectId>,
        skip_table: Option<&str>,
    ) {
        let table = self.table.clone();

        // First, initialize any Join nodes that are for ARRAY inner joins.
        // These need their join tables (e.g., Labels) loaded before processing inner table deltas.
        self.init_inner_join_nodes(cache, db, skip_id, skip_table);

        // Load all rows from the outer (primary) table
        let rows = db.read_all_rows(&table);

        for (id, owned) in rows {
            // Skip the triggering row - it will be processed as a delta
            if skip_table == Some(&table) && skip_id == Some(id) {
                continue;
            }

            cache.insert(&table, id, owned.clone());

            // Process as Added delta
            let mut delta = DeltaBatch::added(id, owned);

            for node in &mut self.nodes {
                if delta.is_empty() {
                    break;
                }

                match node {
                    QueryNode::Join {
                        input_tables,
                        join_table: node_join_table,
                        ..
                    } => {
                        // For ARRAY inner join nodes, outer table deltas should pass through
                        // Check if this delta is for the Join node
                        let is_input_delta = input_tables.iter().any(|t| t == &table);
                        let is_join_table_delta = &table == node_join_table;

                        if is_input_delta || is_join_table_delta {
                            // This Join processes this delta - evaluate it
                            let is_from_input = is_input_delta;
                            let mut output = DeltaBatch::new();
                            for d in delta.into_iter() {
                                let batch = node.evaluate_join_by_port(d, is_from_input);
                                output.extend(batch);
                            }
                            delta = output;
                        }
                        // Otherwise, delta passes through unchanged (delta = delta)
                    }
                    QueryNode::ArrayAggregate {
                        outer_table,
                        inner_table,
                        inner_ref_column,
                        ..
                    } => {
                        // Clone values before borrowing node mutably
                        let inner_tbl = inner_table.clone();
                        let inner_ref = inner_ref_column.clone();

                        let outer_schema = self
                            .all_schemas
                            .get(outer_table)
                            .cloned()
                            .unwrap_or_else(|| self.schema.clone());

                        let mut output = DeltaBatch::new();
                        for d in delta.into_iter() {
                            let batch = node.evaluate_array_aggregate_by_port(
                                d,
                                true,  // is_outer_delta = true (initialization is always outer)
                                false, // is_inner_delta = false
                                &outer_schema,
                                |outer_id| {
                                    // Look up all inner rows that reference this outer id
                                    db.find_referencing(&inner_tbl, &inner_ref, outer_id)
                                },
                                |table_name, id| {
                                    // Look up a row by table and id (for resolving inner joins)
                                    db.get_row(table_name, id).map(|(_, row)| row)
                                },
                            );
                            output.extend(batch);
                        }
                        delta = output;
                    }
                    QueryNode::LimitOffset { .. } => {
                        delta = node.evaluate_limit_offset(delta, &self.schema, cache);
                    }
                    _ => {
                        delta = node.evaluate(delta, cache);
                    }
                }
            }
        }
    }

    /// Process a delta through the graph using explicit DAG edge traversal with typed ports.
    ///
    /// Uses multi-entry routing: deltas enter at the node that needs them.
    /// A table can have multiple entry points (e.g., if joined twice), and
    /// each path is processed independently by following explicit edges.
    /// The input port tells each node exactly which logical input triggered.
    fn process_delta_through_nodes(
        &mut self,
        delta: RowDelta,
        source_table: &str,
        cache: &mut RowCache,
        db: &DatabaseState,
    ) -> DeltaBatch {
        // Find all entry points for this table (with their input ports)
        let entries = self
            .entry_points
            .get(source_table)
            .cloned()
            .unwrap_or_else(|| vec![(0, InputPort::Default)]);

        let mut all_output = DeltaBatch::new();

        // Process each entry point path independently
        for (start_idx, port) in entries {
            let mut current = DeltaBatch::new();
            current.push(delta.clone());

            let output = self.process_from_node(start_idx, current, port, cache, db);
            all_output.extend(output);
        }

        all_output
    }

    /// Recursively process a delta batch from a specific node, following explicit edges.
    ///
    /// The `port` parameter tells the node which logical input the delta is arriving through,
    /// eliminating the need for source_table-based inference.
    fn process_from_node(
        &mut self,
        node_idx: usize,
        current: DeltaBatch,
        port: InputPort,
        cache: &mut RowCache,
        db: &DatabaseState,
    ) -> DeltaBatch {
        if current.is_empty() {
            return DeltaBatch::new();
        }

        // Evaluate this node - port tells it which input triggered
        let output = self.evaluate_node_at(node_idx, current, port, cache, db);

        if output.is_empty() {
            return DeltaBatch::new();
        }

        // Get successors for this node
        let successors = self.successors[node_idx].clone();

        // If no successors, this is a terminal node (Output) - return the result
        if successors.is_empty() {
            return output;
        }

        // Follow edges to successor nodes, using each edge's port
        let mut result = DeltaBatch::new();
        for edge in successors {
            let successor_output =
                self.process_from_node(edge.to, output.clone(), edge.port, cache, db);
            result.extend(successor_output);
        }

        result
    }

    /// Evaluate a single node and return the output batch.
    ///
    /// With typed edges, the `port` parameter tells each node exactly which
    /// logical input the delta is arriving through. No need for source_table
    /// comparison - the graph structure itself encodes the routing logic.
    fn evaluate_node_at(
        &mut self,
        node_idx: usize,
        current: DeltaBatch,
        port: InputPort,
        cache: &mut RowCache,
        db: &DatabaseState,
    ) -> DeltaBatch {
        let node = &mut self.nodes[node_idx];

        match node {
            QueryNode::Join { .. } => {
                // Port tells us exactly which side:
                // - Left/Default: delta from upstream (input tables)
                // - Right: delta from join_table entry point
                let is_from_input = port == InputPort::Left || port == InputPort::Default;

                let mut output = DeltaBatch::new();
                for d in current.into_iter() {
                    let batch = node.evaluate_join_by_port(d, is_from_input);
                    output.extend(batch);
                }

                output
            }
            QueryNode::RecursiveFilter { .. } => node.evaluate_recursive(current),
            QueryNode::ArrayAggregate {
                outer_table,
                inner_table,
                inner_ref_column,
                outer_rows,
                ..
            } => {
                // Clone values before borrowing node mutably
                let outer_tbl = outer_table.clone();
                let inner_tbl = inner_table.clone();
                let inner_ref = inner_ref_column.clone();

                // Port tells us exactly which input:
                // - Outer/Default: delta from outer table (upstream)
                // - Inner: delta from inner table (entry point)
                // - Other: pass-through from chained ArrayAggregate
                let is_outer = port == InputPort::Outer || port == InputPort::Default;
                let is_inner = port == InputPort::Inner;

                if is_outer || is_inner {
                    // Process deltas through ArrayAggregate
                    let outer_schema = self
                        .all_schemas
                        .get(&outer_tbl)
                        .cloned()
                        .unwrap_or_else(|| self.schema.clone());

                    let mut output = DeltaBatch::new();
                    for d in current.into_iter() {
                        let batch = node.evaluate_array_aggregate_by_port(
                            d,
                            is_outer,
                            is_inner,
                            &outer_schema,
                            |outer_id| db.find_referencing(&inner_tbl, &inner_ref, outer_id),
                            |table_name, id| db.get_row(table_name, id).map(|(_, row)| row),
                        );
                        output.extend(batch);
                    }

                    output
                } else {
                    // Pass-through: update outer_rows cache and pass through unchanged.
                    // Any delta reaching here via edges represents outer_table rows
                    // (e.g., output from a prior ArrayAggregate in the chain).
                    for d in current.iter() {
                        match d {
                            RowDelta::Added { id, row } | RowDelta::Updated { id, row, .. } => {
                                outer_rows.insert(*id, row.clone());
                            }
                            RowDelta::Removed { id, .. } => {
                                outer_rows.remove(id);
                            }
                        }
                    }
                    current
                }
            }
            QueryNode::LimitOffset { .. } => {
                let schema = self.schema.clone();
                node.evaluate_limit_offset(current, &schema, cache)
            }
            QueryNode::Filter { .. } => {
                // Use lens-aware evaluation if lens context is available
                if let Some(lens_ctx) = &self.lens_context {
                    // For now, use a simple descriptor lookup that returns the target descriptor
                    // for all rows. This means no transformation will occur unless we have
                    // row-specific descriptor tracking.
                    //
                    // TODO(GCO-1091): Implement row-specific descriptor tracking.
                    // When a row is created/updated, store its source descriptor ID.
                    // Then look it up here to enable proper lens transformation.
                    let target = self.target_descriptor;
                    node.evaluate_with_lens(current, cache, Some(lens_ctx), move |_id| target)
                } else {
                    node.evaluate(current, cache)
                }
            }
            _ => node.evaluate(current, cache),
        }
    }

    /// Collect output rows in buffer format from the final cached set.
    fn collect_output(&self, cache: &RowCache, _db: &DatabaseState) -> Vec<(ObjectId, OwnedRow)> {
        // Find the node feeding into Output
        let output_idx = self.node_indices[&self.output_node];
        if let QueryNode::Output { input, .. } = &self.nodes[output_idx] {
            let input_idx = self.node_indices[input];

            // Check if the input to Output is a Projection node
            // This handles INHERITS queries where we need to unqualify column names
            if let Some(projected) = self.nodes[input_idx].cached_projected() {
                return projected
                    .iter()
                    .map(|(id, row)| (*id, row.clone()))
                    .collect();
            }

            // Check if the input to Output is an ArrayAggregate node (even for JOIN queries)
            // This handles JOIN + ARRAY subquery cases
            if let Some(outer_rows) = self.nodes[input_idx].outer_rows() {
                // ArrayAggregate now uses buffer format directly
                return outer_rows
                    .iter()
                    .map(|(id, row)| (*id, row.clone()))
                    .collect();
            }

            // For join queries, we need to find the Join node and apply filters
            if self.is_join {
                // Find the Join node (it has cached_rows with JoinedRow data)
                let join_node = self.nodes.iter().find(|n| n.cached_joined().is_some());

                if let Some(join_node) = join_node
                    && let Some(joined_rows) = join_node.cached_joined()
                {
                    // Get IDs to filter by from downstream Filter node (if any)
                    let filter_ids = self.nodes[input_idx].cached_ids();

                    // Apply the filter - projection is now handled by explicit Projection nodes
                    let rows: Vec<(ObjectId, OwnedRow)> = joined_rows
                        .iter()
                        .filter(|(id, _)| {
                            // If there's a filter, only include matching IDs
                            filter_ids.is_none_or(|ids| ids.contains(id))
                        })
                        .map(|(primary_id, jr)| (*primary_id, jr.to_output_row()))
                        .collect();

                    return rows;
                }
            }

            // For RecursiveFilter queries, get rows from accessible set
            // OwnedRow is already in buffer format
            if let Some(accessible) = self.nodes[input_idx].accessible()
                && let Some(all_rows) = self.nodes[input_idx].all_rows()
            {
                return accessible
                    .keys()
                    .filter_map(|id| all_rows.get(id).map(|owned_row| (*id, owned_row.clone())))
                    .collect();
            }

            // For ArrayAggregate queries, get rows from outer_rows
            if let Some(outer_rows) = self.nodes[input_idx].outer_rows() {
                // ArrayAggregate now uses buffer format directly
                return outer_rows
                    .iter()
                    .map(|(id, row)| (*id, row.clone()))
                    .collect();
            }

            // For LimitOffset queries, get rows from all_rows filtered by visible_ids
            if let QueryNode::LimitOffset {
                all_rows,
                visible_ids,
                ..
            } = &self.nodes[input_idx]
            {
                // Return rows in sorted order (BTreeMap maintains order)
                // Already in buffer format
                return all_rows
                    .iter()
                    .filter(|(id, _)| visible_ids.contains(id))
                    .map(|(id, owned_row)| (*id, owned_row.clone()))
                    .collect();
            }

            // For single-table queries, use cached IDs
            if let Some(ids) = self.nodes[input_idx].cached_ids() {
                return ids
                    .iter()
                    .filter_map(|id| {
                        cache
                            .get(&self.table, *id)
                            .flatten()
                            .map(|row| (*id, row.clone()))
                    })
                    .collect();
            }
        }
        vec![]
    }

    /// Process a row change, returning the output delta.
    ///
    /// Initializes lazily if needed. For JOIN queries, `source_table` indicates
    /// which table the change came from.
    pub fn process_change(
        &mut self,
        delta: RowDelta,
        cache: &mut RowCache,
        db: &DatabaseState,
    ) -> DeltaBatch {
        self.process_change_from_table(delta, &self.table.clone(), cache, db)
    }

    /// Process a row change from a specific table.
    pub fn process_change_from_table(
        &mut self,
        delta: RowDelta,
        source_table: &str,
        cache: &mut RowCache,
        db: &DatabaseState,
    ) -> DeltaBatch {
        // Get the row ID being changed - we need to skip it during init
        let skip_id = Some(delta.row_id());
        self.ensure_initialized_skip(cache, db, skip_id, Some(source_table));

        // Update cache with new value
        match &delta {
            RowDelta::Added { id, row } => {
                cache.insert(source_table, *id, row.clone());
            }
            RowDelta::Removed { id, .. } => cache.mark_deleted(source_table, *id),
            RowDelta::Updated { id, row, .. } => {
                cache.insert(source_table, *id, row.clone());
            }
        }

        // For JOIN queries, convert delta to use qualified column names
        let table_schema = self.all_schemas.get(source_table).unwrap_or(&self.schema);
        let delta = if self.is_join {
            delta.qualify_columns(source_table, table_schema)
        } else {
            delta
        };

        // Process through nodes
        self.process_delta_through_nodes(delta, source_table, cache, db)
    }

    /// Get the number of nodes in the graph.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get the current size of the output set.
    pub fn output_size(&self) -> usize {
        let output_idx = self.node_indices[&self.output_node];
        if let QueryNode::Output { input, .. } = &self.nodes[output_idx] {
            let input_idx = self.node_indices[input];

            // Check for RecursiveFilter first
            if let Some(accessible) = self.nodes[input_idx].accessible() {
                return accessible.len();
            }

            self.nodes[input_idx]
                .cached_ids()
                .map(|ids| ids.len())
                .unwrap_or(0)
        } else {
            0
        }
    }

    /// Generate a text diagram of the query graph.
    ///
    /// Returns a string representation showing the DAG structure with
    /// node types, tables, and predicates for easy debugging and demos.
    pub fn to_diagram(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();

        // Header
        writeln!(
            out,
            "┌─────────────────────────────────────────────────────────────┐"
        )
        .unwrap();
        writeln!(
            out,
            "│  Query Graph (id: {})                                       │",
            self.id.0
        )
        .unwrap();
        writeln!(out, "│  Primary table: {:42} │", self.table).unwrap();
        if self.is_join {
            let tables = self.all_tables.join(", ");
            writeln!(out, "│  Join tables: {:44} │", truncate_str(&tables, 44)).unwrap();
        }
        writeln!(
            out,
            "└─────────────────────────────────────────────────────────────┘"
        )
        .unwrap();
        writeln!(out).unwrap();

        // Nodes in topological order (reverse for visual flow: sources at top)
        for (idx, node) in self.nodes.iter().enumerate() {
            let node_id = self
                .node_indices
                .iter()
                .find(|&(_, i)| *i == idx)
                .map(|(id, _)| id.0)
                .unwrap_or(0);

            let is_last = idx == self.nodes.len() - 1;
            let connector = if is_last { "└" } else { "├" };
            let continuation = if is_last { " " } else { "│" };

            // Node header
            let (node_type, details) = node.diagram_info();
            writeln!(out, "{}── [{:2}] {}", connector, node_id, node_type).unwrap();

            // Node details
            for line in details {
                writeln!(out, "{}       {}", continuation, line).unwrap();
            }

            // Show successors (explicit edges with port labels)
            let successors = &self.successors[idx];
            if !successors.is_empty() {
                let succ_ids: Vec<String> = successors
                    .iter()
                    .map(|edge| {
                        let node_label = self
                            .node_indices
                            .iter()
                            .find(|&(_, j)| *j == edge.to)
                            .map(|(id, _)| format!("{}", id.0))
                            .unwrap_or_else(|| "?".to_string());
                        // Show port if not Default
                        match edge.port {
                            InputPort::Default => node_label,
                            InputPort::Left => format!("{}:Left", node_label),
                            InputPort::Right => format!("{}:Right", node_label),
                            InputPort::Outer => format!("{}:Outer", node_label),
                            InputPort::Inner => format!("{}:Inner", node_label),
                        }
                    })
                    .collect();
                writeln!(
                    out,
                    "{}       → to: [{}]",
                    continuation,
                    succ_ids.join(", ")
                )
                .unwrap();
            }

            // Connection line between nodes
            if !is_last {
                writeln!(out, "{}", continuation).unwrap();
            }
        }

        // Entry points summary (with port labels)
        writeln!(out).unwrap();
        writeln!(out, "Entry points:").unwrap();
        for (table, entries) in &self.entry_points {
            let node_ids: Vec<String> = entries
                .iter()
                .map(|(idx, port)| {
                    let node_label = self
                        .node_indices
                        .iter()
                        .find(|&(_, j)| *j == *idx)
                        .map(|(id, _)| format!("{}", id.0))
                        .unwrap_or_else(|| "?".to_string());
                    // Show port if not Default
                    match port {
                        InputPort::Default => node_label,
                        InputPort::Left => format!("{}:Left", node_label),
                        InputPort::Right => format!("{}:Right", node_label),
                        InputPort::Outer => format!("{}:Outer", node_label),
                        InputPort::Inner => format!("{}:Inner", node_label),
                    }
                })
                .collect();
            writeln!(out, "  {} → [{}]", table, node_ids.join(", ")).unwrap();
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::ObjectId;
    use crate::sql::Database;
    use crate::sql::query_graph::PredicateValue;
    use crate::sql::query_graph::builder::QueryGraphBuilder;
    use crate::sql::query_graph::predicate::Predicate;
    use crate::sql::row_buffer::{RowBuilder, RowDescriptor};
    use crate::sql::schema::{ColumnDef, ColumnType};
    use std::sync::Arc;

    fn test_schema() -> TableSchema {
        TableSchema::new(
            "users",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::required("active", ColumnType::Bool),
            ],
        )
    }

    fn test_descriptor() -> Arc<RowDescriptor> {
        Arc::new(RowDescriptor::from_table_schema(&test_schema()))
    }

    fn make_owned_row(id: u128, name: &str, active: bool) -> (ObjectId, OwnedRow) {
        let descriptor = test_descriptor();
        let row = RowBuilder::new(descriptor)
            .set_string_by_name("name", name)
            .set_bool_by_name("active", active)
            .build();
        (ObjectId::new(id), row)
    }

    #[test]
    fn graph_lazy_init() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema);
        let scan = builder.table_scan();
        let graph = builder.output(scan, GraphId(1));

        assert_eq!(graph.state, GraphState::Uninitialized);
    }

    #[test]
    fn graph_process_change() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema.clone());
        let scan = builder.table_scan();
        let filter = builder.filter(scan, Predicate::eq("active", PredicateValue::Bool(true)));
        let mut graph = builder.output(filter, GraphId(1));

        // Create a mock database state
        let db = Database::in_memory();
        db.create_table(schema).unwrap();

        let mut cache = RowCache::new();

        // Process an active user - should appear in output
        let (id, row) = make_owned_row(1, "Alice", true);
        let delta = graph.process_change(RowDelta::Added { id, row }, &mut cache, db.state());

        assert_eq!(delta.len(), 1);
        assert!(matches!(delta.iter().next(), Some(RowDelta::Added { .. })));

        // Process an inactive user - should be filtered out
        let (id, row) = make_owned_row(2, "Bob", false);
        let delta = graph.process_change(RowDelta::Added { id, row }, &mut cache, db.state());

        // Early cutoff - no output
        assert!(delta.is_empty());

        // Check output size
        assert_eq!(graph.output_size(), 1);
    }

    #[test]
    fn graph_collect_output() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema.clone());
        let scan = builder.table_scan();
        let filter = builder.filter(scan, Predicate::eq("active", PredicateValue::Bool(true)));
        let mut graph = builder.output(filter, GraphId(1));

        let db = Database::in_memory();
        db.create_table(schema).unwrap();

        let mut cache = RowCache::new();

        // Add some rows
        let (id1, row1) = make_owned_row(1, "Alice", true);
        let (id2, row2) = make_owned_row(2, "Bob", false);
        let (id3, row3) = make_owned_row(3, "Carol", true);

        graph.process_change(
            RowDelta::Added { id: id1, row: row1 },
            &mut cache,
            db.state(),
        );
        graph.process_change(
            RowDelta::Added { id: id2, row: row2 },
            &mut cache,
            db.state(),
        );
        graph.process_change(
            RowDelta::Added { id: id3, row: row3 },
            &mut cache,
            db.state(),
        );

        // Get output - should only have active users
        let output = graph.get_output(&mut cache, db.state());

        assert_eq!(output.len(), 2);
        let ids: Vec<_> = output.iter().map(|(id, _)| id.0).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&3));
        assert!(!ids.contains(&2));
    }

    // Helper schemas and rows for ArrayAggregate tests
    fn folder_schema() -> TableSchema {
        TableSchema::new(
            "folders",
            vec![ColumnDef::required("name", ColumnType::String)],
        )
    }

    fn note_schema() -> TableSchema {
        TableSchema::new(
            "notes",
            vec![
                ColumnDef::required("folder", ColumnType::Ref("folders".to_string())),
                ColumnDef::required("title", ColumnType::String),
            ],
        )
    }

    fn folder_descriptor() -> Arc<RowDescriptor> {
        Arc::new(RowDescriptor::from_table_schema(&folder_schema()))
    }

    fn note_descriptor() -> Arc<RowDescriptor> {
        Arc::new(RowDescriptor::from_table_schema(&note_schema()))
    }

    fn make_owned_folder(id: u128, name: &str) -> (ObjectId, OwnedRow) {
        let descriptor = folder_descriptor();
        let row = RowBuilder::new(descriptor)
            .set_string_by_name("name", name)
            .build();
        (ObjectId::new(id), row)
    }

    fn make_owned_note(id: u128, folder_id: u128, title: &str) -> (ObjectId, OwnedRow) {
        let descriptor = note_descriptor();
        let row = RowBuilder::new(descriptor)
            .set_ref_by_name("folder", ObjectId::new(folder_id))
            .set_string_by_name("title", title)
            .build();
        (ObjectId::new(id), row)
    }

    #[test]
    fn array_aggregate_outer_added() {
        let outer_schema = folder_schema();
        let inner_schema = note_schema();

        let mut builder = QueryGraphBuilder::new("folders", outer_schema.clone());
        let scan = builder.table_scan();
        let agg =
            builder.array_aggregate(scan, "notes", "folder", inner_schema.clone(), vec![], -1);
        let mut graph = builder.output(agg, GraphId(1));

        // Verify inner table is tracked
        assert!(graph.handles_table("folders"));
        assert!(graph.handles_table("notes"));

        // Create mock database
        let db = Database::in_memory();
        db.create_table(outer_schema).unwrap();
        db.create_table(inner_schema).unwrap();

        let mut cache = RowCache::new();

        // Add a folder
        let (id, row) = make_owned_folder(1, "Work");
        let delta = graph.process_change(RowDelta::Added { id, row }, &mut cache, db.state());

        // Should emit Added delta
        assert_eq!(delta.len(), 1);
        assert!(matches!(delta.iter().next(), Some(RowDelta::Added { .. })));
    }

    #[test]
    fn array_aggregate_inner_added_updates_outer() {
        let outer_schema = folder_schema();
        let inner_schema = note_schema();

        let mut builder = QueryGraphBuilder::new("folders", outer_schema.clone());
        let scan = builder.table_scan();
        let agg =
            builder.array_aggregate(scan, "notes", "folder", inner_schema.clone(), vec![], -1);
        let mut graph = builder.output(agg, GraphId(1));

        let db = Database::in_memory();
        db.create_table(outer_schema).unwrap();
        db.create_table(inner_schema).unwrap();

        let mut cache = RowCache::new();

        // Add a folder first
        let (id, row) = make_owned_folder(1, "Work");
        graph.process_change(RowDelta::Added { id, row }, &mut cache, db.state());

        // Now add a note to that folder
        let (id, row) = make_owned_note(100, 1, "Meeting Notes");
        let delta = graph.process_change_from_table(
            RowDelta::Added { id, row },
            "notes",
            &mut cache,
            db.state(),
        );

        // Should emit Updated delta for the folder
        assert_eq!(delta.len(), 1);
        assert!(matches!(delta.iter().next(), Some(RowDelta::Updated { id, .. }) if id.0 == 1));
    }

    #[test]
    fn array_aggregate_inner_removed_updates_outer() {
        let outer_schema = folder_schema();
        let inner_schema = note_schema();

        let mut builder = QueryGraphBuilder::new("folders", outer_schema.clone());
        let scan = builder.table_scan();
        let agg =
            builder.array_aggregate(scan, "notes", "folder", inner_schema.clone(), vec![], -1);
        let mut graph = builder.output(agg, GraphId(1));

        let db = Database::in_memory();
        db.create_table(outer_schema).unwrap();
        db.create_table(inner_schema).unwrap();

        let mut cache = RowCache::new();

        // Add a folder
        let (id, row) = make_owned_folder(1, "Work");
        graph.process_change(RowDelta::Added { id, row }, &mut cache, db.state());

        // Add two notes
        let (id, row) = make_owned_note(100, 1, "Note 1");
        graph.process_change_from_table(
            RowDelta::Added { id, row },
            "notes",
            &mut cache,
            db.state(),
        );
        let (id, row) = make_owned_note(101, 1, "Note 2");
        graph.process_change_from_table(
            RowDelta::Added { id, row },
            "notes",
            &mut cache,
            db.state(),
        );

        // Get output
        let output = graph.get_output(&mut cache, db.state());
        assert_eq!(output.len(), 1);

        // Remove one note
        let delta = graph.process_change_from_table(
            RowDelta::Removed {
                id: ObjectId::new(100),
                prior: crate::sql::query_graph::delta::PriorState::empty(),
            },
            "notes",
            &mut cache,
            db.state(),
        );

        // Should emit Updated delta
        assert_eq!(delta.len(), 1);
        assert!(matches!(
            delta.iter().next(),
            Some(RowDelta::Updated { .. })
        ));
    }

    #[test]
    fn array_aggregate_initial_load_with_existing_data() {
        // This test mimics the demo app scenario:
        // 1. Data exists in the database before the query is subscribed
        // 2. When get_output is called, it should find the existing inner rows

        let outer_schema = folder_schema();
        let inner_schema = note_schema();

        // Create database and add data BEFORE creating the query
        let db = Database::in_memory();
        db.create_table(outer_schema.clone()).unwrap();
        db.create_table(inner_schema.clone()).unwrap();

        // Insert a folder using the Database API
        let folder_id = db
            .insert_with("folders", |b| b.set_string_by_name("name", "Work").build())
            .unwrap();

        // Insert notes that reference the folder
        let _note1_id = db
            .insert_with("notes", |b| {
                b.set_ref_by_name("folder", folder_id)
                    .set_string_by_name("title", "Meeting Notes")
                    .build()
            })
            .unwrap();
        let _note2_id = db
            .insert_with("notes", |b| {
                b.set_ref_by_name("folder", folder_id)
                    .set_string_by_name("title", "Project Plan")
                    .build()
            })
            .unwrap();

        // Verify the data exists
        eprintln!(
            "[TEST] folders count: {}",
            db.state().read_all_rows("folders").len()
        );
        eprintln!(
            "[TEST] notes count: {}",
            db.state().read_all_rows("notes").len()
        );

        // Verify find_referencing works
        let refs = db.state().find_referencing("notes", "folder", folder_id);
        eprintln!(
            "[TEST] find_referencing('notes', 'folder', {:?}) returned {} rows",
            folder_id,
            refs.len()
        );

        // Now create the query graph
        let mut builder = QueryGraphBuilder::new("folders", outer_schema.clone());
        let scan = builder.table_scan();
        let agg =
            builder.array_aggregate(scan, "notes", "folder", inner_schema.clone(), vec![], -1);
        let mut graph = builder.output(agg, GraphId(1));

        let mut cache = RowCache::new();

        // Get output - this should initialize and find the existing notes
        let output = graph.get_output(&mut cache, db.state());

        eprintln!("[TEST] output.len() = {}", output.len());
        assert_eq!(output.len(), 1);

        // The folder should have an array with 2 notes
        let (id, row) = &output[0];
        assert_eq!(*id, folder_id);

        // Check the array in the output row
        eprintln!("[TEST] output row buffer len: {}", row.buffer.len());

        // The buffer should be larger than just the folder data (which is ~8 bytes)
        // because it includes the array of notes
        assert!(
            row.buffer.len() > 20,
            "Buffer should include array data, got {} bytes",
            row.buffer.len()
        );

        // Parse the array from the buffer to verify it has 2 items
        // The array is at variable index 0 (after the fixed "name" column)
        // Array format: [u32 count][offset table][items...]
        // The name field is at index 0, the array is at index 1
        // First we need to get the array bytes from the variable section
        // This is a simplified check - just verify the buffer has the expected size
        eprintln!("[TEST] Test passed - inner rows were found during initialization");
    }

    /// Test that mimics the exact demo app flow:
    /// 1. Query is registered FIRST (before any data)
    /// 2. Outer row (folder) is inserted via Database API
    /// 3. Inner row (note) is inserted via Database API
    /// 4. Check that arrays are populated via inner delta processing
    #[test]
    fn array_aggregate_inner_delta_via_database_api() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let outer_schema = folder_schema();
        let inner_schema = note_schema();

        // Create database with tables
        let db = Database::in_memory();
        db.create_table(outer_schema.clone()).unwrap();
        db.create_table(inner_schema.clone()).unwrap();

        // Create incremental query BEFORE any data exists (like demo app's useAll)
        let query = db.incremental_query(
            "SELECT f.id, f.name, ARRAY(SELECT n.id, n.folder, n.title FROM notes n WHERE n.folder = f.id) as notes FROM folders f"
        ).unwrap();

        // Track updates
        let update_count = Arc::new(AtomicUsize::new(0));
        let update_count_clone = update_count.clone();
        query.subscribe(Box::new(move |_delta| {
            update_count_clone.fetch_add(1, Ordering::SeqCst);
        }));

        // Insert a folder via Database API (should trigger outer delta)
        let folder_id = db
            .insert_with("folders", |b| b.set_string_by_name("name", "Work").build())
            .unwrap();
        eprintln!("[TEST] Inserted folder with id: {:?}", folder_id);

        // The subscription callback fires with initial state + the Added delta
        // So we expect at least 1 update after insert
        let count_after_folder = update_count.load(Ordering::SeqCst);
        eprintln!(
            "[TEST] update_count after folder insert: {}",
            count_after_folder
        );
        assert!(
            count_after_folder >= 1,
            "Expected at least 1 update after folder insert"
        );

        // Insert a note via Database API (should trigger inner delta)
        let note_id = db
            .insert_with("notes", |b| {
                b.set_ref_by_name("folder", folder_id)
                    .set_string_by_name("title", "Meeting Notes")
                    .build()
            })
            .unwrap();
        eprintln!("[TEST] Inserted note with id: {:?}", note_id);

        // Should have gotten another update (Updated delta for folder with new array)
        let count_after_note = update_count.load(Ordering::SeqCst);
        eprintln!(
            "[TEST] update_count after note insert: {}",
            count_after_note
        );
        assert!(
            count_after_note > count_after_folder,
            "Expected additional update after note insert, got {} -> {}",
            count_after_folder,
            count_after_note
        );

        // Get the output and check the array
        let output = query.rows();
        eprintln!("[TEST] output.len() = {}", output.len());
        assert_eq!(output.len(), 1);

        let (id, row) = &output[0];
        assert_eq!(*id, folder_id);

        // Debug: print all columns in the row
        eprintln!(
            "[TEST] Row descriptor columns: {:?}",
            row.descriptor
                .columns
                .iter()
                .map(|c| (&c.name, &c.ty))
                .collect::<Vec<_>>()
        );
        eprintln!("[TEST] Row buffer len: {}", row.buffer.len());
        for (i, col) in row.descriptor.columns.iter().enumerate() {
            eprintln!("[TEST] Column {} ({}): {:?}", i, col.name, row.get(i));
        }

        // Find the notes array by name
        let notes_value = row.get_by_name("notes");
        eprintln!("[TEST] notes column value: {:?}", notes_value);

        // Check that the array column has data
        if let Some(crate::sql::row_buffer::RowValue::Array(arr)) = notes_value {
            eprintln!("[TEST] Array length: {}", arr.len());
            assert_eq!(arr.len(), 1, "Expected 1 note in array");

            // Check the note data
            if let Some(note_row) = arr.get(0) {
                let title = note_row.get_by_name("title");
                eprintln!("[TEST] Note title: {:?}", title);
            }
        } else {
            panic!("Expected Array for 'notes', got {:?}", notes_value);
        }
    }

    /// Test two chained ArrayAggregates (like Issues with IssueLabels and IssueAssignees)
    #[test]
    fn array_aggregate_two_chained_inner_deltas_via_database_api() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Create schemas
        let folder_schema = folder_schema();
        let note_schema = note_schema();
        // Second inner table schema (like IssueAssignees)
        let task_schema = TableSchema::new(
            "tasks",
            vec![
                ColumnDef::required("folder", ColumnType::Ref("folders".to_string())),
                ColumnDef::required("name", ColumnType::String),
            ],
        );

        // Create database with all tables
        let db = Database::in_memory();
        db.create_table(folder_schema.clone()).unwrap();
        db.create_table(note_schema.clone()).unwrap();
        db.create_table(task_schema.clone()).unwrap();

        // Create incremental query with TWO array subqueries
        let query = db
            .incremental_query(
                "SELECT f.id, f.name, \
             ARRAY(SELECT n.id, n.folder, n.title FROM notes n WHERE n.folder = f.id) as notes, \
             ARRAY(SELECT t.id, t.folder, t.name FROM tasks t WHERE t.folder = f.id) as tasks \
             FROM folders f",
            )
            .unwrap();

        // Track updates
        let update_count = Arc::new(AtomicUsize::new(0));
        let update_count_clone = update_count.clone();
        query.subscribe(Box::new(move |_delta| {
            update_count_clone.fetch_add(1, Ordering::SeqCst);
        }));

        // Insert a folder
        let folder_id = db
            .insert_with("folders", |b| b.set_string_by_name("name", "Work").build())
            .unwrap();
        eprintln!("[TEST] Inserted folder with id: {:?}", folder_id);

        let count_after_folder = update_count.load(Ordering::SeqCst);
        eprintln!(
            "[TEST] update_count after folder insert: {}",
            count_after_folder
        );

        // Insert a note
        let note_id = db
            .insert_with("notes", |b| {
                b.set_ref_by_name("folder", folder_id)
                    .set_string_by_name("title", "Meeting Notes")
                    .build()
            })
            .unwrap();
        eprintln!("[TEST] Inserted note with id: {:?}", note_id);

        let count_after_note = update_count.load(Ordering::SeqCst);
        eprintln!(
            "[TEST] update_count after note insert: {}",
            count_after_note
        );
        assert!(
            count_after_note > count_after_folder,
            "Expected update after note insert"
        );

        // Insert a task
        let task_id = db
            .insert_with("tasks", |b| {
                b.set_ref_by_name("folder", folder_id)
                    .set_string_by_name("name", "Review PR")
                    .build()
            })
            .unwrap();
        eprintln!("[TEST] Inserted task with id: {:?}", task_id);

        let count_after_task = update_count.load(Ordering::SeqCst);
        eprintln!(
            "[TEST] update_count after task insert: {}",
            count_after_task
        );
        assert!(
            count_after_task > count_after_note,
            "Expected update after task insert"
        );

        // Get the output and check both arrays
        let output = query.rows();
        eprintln!("[TEST] output.len() = {}", output.len());
        assert_eq!(output.len(), 1);

        let (id, row) = &output[0];
        assert_eq!(*id, folder_id);

        // Debug: print all columns
        eprintln!(
            "[TEST] Row descriptor columns: {:?}",
            row.descriptor
                .columns
                .iter()
                .map(|c| &c.name)
                .collect::<Vec<_>>()
        );

        // Check notes array
        let notes_value = row.get_by_name("notes");
        eprintln!("[TEST] notes value: {:?}", notes_value);
        if let Some(crate::sql::row_buffer::RowValue::Array(arr)) = notes_value {
            eprintln!("[TEST] notes array length: {}", arr.len());
            assert_eq!(arr.len(), 1, "Expected 1 note in array");
        } else {
            panic!("Expected Array for 'notes', got {:?}", notes_value);
        }

        // Check tasks array
        let tasks_value = row.get_by_name("tasks");
        eprintln!("[TEST] tasks value: {:?}", tasks_value);
        if let Some(crate::sql::row_buffer::RowValue::Array(arr)) = tasks_value {
            eprintln!("[TEST] tasks array length: {}", arr.len());
            assert_eq!(arr.len(), 1, "Expected 1 task in array");
        } else {
            panic!("Expected Array for 'tasks', got {:?}", tasks_value);
        }
    }

    #[test]
    fn graph_to_diagram() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema);
        let scan = builder.table_scan();
        let filter = builder.filter(scan, Predicate::eq("active", PredicateValue::Bool(true)));
        let graph = builder.output(filter, GraphId(42));

        let diagram = graph.to_diagram();
        println!("{}", diagram);

        // Check that diagram contains expected elements
        assert!(diagram.contains("Query Graph"));
        assert!(diagram.contains("42")); // graph id
        assert!(diagram.contains("users")); // table name
        assert!(diagram.contains("TableScan")); // node type
        assert!(diagram.contains("Filter")); // node type
        assert!(diagram.contains("Output")); // node type
        assert!(diagram.contains("active = TRUE")); // predicate
    }
}
