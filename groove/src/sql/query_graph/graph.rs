//! Query graph - the main computation DAG.

use std::collections::HashMap;
use std::sync::Arc;

use crate::object::ObjectId;
use crate::sql::query_graph::cache::RowCache;
use crate::sql::query_graph::delta::{DeltaBatch, RowDelta};
use crate::sql::query_graph::node::{NodeId, QueryNode};
use crate::sql::row_buffer::{OwnedRow, RowDescriptor};
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

    /// For JOIN queries, which table to project in output (e.g., "Issues" for SELECT Issues.*).
    /// If None, all columns from all joined tables are returned.
    projection_table: Option<String>,

    /// Nodes in topological order.
    nodes: Vec<QueryNode>,

    /// Map from NodeId to index in nodes vec.
    node_indices: HashMap<NodeId, usize>,

    /// The output node.
    output_node: NodeId,
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
    /// Create a new single-table query graph.
    ///
    /// This is typically called by `QueryGraphBuilder::output()`.
    pub(crate) fn new(
        id: GraphId,
        table: String,
        schema: TableSchema,
        nodes: Vec<QueryNode>,
        node_indices: HashMap<NodeId, usize>,
        output_node: NodeId,
    ) -> Self {
        let mut all_schemas = HashMap::new();
        all_schemas.insert(table.clone(), schema.clone());
        let mut all_tables = vec![table.clone()];

        // Check for ArrayAggregate nodes and add their inner tables
        for node in &nodes {
            if let QueryNode::ArrayAggregate { inner_table, inner_schema, .. } = node {
                if !all_tables.contains(inner_table) {
                    all_tables.push(inner_table.clone());
                    all_schemas.insert(inner_table.clone(), inner_schema.clone());
                }
            }
        }

        Self {
            id,
            state: GraphState::Uninitialized,
            table: table.clone(),
            all_tables,
            schema,
            all_schemas,
            is_join: false,
            projection_table: None,
            nodes,
            node_indices,
            output_node,
        }
    }

    /// Create a new join query graph with optional additional right tables.
    ///
    /// Used for INHERITS chains like: documents → folders → workspaces
    /// The `additional_right_tables` contains tables beyond the first join.
    ///
    /// `projection_table` specifies which table's columns to output (for reverse JOINs
    /// where we SELECT Table.* but swapped the tables for the graph builder).
    pub(crate) fn new_chain_join(
        id: GraphId,
        left_table: String,
        left_schema: TableSchema,
        right_table: String,
        right_schema: TableSchema,
        additional_right_tables: Vec<(String, TableSchema)>,
        projection_table: Option<String>,
        nodes: Vec<QueryNode>,
        node_indices: HashMap<NodeId, usize>,
        output_node: NodeId,
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

        Self {
            id,
            state: GraphState::Uninitialized,
            table: left_table,
            all_tables,
            schema: combined_schema, // Use combined schema for JOIN graphs
            all_schemas,
            is_join: true,
            projection_table,
            nodes,
            node_indices,
            output_node,
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

    /// Get current output rows in buffer format, initializing lazily if needed.
    pub fn get_output(&mut self, cache: &mut RowCache, db: &DatabaseState) -> Vec<(ObjectId, OwnedRow)> {
        self.ensure_initialized_skip(cache, db, None, None);
        self.collect_output(cache, db)
    }

    /// Get the output schema for this query.
    ///
    /// For JOIN queries with projection, returns a qualified version of the projected table's schema.
    pub fn output_schema(&self) -> Option<TableSchema> {
        // If there's a projection, return that table's schema with qualified column names
        if let Some(proj_table) = &self.projection_table {
            if let Some(proj_schema) = self.all_schemas.get(proj_table) {
                // Create a schema with qualified column names to match the OwnedRow
                return Some(proj_schema.qualify(proj_table));
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
            let has_recursive = self.nodes.iter().any(|n| {
                matches!(n, QueryNode::RecursiveFilter { .. })
            });

            // Check if this graph has ArrayAggregate nodes
            let has_array_aggregate = self.nodes.iter().any(|n| {
                matches!(n, QueryNode::ArrayAggregate { .. })
            });

            if has_recursive {
                // RecursiveFilter requires fixpoint iteration
                self.init_recursive_query(cache, db, skip_id, skip_table);
            } else if has_array_aggregate {
                // ArrayAggregate needs database access to look up inner rows
                self.init_array_aggregate_query(cache, db, skip_id, skip_table);
            } else {
                // Single-table query: load all rows from the primary table
                let rows = db.read_all_rows(&self.table);

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
                        delta = node.evaluate(delta, &self.schema, cache);
                    }
                }
            }
        }

        self.state = GraphState::Ready;
    }

    /// Initialize a JOIN query by loading and joining all rows.
    fn init_join_query(
        &mut self,
        cache: &mut RowCache,
        db: &DatabaseState,
        skip_id: Option<ObjectId>,
        skip_table: Option<&str>,
    ) {
        let table = self.table.clone();

        // Load all rows from the primary (left) table
        let left_rows = db.read_all_rows(&table);

        // Get the schema for qualification
        let table_schema = self.all_schemas.get(&table).cloned().unwrap_or_else(|| self.schema.clone());

        for (id, owned) in left_rows {
            // Skip if this is the triggering row
            if skip_table == Some(table.as_str()) && skip_id == Some(id) {
                continue;
            }

            cache.insert(&table, id, owned.clone());

            // For JOIN queries, qualify the column names so predicates with qualified names work
            let qualified_row = owned.qualify_columns(&table, &table_schema);
            let delta = RowDelta::Added { id, row: qualified_row };
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
        let schema = self.schema.clone();

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
                        delta = node.evaluate_recursive(delta, &schema);
                    }
                    _ => {
                        delta = node.evaluate(delta, &schema, cache);
                    }
                }
            }
        }
    }

    /// Initialize a query with ArrayAggregate nodes.
    ///
    /// ArrayAggregate needs database access to look up inner rows for each outer row.
    fn init_array_aggregate_query(
        &mut self,
        cache: &mut RowCache,
        db: &DatabaseState,
        skip_id: Option<ObjectId>,
        skip_table: Option<&str>,
    ) {
        let table = self.table.clone();

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
                    QueryNode::ArrayAggregate { outer_table, inner_table, inner_ref_column, .. } => {
                        // Clone values before borrowing node mutably
                        let inner_tbl = inner_table.clone();
                        let inner_ref = inner_ref_column.clone();

                        let outer_schema = self.all_schemas.get(outer_table)
                            .cloned()
                            .unwrap_or_else(|| self.schema.clone());

                        let mut output = DeltaBatch::new();
                        for d in delta.into_iter() {
                            let batch = node.evaluate_array_aggregate(
                                d,
                                &table, // source is outer table
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
                        delta = node.evaluate(delta, &self.schema, cache);
                    }
                }
            }
        }
    }

    /// Process a delta through all nodes, handling JOIN and RecursiveFilter nodes specially.
    fn process_delta_through_nodes(
        &mut self,
        delta: RowDelta,
        source_table: &str,
        cache: &mut RowCache,
        db: &DatabaseState,
    ) -> DeltaBatch {
        let mut current = DeltaBatch::new();
        current.push(delta);

        // Track tables that are "contained" in the current deltas.
        // For chain joins, after the first Join, deltas contain combined rows
        // with data from multiple tables.
        let mut contained_tables: Vec<String> = vec![source_table.to_string()];

        for node in &mut self.nodes {
            if current.is_empty() {
                break; // Early cutoff
            }

            match node {
                QueryNode::Join { input_tables, join_table, .. } => {
                    // Clone values we need before borrowing node mutably
                    let input_tables_cloned = input_tables.clone();
                    let join_table_str = join_table.clone();

                    // Check if this delta is for this Join node
                    let is_input_delta = input_tables_cloned.iter().any(|t| contained_tables.contains(t));
                    let is_join_table_delta = source_table == &join_table_str && !is_input_delta;
                    let is_for_this_node = is_input_delta || is_join_table_delta;

                    if !is_for_this_node {
                        // Delta is for a downstream node - pass through unchanged
                        // Don't update contained_tables since we didn't process anything
                        continue;
                    }

                    // Check if this delta came from input (prior join output or raw input table)
                    let is_from_input = contained_tables.len() > 1 || is_input_delta;

                    // Use combined schema for chain joins (contains qualified column names)
                    let input_schema = if contained_tables.len() > 1 || input_tables_cloned.len() > 1 {
                        self.schema.clone()
                    } else {
                        // First join - use the input table's schema
                        input_tables_cloned.first()
                            .and_then(|t| self.all_schemas.get(t).cloned())
                            .unwrap_or_else(|| self.schema.clone())
                    };

                    let mut output = DeltaBatch::new();
                    for d in current.into_iter() {
                        let batch = node.evaluate_join(
                            d,
                            source_table,
                            &input_schema,
                            is_from_input,
                            |table, id| db.get_row(table, id).map(|(_, row)| row),
                            |table, column, target_id| {
                                db.find_referencing(table, column, target_id)
                            },
                        );
                        output.extend(batch);
                    }
                    current = output;

                    // After a Join, the delta now contains data from all input tables plus join_table
                    for table in input_tables_cloned {
                        if !contained_tables.contains(&table) {
                            contained_tables.push(table);
                        }
                    }
                    if !contained_tables.contains(&join_table_str) {
                        contained_tables.push(join_table_str);
                    }
                }
                QueryNode::RecursiveFilter { .. } => {
                    // RecursiveFilter needs special evaluation for fixpoint iteration
                    current = node.evaluate_recursive(current, &self.schema);
                }
                QueryNode::ArrayAggregate { outer_table, inner_table, inner_ref_column, .. } => {
                    // Clone values before borrowing node mutably
                    let outer_tbl = outer_table.clone();
                    let inner_tbl = inner_table.clone();
                    let inner_ref = inner_ref_column.clone();

                    // Check if this delta is for this node
                    let is_outer_delta = source_table == outer_tbl || contained_tables.contains(&outer_tbl);
                    let is_inner_delta = source_table == inner_tbl;

                    if is_outer_delta || is_inner_delta {
                        // Process deltas through ArrayAggregate
                        let outer_schema = self.all_schemas.get(&outer_tbl)
                            .cloned()
                            .unwrap_or_else(|| self.schema.clone());

                        let mut output = DeltaBatch::new();
                        for d in current.into_iter() {
                            let batch = node.evaluate_array_aggregate(
                                d,
                                source_table,
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
                        current = output;
                    }
                    // If not for this node, pass through unchanged
                }
                QueryNode::LimitOffset { .. } => {
                    // LimitOffset needs special evaluation with cache access
                    current = node.evaluate_limit_offset(current, &self.schema, cache);
                }
                _ => {
                    current = node.evaluate(current, &self.schema, cache);
                }
            }
        }

        current
    }

    /// Collect output rows in buffer format from the final cached set.
    fn collect_output(&self, cache: &RowCache, _db: &DatabaseState) -> Vec<(ObjectId, OwnedRow)> {
        // Find the node feeding into Output
        let output_idx = self.node_indices[&self.output_node];
        if let QueryNode::Output { input, .. } = &self.nodes[output_idx] {
            let input_idx = self.node_indices[input];

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

                if let Some(join_node) = join_node {
                    if let Some(joined_rows) = join_node.cached_joined() {
                        // Get IDs to filter by from downstream Filter node (if any)
                        let filter_ids = self.nodes[input_idx].cached_ids();

                        // Apply the filter and projection - now returns OwnedRow directly
                        let rows: Vec<(ObjectId, OwnedRow)> = joined_rows
                            .iter()
                            .filter(|(id, _)| {
                                // If there's a filter, only include matching IDs
                                filter_ids.map_or(true, |ids| ids.contains(id))
                            })
                            .map(|(primary_id, jr)| {
                                // Apply projection if set
                                if let Some(proj_table) = &self.projection_table {
                                    if self.all_schemas.contains_key(proj_table) {
                                        if let Some((_, row)) = jr.table_rows.get(proj_table) {
                                            return (*primary_id, row.clone());
                                        }
                                    }
                                }
                                // Default: return all joined columns as OwnedRow
                                (*primary_id, jr.to_output_row())
                            })
                            .collect();

                        return rows;
                    }
                }
            }

            // For RecursiveFilter queries, get rows from accessible set
            // OwnedRow is already in buffer format
            if let Some(accessible) = self.nodes[input_idx].accessible() {
                if let Some(all_rows) = self.nodes[input_idx].all_rows() {
                    return accessible
                        .keys()
                        .filter_map(|id| {
                            all_rows.get(id).map(|owned_row| (*id, owned_row.clone()))
                        })
                        .collect();
                }
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
            if let QueryNode::LimitOffset { all_rows, visible_ids, .. } = &self.nodes[input_idx] {
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
                        cache.get(&self.table, *id).flatten().map(|row| {
                            (*id, row.clone())
                        })
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
        writeln!(out, "┌─────────────────────────────────────────────────────────────┐").unwrap();
        writeln!(out, "│  Query Graph (id: {})                                       │", self.id.0).unwrap();
        writeln!(out, "│  Primary table: {:42} │", self.table).unwrap();
        if self.is_join {
            let tables = self.all_tables.join(", ");
            writeln!(out, "│  Join tables: {:44} │", truncate_str(&tables, 44)).unwrap();
        }
        writeln!(out, "└─────────────────────────────────────────────────────────────┘").unwrap();
        writeln!(out).unwrap();

        // Nodes in topological order (reverse for visual flow: sources at top)
        for (idx, node) in self.nodes.iter().enumerate() {
            let node_id = self.node_indices.iter()
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

            // Connection to next node (if not output)
            if !is_last {
                if let Some(input) = node.input() {
                    writeln!(out, "{}       ↑ from node {}", continuation, input.0).unwrap();
                }
                writeln!(out, "{}", continuation).unwrap();
            }
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::query_graph::builder::QueryGraphBuilder;
    use crate::sql::query_graph::predicate::Predicate;
    use crate::sql::row::Value;
    use crate::sql::row_buffer::RowBuilder;
    use crate::sql::schema::{ColumnDef, ColumnType};
    use crate::sql::Database;
    use crate::object::ObjectId;

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
        let filter = builder.filter(scan, Predicate::eq("active", Value::Bool(true)));
        let mut graph = builder.output(filter, GraphId(1));

        // Create a mock database state
        let db = Database::in_memory();
        db.create_table(schema).unwrap();

        let mut cache = RowCache::new();

        // Process an active user - should appear in output
        let (id, row) = make_owned_row(1, "Alice", true);
        let delta = graph.process_change(RowDelta::Added { id, row }, &mut cache, &db.state());

        assert_eq!(delta.len(), 1);
        assert!(matches!(delta.iter().next(), Some(RowDelta::Added { .. })));

        // Process an inactive user - should be filtered out
        let (id, row) = make_owned_row(2, "Bob", false);
        let delta = graph.process_change(RowDelta::Added { id, row }, &mut cache, &db.state());

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
        let filter = builder.filter(scan, Predicate::eq("active", Value::Bool(true)));
        let mut graph = builder.output(filter, GraphId(1));

        let db = Database::in_memory();
        db.create_table(schema).unwrap();

        let mut cache = RowCache::new();

        // Add some rows
        let (id1, row1) = make_owned_row(1, "Alice", true);
        let (id2, row2) = make_owned_row(2, "Bob", false);
        let (id3, row3) = make_owned_row(3, "Carol", true);

        graph.process_change(RowDelta::Added { id: id1, row: row1 }, &mut cache, &db.state());
        graph.process_change(RowDelta::Added { id: id2, row: row2 }, &mut cache, &db.state());
        graph.process_change(RowDelta::Added { id: id3, row: row3 }, &mut cache, &db.state());

        // Get output - should only have active users
        let output = graph.get_output(&mut cache, &db.state());

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
        let agg = builder.array_aggregate(scan, "notes", "folder", inner_schema.clone(), vec![], -1);
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
        let delta = graph.process_change(RowDelta::Added { id, row }, &mut cache, &db.state());

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
        let agg = builder.array_aggregate(scan, "notes", "folder", inner_schema.clone(), vec![], -1);
        let mut graph = builder.output(agg, GraphId(1));

        let db = Database::in_memory();
        db.create_table(outer_schema).unwrap();
        db.create_table(inner_schema).unwrap();

        let mut cache = RowCache::new();

        // Add a folder first
        let (id, row) = make_owned_folder(1, "Work");
        graph.process_change(RowDelta::Added { id, row }, &mut cache, &db.state());

        // Now add a note to that folder
        let (id, row) = make_owned_note(100, 1, "Meeting Notes");
        let delta = graph.process_change_from_table(
            RowDelta::Added { id, row },
            "notes",
            &mut cache,
            &db.state(),
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
        let agg = builder.array_aggregate(scan, "notes", "folder", inner_schema.clone(), vec![], -1);
        let mut graph = builder.output(agg, GraphId(1));

        let db = Database::in_memory();
        db.create_table(outer_schema).unwrap();
        db.create_table(inner_schema).unwrap();

        let mut cache = RowCache::new();

        // Add a folder
        let (id, row) = make_owned_folder(1, "Work");
        graph.process_change(RowDelta::Added { id, row }, &mut cache, &db.state());

        // Add two notes
        let (id, row) = make_owned_note(100, 1, "Note 1");
        graph.process_change_from_table(RowDelta::Added { id, row }, "notes", &mut cache, &db.state());
        let (id, row) = make_owned_note(101, 1, "Note 2");
        graph.process_change_from_table(RowDelta::Added { id, row }, "notes", &mut cache, &db.state());

        // Get output
        let output = graph.get_output(&mut cache, &db.state());
        assert_eq!(output.len(), 1);

        // Remove one note
        let delta = graph.process_change_from_table(
            RowDelta::Removed {
                id: ObjectId::new(100),
                prior: crate::sql::query_graph::delta::PriorState::empty(),
            },
            "notes",
            &mut cache,
            &db.state(),
        );

        // Should emit Updated delta
        assert_eq!(delta.len(), 1);
        assert!(matches!(delta.iter().next(), Some(RowDelta::Updated { .. })));
    }

    #[test]
    fn graph_to_diagram() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema);
        let scan = builder.table_scan();
        let filter = builder.filter(scan, Predicate::eq("active", Value::Bool(true)));
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
