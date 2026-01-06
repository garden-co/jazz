//! Query graph nodes.

use std::collections::{HashMap, HashSet};

use crate::sql::query_graph::cache::RowCache;
use crate::sql::query_graph::delta::{DeltaBatch, JoinedRow, RowDelta};
use crate::sql::query_graph::predicate::Predicate;
use crate::sql::row::{Row, Value};
use crate::sql::schema::TableSchema;
use crate::sql::types::IndexKey;
use crate::object::ObjectId;

/// Unique identifier for a node within a query graph.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct NodeId(pub u32);

/// Reason why a row is accessible in a RecursiveFilter.
///
/// This is crucial for correctly handling removals - if a row is only
/// accessible via inheritance and its parent loses access, the row
/// must also lose access. But if it has its own base access, it stays.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessReason {
    /// Row satisfies the base predicate (e.g., owner_id = @viewer)
    Base,
    /// Row is accessible because its parent is accessible
    Inherited,
    /// Row satisfies both base and inherited (redundant but safe)
    Both,
}

/// A node in the query graph.
#[derive(Debug)]
pub enum QueryNode {
    /// Source: all row IDs in a table.
    ///
    /// This is typically the starting point for a query.
    /// It maintains a cached set of all row IDs currently in the table.
    TableScan {
        table: String,
        /// Cached set of row IDs in the table.
        cached_ids: HashSet<ObjectId>,
    },

    /// Source: row IDs from a reverse index lookup.
    ///
    /// Used for queries like `SELECT * FROM posts WHERE author = ?`
    /// where we can use the reverse index on `posts.author` to find
    /// all posts referencing a specific user.
    IndexLookup {
        table: String,
        index_key: IndexKey,
        target_id: ObjectId,
        /// Cached set of row IDs pointing at the target.
        cached_ids: HashSet<ObjectId>,
    },

    /// Transform: filter rows by a predicate.
    ///
    /// Takes input from another node and filters rows that match
    /// the predicate. Maintains a cached set of matching row IDs.
    Filter {
        table: String,
        input: NodeId,
        predicate: Predicate,
        /// Cached set of row IDs that pass the filter.
        cached_ids: HashSet<ObjectId>,
    },

    /// Transform: join rows from input with rows from join_table.
    ///
    /// This implements an inner join where:
    /// - `join_column` is a Ref column in the input (may be qualified for chains)
    /// - The join matches `input.join_column = join_table.id`
    ///
    /// Supports chain joins where input is combined rows from prior joins.
    /// On input deltas: looks up the corresponding join_table row by ID
    /// On join_table deltas: uses reverse_index to find affected input rows
    Join {
        /// Tables contained in the input (for first join: single table; for chains: multiple).
        input_tables: Vec<String>,
        /// Table being joined in this step.
        join_table: String,
        /// Column in input that references join_table (may be qualified: "folders.workspace_id").
        join_column: String,
        /// Schema of the join table (for building joined rows).
        join_schema: TableSchema,
        /// Cached rows: primary_id → JoinedRow.
        /// primary_id is always the leftmost table's row ID.
        cached_rows: HashMap<ObjectId, JoinedRow>,
        /// Reverse index: join_table_id → set of primary_ids.
        /// Used for handling deltas from join_table.
        reverse_index: HashMap<ObjectId, HashSet<ObjectId>>,
    },

    /// Terminal: marks the output of the graph.
    ///
    /// This node doesn't transform data, it just marks which node's
    /// output should be used as the query result.
    Output { table: String, input: NodeId },

    /// Transform: recursive filter for self-referential policies.
    ///
    /// Handles policies like `owner_id = @viewer OR INHERITS SELECT FROM parent_id`
    /// where `parent_id` references the same table. Uses fixpoint iteration
    /// to compute the transitive closure of accessible rows.
    ///
    /// A row is accessible if:
    /// - It satisfies the base_predicate (e.g., owner_id = @viewer), OR
    /// - Its parent (via recursive_column) is accessible
    RecursiveFilter {
        table: String,
        input: NodeId,
        /// Base predicate for direct access (e.g., owner_id = @viewer)
        base_predicate: Predicate,
        /// Column that references parent row in same table (e.g., parent_id)
        recursive_column: String,
        /// Currently accessible rows with their access reason
        accessible: HashMap<ObjectId, AccessReason>,
        /// Reverse index: parent_id -> set of children
        /// Used for efficient cascade propagation
        children_index: HashMap<ObjectId, HashSet<ObjectId>>,
        /// All rows in the table (needed for fixpoint iteration)
        all_rows: HashMap<ObjectId, Row>,
    },

    /// Transform: aggregate inner rows into arrays per outer row.
    ///
    /// Used for ARRAY(SELECT ...) subqueries. Groups rows from inner_table
    /// by their reference to outer_table, producing output rows where the
    /// nested column contains Value::Array of matching inner rows.
    ///
    /// Example:
    /// ```sql
    /// SELECT f.*, ARRAY(SELECT n FROM notes n WHERE n.folder_id = f.id) as notes
    /// FROM folders f
    /// ```
    ArrayAggregate {
        /// The outer table (source of correlation, e.g., "folders").
        outer_table: String,
        /// Input node providing outer rows.
        input: NodeId,
        /// The inner table being aggregated (e.g., "notes").
        inner_table: String,
        /// Column in inner table that references outer (e.g., "folder_id").
        inner_ref_column: String,
        /// Schema of inner table (for building Row values).
        inner_schema: TableSchema,
        /// JOINs within the ARRAY subquery (e.g., JOIN Labels ON il.label = Labels.id).
        /// Each tuple: (ref_column_in_inner, target_table, target_schema)
        /// The ref column will be replaced by the joined table's columns.
        inner_joins: Vec<(String, String, TableSchema)>,
        /// Index in output row where the array should be placed.
        /// -1 means append at end.
        array_column_index: i32,
        /// Cached arrays: outer_id → Vec<Row>.
        cached_arrays: HashMap<ObjectId, Vec<Row>>,
        /// Reverse index: inner_id → outer_id (for propagating inner changes).
        inner_to_outer: HashMap<ObjectId, ObjectId>,
        /// Cached outer rows (needed to emit Updated deltas with full row data).
        outer_rows: HashMap<ObjectId, Row>,
    },

    /// Transform: apply LIMIT and OFFSET to input rows.
    ///
    /// Maintains all qualifying rows from input and tracks which subset is
    /// "visible" (within the offset+limit window). Emits deltas when the
    /// visible window changes.
    ///
    /// Without ORDER BY, uses ObjectId ordering (UUIDv7 = insertion order).
    LimitOffset {
        table: String,
        input: NodeId,
        /// Maximum number of rows to return (None = unlimited).
        limit: Option<u64>,
        /// Number of rows to skip from the start.
        offset: u64,
        /// All rows that passed upstream filters, sorted by ObjectId.
        all_rows: std::collections::BTreeMap<ObjectId, Row>,
        /// Currently visible row IDs (in the window [offset, offset+limit)).
        visible_ids: HashSet<ObjectId>,
    },
}

impl QueryNode {
    /// Get the primary table this node operates on.
    pub fn table(&self) -> &str {
        match self {
            QueryNode::TableScan { table, .. } => table,
            QueryNode::IndexLookup { table, .. } => table,
            QueryNode::Filter { table, .. } => table,
            QueryNode::Join { input_tables, .. } => input_tables.first().map(|s| s.as_str()).unwrap_or(""),
            QueryNode::Output { table, .. } => table,
            QueryNode::RecursiveFilter { table, .. } => table,
            QueryNode::ArrayAggregate { outer_table, .. } => outer_table,
            QueryNode::LimitOffset { table, .. } => table,
        }
    }

    /// Get all tables this node depends on.
    pub fn tables(&self) -> Vec<&str> {
        match self {
            QueryNode::TableScan { table, .. } => vec![table],
            QueryNode::IndexLookup { table, .. } => vec![table],
            QueryNode::Filter { table, .. } => vec![table],
            QueryNode::Join { input_tables, join_table, .. } => {
                let mut tables: Vec<&str> = input_tables.iter().map(|s| s.as_str()).collect();
                tables.push(join_table);
                tables
            }
            QueryNode::Output { table, .. } => vec![table],
            QueryNode::RecursiveFilter { table, .. } => vec![table],
            QueryNode::ArrayAggregate { outer_table, inner_table, .. } => {
                vec![outer_table, inner_table]
            }
            QueryNode::LimitOffset { table, .. } => vec![table],
        }
    }

    /// Get the cached IDs if this node caches them (single-table nodes only).
    pub fn cached_ids(&self) -> Option<&HashSet<ObjectId>> {
        match self {
            QueryNode::TableScan { cached_ids, .. } => Some(cached_ids),
            QueryNode::IndexLookup { cached_ids, .. } => Some(cached_ids),
            QueryNode::Filter { cached_ids, .. } => Some(cached_ids),
            QueryNode::Join { .. } => None, // Uses cached_pairs instead
            QueryNode::Output { .. } => None,
            QueryNode::RecursiveFilter { .. } => None, // Uses accessible instead
            QueryNode::ArrayAggregate { .. } => None, // Uses outer_rows instead
            QueryNode::LimitOffset { visible_ids, .. } => Some(visible_ids),
        }
    }

    /// Get a mutable reference to cached IDs.
    pub fn cached_ids_mut(&mut self) -> Option<&mut HashSet<ObjectId>> {
        match self {
            QueryNode::TableScan { cached_ids, .. } => Some(cached_ids),
            QueryNode::IndexLookup { cached_ids, .. } => Some(cached_ids),
            QueryNode::Filter { cached_ids, .. } => Some(cached_ids),
            QueryNode::Join { .. } => None,
            QueryNode::Output { .. } => None,
            QueryNode::RecursiveFilter { .. } => None,
            QueryNode::ArrayAggregate { .. } => None,
            QueryNode::LimitOffset { visible_ids, .. } => Some(visible_ids),
        }
    }

    /// Get the accessible rows for RecursiveFilter nodes.
    pub fn accessible(&self) -> Option<&HashMap<ObjectId, AccessReason>> {
        match self {
            QueryNode::RecursiveFilter { accessible, .. } => Some(accessible),
            _ => None,
        }
    }

    /// Get all rows stored in a RecursiveFilter node.
    pub fn all_rows(&self) -> Option<&HashMap<ObjectId, Row>> {
        match self {
            QueryNode::RecursiveFilter { all_rows, .. } => Some(all_rows),
            _ => None,
        }
    }

    /// Get the cached joined rows keyed by primary_id (for Join nodes).
    pub fn cached_rows(&self) -> Option<&HashMap<ObjectId, JoinedRow>> {
        match self {
            QueryNode::Join { cached_rows, .. } => Some(cached_rows),
            _ => None,
        }
    }

    /// Get the reverse index for Join nodes (join_table_id → primary_ids).
    pub fn reverse_index(&self) -> Option<&HashMap<ObjectId, HashSet<ObjectId>>> {
        match self {
            QueryNode::Join { reverse_index, .. } => Some(reverse_index),
            _ => None,
        }
    }

    /// Compatibility wrapper - returns cached_rows.
    pub fn cached_joined(&self) -> Option<&HashMap<ObjectId, JoinedRow>> {
        self.cached_rows()
    }

    /// Get the cached arrays for ArrayAggregate nodes.
    pub fn cached_arrays(&self) -> Option<&HashMap<ObjectId, Vec<Row>>> {
        match self {
            QueryNode::ArrayAggregate { cached_arrays, .. } => Some(cached_arrays),
            _ => None,
        }
    }

    /// Get the outer rows for ArrayAggregate nodes.
    pub fn outer_rows(&self) -> Option<&HashMap<ObjectId, Row>> {
        match self {
            QueryNode::ArrayAggregate { outer_rows, .. } => Some(outer_rows),
            _ => None,
        }
    }

    /// Get the input node ID if this node has one.
    pub fn input(&self) -> Option<NodeId> {
        match self {
            QueryNode::TableScan { .. } => None,
            QueryNode::IndexLookup { .. } => None,
            QueryNode::Filter { input, .. } => Some(*input),
            QueryNode::Join { .. } => None, // Join is a source-like node
            QueryNode::Output { input, .. } => Some(*input),
            QueryNode::RecursiveFilter { input, .. } => Some(*input),
            QueryNode::ArrayAggregate { input, .. } => Some(*input),
            QueryNode::LimitOffset { input, .. } => Some(*input),
        }
    }

    /// Check if this node handles a specific table.
    pub fn handles_table(&self, table: &str) -> bool {
        self.tables().iter().any(|&t| t == table)
    }

    /// Get diagram information for this node.
    ///
    /// Returns (node_type_name, details_lines) for rendering in a text diagram.
    pub fn diagram_info(&self) -> (String, Vec<String>) {
        match self {
            QueryNode::TableScan { table, cached_ids } => {
                (
                    format!("TableScan [{}]", table),
                    vec![format!("cached: {} rows", cached_ids.len())],
                )
            }

            QueryNode::IndexLookup { table, index_key, target_id, cached_ids } => {
                (
                    format!("IndexLookup [{}]", table),
                    vec![
                        format!("index: {:?}", index_key),
                        format!("target: {}", target_id),
                        format!("cached: {} rows", cached_ids.len()),
                    ],
                )
            }

            QueryNode::Filter { table, predicate, cached_ids, .. } => {
                (
                    format!("Filter [{}]", table),
                    vec![
                        format!("predicate: {}", predicate.to_display_string()),
                        format!("cached: {} rows", cached_ids.len()),
                    ],
                )
            }

            QueryNode::Join { input_tables, join_table, join_column, cached_rows, reverse_index, .. } => {
                (
                    "Join".to_string(),
                    vec![
                        format!("inputs: [{}]", input_tables.join(", ")),
                        format!("join: {} ON {}", join_table, join_column),
                        format!("cached: {} joined rows", cached_rows.len()),
                        format!("reverse_index: {} entries", reverse_index.len()),
                    ],
                )
            }

            QueryNode::Output { table, input } => {
                (
                    format!("Output [{}]", table),
                    vec![format!("← from node {}", input.0)],
                )
            }

            QueryNode::RecursiveFilter { table, base_predicate, recursive_column, accessible, children_index, all_rows, .. } => {
                (
                    format!("RecursiveFilter [{}]", table),
                    vec![
                        format!("base: {}", base_predicate.to_display_string()),
                        format!("recursive on: {}", recursive_column),
                        format!("accessible: {} rows", accessible.len()),
                        format!("children_index: {} parents", children_index.len()),
                        format!("all_rows: {} total", all_rows.len()),
                    ],
                )
            }

            QueryNode::ArrayAggregate { outer_table, inner_table, inner_ref_column, cached_arrays, outer_rows, .. } => {
                (
                    format!("ArrayAggregate [{}]", outer_table),
                    vec![
                        format!("inner: {} via {}", inner_table, inner_ref_column),
                        format!("cached: {} arrays", cached_arrays.len()),
                        format!("outer_rows: {} rows", outer_rows.len()),
                    ],
                )
            }

            QueryNode::LimitOffset { table, limit, offset, all_rows, visible_ids, .. } => {
                let limit_str = limit.map(|l| l.to_string()).unwrap_or_else(|| "∞".to_string());
                (
                    format!("LimitOffset [{}]", table),
                    vec![
                        format!("LIMIT {} OFFSET {}", limit_str, offset),
                        format!("all_rows: {}, visible: {}", all_rows.len(), visible_ids.len()),
                    ],
                )
            }
        }
    }

    /// Evaluate this node given input deltas.
    ///
    /// Returns output deltas (may be empty for early cutoff).
    /// Note: Join nodes should use `evaluate_join` instead.
    pub fn evaluate(
        &mut self,
        input: DeltaBatch,
        schema: &TableSchema,
        _cache: &RowCache,
    ) -> DeltaBatch {
        match self {
            QueryNode::TableScan { cached_ids, .. } => Self::eval_id_passthrough(cached_ids, input),

            QueryNode::IndexLookup { cached_ids, .. } => {
                Self::eval_id_passthrough(cached_ids, input)
            }

            QueryNode::Filter {
                predicate,
                cached_ids,
                ..
            } => Self::eval_filter(predicate, cached_ids, input, schema),

            QueryNode::Join { .. } => {
                // Join nodes need special handling with database access
                // This should be called via evaluate_join instead
                DeltaBatch::new()
            }

            QueryNode::Output { .. } => input, // Passthrough

            QueryNode::RecursiveFilter { .. } => {
                // RecursiveFilter nodes need special handling
                // This should be called via evaluate_recursive instead
                DeltaBatch::new()
            }

            QueryNode::ArrayAggregate { .. } => {
                // ArrayAggregate nodes need special handling with database access
                // This should be called via evaluate_array_aggregate instead
                DeltaBatch::new()
            }

            QueryNode::LimitOffset { .. } => {
                // LimitOffset nodes need special handling
                // This should be called via evaluate_limit_offset instead
                DeltaBatch::new()
            }
        }
    }

    /// Evaluate a RecursiveFilter node with fixpoint iteration.
    ///
    /// This handles self-referential policies like:
    /// `owner_id = @viewer OR INHERITS SELECT FROM parent_id`
    pub fn evaluate_recursive(
        &mut self,
        input: DeltaBatch,
        schema: &TableSchema,
    ) -> DeltaBatch {
        match self {
            QueryNode::RecursiveFilter {
                base_predicate,
                recursive_column,
                accessible,
                children_index,
                all_rows,
                ..
            } => {
                let mut output = DeltaBatch::new();

                for delta in input.into_iter() {
                    match delta {
                        RowDelta::Added(row) => {
                            Self::recursive_handle_insert(
                                row,
                                schema,
                                base_predicate,
                                recursive_column,
                                accessible,
                                children_index,
                                all_rows,
                                &mut output,
                            );
                        }
                        RowDelta::Removed { id, prior } => {
                            Self::recursive_handle_remove(
                                id,
                                prior,
                                recursive_column,
                                schema,
                                accessible,
                                children_index,
                                all_rows,
                                &mut output,
                            );
                        }
                        RowDelta::Updated { id, new, prior } => {
                            // Handle as remove + insert for simplicity
                            // (Could optimize for cases where parent_id doesn't change)
                            Self::recursive_handle_remove(
                                id,
                                prior.clone(),
                                recursive_column,
                                schema,
                                accessible,
                                children_index,
                                all_rows,
                                &mut output,
                            );
                            Self::recursive_handle_insert(
                                new,
                                schema,
                                base_predicate,
                                recursive_column,
                                accessible,
                                children_index,
                                all_rows,
                                &mut output,
                            );
                        }
                    }
                }

                output
            }
            _ => DeltaBatch::new(),
        }
    }

    /// Handle inserting a row into a RecursiveFilter.
    #[allow(clippy::too_many_arguments)]
    fn recursive_handle_insert(
        row: Row,
        schema: &TableSchema,
        base_predicate: &Predicate,
        recursive_column: &str,
        accessible: &mut HashMap<ObjectId, AccessReason>,
        children_index: &mut HashMap<ObjectId, HashSet<ObjectId>>,
        all_rows: &mut HashMap<ObjectId, Row>,
        output: &mut DeltaBatch,
    ) {
        let row_id = row.id;

        // Store the row
        all_rows.insert(row_id, row.clone());

        // Update children index: this row is a child of its parent
        if let Some(parent_id) = Self::get_ref_value(&row, recursive_column, schema) {
            children_index
                .entry(parent_id)
                .or_default()
                .insert(row_id);
        }

        // Check if row is accessible
        let base_match = base_predicate.matches(&row, schema);
        let parent_id = Self::get_ref_value(&row, recursive_column, schema);
        let parent_accessible = parent_id
            .map(|pid| accessible.contains_key(&pid))
            .unwrap_or(false);

        // Null parent is treated as accessible (root node) if base matches,
        // or if we want roots to be inherently accessible
        let is_root = parent_id.is_none();

        if base_match || parent_accessible {
            let reason = match (base_match, parent_accessible) {
                (true, true) => AccessReason::Both,
                (true, false) => AccessReason::Base,
                (false, true) => AccessReason::Inherited,
                (false, false) => unreachable!(),
            };
            accessible.insert(row_id, reason);
            output.push(RowDelta::Added(row.clone()));

            // Cascade: check if any existing rows are children of this row
            // and should now become accessible
            Self::propagate_access_to_children(
                row_id,
                schema,
                base_predicate,
                recursive_column,
                accessible,
                children_index,
                all_rows,
                output,
            );
        } else if is_root {
            // Root node without base access - not accessible
            // (but still in all_rows for structure tracking)
        }
        // else: not accessible, don't add to output
    }

    /// Propagate access to children of a newly-accessible row.
    #[allow(clippy::too_many_arguments)]
    fn propagate_access_to_children(
        parent_id: ObjectId,
        schema: &TableSchema,
        base_predicate: &Predicate,
        recursive_column: &str,
        accessible: &mut HashMap<ObjectId, AccessReason>,
        children_index: &HashMap<ObjectId, HashSet<ObjectId>>,
        all_rows: &HashMap<ObjectId, Row>,
        output: &mut DeltaBatch,
    ) {
        if let Some(children) = children_index.get(&parent_id) {
            for &child_id in children {
                // Skip if already accessible
                if accessible.contains_key(&child_id) {
                    continue;
                }

                // Child becomes accessible via inheritance
                if let Some(child_row) = all_rows.get(&child_id) {
                    let base_match = base_predicate.matches(child_row, schema);
                    let reason = if base_match {
                        AccessReason::Both
                    } else {
                        AccessReason::Inherited
                    };
                    accessible.insert(child_id, reason);
                    output.push(RowDelta::Added(child_row.clone()));

                    // Recursively propagate to grandchildren
                    Self::propagate_access_to_children(
                        child_id,
                        schema,
                        base_predicate,
                        recursive_column,
                        accessible,
                        children_index,
                        all_rows,
                        output,
                    );
                }
            }
        }
    }

    /// Handle removing a row from a RecursiveFilter.
    #[allow(clippy::too_many_arguments)]
    fn recursive_handle_remove(
        row_id: ObjectId,
        prior: crate::sql::query_graph::delta::PriorState,
        recursive_column: &str,
        schema: &TableSchema,
        accessible: &mut HashMap<ObjectId, AccessReason>,
        children_index: &mut HashMap<ObjectId, HashSet<ObjectId>>,
        all_rows: &mut HashMap<ObjectId, Row>,
        output: &mut DeltaBatch,
    ) {
        // Remove from all_rows
        let removed_row = all_rows.remove(&row_id);

        // Remove from children_index (this row as a child of its parent)
        if let Some(row) = &removed_row {
            if let Some(parent_id) = Self::get_ref_value(row, recursive_column, schema) {
                if let Some(siblings) = children_index.get_mut(&parent_id) {
                    siblings.remove(&row_id);
                }
            }
        }

        // If row was accessible, remove it and cascade to children
        if accessible.remove(&row_id).is_some() {
            output.push(RowDelta::Removed { id: row_id, prior: prior.clone() });

            // Cascade removal to children that were only accessible via this parent
            Self::propagate_removal_to_children(
                row_id,
                prior,
                schema,
                accessible,
                children_index,
                all_rows,
                output,
            );
        }

        // Also remove this row's entry in children_index (as a parent)
        children_index.remove(&row_id);
    }

    /// Propagate removal to children of a removed row.
    #[allow(clippy::too_many_arguments)]
    fn propagate_removal_to_children(
        removed_parent_id: ObjectId,
        prior: crate::sql::query_graph::delta::PriorState,
        _schema: &TableSchema,
        accessible: &mut HashMap<ObjectId, AccessReason>,
        children_index: &HashMap<ObjectId, HashSet<ObjectId>>,
        all_rows: &HashMap<ObjectId, Row>,
        output: &mut DeltaBatch,
    ) {
        if let Some(children) = children_index.get(&removed_parent_id) {
            for &child_id in children.clone().iter() {
                if let Some(reason) = accessible.get(&child_id).copied() {
                    match reason {
                        AccessReason::Inherited => {
                            // Only accessible via this parent - loses access
                            accessible.remove(&child_id);
                            output.push(RowDelta::Removed {
                                id: child_id,
                                prior: prior.clone(),
                            });

                            // Recursively remove grandchildren
                            Self::propagate_removal_to_children(
                                child_id,
                                prior.clone(),
                                _schema,
                                accessible,
                                children_index,
                                all_rows,
                                output,
                            );
                        }
                        AccessReason::Both => {
                            // Still accessible via base predicate - downgrade
                            accessible.insert(child_id, AccessReason::Base);
                            // No output delta - still visible
                        }
                        AccessReason::Base => {
                            // Wasn't using parent anyway - no change
                        }
                    }
                }
            }
        }
    }

    /// Evaluate a join node with the given delta and database access.
    ///
    /// For chain joins, this handles:
    /// - Input deltas (from tables in input_tables): Look up join_table row
    /// - Join table deltas: Use reverse_index to find affected combined rows
    ///
    /// `source_table` indicates which table the delta came from.
    /// `input_schema` is the combined schema of input tables (for column lookup).
    /// `is_from_input` indicates if this delta came from a prior node in the chain.
    /// `lookup_row` is a function to look up a row by table and ID.
    /// `lookup_by_ref` is a function to find rows referencing a given ID (DB-level).
    pub fn evaluate_join<F, G>(
        &mut self,
        delta: RowDelta,
        source_table: &str,
        input_schema: &TableSchema,
        is_from_input: bool,
        lookup_row: F,
        _lookup_by_ref: G,
    ) -> DeltaBatch
    where
        F: Fn(&str, ObjectId) -> Option<Row>,
        G: Fn(&str, &str, ObjectId) -> Vec<Row>,
    {
        match self {
            QueryNode::Join {
                input_tables,
                join_table,
                join_column,
                join_schema,
                cached_rows,
                reverse_index,
            } => {
                let mut output = DeltaBatch::new();

                // Determine if this is an input delta or a join_table delta
                let is_join_table_delta = source_table == join_table && !is_from_input;
                let is_input_delta = is_from_input || input_tables.iter().any(|t| t == source_table);

                if is_input_delta && !is_join_table_delta {
                    // Delta from input (either raw table or combined row from prior join)
                    Self::eval_join_input_delta(
                        &delta,
                        input_tables,
                        join_table,
                        join_column,
                        input_schema,
                        join_schema,
                        cached_rows,
                        reverse_index,
                        &mut output,
                        &lookup_row,
                    );
                } else if is_join_table_delta {
                    // Delta from join_table - use reverse_index
                    Self::eval_join_table_delta(
                        &delta,
                        join_table,
                        join_schema,
                        cached_rows,
                        reverse_index,
                        &mut output,
                        &lookup_row,
                    );
                }
                // Note: If neither condition matches, the delta is for a downstream node.
                // The graph layer handles pass-through by skipping this node entirely.

                output
            }
            _ => DeltaBatch::new(),
        }
    }

    /// Handle a delta from the input side (prior tables in chain or single left table).
    #[allow(clippy::too_many_arguments)]
    fn eval_join_input_delta<F>(
        delta: &RowDelta,
        input_tables: &[String],
        join_table: &str,
        join_column: &str,
        input_schema: &TableSchema,
        _join_schema: &TableSchema,
        cached_rows: &mut HashMap<ObjectId, JoinedRow>,
        reverse_index: &mut HashMap<ObjectId, HashSet<ObjectId>>,
        output: &mut DeltaBatch,
        lookup_row: F,
    ) where
        F: Fn(&str, ObjectId) -> Option<Row>,
    {
        let primary_table = input_tables.first().map(|s| s.as_str()).unwrap_or("");

        match delta {
            RowDelta::Added(input_row) => {
                let primary_id = input_row.id;

                // Look up the referenced join_table row
                if let Some(join_id) = Self::get_ref_value(input_row, join_column, input_schema) {
                    if let Some(join_row) = lookup_row(join_table, join_id) {
                        // Create or extend joined row
                        let mut joined = if input_tables.len() == 1 {
                            // First join: start with single table row
                            JoinedRow::from_single(primary_table, input_row.clone())
                        } else {
                            // Chain join: input_row is already combined, convert back to JoinedRow
                            // The input_row.values contains all columns from prior joins
                            let mut jr = JoinedRow::from_single(primary_table, Row::new(primary_id, vec![]));
                            jr.values = input_row.values.clone();
                            // Reconstruct table_offsets from input_tables
                            let mut offset = 0;
                            for (i, table) in input_tables.iter().enumerate() {
                                if i == 0 {
                                    jr.table_offsets.insert(table.clone(), (primary_id, 0));
                                } else {
                                    // For chain joins, we don't have the intermediate IDs in the Row
                                    // This is a simplification - we use primary_id as placeholder
                                    jr.table_offsets.insert(table.clone(), (primary_id, offset));
                                }
                                // Count columns from this table (would need schema info for accuracy)
                                offset = jr.values.len(); // Approximate
                            }
                            jr
                        };
                        joined.add_joined(join_table, join_row);

                        // Update caches
                        cached_rows.insert(primary_id, joined.clone());
                        reverse_index.entry(join_id).or_default().insert(primary_id);

                        output.push(RowDelta::Added(joined.to_output_row()));
                    }
                }
            }

            RowDelta::Removed { id: primary_id, prior } => {
                // Remove from cached_rows
                if let Some(old_joined) = cached_rows.remove(primary_id) {
                    // Remove from reverse_index
                    if let Some(join_id) = old_joined.get_row_id(join_table) {
                        if let Some(set) = reverse_index.get_mut(&join_id) {
                            set.remove(primary_id);
                            if set.is_empty() {
                                reverse_index.remove(&join_id);
                            }
                        }
                    }
                    output.push(RowDelta::Removed {
                        id: *primary_id,
                        prior: prior.clone(),
                    });
                }
            }

            RowDelta::Updated { id: primary_id, new: input_row, prior } => {
                // Get old join_id to update reverse_index
                let old_join_id = cached_rows.get(primary_id)
                    .and_then(|jr| jr.get_row_id(join_table));

                // Get new join_id
                let new_join_id = Self::get_ref_value(input_row, join_column, input_schema);

                // Update reverse_index if join_id changed
                if old_join_id != new_join_id {
                    if let Some(old_id) = old_join_id {
                        if let Some(set) = reverse_index.get_mut(&old_id) {
                            set.remove(primary_id);
                            if set.is_empty() {
                                reverse_index.remove(&old_id);
                            }
                        }
                    }
                }

                // Remove old entry
                let existed = cached_rows.remove(primary_id).is_some();

                // Add new entry if join succeeds
                if let Some(join_id) = new_join_id {
                    if let Some(join_row) = lookup_row(join_table, join_id) {
                        let mut joined = if input_tables.len() == 1 {
                            JoinedRow::from_single(primary_table, input_row.clone())
                        } else {
                            let mut jr = JoinedRow::from_single(primary_table, Row::new(*primary_id, vec![]));
                            jr.values = input_row.values.clone();
                            jr
                        };
                        joined.add_joined(join_table, join_row);

                        cached_rows.insert(*primary_id, joined.clone());
                        reverse_index.entry(join_id).or_default().insert(*primary_id);

                        let output_row = joined.to_output_row();
                        if existed {
                            output.push(RowDelta::Updated {
                                id: *primary_id,
                                new: output_row,
                                prior: prior.clone(),
                            });
                        } else {
                            output.push(RowDelta::Added(output_row));
                        }
                    } else if existed {
                        // Join failed but row existed before
                        output.push(RowDelta::Removed {
                            id: *primary_id,
                            prior: prior.clone(),
                        });
                    }
                } else if existed {
                    // No join reference, row is removed from output
                    output.push(RowDelta::Removed {
                        id: *primary_id,
                        prior: prior.clone(),
                    });
                }
            }
        }
    }

    /// Handle a delta from the join_table side using reverse_index.
    #[allow(clippy::too_many_arguments)]
    fn eval_join_table_delta<F>(
        delta: &RowDelta,
        join_table: &str,
        _join_schema: &TableSchema,
        cached_rows: &mut HashMap<ObjectId, JoinedRow>,
        reverse_index: &mut HashMap<ObjectId, HashSet<ObjectId>>,
        output: &mut DeltaBatch,
        lookup_row: F,
    ) where
        F: Fn(&str, ObjectId) -> Option<Row>,
    {
        let join_id = delta.row_id();

        match delta {
            RowDelta::Added(join_row) => {
                // Find all cached rows that reference this join_id
                // (They wouldn't have joined before if the row didn't exist)
                // Actually, for Added, the reverse_index wouldn't have entries yet
                // unless we pre-populated it. For now, Added from join_table is a no-op
                // because input deltas would have already tried to join and failed.
                //
                // However, if the join_table row is added AFTER input rows were processed,
                // we need to re-check. This requires iterating cached_rows or having
                // pre-populated the reverse_index with "pending" entries.
                //
                // For simplicity, we rely on the fact that initialization processes
                // join_table rows before input rows. For true incrementality, we'd
                // need more sophisticated handling.
                let _ = join_row; // Suppress unused warning
            }

            RowDelta::Removed { prior, .. } => {
                // Find all primary_ids that were joined with this join_id
                if let Some(affected_ids) = reverse_index.remove(&join_id) {
                    for primary_id in affected_ids {
                        if cached_rows.remove(&primary_id).is_some() {
                            output.push(RowDelta::Removed {
                                id: primary_id,
                                prior: prior.clone(),
                            });
                        }
                    }
                }
            }

            RowDelta::Updated { new: join_row, prior, .. } => {
                // Find all primary_ids joined with this join_id and update them
                if let Some(affected_ids) = reverse_index.get(&join_id) {
                    for &primary_id in affected_ids.clone().iter() {
                        if let Some(old_joined) = cached_rows.get(&primary_id) {
                            // Rebuild the joined row with updated join_table data
                            let mut new_joined = old_joined.clone();
                            // Update the join_table portion
                            // This is tricky because we need to replace the join_table values
                            // For now, we look up the row again and rebuild
                            if let Some(fresh_join_row) = lookup_row(join_table, join_id) {
                                // Find where join_table columns start
                                if let Some((_, offset)) = new_joined.table_offsets.get(join_table) {
                                    let offset = *offset;
                                    // Replace values from offset onwards with new join_row values
                                    let end = offset + fresh_join_row.values.len();
                                    if end <= new_joined.values.len() {
                                        new_joined.values.splice(offset..end, fresh_join_row.values.iter().cloned());
                                    }
                                }
                                // Also update the row ID in table_offsets
                                new_joined.table_offsets.insert(join_table.to_string(), (join_row.id, new_joined.table_offsets.get(join_table).map(|(_, o)| *o).unwrap_or(0)));

                                cached_rows.insert(primary_id, new_joined.clone());
                                output.push(RowDelta::Updated {
                                    id: primary_id,
                                    new: new_joined.to_output_row(),
                                    prior: prior.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    /// Extract a Ref value from a row by column name.
    /// Handles both plain Ref values and NullableSome wrapped Ref values.
    fn get_ref_value(row: &Row, column: &str, schema: &TableSchema) -> Option<ObjectId> {
        let col_idx = schema.column_index(column)?;
        match row.values.get(col_idx)? {
            Value::Ref(id) => Some(*id),
            Value::NullableSome(inner) => match inner.as_ref() {
                Value::Ref(id) => Some(*id),
                _ => None,
            },
            Value::NullableNone => None,
            _ => None,
        }
    }

    /// Evaluate an ArrayAggregate node.
    ///
    /// Handles two types of deltas:
    /// - Outer table deltas (from input node): Add/remove/update outer rows
    /// - Inner table deltas: Update the arrays for affected outer rows
    ///
    /// `source_table` indicates which table the delta came from.
    /// `lookup_inner_rows` is a function to find all inner rows matching an outer id.
    /// `lookup_row_by_id` is a function to look up a row from any table by (table_name, id).
    pub fn evaluate_array_aggregate<F, G>(
        &mut self,
        delta: RowDelta,
        source_table: &str,
        outer_schema: &TableSchema,
        lookup_inner_rows: F,
        lookup_row_by_id: G,
    ) -> DeltaBatch
    where
        F: Fn(ObjectId) -> Vec<Row>,
        G: Fn(&str, ObjectId) -> Option<Row>,
    {
        match self {
            QueryNode::ArrayAggregate {
                outer_table,
                inner_table,
                inner_ref_column,
                inner_schema,
                inner_joins,
                array_column_index,
                cached_arrays,
                inner_to_outer,
                outer_rows,
                ..
            } => {
                let mut output = DeltaBatch::new();

                let is_outer_delta = source_table == outer_table;
                let is_inner_delta = source_table == inner_table;

                if is_outer_delta {
                    // Delta from outer table - add/remove/update outer rows
                    Self::array_aggregate_handle_outer_delta(
                        &delta,
                        outer_schema,
                        *array_column_index,
                        inner_joins,
                        inner_schema,
                        cached_arrays,
                        inner_to_outer,
                        outer_rows,
                        &mut output,
                        &lookup_inner_rows,
                        &lookup_row_by_id,
                    );
                } else if is_inner_delta {
                    // Delta from inner table - update arrays for affected outer rows
                    Self::array_aggregate_handle_inner_delta(
                        &delta,
                        inner_ref_column,
                        inner_schema,
                        inner_joins,
                        outer_schema,
                        *array_column_index,
                        cached_arrays,
                        inner_to_outer,
                        outer_rows,
                        &mut output,
                        &lookup_row_by_id,
                    );
                }

                output
            }
            _ => DeltaBatch::new(),
        }
    }

    /// Handle a delta from the outer table in ArrayAggregate.
    #[allow(clippy::too_many_arguments)]
    fn array_aggregate_handle_outer_delta<F, G>(
        delta: &RowDelta,
        _outer_schema: &TableSchema,
        array_column_index: i32,
        inner_joins: &[(String, String, TableSchema)],
        inner_schema: &TableSchema,
        cached_arrays: &mut HashMap<ObjectId, Vec<Row>>,
        inner_to_outer: &mut HashMap<ObjectId, ObjectId>,
        outer_rows: &mut HashMap<ObjectId, Row>,
        output: &mut DeltaBatch,
        lookup_inner_rows: F,
        lookup_row_by_id: G,
    ) where
        F: Fn(ObjectId) -> Vec<Row>,
        G: Fn(&str, ObjectId) -> Option<Row>,
    {
        match delta {
            RowDelta::Added(outer_row) => {
                let outer_id = outer_row.id;

                // Fetch all matching inner rows
                let raw_inner_rows = lookup_inner_rows(outer_id);

                // Resolve inner joins (e.g., replace label ref with full Labels row)
                let inner_rows = Self::resolve_inner_joins(
                    &raw_inner_rows,
                    inner_joins,
                    inner_schema,
                    &lookup_row_by_id,
                );

                // Update inner_to_outer index (use raw rows for ID tracking)
                for inner_row in &raw_inner_rows {
                    inner_to_outer.insert(inner_row.id, outer_id);
                }

                // Cache the resolved array
                cached_arrays.insert(outer_id, inner_rows.clone());

                // Build output row with array
                let output_row =
                    Self::build_output_row_with_array(outer_row, &inner_rows, array_column_index);
                outer_rows.insert(outer_id, output_row.clone());

                output.push(RowDelta::Added(output_row));
            }

            RowDelta::Removed { id, prior } => {
                let outer_id = *id;

                // Clean up inner_to_outer index
                if let Some(inner_rows) = cached_arrays.remove(&outer_id) {
                    for inner_row in inner_rows {
                        inner_to_outer.remove(&inner_row.id);
                    }
                }

                outer_rows.remove(&outer_id);

                output.push(RowDelta::Removed {
                    id: outer_id,
                    prior: prior.clone(),
                });
            }

            RowDelta::Updated { id, new: outer_row, prior } => {
                let outer_id = *id;

                // Fetch updated inner rows (in case correlation changed)
                let raw_inner_rows = lookup_inner_rows(outer_id);

                // Resolve inner joins
                let inner_rows = Self::resolve_inner_joins(
                    &raw_inner_rows,
                    inner_joins,
                    inner_schema,
                    &lookup_row_by_id,
                );

                // Update inner_to_outer index
                if let Some(old_inner_rows) = cached_arrays.get(&outer_id) {
                    for old_inner in old_inner_rows {
                        inner_to_outer.remove(&old_inner.id);
                    }
                }
                for inner_row in &raw_inner_rows {
                    inner_to_outer.insert(inner_row.id, outer_id);
                }

                cached_arrays.insert(outer_id, inner_rows.clone());

                let output_row =
                    Self::build_output_row_with_array(outer_row, &inner_rows, array_column_index);
                outer_rows.insert(outer_id, output_row.clone());

                output.push(RowDelta::Updated {
                    id: outer_id,
                    new: output_row,
                    prior: prior.clone(),
                });
            }
        }
    }

    /// Resolve inner joins for array rows.
    /// For each inner row, replace ref columns with their resolved row values.
    fn resolve_inner_joins<G>(
        inner_rows: &[Row],
        inner_joins: &[(String, String, TableSchema)],
        inner_schema: &TableSchema,
        lookup_row_by_id: G,
    ) -> Vec<Row>
    where
        G: Fn(&str, ObjectId) -> Option<Row>,
    {
        if inner_joins.is_empty() {
            return inner_rows.to_vec();
        }

        inner_rows
            .iter()
            .map(|row| {
                let mut resolved_values = row.values.clone();

                for (ref_column, target_table, _target_schema) in inner_joins {
                    // Find the column index for this ref column
                    if let Some(col_idx) = inner_schema.column_index(ref_column) {
                        // Get the ref value from the row
                        if let Some(Value::Ref(target_id)) = resolved_values.get(col_idx) {
                            // Look up the target row
                            if let Some(target_row) = lookup_row_by_id(target_table, *target_id) {
                                // Replace the ref with a nested Row value
                                // Build the nested row with id and values from target
                                let nested = Row {
                                    id: target_row.id,
                                    values: target_row.values.clone(),
                                };
                                resolved_values[col_idx] = Value::Row(Box::new(nested));
                            }
                        }
                    }
                }

                Row {
                    id: row.id,
                    values: resolved_values,
                }
            })
            .collect()
    }

    /// Handle a delta from the inner table in ArrayAggregate.
    #[allow(clippy::too_many_arguments)]
    fn array_aggregate_handle_inner_delta<G>(
        delta: &RowDelta,
        inner_ref_column: &str,
        inner_schema: &TableSchema,
        inner_joins: &[(String, String, TableSchema)],
        _outer_schema: &TableSchema,
        array_column_index: i32,
        cached_arrays: &mut HashMap<ObjectId, Vec<Row>>,
        inner_to_outer: &mut HashMap<ObjectId, ObjectId>,
        outer_rows: &mut HashMap<ObjectId, Row>,
        output: &mut DeltaBatch,
        lookup_row_by_id: G,
    ) where
        G: Fn(&str, ObjectId) -> Option<Row>,
    {
        match delta {
            RowDelta::Added(inner_row) => {
                let inner_id = inner_row.id;

                // Find which outer row this belongs to
                if let Some(outer_id) = Self::get_ref_value(inner_row, inner_ref_column, inner_schema) {
                    // Update inner_to_outer index
                    inner_to_outer.insert(inner_id, outer_id);

                    // Resolve inner joins for this row
                    let resolved_rows = Self::resolve_inner_joins(
                        &[inner_row.clone()],
                        inner_joins,
                        inner_schema,
                        &lookup_row_by_id,
                    );
                    let resolved_row = resolved_rows.into_iter().next().unwrap_or_else(|| inner_row.clone());

                    // Add to cached array
                    let array = cached_arrays.entry(outer_id).or_default();
                    array.push(resolved_row);

                    // Emit updated delta for outer row
                    if let Some(base_outer_row) = outer_rows.get(&outer_id) {
                        // Extract original outer values (without the array)
                        let outer_values = Self::extract_outer_values(base_outer_row, array_column_index);
                        let base_row = Row::new(outer_id, outer_values);
                        let output_row = Self::build_output_row_with_array(&base_row, array, array_column_index);
                        outer_rows.insert(outer_id, output_row.clone());

                        output.push(RowDelta::Updated {
                            id: outer_id,
                            new: output_row,
                            prior: crate::sql::query_graph::delta::PriorState::empty(),
                        });
                    }
                }
            }

            RowDelta::Removed { id: inner_id, prior } => {
                // Find which outer row this belonged to
                if let Some(outer_id) = inner_to_outer.remove(inner_id) {
                    // Remove from cached array
                    if let Some(array) = cached_arrays.get_mut(&outer_id) {
                        array.retain(|r| r.id != *inner_id);

                        // Emit updated delta for outer row
                        if let Some(base_outer_row) = outer_rows.get(&outer_id) {
                            let outer_values = Self::extract_outer_values(base_outer_row, array_column_index);
                            let base_row = Row::new(outer_id, outer_values);
                            let output_row = Self::build_output_row_with_array(&base_row, array, array_column_index);
                            outer_rows.insert(outer_id, output_row.clone());

                            output.push(RowDelta::Updated {
                                id: outer_id,
                                new: output_row,
                                prior: prior.clone(),
                            });
                        }
                    }
                }
            }

            RowDelta::Updated { id: inner_id, new: inner_row, prior } => {
                let old_outer_id = inner_to_outer.get(inner_id).copied();
                let new_outer_id = Self::get_ref_value(inner_row, inner_ref_column, inner_schema);

                // Resolve inner joins for this row
                let resolved_rows = Self::resolve_inner_joins(
                    &[inner_row.clone()],
                    inner_joins,
                    inner_schema,
                    &lookup_row_by_id,
                );
                let resolved_row = resolved_rows.into_iter().next().unwrap_or_else(|| inner_row.clone());

                if old_outer_id != new_outer_id {
                    // Inner row moved to different outer row
                    // Remove from old
                    if let Some(old_id) = old_outer_id {
                        inner_to_outer.remove(inner_id);
                        if let Some(array) = cached_arrays.get_mut(&old_id) {
                            array.retain(|r| r.id != *inner_id);

                            if let Some(base_outer_row) = outer_rows.get(&old_id) {
                                let outer_values = Self::extract_outer_values(base_outer_row, array_column_index);
                                let base_row = Row::new(old_id, outer_values);
                                let output_row = Self::build_output_row_with_array(&base_row, array, array_column_index);
                                outer_rows.insert(old_id, output_row.clone());

                                output.push(RowDelta::Updated {
                                    id: old_id,
                                    new: output_row,
                                    prior: prior.clone(),
                                });
                            }
                        }
                    }

                    // Add to new
                    if let Some(new_id) = new_outer_id {
                        inner_to_outer.insert(*inner_id, new_id);
                        let array = cached_arrays.entry(new_id).or_default();
                        array.push(resolved_row);

                        if let Some(base_outer_row) = outer_rows.get(&new_id) {
                            let outer_values = Self::extract_outer_values(base_outer_row, array_column_index);
                            let base_row = Row::new(new_id, outer_values);
                            let output_row = Self::build_output_row_with_array(&base_row, array, array_column_index);
                            outer_rows.insert(new_id, output_row.clone());

                            output.push(RowDelta::Updated {
                                id: new_id,
                                new: output_row,
                                prior: crate::sql::query_graph::delta::PriorState::empty(),
                            });
                        }
                    }
                } else if let Some(outer_id) = new_outer_id {
                    // Same outer row - update in place
                    if let Some(array) = cached_arrays.get_mut(&outer_id) {
                        // Replace the old inner row with the resolved new one
                        if let Some(idx) = array.iter().position(|r| r.id == *inner_id) {
                            array[idx] = resolved_row;
                        }

                        if let Some(base_outer_row) = outer_rows.get(&outer_id) {
                            let outer_values = Self::extract_outer_values(base_outer_row, array_column_index);
                            let base_row = Row::new(outer_id, outer_values);
                            let output_row = Self::build_output_row_with_array(&base_row, array, array_column_index);
                            outer_rows.insert(outer_id, output_row.clone());

                            output.push(RowDelta::Updated {
                                id: outer_id,
                                new: output_row,
                                prior: prior.clone(),
                            });
                        }
                    }
                }
            }
        }
    }

    /// Build an output row with the array value appended/inserted.
    fn build_output_row_with_array(outer_row: &Row, inner_rows: &[Row], array_column_index: i32) -> Row {
        let array_value = Value::Array(
            inner_rows
                .iter()
                .map(|r| Value::Row(Box::new(r.clone())))
                .collect(),
        );

        let mut values = outer_row.values.clone();
        if array_column_index < 0 {
            // Append at end
            values.push(array_value);
        } else {
            let idx = array_column_index as usize;
            if idx < values.len() {
                values[idx] = array_value;
            } else {
                values.push(array_value);
            }
        }

        Row::new(outer_row.id, values)
    }

    /// Extract outer row values without the array column.
    fn extract_outer_values(row: &Row, array_column_index: i32) -> Vec<Value> {
        if array_column_index < 0 {
            // Array is at end - remove last element
            if row.values.is_empty() {
                vec![]
            } else {
                row.values[..row.values.len() - 1].to_vec()
            }
        } else {
            let idx = array_column_index as usize;
            row.values
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != idx)
                .map(|(_, v)| v.clone())
                .collect()
        }
    }

    /// Evaluate a node that just passes through IDs while tracking membership.
    fn eval_id_passthrough(cached_ids: &mut HashSet<ObjectId>, input: DeltaBatch) -> DeltaBatch {
        let mut output = DeltaBatch::new();

        for delta in input.into_iter() {
            match &delta {
                RowDelta::Added(row) => {
                    if cached_ids.insert(row.id) {
                        output.push(delta);
                    }
                }
                RowDelta::Removed { id, .. } => {
                    if cached_ids.remove(id) {
                        output.push(delta);
                    }
                }
                RowDelta::Updated { id, .. } => {
                    if cached_ids.contains(id) {
                        output.push(delta);
                    }
                }
            }
        }

        output
    }

    /// Evaluate a filter node.
    fn eval_filter(
        predicate: &Predicate,
        cached_ids: &mut HashSet<ObjectId>,
        input: DeltaBatch,
        schema: &TableSchema,
    ) -> DeltaBatch {
        let mut output = DeltaBatch::new();

        for delta in input.into_iter() {
            match delta {
                RowDelta::Added(row) => {
                    if predicate.matches(&row, schema) {
                        cached_ids.insert(row.id);
                        output.push(RowDelta::Added(row));
                    }
                }

                RowDelta::Removed { id, prior } => {
                    // Only emit removal if it was in our cached set
                    if cached_ids.remove(&id) {
                        output.push(RowDelta::Removed { id, prior });
                    }
                }

                RowDelta::Updated { id, new, prior } => {
                    let was_in_set = cached_ids.contains(&id);
                    let is_match = predicate.matches(&new, schema);

                    match (was_in_set, is_match) {
                        (false, true) => {
                            // Row now matches the filter - enters the set
                            cached_ids.insert(id);
                            output.push(RowDelta::Added(new));
                        }
                        (true, false) => {
                            // Row no longer matches - leaves the set
                            cached_ids.remove(&id);
                            output.push(RowDelta::Removed { id, prior });
                        }
                        (true, true) => {
                            // Row still matches - propagate update
                            output.push(RowDelta::Updated { id, new, prior });
                        }
                        (false, false) => {
                            // Row still doesn't match - no output (early cutoff)
                        }
                    }
                }
            }
        }

        output
    }

    /// Evaluate a LimitOffset node.
    ///
    /// Maintains the full set of qualifying rows and emits deltas
    /// when the visible window changes.
    pub fn evaluate_limit_offset(
        &mut self,
        input: DeltaBatch,
        cache: &RowCache,
    ) -> DeltaBatch {
        match self {
            QueryNode::LimitOffset {
                table,
                limit,
                offset,
                all_rows,
                visible_ids,
                ..
            } => {
                let mut output = DeltaBatch::new();

                for delta in input.into_iter() {
                    match delta {
                        RowDelta::Added(row) => {
                            Self::limit_offset_handle_add(
                                row,
                                *limit,
                                *offset,
                                all_rows,
                                visible_ids,
                                &mut output,
                            );
                        }
                        RowDelta::Removed { id, prior } => {
                            Self::limit_offset_handle_remove(
                                id,
                                prior,
                                *limit,
                                *offset,
                                all_rows,
                                visible_ids,
                                table,
                                cache,
                                &mut output,
                            );
                        }
                        RowDelta::Updated { id, new, prior } => {
                            // Update the stored row if it exists
                            if let Some(row) = all_rows.get_mut(&id) {
                                *row = new.clone();
                                // If the row is visible, emit the update
                                if visible_ids.contains(&id) {
                                    output.push(RowDelta::Updated { id, new, prior });
                                }
                            }
                        }
                    }
                }

                output
            }
            _ => DeltaBatch::new(),
        }
    }

    /// Handle a row being added to the LimitOffset node.
    fn limit_offset_handle_add(
        row: Row,
        limit: Option<u64>,
        offset: u64,
        all_rows: &mut std::collections::BTreeMap<ObjectId, Row>,
        visible_ids: &mut HashSet<ObjectId>,
        output: &mut DeltaBatch,
    ) {
        let row_id = row.id;
        all_rows.insert(row_id, row.clone());

        // Compute the new visible window and changes
        let (new_visible, changes) = Self::compute_window_changes(all_rows, visible_ids, limit, offset);

        // Emit deltas for changes
        for (id, change_type) in changes {
            match change_type {
                WindowChange::Added => {
                    if let Some(r) = all_rows.get(&id) {
                        output.push(RowDelta::Added(r.clone()));
                    }
                }
                WindowChange::Removed => {
                    output.push(RowDelta::Removed {
                        id,
                        prior: crate::sql::query_graph::delta::PriorState::empty(),
                    });
                }
            }
        }

        *visible_ids = new_visible;
    }

    /// Handle a row being removed from the LimitOffset node.
    fn limit_offset_handle_remove(
        id: ObjectId,
        prior: crate::sql::query_graph::delta::PriorState,
        limit: Option<u64>,
        offset: u64,
        all_rows: &mut std::collections::BTreeMap<ObjectId, Row>,
        visible_ids: &mut HashSet<ObjectId>,
        table: &str,
        cache: &RowCache,
        output: &mut DeltaBatch,
    ) {
        let was_visible = visible_ids.contains(&id);
        all_rows.remove(&id);
        visible_ids.remove(&id);

        if was_visible {
            // Row was in visible window - emit removal
            output.push(RowDelta::Removed { id, prior });
        }

        // Recompute window - a row might be promoted or demoted
        let (new_visible, changes) = Self::compute_window_changes(all_rows, visible_ids, limit, offset);

        for (changed_id, change_type) in changes {
            match change_type {
                WindowChange::Added => {
                    // Try to get the row from all_rows first, then cache
                    if let Some(r) = all_rows.get(&changed_id) {
                        output.push(RowDelta::Added(r.clone()));
                    } else if let Some(Some(r)) = cache.get(table, changed_id) {
                        output.push(RowDelta::Added(r.clone()));
                    }
                }
                WindowChange::Removed => {
                    if changed_id != id {
                        // Only emit if not already removed above
                        output.push(RowDelta::Removed {
                            id: changed_id,
                            prior: crate::sql::query_graph::delta::PriorState::empty(),
                        });
                    }
                }
            }
        }

        *visible_ids = new_visible;
    }

    /// Compute the new visible window and return changes from the current state.
    fn compute_window_changes(
        all_rows: &std::collections::BTreeMap<ObjectId, Row>,
        current_visible: &HashSet<ObjectId>,
        limit: Option<u64>,
        offset: u64,
    ) -> (HashSet<ObjectId>, Vec<(ObjectId, WindowChange)>) {
        let offset = offset as usize;
        let limit = limit.map(|l| l as usize);

        // Compute new visible set using BTreeMap's sorted iteration
        let new_visible: HashSet<ObjectId> = all_rows
            .keys()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .copied()
            .collect();

        let mut changes = Vec::new();

        // Find removed (was visible, now not)
        for id in current_visible {
            if !new_visible.contains(id) {
                changes.push((*id, WindowChange::Removed));
            }
        }

        // Find added (was not visible, now is)
        for id in &new_visible {
            if !current_visible.contains(id) {
                changes.push((*id, WindowChange::Added));
            }
        }

        (new_visible, changes)
    }
}

/// Represents a change to the visible window in a LimitOffset node.
enum WindowChange {
    Added,
    Removed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::row::{Row, Value};
    use crate::sql::schema::{ColumnDef, ColumnType};
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

    fn make_row(id: u128, name: &str, active: bool) -> Row {
        Row::new(
            ObjectId::new(id),
            vec![Value::String(name.to_string()), Value::Bool(active)],
        )
    }

    #[test]
    fn table_scan_add() {
        let mut node = QueryNode::TableScan {
            table: "users".to_string(),
            cached_ids: HashSet::new(),
        };

        let schema = test_schema();
        let cache = RowCache::new();

        let row = make_row(1, "Alice", true);
        let delta = DeltaBatch::added(row.clone());

        let output = node.evaluate(delta, &schema, &cache);

        assert_eq!(output.len(), 1);
        assert!(node.cached_ids().unwrap().contains(&ObjectId::new(1)));
    }

    #[test]
    fn table_scan_remove() {
        let mut node = QueryNode::TableScan {
            table: "users".to_string(),
            cached_ids: HashSet::from([ObjectId::new(1)]),
        };

        let schema = test_schema();
        let cache = RowCache::new();

        let delta = DeltaBatch::removed(ObjectId::new(1), vec![]);

        let output = node.evaluate(delta, &schema, &cache);

        assert_eq!(output.len(), 1);
        assert!(!node.cached_ids().unwrap().contains(&ObjectId::new(1)));
    }

    #[test]
    fn table_scan_remove_not_present() {
        let mut node = QueryNode::TableScan {
            table: "users".to_string(),
            cached_ids: HashSet::new(),
        };

        let schema = test_schema();
        let cache = RowCache::new();

        let delta = DeltaBatch::removed(ObjectId::new(1), vec![]);

        let output = node.evaluate(delta, &schema, &cache);

        // Should produce no output since ID wasn't in set
        assert!(output.is_empty());
    }

    #[test]
    fn filter_add_match() {
        let mut node = QueryNode::Filter {
            table: "users".to_string(),
            input: NodeId(0),
            predicate: Predicate::eq("active", Value::Bool(true)),
            cached_ids: HashSet::new(),
        };

        let schema = test_schema();
        let cache = RowCache::new();

        let row = make_row(1, "Alice", true);
        let delta = DeltaBatch::added(row);

        let output = node.evaluate(delta, &schema, &cache);

        assert_eq!(output.len(), 1);
        assert!(node.cached_ids().unwrap().contains(&ObjectId::new(1)));
    }

    #[test]
    fn filter_add_no_match() {
        let mut node = QueryNode::Filter {
            table: "users".to_string(),
            input: NodeId(0),
            predicate: Predicate::eq("active", Value::Bool(true)),
            cached_ids: HashSet::new(),
        };

        let schema = test_schema();
        let cache = RowCache::new();

        let row = make_row(1, "Alice", false); // active = false
        let delta = DeltaBatch::added(row);

        let output = node.evaluate(delta, &schema, &cache);

        // Early cutoff - no output
        assert!(output.is_empty());
        assert!(!node.cached_ids().unwrap().contains(&ObjectId::new(1)));
    }

    #[test]
    fn filter_update_enters_set() {
        let mut node = QueryNode::Filter {
            table: "users".to_string(),
            input: NodeId(0),
            predicate: Predicate::eq("active", Value::Bool(true)),
            cached_ids: HashSet::new(), // Not in set initially
        };

        let schema = test_schema();
        let cache = RowCache::new();

        // Update: was inactive, now active
        let new_row = make_row(1, "Alice", true);
        let delta = DeltaBatch::updated(ObjectId::new(1), new_row, vec![]);

        let output = node.evaluate(delta, &schema, &cache);

        assert_eq!(output.len(), 1);
        // Should be Added since it entered the filtered set
        assert!(matches!(output.iter().next(), Some(RowDelta::Added(_))));
        assert!(node.cached_ids().unwrap().contains(&ObjectId::new(1)));
    }

    #[test]
    fn filter_update_leaves_set() {
        let mut node = QueryNode::Filter {
            table: "users".to_string(),
            input: NodeId(0),
            predicate: Predicate::eq("active", Value::Bool(true)),
            cached_ids: HashSet::from([ObjectId::new(1)]), // In set initially
        };

        let schema = test_schema();
        let cache = RowCache::new();

        // Update: was active, now inactive
        let new_row = make_row(1, "Alice", false);
        let delta = DeltaBatch::updated(ObjectId::new(1), new_row, vec![]);

        let output = node.evaluate(delta, &schema, &cache);

        assert_eq!(output.len(), 1);
        // Should be Removed since it left the filtered set
        assert!(matches!(
            output.iter().next(),
            Some(RowDelta::Removed { .. })
        ));
        assert!(!node.cached_ids().unwrap().contains(&ObjectId::new(1)));
    }

    #[test]
    fn filter_update_stays_in_set() {
        let mut node = QueryNode::Filter {
            table: "users".to_string(),
            input: NodeId(0),
            predicate: Predicate::eq("active", Value::Bool(true)),
            cached_ids: HashSet::from([ObjectId::new(1)]),
        };

        let schema = test_schema();
        let cache = RowCache::new();

        // Update: still active, just name change
        let new_row = make_row(1, "Alicia", true);
        let delta = DeltaBatch::updated(ObjectId::new(1), new_row, vec![]);

        let output = node.evaluate(delta, &schema, &cache);

        assert_eq!(output.len(), 1);
        // Should be Updated since it stayed in the set
        assert!(matches!(
            output.iter().next(),
            Some(RowDelta::Updated { .. })
        ));
    }

    #[test]
    fn filter_update_stays_out_of_set() {
        let mut node = QueryNode::Filter {
            table: "users".to_string(),
            input: NodeId(0),
            predicate: Predicate::eq("active", Value::Bool(true)),
            cached_ids: HashSet::new(),
        };

        let schema = test_schema();
        let cache = RowCache::new();

        // Update: still inactive
        let new_row = make_row(1, "Alicia", false);
        let delta = DeltaBatch::updated(ObjectId::new(1), new_row, vec![]);

        let output = node.evaluate(delta, &schema, &cache);

        // Early cutoff - no output since row never matched
        assert!(output.is_empty());
    }

    // --- RecursiveFilter tests ---

    fn folder_schema() -> TableSchema {
        TableSchema::new(
            "folders",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::required("owner_id", ColumnType::Ref("users".to_string())),
                ColumnDef::optional("parent_id", ColumnType::Ref("folders".to_string())),
            ],
        )
    }

    fn make_folder(id: u128, name: &str, owner_id: u128, parent_id: Option<u128>) -> Row {
        Row::new(
            ObjectId::new(id),
            vec![
                Value::String(name.to_string()),
                Value::Ref(ObjectId::new(owner_id)),
                match parent_id {
                    Some(p) => Value::NullableSome(Box::new(Value::Ref(ObjectId::new(p)))),
                    None => Value::NullableNone,
                },
            ],
        )
    }

    const ALICE: u128 = 100;
    const BOB: u128 = 200;

    #[test]
    fn recursive_filter_base_access() {
        // Test: root folder owned by viewer is accessible
        let schema = folder_schema();
        let viewer = ObjectId::new(ALICE);

        let mut node = QueryNode::RecursiveFilter {
            table: "folders".to_string(),
            input: NodeId(0),
            base_predicate: Predicate::eq("owner_id", Value::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Add a root folder owned by Alice
        let root = make_folder(1, "root", ALICE, None);
        let delta = DeltaBatch::added(root.clone());

        let output = node.evaluate_recursive(delta, &schema);

        assert_eq!(output.len(), 1);
        assert!(matches!(output.iter().next(), Some(RowDelta::Added(r)) if r.id.0 == 1));
        assert!(node.accessible().unwrap().contains_key(&ObjectId::new(1)));
        assert_eq!(node.accessible().unwrap().get(&ObjectId::new(1)), Some(&AccessReason::Base));
    }

    #[test]
    fn recursive_filter_no_base_access() {
        // Test: folder owned by someone else is not accessible
        let schema = folder_schema();
        let viewer = ObjectId::new(ALICE);

        let mut node = QueryNode::RecursiveFilter {
            table: "folders".to_string(),
            input: NodeId(0),
            base_predicate: Predicate::eq("owner_id", Value::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Add a root folder owned by Bob (not Alice)
        let root = make_folder(1, "bobs-folder", BOB, None);
        let delta = DeltaBatch::added(root);

        let output = node.evaluate_recursive(delta, &schema);

        assert!(output.is_empty());
        assert!(!node.accessible().unwrap().contains_key(&ObjectId::new(1)));
    }

    #[test]
    fn recursive_filter_inherited_access() {
        // Test: child folder inherits access from parent
        let schema = folder_schema();
        let viewer = ObjectId::new(ALICE);

        let mut node = QueryNode::RecursiveFilter {
            table: "folders".to_string(),
            input: NodeId(0),
            base_predicate: Predicate::eq("owner_id", Value::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Add root folder owned by Alice
        let root = make_folder(1, "root", ALICE, None);
        let delta = DeltaBatch::added(root);
        node.evaluate_recursive(delta, &schema);

        // Add child folder owned by Bob but parented to Alice's folder
        let child = make_folder(2, "child", BOB, Some(1));
        let delta = DeltaBatch::added(child);
        let output = node.evaluate_recursive(delta, &schema);

        assert_eq!(output.len(), 1);
        assert!(node.accessible().unwrap().contains_key(&ObjectId::new(2)));
        assert_eq!(
            node.accessible().unwrap().get(&ObjectId::new(2)),
            Some(&AccessReason::Inherited)
        );
    }

    #[test]
    fn recursive_filter_cascading_access() {
        // Test: grandchild becomes accessible when parent is added
        let schema = folder_schema();
        let viewer = ObjectId::new(ALICE);

        let mut node = QueryNode::RecursiveFilter {
            table: "folders".to_string(),
            input: NodeId(0),
            base_predicate: Predicate::eq("owner_id", Value::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Add grandchild first (parent doesn't exist yet)
        let grandchild = make_folder(3, "grandchild", BOB, Some(2));
        let delta = DeltaBatch::added(grandchild);
        let output = node.evaluate_recursive(delta, &schema);
        assert!(output.is_empty()); // Not yet accessible

        // Add child (parent doesn't exist yet)
        let child = make_folder(2, "child", BOB, Some(1));
        let delta = DeltaBatch::added(child);
        let output = node.evaluate_recursive(delta, &schema);
        assert!(output.is_empty()); // Still not accessible

        // Add root owned by Alice - should cascade to child and grandchild
        let root = make_folder(1, "root", ALICE, None);
        let delta = DeltaBatch::added(root);
        let output = node.evaluate_recursive(delta, &schema);

        // Should have 3 added deltas: root + child + grandchild
        assert_eq!(output.len(), 3);
        assert!(node.accessible().unwrap().contains_key(&ObjectId::new(1)));
        assert!(node.accessible().unwrap().contains_key(&ObjectId::new(2)));
        assert!(node.accessible().unwrap().contains_key(&ObjectId::new(3)));
    }

    #[test]
    fn recursive_filter_removal_cascades() {
        // Test: removing parent cascades removal to children
        let schema = folder_schema();
        let viewer = ObjectId::new(ALICE);

        let mut node = QueryNode::RecursiveFilter {
            table: "folders".to_string(),
            input: NodeId(0),
            base_predicate: Predicate::eq("owner_id", Value::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Set up: root -> child -> grandchild
        let root = make_folder(1, "root", ALICE, None);
        let child = make_folder(2, "child", BOB, Some(1));
        let grandchild = make_folder(3, "grandchild", BOB, Some(2));

        node.evaluate_recursive(DeltaBatch::added(root), &schema);
        node.evaluate_recursive(DeltaBatch::added(child), &schema);
        node.evaluate_recursive(DeltaBatch::added(grandchild), &schema);

        assert_eq!(node.accessible().unwrap().len(), 3);

        // Remove root - should cascade to child and grandchild
        let delta = DeltaBatch::removed(ObjectId::new(1), vec![]);
        let output = node.evaluate_recursive(delta, &schema);

        // Should have 3 removed deltas
        assert_eq!(output.len(), 3);
        assert!(node.accessible().unwrap().is_empty());
    }

    #[test]
    fn recursive_filter_child_keeps_base_access_after_parent_removal() {
        // Test: child with base access keeps it when parent is removed
        let schema = folder_schema();
        let viewer = ObjectId::new(ALICE);

        let mut node = QueryNode::RecursiveFilter {
            table: "folders".to_string(),
            input: NodeId(0),
            base_predicate: Predicate::eq("owner_id", Value::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Root owned by Alice
        let root = make_folder(1, "root", ALICE, None);
        // Child also owned by Alice (Both access)
        let child = make_folder(2, "child", ALICE, Some(1));

        node.evaluate_recursive(DeltaBatch::added(root), &schema);
        node.evaluate_recursive(DeltaBatch::added(child), &schema);

        assert_eq!(
            node.accessible().unwrap().get(&ObjectId::new(2)),
            Some(&AccessReason::Both)
        );

        // Remove root
        let delta = DeltaBatch::removed(ObjectId::new(1), vec![]);
        let output = node.evaluate_recursive(delta, &schema);

        // Only root should be removed, child keeps access
        assert_eq!(output.len(), 1);
        assert!(!node.accessible().unwrap().contains_key(&ObjectId::new(1)));
        assert!(node.accessible().unwrap().contains_key(&ObjectId::new(2)));
        // Access reason should be downgraded to Base
        assert_eq!(
            node.accessible().unwrap().get(&ObjectId::new(2)),
            Some(&AccessReason::Base)
        );
    }
}
