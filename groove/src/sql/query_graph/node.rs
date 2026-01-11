//! Query graph nodes.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::object::ObjectId;
use crate::sql::query_graph::cache::RowCache;
use crate::sql::query_graph::delta::{BufferJoinedRow, DeltaBatch, PriorState, RowDelta};
use crate::sql::query_graph::predicate::Predicate;
use crate::sql::row_buffer::{OwnedRow, RowDescriptor, RowValue};
use crate::sql::schema::{ColumnType, TableSchema};
use crate::sql::types::IndexKey;

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
        /// Row descriptor for buffer format operations.
        descriptor: Arc<RowDescriptor>,
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
        /// Descriptors for each table (input tables + join table).
        /// Used for buffer format conversion.
        table_descriptors: HashMap<String, Arc<RowDescriptor>>,
        /// Cached rows: primary_id → BufferJoinedRow.
        /// primary_id is always the leftmost table's row ID.
        /// Uses buffer format for efficient storage.
        cached_rows: HashMap<ObjectId, BufferJoinedRow>,
        /// Reverse index: join_table_id → set of primary_ids.
        /// Used for handling deltas from join_table.
        reverse_index: HashMap<ObjectId, HashSet<ObjectId>>,
        /// Optional filter predicate for reverse joins.
        /// Applied to join_table rows during lookup.
        reverse_filter: Option<Predicate>,
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
        /// Row descriptor for buffer format rows.
        descriptor: Arc<RowDescriptor>,
        /// Currently accessible rows with their access reason
        accessible: HashMap<ObjectId, AccessReason>,
        /// Reverse index: parent_id -> set of children
        /// Used for efficient cascade propagation
        children_index: HashMap<ObjectId, HashSet<ObjectId>>,
        /// All rows in the table (needed for fixpoint iteration).
        /// Uses buffer format (OwnedRow) for efficient storage.
        all_rows: HashMap<ObjectId, OwnedRow>,
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
        /// Descriptor for inner table rows (buffer format).
        inner_descriptor: Arc<RowDescriptor>,
        /// Descriptor for output rows (outer columns + array column).
        output_descriptor: Arc<RowDescriptor>,
        /// JOINs within the ARRAY subquery (e.g., JOIN Labels ON il.label = Labels.id).
        /// Each tuple: (ref_column_in_inner, target_table, target_schema)
        /// The ref column will be replaced by the joined table's columns.
        inner_joins: Vec<(String, String, TableSchema)>,
        /// Index in output row where the array should be placed.
        /// -1 means append at end.
        array_column_index: i32,
        /// Cached arrays: outer_id → Vec<OwnedRow> (inner rows).
        cached_arrays: HashMap<ObjectId, Vec<OwnedRow>>,
        /// Reverse index: inner_id → outer_id (for propagating inner changes).
        inner_to_outer: HashMap<ObjectId, ObjectId>,
        /// Cached outer rows (needed to emit Updated deltas with full row data).
        /// Uses buffer format (OwnedRow).
        outer_rows: HashMap<ObjectId, OwnedRow>,
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
        /// Row descriptor for buffer format rows.
        descriptor: Arc<RowDescriptor>,
        /// All rows that passed upstream filters, sorted by ObjectId.
        /// Uses buffer format (OwnedRow) for efficient storage.
        all_rows: BTreeMap<ObjectId, OwnedRow>,
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

    /// Get all rows stored in a RecursiveFilter node (buffer format).
    pub fn all_rows(&self) -> Option<&HashMap<ObjectId, OwnedRow>> {
        match self {
            QueryNode::RecursiveFilter { all_rows, .. } => Some(all_rows),
            _ => None,
        }
    }

    /// Get the cached joined rows keyed by primary_id (for Join nodes).
    /// Returns buffer-format joined rows.
    pub fn cached_rows(&self) -> Option<&HashMap<ObjectId, BufferJoinedRow>> {
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
    pub fn cached_joined(&self) -> Option<&HashMap<ObjectId, BufferJoinedRow>> {
        self.cached_rows()
    }

    /// Get the cached arrays for ArrayAggregate nodes.
    pub fn cached_arrays(&self) -> Option<&HashMap<ObjectId, Vec<OwnedRow>>> {
        match self {
            QueryNode::ArrayAggregate { cached_arrays, .. } => Some(cached_arrays),
            _ => None,
        }
    }

    /// Get the outer rows for ArrayAggregate nodes.
    pub fn outer_rows(&self) -> Option<&HashMap<ObjectId, OwnedRow>> {
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
                descriptor,
                cached_ids,
                ..
            } => Self::eval_filter(predicate, descriptor, cached_ids, input),

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
    ///
    /// Internally stores rows in buffer format (OwnedRow) for efficiency,
    /// converting at boundaries.
    pub fn evaluate_recursive(
        &mut self,
        input: DeltaBatch,
        schema: &TableSchema,
    ) -> DeltaBatch {
        match self {
            QueryNode::RecursiveFilter {
                base_predicate,
                recursive_column,
                descriptor,
                accessible,
                children_index,
                all_rows,
                ..
            } => {
                let mut output = DeltaBatch::new();

                for delta in input.into_iter() {
                    match delta {
                        RowDelta::Added { id, row } => {
                            Self::recursive_handle_insert(
                                id,
                                row,
                                descriptor.clone(),
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
                                accessible,
                                children_index,
                                all_rows,
                                &mut output,
                            );
                        }
                        RowDelta::Updated { id, row, prior } => {
                            // Handle as remove + insert for simplicity
                            // (Could optimize for cases where parent_id doesn't change)
                            Self::recursive_handle_remove(
                                id,
                                prior.clone(),
                                recursive_column,
                                accessible,
                                children_index,
                                all_rows,
                                &mut output,
                            );
                            Self::recursive_handle_insert(
                                id,
                                row,
                                descriptor.clone(),
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
        row_id: ObjectId,
        owned_row: OwnedRow,
        descriptor: Arc<RowDescriptor>,
        base_predicate: &Predicate,
        recursive_column: &str,
        accessible: &mut HashMap<ObjectId, AccessReason>,
        children_index: &mut HashMap<ObjectId, HashSet<ObjectId>>,
        all_rows: &mut HashMap<ObjectId, OwnedRow>,
        output: &mut DeltaBatch,
    ) {
        // Store in buffer format
        all_rows.insert(row_id, owned_row.clone());

        // Update children index: this row is a child of its parent
        if let Some(parent_id) = Self::get_ref_value_buffer(&owned_row, recursive_column, None) {
            children_index
                .entry(parent_id)
                .or_default()
                .insert(row_id);
        }

        // Check if row is accessible (use buffer-based predicate matching)
        let base_match = base_predicate.matches_buffer(row_id, owned_row.as_ref(), &descriptor);
        let parent_id = Self::get_ref_value_buffer(&owned_row, recursive_column, None);
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
            // Output in buffer format
            output.push(RowDelta::Added { id: row_id, row: owned_row.clone() });

            // Cascade: check if any existing rows are children of this row
            // and should now become accessible
            Self::propagate_access_to_children(
                row_id,
                &descriptor,
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
        descriptor: &Arc<RowDescriptor>,
        base_predicate: &Predicate,
        recursive_column: &str,
        accessible: &mut HashMap<ObjectId, AccessReason>,
        children_index: &HashMap<ObjectId, HashSet<ObjectId>>,
        all_rows: &HashMap<ObjectId, OwnedRow>,
        output: &mut DeltaBatch,
    ) {
        if let Some(children) = children_index.get(&parent_id) {
            for &child_id in children {
                // Skip if already accessible
                if accessible.contains_key(&child_id) {
                    continue;
                }

                // Child becomes accessible via inheritance
                if let Some(owned_row) = all_rows.get(&child_id) {
                    let base_match = base_predicate.matches_buffer(child_id, owned_row.as_ref(), descriptor);
                    let reason = if base_match {
                        AccessReason::Both
                    } else {
                        AccessReason::Inherited
                    };
                    accessible.insert(child_id, reason);
                    // Output in buffer format
                    output.push(RowDelta::Added { id: child_id, row: owned_row.clone() });

                    // Recursively propagate to grandchildren
                    Self::propagate_access_to_children(
                        child_id,
                        descriptor,
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
        prior: PriorState,
        recursive_column: &str,
        accessible: &mut HashMap<ObjectId, AccessReason>,
        children_index: &mut HashMap<ObjectId, HashSet<ObjectId>>,
        all_rows: &mut HashMap<ObjectId, OwnedRow>,
        output: &mut DeltaBatch,
    ) {
        // Remove from all_rows
        let removed_row = all_rows.remove(&row_id);

        // Remove from children_index (this row as a child of its parent)
        if let Some(owned_row) = &removed_row {
            if let Some(parent_id) = Self::get_ref_value_buffer(owned_row, recursive_column, None) {
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
        prior: PriorState,
        accessible: &mut HashMap<ObjectId, AccessReason>,
        children_index: &HashMap<ObjectId, HashSet<ObjectId>>,
        _all_rows: &HashMap<ObjectId, OwnedRow>,
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
                                accessible,
                                children_index,
                                _all_rows,
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
        lookup_by_ref: G,
    ) -> DeltaBatch
    where
        F: Fn(&str, ObjectId) -> Option<OwnedRow>,
        G: Fn(&str, &str, ObjectId) -> Vec<(ObjectId, OwnedRow)>,
    {
        match self {
            QueryNode::Join {
                input_tables,
                join_table,
                join_column,
                join_schema,
                table_descriptors,
                cached_rows,
                reverse_index,
                reverse_filter,
            } => {
                let mut output = DeltaBatch::new();

                // Determine if this is an input delta or a join_table delta
                let is_join_table_delta = source_table == join_table && !is_from_input;
                let is_input_delta = is_from_input || input_tables.iter().any(|t| t == source_table);

                // Build combined schema for output conversion
                // The combined schema has qualified column names in order
                // For chain joins (input already combined), use extend_with to preserve
                // existing column names and only qualify the new join table's columns.
                // For first joins (single table input), use combine to qualify both.
                let is_chain_join = input_tables.len() > 1 || input_schema.name.contains('+');
                let combined_schema = if is_chain_join {
                    input_schema.extend_with(join_schema)
                } else {
                    input_schema.combine(join_schema)
                };

                if is_input_delta && !is_join_table_delta {
                    // Delta from input (either raw table or combined row from prior join)
                    Self::eval_join_input_delta(
                        &delta,
                        input_tables,
                        join_table,
                        join_column,
                        input_schema,
                        join_schema,
                        &combined_schema,
                        table_descriptors,
                        cached_rows,
                        reverse_index,
                        reverse_filter.as_ref(),
                        &mut output,
                        &lookup_row,
                        &lookup_by_ref,
                    );
                } else if is_join_table_delta {
                    // Delta from join_table - use reverse_index
                    Self::eval_join_table_delta(
                        &delta,
                        join_table,
                        join_schema,
                        &combined_schema,
                        table_descriptors,
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
    fn eval_join_input_delta<F, G>(
        delta: &RowDelta,
        input_tables: &[String],
        join_table: &str,
        join_column: &str,
        _input_schema: &TableSchema,
        join_schema: &TableSchema,
        _combined_schema: &TableSchema,
        table_descriptors: &HashMap<String, Arc<RowDescriptor>>,
        cached_rows: &mut HashMap<ObjectId, BufferJoinedRow>,
        reverse_index: &mut HashMap<ObjectId, HashSet<ObjectId>>,
        reverse_filter: Option<&Predicate>,
        output: &mut DeltaBatch,
        lookup_row: F,
        find_referencing: G,
    ) where
        F: Fn(&str, ObjectId) -> Option<OwnedRow>,
        G: Fn(&str, &str, ObjectId) -> Vec<(ObjectId, OwnedRow)>,
    {
        let primary_table = input_tables.first().map(|s| s.as_str()).unwrap_or("");

        // Check if this is a reverse chain join (format: "target@existing.column")
        let is_reverse = join_column.contains('@');

        match delta {
            RowDelta::Added { id: primary_id, row: input_row } => {
                if is_reverse {
                    // Reverse join: find join_table rows where ref_column = primary_id
                    // Format: "target@existing.column"
                    // We include the join table's columns in the output so downstream
                    // Filter nodes can filter on them (e.g., for INHERITS policy checks).
                    // The projection_table setting handles final output projection.
                    if let Some(ref_col) = join_column.split('@').nth(1).and_then(|s| s.split('.').nth(1)) {
                        let all_join_rows = find_referencing(join_table, ref_col, *primary_id);

                        // Get descriptor for filter matching
                        let join_descriptor = table_descriptors.get(join_table)
                            .cloned()
                            .unwrap_or_else(|| Arc::new(RowDescriptor::from_table_schema(join_schema)));

                        // Filter join rows if a reverse_filter predicate is provided
                        let join_rows: Vec<_> = if let Some(filter) = reverse_filter {
                            all_join_rows.into_iter()
                                .filter(|(id, row)| {
                                    filter.matches_buffer(*id, row.as_ref(), &join_descriptor)
                                })
                                .collect()
                        } else {
                            all_join_rows
                        };

                        // For reverse joins, output once if there's at least one matching join row.
                        // Include the first matching join row's columns so downstream filters can
                        // check predicates on the join table (e.g., folders.owner_id = @viewer).
                        if !join_rows.is_empty() {
                            // Start with the primary table's row
                            let mut joined = BufferJoinedRow::from_single(primary_table, *primary_id, input_row.clone());

                            // Add the first matching join row (qualified) for downstream filtering
                            let (first_join_id, first_join_row) = &join_rows[0];
                            let qualified_join_row = first_join_row.qualify_columns(join_table, join_schema);
                            joined.add_joined(join_table, *first_join_id, qualified_join_row);

                            // Cache the joined row
                            cached_rows.insert(*primary_id, joined.clone());

                            // Track all reverse join row IDs for this primary row
                            for (join_id, _) in &join_rows {
                                reverse_index.entry(*primary_id).or_default().insert(*join_id);
                            }

                            // Output the joined row (includes both tables' columns)
                            output.push(RowDelta::Added {
                                id: *primary_id,
                                row: joined.to_output_row(),
                            });
                        }
                        // If no join rows match, don't output anything (filtered out)
                    }
                } else {
                    // Forward join: look up join_table row by ref value
                    if let Some(join_id) = Self::get_ref_value_buffer(input_row, join_column, Some(primary_table)) {
                        if let Some(join_row) = lookup_row(join_table, join_id) {
                            // Qualify the join row's columns (lookup returns unqualified names)
                            let qualified_join_row = join_row.qualify_columns(join_table, join_schema);

                            // Check if this is a chain join (input contains multiple tables)
                            let is_chain_join = input_tables.len() > 1;

                            let mut joined = if is_chain_join {
                                // Chain join: input row contains combined data from all input_tables
                                // The input OwnedRow already has all the data, create BufferJoinedRow from it
                                let mut jr = BufferJoinedRow::new(primary_table, *primary_id);

                                // For chain joins, we need to split the combined input_row back into per-table rows
                                // This is complex - for now, use a single entry with the combined data
                                // TODO: Properly track per-table rows in chain joins
                                jr.add_joined(primary_table, *primary_id, input_row.clone());
                                jr
                            } else {
                                // Single table input: simple case - input_row is already in buffer format
                                BufferJoinedRow::from_single(primary_table, *primary_id, input_row.clone())
                            };

                            // Add the join row with qualified column names
                            joined.add_joined(join_table, join_id, qualified_join_row);

                            // Update caches
                            cached_rows.insert(*primary_id, joined.clone());
                            reverse_index.entry(join_id).or_default().insert(*primary_id);

                            output.push(RowDelta::Added {
                                id: *primary_id,
                                row: joined.to_output_row(),
                            });
                        }
                    }
                }
            }

            RowDelta::Removed { id: primary_id, prior } => {
                // Remove from cached_rows
                if cached_rows.remove(primary_id).is_some() {
                    // Remove from reverse_index
                    if is_reverse {
                        // For reverse joins, reverse_index is keyed by primary_id
                        reverse_index.remove(primary_id);
                    } else {
                        // For forward joins, reverse_index is keyed by join_id
                        // We need to find which join_id this primary_id was in
                        // Iterate to find and clean up (this is O(n) but Removed is rare)
                        let mut found_join_id = None;
                        for (join_id, set) in reverse_index.iter_mut() {
                            if set.remove(primary_id) {
                                if set.is_empty() {
                                    found_join_id = Some(*join_id);
                                }
                                break;
                            }
                        }
                        if let Some(join_id) = found_join_id {
                            reverse_index.remove(&join_id);
                        }
                    }
                    output.push(RowDelta::Removed {
                        id: *primary_id,
                        prior: prior.clone(),
                    });
                }
            }

            RowDelta::Updated { id: primary_id, row: input_row, prior } => {
                if is_reverse {
                    // Reverse join: check if updated row still has matching join rows
                    if let Some(ref_col) = join_column.split('@').nth(1).and_then(|s| s.split('.').nth(1)) {
                        let existed = cached_rows.remove(primary_id).is_some();
                        reverse_index.remove(primary_id);

                        let all_join_rows = find_referencing(join_table, ref_col, *primary_id);

                        // Get descriptor for filter matching
                        let join_descriptor = table_descriptors.get(join_table)
                            .cloned()
                            .unwrap_or_else(|| Arc::new(RowDescriptor::from_table_schema(join_schema)));

                        // Filter join rows if a reverse_filter predicate is provided
                        let join_rows: Vec<_> = if let Some(filter) = reverse_filter {
                            all_join_rows.into_iter()
                                .filter(|(id, row)| filter.matches_buffer(*id, row.as_ref(), &join_descriptor))
                                .collect()
                        } else {
                            all_join_rows
                        };

                        if !join_rows.is_empty() {
                            // Still has matching rows - update or add
                            // Include the first matching join row for downstream filtering
                            let mut joined = BufferJoinedRow::from_single(primary_table, *primary_id, input_row.clone());

                            // Add the first matching join row (qualified) for downstream filtering
                            let (first_join_id, first_join_row) = &join_rows[0];
                            let qualified_join_row = first_join_row.qualify_columns(join_table, join_schema);
                            joined.add_joined(join_table, *first_join_id, qualified_join_row);

                            cached_rows.insert(*primary_id, joined.clone());
                            for (join_id, _) in &join_rows {
                                reverse_index.entry(*primary_id).or_default().insert(*join_id);
                            }

                            let output_row = joined.to_output_row();
                            if existed {
                                output.push(RowDelta::Updated {
                                    id: *primary_id,
                                    row: output_row,
                                    prior: prior.clone(),
                                });
                            } else {
                                output.push(RowDelta::Added {
                                    id: *primary_id,
                                    row: output_row,
                                });
                            }
                        } else if existed {
                            // No more matching rows - remove from output
                            output.push(RowDelta::Removed {
                                id: *primary_id,
                                prior: prior.clone(),
                            });
                        }
                    }
                } else {
                    // Forward join
                    // Get old join_id to update reverse_index
                    let old_join_id = cached_rows.get(primary_id)
                        .and_then(|jr| jr.get_row_id(join_table));

                    // Get new join_id from buffer row
                    let new_join_id = Self::get_ref_value_buffer(input_row, join_column, Some(primary_table));

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
                            // Qualify the join row's columns (lookup returns unqualified names)
                            let qualified_join_row = join_row.qualify_columns(join_table, join_schema);

                            // Input row is already in buffer format
                            let mut joined = BufferJoinedRow::from_single(primary_table, *primary_id, input_row.clone());
                            // Add the join row with qualified column names
                            joined.add_joined(join_table, join_id, qualified_join_row);

                            cached_rows.insert(*primary_id, joined.clone());
                            reverse_index.entry(join_id).or_default().insert(*primary_id);

                            let output_row = joined.to_output_row();
                            if existed {
                                output.push(RowDelta::Updated {
                                    id: *primary_id,
                                    row: output_row,
                                    prior: prior.clone(),
                                });
                            } else {
                                output.push(RowDelta::Added {
                                    id: *primary_id,
                                    row: output_row,
                                });
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
    }

    /// Handle a delta from the join_table side using reverse_index.
    #[allow(clippy::too_many_arguments)]
    fn eval_join_table_delta<F>(
        delta: &RowDelta,
        join_table: &str,
        join_schema: &TableSchema,
        _combined_schema: &TableSchema,
        table_descriptors: &HashMap<String, Arc<RowDescriptor>>,
        cached_rows: &mut HashMap<ObjectId, BufferJoinedRow>,
        reverse_index: &mut HashMap<ObjectId, HashSet<ObjectId>>,
        output: &mut DeltaBatch,
        lookup_row: F,
    ) where
        F: Fn(&str, ObjectId) -> Option<OwnedRow>,
    {
        let join_id = delta.row_id();

        match delta {
            RowDelta::Added { row: _join_row, .. } => {
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

            RowDelta::Updated { id: delta_join_id, row: _join_row, prior } => {
                // Find all primary_ids joined with this join_id and update them
                if let Some(affected_ids) = reverse_index.get(&join_id) {
                    for &primary_id in affected_ids.clone().iter() {
                        if let Some(old_joined) = cached_rows.get(&primary_id) {
                            // Rebuild the joined row with updated join_table data
                            let mut new_joined = old_joined.clone();

                            // Look up the fresh row data and update
                            if let Some(fresh_join_row) = lookup_row(join_table, join_id) {
                                // Qualify the join row's columns (lookup returns unqualified names)
                                let qualified_join_row = fresh_join_row.qualify_columns(join_table, join_schema);

                                // Update the join_table row in the BufferJoinedRow
                                new_joined.add_joined(join_table, *delta_join_id, qualified_join_row);

                                cached_rows.insert(primary_id, new_joined.clone());
                                output.push(RowDelta::Updated {
                                    id: primary_id,
                                    row: new_joined.to_output_row(),
                                    prior: prior.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    /// Extract a Ref value from a buffer row by column name.
    /// Handles both plain Ref values and nullable Ref values.
    /// If `table` is provided, tries looking up "table.column" first, then falls back to just "column".
    fn get_ref_value_buffer(row: &OwnedRow, column: &str, table: Option<&str>) -> Option<ObjectId> {
        use crate::sql::row_buffer::RowValue;

        // Try qualified name first if table is provided
        let value = if let Some(tbl) = table {
            let qualified = format!("{}.{}", tbl, column);
            row.get_by_name(&qualified).or_else(|| row.get_by_name(column))
        } else {
            row.get_by_name(column)
        };

        match value? {
            RowValue::Ref(id) => Some(id),
            RowValue::Null => None,
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
    /// `is_outer_delta` and `is_inner_delta` indicate which type of delta this is
    /// (considering contained_tables from upstream ArrayAggregates).
    /// `lookup_inner_rows` is a function to find all inner rows matching an outer id.
    /// `lookup_row_by_id` is a function to look up a row from any table by (table_name, id).
    pub fn evaluate_array_aggregate<F, G>(
        &mut self,
        delta: RowDelta,
        _source_table: &str,
        is_outer_delta: bool,
        is_inner_delta: bool,
        _outer_schema: &TableSchema,
        lookup_inner_rows: F,
        lookup_row_by_id: G,
    ) -> DeltaBatch
    where
        F: Fn(ObjectId) -> Vec<(ObjectId, OwnedRow)>,
        G: Fn(&str, ObjectId) -> Option<OwnedRow>,
    {
        match self {
            QueryNode::ArrayAggregate {
                inner_ref_column,
                inner_schema,
                inner_descriptor,
                inner_joins,
                output_descriptor,
                array_column_index,
                cached_arrays,
                inner_to_outer,
                outer_rows,
                ..
            } => {
                let mut output = DeltaBatch::new();

                if is_outer_delta {
                    // Delta from outer table - add/remove/update outer rows
                    Self::array_aggregate_handle_outer_delta(
                        &delta,
                        output_descriptor,
                        inner_descriptor,
                        inner_schema,
                        inner_joins,
                        *array_column_index,
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
                        inner_descriptor,
                        output_descriptor,
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
        output_descriptor: &Arc<RowDescriptor>,
        inner_descriptor: &Arc<RowDescriptor>,
        inner_schema: &TableSchema,
        inner_joins: &[(String, String, TableSchema)],
        array_column_index: i32,
        cached_arrays: &mut HashMap<ObjectId, Vec<OwnedRow>>,
        inner_to_outer: &mut HashMap<ObjectId, ObjectId>,
        outer_rows: &mut HashMap<ObjectId, OwnedRow>,
        output: &mut DeltaBatch,
        lookup_inner_rows: F,
        lookup_row_by_id: G,
    ) where
        F: Fn(ObjectId) -> Vec<(ObjectId, OwnedRow)>,
        G: Fn(&str, ObjectId) -> Option<OwnedRow>,
    {
        match delta {
            RowDelta::Added { id: outer_id, row: owned_row } => {
                // Fetch all matching inner rows (already in buffer format)
                let raw_inner_rows = lookup_inner_rows(*outer_id);

                // Update inner_to_outer index
                for (inner_id, _) in &raw_inner_rows {
                    inner_to_outer.insert(*inner_id, *outer_id);
                }

                // Resolve inner joins (e.g., replace label ref with full Labels row)
                let inner_rows = Self::resolve_inner_joins_buffer(
                    &raw_inner_rows,
                    inner_joins,
                    inner_schema,
                    &lookup_row_by_id,
                );
                cached_arrays.insert(*outer_id, inner_rows.clone());

                // Build output row with array
                let output_row = Self::build_output_row_with_array_buffer(
                    owned_row,
                    &inner_rows,
                    array_column_index,
                    output_descriptor.clone(),
                    inner_descriptor.clone(),
                );

                outer_rows.insert(*outer_id, output_row.clone());

                output.push(RowDelta::Added { id: *outer_id, row: output_row });
            }

            RowDelta::Removed { id, prior } => {
                let outer_id = *id;

                // Clean up inner_to_outer index
                if let Some(inner_rows) = cached_arrays.remove(&outer_id) {
                    // We don't have inner IDs stored in OwnedRow, but that's ok
                    // The inner_to_outer index was populated when we added
                    inner_to_outer.retain(|_, v| *v != outer_id);
                    let _ = inner_rows; // suppress unused warning
                }

                outer_rows.remove(&outer_id);

                output.push(RowDelta::Removed {
                    id: outer_id,
                    prior: prior.clone(),
                });
            }

            RowDelta::Updated { id, row: owned_row, prior } => {
                let outer_id = *id;

                // Fetch updated inner rows
                let raw_inner_rows = lookup_inner_rows(outer_id);

                // Update inner_to_outer index
                inner_to_outer.retain(|_, v| *v != outer_id);
                for (inner_id, _) in &raw_inner_rows {
                    inner_to_outer.insert(*inner_id, outer_id);
                }

                // Resolve inner joins
                let inner_rows = Self::resolve_inner_joins_buffer(
                    &raw_inner_rows,
                    inner_joins,
                    inner_schema,
                    &lookup_row_by_id,
                );
                cached_arrays.insert(outer_id, inner_rows.clone());

                let output_row = Self::build_output_row_with_array_buffer(
                    owned_row,
                    &inner_rows,
                    array_column_index,
                    output_descriptor.clone(),
                    inner_descriptor.clone(),
                );
                outer_rows.insert(outer_id, output_row.clone());

                output.push(RowDelta::Updated {
                    id: outer_id,
                    row: output_row,
                    prior: prior.clone(),
                });
            }
        }
    }

    /// Resolve inner joins for array rows (buffer format).
    /// For each inner row, replace ref columns with their resolved row values.
    /// Writes directly to RowBuilder instead of using intermediate Vec<Value>.
    fn resolve_inner_joins_buffer<G>(
        inner_rows: &[(ObjectId, OwnedRow)],
        inner_joins: &[(String, String, TableSchema)],
        inner_schema: &TableSchema,
        lookup_row_by_id: G,
    ) -> Vec<OwnedRow>
    where
        G: Fn(&str, ObjectId) -> Option<OwnedRow>,
    {
        use crate::sql::row_buffer::{RowBuilder, RowDescriptor, RowValue};

        if inner_joins.is_empty() {
            return inner_rows.iter().map(|(_, row)| row.clone()).collect();
        }

        inner_rows
            .iter()
            .map(|(_, row)| {
                // First pass: resolve refs to get target row descriptors for the output schema
                let mut resolved_targets: std::collections::HashMap<&str, OwnedRow> =
                    std::collections::HashMap::new();

                for (ref_column, target_table, _) in inner_joins {
                    if let Some(rv) = row.get_by_name(ref_column) {
                        if let RowValue::Ref(target_id) = rv {
                            if let Some(target_row) = lookup_row_by_id(target_table, target_id) {
                                resolved_targets.insert(ref_column.as_str(), target_row);
                            }
                        }
                    }
                }

                // Build the resolved descriptor with Array types for joined columns
                let new_cols: Vec<(String, ColumnType, bool)> = inner_schema
                    .columns
                    .iter()
                    .map(|col| {
                        if let Some(target_row) = resolved_targets.get(col.name.as_str()) {
                            // This column is resolved to a nested row - use Array type
                            (col.name.clone(), ColumnType::Array(target_row.descriptor.clone()), false)
                        } else {
                            (col.name.clone(), col.ty.clone(), col.nullable)
                        }
                    })
                    .collect();
                let resolved_descriptor = Arc::new(RowDescriptor::new_ordered(new_cols));

                // Build the output row directly
                let mut builder = RowBuilder::new(resolved_descriptor.clone());
                for (schema_idx, col) in inner_schema.columns.iter().enumerate() {
                    let col_idx = resolved_descriptor
                        .columns
                        .iter()
                        .position(|c| c.schema_index == schema_idx)
                        .unwrap_or(schema_idx);

                    if let Some(target_row) = resolved_targets.get(col.name.as_str()) {
                        // Set as single-item array containing the resolved row
                        builder = builder.set_array(col_idx, &[target_row.clone()]);
                    } else if let Some(rv) = row.get_by_name(&col.name) {
                        // Copy the value directly
                        builder = builder.set_from_row_value(col_idx, rv);
                    }
                }

                builder.build()
            })
            .collect()
    }

    /// Handle a delta from the inner table in ArrayAggregate.
    /// Uses buffer format throughout.
    #[allow(clippy::too_many_arguments)]
    fn array_aggregate_handle_inner_delta<G>(
        delta: &RowDelta,
        inner_ref_column: &str,
        inner_schema: &TableSchema,
        inner_joins: &[(String, String, TableSchema)],
        inner_descriptor: &Arc<RowDescriptor>,
        output_descriptor: &Arc<RowDescriptor>,
        array_column_index: i32,
        cached_arrays: &mut HashMap<ObjectId, Vec<OwnedRow>>,
        inner_to_outer: &mut HashMap<ObjectId, ObjectId>,
        outer_rows: &mut HashMap<ObjectId, OwnedRow>,
        output: &mut DeltaBatch,
        lookup_row_by_id: G,
    ) where
        G: Fn(&str, ObjectId) -> Option<OwnedRow>,
    {
        // Helper to resolve a single inner row
        let resolve_inner_row = |row: &OwnedRow| -> OwnedRow {
            if inner_joins.is_empty() {
                return row.clone();
            }
            let rows = vec![(ObjectId::default(), row.clone())];
            let resolved = Self::resolve_inner_joins_buffer(&rows, inner_joins, inner_schema, &lookup_row_by_id);
            resolved.into_iter().next().unwrap_or_else(|| row.clone())
        };
        // Helper to rebuild output row with updated array
        let rebuild_output = |outer_id: ObjectId,
                              base_row: &OwnedRow,
                              array: &[OwnedRow],
                              out_desc: Arc<RowDescriptor>,
                              inner_desc: Arc<RowDescriptor>| -> OwnedRow {
            Self::build_output_row_with_array_buffer(
                base_row,
                array,
                array_column_index,
                out_desc,
                inner_desc,
            )
        };

        match delta {
            RowDelta::Added { id: inner_id, row: inner_row } => {
                // Find which outer row this belongs to by looking up the ref column
                if let Some(outer_id) = Self::get_ref_value_from_buffer(inner_row, inner_ref_column, inner_descriptor) {
                    // Update inner_to_outer index
                    inner_to_outer.insert(*inner_id, outer_id);

                    // Add resolved row to cached array
                    let resolved_row = resolve_inner_row(inner_row);
                    let array = cached_arrays.entry(outer_id).or_default();
                    array.push(resolved_row);

                    // Emit updated delta for outer row
                    if let Some(base_outer_row) = outer_rows.get(&outer_id) {
                        let output_row = rebuild_output(
                            outer_id,
                            base_outer_row,
                            array,
                            output_descriptor.clone(),
                            inner_descriptor.clone(),
                        );
                        outer_rows.insert(outer_id, output_row.clone());

                        output.push(RowDelta::Updated {
                            id: outer_id,
                            row: output_row,
                            prior: PriorState::empty(),
                        });
                    }
                }
            }

            RowDelta::Removed { id: inner_id, prior } => {
                // Find which outer row this belonged to
                if let Some(outer_id) = inner_to_outer.remove(inner_id) {
                    // Remove from cached array (we don't have inner_id in OwnedRow, so use index)
                    // The cached_arrays entries were added with the OwnedRow, so just clear the one at this outer_id
                    // Actually, we need to track which row to remove - let's keep the array and filter
                    if let Some(array) = cached_arrays.get_mut(&outer_id) {
                        // We can't easily identify which OwnedRow corresponds to inner_id
                        // For now, we'll need to track this differently or rebuild the array
                        // TODO: Track inner_id -> array index mapping
                        // For now, just leave the array as-is and rely on re-fetch

                        // Emit updated delta for outer row
                        if let Some(base_outer_row) = outer_rows.get(&outer_id) {
                            let output_row = rebuild_output(
                                outer_id,
                                base_outer_row,
                                array,
                                output_descriptor.clone(),
                                inner_descriptor.clone(),
                            );
                            outer_rows.insert(outer_id, output_row.clone());

                            output.push(RowDelta::Updated {
                                id: outer_id,
                                row: output_row,
                                prior: prior.clone(),
                            });
                        }
                    }
                }
            }

            RowDelta::Updated { id: inner_id, row: inner_row, prior } => {
                let old_outer_id = inner_to_outer.get(inner_id).copied();
                let new_outer_id = Self::get_ref_value_from_buffer(inner_row, inner_ref_column, inner_descriptor);

                if old_outer_id != new_outer_id {
                    // Inner row moved to different outer row
                    // Remove from old (update with current array)
                    if let Some(old_id) = old_outer_id {
                        inner_to_outer.remove(inner_id);
                        if let Some(array) = cached_arrays.get(&old_id) {
                            if let Some(base_outer_row) = outer_rows.get(&old_id) {
                                let output_row = rebuild_output(
                                    old_id,
                                    base_outer_row,
                                    array,
                                    output_descriptor.clone(),
                                    inner_descriptor.clone(),
                                );
                                outer_rows.insert(old_id, output_row.clone());

                                output.push(RowDelta::Updated {
                                    id: old_id,
                                    row: output_row,
                                    prior: prior.clone(),
                                });
                            }
                        }
                    }

                    // Add to new
                    if let Some(new_id) = new_outer_id {
                        inner_to_outer.insert(*inner_id, new_id);
                        let resolved_row = resolve_inner_row(inner_row);
                        let array = cached_arrays.entry(new_id).or_default();
                        array.push(resolved_row);

                        if let Some(base_outer_row) = outer_rows.get(&new_id) {
                            let output_row = rebuild_output(
                                new_id,
                                base_outer_row,
                                array,
                                output_descriptor.clone(),
                                inner_descriptor.clone(),
                            );
                            outer_rows.insert(new_id, output_row.clone());

                            output.push(RowDelta::Updated {
                                id: new_id,
                                row: output_row,
                                prior: PriorState::empty(),
                            });
                        }
                    }
                } else if let Some(outer_id) = new_outer_id {
                    // Same outer row - update in place
                    let resolved_row = resolve_inner_row(inner_row);
                    let array = cached_arrays.entry(outer_id).or_default();
                    array.push(resolved_row); // Add updated row (old one still there - TODO: track properly)

                    if let Some(base_outer_row) = outer_rows.get(&outer_id) {
                        let output_row = rebuild_output(
                            outer_id,
                            base_outer_row,
                            array,
                            output_descriptor.clone(),
                            inner_descriptor.clone(),
                        );
                        outer_rows.insert(outer_id, output_row.clone());

                        output.push(RowDelta::Updated {
                            id: outer_id,
                            row: output_row,
                            prior: prior.clone(),
                        });
                    }
                }
            }
        }
    }

    /// Get a Ref value from an OwnedRow by column name.
    fn get_ref_value_from_buffer(row: &OwnedRow, column: &str, descriptor: &RowDescriptor) -> Option<ObjectId> {
        if let Some(idx) = descriptor.column_index(column) {
            if let Some(RowValue::Ref(id)) = row.get(idx) {
                return Some(id);
            }
        }
        None
    }

    /// Build an output row with array column using buffer format.
    fn build_output_row_with_array_buffer(
        outer_row: &OwnedRow,
        inner_rows: &[OwnedRow],
        array_column_index: i32,
        output_descriptor: Arc<RowDescriptor>,
        _inner_descriptor: Arc<RowDescriptor>,
    ) -> OwnedRow {
        use crate::sql::row_buffer::RowBuilder;

        let mut builder = RowBuilder::new(output_descriptor.clone());

        // array_column_index should already be the actual index (computed during graph building)
        // This is the index in output_descriptor where this ArrayAggregate's array column is
        let array_idx = array_column_index as usize;

        // Copy columns from outer_row to output
        // Skip only the array column we're about to set (other arrays should be preserved)
        let mut out_idx = 0;
        for col in &output_descriptor.columns {
            if out_idx == array_idx {
                // This is the array column position - skip it, we'll set it later
                out_idx += 1;
                continue;
            }

            // Find the corresponding column in outer_row by name
            if let Some(value) = outer_row.get_by_name(&col.name) {
                // Copy the value (including any existing arrays from previous ArrayAggregates)
                builder = builder.set_from_row_value(out_idx, value);
            }
            out_idx += 1;
        }

        // Set the array column
        builder = builder.set_array(array_idx, inner_rows);

        builder.build()
    }

    /// Evaluate a node that just passes through IDs while tracking membership.
    fn eval_id_passthrough(cached_ids: &mut HashSet<ObjectId>, input: DeltaBatch) -> DeltaBatch {
        let mut output = DeltaBatch::new();

        for delta in input.into_iter() {
            match &delta {
                RowDelta::Added { id, .. } => {
                    if cached_ids.insert(*id) {
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

    /// Evaluate a filter node using buffer format directly.
    fn eval_filter(
        predicate: &Predicate,
        descriptor: &Arc<RowDescriptor>,
        cached_ids: &mut HashSet<ObjectId>,
        input: DeltaBatch,
    ) -> DeltaBatch {
        let mut output = DeltaBatch::new();

        for delta in input.into_iter() {
            match delta {
                RowDelta::Added { id, row } => {
                    // Use the row's own descriptor - it knows its own layout
                    // The row is self-describing and contains all the info needed for filtering
                    let row_descriptor = &row.descriptor;
                    if predicate.matches_buffer(id, row.as_ref(), row_descriptor) {
                        cached_ids.insert(id);
                        output.push(RowDelta::Added { id, row });
                    }
                }

                RowDelta::Removed { id, prior } => {
                    // Only emit removal if it was in our cached set
                    if cached_ids.remove(&id) {
                        output.push(RowDelta::Removed { id, prior });
                    }
                }

                RowDelta::Updated { id, row, prior } => {
                    let was_in_set = cached_ids.contains(&id);
                    // Use the row's own descriptor
                    let row_descriptor = &row.descriptor;
                    let is_match = predicate.matches_buffer(id, row.as_ref(), row_descriptor);

                    match (was_in_set, is_match) {
                        (false, true) => {
                            // Row now matches the filter - enters the set
                            cached_ids.insert(id);
                            output.push(RowDelta::Added { id, row });
                        }
                        (true, false) => {
                            // Row no longer matches - leaves the set
                            cached_ids.remove(&id);
                            output.push(RowDelta::Removed { id, prior });
                        }
                        (true, true) => {
                            // Row still matches - propagate update
                            output.push(RowDelta::Updated { id, row, prior });
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
    /// when the visible window changes. Uses buffer format (OwnedRow) throughout.
    pub fn evaluate_limit_offset(
        &mut self,
        input: DeltaBatch,
        _schema: &TableSchema,
        _cache: &RowCache,
    ) -> DeltaBatch {
        match self {
            QueryNode::LimitOffset {
                limit,
                offset,
                all_rows,
                visible_ids,
                ..
            } => {
                let mut output = DeltaBatch::new();

                for delta in input.into_iter() {
                    match delta {
                        RowDelta::Added { id, row } => {
                            Self::limit_offset_handle_add(
                                id,
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
                                &mut output,
                            );
                        }
                        RowDelta::Updated { id, row, prior } => {
                            // Update the stored row if it exists
                            if all_rows.contains_key(&id) {
                                all_rows.insert(id, row.clone());
                                // If the row is visible, emit the update
                                if visible_ids.contains(&id) {
                                    output.push(RowDelta::Updated { id, row, prior });
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
    #[allow(clippy::too_many_arguments)]
    fn limit_offset_handle_add(
        row_id: ObjectId,
        row: OwnedRow,
        limit: Option<u64>,
        offset: u64,
        all_rows: &mut BTreeMap<ObjectId, OwnedRow>,
        visible_ids: &mut HashSet<ObjectId>,
        output: &mut DeltaBatch,
    ) {
        all_rows.insert(row_id, row);

        // Compute the new visible window and changes
        let (new_visible, changes) = Self::compute_window_changes(all_rows, visible_ids, limit, offset);

        // Emit deltas for changes
        for (id, change_type) in changes {
            match change_type {
                WindowChange::Added => {
                    if let Some(owned_row) = all_rows.get(&id) {
                        output.push(RowDelta::Added { id, row: owned_row.clone() });
                    }
                }
                WindowChange::Removed => {
                    output.push(RowDelta::Removed {
                        id,
                        prior: PriorState::empty(),
                    });
                }
            }
        }

        *visible_ids = new_visible;
    }

    /// Handle a row being removed from the LimitOffset node.
    #[allow(clippy::too_many_arguments)]
    fn limit_offset_handle_remove(
        id: ObjectId,
        prior: PriorState,
        limit: Option<u64>,
        offset: u64,
        all_rows: &mut BTreeMap<ObjectId, OwnedRow>,
        visible_ids: &mut HashSet<ObjectId>,
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
                    if let Some(owned_row) = all_rows.get(&changed_id) {
                        output.push(RowDelta::Added { id: changed_id, row: owned_row.clone() });
                    }
                }
                WindowChange::Removed => {
                    if changed_id != id {
                        // Only emit if not already removed above
                        output.push(RowDelta::Removed {
                            id: changed_id,
                            prior: PriorState::empty(),
                        });
                    }
                }
            }
        }

        *visible_ids = new_visible;
    }

    /// Compute the new visible window and return changes from the current state.
    fn compute_window_changes(
        all_rows: &BTreeMap<ObjectId, OwnedRow>,
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
    use crate::sql::query_graph::PredicateValue;
    use crate::sql::row_buffer::RowBuilder;
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
    fn table_scan_add() {
        let mut node = QueryNode::TableScan {
            table: "users".to_string(),
            cached_ids: HashSet::new(),
        };

        let schema = test_schema();
        let cache = RowCache::new();

        let (id, row) = make_owned_row(1, "Alice", true);
        let delta = DeltaBatch::added(id, row);

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
            predicate: Predicate::eq("active", PredicateValue::Bool(true)),
            descriptor: test_descriptor(),
            cached_ids: HashSet::new(),
        };

        let schema = test_schema();
        let cache = RowCache::new();

        let (id, row) = make_owned_row(1, "Alice", true);
        let delta = DeltaBatch::added(id, row);

        let output = node.evaluate(delta, &schema, &cache);

        assert_eq!(output.len(), 1);
        assert!(node.cached_ids().unwrap().contains(&ObjectId::new(1)));
    }

    #[test]
    fn filter_add_no_match() {
        let mut node = QueryNode::Filter {
            table: "users".to_string(),
            input: NodeId(0),
            predicate: Predicate::eq("active", PredicateValue::Bool(true)),
            descriptor: test_descriptor(),
            cached_ids: HashSet::new(),
        };

        let schema = test_schema();
        let cache = RowCache::new();

        let (id, row) = make_owned_row(1, "Alice", false); // active = false
        let delta = DeltaBatch::added(id, row);

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
            predicate: Predicate::eq("active", PredicateValue::Bool(true)),
            descriptor: test_descriptor(),
            cached_ids: HashSet::new(), // Not in set initially
        };

        let schema = test_schema();
        let cache = RowCache::new();

        // Update: was inactive, now active
        let (_, new_row) = make_owned_row(1, "Alice", true);
        let delta = DeltaBatch::updated(ObjectId::new(1), new_row, vec![]);

        let output = node.evaluate(delta, &schema, &cache);

        assert_eq!(output.len(), 1);
        // Should be Added since it entered the filtered set
        assert!(matches!(output.iter().next(), Some(RowDelta::Added { .. })));
        assert!(node.cached_ids().unwrap().contains(&ObjectId::new(1)));
    }

    #[test]
    fn filter_update_leaves_set() {
        let mut node = QueryNode::Filter {
            table: "users".to_string(),
            input: NodeId(0),
            predicate: Predicate::eq("active", PredicateValue::Bool(true)),
            descriptor: test_descriptor(),
            cached_ids: HashSet::from([ObjectId::new(1)]), // In set initially
        };

        let schema = test_schema();
        let cache = RowCache::new();

        // Update: was active, now inactive
        let (_, new_row) = make_owned_row(1, "Alice", false);
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
            predicate: Predicate::eq("active", PredicateValue::Bool(true)),
            descriptor: test_descriptor(),
            cached_ids: HashSet::from([ObjectId::new(1)]),
        };

        let schema = test_schema();
        let cache = RowCache::new();

        // Update: still active, just name change
        let (_, new_row) = make_owned_row(1, "Alicia", true);
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
            predicate: Predicate::eq("active", PredicateValue::Bool(true)),
            descriptor: test_descriptor(),
            cached_ids: HashSet::new(),
        };

        let schema = test_schema();
        let cache = RowCache::new();

        // Update: still inactive
        let (_, new_row) = make_owned_row(1, "Alicia", false);
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

    const ALICE: u128 = 100;
    const BOB: u128 = 200;

    fn folder_descriptor() -> Arc<RowDescriptor> {
        Arc::new(RowDescriptor::from_table_schema(&folder_schema()))
    }

    fn make_owned_folder(id: u128, name: &str, owner_id: u128, parent_id: Option<u128>) -> (ObjectId, OwnedRow) {
        let descriptor = folder_descriptor();
        let builder = RowBuilder::new(descriptor)
            .set_string_by_name("name", name)
            .set_ref_by_name("owner_id", ObjectId::new(owner_id));
        let row = match parent_id {
            Some(p) => builder.set_ref_by_name("parent_id", ObjectId::new(p)),
            None => builder.set_null_by_name("parent_id"),
        }.build();
        (ObjectId::new(id), row)
    }

    #[test]
    fn recursive_filter_base_access() {
        // Test: root folder owned by viewer is accessible
        let schema = folder_schema();
        let viewer = ObjectId::new(ALICE);

        let mut node = QueryNode::RecursiveFilter {
            table: "folders".to_string(),
            input: NodeId(0),
            base_predicate: Predicate::eq("owner_id", PredicateValue::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            descriptor: folder_descriptor(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Add a root folder owned by Alice
        let (id, row) = make_owned_folder(1, "root", ALICE, None);
        let delta = DeltaBatch::added(id, row);

        let output = node.evaluate_recursive(delta, &schema);

        assert_eq!(output.len(), 1);
        assert!(matches!(output.iter().next(), Some(RowDelta::Added { id, .. }) if id.0 == 1));
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
            base_predicate: Predicate::eq("owner_id", PredicateValue::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            descriptor: folder_descriptor(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Add a root folder owned by Bob (not Alice)
        let (id, row) = make_owned_folder(1, "bobs-folder", BOB, None);
        let delta = DeltaBatch::added(id, row);

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
            base_predicate: Predicate::eq("owner_id", PredicateValue::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            descriptor: folder_descriptor(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Add root folder owned by Alice
        let (id, row) = make_owned_folder(1, "root", ALICE, None);
        let delta = DeltaBatch::added(id, row);
        node.evaluate_recursive(delta, &schema);

        // Add child folder owned by Bob but parented to Alice's folder
        let (id, row) = make_owned_folder(2, "child", BOB, Some(1));
        let delta = DeltaBatch::added(id, row);
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
            base_predicate: Predicate::eq("owner_id", PredicateValue::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            descriptor: folder_descriptor(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Add grandchild first (parent doesn't exist yet)
        let (id, row) = make_owned_folder(3, "grandchild", BOB, Some(2));
        let delta = DeltaBatch::added(id, row);
        let output = node.evaluate_recursive(delta, &schema);
        assert!(output.is_empty()); // Not yet accessible

        // Add child (parent doesn't exist yet)
        let (id, row) = make_owned_folder(2, "child", BOB, Some(1));
        let delta = DeltaBatch::added(id, row);
        let output = node.evaluate_recursive(delta, &schema);
        assert!(output.is_empty()); // Still not accessible

        // Add root owned by Alice - should cascade to child and grandchild
        let (id, row) = make_owned_folder(1, "root", ALICE, None);
        let delta = DeltaBatch::added(id, row);
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
            base_predicate: Predicate::eq("owner_id", PredicateValue::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            descriptor: folder_descriptor(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Set up: root -> child -> grandchild
        let (id, row) = make_owned_folder(1, "root", ALICE, None);
        node.evaluate_recursive(DeltaBatch::added(id, row), &schema);
        let (id, row) = make_owned_folder(2, "child", BOB, Some(1));
        node.evaluate_recursive(DeltaBatch::added(id, row), &schema);
        let (id, row) = make_owned_folder(3, "grandchild", BOB, Some(2));
        node.evaluate_recursive(DeltaBatch::added(id, row), &schema);

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
            base_predicate: Predicate::eq("owner_id", PredicateValue::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            descriptor: folder_descriptor(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Root owned by Alice
        let (id, row) = make_owned_folder(1, "root", ALICE, None);
        node.evaluate_recursive(DeltaBatch::added(id, row), &schema);
        // Child also owned by Alice (Both access)
        let (id, row) = make_owned_folder(2, "child", ALICE, Some(1));
        node.evaluate_recursive(DeltaBatch::added(id, row), &schema);

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
