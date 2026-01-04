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

    /// Get the input node ID if this node has one.
    pub fn input(&self) -> Option<NodeId> {
        match self {
            QueryNode::TableScan { .. } => None,
            QueryNode::IndexLookup { .. } => None,
            QueryNode::Filter { input, .. } => Some(*input),
            QueryNode::Join { .. } => None, // Join is a source-like node
            QueryNode::Output { input, .. } => Some(*input),
            QueryNode::RecursiveFilter { input, .. } => Some(*input),
        }
    }

    /// Check if this node handles a specific table.
    pub fn handles_table(&self, table: &str) -> bool {
        self.tables().iter().any(|&t| t == table)
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
    fn get_ref_value(row: &Row, column: &str, schema: &TableSchema) -> Option<ObjectId> {
        let col_idx = schema.column_index(column)?;
        match row.values.get(col_idx)? {
            Value::Ref(id) => Some(*id),
            _ => None,
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
                    Some(p) => Value::Ref(ObjectId::new(p)),
                    None => Value::Null,
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
