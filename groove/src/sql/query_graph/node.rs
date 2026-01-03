//! Query graph nodes.

use std::collections::{HashMap, HashSet};

use crate::sql::query_graph::cache::RowCache;
use crate::sql::query_graph::delta::{DeltaBatch, JoinedRow, RowDelta};
use crate::sql::query_graph::predicate::Predicate;
use crate::sql::row::{Row, Value};
use crate::sql::schema::TableSchema;
use crate::sql::types::IndexKey;
use crate::sql::ObjectId;

/// Unique identifier for a node within a query graph.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct NodeId(pub u32);

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

    /// Transform: join rows from left input with rows from right table.
    ///
    /// This implements an inner join where:
    /// - `left_column` is a Ref column in the left table
    /// - The join matches `left.left_column = right.id`
    ///
    /// On left deltas: looks up the corresponding right row by ID
    /// On right deltas: uses the reverse index to find all left rows
    Join {
        /// Left (primary) table name.
        left_table: String,
        /// Right (joined) table name.
        right_table: String,
        /// Column in left table that references right table (a Ref column).
        left_column: String,
        /// Schema of the right table (for building joined rows).
        right_schema: TableSchema,
        /// Cached joined pairs: (left_id, right_id) -> exists.
        /// This tracks which join pairs are currently in the output.
        cached_pairs: HashSet<(ObjectId, ObjectId)>,
        /// Cached joined rows for output.
        cached_joined: HashMap<(ObjectId, ObjectId), JoinedRow>,
    },

    /// Terminal: marks the output of the graph.
    ///
    /// This node doesn't transform data, it just marks which node's
    /// output should be used as the query result.
    Output { table: String, input: NodeId },
}

impl QueryNode {
    /// Get the primary table this node operates on.
    pub fn table(&self) -> &str {
        match self {
            QueryNode::TableScan { table, .. } => table,
            QueryNode::IndexLookup { table, .. } => table,
            QueryNode::Filter { table, .. } => table,
            QueryNode::Join { left_table, .. } => left_table,
            QueryNode::Output { table, .. } => table,
        }
    }

    /// Get all tables this node depends on.
    pub fn tables(&self) -> Vec<&str> {
        match self {
            QueryNode::TableScan { table, .. } => vec![table],
            QueryNode::IndexLookup { table, .. } => vec![table],
            QueryNode::Filter { table, .. } => vec![table],
            QueryNode::Join { left_table, right_table, .. } => vec![left_table, right_table],
            QueryNode::Output { table, .. } => vec![table],
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
        }
    }

    /// Get the cached join pairs (for Join nodes).
    pub fn cached_pairs(&self) -> Option<&HashSet<(ObjectId, ObjectId)>> {
        match self {
            QueryNode::Join { cached_pairs, .. } => Some(cached_pairs),
            _ => None,
        }
    }

    /// Get the cached joined rows (for Join nodes).
    pub fn cached_joined(&self) -> Option<&HashMap<(ObjectId, ObjectId), JoinedRow>> {
        match self {
            QueryNode::Join { cached_joined, .. } => Some(cached_joined),
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
        }
    }

    /// Evaluate a join node with the given delta and database access.
    ///
    /// `source_table` indicates which table the delta came from.
    /// `left_schema` is the schema of the left (primary) table.
    /// `lookup_row` is a function to look up a row by table and ID.
    /// `lookup_by_ref` is a function to find rows referencing a given ID.
    pub fn evaluate_join<F, G>(
        &mut self,
        delta: RowDelta,
        source_table: &str,
        left_schema: &TableSchema,
        lookup_row: F,
        lookup_by_ref: G,
    ) -> DeltaBatch
    where
        F: Fn(&str, ObjectId) -> Option<Row>,
        G: Fn(&str, &str, ObjectId) -> Vec<Row>,
    {
        match self {
            QueryNode::Join {
                left_table,
                right_table,
                left_column,
                right_schema,
                cached_pairs,
                cached_joined,
            } => {
                let mut output = DeltaBatch::new();

                if source_table == left_table {
                    // Delta from left table - look up right row
                    Self::eval_join_left_delta(
                        &delta,
                        left_table,
                        right_table,
                        left_column,
                        left_schema,
                        right_schema,
                        cached_pairs,
                        cached_joined,
                        &mut output,
                        &lookup_row,
                    );
                } else if source_table == right_table {
                    // Delta from right table - find all left rows referencing it
                    Self::eval_join_right_delta(
                        &delta,
                        left_table,
                        right_table,
                        left_column,
                        left_schema,
                        right_schema,
                        cached_pairs,
                        cached_joined,
                        &mut output,
                        &lookup_row,
                        &lookup_by_ref,
                    );
                }

                output
            }
            _ => DeltaBatch::new(),
        }
    }

    /// Handle a delta from the left (primary) table in a join.
    #[allow(clippy::too_many_arguments)]
    fn eval_join_left_delta<F>(
        delta: &RowDelta,
        left_table: &str,
        right_table: &str,
        left_column: &str,
        left_schema: &TableSchema,
        _right_schema: &TableSchema,
        cached_pairs: &mut HashSet<(ObjectId, ObjectId)>,
        cached_joined: &mut HashMap<(ObjectId, ObjectId), JoinedRow>,
        output: &mut DeltaBatch,
        lookup_row: F,
    ) where
        F: Fn(&str, ObjectId) -> Option<Row>,
    {
        match delta {
            RowDelta::Added(left_row) => {
                // Look up the referenced right row
                if let Some(right_id) = Self::get_ref_value(left_row, left_column, left_schema) {
                    if let Some(right_row) = lookup_row(right_table, right_id) {
                        // Create joined row
                        let mut joined = JoinedRow::from_single(left_table, left_row.clone());
                        joined.add_joined(right_table, right_row);

                        let pair = (left_row.id, right_id);
                        if cached_pairs.insert(pair) {
                            let output_row = joined.to_output_row();
                            cached_joined.insert(pair, joined);
                            output.push(RowDelta::Added(output_row));
                        }
                    }
                }
            }

            RowDelta::Removed { id: left_id, prior } => {
                // Remove all join pairs with this left ID
                let pairs_to_remove: Vec<_> = cached_pairs
                    .iter()
                    .filter(|(l, _)| *l == *left_id)
                    .copied()
                    .collect();

                for pair in pairs_to_remove {
                    if cached_pairs.remove(&pair) {
                        cached_joined.remove(&pair);
                        output.push(RowDelta::Removed {
                            id: *left_id,
                            prior: prior.clone(),
                        });
                    }
                }
            }

            RowDelta::Updated { id: left_id, new: left_row, prior } => {
                // First, find existing pairs for this left row
                let old_pairs: Vec<_> = cached_pairs
                    .iter()
                    .filter(|(l, _)| *l == *left_id)
                    .copied()
                    .collect();

                // Get the new right ID from the updated row
                let new_right_id = Self::get_ref_value(left_row, left_column, left_schema);

                // Remove old pairs that no longer match
                for (_, old_right_id) in &old_pairs {
                    if new_right_id != Some(*old_right_id) {
                        let pair = (*left_id, *old_right_id);
                        cached_pairs.remove(&pair);
                        cached_joined.remove(&pair);
                        output.push(RowDelta::Removed {
                            id: *left_id,
                            prior: prior.clone(),
                        });
                    }
                }

                // Add/update the new pair
                if let Some(right_id) = new_right_id {
                    if let Some(right_row) = lookup_row(right_table, right_id) {
                        let mut joined = JoinedRow::from_single(left_table, left_row.clone());
                        joined.add_joined(right_table, right_row);

                        let pair = (*left_id, right_id);
                        let existed = cached_pairs.contains(&pair);
                        cached_pairs.insert(pair);

                        let output_row = joined.to_output_row();
                        cached_joined.insert(pair, joined);

                        if existed {
                            output.push(RowDelta::Updated {
                                id: *left_id,
                                new: output_row,
                                prior: prior.clone(),
                            });
                        } else {
                            output.push(RowDelta::Added(output_row));
                        }
                    }
                }
            }
        }
    }

    /// Handle a delta from the right (joined) table.
    #[allow(clippy::too_many_arguments)]
    fn eval_join_right_delta<F, G>(
        delta: &RowDelta,
        left_table: &str,
        right_table: &str,
        left_column: &str,
        _left_schema: &TableSchema,
        _right_schema: &TableSchema,
        cached_pairs: &mut HashSet<(ObjectId, ObjectId)>,
        cached_joined: &mut HashMap<(ObjectId, ObjectId), JoinedRow>,
        output: &mut DeltaBatch,
        _lookup_row: F,
        lookup_by_ref: G,
    ) where
        F: Fn(&str, ObjectId) -> Option<Row>,
        G: Fn(&str, &str, ObjectId) -> Vec<Row>,
    {
        let right_id = delta.row_id();

        match delta {
            RowDelta::Added(right_row) => {
                // Find all left rows that reference this right row
                let left_rows = lookup_by_ref(left_table, left_column, right_id);

                for left_row in left_rows {
                    let pair = (left_row.id, right_id);
                    if cached_pairs.insert(pair) {
                        let mut joined = JoinedRow::from_single(left_table, left_row.clone());
                        joined.add_joined(right_table, right_row.clone());

                        let output_row = joined.to_output_row();
                        cached_joined.insert(pair, joined);
                        output.push(RowDelta::Added(output_row));
                    }
                }
            }

            RowDelta::Removed { prior, .. } => {
                // Remove all join pairs with this right ID
                let pairs_to_remove: Vec<_> = cached_pairs
                    .iter()
                    .filter(|(_, r)| *r == right_id)
                    .copied()
                    .collect();

                for pair in pairs_to_remove {
                    if cached_pairs.remove(&pair) {
                        cached_joined.remove(&pair);
                        output.push(RowDelta::Removed {
                            id: pair.0, // Left ID
                            prior: prior.clone(),
                        });
                    }
                }
            }

            RowDelta::Updated { new: right_row, prior, .. } => {
                // Find all left rows that reference this right row and update them
                let left_rows = lookup_by_ref(left_table, left_column, right_id);
                let left_ids: HashSet<_> = left_rows.iter().map(|r| r.id).collect();

                for left_row in &left_rows {
                    let pair = (left_row.id, right_id);
                    let existed = cached_pairs.contains(&pair);

                    let mut joined = JoinedRow::from_single(left_table, left_row.clone());
                    joined.add_joined(right_table, right_row.clone());

                    cached_pairs.insert(pair);
                    let output_row = joined.to_output_row();
                    cached_joined.insert(pair, joined);

                    if existed {
                        output.push(RowDelta::Updated {
                            id: left_row.id,
                            new: output_row,
                            prior: prior.clone(),
                        });
                    } else {
                        output.push(RowDelta::Added(output_row));
                    }
                }

                // Also remove pairs for left rows that no longer reference this right row
                let pairs_to_check: Vec<_> = cached_pairs
                    .iter()
                    .filter(|(_, r)| *r == right_id)
                    .copied()
                    .collect();

                for pair in pairs_to_check {
                    if !left_ids.contains(&pair.0) {
                        cached_pairs.remove(&pair);
                        cached_joined.remove(&pair);
                        output.push(RowDelta::Removed {
                            id: pair.0,
                            prior: prior.clone(),
                        });
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
    use crate::sql::ObjectId;

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
}
