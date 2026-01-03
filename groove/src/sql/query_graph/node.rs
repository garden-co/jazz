//! Query graph nodes.

use std::collections::HashSet;

use crate::sql::query_graph::cache::RowCache;
use crate::sql::query_graph::delta::{DeltaBatch, RowDelta};
use crate::sql::query_graph::predicate::Predicate;
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

    /// Terminal: marks the output of the graph.
    ///
    /// This node doesn't transform data, it just marks which node's
    /// output should be used as the query result.
    Output { table: String, input: NodeId },
}

impl QueryNode {
    /// Get the table this node operates on.
    pub fn table(&self) -> &str {
        match self {
            QueryNode::TableScan { table, .. } => table,
            QueryNode::IndexLookup { table, .. } => table,
            QueryNode::Filter { table, .. } => table,
            QueryNode::Output { table, .. } => table,
        }
    }

    /// Get the cached IDs if this node caches them.
    pub fn cached_ids(&self) -> Option<&HashSet<ObjectId>> {
        match self {
            QueryNode::TableScan { cached_ids, .. } => Some(cached_ids),
            QueryNode::IndexLookup { cached_ids, .. } => Some(cached_ids),
            QueryNode::Filter { cached_ids, .. } => Some(cached_ids),
            QueryNode::Output { .. } => None,
        }
    }

    /// Get a mutable reference to cached IDs.
    pub fn cached_ids_mut(&mut self) -> Option<&mut HashSet<ObjectId>> {
        match self {
            QueryNode::TableScan { cached_ids, .. } => Some(cached_ids),
            QueryNode::IndexLookup { cached_ids, .. } => Some(cached_ids),
            QueryNode::Filter { cached_ids, .. } => Some(cached_ids),
            QueryNode::Output { .. } => None,
        }
    }

    /// Get the input node ID if this node has one.
    pub fn input(&self) -> Option<NodeId> {
        match self {
            QueryNode::TableScan { .. } => None,
            QueryNode::IndexLookup { .. } => None,
            QueryNode::Filter { input, .. } => Some(*input),
            QueryNode::Output { input, .. } => Some(*input),
        }
    }

    /// Evaluate this node given input deltas.
    ///
    /// Returns output deltas (may be empty for early cutoff).
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

            QueryNode::Output { .. } => input, // Passthrough
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
