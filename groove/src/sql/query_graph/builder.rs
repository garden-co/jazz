//! Builder for constructing query graphs programmatically.

use std::collections::{HashMap, HashSet};

use crate::sql::query_graph::graph::{GraphId, QueryGraph};
use crate::sql::query_graph::node::{NodeId, QueryNode};
use crate::sql::query_graph::predicate::Predicate;
use crate::sql::schema::TableSchema;
use crate::sql::types::IndexKey;
use crate::sql::ObjectId;

/// Builder for constructing `QueryGraph` instances.
///
/// # Example
///
/// ```ignore
/// let schema = db.get_table("users").unwrap();
/// let mut builder = QueryGraphBuilder::new("users", schema);
///
/// // SELECT * FROM users WHERE active = true
/// let scan = builder.table_scan();
/// let filter = builder.filter(scan, Predicate::eq("active", Value::Bool(true)));
/// let graph = builder.output(filter, GraphId(1));
/// ```
pub struct QueryGraphBuilder {
    table: String,
    schema: TableSchema,
    nodes: Vec<QueryNode>,
    next_id: u32,
}

impl QueryGraphBuilder {
    /// Create a new builder for queries on the given table.
    pub fn new(table: impl Into<String>, schema: TableSchema) -> Self {
        Self {
            table: table.into(),
            schema,
            nodes: Vec::new(),
            next_id: 0,
        }
    }

    /// Allocate a new node ID.
    fn alloc_id(&mut self) -> NodeId {
        let id = NodeId(self.next_id);
        self.next_id += 1;
        id
    }

    /// Add a table scan source node.
    ///
    /// This reads all rows from the table.
    pub fn table_scan(&mut self) -> NodeId {
        let id = self.alloc_id();
        self.nodes.push(QueryNode::TableScan {
            table: self.table.clone(),
            cached_ids: HashSet::new(),
        });
        id
    }

    /// Add an index lookup source node.
    ///
    /// This reads rows that reference a specific target via a Ref column.
    /// Uses the reverse index for efficient lookup.
    pub fn index_lookup(&mut self, column: impl Into<String>, target_id: ObjectId) -> NodeId {
        let id = self.alloc_id();
        let column = column.into();
        self.nodes.push(QueryNode::IndexLookup {
            table: self.table.clone(),
            index_key: IndexKey::new(&self.table, &column),
            target_id,
            cached_ids: HashSet::new(),
        });
        id
    }

    /// Add a filter node.
    ///
    /// Filters rows from the input node using the given predicate.
    pub fn filter(&mut self, input: NodeId, predicate: Predicate) -> NodeId {
        let id = self.alloc_id();
        self.nodes.push(QueryNode::Filter {
            table: self.table.clone(),
            input,
            predicate,
            cached_ids: HashSet::new(),
        });
        id
    }

    /// Add the output node and build the graph.
    ///
    /// This consumes the builder and returns the constructed graph.
    pub fn output(mut self, input: NodeId, graph_id: GraphId) -> QueryGraph {
        let output_id = self.alloc_id();
        self.nodes.push(QueryNode::Output {
            table: self.table.clone(),
            input,
        });

        // Build node_indices map
        let mut node_indices = HashMap::new();
        for (idx, _) in self.nodes.iter().enumerate() {
            node_indices.insert(NodeId(idx as u32), idx);
        }

        QueryGraph::new(
            graph_id,
            self.table,
            self.schema,
            self.nodes,
            node_indices,
            output_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::row::Value;
    use crate::sql::schema::{ColumnDef, ColumnType};

    fn test_schema() -> TableSchema {
        TableSchema::new(
            "users",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::required("active", ColumnType::Bool),
            ],
        )
    }

    #[test]
    fn build_simple_scan() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema);

        let scan = builder.table_scan();
        let graph = builder.output(scan, GraphId(1));

        assert_eq!(graph.node_count(), 2); // scan + output
        assert_eq!(graph.table(), "users");
    }

    #[test]
    fn build_scan_with_filter() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema);

        let scan = builder.table_scan();
        let filter = builder.filter(scan, Predicate::eq("active", Value::Bool(true)));
        let graph = builder.output(filter, GraphId(1));

        assert_eq!(graph.node_count(), 3); // scan + filter + output
    }

    #[test]
    fn build_chained_filters() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema);

        let scan = builder.table_scan();
        let f1 = builder.filter(scan, Predicate::eq("active", Value::Bool(true)));
        let f2 = builder.filter(f1, Predicate::eq("name", Value::String("Alice".to_string())));
        let graph = builder.output(f2, GraphId(1));

        assert_eq!(graph.node_count(), 4); // scan + filter + filter + output
    }

    #[test]
    fn build_index_lookup() {
        let schema = TableSchema::new(
            "posts",
            vec![
                ColumnDef::required("title", ColumnType::String),
                ColumnDef::required("author", ColumnType::Ref("users".to_string())),
            ],
        );

        let mut builder = QueryGraphBuilder::new("posts", schema);

        // SELECT * FROM posts WHERE author = ?
        let lookup = builder.index_lookup("author", ObjectId::new(42));
        let graph = builder.output(lookup, GraphId(1));

        assert_eq!(graph.node_count(), 2); // lookup + output
    }

    #[test]
    fn node_ids_are_sequential() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema);

        let id1 = builder.table_scan();
        let id2 = builder.filter(id1, Predicate::True);
        let id3 = builder.filter(id2, Predicate::True);

        assert_eq!(id1.0, 0);
        assert_eq!(id2.0, 1);
        assert_eq!(id3.0, 2);
    }
}
