//! Registry for managing active query graphs.
//!
//! The registry tracks all active `QueryGraph` instances, routes row changes
//! to relevant graphs, and manages subscriptions.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::listener::ListenerId;
use crate::object::ObjectId;
use crate::sql::DatabaseState;
use crate::sql::catalog::DescriptorId;
use crate::sql::lens::QueryLensContext;
use crate::sql::query_graph::cache::RowCache;
use crate::sql::query_graph::delta::{DeltaBatch, RowDelta};
use crate::sql::query_graph::graph::{GraphId, QueryGraph};
use crate::sql::row_buffer::{OwnedRow, RowDescriptor};
use crate::sql::schema::TableSchema;

/// Callback type for query output changes.
#[cfg(not(feature = "wasm"))]
pub type OutputCallback = Box<dyn Fn(&DeltaBatch) + Send + Sync>;

#[cfg(feature = "wasm")]
pub type OutputCallback = Box<dyn Fn(&DeltaBatch)>;

/// Arc-wrapped callback for internal storage (allows cloning for lock-free notification).
#[cfg(not(feature = "wasm"))]
type ArcCallback = std::sync::Arc<dyn Fn(&DeltaBatch) + Send + Sync>;

#[cfg(feature = "wasm")]
type ArcCallback = std::sync::Arc<dyn Fn(&DeltaBatch)>;

/// A registered query with its graph and callbacks.
struct RegisteredQuery {
    graph: QueryGraph,
    callbacks: HashMap<ListenerId, ArcCallback>,
    next_listener_id: u64,
}

impl RegisteredQuery {
    fn new(graph: QueryGraph) -> Self {
        Self {
            graph,
            callbacks: HashMap::new(),
            next_listener_id: 1,
        }
    }

    fn subscribe(&mut self, callback: OutputCallback) -> ListenerId {
        let id = ListenerId::new(self.next_listener_id);
        self.next_listener_id += 1;
        // Convert Box to Arc for clonability
        self.callbacks.insert(id, std::sync::Arc::from(callback));
        id
    }

    fn unsubscribe(&mut self, id: ListenerId) -> bool {
        self.callbacks.remove(&id).is_some()
    }

    /// Get clones of all callbacks (for lock-free notification).
    fn get_callbacks(&self) -> Vec<ArcCallback> {
        self.callbacks.values().cloned().collect()
    }
}

/// Registry managing all active query graphs.
///
/// The registry:
/// - Tracks active graphs by ID
/// - Indexes graphs by table for efficient change routing
/// - Manages the shared row cache
/// - Handles subscriptions and notifications
pub struct GraphRegistry {
    /// All registered queries.
    queries: RwLock<HashMap<GraphId, RegisteredQuery>>,

    /// Index: table -> graphs that query it.
    table_index: RwLock<HashMap<String, Vec<GraphId>>>,

    /// Shared row cache.
    cache: RwLock<RowCache>,

    /// Next graph ID.
    next_graph_id: RwLock<u64>,
}

impl Default for GraphRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            queries: RwLock::new(HashMap::new()),
            table_index: RwLock::new(HashMap::new()),
            cache: RwLock::new(RowCache::new()),
            next_graph_id: RwLock::new(1),
        }
    }

    /// Register a new query graph.
    ///
    /// Returns the assigned graph ID.
    pub fn register(&self, mut graph: QueryGraph) -> GraphId {
        let id = {
            let mut next = self.next_graph_id.write().unwrap();
            let id = GraphId(*next);
            *next += 1;
            id
        };

        graph.set_id(id);

        // Get all tables this graph depends on (for JOIN queries)
        let tables: Vec<String> = graph.all_tables().to_vec();

        // Add to queries
        self.queries
            .write()
            .unwrap()
            .insert(id, RegisteredQuery::new(graph));

        // Add to table index for ALL tables this graph depends on
        let mut index = self.table_index.write().unwrap();
        for table in tables {
            index.entry(table).or_default().push(id);
        }

        id
    }

    /// Unregister a query graph.
    pub fn unregister(&self, id: GraphId) {
        let mut queries = self.queries.write().unwrap();
        if let Some(query) = queries.remove(&id) {
            // Remove from table index for ALL tables
            let mut index = self.table_index.write().unwrap();
            for table in query.graph.all_tables() {
                if let Some(graphs) = index.get_mut(table) {
                    graphs.retain(|&g| g != id);
                    if graphs.is_empty() {
                        index.remove(table);
                    }
                }
            }
        }
    }

    /// Subscribe to a query's output changes.
    ///
    /// Returns the listener ID, or None if the graph doesn't exist.
    pub fn subscribe(&self, graph_id: GraphId, callback: OutputCallback) -> Option<ListenerId> {
        self.queries
            .write()
            .unwrap()
            .get_mut(&graph_id)
            .map(|q| q.subscribe(callback))
    }

    /// Unsubscribe from a query.
    pub fn unsubscribe(&self, graph_id: GraphId, listener_id: ListenerId) -> bool {
        self.queries
            .write()
            .unwrap()
            .get_mut(&graph_id)
            .map(|q| q.unsubscribe(listener_id))
            .unwrap_or(false)
    }

    /// Get current output for a query in buffer format (initializes lazily).
    pub fn get_output(
        &self,
        graph_id: GraphId,
        db: &DatabaseState,
    ) -> Option<Vec<(ObjectId, OwnedRow)>> {
        let mut cache = self.cache.write().unwrap();
        self.queries
            .write()
            .unwrap()
            .get_mut(&graph_id)
            .map(|q| q.graph.get_output(&mut cache, db))
    }

    /// Get the output schema and descriptor for a query.
    pub fn get_output_schema(
        &self,
        graph_id: GraphId,
    ) -> Option<(TableSchema, Arc<RowDescriptor>)> {
        self.queries.read().unwrap().get(&graph_id).map(|q| {
            let schema = q
                .graph
                .output_schema()
                .unwrap_or_else(|| TableSchema::new("_output", vec![]));
            let descriptor = Arc::new(RowDescriptor::from_table_schema(&schema));
            (schema, descriptor)
        })
    }

    /// Notify all relevant graphs of a row change.
    ///
    /// This is called by the database after insert/update/delete operations.
    /// For JOIN queries, the graph will process the delta from the source table.
    pub fn notify_row_change(&self, table: &str, delta: RowDelta, db: &DatabaseState) {
        // Find all graphs that depend on this table
        let graph_ids: Vec<GraphId> = {
            let index = self.table_index.read().unwrap();
            index.get(table).cloned().unwrap_or_default()
        };

        if graph_ids.is_empty() {
            return;
        }

        // Phase 1: Process graphs and collect output deltas + callbacks (with locks held)
        let pending: Vec<(DeltaBatch, Vec<ArcCallback>)> = {
            let mut cache = self.cache.write().unwrap();
            let mut queries = self.queries.write().unwrap();

            graph_ids
                .into_iter()
                .filter_map(|graph_id| {
                    queries.get_mut(&graph_id).map(|query| {
                        // Use process_change_from_table so JOIN graphs know which table changed
                        let output_delta = query.graph.process_change_from_table(
                            delta.clone(),
                            table,
                            &mut cache,
                            db,
                        );
                        let callbacks = query.get_callbacks();
                        (output_delta, callbacks)
                    })
                })
                .filter(|(delta, _)| !delta.is_empty())
                .collect()
        };
        // All locks released here

        // Phase 2: Call callbacks without holding any locks (prevents deadlock)
        // Callbacks may call get_output() which needs to acquire locks
        for (output_delta, callbacks) in pending {
            for callback in callbacks {
                callback(&output_delta);
            }
        }
    }

    /// Invalidate a cached row (e.g., when synced from server).
    pub fn invalidate_row(&self, table: &str, id: crate::object::ObjectId) {
        self.cache.write().unwrap().invalidate(table, id);
    }

    /// Set the lens context for a registered query graph.
    ///
    /// This enables schema-aware query evaluation, where rows from older
    /// schema versions are transformed before predicate evaluation.
    ///
    /// Returns true if the graph was found and updated.
    pub fn set_lens_context(
        &self,
        graph_id: GraphId,
        target_descriptor: DescriptorId,
        lens_ctx: QueryLensContext,
    ) -> bool {
        if let Some(query) = self.queries.write().unwrap().get_mut(&graph_id) {
            query.graph.set_target_descriptor(target_descriptor);
            query.graph.set_lens_context(lens_ctx);
            true
        } else {
            false
        }
    }

    /// Get a text diagram of a query graph.
    pub fn get_diagram(&self, graph_id: GraphId) -> Option<String> {
        self.queries
            .read()
            .unwrap()
            .get(&graph_id)
            .map(|q| q.graph.to_diagram())
    }

    /// Get the number of registered graphs (for testing).
    #[cfg(test)]
    pub fn graph_count(&self) -> usize {
        self.queries.read().unwrap().len()
    }

    /// Get the number of graphs for a table (for testing).
    #[cfg(test)]
    pub fn graph_count_for_table(&self, table: &str) -> usize {
        self.table_index
            .read()
            .unwrap()
            .get(table)
            .map(|v| v.len())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::sql::query_graph::PredicateValue;
    use crate::sql::query_graph::builder::QueryGraphBuilder;
    use crate::sql::query_graph::predicate::Predicate;
    use crate::sql::row_buffer::{RowBuilder, RowDescriptor};
    use crate::sql::schema::{ColumnDef, ColumnType};
    use crate::sql::{Database, TableSchema};

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
    fn registry_register_unregister() {
        let registry = GraphRegistry::new();
        let schema = test_schema();

        let mut builder = QueryGraphBuilder::new("users", schema);
        let scan = builder.table_scan();
        let graph = builder.output(scan, GraphId(0));

        let id = registry.register(graph);

        assert_eq!(registry.graph_count(), 1);
        assert_eq!(registry.graph_count_for_table("users"), 1);

        registry.unregister(id);

        assert_eq!(registry.graph_count(), 0);
        assert_eq!(registry.graph_count_for_table("users"), 0);
    }

    #[test]
    fn registry_notify_change() {
        let registry = GraphRegistry::new();
        let schema = test_schema();

        // Create database and table
        let db = Database::in_memory();
        db.create_table(schema.clone()).unwrap();

        // Build and register a filtered query
        let mut builder = QueryGraphBuilder::new("users", schema);
        let scan = builder.table_scan();
        let filter = builder.filter(scan, Predicate::eq("active", PredicateValue::Bool(true)));
        let graph = builder.output(filter, GraphId(0));

        let graph_id = registry.register(graph);

        // Track callback invocations
        use std::sync::atomic::{AtomicUsize, Ordering};
        let call_count = Arc::new(AtomicUsize::new(0));
        let count_clone = call_count.clone();

        registry.subscribe(
            graph_id,
            Box::new(move |delta| {
                count_clone.fetch_add(delta.len(), Ordering::SeqCst);
            }),
        );

        // Notify of an active user - should trigger callback
        let (id, row) = make_owned_row(1, "Alice", true);
        registry.notify_row_change("users", RowDelta::Added { id, row }, db.state());

        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Notify of an inactive user - should NOT trigger callback (filtered out)
        let (id, row) = make_owned_row(2, "Bob", false);
        registry.notify_row_change("users", RowDelta::Added { id, row }, db.state());

        assert_eq!(call_count.load(Ordering::SeqCst), 1); // Still 1
    }

    #[test]
    fn registry_get_output() {
        let registry = GraphRegistry::new();
        let schema = test_schema();

        let db = Database::in_memory();
        db.create_table(schema.clone()).unwrap();

        let mut builder = QueryGraphBuilder::new("users", schema);
        let scan = builder.table_scan();
        let filter = builder.filter(scan, Predicate::eq("active", PredicateValue::Bool(true)));
        let graph = builder.output(filter, GraphId(0));

        let graph_id = registry.register(graph);

        // Add some rows
        let (id1, row1) = make_owned_row(1, "Alice", true);
        let (id2, row2) = make_owned_row(2, "Bob", false);
        let (id3, row3) = make_owned_row(3, "Carol", true);

        registry.notify_row_change("users", RowDelta::Added { id: id1, row: row1 }, db.state());
        registry.notify_row_change("users", RowDelta::Added { id: id2, row: row2 }, db.state());
        registry.notify_row_change("users", RowDelta::Added { id: id3, row: row3 }, db.state());

        // Get output
        let output = registry.get_output(graph_id, db.state()).unwrap();

        assert_eq!(output.len(), 2);
    }
}
