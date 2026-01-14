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
use crate::sql::query_graph::delta::{DeltaBatch, PriorState, RowDelta};
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

    /// Unified notification when an object changes.
    ///
    /// This is the single notification entry point for all queries.
    /// Creates RowDeltas from the Object's current state (via BranchMerge
    /// evaluation) and routes through the graph using process_change_from_table.
    /// This maintains compatibility with ARRAY/JOIN queries which use entry_points
    /// for routing.
    ///
    /// # Arguments
    ///
    /// * `table` - The table the object belongs to
    /// * `object_id` - The object that changed
    /// * `object` - Reference to the Object (for reading branch data)
    /// * `db` - Database state for routing through downstream nodes
    pub fn notify_object_changed(
        &self,
        table: &str,
        object_id: ObjectId,
        object: &crate::object::Object,
        db: &DatabaseState,
    ) {
        use crate::sql::query_graph::node::QueryNode;

        // Find all graphs that depend on this table
        let graph_ids: Vec<GraphId> = {
            let index = self.table_index.read().unwrap();
            index.get(table).cloned().unwrap_or_default()
        };

        if graph_ids.is_empty() {
            return;
        }

        // Build lens context for cross-schema transforms
        // This traverses the descriptor chain and collects all lenses for this table
        let lens_context = db.build_lens_context_for_table(table);

        // Phase 1: Process graphs and collect output deltas + callbacks
        let pending: Vec<(DeltaBatch, Vec<ArcCallback>)> = {
            let mut cache = self.cache.write().unwrap();
            let mut queries = self.queries.write().unwrap();

            graph_ids
                .into_iter()
                .filter_map(|graph_id| {
                    queries.get_mut(&graph_id).and_then(|query| {
                        // First try to find a BranchMerge node for this table
                        let mut branch_delta = DeltaBatch::new();
                        let mut found_branch_merge = false;

                        for node in query.graph.nodes_mut() {
                            if let QueryNode::BranchMerge {
                                table: node_table, ..
                            } = node
                                && node_table == table
                            {
                                found_branch_merge = true;
                                // Use evaluate_branch_merge_with_lenses to get row deltas
                                // from Object branches with proper merging
                                let row_deltas = node.evaluate_branch_merge_with_lenses(
                                    object_id,
                                    object,
                                    &lens_context,
                                    |desc_id| db.load_row_descriptor_by_id(desc_id),
                                );
                                branch_delta.extend(row_deltas);
                                break;
                            }
                        }

                        // If no BranchMerge found (e.g., inner table in ARRAY subquery),
                        // read the Object's main branch directly to create the delta
                        if !found_branch_merge {
                            // Get schema for this table to create the row descriptor
                            if let Some((_schema, descriptor)) = query.graph.get_table_schema(table)
                            {
                                // Read from Object's "main" branch - get tip content
                                if let Some(branch) = object.branch("main")
                                    && let Some(tip_id) = branch.frontier().first()
                                    && let Some(commit) = branch.get_commit(tip_id)
                                {
                                    // Check if content is empty (deleted object)
                                    if commit.content.is_empty() {
                                        // Object was deleted - emit Removed if it was in cache
                                        if cache.get(table, object_id).is_some() {
                                            branch_delta.push(RowDelta::Removed {
                                                id: object_id,
                                                prior: PriorState::empty(),
                                            });
                                        }
                                    } else {
                                        let row =
                                            OwnedRow::new(descriptor, commit.content.to_vec());
                                        // Check if this is Add vs Update based on cache
                                        let prev = cache.get(table, object_id);
                                        let delta = if prev.is_some() {
                                            RowDelta::Updated {
                                                id: object_id,
                                                row,
                                                prior: PriorState::empty(),
                                            }
                                        } else {
                                            RowDelta::Added { id: object_id, row }
                                        };
                                        branch_delta.push(delta);
                                    }
                                }
                            } else {
                                // No schema found - try to get row from db state
                                if let Some((_, row)) = db.get_row(table, object_id) {
                                    // Check if this is Add vs Update based on cache
                                    let prev = cache.get(table, object_id);
                                    let delta = if prev.is_some() {
                                        RowDelta::Updated {
                                            id: object_id,
                                            row,
                                            prior: PriorState::empty(),
                                        }
                                    } else {
                                        RowDelta::Added { id: object_id, row }
                                    };
                                    branch_delta.push(delta);
                                } else {
                                    // Row not found in db state - might be deleted
                                    // Emit Removed if it was in cache
                                    if cache.get(table, object_id).is_some() {
                                        branch_delta.push(RowDelta::Removed {
                                            id: object_id,
                                            prior: PriorState::empty(),
                                        });
                                    }
                                }
                            }
                        }

                        if branch_delta.is_empty() {
                            return None;
                        }

                        // Update RowCache with new row data, but only for non-BranchMerge paths.
                        // BranchMerge maintains its own cache in object_states.cached_merged,
                        // and collect_output reads from there for BranchMerge queries.
                        // This avoids duplicating row storage between BranchMerge and RowCache.
                        if !found_branch_merge {
                            for delta in branch_delta.iter() {
                                match delta {
                                    RowDelta::Added { id, row } => {
                                        cache.insert(table, *id, row.clone());
                                    }
                                    RowDelta::Removed { id, .. } => {
                                        cache.mark_deleted(table, *id);
                                    }
                                    RowDelta::Updated { id, row, .. } => {
                                        cache.insert(table, *id, row.clone());
                                    }
                                }
                            }
                        }

                        // Route through downstream nodes (Filter, Projection, Output)
                        // using route_from_branch_merge which skips the BranchMerge node
                        // (since we already evaluated it above)
                        let all_output = if found_branch_merge {
                            query
                                .graph
                                .route_from_branch_merge(branch_delta, &mut cache, db)
                        } else {
                            // For non-BranchMerge queries (e.g., inner tables in ARRAY subquery),
                            // use standard routing through entry points
                            let mut output = DeltaBatch::new();
                            for delta in branch_delta.into_iter() {
                                let routed = query
                                    .graph
                                    .process_change_from_table(delta, table, &mut cache, db);
                                output.extend(routed);
                            }
                            output
                        };

                        if all_output.is_empty() {
                            return None;
                        }

                        let callbacks = query.get_callbacks();
                        Some((all_output, callbacks))
                    })
                })
                .collect()
        };

        // Phase 2: Call callbacks without holding locks
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

    /// Create an Object with a commit containing the given row data on "main" branch.
    fn make_object_with_row(id: u128, name: &str, active: bool) -> crate::object::Object {
        use crate::commit::Commit;
        use crate::object::Object;

        let descriptor = test_descriptor();
        let row = RowBuilder::new(descriptor.clone())
            .set_string_by_name("name", name)
            .set_bool_by_name("active", active)
            .build();

        let object_id = ObjectId::new(id);
        let object = Object::new(object_id, "users");

        {
            let mut main_branch = object.branch_mut("main").unwrap();
            main_branch
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![],
                        content: row.buffer.into_boxed_slice(),
                        timestamp: 1000,
                        author: "test".to_string(),
                        meta: None,
                    },
                    &descriptor,
                )
                .unwrap();
        }

        object
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
        let object1 = make_object_with_row(1, "Alice", true);
        registry.notify_object_changed("users", ObjectId::new(1), &object1, db.state());

        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Notify of an inactive user - should NOT trigger callback (filtered out)
        let object2 = make_object_with_row(2, "Bob", false);
        registry.notify_object_changed("users", ObjectId::new(2), &object2, db.state());

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

        // Add some rows via Objects
        let object1 = make_object_with_row(1, "Alice", true);
        let object2 = make_object_with_row(2, "Bob", false);
        let object3 = make_object_with_row(3, "Carol", true);

        registry.notify_object_changed("users", ObjectId::new(1), &object1, db.state());
        registry.notify_object_changed("users", ObjectId::new(2), &object2, db.state());
        registry.notify_object_changed("users", ObjectId::new(3), &object3, db.state());

        // Get output
        let output = registry.get_output(graph_id, db.state()).unwrap();

        assert_eq!(output.len(), 2);
    }

    /// Test that branch-aware queries get notified when commits are added.
    ///
    /// This test verifies the full flow:
    /// 1. Create a branch-aware query (using BranchMerge node)
    /// 2. Add a commit to an object's branch
    /// 3. Notify the registry
    /// 4. Verify subscribers receive the update
    #[test]
    fn branch_merge_notified_on_commit() {
        use crate::branch::SchemaBranchName;
        use crate::commit::Commit;
        use crate::object::Object;
        use crate::sql::catalog::DescriptorId;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let schema = test_schema();
        let descriptor = test_descriptor();

        // Create descriptor ID for the schema version
        let desc_id = DescriptorId::new_v1(ObjectId::new(0x123456));

        // Create branch name with schema version
        let branch_name = SchemaBranchName::from_descriptor("dev", &desc_id, "main");
        let branch_name_str = branch_name.to_string();

        // Create an Object
        let object_id = ObjectId::new(1);
        let mut object = Object::new(object_id, "users");

        // Add a commit to "main" first so we have something to branch from
        let initial_row = RowBuilder::new(descriptor.clone())
            .set_string_by_name("name", "Initial")
            .set_bool_by_name("active", false)
            .build();

        let initial_commit_id = {
            let mut main_branch = object.branch_mut("main").unwrap();
            main_branch
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![],
                        content: initial_row.buffer.clone().into_boxed_slice(),
                        timestamp: 500,
                        author: "setup".to_string(),
                        meta: None,
                    },
                    &descriptor,
                )
                .unwrap()
        };

        // Create the schema-aware branch from the initial commit
        object
            .create_branch(&branch_name_str, "main", &initial_commit_id)
            .unwrap();

        // Create database for state reference
        let db = Database::in_memory();
        db.create_table(schema.clone()).unwrap();

        // Create a branch-aware query using BranchMerge
        let builder = QueryGraphBuilder::new("users", schema.clone())
            .with_branches(vec![branch_name_str.clone()], desc_id);
        let graph = builder.build_branch_merge_query(GraphId(0));

        // Register with registry
        let registry = GraphRegistry::new();
        let graph_id = registry.register(graph);

        // Track callback invocations
        let call_count = Arc::new(AtomicUsize::new(0));
        let count_clone = call_count.clone();

        registry.subscribe(
            graph_id,
            Box::new(move |delta| {
                eprintln!("[TEST] Callback received delta with {} items", delta.len());
                count_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        // Initial state: no callbacks yet
        assert_eq!(call_count.load(Ordering::SeqCst), 0);

        // Add a NEW commit to the schema-aware branch
        let new_row = RowBuilder::new(descriptor.clone())
            .set_string_by_name("name", "Alice")
            .set_bool_by_name("active", true)
            .build();

        {
            let mut branch = object.branch_mut(&branch_name_str).unwrap();
            let parent_id = branch.frontier()[0]; // Get the current frontier commit
            let _new_commit_id = branch
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![parent_id],
                        content: new_row.buffer.clone().into_boxed_slice(),
                        timestamp: 1000,
                        author: "test".to_string(),
                        meta: None,
                    },
                    &descriptor,
                )
                .unwrap();
        }

        // Notify the registry that the object changed.
        // In a real system, this would be called automatically by:
        // - The sync layer when receiving commits from the server
        // - The Database when local writes create commits
        //
        // For now, we call it explicitly to demonstrate the notification works.
        registry.notify_object_changed("users", object_id, &object, db.state());

        // Verify the callback was invoked
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "Expected callback to be invoked when branch change is notified"
        );
    }

    // ========== Multi-Branch Integration Tests ==========

    /// Helper to create an Object with commits on multiple branches.
    ///
    /// Creates an object with:
    /// - "main" branch with an initial commit
    /// - Additional branches forked from main with their own commits
    fn make_multi_branch_object(
        id: u128,
        initial_name: &str,
        initial_active: bool,
    ) -> crate::object::Object {
        use crate::commit::Commit;
        use crate::object::Object;

        let descriptor = test_descriptor();
        let object_id = ObjectId::new(id);
        let object = Object::new(object_id, "users");

        // Add initial commit to main
        let initial_row = RowBuilder::new(descriptor.clone())
            .set_string_by_name("name", initial_name)
            .set_bool_by_name("active", initial_active)
            .build();

        {
            let mut main_branch = object.branch_mut("main").unwrap();
            main_branch
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![],
                        content: initial_row.buffer.into_boxed_slice(),
                        timestamp: 1000,
                        author: "setup".to_string(),
                        meta: None,
                    },
                    &descriptor,
                )
                .unwrap();
        }

        object
    }

    /// Test: Two branches with disjoint column edits -> merge combines both
    ///
    /// Branch A modifies "name" at t=2000
    /// Branch B modifies "active" at t=3000
    /// Result should have name from A and active from B
    #[test]
    fn multi_branch_disjoint_edits_merge() {
        use crate::branch::SchemaBranchName;
        use crate::commit::Commit;
        use crate::sql::catalog::DescriptorId;

        let schema = test_schema();
        let descriptor = test_descriptor();
        let desc_id = DescriptorId::new_v1(ObjectId::new(0x100));

        // Create object with initial state: name="Initial", active=false
        let mut object = make_multi_branch_object(1, "Initial", false);

        // Create two branches from main
        let branch_a_name = SchemaBranchName::from_descriptor("dev", &desc_id, "branch-a");
        let branch_b_name = SchemaBranchName::from_descriptor("dev", &desc_id, "branch-b");

        let main_tip = object.branch("main").unwrap().frontier()[0];
        object
            .create_branch(branch_a_name.to_string(), "main", &main_tip)
            .unwrap();
        object
            .create_branch(branch_b_name.to_string(), "main", &main_tip)
            .unwrap();

        // Branch A: change name to "Alice" at t=2000
        let row_a = RowBuilder::new(descriptor.clone())
            .set_string_by_name("name", "Alice")
            .set_bool_by_name("active", false) // unchanged
            .build();

        {
            let mut branch_a = object.branch_mut(&branch_a_name.to_string()).unwrap();
            let parent = branch_a.frontier()[0];
            branch_a
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![parent],
                        content: row_a.buffer.into_boxed_slice(),
                        timestamp: 2000,
                        author: "alice".to_string(),
                        meta: None,
                    },
                    &descriptor,
                )
                .unwrap();
        }

        // Branch B: change active to true at t=3000
        let row_b = RowBuilder::new(descriptor.clone())
            .set_string_by_name("name", "Initial") // unchanged
            .set_bool_by_name("active", true)
            .build();

        {
            let mut branch_b = object.branch_mut(&branch_b_name.to_string()).unwrap();
            let parent = branch_b.frontier()[0];
            branch_b
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![parent],
                        content: row_b.buffer.into_boxed_slice(),
                        timestamp: 3000,
                        author: "bob".to_string(),
                        meta: None,
                    },
                    &descriptor,
                )
                .unwrap();
        }

        // Create a query that reads from both branches
        let builder = QueryGraphBuilder::new("users", schema.clone()).with_branches(
            vec![branch_a_name.to_string(), branch_b_name.to_string()],
            desc_id,
        );
        let graph = builder.build_branch_merge_query(GraphId(0));

        // Create database for state reference
        let db = Database::in_memory();
        db.create_table(schema.clone()).unwrap();

        let registry = GraphRegistry::new();
        let graph_id = registry.register(graph);

        // Notify to trigger evaluation
        registry.notify_object_changed("users", ObjectId::new(1), &object, db.state());

        // Get the output from the registry cache
        let output = registry
            .get_output(graph_id, db.state())
            .expect("should have output");

        assert_eq!(output.len(), 1, "Should have exactly one merged row");

        let (id, row) = &output[0];
        assert_eq!(*id, ObjectId::new(1));

        // Verify: name from branch A (changed), active from branch B (changed)
        let name = row.get_by_name("name");
        let active = row.get_by_name("active");

        assert_eq!(
            name,
            Some(crate::sql::row_buffer::RowValue::String("Alice")),
            "name should be 'Alice' from branch A"
        );
        assert_eq!(
            active,
            Some(crate::sql::row_buffer::RowValue::Bool(true)),
            "active should be true from branch B"
        );
    }

    /// Test: Two branches with conflicting edits -> LWW picks latest timestamp
    ///
    /// Branch A sets name="Alice" at t=2000
    /// Branch B sets name="Bob" at t=3000
    /// Result should have name="Bob" (later timestamp wins)
    #[test]
    fn multi_branch_conflicting_edits_lww() {
        use crate::branch::SchemaBranchName;
        use crate::commit::Commit;
        use crate::sql::catalog::DescriptorId;

        let schema = test_schema();
        let descriptor = test_descriptor();
        let desc_id = DescriptorId::new_v1(ObjectId::new(0x200));

        // Create object with initial state
        let mut object = make_multi_branch_object(2, "Initial", false);

        // Create two branches from main
        let branch_a_name = SchemaBranchName::from_descriptor("dev", &desc_id, "branch-a");
        let branch_b_name = SchemaBranchName::from_descriptor("dev", &desc_id, "branch-b");

        let main_tip = object.branch("main").unwrap().frontier()[0];
        object
            .create_branch(branch_a_name.to_string(), "main", &main_tip)
            .unwrap();
        object
            .create_branch(branch_b_name.to_string(), "main", &main_tip)
            .unwrap();

        // Branch A: set name="Alice" at t=2000 (earlier)
        let row_a = RowBuilder::new(descriptor.clone())
            .set_string_by_name("name", "Alice")
            .set_bool_by_name("active", false)
            .build();

        {
            let mut branch_a = object.branch_mut(&branch_a_name.to_string()).unwrap();
            let parent = branch_a.frontier()[0];
            branch_a
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![parent],
                        content: row_a.buffer.into_boxed_slice(),
                        timestamp: 2000,
                        author: "alice".to_string(),
                        meta: None,
                    },
                    &descriptor,
                )
                .unwrap();
        }

        // Branch B: set name="Bob" at t=3000 (later - should win)
        let row_b = RowBuilder::new(descriptor.clone())
            .set_string_by_name("name", "Bob")
            .set_bool_by_name("active", false)
            .build();

        {
            let mut branch_b = object.branch_mut(&branch_b_name.to_string()).unwrap();
            let parent = branch_b.frontier()[0];
            branch_b
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![parent],
                        content: row_b.buffer.into_boxed_slice(),
                        timestamp: 3000,
                        author: "bob".to_string(),
                        meta: None,
                    },
                    &descriptor,
                )
                .unwrap();
        }

        // Create a query that reads from both branches
        let builder = QueryGraphBuilder::new("users", schema.clone()).with_branches(
            vec![branch_a_name.to_string(), branch_b_name.to_string()],
            desc_id,
        );
        let graph = builder.build_branch_merge_query(GraphId(0));

        let db = Database::in_memory();
        db.create_table(schema.clone()).unwrap();

        let registry = GraphRegistry::new();
        let graph_id = registry.register(graph);

        registry.notify_object_changed("users", ObjectId::new(2), &object, db.state());

        let output = registry
            .get_output(graph_id, db.state())
            .expect("should have output");

        assert_eq!(output.len(), 1);

        let (_, row) = &output[0];
        let name = row.get_by_name("name");

        // Bob's change at t=3000 should win over Alice's at t=2000
        assert_eq!(
            name,
            Some(crate::sql::row_buffer::RowValue::String("Bob")),
            "name should be 'Bob' (later timestamp wins)"
        );
    }

    /// Test: Multi-branch query with Filter predicate
    ///
    /// Create two objects, each with commits on multiple branches.
    /// Query with WHERE active=true should only return matching rows.
    #[test]
    fn multi_branch_with_filter_predicate() {
        use crate::branch::SchemaBranchName;
        use crate::commit::Commit;
        use crate::sql::catalog::DescriptorId;

        let schema = test_schema();
        let descriptor = test_descriptor();
        let desc_id = DescriptorId::new_v1(ObjectId::new(0x300));

        let branch_name = SchemaBranchName::from_descriptor("dev", &desc_id, "feature");
        let branch_str = branch_name.to_string();

        // Object 1: ends up with active=true after merge
        let mut object1 = make_multi_branch_object(3, "Alice", false);
        let main_tip1 = object1.branch("main").unwrap().frontier()[0];
        object1
            .create_branch(&branch_str, "main", &main_tip1)
            .unwrap();

        // Set active=true on the branch
        let row1 = RowBuilder::new(descriptor.clone())
            .set_string_by_name("name", "Alice")
            .set_bool_by_name("active", true)
            .build();

        {
            let mut branch = object1.branch_mut(&branch_str).unwrap();
            let parent = branch.frontier()[0];
            branch
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![parent],
                        content: row1.buffer.into_boxed_slice(),
                        timestamp: 2000,
                        author: "test".to_string(),
                        meta: None,
                    },
                    &descriptor,
                )
                .unwrap();
        }

        // Object 2: ends up with active=false after merge
        let mut object2 = make_multi_branch_object(4, "Bob", false);
        let main_tip2 = object2.branch("main").unwrap().frontier()[0];
        object2
            .create_branch(&branch_str, "main", &main_tip2)
            .unwrap();

        // Keep active=false on the branch (just update name to trigger change tracking)
        let row2 = RowBuilder::new(descriptor.clone())
            .set_string_by_name("name", "Bob Updated")
            .set_bool_by_name("active", false)
            .build();

        {
            let mut branch = object2.branch_mut(&branch_str).unwrap();
            let parent = branch.frontier()[0];
            branch
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![parent],
                        content: row2.buffer.into_boxed_slice(),
                        timestamp: 2000,
                        author: "test".to_string(),
                        meta: None,
                    },
                    &descriptor,
                )
                .unwrap();
        }

        // Create a filtered query: SELECT * FROM users WHERE active = true
        let mut builder = QueryGraphBuilder::new("users", schema.clone())
            .with_branches(vec![branch_str], desc_id);
        let scan = builder.table_scan();
        let filtered = builder.filter(scan, Predicate::eq("active", PredicateValue::Bool(true)));
        let graph = builder.output(filtered, GraphId(0));

        let db = Database::in_memory();
        db.create_table(schema.clone()).unwrap();

        let registry = GraphRegistry::new();
        let graph_id = registry.register(graph);

        // Notify both objects
        registry.notify_object_changed("users", ObjectId::new(3), &object1, db.state());
        registry.notify_object_changed("users", ObjectId::new(4), &object2, db.state());

        let output = registry
            .get_output(graph_id, db.state())
            .expect("should have output");

        // Only object1 (Alice with active=true) should be in output
        assert_eq!(output.len(), 1, "Should have exactly one row after filter");

        let (id, row) = &output[0];
        assert_eq!(*id, ObjectId::new(3), "Should be object 3 (Alice)");

        let name = row.get_by_name("name");
        assert_eq!(
            name,
            Some(crate::sql::row_buffer::RowValue::String("Alice")),
            "Filtered row should be Alice"
        );
    }

    /// Test: Concurrent edits on same branch (multiple frontier tips)
    ///
    /// When a single branch has multiple concurrent commits (forked frontier),
    /// merge should combine them using per-column LWW.
    #[test]
    fn single_branch_concurrent_edits() {
        use crate::commit::Commit;

        let schema = test_schema();
        let descriptor = test_descriptor();

        // Create object with initial commit
        let object = make_multi_branch_object(5, "Initial", false);
        let main_tip = object.branch("main").unwrap().frontier()[0];

        // Create two concurrent commits (both have main_tip as parent)
        // Commit A: changes name at t=2000
        let row_a = RowBuilder::new(descriptor.clone())
            .set_string_by_name("name", "ConcurrentA")
            .set_bool_by_name("active", false)
            .build();

        // Commit B: changes active at t=3000
        let row_b = RowBuilder::new(descriptor.clone())
            .set_string_by_name("name", "Initial")
            .set_bool_by_name("active", true)
            .build();

        {
            let mut main_branch = object.branch_mut("main").unwrap();
            // Add commit A
            main_branch
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![main_tip],
                        content: row_a.buffer.into_boxed_slice(),
                        timestamp: 2000,
                        author: "alice".to_string(),
                        meta: None,
                    },
                    &descriptor,
                )
                .unwrap();

            // Add commit B with same parent (creates concurrent tips)
            main_branch
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![main_tip],
                        content: row_b.buffer.into_boxed_slice(),
                        timestamp: 3000,
                        author: "bob".to_string(),
                        meta: None,
                    },
                    &descriptor,
                )
                .unwrap();
        }

        // Verify we have concurrent tips
        assert_eq!(
            object.branch("main").unwrap().frontier().len(),
            2,
            "Should have 2 concurrent frontier tips"
        );

        // Create a simple query on main branch
        let mut builder = QueryGraphBuilder::new("users", schema.clone());
        let scan = builder.table_scan();
        let graph = builder.output(scan, GraphId(0));

        let db = Database::in_memory();
        db.create_table(schema.clone()).unwrap();

        let registry = GraphRegistry::new();
        let graph_id = registry.register(graph);

        registry.notify_object_changed("users", ObjectId::new(5), &object, db.state());

        let output = registry
            .get_output(graph_id, db.state())
            .expect("should have output");

        assert_eq!(output.len(), 1);

        let (_, row) = &output[0];
        let name = row.get_by_name("name");
        let active = row.get_by_name("active");

        // Merge should combine: name from A (t=2000), active from B (t=3000)
        assert_eq!(
            name,
            Some(crate::sql::row_buffer::RowValue::String("ConcurrentA")),
            "name should be from commit A"
        );
        assert_eq!(
            active,
            Some(crate::sql::row_buffer::RowValue::Bool(true)),
            "active should be from commit B"
        );
    }

    /// Test: Cross-schema branch merge with lens transformation
    ///
    /// Branch A (v1 schema): has "title" column, edits title="My Title" at t=2000
    /// Branch B (v2 schema): has "name" column (renamed from title), edits status=true at t=3000
    ///
    /// When querying with target schema v2:
    /// - title from v1 should be transformed to name via lens
    /// - Merge should combine: name="My Title" (from A, transformed), status=true (from B)
    #[test]
    fn cross_schema_branch_merge_with_lens() {
        use crate::branch::SchemaBranchName;
        use crate::commit::Commit;
        use crate::object::Object;
        use crate::sql::RowBuilder;
        use crate::sql::lens::LensGenerationOptions;
        use crate::sql::row_buffer::RowDescriptor;
        use crate::sql::schema::{ColumnDef, ColumnType, TableSchema};
        use std::sync::Arc;

        // Create database and table with v1 schema: "title" and "status" columns
        let db = Database::in_memory();
        db.execute("CREATE TABLE documents (title STRING NOT NULL, status BOOL NOT NULL)")
            .unwrap();

        // Get v1 descriptor ID
        let desc_v1_id = db.get_descriptor_id("documents").unwrap();
        let schema_v1 = db.get_table("documents").unwrap();
        let desc_v1 = Arc::new(RowDescriptor::from_table_schema(&schema_v1));

        // Execute migration to v2: rename "title" → "name"
        let schema_v2 = TableSchema::new(
            "documents",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::required("status", ColumnType::Bool),
            ],
        );
        let options = LensGenerationOptions {
            confirmed_renames: vec![("title".into(), "name".into())],
        };
        db.execute_migration("documents", schema_v2.clone(), options)
            .unwrap();

        // Get v2 descriptor ID
        let desc_v2_id = db.get_descriptor_id("documents").unwrap();
        let desc_v2 = Arc::new(RowDescriptor::from_table_schema(&schema_v2));

        // Verify lens context is set up correctly
        let lens_context = db.state().build_lens_context_for_table("documents");
        assert!(
            lens_context.get_lens(&desc_v1_id, &desc_v2_id).is_some(),
            "Lens should exist from v1 to v2"
        );

        // Create initial row (in v1 format)
        let initial_row_v1 = RowBuilder::new(desc_v1.clone())
            .set_string_by_name("title", "Initial")
            .set_bool_by_name("status", false)
            .build();

        // Create object with default "main" branch
        let branch_v1_name = SchemaBranchName::from_descriptor("dev", &desc_v1_id, "branch-v1");
        let branch_v2_name = SchemaBranchName::from_descriptor("dev", &desc_v2_id, "branch-v2");

        let mut object = Object::new(ObjectId::new(1), "documents");

        // Add initial commit to main
        {
            let mut main_branch = object.branch_mut("main").unwrap();
            main_branch
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![],
                        content: initial_row_v1.buffer.clone().into_boxed_slice(),
                        timestamp: 1000,
                        author: "system".to_string(),
                        meta: None,
                    },
                    &desc_v1,
                )
                .unwrap();
        }

        // Create v1 branch from main's tip
        let main_tip = object.branch("main").unwrap().frontier()[0];
        object
            .create_branch(branch_v1_name.to_string(), "main", &main_tip)
            .unwrap();

        // Create v2 branch from main's tip (same starting point)
        object
            .create_branch(branch_v2_name.to_string(), "main", &main_tip)
            .unwrap();

        // Branch v1: Update title to "My Title" at t=2000
        let row_v1_update = RowBuilder::new(desc_v1.clone())
            .set_string_by_name("title", "My Title")
            .set_bool_by_name("status", false) // unchanged
            .build();

        {
            let mut branch_v1 = object.branch_mut(&branch_v1_name.to_string()).unwrap();
            let parent = branch_v1.frontier()[0];
            branch_v1
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![parent],
                        content: row_v1_update.buffer.into_boxed_slice(),
                        timestamp: 2000,
                        author: "alice".to_string(),
                        meta: None,
                    },
                    &desc_v1,
                )
                .unwrap();
        }

        // Branch v2: Update status to true at t=3000 (in v2 format)
        let row_v2_update = RowBuilder::new(desc_v2.clone())
            .set_string_by_name("name", "Initial") // unchanged (was "title" in v1)
            .set_bool_by_name("status", true)
            .build();

        {
            let mut branch_v2 = object.branch_mut(&branch_v2_name.to_string()).unwrap();
            // The v2 branch was forked from v1's tip, so we need to find its frontier
            let parent = branch_v2.frontier()[0];
            branch_v2
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![parent],
                        content: row_v2_update.buffer.into_boxed_slice(),
                        timestamp: 3000,
                        author: "bob".to_string(),
                        meta: None,
                    },
                    &desc_v2,
                )
                .unwrap();
        }

        // Create a query targeting v2 schema, reading from both branches
        let builder = QueryGraphBuilder::new("documents", schema_v2.clone()).with_branches(
            vec![branch_v1_name.to_string(), branch_v2_name.to_string()],
            desc_v2_id,
        );
        let graph = builder.build_branch_merge_query(GraphId(0));

        let registry = GraphRegistry::new();
        let graph_id = registry.register(graph);

        // Notify to trigger evaluation
        registry.notify_object_changed("documents", ObjectId::new(1), &object, db.state());

        // Get output
        let output = registry
            .get_output(graph_id, db.state())
            .expect("should have output");

        assert_eq!(output.len(), 1, "Should have exactly one merged row");

        let (id, row) = &output[0];
        assert_eq!(*id, ObjectId::new(1));

        // Verify: name from v1 (transformed from "title"), status from v2
        let name = row.get_by_name("name");
        let status = row.get_by_name("status");

        assert_eq!(
            name,
            Some(crate::sql::row_buffer::RowValue::String("My Title")),
            "name should be 'My Title' from v1 branch (transformed from title)"
        );
        assert_eq!(
            status,
            Some(crate::sql::row_buffer::RowValue::Bool(true)),
            "status should be true from v2 branch"
        );
    }
}
