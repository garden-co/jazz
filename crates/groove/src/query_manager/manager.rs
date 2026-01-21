use std::collections::HashMap;

use crate::commit::{CommitId, StoredState};
use crate::object::{BranchName, ObjectId, ObjectState};
use crate::object_manager::AllObjectUpdate;
use crate::sync_manager::SyncManager;

use super::encoding::{decode_row, encode_row};
use super::graph::QueryGraph;
use super::graph_nodes::output::QuerySubscriptionId;
use super::index::{IndexError, IndexState};
use super::query::{Query, QueryBuilder};
use super::types::{RowDelta, RowDescriptor, Schema, TableName, Value};

/// Row branch name (all row data goes on "main" branch).
const ROW_BRANCH: &str = "main";

/// Error types for QueryManager operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    TableNotFound(TableName),
    ColumnCountMismatch { expected: usize, actual: usize },
    EncodingError(String),
    ObjectNotFound(ObjectId),
    QueryCompilationError(String),
    IndexError(String),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::TableNotFound(t) => write!(f, "table not found: {}", t),
            QueryError::ColumnCountMismatch { expected, actual } => {
                write!(
                    f,
                    "column count mismatch: expected {expected}, got {actual}"
                )
            }
            QueryError::EncodingError(msg) => write!(f, "encoding error: {msg}"),
            QueryError::ObjectNotFound(id) => write!(f, "object not found: {:?}", id),
            QueryError::QueryCompilationError(msg) => write!(f, "query compilation error: {msg}"),
            QueryError::IndexError(msg) => write!(f, "index error: {msg}"),
        }
    }
}

impl From<IndexError> for QueryError {
    fn from(e: IndexError) -> Self {
        QueryError::IndexError(format!("{:?}", e))
    }
}

impl std::error::Error for QueryError {}

/// Handle for tracking insert completion.
///
/// Poll via `is_complete()` to check if the row is persisted.
/// Poll via `is_indexed()` to check if the row is indexed.
#[derive(Debug, Clone)]
pub struct InsertHandle {
    /// The row's ObjectId.
    pub row_id: ObjectId,
    /// CommitId of the row data.
    pub row_commit_id: CommitId,
}

impl InsertHandle {
    /// Check if the row data is durable (persisted to storage).
    ///
    /// Must call `QueryManager::process()` between checks to drive storage operations.
    pub fn is_complete(&self, qm: &QueryManager) -> bool {
        qm.is_commit_stored(self.row_id, &self.row_commit_id)
    }

    /// Check if the row is indexed (appears in the _id index).
    ///
    /// After insert + process(), the row should be indexed.
    pub fn is_indexed(&self, qm: &QueryManager, table: &str) -> bool {
        qm.row_is_indexed(table, self.row_id)
    }
}

/// Query subscription info.
#[derive(Debug)]
struct QuerySubscription {
    graph: QueryGraph,
    #[allow(dead_code)]
    mode: SubscriptionMode,
}

/// Subscription mode.
#[derive(Debug, Clone, Copy)]
pub enum SubscriptionMode {
    Delta,
    Full,
}

/// Update for a query subscription.
#[derive(Debug, Clone)]
pub struct QueryUpdate {
    pub subscription_id: QuerySubscriptionId,
    pub delta: RowDelta,
}

/// Manages reactive SQL queries over object-based storage.
///
/// No global Setup/Ready state machine - indices and data are loaded lazily
/// from ObjectManager. Operations work immediately; queries return empty/Pending
/// results until data is available.
///
/// ObjectManager is the source of truth for row data - no caching layer on top.
pub struct QueryManager {
    sync_manager: SyncManager,
    schema: Schema,

    /// Indices: (table, column) -> IndexState
    indices: HashMap<(String, String), IndexState>,

    /// Active query subscriptions
    subscriptions: HashMap<QuerySubscriptionId, QuerySubscription>,
    next_subscription_id: u64,

    /// Pending query updates
    update_outbox: Vec<QueryUpdate>,
}

impl QueryManager {
    /// Create a new QueryManager with the given schema.
    ///
    /// Indices are created lazily when data is inserted. Existing data in
    /// ObjectManager is discovered via index scans (zero-copy reads).
    pub fn new(sync_manager: SyncManager, schema: Schema) -> Self {
        // Initialize indices for all tables
        let mut indices = HashMap::new();
        for (table_name, descriptor) in &schema {
            // Primary "_id" index
            indices.insert(
                (table_name.0.clone(), "_id".to_string()),
                IndexState::new(&table_name.0, "_id"),
            );

            // Index for each column
            for col in &descriptor.columns {
                indices.insert(
                    (table_name.0.clone(), col.name.clone()),
                    IndexState::new(&table_name.0, &col.name),
                );
            }
        }

        Self {
            sync_manager,
            schema,
            indices,
            subscriptions: HashMap::new(),
            next_subscription_id: 0,
            update_outbox: Vec::new(),
        }
    }

    /// Get the underlying SyncManager.
    pub fn sync_manager(&self) -> &SyncManager {
        &self.sync_manager
    }

    /// Get mutable reference to the underlying SyncManager.
    pub fn sync_manager_mut(&mut self) -> &mut SyncManager {
        &mut self.sync_manager
    }

    /// Check if a row is indexed (appears in the _id index for its table).
    pub fn row_is_indexed(&self, table: &str, row_id: ObjectId) -> bool {
        let id_key = (table.to_string(), "_id".to_string());
        if let Some(index) = self.indices.get(&id_key) {
            index.contains_row(row_id, &self.sync_manager.object_manager)
        } else {
            false
        }
    }

    /// Check if a commit has been stored to disk.
    ///
    /// Used by `InsertHandle::is_complete()` to check durability.
    pub fn is_commit_stored(&self, object_id: ObjectId, commit_id: &CommitId) -> bool {
        if let Some(state) = self.sync_manager.object_manager.get_state(object_id) {
            match state {
                ObjectState::Creating(obj) | ObjectState::Available(obj) => {
                    // Check all branches for the commit
                    for branch in obj.branches.values() {
                        if let Some(commit) = branch.commits.get(commit_id) {
                            return matches!(commit.stored_state, StoredState::Stored);
                        }
                    }
                }
                ObjectState::Loading => {}
            }
        }
        false
    }

    /// Insert a new row into a table.
    ///
    /// Returns an `InsertHandle` that can be polled to check durability.
    /// Index updates happen immediately (creating sentinels if needed).
    pub fn insert(&mut self, table: &str, values: &[Value]) -> Result<InsertHandle, QueryError> {
        let table_name = TableName::new(table);
        let descriptor = self
            .schema
            .get(&table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.clone()))?
            .clone();

        if values.len() != descriptor.columns.len() {
            return Err(QueryError::ColumnCountMismatch {
                expected: descriptor.columns.len(),
                actual: values.len(),
            });
        }

        // Encode to binary
        let data = encode_row(&descriptor, values)
            .map_err(|e| QueryError::EncodingError(e.to_string()))?;

        // Create object with table metadata
        let mut metadata = HashMap::new();
        metadata.insert("table".to_string(), table.to_string());

        let object_id = self.sync_manager.object_manager.create(Some(metadata));
        let author = object_id; // Self-authored

        // Add commit with row data
        let row_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(object_id, ROW_BRANCH, vec![], data.clone(), author, None)
            .map_err(|_| QueryError::ObjectNotFound(object_id))?;

        // Update indices immediately and persist
        self.update_indices_for_insert(table, object_id, &data, &descriptor)?;

        // Mark subscriptions dirty
        self.mark_subscriptions_dirty(table);

        Ok(InsertHandle {
            row_id: object_id,
            row_commit_id,
        })
    }

    /// Update a row.
    pub fn update(&mut self, id: ObjectId, values: &[Value]) -> Result<(), QueryError> {
        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get("table").cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Get old data from ObjectManager
        let (old_data, _) = self
            .load_row_from_object(id)
            .ok_or(QueryError::ObjectNotFound(id))?;

        let descriptor = self
            .schema
            .get(&table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.clone()))?
            .clone();

        if values.len() != descriptor.columns.len() {
            return Err(QueryError::ColumnCountMismatch {
                expected: descriptor.columns.len(),
                actual: values.len(),
            });
        }

        // Encode new data
        let new_data = encode_row(&descriptor, values)
            .map_err(|e| QueryError::EncodingError(e.to_string()))?;

        // Get parent commit
        let tips = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, ROW_BRANCH)
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Add commit with new data
        let _commit_id = self
            .sync_manager
            .object_manager
            .add_commit(id, ROW_BRANCH, parents, new_data.clone(), author, None)
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Update indices and persist modified nodes
        self.update_indices_for_update(&table_name.0, id, &old_data, &new_data, &descriptor)?;

        // Mark subscriptions dirty and notify about content update
        self.mark_subscriptions_dirty(&table_name.0);
        self.mark_row_updated_in_subscriptions(&table_name.0, id);

        Ok(())
    }

    /// Test helper: get a row by ID if loaded in ObjectManager.
    ///
    /// Production code should use queries to read data, not this method.
    /// This exists only to verify test expectations about what's loaded.
    #[cfg(test)]
    pub fn test_get_row_if_loaded(&self, id: ObjectId) -> Option<Vec<Value>> {
        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)?
            .metadata
            .get("table")?
            .clone();
        let table_name = TableName::new(&table);

        // Get row data from ObjectManager
        let (data, _) = self.load_row_from_object(id)?;

        let descriptor = self.schema.get(&table_name)?;
        decode_row(descriptor, &data).ok()
    }

    /// Create a query builder for a table.
    pub fn query(&self, table: &str) -> QueryBuilder {
        QueryBuilder::new(table)
    }

    /// Execute a query and return results (one-shot).
    pub fn execute(&mut self, query: Query) -> Result<Vec<Vec<Value>>, QueryError> {
        let descriptor = self
            .schema
            .get(&query.table)
            .ok_or_else(|| QueryError::TableNotFound(query.table.clone()))?
            .clone();

        let mut graph = QueryGraph::compile(&query, &self.schema)
            .ok_or_else(|| QueryError::QueryCompilationError("failed to compile query".into()))?;

        // Settle the graph - row_loader reads directly from ObjectManager
        let row_loader =
            |id: ObjectId| -> Option<(Vec<u8>, CommitId)> { self.load_row_from_object(id) };

        graph.settle(&self.indices, &self.sync_manager.object_manager, row_loader);

        // Decode results
        let rows = graph.current_result();
        let results = rows
            .iter()
            .filter_map(|row| decode_row(&descriptor, &row.data).ok())
            .collect();

        Ok(results)
    }

    /// Subscribe to query results (delta mode).
    pub fn subscribe(&mut self, query: Query) -> Result<QuerySubscriptionId, QueryError> {
        let graph = QueryGraph::compile(&query, &self.schema)
            .ok_or_else(|| QueryError::QueryCompilationError("failed to compile query".into()))?;

        let id = QuerySubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;

        self.subscriptions.insert(
            id,
            QuerySubscription {
                graph,
                mode: SubscriptionMode::Delta,
            },
        );

        Ok(id)
    }

    /// Unsubscribe from a query.
    pub fn unsubscribe(&mut self, id: QuerySubscriptionId) {
        self.subscriptions.remove(&id);
    }

    /// Take pending query updates.
    pub fn take_updates(&mut self) -> Vec<QueryUpdate> {
        std::mem::take(&mut self.update_outbox)
    }

    /// Process pending changes and settle all subscription graphs.
    ///
    /// This method drives async progress:
    /// - Processes object updates from SyncManager
    /// - Flushes pending index updates when indices become ready
    /// - Marks subscriptions with pending IDs dirty when objects become available
    /// - Settles all subscription graphs (row data loaded on-demand from ObjectManager)
    pub fn process(&mut self) {
        // Process object updates from SyncManager
        let updates = self.sync_manager.object_manager.take_all_object_updates();
        for update in updates {
            self.handle_object_update(update);
        }

        // Flush pending index updates for indices that became ready
        self.flush_pending_index_updates();

        // Mark subscriptions dirty if they have pending IDs that might now be available
        // This ensures settle() will be called to check pending rows
        self.mark_subscriptions_with_pending_dirty();

        // Settle all subscriptions - row_loader reads directly from ObjectManager
        // Extract references to avoid borrowing self in the closure
        let om = &self.sync_manager.object_manager;
        let indices = &self.indices;

        for (sub_id, subscription) in &mut self.subscriptions {
            let row_loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
                let state = om.get_state(id)?;
                match state {
                    ObjectState::Creating(obj) | ObjectState::Available(obj) => {
                        let branch = obj.branches.get(&BranchName::new(ROW_BRANCH))?;
                        let tip_id = branch.tips.iter().next()?;
                        let commit = branch.commits.get(tip_id)?;
                        Some((commit.content.clone(), *tip_id))
                    }
                    ObjectState::Loading => None,
                }
            };

            let delta = subscription.graph.settle(indices, om, row_loader);
            if !delta.is_empty() {
                self.update_outbox.push(QueryUpdate {
                    subscription_id: *sub_id,
                    delta,
                });
            }
        }
    }

    /// Mark subscriptions dirty if they have pending IDs.
    /// This ensures settle() will re-check pending rows on each process() call.
    fn mark_subscriptions_with_pending_dirty(&mut self) {
        for subscription in self.subscriptions.values_mut() {
            // Check if the MaterializeNode has any pending IDs
            for node in subscription.graph.nodes.values() {
                if let super::graph::GraphNode::Materialize(mat_node) = node
                    && mat_node.has_pending()
                {
                    // Mark the graph dirty so settle() will be called
                    subscription.graph.mark_materialize_dirty();
                    break;
                }
            }
        }
    }

    /// Flush pending index updates for indices that became ready.
    fn flush_pending_index_updates(&mut self) {
        // Collect indices that have pending updates and are now ready
        let ready_indices: Vec<(String, String)> = self
            .indices
            .iter()
            .filter(|(_, index)| {
                index.has_pending_updates() && index.root_exists(&self.sync_manager.object_manager)
            })
            .map(|(key, _)| key.clone())
            .collect();

        // Flush pending updates for each ready index
        for key in ready_indices {
            if let Some(index) = self.indices.get_mut(&key) {
                // Ignore errors - pending updates will be retried next process()
                let _ = index.flush_pending(&mut self.sync_manager.object_manager);
            }
        }
    }

    /// Load a row's data from ObjectManager.
    fn load_row_from_object(&self, row_id: ObjectId) -> Option<(Vec<u8>, CommitId)> {
        let state = self.sync_manager.object_manager.get_state(row_id)?;
        match state {
            ObjectState::Creating(obj) | ObjectState::Available(obj) => {
                let branch = obj.branches.get(&BranchName::new(ROW_BRANCH))?;
                let tip_id = branch.tips.iter().next()?;
                let commit = branch.commits.get(tip_id)?;
                Some((commit.content.clone(), *tip_id))
            }
            ObjectState::Loading => None,
        }
    }

    /// Handle an object update from the global subscription.
    fn handle_object_update(&mut self, update: AllObjectUpdate) {
        // Check if this is a row object
        let table = match update.metadata.get("table") {
            Some(t) => t.clone(),
            None => return,
        };

        // Extract current (new) data from the object
        let new_data = match self.load_row_from_object(update.object_id) {
            Some((data, _)) => data,
            None => return,
        };

        let table_name = TableName::new(&table);
        let descriptor = match self.schema.get(&table_name).cloned() {
            Some(desc) => desc,
            None => return,
        };

        if update.is_new_object || update.previous_commit_ids.is_empty() {
            // First commit on branch (new object or synced first commit) - insert into all indices
            let _ =
                self.update_indices_for_insert(&table, update.object_id, &new_data, &descriptor);
        } else if let Some(old_data) = update.old_content {
            // Synced update - compute index delta using old_content
            // TODO: Future merge strategies - currently last-writer-wins by timestamp
            let _ = self.update_indices_for_update(
                &table,
                update.object_id,
                &old_data,
                &new_data,
                &descriptor,
            );
        }
        // If old_content is None with previous_commit_ids: truncated old data, accept staleness

        self.mark_subscriptions_dirty(&table);
        self.mark_row_updated_in_subscriptions(&table, update.object_id);
    }

    /// Update indices when a row is inserted.
    fn update_indices_for_insert(
        &mut self,
        table: &str,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Update "_id" index (persists immediately or queues)
        let id_key = (table.to_string(), "_id".to_string());
        if let Some(index) = self.indices.get_mut(&id_key) {
            index.insert(
                object_id.0.as_bytes(),
                object_id,
                &mut self.sync_manager.object_manager,
            )?;
        }

        // Update column indices (persists immediately or queues)
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            let col_key = (table.to_string(), col.name.clone());
            if let Some(index) = self.indices.get_mut(&col_key)
                && let Ok(Some(value_bytes)) =
                    super::encoding::column_bytes(descriptor, data, col_idx)
            {
                index.insert(
                    value_bytes,
                    object_id,
                    &mut self.sync_manager.object_manager,
                )?;
            }
        }

        Ok(())
    }

    /// Update indices when a row is updated.
    fn update_indices_for_update(
        &mut self,
        table: &str,
        object_id: ObjectId,
        old_data: &[u8],
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // "_id" index doesn't change on update

        // Update column indices (remove old value, add new value - persists immediately)
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            let col_key = (table.to_string(), col.name.clone());
            if let Some(index) = self.indices.get_mut(&col_key) {
                // Remove old value
                if let Ok(Some(old_bytes)) =
                    super::encoding::column_bytes(descriptor, old_data, col_idx)
                {
                    index.remove(old_bytes, object_id, &mut self.sync_manager.object_manager)?;
                }
                // Add new value
                if let Ok(Some(new_bytes)) =
                    super::encoding::column_bytes(descriptor, new_data, col_idx)
                {
                    index.insert(new_bytes, object_id, &mut self.sync_manager.object_manager)?;
                }
            }
        }

        Ok(())
    }

    /// Mark subscriptions dirty for a table.
    fn mark_subscriptions_dirty(&mut self, table: &str) {
        for subscription in self.subscriptions.values_mut() {
            if subscription.graph.table.0 == table {
                subscription.graph.mark_dirty_for_table(table);
            }
        }
    }

    /// Mark a row as updated in all subscriptions for a table.
    /// This triggers content change detection during settle().
    fn mark_row_updated_in_subscriptions(&mut self, table: &str, id: ObjectId) {
        for subscription in self.subscriptions.values_mut() {
            if subscription.graph.table.0 == table {
                subscription.graph.mark_row_updated(id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::types::ColumnDescriptor;
    use crate::query_manager::types::ColumnType;

    fn test_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("name", ColumnType::Text),
                ColumnDescriptor::new("score", ColumnType::Integer),
            ]),
        );
        schema
    }

    #[test]
    fn insert_and_get() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        let row = qm.test_get_row_if_loaded(handle.row_id).unwrap();
        assert_eq!(row[0], Value::Text("Alice".into()));
        assert_eq!(row[1], Value::Integer(100));
    }

    #[test]
    fn insert_and_query() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        qm.insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        qm.insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();
        qm.insert(
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

        // Query all
        let query = qm.query("users").build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 3);

        // Query with filter
        let query = qm
            .query("users")
            .filter_ge("score", Value::Integer(75))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn query_with_sort_and_limit() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        qm.insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        qm.insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();
        qm.insert(
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

        let query = qm.query("users").order_by_desc("score").limit(2).build();
        let results = qm.execute(query).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0][0], Value::Text("Alice".into())); // 100
        assert_eq!(results[1][0], Value::Text("Charlie".into())); // 75
    }

    #[test]
    fn update_row() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        qm.update(
            handle.row_id,
            &[Value::Text("Alice Updated".into()), Value::Integer(150)],
        )
        .unwrap();

        let row = qm.test_get_row_if_loaded(handle.row_id).unwrap();
        assert_eq!(row[0], Value::Text("Alice Updated".into()));
        assert_eq!(row[1], Value::Integer(150));
    }

    #[test]
    fn table_not_found_error() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        let result = qm.insert("nonexistent", &[Value::Text("test".into())]);
        assert!(matches!(result, Err(QueryError::TableNotFound(_))));
    }

    #[test]
    fn column_count_mismatch_error() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        let result = qm.insert("users", &[Value::Text("Alice".into())]);
        assert!(matches!(
            result,
            Err(QueryError::ColumnCountMismatch { .. })
        ));
    }

    #[test]
    fn insert_returns_handle_with_commit_id() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Handle should have the row ID
        assert!(qm.test_get_row_if_loaded(handle.row_id).is_some());

        // Handle should have a valid row commit ID
        assert!(handle.row_commit_id.0 != [0; 32]);
    }

    #[test]
    fn row_is_indexed_after_insert() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Row should be indexed immediately after insert
        assert!(handle.is_indexed(&qm, "users"));
    }

    #[test]
    fn index_persistence_via_insert() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Test".into()), Value::Integer(42)])
            .unwrap();

        // Verify row is indexed
        assert!(handle.is_indexed(&qm, "users"));
    }

    // ========================================================================
    // Lazy loading and subscription tests
    // ========================================================================

    #[test]
    fn can_register_query_immediately() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Can register a query subscription immediately
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query);
        assert!(sub_id.is_ok());
    }

    #[test]
    fn subscription_updates_after_insert_and_process() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Register subscription
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query).unwrap();

        // Insert a row
        qm.insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Process - should settle subscriptions
        qm.process();

        // Now we should have subscription updates
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(updates[0].delta.added.len(), 1);
    }

    #[test]
    fn multiple_inserts_all_visible_in_query() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Multiple inserts
        let h1 = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        let h2 = qm
            .insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();
        let h3 = qm
            .insert(
                "users",
                &[Value::Text("Charlie".into()), Value::Integer(75)],
            )
            .unwrap();

        // All rows visible via get() immediately
        assert!(qm.test_get_row_if_loaded(h1.row_id).is_some());
        assert!(qm.test_get_row_if_loaded(h2.row_id).is_some());
        assert!(qm.test_get_row_if_loaded(h3.row_id).is_some());

        // Query returns all rows
        let query = qm.query("users").build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 3);

        // Sorted query works
        let query = qm.query("users").order_by_desc("score").limit(2).build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0][0], Value::Text("Alice".into())); // 100
        assert_eq!(results[1][0], Value::Text("Charlie".into())); // 75
    }

    #[test]
    fn cold_start_loads_persisted_indices_and_rows() {
        // Phase 1: Create QM, insert rows, persist indices
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm1 = QueryManager::new(sync_manager, schema.clone());

        // Insert some rows
        let h1 = qm1
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        let h2 = qm1
            .insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();

        // Rows are indexed
        assert!(h1.is_indexed(&qm1, "users"));
        assert!(h2.is_indexed(&qm1, "users"));

        // Phase 2: "Cold start" - create new QM with same underlying ObjectManager
        // Extract the SyncManager to reuse its ObjectManager
        let sync_manager2 = std::mem::replace(qm1.sync_manager_mut(), SyncManager::new());

        // Create new QueryManager - reads indices lazily from ObjectManager
        let mut qm2 = QueryManager::new(sync_manager2, schema);

        // Process to discover rows from indices
        qm2.process();

        // Verify rows are discoverable via get()
        assert!(qm2.test_get_row_if_loaded(h1.row_id).is_some());
        assert!(qm2.test_get_row_if_loaded(h2.row_id).is_some());

        // Verify queries work
        let query = qm2.query("users").build();
        let results = qm2.execute(query).unwrap();
        assert_eq!(results.len(), 2);

        // Verify filtered query works (proves indices were loaded)
        let query = qm2
            .query("users")
            .filter_ge("score", Value::Integer(75))
            .build();
        let results = qm2.execute(query).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::Text("Alice".into()));
    }

    #[test]
    fn cold_start_only_loads_queried_rows() {
        // This test verifies that after cold start:
        // 1. process() does NOT eagerly load all rows into a cache
        // 2. Queries access ObjectManager directly (no redundant row_cache)
        // 3. Rows not yet in ObjectManager return None gracefully

        // Phase 1: Create QM, insert multiple rows
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm1 = QueryManager::new(sync_manager, schema.clone());

        let h1 = qm1
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        let h2 = qm1
            .insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();
        let h3 = qm1
            .insert(
                "users",
                &[Value::Text("Charlie".into()), Value::Integer(75)],
            )
            .unwrap();

        // Phase 2: Simulate cold start with new QM
        let sync_manager2 = std::mem::replace(qm1.sync_manager_mut(), SyncManager::new());
        let mut qm2 = QueryManager::new(sync_manager2, schema);

        // Call process() - should NOT eagerly load all row content into a cache
        // Row data should be read directly from ObjectManager on demand
        qm2.process();

        // Query for specific rows (filter: score >= 75)
        let query = qm2
            .query("users")
            .filter_ge("score", Value::Integer(75))
            .build();
        let results = qm2.execute(query).unwrap();

        // Should find 2 rows (Alice: 100, Charlie: 75)
        assert_eq!(results.len(), 2);

        // Verify all rows are accessible via get() - reads from ObjectManager directly
        assert!(qm2.test_get_row_if_loaded(h1.row_id).is_some()); // Alice
        assert!(qm2.test_get_row_if_loaded(h2.row_id).is_some()); // Bob
        assert!(qm2.test_get_row_if_loaded(h3.row_id).is_some()); // Charlie
    }

    #[test]
    fn cold_start_with_sorted_query() {
        // Phase 1: Insert rows
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm1 = QueryManager::new(sync_manager, schema.clone());

        qm1.insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        qm1.insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();
        qm1.insert(
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

        // Phase 2: Cold start
        let sync_manager2 = std::mem::replace(qm1.sync_manager_mut(), SyncManager::new());
        let mut qm2 = QueryManager::new(sync_manager2, schema);
        qm2.process();

        // Sorted query should work
        let query = qm2.query("users").order_by_desc("score").build();
        let results = qm2.execute(query).unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0][0], Value::Text("Alice".into())); // 100
        assert_eq!(results[1][0], Value::Text("Charlie".into())); // 75
        assert_eq!(results[2][0], Value::Text("Bob".into())); // 50
    }

    #[test]
    fn local_update_updates_all_column_indices() {
        // Verifies that local update() correctly:
        // 1. Removes old values from column indices
        // 2. Adds new values to column indices
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert row with name="Alice", score=100
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Query by name="Alice" → finds row
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1);

        // Query by score=100 → finds row
        let query = qm
            .query("users")
            .filter_eq("score", Value::Integer(100))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1);

        // Update to name="Bob", score=200
        qm.update(
            handle.row_id,
            &[Value::Text("Bob".into()), Value::Integer(200)],
        )
        .unwrap();

        // Query by name="Alice" → empty (old value removed from index)
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            0,
            "Old name value should be removed from index"
        );

        // Query by name="Bob" → finds row (new value in index)
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Bob".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1, "New name value should be in index");

        // Query by score=100 → empty (old value removed from index)
        let query = qm
            .query("users")
            .filter_eq("score", Value::Integer(100))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            0,
            "Old score value should be removed from index"
        );

        // Query by score=200 → finds row (new value in index)
        let query = qm
            .query("users")
            .filter_eq("score", Value::Integer(200))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1, "New score value should be in index");
    }

    #[test]
    fn synced_update_updates_column_indices() {
        use crate::commit::{Commit, StoredState};
        use crate::query_manager::encoding::encode_row;
        use std::collections::HashMap;

        // This test verifies that updates received via sync (receive_commit)
        // correctly update column indices using old_content from AllObjectUpdate.

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Simulate receiving a new object from sync
        let row_id = crate::object::ObjectId::new();
        let author = row_id;

        // Receive object with table metadata
        let mut metadata = HashMap::new();
        metadata.insert("table".to_string(), "users".to_string());
        qm.sync_manager_mut()
            .object_manager
            .receive_object(row_id, metadata);

        // Subscribe to all objects so we get AllObjectUpdate notifications
        qm.sync_manager_mut().object_manager.subscribe_all();

        // Encode the initial row data (name="Alice", score=100)
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let initial_data = encode_row(
            &descriptor,
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

        // Receive the first commit (insert)
        let commit1 = Commit {
            parents: vec![],
            content: initial_data.clone(),
            timestamp: 1000,
            author,
            metadata: None,
            stored_state: StoredState::Stored,
        };
        let commit1_id = qm
            .sync_manager_mut()
            .object_manager
            .receive_commit(row_id, ROW_BRANCH, commit1)
            .unwrap();

        // Process to handle the AllObjectUpdate
        qm.process();

        // Query by name="Alice" → finds row
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            1,
            "Should find row by name=Alice after sync insert"
        );

        // Query by score=100 → finds row
        let query = qm
            .query("users")
            .filter_eq("score", Value::Integer(100))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            1,
            "Should find row by score=100 after sync insert"
        );

        // Encode updated row data (name="Bob", score=200)
        let updated_data = encode_row(
            &descriptor,
            &[Value::Text("Bob".into()), Value::Integer(200)],
        )
        .unwrap();

        // Receive the second commit (update)
        let commit2 = Commit {
            parents: vec![commit1_id],
            content: updated_data.clone(),
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: StoredState::Stored,
        };
        qm.sync_manager_mut()
            .object_manager
            .receive_commit(row_id, ROW_BRANCH, commit2)
            .unwrap();

        // Process to handle the AllObjectUpdate with old_content
        qm.process();

        // Query by name="Alice" → empty (old value removed from index)
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            0,
            "Old name value should be removed from index after sync update"
        );

        // Query by name="Bob" → finds row (new value in index)
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Bob".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            1,
            "New name value should be in index after sync update"
        );

        // Query by score=100 → empty (old value removed from index)
        let query = qm
            .query("users")
            .filter_eq("score", Value::Integer(100))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            0,
            "Old score value should be removed from index after sync update"
        );

        // Query by score=200 → finds row (new value in index)
        let query = qm
            .query("users")
            .filter_eq("score", Value::Integer(200))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            1,
            "New score value should be in index after sync update"
        );
    }

    #[test]
    fn synced_insert_appears_in_subscription_delta() {
        use crate::commit::{Commit, StoredState};
        use crate::query_manager::encoding::{decode_row, encode_row};
        use std::collections::HashMap;

        // Verify that a synced insert appears in subscription deltas

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Simulate receiving a new row from sync BEFORE subscribing
        // (similar to existing synced_update_updates_column_indices pattern)
        let row_id = crate::object::ObjectId::new();
        let author = row_id;

        // Receive object with table metadata
        let mut metadata = HashMap::new();
        metadata.insert("table".to_string(), "users".to_string());
        qm.sync_manager_mut()
            .object_manager
            .receive_object(row_id, metadata);

        // Subscribe to all objects so we get AllObjectUpdate notifications
        qm.sync_manager_mut().object_manager.subscribe_all();

        // NOW subscribe to query (after subscribe_all but before receive_commit)
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query).unwrap();

        // Encode the row data (name="SyncedUser", score=42)
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let row_data = encode_row(
            &descriptor,
            &[Value::Text("SyncedUser".into()), Value::Integer(42)],
        )
        .unwrap();

        // Receive the commit (insert)
        let commit = Commit {
            parents: vec![],
            content: row_data,
            timestamp: 1000,
            author,
            metadata: None,
            stored_state: StoredState::Stored,
        };
        qm.sync_manager_mut()
            .object_manager
            .receive_commit(row_id, ROW_BRANCH, commit)
            .unwrap();

        // Process to handle the AllObjectUpdate
        qm.process();

        // Verify subscription delta contains the added row
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1, "Should have one subscription update");
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.added.len(),
            1,
            "Delta should contain one added row"
        );

        // Decode the row to verify contents
        let row = &updates[0].delta.added[0];
        let values = decode_row(&descriptor, &row.data).unwrap();
        assert_eq!(values[0], Value::Text("SyncedUser".into()));
        assert_eq!(values[1], Value::Integer(42));
    }

    #[test]
    fn synced_update_is_visible_in_query() {
        use crate::commit::{Commit, StoredState};
        use crate::query_manager::encoding::encode_row;

        // Verify that synced updates (same row, new content) update indices correctly
        // and are visible in subsequent queries.
        //
        // Note: Currently, row content updates for existing IDs don't emit subscription
        // deltas because the graph tracks ID changes, not content changes. The
        // MaterializeNode has a check_update() method for this, but it's not wired
        // into the settle() flow yet. For now, we verify that queries see the updated data.

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Subscribe to all objects for sync updates
        qm.sync_manager_mut().object_manager.subscribe_all();

        // Insert a row locally first
        let insert_handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        let row_id = insert_handle.row_id;
        let first_commit_id = insert_handle.row_commit_id;

        // Process to settle the initial insert
        qm.process();

        // Verify initial data is queryable
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1, "Should find initial row");
        assert_eq!(results[0][0], Value::Text("Alice".into()));
        assert_eq!(results[0][1], Value::Integer(100));

        // Now simulate a synced update to this row (e.g., from another peer)
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let updated_data = encode_row(
            &descriptor,
            &[Value::Text("Alice Updated".into()), Value::Integer(200)],
        )
        .unwrap();

        let author = row_id; // Self-authored for simplicity
        let update_commit = Commit {
            parents: vec![first_commit_id],
            content: updated_data,
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: StoredState::Stored,
        };
        qm.sync_manager_mut()
            .object_manager
            .receive_commit(row_id, ROW_BRANCH, update_commit)
            .unwrap();

        // Process to handle the synced update
        qm.process();

        // Old data should no longer be in index
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 0, "Old name should not be found");

        // New data should be queryable
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Alice Updated".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1, "Should find updated row by new name");
        assert_eq!(results[0][0], Value::Text("Alice Updated".into()));
        assert_eq!(results[0][1], Value::Integer(200));

        // Score index should also be updated
        let query = qm
            .query("users")
            .filter_eq("score", Value::Integer(200))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1, "Should find updated row by new score");
    }

    #[test]
    fn synced_row_visible_in_filtered_subscription() {
        use crate::commit::{Commit, StoredState};
        use crate::query_manager::encoding::{decode_row, encode_row};
        use std::collections::HashMap;

        // Verify that synced rows are correctly filtered by subscription predicates.
        // Rows matching the filter appear in deltas; rows not matching are excluded.

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Subscribe to all objects for sync updates
        qm.sync_manager_mut().object_manager.subscribe_all();

        // Subscribe to filtered query: users with score > 25
        let query = qm
            .query("users")
            .filter_gt("score", Value::Integer(25))
            .build();
        let sub_id = qm.subscribe(query).unwrap();

        // Row descriptor for encoding
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);

        // --- Test 1: Synced row that matches filter (score=30 > 25) ---

        let row_id_1 = crate::object::ObjectId::new();
        let author_1 = row_id_1;

        let mut metadata_1 = HashMap::new();
        metadata_1.insert("table".to_string(), "users".to_string());
        qm.sync_manager_mut()
            .object_manager
            .receive_object(row_id_1, metadata_1);

        let data_1 = encode_row(
            &descriptor,
            &[Value::Text("HighScorer".into()), Value::Integer(30)],
        )
        .unwrap();

        let commit_1 = Commit {
            parents: vec![],
            content: data_1,
            timestamp: 1000,
            author: author_1,
            metadata: None,
            stored_state: StoredState::Stored,
        };
        qm.sync_manager_mut()
            .object_manager
            .receive_commit(row_id_1, ROW_BRANCH, commit_1)
            .unwrap();

        qm.process();

        let updates = qm.take_updates();
        assert_eq!(
            updates.len(),
            1,
            "Should have subscription update for matching row"
        );
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.added.len(),
            1,
            "Delta should contain the matching row"
        );

        // Verify the row data
        let row = &updates[0].delta.added[0];
        let values = decode_row(&descriptor, &row.data).unwrap();
        assert_eq!(values[0], Value::Text("HighScorer".into()));
        assert_eq!(values[1], Value::Integer(30));

        // --- Test 2: Synced row that does NOT match filter (score=20 < 25) ---

        let row_id_2 = crate::object::ObjectId::new();
        let author_2 = row_id_2;

        let mut metadata_2 = HashMap::new();
        metadata_2.insert("table".to_string(), "users".to_string());
        qm.sync_manager_mut()
            .object_manager
            .receive_object(row_id_2, metadata_2);

        let data_2 = encode_row(
            &descriptor,
            &[Value::Text("LowScorer".into()), Value::Integer(20)],
        )
        .unwrap();

        let commit_2 = Commit {
            parents: vec![],
            content: data_2,
            timestamp: 2000,
            author: author_2,
            metadata: None,
            stored_state: StoredState::Stored,
        };
        qm.sync_manager_mut()
            .object_manager
            .receive_commit(row_id_2, ROW_BRANCH, commit_2)
            .unwrap();

        qm.process();

        let updates = qm.take_updates();
        // Should have NO updates because the row doesn't match the filter
        assert_eq!(
            updates.len(),
            0,
            "Should have no subscription update for non-matching row"
        );

        // But verify it's in the index (just not in the filtered subscription)
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("LowScorer".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            1,
            "Non-matching row should still be in index"
        );
    }

    // ========================================================================
    // Row content update propagation tests
    // ========================================================================

    #[test]
    fn local_update_emits_subscription_delta() {
        // Verify that local qm.update() causes subscription to emit an update delta
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Subscribe to all users
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query).unwrap();

        // Process to get the initial add
        qm.process();
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].delta.added.len(), 1);

        // Update the row
        qm.update(
            handle.row_id,
            &[Value::Text("Alice Updated".into()), Value::Integer(200)],
        )
        .unwrap();

        // Process
        qm.process();

        // Should have an update delta
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1, "Should have one subscription update");
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.updated.len(),
            1,
            "Delta should contain one updated row"
        );

        // Verify old and new values
        let (old_row, new_row) = &updates[0].delta.updated[0];
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let old_values =
            crate::query_manager::encoding::decode_row(&descriptor, &old_row.data).unwrap();
        let new_values =
            crate::query_manager::encoding::decode_row(&descriptor, &new_row.data).unwrap();

        assert_eq!(old_values[0], Value::Text("Alice".into()));
        assert_eq!(old_values[1], Value::Integer(100));
        assert_eq!(new_values[0], Value::Text("Alice Updated".into()));
        assert_eq!(new_values[1], Value::Integer(200));
    }

    #[test]
    fn synced_update_emits_subscription_delta() {
        use crate::commit::{Commit, StoredState};
        use crate::query_manager::encoding::encode_row;

        // Verify that synced updates (receive_commit) cause subscription to emit update delta

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Subscribe to all objects for sync updates
        qm.sync_manager_mut().object_manager.subscribe_all();

        // Insert a row locally first
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        let row_id = handle.row_id;
        let first_commit_id = handle.row_commit_id;

        // Subscribe to all users
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query).unwrap();

        // Process to get the initial add
        qm.process();
        let _updates = qm.take_updates(); // Clear initial add

        // Now simulate a synced update
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let updated_data = encode_row(
            &descriptor,
            &[Value::Text("Alice Synced".into()), Value::Integer(300)],
        )
        .unwrap();

        let author = row_id;
        let update_commit = Commit {
            parents: vec![first_commit_id],
            content: updated_data,
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: StoredState::Stored,
        };
        qm.sync_manager_mut()
            .object_manager
            .receive_commit(row_id, ROW_BRANCH, update_commit)
            .unwrap();

        // Process
        qm.process();

        // Should have an update delta
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1, "Should have one subscription update");
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.updated.len(),
            1,
            "Delta should contain one updated row"
        );

        // Verify new values
        let (_old_row, new_row) = &updates[0].delta.updated[0];
        let new_values =
            crate::query_manager::encoding::decode_row(&descriptor, &new_row.data).unwrap();
        assert_eq!(new_values[0], Value::Text("Alice Synced".into()));
        assert_eq!(new_values[1], Value::Integer(300));
    }

    #[test]
    fn multiple_updates_same_row_single_delta() {
        // Verify that marking a row updated multiple times before process()
        // results in a single update delta reflecting final state

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert and subscribe
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        let query = qm.query("users").build();
        let _sub_id = qm.subscribe(query).unwrap();

        qm.process();
        let _updates = qm.take_updates(); // Clear initial add

        // Update twice before process()
        qm.update(
            handle.row_id,
            &[Value::Text("Alice V2".into()), Value::Integer(200)],
        )
        .unwrap();
        qm.update(
            handle.row_id,
            &[Value::Text("Alice V3".into()), Value::Integer(300)],
        )
        .unwrap();

        // Single process()
        qm.process();

        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1, "Should have one subscription update");
        assert_eq!(
            updates[0].delta.updated.len(),
            1,
            "Should have single update delta, not two"
        );

        // Verify it reflects final state
        let (_old_row, new_row) = &updates[0].delta.updated[0];
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let new_values =
            crate::query_manager::encoding::decode_row(&descriptor, &new_row.data).unwrap();
        assert_eq!(new_values[0], Value::Text("Alice V3".into()));
        assert_eq!(new_values[1], Value::Integer(300));
    }

    #[test]
    fn update_fails_filter_emits_removal() {
        // Verify: row passes filter, then update fails filter -> removal delta

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert row with score=100
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Subscribe to score > 50
        let query = qm
            .query("users")
            .filter_gt("score", Value::Integer(50))
            .build();
        let sub_id = qm.subscribe(query).unwrap();

        qm.process();
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].delta.added.len(),
            1,
            "Row should be added initially"
        );

        // Update score to 30 (fails filter)
        qm.update(
            handle.row_id,
            &[Value::Text("Alice".into()), Value::Integer(30)],
        )
        .unwrap();

        qm.process();

        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.removed.len(),
            1,
            "Row should be removed when it fails filter"
        );
    }

    #[test]
    fn update_passes_filter_emits_addition() {
        // Verify: row fails filter initially, then update passes filter -> addition delta

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert row with score=30 (fails filter)
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(30)])
            .unwrap();

        // Subscribe to score > 50
        let query = qm
            .query("users")
            .filter_gt("score", Value::Integer(50))
            .build();
        let sub_id = qm.subscribe(query).unwrap();

        qm.process();
        let updates = qm.take_updates();
        // Row doesn't match filter, so no delta or empty delta
        assert!(updates.is_empty() || updates[0].delta.added.is_empty());

        // Update score to 100 (passes filter)
        qm.update(
            handle.row_id,
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

        qm.process();

        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.added.len(),
            1,
            "Row should be added when it now passes filter"
        );
    }

    #[test]
    fn update_still_passes_filter_emits_update() {
        // Verify: row passes filter, update still passes filter -> update delta

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert row with score=100
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Subscribe to score > 50
        let query = qm
            .query("users")
            .filter_gt("score", Value::Integer(50))
            .build();
        let sub_id = qm.subscribe(query).unwrap();

        qm.process();
        let _updates = qm.take_updates(); // Clear initial add

        // Update score to 200 (still passes filter)
        qm.update(
            handle.row_id,
            &[Value::Text("Alice Updated".into()), Value::Integer(200)],
        )
        .unwrap();

        qm.process();

        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.updated.len(),
            1,
            "Row should be updated when it still passes filter"
        );
    }

    #[test]
    fn update_to_untracked_row_is_silent() {
        // Verify: row doesn't match filter, update still doesn't match -> no delta

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert row with score=30 (fails filter)
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(30)])
            .unwrap();

        // Subscribe to score > 50
        let query = qm
            .query("users")
            .filter_gt("score", Value::Integer(50))
            .build();
        let _sub_id = qm.subscribe(query).unwrap();

        qm.process();
        let _updates = qm.take_updates();

        // Update score to 40 (still fails filter)
        qm.update(
            handle.row_id,
            &[Value::Text("Alice".into()), Value::Integer(40)],
        )
        .unwrap();

        qm.process();

        let updates = qm.take_updates();
        // Should be no updates (or empty delta)
        assert!(
            updates.is_empty()
                || (updates[0].delta.added.is_empty()
                    && updates[0].delta.removed.is_empty()
                    && updates[0].delta.updated.is_empty()),
            "No delta for row that doesn't match filter before or after update"
        );
    }

    #[test]
    fn insert_then_update_same_cycle() {
        // Verify: insert + update before process() -> single added delta with final values

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Subscribe first
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query).unwrap();

        // Insert
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Update before process()
        qm.update(
            handle.row_id,
            &[Value::Text("Alice Updated".into()), Value::Integer(200)],
        )
        .unwrap();

        // Single process()
        qm.process();

        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);

        // Should be added (not updated, since this is new to subscription)
        assert_eq!(updates[0].delta.added.len(), 1, "Row should be added");
        assert!(
            updates[0].delta.updated.is_empty(),
            "No spurious update delta"
        );

        // Verify final values
        let row = &updates[0].delta.added[0];
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let values = crate::query_manager::encoding::decode_row(&descriptor, &row.data).unwrap();
        assert_eq!(values[0], Value::Text("Alice Updated".into()));
        assert_eq!(values[1], Value::Integer(200));
    }
}
