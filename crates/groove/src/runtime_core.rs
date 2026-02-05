//! RuntimeCore - Unified synchronous runtime logic for both native and WASM.
//!
//! This module provides the shared core logic that both groove-tokio
//! and groove-wasm wrap. RuntimeCore is generic over `IoHandler` which
//! provides platform-specific I/O and scheduling.
//!
//! ## Design
//!
//! - `immediate_tick()` - processes managers synchronously, schedules batched_tick if needed
//! - `batched_tick()` - sends I/O, applies parked responses/messages, calls immediate_tick
//! - Queries return `QueryFuture` for cross-platform awaiting
//! - Storage/sync responses are "parked" and processed in batched_tick
//!
//! ## Usage
//!
//! ```ignore
//! // Create runtime with platform-specific IoHandler
//! let runtime = RuntimeCore::new(schema_manager, io_handler);
//!
//! // Execute operations - they schedule batched_tick automatically
//! runtime.insert("users", values)?;
//! runtime.immediate_tick();
//!
//! // Query returns a future
//! let future = runtime.query(query);
//! let results = future.await?;
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::oneshot;

use crate::io_handler::IoHandler;
use crate::object::ObjectId;
use crate::query_manager::QuerySubscriptionId;
use crate::query_manager::encoding::decode_row;
use crate::query_manager::manager::{QueryError, QueryUpdate};
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::query_manager::types::{RowDelta, Schema, TableName, Value};
use crate::schema_manager::SchemaManager;
use crate::storage::StorageResponse;
use crate::sync_manager::{ClientId, InboxEntry, ServerId};

/// Handle to a subscription managed by RuntimeCore.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionHandle(pub u64);

// Re-export QueryHandle from query_manager for convenience
pub use crate::query_manager::manager::QueryHandle as QMQueryHandle;

/// Errors from runtime operations.
#[derive(Debug, Clone)]
pub enum RuntimeError {
    QueryError(String),
    WriteError(String),
    NotFound,
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::QueryError(s) => write!(f, "Query error: {}", s),
            RuntimeError::WriteError(s) => write!(f, "Write error: {}", s),
            RuntimeError::NotFound => write!(f, "Not found"),
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<QueryError> for RuntimeError {
    fn from(e: QueryError) -> Self {
        RuntimeError::QueryError(format!("{:?}", e))
    }
}

/// Type alias for query results.
pub type QueryResult = Result<Vec<(ObjectId, Vec<Value>)>, RuntimeError>;

/// Future that resolves to query results.
///
/// Cross-platform future implementation using `futures::channel::oneshot`.
/// Works with both tokio and wasm_bindgen_futures executors.
pub struct QueryFuture {
    receiver: oneshot::Receiver<QueryResult>,
}

impl QueryFuture {
    /// Create a new QueryFuture from a oneshot receiver.
    pub fn new(receiver: oneshot::Receiver<QueryResult>) -> Self {
        Self { receiver }
    }
}

impl Future for QueryFuture {
    type Output = QueryResult;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(cx)
            .map(|r| r.unwrap_or_else(|_| Err(RuntimeError::QueryError("Query cancelled".into()))))
    }
}

/// Sender for fulfilling a QueryFuture.
pub type QuerySender = oneshot::Sender<QueryResult>;

/// Result of an immediate_tick cycle.
#[derive(Debug, Default)]
pub struct TickOutput {
    /// Subscription updates for this tick.
    pub subscription_updates: Vec<QueryUpdate>,
}

/// Delta for a subscription callback.
#[derive(Debug, Clone)]
pub struct SubscriptionDelta {
    /// The subscription handle.
    pub handle: SubscriptionHandle,
    /// The row changes (binary encoded).
    pub delta: RowDelta,
    /// Output descriptor for decoding the binary row data.
    /// Use with `decode_row(&descriptor, &row.data)` to get `Vec<Value>`.
    pub descriptor: crate::query_manager::types::RowDescriptor,
}

/// Callback type for subscriptions.
///
/// On native platforms, callbacks must be `Send` for thread safety.
/// On WASM (single-threaded), `Send` is not required.
#[cfg(target_arch = "wasm32")]
pub type SubscriptionCallback = Box<dyn Fn(SubscriptionDelta) + 'static>;

#[cfg(not(target_arch = "wasm32"))]
pub type SubscriptionCallback = Box<dyn Fn(SubscriptionDelta) + Send + 'static>;

/// State for a subscription.
struct SubscriptionState {
    /// QueryManager's internal subscription ID.
    query_sub_id: QuerySubscriptionId,
    /// Callback invoked on updates.
    callback: SubscriptionCallback,
}

/// Pending one-shot query waiting for first subscription callback.
struct PendingOneShotQuery {
    subscription_id: QuerySubscriptionId,
    sender: Option<QuerySender>,
}

/// Unified runtime core for both native and WASM platforms.
///
/// Generic over `IoHandler` which provides platform-specific I/O and scheduling.
/// All business logic is synchronous - IoHandler handles async dispatch.
pub struct RuntimeCore<H: IoHandler> {
    schema_manager: SchemaManager,
    io_handler: H,

    /// Parked storage responses (from IoHandler callback).
    parked_storage_responses: Vec<StorageResponse>,
    /// Parked sync messages (from network).
    parked_sync_messages: Vec<InboxEntry>,

    /// Subscription tracking with callbacks.
    subscriptions: HashMap<SubscriptionHandle, SubscriptionState>,
    /// Reverse map for routing updates.
    subscription_reverse: HashMap<QuerySubscriptionId, SubscriptionHandle>,
    next_subscription_handle: u64,

    /// Pending one-shot queries (query() calls waiting for first callback).
    pending_one_shot_queries: HashMap<SubscriptionHandle, PendingOneShotQuery>,
}

impl<H: IoHandler> RuntimeCore<H> {
    /// Create a new RuntimeCore wrapping a SchemaManager.
    ///
    /// Call `load_indices` with a driver to initialize from storage,
    /// or use `load_indices_batched()` for async initialization.
    pub fn new(schema_manager: SchemaManager, io_handler: H) -> Self {
        Self {
            schema_manager,
            io_handler,
            parked_storage_responses: Vec::new(),
            parked_sync_messages: Vec::new(),
            subscriptions: HashMap::new(),
            subscription_reverse: HashMap::new(),
            next_subscription_handle: 0,
            pending_one_shot_queries: HashMap::new(),
        }
    }

    /// Get mutable reference to the IoHandler.
    pub fn io_handler_mut(&mut self) -> &mut H {
        &mut self.io_handler
    }

    /// Get reference to the IoHandler.
    pub fn io_handler(&self) -> &H {
        &self.io_handler
    }

    /// Consume RuntimeCore and return the IoHandler.
    /// Used for cold-start testing to transfer driver state.
    pub fn into_io_handler(self) -> H {
        self.io_handler
    }

    /// Get mutable access to parked storage responses.
    ///
    /// Used by IoHandler implementations that need to directly park
    /// responses without triggering schedule_batched_tick.
    pub fn parked_storage_responses_mut(&mut self) -> &mut Vec<StorageResponse> {
        &mut self.parked_storage_responses
    }

    // =========================================================================
    // Tick Methods
    // =========================================================================

    /// Synchronous tick - processes managers, fulfills completed queries.
    ///
    /// Schedules batched_tick if there are outbound messages (storage requests
    /// or sync messages to send).
    ///
    /// Call this after any mutation operation (insert, update, delete, etc.)
    /// to process the change and schedule any required I/O.
    pub fn immediate_tick(&mut self) -> TickOutput {
        // 1. Process logical updates (sync, subscriptions)
        self.schema_manager.process();

        // 2. Collect subscription updates
        let subscription_updates = self.schema_manager.query_manager_mut().take_updates();

        // Track one-shot queries that completed this tick
        let mut completed_one_shots: Vec<SubscriptionHandle> = Vec::new();

        // 3. Call subscription callbacks AND handle one-shot queries
        for update in &subscription_updates {
            if let Some(&handle) = self.subscription_reverse.get(&update.subscription_id) {
                // Check if this is a one-shot query
                if let Some(pending) = self.pending_one_shot_queries.get_mut(&handle) {
                    // First callback = graph settled, fulfill the future
                    if let Some(sender) = pending.sender.take() {
                        // Decode rows using the query's output descriptor
                        let results: Vec<(ObjectId, Vec<Value>)> = update
                            .delta
                            .added
                            .iter()
                            .filter_map(|row| {
                                decode_row(&update.descriptor, &row.data)
                                    .ok()
                                    .map(|values| (row.id, values))
                            })
                            .collect();
                        let _ = sender.send(Ok(results));
                    }
                    // Mark for cleanup (unsubscribe happens after loop)
                    completed_one_shots.push(handle);
                } else if let Some(state) = self.subscriptions.get(&handle) {
                    // Regular subscription - call callback
                    let delta = SubscriptionDelta {
                        handle,
                        delta: update.delta.clone(),
                        descriptor: update.descriptor.clone(),
                    };
                    (state.callback)(delta);
                }
            }
        }

        // 2b. Cleanup completed one-shot queries
        for handle in completed_one_shots {
            if let Some(pending) = self.pending_one_shot_queries.remove(&handle) {
                // Unsubscribe from the underlying subscription
                self.schema_manager
                    .query_manager_mut()
                    .unsubscribe_with_sync(pending.subscription_id);
                self.subscription_reverse.remove(&pending.subscription_id);
            }
        }

        // 4. Schedule batched_tick if outbound messages exist
        if self.has_outbound() {
            self.io_handler.schedule_batched_tick();
        }

        TickOutput {
            subscription_updates,
        }
    }

    /// Batched tick - handles all I/O, then processes parked messages.
    ///
    /// Called by the platform when the scheduled tick fires. This:
    /// 1. Sends all storage requests (fire-and-forget)
    /// 2. Sends all outgoing sync messages
    /// 3. Drains any pending responses from IoHandler (for sync drivers)
    /// 4. Applies parked storage responses
    /// 5. Applies parked sync messages
    ///
    /// Each step is followed by an immediate_tick to process results.
    pub fn batched_tick(&mut self) {
        // 1. Send all storage requests (fire-and-forget)
        let requests = self
            .schema_manager
            .query_manager_mut()
            .take_storage_requests();
        for req in requests {
            self.io_handler.send_storage_request(req);
        }

        // 2. Send all outgoing sync messages
        let outbox = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        for msg in outbox {
            self.io_handler.send_sync_message(msg);
        }

        // 3. Drain pending responses from IoHandler (for sync drivers like RocksDB)
        let pending = self.io_handler.take_pending_responses();
        self.parked_storage_responses.extend(pending);

        // 4. Process parked storage responses
        // (calls immediate_tick internally, which may generate new outbox entries
        // and schedule another batched_tick to send them)
        self.handle_storage();

        // 5. Process parked sync messages
        // (calls immediate_tick internally, which may generate new outbox entries
        // and schedule another batched_tick to send them)
        self.handle_sync_messages();
    }

    /// Apply parked storage responses and tick.
    fn handle_storage(&mut self) {
        let responses = std::mem::take(&mut self.parked_storage_responses);
        if !responses.is_empty() {
            self.schema_manager
                .query_manager_mut()
                .apply_storage_responses(responses);
            self.immediate_tick();
        }
    }

    /// Apply parked sync messages and tick.
    fn handle_sync_messages(&mut self) {
        let messages = std::mem::take(&mut self.parked_sync_messages);
        let had_messages = !messages.is_empty();
        for msg in messages {
            self.push_sync_inbox(msg);
        }
        if had_messages {
            self.immediate_tick();
        }
    }

    /// Check if there are outbound messages requiring a batched_tick.
    pub fn has_outbound(&self) -> bool {
        self.schema_manager
            .query_manager()
            .has_pending_storage_requests()
            || !self
                .schema_manager
                .query_manager()
                .sync_manager()
                .outbox()
                .is_empty()
    }

    /// Park a storage response for processing in next batched_tick.
    ///
    /// Called by the IoHandler when a storage response arrives.
    pub fn park_storage_response(&mut self, response: StorageResponse) {
        self.parked_storage_responses.push(response);
        self.io_handler.schedule_batched_tick();
    }

    /// Park a sync message for processing in next batched_tick.
    ///
    /// Called by the IoHandler when a sync message arrives from the network.
    pub fn park_sync_message(&mut self, message: InboxEntry) {
        self.parked_sync_messages.push(message);
        self.io_handler.schedule_batched_tick();
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    /// Subscribe to a query with a callback.
    ///
    /// The callback is invoked during immediate_tick() when results change.
    /// Returns a handle for later unsubscription.
    ///
    /// On native platforms, the callback must be `Send` for thread safety.
    /// On WASM (single-threaded), `Send` is not required.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn subscribe<F>(
        &mut self,
        query: Query,
        callback: F,
        session: Option<Session>,
    ) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + Send + 'static,
    {
        self.subscribe_impl(query, Box::new(callback), session)
    }

    /// Subscribe to a query with a callback (WASM version - no Send required).
    #[cfg(target_arch = "wasm32")]
    pub fn subscribe<F>(
        &mut self,
        query: Query,
        callback: F,
        session: Option<Session>,
    ) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + 'static,
    {
        self.subscribe_impl(query, Box::new(callback), session)
    }

    /// Internal subscribe implementation.
    ///
    /// Uses `subscribe_with_sync` so that subscriptions flow through the outbox
    /// and are sent to connected servers.
    fn subscribe_impl(
        &mut self,
        query: Query,
        callback: SubscriptionCallback,
        session: Option<Session>,
    ) -> Result<SubscriptionHandle, RuntimeError> {
        // Use subscribe_with_sync to ensure subscriptions flow through outbox
        let query_sub_id = self
            .schema_manager
            .query_manager_mut()
            .subscribe_with_sync(query, session)
            .map_err(|e| RuntimeError::QueryError(format!("{:?}", e)))?;

        let handle = SubscriptionHandle(self.next_subscription_handle);
        self.next_subscription_handle += 1;

        self.subscriptions.insert(
            handle,
            SubscriptionState {
                query_sub_id,
                callback,
            },
        );
        self.subscription_reverse.insert(query_sub_id, handle);

        self.immediate_tick();
        Ok(handle)
    }

    /// Unsubscribe from a query.
    pub fn unsubscribe(&mut self, handle: SubscriptionHandle) {
        if let Some(state) = self.subscriptions.remove(&handle) {
            self.subscription_reverse.remove(&state.query_sub_id);
            self.schema_manager
                .query_manager_mut()
                .unsubscribe_with_sync(state.query_sub_id);
        }
    }

    /// Subscribe to a query with explicit schema context (for server use).
    ///
    /// This is used by servers to create subscriptions on behalf of clients
    /// that may be using different schema versions. Returns a QueryId for
    /// server-side subscription tracking.
    pub fn subscribe_with_schema_context(
        &mut self,
        query: Query,
        schema_context: &crate::schema_manager::QuerySchemaContext,
        session: Option<Session>,
    ) -> Result<crate::sync_manager::QueryId, RuntimeError> {
        let query_sub_id = self
            .schema_manager
            .subscribe_with_schema_context(query, schema_context, session)
            .map_err(|e| RuntimeError::QueryError(format!("{:?}", e)))?;

        self.immediate_tick();
        Ok(crate::sync_manager::QueryId(query_sub_id.0))
    }

    // =========================================================================
    // Queries
    // =========================================================================

    /// Execute a one-shot query.
    ///
    /// Returns results once the local query graph settles. This uses subscribe
    /// internally, which triggers sync with upstream servers.
    ///
    /// **Limitation:** "Complete" means the local query graph has settled on
    /// locally persisted data. We do NOT currently wait for confirmation that
    /// results reflect all upstream server tiers. See sync_manager.md Future Work.
    pub fn query(&mut self, query: Query, session: Option<Session>) -> QueryFuture {
        let (sender, receiver) = oneshot::channel();

        // Subscribe with sync - this triggers server to send matching data
        let sub_id = match self
            .schema_manager
            .query_manager_mut()
            .subscribe_with_sync(query, session)
        {
            Ok(id) => id,
            Err(e) => {
                let _ = sender.send(Err(RuntimeError::QueryError(format!("{:?}", e))));
                return QueryFuture::new(receiver);
            }
        };

        // Store as pending one-shot query waiting for first callback
        let handle = SubscriptionHandle(self.next_subscription_handle);
        self.next_subscription_handle += 1;

        self.pending_one_shot_queries.insert(
            handle,
            PendingOneShotQuery {
                subscription_id: sub_id,
                sender: Some(sender),
            },
        );
        self.subscription_reverse.insert(sub_id, handle);

        self.immediate_tick();
        QueryFuture::new(receiver)
    }

    // =========================================================================
    // CRUD Operations
    // =========================================================================

    /// Insert a row into a table.
    pub fn insert(
        &mut self,
        table: &str,
        values: Vec<Value>,
        session: Option<&Session>,
    ) -> Result<ObjectId, RuntimeError> {
        let result = self
            .schema_manager
            .insert_with_session(table, &values, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;
        self.immediate_tick();
        Ok(result.row_id)
    }

    /// Update a row (partial update by column name).
    pub fn update(
        &mut self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        session: Option<&Session>,
    ) -> Result<(), RuntimeError> {
        let (table, mut current_values) = self
            .schema_manager
            .query_manager_mut()
            .get_row(object_id)
            .ok_or(RuntimeError::NotFound)?;

        let schema = self.schema_manager.current_schema();
        let table_name = TableName::new(&table);
        let table_schema = schema
            .get(&table_name)
            .ok_or_else(|| RuntimeError::WriteError("Table not found".to_string()))?;

        for (col_name, new_value) in values {
            if let Some(idx) = table_schema.descriptor.column_index(&col_name) {
                current_values[idx] = new_value;
            } else {
                return Err(RuntimeError::WriteError(format!(
                    "Column '{}' not found",
                    col_name
                )));
            }
        }

        self.schema_manager
            .query_manager_mut()
            .update_with_session(object_id, &current_values, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;

        self.immediate_tick();
        Ok(())
    }

    /// Delete a row.
    pub fn delete(
        &mut self,
        object_id: ObjectId,
        session: Option<&Session>,
    ) -> Result<(), RuntimeError> {
        self.schema_manager
            .query_manager_mut()
            .delete_with_session(object_id, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;
        self.immediate_tick();
        Ok(())
    }

    // =========================================================================
    // Sync Operations
    // =========================================================================

    /// Push a sync message to the inbox (from network).
    pub fn push_sync_inbox(&mut self, entry: InboxEntry) {
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(entry);
    }

    /// Add a server connection.
    pub fn add_server(&mut self, server_id: ServerId) {
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .add_server(server_id);
        self.immediate_tick();
    }

    /// Remove a server connection.
    pub fn remove_server(&mut self, server_id: ServerId) {
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .remove_server(server_id);
    }

    /// Add a client connection.
    pub fn add_client(&mut self, client_id: ClientId, session: Option<Session>) {
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
        sm.add_client(client_id);
        if let Some(s) = session {
            sm.set_client_session(client_id, s);
        }
        self.immediate_tick();
    }

    /// Ensure a client exists with the given session.
    ///
    /// If the client already exists with the same session, this is a no-op.
    /// If the client exists with a different session, we currently panic with todo!()
    /// as session migration is not yet implemented.
    /// If the client doesn't exist, it's added with the given session.
    pub fn ensure_client_with_session(&mut self, client_id: ClientId, session: Option<Session>) {
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
        if let Some(existing) = sm.get_client(client_id) {
            // Client exists - check session matches
            if existing.session != session {
                todo!(
                    "Client {:?} exists with different session - handle session change",
                    client_id
                );
            }
            // Session matches, nothing to do
        } else {
            // Client doesn't exist, add it
            sm.add_client(client_id);
            if let Some(s) = session {
                sm.set_client_session(client_id, s);
            }
            self.immediate_tick();
        }
    }

    /// Add a client connection and sync all data to them.
    pub fn add_client_with_full_sync(&mut self, client_id: ClientId, session: Option<Session>) {
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
        sm.add_client_with_full_sync(client_id);
        if let Some(s) = session {
            sm.set_client_session(client_id, s);
        }
        self.immediate_tick();
    }

    /// Remove a client connection.
    pub fn remove_client(&mut self, client_id: ClientId) {
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .remove_client(client_id);
    }

    // =========================================================================
    // Schema/State Access
    // =========================================================================

    /// Get the current schema.
    pub fn current_schema(&self) -> &Schema {
        self.schema_manager.current_schema()
    }

    /// Get mutable access to the underlying SchemaManager.
    pub fn schema_manager_mut(&mut self) -> &mut SchemaManager {
        &mut self.schema_manager
    }

    /// Get access to the underlying SchemaManager.
    pub fn schema_manager(&self) -> &SchemaManager {
        &self.schema_manager
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::TestDriver;
    use crate::io_handler::TestIoHandler;
    use crate::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};
    use crate::schema_manager::AppId;
    use crate::sync_manager::SyncManager;
    use std::sync::{Arc, Mutex};

    fn test_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build()
    }

    fn create_test_runtime() -> RuntimeCore<TestIoHandler<TestDriver>> {
        let schema = test_schema();
        let app_id = AppId::from_name("test-app");
        let sync_manager = SyncManager::new();
        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
        let handler = TestIoHandler::new(TestDriver::new());
        let mut core = RuntimeCore::new(schema_manager, handler);
        // Load indices via batched_tick (TestIoHandler processes synchronously)
        core.schema_manager_mut()
            .query_manager_mut()
            .reset_indices_for_cold_start();
        for _ in 0..10 {
            core.batched_tick();
        }
        core
    }

    /// Helper to execute a query synchronously via subscribe/process/unsubscribe.
    fn execute_query(
        core: &mut RuntimeCore<TestIoHandler<TestDriver>>,
        query: Query,
    ) -> Vec<(ObjectId, Vec<Value>)> {
        let qm = core.schema_manager_mut().query_manager_mut();
        let sub_id = qm.subscribe(query).unwrap();
        qm.process();
        let results = qm.get_subscription_results(sub_id);
        qm.unsubscribe_with_sync(sub_id);
        results
    }

    #[test]
    fn test_runtime_core_new() {
        let core = create_test_runtime();
        let schema = core.current_schema();
        assert!(schema.contains_key(&TableName::new("users")));
    }

    #[test]
    fn test_runtime_core_insert_query() {
        let mut core = create_test_runtime();

        // Insert a row
        let values = vec![
            Value::Uuid(ObjectId::new()),
            Value::Text("Alice".to_string()),
        ];
        let object_id = core.insert("users", values.clone(), None).unwrap();
        assert!(!object_id.0.is_nil());

        // Tick to process (batched_tick drains pending responses from TestIoHandler)
        core.immediate_tick();
        core.batched_tick();

        // Query for the row - using the sync execute_query helper for testing
        let query = Query::new("users");
        let results = execute_query(&mut core, query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, object_id);
    }

    #[test]
    fn test_runtime_core_subscription() {
        let mut core = create_test_runtime();

        // Track callback invocations
        let updates: Arc<Mutex<Vec<SubscriptionDelta>>> = Arc::new(Mutex::new(Vec::new()));
        let updates_clone = updates.clone();

        // Subscribe
        let query = Query::new("users");
        let handle = core
            .subscribe(
                query,
                move |delta| {
                    updates_clone.lock().unwrap().push(delta);
                },
                None,
            )
            .unwrap();

        // Insert a row
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Bob".to_string())];
        let _object_id = core.insert("users", values, None).unwrap();

        // Tick to process - callbacks are invoked during immediate_tick
        core.immediate_tick();
        core.batched_tick();

        // Should have received an update
        let updates_vec = updates.lock().unwrap();
        assert!(
            !updates_vec.is_empty(),
            "Should receive subscription update"
        );
        assert_eq!(updates_vec[0].handle, handle);

        // Unsubscribe
        drop(updates_vec);
        core.unsubscribe(handle);
    }

    #[test]
    fn test_runtime_core_update_delete() {
        let mut core = create_test_runtime();

        // Insert a row
        let id = ObjectId::new();
        let values = vec![Value::Uuid(id), Value::Text("Charlie".to_string())];
        let object_id = core.insert("users", values, None).unwrap();
        core.immediate_tick();
        core.batched_tick();

        // Partial update
        let updates = vec![("name".to_string(), Value::Text("Dave".to_string()))];
        core.update(object_id, updates, None).unwrap();
        core.immediate_tick();
        core.batched_tick();

        // Verify via query
        let query = Query::new("users");
        let results = execute_query(&mut core, query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[1], Value::Text("Dave".to_string()));

        // Delete
        core.delete(object_id, None).unwrap();
        core.immediate_tick();
        core.batched_tick();

        // Verify deleted
        let query = Query::new("users");
        let results = execute_query(&mut core, query);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_park_storage_response() {
        let mut core = create_test_runtime();

        // Park a response
        let response = StorageResponse::CreateObject {
            id: ObjectId::new(),
            result: Ok(()),
        };
        core.park_storage_response(response);

        // Should have parked response
        assert_eq!(core.parked_storage_responses.len(), 1);
    }

    #[test]
    fn test_park_sync_message() {
        use crate::object::BranchName;
        use crate::sync_manager::{Source, SyncPayload};

        let mut core = create_test_runtime();

        // Park a message
        let message = InboxEntry {
            source: Source::Server(ServerId::new()),
            payload: SyncPayload::ObjectUpdated {
                object_id: ObjectId::new(),
                metadata: None,
                branch_name: BranchName::new("main"),
                commits: vec![],
            },
        };
        core.park_sync_message(message);

        // Should have parked message
        assert_eq!(core.parked_sync_messages.len(), 1);
    }
}

#[cfg(test)]
mod delayed_io_tests {
    use super::*;
    use crate::driver::TestDriver;
    use crate::io_handler::{DelayedIoHandler, TestIoHandler};
    use crate::query_manager::query::Query;
    use crate::query_manager::types::{ColumnType, SchemaBuilder, TableSchema, Value};
    use crate::schema_manager::AppId;
    use crate::sync_manager::SyncManager;
    use std::sync::{Arc, Mutex};

    // ============================================================
    // Test Helpers
    // ============================================================

    fn test_users_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build()
    }

    fn test_multi_table_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .table(
                TableSchema::builder("todos")
                    .column("id", ColumnType::Uuid)
                    .column("title", ColumnType::Text),
            )
            .build()
    }

    /// Create RuntimeCore with DelayedIoHandler (no auto-processing)
    /// For tests that don't need pre-populated data.
    /// Note: Fresh runtimes initialize indices in memory immediately,
    /// so this is mainly useful for unit tests of the DelayedIoHandler itself.
    #[allow(dead_code)]
    fn create_delayed_runtime() -> RuntimeCore<DelayedIoHandler> {
        let schema = test_users_schema();
        let app_id = AppId::from_name("test-app");
        let sync_manager = SyncManager::new();
        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
        let handler = DelayedIoHandler::new();
        RuntimeCore::new(schema_manager, handler)
    }

    /// Create RuntimeCore with DelayedIoHandler for multi-table schema
    #[allow(dead_code)]
    fn create_delayed_runtime_multi_table() -> RuntimeCore<DelayedIoHandler> {
        let schema = test_multi_table_schema();
        let app_id = AppId::from_name("test-app");
        let sync_manager = SyncManager::new();
        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
        let handler = DelayedIoHandler::new();
        RuntimeCore::new(schema_manager, handler)
    }

    /// Insert data with sync driver, then create fresh runtime with async driver.
    /// This simulates reopening a database with persisted data.
    fn create_cold_start_runtime(
        setup: impl FnOnce(&mut RuntimeCore<TestIoHandler<TestDriver>>),
    ) -> RuntimeCore<DelayedIoHandler> {
        let schema = test_users_schema();
        let app_id = AppId::from_name("test-app");

        // Phase 1: Create and populate with TestIoHandler (synchronous)
        let mut warm_runtime = {
            let sync_manager = SyncManager::new();
            let schema_manager =
                SchemaManager::new(sync_manager, schema.clone(), app_id.clone(), "dev", "main")
                    .unwrap();
            let handler = TestIoHandler::new(TestDriver::new());
            let mut core = RuntimeCore::new(schema_manager, handler);
            // Reset indices and tick to stabilize
            core.schema_manager_mut()
                .query_manager_mut()
                .reset_indices_for_cold_start();
            for _ in 0..10 {
                core.batched_tick();
            }
            core
        };

        // Run setup (inserts, etc.)
        setup(&mut warm_runtime);
        for _ in 0..10 {
            warm_runtime.batched_tick();
        }

        // Extract driver with persisted data
        let driver = warm_runtime.into_io_handler().into_driver();

        // Phase 2: Create fresh runtime with that driver's data (async)
        let sync_manager = SyncManager::new();
        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
        let handler = DelayedIoHandler::with_driver(driver);
        let mut core = RuntimeCore::new(schema_manager, handler);

        // CRITICAL: Reset indices so they know to load from storage
        // Without this, indices are "fresh" and don't request loading
        core.schema_manager_mut()
            .query_manager_mut()
            .reset_indices_for_cold_start();

        core
    }

    fn make_user_values(email: &str) -> Vec<Value> {
        vec![
            Value::Uuid(crate::object::ObjectId::new()),
            Value::Text(email.to_string()),
        ]
    }

    #[allow(dead_code)]
    fn make_todo_values(title: &str) -> Vec<Value> {
        vec![
            Value::Uuid(crate::object::ObjectId::new()),
            Value::Text(title.to_string()),
        ]
    }

    /// Run runtime to completion, delivering all storage responses
    fn run_to_completion(core: &mut RuntimeCore<DelayedIoHandler>) {
        let mut iterations = 0;
        loop {
            core.immediate_tick();
            let responses = core.io_handler_mut().flush();
            if responses.is_empty() {
                break;
            }
            for response in responses {
                core.park_storage_response(response);
            }
            iterations += 1;
            if iterations > 100 {
                panic!("Infinite loop in run_to_completion");
            }
        }
    }

    // ============================================================
    // Category A: Index Readiness and Pending State
    // ============================================================

    /// A1: BTreeIndex reports not ready when meta not loaded
    /// Status: RED - no is_ready() method exists yet
    /// Fixed by: Phase 1 (add is_ready() method)
    #[test]
    fn a1_btree_index_not_ready_without_meta() {
        use crate::query_manager::index::btree_index::BTreeIndex;

        let index = BTreeIndex::new("users", "_id");

        // THE INVARIANT: Fresh index with no meta should report is_ready() == false
        // Phase 1 will add the is_ready() method.
        //
        // For now, we test that scan_all returns empty AND the index signals
        // that it's not ready to serve queries. Today, scan_all silently returns
        // empty without any indication that loading is needed - that's the bug.

        let results = index.scan_all();
        assert!(results.is_empty(), "No results without meta loaded");

        // THE BUG: There's no way to distinguish "empty index" from "not loaded yet"
        // After Phase 1: assert!(!index.is_ready(), "Should not be ready without meta");

        // This assertion will FAIL today because is_ready() doesn't exist.
        // Workaround: We check that the index considers itself not ready by
        // verifying that operations return "not ready" indicators.
        // But insert() triggers loading, scan_all() just returns empty silently.
        // The invariant we need: a way to ASK if the index is ready.

        // For now, fail explicitly to document the missing API:
        assert!(
            false,
            "BTreeIndex lacks is_ready() method - cannot distinguish empty from not-loaded"
        );
    }

    /// A1: Index not ready when meta loaded but root page still missing
    /// Status: RED - need to simulate cold-start with existing meta
    /// Fixed by: Phase 1
    #[test]
    fn a1_btree_index_not_ready_without_root_page() {
        use crate::query_manager::index::btree_index::BTreeIndex;
        use crate::query_manager::index::btree_page::IndexMeta;

        let mut index = BTreeIndex::new("users", "_id");

        // Create meta that references a root page (page ID 1, not 0)
        // This simulates loading existing index meta from storage
        let mut meta = IndexMeta::new();
        meta.root_page_id = crate::query_manager::index::btree_page::PageId(1);
        meta.next_page_id = 2;
        let meta_bytes = meta.serialize();

        // Load the meta - this should NOT auto-create the root page
        // because meta says root is page 1, which needs to be loaded
        index.process_meta_load(Some(meta_bytes));

        // Meta is loaded, but root page (ID 1) is not loaded yet
        // The index should queue a load request for page 1
        assert!(
            index.has_pending_requests(),
            "Index should queue root page load after meta loads"
        );

        // Root page is NOT loaded (only Loading state, not Loaded)
        // After Phase 1: assert!(!index.is_ready(), "Should not be ready until root page loads");

        // For now, verify root_exists returns false (page 1 not loaded)
        assert!(
            !index.root_exists(),
            "Root page should not exist until loaded"
        );
    }

    /// A1: Index ready once meta AND root page are both loaded
    /// Status: RED - is_ready() method doesn't exist
    /// Fixed by: Phase 1
    #[test]
    fn a1_btree_index_ready_with_meta_and_root() {
        use crate::query_manager::index::btree_index::BTreeIndex;
        use crate::query_manager::index::btree_page::{BTreePage, IndexMeta, PageId};

        let mut index = BTreeIndex::new("users", "_id");

        // Create meta referencing root page 1
        let mut meta = IndexMeta::new();
        meta.root_page_id = PageId(1);
        meta.next_page_id = 2;
        index.process_meta_load(Some(meta.serialize()));

        // Drain the pending load request (meta_load queues a request for page 1)
        let requests = index.take_storage_requests();
        assert!(
            requests.iter().any(|r| matches!(
                r,
                crate::storage::StorageRequest::LoadIndexPage { page_id: 1, .. }
            )),
            "Should have queued load request for page 1"
        );

        // Now load the root page (simulating storage response)
        let root_page = BTreePage::new_leaf();
        index.process_page_load(PageId(1), Some(root_page.serialize()));

        // THE INVARIANT: Once meta AND root are loaded, is_ready() == true
        // Today we can only check root_exists() which is a partial check.

        assert!(index.root_exists(), "Root page should exist after loading");

        // After Phase 1, this is the real test:
        // assert!(index.is_ready(), "Index should be ready with meta and root");

        // For now, fail to document the missing API:
        assert!(
            false,
            "BTreeIndex lacks is_ready() method to confirm full readiness"
        );
    }

    /// A2: IndexScanNode returns pending=true when index.is_ready() == false
    /// Status: RED - IndexScanNode always returns pending=false
    /// Fixed by: Phase 1
    #[test]
    fn a2_index_scan_returns_pending_when_not_ready() {
        use crate::query_manager::graph_nodes::SourceContext;
        use crate::query_manager::graph_nodes::SourceNode;
        use crate::query_manager::graph_nodes::index_scan::IndexScanNode;
        use crate::query_manager::index::ScanCondition;
        use crate::query_manager::index::btree_index::BTreeIndex;
        use crate::query_manager::types::{ColumnDescriptor, RowDescriptor};
        use ahash::AHashMap;

        // Create index without loading meta
        let index = BTreeIndex::new("users", "_id");

        let mut indices: AHashMap<(String, String, String), BTreeIndex> = AHashMap::new();
        indices.insert(
            ("users".to_string(), "_id".to_string(), "main".to_string()),
            index,
        );

        let row_descriptor = RowDescriptor {
            columns: vec![
                ColumnDescriptor::new("id", ColumnType::Uuid),
                ColumnDescriptor::new("email", ColumnType::Text),
            ],
        };

        let mut node = IndexScanNode::new("users", "_id", ScanCondition::All, row_descriptor);

        let ctx = SourceContext { indices: &indices };
        let delta = node.scan(&ctx);

        // TODAY: delta.pending == false (WRONG)
        // AFTER Phase 1: delta.pending == true (CORRECT)
        assert!(
            delta.pending,
            "IndexScanNode should signal pending when index not ready"
        );
    }

    /// A2: After transitioning from pending to ready, scan forces full rescan
    /// Status: RED - no pending tracking exists, no full rescan on transition
    /// Fixed by: Phase 1
    #[test]
    fn a2_index_scan_becomes_ready_after_loading() {
        use crate::query_manager::graph_nodes::SourceContext;
        use crate::query_manager::graph_nodes::SourceNode;
        use crate::query_manager::graph_nodes::index_scan::IndexScanNode;
        use crate::query_manager::index::ScanCondition;
        use crate::query_manager::index::btree_index::BTreeIndex;
        use crate::query_manager::types::{ColumnDescriptor, RowDescriptor};
        use ahash::AHashMap;

        // Create index and insert some data BEFORE we start scanning
        let mut index = BTreeIndex::new("users", "_id");
        index.process_meta_load(None); // Initialize

        // Insert 3 rows into the index
        let id1 = crate::object::ObjectId::new();
        let id2 = crate::object::ObjectId::new();
        let id3 = crate::object::ObjectId::new();
        let _ = index.insert(&id1.0.as_bytes()[..], id1);
        let _ = index.insert(&id2.0.as_bytes()[..], id2);
        let _ = index.insert(&id3.0.as_bytes()[..], id3);

        // Now simulate cold-start by creating a FRESH index (unloaded)
        let fresh_index = BTreeIndex::new("users", "_id");

        let mut indices: AHashMap<(String, String, String), BTreeIndex> = AHashMap::new();
        indices.insert(
            ("users".to_string(), "_id".to_string(), "main".to_string()),
            fresh_index,
        );

        let row_descriptor = RowDescriptor {
            columns: vec![
                ColumnDescriptor::new("id", ColumnType::Uuid),
                ColumnDescriptor::new("email", ColumnType::Text),
            ],
        };

        let mut node = IndexScanNode::new("users", "_id", ScanCondition::All, row_descriptor);

        let ctx = SourceContext { indices: &indices };

        // First scan: index not ready, should be pending
        let delta1 = node.scan(&ctx);
        // TODAY: delta1.pending is always false (bug)
        // AFTER Phase 1: delta1.pending is true
        assert!(
            delta1.pending,
            "First scan should be pending when index not ready"
        );
        assert!(delta1.added.is_empty(), "No results while pending");

        // Now "load" the index by putting our pre-populated index in place
        indices.insert(
            ("users".to_string(), "_id".to_string(), "main".to_string()),
            index, // The one with 3 rows
        );

        // Second scan: should be ready and return ALL rows (full rescan)
        let ctx = SourceContext { indices: &indices };
        let delta2 = node.scan(&ctx);

        assert!(!delta2.pending, "Should be ready after index loads");
        // The key invariant: transition from pending to ready triggers FULL rescan
        // All 3 rows should appear in the delta
        assert_eq!(
            delta2.added.len(),
            3,
            "Full rescan should return all 3 rows after transitioning from pending"
        );
    }

    /// A3: OutputNode holds back results when input has pending=true
    /// Status: GREEN - OutputNode infrastructure already exists
    #[test]
    fn a3_output_node_holds_back_when_input_pending() {
        use crate::query_manager::graph_nodes::RowNode;
        use crate::query_manager::graph_nodes::output::{OutputMode, OutputNode};
        use crate::query_manager::types::{
            ColumnDescriptor, RowDescriptor, Tuple, TupleDelta, TupleDescriptor,
        };

        let row_descriptor = RowDescriptor {
            columns: vec![
                ColumnDescriptor::new("id", ColumnType::Uuid),
                ColumnDescriptor::new("email", ColumnType::Text),
            ],
        };

        let tuple_descriptor = TupleDescriptor::single("users", row_descriptor);
        let mut node = OutputNode::with_tuple_descriptor(tuple_descriptor, OutputMode::Delta);

        let id = crate::object::ObjectId::new();
        let tuple = Tuple::from_id(id);

        // Process with pending=true
        node.process(TupleDelta {
            pending: true,
            added: vec![tuple],
            removed: vec![],
            updated: vec![],
        });

        // Internal state updated
        assert_eq!(
            node.current_tuples().len(),
            1,
            "Internal state should update"
        );

        // But nothing in output queue (no callback would fire)
        let deltas = node.take_tuple_deltas();
        assert!(
            deltas.is_empty(),
            "OutputNode should hold back - no callback fires while pending"
        );
    }

    // ============================================================
    // Category B: Cold Start and Query Waiting
    // ============================================================

    /// B1: Callback doesn't fire until index is ready
    /// Status: RED - callback fires immediately with empty results
    /// Fixed by: Phase 1 + Phase 3
    #[test]
    fn b1_no_callback_while_index_loading() {
        let mut core = create_cold_start_runtime(|warm| {
            let _ = warm.insert("users", make_user_values("alice@test.com"), None);
            let _ = warm.insert("users", make_user_values("bob@test.com"), None);
        });

        let callbacks = Arc::new(Mutex::new(Vec::<usize>::new()));
        let cb_clone = callbacks.clone();

        // Subscribe to users query
        let query = Query::new("users");
        let _handle = core
            .subscribe(
                query,
                move |delta| {
                    cb_clone.lock().unwrap().push(delta.delta.added.len());
                },
                None,
            )
            .unwrap();

        // Run immediate_tick - graph settles but index not loaded
        core.immediate_tick();

        // Callback should NOT have fired yet
        let cbs = callbacks.lock().unwrap();
        assert!(
            cbs.is_empty(),
            "Callback should NOT fire while index loading, got {} callbacks",
            cbs.len()
        );
    }

    /// B1b: After index loads, callback fires with correct data
    /// Status: RED - fires prematurely with empty
    /// Fixed by: Phase 1 + Phase 2 + Phase 3
    #[test]
    fn b1_callback_fires_after_index_loads() {
        let mut core = create_cold_start_runtime(|warm| {
            let _ = warm.insert("users", make_user_values("alice@test.com"), None);
        });

        let callbacks = Arc::new(Mutex::new(Vec::<usize>::new()));
        let cb_clone = callbacks.clone();

        let query = Query::new("users");
        let _handle = core
            .subscribe(
                query,
                move |delta| {
                    cb_clone.lock().unwrap().push(delta.delta.added.len());
                },
                None,
            )
            .unwrap();

        // Tick 1: Index not loaded - no callback yet
        core.immediate_tick();
        assert!(
            callbacks.lock().unwrap().is_empty(),
            "No callback before load"
        );

        // Deliver storage responses until stable
        run_to_completion(&mut core);

        // NOW callback should have fired with data
        let cbs = callbacks.lock().unwrap();
        assert!(!cbs.is_empty(), "Callback should fire after index loaded");
        assert!(cbs.last().unwrap() > &0, "Should have found the user");
    }

    /// B2: Cold start produces exactly ONE callback with complete data
    /// Status: RED - multiple spurious callbacks or empty callback fires before data loads
    /// Fixed by: Phase 1
    #[test]
    fn b2_exactly_one_callback_after_cold_start() {
        let mut core = create_cold_start_runtime(|warm| {
            let _ = warm.insert("users", make_user_values("alice@test.com"), None);
        });

        let callbacks = Arc::new(Mutex::new(Vec::<usize>::new()));
        let cb_clone = callbacks.clone();

        let query = Query::new("users");
        let _handle = core
            .subscribe(
                query,
                move |delta| {
                    cb_clone.lock().unwrap().push(delta.delta.added.len());
                },
                None,
            )
            .unwrap();

        // Run to completion
        run_to_completion(&mut core);

        let cbs = callbacks.lock().unwrap();
        assert_eq!(
            cbs.len(),
            1,
            "Should have exactly 1 callback, not {}",
            cbs.len()
        );
        assert_eq!(cbs[0], 1, "The single callback should contain the user");
    }

    // ============================================================
    // Category C: Dirty Marking When Index Loads
    // ============================================================

    /// C1: Subscriptions marked dirty when index meta loads
    /// Status: RED - no dirty marking on meta load
    /// Fixed by: Phase 2
    #[test]
    fn c1_subscription_marked_dirty_when_index_meta_loads() {
        // Use cold-start runtime so there's actual data to load
        let mut core = create_cold_start_runtime(|warm| {
            let _ = warm.insert("users", make_user_values("alice@test.com"), None);
        });

        let callbacks = Arc::new(Mutex::new(Vec::<usize>::new()));
        let cb_clone = callbacks.clone();

        // Subscribe to users query
        let query = Query::new("users");
        let _handle = core
            .subscribe(
                query,
                move |delta| {
                    cb_clone.lock().unwrap().push(delta.delta.added.len());
                },
                None,
            )
            .unwrap();

        // Initial tick - subscription settles, but index not loaded
        // With cold-start, there should be pending storage requests
        core.immediate_tick();

        // Check that load requests were queued (or callback was held back)
        // The key invariant: no callback should fire yet because index isn't ready
        let cbs_before = callbacks.lock().unwrap().clone();
        assert!(
            cbs_before.is_empty(),
            "No callback should fire before index loads"
        );

        // Deliver the storage responses
        let responses = core.io_handler_mut().flush();
        for response in responses {
            core.park_storage_response(response);
        }
        core.immediate_tick();

        // After loading, subscription should have been marked dirty and re-settled
        // resulting in a callback with the data
        run_to_completion(&mut core);

        let cbs_after = callbacks.lock().unwrap();
        assert!(
            !cbs_after.is_empty(),
            "Callback should fire after index loads"
        );
        assert_eq!(cbs_after[0], 1, "Should have found the user");
    }

    /// C2: Only relevant subscriptions marked dirty when index loads
    /// Status: RED - both subscriptions get callbacks regardless of which index loads
    /// Fixed by: Phase 2
    #[test]
    fn c2_only_relevant_subscriptions_marked_dirty() {
        // Use cold-start with data in BOTH tables
        let schema = test_multi_table_schema();
        let app_id = AppId::from_name("test-app");

        // Phase 1: Create warm runtime with data in both tables
        let driver = {
            let sync_manager = SyncManager::new();
            let schema_manager =
                SchemaManager::new(sync_manager, schema.clone(), app_id.clone(), "dev", "main")
                    .unwrap();
            let handler = TestIoHandler::new(TestDriver::new());
            let mut core = RuntimeCore::new(schema_manager, handler);
            core.schema_manager_mut()
                .query_manager_mut()
                .reset_indices_for_cold_start();
            for _ in 0..10 {
                core.batched_tick();
            }

            // Insert data into users table only (not todos)
            let _ = core.insert("users", make_user_values("alice@test.com"), None);
            for _ in 0..10 {
                core.batched_tick();
            }
            core.into_io_handler().into_driver()
        };

        // Phase 2: Cold-start with delayed I/O
        let sync_manager = SyncManager::new();
        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
        let handler = DelayedIoHandler::with_driver(driver);
        let mut core = RuntimeCore::new(schema_manager, handler);

        let users_callbacks = Arc::new(Mutex::new(Vec::<usize>::new()));
        let todos_callbacks = Arc::new(Mutex::new(Vec::<usize>::new()));

        let users_cb = users_callbacks.clone();
        let todos_cb = todos_callbacks.clone();

        // Subscribe to both tables
        let _users_handle = core
            .subscribe(
                Query::new("users"),
                move |delta| users_cb.lock().unwrap().push(delta.delta.added.len()),
                None,
            )
            .unwrap();
        let _todos_handle = core
            .subscribe(
                Query::new("todos"),
                move |delta| todos_cb.lock().unwrap().push(delta.delta.added.len()),
                None,
            )
            .unwrap();

        core.immediate_tick();

        // No callbacks should have fired yet (both indices loading)
        assert!(
            users_callbacks.lock().unwrap().is_empty(),
            "Users callback should not fire while loading"
        );
        assert!(
            todos_callbacks.lock().unwrap().is_empty(),
            "Todos callback should not fire while loading"
        );

        // Run to completion
        run_to_completion(&mut core);

        // Key assertions:
        // 1. Users subscription should get exactly 1 callback with 1 row
        let users_cbs = users_callbacks.lock().unwrap();
        assert_eq!(users_cbs.len(), 1, "Users should get exactly 1 callback");
        assert_eq!(users_cbs[0], 1, "Users callback should have 1 row");

        // 2. Todos subscription should get exactly 1 callback with 0 rows (empty table)
        let todos_cbs = todos_callbacks.lock().unwrap();
        assert_eq!(todos_cbs.len(), 1, "Todos should get exactly 1 callback");
        assert_eq!(todos_cbs[0], 0, "Todos callback should have 0 rows");
    }

    // ============================================================
    // Category D: Sync Writes Visible via pending_writes
    // ============================================================

    /// D1: Insert while loading appears in first callback
    /// Status: RED - pending_writes concept doesn't exist
    /// Fixed by: Phase 4
    #[test]
    fn d1_insert_during_index_loading_appears_in_first_callback() {
        let mut core = create_cold_start_runtime(|warm| {
            let _ = warm.insert("users", make_user_values("old@test.com"), None);
        });

        let callbacks = Arc::new(Mutex::new(Vec::<usize>::new()));
        let cb_clone = callbacks.clone();

        let query = Query::new("users");
        let _handle = core
            .subscribe(
                query,
                move |delta| {
                    cb_clone.lock().unwrap().push(delta.delta.added.len());
                },
                None,
            )
            .unwrap();

        // Initial tick - index not loaded, no callback
        core.immediate_tick();
        assert!(callbacks.lock().unwrap().is_empty(), "No callback yet");

        // Insert new user WHILE index still loading
        let _ = core.insert("users", make_user_values("new@test.com"), None);
        core.immediate_tick();

        // Still no callback (index still loading)
        assert!(callbacks.lock().unwrap().is_empty(), "Still no callback");

        // Now load the index
        run_to_completion(&mut core);

        // First callback contains BOTH old and new user
        let cbs = callbacks.lock().unwrap();
        assert_eq!(cbs.len(), 1, "Should have exactly 1 callback");
        assert_eq!(
            cbs[0], 2,
            "First callback should have both old and new user"
        );
    }

    /// D2: Multiple inserts while loading appear in ONE callback
    /// Status: RED - no pending_writes batching
    /// Fixed by: Phase 4
    /// Uses: cold_start_runtime - index needs to be loading for this test
    #[test]
    fn d2_multiple_inserts_while_loading_appear_together() {
        // Start with empty data but cold-start scenario
        let mut core = create_cold_start_runtime(|_| {});

        let callbacks = Arc::new(Mutex::new(Vec::<usize>::new()));
        let cb_clone = callbacks.clone();

        let query = Query::new("users");
        let _handle = core
            .subscribe(
                query,
                move |delta| {
                    cb_clone.lock().unwrap().push(delta.delta.added.len());
                },
                None,
            )
            .unwrap();

        // Insert 3 users while index might not be loaded
        // Note: With cold-start from empty, index still loads fast
        // The key test is whether inserts batch together
        let _ = core.insert("users", make_user_values("a@test.com"), None);
        core.immediate_tick();

        let _ = core.insert("users", make_user_values("b@test.com"), None);
        core.immediate_tick();

        let _ = core.insert("users", make_user_values("c@test.com"), None);
        core.immediate_tick();

        // Load index if needed
        run_to_completion(&mut core);

        // Should have exactly ONE callback with 3 rows (batched)
        let cbs = callbacks.lock().unwrap();
        assert_eq!(cbs.len(), 1, "Should have exactly 1 callback");
        assert_eq!(cbs[0], 3, "Callback should contain all 3 users");
    }

    /// D3: Same row in pending_writes and B-tree appears once (no duplicates)
    /// Status: RED - no merging logic
    /// Fixed by: Phase 4
    /// Uses: cold_start_runtime (to have loading scenario)
    #[test]
    fn d3_no_duplicate_rows_in_callback() {
        let mut core = create_cold_start_runtime(|_| {});

        // Insert user (goes to pending_writes AND eventually B-tree)
        let _ = core.insert("users", make_user_values("alice@test.com"), None);

        let total_rows = Arc::new(Mutex::new(0usize));
        let rows_clone = total_rows.clone();

        let query = Query::new("users");
        let _handle = core
            .subscribe(
                query,
                move |delta| {
                    *rows_clone.lock().unwrap() += delta.delta.added.len();
                },
                None,
            )
            .unwrap();

        // Process everything
        run_to_completion(&mut core);

        // Total across all callbacks should be exactly 1 row
        let total = *total_rows.lock().unwrap();
        assert_eq!(total, 1, "Same row should not appear twice");
    }

    // ============================================================
    // Category E: Page Discovery and Load Requirements
    // ============================================================

    /// E1: scan_all should report missing meta when meta not loaded
    /// Status: RED - scan_all returns empty Vec, doesn't report what's missing
    /// Fixed by: Phase 5 (change return type to ScanResult with missing field)
    #[test]
    fn e1_scan_all_reports_missing_meta() {
        use crate::query_manager::index::btree_index::BTreeIndex;

        let index = BTreeIndex::new("users", "_id");

        // THE INVARIANT: scan_all should return a ScanResult that includes
        // information about what's missing (meta, pages, etc.)
        //
        // Today: scan_all returns Vec<ObjectId> - just empty when not loaded.
        // There's no way for the caller to know WHY it's empty.

        let result = index.scan_all();

        // The result is empty, but we can't distinguish:
        // - "Index is loaded and empty" vs "Index needs to load meta first"

        // After Phase 5, scan_all returns ScanResult:
        // assert!(result.missing.contains(&LoadRequirement::Meta));

        // For now, fail because this invariant cannot be tested with current API:
        assert!(
            false,
            "scan_all() returns Vec<ObjectId>, cannot report missing meta - implement Phase 5"
        );
    }

    /// E1: scan_all should report missing root page after meta loads
    /// Status: RED - scan_all doesn't report missing pages
    /// Fixed by: Phase 5
    #[test]
    fn e1_scan_all_reports_missing_root_page() {
        use crate::query_manager::index::btree_index::BTreeIndex;
        use crate::query_manager::index::btree_page::{IndexMeta, PageId};

        let mut index = BTreeIndex::new("users", "_id");

        // Create meta that references root page 5 (which doesn't exist yet)
        let mut meta = IndexMeta::new();
        meta.root_page_id = PageId(5);
        meta.next_page_id = 6;
        meta.entry_count = 10; // Claims to have data

        index.process_meta_load(Some(meta.serialize()));

        // Meta is loaded, but root page 5 is NOT loaded yet
        // THE INVARIANT: scan_all should report that page 5 is missing

        let result = index.scan_all();

        // Result is empty, but we can't tell if it's because:
        // - The index is empty, OR
        // - Page 5 hasn't been loaded yet

        // After Phase 5:
        // assert!(result.missing.iter().any(|m| matches!(m, LoadRequirement::Page(5))));

        // For now, fail because this invariant cannot be tested:
        assert!(
            false,
            "scan_all() cannot report which pages are missing - implement Phase 5"
        );
    }

    /// E2: Settlement queues load requests AND holds back callbacks
    /// Status: RED - callbacks fire prematurely with empty results
    /// Fixed by: Phase 5
    #[test]
    fn e2_settlement_returns_pending_with_load_requirements() {
        // Use cold-start so there's actual data requiring index load
        let mut core = create_cold_start_runtime(|warm| {
            let _ = warm.insert("users", make_user_values("alice@test.com"), None);
        });

        let callbacks = Arc::new(Mutex::new(Vec::<usize>::new()));
        let cb_clone = callbacks.clone();

        let _handle = core
            .subscribe(
                Query::new("users"),
                move |delta| {
                    cb_clone.lock().unwrap().push(delta.delta.added.len());
                },
                None,
            )
            .unwrap();

        // First settle - index not loaded
        core.immediate_tick();

        // TWO invariants to check:

        // 1. Load requests should have been queued
        assert!(
            core.io_handler().has_pending_requests(),
            "Settlement should queue index load requests"
        );

        // 2. No callback should fire yet (results are pending)
        let cbs = callbacks.lock().unwrap();
        assert!(
            cbs.is_empty(),
            "Callback should NOT fire while index is loading"
        );
    }

    /// E3: range_scan reports missing sibling pages via next_leaf pointers
    /// Status: RED - range_scan silently stops at unloaded pages
    /// Fixed by: Phase 5
    #[test]
    fn e3_range_scan_reports_missing_siblings() {
        use crate::query_manager::index::btree_index::BTreeIndex;
        use crate::query_manager::index::btree_page::{BTreePage, IndexMeta, PageId};
        use std::ops::Bound;

        // Create an index with meta pointing to a multi-page structure
        let mut index = BTreeIndex::new("users", "score");

        // Set up meta that claims root is page 1
        let mut meta = IndexMeta::new();
        meta.root_page_id = PageId(1);
        meta.next_page_id = 4;
        meta.entry_count = 100;
        index.process_meta_load(Some(meta.serialize()));

        // Drain the pending request for page 1
        let _ = index.take_storage_requests();

        // Load ONLY the root page (page 1), which is a leaf with next_leaf pointing to page 2
        let mut root_leaf = BTreePage::new_leaf();
        if let BTreePage::Leaf {
            entries, next_leaf, ..
        } = &mut root_leaf
        {
            for i in 0i32..10 {
                let mut row_ids = std::collections::HashSet::new();
                row_ids.insert(crate::object::ObjectId::new());
                entries.push(crate::query_manager::index::btree_page::LeafEntry {
                    key: i.to_be_bytes().to_vec(),
                    row_ids,
                });
            }
            // Point to sibling page 2 (which is NOT loaded)
            *next_leaf = Some(PageId(2));
        }
        index.process_page_load(PageId(1), Some(root_leaf.serialize()));

        // Range scan that needs to traverse to page 2 for complete results
        let min = Bound::Included(0i32.to_be_bytes().to_vec());
        let max = Bound::Included(100i32.to_be_bytes().to_vec());
        let result = index.range_scan(&min, &max);

        // We get 10 results from page 1, but page 2 might have more!
        // THE INVARIANT: range_scan should indicate that results are PARTIAL
        // and that page 2 needs to be loaded.

        // Today: range_scan returns Vec<ObjectId>, can't indicate partial results
        // After Phase 5: range_scan returns ScanResult with missing pages

        // For now, fail because this invariant cannot be tested:
        assert!(
            false,
            "range_scan() cannot report missing sibling pages - implement Phase 5"
        );
    }

    // ============================================================
    // Category F: E2E Integration
    // ============================================================

    /// F1: Cold start eventually delivers all persisted data in ONE callback
    /// Status: RED - multiple callbacks or missing data
    /// Fixed by: All phases working together
    #[test]
    fn f1_cold_start_delivers_all_data() {
        let mut core = create_cold_start_runtime(|warm| {
            for i in 0..10 {
                let _ = warm.insert(
                    "users",
                    make_user_values(&format!("user{}@test.com", i)),
                    None,
                );
            }
        });

        let callbacks = Arc::new(Mutex::new(Vec::<usize>::new()));
        let cb_clone = callbacks.clone();

        let query = Query::new("users");
        let _handle = core
            .subscribe(
                query,
                move |delta| {
                    cb_clone.lock().unwrap().push(delta.delta.added.len());
                },
                None,
            )
            .unwrap();

        // Run until stable
        run_to_completion(&mut core);

        let cbs = callbacks.lock().unwrap();
        assert_eq!(cbs.len(), 1, "Should have exactly 1 callback");
        assert_eq!(cbs[0], 10, "Should have all 10 users");
    }

    /// F2: Insert during cold-start included in final result
    /// Status: RED - new write lost or delivered separately
    /// Fixed by: All phases working together
    #[test]
    fn f2_insert_during_cold_start_included() {
        let mut core = create_cold_start_runtime(|warm| {
            let _ = warm.insert("users", make_user_values("old@test.com"), None);
        });

        let callbacks = Arc::new(Mutex::new(Vec::<usize>::new()));
        let cb_clone = callbacks.clone();

        let query = Query::new("users");
        let _handle = core
            .subscribe(
                query,
                move |delta| {
                    cb_clone.lock().unwrap().push(delta.delta.added.len());
                },
                None,
            )
            .unwrap();

        // First tick - no callback yet
        core.immediate_tick();
        assert!(callbacks.lock().unwrap().is_empty(), "No callback yet");

        // Insert new user mid-loading
        let _ = core.insert("users", make_user_values("new@test.com"), None);

        // Run to completion
        run_to_completion(&mut core);

        // ONE callback with both rows
        let cbs = callbacks.lock().unwrap();
        assert_eq!(cbs.len(), 1, "Should have exactly 1 callback");
        assert_eq!(cbs[0], 2, "Should have old and new user together");
    }

    /// F3: Sync edit fires callback synchronously (when index IS ready)
    /// Status: GREEN - documents the invariant to preserve
    #[test]
    fn f3_sync_edit_fires_callback_synchronously() {
        let schema = test_users_schema();
        let app_id = AppId::from_name("test-app");

        // Create with synchronous TestIoHandler
        let sync_manager = SyncManager::new();
        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
        let handler = TestIoHandler::new(TestDriver::new());
        let mut core = RuntimeCore::new(schema_manager, handler);
        core.schema_manager_mut()
            .query_manager_mut()
            .reset_indices_for_cold_start();
        for _ in 0..10 {
            core.batched_tick();
        }

        let callback_count = Arc::new(Mutex::new(0usize));
        let count_clone = callback_count.clone();

        let query = Query::new("users");
        let _handle = core
            .subscribe(
                query,
                move |delta| {
                    if !delta.delta.added.is_empty() {
                        *count_clone.lock().unwrap() += 1;
                    }
                },
                None,
            )
            .unwrap();

        // Initial tick
        core.immediate_tick();
        let initial_count = *callback_count.lock().unwrap();

        // Insert - should fire callback synchronously
        let _ = core.insert("users", make_user_values("test@test.com"), None);
        core.immediate_tick();

        let final_count = *callback_count.lock().unwrap();
        assert!(
            final_count > initial_count,
            "Callback must fire synchronously after insert when index ready"
        );
    }
}
