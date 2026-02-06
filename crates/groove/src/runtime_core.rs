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

use crate::commit::CommitId;
use crate::io_handler::IoHandler;
use crate::object::ObjectId;
use crate::query_manager::QuerySubscriptionId;
use crate::query_manager::encoding::decode_row;
use crate::query_manager::manager::{QueryError, QueryUpdate};
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::query_manager::types::{RowDelta, Schema, TableName, Value};
use crate::schema_manager::SchemaManager;
use crate::sync_manager::{ClientId, InboxEntry, PersistenceTier, ServerId};

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

    /// Parked sync messages (from network).
    parked_sync_messages: Vec<InboxEntry>,

    /// Subscription tracking with callbacks.
    subscriptions: HashMap<SubscriptionHandle, SubscriptionState>,
    /// Reverse map for routing updates.
    subscription_reverse: HashMap<QuerySubscriptionId, SubscriptionHandle>,
    next_subscription_handle: u64,

    /// Pending one-shot queries (query() calls waiting for first callback).
    pending_one_shot_queries: HashMap<SubscriptionHandle, PendingOneShotQuery>,

    /// Watchers for persistence acks: (commit_id, requested_tier) → senders.
    /// A tier >= requested tier satisfies the watcher (e.g., EdgeServer ack satisfies Worker).
    ack_watchers: HashMap<CommitId, Vec<(PersistenceTier, oneshot::Sender<()>)>>,
}

impl<H: IoHandler> RuntimeCore<H> {
    /// Create a new RuntimeCore wrapping a SchemaManager.
    pub fn new(schema_manager: SchemaManager, io_handler: H) -> Self {
        Self {
            schema_manager,
            io_handler,
            parked_sync_messages: Vec::new(),
            subscriptions: HashMap::new(),
            subscription_reverse: HashMap::new(),
            next_subscription_handle: 0,
            pending_one_shot_queries: HashMap::new(),
            ack_watchers: HashMap::new(),
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

    /// Persist the current schema to the catalogue for server sync.
    pub fn persist_schema(&mut self) -> ObjectId {
        self.schema_manager.persist_schema(&mut self.io_handler)
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
        self.schema_manager.process(&mut self.io_handler);

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

        // 3b. Process received persistence acks — resolve matching watchers
        let received_acks = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .take_received_acks();
        for (commit_id, acked_tier) in received_acks {
            if let Some(watchers) = self.ack_watchers.remove(&commit_id) {
                let mut remaining = Vec::new();
                for (requested_tier, sender) in watchers {
                    if acked_tier >= requested_tier {
                        let _ = sender.send(());
                    } else {
                        remaining.push((requested_tier, sender));
                    }
                }
                if !remaining.is_empty() {
                    self.ack_watchers.insert(commit_id, remaining);
                }
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
        // Storage is now synchronous - no requests to send or responses to process.
        // Only sync messages (network) remain async.

        // 1. Send all outgoing sync messages
        let outbox = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        for msg in outbox {
            self.io_handler.send_sync_message(msg);
        }

        // 2. Process parked sync messages
        // (calls immediate_tick internally, which may generate new outbox entries
        // and schedule another batched_tick to send them)
        self.handle_sync_messages();
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
        !self
            .schema_manager
            .query_manager()
            .sync_manager()
            .outbox()
            .is_empty()
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
        self.subscribe_impl(query, Box::new(callback), session, None)
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
        self.subscribe_impl(query, Box::new(callback), session, None)
    }

    /// Subscribe to a query with a callback and optional settled tier.
    ///
    /// If `settled_tier` is provided, the initial callback is held until
    /// the query data has been confirmed at that persistence tier.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn subscribe_with_settled_tier<F>(
        &mut self,
        query: Query,
        callback: F,
        session: Option<Session>,
        settled_tier: Option<PersistenceTier>,
    ) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + Send + 'static,
    {
        self.subscribe_impl(query, Box::new(callback), session, settled_tier)
    }

    /// Subscribe with settled tier (WASM version - no Send required).
    #[cfg(target_arch = "wasm32")]
    pub fn subscribe_with_settled_tier<F>(
        &mut self,
        query: Query,
        callback: F,
        session: Option<Session>,
        settled_tier: Option<PersistenceTier>,
    ) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + 'static,
    {
        self.subscribe_impl(query, Box::new(callback), session, settled_tier)
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
        settled_tier: Option<PersistenceTier>,
    ) -> Result<SubscriptionHandle, RuntimeError> {
        // Use subscribe_with_sync to ensure subscriptions flow through outbox
        let query_sub_id = self
            .schema_manager
            .query_manager_mut()
            .subscribe_with_sync(query, session, settled_tier)
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
    /// Returns results once the local query graph settles.
    pub fn query(&mut self, query: Query, session: Option<Session>) -> QueryFuture {
        self.query_with_settled_tier(query, session, None)
    }

    /// Execute a one-shot query with optional settled tier.
    ///
    /// If `settled_tier` is provided, the query is held until the data has been
    /// confirmed at that persistence tier (via QuerySettled from the server).
    pub fn query_with_settled_tier(
        &mut self,
        query: Query,
        session: Option<Session>,
        settled_tier: Option<PersistenceTier>,
    ) -> QueryFuture {
        let (sender, receiver) = oneshot::channel();

        // Subscribe with sync - this triggers server to send matching data
        let sub_id = match self.schema_manager.query_manager_mut().subscribe_with_sync(
            query,
            session,
            settled_tier,
        ) {
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
            .insert_with_session(&mut self.io_handler, table, &values, session)
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
            .update_with_session(&mut self.io_handler, object_id, &current_values, session)
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
            .delete_with_session(&mut self.io_handler, object_id, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;
        self.immediate_tick();
        Ok(())
    }

    // =========================================================================
    // Persisted CRUD Operations
    // =========================================================================

    /// Insert a row and return a receiver that resolves when the requested
    /// persistence tier (or higher) acknowledges.
    pub fn insert_persisted(
        &mut self,
        table: &str,
        values: Vec<Value>,
        session: Option<&Session>,
        tier: PersistenceTier,
    ) -> Result<(ObjectId, oneshot::Receiver<()>), RuntimeError> {
        let result = self
            .schema_manager
            .insert_with_session(&mut self.io_handler, table, &values, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;

        let (sender, receiver) = oneshot::channel();
        self.ack_watchers
            .entry(result.row_commit_id)
            .or_default()
            .push((tier, sender));

        self.immediate_tick();
        Ok((result.row_id, receiver))
    }

    /// Update a row and return a receiver that resolves when the requested
    /// persistence tier (or higher) acknowledges.
    pub fn update_persisted(
        &mut self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        session: Option<&Session>,
        tier: PersistenceTier,
    ) -> Result<oneshot::Receiver<()>, RuntimeError> {
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

        let commit_id = self
            .schema_manager
            .query_manager_mut()
            .update_with_session(&mut self.io_handler, object_id, &current_values, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;

        let (sender, receiver) = oneshot::channel();
        self.ack_watchers
            .entry(commit_id)
            .or_default()
            .push((tier, sender));

        self.immediate_tick();
        Ok(receiver)
    }

    /// Delete a row and return a receiver that resolves when the requested
    /// persistence tier (or higher) acknowledges.
    pub fn delete_persisted(
        &mut self,
        object_id: ObjectId,
        session: Option<&Session>,
        tier: PersistenceTier,
    ) -> Result<oneshot::Receiver<()>, RuntimeError> {
        let handle = self
            .schema_manager
            .query_manager_mut()
            .delete_with_session(&mut self.io_handler, object_id, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;

        let (sender, receiver) = oneshot::channel();
        self.ack_watchers
            .entry(handle.delete_commit_id)
            .or_default()
            .push((tier, sender));

        self.immediate_tick();
        Ok(receiver)
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
    use crate::io_handler::MemoryIoHandler;
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

    fn create_test_runtime() -> RuntimeCore<MemoryIoHandler> {
        let schema = test_schema();
        let app_id = AppId::from_name("test-app");
        let sync_manager = SyncManager::new();
        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
        let handler = MemoryIoHandler::new();
        let mut core = RuntimeCore::new(schema_manager, handler);
        // MemoryIoHandler is synchronous - no cold start needed.
        // BTreeIndex starts with meta_loaded=true, so inserts work immediately.
        core.immediate_tick();
        core
    }

    /// Helper to execute a query synchronously via subscribe/tick/unsubscribe.
    fn execute_query(
        core: &mut RuntimeCore<MemoryIoHandler>,
        query: Query,
    ) -> Vec<(ObjectId, Vec<Value>)> {
        let sub_id = core
            .schema_manager_mut()
            .query_manager_mut()
            .subscribe(query)
            .unwrap();
        // Process via immediate_tick which calls schema_manager.process(&mut io_handler)
        core.immediate_tick();
        let results = core
            .schema_manager_mut()
            .query_manager_mut()
            .get_subscription_results(sub_id);
        core.schema_manager_mut()
            .query_manager_mut()
            .unsubscribe_with_sync(sub_id);
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

        // Tick to process
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

    // =========================================================================
    // Durability API Tests (3-tier: A ↔ B[Worker] ↔ C[EdgeServer])
    // =========================================================================

    use crate::sync_manager::{
        ClientId, Destination, InboxEntry, OutboxEntry, PersistenceTier, ServerId, Source,
    };

    /// Three-tier RuntimeCore setup for durability tests.
    struct ThreeTierRC {
        a: RuntimeCore<MemoryIoHandler>,
        b: RuntimeCore<MemoryIoHandler>,
        c: RuntimeCore<MemoryIoHandler>,
        a_client_of_b: ClientId,
        b_server_for_a: ServerId,
        b_client_of_c: ClientId,
        c_server_for_b: ServerId,
    }

    fn create_3tier_rc() -> ThreeTierRC {
        let schema = test_schema();
        let app_id = AppId::from_name("durability-test");

        // A = client (no tier)
        let sm_a = SyncManager::new();
        let mgr_a =
            SchemaManager::new(sm_a, schema.clone(), app_id.clone(), "dev", "main").unwrap();
        let mut a = RuntimeCore::new(mgr_a, MemoryIoHandler::new());

        // B = Worker server
        let sm_b = SyncManager::new().with_tier(PersistenceTier::Worker);
        let mgr_b =
            SchemaManager::new(sm_b, schema.clone(), app_id.clone(), "dev", "main").unwrap();
        let mut b = RuntimeCore::new(mgr_b, MemoryIoHandler::new());

        // C = EdgeServer
        let sm_c = SyncManager::new().with_tier(PersistenceTier::EdgeServer);
        let mgr_c = SchemaManager::new(sm_c, schema, app_id, "dev", "main").unwrap();
        let mut c = RuntimeCore::new(mgr_c, MemoryIoHandler::new());

        let a_client_of_b = ClientId::new();
        let b_server_for_a = ServerId::new();
        let b_client_of_c = ClientId::new();
        let c_server_for_b = ServerId::new();

        // Topology: A ↔ B ↔ C
        b.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .add_client_with_full_sync(a_client_of_b);
        a.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .add_server(b_server_for_a);

        c.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .add_client_with_full_sync(b_client_of_c);
        b.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .add_server(c_server_for_b);

        // Initial tick + clear initial sync messages
        a.immediate_tick();
        b.immediate_tick();
        c.immediate_tick();
        a.batched_tick();
        b.batched_tick();
        c.batched_tick();
        a.io_handler_mut().take_outbox();
        b.io_handler_mut().take_outbox();
        c.io_handler_mut().take_outbox();

        ThreeTierRC {
            a,
            b,
            c,
            a_client_of_b,
            b_server_for_a,
            b_client_of_c,
            c_server_for_b,
        }
    }

    /// Approve all pending updates on a RuntimeCore's SyncManager.
    /// Uses direct field access to avoid double mutable borrow issues.
    fn approve_all_pending_rc(core: &mut RuntimeCore<MemoryIoHandler>) {
        let pending_ids: Vec<_> = core
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .pending_update_ids();
        for id in pending_ids {
            core.schema_manager
                .query_manager_mut()
                .sync_manager_mut()
                .approve_update(&mut core.io_handler, id);
        }
    }

    /// Pump all messages between 3 RuntimeCore nodes until quiescent.
    fn pump_3tier(s: &mut ThreeTierRC) {
        for _ in 0..10 {
            let mut any_messages = false;

            // A outbox → B
            s.a.batched_tick();
            let a_out = s.a.io_handler_mut().take_outbox();
            for entry in a_out {
                if entry.destination == Destination::Server(s.b_server_for_a) {
                    any_messages = true;
                    s.b.park_sync_message(InboxEntry {
                        source: Source::Client(s.a_client_of_b),
                        payload: entry.payload,
                    });
                }
            }

            // B process + approve, then route outbox to A or C
            s.b.batched_tick();
            approve_all_pending_rc(&mut s.b);
            s.b.immediate_tick();
            s.b.batched_tick();
            let b_out = s.b.io_handler_mut().take_outbox();
            for entry in b_out {
                match &entry.destination {
                    Destination::Client(cid) if *cid == s.a_client_of_b => {
                        any_messages = true;
                        s.a.park_sync_message(InboxEntry {
                            source: Source::Server(s.b_server_for_a),
                            payload: entry.payload,
                        });
                    }
                    Destination::Server(sid) if *sid == s.c_server_for_b => {
                        any_messages = true;
                        s.c.park_sync_message(InboxEntry {
                            source: Source::Client(s.b_client_of_c),
                            payload: entry.payload,
                        });
                    }
                    _ => {}
                }
            }

            // C process + approve, then route outbox to B
            s.c.batched_tick();
            approve_all_pending_rc(&mut s.c);
            s.c.immediate_tick();
            s.c.batched_tick();
            let c_out = s.c.io_handler_mut().take_outbox();
            for entry in c_out {
                if entry.destination == Destination::Client(s.b_client_of_c) {
                    any_messages = true;
                    s.b.park_sync_message(InboxEntry {
                        source: Source::Server(s.c_server_for_b),
                        payload: entry.payload,
                    });
                }
            }

            // A processes incoming
            s.a.batched_tick();
            s.a.immediate_tick();

            if !any_messages {
                break;
            }
        }
    }

    /// Pump only A → B (one hop, no C).
    fn pump_a_to_b(s: &mut ThreeTierRC) {
        s.a.batched_tick();
        let a_out = s.a.io_handler_mut().take_outbox();
        for entry in a_out {
            if entry.destination == Destination::Server(s.b_server_for_a) {
                s.b.park_sync_message(InboxEntry {
                    source: Source::Client(s.a_client_of_b),
                    payload: entry.payload,
                });
            }
        }
        s.b.batched_tick();
        approve_all_pending_rc(&mut s.b);
        s.b.immediate_tick();
    }

    /// Route B's outbox to both A and C as appropriate.
    fn route_b_outbox(s: &mut ThreeTierRC) {
        s.b.batched_tick();
        let b_out = s.b.io_handler_mut().take_outbox();
        for entry in b_out {
            match &entry.destination {
                Destination::Client(cid) if *cid == s.a_client_of_b => {
                    s.a.park_sync_message(InboxEntry {
                        source: Source::Server(s.b_server_for_a),
                        payload: entry.payload,
                    });
                }
                Destination::Server(sid) if *sid == s.c_server_for_b => {
                    s.c.park_sync_message(InboxEntry {
                        source: Source::Client(s.b_client_of_c),
                        payload: entry.payload,
                    });
                }
                _ => {}
            }
        }
    }

    /// Pump B → A (acks back). Also routes B→C messages properly.
    fn pump_b_to_a(s: &mut ThreeTierRC) {
        route_b_outbox(s);
        s.a.batched_tick();
        s.a.immediate_tick();
    }

    /// Pump B → C (forward to edge). Also routes B→A messages properly.
    fn pump_b_to_c(s: &mut ThreeTierRC) {
        route_b_outbox(s);
        s.c.batched_tick();
        approve_all_pending_rc(&mut s.c);
        s.c.immediate_tick();
    }

    /// Pump C → B → A (edge ack relay).
    fn pump_c_to_b_to_a(s: &mut ThreeTierRC) {
        // C → B
        s.c.batched_tick();
        let c_out = s.c.io_handler_mut().take_outbox();
        for entry in c_out {
            if entry.destination == Destination::Client(s.b_client_of_c) {
                s.b.park_sync_message(InboxEntry {
                    source: Source::Server(s.c_server_for_b),
                    payload: entry.payload,
                });
            }
        }
        s.b.batched_tick();
        s.b.immediate_tick();

        // B → A
        pump_b_to_a(s);
    }

    // --- Test 1: insert returns immediately ---

    #[test]
    fn rc_insert_returns_immediately() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();
        assert!(!id.0.is_nil());

        // Query locally to verify row is present
        let query = Query::new("users");
        let results = execute_query(&mut s.a, query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, id);
    }

    // --- Test 2: insert data syncs to server ---

    #[test]
    fn rc_insert_data_syncs_to_server() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();

        // Pump A → B
        pump_a_to_b(&mut s);

        // Query on B
        let query = Query::new("users");
        let results = execute_query(&mut s.b, query);
        assert_eq!(results.len(), 1, "Server B should have the synced row");
        assert_eq!(results[0].0, id);
    }

    // --- Test 3: update syncs ---

    #[test]
    fn rc_update_sync() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();
        pump_a_to_b(&mut s);

        // Update on A
        s.a.update(id, vec![("name".into(), Value::Text("Bob".into()))], None)
            .unwrap();
        pump_a_to_b(&mut s);

        // Verify on B
        let query = Query::new("users");
        let results = execute_query(&mut s.b, query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[1], Value::Text("Bob".into()));
    }

    // --- Test 4: delete syncs ---

    #[test]
    fn rc_delete_sync() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();
        pump_a_to_b(&mut s);

        s.a.delete(id, None).unwrap();
        pump_a_to_b(&mut s);

        let query = Query::new("users");
        let results = execute_query(&mut s.b, query);
        assert_eq!(results.len(), 0, "Row should be deleted on B");
    }

    // --- Test 6: insert_persisted resolves on Worker ack ---

    #[test]
    fn rc_insert_persisted_resolves_on_worker_ack() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let (id, mut receiver) =
            s.a.insert_persisted("users", values, None, PersistenceTier::Worker)
                .unwrap();
        assert!(!id.0.is_nil());

        // Receiver should NOT be ready yet
        assert!(
            receiver.try_recv().is_err() || receiver.try_recv() == Ok(None),
            "Receiver should not be resolved before ack"
        );

        // Pump A → B (B persists and emits Worker ack) → A
        pump_a_to_b(&mut s);
        pump_b_to_a(&mut s);

        // Now receiver should be resolved
        // We can't use try_recv after the first check since oneshot is consumed,
        // so let's restructure: use a fresh test flow
        // Actually, oneshot::Receiver::try_recv() returns Ok(None) if not ready, Ok(Some(T)) if ready
        // Let's just check the final state
        match receiver.try_recv() {
            Ok(Some(())) => {} // Success
            Ok(None) => panic!("Receiver should be resolved after Worker ack"),
            Err(_) => panic!("Receiver was cancelled"),
        }
    }

    // --- Test 7: insert_persisted holds until correct tier ---

    #[test]
    fn rc_insert_persisted_holds_until_correct_tier() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let (_id, mut receiver) =
            s.a.insert_persisted("users", values, None, PersistenceTier::EdgeServer)
                .unwrap();

        // Pump A → B (Worker ack) → A
        pump_a_to_b(&mut s);
        pump_b_to_a(&mut s);

        // Worker ack should NOT satisfy EdgeServer request
        assert_eq!(
            receiver.try_recv(),
            Ok(None),
            "Worker ack should not satisfy EdgeServer request"
        );

        // Pump B → C (forward to edge) → C acks → back through B → A
        pump_b_to_c(&mut s);
        pump_c_to_b_to_a(&mut s);

        // Now EdgeServer ack should satisfy
        match receiver.try_recv() {
            Ok(Some(())) => {} // Success
            Ok(None) => panic!("Receiver should be resolved after EdgeServer ack"),
            Err(_) => panic!("Receiver was cancelled"),
        }
    }

    // --- Test 8: higher tier satisfies lower ---

    #[test]
    fn rc_insert_persisted_higher_tier_satisfies_lower() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let (_id, mut receiver) =
            s.a.insert_persisted("users", values, None, PersistenceTier::Worker)
                .unwrap();

        // Pump all the way to EdgeServer and back
        pump_3tier(&mut s);

        // EdgeServer ack >= Worker, should resolve
        match receiver.try_recv() {
            Ok(Some(())) => {}
            Ok(None) => panic!("EdgeServer ack should satisfy Worker request"),
            Err(_) => panic!("Receiver was cancelled"),
        }
    }

    // --- Test 9: update_persisted resolves on ack ---

    #[test]
    fn rc_update_persisted_resolves_on_ack() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();
        pump_a_to_b(&mut s);

        let mut receiver =
            s.a.update_persisted(
                id,
                vec![("name".into(), Value::Text("Bob".into()))],
                None,
                PersistenceTier::Worker,
            )
            .unwrap();

        pump_a_to_b(&mut s);
        pump_b_to_a(&mut s);

        match receiver.try_recv() {
            Ok(Some(())) => {}
            Ok(None) => panic!("Update receiver should be resolved after Worker ack"),
            Err(_) => panic!("Receiver was cancelled"),
        }

        // Verify updated values on B
        let query = Query::new("users");
        let results = execute_query(&mut s.b, query);
        assert_eq!(results[0].1[1], Value::Text("Bob".into()));
    }

    // --- Test 10: delete_persisted resolves on ack ---

    #[test]
    fn rc_delete_persisted_resolves_on_ack() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();
        pump_a_to_b(&mut s);

        let mut receiver =
            s.a.delete_persisted(id, None, PersistenceTier::Worker)
                .unwrap();

        pump_a_to_b(&mut s);
        pump_b_to_a(&mut s);

        match receiver.try_recv() {
            Ok(Some(())) => {}
            Ok(None) => panic!("Delete receiver should be resolved after Worker ack"),
            Err(_) => panic!("Receiver was cancelled"),
        }

        // Verify deleted on B
        let query = Query::new("users");
        let results = execute_query(&mut s.b, query);
        assert_eq!(results.len(), 0);
    }

    // --- Test 11: multiple persisted inserts are independent ---

    #[test]
    fn rc_multiple_persisted_inserts_independent() {
        let mut s = create_3tier_rc();

        let values1 = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let (_id1, mut receiver1) =
            s.a.insert_persisted("users", values1, None, PersistenceTier::Worker)
                .unwrap();

        let values2 = vec![Value::Uuid(ObjectId::new()), Value::Text("Bob".into())];
        let (_id2, mut receiver2) =
            s.a.insert_persisted("users", values2, None, PersistenceTier::Worker)
                .unwrap();

        // Pump everything through
        pump_3tier(&mut s);

        // Both should resolve
        match receiver1.try_recv() {
            Ok(Some(())) => {}
            Ok(None) => panic!("receiver1 should be resolved"),
            Err(_) => panic!("receiver1 cancelled"),
        }
        match receiver2.try_recv() {
            Ok(Some(())) => {}
            Ok(None) => panic!("receiver2 should be resolved"),
            Err(_) => panic!("receiver2 cancelled"),
        }
    }

    // --- Test 12: query with no settled tier resolves immediately ---

    #[test]
    fn rc_query_no_settled_tier_immediate() {
        let mut s = create_3tier_rc();

        // Insert data first
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();

        // query_with_settled_tier(None) should resolve during its own immediate_tick
        let mut future = s.a.query_with_settled_tier(Query::new("users"), None, None);

        // Poll the future — it should be Ready immediately
        let waker = noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        match Pin::new(&mut future).poll(&mut cx) {
            Poll::Ready(Ok(results)) => {
                assert_eq!(results.len(), 1, "Should have one row");
                assert_eq!(results[0].0, id);
            }
            Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
            Poll::Pending => panic!("Query with settled_tier=None should resolve immediately"),
        }
    }

    // --- Test 13: query with settled_tier holds until tier confirms ---

    #[test]
    fn rc_query_settled_tier_holds() {
        let mut s = create_3tier_rc();

        // Insert data locally
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();

        // Query with settled_tier=Worker — should NOT resolve yet
        let mut future =
            s.a.query_with_settled_tier(Query::new("users"), None, Some(PersistenceTier::Worker));

        let waker = noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(
            Pin::new(&mut future).poll(&mut cx).is_pending(),
            "Query should be pending before Worker settlement"
        );

        // Pump sync: A → B (subscription flows to server, B settles, sends QuerySettled)
        pump_a_to_b(&mut s);
        // B should have settled and produced QuerySettled — pump back to A
        pump_b_to_a(&mut s);

        // Now the query should resolve
        match Pin::new(&mut future).poll(&mut cx) {
            Poll::Ready(Ok(results)) => {
                assert_eq!(results.len(), 1, "Should have one row after settlement");
                assert_eq!(results[0].0, id);
            }
            Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
            Poll::Pending => panic!("Query should resolve after Worker QuerySettled"),
        }
    }

    // --- Test 14: subscribe with settled_tier holds callback ---

    #[test]
    fn rc_subscribe_settled_tier() {
        let mut s = create_3tier_rc();

        let received = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
        let received_clone = received.clone();

        // Subscribe with settled_tier=Worker
        let _handle =
            s.a.subscribe_with_settled_tier(
                Query::new("users"),
                move |delta| {
                    let rows: Vec<(ObjectId, Vec<Value>)> = delta
                        .delta
                        .added
                        .iter()
                        .filter_map(|row| {
                            decode_row(&delta.descriptor, &row.data)
                                .ok()
                                .map(|vals| (row.id, vals))
                        })
                        .collect();
                    received_clone.lock().unwrap().push(rows);
                },
                None,
                Some(PersistenceTier::Worker),
            )
            .unwrap();

        // Insert data on A
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();
        s.a.immediate_tick();

        // Callback should NOT have fired yet (tier not satisfied)
        assert!(
            received.lock().unwrap().is_empty(),
            "Callback should not fire before Worker settlement"
        );

        // Pump sync: subscription + data to B, B settles, QuerySettled back
        pump_a_to_b(&mut s);
        pump_b_to_a(&mut s);

        // Callback should now fire with accumulated data
        let calls = received.lock().unwrap();
        assert!(
            !calls.is_empty(),
            "Callback should fire after Worker QuerySettled"
        );
        // First delivery should contain the full accumulated state
        let first_delivery = &calls[0];
        assert_eq!(first_delivery.len(), 1, "Should have one row");
        assert_eq!(first_delivery[0].0, id);
    }

    /// Helper to create a no-op waker for polling futures in tests.
    fn noop_waker() -> std::task::Waker {
        fn noop(_: *const ()) {}
        fn clone(_: *const ()) -> std::task::RawWaker {
            std::task::RawWaker::new(std::ptr::null(), &VTABLE)
        }
        static VTABLE: std::task::RawWakerVTable =
            std::task::RawWakerVTable::new(clone, noop, noop, noop);
        unsafe { std::task::Waker::from_raw(std::task::RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    /// Sync edit fires callback synchronously (when index IS ready).
    /// This documents the invariant that inserts are visible immediately
    /// with a synchronous IoHandler.
    #[test]
    fn test_sync_edit_fires_callback_synchronously() {
        let mut core = create_test_runtime();

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
        let values = vec![
            Value::Uuid(ObjectId::new()),
            Value::Text("test@test.com".to_string()),
        ];
        let _ = core.insert("users", values, None);
        core.immediate_tick();

        let final_count = *callback_count.lock().unwrap();
        assert!(
            final_count > initial_count,
            "Callback must fire synchronously after insert when index ready"
        );
    }
}
