//! RuntimeCore - Unified synchronous runtime logic for both native and WASM.
//!
//! This module provides the shared core logic that both groove-tokio
//! and groove-wasm wrap. RuntimeCore is generic over `Storage`, `Scheduler`,
//! and `SyncSender` which provide platform-specific behavior.
//!
//! ## Design
//!
//! - `immediate_tick()` - processes managers synchronously, schedules batched_tick if needed
//! - `batched_tick()` - sends sync messages, applies parked responses/messages, calls immediate_tick
//! - Queries return `QueryFuture` for cross-platform awaiting
//! - Sync messages are "parked" and processed in batched_tick
//!
//! ## Usage
//!
//! ```ignore
//! let runtime = RuntimeCore::new(schema_manager, storage, scheduler, sync_sender);
//! runtime.insert("users", values)?;
//! runtime.immediate_tick();
//! let future = runtime.query(query);
//! let results = future.await?;
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::oneshot;
use tracing::{debug, debug_span, info, trace, trace_span};

use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::query_manager::QuerySubscriptionId;
use crate::query_manager::encoding::decode_row;
use crate::query_manager::manager::{QueryError, QueryUpdate};
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::query_manager::types::{RowDelta, Schema, TableName, Value};
use crate::schema_manager::SchemaManager;
use crate::storage::Storage;
use crate::sync_manager::{ClientId, InboxEntry, OutboxEntry, PersistenceTier, ServerId};

// ============================================================================
// Scheduler and SyncSender traits
// ============================================================================

/// Schedules batched ticks on the platform's event loop.
///
/// No `Send` bound — WASM types (`Rc`, `Function`) are `!Send`.
/// Tokio enforces `Send` at the point of use (`Arc<Mutex<...>>`).
pub trait Scheduler {
    fn schedule_batched_tick(&self);
}

/// Sends sync messages to the network.
///
/// No `Send` bound — WASM types are `!Send`. Send is enforced
/// by the concrete wrapping type where needed.
pub trait SyncSender {
    fn send_sync_message(&self, message: OutboxEntry);
}

// ============================================================================
// Test helpers
// ============================================================================

/// No-op scheduler for tests — tests call tick explicitly.
pub struct NoopScheduler;

impl Scheduler for NoopScheduler {
    fn schedule_batched_tick(&self) {}
}

/// Collects sync messages for test inspection.
pub struct VecSyncSender {
    messages: std::sync::Mutex<Vec<OutboxEntry>>,
}

impl Default for VecSyncSender {
    fn default() -> Self {
        Self {
            messages: std::sync::Mutex::new(Vec::new()),
        }
    }
}

impl VecSyncSender {
    pub fn new() -> Self {
        Self::default()
    }

    /// Take all collected messages.
    pub fn take(&self) -> Vec<OutboxEntry> {
        std::mem::take(&mut self.messages.lock().unwrap())
    }
}

impl SyncSender for VecSyncSender {
    fn send_sync_message(&self, message: OutboxEntry) {
        self.messages.lock().unwrap().push(message);
    }
}

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
/// Generic over `Storage` for data persistence, `Scheduler` for tick scheduling,
/// and `SyncSender` for network message dispatch.
/// All business logic is synchronous.
pub struct RuntimeCore<S: Storage, Sch: Scheduler, Sy: SyncSender> {
    schema_manager: SchemaManager,
    pub(crate) storage: S,
    scheduler: Sch,
    sync_sender: Sy,

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

    /// Label for tracing (e.g. "worker", "edge", "client").
    tier_label: &'static str,
}

impl<S: Storage, Sch: Scheduler, Sy: SyncSender> RuntimeCore<S, Sch, Sy> {
    /// Create a new RuntimeCore.
    pub fn new(schema_manager: SchemaManager, storage: S, scheduler: Sch, sync_sender: Sy) -> Self {
        Self {
            schema_manager,
            storage,
            scheduler,
            sync_sender,
            parked_sync_messages: Vec::new(),
            subscriptions: HashMap::new(),
            subscription_reverse: HashMap::new(),
            next_subscription_handle: 0,
            pending_one_shot_queries: HashMap::new(),
            ack_watchers: HashMap::new(),
            tier_label: "unknown",
        }
    }

    /// Set the tier label used in tracing spans.
    pub fn set_tier_label(&mut self, label: &'static str) {
        self.tier_label = label;
    }

    /// Get mutable reference to the Storage.
    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }

    /// Get reference to the Storage.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Flush the storage to persistent medium.
    pub fn flush_storage(&self) {
        self.storage.flush();
    }

    /// Flush only the WAL buffer (not the full snapshot).
    pub fn flush_wal(&self) {
        self.storage.flush_wal();
    }

    /// Consume RuntimeCore and return the Storage.
    /// Used for cold-start testing to transfer driver state.
    pub fn into_storage(self) -> S {
        self.storage
    }

    /// Get reference to the SyncSender.
    pub fn sync_sender(&self) -> &Sy {
        &self.sync_sender
    }

    /// Get reference to the Scheduler.
    pub fn scheduler(&self) -> &Sch {
        &self.scheduler
    }

    /// Get mutable reference to the Scheduler.
    pub fn scheduler_mut(&mut self) -> &mut Sch {
        &mut self.scheduler
    }

    /// Persist the current schema to the catalogue for server sync.
    pub fn persist_schema(&mut self) -> ObjectId {
        let id = self.schema_manager.persist_schema(&mut self.storage);
        info!(object_id = %id, "persisted schema to catalogue");
        id
    }

    // =========================================================================
    // Tick Methods
    // =========================================================================

    /// Synchronous tick - processes managers, fulfills completed queries.
    ///
    /// Schedules batched_tick if there are outbound messages.
    ///
    /// Call this after any mutation operation (insert, update, delete, etc.)
    /// to process the change and schedule any required I/O.
    pub fn immediate_tick(&mut self) -> TickOutput {
        let _span = trace_span!("immediate_tick", tier = self.tier_label).entered();

        // 1. Process logical updates (sync, subscriptions)
        self.schema_manager.process(&mut self.storage);

        // 2. Second process() handles deferred query subscriptions that couldn't
        //    compile on first pass (schema wasn't available yet, e.g. catalogue
        //    was just processed and made the schema available).
        self.schema_manager.process(&mut self.storage);

        // 3. Collect subscription updates
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
            self.scheduler.schedule_batched_tick();
        }

        TickOutput {
            subscription_updates,
        }
    }

    /// Batched tick - handles all I/O, then processes parked messages.
    ///
    /// Called by the platform when the scheduled tick fires. This:
    /// 1. Sends all outgoing sync messages via SyncSender
    /// 2. Processes parked sync messages
    ///
    /// Each step is followed by an immediate_tick to process results.
    pub fn batched_tick(&mut self) {
        let _span = debug_span!("batched_tick", tier = self.tier_label).entered();

        // 1. Send all outgoing sync messages
        let outbox = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        if !outbox.is_empty() {
            debug!(count = outbox.len(), "flushing outbox");
        }
        for msg in outbox {
            self.sync_sender.send_sync_message(msg);
        }

        // 2. Process parked sync messages
        self.handle_sync_messages();

        // 3. Flush any new outbox entries generated by processing.
        // The scheduler's debounce prevents immediate_tick() from scheduling
        // another batched_tick while we're inside one, so we must flush here.
        let outbox = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        if !outbox.is_empty() {
            debug!(count = outbox.len(), "flushing post-process outbox");
        }
        for msg in outbox {
            self.sync_sender.send_sync_message(msg);
        }

        // Flush WAL so writes survive a hard kill (tab close, crash).
        // This is cheap (append-only buffer → OPFS) vs snapshot which rewrites everything.
        self.storage.flush_wal();
    }

    /// Apply parked sync messages and tick.
    fn handle_sync_messages(&mut self) {
        let messages = std::mem::take(&mut self.parked_sync_messages);
        let had_messages = !messages.is_empty();
        if had_messages {
            debug!(count = messages.len(), "processing parked sync messages");
        }
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
    pub fn park_sync_message(&mut self, message: InboxEntry) {
        trace!(source = ?message.source, payload = message.payload.variant_name(), "parking sync message");
        self.parked_sync_messages.push(message);
        self.scheduler.schedule_batched_tick();
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    /// Subscribe to a query with a callback.
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

    /// Subscribe with optional settled tier.
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
    fn subscribe_impl(
        &mut self,
        query: Query,
        callback: SubscriptionCallback,
        session: Option<Session>,
        settled_tier: Option<PersistenceTier>,
    ) -> Result<SubscriptionHandle, RuntimeError> {
        let _span = debug_span!("subscribe", table = query.table.as_str()).entered();
        let query_sub_id = self
            .schema_manager
            .query_manager_mut()
            .subscribe_with_sync(query, session, settled_tier)
            .map_err(|e| RuntimeError::QueryError(format!("{:?}", e)))?;

        let handle = SubscriptionHandle(self.next_subscription_handle);
        self.next_subscription_handle += 1;
        debug!(handle = handle.0, sub_id = query_sub_id.0, "subscribed");

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

    /// Subscribe with explicit schema context (for server use).
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

    /// Execute a one-shot query, optionally waiting for a settled tier.
    pub fn query(
        &mut self,
        query: Query,
        session: Option<Session>,
        settled_tier: Option<PersistenceTier>,
    ) -> QueryFuture {
        let _span = debug_span!("query", table = query.table.as_str(), ?settled_tier).entered();
        let (sender, receiver) = oneshot::channel();

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
        let _span = debug_span!("insert", table).entered();
        let result = self
            .schema_manager
            .insert_with_session(&mut self.storage, table, &values, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;
        debug!(object_id = %result.row_id, "inserted");
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
        let _span = debug_span!("update", %object_id).entered();
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
            .update_with_session(&mut self.storage, object_id, &current_values, session)
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
        let _span = debug_span!("delete", %object_id).entered();
        self.schema_manager
            .query_manager_mut()
            .delete_with_session(&mut self.storage, object_id, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;
        debug!("deleted");
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
            .insert_with_session(&mut self.storage, table, &values, session)
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
            .update_with_session(&mut self.storage, object_id, &current_values, session)
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
            .delete_with_session(&mut self.storage, object_id, session)
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
        info!(%server_id, "adding server");
        self.schema_manager
            .query_manager_mut()
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
        info!(%client_id, has_session = session.is_some(), "adding client");
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
        sm.add_client(client_id);
        if let Some(s) = session {
            sm.set_client_session(client_id, s);
        }
        self.immediate_tick();
    }

    /// Ensure a client exists with the given session.
    ///
    /// If the client already exists, updates the session. This is idempotent —
    /// calling with the same session is a no-op. Calling with a new session
    /// updates it in place without resetting the client's role or other state.
    ///
    /// A session is always required — callers must authenticate before
    /// registering a client.
    pub fn ensure_client_with_session(&mut self, client_id: ClientId, session: Session) {
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
        if sm.get_client(client_id).is_some() {
            sm.set_client_session(client_id, session);
        } else {
            sm.add_client(client_id);
            sm.set_client_session(client_id, session);
            self.immediate_tick();
        }
    }

    /// Remove a client connection.
    pub fn remove_client(&mut self, client_id: ClientId) {
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .remove_client(client_id);
    }

    /// Promote a client to Admin role (full access, no ReBAC).
    pub fn set_client_admin(&mut self, client_id: ClientId) {
        use crate::sync_manager::ClientRole;
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(client_id, ClientRole::Admin);
    }

    /// Set a client's role.
    pub fn set_client_role_by_name(
        &mut self,
        client_id: ClientId,
        role: crate::sync_manager::ClientRole,
    ) {
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(client_id, role);
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
    use crate::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};
    use crate::schema_manager::AppId;
    use crate::storage::MemoryStorage;
    use crate::sync_manager::SyncManager;
    use std::sync::{Arc, Mutex};

    type TestCore = RuntimeCore<MemoryStorage, NoopScheduler, VecSyncSender>;

    fn test_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build()
    }

    fn create_test_runtime() -> TestCore {
        let schema = test_schema();
        let app_id = AppId::from_name("test-app");
        let sync_manager = SyncManager::new();
        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
        let mut core = RuntimeCore::new(
            schema_manager,
            MemoryStorage::new(),
            NoopScheduler,
            VecSyncSender::new(),
        );
        core.immediate_tick();
        core
    }

    /// Helper to execute a query synchronously via subscribe/tick/unsubscribe.
    fn execute_query(core: &mut TestCore, query: Query) -> Vec<(ObjectId, Vec<Value>)> {
        let sub_id = core
            .schema_manager_mut()
            .query_manager_mut()
            .subscribe(query)
            .unwrap();
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

        let values = vec![
            Value::Uuid(ObjectId::new()),
            Value::Text("Alice".to_string()),
        ];
        let object_id = core.insert("users", values.clone(), None).unwrap();
        assert!(!object_id.0.is_nil());

        core.immediate_tick();
        core.batched_tick();

        let query = Query::new("users");
        let results = execute_query(&mut core, query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, object_id);
    }

    #[test]
    fn test_runtime_core_subscription() {
        let mut core = create_test_runtime();

        let updates: Arc<Mutex<Vec<SubscriptionDelta>>> = Arc::new(Mutex::new(Vec::new()));
        let updates_clone = updates.clone();

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

        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Bob".to_string())];
        let _object_id = core.insert("users", values, None).unwrap();

        core.immediate_tick();
        core.batched_tick();

        let updates_vec = updates.lock().unwrap();
        assert!(
            !updates_vec.is_empty(),
            "Should receive subscription update"
        );
        assert_eq!(updates_vec[0].handle, handle);

        drop(updates_vec);
        core.unsubscribe(handle);
    }

    #[test]
    fn test_runtime_core_update_delete() {
        let mut core = create_test_runtime();

        let id = ObjectId::new();
        let values = vec![Value::Uuid(id), Value::Text("Charlie".to_string())];
        let object_id = core.insert("users", values, None).unwrap();
        core.immediate_tick();
        core.batched_tick();

        let updates = vec![("name".to_string(), Value::Text("Dave".to_string()))];
        core.update(object_id, updates, None).unwrap();
        core.immediate_tick();
        core.batched_tick();

        let query = Query::new("users");
        let results = execute_query(&mut core, query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[1], Value::Text("Dave".to_string()));

        core.delete(object_id, None).unwrap();
        core.immediate_tick();
        core.batched_tick();

        let query = Query::new("users");
        let results = execute_query(&mut core, query);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_park_sync_message() {
        use crate::object::BranchName;
        use crate::sync_manager::{Source, SyncPayload};

        let mut core = create_test_runtime();

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

        assert_eq!(core.parked_sync_messages.len(), 1);
    }

    // =========================================================================
    // Durability API Tests (3-tier: A ↔ B[Worker] ↔ C[EdgeServer])
    // =========================================================================

    use crate::sync_manager::{
        ClientId, ClientRole, Destination, InboxEntry, OutboxEntry, PersistenceTier, ServerId,
        Source, SyncPayload,
    };

    /// Three-tier RuntimeCore setup for durability tests.
    struct ThreeTierRC {
        a: TestCore,
        b: TestCore,
        c: TestCore,
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
        let mut a = RuntimeCore::new(
            mgr_a,
            MemoryStorage::new(),
            NoopScheduler,
            VecSyncSender::new(),
        );

        // B = Worker server
        let sm_b = SyncManager::new().with_tier(PersistenceTier::Worker);
        let mgr_b =
            SchemaManager::new(sm_b, schema.clone(), app_id.clone(), "dev", "main").unwrap();
        let mut b = RuntimeCore::new(
            mgr_b,
            MemoryStorage::new(),
            NoopScheduler,
            VecSyncSender::new(),
        );

        // C = EdgeServer
        let sm_c = SyncManager::new().with_tier(PersistenceTier::EdgeServer);
        let mgr_c = SchemaManager::new(sm_c, schema, app_id, "dev", "main").unwrap();
        let mut c = RuntimeCore::new(
            mgr_c,
            MemoryStorage::new(),
            NoopScheduler,
            VecSyncSender::new(),
        );

        let a_client_of_b = ClientId::new();
        let b_server_for_a = ServerId::new();
        let b_client_of_c = ClientId::new();
        let c_server_for_b = ServerId::new();

        // Topology: A ↔ B ↔ C
        {
            let sm = b
                .schema_manager_mut()
                .query_manager_mut()
                .sync_manager_mut();
            sm.add_client(a_client_of_b);
            sm.set_client_role(a_client_of_b, ClientRole::Peer);
        }
        a.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .add_server(b_server_for_a);

        {
            let sm = c
                .schema_manager_mut()
                .query_manager_mut()
                .sync_manager_mut();
            sm.add_client(b_client_of_c);
            sm.set_client_role(b_client_of_c, ClientRole::Peer);
        }
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
        a.sync_sender().take();
        b.sync_sender().take();
        c.sync_sender().take();

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

    /// Pump all messages between 3 RuntimeCore nodes until quiescent.
    fn pump_3tier(s: &mut ThreeTierRC) {
        for _ in 0..10 {
            let mut any_messages = false;

            // A outbox → B
            s.a.batched_tick();
            let a_out = s.a.sync_sender().take();
            for entry in a_out {
                if entry.destination == Destination::Server(s.b_server_for_a) {
                    any_messages = true;
                    s.b.park_sync_message(InboxEntry {
                        source: Source::Client(s.a_client_of_b),
                        payload: entry.payload,
                    });
                }
            }

            // B process, then route outbox to A or C
            s.b.batched_tick();
            s.b.immediate_tick();
            s.b.batched_tick();
            let b_out = s.b.sync_sender().take();
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

            // C process, then route outbox to B
            s.c.batched_tick();
            s.c.immediate_tick();
            s.c.batched_tick();
            let c_out = s.c.sync_sender().take();
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
        let a_out = s.a.sync_sender().take();
        for entry in a_out {
            if entry.destination == Destination::Server(s.b_server_for_a) {
                s.b.park_sync_message(InboxEntry {
                    source: Source::Client(s.a_client_of_b),
                    payload: entry.payload,
                });
            }
        }
        s.b.batched_tick();
        s.b.immediate_tick();
    }

    /// Route B's outbox to both A and C as appropriate.
    fn route_b_outbox(s: &mut ThreeTierRC) {
        s.b.batched_tick();
        let b_out = s.b.sync_sender().take();
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

    /// Pump B → A (acks back).
    fn pump_b_to_a(s: &mut ThreeTierRC) {
        route_b_outbox(s);
        s.a.batched_tick();
        s.a.immediate_tick();
    }

    /// Pump B → C (forward to edge).
    fn pump_b_to_c(s: &mut ThreeTierRC) {
        route_b_outbox(s);
        s.c.batched_tick();
        s.c.immediate_tick();
    }

    /// Pump C → B → A (edge ack relay).
    fn pump_c_to_b_to_a(s: &mut ThreeTierRC) {
        // C → B
        s.c.batched_tick();
        let c_out = s.c.sync_sender().take();
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

    fn count_query_subscriptions_to_server(entries: &[OutboxEntry], server_id: ServerId) -> usize {
        entries
            .iter()
            .filter(|entry| {
                matches!(
                    &entry.destination,
                    Destination::Server(dest_server_id) if *dest_server_id == server_id
                ) && matches!(&entry.payload, SyncPayload::QuerySubscription { .. })
            })
            .count()
    }

    #[test]
    fn rc_replays_downstream_query_when_upstream_added_late() {
        // Build A <-> B first (no B <-> C yet), so B processes a downstream
        // query subscription before it has any upstream server.
        let schema = test_schema();
        let app_id = AppId::from_name("query-replay-test");

        let mgr_a = SchemaManager::new(
            SyncManager::new(),
            schema.clone(),
            app_id.clone(),
            "dev",
            "main",
        )
        .unwrap();
        let mut a = RuntimeCore::new(
            mgr_a,
            MemoryStorage::new(),
            NoopScheduler,
            VecSyncSender::new(),
        );

        let mgr_b = SchemaManager::new(
            SyncManager::new().with_tier(PersistenceTier::Worker),
            schema.clone(),
            app_id.clone(),
            "dev",
            "main",
        )
        .unwrap();
        let mut b = RuntimeCore::new(
            mgr_b,
            MemoryStorage::new(),
            NoopScheduler,
            VecSyncSender::new(),
        );

        let mgr_c = SchemaManager::new(
            SyncManager::new().with_tier(PersistenceTier::EdgeServer),
            schema,
            app_id,
            "dev",
            "main",
        )
        .unwrap();
        let mut c = RuntimeCore::new(
            mgr_c,
            MemoryStorage::new(),
            NoopScheduler,
            VecSyncSender::new(),
        );

        let a_client_of_b = ClientId::new();
        let b_server_for_a = ServerId::new();
        let b_client_of_c = ClientId::new();
        let c_server_for_b = ServerId::new();

        {
            let sm = b
                .schema_manager_mut()
                .query_manager_mut()
                .sync_manager_mut();
            sm.add_client(a_client_of_b);
            sm.set_client_role(a_client_of_b, ClientRole::Peer);
        }
        a.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .add_server(b_server_for_a);

        // Clear any startup sync traffic.
        a.immediate_tick();
        b.immediate_tick();
        c.immediate_tick();
        a.batched_tick();
        b.batched_tick();
        c.batched_tick();
        a.sync_sender().take();
        b.sync_sender().take();
        c.sync_sender().take();

        // Downstream client A subscribes before B has an upstream.
        let _handle = a.subscribe(Query::new("users"), |_delta| {}, None).unwrap();

        // Deliver only A -> B messages.
        a.batched_tick();
        for entry in a.sync_sender().take() {
            if entry.destination == Destination::Server(b_server_for_a) {
                b.park_sync_message(InboxEntry {
                    source: Source::Client(a_client_of_b),
                    payload: entry.payload,
                });
            }
        }
        b.batched_tick();
        b.immediate_tick();
        b.batched_tick();
        b.sync_sender().take();

        // Bring up B <-> C after B already has active downstream query state.
        {
            let sm = c
                .schema_manager_mut()
                .query_manager_mut()
                .sync_manager_mut();
            sm.add_client(b_client_of_c);
            sm.set_client_role(b_client_of_c, ClientRole::Peer);
        }
        b.add_server(c_server_for_b);
        b.batched_tick();

        let forwarded_query_subscriptions = b
            .sync_sender()
            .take()
            .into_iter()
            .filter(|entry| {
                matches!(
                    &entry.destination,
                    Destination::Server(server_id) if *server_id == c_server_for_b
                ) && matches!(&entry.payload, SyncPayload::QuerySubscription { .. })
            })
            .count();

        assert!(
            forwarded_query_subscriptions > 0,
            "Expected B to replay existing downstream QuerySubscription(s) when adding upstream"
        );
    }

    #[test]
    fn rc_replays_active_queries_on_upstream_reconnect() {
        let mut s = create_3tier_rc();

        let _handle =
            s.a.subscribe(Query::new("users"), |_delta| {}, None)
                .unwrap();
        pump_a_to_b(&mut s);

        let initial_forwarded = s.b.sync_sender().take();
        assert!(
            count_query_subscriptions_to_server(&initial_forwarded, s.c_server_for_b) > 0,
            "Expected initial QuerySubscription forwarding from B to C"
        );

        // Simulate upstream disconnect/reconnect.
        s.b.remove_server(s.c_server_for_b);
        s.b.add_server(s.c_server_for_b);
        s.b.batched_tick();

        let replayed_forwarded = s.b.sync_sender().take();
        assert!(
            count_query_subscriptions_to_server(&replayed_forwarded, s.c_server_for_b) > 0,
            "Expected active QuerySubscription replay after upstream reconnect"
        );
    }

    #[test]
    fn rc_does_not_replay_unsubscribed_queries_on_upstream_reconnect() {
        let mut s = create_3tier_rc();

        let handle =
            s.a.subscribe(Query::new("users"), |_delta| {}, None)
                .unwrap();
        pump_a_to_b(&mut s);

        let initial_forwarded = s.b.sync_sender().take();
        assert!(
            count_query_subscriptions_to_server(&initial_forwarded, s.c_server_for_b) > 0,
            "Expected initial QuerySubscription forwarding from B to C"
        );

        s.a.unsubscribe(handle);
        pump_a_to_b(&mut s);
        s.b.sync_sender().take(); // Drain unsubscription forwarding and unrelated traffic.

        // Reconnect upstream and ensure replay no longer includes this query.
        s.b.remove_server(s.c_server_for_b);
        s.b.add_server(s.c_server_for_b);
        s.b.batched_tick();

        let replayed_forwarded = s.b.sync_sender().take();
        assert_eq!(
            count_query_subscriptions_to_server(&replayed_forwarded, s.c_server_for_b),
            0,
            "Unsubscribed query must not be replayed after upstream reconnect"
        );
    }

    #[test]
    fn rc_insert_returns_immediately() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();
        assert!(!id.0.is_nil());

        let query = Query::new("users");
        let results = execute_query(&mut s.a, query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, id);
    }

    #[test]
    fn rc_insert_data_syncs_to_server() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();

        pump_a_to_b(&mut s);

        let query = Query::new("users");
        let results = execute_query(&mut s.b, query);
        assert_eq!(results.len(), 1, "Server B should have the synced row");
        assert_eq!(results[0].0, id);
    }

    #[test]
    fn rc_update_sync() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();
        pump_a_to_b(&mut s);

        s.a.update(id, vec![("name".into(), Value::Text("Bob".into()))], None)
            .unwrap();
        pump_a_to_b(&mut s);

        let query = Query::new("users");
        let results = execute_query(&mut s.b, query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[1], Value::Text("Bob".into()));
    }

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

    #[test]
    fn rc_insert_persisted_resolves_on_worker_ack() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let (id, mut receiver) =
            s.a.insert_persisted("users", values, None, PersistenceTier::Worker)
                .unwrap();
        assert!(!id.0.is_nil());

        assert!(
            receiver.try_recv().is_err() || receiver.try_recv() == Ok(None),
            "Receiver should not be resolved before ack"
        );

        pump_a_to_b(&mut s);
        pump_b_to_a(&mut s);

        match receiver.try_recv() {
            Ok(Some(())) => {}
            Ok(None) => panic!("Receiver should be resolved after Worker ack"),
            Err(_) => panic!("Receiver was cancelled"),
        }
    }

    #[test]
    fn rc_insert_persisted_holds_until_correct_tier() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let (_id, mut receiver) =
            s.a.insert_persisted("users", values, None, PersistenceTier::EdgeServer)
                .unwrap();

        pump_a_to_b(&mut s);
        pump_b_to_a(&mut s);

        assert_eq!(
            receiver.try_recv(),
            Ok(None),
            "Worker ack should not satisfy EdgeServer request"
        );

        pump_b_to_c(&mut s);
        pump_c_to_b_to_a(&mut s);

        match receiver.try_recv() {
            Ok(Some(())) => {}
            Ok(None) => panic!("Receiver should be resolved after EdgeServer ack"),
            Err(_) => panic!("Receiver was cancelled"),
        }
    }

    #[test]
    fn rc_insert_persisted_higher_tier_satisfies_lower() {
        let mut s = create_3tier_rc();
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let (_id, mut receiver) =
            s.a.insert_persisted("users", values, None, PersistenceTier::Worker)
                .unwrap();

        pump_3tier(&mut s);

        match receiver.try_recv() {
            Ok(Some(())) => {}
            Ok(None) => panic!("EdgeServer ack should satisfy Worker request"),
            Err(_) => panic!("Receiver was cancelled"),
        }
    }

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

        let query = Query::new("users");
        let results = execute_query(&mut s.b, query);
        assert_eq!(results[0].1[1], Value::Text("Bob".into()));
    }

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

        let query = Query::new("users");
        let results = execute_query(&mut s.b, query);
        assert_eq!(results.len(), 0);
    }

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

        pump_3tier(&mut s);

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

    #[test]
    fn rc_query_no_settled_tier_immediate() {
        let mut s = create_3tier_rc();

        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();

        let mut future = s.a.query(Query::new("users"), None, None);

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

    #[test]
    fn rc_query_settled_tier_holds() {
        let mut s = create_3tier_rc();

        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();

        let mut future =
            s.a.query(Query::new("users"), None, Some(PersistenceTier::Worker));

        let waker = noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(
            Pin::new(&mut future).poll(&mut cx).is_pending(),
            "Query should be pending before Worker settlement"
        );

        pump_a_to_b(&mut s);
        pump_b_to_a(&mut s);

        match Pin::new(&mut future).poll(&mut cx) {
            Poll::Ready(Ok(results)) => {
                assert_eq!(results.len(), 1, "Should have one row after settlement");
                assert_eq!(results[0].0, id);
            }
            Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
            Poll::Pending => panic!("Query should resolve after Worker QuerySettled"),
        }
    }

    #[test]
    fn rc_subscribe_settled_tier() {
        let mut s = create_3tier_rc();

        let received = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
        let received_clone = received.clone();

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

        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
        let id = s.a.insert("users", values, None).unwrap();
        s.a.immediate_tick();

        assert!(
            received.lock().unwrap().is_empty(),
            "Callback should not fire before Worker settlement"
        );

        pump_a_to_b(&mut s);
        pump_b_to_a(&mut s);

        let calls = received.lock().unwrap();
        assert!(
            !calls.is_empty(),
            "Callback should fire after Worker QuerySettled"
        );
        let first_delivery = &calls[0];
        assert_eq!(first_delivery.len(), 1, "Should have one row");
        assert_eq!(first_delivery[0].0, id);
    }

    fn noop_waker() -> std::task::Waker {
        fn noop(_: *const ()) {}
        fn clone(_: *const ()) -> std::task::RawWaker {
            std::task::RawWaker::new(std::ptr::null(), &VTABLE)
        }
        static VTABLE: std::task::RawWakerVTable =
            std::task::RawWakerVTable::new(clone, noop, noop, noop);
        unsafe { std::task::Waker::from_raw(std::task::RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

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

        core.immediate_tick();
        let initial_count = *callback_count.lock().unwrap();

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

    #[test]
    fn test_persist_schema_then_add_server_sends_catalogue() {
        // Mirror the WASM flow EXACTLY: NO immediate_tick before persist_schema
        let schema = test_schema();
        let app_id = AppId::from_name("test-app");
        let sync_manager = SyncManager::new();
        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
        let mut core = RuntimeCore::new(
            schema_manager,
            MemoryStorage::new(),
            NoopScheduler,
            VecSyncSender::new(),
        );
        // NO immediate_tick() here — matches WASM openPersistent flow

        // persist_schema — creates catalogue object in ObjectManager
        let schema_obj_id = core.persist_schema();

        // add_server — should call queue_full_sync_to_server which includes the catalogue
        let server_id = ServerId::new();
        core.add_server(server_id);

        // batched_tick — should flush catalogue to outbox → sync sender
        core.batched_tick();

        // Check that the catalogue was sent
        let messages = core.sync_sender().take();
        let catalogue_msg = messages.iter().find(|m| {
            if let SyncPayload::ObjectUpdated {
                object_id,
                metadata,
                ..
            } = &m.payload
            {
                *object_id == schema_obj_id
                    && metadata
                        .as_ref()
                        .and_then(|m| m.metadata.get(crate::metadata::MetadataKey::Type.as_str()))
                        .map(|t| t == crate::metadata::ObjectType::CatalogueSchema.as_str())
                        .unwrap_or(false)
            } else {
                false
            }
        });

        assert!(
            catalogue_msg.is_some(),
            "Catalogue schema object should be in outbox after add_server + batched_tick. \
             Messages found: {}",
            messages
                .iter()
                .map(|m| format!("{:?}", m.payload))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
}
