//! RuntimeCore - Unified synchronous runtime logic for both native and WASM.
//!
//! This module provides the shared core logic that both jazz-tokio
//! and jazz-wasm wrap. RuntimeCore is generic over `Storage` and `Scheduler`
//! which provide platform-specific behavior.
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
//! let runtime = RuntimeCore::new(schema_manager, storage, scheduler);
//! runtime.insert(
//!     "users",
//!     std::collections::HashMap::from([
//!         ("id".to_string(), id),
//!         ("name".to_string(), name),
//!     ]),
//! )?;
//! runtime.immediate_tick();
//! let future = runtime.query(query);
//! let results = future.await?;
//! ```

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::oneshot;
use tracing::{debug, debug_span, info, trace, trace_span};

use crate::object::ObjectId;
use crate::query_manager::QuerySubscriptionId;
use crate::query_manager::manager::{QueryError, QueryUpdate};
use crate::query_manager::query::Query;
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{
    OrderedRowDelta, Schema, SchemaHash, TableName, TablePolicies, Value,
};
use crate::row_format::decode_row;
use crate::schema_manager::{Lens, SchemaManager};
use crate::storage::Storage;
use crate::sync_manager::{
    ClientId, DurabilityTier, InboxEntry, OutboxEntry, RowVersionKey, ServerId,
};

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

    /// Drain all buffered messages (test helper). Returns empty by default.
    fn take_messages(&self) -> Vec<OutboxEntry> {
        Vec::new()
    }
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
    messages: std::sync::Arc<std::sync::Mutex<Vec<OutboxEntry>>>,
}

impl Default for VecSyncSender {
    fn default() -> Self {
        Self {
            messages: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

impl VecSyncSender {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a VecSyncSender backed by a shared buffer.
    pub fn from_shared(messages: std::sync::Arc<std::sync::Mutex<Vec<OutboxEntry>>>) -> Self {
        Self { messages }
    }

    /// Get a shared handle to the internal message buffer.
    pub fn shared(&self) -> std::sync::Arc<std::sync::Mutex<Vec<OutboxEntry>>> {
        self.messages.clone()
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

    fn take_messages(&self) -> Vec<OutboxEntry> {
        self.take()
    }
}

/// SyncSender adapter that delegates to a callback closure.
///
/// Useful for native platforms that dispatch outbox entries via a closure
/// (e.g., spawning tokio tasks to push to a server connection).
pub struct CallbackSyncSender {
    callback: std::sync::Arc<dyn Fn(OutboxEntry) + Send + Sync>,
}

impl CallbackSyncSender {
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn(OutboxEntry) + Send + Sync + 'static,
    {
        Self {
            callback: std::sync::Arc::new(callback),
        }
    }
}

impl SyncSender for CallbackSyncSender {
    fn send_sync_message(&self, message: OutboxEntry) {
        (self.callback)(message);
    }
}

/// Handle to a subscription managed by RuntimeCore.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionHandle(pub u64);

// Re-export QueryHandle from query_manager for convenience
pub use crate::query_manager::manager::QueryHandle as QMQueryHandle;
pub use subscriptions::ReadDurabilityOptions;

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
        RuntimeError::QueryError(e.to_string())
    }
}

/// Type alias for query results.
pub type QueryResult = Result<Vec<(ObjectId, Vec<Value>)>, RuntimeError>;
/// Type alias for inserted row payloads.
pub type InsertedRow = (ObjectId, Vec<Value>);

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
    /// The row changes with position-annotated ordering.
    pub ordered_delta: OrderedRowDelta,
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

/// Boxed peer sender type.
///
/// On native platforms, must be `Send` for thread safety.
/// On WASM (single-threaded), `Send` is not required.
#[cfg(target_arch = "wasm32")]
type PeerSenderBox = Box<dyn SyncSender>;

#[cfg(not(target_arch = "wasm32"))]
type PeerSenderBox = Box<dyn SyncSender + Send>;

/// Boxed outbox observer. Called for every outbox entry just before it is
/// dispatched to the transport or peer sender. On native platforms must be
/// `Send`; on WASM the runtime is single-threaded so `Send` is not required.
#[cfg(target_arch = "wasm32")]
pub type OutboxObserverBox = Box<dyn Fn(&OutboxEntry) + 'static>;

#[cfg(not(target_arch = "wasm32"))]
pub type OutboxObserverBox = Box<dyn Fn(&OutboxEntry) + Send + Sync + 'static>;

/// Boxed inbox observer. Called for every inbound sync entry drained from the
/// transport. Used by the SyncTracer to capture incoming traffic with a
/// human-readable client name.
#[cfg(target_arch = "wasm32")]
pub type InboxObserverBox = Box<dyn Fn(&InboxEntry) + 'static>;

#[cfg(not(target_arch = "wasm32"))]
pub type InboxObserverBox = Box<dyn Fn(&InboxEntry) + Send + Sync + 'static>;

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
/// Generic over `Storage` for data persistence and `Scheduler` for tick scheduling.
/// All business logic is synchronous.
pub struct RuntimeCore<S: Storage, Sch: Scheduler> {
    schema_manager: SchemaManager,
    pub(crate) storage: S,
    scheduler: Sch,
    /// True when storage was mutated since the last WAL flush barrier.
    storage_write_pending_flush: bool,

    /// WebSocket transport handle (server sync).
    transport: Option<crate::transport_manager::TransportHandle>,
    /// Peer/worker-bridge sender (WASM main-thread runtime only).
    peer_sender: Option<PeerSenderBox>,
    /// Optional observer called for every outbox entry just before it is
    /// dispatched to the transport or peer sender. Used by the SyncTracer in
    /// tests to capture outgoing messages with a human-readable client name.
    outbox_observer: Option<OutboxObserverBox>,
    /// Optional observer called for every inbound sync entry drained from the
    /// transport. Used by the SyncTracer to capture incoming messages with a
    /// human-readable client name.
    inbox_observer: Option<InboxObserverBox>,

    /// Parked sync messages (from network).
    parked_sync_messages: Vec<InboxEntry>,
    /// Sequenced server messages buffered for in-order application.
    parked_sync_messages_by_server_seq: HashMap<ServerId, BTreeMap<u64, InboxEntry>>,
    /// Next expected per-server stream sequence.
    next_expected_server_seq: HashMap<ServerId, u64>,
    /// Highest per-server stream sequence already applied to the inbox.
    last_applied_server_seq: HashMap<ServerId, u64>,

    /// Subscription tracking with callbacks.
    subscriptions: HashMap<SubscriptionHandle, SubscriptionState>,
    /// Reverse map for routing updates.
    subscription_reverse: HashMap<QuerySubscriptionId, SubscriptionHandle>,
    next_subscription_handle: u64,
    /// Created-but-not-yet-executed subscriptions (2-phase subscribe).
    pending_subscriptions: HashMap<SubscriptionHandle, subscriptions::PendingSubscription>,

    /// Pending one-shot queries (query() calls waiting for first callback).
    pending_one_shot_queries: HashMap<SubscriptionHandle, PendingOneShotQuery>,

    /// Watchers for persistence acks: (row version, requested_tier) → senders.
    /// A tier >= requested tier satisfies the watcher (e.g., EdgeServer ack satisfies Worker).
    ack_watchers: HashMap<RowVersionKey, Vec<(DurabilityTier, oneshot::Sender<()>)>>,

    /// Label for tracing (e.g. "worker", "edge", "client").
    tier_label: &'static str,

    /// Latest auth-rejection reason from the transport, awaiting host pickup.
    /// `take_auth_failure()` consumes it. Set whenever the transport emits
    /// `TransportInbound::AuthFailure`; the reconnect loop is permanently
    /// stopped after that, so the host must refresh creds and `connect()`.
    pending_auth_failure: Option<crate::transport_manager::AuthFailureReason>,
}

impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch> {
    /// Create a new RuntimeCore.
    pub fn new(mut schema_manager: SchemaManager, mut storage: S, scheduler: Sch) -> Self {
        let _ = schema_manager.ensure_current_schema_persisted(&mut storage);

        Self {
            schema_manager,
            storage,
            scheduler,
            storage_write_pending_flush: false,
            transport: None,
            peer_sender: None,
            outbox_observer: None,
            inbox_observer: None,
            parked_sync_messages: Vec::new(),
            parked_sync_messages_by_server_seq: HashMap::new(),
            next_expected_server_seq: HashMap::new(),
            last_applied_server_seq: HashMap::new(),
            subscriptions: HashMap::new(),
            subscription_reverse: HashMap::new(),
            next_subscription_handle: 0,
            pending_subscriptions: HashMap::new(),
            pending_one_shot_queries: HashMap::new(),
            ack_watchers: HashMap::new(),
            tier_label: "unknown",
            pending_auth_failure: None,
        }
    }

    /// Consume and return the most recent auth-rejection reason, if any.
    /// The host runtime should poll this after each `batched_tick` and call
    /// `onAuthFailure(reason)` on the JS side when present.
    pub fn take_auth_failure(&mut self) -> Option<crate::transport_manager::AuthFailureReason> {
        self.pending_auth_failure.take()
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

    pub(crate) fn mark_storage_write_pending_flush(&mut self) {
        self.storage_write_pending_flush = true;
    }

    pub(crate) fn clear_storage_write_pending_flush(&mut self) {
        self.storage_write_pending_flush = false;
    }

    /// Consume RuntimeCore and return the Storage.
    /// Used for cold-start testing to transfer driver state.
    pub fn into_storage(self) -> S {
        self.storage
    }

    /// Set the transport handle for server sync.
    /// Called by platform runtimes (TokioRuntime::connect, NapiRuntime::connect, etc.)
    /// when a WebSocket connection is established.
    pub fn set_transport(&mut self, handle: crate::transport_manager::TransportHandle) {
        self.transport = Some(handle);
    }

    /// Clear the transport handle (on disconnect or shutdown).
    ///
    /// Tears down upstream server state for the dropped handle so a follow-up
    /// `connect()` (which always allocates a fresh `ServerId`) does not leave
    /// a zombie upstream behind. This matches what the `Disconnected` inbound
    /// event would do during a clean disconnect.
    pub fn clear_transport(&mut self) {
        if let Some(handle) = self.transport.take() {
            self.remove_server(handle.server_id);
        }
    }

    /// Returns true once the current transport handle has successfully
    /// completed at least one auth handshake with the server.
    pub fn transport_ever_connected(&self) -> bool {
        self.transport
            .as_ref()
            .map(|h| h.has_ever_connected())
            .unwrap_or(false)
    }

    /// Set the peer/worker-bridge sender.
    pub fn set_peer_sender(&mut self, sender: PeerSenderBox) {
        self.peer_sender = Some(sender);
    }

    /// Install an observer closure called for every outbox entry just before
    /// it is dispatched. Used by tests to record outgoing sync traffic with a
    /// human-readable client name.
    pub fn set_outbox_observer(&mut self, observer: OutboxObserverBox) {
        self.outbox_observer = Some(observer);
    }

    pub(crate) fn outbox_observer(&self) -> Option<&OutboxObserverBox> {
        self.outbox_observer.as_ref()
    }

    /// Install an observer closure called for every inbound sync entry
    /// drained from the transport. Used by tests to record incoming sync
    /// traffic with a human-readable client name.
    pub fn set_inbox_observer(&mut self, observer: InboxObserverBox) {
        self.inbox_observer = Some(observer);
    }

    pub(crate) fn inbox_observer(&self) -> Option<&InboxObserverBox> {
        self.inbox_observer.as_ref()
    }

    /// Take all buffered messages from the peer sender (test helper).
    pub fn take_peer_messages(&self) -> Vec<OutboxEntry> {
        self.peer_sender
            .as_ref()
            .map(|s| s.take_messages())
            .unwrap_or_default()
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
        self.mark_storage_write_pending_flush();
        info!(object_id = %id, "persisted schema to catalogue");
        id
    }

    /// Publish any known schema object to the catalogue and in-memory schema manager.
    pub fn publish_schema(&mut self, schema: Schema) -> ObjectId {
        let schema_hash = crate::query_manager::types::SchemaHash::compute(&schema);

        if self.schema_manager.get_known_schema(&schema_hash).is_none() {
            self.schema_manager.add_known_schema(schema.clone());
        }

        let id = self
            .schema_manager
            .persist_schema_object(&mut self.storage, &schema);
        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        id
    }

    pub fn publish_permissions_bundle(
        &mut self,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, crate::schema_manager::SchemaError> {
        let id = self.schema_manager.publish_permissions_bundle(
            &mut self.storage,
            schema_hash,
            permissions,
            expected_parent_bundle_object_id,
        )?;
        if id.is_some() {
            self.mark_storage_write_pending_flush();
        }
        self.immediate_tick();
        Ok(id)
    }

    /// Publish a reviewed lens edge to the active schema manager and catalogue.
    pub fn publish_lens(&mut self, lens: &Lens) -> Result<ObjectId, RuntimeError> {
        let id = self
            .schema_manager
            .publish_lens(&mut self.storage, lens)
            .map_err(|error| RuntimeError::WriteError(error.to_string()))?;
        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(id)
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

    /// Add a historical live schema and persist both schema and lens catalogue objects.
    pub fn add_live_schema_and_persist_catalogue(
        &mut self,
        schema: Schema,
    ) -> Result<(), crate::schema_manager::context::SchemaError> {
        let lens = self.schema_manager.add_live_schema(schema.clone())?.clone();
        self.schema_manager
            .persist_schema_object(&mut self.storage, &schema);
        self.schema_manager.persist_lens(&mut self.storage, &lens);
        Ok(())
    }

    /// Get access to the underlying SchemaManager.
    pub fn schema_manager(&self) -> &SchemaManager {
        &self.schema_manager
    }
}

mod subscriptions;
mod sync;
mod ticks;
mod writes;

#[cfg(test)]
mod tests;
