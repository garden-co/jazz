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
//! - Outbox entries are sent via `TransportHandle` channels (the only outbox path)
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

use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::query_manager::QuerySubscriptionId;
use crate::query_manager::encoding::decode_row;
use crate::query_manager::manager::{QueryError, QueryUpdate};
use crate::query_manager::query::Query;
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{
    OrderedRowDelta, Schema, SchemaHash, TableName, TablePolicies, Value,
};
use crate::schema_manager::{Lens, SchemaManager};
use crate::storage::Storage;
use crate::sync_manager::{ClientId, DurabilityTier, InboxEntry, OutboxEntry, ServerId};

// ============================================================================
// Scheduler trait
// ============================================================================

/// Schedules batched ticks on the platform's event loop.
///
/// No `Send` bound — WASM types (`Rc`, `Function`) are `!Send`.
/// Tokio enforces `Send` at the point of use (`Arc<Mutex<...>>`).
pub trait Scheduler {
    fn schedule_batched_tick(&self);
}

// ============================================================================
// TransportHandle — channel-based outbox/inbox for all platforms
// ============================================================================

/// Channel endpoints held by `RuntimeCore`. Concrete type on all platforms.
///
/// - `outbox_tx`: `batched_tick()` pushes outgoing messages here
/// - `inbound_rx`: `batched_tick()` drains incoming messages from here
///
/// Uses `std::sync::mpsc` so it works everywhere (tokio, WASM, NAPI, RN).
pub struct TransportHandle {
    pub outbox_tx: std::sync::mpsc::Sender<OutboxEntry>,
    pub inbound_rx: std::sync::mpsc::Receiver<InboxEntry>,
}

impl TransportHandle {
    /// Create a new transport handle pair: (handle_for_runtime, outbox_rx, inbound_tx).
    ///
    /// The caller keeps `outbox_rx` (to consume outbox entries) and `inbound_tx`
    /// (to push inbound entries). RuntimeCore gets the `TransportHandle`.
    pub fn create() -> (
        Self,
        std::sync::mpsc::Receiver<OutboxEntry>,
        std::sync::mpsc::Sender<InboxEntry>,
    ) {
        let (outbox_tx, outbox_rx) = std::sync::mpsc::channel();
        let (inbound_tx, inbound_rx) = std::sync::mpsc::channel();
        (
            Self {
                outbox_tx,
                inbound_rx,
            },
            outbox_rx,
            inbound_tx,
        )
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

/// Collects outbox messages for test inspection via a TransportHandle.
pub struct TestOutbox {
    rx: std::sync::mpsc::Receiver<OutboxEntry>,
}

impl TestOutbox {
    /// Take all collected messages (drains the channel).
    pub fn take(&self) -> Vec<OutboxEntry> {
        let mut messages = Vec::new();
        while let Ok(msg) = self.rx.try_recv() {
            messages.push(msg);
        }
        messages
    }
}

/// Create a `TransportHandle` and a `TestOutbox` for test use.
/// The `TestOutbox` collects all outbox entries sent through the handle.
pub fn test_transport() -> (TransportHandle, TestOutbox) {
    let (handle, outbox_rx, _inbound_tx) = TransportHandle::create();
    (handle, TestOutbox { rx: outbox_rx })
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
/// All business logic is synchronous. Outbox entries are sent via `TransportHandle`
/// channels (the only outbox path).
pub struct RuntimeCore<S: Storage, Sch: Scheduler> {
    schema_manager: SchemaManager,
    pub(crate) storage: S,
    scheduler: Sch,

    /// Channel-based transport for outbox/inbox I/O.
    /// When `Some`, `batched_tick()` sends outbox entries through here and drains inbound.
    /// When `None`, outbox entries are silently dropped (no server connected).
    transport: Option<TransportHandle>,

    /// Parked sync messages (from network).
    parked_sync_messages: Vec<InboxEntry>,
    /// Sequenced server messages buffered for in-order application.
    parked_sync_messages_by_server_seq: HashMap<ServerId, BTreeMap<u64, InboxEntry>>,
    /// Next expected per-server stream sequence.
    next_expected_server_seq: HashMap<ServerId, u64>,

    /// Subscription tracking with callbacks.
    subscriptions: HashMap<SubscriptionHandle, SubscriptionState>,
    /// Reverse map for routing updates.
    subscription_reverse: HashMap<QuerySubscriptionId, SubscriptionHandle>,
    next_subscription_handle: u64,
    /// Created-but-not-yet-executed subscriptions (2-phase subscribe).
    pending_subscriptions: HashMap<SubscriptionHandle, subscriptions::PendingSubscription>,

    /// Pending one-shot queries (query() calls waiting for first callback).
    pending_one_shot_queries: HashMap<SubscriptionHandle, PendingOneShotQuery>,

    /// Watchers for persistence acks: (commit_id, requested_tier) → senders.
    /// A tier >= requested tier satisfies the watcher (e.g., EdgeServer ack satisfies Worker).
    ack_watchers: HashMap<CommitId, Vec<(DurabilityTier, oneshot::Sender<()>)>>,

    /// Label for tracing (e.g. "worker", "edge", "client").
    tier_label: &'static str,

    /// Buffer for outbox entries (populated by `send_outbox()`).
    /// Used by tests and benchmarks to inspect outbox entries without a real transport.
    outbox_tap: Vec<OutboxEntry>,
}

impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch> {
    /// Create a new RuntimeCore.
    pub fn new(schema_manager: SchemaManager, storage: S, scheduler: Sch) -> Self {
        Self {
            schema_manager,
            storage,
            scheduler,
            transport: None,
            parked_sync_messages: Vec::new(),
            parked_sync_messages_by_server_seq: HashMap::new(),
            next_expected_server_seq: HashMap::new(),
            subscriptions: HashMap::new(),
            subscription_reverse: HashMap::new(),
            next_subscription_handle: 0,
            pending_subscriptions: HashMap::new(),
            pending_one_shot_queries: HashMap::new(),
            ack_watchers: HashMap::new(),
            tier_label: "unknown",
            outbox_tap: Vec::new(),
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

    /// Set the channel-based transport.
    pub fn set_transport(&mut self, transport: TransportHandle) {
        self.transport = Some(transport);
    }

    /// Clear the channel-based transport.
    pub fn clear_transport(&mut self) {
        self.transport = None;
    }

    /// Take all buffered outbox entries. Returns entries accumulated since the last call.
    /// Used by tests and benchmarks to inspect outbox entries.
    pub fn take_outbox_tap(&mut self) -> Vec<OutboxEntry> {
        std::mem::take(&mut self.outbox_tap)
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

    /// Publish any known schema object to the catalogue and in-memory schema manager.
    pub fn publish_schema(&mut self, schema: Schema) -> ObjectId {
        let schema_hash = crate::query_manager::types::SchemaHash::compute(&schema);

        if self.schema_manager.get_known_schema(&schema_hash).is_none() {
            self.schema_manager.add_known_schema(schema.clone());
        }

        let id = self
            .schema_manager
            .persist_schema_object(&mut self.storage, &schema);
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
        self.immediate_tick();
        Ok(id)
    }

    /// Publish a reviewed lens edge to the active schema manager and catalogue.
    pub fn publish_lens(&mut self, lens: &Lens) -> Result<ObjectId, RuntimeError> {
        let id = self
            .schema_manager
            .publish_lens(&mut self.storage, lens)
            .map_err(|error| RuntimeError::WriteError(error.to_string()))?;
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
