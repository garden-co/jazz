//! RuntimeCore - Unified synchronous runtime logic for both native and WASM.
//!
//! This module provides the shared core logic that both jazz-tokio
//! and jazz-wasm wrap. RuntimeCore is generic over `Storage`, `Scheduler`,
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

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::oneshot;
use tracing::{debug, debug_span, info, trace, trace_span};

use crate::object::ObjectId;
use crate::query_manager::QuerySubscriptionId;
use crate::query_manager::encoding::decode_row;
use crate::query_manager::manager::{QueryError, QueryUpdate};
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::query_manager::types::{OrderedRowDelta, Schema, TableName, Value};
use crate::schema_manager::SchemaManager;
use crate::storage::Storage;
use crate::sync_manager::{ClientId, DurabilityTier, InboxEntry, OutboxEntry, ServerId};

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

    /// Watchers for persistence acks: (doc_id, requested_tier) → senders.
    /// A tier >= requested tier satisfies the watcher (e.g., EdgeServer ack satisfies Worker).
    ack_watchers: HashMap<ObjectId, Vec<(DurabilityTier, oneshot::Sender<()>)>>,

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
            parked_sync_messages_by_server_seq: HashMap::new(),
            next_expected_server_seq: HashMap::new(),
            subscriptions: HashMap::new(),
            subscription_reverse: HashMap::new(),
            next_subscription_handle: 0,
            pending_subscriptions: HashMap::new(),
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
    ///
    /// Persists all in-memory DocManager docs to the outer storage first,
    /// then flushes the storage to disk.
    pub fn flush_storage(&mut self) {
        // Persist all DocManager docs to the real storage
        let doc_ids: Vec<ObjectId> = self
            .schema_manager
            .query_manager()
            .sync_manager()
            .doc_manager
            .all_docs()
            .map(|(id, _)| id)
            .collect();
        for doc_id in doc_ids {
            let row_doc = self
                .schema_manager
                .query_manager()
                .sync_manager()
                .doc_manager
                .get(doc_id);
            if let Some(row_doc) = row_doc {
                use yrs::{ReadTxn, Transact};
                let snapshot = row_doc
                    .doc
                    .transact()
                    .encode_state_as_update_v1(&yrs::StateVector::default());
                let metadata = row_doc.metadata.clone();

                // Ensure doc exists in storage
                if self
                    .storage
                    .load_doc_metadata(doc_id)
                    .ok()
                    .flatten()
                    .is_none()
                {
                    let _ = self.storage.create_doc(doc_id, &metadata);
                }
                let _ = self.storage.save_snapshot(doc_id, &snapshot);
            }
        }

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

    /// Load all persisted docs from storage into DocManager.
    ///
    /// Called at startup to restore in-memory state from persistent storage.
    /// DocManager uses MemoryStorage internally (it's just a cache), so on
    /// restart we need to reload docs from RuntimeCore's persistent storage.
    pub fn load_persisted_docs(&mut self) {
        type PersistedDocData = (
            ObjectId,
            HashMap<String, String>,
            Option<Vec<u8>>,
            Vec<Vec<u8>>,
        );

        let doc_ids = match self.storage.list_doc_ids() {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!("failed to list persisted doc IDs: {e}");
                return;
            }
        };

        // Collect all doc data from storage first (avoids borrow conflict with DocManager).
        let mut to_load: Vec<PersistedDocData> = Vec::new();
        for doc_id in &doc_ids {
            let metadata = match self.storage.load_doc_metadata(*doc_id) {
                Ok(Some(m)) => m,
                _ => continue,
            };
            let snapshot = self.storage.load_snapshot(*doc_id).ok().flatten();
            let updates = self.storage.load_updates(*doc_id).unwrap_or_default();
            to_load.push((*doc_id, metadata, snapshot, updates));
        }

        // Now apply to DocManager.
        let doc_manager = &mut self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .doc_manager;

        for (doc_id, metadata, snapshot, updates) in &to_load {
            if doc_manager.get(*doc_id).is_some() {
                continue;
            }

            doc_manager.create_with_id(*doc_id, metadata.clone());

            if let Some(snapshot) = snapshot {
                let _ = doc_manager.apply_update(*doc_id, snapshot);
            }

            for update_bytes in updates {
                let _ = doc_manager.apply_update(*doc_id, update_bytes);
            }
        }

        // Queue loaded docs for indexing by process_synced_docs on the next tick.
        let loaded_ids: Vec<ObjectId> = to_load.iter().map(|(id, _, _, _)| *id).collect();
        let sync_manager = &mut self.schema_manager.query_manager_mut().sync_manager_mut();
        for id in &loaded_ids {
            sync_manager.push_synced_doc_id(*id);
        }

        info!(
            count = loaded_ids.len(),
            "loaded persisted docs into DocManager"
        );
    }
}

mod subscriptions;
mod sync;
mod ticks;
mod writes;

#[cfg(test)]
mod tests;
