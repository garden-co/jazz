//! High-level thread-affine database facade described by `jazz/API.md`. This
//! module owns application-facing handles, read/write options, and facade-level
//! sync plumbing; durable version storage, validation, policy checks, and view
//! construction live in [`crate::node`], while link-local shipped state lives in
//! [`crate::peer`]. In the layer map this is the top `Db` facade over the node.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::rc::{Rc, Weak};
#[cfg(feature = "sync-autopsy")]
use std::sync::{
    LazyLock, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll, Waker};

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_channel::oneshot;
use futures_core::Stream;
use groove::records::{OwnedRecord, RecordDescriptor, Value};
use groove::schema::ColumnType as GrooveColumnType;
use groove::storage::{OrderedKvStorage, ReopenableStorage};
use thiserror::Error;
use web_time::Instant;

/// Maximum history-codec windows built during one outer `Db::tick` post-tick
/// maintenance pass.
///
/// The pass runs after connection work and cache eviction, between runtime
/// ticks. Keeping this small bounds foreground tick work while allowing old
/// plain history runs to compact incrementally.
const POST_TICK_HISTORY_WINDOW_BUDGET: usize = 4;

use crate::ids::{AuthorId, NodeUuid, RowUuid, SchemaVersionId};
pub use crate::node::CommitUnitTrust;
use crate::node::{
    CommitUnitIngestContext, CurrentRow, EdgeCacheBudget, LargeValueEditCommit, LargeValueEditOp,
    LocalMaintainedViewSubscription, LocalMaintainedViewSubscriptionUpdate, MergeableCommit,
    NodeState, OpenTxId, PreparedQueryPlanHandle, QueryReadProfile, RelationEdge, RelationSnapshot,
    RowProvenance, ViewUpdateParts,
};
use crate::peer::{PeerRole, PeerState};
#[cfg(feature = "sync-autopsy")]
use crate::protocol::expand_version_carriers;
use crate::protocol::{
    BindingViewKey, ContentExtent, CoverageKey, CurrentWriteSchema, LargeValueOwnerRef,
    MigrationLens, ReadViewKey, ReadViewSourceSpec, ReadViewSpec, RegisterShapeOptions,
    SchemaLineagePublication, SchemaVersion, ShapeAst, Subscribe, SubscribeRejectReason,
    SubscribeServerFailureCode, SubscriptionKey, SyncMessage,
};
use crate::protocol_limits::{
    MAX_FETCH_BRANCH_METADATA, MAX_INFLIGHT_LOGICAL_MESSAGE_BYTES, MAX_INFLIGHT_LOGICAL_MESSAGES,
    validate_content_extents, validate_fetch_branch_metadata, validate_fetch_row_versions,
    validate_known_state_declaration, validate_logical_message_len, validate_shape_ast_size,
    validate_wire_frame_len,
};
use crate::query::{
    Binding, BindingId, Query, QueryError, RelationQuery, ShapeId, ValidatedQuery,
    relation_query_to_query,
};
#[cfg(test)]
use crate::query::{
    RelationCmpOp, RelationColumnRef, RelationExpr, RelationJoinKind, RelationPredicate,
    RelationProjectExpr, RelationRowIdRef, RelationValueRef,
};
pub use crate::result_tree::{ResultNode, ResultRelation, ResultTree, ResultTreeReplacement};
use crate::schema::{JazzSchema, TableSchema};
use crate::time::GlobalSeq;
use crate::tools::{ObjectId, OutputOccurrenceId};
use crate::tx::{DeletionEvent, DurabilityTier, Fate, RejectionReason, TxId, TxKind};
use crate::wire::{
    FEATURE_MESSAGE_FRAGMENTATION, TransportError, WIRE_PROTOCOL_VERSION, WireEnvelope, WireError,
    WireErrorCode, WireFeatures, WireFrame, WireMessageFragment, WireRetry, WireSession,
    WireStreamDecoder, WireStreamEncoder, WireTransport, current_wire_features, decode_frame,
    decode_sync_message_for_receive, encode_frame, encode_sync_message,
};

const WIRE_FRAGMENT_PAYLOAD_BYTES: usize = 512 * 1024;
const RECENT_COMPLETED_LOGICAL_MESSAGES: usize = 64;

/// How urgently a runtime should service pending peer-connection work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TickUrgency {
    /// Run as soon as the runtime can do so without re-entering the current
    /// mutable operation. Used when a query/subscription/transport event needs
    /// prompt coverage or inbound draining.
    Immediate,
    /// Coalesce bursty local work before ticking. Used for uploads created by
    /// local writes.
    Deferred,
}

/// Runtime-neutral wake hook for thread-affine [`Node`] sync work.
pub trait TickScheduler {
    /// Schedule a future [`Db::tick`] for pending peer-connection work.
    fn schedule_tick(&self, urgency: TickUrgency);
}

#[cfg(feature = "sync-autopsy")]
/// Debug-build sync trace buffer used by integration-test timeout autopsies.
pub mod sync_autopsy {
    use super::*;

    const MAX_EVENTS: usize = 512;

    static ENABLED: AtomicBool = AtomicBool::new(false);
    static EVENTS: LazyLock<Mutex<VecDeque<String>>> =
        LazyLock::new(|| Mutex::new(VecDeque::with_capacity(MAX_EVENTS)));

    /// Enable passive event capture for the current process.
    pub fn enable() {
        ENABLED.store(true, Ordering::Relaxed);
    }

    /// Clear buffered events.
    pub fn clear() {
        if let Ok(mut events) = EVENTS.lock() {
            events.clear();
        }
    }

    /// Return the current buffered event log.
    pub fn dump() -> String {
        let events = EVENTS.lock().ok();
        let mut out = String::from("sync autopsy events:\n");
        if let Some(events) = events {
            for event in events.iter() {
                out.push_str("  ");
                out.push_str(event);
                out.push('\n');
            }
        } else {
            out.push_str("  <event buffer poisoned>\n");
        }
        out
    }

    /// Append one event to the ring buffer when capture is enabled.
    pub fn record(event: impl Into<String>) {
        if !ENABLED.load(Ordering::Relaxed) {
            return;
        }
        let Ok(mut events) = EVENTS.lock() else {
            return;
        };
        if events.len() == MAX_EVENTS {
            events.pop_front();
        }
        events.push_back(event.into());
    }
}

#[cfg(not(feature = "sync-autopsy"))]
/// No-op sync trace buffer when sync autopsy capture is not compiled in.
pub mod sync_autopsy {
    /// Enable passive event capture for the current process.
    pub fn enable() {}
    /// Clear buffered events.
    pub fn clear() {}
    /// Return the current buffered event log.
    pub fn dump() -> String {
        String::new()
    }
}

/// Poll a ready-immediate thread-affine database future to completion.
///
/// This helper is intentionally tiny: it drives local-lane futures that are
/// expected to complete without an async runtime by using a no-op waker and
/// yielding the current thread when a future reports `Pending`.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = pin!(future);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

/// Thread-affine high-level database handle.
pub struct Db<S>
where
    S: OrderedKvStorage,
{
    schema: JazzSchema,
    schema_version_id: SchemaVersionId,
    identity: DbIdentity,
    node: Node<S>,
    row_id_source: RefCell<Box<dyn RowIdSource>>,
    next_now_ms: Cell<u64>,
}

/// Shared list of live subscriptions. Held by both the `Node` and any
/// [`PeerConnection`], so an inbound sync update can push subscription events
/// through the same path a local write does.
type SubscriptionList = Rc<RefCell<Vec<Weak<RefCell<SubscriptionState>>>>>;
type PendingUpstreamCommands = Rc<RefCell<Vec<PendingUpstreamCommand>>>;
type LatestCoverageSubscriptions = Rc<RefCell<BTreeMap<CoverageKey, SubscriptionKey>>>;
type UpstreamCoverageRefCounts = Rc<RefCell<BTreeMap<CoverageKey, usize>>>;
type UpstreamSubscriptionOwners =
    Rc<RefCell<BTreeMap<SubscriptionKey, Vec<Weak<RefCell<SubscriptionState>>>>>>;
type SharedTickScheduler = Rc<RefCell<Option<Rc<dyn TickScheduler>>>>;
type WriteStateWaiters = Rc<RefCell<BTreeMap<TxId, Vec<WriteStateWaiter>>>>;
type ShapeRegistrationKey = (ShapeId, ReadViewKey);

/// Per-subscriber state for a shape/read-view registration.
///
/// A missing runtime shape is ambiguous: catalogue admission is temporary,
/// whereas a capability rejection is permanent for this connection. Keep the
/// distinction at the protocol boundary instead of inferring either from the
/// node's registered-shape map.
#[derive(Clone)]
enum SubscriberShapeRegistration {
    Registered(RegisterShapeOptions),
    PendingCatalogueAdmission(RegisterShapeOptions),
    RejectedUnsupportedCapability(String),
}

fn default_cell_for_column_type(column_type: &GrooveColumnType, default: &Value) -> Value {
    match (column_type, default) {
        (GrooveColumnType::Nullable(_), Value::Nullable(_)) => default.clone(),
        (GrooveColumnType::Nullable(_), default) => {
            Value::Nullable(Some(Box::new(default.clone())))
        }
        _ => default.clone(),
    }
}

struct WriteStateWaiter {
    id: u64,
    notify: WriteStateWaiterNotify,
}

enum WriteStateWaiterNotify {
    Future(oneshot::Sender<()>),
    Callback(Box<dyn FnOnce()>),
}

#[derive(Clone)]
enum PendingUpstreamCommand {
    Subscribe(PendingUpstreamSubscription),
    Unsubscribe(SubscriptionKey),
    FetchContentExtent {
        owner: LargeValueOwnerRef,
        extent: crate::node::content_store::Extent,
    },
}

#[derive(Clone)]
struct PendingUpstreamSubscription {
    subscription: SubscriptionKey,
    shape: ValidatedQuery,
    binding: Binding,
    opts: RegisterShapeOptions,
    identity: AuthorId,
}

#[derive(Clone)]
struct UpstreamCoverageHandle {
    coverage: CoverageKey,
    subscription: SubscriptionKey,
}

struct CoverageGroup {
    shape: ValidatedQuery,
    binding: Binding,
    subscribers: BTreeSet<SubscriptionKey>,
}

/// Locally-authored transactions awaiting upload, oldest first. Shared with
/// upstream [`PeerConnection`]s, each of which tracks how far it has shipped.
type Outbox = Rc<RefCell<Vec<PendingUpload>>>;

#[derive(Clone)]
struct PendingUpload {
    tx_id: TxId,
    unit: Option<SyncMessage>,
}

/// Application-visible fate and durability for a local write transaction.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct WriteState {
    /// Latest authority fate observed by this `Db`.
    pub fate: Fate,
    /// Highest durability tier observed by this `Db`.
    pub durability: DurabilityTier,
}

/// Explicit client durability cadence while the first server snapshot is
/// loading.
///
/// A crash can lose up to `M - 1` writes since the last completed boundary,
/// where `M` is this value. Older completed boundaries recover from the
/// storage WAL. The final partial initial-sync batch is always flushed before
/// the client returns to per-write durability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InitialSyncFlushCadence(NonZeroUsize);

impl InitialSyncFlushCadence {
    /// The client default: a durability boundary every 512 initial-sync writes.
    pub const DEFAULT: Self = Self(NonZeroUsize::new(512).expect("non-zero"));

    /// Create a cadence with one durability boundary per `writes` initial-sync
    /// writes.
    pub const fn every(writes: NonZeroUsize) -> Self {
        Self(writes)
    }

    /// Number of writes between completed durability boundaries.
    pub const fn writes(self) -> usize {
        self.0.get()
    }
}

/// Usage-site query coverage attachment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryAttachment {
    subscriptions: Vec<SubscriptionKey>,
}

impl QueryAttachment {
    /// Wire subscription id owned by this attachment.
    pub fn subscription(&self) -> SubscriptionKey {
        self.subscriptions[0]
    }
}

/// Future that resolves when a database observes a write-state change.
///
/// This is a wake primitive: callers should read [`Db::write_state`] before
/// registering it, read again after registration, and then re-read after it
/// resolves.
pub struct WriteStateChange {
    waiters: WriteStateWaiters,
    tx_id: TxId,
    waiter_id: u64,
    receiver: oneshot::Receiver<()>,
}

impl Future for WriteStateChange {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(_) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for WriteStateChange {
    fn drop(&mut self) {
        let mut waiters = self.waiters.borrow_mut();
        let Some(tx_waiters) = waiters.get_mut(&self.tx_id) else {
            return;
        };
        tx_waiters.retain(|waiter| waiter.id != self.waiter_id);
        let empty = tx_waiters.is_empty();
        if empty {
            waiters.remove(&self.tx_id);
        }
    }
}

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Open a database over the supplied storage and recover local state.
    ///
    /// ```rust
    /// # use jazz::db::{Db, DbConfig, DbIdentity, SeededRowIdSource};
    /// # use jazz::db::doctest_support::{block_on, schema, MemoryStorage};
    /// # use jazz::ids::{AuthorId, NodeUuid};
    /// let schema = schema();
    /// let column_families = schema.column_families();
    /// let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    /// let storage = MemoryStorage::new(&refs);
    ///
    /// let db = block_on(Db::open(DbConfig {
    ///     schema,
    ///     storage,
    ///     identity: DbIdentity {
    ///         node: NodeUuid::from_bytes([1; 16]),
    ///         author: AuthorId::from_bytes([2; 16]),
    ///     },
    ///     id_source: Some(Box::new(SeededRowIdSource::new(1))),
    ///     large_value_checkpoint_op_interval: 1024,
    /// }))?;
    ///
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert!(db.read(&todos)?.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn open(config: DbConfig<S>) -> Result<Self, Error> {
        let schema_version_id = config.schema.version_id();
        let node = NodeState::new_with_large_value_checkpoint_op_interval(
            config.identity.node,
            config.schema.clone(),
            config.storage,
            false,
            config.large_value_checkpoint_op_interval,
        )?;
        let node = Node::new(node);
        node.restore_pending_uploads(config.identity)?;
        Ok(Self {
            schema: config.schema,
            schema_version_id,
            identity: config.identity,
            node,
            row_id_source: RefCell::new(
                config
                    .id_source
                    .unwrap_or_else(|| Box::new(ProductionRowIdSource)),
            ),
            next_now_ms: Cell::new(1),
        })
    }

    /// Open a database as a history-complete serving core.
    ///
    /// This mode is intended for server shells and tests that own authoritative
    /// in-memory history rather than a partial client replica.
    pub async fn open_history_complete(config: DbConfig<S>) -> Result<Self, Error> {
        let schema_version_id = config.schema.version_id();
        let node = NodeState::new_history_complete(
            config.identity.node,
            config.schema.clone(),
            config.storage,
        )?;
        Ok(Self {
            schema: config.schema,
            schema_version_id,
            identity: config.identity,
            node: Node::new(node),
            row_id_source: RefCell::new(
                config
                    .id_source
                    .unwrap_or_else(|| Box::new(ProductionRowIdSource)),
            ),
            next_now_ms: Cell::new(1),
        })
    }

    /// Flush node-local maintenance state, write a clean-close marker, and
    /// close the underlying storage.
    pub fn close(&self) -> Result<(), Error> {
        Ok(self.node.node.borrow_mut().close()?)
    }

    /// Configure this client database's first-snapshot durability cadence.
    ///
    /// Servers do not call this client-only setting and retain their existing
    /// storage durability behavior.
    pub fn set_initial_sync_flush_cadence(
        &self,
        cadence: InitialSyncFlushCadence,
    ) -> Result<(), Error> {
        Ok(self
            .node
            .node
            .borrow_mut()
            .set_initial_sync_flush_cadence(cadence.writes())?)
    }

    /// Create a snapshot-base branch immediately in local durable storage.
    ///
    /// Branch creation is local-first: no serving node round trip is required.
    /// The authenticated database identity is recorded as the immutable creator.
    pub fn create_branch(&self) -> Result<crate::ids::BranchId, Error> {
        let branch = crate::ids::BranchId(uuid::Uuid::now_v7());
        self.create_branch_with_id(branch)?;
        Ok(branch)
    }

    /// Create a local snapshot-base branch with a caller-supplied stable id.
    pub fn create_branch_with_id(&self, branch: crate::ids::BranchId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .create_branch_as(branch, self.identity.author)?;
        Ok(())
    }

    /// Insert a row into a locally-created branch and queue it for ordinary sync.
    pub fn insert_on_branch(
        &self,
        branch: crate::ids::BranchId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let row = self.row_id_source.borrow_mut().next_row_id();
        let cells = self.apply_insert_defaults(table, cells)?;
        let tx_id = self.node.node.borrow_mut().commit_mergeable_on_branch(
            branch,
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(self.identity.author)
                .cells(cells),
        )?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    /// Seed a settled mergeable row for server bootstrap/import flows.
    ///
    /// This bypasses the client pending-upload path and immediately finalizes
    /// the commit in local history. It is intended only for history-complete
    /// server bootstrap/import state, not for general application writes or
    /// pending client write semantics.
    pub fn seed_settled_mergeable_for_bootstrap(
        &self,
        table: &str,
        row: RowUuid,
        made_by: AuthorId,
        cells: RowCells,
    ) -> Result<TxId, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        let tx_id = self.node.node.borrow_mut().commit_mergeable(
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(made_by)
                .cells(cells),
        )?;
        self.node
            .node
            .borrow_mut()
            .finalize_local_mergeable_commit(tx_id)?;
        self.refresh_subscriptions()?;
        self.node.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }

    /// Seed a branch-local mergeable row for history-complete server bootstrap
    /// or import flows.
    ///
    /// The resulting row is evaluated through the ordinary branch read-view
    /// lowering path; this does not provide an application-facing branch write
    /// facade.
    pub fn seed_branch_mergeable_for_bootstrap(
        &self,
        branch: crate::ids::BranchId,
        table: &str,
        row: RowUuid,
        made_by: AuthorId,
        cells: RowCells,
    ) -> Result<TxId, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        let mut node = self.node.node.borrow_mut();
        if node.branch_record(branch).is_none() {
            node.create_branch(branch)?;
        }
        let tx_id = node.commit_mergeable_on_branch(
            branch,
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(made_by)
                .cells(cells),
        )?;
        drop(node);
        self.refresh_subscriptions()?;
        self.node.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only authority finalization for a locally committed mergeable
    /// transaction.
    ///
    /// This allows scale fixtures to use the ordinary batched transaction API
    /// before performing the same self-acceptance step as
    /// [`Db::seed_settled_mergeable_for_bootstrap`].
    pub fn finalize_local_mergeable_commit_for_test(&self, tx_id: TxId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .finalize_local_mergeable_commit(tx_id)?;
        self.refresh_subscriptions()?;
        self.node.mark_subscriber_connections_dirty();
        Ok(())
    }

    /// Return the locally observed fate and durability for a write transaction.
    pub fn write_state(&self, tx_id: TxId) -> Result<WriteState, Error> {
        let Some((fate, _, durability)) = self.node.node.borrow_mut().transaction_state(tx_id)
        else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "transaction is not known locally",
            ));
        };
        Ok(WriteState { fate, durability })
    }

    /// Wait until this database observes another state transition for `tx_id`.
    ///
    /// Callers should always check [`Db::write_state`] before and after
    /// registering this future; this method is a wake primitive, not a predicate.
    pub fn next_write_state_change(&self, tx_id: TxId) -> WriteStateChange {
        self.node.register_write_state_waiter(tx_id)
    }

    /// Register a one-shot same-thread callback for the next state transition of
    /// `tx_id`.
    ///
    /// This is the callback equivalent of [`Db::next_write_state_change`].
    /// Callers should still read [`Db::write_state`] before and after
    /// registration to avoid lost wakeups.
    pub fn on_next_write_state_change(&self, tx_id: TxId, callback: impl FnOnce() + 'static) {
        self.node
            .register_write_state_callback(tx_id, Box::new(callback));
    }

    /// Start a query rooted at `table`.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db};
    /// # use jazz::query::{col, eq, lit};
    /// let db = block_on(open_todos_db())?;
    /// let open_todos = db
    ///     .table("todos")
    ///     .filter(eq(col("done"), lit(false)))
    ///     .select(["title", "done"]);
    ///
    /// let open_todos = db.prepare_query(&open_todos)?;
    /// assert!(db.read(&open_todos)?.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn table(&self, table: impl Into<String>) -> Query {
        Query::from(table)
    }

    /// Prepare a query for repeated reads or subscriptions.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// let db = block_on(open_todos_db())?;
    /// let write = db.insert("todos", todo_cells("write docs", false))?;
    /// let todo = write.row_uuid();
    ///
    /// let query = db.prepare_query(&db.table("todos"))?;
    /// let rows = db.read(&query)?;
    /// assert_eq!(rows.len(), 1);
    /// assert_eq!(rows[0].row_uuid(), todo);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn prepare_query(&self, query: &Query) -> Result<PreparedQuery, Error> {
        self.prepare_query_bound(query, BTreeMap::new())
    }

    /// Prepare a query with explicit parameter bindings.
    pub fn prepare_query_bound(
        &self,
        query: &Query,
        params: BTreeMap<String, Value>,
    ) -> Result<PreparedQuery, Error> {
        let (schema, schema_version) = self.current_write_schema_for_query()?;
        self.prepare_query_bound_for_schema(query, params, &schema, schema_version)
    }

    /// Prepare a query against the schema this database handle was opened with.
    ///
    /// Typed client facades are pinned to that schema even when a catalogue
    /// snapshot advances or rolls back the separate current-write pointer.
    #[cfg(feature = "client")]
    pub(crate) fn prepare_query_for_open_schema(
        &self,
        query: &Query,
    ) -> Result<PreparedQuery, Error> {
        self.prepare_query_bound_for_schema(
            query,
            BTreeMap::new(),
            &self.schema,
            self.schema_version_id,
        )
    }

    fn prepare_query_bound_for_schema(
        &self,
        query: &Query,
        params: BTreeMap<String, Value>,
        schema: &JazzSchema,
        schema_version: SchemaVersionId,
    ) -> Result<PreparedQuery, Error> {
        let shape = query.validate_with_schema_version(schema, schema_version)?;
        let binding = shape.bind(params)?;
        let (local_plan, global_plan) = if should_install_prepared_plan(&shape)
            && !self.node.node.borrow().uses_schema_projected_read(&shape)
        {
            let mut node = self.node.node.borrow_mut();
            (
                Some(node.prepared_query_plan(
                    &shape,
                    &binding,
                    DurabilityTier::Local,
                    AuthorId::SYSTEM,
                )?),
                Some(node.prepared_query_plan(
                    &shape,
                    &binding,
                    DurabilityTier::Global,
                    AuthorId::SYSTEM,
                )?),
            )
        } else {
            (None, None)
        };
        Ok(PreparedQuery {
            shape,
            binding,
            local_plan,
            global_plan,
        })
    }

    /// Synchronously read rows for a prepared query.
    ///
    /// This is a synchronous local-preview read. Upstream/server settled
    /// coverage is tracked separately by query attachments and durability-aware
    /// subscription reads.
    pub fn read(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        self.node
            .node
            .borrow_mut()
            .query_rows_local_preview(
                &prepared.shape,
                &prepared.binding,
                prepared.plan_for_tier(DurabilityTier::Local),
            )
            .map_err(Into::into)
    }

    /// Synchronously read rows and attribute work inside the node query path.
    ///
    /// The returned rows are identical to [`Self::read`]. This diagnostic
    /// variant exists so persisted-read benchmarks can locate first-read cost
    /// without adding clocks to the ordinary read path.
    pub fn read_profiled(
        &self,
        prepared: &PreparedQuery,
    ) -> Result<(Vec<CurrentRow>, QueryReadProfile), Error> {
        self.node
            .node
            .borrow_mut()
            .query_rows_local_preview_profiled(
                &prepared.shape,
                &prepared.binding,
                prepared.plan_for_tier(DurabilityTier::Local),
            )
            .map_err(Into::into)
    }

    /// Synchronously read exactly one local row if present.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// let db = block_on(open_todos_db())?;
    /// let todo = db.insert("todos", todo_cells("first item", false))?.row_uuid();
    ///
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// let found = db.one(&todos)?;
    /// assert_eq!(found.map(|row| row.row_uuid()), Some(todo));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn one(&self, prepared: &PreparedQuery) -> Result<Option<CurrentRow>, Error> {
        Ok(self.read(prepared)?.into_iter().next())
    }

    /// Resolve creator/updater provenance for a row returned by this database.
    pub fn row_provenance(&self, row: &CurrentRow) -> Result<Option<RowProvenance>, Error> {
        self.node
            .node
            .borrow_mut()
            .row_provenance(row)
            .map_err(Into::into)
    }

    /// Read the bytes behind a materialized large-value handle.
    ///
    /// This is explicit content access: ordinary row reads return handles and
    /// never pull extent bytes. If the referenced extents are not hydrated
    /// locally, this returns a protocol error whose source is the node's
    /// `MissingContentExtent` condition.
    pub fn hydrate_large_value_handle(&self, handle: &[u8]) -> Result<Vec<u8>, Error> {
        let mut attempts = 0usize;
        loop {
            let result = {
                self.node
                    .node
                    .borrow_mut()
                    .hydrate_large_value_handle(handle)
            };
            match result {
                Ok(bytes) => return Ok(bytes),
                Err(crate::node::Error::MissingContentExtent(extent)) if attempts < 16 => {
                    attempts += 1;
                    self.node.queue_content_extent_fetch(extent);
                    for _ in 0..64 {
                        self.tick()?;
                        let result = {
                            self.node
                                .node
                                .borrow_mut()
                                .hydrate_large_value_handle(handle)
                        };
                        match result {
                            Ok(bytes) => return Ok(bytes),
                            Err(crate::node::Error::MissingContentExtent(_)) => {
                                std::thread::yield_now();
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Read local settled history at an exact global sequence cut.
    ///
    /// History-incomplete facades return `HistoricalReadRequiresServer` from
    /// the node layer instead of answering from a partial local prefix
    /// (ch11/INV-BRANCH-4).
    pub fn at(
        &self,
        position: GlobalSeq,
        prepared: &PreparedQuery,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.at_prepared(position, prepared)
    }

    fn at_prepared(
        &self,
        position: GlobalSeq,
        prepared: &PreparedQuery,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.node
            .node
            .borrow_mut()
            .at(position)
            .read(&prepared.shape, &prepared.binding)
            .map_err(Into::into)
    }

    /// Tier-gated one-shot read.
    ///
    /// ```rust
    /// # use jazz::db::{ReadOpts, LocalUpdates, Propagation};
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// db.insert("todos", todo_cells("visible locally", false))?;
    ///
    /// let opts = ReadOpts {
    ///     tier: DurabilityTier::Local,
    ///     local_updates: LocalUpdates::Immediate,
    ///     propagation: Propagation::LocalOnly,
    ///     include_deleted: false,
    ///     ..ReadOpts::default()
    /// };
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// let rows = block_on(db.all(&todos, opts))?;
    /// assert_eq!(rows.len(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn all(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_for_identity(prepared, opts, self.identity.author)
            .await
    }

    /// Tier-gated one-shot read evaluated as `author`.
    pub async fn all_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let tier = effective_read_tier(&opts);
        let mut node = self.node.node.borrow_mut();
        match &opts.read_view.source {
            ReadViewSourceSpec::Current => {}
            ReadViewSourceSpec::Branch { branch } if !opts.include_deleted => {
                return node
                    .query_rows_on_branch_for_link(
                        crate::ids::BranchId(*branch),
                        &prepared.shape,
                        &prepared.binding,
                        author,
                    )
                    .map_err(Into::into);
            }
            _ => ensure_default_read_view(&opts)?,
        }
        if opts.include_deleted {
            node.query_rows_for_link_including_deleted(
                &prepared.shape,
                &prepared.binding,
                tier,
                author,
            )
        } else {
            let (shape, binding, _plan) = node
                .prepare_query_binding_for_link_with_shared_claim_fragments(
                    &prepared.shape,
                    &prepared.binding,
                    tier,
                    author,
                )?;
            node.query_rows_for_link_with_prepared_plan(&shape, &binding, tier, author, None)
        }
        .map_err(Into::into)
    }

    /// Tier-gated one-shot relation read evaluated as the database identity.
    pub async fn all_relation_snapshot(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        self.all_relation_snapshot_for_identity(prepared, opts, self.identity.author)
            .await
    }

    /// Tier-gated one-shot relation read evaluated as `author`.
    pub async fn all_relation_snapshot_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<RelationSnapshot, Error> {
        ensure_supported_read_view(&opts)?;
        if opts.include_deleted {
            return Err(Error::new(
                ErrorCode::Query,
                "relation snapshots do not support include_deleted yet",
            ));
        }
        let tier = effective_read_tier(&opts);
        self.node
            .node
            .borrow_mut()
            .query_relation_snapshot_for_link_in_read_view(
                &prepared.shape,
                &prepared.binding,
                tier,
                author,
                &opts.read_view,
            )
            .map_err(Into::into)
    }

    /// Tier-gated canonical structured result read.
    ///
    /// This is the sole Jazz-boundary materialization of relation facts into
    /// recursive output. Wire delivery deliberately remains on its v3 carrier
    /// until the structured delivery migration.
    pub async fn all_result_tree(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<ResultTree, Error> {
        let snapshot = self.all_relation_snapshot(prepared, opts).await?;
        materialize_result_tree(prepared.shape.query(), snapshot)
    }

    /// Tier-gated one-shot output-changing relation read evaluated as the database identity.
    pub async fn all_relation_query(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        self.all_relation_query_for_identity(query, opts, self.identity.author)
            .await
    }

    /// Tier-gated one-shot output-changing relation read evaluated as `author`.
    pub async fn all_relation_query_for_identity(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<RelationSnapshot, Error> {
        ensure_default_read_view(&opts)?;
        let query = relation_query_to_query(query)?;
        let prepared = self.prepare_query(&query)?;
        // Output-changing relation queries currently normalize to a single
        // root row set.  They have no array payload edges, so request ordinary
        // app rows instead of the relation-snapshot fact output (which is
        // reserved for correlated array/path materialization).
        let rows = self.all_for_identity(&prepared, opts, author).await?;
        Ok(RelationSnapshot {
            root_count: rows.len(),
            rows,
            edges: Vec::new(),
        })
    }

    /// Subscribe to a query and return a stream of materialized subscription events.
    ///
    /// ```rust
    /// # use jazz::db::{LocalUpdates, Propagation, ReadOpts, SubscriptionEvent};
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// let query = db.prepare_query(&db.table("todos"))?;
    /// let mut subscription = block_on(db.subscribe(
    ///     &query,
    ///     ReadOpts {
    ///         tier: DurabilityTier::Local,
    ///         local_updates: LocalUpdates::Immediate,
    ///         propagation: Propagation::LocalOnly,
    ///         include_deleted: false,
    ///         ..ReadOpts::default()
    ///     },
    /// ))?;
    /// let opened = block_on(subscription.next_event()).unwrap();
    /// let SubscriptionEvent::Delta { reset, added, .. } = opened else {
    ///     panic!("expected reset delta");
    /// };
    /// assert!(reset);
    /// assert!(added.is_empty());
    ///
    /// db.insert("todos", todo_cells("notify subscribers", false))?;
    /// let changed = block_on(subscription.next_event()).unwrap();
    /// let SubscriptionEvent::Delta { added, updated, removed, .. } = changed else {
    ///     panic!("expected subscription delta");
    /// };
    /// assert_eq!(added.len(), 1);
    /// assert!(updated.is_empty());
    /// assert!(removed.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn subscribe(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        self.subscribe_for_identity(prepared, opts, self.identity.author)
            .await
    }

    /// Subscribe to a query evaluated as `author`.
    pub async fn subscribe_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<SubscriptionStream, Error> {
        self.open_subscription(prepared, opts, author).await
    }

    /// Subscribe to an output-changing relation query.
    pub async fn subscribe_relation_query(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        self.subscribe_relation_query_for_identity(query, opts, self.identity.author)
            .await
    }

    /// Subscribe to an output-changing relation query evaluated as `author`.
    pub async fn subscribe_relation_query_for_identity(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<SubscriptionStream, Error> {
        self.open_relation_subscription(query, opts, author).await
    }

    /// Attach a one-shot usage-site query coverage request.
    ///
    /// Bindings call this before an edge/global one-shot read, drive
    /// [`Db::tick`] until [`Db::query_attachment_is_covered`] is true, read, then
    /// call [`Db::detach_query`].
    pub fn attach_query_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<QueryAttachment, Error> {
        ensure_supported_read_view(&opts)?;
        let upstream_opts =
            upstream_register_shape_options(effective_read_tier(&opts), opts.read_view.clone());
        Ok(QueryAttachment {
            subscriptions: vec![self.attach_query_shape_binding_with_opts(
                &prepared.shape,
                &prepared.binding,
                upstream_opts,
                self.identity.author,
            )?],
        })
    }

    /// Attach a one-shot usage-site query coverage request evaluated as `author`.
    pub fn attach_query_with_opts_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<QueryAttachment, Error> {
        ensure_supported_read_view(&opts)?;
        let upstream_opts =
            upstream_register_shape_options(effective_read_tier(&opts), opts.read_view.clone());
        let (shape, binding, _) = self.node.node.borrow_mut().prepare_query_binding_for_link(
            &prepared.shape,
            &prepared.binding,
            upstream_opts.tier,
            author,
        )?;
        Ok(QueryAttachment {
            subscriptions: vec![self.attach_query_shape_binding_with_opts(
                &shape,
                &binding,
                upstream_opts,
                author,
            )?],
        })
    }

    fn attach_query_shape_binding_with_opts(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        identity: AuthorId,
    ) -> Result<SubscriptionKey, Error> {
        let subscription = self.node.next_subscription_key(shape, opts.read_view_key());
        self.node
            .upstream_subscriptions
            .borrow_mut()
            .push(PendingUpstreamCommand::Subscribe(
                PendingUpstreamSubscription {
                    subscription,
                    shape: shape.clone(),
                    binding: binding.clone(),
                    opts: opts.clone(),
                    identity,
                },
            ));
        self.node
            .latest_coverage_subscriptions
            .borrow_mut()
            .insert(coverage_key(shape, binding, opts), subscription);
        self.node.schedule_tick(TickUrgency::Immediate);
        Ok(subscription)
    }

    /// Attach a one-shot usage-site query coverage request at the default tier.
    pub fn attach_query(&self, prepared: &PreparedQuery) -> Result<QueryAttachment, Error> {
        self.attach_query_with_opts(prepared, ReadOpts::default())
    }

    /// Return whether a query attachment has received at least one upstream view update.
    pub fn query_attachment_is_covered(&self, attachment: &QueryAttachment) -> bool {
        let node = self.node.node.borrow();
        attachment.subscriptions.iter().all(|subscription| {
            node.binding_view_key_for_subscription(*subscription)
                .is_ok_and(|binding_view_key| node.has_settled_result_set(binding_view_key))
        })
    }

    /// Detach a one-shot query coverage request.
    pub fn detach_query(&self, attachment: QueryAttachment) {
        for subscription in attachment.subscriptions {
            self.node.node.borrow_mut().apply_unsubscribe(subscription);
            self.node
                .latest_coverage_subscriptions
                .borrow_mut()
                .retain(|_, attached| *attached != subscription);
            self.node
                .upstream_subscriptions
                .borrow_mut()
                .push(PendingUpstreamCommand::Unsubscribe(subscription));
        }
        self.node.schedule_tick(TickUrgency::Immediate);
    }

    async fn open_subscription(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<SubscriptionStream, Error> {
        ensure_supported_subscription_read_opts(&opts)?;
        self.validate_prepared_shape_for_registration(prepared)?;
        let read_tier = effective_read_tier(&opts);
        self.node
            .node
            .borrow_mut()
            .ensure_peer_maintained_subscription_view_supported(
                &prepared.shape,
                &prepared.binding,
                read_tier,
                author,
                &opts.read_view,
            )?;
        let (local_shape, local_binding, _local_plan) = self
            .node
            .node
            .borrow_mut()
            .prepare_query_binding_for_link_with_shared_claim_fragments(
                &prepared.shape,
                &prepared.binding,
                read_tier,
                author,
            )?;
        let (subscription, snapshot) = self
            .node
            .node
            .borrow_mut()
            .open_local_maintained_view_subscription(
                &local_shape,
                &local_binding,
                author,
                read_tier,
                &opts.read_view,
                Some(_local_plan),
            )?;
        let local_subscription_id = subscription.subscription_id();
        let local_node = Rc::clone(&self.node.node);
        let mut local_cleanup = CleanupGuard::new(Box::new(move || {
            local_node
                .borrow_mut()
                .unsubscribe_groove_subscription(local_subscription_id);
        }));
        let maintained_subscription = Some(subscription);
        let terminal_rows = !local_shape.query().array_subqueries.is_empty();
        let mut state_shape = local_shape;
        let mut state_binding = local_binding;
        let mut remote_read_tier = None;
        let mut upstream_subscription_handles = Vec::new();
        let propagates_upstream = opts.propagation == Propagation::Full;
        if opts.propagation == Propagation::Full {
            let upstream_opts =
                upstream_register_shape_options(effective_read_tier(&opts), opts.read_view.clone());
            let (shape, binding) = if upstream_opts.tier == read_tier {
                (state_shape.clone(), state_binding.clone())
            } else {
                let (shape, binding, _) = self
                    .node
                    .node
                    .borrow_mut()
                    .prepare_query_binding_for_link_with_shared_claim_fragments(
                        &prepared.shape,
                        &prepared.binding,
                        upstream_opts.tier,
                        author,
                    )?;
                (shape, binding)
            };
            state_shape = shape.clone();
            state_binding = binding.clone();
            remote_read_tier = Some(upstream_opts.tier);
            let upstream_subscriptions =
                self.open_subscription_upstream_coverage(&shape, &binding, upstream_opts, author)?;
            upstream_subscription_handles = upstream_subscriptions;
        }
        let settled_tier = remote_read_tier.unwrap_or(read_tier);
        let settled = subscription_is_settled(
            &self.node.node.borrow(),
            &state_shape,
            &state_binding,
            settled_tier,
            opts.read_view.clone(),
        );
        let (sender, receiver) = unbounded();
        let state_snapshot = relation_snapshot_with_delta_slack(&snapshot);
        let snapshot_index = RelationSnapshotIndex::from_snapshot(&state_snapshot);
        let state = Rc::new(RefCell::new(SubscriptionState {
            terminal_rows,
            kind: SubscriptionKind::Prepared {
                shape: state_shape,
                binding: state_binding,
                maintained_subscription,
            },
            groove_runtime_token: self.node.node.borrow().groove_runtime_token(),
            propagates_upstream,
            author,
            read_tier,
            remote_read_tier,
            read_view: opts.read_view.clone(),
            snapshot: state_snapshot,
            snapshot_index,
            snapshot_source: SubscriptionSnapshotSource::LocalMaintained,
            settled,
            sender,
        }));
        state
            .borrow()
            .sender
            .unbounded_send(subscription_reset_event(
                read_tier,
                settled,
                snapshot,
                terminal_rows,
            ))
            .map_err(|_| Error::new(ErrorCode::Protocol, "subscription receiver closed"))?;
        self.node
            .subscriptions
            .borrow_mut()
            .push(Rc::downgrade(&state));
        let cleanup = if upstream_subscription_handles.is_empty() {
            local_cleanup.take()
        } else {
            let owner = Rc::downgrade(&state);
            register_upstream_subscription_owner(
                &self.node.upstream_subscription_owners,
                &upstream_subscription_handles,
                &state,
            );
            let upstream_cleanup =
                self.upstream_subscription_cleanup(upstream_subscription_handles, owner);
            let local_cleanup = local_cleanup.take();
            Box::new(move || {
                local_cleanup();
                upstream_cleanup();
            })
        };
        Ok(SubscriptionStream {
            receiver,
            _state: state,
            cleanup: Some(cleanup),
        })
    }

    fn validate_prepared_shape_for_registration(
        &self,
        prepared: &PreparedQuery,
    ) -> Result<(), Error> {
        let ast = ShapeAst::from_validated(&prepared.shape);
        let validation = {
            let node = self.node.node.borrow();
            validate_shape_ast_for_registration(&node, prepared.shape.shape_id(), &ast)
        };
        validation.map(|_| ()).map_err(Error::from)
    }

    async fn open_relation_subscription(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<SubscriptionStream, Error> {
        ensure_supported_subscription_read_opts(&opts)?;
        let query = relation_query_to_query(query)?;
        let prepared = self.prepare_query(&query)?;
        self.open_subscription(&prepared, opts, author).await
    }

    fn open_subscription_upstream_coverage(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        identity: AuthorId,
    ) -> Result<Vec<UpstreamCoverageHandle>, Error> {
        self.node
            .node
            .borrow_mut()
            .ensure_peer_maintained_subscription_view_supported(
                shape,
                binding,
                opts.tier,
                identity,
                &opts.read_view,
            )?;
        let coverage = coverage_key(shape, binding, opts.clone());
        if self
            .node
            .upstream_coverage_refcounts
            .borrow()
            .contains_key(&coverage)
        {
            if let Some(subscription) = self
                .node
                .latest_coverage_subscriptions
                .borrow()
                .get(&coverage)
                .copied()
            {
                *self
                    .node
                    .upstream_coverage_refcounts
                    .borrow_mut()
                    .entry(coverage.clone())
                    .or_insert(0) += 1;
                return Ok(vec![UpstreamCoverageHandle {
                    coverage,
                    subscription,
                }]);
            }
        }
        let subscription =
            self.attach_query_shape_binding_with_opts(shape, binding, opts, identity)?;
        self.node
            .upstream_coverage_refcounts
            .borrow_mut()
            .insert(coverage.clone(), 1);
        Ok(vec![UpstreamCoverageHandle {
            coverage,
            subscription,
        }])
    }

    fn upstream_subscription_cleanup(
        &self,
        upstream_subscriptions: Vec<UpstreamCoverageHandle>,
        owner: Weak<RefCell<SubscriptionState>>,
    ) -> Box<dyn FnOnce()> {
        let node = Rc::clone(&self.node.node);
        let latest_coverage_subscriptions = Rc::clone(&self.node.latest_coverage_subscriptions);
        let upstream_coverage_refcounts = Rc::clone(&self.node.upstream_coverage_refcounts);
        let upstream_subscription_owners = Rc::clone(&self.node.upstream_subscription_owners);
        let pending_upstream_subscriptions = Rc::clone(&self.node.upstream_subscriptions);
        let scheduler = Rc::clone(&self.node.scheduler);
        Box::new(move || {
            for handle in upstream_subscriptions {
                unregister_upstream_subscription_owner(
                    &upstream_subscription_owners,
                    handle.subscription,
                    &owner,
                );
                let mut refcounts = upstream_coverage_refcounts.borrow_mut();
                let Some(count) = refcounts.get_mut(&handle.coverage) else {
                    continue;
                };
                *count = count.saturating_sub(1);
                if *count > 0 {
                    continue;
                }
                refcounts.remove(&handle.coverage);
                drop(refcounts);
                let upstream_subscription = handle.subscription;
                node.borrow_mut().apply_unsubscribe(upstream_subscription);
                latest_coverage_subscriptions
                    .borrow_mut()
                    .retain(|coverage, subscription| {
                        coverage != &handle.coverage && *subscription != upstream_subscription
                    });
                pending_upstream_subscriptions
                    .borrow_mut()
                    .push(PendingUpstreamCommand::Unsubscribe(upstream_subscription));
            }
            schedule_tick_in(&scheduler, TickUrgency::Immediate);
        })
    }

    /// Insert a row locally, generating a uuidv7-shaped row id.
    ///
    /// The generated id is available from [`WriteHandle::row_uuid`].
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// let write = db.insert("todos", jazz::row! { title: "new todo", done: false })?;
    /// let row = write.row_uuid();
    /// block_on(write.wait(DurabilityTier::Local))?;
    ///
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert_eq!(db.read(&todos)?.len(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn insert(&self, table: &str, cells: RowCells) -> Result<WriteHandle<S>, Error> {
        let row = self.row_id_source.borrow_mut().next_row_id();
        self.write_mergeable(
            self.identity.author,
            None,
            table,
            row,
            cells,
            Vec::new(),
            None,
        )
    }

    /// Insert a row while attributing provenance to `made_by`.
    ///
    /// The Db's authenticated identity remains the write-policy subject. Client
    /// facades can only write as themselves; trusted-backend attribution is a
    /// serving-node concern on inbound commit-unit ingestion.
    pub fn insert_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let row = self.row_id_source.borrow_mut().next_row_id();
        self.write_mergeable_as_session_subject(made_by, table, row, cells, Vec::new(), None)
    }

    /// Insert a row with a caller-supplied id.
    ///
    /// This is a niche path for imports from legacy systems or other cases
    /// where row identity already exists. New local rows should generally use
    /// [`Db::insert`] so the database generates the id.
    pub fn insert_with_id(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, self.identity.author)?;
        self.write_mergeable(
            self.identity.author,
            None,
            table,
            row,
            cells,
            Vec::new(),
            None,
        )
    }

    /// Insert a caller-id row while attributing provenance to `made_by`.
    ///
    /// See [`Db::insert_attributed`] for the security boundary.
    pub fn insert_with_id_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, self.identity.author)?;
        self.write_mergeable_as_session_subject(made_by, table, row, cells, Vec::new(), None)
    }

    /// Insert a row while evaluating write policy as `identity`.
    pub fn insert_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let row = self.row_id_source.borrow_mut().next_row_id();
        self.insert_with_id_for_identity(identity, table, row, cells)
    }

    /// Insert a caller-id row with an explicit millisecond provenance time.
    pub fn insert_with_id_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, self.identity.author)?;
        self.write_mergeable_at_ms(
            self.identity.author,
            None,
            table,
            row,
            cells,
            Vec::new(),
            None,
            now_ms,
        )
    }

    /// Insert a caller-id row while evaluating write policy as `identity`.
    ///
    /// This is a trusted serving-node API for terminated backend/request
    /// sessions. It records provenance as `identity` and evaluates policy as
    /// the same identity, without changing the Db's own authority.
    pub fn insert_with_id_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, identity)?;
        let cells = self.apply_insert_defaults(table, cells)?;
        let allowed = self
            .node
            .node
            .borrow_mut()
            .dry_run_insert_allows(
                MergeableCommit::new(table, row, self.next_now_ms())
                    .made_by(identity)
                    .permission_subject(identity)
                    .cells(cells.clone()),
            )
            .map_err(Error::from)?;
        if !allowed {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("policy denied INSERT on table {table}"),
            ));
        }
        self.write_mergeable(
            identity,
            Some(identity),
            table,
            row,
            cells,
            Vec::new(),
            None,
        )
    }

    /// Insert a caller-id row for `identity` with an explicit millisecond provenance time.
    pub fn insert_with_id_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, identity)?;
        let cells = self.apply_insert_defaults(table, cells)?;
        let allowed = self
            .node
            .node
            .borrow_mut()
            .dry_run_insert_allows(
                MergeableCommit::new(table, row, now_ms)
                    .made_by(identity)
                    .permission_subject(identity)
                    .cells(cells.clone()),
            )
            .map_err(Error::from)?;
        if !allowed {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("policy denied INSERT on table {table}"),
            ));
        }
        self.write_mergeable_at_ms(
            identity,
            Some(identity),
            table,
            row,
            cells,
            Vec::new(),
            None,
            now_ms,
        )
    }

    /// Return whether an insert with these cells would pass write policy.
    ///
    /// This is a dry-run over the current local preview: it builds the
    /// hypothetical version used by the write path, evaluates policy as this
    /// Db's authenticated author, and does not store a version or advance time.
    pub fn can_insert(&self, table: &str, cells: RowCells) -> Result<bool, Error> {
        self.can_insert_for_identity(table, cells, self.identity.author)
    }

    /// Return whether an insert with these cells would pass write policy for `identity`.
    ///
    /// This trusted serving-node dry-run mirrors `insert_with_id_for_identity`
    /// without storing a version or advancing time.
    pub fn can_insert_for_identity(
        &self,
        table: &str,
        cells: RowCells,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_insert_allows(
                MergeableCommit::new(table, RowUuid::from_bytes([0; 16]), 0)
                    .made_by(identity)
                    .permission_subject(identity)
                    .cells(cells),
            )
            .map_err(Into::into)
    }

    /// Update a row locally; omitted fields keep their current local value.
    ///
    /// ```rust
    /// # use std::collections::BTreeMap;
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::ids::RowUuid;
    /// # use jazz::groove::records::Value;
    /// let db = block_on(open_todos_db())?;
    /// let todo = RowUuid::from_bytes([1; 16]);
    /// db.insert_with_id("todos", todo, todo_cells("draft", false))?;
    ///
    /// db.update(
    ///     "todos",
    ///     todo,
    ///     BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    /// )?;
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert_eq!(db.read(&todos)?.len(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn update(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        if patch.is_empty() {
            return self.no_op_update_handle(table, row, self.identity.author);
        }
        let (cells, parent, authored_columns) = self.merge_existing_cells(table, row, patch)?;
        self.write_mergeable_with_authored_columns(
            self.identity.author,
            None,
            table,
            row,
            cells,
            parent.into_iter().collect(),
            None,
            authored_columns,
        )
    }

    /// Update a row with an explicit millisecond provenance time.
    pub fn update_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        if patch.is_empty() {
            return self.no_op_update_handle(table, row, self.identity.author);
        }
        let (cells, parent, authored_columns) = self.merge_existing_cells(table, row, patch)?;
        self.write_mergeable_at_ms_with_authorship(
            self.identity.author,
            None,
            table,
            row,
            cells,
            parent.into_iter().collect(),
            None,
            Some(authored_columns),
            now_ms,
        )
    }

    /// Update a row while attributing provenance to `made_by`.
    ///
    /// See [`Db::insert_attributed`] for the security boundary.
    pub fn update_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.check_attribution_allowed(made_by)?;
        if patch.is_empty() {
            return self.no_op_update_handle(table, row, self.identity.author);
        }
        let (cells, parent, authored_columns) = self.merge_existing_cells(table, row, patch)?;
        self.write_mergeable_as_session_subject_with_authored_columns(
            made_by,
            table,
            row,
            cells,
            parent.into_iter().collect(),
            None,
            authored_columns,
        )
    }

    /// Update a row while evaluating write policy as `identity`.
    pub fn update_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        if patch.is_empty() {
            return self.no_op_update_handle(table, row, identity);
        }
        let (cells, parent, authored_columns) =
            self.merge_existing_cells_for_identity(table, row, patch, identity)?;
        let parents = parent.into_iter().collect::<Vec<_>>();
        let dry_run = MergeableCommit::new(table, row, self.next_now_ms())
            .made_by(identity)
            .permission_subject(identity)
            .cells(cells.clone())
            .parents(parents.clone());
        let allowed = self
            .node
            .node
            .borrow_mut()
            .dry_run_mergeable_write_allows(dry_run)
            .map_err(Error::from)?;
        if !allowed {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("policy denied UPDATE on table {table}"),
            ));
        }
        self.write_mergeable_with_authored_columns(
            identity,
            Some(identity),
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
        )
    }

    /// Update a row for `identity` with an explicit millisecond provenance time.
    pub fn update_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        if patch.is_empty() {
            return self.no_op_update_handle(table, row, identity);
        }
        let (cells, parent, authored_columns) =
            self.merge_existing_cells_for_identity(table, row, patch, identity)?;
        let parents = parent.into_iter().collect::<Vec<_>>();
        let dry_run = MergeableCommit::new(table, row, now_ms)
            .made_by(identity)
            .permission_subject(identity)
            .cells(cells.clone())
            .parents(parents.clone());
        let allowed = self
            .node
            .node
            .borrow_mut()
            .dry_run_mergeable_write_allows(dry_run)
            .map_err(Error::from)?;
        if !allowed {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("policy denied UPDATE on table {table}"),
            ));
        }
        self.write_mergeable_at_ms_with_authorship(
            identity,
            Some(identity),
            table,
            row,
            cells,
            parents,
            None,
            Some(authored_columns),
            now_ms,
        )
    }

    /// Apply explicit edit operations to a text/blob column.
    ///
    /// Insert and delete positions are byte offsets relative to the current
    /// local parent value for the column.
    pub fn edit_text(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        edit: TextEdit,
    ) -> Result<WriteHandle<S>, Error> {
        self.table_schema(table)?;
        let tx_id = self.node.node.borrow_mut().commit_large_value_edit(
            LargeValueEditCommit::new(table, row, column, self.next_now_ms())
                .made_by(self.identity.author)
                .ops(edit.into_node_ops()),
        )?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    /// Upsert a row locally.
    ///
    /// This explicit-id path is primarily for importing rows from legacy
    /// systems. New local rows should generally use [`Db::insert`] and then
    /// update the returned [`WriteHandle::row_uuid`] when needed.
    ///
    /// ```rust
    /// # use std::collections::BTreeMap;
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::ids::RowUuid;
    /// # use jazz::groove::records::Value;
    /// let db = block_on(open_todos_db())?;
    /// let todo = RowUuid::from_bytes([1; 16]);
    ///
    /// db.upsert("todos", todo, todo_cells("created", false))?;
    /// db.upsert(
    ///     "todos",
    ///     todo,
    ///     BTreeMap::from([("title".to_owned(), Value::String("renamed".to_owned()))]),
    /// )?;
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert_eq!(db.one(&todos)?.unwrap().row_uuid(), todo);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn upsert(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let (cells, parents, authored_columns) = if self
            .upsert_target_for_identity(table, row, self.identity.author)?
            .is_some()
        {
            let (cells, parent, authored_columns) = self.merge_existing_cells(table, row, cells)?;
            (cells, parent.into_iter().collect(), Some(authored_columns))
        } else {
            (cells, Vec::new(), None)
        };
        self.write_mergeable_at_ms_with_authorship(
            self.identity.author,
            None,
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
            self.next_now_ms(),
        )
    }

    /// Upsert a row with an explicit millisecond provenance time.
    pub fn upsert_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let (cells, parents, authored_columns) = if self
            .upsert_target_for_identity(table, row, self.identity.author)?
            .is_some()
        {
            let (cells, parent, authored_columns) = self.merge_existing_cells(table, row, cells)?;
            (cells, parent.into_iter().collect(), Some(authored_columns))
        } else {
            (cells, Vec::new(), None)
        };
        self.write_mergeable_at_ms_with_authorship(
            self.identity.author,
            None,
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
            now_ms,
        )
    }

    /// Upsert a row while evaluating write policy as `identity`.
    pub fn upsert_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let (cells, parents, authored_columns) = if self
            .upsert_target_for_identity(table, row, identity)?
            .is_some()
        {
            let (cells, parent, authored_columns) =
                self.merge_existing_cells_for_identity(table, row, cells, identity)?;
            (cells, parent.into_iter().collect(), Some(authored_columns))
        } else {
            (cells, Vec::new(), None)
        };
        self.write_mergeable_at_ms_with_authorship(
            identity,
            Some(identity),
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
            self.next_now_ms(),
        )
    }

    /// Upsert a row for `identity` with an explicit millisecond provenance time.
    pub fn upsert_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let (cells, parents, authored_columns) = if self
            .upsert_target_for_identity(table, row, identity)?
            .is_some()
        {
            let (cells, parent, authored_columns) =
                self.merge_existing_cells_for_identity(table, row, cells, identity)?;
            (cells, parent.into_iter().collect(), Some(authored_columns))
        } else {
            (cells, Vec::new(), None)
        };
        self.write_mergeable_at_ms_with_authorship(
            identity,
            Some(identity),
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
            now_ms,
        )
    }

    /// Soft-delete a row locally.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::ids::RowUuid;
    /// let db = block_on(open_todos_db())?;
    /// let todo = RowUuid::from_bytes([1; 16]);
    /// db.insert_with_id("todos", todo, todo_cells("remove me", false))?;
    ///
    /// db.delete("todos", todo)?;
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert!(db.read(&todos)?.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn delete(&self, table: &str, row: RowUuid) -> Result<WriteHandle<S>, Error> {
        self.delete_at_ms_option(table, row, None)
    }

    /// Soft-delete a row with explicit millisecond provenance time.
    pub fn delete_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.delete_at_ms_option(table, row, Some(now_ms))
    }

    fn delete_at_ms_option(
        &self,
        table: &str,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let (parents, _) = self.row_layer_parents(table, row)?;
        match now_ms {
            Some(now_ms) => self.write_mergeable_at_ms(
                self.identity.author,
                None,
                table,
                row,
                BTreeMap::new(),
                parents,
                Some(DeletionEvent::Deleted),
                now_ms,
            ),
            None => self.write_mergeable(
                self.identity.author,
                None,
                table,
                row,
                BTreeMap::new(),
                parents,
                Some(DeletionEvent::Deleted),
            ),
        }
    }

    /// Soft-delete a row while attributing provenance to `made_by`.
    ///
    /// See [`Db::insert_attributed`] for the security boundary.
    pub fn delete_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let (parents, _) = self.row_layer_parents(table, row)?;
        self.write_mergeable_as_session_subject(
            made_by,
            table,
            row,
            BTreeMap::new(),
            parents,
            Some(DeletionEvent::Deleted),
        )
    }

    /// Soft-delete a row while evaluating write policy as `identity`.
    pub fn delete_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
    ) -> Result<WriteHandle<S>, Error> {
        self.delete_for_identity_at_ms_option(identity, table, row, None)
    }

    /// Soft-delete a row while evaluating write policy as `identity`, with explicit time.
    pub fn delete_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.delete_for_identity_at_ms_option(identity, table, row, Some(now_ms))
    }

    fn delete_for_identity_at_ms_option(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        if !self.can_delete_for_identity(table, row, identity)? {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("policy denied DELETE on table {table}"),
            ));
        }
        let (parents, _) = self.row_layer_parents(table, row)?;
        match now_ms {
            Some(now_ms) => self.write_mergeable_at_ms(
                identity,
                Some(identity),
                table,
                row,
                BTreeMap::new(),
                parents,
                Some(DeletionEvent::Deleted),
                now_ms,
            ),
            None => self.write_mergeable(
                identity,
                Some(identity),
                table,
                row,
                BTreeMap::new(),
                parents,
                Some(DeletionEvent::Deleted),
            ),
        }
    }

    /// Return whether this Db's author can read the current local row.
    pub fn can_read(&self, table: &str, row: RowUuid) -> Result<bool, Error> {
        self.can_read_for_identity(table, row, self.identity.author)
    }

    /// Return whether `author` can read the current local row.
    pub fn can_read_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorId,
    ) -> Result<bool, Error> {
        self.table_schema(table)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_read_current_allows(table, row, author)
            .map_err(Into::into)
    }

    /// Return whether this Db's author can update the current local row.
    pub fn can_update(&self, table: &str, row: RowUuid) -> Result<bool, Error> {
        self.can_update_for_identity(table, row, self.identity.author)
    }

    /// Return whether `author` can update the current local row.
    pub fn can_update_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorId,
    ) -> Result<bool, Error> {
        self.table_schema(table)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_write_current_allows(table, row, author)
            .map_err(Into::into)
    }

    /// Attach process-local auth claims for `identity`.
    pub fn set_identity_claims(&self, identity: AuthorId, claims: BTreeMap<String, Value>) {
        let changed = {
            let mut node = self.node.node.borrow_mut();
            let previous_revision = node.session_claim_revision(identity);
            node.set_session_claims(identity, claims);
            node.session_claim_revision(identity) != previous_revision
        };
        if changed {
            self.node.schedule_tick(TickUrgency::Deferred);
        }
    }

    /// Return whether this Db's author can delete the current local row.
    pub fn can_delete(&self, table: &str, row: RowUuid) -> Result<bool, Error> {
        self.can_delete_for_identity(table, row, self.identity.author)
    }

    /// Return whether `author` can delete the current local row.
    pub fn can_delete_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorId,
    ) -> Result<bool, Error> {
        self.table_schema(table)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_delete_current_allows(table, row, author)
            .map_err(Into::into)
    }

    /// Build a mergeable transaction that commits multiple writes under one id.
    pub fn mergeable_tx(&self) -> Result<MergeableTx<'_, S>, Error> {
        Ok(MergeableTx {
            db: self,
            tx_id: self.begin_mergeable()?,
            committed: false,
        })
    }

    /// Run `callback` in a mergeable transaction and commit all staged writes as one transaction.
    ///
    /// If `callback` returns an error, the transaction is dropped without committing. Reads and
    /// writes through the [`MergeableTx`] observe earlier writes staged in the same callback.
    pub fn transaction<T>(
        &self,
        callback: impl FnOnce(&mut MergeableTx<'_, S>) -> Result<T, Error>,
    ) -> Result<(T, TxId), Error> {
        let mut tx = self.mergeable_tx()?;
        let value = callback(&mut tx)?;
        let tx_id = tx.commit()?;
        Ok((value, tx_id))
    }

    /// Build a mergeable transaction authored and permission-checked as `author`.
    pub fn mergeable_tx_for_identity(&self, author: AuthorId) -> Result<MergeableTx<'_, S>, Error> {
        Ok(MergeableTx {
            db: self,
            tx_id: self.begin_mergeable_for_identity(author)?,
            committed: false,
        })
    }

    /// Run `callback` in a mergeable transaction authored and permission-checked as `author`.
    ///
    /// If `callback` returns an error, the transaction is dropped without committing.
    pub fn transaction_for_identity<T>(
        &self,
        author: AuthorId,
        callback: impl FnOnce(&mut MergeableTx<'_, S>) -> Result<T, Error>,
    ) -> Result<(T, TxId), Error> {
        let mut tx = self.mergeable_tx_for_identity(author)?;
        let value = callback(&mut tx)?;
        let tx_id = tx.commit()?;
        Ok((value, tx_id))
    }

    /// Publish an immutable schema-version payload through the catalogue lane.
    pub fn publish_schema(&self, schema: SchemaVersion) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
        self.node
            .node
            .borrow_mut()
            .apply_trusted_catalogue_message(SyncMessage::PublishSchema {
                author: self.identity.author,
                schema: Box::new(schema),
            })
            .map_err(Into::into)
    }

    /// Atomically publish a non-genesis schema and its lineage-defining lens.
    pub fn publish_schema_with_lens(
        &self,
        catalogue_seq: u64,
        publication: SchemaLineagePublication,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
        self.node
            .node
            .borrow_mut()
            .apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
                author: self.identity.author,
                catalogue_seq,
                publication: Box::new(publication),
            })
            .map_err(Into::into)
    }

    /// Publish an immutable migration lens through the catalogue lane.
    pub fn publish_lens(&self, lens: MigrationLens) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
        self.node
            .node
            .borrow_mut()
            .apply_trusted_catalogue_message(SyncMessage::PublishLens {
                author: self.identity.author,
                lens,
            })
            .map_err(Into::into)
    }

    /// Set the current write-schema pointer through the catalogue lane.
    pub fn set_current_write_schema(
        &self,
        pointer: CurrentWriteSchema,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
        self.node
            .node
            .borrow_mut()
            .apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
                author: self.identity.author,
                pointer,
            })
            .map_err(Into::into)
    }

    /// Set whether this authority may settle session-scoped reads and writes.
    /// Enabling it rehydrates all live subscriber views.
    pub fn set_permissions_ready(&self, ready: bool) -> Result<(), Error> {
        self.node.set_permissions_ready(ready)
    }

    /// Return the current write-schema pointer known to this database.
    pub fn current_write_schema(&self) -> CurrentWriteSchema {
        self.node.node.borrow().current_write_schema()
    }

    /// Return a published schema-version payload known to this database.
    pub fn catalogue_schema(&self, schema: SchemaVersionId) -> Option<JazzSchema> {
        self.node
            .node
            .borrow()
            .catalogue_schemas()
            .get(&schema)
            .map(|schema| schema.schema.clone())
    }

    /// Highest contiguously activated authoritative catalogue position.
    pub fn active_catalogue_seq(&self) -> u64 {
        self.node.node.borrow().active_catalogue_seq()
    }

    /// Return a published migration lens known to this database.
    pub fn catalogue_lens(&self, lens: crate::ids::MigrationLensId) -> Option<MigrationLens> {
        self.node
            .node
            .borrow()
            .catalogue_lenses()
            .get(&lens)
            .cloned()
    }

    /// Open a mergeable transaction and return its id.
    ///
    /// The caller owns this transaction's lifetime and must commit it with
    /// [`Db::commit_mergeable_handle`] or abandon it with
    /// [`Db::abandon_transaction_handle`]. Perform its writes through a
    /// [`MergeableTxRef`], which can be reconstructed from this id for each
    /// foreign-function call. Rust callers that want RAII should use
    /// [`Db::mergeable_tx`] instead.
    pub fn begin_mergeable(&self) -> Result<OpenTxId, Error> {
        self.node
            .node
            .borrow_mut()
            .open_mergeable(self.identity.author, None)
            .map_err(Into::into)
    }

    /// Open a mergeable transaction authored and permission-checked as `author`.
    ///
    /// See [`Db::begin_mergeable`] for ownership and operation-handle guidance.
    pub fn begin_mergeable_for_identity(&self, author: AuthorId) -> Result<OpenTxId, Error> {
        self.node
            .node
            .borrow_mut()
            .open_mergeable(author, Some(author))
            .map_err(Into::into)
    }

    /// Return a non-owning operations handle for an already-open mergeable transaction.
    ///
    /// This handle never closes the transaction when dropped, so it is suitable
    /// for a single call in a binding that retains `tx_id` between calls. Its
    /// CRUD API is defined by [`MergeableTxOps`] and is shared with the owning
    /// [`MergeableTx`] handle.
    pub fn mergeable_tx_ref(&self, tx_id: OpenTxId) -> MergeableTxRef<'_, S> {
        MergeableTxRef { db: self, tx_id }
    }

    fn stage_mergeable_insert(
        &self,
        tx_id: OpenTxId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.node
            .node
            .borrow_mut()
            .tx_write_mergeable(tx_id, table, row, cells, None, Vec::new(), now_ms, false)
            .map_err(Into::into)
    }

    fn stage_mergeable_update(
        &self,
        tx_id: OpenTxId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .tx_patch_mergeable(tx_id, table, row, patch, now_ms)
            .map_err(Into::into)
    }

    fn stage_mergeable_delete(
        &self,
        tx_id: OpenTxId,
        table: &str,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .tx_write_mergeable(
                tx_id,
                table,
                row,
                BTreeMap::new(),
                Some(DeletionEvent::Deleted),
                Vec::new(),
                now_ms,
                false,
            )
            .map_err(Into::into)
    }

    fn stage_mergeable_restore(
        &self,
        tx_id: OpenTxId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        let mut node = self.node.node.borrow_mut();
        let content_parents = node
            .local_content_winner_tx_id(table, row)?
            .into_iter()
            .collect();
        let deletion_parents = node
            .local_deletion_winner_tx_id(table, row)?
            .into_iter()
            .collect();
        node.tx_write_mergeable(
            tx_id,
            table,
            row,
            cells,
            None,
            content_parents,
            now_ms,
            true,
        )?;
        node.tx_write_mergeable(
            tx_id,
            table,
            row,
            BTreeMap::new(),
            Some(DeletionEvent::Restored),
            deletion_parents,
            now_ms,
            true,
        )?;
        Ok(())
    }

    /// Commit an owned mergeable transaction handle.
    pub fn commit_mergeable_handle(&self, open_tx_id: OpenTxId) -> Result<TxId, Error> {
        let tx_id = self
            .node
            .node
            .borrow_mut()
            .commit_mergeable_open(open_tx_id, || self.next_now_ms())?;
        self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(tx_id)
    }

    /// Abandon an owned open transaction handle.
    pub fn abandon_transaction_handle(&self, open_tx_id: OpenTxId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .abandon_tx(open_tx_id)
            .map_err(Into::into)
    }

    /// Open an exclusive transaction over the current local snapshot.
    ///
    /// This is the owning, RAII flavour. It abandons an uncommitted transaction
    /// on drop. Use [`Db::exclusive_tx_ref`] only when another layer retains the
    /// `OpenTxId` and owns that lifetime explicitly.
    pub fn exclusive_tx(&self) -> Result<ExclusiveTx<'_, S>, Error> {
        let tx_id = self.open_exclusive_handle()?;
        Ok(ExclusiveTx {
            db: self,
            tx_id,
            committed: false,
        })
    }

    /// Open an exclusive transaction and return its id.
    ///
    /// The caller owns this transaction's lifetime and must commit it with
    /// [`Db::commit_exclusive_handle`] or abandon it with
    /// [`Db::abandon_exclusive_handle`]. Perform its operations through an
    /// [`ExclusiveTxRef`]. Rust callers that want RAII should use
    /// [`Db::exclusive_tx`] instead.
    pub fn begin_exclusive(&self) -> Result<OpenTxId, Error> {
        self.open_exclusive_handle()
    }

    /// Return a non-owning operations handle for an already-open exclusive transaction.
    ///
    /// This handle never closes the transaction when dropped, so it is suitable
    /// for a single call in a binding that retains `tx_id` between calls. Its
    /// CRUD API is defined by [`ExclusiveTxOps`] and is shared with the owning
    /// [`ExclusiveTx`] handle.
    pub fn exclusive_tx_ref(&self, tx_id: OpenTxId) -> ExclusiveTxRef<'_, S> {
        ExclusiveTxRef { db: self, tx_id }
    }

    fn exclusive_read(
        &self,
        tx_id: OpenTxId,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<RowCells>, Error> {
        self.node
            .node
            .borrow_mut()
            .tx_read(tx_id, table, row)
            .map_err(Into::into)
    }

    fn exclusive_all(
        &self,
        tx_id: OpenTxId,
        prepared: &PreparedQuery,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.exclusive_all_for_identity(tx_id, prepared, self.identity.author)
    }

    pub(crate) fn exclusive_all_for_identity(
        &self,
        tx_id: OpenTxId,
        prepared: &PreparedQuery,
        author: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.node
            .node
            .borrow_mut()
            .tx_query_for_identity(tx_id, &prepared.shape, &prepared.binding, author)
            .map_err(Into::into)
    }

    fn stage_exclusive_insert(
        &self,
        tx_id: OpenTxId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<(), Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.node
            .node
            .borrow_mut()
            .tx_write(tx_id, table, row, cells, None)
            .map_err(Into::into)
    }

    fn stage_exclusive_delete(
        &self,
        tx_id: OpenTxId,
        table: &str,
        row: RowUuid,
    ) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .tx_write(
                tx_id,
                table,
                row,
                BTreeMap::<String, Value>::new(),
                Some(DeletionEvent::Deleted),
            )
            .map_err(Into::into)
    }

    fn stage_exclusive_restore(
        &self,
        tx_id: OpenTxId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<(), Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        let mut node = self.node.node.borrow_mut();
        // Restore needs one content version and one deletion-register version:
        // `tx_write` rejects a version carrying both. The layers have separate
        // winners and parent chains; see `restore`'s `local_*_winner_tx_id` pair.
        // Keep this staged form aligned with the committed restore path.
        node.tx_write(tx_id, table, row, cells, None)?;
        node.tx_write(
            tx_id,
            table,
            row,
            BTreeMap::<String, Value>::new(),
            Some(DeletionEvent::Restored),
        )?;
        Ok(())
    }

    /// Commit an owned exclusive transaction handle.
    pub fn commit_exclusive_handle(&self, open_tx_id: OpenTxId) -> Result<TxId, Error> {
        let (tx_id, unit) = self.node.node.borrow_mut().commit_exclusive(
            open_tx_id,
            self.identity.author,
            self.next_now_ms(),
        )?;
        self.finalize_local_exclusive_unit(tx_id, unit)?;
        self.refresh_subscriptions()?;
        Ok(tx_id)
    }

    /// Abandon an owned exclusive transaction handle.
    pub fn abandon_exclusive_handle(&self, open_tx_id: OpenTxId) -> Result<(), Error> {
        self.abandon_transaction_handle(open_tx_id)
    }

    pub(crate) fn open_exclusive_handle(&self) -> Result<OpenTxId, Error> {
        self.node
            .node
            .borrow_mut()
            .open_exclusive()
            .map_err(Into::into)
    }

    /// Restore a row locally, applying defaults for omitted columns.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::ids::RowUuid;
    /// let db = block_on(open_todos_db())?;
    /// let todo = RowUuid::from_bytes([1; 16]);
    /// db.insert_with_id("todos", todo, todo_cells("archived", false))?;
    /// db.delete("todos", todo)?;
    ///
    /// db.restore("todos", todo, todo_cells("restored", false))?;
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert_eq!(db.one(&todos)?.unwrap().row_uuid(), todo);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn restore(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.ensure_row_deleted(table, row, self.identity.author)?;
        let (content_parents, deletion_parents) = {
            let mut node = self.node.node.borrow_mut();
            let content_parents = node
                .local_content_winner_tx_id(table, row)?
                .into_iter()
                .collect::<Vec<_>>();
            let deletion_parents = node
                .local_deletion_winner_tx_id(table, row)?
                .into_iter()
                .collect::<Vec<_>>();
            (content_parents, deletion_parents)
        };
        let tx_id = self.node.node.borrow_mut().commit_mergeable_many(vec![
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(self.identity.author)
                .parents(content_parents)
                .cells(cells),
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(self.identity.author)
                .parents(deletion_parents)
                .cells(BTreeMap::<String, Value>::new())
                .deletion(DeletionEvent::Restored),
        ])?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    /// Restore a row while evaluating write policy as `identity`.
    pub fn restore_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.ensure_row_deleted(table, row, identity)?;
        let (content_parents, deletion_parents) = {
            let mut node = self.node.node.borrow_mut();
            let content_parents = node
                .local_content_winner_tx_id(table, row)?
                .into_iter()
                .collect::<Vec<_>>();
            let deletion_parents = node
                .local_deletion_winner_tx_id(table, row)?
                .into_iter()
                .collect::<Vec<_>>();
            (content_parents, deletion_parents)
        };
        let tx_id = self.node.node.borrow_mut().commit_mergeable_many(vec![
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(identity)
                .permission_subject(identity)
                .parents(content_parents)
                .cells(cells),
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(identity)
                .permission_subject(identity)
                .parents(deletion_parents)
                .cells(BTreeMap::<String, Value>::new())
                .deletion(DeletionEvent::Restored),
        ])?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    fn write_mergeable_as_session_subject(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
    ) -> Result<WriteHandle<S>, Error> {
        self.check_attribution_allowed(made_by)?;
        self.write_mergeable(
            made_by,
            Some(self.identity.author),
            table,
            row,
            cells,
            parents,
            deletion,
        )
    }

    fn write_mergeable_as_session_subject_with_authored_columns(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        authored_columns: BTreeSet<String>,
    ) -> Result<WriteHandle<S>, Error> {
        self.check_attribution_allowed(made_by)?;
        self.write_mergeable_with_authored_columns(
            made_by,
            Some(self.identity.author),
            table,
            row,
            cells,
            parents,
            deletion,
            authored_columns,
        )
    }

    /// Restore a row with an explicit millisecond provenance time.
    pub fn restore_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.ensure_row_deleted(table, row, self.identity.author)?;
        let (content_parents, deletion_parents) = self.row_layer_parents(table, row)?;
        let tx_id = self.node.node.borrow_mut().commit_mergeable_many(vec![
            MergeableCommit::new(table, row, now_ms)
                .made_by(self.identity.author)
                .parents(content_parents)
                .cells(cells),
            MergeableCommit::new(table, row, now_ms)
                .made_by(self.identity.author)
                .parents(deletion_parents)
                .cells(BTreeMap::<String, Value>::new())
                .deletion(DeletionEvent::Restored),
        ])?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    /// Restore a row for `identity` with an explicit millisecond provenance time.
    pub fn restore_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.ensure_row_deleted(table, row, identity)?;
        let (content_parents, deletion_parents) = self.row_layer_parents(table, row)?;
        let tx_id = self.node.node.borrow_mut().commit_mergeable_many(vec![
            MergeableCommit::new(table, row, now_ms)
                .made_by(identity)
                .permission_subject(identity)
                .parents(content_parents)
                .cells(cells),
            MergeableCommit::new(table, row, now_ms)
                .made_by(identity)
                .permission_subject(identity)
                .parents(deletion_parents)
                .cells(BTreeMap::<String, Value>::new())
                .deletion(DeletionEvent::Restored),
        ])?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    fn write_mergeable(
        &self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
    ) -> Result<WriteHandle<S>, Error> {
        self.write_mergeable_at_ms(
            made_by,
            permission_subject,
            table,
            row,
            cells,
            parents,
            deletion,
            self.next_now_ms(),
        )
    }

    fn write_mergeable_at_ms(
        &self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.write_mergeable_at_ms_with_authorship(
            made_by,
            permission_subject,
            table,
            row,
            cells,
            parents,
            deletion,
            None,
            now_ms,
        )
    }

    fn write_mergeable_with_authored_columns(
        &self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        authored_columns: BTreeSet<String>,
    ) -> Result<WriteHandle<S>, Error> {
        self.write_mergeable_at_ms_with_authorship(
            made_by,
            permission_subject,
            table,
            row,
            cells,
            parents,
            deletion,
            Some(authored_columns),
            self.next_now_ms(),
        )
    }

    fn write_mergeable_at_ms_with_authorship(
        &self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        authored_columns: Option<BTreeSet<String>>,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        let operation = if deletion == Some(DeletionEvent::Deleted) {
            "DELETE"
        } else if parents.is_empty() {
            "INSERT"
        } else {
            "UPDATE"
        };
        let cells = if operation == "INSERT" {
            self.apply_insert_defaults(table, cells)?
        } else {
            cells
        };
        let mut commit = MergeableCommit::new(table, row, now_ms)
            .made_by(made_by)
            .parents(parents)
            .cells(cells);
        if let Some(authored_columns) = authored_columns {
            commit = commit.authored_columns(authored_columns);
        }
        if let Some(subject) = permission_subject {
            commit = commit.permission_subject(subject);
        }
        if let Some(deletion) = deletion {
            commit = commit.deletion(deletion);
        }
        let allowed = self
            .node
            .node
            .borrow_mut()
            .dry_run_mergeable_write_allows(commit.clone())
            .map_err(Error::from)?;
        if !allowed {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("policy denied {operation} on table {table}"),
            ));
        }
        let tx_id = self.node.node.borrow_mut().commit_mergeable(commit)?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    fn check_attribution_allowed(&self, made_by: AuthorId) -> Result<(), Error> {
        if made_by == self.identity.author {
            return Ok(());
        }
        Err(Error::new(
            ErrorCode::WriteRejected,
            "attribution requires a trusted serving node",
        ))
    }

    fn check_catalogue_admin(&self) -> Result<(), Error> {
        if self.identity.author == AuthorId::SYSTEM {
            return Ok(());
        }
        Err(Error::new(
            ErrorCode::Protocol,
            "catalogue updates require a serving Node",
        ))
    }

    /// Finalize a locally-committed exclusive transaction. A `Core` authority
    /// validates and accepts/rejects it now, using the in-memory commit unit
    /// (which still carries `base_snapshot` and the read sets); other roles
    /// queue it for upstream, leaving it Pending/Local.
    fn finalize_local_exclusive_unit(
        &self,
        tx_id: TxId,
        unit: SyncMessage,
    ) -> Result<DurabilityTier, Error> {
        self.node.queue_pending_upload(tx_id, Some(unit));
        Ok(DurabilityTier::Local)
    }

    /// Client writes stay Pending/Local until upstream fates arrive over a
    /// connection.
    fn finalize_local_commit(&self, tx_id: TxId) -> Result<DurabilityTier, Error> {
        self.node.queue_pending_upload(tx_id, None);
        Ok(DurabilityTier::Local)
    }

    fn current_write_schema_for_query(&self) -> Result<(JazzSchema, SchemaVersionId), Error> {
        let node = self.node.node.borrow();
        let current = node.current_write_schema();
        if current.schema == self.schema_version_id {
            return Ok((self.schema.clone(), self.schema_version_id));
        }
        node.catalogue_schemas()
            .get(&current.schema)
            .map(|schema| (schema.schema.clone(), current.schema))
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::Schema,
                    format!(
                        "current write schema {:?} is missing from catalogue",
                        current.schema
                    ),
                )
            })
    }

    fn next_now_ms(&self) -> u64 {
        let next = self.next_now_ms.get();
        self.next_now_ms.set(next + 1);
        next
    }

    fn table_schema(&self, table: &str) -> Result<&TableSchema, Error> {
        self.schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .ok_or_else(|| Error::new(ErrorCode::Schema, format!("unknown table {table}")))
    }

    fn apply_insert_defaults(&self, table: &str, mut cells: RowCells) -> Result<RowCells, Error> {
        let table_schema = self.table_schema(table)?;
        for column in &table_schema.columns {
            if !cells.contains_key(&column.name) {
                if let Some(default) = &column.default {
                    cells.insert(
                        column.name.clone(),
                        default_cell_for_column_type(&column.column_type, default),
                    );
                }
            }
        }
        Ok(cells)
    }

    fn upsert_target_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<Option<CurrentRow>, Error> {
        let target = self.local_row_for_identity(table, row, identity)?;
        if target.is_some() {
            return Ok(target);
        }
        // A policy-filtered point read cannot by itself distinguish an absent
        // row from an existing row hidden from this identity. Upsert needs
        // exactly that distinction: a genuinely absent target follows INSERT
        // policy and does not require read permission, while merging into an
        // existing target must not expose or copy hidden cells.
        if self.local_current_row(table, row)?.is_none() {
            return Ok(None);
        }
        if identity == AuthorId::SYSTEM || self.table_schema(table)?.read_policy.is_none() {
            return Ok(None);
        }
        Err(read_for_write_denied("UPSERT", table))
    }

    /// Read one locally-current row by primary key without evaluating a table
    /// query. This backend-scoped helper is used by import/upsert bridges that
    /// already operate with database authority and need an O(row) existence
    /// check before staging a write.
    pub fn local_current_row(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<CurrentRow>, Error> {
        self.table_schema(table)?;
        Ok(self.node.node.borrow_mut().local_current_row(table, row)?)
    }

    fn ensure_row_absent(
        &self,
        table: &str,
        row: RowUuid,
        _identity: AuthorId,
    ) -> Result<(), Error> {
        self.table_schema(table)?;
        let (content_parent, deletion_parent) = {
            let mut node = self.node.node.borrow_mut();
            (
                node.local_content_winner_tx_id(table, row)?,
                node.local_deletion_winner_tx_id(table, row)?,
            )
        };
        if deletion_parent.is_some() {
            return Err(row_already_deleted(row));
        }
        if content_parent.is_some() {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("encoding error: object already exists: {}", row.0),
            ));
        }
        Ok(())
    }

    fn ensure_row_deleted(
        &self,
        table: &str,
        row: RowUuid,
        _identity: AuthorId,
    ) -> Result<(), Error> {
        self.table_schema(table)?;
        let deleted = self
            .node
            .node
            .borrow_mut()
            .local_deletion_winner_tx_id(table, row)?
            .is_some();
        if deleted {
            Ok(())
        } else {
            Err(Error::new(
                ErrorCode::WriteRejected,
                format!("row not deleted: {}", row.0),
            ))
        }
    }

    fn ensure_row_not_deleted(&self, table: &str, row: RowUuid) -> Result<(), Error> {
        self.table_schema(table)?;
        let deleted = self
            .node
            .node
            .borrow_mut()
            .local_deletion_winner_tx_id(table, row)?
            .is_some();
        if deleted {
            Err(row_already_deleted(row))
        } else {
            Ok(())
        }
    }

    fn row_layer_parents(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<(Vec<TxId>, Vec<TxId>), Error> {
        let mut node = self.node.node.borrow_mut();
        let content_parents = node
            .local_content_winner_tx_id(table, row)?
            .into_iter()
            .collect::<Vec<_>>();
        let deletion_parents = node
            .local_deletion_winner_tx_id(table, row)?
            .into_iter()
            .collect::<Vec<_>>();
        Ok((content_parents, deletion_parents))
    }

    fn local_row_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<Option<CurrentRow>, Error> {
        let query = self.prepare_query(&Query::from(table))?;
        Ok(self
            .node
            .node
            .borrow_mut()
            .query_rows_for_link_with_prepared_plan(
                &query.shape,
                &query.binding,
                DurabilityTier::Local,
                identity,
                query.plan_for_tier(DurabilityTier::Local),
            )?
            .into_iter()
            .find(|candidate| candidate.row_uuid() == row))
    }

    fn no_op_update_handle(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let existing = self
            .local_row_for_identity(table, row, identity)?
            .ok_or_else(|| read_for_write_denied("partial UPDATE", table))?;
        let tx_id = self
            .node
            .node
            .borrow_mut()
            .current_row_tx_id(&existing)
            .ok_or_else(|| Error::new(ErrorCode::NotObserved, "current row has no transaction"))?;
        let local_tier = self.write_state(tx_id)?.durability;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    fn merge_existing_cells(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<(RowCells, Option<TxId>, BTreeSet<String>), Error> {
        self.merge_existing_cells_for_identity(table, row, patch, self.identity.author)
    }

    fn merge_existing_cells_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        identity: AuthorId,
    ) -> Result<(RowCells, Option<TxId>, BTreeSet<String>), Error> {
        let table_schema = self.table_schema(table)?;
        self.ensure_row_not_deleted(table, row)?;
        if table_schema
            .columns
            .iter()
            .all(|column| patch.contains_key(&column.name))
        {
            // A full-row write does not observe user data. Its causal parent is
            // storage bookkeeping, so obtain only that parent with system
            // authority rather than evaluating the writer's read policy.
            let parent = self
                .local_current_row(table, row)?
                .as_ref()
                .and_then(|existing| self.node.node.borrow_mut().current_row_tx_id(existing));
            let authored_columns = patch.keys().cloned().collect();
            return Ok((patch, parent, authored_columns));
        }
        if !self.can_read_for_identity(table, row, identity)? {
            return Err(read_for_write_denied("partial UPDATE", table));
        }
        let mut cells = BTreeMap::new();
        let existing = self
            .local_row_for_identity(table, row, identity)?
            .ok_or_else(|| read_for_write_denied("partial UPDATE", table))?;
        for column in &table_schema.columns {
            if column.large_value.is_some() {
                continue;
            }
            if let Some(value) = existing.cell(table_schema, &column.name) {
                cells.insert(column.name.clone(), value);
            }
        }
        let parent = self.node.node.borrow_mut().current_row_tx_id(&existing);
        let authored_columns = patch.keys().cloned().collect();
        cells.extend(patch);
        Ok((cells, parent, authored_columns))
    }

    /// Attach this `Db` to an upstream peer over a binding-supplied transport.
    ///
    /// The returned [`PeerConnection`] carries this Db's subscriptions upstream
    /// under this Db's own identity and applies the view updates that come back.
    /// The binding drives it by calling [`PeerConnection::tick`] (or
    /// [`Db::tick`]) whenever it has staged inbound bytes or wants to flush.
    pub fn connect_upstream(
        &self,
        transport: Box<dyn Transport>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node.connect_upstream(transport)
    }

    /// Install or clear the scheduler used to wake this database's live peer
    /// connections when local writes, subscription registrations, or transport
    /// events create sync work.
    pub fn set_tick_scheduler(&self, scheduler: Option<Rc<dyn TickScheduler>>) {
        self.node.set_scheduler(scheduler);
    }

    /// Configure automatic edge-cache byte-budget eviction.
    ///
    /// `None` disables automatic eviction and preserves the historical manual
    /// `evict_cold` behavior.
    pub fn set_edge_cache_budget(&self, budget: Option<EdgeCacheBudget>) {
        self.node.set_edge_cache_budget(budget);
    }

    /// Ask the installed scheduler to service pending peer-connection work.
    pub fn schedule_tick(&self, urgency: TickUrgency) {
        self.node.schedule_tick(urgency);
    }

    /// Accept a subscriber connection served under `identity`.
    pub fn accept_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node.accept_subscriber(transport, identity)
    }

    /// Accept a subscriber connection served under `identity` with auth claims.
    pub fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_subscriber_with_claims(transport, identity, claims)
    }

    /// Accept a subscriber connection with explicit auth claims and upload trust mode.
    pub fn accept_subscriber_with_claims_and_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_subscriber_with_claims_and_trust(transport, identity, claims, trust)
    }

    /// Accept an edge-terminated subscriber with session claims.
    pub fn accept_edge_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_edge_subscriber_with_claims(transport, identity, claims)
    }

    /// Accept a subscriber whose host shell is wired as an edge fate authority.
    pub fn accept_edge_authority_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_edge_authority_subscriber_with_claims(transport, identity, claims)
    }

    /// Accept a reconnecting subscriber, resuming from a previous cursor.
    pub fn accept_subscriber_with_resume(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        cursor: ResumeCursor,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_subscriber_with_resume(transport, identity, cursor)
    }

    /// Detach a previously attached peer connection from this database.
    pub fn detach_connection(&self, connection: &Rc<RefCell<PeerConnection<S>>>) -> bool {
        self.node.detach_connection(connection)
    }

    /// Service every connection once (a convenience over
    /// [`PeerConnection::tick`] for the common single-upstream client).
    pub fn tick(&self) -> Result<(), Error> {
        self.node.tick().map(|_| ())
    }

    /// Service every connection once and return binding-observable wake counts.
    pub fn tick_stats(&self) -> Result<DbTickStats, Error> {
        self.node.tick()
    }

    fn refresh_subscriptions(&self) -> Result<usize, Error> {
        let refreshed = self.node.refresh_subscriptions()?;
        if refreshed > 0 {
            self.node.mark_subscriber_connections_dirty();
        }
        Ok(refreshed)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only history-class byte estimate. This is intentionally the
    /// cheap physical-class counter, not a logical table-prefix scan.
    pub fn history_class_bytes_for_test(&self) -> Result<Option<u64>, Error> {
        Ok(self.node.node.borrow().history_class_bytes_for_test()?)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only encoded storage byte estimate across Jazz physical
    /// classes.
    pub fn encoded_storage_bytes_for_test(&self) -> Result<u64, Error> {
        Ok(self.node.node.borrow().encoded_storage_bytes_for_test()?)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only durability boundary for harnesses that reopen the same
    /// storage path immediately after a synthetic lifecycle transition.
    pub fn flush_for_test(&self) -> Result<(), Error> {
        Ok(self.node.node.borrow_mut().flush_query_runtime()?)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only reset for logical storage-read attribution.
    pub fn reset_storage_read_metrics_for_test(&self) {
        self.node.node.borrow().reset_storage_read_metrics();
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only drain for logical storage-read attribution.
    pub fn take_storage_read_metrics_for_test(&self) -> groove::db::StorageReadMetrics {
        self.node.node.borrow().take_storage_read_metrics()
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only snapshot of sync-path counters.
    pub fn sync_metrics_for_test(&self) -> crate::node::SyncMetrics {
        self.node.node.borrow().sync_metrics().clone()
    }

    #[cfg(any(test, feature = "testing"))]
    /// Test/bench-only runtime diagnostics used by performance receipts.
    pub fn runtime_stats_for_test(&self) -> groove::ivm::RuntimeStats {
        self.node.node.borrow().runtime_stats_for_test()
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only maintained subscription sizing diagnostics used by
    /// warm-cache performance receipts.
    pub fn maintained_subscription_size_receipts_for_test(
        &self,
    ) -> Vec<MaintainedSubscriptionSizeReceipt> {
        self.node
            .subscriptions
            .borrow()
            .iter()
            .filter_map(Weak::upgrade)
            .filter_map(|state| {
                let state = state.borrow();
                let SubscriptionKind::Prepared {
                    shape,
                    binding,
                    maintained_subscription,
                } = &state.kind;
                let maintained_subscription = maintained_subscription.as_ref()?;
                let snapshot = &state.snapshot;
                let snapshot_bytes = encode_relation_snapshot_for_size(snapshot)
                    .map(|bytes| bytes.len())
                    .unwrap_or_default();
                let reset_frame_bytes = encode_subscription_reset_frame_for_size(
                    state.read_tier,
                    state.settled,
                    snapshot,
                )
                .map(|bytes| bytes.len())
                .unwrap_or_default();
                Some(MaintainedSubscriptionSizeReceipt {
                    name: shape.query().table.clone(),
                    shape_id: shape.shape_id().0,
                    binding_id: binding.binding_id().0,
                    rows: snapshot.rows.len(),
                    root_rows: snapshot.root_count,
                    relation_edges: snapshot.edges.len(),
                    footprint: DbMaintainedSubscriptionFootprint::from_local(
                        maintained_subscription.footprint(),
                    ),
                    snapshot_bytes,
                    reset_frame_bytes,
                    validation_tuple_estimate_bytes: validation_tuple_estimate_bytes(
                        shape,
                        binding,
                        state.author,
                        state.read_tier,
                        &state.read_view,
                    ),
                })
            })
            .collect()
    }
}

#[cfg(feature = "testing")]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Test/bench-only sizing receipt for one active maintained subscription.
pub struct MaintainedSubscriptionSizeReceipt {
    /// Debug label for the subscription, currently the root query table.
    pub name: String,
    /// Stable query shape id.
    pub shape_id: uuid::Uuid,
    /// Stable binding id.
    pub binding_id: uuid::Uuid,
    /// Materialized snapshot row count, including related rows.
    pub rows: usize,
    /// Materialized root row count.
    pub root_rows: usize,
    /// Materialized relation/include edge count.
    pub relation_edges: usize,
    /// Approximate maintained-view and local control-state footprint.
    pub footprint: DbMaintainedSubscriptionFootprint,
    /// Postcard bytes for the materialized relation snapshot shape used by native runtimes.
    pub snapshot_bytes: usize,
    /// Postcard bytes for the native reset delta row payload.
    pub reset_frame_bytes: usize,
    /// Estimated validation tuple bytes for a future warm-cache key.
    pub validation_tuple_estimate_bytes: usize,
}

#[cfg(feature = "testing")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// Test/bench-only approximate heap footprint for a maintained subscription.
pub struct DbMaintainedSubscriptionFootprint {
    /// Active result-current rows in the maintained index.
    pub result_rows: usize,
    /// Result weight map entries, including non-positive transient entries.
    pub result_weights: usize,
    /// Result payload map entries retained for projected/synthetic output.
    pub result_payloads: usize,
    /// Active readable version identities retained by full record identity.
    pub version_identities: usize,
    /// Entries reachable through the version-by-transaction index.
    pub version_tx_entries: usize,
    /// Active replacement winner entries across content and deletion maps.
    pub replacement_entries: usize,
    /// Approximate heap bytes retained by result_weights.
    pub result_weights_bytes: usize,
    /// Approximate heap bytes retained by result_payloads.
    pub result_payloads_bytes: usize,
    /// Approximate heap bytes retained by WeightedVersionIndex.
    pub versions_bytes: usize,
    /// Approximate heap bytes retained by ReplacementIndex.
    pub replacements_bytes: usize,
    /// Approximate heap bytes retained by maintained-view indexes.
    pub maintained_heap_bytes: usize,
    /// Lowered terminal schema count.
    pub terminal_schemas: usize,
    /// Approximate heap bytes retained by terminal schemas.
    pub terminal_schemas_bytes: usize,
    /// Table schema count retained by the local subscription.
    pub tables: usize,
    /// Local result-set member count.
    pub result_set: usize,
    /// Local result payload count.
    pub local_result_payloads: usize,
    /// Local program fact count.
    pub program_facts: usize,
    /// Approximate heap bytes retained by local subscription control state.
    pub control_state_bytes: usize,
    /// Approximate maintained plus local control-state heap bytes.
    pub total_heap_bytes: usize,
}

#[cfg(feature = "testing")]
impl DbMaintainedSubscriptionFootprint {
    fn from_local(footprint: crate::node::LocalMaintainedViewSubscriptionFootprint) -> Self {
        Self {
            result_rows: footprint.maintained.result_rows,
            result_weights: footprint.maintained.result_weights,
            result_payloads: footprint.maintained.result_payloads,
            version_identities: footprint.maintained.version_identities,
            version_tx_entries: footprint.maintained.version_tx_entries,
            replacement_entries: footprint.maintained.replacement_entries,
            result_weights_bytes: footprint.maintained.result_weights_bytes,
            result_payloads_bytes: footprint.maintained.result_payloads_bytes,
            versions_bytes: footprint.maintained.versions_bytes,
            replacements_bytes: footprint.maintained.replacements_bytes,
            maintained_heap_bytes: footprint.maintained.total_heap_bytes,
            terminal_schemas: footprint.terminal_schemas.terminal_schemas,
            terminal_schemas_bytes: footprint.terminal_schemas.terminal_schemas_bytes,
            tables: footprint.tables,
            result_set: footprint.result_set,
            local_result_payloads: footprint.result_payloads,
            program_facts: footprint.program_facts,
            control_state_bytes: footprint.control_state_bytes,
            total_heap_bytes: footprint.total_heap_bytes,
        }
    }
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeRelationSnapshot<'a> {
    root_count: u64,
    rows: Vec<SizeRowBatch<'a>>,
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeSubscriptionDelta<'a> {
    added: Vec<SizeRowBatch<'a>>,
    updated: Vec<SizeRowBatch<'a>>,
    removed: Vec<SizeRemovedRow>,
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeRowBatch<'a> {
    table: &'a str,
    descriptor: groove::records::RecordDescriptor,
    rows: Vec<SizeRow<'a>>,
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeRow<'a> {
    row_id: RowUuid,
    deleted: bool,
    raw: &'a [u8],
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeRemovedRow {
    table: String,
    row_id: RowUuid,
}

#[cfg(feature = "testing")]
fn encode_relation_snapshot_for_size(
    snapshot: &RelationSnapshot,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&SizeRelationSnapshot {
        root_count: snapshot.root_count as u64,
        rows: size_row_batches(&snapshot.rows),
    })
}

#[cfg(feature = "testing")]
fn encode_subscription_reset_frame_for_size(
    _tier: DurabilityTier,
    _settled: bool,
    snapshot: &RelationSnapshot,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&SizeSubscriptionDelta {
        added: size_row_batches(&snapshot.rows),
        updated: Vec::new(),
        removed: Vec::new(),
        terminal_operations: Vec::new(),
    })
}

#[cfg(feature = "testing")]
fn size_row_batches(rows: &[CurrentRow]) -> Vec<SizeRowBatch<'_>> {
    let mut batches = Vec::<SizeRowBatch<'_>>::new();
    for row in rows {
        let (descriptor, raw) = row.encoded_record();
        match batches.last_mut() {
            Some(batch) if batch.table == row.table() && batch.descriptor == *descriptor => {
                batch.rows.push(size_row(row, raw));
            }
            _ => batches.push(SizeRowBatch {
                table: row.table(),
                descriptor: *descriptor,
                rows: vec![size_row(row, raw)],
            }),
        }
    }
    batches
}

#[cfg(feature = "testing")]
fn size_row<'a>(row: &CurrentRow, raw: &'a [u8]) -> SizeRow<'a> {
    SizeRow {
        row_id: row.row_uuid(),
        deleted: row.is_deleted(),
        raw,
    }
}

#[cfg(feature = "testing")]
fn validation_tuple_estimate_bytes(
    shape: &ValidatedQuery,
    binding: &Binding,
    author: AuthorId,
    tier: DurabilityTier,
    read_view: &ReadViewSpec,
) -> usize {
    #[derive(serde::Serialize)]
    struct ValidationTuple<'a> {
        shape_id: uuid::Uuid,
        binding_id: uuid::Uuid,
        schema_version: SchemaVersionId,
        canonical_query: &'a [u8],
        canonical_binding: &'a [u8],
        author: AuthorId,
        tier: DurabilityTier,
        read_view: &'a ReadViewSpec,
    }

    postcard::to_allocvec(&ValidationTuple {
        shape_id: shape.shape_id().0,
        binding_id: binding.binding_id().0,
        schema_version: shape.schema_version(),
        canonical_query: shape.canonical_bytes(),
        canonical_binding: binding.canonical_bytes(),
        author,
        tier,
        read_view,
    })
    .map(|bytes| bytes.len())
    .unwrap_or_default()
}

/// Counts produced while servicing non-blocking database connection work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DbTickStats {
    /// Number of live subscriptions that received a queued event.
    pub subscription_events: usize,
    /// Number of connection ticks that applied remote sync state locally.
    pub remote_sync_applied: usize,
    /// Number of history codec windows built by post-tick maintenance.
    pub consolidated_windows: usize,
    /// Number of plain history records folded into codec windows.
    pub consolidated_window_records: usize,
    /// Foreground time spent in post-tick history window consolidation.
    pub history_window_consolidation_us: u128,
}

/// Node-owned participant surface for upstream and subscriber connections.
pub struct Node<S>
where
    S: OrderedKvStorage,
{
    node: Rc<RefCell<NodeState<S>>>,
    subscriptions: SubscriptionList,
    outbox: Outbox,
    upstream_subscriptions: PendingUpstreamCommands,
    latest_coverage_subscriptions: LatestCoverageSubscriptions,
    upstream_coverage_refcounts: UpstreamCoverageRefCounts,
    upstream_subscription_owners: UpstreamSubscriptionOwners,
    connections: RefCell<Vec<Rc<RefCell<PeerConnection<S>>>>>,
    scheduler: SharedTickScheduler,
    write_state_waiters: WriteStateWaiters,
    next_write_state_waiter_id: Cell<u64>,
    next_subscription_nonce: Cell<u64>,
    subscriber_dirty_epoch: Rc<Cell<u64>>,
    edge_cache_budget: Cell<Option<EdgeCacheBudget>>,
}

impl<S> Node<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Wrap a node for serving subscriber links.
    pub fn new(node: NodeState<S>) -> Self {
        Self {
            node: Rc::new(RefCell::new(node)),
            subscriptions: Rc::new(RefCell::new(Vec::new())),
            outbox: Rc::new(RefCell::new(Vec::new())),
            upstream_subscriptions: Rc::new(RefCell::new(Vec::new())),
            latest_coverage_subscriptions: Rc::new(RefCell::new(BTreeMap::new())),
            upstream_coverage_refcounts: Rc::new(RefCell::new(BTreeMap::new())),
            upstream_subscription_owners: Rc::new(RefCell::new(BTreeMap::new())),
            connections: RefCell::new(Vec::new()),
            scheduler: Rc::new(RefCell::new(None)),
            write_state_waiters: Rc::new(RefCell::new(BTreeMap::new())),
            next_write_state_waiter_id: Cell::new(1),
            next_subscription_nonce: Cell::new(1),
            subscriber_dirty_epoch: Rc::new(Cell::new(0)),
            edge_cache_budget: Cell::new(None),
        }
    }

    /// Borrow the served node.
    pub fn node(&self) -> Rc<RefCell<NodeState<S>>> {
        Rc::clone(&self.node)
    }

    /// Change whether subscriber links may serve their registered views.
    /// Publishing a permissions head always rehydrates every live view, so a
    /// tighter head retracts rows without requiring a reconnect.
    pub fn set_permissions_ready(&self, ready: bool) -> Result<(), Error> {
        self.node.borrow_mut().set_permissions_ready(ready);
        if ready {
            for connection in self.connections.borrow().iter() {
                connection.borrow_mut().rehydrate_subscriber_views()?;
            }
        }
        Ok(())
    }

    fn queue_pending_upload(&self, tx_id: TxId, unit: Option<SyncMessage>) {
        let mut outbox = self.outbox.borrow_mut();
        if outbox.iter().any(|pending| pending.tx_id == tx_id) {
            return;
        }
        outbox.push(PendingUpload { tx_id, unit });
        drop(outbox);
        self.mark_subscriber_connections_dirty();
        self.schedule_tick(TickUrgency::Deferred);
    }

    /// Restore locally originated, unsettled durable writes into the
    /// process-local upload queue after reopening client storage.
    fn restore_pending_uploads(&self, identity: DbIdentity) -> Result<(), Error> {
        let pending = self
            .node
            .borrow_mut()
            .pending_transaction_ids_for(identity.node, identity.author)?;
        let mut restored = HashSet::new();
        for tx_id in pending {
            if restored.insert(tx_id) {
                self.queue_pending_upload(tx_id, None);
            }
        }
        Ok(())
    }

    fn mark_subscriber_connections_dirty(&self) {
        let next = self.subscriber_dirty_epoch.get().wrapping_add(1);
        self.subscriber_dirty_epoch.set(next);
        for connection in self.connections.borrow().iter() {
            let mut connection = connection.borrow_mut();
            if let ConnectionLink::Subscriber { serve_dirty, .. } = &mut connection.link {
                *serve_dirty = true;
                connection.observed_subscriber_dirty_epoch.set(next);
            }
        }
    }

    #[cfg(feature = "testing")]
    /// Test/bench harnesses that mutate the served [`NodeState`] directly must
    /// mark subscriber links dirty. Production writes go through `Db`/sync
    /// boundaries that call this as a boundary effect.
    pub fn mark_subscriber_connections_dirty_for_test(&self) {
        self.mark_subscriber_connections_dirty();
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only encoded storage byte estimate across Jazz physical
    /// classes.
    pub fn encoded_storage_bytes_for_test(&self) -> Result<u64, Error> {
        Ok(self.node.borrow().encoded_storage_bytes_for_test()?)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only runtime diagnostics used by performance receipts.
    pub fn runtime_stats_for_test(&self) -> groove::ivm::RuntimeStats {
        self.node.borrow().runtime_stats_for_test()
    }

    fn next_subscription_key(
        &self,
        shape: &ValidatedQuery,
        read_view: crate::protocol::ReadViewKey,
    ) -> SubscriptionKey {
        let nonce = self.next_subscription_nonce.get();
        self.next_subscription_nonce.set(nonce.saturating_add(1));
        SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: crate::query::BindingId(uuid::Uuid::new_v5(
                &crate::query::QUERY_NAMESPACE,
                &nonce.to_be_bytes(),
            )),
            read_view,
        }
    }

    fn set_scheduler(&self, scheduler: Option<Rc<dyn TickScheduler>>) {
        *self.scheduler.borrow_mut() = scheduler;
    }

    fn set_edge_cache_budget(&self, budget: Option<EdgeCacheBudget>) {
        self.edge_cache_budget.set(budget);
    }

    fn schedule_tick(&self, urgency: TickUrgency) {
        schedule_tick_in(&self.scheduler, urgency);
    }

    fn queue_content_extent_fetch(&self, extent: crate::node::content_store::Extent) {
        self.upstream_subscriptions
            .borrow_mut()
            .push(PendingUpstreamCommand::FetchContentExtent {
                owner: LargeValueOwnerRef::current_row(extent.row),
                extent,
            });
        self.schedule_tick(TickUrgency::Immediate);
    }

    fn register_write_state_waiter(&self, tx_id: TxId) -> WriteStateChange {
        let waiter_id = self.next_write_state_waiter_id.get();
        self.next_write_state_waiter_id
            .set(waiter_id.wrapping_add(1).max(1));
        let (sender, receiver) = oneshot::channel();
        self.write_state_waiters
            .borrow_mut()
            .entry(tx_id)
            .or_default()
            .push(WriteStateWaiter {
                id: waiter_id,
                notify: WriteStateWaiterNotify::Future(sender),
            });
        WriteStateChange {
            waiters: Rc::clone(&self.write_state_waiters),
            tx_id,
            waiter_id,
            receiver,
        }
    }

    fn register_write_state_callback(&self, tx_id: TxId, callback: Box<dyn FnOnce()>) {
        let waiter_id = self.next_write_state_waiter_id.get();
        self.next_write_state_waiter_id
            .set(waiter_id.wrapping_add(1).max(1));
        self.write_state_waiters
            .borrow_mut()
            .entry(tx_id)
            .or_default()
            .push(WriteStateWaiter {
                id: waiter_id,
                notify: WriteStateWaiterNotify::Callback(callback),
            });
    }

    fn refresh_subscriptions(&self) -> Result<usize, Error> {
        refresh_subscriptions_in(&self.node, &self.subscriptions)
    }

    /// Attach this node to an upstream peer over a binding-supplied transport.
    pub fn connect_upstream(
        &self,
        transport: Box<dyn Transport>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        // Carry queued and already-registered subscriptions upstream immediately.
        let mut pending = self
            .upstream_subscriptions
            .borrow_mut()
            .drain(..)
            .collect::<Vec<_>>();
        let mut pending_coverage = pending
            .iter()
            .filter_map(|command| {
                let PendingUpstreamCommand::Subscribe(subscription) = command else {
                    return None;
                };
                Some(coverage_key(
                    &subscription.shape,
                    &subscription.binding,
                    subscription.opts.clone(),
                ))
            })
            .collect::<BTreeSet<_>>();
        for state in self.subscriptions.borrow().iter().filter_map(Weak::upgrade) {
            {
                let state = state.borrow();
                if !state.propagates_upstream {
                    continue;
                }
                let SubscriptionKind::Prepared { shape, binding, .. } = &state.kind;
                let opts =
                    upstream_register_shape_options(state.read_tier, state.read_view.clone());
                let coverage = coverage_key(shape, binding, opts.clone());
                if !pending_coverage.insert(coverage.clone()) {
                    continue;
                }
                let subscription = self.next_subscription_key(shape, opts.read_view_key());
                self.latest_coverage_subscriptions
                    .borrow_mut()
                    .insert(coverage, subscription);
                pending.push(PendingUpstreamCommand::Subscribe(
                    PendingUpstreamSubscription {
                        subscription,
                        shape: shape.clone(),
                        binding: binding.clone(),
                        opts,
                        identity: state.author,
                    },
                ));
            }
        }
        let connection = Rc::new(RefCell::new(PeerConnection {
            transport,
            node: Rc::clone(&self.node),
            subscriptions: Rc::clone(&self.subscriptions),
            upstream_subscription_owners: Rc::clone(&self.upstream_subscription_owners),
            scheduler: Rc::clone(&self.scheduler),
            write_state_waiters: Rc::clone(&self.write_state_waiters),
            subscriber_dirty_epoch: Rc::clone(&self.subscriber_dirty_epoch),
            observed_subscriber_dirty_epoch: Cell::new(self.subscriber_dirty_epoch.get()),
            observed_session_claim_revision: Cell::new(0),
            next_now_ms: Cell::new(1),
            link: ConnectionLink::Upstream {
                pending,
                upstream_subscriptions: Rc::clone(&self.upstream_subscriptions),
                announced_shapes: BTreeSet::new(),
                sent_session_claim_revisions: BTreeMap::new(),
                outbox: Rc::clone(&self.outbox),
                uploaded: BTreeSet::new(),
                pending_row_version_repairs: VecDeque::new(),
                branch_views: BTreeMap::new(),
                pending_branch_view_updates: BTreeMap::new(),
                pending_branch_metadata_repairs: BTreeMap::new(),
                branch_metadata_repair_cursor: None,
            },
            last_resume_bytes: None,
        }));
        self.connections.borrow_mut().push(Rc::clone(&connection));
        self.schedule_tick(TickUrgency::Immediate);
        connection
    }

    /// Accept a subscriber connection served under `identity`.
    pub fn accept_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.accept_subscriber_with_trust(transport, identity, CommitUnitTrust::Session)
    }

    /// Accept a subscriber connection with explicit auth claims.
    pub fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.accept_subscriber_with_claims_and_trust(
            transport,
            identity,
            claims,
            CommitUnitTrust::Session,
        )
    }

    /// Accept a subscriber connection with an explicit commit-upload trust mode.
    pub fn accept_subscriber_with_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        trust: CommitUnitTrust,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.accept_subscriber_with_resume_and_trust(transport, identity, trust, None)
    }

    /// Accept a subscriber connection with explicit auth claims and upload trust mode.
    pub fn accept_subscriber_with_claims_and_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node.borrow_mut().set_session_claims(identity, claims);
        self.accept_subscriber_with_resume_and_trust(transport, identity, trust, None)
    }

    /// Accept an edge-terminated subscriber with explicit auth claims.
    pub fn accept_edge_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node.borrow_mut().set_session_claims(identity, claims);
        self.accept_subscriber_with_peer(
            transport,
            identity,
            CommitUnitTrust::Session,
            None,
            PeerState::edge_client(identity),
            false,
        )
    }

    /// Accept a subscriber whose host shell is wired as an edge fate authority.
    pub fn accept_edge_authority_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node.borrow_mut().set_session_claims(identity, claims);
        self.accept_subscriber_with_peer(
            transport,
            identity,
            CommitUnitTrust::Session,
            None,
            PeerState::edge_client(identity),
            true,
        )
    }

    /// Accept a reconnecting subscriber, resuming from a previous cursor.
    pub fn accept_subscriber_with_resume(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        cursor: ResumeCursor,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.accept_subscriber_with_resume_and_trust(
            transport,
            identity,
            CommitUnitTrust::Session,
            Some(cursor),
        )
    }

    fn accept_subscriber_with_resume_and_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        trust: CommitUnitTrust,
        cursor: Option<ResumeCursor>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        let peer = match trust {
            CommitUnitTrust::TrustedBackend => {
                PeerState::edge_client_with_permission_identity(identity, AuthorId::SYSTEM)
            }
            CommitUnitTrust::Session => PeerState::client_link(identity),
        };
        self.accept_subscriber_with_peer(transport, identity, trust, cursor, peer, false)
    }

    fn accept_subscriber_with_peer(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        trust: CommitUnitTrust,
        cursor: Option<ResumeCursor>,
        peer: PeerState,
        edge_authority: bool,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        let peer = cursor.map(|cursor| cursor.peer).unwrap_or(peer);
        let session_claim_revision = self.node.borrow().session_claim_revision(identity);
        let connection = Rc::new(RefCell::new(PeerConnection {
            transport,
            node: Rc::clone(&self.node),
            subscriptions: Rc::clone(&self.subscriptions),
            upstream_subscription_owners: Rc::clone(&self.upstream_subscription_owners),
            scheduler: Rc::clone(&self.scheduler),
            write_state_waiters: Rc::clone(&self.write_state_waiters),
            subscriber_dirty_epoch: Rc::clone(&self.subscriber_dirty_epoch),
            observed_subscriber_dirty_epoch: Cell::new(self.subscriber_dirty_epoch.get()),
            observed_session_claim_revision: Cell::new(session_claim_revision),
            next_now_ms: Cell::new(1),
            link: ConnectionLink::Subscriber {
                peer,
                ingest_context: CommitUnitIngestContext {
                    identity,
                    trust,
                    edge_authority,
                },
                outbox: Rc::clone(&self.outbox),
                upstream_subscriptions: Rc::clone(&self.upstream_subscriptions),
                served: BTreeMap::new(),
                coverage_groups: BTreeMap::new(),
                shape_registrations: BTreeMap::new(),
                deferred_subscribe_rejections: VecDeque::new(),
                served_current_rows: BTreeMap::new(),
                pending_branch_metadata_repairs: BTreeMap::new(),
                pending_session_branch_metadata: BTreeMap::new(),
                branch_metadata_repair_cursor: None,
                serve_dirty: true,
            },
            last_resume_bytes: None,
        }));
        self.connections.borrow_mut().push(Rc::clone(&connection));
        self.schedule_tick(TickUrgency::Immediate);
        connection
    }

    /// Detach a previously attached peer connection from this node.
    pub fn detach_connection(&self, connection: &Rc<RefCell<PeerConnection<S>>>) -> bool {
        let mut connections = self.connections.borrow_mut();
        let before = connections.len();
        connections.retain(|candidate| !Rc::ptr_eq(candidate, connection));
        connections.len() != before
    }

    /// Service every accepted subscriber connection once.
    pub fn tick(&self) -> Result<DbTickStats, Error> {
        let mut stats = DbTickStats::default();
        let mut remote_sync_applied = false;
        for connection in self.connections.borrow().iter() {
            let next = connection.borrow_mut().tick()?;
            stats.subscription_events += next.subscription_events;
            stats.remote_sync_applied += next.remote_sync_applied;
            remote_sync_applied |= next.remote_sync_applied > 0;
        }
        if remote_sync_applied {
            for connection in self.connections.borrow().iter() {
                if connection.borrow_mut().mark_subscriber_dirty() {
                    let next = connection.borrow_mut().tick()?;
                    stats.subscription_events += next.subscription_events;
                    stats.remote_sync_applied += next.remote_sync_applied;
                }
            }
        }
        if let Some(budget) = self.edge_cache_budget.get() {
            let mut pins = crate::peer::PeerEvictionPins::default();
            for connection in self.connections.borrow().iter() {
                pins.extend(connection.borrow().eviction_pins());
            }
            self.node
                .borrow_mut()
                .enforce_edge_cache_budget(&pins, budget)?;
        }
        self.prune_settled_outbox_uploads();
        let consolidation_start = Instant::now();
        let consolidation = self
            .node
            .borrow_mut()
            .post_tick_consolidate_history_windows(POST_TICK_HISTORY_WINDOW_BUDGET)?;
        let consolidation_us = consolidation_start.elapsed().as_micros();
        stats.consolidated_windows += consolidation.windows;
        stats.consolidated_window_records += consolidation.records;
        if consolidation.windows > 0 {
            stats.history_window_consolidation_us += consolidation_us;
        }
        Ok(stats)
    }

    fn prune_settled_outbox_uploads(&self) {
        let mut outbox = self.outbox.borrow_mut();
        if outbox.is_empty() {
            return;
        }
        let mut node = self.node.borrow_mut();
        outbox.retain(|pending| {
            let Some((fate, _, durability)) = node.transaction_state(pending.tx_id) else {
                return true;
            };
            matches!(fate, Fate::Pending | Fate::Accepted) && durability < DurabilityTier::Global
        });
    }
}

/// Re-evaluate every live subscription against the node and push a delta event
/// for any whose rows changed. Shared by local writes
/// ([`Db::refresh_subscriptions`]) and by inbound sync application
/// ([`PeerConnection::tick`]).
fn refresh_subscriptions_in<S>(
    node: &Rc<RefCell<NodeState<S>>>,
    subscriptions: &SubscriptionList,
) -> Result<usize, Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let mut retained = Vec::new();
    let mut changed = 0;
    let pending_authoritative_resets = node
        .borrow_mut()
        .take_pending_authoritative_reset_binding_views();
    node.borrow_mut().flush_query_runtime()?;
    for weak in subscriptions.borrow().iter() {
        let Some(state) = weak.upgrade() else {
            continue;
        };
        let (
            read_tier,
            remote_read_tier,
            read_view,
            previous_source,
            previous_settled,
            author,
            terminal_rows,
        ) = {
            let state = state.borrow();
            (
                state.read_tier,
                state.remote_read_tier,
                state.read_view.clone(),
                state.snapshot_source,
                state.settled,
                state.author,
                state.terminal_rows,
            )
        };
        let groove_runtime_token = node.borrow().groove_runtime_token();
        if state.borrow().groove_runtime_token != groove_runtime_token {
            let (shape, binding) = {
                let state = state.borrow();
                match &state.kind {
                    SubscriptionKind::Prepared { shape, binding, .. } => {
                        (shape.clone(), binding.clone())
                    }
                }
            };
            let (maintained, snapshot) =
                node.borrow_mut().open_local_maintained_view_subscription(
                    &shape, &binding, author, read_tier, &read_view, None,
                )?;
            let settled_tier = remote_read_tier.unwrap_or(read_tier);
            let settled = subscription_is_settled(
                &node.borrow(),
                &shape,
                &binding,
                settled_tier,
                read_view.clone(),
            );
            let mut state_ref = state.borrow_mut();
            match &mut state_ref.kind {
                SubscriptionKind::Prepared {
                    maintained_subscription,
                    ..
                } => *maintained_subscription = Some(maintained),
            }
            let event = subscription_delta_event_with_reset(
                read_tier,
                settled,
                &state_ref.snapshot,
                &snapshot,
                true,
                terminal_rows,
            );
            state_ref.groove_runtime_token = groove_runtime_token;
            state_ref.snapshot = relation_snapshot_with_delta_slack(&snapshot);
            state_ref.snapshot_index = RelationSnapshotIndex::from_snapshot(&state_ref.snapshot);
            state_ref.snapshot_source = SubscriptionSnapshotSource::LocalMaintained;
            state_ref.settled = settled;
            if state_ref.sender.unbounded_send(event).is_ok() {
                changed += 1;
            }
            drop(state_ref);
            retained.push(Rc::downgrade(&state));
            continue;
        }
        let (snapshot, snapshot_source, settled, snapshot_tier, force_reset_event) = {
            let mut state_ref = state.borrow_mut();
            match &mut state_ref.kind {
                SubscriptionKind::Prepared {
                    shape,
                    binding,
                    maintained_subscription,
                } => {
                    let shape = shape.clone();
                    let binding = binding.clone();
                    let has_maintained_subscription = maintained_subscription.is_some();
                    let remote_settled_tier = remote_read_tier.filter(|tier| {
                        node.borrow().has_settled_result_set(BindingViewKey {
                            shape_id: shape.shape_id(),
                            binding_id: binding.binding_id(),
                            read_view: RegisterShapeOptions {
                                tier: *tier,
                                read_view: read_view.clone(),
                            }
                            .read_view_key(),
                        })
                    });
                    let settled_tier = remote_read_tier.unwrap_or(read_tier);
                    let settled_binding_view = BindingViewKey {
                        shape_id: shape.shape_id(),
                        binding_id: binding.binding_id(),
                        read_view: RegisterShapeOptions {
                            tier: settled_tier,
                            read_view: read_view.clone(),
                        }
                        .read_view_key(),
                    };
                    let delivered_binding_view = BindingViewKey {
                        shape_id: shape.shape_id(),
                        binding_id: binding.binding_id(),
                        read_view: RegisterShapeOptions {
                            tier: read_tier,
                            read_view: read_view.clone(),
                        }
                        .read_view_key(),
                    };
                    let authoritative_reset_binding_view =
                        if pending_authoritative_resets.contains(&delivered_binding_view) {
                            delivered_binding_view
                        } else {
                            settled_binding_view
                        };
                    let authoritative_reset_pending =
                        pending_authoritative_resets.contains(&authoritative_reset_binding_view);
                    if node
                        .borrow()
                        .publication_deferred_for_binding_view(settled_binding_view)
                        || node
                            .borrow()
                            .publication_deferred_for_binding_view(delivered_binding_view)
                    {
                        if authoritative_reset_pending {
                            node.borrow_mut()
                                .defer_authoritative_reset_for_binding_view(
                                    authoritative_reset_binding_view,
                                );
                        }
                        retained.push(Rc::downgrade(&state));
                        continue;
                    }
                    let peer_terminal_operations = node
                        .borrow_mut()
                        .take_pending_terminal_operations(delivered_binding_view);
                    let snapshot_tier = remote_settled_tier.unwrap_or(read_tier);
                    let authoritative_reset = authoritative_reset_pending;
                    if authoritative_reset && terminal_rows {
                        let Some(maintained) = maintained_subscription.as_mut() else {
                            return Err(Error::new(
                                ErrorCode::Protocol,
                                "structured subscription lost its Groove terminal",
                            ));
                        };
                        // A structural-patch stream deliberately does not keep
                        // facade-level replacement rows current. Re-open the
                        // Groove terminal at an authoritative boundary so the
                        // reset is a fresh complete value and subsequent FIFO
                        // patches are relative to exactly that value.
                        let (replacement, snapshot) =
                            node.borrow_mut().open_local_maintained_view_subscription(
                                &shape, &binding, author, read_tier, &read_view, None,
                            )?;
                        *maintained = replacement;
                        let settled = subscription_is_settled(
                            &node.borrow(),
                            &shape,
                            &binding,
                            settled_tier,
                            read_view,
                        );
                        (
                            snapshot,
                            SubscriptionSnapshotSource::LocalMaintained,
                            settled,
                            snapshot_tier,
                            true,
                        )
                    } else if authoritative_reset {
                        let authoritative_snapshot = {
                            let mut node_ref = node.borrow_mut();
                            match node_ref.authoritative_reset_snapshot_for_binding_view(
                                &shape,
                                authoritative_reset_binding_view,
                            ) {
                                Ok(snapshot) => snapshot,
                                Err(crate::node::Error::MissingTransaction(_)) => {
                                    node_ref.record_authoritative_reset_missing_payload_fallback();
                                    node_ref.defer_authoritative_reset_for_binding_view(
                                        authoritative_reset_binding_view,
                                    );
                                    None
                                }
                                Err(error) => return Err(error.into()),
                            }
                        };
                        let authoritative_snapshot_available = authoritative_snapshot.is_some();
                        let maintained_update = if let Some(maintained) =
                            maintained_subscription.as_mut()
                        {
                            let mut node_ref = node.borrow_mut();
                            if authoritative_snapshot_available {
                                match node_ref
                                    .drain_local_maintained_view_subscription_state(maintained)
                                {
                                    Ok(_) => {
                                        node_ref
                                            .reset_local_maintained_view_subscription_from_binding_view(
                                                maintained,
                                                authoritative_reset_binding_view,
                                            );
                                        None
                                    }
                                    Err(error) => return Err(error.into()),
                                }
                            } else {
                                match node_ref.drain_local_maintained_view_subscription(maintained)
                                {
                                    Ok(update) => update,
                                    Err(crate::node::Error::MissingTransaction(_)) => {
                                        node_ref
                                            .record_authoritative_reset_missing_payload_fallback();
                                        node_ref.defer_authoritative_reset_for_binding_view(
                                            authoritative_reset_binding_view,
                                        );
                                        retained.push(Rc::downgrade(&state));
                                        continue;
                                    }
                                    Err(error) => return Err(error.into()),
                                }
                            }
                        } else {
                            None
                        };
                        let (mut snapshot, force_reset_event) =
                            if let Some(snapshot) = authoritative_snapshot {
                                (snapshot, true)
                            } else {
                                let fallback = {
                                    let mut node_ref = node.borrow_mut();
                                    match node_ref.subscription_snapshot_for_link(
                                        &shape,
                                        &binding,
                                        snapshot_tier,
                                        author,
                                    ) {
                                        Ok(snapshot) => snapshot,
                                        Err(crate::node::Error::MissingTransaction(_)) => {
                                            node_ref
                                            .record_authoritative_reset_missing_payload_fallback();
                                            node_ref.defer_authoritative_reset_for_binding_view(
                                                authoritative_reset_binding_view,
                                            );
                                            retained.push(Rc::downgrade(&state));
                                            continue;
                                        }
                                        Err(error) => return Err(error.into()),
                                    }
                                };
                                (fallback, false)
                            };
                        if let Some(update) = maintained_update {
                            let mut snapshot_index =
                                RelationSnapshotIndex::from_snapshot(&snapshot);
                            let _ = apply_maintained_update_to_snapshot(
                                &mut snapshot,
                                &mut snapshot_index,
                                update,
                                snapshot_tier,
                                previous_settled,
                                terminal_rows,
                            );
                        }
                        let settled = subscription_is_settled(
                            &node.borrow(),
                            &shape,
                            &binding,
                            settled_tier,
                            read_view,
                        );
                        (
                            snapshot,
                            SubscriptionSnapshotSource::LinkSnapshot,
                            settled,
                            snapshot_tier,
                            force_reset_event,
                        )
                    } else {
                        if terminal_rows && !peer_terminal_operations.is_empty() {
                            if let Some(maintained) = maintained_subscription.as_mut() {
                                // The serving terminal is authoritative for
                                // structural publication. Advance the local
                                // Groove mirror for future resets without
                                // publishing its redundant reconstruction.
                                node.borrow_mut()
                                    .drain_local_maintained_view_subscription_state(maintained)?;
                            }
                            let settled = subscription_is_settled(
                                &node.borrow(),
                                &shape,
                                &binding,
                                settled_tier,
                                read_view.clone(),
                            );
                            let state_ref = &mut *state_ref;
                            let event = SubscriptionEvent::Delta {
                                reset: false,
                                added: Vec::new(),
                                updated: Vec::new(),
                                removed: Vec::new(),
                                terminal_operations: peer_terminal_operations,
                                settled,
                                tier: snapshot_tier,
                            };
                            state_ref.settled = settled;
                            retained.push(Rc::downgrade(&state));
                            if state_ref.sender.unbounded_send(event).is_ok() {
                                changed += 1;
                            }
                            continue;
                        }
                        let maintained_update = if let Some(maintained) =
                            maintained_subscription.as_mut()
                        {
                            let mut node_ref = node.borrow_mut();
                            match node_ref.drain_local_maintained_view_subscription(maintained) {
                                Ok(update) => update,
                                Err(crate::node::Error::MissingTransaction(_)) => {
                                    node_ref.record_authoritative_reset_missing_payload_fallback();
                                    node_ref.defer_authoritative_reset_for_binding_view(
                                        authoritative_reset_binding_view,
                                    );
                                    retained.push(Rc::downgrade(&state));
                                    continue;
                                }
                                Err(error) => return Err(error.into()),
                            }
                        } else {
                            None
                        };
                        if let Some(update) = maintained_update {
                            if terminal_rows {
                                if !update.terminal_operations.is_empty() {
                                    let settled = subscription_is_settled(
                                        &node.borrow(),
                                        &shape,
                                        &binding,
                                        settled_tier,
                                        read_view,
                                    );
                                    let state_ref = &mut *state_ref;
                                    let event = SubscriptionEvent::Delta {
                                        reset: false,
                                        added: Vec::new(),
                                        updated: Vec::new(),
                                        removed: Vec::new(),
                                        terminal_operations: update.terminal_operations,
                                        settled,
                                        tier: snapshot_tier,
                                    };
                                    state_ref.settled = settled;
                                    retained.push(Rc::downgrade(&state));
                                    if state_ref.sender.unbounded_send(event).is_ok() {
                                        changed += 1;
                                    }
                                    continue;
                                }
                                let Some(maintained) = maintained_subscription.as_ref() else {
                                    return Err(Error::new(
                                        ErrorCode::Protocol,
                                        "structured subscription lost its Groove terminal",
                                    ));
                                };
                                let snapshot = node
                                    .borrow_mut()
                                    .materialize_local_maintained_relation_snapshot(maintained)?;
                                let settled = subscription_is_settled(
                                    &node.borrow(),
                                    &shape,
                                    &binding,
                                    settled_tier,
                                    read_view,
                                );
                                let state_ref = &mut *state_ref;
                                let event = subscription_terminal_delta_event(
                                    snapshot_tier,
                                    settled,
                                    &state_ref.snapshot,
                                    &snapshot,
                                );
                                state_ref.snapshot = relation_snapshot_with_delta_slack(&snapshot);
                                state_ref.snapshot_index =
                                    RelationSnapshotIndex::from_snapshot(&state_ref.snapshot);
                                state_ref.snapshot_source =
                                    SubscriptionSnapshotSource::LocalMaintained;
                                state_ref.settled = settled;
                                retained.push(Rc::downgrade(&state));
                                if state_ref.sender.unbounded_send(event).is_ok() {
                                    changed += 1;
                                }
                                continue;
                            } else {
                                let state_ref = &mut *state_ref;
                                let mut event = apply_maintained_update_to_snapshot(
                                    &mut state_ref.snapshot,
                                    &mut state_ref.snapshot_index,
                                    update,
                                    snapshot_tier,
                                    previous_settled,
                                    terminal_rows,
                                );
                                state_ref.snapshot_source =
                                    SubscriptionSnapshotSource::LocalMaintained;
                                let settled = subscription_is_settled(
                                    &node.borrow(),
                                    &shape,
                                    &binding,
                                    settled_tier,
                                    read_view,
                                );
                                state_ref.settled = settled;
                                retained.push(Rc::downgrade(&state));
                                if let SubscriptionEvent::Delta {
                                    settled: event_settled,
                                    ..
                                } = &mut event
                                {
                                    *event_settled = settled;
                                }
                                if state_ref.sender.unbounded_send(event).is_ok() {
                                    changed += 1;
                                }
                                continue;
                            }
                        }
                        let (snapshot, snapshot_source) = if terminal_rows {
                            (
                                state_ref.snapshot.clone(),
                                SubscriptionSnapshotSource::LocalMaintained,
                            )
                        } else if remote_settled_tier.is_some() {
                            let previous = state_ref.snapshot.clone();
                            if previous.root_count == 0
                                && previous.edges.is_empty()
                                && node
                                    .borrow()
                                    .has_settled_result_set(authoritative_reset_binding_view)
                            {
                                let authoritative_snapshot = {
                                    let mut node_ref = node.borrow_mut();
                                    match node_ref.authoritative_reset_snapshot_for_binding_view(
                                        &shape,
                                        authoritative_reset_binding_view,
                                    ) {
                                        Ok(snapshot) => snapshot,
                                        Err(crate::node::Error::MissingTransaction(_)) => {
                                            node_ref
                                                .record_authoritative_reset_missing_payload_fallback();
                                            node_ref.defer_authoritative_reset_for_binding_view(
                                                authoritative_reset_binding_view,
                                            );
                                            None
                                        }
                                        Err(error) => return Err(error.into()),
                                    }
                                };
                                if let Some(snapshot) = authoritative_snapshot {
                                    (snapshot, SubscriptionSnapshotSource::LinkSnapshot)
                                } else {
                                    let fallback = {
                                        let mut node_ref = node.borrow_mut();
                                        match node_ref.subscription_snapshot_for_link(
                                            &shape,
                                            &binding,
                                            snapshot_tier,
                                            author,
                                        ) {
                                            Ok(snapshot) => snapshot,
                                            Err(crate::node::Error::MissingTransaction(_)) => {
                                                node_ref
                                                    .record_authoritative_reset_missing_payload_fallback();
                                                node_ref
                                                    .defer_authoritative_reset_for_binding_view(
                                                        authoritative_reset_binding_view,
                                                    );
                                                retained.push(Rc::downgrade(&state));
                                                continue;
                                            }
                                            Err(error) => return Err(error.into()),
                                        }
                                    };
                                    (fallback, SubscriptionSnapshotSource::LinkSnapshot)
                                }
                            } else {
                                let remote_snapshot = {
                                    let mut node_ref = node.borrow_mut();
                                    match node_ref.subscription_snapshot_for_link(
                                        &shape,
                                        &binding,
                                        snapshot_tier,
                                        author,
                                    ) {
                                        Ok(snapshot) => snapshot,
                                        Err(crate::node::Error::MissingTransaction(_)) => {
                                            node_ref
                                                .record_authoritative_reset_missing_payload_fallback();
                                            node_ref.defer_authoritative_reset_for_binding_view(
                                                authoritative_reset_binding_view,
                                            );
                                            retained.push(Rc::downgrade(&state));
                                            continue;
                                        }
                                        Err(error) => return Err(error.into()),
                                    }
                                };
                                if has_maintained_subscription
                                    && previous.root_count > 0
                                    && previous_source
                                        == SubscriptionSnapshotSource::LocalMaintained
                                    && remote_snapshot.root_count == 0
                                {
                                    (previous.clone(), previous_source)
                                } else {
                                    (remote_snapshot, SubscriptionSnapshotSource::LinkSnapshot)
                                }
                            }
                        } else if has_maintained_subscription {
                            let previous = state_ref.snapshot.clone();
                            (previous.clone(), previous_source)
                        } else {
                            (
                                node.borrow_mut().subscription_snapshot_for_link(
                                    &shape, &binding, read_tier, author,
                                )?,
                                SubscriptionSnapshotSource::LinkSnapshot,
                            )
                        };
                        let settled = subscription_is_settled(
                            &node.borrow(),
                            &shape,
                            &binding,
                            settled_tier,
                            read_view,
                        );
                        (snapshot, snapshot_source, settled, snapshot_tier, false)
                    }
                }
            }
        };
        let previous = state.borrow().snapshot.clone();
        if force_reset_event || snapshot != previous || settled != previous_settled {
            let mut state = state.borrow_mut();
            let event = if force_reset_event {
                subscription_delta_event_with_reset(
                    snapshot_tier,
                    settled,
                    &previous,
                    &snapshot,
                    true,
                    terminal_rows,
                )
            } else {
                subscription_delta_event(
                    snapshot_tier,
                    settled,
                    &previous,
                    &snapshot,
                    terminal_rows,
                )
            };
            state.snapshot = relation_snapshot_with_delta_slack(&snapshot);
            state.snapshot_index = RelationSnapshotIndex::from_snapshot(&state.snapshot);
            state.snapshot_source = snapshot_source;
            state.settled = settled;
            if state.sender.unbounded_send(event).is_ok() {
                changed += 1;
            }
        }
        retained.push(Rc::downgrade(&state));
    }
    *subscriptions.borrow_mut() = retained;
    Ok(changed)
}

fn register_upstream_subscription_owner(
    owners: &UpstreamSubscriptionOwners,
    handles: &[UpstreamCoverageHandle],
    state: &Rc<RefCell<SubscriptionState>>,
) {
    let weak = Rc::downgrade(state);
    let mut owners = owners.borrow_mut();
    for handle in handles {
        owners
            .entry(handle.subscription)
            .or_default()
            .push(weak.clone());
    }
}

fn unregister_upstream_subscription_owner(
    owners: &UpstreamSubscriptionOwners,
    subscription: SubscriptionKey,
    state: &Weak<RefCell<SubscriptionState>>,
) {
    let mut owners = owners.borrow_mut();
    let Some(entries) = owners.get_mut(&subscription) else {
        return;
    };
    entries.retain(|entry| !entry.ptr_eq(state) && entry.strong_count() > 0);
    if entries.is_empty() {
        owners.remove(&subscription);
    }
}

fn route_upstream_subscription_rejection(
    subscriptions: &SubscriptionList,
    owners: &UpstreamSubscriptionOwners,
    subscription: SubscriptionKey,
    reason: SubscribeRejectReason,
) -> usize {
    let mut delivered = 0;
    if let Some(entries) = owners.borrow_mut().get_mut(&subscription) {
        entries.retain(|entry| entry.strong_count() > 0);
        for state in entries.iter().filter_map(Weak::upgrade) {
            let event = SubscriptionEvent::Rejected {
                reason: reason.clone(),
            };
            if state.borrow().sender.unbounded_send(event).is_ok() {
                delivered += 1;
            }
        }
        if delivered > 0 {
            return delivered;
        }
    }

    for state in subscriptions.borrow().iter().filter_map(Weak::upgrade) {
        let state_ref = state.borrow();
        if !state_ref.propagates_upstream {
            continue;
        }
        let SubscriptionKind::Prepared { shape, binding, .. } = &state_ref.kind;
        let read_view =
            upstream_register_shape_options(state_ref.read_tier, state_ref.read_view.clone())
                .read_view_key();
        if shape.shape_id() != subscription.shape_id || read_view != subscription.read_view {
            continue;
        }
        if subscription.binding_id != BindingId(uuid::Uuid::nil())
            && binding.binding_id() != subscription.binding_id
        {
            continue;
        }
        let event = SubscriptionEvent::Rejected {
            reason: reason.clone(),
        };
        if state_ref.sender.unbounded_send(event).is_ok() {
            delivered += 1;
        }
    }
    delivered
}

/// Binding-supplied transport for one peer link.
///
/// The `Db` writes outbound messages with [`Transport::send`] and pulls inbound
/// ones with [`Transport::try_recv`]; the binding owns the actual socket and
/// scheduling and bridges these to real I/O on its own runtime. Both methods are
/// non-blocking — `try_recv` returning `None` means "nothing staged right now,"
/// not "closed" (a disconnect surface lands with a later B slice). This is the
/// single seam that keeps the async boundary *between* nodes, never inside `Db`.
pub trait Transport {
    /// Hand an outbound message to the binding's wire.
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError>;
    /// Pull the next inbound message the binding has staged, if any.
    fn try_recv(&mut self) -> Option<SyncMessage>;
}

/// Adapter from postcard wire frames to the internal sync-message transport.
struct IncompleteLogicalMessage {
    protocol_version: u16,
    features: WireFeatures,
    session: Option<WireSession>,
    message_digest: [u8; 32],
    total_len: usize,
    received_len: usize,
    extents: BTreeMap<usize, Vec<u8>>,
}

#[derive(Default)]
struct LogicalMessageReassembler {
    incomplete: HashMap<u64, IncompleteLogicalMessage>,
    staged_bytes: usize,
    recently_completed: VecDeque<(u64, [u8; 32])>,
    highest_completed_message_id: Option<u64>,
}

impl LogicalMessageReassembler {
    fn discard(&mut self, message_id: u64) {
        if let Some(state) = self.incomplete.remove(&message_id) {
            self.staged_bytes = self.staged_bytes.saturating_sub(state.received_len);
        }
    }

    fn push(&mut self, fragment: WireMessageFragment) -> Result<Option<WireEnvelope>, String> {
        if let Some((_, digest)) = self
            .recently_completed
            .iter()
            .find(|(message_id, _)| *message_id == fragment.message_id)
        {
            return if digest == &fragment.message_digest {
                Ok(None)
            } else {
                Err("completed logical message id was reused with another digest".to_owned())
            };
        }
        if self
            .highest_completed_message_id
            .is_some_and(|completed| fragment.message_id <= completed)
        {
            return Ok(None);
        }
        let total_len = usize::try_from(fragment.total_len)
            .map_err(|_| "logical message length does not fit this receiver".to_owned())?;
        validate_logical_message_len(total_len)?;
        let offset = usize::try_from(fragment.offset)
            .map_err(|_| "logical message fragment offset does not fit this receiver".to_owned())?;
        let end = offset
            .checked_add(fragment.payload.len())
            .ok_or_else(|| "logical message fragment range overflow".to_owned())?;
        if fragment.payload.is_empty() || end > total_len {
            return Err("logical message fragment has an empty or out-of-range extent".to_owned());
        }
        if !self.incomplete.contains_key(&fragment.message_id) {
            if self.incomplete.len() >= MAX_INFLIGHT_LOGICAL_MESSAGES {
                return Err("too many incomplete logical messages for peer".to_owned());
            }
            self.incomplete.insert(
                fragment.message_id,
                IncompleteLogicalMessage {
                    protocol_version: fragment.protocol_version,
                    features: fragment.features,
                    session: fragment.session.clone(),
                    message_digest: fragment.message_digest,
                    total_len,
                    received_len: 0,
                    extents: BTreeMap::new(),
                },
            );
        }
        let state = self
            .incomplete
            .get_mut(&fragment.message_id)
            .expect("logical message state inserted");
        if state.total_len != total_len
            || state.protocol_version != fragment.protocol_version
            || state.features != fragment.features
            || state.session != fragment.session
            || state.message_digest != fragment.message_digest
        {
            return Err("logical message fragments disagree on metadata".to_owned());
        }
        if let Some(existing) = state.extents.get(&offset) {
            return if existing == &fragment.payload {
                Ok(None)
            } else {
                Err("conflicting duplicate logical message fragment".to_owned())
            };
        }
        if state
            .extents
            .range(..=offset)
            .next_back()
            .is_some_and(|(start, bytes)| *start + bytes.len() > offset)
            || state
                .extents
                .range(offset..)
                .next()
                .is_some_and(|(start, _)| *start < end)
        {
            return Err("overlapping logical message fragments".to_owned());
        }
        let next_staged = self
            .staged_bytes
            .checked_add(fragment.payload.len())
            .ok_or_else(|| "logical message staging byte count overflow".to_owned())?;
        if next_staged > MAX_INFLIGHT_LOGICAL_MESSAGE_BYTES {
            return Err("incomplete logical messages exceed peer staging budget".to_owned());
        }
        self.staged_bytes = next_staged;
        state.received_len += fragment.payload.len();
        state.extents.insert(offset, fragment.payload);
        if state.received_len != state.total_len {
            return Ok(None);
        }

        let state = self
            .incomplete
            .remove(&fragment.message_id)
            .expect("completed logical message state exists");
        self.staged_bytes -= state.received_len;
        let mut cursor = 0;
        let mut payload = Vec::with_capacity(state.total_len);
        for (offset, extent) in state.extents {
            if offset != cursor {
                return Err(
                    "logical message fragments do not provide contiguous coverage".to_owned(),
                );
            }
            cursor += extent.len();
            payload.extend_from_slice(&extent);
        }
        if cursor != state.total_len || *blake3::hash(&payload).as_bytes() != state.message_digest {
            return Err("logical message digest mismatch".to_owned());
        }
        self.recently_completed
            .push_back((fragment.message_id, state.message_digest));
        self.highest_completed_message_id = Some(fragment.message_id);
        if self.recently_completed.len() > RECENT_COMPLETED_LOGICAL_MESSAGES {
            self.recently_completed.pop_front();
        }
        Ok(Some(WireEnvelope {
            protocol_version: state.protocol_version,
            features: state.features,
            session: state.session,
            payload,
        }))
    }
}

/// Converts logical sync messages to negotiated bounded wire frames and back.
pub struct WireTransportAdapter<T> {
    inner: T,
    protocol_version: u16,
    features: WireFeatures,
    session: Option<WireSession>,
    outbound_stream: WireStreamEncoder,
    inbound_stream: WireStreamDecoder,
    reassembler: LogicalMessageReassembler,
    pending_outbound_frames: VecDeque<Vec<u8>>,
    next_outbound_message_id: u64,
}

impl<T> WireTransportAdapter<T>
where
    T: WireTransport,
{
    /// Wrap a byte transport with the current Jazz wire defaults.
    pub fn current(inner: T) -> Self {
        Self::new(inner, WIRE_PROTOCOL_VERSION, current_wire_features(), None)
    }

    /// Wrap a byte transport with explicit negotiated frame metadata.
    pub fn new(
        inner: T,
        protocol_version: u16,
        features: WireFeatures,
        session: Option<WireSession>,
    ) -> Self {
        let outbound_stream = WireStreamEncoder::new(features)
            .expect("negotiated wire compression must be compiled into this binary");
        let inbound_stream = WireStreamDecoder::new(features)
            .expect("negotiated wire compression must be compiled into this binary");
        Self {
            inner,
            protocol_version,
            features,
            session,
            outbound_stream,
            inbound_stream,
            reassembler: LogicalMessageReassembler::default(),
            pending_outbound_frames: VecDeque::new(),
            next_outbound_message_id: 0,
        }
    }

    /// Consume the adapter and return the wrapped byte transport.
    pub fn into_inner(self) -> T {
        self.inner
    }

    fn send_wire_error(&mut self, error: WireError) {
        if let Ok(frame) = encode_frame(&WireFrame::Error(error)) {
            let _ = self.inner.send_frame(frame);
        }
    }

    fn flush_pending_outbound(&mut self) -> Result<(), TransportError> {
        while let Some(frame) = self.pending_outbound_frames.pop_front() {
            if let Err(error) = self.inner.send_frame(frame.clone()) {
                self.pending_outbound_frames.push_front(frame);
                return Err(error);
            }
        }
        Ok(())
    }

    fn send_encoded_frames(&mut self, frames: Vec<Vec<u8>>) -> Result<(), TransportError> {
        for (index, frame) in frames.iter().enumerate() {
            match self.inner.send_frame(frame.clone()) {
                Ok(()) => {}
                Err(TransportError::Backpressure) => {
                    // Encoding may have advanced a connection-stream compressor. Accept the
                    // logical message once encoded and retain every unaccepted frame so the
                    // semantic caller must never retry it against advanced codec state.
                    self.pending_outbound_frames
                        .extend(frames[index..].iter().cloned());
                    return Ok(());
                }
                Err(error @ TransportError::Failed(_)) => return Err(error),
            }
        }
        Ok(())
    }

    fn validate_inbound_session(&self, envelope: &WireEnvelope) -> Result<(), WireError> {
        let Some(expected) = &self.session else {
            return Ok(());
        };
        let Some(actual) = &envelope.session else {
            return Err(WireError::new(
                WireErrorCode::AuthFailed,
                WireRetry::AfterAuth,
                "missing wire session metadata",
            ));
        };
        if actual.session_id != expected.session_id {
            return Err(WireError::new(
                WireErrorCode::AuthFailed,
                WireRetry::AfterResume,
                "wire session id does not match this connection",
            ));
        }
        if actual.identity != expected.identity {
            return Err(WireError::new(
                WireErrorCode::AuthFailed,
                WireRetry::AfterAuth,
                "wire session identity does not match this connection",
            ));
        }
        if actual.epoch < expected.epoch {
            return Err(WireError::new(
                WireErrorCode::AuthFailed,
                WireRetry::AfterResume,
                "stale wire session epoch",
            ));
        }
        if actual.epoch != expected.epoch {
            return Err(WireError::new(
                WireErrorCode::AuthFailed,
                WireRetry::AfterResume,
                "wire session epoch does not match this connection",
            ));
        }
        Ok(())
    }

    fn validate_inbound_metadata(&self, envelope: &WireEnvelope) -> Result<(), WireError> {
        if envelope.protocol_version != self.protocol_version {
            return Err(WireError::new(
                WireErrorCode::UnsupportedProtocolVersion,
                WireRetry::AfterResume,
                format!(
                    "wire message protocol version {} does not match negotiated {}",
                    envelope.protocol_version, self.protocol_version
                ),
            ));
        }
        let unnegotiated = envelope.features & !self.features;
        if unnegotiated != 0 {
            return Err(WireError::new(
                WireErrorCode::UnsupportedFeature,
                WireRetry::AfterResume,
                format!("wire message declares unnegotiated features {unnegotiated:#x}"),
            ));
        }
        Ok(())
    }

    fn decode_inbound_envelope(
        &mut self,
        envelope: WireEnvelope,
    ) -> Result<SyncMessage, WireError> {
        self.validate_inbound_metadata(&envelope)?;
        self.validate_inbound_session(&envelope)?;
        let payload = self
            .inbound_stream
            .decode_message(&envelope.payload, envelope.features)
            .map_err(|message| {
                WireError::new(WireErrorCode::MalformedFrame, WireRetry::Never, message)
            })?;
        validate_logical_message_len(payload.len()).map_err(|message| {
            WireError::new(WireErrorCode::MalformedFrame, WireRetry::Never, message)
        })?;
        let message = decode_sync_message_for_receive(&payload).map_err(|error| {
            WireError::new(
                WireErrorCode::MalformedFrame,
                WireRetry::Never,
                format!(
                    "failed to decode sync message payload: {error}; payload_bytes={}; payload_hex={}",
                    payload.len(),
                    hex_diagnostic(&payload)
                ),
            )
        })?;
        Ok(message)
    }
}

impl<T> Transport for WireTransportAdapter<T>
where
    T: WireTransport,
{
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.flush_pending_outbound()?;
        let payload = match encode_sync_message(&message) {
            Ok(payload) => payload,
            Err(err) => {
                self.send_wire_error(WireError::new(
                    WireErrorCode::Internal,
                    WireRetry::Never,
                    format!("failed to encode sync message: {err}"),
                ));
                return Ok(());
            }
        };
        if let Err(message) = validate_logical_message_len(payload.len()) {
            return Err(TransportError::Failed(message));
        }
        let payload = match self.outbound_stream.encode_message(&payload) {
            Ok(payload) => payload,
            Err(message) => return Err(TransportError::Failed(message)),
        };
        let active_features = (self.features
            & !(crate::wire::FEATURE_PAYLOAD_LZ4 | crate::wire::FEATURE_PAYLOAD_ZSTD))
            | self.outbound_stream.active_feature();
        let mut envelope = WireEnvelope::new(self.protocol_version, active_features, payload);
        if let Some(session) = self.session.clone() {
            envelope = envelope.with_session(session);
        }
        match encode_frame(&WireFrame::Message(envelope.clone())) {
            Ok(frame) if frame.len() <= WIRE_FRAGMENT_PAYLOAD_BYTES => {
                self.send_encoded_frames(vec![frame])
            }
            Ok(_) if self.features & FEATURE_MESSAGE_FRAGMENTATION == 0 => {
                Err(TransportError::Failed(
                    "peer did not negotiate logical-message fragmentation".to_owned(),
                ))
            }
            Ok(_) => {
                let message_digest = *blake3::hash(&envelope.payload).as_bytes();
                let message_id = self.next_outbound_message_id;
                self.next_outbound_message_id = self
                    .next_outbound_message_id
                    .checked_add(1)
                    .ok_or_else(|| {
                        TransportError::Failed("logical message id space exhausted".to_owned())
                    })?;
                let total_len = u64::try_from(envelope.payload.len()).map_err(|_| {
                    TransportError::Failed("logical message is too large".to_owned())
                })?;
                let mut frames = Vec::new();
                for (index, payload) in envelope
                    .payload
                    .chunks(WIRE_FRAGMENT_PAYLOAD_BYTES)
                    .enumerate()
                {
                    let offset = u64::try_from(index * WIRE_FRAGMENT_PAYLOAD_BYTES)
                        .expect("fragment offset is bounded by logical message size");
                    let fragment = WireMessageFragment {
                        protocol_version: envelope.protocol_version,
                        features: envelope.features,
                        session: envelope.session.clone(),
                        message_id,
                        message_digest,
                        total_len,
                        offset,
                        payload: payload.to_vec(),
                    };
                    let frame =
                        encode_frame(&WireFrame::MessageFragment(fragment)).map_err(|error| {
                            TransportError::Failed(format!(
                                "failed to encode logical message fragment: {error}"
                            ))
                        })?;
                    validate_wire_frame_len(frame.len()).map_err(TransportError::Failed)?;
                    frames.push(frame);
                }
                self.send_encoded_frames(frames)
            }
            Err(err) => {
                self.send_wire_error(WireError::new(
                    WireErrorCode::Internal,
                    WireRetry::Never,
                    format!("failed to encode wire frame: {err}"),
                ));
                Ok(())
            }
        }
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        let _ = self.flush_pending_outbound();
        while let Some(bytes) = self.inner.try_recv_frame() {
            if let Err(message) = validate_wire_frame_len(bytes.len()) {
                self.send_wire_error(WireError::new(
                    WireErrorCode::MalformedFrame,
                    WireRetry::Never,
                    message,
                ));
                continue;
            }
            let frame = match decode_frame(&bytes) {
                Ok(frame) => frame,
                Err(err) => {
                    self.send_wire_error(WireError::new(
                        WireErrorCode::MalformedFrame,
                        WireRetry::Never,
                        format!("failed to decode wire frame: {err}"),
                    ));
                    continue;
                }
            };
            match frame {
                WireFrame::Message(envelope) => match self.decode_inbound_envelope(envelope) {
                    Ok(message) => return Some(message),
                    Err(error) => self.send_wire_error(error),
                },
                WireFrame::MessageFragment(fragment) => {
                    let fragment_message_id = fragment.message_id;
                    let metadata = WireEnvelope {
                        protocol_version: fragment.protocol_version,
                        features: fragment.features,
                        session: fragment.session.clone(),
                        payload: Vec::new(),
                    };
                    if let Err(error) = self.validate_inbound_metadata(&metadata) {
                        self.send_wire_error(error);
                        continue;
                    }
                    if let Err(error) = self.validate_inbound_session(&metadata) {
                        self.send_wire_error(error);
                        continue;
                    }
                    if self.features & FEATURE_MESSAGE_FRAGMENTATION == 0
                        || fragment.features & FEATURE_MESSAGE_FRAGMENTATION == 0
                    {
                        self.send_wire_error(WireError::new(
                            WireErrorCode::UnsupportedFeature,
                            WireRetry::Never,
                            "fragment does not declare logical-message fragmentation",
                        ));
                        continue;
                    }
                    match self.reassembler.push(fragment) {
                        Ok(Some(envelope)) => match self.decode_inbound_envelope(envelope) {
                            Ok(message) => return Some(message),
                            Err(error) => self.send_wire_error(error),
                        },
                        Ok(None) => {}
                        Err(message) => {
                            self.reassembler.discard(fragment_message_id);
                            self.send_wire_error(WireError::new(
                                WireErrorCode::MalformedFrame,
                                WireRetry::AfterResume,
                                message,
                            ));
                        }
                    }
                }
                WireFrame::Hello(_) => self.send_wire_error(WireError::new(
                    WireErrorCode::UnsupportedFeature,
                    WireRetry::AfterResume,
                    "hello frames must be handled before constructing a peer connection",
                )),
                WireFrame::Error(_) => {}
            }
        }
        None
    }
}

fn hex_diagnostic(bytes: &[u8]) -> String {
    if bytes.len() <= 128 {
        return hex_prefix(bytes, bytes.len());
    }
    hex_prefix(bytes, 16)
}

fn hex_prefix(bytes: &[u8], max: usize) -> String {
    bytes
        .iter()
        .take(max)
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

/// A live link between this `Db` and one peer, owned by the `Db`.
///
/// Two link shapes — a client/backend attached to an upstream, or a server
/// serving one subscriber under their identity. An edge is simply both at once
/// (one upstream connection plus many subscriber connections); edge authority
/// (relay/edge/core) stays below this facade in [`crate::peer`].
pub struct PeerConnection<S>
where
    S: OrderedKvStorage,
{
    transport: Box<dyn Transport>,
    node: Rc<RefCell<NodeState<S>>>,
    subscriptions: SubscriptionList,
    upstream_subscription_owners: UpstreamSubscriptionOwners,
    scheduler: SharedTickScheduler,
    write_state_waiters: WriteStateWaiters,
    subscriber_dirty_epoch: Rc<Cell<u64>>,
    observed_subscriber_dirty_epoch: Cell<u64>,
    observed_session_claim_revision: Cell<u64>,
    next_now_ms: Cell<u64>,
    link: ConnectionLink,
    last_resume_bytes: Option<usize>,
}

enum ConnectionLink {
    /// Attached to an upstream: send subscribe requests and local commit units
    /// up, apply view updates and fates that come back.
    Upstream {
        /// Shapes registered locally but not yet announced upstream.
        pending: Vec<PendingUpstreamCommand>,
        /// Shapes registered through downstream subscribers.
        upstream_subscriptions: PendingUpstreamCommands,
        /// Shapes already registered on this connection.
        announced_shapes: BTreeSet<ShapeRegistrationKey>,
        /// Latest session-claim revision shipped for each identity on this
        /// connection. A fresh link starts empty and therefore receives every
        /// current claim map, even if another link has already received it.
        sent_session_claim_revisions: BTreeMap<AuthorId, u64>,
        /// Locally-authored transactions to upload (shared with the `Db`).
        outbox: Outbox,
        /// Transactions already shipped on this connection (dedup across ticks).
        uploaded: BTreeSet<TxId>,
        /// Declared known-state ViewUpdates parked until missing row bodies arrive.
        pending_row_version_repairs: VecDeque<PendingRowVersionRepair>,
        /// Branch selected by each upstream usage-site subscription.
        branch_views: BTreeMap<SubscriptionKey, crate::ids::BranchId>,
        /// View updates held until their branch routing record arrives.
        pending_branch_view_updates: BTreeMap<crate::ids::BranchId, Vec<SyncMessage>>,
        /// Deduplicated outstanding metadata repairs on this link.
        pending_branch_metadata_repairs: BTreeMap<crate::ids::BranchId, ()>,
        /// Round-robin cursor so a saturated repair set cannot starve later ids.
        branch_metadata_repair_cursor: Option<crate::ids::BranchId>,
    },
    /// Serving one subscriber: apply their subscribe requests, ship view
    /// updates under their identity.
    Subscriber {
        peer: PeerState,
        ingest_context: CommitUnitIngestContext,
        /// Accepted subscriber commit units awaiting upstream relay.
        outbox: Outbox,
        /// Subscriber-maintained views that must be announced upstream.
        upstream_subscriptions: PendingUpstreamCommands,
        /// Usage-site subscriptions this subscriber registered.
        served: BTreeMap<SubscriptionKey, CoverageKey>,
        /// Shared maintained views keyed by query shape, binding, and options.
        coverage_groups: BTreeMap<CoverageKey, CoverageGroup>,
        /// Explicit state for each subscriber `RegisterShape`, keyed by shape and read view.
        shape_registrations: BTreeMap<ShapeRegistrationKey, SubscriberShapeRegistration>,
        /// Permanent rejections received as later `Subscribe` messages. These
        /// wait until an unrelated view update has been flushed, so they cannot
        /// starve a supported subscription on the same connection.
        deferred_subscribe_rejections: VecDeque<(SubscriptionKey, String)>,
        /// Whole-table current-row views explicitly served through the facade.
        served_current_rows: BTreeMap<SubscriptionKey, String>,
        /// Deduplicated branch-routing repairs for data-first commit relays.
        pending_branch_metadata_repairs: BTreeMap<crate::ids::BranchId, ()>,
        /// Authenticated session metadata waiting for its parent/base dependency.
        pending_session_branch_metadata:
            BTreeMap<crate::ids::BranchId, crate::protocol::BranchMetadata>,
        /// Round-robin cursor so a saturated repair set cannot starve later ids.
        branch_metadata_repair_cursor: Option<crate::ids::BranchId>,
        /// True when this subscriber's maintained views may have queued deltas
        /// to serve. Idle transport ticks must not poll every view.
        serve_dirty: bool,
    },
}

struct PendingRowVersionRepair {
    requests: Vec<crate::protocol::RowVersionRef>,
    update: SyncMessage,
}

/// Return one fair bounded repair page, advancing the cursor after the page.
fn next_branch_metadata_repairs(
    repairs: &BTreeMap<crate::ids::BranchId, ()>,
    cursor: &mut Option<crate::ids::BranchId>,
) -> Vec<crate::ids::BranchId> {
    let mut page = repairs
        .keys()
        .copied()
        .filter(|branch| cursor.is_none_or(|after| *branch > after))
        .take(MAX_FETCH_BRANCH_METADATA)
        .collect::<Vec<_>>();
    if page.is_empty() && !repairs.is_empty() {
        page = repairs
            .keys()
            .copied()
            .take(MAX_FETCH_BRANCH_METADATA)
            .collect();
    }
    *cursor = page.last().copied();
    page
}

/// Per-connection resume state for a served subscriber.
///
/// Bindings keep this after a disconnect and pass it into
/// [`Node::accept_subscriber_with_resume`] for the reconnecting subscriber. It is
/// the facade handle for the peer-layer complete-tx payload inventory and
/// result-set cursor.
#[derive(Debug)]
pub struct ResumeCursor {
    peer: PeerState,
}

impl<S> PeerConnection<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Rebuild this subscriber's maintained views if its process-local claims
    /// changed. Policy claim values are bound when a maintained view opens, so
    /// retaining the old view after a claim change would retain its authority.
    fn rebind_subscriber_views_after_claim_change(&mut self) -> Result<bool, Error> {
        let identity = match &self.link {
            ConnectionLink::Subscriber { ingest_context, .. } => ingest_context.identity,
            ConnectionLink::Upstream { .. } => return Ok(false),
        };
        let current_revision = self.node.borrow().session_claim_revision(identity);
        if self.observed_session_claim_revision.get() == current_revision {
            return Ok(false);
        }

        let ConnectionLink::Subscriber {
            peer,
            coverage_groups,
            served_current_rows,
            ..
        } = &mut self.link
        else {
            unreachable!("subscriber identity requires a subscriber link")
        };
        peer.advance_authorization_progress();
        let groups = coverage_groups
            .iter()
            .map(|(coverage, group)| {
                (
                    coverage.clone(),
                    group.shape.clone(),
                    group.binding.clone(),
                    group.subscribers.iter().copied().collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        for (coverage, shape, binding, subscribers) in groups {
            let branch_metadata = match coverage.opts.read_view.source {
                ReadViewSourceSpec::Branch { branch } => {
                    let branch = crate::ids::BranchId(branch);
                    let mut node = self.node.borrow_mut();
                    node.branch_metadata_visible_to(branch, identity)?
                        .then(|| {
                            node.branch_record(branch)
                                .map(crate::protocol::BranchMetadata::from)
                        })
                        .flatten()
                }
                _ => None,
            };
            let maintained_subscription = SubscriptionKey {
                shape_id: coverage.shape_id,
                binding_id: coverage.binding_id,
                read_view: coverage.opts.read_view_key(),
            };
            let update = {
                let mut node = self.node.borrow_mut();
                peer.rehydrate_query_for_subscription_with_opts(
                    &mut node,
                    maintained_subscription,
                    &shape,
                    &binding,
                    coverage.opts,
                )?
            };
            // Route metadata is an explicit prerequisite for branch-target
            // bundles. Send it before the first view update so a receiver can
            // create the partition instead of parking the payload forever.
            if let Some(metadata) = branch_metadata {
                self.transport
                    .send(SyncMessage::BranchMetadata(metadata))
                    .map_err(transport_error)?;
            }
            for subscription in subscribers {
                send_with_content_extents(
                    &self.node,
                    peer,
                    self.transport.as_mut(),
                    retarget_view_update(update.clone(), subscription),
                )?;
            }
        }
        for table in served_current_rows.values() {
            let update = {
                let mut node = self.node.borrow_mut();
                peer.current_rows_update(&mut node, table)?
            };
            send_with_content_extents(&self.node, peer, self.transport.as_mut(), update)?;
        }

        self.observed_session_claim_revision.set(current_revision);
        Ok(true)
    }

    /// Serve a whole-table current-row view to this subscriber immediately and
    /// refresh it on later ticks.
    pub fn serve_current_rows(&mut self, table: &str) -> Result<(), Error> {
        self.tick()?;
        let ConnectionLink::Subscriber {
            peer,
            served_current_rows,
            ..
        } = &mut self.link
        else {
            return Ok(());
        };
        let update = {
            let mut node = self.node.borrow_mut();
            peer.current_rows_update(&mut node, table)?
        };
        self.last_resume_bytes = Some(serialized_sync_message_len(&update));
        let subscription = view_update_subscription(&update);
        send_sync_message_chunked(self.transport.as_mut(), update)?;
        if let Some(subscription) = subscription {
            served_current_rows.insert(subscription, table.to_owned());
        }
        if let ConnectionLink::Subscriber { serve_dirty, .. } = &mut self.link {
            *serve_dirty = true;
        }
        Ok(())
    }

    /// Return the serialized byte size of the latest resume/catch-up response
    /// sent by this connection.
    pub fn last_resume_bytes(&self) -> Option<usize> {
        self.last_resume_bytes
    }

    /// Extract this subscriber connection's resume cursor for a reconnect.
    pub fn take_resume_cursor(&mut self) -> Option<ResumeCursor> {
        let ConnectionLink::Subscriber { peer, .. } = &mut self.link else {
            return None;
        };
        let replacement = PeerState::client_link(peer.link_identity());
        Some(ResumeCursor {
            peer: std::mem::replace(peer, replacement),
        })
    }

    /// Rehydrate every view served on this subscriber link. This is used when
    /// an authority installs its first permissions head or replaces it with a
    /// tighter one: the client keeps the same subscription, but its visible
    /// membership must be recalculated immediately.
    fn rehydrate_subscriber_views(&mut self) -> Result<(), Error> {
        let ConnectionLink::Subscriber {
            peer,
            coverage_groups,
            serve_dirty,
            ..
        } = &mut self.link
        else {
            return Ok(());
        };
        peer.advance_authorization_progress();
        let groups = coverage_groups
            .iter()
            .map(|(coverage, group)| {
                (
                    coverage.clone(),
                    group.shape.clone(),
                    group.binding.clone(),
                    group.subscribers.iter().copied().collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        for (coverage, shape, binding, subscribers) in groups {
            let group_subscription = SubscriptionKey {
                shape_id: coverage.shape_id,
                binding_id: coverage.binding_id,
                read_view: coverage.opts.read_view_key(),
            };
            let update = {
                let mut node = self.node.borrow_mut();
                peer.rehydrate_query_for_subscription_with_opts(
                    &mut node,
                    group_subscription,
                    &shape,
                    &binding,
                    coverage.opts.clone(),
                )?
            };
            for subscription in subscribers {
                let update = retarget_view_update(update.clone(), subscription);
                self.last_resume_bytes = Some(serialized_sync_message_len(&update));
                send_with_content_extents(&self.node, peer, self.transport.as_mut(), update)?;
            }
        }
        *serve_dirty = true;
        Ok(())
    }

    /// Service this connection once: drain inbound, apply, wake subscriptions, and
    /// flush pending outbound. Non-blocking; the binding calls it in its loop.
    pub fn tick(&mut self) -> Result<DbTickStats, Error> {
        let mut stats = DbTickStats::default();
        let tick_now_ms = self.next_now_ms();
        self.observe_shared_subscriber_dirty_epoch();
        self.rebind_subscriber_views_after_claim_change()?;
        match &mut self.link {
            ConnectionLink::Upstream {
                pending,
                upstream_subscriptions,
                announced_shapes,
                sent_session_claim_revisions,
                outbox,
                uploaded,
                pending_row_version_repairs,
                branch_views,
                pending_branch_view_updates,
                pending_branch_metadata_repairs,
                branch_metadata_repair_cursor,
            } => {
                // Repair is deliberately retried on each non-blocked tick. The
                // request set is bounded and deduplicated; a dropped request or
                // response therefore cannot permanently strand a parked unit.
                let repairs = next_branch_metadata_repairs(
                    pending_branch_metadata_repairs,
                    branch_metadata_repair_cursor,
                );
                if !repairs.is_empty() {
                    self.transport
                        .send(SyncMessage::FetchBranchMetadata { branches: repairs })
                        .map_err(transport_error)?;
                }
                for metadata in self.node.borrow().pending_branch_metadata_uploads() {
                    self.transport
                        .send(SyncMessage::BranchMetadata(metadata))
                        .map_err(transport_error)?;
                }
                pending.extend(upstream_subscriptions.borrow_mut().drain(..));
                let claims = self.node.borrow().session_claims_with_revisions();
                for (identity, claims, revision) in claims {
                    if sent_session_claim_revisions
                        .get(&identity)
                        .is_some_and(|sent| *sent >= revision)
                    {
                        continue;
                    }
                    if let Err(error) = self
                        .transport
                        .send(SyncMessage::SessionClaims { identity, claims })
                    {
                        if handle_transport_backpressure(&self.node, &self.scheduler, &error) {
                            return Ok(stats);
                        }
                        return Err(transport_error(error));
                    }
                    sent_session_claim_revisions.insert(identity, revision);
                }
                let pending_index = 0;
                while pending_index < pending.len() {
                    match &pending[pending_index] {
                        PendingUpstreamCommand::Subscribe(pending_subscription) => {
                            let shape = &pending_subscription.shape;
                            let binding = &pending_subscription.binding;
                            let registration_key =
                                (shape.shape_id(), pending_subscription.opts.read_view_key());
                            if announced_shapes.insert(registration_key) {
                                self.node.borrow_mut().apply_sync_message(
                                    SyncMessage::RegisterShape {
                                        shape_id: shape.shape_id(),
                                        ast: ShapeAst::from_validated(shape),
                                        opts: RegisterShapeOptions::default(),
                                    },
                                )?;
                                if let Err(error) =
                                    self.transport.send(SyncMessage::RegisterShape {
                                        shape_id: shape.shape_id(),
                                        ast: ShapeAst::from_validated(shape),
                                        opts: pending_subscription.opts.clone(),
                                    })
                                {
                                    announced_shapes.remove(&registration_key);
                                    if handle_transport_backpressure(
                                        &self.node,
                                        &self.scheduler,
                                        &error,
                                    ) {
                                        return Ok(stats);
                                    }
                                    return Err(transport_error(error));
                                }
                            }
                            let values = binding_values_in_param_order(shape, binding);
                            let known_state = self
                                .node
                                .borrow_mut()
                                .known_state_declaration_for_subscription(
                                    shape,
                                    binding,
                                    pending_subscription.subscription,
                                    &values,
                                    pending_subscription.identity,
                                )?;
                            let subscribe = Subscribe {
                                shape_id: shape.shape_id(),
                                subscription: pending_subscription.subscription,
                                values,
                                known_state,
                            };
                            if let ReadViewSourceSpec::Branch { branch } =
                                &pending_subscription.opts.read_view.source
                            {
                                branch_views.insert(
                                    pending_subscription.subscription,
                                    crate::ids::BranchId(*branch),
                                );
                            }
                            #[cfg(feature = "sync-autopsy")]
                            sync_autopsy::record(format!(
                                "upstream send subscribe {}",
                                summarize_subscription_key(subscribe.subscription)
                            ));
                            self.node
                                .borrow_mut()
                                .apply_sync_message(SyncMessage::Subscribe(subscribe.clone()))?;
                            if let Err(error) =
                                self.transport.send(SyncMessage::Subscribe(subscribe))
                            {
                                if handle_transport_backpressure(
                                    &self.node,
                                    &self.scheduler,
                                    &error,
                                ) {
                                    return Ok(stats);
                                }
                                return Err(transport_error(error));
                            }
                        }
                        PendingUpstreamCommand::Unsubscribe(subscription) => {
                            self.node.borrow_mut().apply_unsubscribe(*subscription);
                            if let Err(error) = self.transport.send(SyncMessage::Unsubscribe {
                                subscription: *subscription,
                            }) {
                                if handle_transport_backpressure(
                                    &self.node,
                                    &self.scheduler,
                                    &error,
                                ) {
                                    return Ok(stats);
                                }
                                return Err(transport_error(error));
                            }
                        }
                        PendingUpstreamCommand::FetchContentExtent { owner, extent } => {
                            if let Err(error) =
                                self.transport.send(SyncMessage::FetchContentExtent {
                                    owner: owner.clone(),
                                    extent: extent.clone(),
                                })
                            {
                                if handle_transport_backpressure(
                                    &self.node,
                                    &self.scheduler,
                                    &error,
                                ) {
                                    return Ok(stats);
                                }
                                return Err(transport_error(error));
                            }
                        }
                    }
                    pending.remove(pending_index);
                }
                // Upload locally-authored commits not yet shipped on this link.
                let to_upload: Vec<TxId> = outbox
                    .borrow()
                    .iter()
                    .map(|pending| pending.tx_id)
                    .filter(|tx_id| !uploaded.contains(tx_id))
                    .collect();
                for tx_id in to_upload {
                    let unit = outbox
                        .borrow()
                        .iter()
                        .find(|pending| pending.tx_id == tx_id)
                        .and_then(|pending| pending.unit.clone())
                        .map(Ok)
                        .unwrap_or_else(|| self.node.borrow_mut().commit_unit_for(tx_id))?;
                    if let SyncMessage::CommitUnit { tx, .. } = &unit
                        && let crate::tx::BranchLineage::Branch(branch) = tx.target_lineage
                        && let Some(metadata) = self.node.borrow().branch_record(branch).cloned()
                    {
                        self.transport
                            .send(SyncMessage::BranchMetadata((&metadata).into()))
                            .map_err(transport_error)?;
                    }
                    if let Err(error) =
                        send_with_local_content_extents(&self.node, self.transport.as_mut(), unit)
                    {
                        if handle_db_backpressure(&self.node, &self.scheduler, &error) {
                            return Ok(stats);
                        }
                        return Err(error);
                    }
                    uploaded.insert(tx_id);
                }
                let mut applied = false;
                let mut pending_view_updates = Vec::<ViewUpdateParts>::new();
                while let Some(message) = self.transport.try_recv() {
                    let write_state_tx_id = write_state_update_tx_id(&message);
                    #[cfg(feature = "sync-autopsy")]
                    sync_autopsy::record(format!(
                        "upstream recv {}",
                        summarize_sync_message(&message)
                    ));
                    match message {
                        SyncMessage::CatalogueSnapshot(snapshot) => {
                            if !pending_view_updates.is_empty() {
                                self.node.borrow_mut().apply_view_updates_in_batch(
                                    std::mem::take(&mut pending_view_updates),
                                )?;
                            }
                            self.node
                                .borrow_mut()
                                .apply_trusted_catalogue_snapshot(*snapshot)?;
                        }
                        SyncMessage::RowVersionPayloads { version_bundles } => {
                            if !pending_view_updates.is_empty() {
                                self.node.borrow_mut().apply_view_updates_in_batch(
                                    std::mem::take(&mut pending_view_updates),
                                )?;
                            }
                            let Some(repair) = pending_row_version_repairs.pop_front() else {
                                drop_peer_request(&self.node);
                                continue;
                            };
                            {
                                let mut node = self.node.borrow_mut();
                                node.apply_row_version_payloads_for_requests(
                                    &repair.requests,
                                    version_bundles,
                                )?;
                            }
                            push_view_update_message_for_receiver(
                                &mut pending_view_updates,
                                repair.update,
                            )?;
                        }
                        message @ SyncMessage::ViewUpdate { subscription, .. } => {
                            if let Some(branch) = branch_views.get(&subscription).copied()
                                && self.node.borrow().branch_record(branch).is_none()
                            {
                                pending_branch_view_updates
                                    .entry(branch)
                                    .or_default()
                                    .push(message);
                                if let std::collections::btree_map::Entry::Vacant(entry) =
                                    pending_branch_metadata_repairs.entry(branch)
                                {
                                    entry.insert(());
                                    self.transport
                                        .send(SyncMessage::FetchBranchMetadata {
                                            branches: vec![branch],
                                        })
                                        .map_err(transport_error)?;
                                }
                                continue;
                            }
                            #[cfg(not(feature = "sync-autopsy"))]
                            let _ = subscription;
                            let missing = {
                                let mut node = self.node.borrow_mut();
                                node.missing_known_state_row_version_refs(&message)?
                            };
                            if missing.is_empty() {
                                push_view_update_message_for_receiver(
                                    &mut pending_view_updates,
                                    message,
                                )?;
                                #[cfg(feature = "sync-autopsy")]
                                sync_autopsy::record(format!(
                                    "upstream applied view update {}",
                                    summarize_subscription_key(subscription)
                                ));
                            } else {
                                #[cfg(feature = "sync-autopsy")]
                                sync_autopsy::record(format!(
                                    "upstream queued repair {} missing={}",
                                    summarize_subscription_key(subscription),
                                    missing.len()
                                ));
                                self.transport
                                    .send(SyncMessage::FetchRowVersions {
                                        requests: missing.clone(),
                                    })
                                    .map_err(transport_error)?;
                                pending_row_version_repairs.push_back(PendingRowVersionRepair {
                                    requests: missing,
                                    update: message,
                                });
                            }
                        }
                        SyncMessage::SubscribeRejected {
                            subscription,
                            reason,
                        } => {
                            stats.subscription_events += route_upstream_subscription_rejection(
                                &self.subscriptions,
                                &self.upstream_subscription_owners,
                                subscription,
                                reason,
                            );
                        }
                        SyncMessage::BranchMetadata(metadata) => {
                            let branch = metadata.branch_id;
                            self.node
                                .borrow_mut()
                                .acknowledge_branch_metadata(&metadata)?;
                            self.node
                                .borrow_mut()
                                .apply_sync_message(SyncMessage::BranchMetadata(metadata))?;
                            pending_branch_metadata_repairs.remove(&branch);
                            if let Some(updates) = pending_branch_view_updates.remove(&branch) {
                                for update in updates {
                                    push_view_update_message_for_receiver(
                                        &mut pending_view_updates,
                                        update,
                                    )?;
                                }
                            }
                        }
                        message => {
                            if let SyncMessage::CommitUnit { tx, .. } = &message
                                && let crate::tx::BranchLineage::Branch(branch) = tx.target_lineage
                                && self.node.borrow().branch_record(branch).is_none()
                            {
                                if let std::collections::btree_map::Entry::Vacant(entry) =
                                    pending_branch_metadata_repairs.entry(branch)
                                {
                                    entry.insert(());
                                    self.transport
                                        .send(SyncMessage::FetchBranchMetadata {
                                            branches: vec![branch],
                                        })
                                        .map_err(transport_error)?;
                                }
                            }
                            if !pending_view_updates.is_empty() {
                                self.node.borrow_mut().apply_view_updates_in_batch(
                                    std::mem::take(&mut pending_view_updates),
                                )?;
                            }
                            self.node
                                .borrow_mut()
                                .apply_sync_message_with_ingest_context(message, None)?;
                        }
                    }
                    if let Some(tx_id) = write_state_tx_id {
                        notify_write_state_waiters(&self.write_state_waiters, tx_id);
                    }
                    applied = true;
                }
                if !pending_view_updates.is_empty() {
                    self.node
                        .borrow_mut()
                        .apply_view_updates_in_batch(pending_view_updates)?;
                }
                if applied {
                    stats.subscription_events +=
                        refresh_subscriptions_in(&self.node, &self.subscriptions)?;
                    stats.remote_sync_applied += 1;
                    let next = self.subscriber_dirty_epoch.get().wrapping_add(1);
                    self.subscriber_dirty_epoch.set(next);
                    schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                }
            }
            ConnectionLink::Subscriber {
                peer,
                ingest_context,
                outbox,
                upstream_subscriptions,
                served,
                coverage_groups,
                shape_registrations,
                deferred_subscribe_rejections,
                served_current_rows,
                pending_branch_metadata_repairs,
                pending_session_branch_metadata,
                branch_metadata_repair_cursor,
                serve_dirty,
            } => {
                let repairs = next_branch_metadata_repairs(
                    pending_branch_metadata_repairs,
                    branch_metadata_repair_cursor,
                );
                if !repairs.is_empty() {
                    self.transport
                        .send(SyncMessage::FetchBranchMetadata { branches: repairs })
                        .map_err(transport_error)?;
                }
                if ingest_context.trust == CommitUnitTrust::Session {
                    let pending_ids = pending_session_branch_metadata
                        .keys()
                        .copied()
                        .collect::<Vec<_>>();
                    for branch in pending_ids {
                        let metadata = pending_session_branch_metadata
                            .get(&branch)
                            .cloned()
                            .expect("pending branch metadata id remains present");
                        if self.node.borrow_mut().admit_session_branch_metadata(
                            metadata.clone(),
                            ingest_context.identity,
                        )? {
                            pending_session_branch_metadata.remove(&branch);
                            let responses = self.node.borrow_mut().apply_sync_message(
                                SyncMessage::BranchMetadata(metadata.clone()),
                            )?;
                            for response in responses {
                                send_with_content_extents(
                                    &self.node,
                                    peer,
                                    self.transport.as_mut(),
                                    response,
                                )?;
                            }
                            self.transport
                                .send(SyncMessage::BranchMetadata(metadata))
                                .map_err(transport_error)?;
                        }
                    }
                }
                let mut applied_inbound = false;
                let mut scheduled_immediate = false;
                let mut sent_view_update = false;
                while let Some(message) = self.transport.try_recv() {
                    applied_inbound = true;
                    let admitted_metadata = match &message {
                        SyncMessage::BranchMetadata(metadata) => Some(metadata.branch_id),
                        _ => None,
                    };
                    #[cfg(feature = "sync-autopsy")]
                    sync_autopsy::record(format!(
                        "subscriber recv {}",
                        summarize_sync_message(&message)
                    ));
                    match message {
                        SyncMessage::RegisterShape {
                            shape_id,
                            opts,
                            ast,
                        } => {
                            let registration_key = (shape_id, opts.read_view_key());
                            if let Err(message) = validate_shape_ast_size(&ast) {
                                shape_registrations.insert(
                                    registration_key,
                                    SubscriberShapeRegistration::RejectedUnsupportedCapability(
                                        message.clone(),
                                    ),
                                );
                                send_unsupported_shape_capability_rejection(
                                    &mut *self.transport,
                                    register_shape_rejection_subscription(
                                        shape_id,
                                        opts.read_view_key(),
                                    ),
                                    message,
                                )
                                .map_err(transport_error)?;
                                continue;
                            }
                            if let Err(error) = ensure_supported_register_shape_options(&opts) {
                                shape_registrations.insert(
                                    registration_key,
                                    SubscriberShapeRegistration::RejectedUnsupportedCapability(
                                        error.message.clone(),
                                    ),
                                );
                                send_unsupported_shape_capability_rejection(
                                    &mut *self.transport,
                                    register_shape_rejection_subscription(
                                        shape_id,
                                        opts.read_view_key(),
                                    ),
                                    error.message,
                                )
                                .map_err(transport_error)?;
                                continue;
                            }
                            let shape_validation = {
                                let node = self.node.borrow();
                                validate_shape_ast_for_registration(&node, shape_id, &ast)
                            };
                            let shape = match shape_validation {
                                Ok(Some(shape)) => Some(shape),
                                Ok(None) => None,
                                Err(error) => {
                                    if is_server_shape_validation_failure(&error) {
                                        reject_server_subscription_failure(
                                            &mut *self.transport,
                                            register_shape_rejection_subscription(
                                                shape_id,
                                                opts.read_view_key(),
                                            ),
                                            &error,
                                        )
                                        .map_err(transport_error)?;
                                    } else {
                                        drop_peer_request(&self.node);
                                    }
                                    continue;
                                }
                            };
                            if let Some(shape) = &shape {
                                if shape.params().is_empty() {
                                    let binding = shape.bind(BTreeMap::new()).map_err(Error::from);
                                    let binding = match binding {
                                        Ok(binding) => binding,
                                        Err(_) => {
                                            drop_peer_request(&self.node);
                                            continue;
                                        }
                                    };
                                    let supported = self
                                        .node
                                        .borrow_mut()
                                        .ensure_peer_maintained_subscription_view_supported(
                                            shape,
                                            &binding,
                                            opts.tier,
                                            subscriber_permission_subject(*ingest_context),
                                            &opts.read_view,
                                        );
                                    if let Err(crate::node::Error::QueryCapability(detail)) =
                                        supported
                                    {
                                        shape_registrations.insert(
                                            registration_key,
                                            SubscriberShapeRegistration::RejectedUnsupportedCapability(
                                                detail.clone(),
                                            ),
                                        );
                                        let subscription = SubscriptionKey {
                                            shape_id,
                                            binding_id: binding.binding_id(),
                                            read_view: opts.read_view_key(),
                                        };
                                        send_unsupported_shape_capability_rejection(
                                            &mut *self.transport,
                                            subscription,
                                            detail,
                                        )
                                        .map_err(transport_error)?;
                                        continue;
                                    } else if let Err(error) = supported {
                                        reject_server_subscription_failure(
                                            &mut *self.transport,
                                            SubscriptionKey {
                                                shape_id,
                                                binding_id: binding.binding_id(),
                                                read_view: opts.read_view_key(),
                                            },
                                            &error,
                                        )
                                        .map_err(transport_error)?;
                                        continue;
                                    }
                                }
                            }
                            let awaiting_catalogue_admission = shape.is_none();
                            if let Some(existing) = shape_registrations.get(&registration_key) {
                                match existing {
                                    SubscriberShapeRegistration::Registered(existing_opts)
                                    | SubscriberShapeRegistration::PendingCatalogueAdmission(
                                        existing_opts,
                                    ) if existing_opts != &opts => {
                                        drop_peer_request(&self.node);
                                        continue;
                                    }
                                    SubscriberShapeRegistration::RejectedUnsupportedCapability(
                                        detail,
                                    ) => {
                                        send_unsupported_shape_capability_rejection(
                                            &mut *self.transport,
                                            register_shape_rejection_subscription(
                                                shape_id,
                                                opts.read_view_key(),
                                            ),
                                            detail.clone(),
                                        )
                                        .map_err(transport_error)?;
                                        continue;
                                    }
                                    _ => {}
                                }
                            }
                            let rejection_subscription =
                                register_shape_rejection_subscription(shape_id, registration_key.1);
                            let register_result = {
                                self.node.borrow_mut().apply_sync_message(
                                    SyncMessage::RegisterShape {
                                        shape_id,
                                        ast,
                                        opts: RegisterShapeOptions::default(),
                                    },
                                )
                            };
                            if let Err(error) = register_result {
                                reject_server_subscription_failure(
                                    &mut *self.transport,
                                    rejection_subscription,
                                    &error,
                                )
                                .map_err(transport_error)?;
                                continue;
                            }
                            let registration = if awaiting_catalogue_admission {
                                SubscriberShapeRegistration::PendingCatalogueAdmission(opts)
                            } else {
                                SubscriberShapeRegistration::Registered(opts)
                            };
                            shape_registrations.insert(registration_key, registration);
                        }
                        SyncMessage::Subscribe(subscribe) => {
                            if let Err(message) =
                                validate_known_state_declaration(&subscribe.known_state)
                            {
                                let _ = message;
                                drop_peer_request(&self.node);
                                continue;
                            }
                            let shape_id = subscribe.shape_id;
                            let subscription = subscribe.subscription;
                            let values = subscribe.values.clone();
                            let known_state = subscribe.known_state.clone();
                            let registration_key = (shape_id, subscription.read_view);
                            let Some(registration) =
                                shape_registrations.get(&registration_key).cloned()
                            else {
                                drop_peer_request(&self.node);
                                continue;
                            };
                            let pending_catalogue_admission = matches!(
                                &registration,
                                SubscriberShapeRegistration::PendingCatalogueAdmission(_)
                            );
                            let opts = match registration {
                                SubscriberShapeRegistration::RejectedUnsupportedCapability(
                                    detail,
                                ) => {
                                    // Keep the original permanent rejection, but let views
                                    // already served by this connection flush first. A rejected
                                    // shape must not starve unrelated subscriptions.
                                    deferred_subscribe_rejections.push_back((subscription, detail));
                                    continue;
                                }
                                SubscriberShapeRegistration::Registered(opts)
                                | SubscriberShapeRegistration::PendingCatalogueAdmission(opts) => {
                                    opts
                                }
                            };
                            let Some(shape) = self.node.borrow().registered_shape(shape_id) else {
                                if pending_catalogue_admission {
                                    self.transport
                                        .send(SyncMessage::SubscribeRejected {
                                            subscription,
                                            reason: SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission,
                                        })
                                        .map_err(transport_error)?;
                                } else {
                                    drop_peer_request(&self.node);
                                }
                                continue;
                            };
                            if pending_catalogue_admission {
                                shape_registrations.insert(
                                    registration_key,
                                    SubscriberShapeRegistration::Registered(opts.clone()),
                                );
                            }
                            let value_map = shape
                                .params()
                                .keys()
                                .cloned()
                                .zip(values)
                                .collect::<BTreeMap<_, _>>();
                            let binding = match shape.bind(value_map) {
                                Ok(binding) => binding,
                                Err(_) => {
                                    drop_peer_request(&self.node);
                                    continue;
                                }
                            };
                            if ensure_supported_register_shape_options(&opts).is_err() {
                                drop_peer_request(&self.node);
                                continue;
                            }
                            let supported = self
                                .node
                                .borrow_mut()
                                .ensure_peer_maintained_subscription_view_supported(
                                    &shape,
                                    &binding,
                                    opts.tier,
                                    subscriber_permission_subject(*ingest_context),
                                    &opts.read_view,
                                );
                            if let Err(crate::node::Error::QueryCapability(detail)) = supported {
                                send_unsupported_shape_capability_rejection(
                                    &mut *self.transport,
                                    subscription,
                                    detail,
                                )
                                .map_err(transport_error)?;
                                continue;
                            } else if let Err(error) = supported {
                                reject_server_subscription_failure(
                                    &mut *self.transport,
                                    subscription,
                                    &error,
                                )
                                .map_err(transport_error)?;
                                continue;
                            }
                            let coverage = coverage_key(&shape, &binding, opts.clone());
                            let group_subscription = SubscriptionKey {
                                shape_id: coverage.shape_id,
                                binding_id: coverage.binding_id,
                                read_view: coverage.opts.read_view_key(),
                            };
                            let first_subscriber = coverage_groups
                                .get(&coverage)
                                .is_none_or(|group| group.subscribers.is_empty());
                            let permissions_ready = subscriber_permissions_ready(
                                self.node.borrow().permissions_ready(),
                                ingest_context.trust,
                            );
                            let update = if !permissions_ready {
                                None
                            } else if first_subscriber {
                                peer.declare_known_state(group_subscription, known_state.clone());
                                let mut node = self.node.borrow_mut();
                                let update_result = peer
                                    .rehydrate_query_for_subscription_with_opts(
                                        &mut node,
                                        group_subscription,
                                        &shape,
                                        &binding,
                                        opts.clone(),
                                    );
                                let update = match update_result {
                                    Ok(update) => update,
                                    Err(crate::node::Error::QueryCapability(detail)) => {
                                        send_unsupported_shape_capability_rejection(
                                            &mut *self.transport,
                                            subscription,
                                            detail,
                                        )
                                        .map_err(transport_error)?;
                                        continue;
                                    }
                                    Err(error) => {
                                        reject_server_subscription_failure(
                                            &mut *self.transport,
                                            subscription,
                                            &error,
                                        )
                                        .map_err(transport_error)?;
                                        continue;
                                    }
                                };
                                #[cfg(feature = "sync-autopsy")]
                                sync_autopsy::record(format!(
                                    "subscriber rehydrate first usage={} group={} update={}",
                                    summarize_subscription_key(subscription),
                                    summarize_subscription_key(group_subscription),
                                    summarize_sync_message(&update)
                                ));
                                Some(retarget_view_update(update, subscription))
                            } else {
                                peer.declare_known_state(subscription, known_state.clone());
                                let mut node = self.node.borrow_mut();
                                let update_result = peer
                                    .rehydrate_query_for_subscription_from_maintained_subscription(
                                        &mut node,
                                        group_subscription,
                                        subscription,
                                        &shape,
                                    );
                                let update = match update_result {
                                    Ok(update) => update,
                                    Err(error) => {
                                        reject_server_subscription_failure(
                                            &mut *self.transport,
                                            subscription,
                                            &error,
                                        )
                                        .map_err(transport_error)?;
                                        continue;
                                    }
                                };
                                #[cfg(feature = "sync-autopsy")]
                                sync_autopsy::record(format!(
                                    "subscriber rehydrate duplicate usage={} group={} update={}",
                                    summarize_subscription_key(subscription),
                                    summarize_subscription_key(group_subscription),
                                    summarize_sync_message(&update)
                                ));
                                Some(update)
                            };
                            self.node
                                .borrow_mut()
                                .apply_sync_message(SyncMessage::Subscribe(subscribe))?;
                            let group =
                                coverage_groups.entry(coverage.clone()).or_insert_with(|| {
                                    CoverageGroup {
                                        shape: shape.clone(),
                                        binding: binding.clone(),
                                        subscribers: BTreeSet::new(),
                                    }
                                });
                            group.subscribers.insert(subscription);
                            served.insert(subscription, coverage);
                            if let Some(update) = update {
                                #[cfg(feature = "sync-autopsy")]
                                sync_autopsy::record(format!(
                                    "subscriber send rehydrate {}",
                                    summarize_sync_message(&update)
                                ));
                                self.last_resume_bytes = Some(serialized_sync_message_len(&update));
                                if let ReadViewSourceSpec::Branch { branch } =
                                    &opts.read_view.source
                                {
                                    let metadata =
                                        self.node.borrow_mut().branch_metadata_visible_to(
                                            crate::ids::BranchId(*branch),
                                            peer.link_identity(),
                                        )?;
                                    if metadata {
                                        let metadata = self
                                            .node
                                            .borrow()
                                            .branch_record(crate::ids::BranchId(*branch))
                                            .cloned()
                                            .expect("visible branch metadata remains present");
                                        self.transport
                                            .send(SyncMessage::BranchMetadata((&metadata).into()))
                                            .map_err(transport_error)?;
                                    }
                                }
                                send_with_content_extents(
                                    &self.node,
                                    peer,
                                    self.transport.as_mut(),
                                    update,
                                )?;
                                sent_view_update = true;
                            }
                            if first_subscriber {
                                upstream_subscriptions.borrow_mut().push(
                                    PendingUpstreamCommand::Subscribe(
                                        PendingUpstreamSubscription {
                                            subscription: group_subscription,
                                            shape: shape.clone(),
                                            binding,
                                            opts,
                                            identity: peer.link_identity(),
                                        },
                                    ),
                                );
                            }
                            schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                            scheduled_immediate = true;
                        }
                        SyncMessage::Unsubscribe { subscription } => {
                            self.node.borrow_mut().apply_unsubscribe(subscription);
                            if let Some(coverage) = served.remove(&subscription) {
                                if let Some(group) = coverage_groups.get_mut(&coverage) {
                                    group.subscribers.remove(&subscription);
                                    if group.subscribers.is_empty() {
                                        let group_subscription = SubscriptionKey {
                                            shape_id: coverage.shape_id,
                                            binding_id: coverage.binding_id,
                                            read_view: coverage.opts.read_view_key(),
                                        };
                                        peer.forget_subscription(group_subscription);
                                        coverage_groups.remove(&coverage);
                                        upstream_subscriptions.borrow_mut().push(
                                            PendingUpstreamCommand::Unsubscribe(group_subscription),
                                        );
                                    }
                                }
                            }
                        }
                        SyncMessage::FetchRowVersions { requests } => {
                            if let Err(message) = validate_fetch_row_versions(&requests) {
                                let _ = message;
                                drop_peer_request(&self.node);
                                continue;
                            }
                            let responses = {
                                let mut node = self.node.borrow_mut();
                                peer.serve_row_versions(&mut node, &requests)?
                            };
                            for response in responses {
                                send_with_content_extents(
                                    &self.node,
                                    peer,
                                    self.transport.as_mut(),
                                    response,
                                )?;
                            }
                        }
                        SyncMessage::FetchBranchMetadata { branches } => {
                            if let Err(message) = validate_fetch_branch_metadata(&branches) {
                                let _ = message;
                                drop_peer_request(&self.node);
                                continue;
                            }
                            // A repair request is not a branch-discovery API.  Only
                            // serve a branch this authenticated link has already
                            // been admitted to read through one of its views.
                            for branch in branches {
                                let admitted = served.values().any(|coverage| {
                                    matches!(
                                        coverage.opts.read_view.source,
                                        ReadViewSourceSpec::Branch { branch: view_branch }
                                            if crate::ids::BranchId(view_branch) == branch
                                    )
                                });
                                let visible = admitted
                                    && self
                                        .node
                                        .borrow_mut()
                                        .branch_metadata_visible_to(branch, peer.link_identity())?;
                                if visible {
                                    let metadata =
                                        self.node.borrow().branch_record(branch).cloned();
                                    if let Some(metadata) = metadata {
                                        self.transport
                                            .send(SyncMessage::BranchMetadata((&metadata).into()))
                                            .map_err(transport_error)?;
                                    }
                                }
                            }
                        }
                        SyncMessage::FetchContentExtent { owner, extent } => {
                            let response = {
                                let mut node = self.node.borrow_mut();
                                peer.serve_content_extents(&mut node, owner.row, vec![extent])?
                            };
                            self.transport.send(response).map_err(transport_error)?;
                        }
                        // Branch routing records select persistent partitions.
                        // Sessions may introduce their own locally-authored
                        // record only when its creator matches the authenticated
                        // link and its declared dependencies are available.
                        other => {
                            if let SyncMessage::BranchMetadata(metadata) = &other
                                && ingest_context.trust == CommitUnitTrust::Session
                            {
                                let admitted =
                                    self.node.borrow_mut().admit_session_branch_metadata(
                                        metadata.clone(),
                                        ingest_context.identity,
                                    )?;
                                if !admitted {
                                    if let Some(existing) =
                                        pending_session_branch_metadata.get(&metadata.branch_id)
                                        && existing != metadata
                                    {
                                        return Err(Error::new(
                                            ErrorCode::Protocol,
                                            "conflicting pending branch metadata",
                                        ));
                                    }
                                    pending_session_branch_metadata
                                        .insert(metadata.branch_id, metadata.clone());
                                    continue;
                                }
                            }
                            if let SyncMessage::CommitUnit { tx, .. } = &other
                                && let crate::tx::BranchLineage::Branch(branch) = tx.target_lineage
                                && self.node.borrow().branch_record(branch).is_none()
                            {
                                if let std::collections::btree_map::Entry::Vacant(entry) =
                                    pending_branch_metadata_repairs.entry(branch)
                                {
                                    entry.insert(());
                                    self.transport
                                        .send(SyncMessage::FetchBranchMetadata {
                                            branches: vec![branch],
                                        })
                                        .map_err(transport_error)?;
                                }
                            }
                            if let SyncMessage::ContentExtents { extents } = &other
                                && let Err(message) = validate_content_extents(extents)
                            {
                                let _ = message;
                                drop_peer_request(&self.node);
                                continue;
                            }
                            let relay_upload = match &other {
                                SyncMessage::CommitUnit { tx, .. } => {
                                    Some((tx.tx_id, other.clone()))
                                }
                                _ => None,
                            };
                            if let Some((tx_id, _)) = &relay_upload
                                && !subscriber_permissions_ready(
                                    self.node.borrow().permissions_ready(),
                                    ingest_context.trust,
                                )
                            {
                                let response = SyncMessage::FateUpdate {
                                    tx_id: *tx_id,
                                    fate: Fate::Rejected(RejectionReason::MalformedCommit(
                                        "permissions_head_missing: no published permissions head"
                                            .to_owned(),
                                    )),
                                    global_seq: None,
                                    durability: None,
                                };
                                send_with_content_extents(
                                    &self.node,
                                    peer,
                                    self.transport.as_mut(),
                                    response,
                                )?;
                                continue;
                            }
                            let write_state_tx_id = write_state_update_tx_id(&other);
                            // RegisterShape (registers the shape ahead of its
                            // binding), plus the write-upload path: any
                            // responses (e.g. fate updates) flow back to the
                            // subscriber.
                            let responses = match other {
                                SyncMessage::CommitUnit { tx, versions }
                                    if ingest_context.edge_authority
                                        && matches!(peer.role(), PeerRole::ClientLink { .. }) =>
                                {
                                    if tx.kind == TxKind::Mergeable {
                                        peer.ingest_edge_mergeable_commit_unit(
                                            &mut self.node.borrow_mut(),
                                            tx,
                                            versions,
                                            tick_now_ms,
                                        )?
                                    } else {
                                        self.node
                                            .borrow_mut()
                                            .ingest_relay_commit_unit(tx, versions)?;
                                        Vec::new()
                                    }
                                }
                                other => self
                                    .node
                                    .borrow_mut()
                                    .apply_sync_message_with_ingest_context(
                                        other,
                                        Some(*ingest_context),
                                    )?,
                            };
                            if let Some(tx_id) = write_state_tx_id {
                                notify_write_state_waiters(&self.write_state_waiters, tx_id);
                            }
                            for response in responses {
                                send_with_content_extents(
                                    &self.node,
                                    peer,
                                    self.transport.as_mut(),
                                    response,
                                )?;
                            }
                            if let Some(branch) = admitted_metadata {
                                pending_branch_metadata_repairs.remove(&branch);
                                let metadata = self
                                    .node
                                    .borrow()
                                    .branch_record(branch)
                                    .map(Into::into)
                                    .expect("admitted branch metadata remains present");
                                // This exact echo acknowledges only the
                                // downstream hop. Session admission may have
                                // independently persisted an upstream relay.
                                self.transport
                                    .send(SyncMessage::BranchMetadata(metadata))
                                    .map_err(transport_error)?;
                            }
                            if let Some((tx_id, unit)) = relay_upload {
                                let mut outbox = outbox.borrow_mut();
                                if !outbox.iter().any(|pending| pending.tx_id == tx_id) {
                                    outbox.push(PendingUpload {
                                        tx_id,
                                        unit: Some(unit),
                                    });
                                    schedule_tick_in(&self.scheduler, TickUrgency::Deferred);
                                }
                            }
                        }
                    }
                }
                if applied_inbound && !scheduled_immediate {
                    schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                }
                if applied_inbound {
                    let next = self.subscriber_dirty_epoch.get().wrapping_add(1);
                    self.subscriber_dirty_epoch.set(next);
                    self.observed_subscriber_dirty_epoch.set(next);
                    *serve_dirty = true;
                }
                if *serve_dirty
                    && subscriber_permissions_ready(
                        self.node.borrow().permissions_ready(),
                        ingest_context.trust,
                    )
                {
                    // A coverage-group refresh drains every maintained view below.
                    // Tick the shared runtime once before that drain rather than once
                    // per group.
                    if !coverage_groups.is_empty() {
                        self.node.borrow_mut().flush_query_runtime()?;
                    }
                    for (coverage, group) in coverage_groups.iter() {
                        let group_subscription = SubscriptionKey {
                            shape_id: coverage.shape_id,
                            binding_id: coverage.binding_id,
                            read_view: coverage.opts.read_view_key(),
                        };
                        let update_result = {
                            let mut node = self.node.borrow_mut();
                            peer.query_update_for_subscription_with_opts_after_runtime_flush(
                                &mut node,
                                group_subscription,
                                &group.shape,
                                &group.binding,
                                coverage.opts.clone(),
                            )
                        };
                        let update = match update_result {
                            Ok(update) => update,
                            Err(error) => {
                                for subscription in group.subscribers.iter().copied() {
                                    reject_server_subscription_failure(
                                        &mut *self.transport,
                                        subscription,
                                        &error,
                                    )
                                    .map_err(transport_error)?;
                                }
                                continue;
                            }
                        };
                        if !view_update_is_empty(&update) {
                            #[cfg(feature = "sync-autopsy")]
                            sync_autopsy::record(format!(
                                "subscriber generated group delta group={} update={}",
                                summarize_subscription_key(group_subscription),
                                summarize_sync_message(&update)
                            ));
                            for subscription in group.subscribers.iter().copied() {
                                let update = retarget_view_update(update.clone(), subscription);
                                #[cfg(feature = "sync-autopsy")]
                                sync_autopsy::record(format!(
                                    "subscriber send group delta {}",
                                    summarize_sync_message(&update)
                                ));
                                send_with_content_extents(
                                    &self.node,
                                    peer,
                                    self.transport.as_mut(),
                                    update,
                                )?;
                                sent_view_update = true;
                            }
                        }
                    }
                    for table in served_current_rows.values() {
                        let update = {
                            let mut node = self.node.borrow_mut();
                            peer.current_rows_update(&mut node, table)?
                        };
                        if !view_update_is_empty(&update) {
                            send_with_content_extents(
                                &self.node,
                                peer,
                                self.transport.as_mut(),
                                update,
                            )?;
                            sent_view_update = true;
                        }
                    }
                    *serve_dirty = false;
                }
                if sent_view_update {
                    while let Some((subscription, detail)) =
                        deferred_subscribe_rejections.pop_front()
                    {
                        send_unsupported_shape_capability_rejection(
                            &mut *self.transport,
                            subscription,
                            detail,
                        )
                        .map_err(transport_error)?;
                    }
                }
                let fate_updates = {
                    let mut node = self.node.borrow_mut();
                    peer.drain_deferred_edge_fates(&mut node, tick_now_ms)?
                };
                for update in fate_updates {
                    send_with_content_extents(&self.node, peer, self.transport.as_mut(), update)?;
                }
            }
        }
        Ok(stats)
    }
}

impl<S> PeerConnection<S>
where
    S: OrderedKvStorage,
{
    fn mark_subscriber_dirty(&mut self) -> bool {
        if let ConnectionLink::Subscriber { serve_dirty, .. } = &mut self.link {
            *serve_dirty = true;
            self.observed_subscriber_dirty_epoch
                .set(self.subscriber_dirty_epoch.get());
            true
        } else {
            false
        }
    }

    fn observe_shared_subscriber_dirty_epoch(&mut self) {
        let epoch = self.subscriber_dirty_epoch.get();
        if self.observed_subscriber_dirty_epoch.get() == epoch {
            return;
        }
        self.observed_subscriber_dirty_epoch.set(epoch);
        if let ConnectionLink::Subscriber { serve_dirty, .. } = &mut self.link {
            *serve_dirty = true;
        }
    }

    fn next_now_ms(&self) -> u64 {
        let next = self.next_now_ms.get();
        self.next_now_ms.set(next + 1);
        next
    }

    fn eviction_pins(&self) -> crate::peer::PeerEvictionPins {
        match &self.link {
            ConnectionLink::Subscriber { peer, .. } => peer.eviction_pins(),
            ConnectionLink::Upstream { .. } => crate::peer::PeerEvictionPins::default(),
        }
    }
}

fn schedule_tick_in(scheduler: &SharedTickScheduler, urgency: TickUrgency) {
    if let Some(scheduler) = scheduler.borrow().as_ref() {
        scheduler.schedule_tick(urgency);
    }
}

fn serialized_sync_message_len(message: &SyncMessage) -> usize {
    #[cfg(feature = "cold-settle-attribution")]
    let started = Instant::now();
    let encoded = encode_sync_message(message);
    #[cfg(feature = "cold-settle-attribution")]
    crate::cold_settle_attribution::record_preflight_payload(
        started.elapsed().as_nanos() as u64,
        encoded.as_ref().map_or(0, Vec::len),
    );
    encoded.map_or(0, |bytes| bytes.len())
}

fn view_update_parts_from_message(message: SyncMessage) -> ViewUpdateParts {
    match message {
        SyncMessage::ViewUpdate {
            subscription,
            settled_through,
            reset_result_set,
            version_carriers,
            version_bundles,
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            terminal_operations,
            program_fact_adds,
            program_fact_removes,
        } => ViewUpdateParts {
            subscription,
            settled_through,
            defer_settlement: false,
            reset_result_set,
            version_carriers,
            version_bundles,
            peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
            authorization_progress: peer_payload_inventory.authorization_progress,
            result_member_adds,
            result_member_removes,
            terminal_operations,
            program_fact_adds,
            program_fact_removes,
        },
        _ => unreachable!("expected view update message"),
    }
}

fn push_view_update_message_for_receiver(
    ready: &mut Vec<ViewUpdateParts>,
    message: SyncMessage,
) -> Result<(), Error> {
    ready.push(view_update_parts_from_message(message));
    Ok(())
}

fn transport_error(error: TransportError) -> Error {
    match error {
        TransportError::Backpressure => {
            Error::new(ErrorCode::Backpressure, "transport backpressure")
        }
        TransportError::Failed(message) => Error::new(ErrorCode::Protocol, message),
    }
}

fn drop_peer_request<S>(node: &Rc<RefCell<NodeState<S>>>)
where
    S: OrderedKvStorage,
{
    node.borrow_mut().record_dropped_peer_request();
}

fn handle_transport_backpressure<S>(
    node: &Rc<RefCell<NodeState<S>>>,
    scheduler: &SharedTickScheduler,
    error: &TransportError,
) -> bool
where
    S: OrderedKvStorage,
{
    match error {
        TransportError::Backpressure => {
            node.borrow_mut().record_transport_backpressure_retry();
            schedule_tick_in(scheduler, TickUrgency::Deferred);
            true
        }
        TransportError::Failed(_) => false,
    }
}

fn handle_db_backpressure<S>(
    node: &Rc<RefCell<NodeState<S>>>,
    scheduler: &SharedTickScheduler,
    error: &Error,
) -> bool
where
    S: OrderedKvStorage,
{
    if error.code == ErrorCode::Backpressure {
        node.borrow_mut().record_transport_backpressure_retry();
        schedule_tick_in(scheduler, TickUrgency::Deferred);
        true
    } else {
        false
    }
}

#[cfg(feature = "sync-autopsy")]
fn summarize_subscription_key(subscription: SubscriptionKey) -> String {
    format!(
        "shape={} binding={} read_view={}",
        subscription.shape_id.0, subscription.binding_id.0, subscription.read_view.id
    )
}

#[cfg(feature = "sync-autopsy")]
fn summarize_sync_message(message: &SyncMessage) -> String {
    match message {
        SyncMessage::RegisterShape { shape_id, opts, .. } => {
            format!(
                "RegisterShape shape={} read_view={}",
                shape_id.0,
                opts.read_view_key().id
            )
        }
        SyncMessage::Subscribe(subscribe) => {
            format!(
                "Subscribe {} values={} known_state={}",
                summarize_subscription_key(subscribe.subscription),
                subscribe.values.len(),
                subscribe.known_state.is_some()
            )
        }
        SyncMessage::Unsubscribe { subscription } => {
            format!("Unsubscribe {}", summarize_subscription_key(*subscription))
        }
        SyncMessage::SubscribeRejected {
            subscription,
            reason,
        } => format!(
            "SubscribeRejected {} reason={reason:?}",
            summarize_subscription_key(*subscription)
        ),
        SyncMessage::ViewUpdate {
            subscription,
            settled_through,
            reset_result_set,
            version_carriers,
            version_bundles,
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
            terminal_operations,
        } => format!(
            "ViewUpdate {} settled={} reset={} bundles={} inventory={} adds={} removes={} fact_adds={} fact_removes={} terminal_ops={}",
            summarize_subscription_key(*subscription),
            settled_through.0,
            reset_result_set,
            version_bundles.len()
                + expand_version_carriers(version_carriers)
                    .map(|bundles| bundles.len())
                    .unwrap_or_default(),
            peer_payload_inventory.complete_tx_payloads.len(),
            result_member_adds.len(),
            result_member_removes.len(),
            program_fact_adds.len(),
            program_fact_removes.len(),
            terminal_operations.len()
        ),
        SyncMessage::CommitUnit { tx, .. } => format!("CommitUnit tx={:?}", tx.tx_id),
        SyncMessage::FateUpdate { tx_id, fate, .. } => {
            format!("FateUpdate tx={tx_id:?} fate={fate:?}")
        }
        SyncMessage::FetchRowVersions { requests } => {
            format!("FetchRowVersions requests={}", requests.len())
        }
        SyncMessage::RowVersionPayloads { version_bundles } => {
            format!("RowVersionPayloads bundles={}", version_bundles.len())
        }
        SyncMessage::ContentExtents { extents } => {
            format!("ContentExtents extents={}", extents.len())
        }
        other => format!("{other:?}"),
    }
}

fn send_with_content_extents<S>(
    node: &Rc<RefCell<NodeState<S>>>,
    peer: &mut PeerState,
    transport: &mut dyn Transport,
    message: SyncMessage,
) -> Result<(), Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let snapshot = node.borrow().catalogue_snapshot();
    let catalogue_fingerprint = *blake3::hash(
        &serde_json::to_vec(&snapshot).expect("catalogue snapshot serialization is infallible"),
    )
    .as_bytes();
    if peer.needs_catalogue_snapshot(catalogue_fingerprint) {
        transport
            .send(SyncMessage::CatalogueSnapshot(Box::new(snapshot)))
            .map_err(transport_error)?;
        peer.mark_catalogue_snapshot_announced(catalogue_fingerprint);
    }
    let mut message = message;
    if let SyncMessage::ViewUpdate {
        subscription,
        peer_payload_inventory,
        ..
    } = &mut message
    {
        peer_payload_inventory.authorization_progress =
            Some(peer.authorization_progress_for_subscription(*subscription));
    }
    let extents = match &message {
        SyncMessage::ViewUpdate { .. } => BTreeSet::new(),
        _ => node.borrow().content_refs_in_sync_message(&message)?,
    };
    let mut extents_by_row = BTreeMap::new();
    for extent in extents {
        extents_by_row
            .entry(extent.row)
            .or_insert_with(Vec::new)
            .push(extent);
    }
    for (row, extents) in extents_by_row {
        let response = {
            let mut node = node.borrow_mut();
            peer.serve_content_extents(&mut node, row, extents)?
        };
        #[cfg(feature = "sync-autopsy")]
        sync_autopsy::record(format!(
            "transport send {}",
            summarize_sync_message(&response)
        ));
        transport.send(response).map_err(transport_error)?;
    }
    #[cfg(feature = "sync-autopsy")]
    sync_autopsy::record(format!(
        "transport send {}",
        summarize_sync_message(&message)
    ));
    send_sync_message_chunked(transport, message)
}

fn send_sync_message_chunked(
    transport: &mut dyn Transport,
    message: SyncMessage,
) -> Result<(), Error> {
    transport.send(message).map_err(transport_error)
}

fn send_with_local_content_extents<S>(
    node: &Rc<RefCell<NodeState<S>>>,
    transport: &mut dyn Transport,
    message: SyncMessage,
) -> Result<(), Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let extents = node.borrow().content_refs_in_sync_message(&message)?;
    if !extents.is_empty() {
        let response = {
            let node = node.borrow();
            let mut out = Vec::new();
            for extent in extents {
                out.push(ContentExtent {
                    owner: LargeValueOwnerRef::current_row(extent.row),
                    bytes: node.content_store().read(&extent)?,
                    extent,
                });
            }
            SyncMessage::ContentExtents { extents: out }
        };
        #[cfg(feature = "sync-autopsy")]
        sync_autopsy::record(format!(
            "transport send {}",
            summarize_sync_message(&response)
        ));
        transport.send(response).map_err(transport_error)?;
    }
    #[cfg(feature = "sync-autopsy")]
    sync_autopsy::record(format!(
        "transport send {}",
        summarize_sync_message(&message)
    ));
    transport.send(message).map_err(transport_error)
}

fn view_update_subscription(message: &SyncMessage) -> Option<SubscriptionKey> {
    match message {
        SyncMessage::ViewUpdate { subscription, .. } => Some(*subscription),
        _ => None,
    }
}

fn retarget_view_update(mut message: SyncMessage, target: SubscriptionKey) -> SyncMessage {
    if let SyncMessage::ViewUpdate { subscription, .. } = &mut message {
        *subscription = target;
    }
    message
}

fn write_state_update_tx_id(message: &SyncMessage) -> Option<TxId> {
    match message {
        SyncMessage::FateUpdate { tx_id, .. } => Some(*tx_id),
        _ => None,
    }
}

fn notify_write_state_waiters(waiters: &WriteStateWaiters, tx_id: TxId) {
    let Some(waiters) = waiters.borrow_mut().remove(&tx_id) else {
        return;
    };
    for waiter in waiters {
        match waiter.notify {
            WriteStateWaiterNotify::Future(sender) => {
                let _ = sender.send(());
            }
            WriteStateWaiterNotify::Callback(callback) => callback(),
        }
    }
}

/// Bindings carry values positionally; the shape orders them by param name.
fn binding_values_in_param_order(shape: &ValidatedQuery, binding: &Binding) -> Vec<Value> {
    shape
        .params()
        .keys()
        .map(|name| {
            binding
                .values()
                .get(name)
                .cloned()
                .expect("binding is missing a shape parameter value")
        })
        .collect()
}

/// A `ViewUpdate` that carries no version, result-set, or program-fact change —
/// nothing to ship to the subscriber this tick.
fn view_update_is_empty(message: &SyncMessage) -> bool {
    match message {
        SyncMessage::ViewUpdate {
            reset_result_set,
            version_carriers,
            version_bundles,
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
            ..
        } => {
            !reset_result_set
                && version_carriers.is_empty()
                && version_bundles.is_empty()
                && peer_payload_inventory.complete_tx_payloads.is_empty()
                && result_member_adds.is_empty()
                && result_member_removes.is_empty()
                && program_fact_adds.is_empty()
                && program_fact_removes.is_empty()
        }
        _ => false,
    }
}

/// Identity attached to locally-authored writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DbIdentity {
    /// Node identity.
    pub node: NodeUuid,
    /// Application author identity.
    pub author: AuthorId,
}

/// Configuration for [`Db::open`].
pub struct DbConfig<S> {
    /// Runtime schema.
    pub schema: JazzSchema,
    /// Storage implementation.
    pub storage: S,
    /// Local identity.
    pub identity: DbIdentity,
    /// Row id source used by [`Db::insert`].
    ///
    /// `None` selects the production source.
    pub id_source: Option<Box<dyn RowIdSource>>,
    /// Local large-value checkpoint density in edit operations.
    ///
    /// Checkpoints are derived content-store state and are not synced. A zero
    /// value is treated as one.
    pub large_value_checkpoint_op_interval: usize,
}

impl<S> DbConfig<S> {
    /// Build a config using the production row id source.
    pub fn new(schema: JazzSchema, storage: S, identity: DbIdentity) -> Self {
        Self {
            schema,
            storage,
            identity,
            id_source: None,
            large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
        }
    }

    /// Override the row id source, typically with [`SeededRowIdSource`] in tests.
    pub fn with_id_source(mut self, id_source: impl RowIdSource + 'static) -> Self {
        self.id_source = Some(Box::new(id_source));
        self
    }
}

/// Source of uuidv7-shaped row ids for [`Db::insert`].
pub trait RowIdSource {
    /// Return the next row id.
    fn next_row_id(&mut self) -> RowUuid;
}

/// Production row id source using the system clock and OS randomness.
///
/// Tests and simulations should use [`SeededRowIdSource`] instead.
#[derive(Clone, Debug, Default)]
pub struct ProductionRowIdSource;

impl RowIdSource for ProductionRowIdSource {
    fn next_row_id(&mut self) -> RowUuid {
        RowUuid(uuid::Uuid::now_v7())
    }
}

/// Deterministic uuidv7-shaped row id source for tests and simulations.
#[derive(Clone, Debug)]
pub struct SeededRowIdSource {
    millis: u64,
    state: u64,
}

impl SeededRowIdSource {
    /// Create a deterministic source from a caller-provided seed.
    pub fn new(seed: u64) -> Self {
        Self {
            millis: seed & ((1_u64 << 48) - 1),
            state: seed ^ 0x9e37_79b9_7f4a_7c15,
        }
    }
}

impl RowIdSource for SeededRowIdSource {
    fn next_row_id(&mut self) -> RowUuid {
        let millis = self.millis & ((1_u64 << 48) - 1);
        self.millis = self.millis.wrapping_add(1);

        let rand_a = (splitmix64(&mut self.state) & 0x0fff) as u16;
        let rand_b = splitmix64(&mut self.state) & ((1_u64 << 62) - 1);

        let mut bytes = [0_u8; 16];
        bytes[..6].copy_from_slice(&millis.to_be_bytes()[2..]);
        let version_and_rand_a = 0x7000_u16 | rand_a;
        bytes[6..8].copy_from_slice(&version_and_rand_a.to_be_bytes());
        let variant_and_rand_b = 0x8000_0000_0000_0000_u64 | rand_b;
        bytes[8..16].copy_from_slice(&variant_and_rand_b.to_be_bytes());
        RowUuid::from_bytes(bytes)
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// One-shot read options.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ReadOpts {
    /// Durability tier that gates the first result.
    pub tier: DurabilityTier,
    /// Whether own local updates are visible immediately.
    pub local_updates: LocalUpdates,
    /// Whether evaluation may propagate upstream.
    pub propagation: Propagation,
    /// Include current rows whose deletion winner is `Deleted`.
    pub include_deleted: bool,
    /// Semantic read view to evaluate against.
    pub read_view: ReadViewSpec,
}

impl Default for ReadOpts {
    fn default() -> Self {
        Self {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::Full,
            include_deleted: false,
            read_view: ReadViewSpec::default(),
        }
    }
}

/// Own-write overlay policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum LocalUpdates {
    /// Include local writes immediately.
    Immediate,
    /// Defer local writes until the requested tier observes them.
    Deferred,
}

/// Read propagation policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Propagation {
    /// Full propagation may be used by future remote paths.
    Full,
    /// Evaluate only against local knowledge.
    LocalOnly,
}

/// Public API error with stable machine-readable codes.
#[derive(Debug, Error, serde::Deserialize, serde::Serialize)]
#[error("{code:?}: {message}")]
pub struct Error {
    /// Stable error code.
    pub code: ErrorCode,
    /// Human-readable detail.
    pub message: String,
}

impl Error {
    fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

fn row_already_deleted(row: RowUuid) -> Error {
    Error::new(
        ErrorCode::WriteRejected,
        format!("row already deleted: {}", row.0),
    )
}

fn read_for_write_denied(operation: &str, table: &str) -> Error {
    Error::new(
        ErrorCode::WriteRejected,
        format!(
            "read policy denied {operation} on table {table}: the operation requires read permission on the target row"
        ),
    )
}

/// Stable API error code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ErrorCode {
    /// Schema validation failed.
    Schema,
    /// Query validation or binding failed.
    Query,
    /// Write was rejected.
    WriteRejected,
    /// Storage failed.
    Storage,
    /// Protocol or local node operation failed.
    Protocol,
    /// Local transport queue is full and the operation should be retried later.
    Backpressure,
    /// Requested observation is not locally available in this slice.
    NotObserved,
    /// Historical read must be evaluated by a complete-history server.
    HistoricalReadRequiresServer,
}

impl From<crate::node::Error> for Error {
    fn from(error: crate::node::Error) -> Self {
        let code = match &error {
            crate::node::Error::HistoricalReadRequiresServer => {
                ErrorCode::HistoricalReadRequiresServer
            }
            crate::node::Error::Storage(_) | crate::node::Error::Groove(_) => ErrorCode::Storage,
            crate::node::Error::Query(_) => ErrorCode::Query,
            crate::node::Error::TableNotFound(_)
            | crate::node::Error::UnsupportedColumnType(_)
            | crate::node::Error::InvalidMergeableCommit(_) => ErrorCode::Schema,
            _ => ErrorCode::Protocol,
        };
        Self::new(code, error.to_string())
    }
}

impl From<QueryError> for Error {
    fn from(error: QueryError) -> Self {
        Self::new(ErrorCode::Query, error.to_string())
    }
}

#[doc(hidden)]
pub mod doctest_support {
    use std::collections::BTreeMap;
    use std::future::Future;

    use groove::records::Value;
    use groove::schema::{ColumnSchema, ColumnType};
    pub use groove::storage::MemoryStorage;

    use crate::db::{Db, DbConfig, DbIdentity, Error, RowCells, SeededRowIdSource};
    use crate::ids::{AuthorId, NodeUuid};
    use crate::schema::{JazzSchema, Policy, TableSchema};

    /// Poll a ready-immediate Db future in examples.
    pub fn block_on<F: Future>(future: F) -> F::Output {
        crate::db::block_on(future)
    }

    /// Example schema used by Db doctests.
    pub fn schema() -> JazzSchema {
        JazzSchema::new([TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("done", ColumnType::Bool),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public())])
    }

    /// Open a fresh Db over in-memory storage.
    pub async fn open_todos_db() -> Result<Db<MemoryStorage>, Error> {
        let schema = schema();
        let cfs = schema.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        Db::open(DbConfig {
            schema,
            storage: MemoryStorage::new(&refs),
            identity: DbIdentity {
                node: NodeUuid::from_bytes([0x11; 16]),
                author: AuthorId::from_bytes([0xa1; 16]),
            },
            id_source: Some(Box::new(SeededRowIdSource::new(0x1111))),
            large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
        })
        .await
    }

    /// Todo row payload for examples.
    pub fn todo_cells(title: &str, done: bool) -> RowCells {
        BTreeMap::from([
            ("title".to_owned(), Value::String(title.to_owned())),
            ("done".to_owned(), Value::Bool(done)),
        ])
    }
}

fn effective_read_tier(opts: &ReadOpts) -> DurabilityTier {
    if opts.local_updates == LocalUpdates::Immediate {
        opts.tier.max(DurabilityTier::Local)
    } else {
        opts.tier
    }
}

fn upstream_register_shape_options(
    tier: DurabilityTier,
    read_view: ReadViewSpec,
) -> RegisterShapeOptions {
    RegisterShapeOptions {
        tier: remote_subscription_tier(tier),
        read_view,
    }
}

fn remote_subscription_tier(_tier: DurabilityTier) -> DurabilityTier {
    DurabilityTier::Global
}

fn ensure_default_read_view(opts: &ReadOpts) -> Result<(), Error> {
    if opts.read_view.is_default() {
        return Ok(());
    }
    Err(Error::new(
        ErrorCode::Query,
        "non-default read_view is not supported yet; reads currently execute against the current/default view",
    ))
}

fn ensure_supported_read_view(opts: &ReadOpts) -> Result<(), Error> {
    match &opts.read_view.source {
        ReadViewSourceSpec::Current => Ok(()),
        ReadViewSourceSpec::Branch { .. }
            if opts.read_view.schema == Default::default()
                && opts.read_view.overlays.is_empty() =>
        {
            Ok(())
        }
        _ => ensure_default_read_view(opts),
    }
}

fn ensure_supported_subscription_read_opts(opts: &ReadOpts) -> Result<(), Error> {
    if opts.include_deleted {
        return Err(Error::new(
            ErrorCode::Query,
            "live subscriptions do not support include_deleted yet",
        ));
    }
    ensure_supported_read_view(opts)
}

fn ensure_supported_register_shape_read_view(opts: &RegisterShapeOptions) -> Result<(), Error> {
    let read_opts = ReadOpts {
        read_view: opts.read_view.clone(),
        ..ReadOpts::default()
    };
    ensure_supported_read_view(&read_opts)
}

fn ensure_supported_register_shape_options(opts: &RegisterShapeOptions) -> Result<(), Error> {
    ensure_supported_register_shape_read_view(opts)?;
    if opts.tier != DurabilityTier::Global {
        return Err(Error::new(
            ErrorCode::Query,
            "sync subscription serving supports only global-tier registration",
        ));
    }
    Ok(())
}

fn validate_shape_ast_for_registration<S>(
    node: &NodeState<S>,
    shape_id: ShapeId,
    ast: &ShapeAst,
) -> Result<Option<ValidatedQuery>, crate::node::Error>
where
    S: OrderedKvStorage,
{
    node.validate_shape_ast_for_registration(shape_id, ast)
}

fn send_unsupported_shape_capability_rejection(
    transport: &mut dyn Transport,
    subscription: SubscriptionKey,
    detail: String,
) -> Result<(), TransportError> {
    send_subscription_rejection(
        transport,
        subscription,
        SubscribeRejectReason::UnsupportedShapeCapability { detail },
    )
}

fn reject_server_subscription_failure(
    transport: &mut dyn Transport,
    subscription: SubscriptionKey,
    error: &crate::node::Error,
) -> Result<(), TransportError> {
    // Keep the complete error on the serving process only. Subscription keys
    // provide a correlation handle without disclosing schema, policy, or
    // storage details to the peer.
    eprintln!(
        "jazz subscription rejected: shape={} binding={} read_view={} server_error={error}",
        subscription.shape_id.0, subscription.binding_id.0, subscription.read_view.id,
    );
    send_subscription_rejection(
        transport,
        subscription,
        SubscribeRejectReason::ServerFailure {
            code: server_failure_code(error),
        },
    )
}

fn send_subscription_rejection(
    transport: &mut dyn Transport,
    subscription: SubscriptionKey,
    reason: SubscribeRejectReason,
) -> Result<(), TransportError> {
    transport.send(SyncMessage::SubscribeRejected {
        subscription,
        reason,
    })
}

fn server_failure_code(error: &crate::node::Error) -> SubscribeServerFailureCode {
    match error {
        crate::node::Error::TableNotFound(_)
        | crate::node::Error::Query(QueryError::UnknownTable(_)) => {
            SubscribeServerFailureCode::TableNotFound
        }
        crate::node::Error::Query(_) => SubscribeServerFailureCode::QueryValidation,
        crate::node::Error::QueryLowering(_) => SubscribeServerFailureCode::QueryLowering,
        crate::node::Error::AuthorizationDenied => SubscribeServerFailureCode::PolicyEvaluation,
        crate::node::Error::InvalidStoredValue(_)
        | crate::node::Error::InvalidCatalogueUpdate(_) => {
            SubscribeServerFailureCode::SchemaResolution
        }
        _ => SubscribeServerFailureCode::Internal,
    }
}

fn is_server_shape_validation_failure(error: &crate::node::Error) -> bool {
    matches!(
        error,
        crate::node::Error::TableNotFound(_) | crate::node::Error::Query(_)
    )
}

fn register_shape_rejection_subscription(
    shape_id: ShapeId,
    read_view: ReadViewKey,
) -> SubscriptionKey {
    SubscriptionKey {
        shape_id,
        binding_id: BindingId(uuid::Uuid::nil()),
        read_view,
    }
}

fn coverage_key(
    shape: &ValidatedQuery,
    binding: &Binding,
    opts: RegisterShapeOptions,
) -> CoverageKey {
    CoverageKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        opts,
    }
}

fn subscriber_permissions_ready(permissions_ready: bool, trust: CommitUnitTrust) -> bool {
    trust == CommitUnitTrust::TrustedBackend || permissions_ready
}

fn subscriber_permission_subject(ingest: CommitUnitIngestContext) -> AuthorId {
    match ingest.trust {
        CommitUnitTrust::Session => ingest.identity,
        CommitUnitTrust::TrustedBackend => AuthorId::SYSTEM,
    }
}

/// Row cells supplied to write methods.
pub type RowCells = BTreeMap<String, Value>;

/// Builder for explicit text/blob column edits.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TextEdit {
    ops: Vec<TextEditOp>,
}

impl TextEdit {
    /// Construct an empty edit builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert bytes at `pos`.
    pub fn insert(mut self, pos: usize, bytes: impl Into<Vec<u8>>) -> Self {
        self.ops.push(TextEditOp::Insert {
            pos,
            bytes: bytes.into(),
        });
        self
    }

    /// Delete `len` bytes starting at `pos`.
    pub fn delete(mut self, pos: usize, len: usize) -> Self {
        self.ops.push(TextEditOp::Delete { pos, len });
        self
    }

    fn into_node_ops(self) -> Vec<LargeValueEditOp> {
        self.ops
            .into_iter()
            .map(|op| match op {
                TextEditOp::Insert { pos, bytes } => LargeValueEditOp::Insert(pos, bytes),
                TextEditOp::Delete { pos, len } => LargeValueEditOp::Delete(pos, len),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TextEditOp {
    Insert { pos: usize, bytes: Vec<u8> },
    Delete { pos: usize, len: usize },
}

/// Build [`RowCells`] with bare identifier column names.
///
/// Keys are converted to column names with `stringify!`, and values are
/// converted with `Into<Value>`. Column and type validation remains lazy at
/// write/query validation time.
///
/// ```rust
/// # use jazz::db::doctest_support::{block_on, open_todos_db};
/// # use jazz::tx::DurabilityTier;
/// let db = block_on(open_todos_db())?;
/// let write = db.insert(
///     "todos",
///     jazz::row! {
///         title: "Ship it",
///         done: false,
///     },
/// )?;
/// block_on(write.wait(DurabilityTier::Local))?;
///
/// let todos = db.prepare_query(&db.table("todos"))?;
/// assert_eq!(db.read(&todos)?.len(), 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[macro_export]
macro_rules! row {
    () => {
        $crate::db::RowCells::new()
    };
    ($($key:ident : $value:expr),+ $(,)?) => {{
        let mut cells = $crate::db::RowCells::new();
        $(
            cells.insert(::std::string::String::from(stringify!($key)), ($value).into());
        )+
        cells
    }};
}

/// CRUD operations for an open mergeable transaction.
///
/// [`MergeableTx`] and [`MergeableTxRef`] implement this trait, so mergeable
/// CRUD has one definition regardless of who owns the transaction lifetime.
/// Import this trait to call its methods.
pub trait MergeableTxOps<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// The database that owns the open transaction.
    fn db(&self) -> &Db<S>;

    /// The id of the already-open transaction.
    fn tx_id(&self) -> OpenTxId;

    /// Stage an insert with a generated row id.
    fn insert(&self, table: &str, cells: RowCells) -> Result<RowUuid, Error> {
        let row = self.db().row_id_source.borrow_mut().next_row_id();
        self.insert_with_id(table, row, cells)?;
        Ok(row)
    }

    /// Stage an insert with a caller-supplied row id.
    fn insert_with_id(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<(), Error> {
        self.insert_with_id_at_ms_option(table, row, cells, None)
    }

    /// Stage an insert with a caller-supplied row id and explicit millisecond provenance time.
    fn insert_with_id_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<(), Error> {
        self.insert_with_id_at_ms_option(table, row, cells, Some(now_ms))
    }

    /// Stage an update; omitted fields keep the transaction-local value.
    fn update(&self, table: &str, row: RowUuid, patch: RowCells) -> Result<(), Error> {
        self.update_at_ms_option(table, row, patch, None)
    }

    /// Stage an update with an explicit millisecond provenance time.
    fn update_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        now_ms: u64,
    ) -> Result<(), Error> {
        self.update_at_ms_option(table, row, patch, Some(now_ms))
    }

    /// Stage a soft delete.
    fn delete(&self, table: &str, row: RowUuid) -> Result<(), Error> {
        self.delete_at_ms_option(table, row, None)
    }

    /// Stage a soft delete with explicit millisecond provenance time.
    fn delete_at_ms(&self, table: &str, row: RowUuid, now_ms: u64) -> Result<(), Error> {
        self.delete_at_ms_option(table, row, Some(now_ms))
    }

    /// Stage a restore, applying defaults for omitted columns.
    fn restore(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<(), Error> {
        self.restore_at_ms_option(table, row, cells, None)
    }

    /// Stage a restore with explicit millisecond provenance time, applying defaults for omitted columns.
    fn restore_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<(), Error> {
        self.restore_at_ms_option(table, row, cells, Some(now_ms))
    }

    /// Read one row with this transaction's pending writes overlaid.
    fn read(&self, table: &str, row: RowUuid) -> Result<Option<RowCells>, Error> {
        self.db()
            .node
            .node
            .borrow_mut()
            .tx_read(self.tx_id(), table, row)
            .map_err(Into::into)
    }

    /// Stage an insert with an optional explicit provenance time.
    fn insert_with_id_at_ms_option(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.db()
            .stage_mergeable_insert(self.tx_id(), table, row, cells, now_ms)
    }

    /// Stage an update with an optional explicit provenance time.
    fn update_at_ms_option(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.db()
            .stage_mergeable_update(self.tx_id(), table, row, patch, now_ms)
    }

    /// Stage a deletion with an optional explicit provenance time.
    fn delete_at_ms_option(
        &self,
        table: &str,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.db()
            .stage_mergeable_delete(self.tx_id(), table, row, now_ms)
    }

    /// Stage a restore with an optional explicit provenance time.
    fn restore_at_ms_option(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.db()
            .stage_mergeable_restore(self.tx_id(), table, row, cells, now_ms)
    }
}

/// Owning, Rust-facing handle for a group of mergeable writes.
///
/// This handle owns the transaction lifetime and abandons an uncommitted
/// transaction on drop. Use [`MergeableTxRef`] when a caller retains an
/// [`OpenTxId`] between calls and must not close the transaction on return.
pub struct MergeableTx<'a, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db: &'a Db<S>,
    tx_id: OpenTxId,
    /// Set once the transaction has been committed, so `Drop` does not then
    /// abandon it. Without this, `commit` consumed `self` and `Drop` still ran
    /// `abandon_transaction_handle` on an already-committed transaction — benign
    /// only because `abandon_tx` tolerates an unknown id, and silent because the
    /// result was discarded.
    committed: bool,
}

impl<S> MergeableTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Commit all staged writes as one mergeable transaction.
    ///
    /// Once the commit succeeds, dropping this handle does not abandon the
    /// already-committed transaction. If it fails, dropping the handle attempts
    /// to abandon any transaction that remains open.
    pub fn commit(mut self) -> Result<TxId, Error> {
        let result = self.db.commit_mergeable_handle(self.tx_id);
        if result.is_ok() {
            self.committed = true;
        }
        result
    }
}

impl<S> MergeableTxOps<S> for MergeableTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn db(&self) -> &Db<S> {
        self.db
    }

    fn tx_id(&self) -> OpenTxId {
        self.tx_id
    }
}

/// Non-owning operations handle for an already-open mergeable transaction.
///
/// Construct this with [`Db::mergeable_tx_ref`] when another layer owns the
/// [`OpenTxId`] lifetime. Dropping this ref never abandons the transaction.
pub struct MergeableTxRef<'a, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db: &'a Db<S>,
    tx_id: OpenTxId,
}

impl<S> MergeableTxOps<S> for MergeableTxRef<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn db(&self) -> &Db<S> {
        self.db
    }

    fn tx_id(&self) -> OpenTxId {
        self.tx_id
    }
}

impl<S> Drop for MergeableTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let _ = self.db.abandon_transaction_handle(self.tx_id);
    }
}

/// CRUD and read operations for an open exclusive transaction.
///
/// [`ExclusiveTx`] and [`ExclusiveTxRef`] implement this trait, so exclusive
/// operations have one definition regardless of who owns the transaction
/// lifetime. Import this trait to call its methods.
pub trait ExclusiveTxOps<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// The database that owns the open transaction.
    fn db(&self) -> &Db<S>;

    /// The id of the already-open transaction.
    fn tx_id(&self) -> OpenTxId;

    /// Read one row inside the exclusive transaction.
    fn read(&self, table: &str, row: RowUuid) -> Result<Option<RowCells>, Error> {
        self.db().exclusive_read(self.tx_id(), table, row)
    }

    /// Read all current rows in a table inside the exclusive transaction.
    fn all(&self, table: &str) -> Result<Vec<CurrentRow>, Error> {
        self.db()
            .node
            .node
            .borrow_mut()
            .tx_current_rows(self.tx_id(), table)
            .map_err(Into::into)
    }

    /// Read a prepared query inside the exclusive transaction.
    fn all_prepared(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        self.db().exclusive_all(self.tx_id(), prepared)
    }

    /// Read a prepared query inside the exclusive transaction as `author`.
    fn all_prepared_for_identity(
        &self,
        prepared: &PreparedQuery,
        author: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.db()
            .exclusive_all_for_identity(self.tx_id(), prepared, author)
    }

    /// Stage an insert with a generated row id.
    fn insert(&self, table: &str, cells: RowCells) -> Result<RowUuid, Error> {
        let row = self.db().row_id_source.borrow_mut().next_row_id();
        self.insert_with_id(table, row, cells)?;
        Ok(row)
    }

    /// Stage an insert with a caller-supplied row id.
    fn insert_with_id(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<(), Error> {
        self.db()
            .stage_exclusive_insert(self.tx_id(), table, row, cells)
    }

    /// Stage an update; omitted fields keep the transaction-local value.
    fn update(&self, table: &str, row: RowUuid, patch: RowCells) -> Result<(), Error> {
        let mut cells = self.read(table, row)?.unwrap_or_default();
        cells.extend(patch);
        self.insert_with_id(table, row, cells)
    }

    /// Stage a soft delete.
    fn delete(&self, table: &str, row: RowUuid) -> Result<(), Error> {
        self.db().stage_exclusive_delete(self.tx_id(), table, row)
    }

    /// Stage a restore, applying defaults for omitted columns.
    fn restore(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<(), Error> {
        self.db()
            .stage_exclusive_restore(self.tx_id(), table, row, cells)
    }
}

/// Owning, Rust-facing handle for an exclusive transaction over a stable snapshot.
///
/// This handle owns the transaction lifetime and abandons an uncommitted
/// transaction on drop. Use [`ExclusiveTxRef`] when a caller retains an
/// [`OpenTxId`] between calls and must not close the transaction on return.
pub struct ExclusiveTx<'a, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db: &'a Db<S>,
    tx_id: OpenTxId,
    committed: bool,
}

impl<S> ExclusiveTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Commit the exclusive transaction.
    ///
    /// Once the commit succeeds, dropping this handle does not abandon the
    /// already-committed transaction. If it fails, dropping the handle attempts
    /// to abandon any transaction that remains open.
    pub fn commit(mut self) -> Result<TxId, Error> {
        let result = self.db.commit_exclusive_handle(self.tx_id);
        if result.is_ok() {
            self.committed = true;
        }
        result
    }
}

impl<S> ExclusiveTxOps<S> for ExclusiveTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn db(&self) -> &Db<S> {
        self.db
    }

    fn tx_id(&self) -> OpenTxId {
        self.tx_id
    }
}

impl<S> Drop for ExclusiveTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let _ = self.db.abandon_exclusive_handle(self.tx_id);
    }
}

/// Non-owning operations handle for an already-open exclusive transaction.
///
/// Construct this with [`Db::exclusive_tx_ref`] when another layer owns the
/// [`OpenTxId`] lifetime. Dropping this ref never abandons the transaction.
pub struct ExclusiveTxRef<'a, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db: &'a Db<S>,
    tx_id: OpenTxId,
}

impl<S> ExclusiveTxOps<S> for ExclusiveTxRef<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn db(&self) -> &Db<S> {
        self.db
    }

    fn tx_id(&self) -> OpenTxId {
        self.tx_id
    }
}

/// Handle for an applied local write.
pub struct WriteHandle<S>
where
    S: OrderedKvStorage,
{
    node: Weak<RefCell<NodeState<S>>>,
    row_uuid: RowUuid,
    tx_id: TxId,
    local_tier: DurabilityTier,
}

impl<S> WriteHandle<S>
where
    S: OrderedKvStorage,
{
    /// Generated or caller-supplied row id affected by this write.
    pub fn row_uuid(&self) -> RowUuid {
        self.row_uuid
    }

    /// Mergeable transaction id backing this write.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// let db = block_on(open_todos_db())?;
    /// let write = db.insert("todos", todo_cells("has id", false))?;
    ///
    /// let _row_id = write.row_uuid();
    /// let _tx_id = write.mergeable_tx_id();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn mergeable_tx_id(&self) -> TxId {
        self.tx_id
    }

    /// Wait until this write has reached the requested tier.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// let write = db.insert("todos", todo_cells("wait locally", false))?;
    ///
    /// let tx_id = block_on(write.wait(DurabilityTier::Local))?;
    /// assert_eq!(tx_id, write.mergeable_tx_id());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn wait(&self, tier: DurabilityTier) -> Result<TxId, Error> {
        if tier <= self.local_tier {
            return Ok(self.tx_id);
        }
        let state = self.write_state()?;
        match state.fate {
            Fate::Rejected(reason) => Err(write_rejected(reason)),
            Fate::Pending if tier >= DurabilityTier::Edge => Err(Error::new(
                ErrorCode::NotObserved,
                format!("write has not been accepted at requested tier {tier:?}"),
            )),
            Fate::Pending | Fate::Accepted if state.durability < tier => Err(Error::new(
                ErrorCode::NotObserved,
                format!("write has not reached requested tier {tier:?}"),
            )),
            Fate::Pending | Fate::Accepted => Ok(self.tx_id),
        }
    }

    /// Return the locally observed fate and durability for this write.
    pub fn write_state(&self) -> Result<WriteState, Error> {
        let Some(node) = self.node.upgrade() else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "database handle was dropped",
            ));
        };
        let Some((fate, _, durability)) = node.borrow_mut().transaction_state(self.tx_id) else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "transaction is not known locally",
            ));
        };
        Ok(WriteState { fate, durability })
    }
}

fn write_rejected(reason: RejectionReason) -> Error {
    Error::new(ErrorCode::WriteRejected, format!("{reason:?}"))
}

/// Decode Groove's recursively assembled terminal value into Jazz's typed
/// structured-result view. This separates relation fields from scalar row
/// fields, but does not join or assemble relation facts.
fn materialize_result_tree(query: &Query, snapshot: RelationSnapshot) -> Result<ResultTree, Error> {
    fn node_from_record(
        table: &str,
        record: OwnedRecord,
        arrays: &[crate::query::ArraySubquery],
    ) -> Result<ResultNode, Error> {
        let descriptor = *record.descriptor();
        let values = record.to_values().map_err(|error| {
            Error::new(
                ErrorCode::Protocol,
                format!("invalid Groove terminal record: {error}"),
            )
        })?;
        let array_by_name = arrays
            .iter()
            .map(|array| (array.column_name.as_str(), array))
            .collect::<BTreeMap<_, _>>();
        let mut scalar_fields = Vec::new();
        let mut scalar_values = Vec::new();
        let mut relations = BTreeMap::new();
        for (field, value) in descriptor.fields().iter().zip(values) {
            let Some(name) = field.name.as_deref() else {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    "Groove terminal emitted an unnamed field",
                ));
            };
            let Some(array) = array_by_name.get(name) else {
                scalar_fields.push((name.to_owned(), field.value_type.clone()));
                scalar_values.push(value);
                continue;
            };
            let Value::Array(children) = value else {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    format!("Groove terminal relation {name} was not an array"),
                ));
            };
            let children = children
                .into_iter()
                .map(|value| {
                    let Value::Record(record) = value else {
                        return Err(Error::new(
                            ErrorCode::Protocol,
                            format!("Groove terminal relation {name} contained a non-record"),
                        ));
                    };
                    node_from_record(&array.table, record, &array.nested_arrays)
                })
                .collect::<Result<Vec<_>, _>>()?;
            relations.insert(name.to_owned(), ResultRelation::Array(children));
        }
        let scalar_descriptor = RecordDescriptor::new(scalar_fields);
        let raw = scalar_descriptor.create(&scalar_values).map_err(|error| {
            Error::new(
                ErrorCode::Protocol,
                format!("invalid scalar projection from Groove terminal: {error}"),
            )
        })?;
        let row = CurrentRow::new(table, OwnedRecord::new(raw, scalar_descriptor));
        let occurrence = OutputOccurrenceId::single_source(ObjectId::from_uuid(row.row_uuid().0));
        Ok(ResultNode {
            occurrence,
            row,
            relations,
        })
    }

    if !snapshot.edges.is_empty() {
        return Err(Error::new(
            ErrorCode::Protocol,
            "structured result unexpectedly contained relation-fact edges",
        ));
    }
    let roots = snapshot
        .rows
        .into_iter()
        .take(snapshot.root_count)
        .map(|row| {
            let (descriptor, raw) = row.encoded_record();
            node_from_record(
                row.table(),
                OwnedRecord::new(raw.to_vec(), *descriptor),
                &query.array_subqueries,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ResultTree { roots })
}

struct SubscriptionState {
    terminal_rows: bool,
    kind: SubscriptionKind,
    groove_runtime_token: u64,
    propagates_upstream: bool,
    author: AuthorId,
    read_tier: DurabilityTier,
    remote_read_tier: Option<DurabilityTier>,
    read_view: ReadViewSpec,
    snapshot: RelationSnapshot,
    snapshot_index: RelationSnapshotIndex,
    snapshot_source: SubscriptionSnapshotSource,
    settled: bool,
    sender: UnboundedSender<SubscriptionEvent>,
}

#[derive(Clone, Default)]
struct RelationSnapshotIndex {
    roots: BTreeMap<OutputOccurrenceId, usize>,
    related: BTreeMap<(String, RowUuid), usize>,
    edges: BTreeSet<RelationEdge>,
}

impl RelationSnapshotIndex {
    fn from_snapshot(snapshot: &RelationSnapshot) -> Self {
        let mut index = Self::default();
        for (position, row) in snapshot.rows.iter().take(snapshot.root_count).enumerate() {
            index
                .roots
                .insert(subscription_row_occurrence_id(row), position);
        }
        for (offset, row) in snapshot.rows.iter().skip(snapshot.root_count).enumerate() {
            index.related.insert(
                (row.table().to_owned(), row.row_uuid()),
                snapshot.root_count + offset,
            );
        }
        index.edges = snapshot.edges.iter().cloned().collect();
        index
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubscriptionSnapshotSource {
    LocalMaintained,
    LinkSnapshot,
}

enum SubscriptionKind {
    Prepared {
        shape: ValidatedQuery,
        binding: Binding,
        maintained_subscription: Option<LocalMaintainedViewSubscription>,
    },
}

/// Row identity removed from a materialized subscription result.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RemovedRow {
    /// Logical table that contained the removed row.
    pub table: String,
    /// Stable row identity.
    pub row_uuid: RowUuid,
    /// Stable identity of the removed output occurrence.
    pub occurrence_id: OutputOccurrenceId,
}

/// One row addressed by its maintained output occurrence identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionOutputRow {
    /// Stable output occurrence identity.
    pub occurrence_id: OutputOccurrenceId,
    /// Materialized row body for this output occurrence.
    pub row: CurrentRow,
}

impl std::ops::Deref for SubscriptionOutputRow {
    type Target = CurrentRow;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

/// Delta event emitted by a database subscription stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionEvent {
    /// Incremental or reset result change.
    Delta {
        /// Whether this delta replaces all previously observed rows and edges.
        ///
        /// Fresh subscriptions start with a reset delta from the empty result.
        reset: bool,
        /// Rows newly visible to the subscription.
        added: Vec<SubscriptionOutputRow>,
        /// Rows still visible with changed projected cells.
        updated: Vec<SubscriptionOutputRow>,
        /// Rows no longer visible to the subscription.
        removed: Vec<RemovedRow>,
        /// Typed structural edits to already hydrated terminal rows.
        terminal_operations: Vec<groove::ivm::TerminalOperation>,
        /// Whether the result is complete at the requested read tier.
        settled: bool,
        /// Read tier used to materialize the rows.
        tier: DurabilityTier,
    },
    /// The serving peer rejected the propagated upstream subscription.
    Rejected {
        /// Stable rejection class plus diagnostic detail from the serving peer.
        reason: SubscribeRejectReason,
    },
    /// The subscription stream was closed by the producer.
    Closed,
}

/// Stream of materialized subscription events.
pub struct SubscriptionStream {
    receiver: UnboundedReceiver<SubscriptionEvent>,
    _state: Rc<RefCell<SubscriptionState>>,
    cleanup: Option<Box<dyn FnOnce()>>,
}

struct CleanupGuard {
    cleanup: Option<Box<dyn FnOnce()>>,
}

impl CleanupGuard {
    fn new(cleanup: Box<dyn FnOnce()>) -> Self {
        Self {
            cleanup: Some(cleanup),
        }
    }

    fn take(&mut self) -> Box<dyn FnOnce()> {
        self.cleanup
            .take()
            .expect("cleanup guard must only be disarmed once")
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

impl SubscriptionStream {
    /// Await the next materialized subscription event.
    pub async fn next_event(&mut self) -> Option<SubscriptionEvent> {
        std::future::poll_fn(|cx| Pin::new(&mut self.receiver).poll_next(cx)).await
    }

    /// Return the next queued materialized subscription event without waiting.
    pub fn try_next_event(&mut self) -> Option<SubscriptionEvent> {
        self.receiver.try_recv().ok()
    }
}

impl Stream for SubscriptionStream {
    type Item = SubscriptionEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.receiver).poll_next(cx)
    }
}

impl Drop for SubscriptionStream {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

/// Validated and bound query plan used by all `Db` reads and subscriptions.
#[derive(Clone, Debug)]
pub struct PreparedQuery {
    shape: ValidatedQuery,
    binding: Binding,
    local_plan: Option<PreparedQueryPlanHandle>,
    global_plan: Option<PreparedQueryPlanHandle>,
}

impl PreparedQuery {
    /// Validated query shape.
    pub fn shape(&self) -> &ValidatedQuery {
        &self.shape
    }

    /// Bound parameter values.
    pub fn binding(&self) -> &Binding {
        &self.binding
    }

    fn plan_for_tier(&self, tier: DurabilityTier) -> Option<&PreparedQueryPlanHandle> {
        match tier {
            DurabilityTier::Local => self.local_plan.as_ref(),
            DurabilityTier::Global => self.global_plan.as_ref(),
            DurabilityTier::None | DurabilityTier::Edge => None,
        }
    }

    #[cfg(test)]
    fn has_plan_for_tier(&self, tier: DurabilityTier) -> bool {
        self.plan_for_tier(tier).is_some()
    }
}

fn should_install_prepared_plan(shape: &ValidatedQuery) -> bool {
    !shape.query().joins.is_empty() || !shape.query().reachable.is_empty()
}

fn subscription_delta_event(
    tier: DurabilityTier,
    settled: bool,
    previous: &RelationSnapshot,
    current: &RelationSnapshot,
    _terminal_rows: bool,
) -> SubscriptionEvent {
    subscription_delta_event_with_reset(tier, settled, previous, current, false, _terminal_rows)
}

fn subscription_reset_event(
    tier: DurabilityTier,
    settled: bool,
    current: RelationSnapshot,
    _terminal_rows: bool,
) -> SubscriptionEvent {
    SubscriptionEvent::Delta {
        reset: true,
        added: current
            .rows
            .into_iter()
            .map(subscription_output_row)
            .collect(),
        updated: Vec::new(),
        removed: Vec::new(),
        terminal_operations: Vec::new(),
        settled,
        tier,
    }
}

/// Publishes an ordered terminal as a root-addressed suffix splice.
///
/// A row delta has stable occurrence identities but no positional field. When
/// terminal ordering changes, retracting and re-adding the first changed suffix
/// is therefore the smallest representation that lets every consumer recover
/// the authoritative Groove order. Content-only changes retain their position
/// and remain ordinary `updated` roots, independent of total result size.
fn subscription_terminal_delta_event(
    tier: DurabilityTier,
    settled: bool,
    previous: &RelationSnapshot,
    current: &RelationSnapshot,
) -> SubscriptionEvent {
    let previous_roots = &previous.rows[..previous.root_count];
    let current_roots = &current.rows[..current.root_count];
    let common_prefix = previous_roots
        .iter()
        .zip(current_roots)
        .take_while(|(previous, current)| {
            subscription_row_occurrence_id(previous) == subscription_row_occurrence_id(current)
        })
        .count();

    let updated = previous_roots[..common_prefix]
        .iter()
        .zip(&current_roots[..common_prefix])
        .filter(|(previous, current)| previous != current)
        .map(|(_, current)| subscription_output_row(current.clone()))
        .collect();
    let removed = previous_roots[common_prefix..]
        .iter()
        .map(|row| RemovedRow {
            table: row.table().to_owned(),
            row_uuid: row.row_uuid(),
            occurrence_id: subscription_row_occurrence_id(row),
        })
        .collect();
    let added = current_roots[common_prefix..]
        .iter()
        .cloned()
        .map(subscription_output_row)
        .collect();

    SubscriptionEvent::Delta {
        reset: false,
        added,
        updated,
        removed,
        terminal_operations: Vec::new(),
        settled,
        tier,
    }
}

fn subscription_delta_event_with_reset(
    tier: DurabilityTier,
    settled: bool,
    previous: &RelationSnapshot,
    current: &RelationSnapshot,
    reset: bool,
    _terminal_rows: bool,
) -> SubscriptionEvent {
    let mut previous_by_id = BTreeMap::new();
    for row in &previous.rows {
        previous_by_id.insert(subscription_row_key(row), row);
    }

    let mut current_by_id = BTreeMap::new();
    for row in &current.rows {
        current_by_id.insert(subscription_row_key(row), row);
    }

    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut removed = Vec::new();
    for (key, row) in &current_by_id {
        match previous_by_id.get(key) {
            None => added.push(subscription_output_row((*row).clone())),
            Some(previous_row) if *previous_row != *row => {
                updated.push(subscription_output_row((*row).clone()))
            }
            Some(_) => {}
        }
    }

    for (key, _) in &previous_by_id {
        if !current_by_id.contains_key(key) {
            let row = previous_by_id[key];
            removed.push(RemovedRow {
                table: row.table().to_owned(),
                row_uuid: row.row_uuid(),
                occurrence_id: key.clone(),
            });
        }
    }

    SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        terminal_operations: Vec::new(),
        settled,
        tier,
    }
}

fn apply_maintained_update_to_snapshot(
    snapshot: &mut RelationSnapshot,
    snapshot_index: &mut RelationSnapshotIndex,
    update: LocalMaintainedViewSubscriptionUpdate,
    tier: DurabilityTier,
    settled: bool,
    _terminal_rows: bool,
) -> SubscriptionEvent {
    let LocalMaintainedViewSubscriptionUpdate {
        added: update_added,
        removed: update_removed,
        added_edges: update_added_edges,
        removed_edges: update_removed_edges,
        terminal_operations,
    } = update;

    if snapshot.rows.is_empty()
        && snapshot.edges.is_empty()
        && snapshot.root_count == 0
        && update_removed.is_empty()
        && update_removed_edges.is_empty()
    {
        if update_added_edges.is_empty() {
            snapshot.root_count = update_added.len();
            snapshot.rows.reserve(update_added.len());
            snapshot.rows.extend(update_added.iter().cloned());
            *snapshot_index = RelationSnapshotIndex::from_snapshot(snapshot);
            return SubscriptionEvent::Delta {
                reset: false,
                added: update_added
                    .into_iter()
                    .map(subscription_output_row)
                    .collect(),
                updated: Vec::new(),
                removed: Vec::new(),
                terminal_operations: terminal_operations.clone(),
                settled,
                tier,
            };
        }

        let mut event_added = Vec::with_capacity(update_added.len());
        let mut added_related = Vec::new();
        let mut seen_rows = BTreeSet::new();
        for row in &update_added {
            seen_rows.insert((row.table().to_owned(), row.row_uuid()));
            event_added.push(row.clone());
        }

        let mut seen_edges = BTreeSet::new();
        for (edge, row) in &update_added_edges {
            if seen_edges.insert(edge.clone()) {
                snapshot.edges.push(edge.clone());
            }
            let Some(row) = row else {
                continue;
            };
            if seen_rows.insert((row.table().to_owned(), row.row_uuid())) {
                added_related.push(row.clone());
            }
        }

        snapshot.root_count = event_added.len();
        snapshot
            .rows
            .reserve(event_added.len() + added_related.len());
        snapshot.rows.extend(event_added.iter().cloned());
        snapshot.rows.extend(added_related.iter().cloned());
        *snapshot_index = RelationSnapshotIndex::from_snapshot(snapshot);

        return SubscriptionEvent::Delta {
            reset: false,
            added: event_added
                .iter()
                .cloned()
                .map(subscription_output_row)
                .collect(),
            updated: Vec::new(),
            removed: Vec::new(),
            terminal_operations: terminal_operations.clone(),
            settled,
            tier,
        };
    }

    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut removed = Vec::new();
    let mut added_related = Vec::new();

    for row in &update_added {
        let key = subscription_row_occurrence_id(row);
        if let Some(position) = snapshot_index.roots.get(&key).copied() {
            if snapshot.rows[position] != *row {
                snapshot.rows[position] = row.clone();
                updated.push(subscription_output_row(row.clone()));
            }
        } else {
            snapshot.rows.insert(snapshot.root_count, row.clone());
            for position in snapshot_index.related.values_mut() {
                *position += 1;
            }
            snapshot_index
                .roots
                .insert(subscription_row_occurrence_id(row), snapshot.root_count);
            snapshot.root_count += 1;
            added.push(subscription_output_row(row.clone()));
        }
    }

    let mut index = 0;
    while index < snapshot.root_count {
        let occurrence_id = subscription_row_occurrence_id(&snapshot.rows[index]);
        if update_removed.contains(&occurrence_id)
            && !update_added
                .iter()
                .any(|added| subscription_row_occurrence_id(added) == occurrence_id)
        {
            let row = snapshot.rows.remove(index);
            snapshot.root_count -= 1;
            snapshot_index
                .roots
                .remove(&subscription_row_occurrence_id(&row));
            removed.push(RemovedRow {
                table: row.table().to_owned(),
                row_uuid: row.row_uuid(),
                occurrence_id,
            });
        } else {
            index += 1;
        }
    }
    if !update_removed.is_empty() {
        *snapshot_index = RelationSnapshotIndex::from_snapshot(snapshot);
    }

    if !update_removed_edges.is_empty() {
        snapshot.edges.retain(|edge| {
            let remove = update_removed_edges.iter().any(|removed| removed == edge);
            if remove {
                snapshot_index.edges.remove(edge);
            }
            !remove
        });
    }

    for (edge, row) in &update_added_edges {
        if snapshot_index.edges.insert(edge.clone()) {
            snapshot.edges.push(edge.clone());
        }
        let Some(row) = row else {
            continue;
        };
        let root_key = subscription_row_occurrence_id(row);
        if snapshot_index.roots.contains_key(&root_key) {
            continue;
        }
        let key = (row.table().to_owned(), row.row_uuid());
        if let Some(position) = snapshot_index.related.get(&key).copied() {
            snapshot.rows[position] = row.clone();
        } else {
            snapshot_index.related.insert(key, snapshot.rows.len());
            snapshot.rows.push(row.clone());
        }
        added_related.push(row.clone());
    }

    for removed_edge in &update_removed_edges {
        let still_referenced = snapshot_index.edges.iter().any(|edge| {
            edge.target_table == removed_edge.target_table
                && edge.target_row == removed_edge.target_row
        });
        let target_key = (removed_edge.target_table.clone(), removed_edge.target_row);
        let is_root = snapshot_index
            .roots
            .contains_key(&OutputOccurrenceId::single_source(ObjectId::from_uuid(
                target_key.1.0,
            )));
        if !still_referenced && !is_root {
            if let Some(position) = snapshot_index.related.remove(&target_key) {
                snapshot.rows.remove(position);
                for indexed_position in snapshot_index.related.values_mut() {
                    if *indexed_position > position {
                        *indexed_position -= 1;
                    }
                }
            }
        }
    }

    SubscriptionEvent::Delta {
        reset: false,
        added,
        updated,
        removed,
        terminal_operations,
        settled,
        tier,
    }
}

fn relation_snapshot_with_delta_slack(snapshot: &RelationSnapshot) -> RelationSnapshot {
    let mut snapshot = snapshot.clone();
    reserve_relation_snapshot_delta_slack(&mut snapshot);
    snapshot
}

fn reserve_relation_snapshot_delta_slack(snapshot: &mut RelationSnapshot) {
    fn slack(len: usize) -> usize {
        (len / 8).max(64)
    }

    snapshot.rows.reserve(slack(snapshot.rows.len()));
    snapshot.edges.reserve(slack(snapshot.edges.len()));
}

fn subscription_is_settled<S>(
    node: &NodeState<S>,
    shape: &ValidatedQuery,
    binding: &Binding,
    tier: DurabilityTier,
    read_view: ReadViewSpec,
) -> bool
where
    S: OrderedKvStorage,
{
    if tier <= DurabilityTier::Local {
        return true;
    }
    node.has_settled_result_set(BindingViewKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions { tier, read_view }.read_view_key(),
    })
}

pub(crate) fn subscription_row_occurrence_id(row: &CurrentRow) -> OutputOccurrenceId {
    let root = ObjectId::from_uuid(row.row_uuid().0);
    let mut joined = Vec::new();
    for position in 1.. {
        let Some(Value::Uuid(row_id)) = row.raw_field(&format!("__flat_join_row_{position}"))
        else {
            break;
        };
        joined.push(ObjectId::from_uuid(row_id));
    }
    OutputOccurrenceId::new(root, joined)
}

fn subscription_output_row(row: CurrentRow) -> SubscriptionOutputRow {
    SubscriptionOutputRow {
        occurrence_id: subscription_row_occurrence_id(&row),
        row,
    }
}

fn subscription_row_key(row: &CurrentRow) -> OutputOccurrenceId {
    subscription_row_occurrence_id(row)
}

#[cfg(test)]
mod tests;
