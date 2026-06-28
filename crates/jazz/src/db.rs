//! High-level thread-affine database facade described by `jazz/API.md`. This
//! module owns application-facing handles, read/write options, and facade-level
//! sync plumbing; durable version storage, validation, policy checks, and view
//! construction live in [`crate::node`], while link-local shipped state lives in
//! [`crate::peer`]. In the layer map this is the top `Db` facade over the node.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::{Pin, pin};
use std::rc::{Rc, Weak};
use std::task::{Context, Poll, Waker};

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_channel::oneshot;
use futures_core::Stream;
use groove::records::Value;
use groove::storage::{OrderedKvStorage, ReopenableStorage};
use thiserror::Error;

use crate::ids::{AuthorId, NodeUuid, RowUuid};
pub use crate::node::CommitUnitTrust;
use crate::node::{
    CommitUnitIngestContext, CurrentRow, LargeValueEditCommit, LargeValueEditOp,
    LocalMaintainedViewSubscription, MergeableCommit, NodeState, OpenTxId, PreparedQueryPlan,
};
use crate::peer::PeerState;
use crate::protocol::{
    ContentExtent, CoverageKey, CurrentWriteSchema, MigrationLens, RegisterShapeOptions,
    SchemaVersion, ShapeAst, Subscribe, SubscriptionKey, SyncMessage,
};
use crate::query::{Binding, Query, QueryError, ShapeId, ValidatedQuery};
use crate::schema::{JazzSchema, TableSchema};
use crate::time::GlobalSeq;
use crate::tx::{DeletionEvent, DurabilityTier, Fate, RejectionReason, TxId};
use crate::wire::{
    FEATURE_STRUCTURED_ERRORS, FEATURE_SYNC_MESSAGE_PAYLOAD, TransportError, WIRE_PROTOCOL_VERSION,
    WireEnvelope, WireError, WireErrorCode, WireFeatures, WireFrame, WireRetry, WireSession,
    WireTransport, decode_frame, decode_sync_message, encode_frame, encode_sync_message,
};

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
type SharedTickScheduler = Rc<RefCell<Option<Rc<dyn TickScheduler>>>>;
type WriteStateWaiters = Rc<RefCell<BTreeMap<TxId, Vec<WriteStateWaiter>>>>;

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
}

#[derive(Clone)]
struct PendingUpstreamSubscription {
    subscription: SubscriptionKey,
    shape: ValidatedQuery,
    binding: Binding,
    opts: RegisterShapeOptions,
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

/// Usage-site query coverage attachment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryAttachment {
    subscription: SubscriptionKey,
}

impl QueryAttachment {
    /// Wire subscription id owned by this attachment.
    pub fn subscription(&self) -> SubscriptionKey {
        self.subscription
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
    #[cfg(test)]
    pub(crate) fn commit_unit_for_test(&self, tx_id: TxId) -> Result<SyncMessage, Error> {
        Ok(self.node.node.borrow_mut().commit_unit_for(tx_id)?)
    }

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
        let node = NodeState::new_with_large_value_checkpoint_op_interval(
            config.identity.node,
            config.schema.clone(),
            config.storage,
            false,
            config.large_value_checkpoint_op_interval,
        )?;
        Ok(Self {
            schema: config.schema,
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

    /// Open a database as a history-complete serving core.
    ///
    /// This mode is intended for server shells and tests that own authoritative
    /// in-memory history rather than a partial client replica.
    pub async fn open_history_complete(config: DbConfig<S>) -> Result<Self, Error> {
        let node = NodeState::new_history_complete(
            config.identity.node,
            config.schema.clone(),
            config.storage,
        )?;
        Ok(Self {
            schema: config.schema,
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
        Ok(tx_id)
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
        let schema = self.current_write_schema_for_query()?;
        let shape = query.validate(&schema)?;
        let binding = shape.bind(params)?;
        let (local_plan, global_plan) = if should_install_prepared_plan(&shape)
            && !self
                .node
                .node
                .borrow()
                .uses_partitioned_or_schema_projected_read(&shape)
        {
            let mut node = self.node.node.borrow_mut();
            (
                Some(node.prepared_query_plan(&shape, DurabilityTier::Local)?),
                Some(node.prepared_query_plan(&shape, DurabilityTier::Global)?),
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
    /// If an upstream/server subscription has covered this exact shape and
    /// binding, prefer that authoritative settled result set; otherwise read
    /// the local preview.
    pub fn read(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        self.node
            .node
            .borrow_mut()
            .query_rows_prefer_settled_result_set(
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
        let tier = effective_read_tier(opts);
        let mut node = self.node.node.borrow_mut();
        if opts.include_deleted {
            node.query_rows_for_link_including_deleted(
                &prepared.shape,
                &prepared.binding,
                tier,
                author,
            )
        } else {
            node.query_rows_for_link(&prepared.shape, &prepared.binding, tier, author)
        }
        .map_err(Into::into)
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
    ///     },
    /// ))?;
    /// let opened = block_on(subscription.next_event()).unwrap();
    /// assert!(opened.current_rows().unwrap().is_empty());
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

    /// Attach a one-shot usage-site query coverage request.
    ///
    /// Bindings call this before an edge/global one-shot read, drive
    /// [`Db::tick`] until [`Db::query_attachment_is_covered`] is true, read, then
    /// call [`Db::detach_query`].
    pub fn attach_query_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> QueryAttachment {
        let subscription = self.node.next_subscription_key(&prepared.shape);
        let opts = RegisterShapeOptions {
            tier: effective_read_tier(opts),
        };
        self.node
            .upstream_subscriptions
            .borrow_mut()
            .push(PendingUpstreamCommand::Subscribe(
                PendingUpstreamSubscription {
                    subscription,
                    shape: prepared.shape.clone(),
                    binding: prepared.binding.clone(),
                    opts: opts.clone(),
                },
            ));
        self.node.latest_coverage_subscriptions.borrow_mut().insert(
            coverage_key(&prepared.shape, &prepared.binding, opts),
            subscription,
        );
        self.node.schedule_tick(TickUrgency::Immediate);
        QueryAttachment { subscription }
    }

    /// Attach a one-shot usage-site query coverage request at the default tier.
    pub fn attach_query(&self, prepared: &PreparedQuery) -> QueryAttachment {
        self.attach_query_with_opts(prepared, ReadOpts::default())
    }

    /// Return whether a query attachment has received at least one upstream view update.
    pub fn query_attachment_is_covered(&self, attachment: QueryAttachment) -> bool {
        self.node
            .node
            .borrow()
            .has_settled_result_set(attachment.subscription)
    }

    /// Detach a one-shot query coverage request.
    pub fn detach_query(&self, attachment: QueryAttachment) {
        self.node
            .node
            .borrow_mut()
            .apply_unsubscribe(attachment.subscription);
        self.node
            .latest_coverage_subscriptions
            .borrow_mut()
            .retain(|_, subscription| *subscription != attachment.subscription);
        self.node
            .upstream_subscriptions
            .borrow_mut()
            .push(PendingUpstreamCommand::Unsubscribe(attachment.subscription));
        self.node.schedule_tick(TickUrgency::Immediate);
    }

    async fn open_subscription(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<SubscriptionStream, Error> {
        let read_tier = effective_read_tier(opts);
        let (maintained_subscription, rows) = if read_tier == DurabilityTier::Global {
            let (subscription, rows) = self
                .node
                .node
                .borrow_mut()
                .open_local_maintained_view_subscription(
                    &prepared.shape,
                    &prepared.binding,
                    author,
                )?;
            (Some(subscription), rows)
        } else {
            let rows = self.node.node.borrow_mut().query_rows_for_link(
                &prepared.shape,
                &prepared.binding,
                read_tier,
                author,
            )?;
            (None, rows)
        };
        let settled = subscription_is_settled(
            &self.node.node.borrow(),
            &prepared.shape,
            &prepared.binding,
            read_tier,
        );
        let (sender, receiver) = unbounded();
        let state = Rc::new(RefCell::new(SubscriptionState {
            shape: prepared.shape.clone(),
            binding: prepared.binding.clone(),
            author,
            read_tier,
            rows: rows.clone(),
            settled,
            maintained_subscription,
            sender,
        }));
        state
            .borrow()
            .sender
            .unbounded_send(SubscriptionEvent::Opened {
                current: rows,
                settled,
                tier: read_tier,
            })
            .map_err(|_| Error::new(ErrorCode::Protocol, "subscription receiver closed"))?;
        self.node
            .subscriptions
            .borrow_mut()
            .push(Rc::downgrade(&state));
        if opts.propagation == Propagation::Full {
            // If this Db is attached to upstreams, ask them to carry this shape so
            // the subscription fills from synced rows, not just local writes. The consumer
            // sees only the handle; the request flows out on the next `tick`.
            let upstream_subscription = self.node.next_subscription_key(&prepared.shape);
            let upstream_opts = RegisterShapeOptions { tier: read_tier };
            self.node
                .upstream_subscriptions
                .borrow_mut()
                .push(PendingUpstreamCommand::Subscribe(
                    PendingUpstreamSubscription {
                        subscription: upstream_subscription,
                        shape: prepared.shape.clone(),
                        binding: prepared.binding.clone(),
                        opts: upstream_opts.clone(),
                    },
                ));
            self.node.latest_coverage_subscriptions.borrow_mut().insert(
                coverage_key(&prepared.shape, &prepared.binding, upstream_opts),
                upstream_subscription,
            );
            self.node.schedule_tick(TickUrgency::Immediate);
        }
        Ok(SubscriptionStream {
            receiver,
            _state: state,
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
        self.write_mergeable_as_session_subject(made_by, table, row, cells, Vec::new(), None)
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

    /// Return whether an insert with these cells would pass write policy.
    ///
    /// This is a dry-run over the current local preview: it builds the
    /// hypothetical version used by the write path, evaluates policy as this
    /// Db's authenticated author, and does not store a version or advance time.
    pub fn can_insert(&self, table: &str, cells: RowCells) -> Result<bool, Error> {
        self.table_schema(table)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_insert_allows(
                MergeableCommit::new(table, RowUuid::from_bytes([0; 16]), 0)
                    .made_by(self.identity.author)
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
        let (cells, parent) = self.merge_existing_cells(table, row, patch)?;
        self.write_mergeable(
            self.identity.author,
            None,
            table,
            row,
            cells,
            parent.into_iter().collect(),
            None,
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
        let (cells, parent) = self.merge_existing_cells(table, row, patch)?;
        self.write_mergeable_as_session_subject(
            made_by,
            table,
            row,
            cells,
            parent.into_iter().collect(),
            None,
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
        if !self.can_update_for_identity(table, row, identity)? {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("policy denied UPDATE on table {table}"),
            ));
        }
        let (cells, parent) =
            self.merge_existing_cells_for_identity(table, row, patch, identity)?;
        self.write_mergeable(
            identity,
            Some(identity),
            table,
            row,
            cells,
            parent.into_iter().collect(),
            None,
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
        let (cells, parents) = if self.local_row(table, row)?.is_some() {
            let (cells, parent) = self.merge_existing_cells(table, row, cells)?;
            (cells, parent.into_iter().collect())
        } else {
            (cells, Vec::new())
        };
        self.write_mergeable(self.identity.author, None, table, row, cells, parents, None)
    }

    /// Upsert a row while evaluating write policy as `identity`.
    pub fn upsert_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let (cells, parents) = if self.local_row_for_identity(table, row, identity)?.is_some() {
            let (cells, parent) =
                self.merge_existing_cells_for_identity(table, row, cells, identity)?;
            (cells, parent.into_iter().collect())
        } else {
            (cells, Vec::new())
        };
        self.write_mergeable(identity, Some(identity), table, row, cells, parents, None)
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
        let (_, parent) = self.merge_existing_cells(table, row, BTreeMap::new())?;
        self.write_mergeable(
            self.identity.author,
            None,
            table,
            row,
            BTreeMap::new(),
            parent.into_iter().collect(),
            Some(DeletionEvent::Deleted),
        )
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
        let (_, parent) = self.merge_existing_cells(table, row, BTreeMap::new())?;
        self.write_mergeable_as_session_subject(
            made_by,
            table,
            row,
            BTreeMap::new(),
            parent.into_iter().collect(),
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
        if !self.can_delete_for_identity(table, row, identity)? {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("policy denied DELETE on table {table}"),
            ));
        }
        let (_, parent) =
            self.merge_existing_cells_for_identity(table, row, BTreeMap::new(), identity)?;
        self.write_mergeable(
            identity,
            Some(identity),
            table,
            row,
            BTreeMap::new(),
            parent.into_iter().collect(),
            Some(DeletionEvent::Deleted),
        )
    }

    /// Return whether this Db's author can read the current local row.
    pub fn can_read(&self, table: &str, row: RowUuid) -> Result<bool, Error> {
        self.table_schema(table)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_read_current_allows(table, row, self.identity.author)
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
        self.node
            .node
            .borrow_mut()
            .set_session_claims(identity, claims);
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
    pub fn mergeable_tx(&self) -> MergeableTx<'_, S> {
        MergeableTx {
            db: self,
            author: self.identity.author,
            permission_subject: None,
            writes: Vec::new(),
        }
    }

    /// Build a mergeable transaction authored and permission-checked as `author`.
    pub fn mergeable_tx_for_identity(&self, author: AuthorId) -> MergeableTx<'_, S> {
        MergeableTx {
            db: self,
            author,
            permission_subject: Some(author),
            writes: Vec::new(),
        }
    }

    /// Publish an immutable schema-version payload through the catalogue lane.
    pub fn publish_schema(&self, schema: SchemaVersion) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
        self.node
            .node
            .borrow_mut()
            .apply_sync_message(SyncMessage::PublishSchema {
                author: self.identity.author,
                schema: Box::new(schema),
            })
            .map_err(Into::into)
    }

    /// Publish an immutable migration lens through the catalogue lane.
    pub fn publish_lens(&self, lens: MigrationLens) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
        self.node
            .node
            .borrow_mut()
            .apply_sync_message(SyncMessage::PublishLens {
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
            .apply_sync_message(SyncMessage::SetCurrentWriteSchema {
                author: self.identity.author,
                pointer,
            })
            .map_err(Into::into)
    }

    /// Open an exclusive transaction over the current local snapshot.
    pub fn exclusive_tx(&self) -> Result<ExclusiveTx<'_, S>, Error> {
        let tx_id = self.open_exclusive_handle()?;
        Ok(ExclusiveTx {
            db: self,
            tx_id,
            has_reads: Cell::new(false),
        })
    }

    pub(crate) fn open_exclusive_handle(&self) -> Result<OpenTxId, Error> {
        self.node
            .node
            .borrow_mut()
            .open_exclusive()
            .map_err(Into::into)
    }

    /// Restore a row locally. Data is required by the public API contract.
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
        if cells.is_empty() {
            return Err(Error::new(ErrorCode::Schema, "restore requires row data"));
        }
        self.table_schema(table)?;
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
        if cells.is_empty() {
            return Err(Error::new(ErrorCode::Schema, "restore requires row data"));
        }
        self.table_schema(table)?;
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
        let mut commit = MergeableCommit::new(table, row, self.next_now_ms())
            .made_by(made_by)
            .parents(parents)
            .cells(cells);
        if let Some(subject) = permission_subject {
            commit = commit.permission_subject(subject);
        }
        if let Some(deletion) = deletion {
            commit = commit.deletion(deletion);
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

    fn current_write_schema_for_query(&self) -> Result<JazzSchema, Error> {
        let node = self.node.node.borrow();
        let current = node.current_write_schema();
        if current.schema == self.schema.version_id() {
            return Ok(self.schema.clone());
        }
        node.catalogue_schemas()
            .get(&current.schema)
            .map(|schema| schema.schema.clone())
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

    fn local_row(&self, table: &str, row: RowUuid) -> Result<Option<CurrentRow>, Error> {
        self.local_row_for_identity(table, row, self.identity.author)
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
            .query_rows_for_link(
                &query.shape,
                &query.binding,
                DurabilityTier::Local,
                identity,
            )?
            .into_iter()
            .find(|candidate| candidate.row_uuid() == row))
    }

    fn merge_existing_cells(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<(RowCells, Option<TxId>), Error> {
        self.merge_existing_cells_for_identity(table, row, patch, self.identity.author)
    }

    fn merge_existing_cells_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        identity: AuthorId,
    ) -> Result<(RowCells, Option<TxId>), Error> {
        let table_schema = self.table_schema(table)?;
        let mut cells = BTreeMap::new();
        let mut parent = None;
        if let Some(existing) = self.local_row_for_identity(table, row, identity)? {
            for column in &table_schema.columns {
                if let Some(value) = existing.cell(table_schema, &column.name) {
                    cells.insert(column.name.clone(), value);
                }
            }
            parent = self.node.node.borrow_mut().current_row_tx_id(&existing);
        }
        cells.extend(patch);
        Ok((cells, parent))
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
        self.node.refresh_subscriptions()
    }
}

/// Counts produced while servicing non-blocking database connection work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DbTickStats {
    /// Number of live subscriptions that received a queued event.
    pub subscription_events: usize,
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
    connections: RefCell<Vec<Rc<RefCell<PeerConnection<S>>>>>,
    scheduler: SharedTickScheduler,
    write_state_waiters: WriteStateWaiters,
    next_write_state_waiter_id: Cell<u64>,
    next_subscription_nonce: Cell<u64>,
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
            connections: RefCell::new(Vec::new()),
            scheduler: Rc::new(RefCell::new(None)),
            write_state_waiters: Rc::new(RefCell::new(BTreeMap::new())),
            next_write_state_waiter_id: Cell::new(1),
            next_subscription_nonce: Cell::new(1),
        }
    }

    /// Borrow the served node.
    pub fn node(&self) -> Rc<RefCell<NodeState<S>>> {
        Rc::clone(&self.node)
    }

    fn queue_pending_upload(&self, tx_id: TxId, unit: Option<SyncMessage>) {
        self.outbox.borrow_mut().push(PendingUpload { tx_id, unit });
        self.schedule_tick(TickUrgency::Deferred);
    }

    fn next_subscription_key(&self, shape: &ValidatedQuery) -> SubscriptionKey {
        let nonce = self.next_subscription_nonce.get();
        self.next_subscription_nonce.set(nonce.saturating_add(1));
        SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: crate::query::BindingId(uuid::Uuid::new_v5(
                &crate::query::QUERY_NAMESPACE,
                &nonce.to_be_bytes(),
            )),
        }
    }

    fn set_scheduler(&self, scheduler: Option<Rc<dyn TickScheduler>>) {
        *self.scheduler.borrow_mut() = scheduler;
    }

    fn schedule_tick(&self, urgency: TickUrgency) {
        schedule_tick_in(&self.scheduler, urgency);
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
                let opts = RegisterShapeOptions {
                    tier: state.read_tier,
                };
                let coverage = coverage_key(&state.shape, &state.binding, opts.clone());
                if !pending_coverage.insert(coverage.clone()) {
                    continue;
                }
                let subscription = self.next_subscription_key(&state.shape);
                self.latest_coverage_subscriptions
                    .borrow_mut()
                    .insert(coverage, subscription);
                pending.push(PendingUpstreamCommand::Subscribe(
                    PendingUpstreamSubscription {
                        subscription,
                        shape: state.shape.clone(),
                        binding: state.binding.clone(),
                        opts,
                    },
                ));
            }
        }
        let connection = Rc::new(RefCell::new(PeerConnection {
            transport,
            node: Rc::clone(&self.node),
            subscriptions: Rc::clone(&self.subscriptions),
            scheduler: Rc::clone(&self.scheduler),
            write_state_waiters: Rc::clone(&self.write_state_waiters),
            next_now_ms: Cell::new(1),
            link: ConnectionLink::Upstream {
                pending,
                upstream_subscriptions: Rc::clone(&self.upstream_subscriptions),
                announced_shapes: BTreeSet::new(),
                outbox: Rc::clone(&self.outbox),
                uploaded: BTreeSet::new(),
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
        let peer = cursor
            .map(|cursor| cursor.peer)
            .unwrap_or_else(|| match trust {
                CommitUnitTrust::TrustedBackend => {
                    PeerState::edge_client_with_permission_identity(identity, AuthorId::SYSTEM)
                }
                CommitUnitTrust::Session => PeerState::for_author(identity),
            });
        let connection = Rc::new(RefCell::new(PeerConnection {
            transport,
            node: Rc::clone(&self.node),
            subscriptions: Rc::clone(&self.subscriptions),
            scheduler: Rc::clone(&self.scheduler),
            write_state_waiters: Rc::clone(&self.write_state_waiters),
            next_now_ms: Cell::new(1),
            link: ConnectionLink::Subscriber {
                peer,
                ingest_context: CommitUnitIngestContext { identity, trust },
                outbox: Rc::clone(&self.outbox),
                upstream_subscriptions: Rc::clone(&self.upstream_subscriptions),
                served: BTreeMap::new(),
                coverage_groups: BTreeMap::new(),
                registered_shape_opts: BTreeMap::new(),
                served_current_rows: BTreeMap::new(),
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
        for connection in self.connections.borrow().iter() {
            let next = connection.borrow_mut().tick()?;
            stats.subscription_events += next.subscription_events;
        }
        Ok(stats)
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
    for weak in subscriptions.borrow().iter() {
        let Some(state) = weak.upgrade() else {
            continue;
        };
        let (read_tier, previous, previous_settled, shape, binding, author) = {
            let state = state.borrow();
            (
                state.read_tier,
                state.rows.clone(),
                state.settled,
                state.shape.clone(),
                state.binding.clone(),
                state.author,
            )
        };
        let maybe_rows = {
            let mut state_ref = state.borrow_mut();
            if let Some(maintained) = state_ref.maintained_subscription.as_mut() {
                let update = {
                    node.borrow_mut()
                        .drain_local_maintained_view_subscription(maintained)?
                };
                if let Some(update) = update {
                    let mut rows_by_id = previous
                        .iter()
                        .cloned()
                        .map(|row| (subscription_row_key(&row), row))
                        .collect::<BTreeMap<_, _>>();
                    for (_, row_uuid, _) in update.removes {
                        rows_by_id
                            .retain(|(_, existing_row_uuid), _| *existing_row_uuid != row_uuid);
                    }
                    for row in update.adds {
                        rows_by_id.insert(subscription_row_key(&row), row);
                    }
                    let mut rows = rows_by_id.into_values().collect::<Vec<_>>();
                    node.borrow().apply_query_order(shape.query(), &mut rows)?;
                    rows
                } else {
                    previous.clone()
                }
            } else {
                node.borrow_mut()
                    .query_rows_for_link(&shape, &binding, read_tier, author)?
            }
        };
        let rows = maybe_rows;
        let settled = subscription_is_settled(&node.borrow(), &shape, &binding, read_tier);
        if rows != previous || settled != previous_settled {
            let mut state = state.borrow_mut();
            let event = subscription_delta_event(read_tier, settled, &previous, &rows);
            state.rows = rows;
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
pub struct WireTransportAdapter<T> {
    inner: T,
    protocol_version: u16,
    features: WireFeatures,
    session: Option<WireSession>,
}

impl<T> WireTransportAdapter<T>
where
    T: WireTransport,
{
    /// Wrap a byte transport with the current Jazz wire defaults.
    pub fn current(inner: T) -> Self {
        Self::new(
            inner,
            WIRE_PROTOCOL_VERSION,
            FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS,
            None,
        )
    }

    /// Wrap a byte transport with explicit negotiated frame metadata.
    pub fn new(
        inner: T,
        protocol_version: u16,
        features: WireFeatures,
        session: Option<WireSession>,
    ) -> Self {
        Self {
            inner,
            protocol_version,
            features,
            session,
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
}

impl<T> Transport for WireTransportAdapter<T>
where
    T: WireTransport,
{
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
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
        let mut envelope = WireEnvelope::new(self.protocol_version, self.features, payload);
        if let Some(session) = self.session.clone() {
            envelope = envelope.with_session(session);
        }
        match encode_frame(&WireFrame::Message(envelope)) {
            Ok(frame) => self.inner.send_frame(frame),
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
        while let Some(bytes) = self.inner.try_recv_frame() {
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
                WireFrame::Message(envelope) => {
                    if let Err(error) = self.validate_inbound_session(&envelope) {
                        self.send_wire_error(error);
                        continue;
                    }
                    match decode_sync_message(&envelope.payload) {
                        Ok(message) => return Some(message),
                        Err(err) => self.send_wire_error(WireError::new(
                            WireErrorCode::MalformedFrame,
                            WireRetry::Never,
                            format!(
                                "failed to decode sync message payload: {err}; frame_bytes={}; payload_bytes={}; frame_hex={}; payload_hex={}",
                                bytes.len(),
                                envelope.payload.len(),
                                hex_diagnostic(&bytes),
                                hex_diagnostic(&envelope.payload),
                            ),
                        )),
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
    scheduler: SharedTickScheduler,
    write_state_waiters: WriteStateWaiters,
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
        announced_shapes: BTreeSet<ShapeId>,
        /// Locally-authored transactions to upload (shared with the `Db`).
        outbox: Outbox,
        /// Transactions already shipped on this connection (dedup across ticks).
        uploaded: BTreeSet<TxId>,
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
        /// Options from each subscriber `RegisterShape`, applied to later bindings.
        registered_shape_opts: BTreeMap<ShapeId, RegisterShapeOptions>,
        /// Whole-table current-row views explicitly served through the facade.
        served_current_rows: BTreeMap<SubscriptionKey, String>,
    },
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
        self.transport.send(update).map_err(transport_error)?;
        if let Some(subscription) = subscription {
            served_current_rows.insert(subscription, table.to_owned());
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
        let replacement = PeerState::for_author(peer.link_identity());
        Some(ResumeCursor {
            peer: std::mem::replace(peer, replacement),
        })
    }

    /// Service this connection once: drain inbound, apply, wake subscriptions, and
    /// flush pending outbound. Non-blocking; the binding calls it in its loop.
    pub fn tick(&mut self) -> Result<DbTickStats, Error> {
        let mut stats = DbTickStats::default();
        let tick_now_ms = self.next_now_ms();
        match &mut self.link {
            ConnectionLink::Upstream {
                pending,
                upstream_subscriptions,
                announced_shapes,
                outbox,
                uploaded,
            } => {
                pending.extend(upstream_subscriptions.borrow_mut().drain(..));
                let pending_index = 0;
                while pending_index < pending.len() {
                    match &pending[pending_index] {
                        PendingUpstreamCommand::Subscribe(pending_subscription) => {
                            let shape = &pending_subscription.shape;
                            let binding = &pending_subscription.binding;
                            if announced_shapes.insert(shape.shape_id()) {
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
                                    announced_shapes.remove(&shape.shape_id());
                                    return Err(transport_error(error));
                                }
                            }
                            let values = binding_values_in_param_order(shape, binding);
                            let subscribe = Subscribe {
                                shape_id: shape.shape_id(),
                                subscription: pending_subscription.subscription,
                                values,
                            };
                            self.node
                                .borrow_mut()
                                .apply_sync_message(SyncMessage::Subscribe(subscribe.clone()))?;
                            if let Err(error) =
                                self.transport.send(SyncMessage::Subscribe(subscribe))
                            {
                                return Err(transport_error(error));
                            }
                        }
                        PendingUpstreamCommand::Unsubscribe(subscription) => {
                            self.node.borrow_mut().apply_unsubscribe(*subscription);
                            if let Err(error) = self.transport.send(SyncMessage::Unsubscribe {
                                subscription: *subscription,
                            }) {
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
                    send_with_local_content_extents(&self.node, self.transport.as_mut(), unit)?;
                    uploaded.insert(tx_id);
                }
                let mut applied = false;
                while let Some(message) = self.transport.try_recv() {
                    let write_state_tx_id = write_state_update_tx_id(&message);
                    self.node.borrow_mut().apply_sync_message(message)?;
                    if let Some(tx_id) = write_state_tx_id {
                        notify_write_state_waiters(&self.write_state_waiters, tx_id);
                    }
                    applied = true;
                }
                if applied {
                    stats.subscription_events +=
                        refresh_subscriptions_in(&self.node, &self.subscriptions)?;
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
                registered_shape_opts,
                served_current_rows,
            } => {
                let mut applied_inbound = false;
                let mut scheduled_immediate = false;
                while let Some(message) = self.transport.try_recv() {
                    applied_inbound = true;
                    match message {
                        SyncMessage::RegisterShape {
                            shape_id,
                            opts,
                            ast,
                        } => {
                            registered_shape_opts.insert(shape_id, opts);
                            self.node.borrow_mut().apply_sync_message(
                                SyncMessage::RegisterShape {
                                    shape_id,
                                    ast,
                                    opts: RegisterShapeOptions::default(),
                                },
                            )?;
                        }
                        SyncMessage::Subscribe(subscribe) => {
                            let shape_id = subscribe.shape_id;
                            let subscription = subscribe.subscription;
                            let values = subscribe.values.clone();
                            self.node
                                .borrow_mut()
                                .apply_sync_message(SyncMessage::Subscribe(subscribe))?;
                            let Some(shape) = self.node.borrow().registered_shape(shape_id) else {
                                continue;
                            };
                            let value_map = shape
                                .params()
                                .keys()
                                .cloned()
                                .zip(values)
                                .collect::<BTreeMap<_, _>>();
                            let binding = shape.bind(value_map)?;
                            let opts = registered_shape_opts
                                .get(&shape_id)
                                .cloned()
                                .unwrap_or_default();
                            let coverage = coverage_key(&shape, &binding, opts.clone());
                            let group_subscription = SubscriptionKey {
                                shape_id: coverage.shape_id,
                                binding_id: coverage.binding_id,
                            };
                            let group =
                                coverage_groups.entry(coverage.clone()).or_insert_with(|| {
                                    CoverageGroup {
                                        shape: shape.clone(),
                                        binding: binding.clone(),
                                        subscribers: BTreeSet::new(),
                                    }
                                });
                            let first_subscriber = group.subscribers.is_empty();
                            let update = if first_subscriber {
                                let mut node = self.node.borrow_mut();
                                let update = peer.rehydrate_query_for_subscription_with_opts(
                                    &mut node,
                                    group_subscription,
                                    &shape,
                                    &binding,
                                    opts.clone(),
                                )?;
                                retarget_view_update(update, subscription)
                            } else {
                                let update = self.node.borrow_mut().view_update_for_query_binding_with_peer_payload_inventory_and_plan_at_tier(
                                    &shape,
                                    &binding,
                                    subscription,
                                    BTreeSet::new(),
                                    BTreeSet::new(),
                                    BTreeSet::new(),
                                    peer.identity(),
                                    None,
                                    opts.tier,
                                )?;
                                reset_view_update(update)
                            };
                            self.last_resume_bytes = Some(serialized_sync_message_len(&update));
                            send_with_content_extents(
                                &self.node,
                                peer,
                                self.transport.as_mut(),
                                update,
                            )?;
                            group.subscribers.insert(subscription);
                            served.insert(subscription, coverage);
                            if first_subscriber {
                                upstream_subscriptions.borrow_mut().push(
                                    PendingUpstreamCommand::Subscribe(
                                        PendingUpstreamSubscription {
                                            subscription,
                                            shape: shape.clone(),
                                            binding,
                                            opts,
                                        },
                                    ),
                                );
                            }
                            schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                            scheduled_immediate = true;
                        }
                        SyncMessage::Unsubscribe { subscription } => {
                            self.node.borrow_mut().apply_unsubscribe(subscription);
                            if let Some(coverage) = served.remove(&subscription)
                                && let Some(group) = coverage_groups.get_mut(&coverage)
                            {
                                group.subscribers.remove(&subscription);
                                if group.subscribers.is_empty() {
                                    let group_subscription = SubscriptionKey {
                                        shape_id: coverage.shape_id,
                                        binding_id: coverage.binding_id,
                                    };
                                    peer.forget_subscription(group_subscription);
                                    coverage_groups.remove(&coverage);
                                    upstream_subscriptions
                                        .borrow_mut()
                                        .push(PendingUpstreamCommand::Unsubscribe(subscription));
                                }
                            }
                        }
                        other => {
                            let relay_upload = match &other {
                                SyncMessage::CommitUnit { tx, .. } => {
                                    Some((tx.tx_id, other.clone()))
                                }
                                _ => None,
                            };
                            let write_state_tx_id = write_state_update_tx_id(&other);
                            // RegisterShape (registers the shape ahead of its
                            // binding), plus the write-upload path: any
                            // responses (e.g. fate updates) flow back to the
                            // subscriber.
                            let responses = self
                                .node
                                .borrow_mut()
                                .apply_sync_message_with_ingest_context(
                                    other,
                                    Some(*ingest_context),
                                )?;
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
                for (coverage, group) in coverage_groups.iter() {
                    let group_subscription = SubscriptionKey {
                        shape_id: coverage.shape_id,
                        binding_id: coverage.binding_id,
                    };
                    let update = {
                        let mut node = self.node.borrow_mut();
                        peer.query_update_for_subscription(
                            &mut node,
                            group_subscription,
                            &group.shape,
                            &group.binding,
                        )?
                    };
                    if !view_update_is_empty(&update) {
                        for subscription in group.subscribers.iter().copied() {
                            let update = retarget_view_update(update.clone(), subscription);
                            send_with_content_extents(
                                &self.node,
                                peer,
                                self.transport.as_mut(),
                                update,
                            )?;
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
    fn next_now_ms(&self) -> u64 {
        let next = self.next_now_ms.get();
        self.next_now_ms.set(next + 1);
        next
    }
}

fn schedule_tick_in(scheduler: &SharedTickScheduler, urgency: TickUrgency) {
    if let Some(scheduler) = scheduler.borrow().as_ref() {
        scheduler.schedule_tick(urgency);
    }
}

fn serialized_sync_message_len(message: &SyncMessage) -> usize {
    encode_sync_message(message).map_or(0, |bytes| bytes.len())
}

fn transport_error(error: TransportError) -> Error {
    match error {
        TransportError::Backpressure => {
            Error::new(ErrorCode::Backpressure, "transport backpressure")
        }
        TransportError::Failed(message) => Error::new(ErrorCode::Protocol, message),
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
    let extents = node.borrow().content_refs_in_sync_message(&message)?;
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
        transport.send(response).map_err(transport_error)?;
    }
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
                    bytes: node.content_store().read(&extent)?,
                    extent,
                });
            }
            SyncMessage::ContentExtents { extents: out }
        };
        transport.send(response).map_err(transport_error)?;
    }
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

fn reset_view_update(mut message: SyncMessage) -> SyncMessage {
    if let SyncMessage::ViewUpdate {
        reset_result_set, ..
    } = &mut message
    {
        *reset_result_set = true;
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

/// A `ViewUpdate` that carries no version or result-set change — nothing to
/// ship to the subscriber this tick.
fn view_update_is_empty(message: &SyncMessage) -> bool {
    match message {
        SyncMessage::ViewUpdate {
            reset_result_set,
            version_bundles,
            peer_payload_inventory,
            result_row_adds,
            result_row_removes,
            ..
        } => {
            !reset_result_set
                && version_bundles.is_empty()
                && peer_payload_inventory.complete_tx_payloads.is_empty()
                && result_row_adds.is_empty()
                && result_row_removes.is_empty()
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ReadOpts {
    /// Durability tier that gates the first result.
    pub tier: DurabilityTier,
    /// Whether own local updates are visible immediately.
    pub local_updates: LocalUpdates,
    /// Whether evaluation may propagate upstream.
    pub propagation: Propagation,
    /// Include current rows whose deletion winner is `Deleted`.
    pub include_deleted: bool,
}

impl Default for ReadOpts {
    fn default() -> Self {
        Self {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::Full,
            include_deleted: false,
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

fn effective_read_tier(opts: ReadOpts) -> DurabilityTier {
    if opts.local_updates == LocalUpdates::Immediate {
        opts.tier.max(DurabilityTier::Local)
    } else {
        opts.tier
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

struct PendingMergeableWrite {
    table: String,
    row_uuid: RowUuid,
    cells: RowCells,
    deletion: Option<DeletionEvent>,
    parents: Vec<TxId>,
}

/// Builder for a group of mergeable writes committed as one transaction.
pub struct MergeableTx<'a, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db: &'a Db<S>,
    author: AuthorId,
    permission_subject: Option<AuthorId>,
    writes: Vec<PendingMergeableWrite>,
}

impl<S> MergeableTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Stage an insert with a generated row id.
    pub fn insert(&mut self, table: &str, cells: RowCells) -> Result<RowUuid, Error> {
        let row = self.db.row_id_source.borrow_mut().next_row_id();
        self.insert_with_id(table, row, cells)?;
        Ok(row)
    }

    /// Stage an insert with a caller-supplied row id.
    pub fn insert_with_id(
        &mut self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<(), Error> {
        self.db.table_schema(table)?;
        self.stage_value_write(PendingMergeableWrite {
            table: table.to_owned(),
            row_uuid: row,
            cells,
            deletion: None,
            parents: Vec::new(),
        });
        Ok(())
    }

    /// Stage an update; omitted fields keep the transaction-local value.
    pub fn update(&mut self, table: &str, row: RowUuid, patch: RowCells) -> Result<(), Error> {
        let mut cells = self.current_cells(table, row)?;
        cells.extend(patch);
        self.insert_with_id(table, row, cells)
    }

    /// Stage a soft delete.
    pub fn delete(&mut self, table: &str, row: RowUuid) -> Result<(), Error> {
        self.db.table_schema(table)?;
        self.stage_deletion_write(PendingMergeableWrite {
            table: table.to_owned(),
            row_uuid: row,
            cells: BTreeMap::new(),
            deletion: Some(DeletionEvent::Deleted),
            parents: Vec::new(),
        });
        Ok(())
    }

    /// Stage a restore with explicit row data.
    pub fn restore(&mut self, table: &str, row: RowUuid, cells: RowCells) -> Result<(), Error> {
        if cells.is_empty() {
            return Err(Error::new(ErrorCode::Schema, "restore requires row data"));
        }
        self.db.table_schema(table)?;
        let (content_parents, deletion_parents) = {
            let mut node = self.db.node.node.borrow_mut();
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
        self.stage_value_write(PendingMergeableWrite {
            table: table.to_owned(),
            row_uuid: row,
            cells,
            deletion: None,
            parents: content_parents,
        });
        self.stage_deletion_write(PendingMergeableWrite {
            table: table.to_owned(),
            row_uuid: row,
            cells: BTreeMap::new(),
            deletion: Some(DeletionEvent::Restored),
            parents: deletion_parents,
        });
        Ok(())
    }

    /// Commit all staged writes as one mergeable transaction.
    pub fn commit(self) -> Result<TxId, Error> {
        let writes = self
            .writes
            .into_iter()
            .map(|write| {
                let mut commit =
                    MergeableCommit::new(write.table, write.row_uuid, self.db.next_now_ms())
                        .made_by(self.author)
                        .parents(write.parents)
                        .cells(write.cells);
                if let Some(subject) = self.permission_subject {
                    commit = commit.permission_subject(subject);
                }
                if let Some(deletion) = write.deletion {
                    commit = commit.deletion(deletion);
                }
                commit
            })
            .collect();
        let tx_id = self
            .db
            .node
            .node
            .borrow_mut()
            .commit_mergeable_many(writes)?;
        self.db.finalize_local_commit(tx_id)?;
        self.db.refresh_subscriptions()?;
        Ok(tx_id)
    }

    fn current_cells(&self, table: &str, row: RowUuid) -> Result<RowCells, Error> {
        let table_schema = self.db.table_schema(table)?;
        for write in self.writes.iter().rev() {
            if write.table == table && write.row_uuid == row && write.deletion.is_none() {
                if self.writes.iter().rev().any(|deletion| {
                    deletion.table == table
                        && deletion.row_uuid == row
                        && deletion.deletion == Some(DeletionEvent::Deleted)
                }) {
                    return Ok(BTreeMap::new());
                }
                return Ok(write.cells.clone());
            }
        }
        let mut cells = BTreeMap::new();
        if let Some(existing) = self.db.local_row(table, row)? {
            for column in &table_schema.columns {
                if let Some(value) = existing.cell(table_schema, &column.name) {
                    cells.insert(column.name.clone(), value);
                }
            }
        }
        Ok(cells)
    }

    fn stage_value_write(&mut self, write: PendingMergeableWrite) {
        if let Some(existing) = self.writes.iter_mut().find(|existing| {
            existing.table == write.table
                && existing.row_uuid == write.row_uuid
                && existing.deletion.is_none()
        }) {
            *existing = write;
        } else {
            self.writes.push(write);
        }
    }

    fn stage_deletion_write(&mut self, write: PendingMergeableWrite) {
        self.writes.retain(|existing| {
            existing.table != write.table
                || existing.row_uuid != write.row_uuid
                || match write.deletion {
                    Some(DeletionEvent::Deleted) => false,
                    Some(DeletionEvent::Restored) => existing.deletion.is_none(),
                    None => true,
                }
        });
        self.writes.push(write);
    }
}

/// Builder for an exclusive transaction over a stable snapshot.
pub struct ExclusiveTx<'a, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db: &'a Db<S>,
    tx_id: OpenTxId,
    has_reads: Cell<bool>,
}

impl<S> ExclusiveTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Read one row inside the exclusive transaction.
    pub fn read(&self, table: &str, row: RowUuid) -> Result<Option<RowCells>, Error> {
        self.has_reads.set(true);
        self.db
            .node
            .node
            .borrow_mut()
            .tx_read(self.tx_id, table, row)
            .map_err(Into::into)
    }

    /// Read all current rows in a table inside the exclusive transaction.
    pub fn all(&self, table: &str) -> Result<Vec<CurrentRow>, Error> {
        self.has_reads.set(true);
        self.db
            .node
            .node
            .borrow_mut()
            .tx_current_rows(self.tx_id, table)
            .map_err(Into::into)
    }

    /// Stage an insert with a generated row id.
    pub fn insert(&self, table: &str, cells: RowCells) -> Result<RowUuid, Error> {
        let row = self.db.row_id_source.borrow_mut().next_row_id();
        self.insert_with_id(table, row, cells)?;
        Ok(row)
    }

    /// Stage an insert with a caller-supplied row id.
    pub fn insert_with_id(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<(), Error> {
        self.db
            .node
            .node
            .borrow_mut()
            .tx_write(self.tx_id, table, row, cells, None)
            .map_err(Into::into)
    }

    /// Stage an update; omitted fields keep the transaction-local value.
    pub fn update(&self, table: &str, row: RowUuid, patch: RowCells) -> Result<(), Error> {
        let mut cells = self.read(table, row)?.unwrap_or_default();
        cells.extend(patch);
        self.insert_with_id(table, row, cells)
    }

    /// Stage a soft delete.
    pub fn delete(&self, table: &str, row: RowUuid) -> Result<(), Error> {
        self.db
            .node
            .node
            .borrow_mut()
            .tx_write(
                self.tx_id,
                table,
                row,
                BTreeMap::<String, Value>::new(),
                Some(DeletionEvent::Deleted),
            )
            .map_err(Into::into)
    }

    /// Commit the exclusive transaction.
    pub fn commit(self) -> Result<TxId, Error> {
        let (tx_id, unit) = self.db.node.node.borrow_mut().commit_exclusive(
            self.tx_id,
            self.db.identity.author,
            self.db.next_now_ms(),
        )?;
        self.db.finalize_local_exclusive_unit(tx_id, unit)?;
        self.db.refresh_subscriptions()?;
        Ok(tx_id)
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

struct SubscriptionState {
    shape: ValidatedQuery,
    binding: Binding,
    author: AuthorId,
    read_tier: DurabilityTier,
    rows: Vec<CurrentRow>,
    settled: bool,
    maintained_subscription: Option<LocalMaintainedViewSubscription>,
    sender: UnboundedSender<SubscriptionEvent>,
}

/// Row identity removed from a materialized subscription result.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RemovedRow {
    /// Logical table that contained the removed row.
    pub table: String,
    /// Stable row identity.
    pub row_uuid: RowUuid,
}

/// Materialized event emitted by a database subscription stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionEvent {
    /// Initial materialized result for the subscription.
    Opened {
        /// Complete materialized rows visible at subscription open.
        current: Vec<CurrentRow>,
        /// Whether the result is complete at the requested read tier.
        settled: bool,
        /// Read tier used to materialize the rows.
        tier: DurabilityTier,
    },
    /// Incremental materialized result change.
    Delta {
        /// Complete materialized rows after applying this change.
        current: Vec<CurrentRow>,
        /// Rows newly visible to the subscription.
        added: Vec<CurrentRow>,
        /// Rows still visible with changed projected cells.
        updated: Vec<CurrentRow>,
        /// Rows no longer visible to the subscription.
        removed: Vec<RemovedRow>,
        /// Whether the result is complete at the requested read tier.
        settled: bool,
        /// Read tier used to materialize the rows.
        tier: DurabilityTier,
    },
    /// Full replacement result, reserved for future stream resumption and
    /// internal invalidation cases where a precise delta is unavailable.
    Reset {
        /// Complete replacement materialized rows.
        current: Vec<CurrentRow>,
        /// Whether the result is complete at the requested read tier.
        settled: bool,
        /// Read tier used to materialize the rows.
        tier: DurabilityTier,
    },
    /// The subscription stream was closed by the producer.
    Closed,
}

impl SubscriptionEvent {
    /// Return full materialized rows for snapshot-like events.
    pub fn current_rows(&self) -> Option<&[CurrentRow]> {
        match self {
            Self::Opened { current, .. } | Self::Reset { current, .. } => Some(current),
            Self::Delta { .. } | Self::Closed => None,
        }
    }
}

/// Stream of materialized subscription events.
pub struct SubscriptionStream {
    receiver: UnboundedReceiver<SubscriptionEvent>,
    _state: Rc<RefCell<SubscriptionState>>,
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

/// Validated and bound query plan used by all `Db` reads and subscriptions.
#[derive(Clone, Debug)]
pub struct PreparedQuery {
    shape: ValidatedQuery,
    binding: Binding,
    local_plan: Option<PreparedQueryPlan>,
    global_plan: Option<PreparedQueryPlan>,
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

    fn plan_for_tier(&self, tier: DurabilityTier) -> Option<&PreparedQueryPlan> {
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
    previous: &[CurrentRow],
    current: &[CurrentRow],
) -> SubscriptionEvent {
    let mut previous_by_id = BTreeMap::new();
    for row in previous {
        previous_by_id.insert(subscription_row_key(row), row);
    }

    let mut current_by_id = BTreeMap::new();
    for row in current {
        current_by_id.insert(subscription_row_key(row), row);
    }

    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut removed = Vec::new();

    for (key, row) in &current_by_id {
        match previous_by_id.get(key) {
            None => added.push((*row).clone()),
            Some(previous_row) if *previous_row != *row => updated.push((*row).clone()),
            Some(_) => {}
        }
    }

    for (key, _) in &previous_by_id {
        if !current_by_id.contains_key(key) {
            removed.push(RemovedRow {
                table: key.0.clone(),
                row_uuid: key.1,
            });
        }
    }

    SubscriptionEvent::Delta {
        current: current.to_vec(),
        added,
        updated,
        removed,
        settled,
        tier,
    }
}

fn subscription_is_settled<S>(
    node: &NodeState<S>,
    shape: &ValidatedQuery,
    binding: &Binding,
    tier: DurabilityTier,
) -> bool
where
    S: OrderedKvStorage,
{
    if tier <= DurabilityTier::Local {
        return true;
    }
    node.has_settled_result_set(SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
    })
}

fn subscription_row_key(row: &CurrentRow) -> (String, RowUuid) {
    (row.table().to_owned(), row.row_uuid())
}

#[cfg(test)]
mod tests;
