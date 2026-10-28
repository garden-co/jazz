//! Rust client facade over the SQLite engine ([`crate::db::Db`]).
//!
//! One client context owns one local `Db` on the thread that connected it,
//! so a [`JazzClient`] runs inside a `tokio::task::LocalSet`. Every clone
//! shares that context. A local driver task ticks the database whenever there
//! is work (a local write, a new subscription, or frames from the native
//! transport) and on a short fallback interval for read deadlines. After each
//! tick it forwards subscription events and wakes callers waiting on sync
//! progress (remote reads, durability waits).
//!
//! The upstream link comes from a [`NativeTransportConnector`] chosen by the
//! process shell. When an established link stops, the client keeps working
//! locally and reconnects with backoff; the engine resubscribes and resends
//! unsettled writes on the new link.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::pin::pin;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use base64::Engine;
use serde::Deserialize;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use uuid::Uuid;

use crate::db::{
    CurrentRow, Db, DbConfig, DbIdentity, DeleteOptions, Error as CoreError, FileOptions,
    FirstLoad, InsertOptions, ReadOpts, RemoteLinkHint, RemovedRow, RestoreOptions,
    SubscriptionEvent, SubscriptionOutputRow, SubscriptionStream as CoreSubscriptionStream,
    UpdateOptions, UpsertOptions, Upstream, WireTransportAdapter, WriteIdentity,
};
use crate::ids::{AuthorSubject, NodeUuid, RowUuid};
use crate::model::public_api::types::{
    OrderedAdded, OrderedRemoved, OrderedUpdated, QueryResultField,
};
use crate::model::public_schema::{
    ColumnDescriptor, ColumnType, OrderedRowDelta, QueryResult, Row, Schema, Session, TableName,
    TableSchema, Value, WriteContext, validate_json_value,
};
use crate::model::transaction::{OpenTransactionId, TransactionId};
use crate::object::{ObjectId, OutputOccurrenceId, ResultKey};
use crate::query::Query;
use crate::records::Value as CoreValue;
use crate::schema::history;
use crate::tools::native_transport_connector::{
    NativeTransportConnector, NativeTransportError, NativeTransportLink, NativeTransportRequest,
    NativeTransportTerminalFuture,
};
use crate::tools::sync::{DurabilityTier, ReadTier};
use crate::tools::websocket_prelude_auth::AuthConfig;
use crate::tools::{
    AppContext, ClientStorage, JazzError, Result, SubscriptionHandle, SubscriptionRejectReason,
    SubscriptionStream, SubscriptionStreamItem,
};
use crate::tx::{DurabilityTier as CoreDurabilityTier, TxId};

/// The driver ticks at least this often while it runs, so read deadlines
/// (`FirstLoad::WaitForRemote`) expire without other work.
const FALLBACK_TICK: Duration = Duration::from_millis(50);
const RECONNECT_BASE_DELAY: Duration = Duration::from_millis(50);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(2);
/// The local store under `AppContext::data_dir`.
const STORE_FILE: &str = "jazz.sqlite";
/// The node id a persistent store's writes are uploaded under. It must
/// survive restarts: the server dedupes resent commits by node.
const CLIENT_ID_FILE: &str = "client_id";
/// The RocksDB store written before the SQLite engine.
const PRE_SQLITE_STORE: &str = "jazz-core.rocksdb";

/// Jazz client for building applications.
///
/// Combines local storage with server sync.
pub struct JazzClient {
    /// Write metadata applied to mutations issued through this client.
    write_context: Option<WriteContext>,
    shared: Rc<Shared>,
}

impl Clone for JazzClient {
    fn clone(&self) -> Self {
        Self {
            write_context: self.write_context.clone(),
            shared: self.shared.clone(),
        }
    }
}

struct Shared {
    db: Db,
    /// The app, named in slow-query reports.
    app: String,
    identity: DbIdentity,
    schema: Schema,
    /// Asks the driver for a tick.
    work: Arc<Notify>,
    /// Bumped after every tick; waiters re-poll on each change.
    progress: watch::Sender<u64>,
    /// True while a test holds the upstream detached; the keeper neither
    /// keeps nor re-opens a link until it turns false.
    upstream_paused: watch::Sender<bool>,
    /// No server is configured, so nothing can confirm a write past Local.
    serverless: bool,
    /// The upstream link is a trusted backend or admin link, which may
    /// attribute writes to other authors.
    trusted: bool,
    state: RefCell<State>,
}

#[derive(Default)]
struct State {
    closed: bool,
    upstream: Option<Upstream>,
    /// Open transactions and whether they staged a write.
    transactions: HashMap<OpenTransactionId, bool>,
    closed_transactions: HashMap<OpenTransactionId, ClosedTransaction>,
    /// Engine seqs of this client's commits, by public transaction id.
    writes: HashMap<TransactionId, u64>,
    forwarders: Vec<Forwarder>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Whether the background tick loop runs. A client without an
    /// upstream starts it with its first subscription; until then its
    /// calls tick inline, so it needs no `LocalSet`.
    driven: bool,
    /// Failed upstream connect attempts so far, and the last one's error.
    connect_failures: (u64, Option<String>),
}

#[derive(Clone, Copy, Debug)]
enum ClosedTransaction {
    Committed,
    RolledBack,
}

/// Moves one engine subscription's events to its public stream.
struct Forwarder {
    stream: CoreSubscriptionStream,
    /// The caller's query, which results are decoded against.
    query: Query,
    sender: mpsc::UnboundedSender<SubscriptionStreamItem>,
    cancellation: oneshot::Receiver<()>,
    /// The rows the public stream holds, in order.
    rows: Vec<SubscriptionOutputRow>,
    /// It asks for the server's answer and this client has no server:
    /// every delivery stays pending.
    unsettleable: bool,
}

impl Shared {
    fn state(&self) -> std::cell::RefMut<'_, State> {
        self.state.borrow_mut()
    }

    fn ensure_open(&self) -> Result<()> {
        if self.state.borrow().closed {
            return Err(JazzError::Connection("client is shut down".to_owned()));
        }
        Ok(())
    }

    fn tick_soon(&self) {
        self.work.notify_one();
    }

    /// Re-poll `poll` after every tick until it is ready.
    async fn until<T>(&self, mut poll: impl FnMut() -> Option<T>) -> Result<T> {
        let mut progress = self.progress.subscribe();
        loop {
            self.ensure_open()?;
            if let Some(value) = poll() {
                return Ok(value);
            }
            if !self.state.borrow().driven {
                self.tick().await;
                tokio::task::yield_now().await;
                continue;
            }
            self.tick_soon();
            if progress.changed().await.is_err() {
                return Err(JazzError::ChannelClosed);
            }
        }
    }

    /// Start the background tick loop if it is not running.
    fn ensure_driven(self: &Rc<Self>) {
        if std::mem::replace(&mut self.state().driven, true) {
            return;
        }
        let driver = tokio::task::spawn_local(drive(Rc::downgrade(self), self.work.clone()));
        self.state().tasks.push(driver);
    }

    /// Await an engine future whose progress depends on ticks. Engine
    /// futures re-check their condition when polled and register no waker.
    async fn settle<T>(&self, future: impl std::future::Future<Output = T>) -> Result<T> {
        let mut future = pin!(future);
        let mut context = Context::from_waker(Waker::noop());
        self.until(|| match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => Some(value),
            Poll::Pending => None,
        })
        .await
    }

    async fn tick(&self) {
        if let Err(error) = self.db.tick().await {
            tracing::warn!(%error, "jazz client tick failed");
        }
        for line in self.db.take_slow_query_report(Some(&self.app)) {
            tracing::warn!(app = %self.app, "{line}");
        }
        self.forward();
        self.progress.send_modify(|generation| *generation += 1);
    }

    fn forward(&self) {
        let mut state = self.state();
        state.forwarders.retain_mut(|forwarder| {
            if !matches!(
                forwarder.cancellation.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ) || forwarder.sender.is_closed()
            {
                return false;
            }
            while let Some(event) = forwarder.stream.try_next_event() {
                let item = match event {
                    SubscriptionEvent::Delta {
                        reset,
                        added,
                        updated,
                        removed,
                        settled,
                        ..
                    } => match forwarder.delta(
                        &self.schema,
                        &self.db,
                        reset,
                        added,
                        updated,
                        removed,
                    ) {
                        // Not yet settled at the tier the subscription asked for.
                        Ok(delta) => SubscriptionStreamItem::Delta(OrderedRowDelta {
                            pending: !settled || forwarder.unsettleable,
                            ..delta
                        }),
                        Err(error) => SubscriptionStreamItem::Rejected {
                            reason: SubscriptionRejectReason::UnsupportedShapeCapability {
                                detail: error.to_string(),
                            },
                        },
                    },
                    SubscriptionEvent::Rejected { reason } => SubscriptionStreamItem::Rejected {
                        reason: SubscriptionRejectReason::UnsupportedShapeCapability {
                            detail: reason,
                        },
                    },
                    SubscriptionEvent::Closed => return false,
                };
                if forwarder.sender.send(item).is_err() {
                    return false;
                }
            }
            true
        });
    }
}

impl Forwarder {
    fn delta(
        &mut self,
        schema: &Schema,
        db: &Db,
        reset: bool,
        added_rows: Vec<SubscriptionOutputRow>,
        updated_rows: Vec<SubscriptionOutputRow>,
        removed_rows: Vec<RemovedRow>,
    ) -> Result<OrderedRowDelta> {
        let decoder = Decoder {
            schema,
            query: &self.query,
            db: Some(db),
        };
        let mut delta = OrderedRowDelta::default();
        if reset {
            // A reset replaces the whole result: everything held leaves,
            // highest index first so each index is valid when applied.
            let previous = std::mem::take(&mut self.rows);
            for (index, row) in previous.iter().enumerate().rev() {
                delta.removed.push(OrderedRemoved {
                    id: decoder.key(row)?,
                    index,
                });
            }
        }
        for row in &removed_rows {
            let held = self.rows.iter().position(|held| {
                held.row_uuid() == row.row_uuid
                    && held.table() == row.table
                    && held.joined_rows() == row.joined.as_slice()
                    && held.union_arm() == row.union_arm.as_deref()
            });
            let id = match held {
                Some(at) => decoder.key(&self.rows.remove(at))?,
                None => jazz_db::values::occurrence_key(
                    row.row_uuid,
                    row.version,
                    &row.joined,
                    row.union_arm.as_deref(),
                ),
            };
            delta.removed.push(OrderedRemoved {
                id,
                index: row.index,
            });
        }
        for row in &updated_rows {
            let held = self.rows.iter().position(|held| {
                held.row_uuid() == row.row_uuid()
                    && held.table() == row.table()
                    && held.joined_rows() == row.joined_rows()
                    && held.union_arm() == row.union_arm()
            });
            // A row that only moved, or got a new version with the same
            // shown cells, carries no payload: they are what it was
            // delivered with.
            let changed = held.is_none_or(|at| {
                let before = &self.rows[at].row;
                before.is_deleted() != row.row.is_deleted() || !before.cells().eq(row.row.cells())
            });
            if let Some(at) = held {
                self.rows.remove(at);
            }
            let public = decoder.subscription_row(row)?;
            delta.updated.push(OrderedUpdated {
                id: public.id.clone(),
                old_index: row.previous_index.unwrap_or(row.index),
                new_index: row.index,
                row: changed.then_some(public),
            });
        }
        for row in &added_rows {
            let public = decoder.subscription_row(row)?;
            delta.added.push(OrderedAdded {
                id: public.id.clone(),
                index: row.index,
                row: public,
            });
        }
        let mut placements: Vec<SubscriptionOutputRow> =
            added_rows.into_iter().chain(updated_rows).collect();
        placements.sort_by_key(|row| row.index);
        for row in placements {
            let at = row.index.min(self.rows.len());
            self.rows.insert(at, row);
        }
        Ok(delta)
    }
}

/// Drives one client context until it shuts down or every facade is gone.
async fn drive(shared: Weak<Shared>, work: Arc<Notify>) {
    loop {
        {
            let Some(shared) = shared.upgrade() else {
                return;
            };
            if shared.state.borrow().closed {
                return;
            }
            shared.tick().await;
        }
        tokio::select! {
            _ = work.notified() => {}
            _ = tokio::time::sleep(FALLBACK_TICK) => {}
        }
    }
}

/// Keeps one upstream link: waits for the attached link to stop, detaches
/// it, and reconnects with backoff.
async fn keep_upstream(
    shared: Weak<Shared>,
    connector: Arc<dyn NativeTransportConnector>,
    request: NativeTransportRequest,
    mut attached: Option<NativeTransportTerminalFuture>,
) {
    let mut delay = RECONNECT_BASE_DELAY;
    let mut paused = match shared.upgrade() {
        Some(shared) => shared.upstream_paused.subscribe(),
        None => return,
    };
    loop {
        let terminal = match attached.take() {
            Some(terminal) => terminal,
            None => {
                if paused.wait_for(|paused| !*paused).await.is_err() {
                    return;
                }
                {
                    let Some(shared) = shared.upgrade() else {
                        return;
                    };
                    if shared.state.borrow().closed {
                        return;
                    }
                    shared.db.set_remote_link_hint(RemoteLinkHint::Attempting);
                }
                match connector.connect(request.clone()).await {
                    Ok(link) => {
                        let Some(shared) = shared.upgrade() else {
                            return;
                        };
                        attach(&shared, link.transport).await;
                        delay = RECONNECT_BASE_DELAY;
                        link.terminal
                    }
                    Err(error) => {
                        tracing::debug!(%error, "jazz client upstream connect failed");
                        if let Some(shared) = shared.upgrade() {
                            shared.db.set_remote_link_hint(RemoteLinkHint::Failed);
                            let mut state = shared.state();
                            let failures = state.connect_failures.0 + 1;
                            state.connect_failures = (failures, Some(error.to_string()));
                            drop(state);
                            shared.tick_soon();
                        }
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(RECONNECT_MAX_DELAY);
                        continue;
                    }
                }
            }
        };
        tokio::select! {
            stopped = terminal => {
                tracing::debug!(?stopped, "jazz client upstream stopped");
            }
            // A test detached the link; dropping its terminal closes it.
            _ = paused.wait_for(|paused| *paused) => {}
        }
        let Some(shared) = shared.upgrade() else {
            return;
        };
        detach(&shared);
        shared.db.set_remote_link_hint(RemoteLinkHint::Failed);
        shared.tick_soon();
    }
}

async fn attach(shared: &Shared, transport: Box<dyn crate::wire::WireTransport + Send>) {
    let upstream = shared
        .db
        .connect_upstream(Box::new(WireTransportAdapter::current(transport)))
        .await;
    shared.db.set_remote_link_hint(RemoteLinkHint::Live);
    shared.state().upstream = Some(upstream);
    shared.tick_soon();
}

fn detach(shared: &Shared) -> bool {
    let upstream = shared.state().upstream.take();
    upstream.is_some_and(|upstream| shared.db.detach_connection(&upstream))
}

impl JazzClient {
    /// Open a local client with no target-specific network adapter.
    ///
    /// An empty `server_url` is an offline client. Native online applications
    /// use [`Self::connect_with_native_transport`] from their process or
    /// binding composition crate.
    pub async fn connect(context: AppContext) -> Result<Self> {
        Self::connect_inner(context, None).await
    }

    /// Connect using a transport selected at a native composition point.
    pub async fn connect_with_native_transport(
        context: AppContext,
        connector: Arc<dyn NativeTransportConnector>,
    ) -> Result<Self> {
        Self::connect_inner(context, Some(connector)).await
    }

    async fn connect_inner(
        context: AppContext,
        connector: Option<Arc<dyn NativeTransportConnector>>,
    ) -> Result<Self> {
        let has_server = !context.server_url.is_empty();
        let connector = match (has_server, connector) {
            (false, _) => None,
            (true, Some(connector)) => Some(connector),
            (true, None) => {
                return Err(JazzError::Connection(
                    "a server_url needs a native transport: use \
                     JazzClient::connect_with_native_transport"
                        .to_owned(),
                ));
            }
        };
        let default_session = default_session_from_context(&context);
        let core_schema = crate::schema::JazzSchema::new(&context.schema)
            .map_err(|error| JazzError::Schema(error.to_string()))?;
        let identity = core_identity(&context, client_id(&context)?, default_session.as_ref())?;
        let config = DbConfig::new(core_schema, identity);
        let db = match context.storage {
            ClientStorage::Memory => Db::open(config).await,
            ClientStorage::Persistent => {
                Db::open_at(
                    config,
                    &context.data_dir.join(STORE_FILE),
                    FileOptions::default(),
                )
                .await
            }
        }
        .map_err(|error| match error.code {
            crate::db::ErrorCode::UnsupportedStorageCodecs => {
                JazzError::UnsupportedStorage(error.message)
            }
            _ => JazzError::Storage(error.to_string()),
        })?;
        if let Some(session) = default_session.as_ref() {
            db.set_identity_claims(identity.author, session_claims_to_core_claims(session)?);
        }
        let work = Arc::new(Notify::new());
        let (progress, _) = watch::channel(0);
        let shared = Rc::new(Shared {
            db,
            app: context.app_id.to_string(),
            identity,
            schema: context.schema.clone(),
            work: work.clone(),
            progress,
            upstream_paused: watch::channel(false).0,
            serverless: connector.is_none(),
            trusted: context.backend_secret.is_some() || context.admin_secret.is_some(),
            state: RefCell::new(State::default()),
        });

        if connector.is_none() {
            // Still a replica: its writes queue for upload, so a store opened
            // without a server keeps them for the first one it connects to.
            shared.db.set_remote_link_hint(RemoteLinkHint::NoServer);
        }
        if let Some(connector) = connector {
            let wake = work.clone();
            let request = NativeTransportRequest {
                requested_link: NativeTransportLink::OrdinarySession,
                server_url: context.server_url.clone(),
                app_id: context.app_id,
                peer_identity: identity.author,
                auth: AuthConfig {
                    jwt_token: if context.backend_secret.is_some() {
                        None
                    } else {
                        context.jwt_token.clone()
                    },
                    backend_secret: context.backend_secret.clone(),
                    admin_secret: context.admin_secret.clone(),
                    backend_session: None,
                },
                wake: Arc::new(move || wake.notify_one()),
            };
            shared.db.set_remote_link_hint(RemoteLinkHint::Attempting);
            // The first attempt decides whether the server admits this
            // client at all; an unreachable server only means offline.
            let attached = match connector.connect(request.clone()).await {
                Ok(link) => {
                    attach(&shared, link.transport).await;
                    Some(link.terminal)
                }
                Err(
                    NativeTransportError::Terminal(error)
                    | NativeTransportError::AuthRejected(error),
                ) => {
                    return Err(JazzError::Connection(error));
                }
                Err(NativeTransportError::Retryable(error)) => {
                    tracing::debug!(%error, "jazz client starts offline");
                    shared.db.set_remote_link_hint(RemoteLinkHint::Failed);
                    None
                }
            };
            let keeper = tokio::task::spawn_local(keep_upstream(
                Rc::downgrade(&shared),
                connector,
                request,
                attached,
            ));
            shared.state().tasks.push(keeper);
            shared.ensure_driven();
        }
        Ok(Self {
            write_context: None,
            shared,
        })
    }

    fn db(&self) -> Result<&Db> {
        self.shared.ensure_open()?;
        Ok(&self.shared.db)
    }

    fn transaction_id(&self) -> Option<OpenTransactionId> {
        self.write_context
            .as_ref()
            .and_then(|context| context.transaction_id)
    }

    /// The explicit session reads and writes run as, with its claims
    /// registered for policy evaluation.
    fn session_identity(&self) -> Result<Option<AuthorSubject>> {
        let Some(session) = self
            .write_context
            .as_ref()
            .and_then(|context| context.session())
        else {
            return Ok(None);
        };
        let author = session.author_subject()?;
        self.shared
            .db
            .set_identity_claims(author, session_claims_to_core_claims(session)?);
        Ok(Some(author))
    }

    fn write_identity(&self) -> Result<WriteIdentity> {
        if let Some(context) = &self.write_context
            && let Some(attribution) = &context.attribution
        {
            if context.session.is_some() {
                return Err(JazzError::Write(
                    "attribution cannot override the permission session".into(),
                ));
            }
            let author = AuthorSubject::from_untrusted_canonical(attribution)
                .map_err(|error| JazzError::Write(error.to_string()))?;
            if author.account_id().is_none() {
                return Err(JazzError::Write(
                    "attribution requires an admitted account identity".into(),
                ));
            }
            if !self.shared.trusted && !self.shared.serverless {
                return Err(JazzError::Write(
                    "attribution requires a trusted serving node".into(),
                ));
            }
            return Ok(WriteIdentity::Attribution(author));
        }
        Ok(match self.session_identity()? {
            Some(author) => WriteIdentity::Session(author),
            None => WriteIdentity::Database,
        })
    }

    /// Validate a public write-context physical-millisecond timestamp.
    fn write_updated_at(&self) -> Result<Option<u64>> {
        let Some(context) = self.write_context.as_ref() else {
            return Ok(None);
        };
        let Some(updated_at) = context.updated_at() else {
            return Ok(None);
        };
        if context.transaction_id().is_some() {
            return Err(JazzError::Write(
                "updated_at is not supported for transaction-scoped writes".to_owned(),
            ));
        }
        if updated_at > crate::time::HLC_MAX_PHYSICAL_MS {
            return Err(JazzError::Write(format!(
                "updated_at {updated_at} exceeds the packed-HLC physical millisecond range"
            )));
        }
        Ok(Some(updated_at))
    }

    fn reject_updated_at_override(&self, operation: &str) -> Result<()> {
        if self.write_updated_at()?.is_some() {
            return Err(JazzError::Write(format!(
                "updated_at is not supported for {operation}"
            )));
        }
        Ok(())
    }

    fn table_schema(&self, table: &str) -> Result<&TableSchema> {
        self.shared
            .schema
            .get(&TableName::new(table))
            .ok_or_else(|| JazzError::Write(format!("unknown table {table}")))
    }

    fn core_cells(
        &self,
        table: &str,
        values: HashMap<String, Value>,
    ) -> Result<BTreeMap<String, CoreValue>> {
        let table_schema = self.table_schema(table)?;
        values
            .into_iter()
            .map(|(name, value)| {
                let column = table_schema.columns.column(&name).ok_or_else(|| {
                    JazzError::Write(format!("unknown column {name} on table {table}"))
                })?;
                Ok((name, public_to_core_value_for_column(value, column)?))
            })
            .collect()
    }

    fn ordered_values(&self, table: &str, values: &HashMap<String, Value>) -> Result<Vec<Value>> {
        self.table_schema(table)?
            .columns
            .columns
            .iter()
            .map(|column| {
                values
                    .get(column.name.as_str())
                    .cloned()
                    .or_else(|| column.default.clone())
                    .or_else(|| column.nullable.then_some(Value::Null))
                    .ok_or_else(|| {
                        JazzError::Write(format!(
                            "insert is missing required column {}",
                            column.name.as_str()
                        ))
                    })
            })
            .collect()
    }

    /// Record a committed write and ask for its upload.
    fn committed(&self, seq: u64) -> TransactionId {
        let id = TransactionId::from_committed_tx(TxId {
            time: crate::time::TxTime(seq),
            node: self.shared.identity.node,
        });
        self.shared.state().writes.insert(id, seq);
        self.shared.tick_soon();
        id
    }

    fn staged(&self, transaction_id: OpenTransactionId) {
        self.shared
            .state()
            .transactions
            .insert(transaction_id, true);
    }

    fn ensure_transaction_open(&self, transaction_id: OpenTransactionId) -> Result<()> {
        let state = self.shared.state.borrow();
        if state.transactions.contains_key(&transaction_id) {
            return Ok(());
        }
        Err(JazzError::Write(
            match state.closed_transactions.get(&transaction_id) {
                Some(ClosedTransaction::Committed) => {
                    format!("open transaction {transaction_id} is already committed")
                }
                Some(ClosedTransaction::RolledBack) | None => format!(
                    "open transaction {transaction_id} has already been completed or was never opened"
                ),
            },
        ))
    }

    /// The open transaction a transaction-scoped handle stages into.
    fn open_transaction(&self) -> Result<Option<OpenTransactionId>> {
        let Some(transaction_id) = self.transaction_id() else {
            return Ok(None);
        };
        self.ensure_transaction_open(transaction_id)?;
        Ok(Some(transaction_id))
    }

    fn prepare(&self, query: &Query) -> Result<crate::db::PreparedQuery> {
        self.db()?
            .prepare_query(query)
            .map_err(|error| JazzError::Query(error.to_string()))
    }

    /// Subscribe to a query.
    ///
    /// Returns a stream of row deltas as the data changes.
    pub async fn subscribe(&self, query: Query) -> Result<SubscriptionStream> {
        self.subscribe_with_read_tier(query, ReadTier::Remote).await
    }

    /// Subscribe local first, waiting up to `first_load_remote_wait` for the
    /// server's answer before the first delivery.
    ///
    /// While the server could answer, the stream withholds its opening
    /// until the server has answered, the link is lost, or the wait elapses.
    /// Offline, without a server, or with a zero wait it opens on local data
    /// at once. Afterwards it behaves exactly like [`ReadTier::LocalFirst`].
    pub async fn subscribe_local_first(
        &self,
        query: Query,
        first_load_remote_wait: Duration,
    ) -> Result<SubscriptionStream> {
        self.subscribe_with_opts(query, local_first_opts(first_load_remote_wait))
            .await
    }

    /// One-shot local-first query that waits up to `first_load_remote_wait`
    /// for the server's answer, then answers locally.
    pub async fn query_local_first(
        &self,
        query: Query,
        first_load_remote_wait: Duration,
    ) -> Result<Vec<QueryResult>> {
        if first_load_remote_wait.is_zero() || self.transaction_id().is_some() {
            return self.query(query, ReadTier::LocalFirst).await;
        }
        self.query_with_opts(query, local_first_opts(first_load_remote_wait))
            .await
    }

    /// Subscribe using a product-level read tier.
    pub async fn subscribe_with_read_tier(
        &self,
        query: Query,
        tier: ReadTier,
    ) -> Result<SubscriptionStream> {
        self.subscribe_with_opts(query, ReadOpts::for_read_tier(tier))
            .await
    }

    /// Subscribe to a query with explicit core read options.
    pub async fn subscribe_with_opts(
        &self,
        query: Query,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream> {
        if self.transaction_id().is_some() {
            return Err(JazzError::Query(
                "subscriptions in a transaction are not supported".to_owned(),
            ));
        }
        let unsettleable = self.shared.serverless && opts.tier != ReadTier::LocalOnly;
        let stream = self.open_core_subscription(&with_history_identity(&query), opts)?;
        let (sender, receiver) = mpsc::unbounded_channel();
        let (cancel, cancellation) = oneshot::channel();
        self.shared.state().forwarders.push(Forwarder {
            stream,
            query,
            sender,
            cancellation,
            rows: Vec::new(),
            unsettleable,
        });
        self.shared.ensure_driven();
        self.shared.tick_soon();
        Ok(SubscriptionStream::new(receiver, cancel))
    }

    fn open_core_subscription(
        &self,
        query: &Query,
        opts: ReadOpts,
    ) -> Result<CoreSubscriptionStream> {
        let prepared = self.prepare(query)?;
        let session = self.session_identity()?;
        let db = self.db()?;
        crate::db::block_on(async {
            match session {
                Some(author) => db.subscribe_for_identity(&prepared, opts, author).await,
                None => db.subscribe(&prepared, opts).await,
            }
        })
        .map_err(|error| JazzError::Query(error.to_string()))
    }

    /// One-shot query with read tier.
    pub async fn query(&self, query: Query, tier: ReadTier) -> Result<Vec<QueryResult>> {
        self.query_with_opts(query, ReadOpts::for_read_tier(tier))
            .await
    }

    /// Execute a query providing all read options.
    ///
    /// A [`ReadTier::Remote`] read, and a local-first read that waits for
    /// the server, return the first answer of a subscription held for the
    /// read: the server's settled result once it arrives.
    pub async fn query_with_opts(&self, query: Query, opts: ReadOpts) -> Result<Vec<QueryResult>> {
        let read = with_history_identity(&query);
        let rows = if let Some(transaction_id) = self.open_transaction()? {
            let prepared = self.prepare(&read)?;
            let db = self.db()?;
            // A client's serializable read hydrates from the authority,
            // which needs ticks to answer.
            self.shared
                .settle(db.tx_ref(transaction_id).all_prepared(&prepared))
                .await?
                .map_err(|error| JazzError::Query(error.to_string()))?
        } else if opts.tier == ReadTier::Remote
            || matches!(opts.first_load, FirstLoad::WaitForRemote { timeout_ms } if timeout_ms > 0)
        {
            let mut stream = self.open_core_subscription(&read, opts)?;
            match self.shared.until(|| stream.try_next_event()).await? {
                SubscriptionEvent::Delta { mut added, .. } => {
                    added.sort_by_key(|row| row.index);
                    added.into_iter().map(|row| row.row).collect()
                }
                SubscriptionEvent::Rejected { reason } => return Err(JazzError::Query(reason)),
                SubscriptionEvent::Closed => {
                    return Err(JazzError::Query("the read closed before answering".into()));
                }
            }
        } else {
            let prepared = self.prepare(&read)?;
            let session = self.session_identity()?;
            let db = self.db()?;
            let read_local = || {
                crate::db::block_on(async {
                    let roots = match &session {
                        Some(author) => {
                            db.all_for_identity(&prepared, opts.clone(), author.clone())
                                .await
                        }
                        None => db.all(&prepared, opts.clone()).await,
                    }?;
                    // Array relations nest in their parents' cells, as a
                    // subscription's rows carry them.
                    db.nest_relations(&prepared, roots)
                })
            };
            // A row whose large values are still being fetched answers once
            // their chunks arrive, while the upstream that will send them is
            // connected.
            self.shared
                .until(|| match read_local() {
                    Err(error) if error.is_missing_chunks() && db.awaiting_chunks() => None,
                    result => Some(result),
                })
                .await?
                .map_err(|error| JazzError::Query(error.to_string()))?
        };
        Decoder {
            schema: &self.shared.schema,
            query: &query,
            db: None,
        }
        .results(&rows)
    }

    /// Read a byte range from a bytes or string cell.
    ///
    /// Only intersecting large-value chunks are read. Explicit backend
    /// sessions (`for_session`) and transaction-scoped reads are not
    /// supported.
    pub async fn read_value_range(
        &self,
        table: &str,
        object_id: ObjectId,
        column: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>> {
        if let Some(context) = &self.write_context {
            if context.transaction_id.is_some() {
                return Err(JazzError::Query(
                    "byte-range reads in a transaction are not supported".into(),
                ));
            }
            if context.session().is_some() {
                return Err(JazzError::Query(
                    "byte-range reads with an explicit session are not supported".into(),
                ));
            }
        }
        let db = self.db()?;
        db.read_value_range(table, RowUuid(*object_id.uuid()), column, range)
            .await
            .map_err(|error| JazzError::Query(error.to_string()))
    }

    /// Stream one bytes, text or JSON cell into a new row from a reader.
    ///
    /// `values` supplies the other columns and must omit `column`. The value
    /// is chunked as it streams and the row is published only after EOF.
    /// Transaction, explicit session and attribution contexts are not
    /// supported and are rejected before the reader is consumed.
    #[cfg(not(target_family = "wasm"))]
    pub async fn insert_streaming_value<R>(
        &self,
        table: &str,
        values: HashMap<String, Value>,
        column: &str,
        reader: R,
    ) -> Result<(ObjectId, TransactionId)>
    where
        R: std::io::Read + Send + 'static,
    {
        self.reject_updated_at_override("streaming inserts")?;
        if let Some(context) = &self.write_context {
            if context.transaction_id.is_some() {
                return Err(JazzError::Write(
                    "streaming inserts in a transaction are not supported".into(),
                ));
            }
            if context.session().is_some() || context.attribution.is_some() {
                return Err(JazzError::Write(
                    "streaming inserts with an explicit session or attribution are not supported"
                        .into(),
                ));
            }
        }
        let cells = self.core_cells(table, values)?;
        let write = self
            .db()?
            .insert_streaming_value(table, cells, column, reader)
            .await
            .map_err(write_error)?;
        Ok((
            ObjectId::from_uuid(write.row_uuid().0),
            self.committed(write.mergeable_tx_id()),
        ))
    }

    /// Create a new row in a table.
    pub fn insert(
        &self,
        table: &str,
        values: HashMap<String, Value>,
    ) -> Result<(ObjectId, Vec<Value>, Option<TransactionId>)> {
        self.insert_with_id(table, Option::<Uuid>::None, values)
    }

    /// Create a new row in a table using a caller-supplied UUID.
    pub fn insert_with_id(
        &self,
        table: &str,
        object_id: impl Into<Option<Uuid>>,
        values: HashMap<String, Value>,
    ) -> Result<(ObjectId, Vec<Value>, Option<TransactionId>)> {
        self.reject_updated_at_override("inserts")?;
        let row_values = self.ordered_values(table, &values)?;
        let cells = self.core_cells(table, values)?;
        let row_id = object_id.into().map(RowUuid);
        let db = self.db()?;
        if let Some(transaction_id) = self.open_transaction()? {
            let write = crate::db::block_on(db.tx_ref(transaction_id).insert(
                table,
                cells,
                InsertOptions {
                    row_id,
                    identity: self.write_identity()?,
                    ..Default::default()
                },
            ))
            .map_err(write_error)?;
            self.staged(transaction_id);
            return Ok((ObjectId::from_uuid(write.row_uuid().0), row_values, None));
        }
        let write = crate::db::block_on(db.insert(
            table,
            cells,
            InsertOptions {
                row_id,
                identity: self.write_identity()?,
                ..Default::default()
            },
        ))
        .map_err(write_error)?;
        Ok((
            ObjectId::from_uuid(write.row_uuid().0),
            row_values,
            Some(self.committed(write.mergeable_tx_id())),
        ))
    }

    /// Create or update a row using a caller-supplied UUID.
    pub fn upsert(
        &self,
        table: &str,
        object_id: Uuid,
        values: HashMap<String, Value>,
    ) -> Result<Option<TransactionId>> {
        let cells = self.core_cells(table, values)?;
        let updated_at_ms = self.write_updated_at()?;
        let row = RowUuid(object_id);
        let db = self.db()?;
        if let Some(transaction_id) = self.open_transaction()? {
            crate::db::block_on(db.tx_ref(transaction_id).upsert(
                table,
                row,
                cells,
                UpsertOptions {
                    identity: self.write_identity()?,
                    updated_at_ms,
                    ..Default::default()
                },
            ))
            .map_err(write_error)?;
            self.staged(transaction_id);
            return Ok(None);
        }
        let write = crate::db::block_on(db.upsert(
            table,
            row,
            cells,
            UpsertOptions {
                identity: self.write_identity()?,
                updated_at_ms,
                ..Default::default()
            },
        ))
        .map_err(write_error)?;
        Ok(Some(self.committed(write.mergeable_tx_id())))
    }

    /// Update a row in the named table.
    ///
    /// The row need not have appeared in a previous query result. The table and
    /// ID identify the target; the database still applies write policies.
    pub fn update(
        &self,
        table: &str,
        object_id: ObjectId,
        updates: Vec<(String, Value)>,
    ) -> Result<Option<TransactionId>> {
        let cells = self.core_cells(table, updates.into_iter().collect())?;
        let updated_at_ms = self.write_updated_at()?;
        let row = RowUuid(*object_id.uuid());
        let db = self.db()?;
        if let Some(transaction_id) = self.open_transaction()? {
            crate::db::block_on(db.tx_ref(transaction_id).update(
                table,
                row,
                cells,
                UpdateOptions {
                    identity: self.write_identity()?,
                    updated_at_ms,
                    ..Default::default()
                },
            ))
            .map_err(write_error)?;
            self.staged(transaction_id);
            return Ok(None);
        }
        let write = crate::db::block_on(db.update(
            table,
            row,
            cells,
            UpdateOptions {
                identity: self.write_identity()?,
                updated_at_ms,
                ..Default::default()
            },
        ))
        .map_err(write_error)?;
        Ok(Some(self.committed(write.mergeable_tx_id())))
    }

    /// Delete a row in the named table.
    ///
    /// The row need not have appeared in a previous query result. The table and
    /// ID identify the target; the database still applies write policies.
    pub fn delete(&self, table: &str, object_id: ObjectId) -> Result<Option<TransactionId>> {
        self.reject_updated_at_override("deletes")?;
        let row = RowUuid(*object_id.uuid());
        let db = self.db()?;
        if let Some(transaction_id) = self.open_transaction()? {
            crate::db::block_on(db.tx_ref(transaction_id).delete(
                table,
                row,
                DeleteOptions {
                    identity: self.write_identity()?,
                    ..Default::default()
                },
            ))
            .map_err(write_error)?;
            self.staged(transaction_id);
            return Ok(None);
        }
        let write = crate::db::block_on(db.delete(
            table,
            row,
            DeleteOptions {
                identity: self.write_identity()?,
                ..Default::default()
            },
        ))
        .map_err(write_error)?;
        Ok(Some(self.committed(write.mergeable_tx_id())))
    }

    /// Restore a deleted row, preserving its content when `values` is `None`.
    /// Supplying values replaces its content atomically. Uses the same session
    /// and transaction context as other mutations, including write policies.
    /// Returns `None` when staged in an open transaction.
    pub fn restore(
        &self,
        table: &str,
        object_id: ObjectId,
        values: Option<Vec<(String, Value)>>,
    ) -> Result<Option<TransactionId>> {
        let cells = values
            .map(|values| self.core_cells(table, values.into_iter().collect()))
            .transpose()?;
        let updated_at_ms = self.write_updated_at()?;
        let row = RowUuid(*object_id.uuid());
        let db = self.db()?;
        if let Some(transaction_id) = self.open_transaction()? {
            crate::db::block_on(db.tx_ref(transaction_id).restore(
                table,
                row,
                cells,
                RestoreOptions {
                    identity: self.write_identity()?,
                    updated_at_ms,
                    ..Default::default()
                },
            ))
            .map_err(write_error)?;
            self.staged(transaction_id);
            return Ok(None);
        }
        let write = crate::db::block_on(db.restore(
            table,
            row,
            cells,
            RestoreOptions {
                identity: self.write_identity()?,
                updated_at_ms,
                ..Default::default()
            },
        ))
        .map_err(write_error)?;
        Ok(Some(self.committed(write.mergeable_tx_id())))
    }

    /// Begin a transaction and return a transaction-scoped client handle.
    ///
    /// Transactions are serializable: reads run at the snapshot the replica
    /// is known complete through, and the authority applies the writes only
    /// if every read and written row is unchanged since then. Staged writes
    /// are not visible to ordinary reads until the transaction commits.
    pub fn begin_transaction(&self) -> Result<JazzTransaction> {
        // Each staged write carries this handle's identity (checked when
        // staged, and named on the commit for the authority).
        self.write_identity()?;
        let db = self.db()?;
        let mut transaction_id = OpenTransactionId::new();
        {
            let state = self.shared.state.borrow();
            while state.transactions.contains_key(&transaction_id)
                || state.closed_transactions.contains_key(&transaction_id)
            {
                transaction_id = OpenTransactionId::new();
            }
        }
        crate::db::block_on(db.begin_serializable(transaction_id)).map_err(write_error)?;
        self.shared
            .state()
            .transactions
            .insert(transaction_id, false);
        let write_context = self
            .write_context
            .clone()
            .unwrap_or_default()
            .with_transaction_id(transaction_id);
        Ok(JazzTransaction {
            transaction_id,
            client: self.with_write_context(write_context),
        })
    }

    /// Commit an open transaction by transaction id.
    pub fn commit_transaction(&self, transaction_id: OpenTransactionId) -> Result<TransactionId> {
        self.ensure_transaction_open(transaction_id)?;
        if !self.shared.state.borrow().transactions[&transaction_id] {
            return Err(JazzError::Write(
                "transaction cannot commit without writes".to_string(),
            ));
        }
        let receipt = crate::db::block_on(self.db()?.commit_serializable_handle(transaction_id));
        {
            let mut state = self.shared.state();
            state.transactions.remove(&transaction_id);
            state
                .closed_transactions
                .insert(transaction_id, ClosedTransaction::Committed);
        }
        Ok(self.committed(receipt.map_err(write_error)?.0))
    }

    /// Roll back an open transaction by transaction id.
    ///
    /// Returns whether a local batch record existed for the transaction.
    pub fn rollback_transaction(&self, transaction_id: OpenTransactionId) -> Result<bool> {
        self.ensure_transaction_open(transaction_id)?;
        self.db()?
            .abandon_transaction_handle(transaction_id)
            .map_err(write_error)?;
        let mut state = self.shared.state();
        state.transactions.remove(&transaction_id);
        state
            .closed_transactions
            .insert(transaction_id, ClosedTransaction::RolledBack);
        Ok(true)
    }

    /// Wait until a commit of this client reaches `tier`, failing if the
    /// authority rejected it.
    pub async fn wait_for_transaction(
        &self,
        transaction_id: TransactionId,
        tier: DurabilityTier,
    ) -> Result<()> {
        let seq = self
            .shared
            .state
            .borrow()
            .writes
            .get(&transaction_id)
            .copied()
            .ok_or_else(|| {
                JazzError::Write(format!(
                    "transaction {transaction_id} was not committed by this client"
                ))
            })?;
        let core_tier = match tier {
            DurabilityTier::Local => CoreDurabilityTier::Local,
            DurabilityTier::GlobalServer => CoreDurabilityTier::Global,
        };
        if self.shared.serverless && core_tier > CoreDurabilityTier::Local {
            return Err(JazzError::Write(format!(
                "{tier:?} durability is unreachable: this client has no server"
            )));
        }
        let db = self.db()?;
        self.shared
            .settle(db.wait_for_transaction(seq, core_tier))
            .await?
            .map(drop)
            .map_err(|error| {
                // A policy rejection is reported by its reason, as the
                // authority's verdict on the transaction.
                if error.is_authorization_denied() {
                    JazzError::Sync(format!(
                        "transaction was rejected before reaching {tier:?} durability: {}",
                        crate::db::AUTHORIZATION_DENIED
                    ))
                } else {
                    JazzError::Write(format!(
                        "transaction was rejected before reaching {tier:?} durability: {error}"
                    ))
                }
            })
    }

    /// Unsubscribe from a subscription. Dropping the stream unsubscribes.
    pub async fn unsubscribe(&self, _handle: SubscriptionHandle) -> Result<()> {
        Ok(())
    }

    /// Get the current schema.
    pub fn schema(&self) -> Result<Schema> {
        Ok(self.shared.schema.clone())
    }

    /// Log query shapes whose runs take at least `threshold` as `tracing`
    /// warnings, one line per shape, at most every 10 s (`None` turns the log
    /// off). Defaults to [`crate::slow::DEFAULT_SLOW_QUERY_THRESHOLD`].
    pub fn set_slow_query_threshold(&self, threshold: Option<Duration>) {
        self.shared.db.set_slow_query_threshold(threshold);
    }

    /// Check if connected to server.
    pub fn is_connected(&self) -> bool {
        self.shared.state.borrow().upstream.is_some()
    }

    /// Create a client that uses the given write context for mutations.
    pub fn with_write_context(&self, write_context: WriteContext) -> JazzClient {
        JazzClient {
            write_context: Some(write_context),
            shared: self.shared.clone(),
        }
    }

    /// Create a session-scoped client for backend operations.
    pub fn for_session(&self, session: Session) -> JazzClient {
        self.with_write_context(WriteContext::from_session(session))
    }

    /// Shutdown this shared client context and release its resources.
    ///
    /// Every [`JazzClient`] clone for this context becomes unusable.
    pub async fn shutdown(self) -> Result<()> {
        let tasks = {
            let mut state = self.shared.state();
            if state.closed {
                return Ok(());
            }
            state.closed = true;
            state.forwarders.clear();
            std::mem::take(&mut state.tasks)
        };
        for task in tasks {
            task.abort();
        }
        detach(&self.shared);
        // Wake waiters so they observe the shutdown.
        self.shared
            .progress
            .send_modify(|generation| *generation += 1);
        self.shared
            .db
            .close()
            .await
            .map_err(|error| JazzError::Storage(error.to_string()))
    }
}

#[cfg(feature = "testing")]
impl JazzClient {
    pub fn client_id(&self) -> Option<crate::tools::ClientId> {
        Some(crate::tools::ClientId(self.shared.identity.node.0))
    }

    pub async fn test_client(schema: Schema) -> crate::tools::JazzClient {
        let mut context = crate::tools::AppContext::test(schema);
        // This isolated fixture is a trusted backend. Ordinary accountless
        // contexts remain anonymous readers and cannot author rows.
        context.admin_secret = Some("test-admin".to_owned());
        crate::tools::JazzClient::connect(context)
            .await
            .expect("connect local JazzClient")
    }

    /// The engine's sync state, for test failure messages.
    pub(crate) fn sync_debug_for_test(&self) -> String {
        self.shared.db.sync_debug()
    }

    pub(crate) fn disconnect_upstream_for_test(&self) -> bool {
        self.shared.upstream_paused.send_replace(true);
        let detached = detach(&self.shared);
        self.shared.db.set_remote_link_hint(RemoteLinkHint::Failed);
        detached
    }

    /// Wait for a durability tier using an exact timeout rather than the
    /// load-tolerant test multiplier.
    pub async fn wait_for_transaction_with_timeout_for_test(
        &self,
        transaction_id: TransactionId,
        tier: DurabilityTier,
        timeout: Duration,
    ) -> Result<()> {
        tokio::time::timeout(timeout, self.wait_for_transaction(transaction_id, tier))
            .await
            .map_err(|_| JazzError::Sync(format!("timed out waiting for {tier:?}")))?
    }

    /// Let the keeper open a fresh link for the preserved client state and
    /// wait until it is attached.
    pub(crate) async fn reconnect_upstream_for_test(&self) -> Result<bool> {
        let (failed, attached) = {
            let state = self.shared.state.borrow();
            (state.connect_failures.0, state.upstream.is_some())
        };
        // Neither detached nor reconnecting: nothing to reattach.
        if !self.shared.upstream_paused.send_replace(false) && attached {
            return Ok(false);
        }
        // The first attempt either attaches or fails; the keeper retries
        // after a failure on its own.
        self.shared
            .until(|| {
                let state = self.shared.state.borrow();
                if state.upstream.is_some() {
                    return Some(Ok(true));
                }
                match &state.connect_failures {
                    (n, error) if *n > failed => Some(Err(JazzError::Connection(
                        error.clone().unwrap_or_default(),
                    ))),
                    _ => None,
                }
            })
            .await?
    }
}

/// A transaction-scoped client handle.
pub struct JazzTransaction {
    transaction_id: OpenTransactionId,
    client: JazzClient,
}

impl JazzTransaction {
    /// Logical transaction id backing this transaction.
    pub fn transaction_id(&self) -> OpenTransactionId {
        self.transaction_id
    }

    /// The transaction-scoped client.
    pub fn client(&self) -> &JazzClient {
        &self.client
    }

    /// Commit this transaction.
    ///
    /// Returns the transaction id so callers can wait for durability with
    /// [`JazzClient::wait_for_transaction`] if needed.
    pub fn commit(self) -> Result<TransactionId> {
        self.client.commit_transaction(self.transaction_id)
    }

    /// Roll back this transaction locally.
    pub fn rollback(self) -> Result<bool> {
        self.client.rollback_transaction(self.transaction_id)
    }
}

impl Deref for JazzTransaction {
    type Target = JazzClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

fn write_error(error: CoreError) -> JazzError {
    JazzError::Write(error.to_string())
}

fn local_first_opts(first_load_remote_wait: Duration) -> ReadOpts {
    let mut opts = ReadOpts::for_read_tier(ReadTier::LocalFirst);
    opts.first_load = FirstLoad::WaitForRemote {
        timeout_ms: u64::try_from(first_load_remote_wait.as_millis()).unwrap_or(u64::MAX),
    };
    opts
}

/// The query as read: a projected history read also reads each version's
/// identity, which keys its results. Results decode against the caller's
/// projection.
fn with_history_identity(query: &Query) -> Query {
    let mut read = query.clone();
    if history::is_history_table(&read.table)
        && let Some(columns) = &mut read.select
        && !columns
            .iter()
            .any(|column| column == history::TRANSACTION_ID_COLUMN)
    {
        columns.push(history::TRANSACTION_ID_COLUMN.to_owned());
    }
    read
}

/// The node id this client's writes are uploaded under: the configured one,
/// or for a persistent store the one it was first opened with.
fn client_id(context: &AppContext) -> Result<Uuid> {
    if let Some(id) = context.client_id {
        return Ok(id.0);
    }
    if context.storage == ClientStorage::Memory {
        return Ok(Uuid::now_v7());
    }
    if context.data_dir.join(PRE_SQLITE_STORE).exists() {
        return Err(JazzError::UnsupportedStorage(format!(
            "{} holds a store from before the SQLite engine; delete it and resync from Core",
            context.data_dir.display()
        )));
    }
    std::fs::create_dir_all(&context.data_dir)?;
    let path = context.data_dir.join(CLIENT_ID_FILE);
    match std::fs::read_to_string(&path) {
        Ok(stored) => Uuid::parse_str(stored.trim()).map_err(|error| {
            JazzError::Storage(format!("invalid client id in {}: {error}", path.display()))
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let id = Uuid::now_v7();
            std::fs::write(&path, id.to_string())?;
            Ok(id)
        }
        Err(error) => Err(error.into()),
    }
}

// ---- identity ---------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct UnverifiedJwtClaims {
    iss: String,
    sub: String,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}

fn session_from_unverified_jwt(token: &str) -> Option<Session> {
    let payload = token.split('.').nth(1)?;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))
        .ok()?;
    let claims: UnverifiedJwtClaims = serde_json::from_slice(&payload).ok()?;
    let user_id = claims.sub.as_str();
    if !crate::identity::principal_is_nonempty(user_id) {
        return None;
    }
    let auth_mode = match claims.iss.as_str() {
        AuthorSubject::LOCAL_FIRST_ISSUER => {
            crate::model::public_api::session::AuthMode::LocalFirst
        }
        AuthorSubject::ANONYMOUS_ISSUER => crate::model::public_api::session::AuthMode::Anonymous,
        _ => crate::model::public_api::session::AuthMode::External,
    };
    Some(Session {
        account_id: None,
        issuer: claims.iss.clone(),
        user_id: user_id.to_string(),
        claims: serde_json::Value::Object(
            claims
                .extra
                .into_iter()
                .filter(|(name, _)| !is_registered_jwt_claim(name))
                .collect(),
        ),
        auth_mode,
    })
}

fn default_session_from_context(context: &AppContext) -> Option<Session> {
    if context.backend_secret.is_some() || context.admin_secret.is_some() {
        return None;
    }
    context
        .jwt_token
        .as_deref()
        .and_then(session_from_unverified_jwt)
        .map(|mut session| {
            session.account_id = context.account_id;
            session
        })
}

fn core_identity(
    context: &AppContext,
    node: Uuid,
    default_session: Option<&Session>,
) -> Result<DbIdentity> {
    let author = match default_session {
        Some(session) => session.author_subject()?,
        // A backend/admin credential is an internal authority context, not
        // an unauthenticated end-user session.
        None if context.backend_secret.is_some() || context.admin_secret.is_some() => {
            AuthorSubject::SYSTEM
        }
        None => AuthorSubject::reserved(AuthorSubject::ANONYMOUS_ISSUER, &node.to_string())?,
    };
    Ok(DbIdentity {
        node: NodeUuid(node),
        author,
    })
}

fn session_claims_to_core_claims(session: &Session) -> Result<BTreeMap<String, CoreValue>> {
    let claims = match session.claims.clone() {
        serde_json::Value::Object(claims) => claims,
        _ => serde_json::Map::new(),
    };
    let mut core_claims = BTreeMap::new();
    for (name, value) in claims {
        if let Some(value) = crate::model::policy_claims::json_value_to_policy_claim(
            value,
            crate::model::policy_claims::NumericClaimOrigin::ExactJson,
        )
        .map_err(JazzError::Connection)?
        {
            core_claims.insert(crate::query::provider_claim_key(&name), value);
        }
    }
    core_claims.insert(
        crate::query::provider_claim_key("sub"),
        CoreValue::String(session.user_id.clone()),
    );
    core_claims.insert(
        crate::query::provider_claim_key("iss"),
        CoreValue::String(session.issuer.clone()),
    );
    core_claims.insert(
        "authMode".to_owned(),
        CoreValue::String(auth_mode_claim_value(session.auth_mode).to_owned()),
    );
    core_claims.extend(crate::model::policy_claims::author_policy_claims(
        session.author_subject()?,
    ));
    Ok(core_claims)
}

fn is_registered_jwt_claim(name: &str) -> bool {
    matches!(name, "iss" | "sub" | "aud" | "exp" | "nbf" | "iat" | "jti")
}

fn auth_mode_claim_value(auth_mode: crate::model::public_api::session::AuthMode) -> &'static str {
    match auth_mode {
        crate::model::public_api::session::AuthMode::External => "external",
        crate::model::public_api::session::AuthMode::LocalFirst => "local-first",
        crate::model::public_api::session::AuthMode::Anonymous => "anonymous",
    }
}

// ---- values -----------------------------------------------------------------

fn public_to_core_value(value: Value) -> Result<CoreValue> {
    match value {
        Value::Boolean(value) => Ok(CoreValue::Bool(value)),
        Value::Text(value) => Ok(CoreValue::String(value)),
        Value::Integer(value) => Ok(CoreValue::I32(value)),
        Value::BigInt(value) => Ok(CoreValue::I64(value)),
        Value::Double(value) => Ok(CoreValue::F64(value)),
        Value::Timestamp(value) => Ok(CoreValue::U64(value)),
        Value::Uuid(value) => Ok(CoreValue::Uuid(*value.uuid())),
        Value::Bytea(value) => Ok(CoreValue::Bytes(value)),
        Value::Null => Ok(CoreValue::Nullable(None)),
        Value::Array(values) => values
            .into_iter()
            .map(public_to_core_value)
            .collect::<Result<Vec<_>>>()
            .map(CoreValue::Array),
        other => Err(JazzError::Write(format!(
            "client does not support public value {other:?}"
        ))),
    }
}

fn public_to_core_value_for_column_type(
    value: Value,
    column_type: &ColumnType,
) -> Result<CoreValue> {
    match (value, column_type) {
        (Value::Null, _) => Ok(CoreValue::Nullable(None)),
        (Value::Integer(value), ColumnType::Integer) => Ok(CoreValue::I32(value)),
        (Value::BigInt(value), ColumnType::Integer) => {
            i32::try_from(value).map(CoreValue::I32).map_err(|_| {
                JazzError::Write(format!("BIGINT value {value} is outside INTEGER range"))
            })
        }
        (Value::Integer(value), ColumnType::BigInt) => Ok(CoreValue::I64(i64::from(value))),
        (Value::BigInt(value), ColumnType::BigInt) => Ok(CoreValue::I64(value)),
        (Value::Array(values), ColumnType::Array { element }) => values
            .into_iter()
            .map(|value| public_to_core_value_for_column_type(value, element))
            .collect::<Result<Vec<_>>>()
            .map(CoreValue::Array),
        (value, _) => public_to_core_value(value),
    }
}

fn public_to_core_value_for_column(value: Value, column: &ColumnDescriptor) -> Result<CoreValue> {
    validate_json_value(&value, &column.column_type, column.name_str())
        .map_err(JazzError::Write)?;
    let value = public_to_core_value_for_column_type(value, &column.column_type)?;
    if column.nullable && !matches!(value, CoreValue::Nullable(_)) {
        Ok(CoreValue::Nullable(Some(Box::new(value))))
    } else {
        Ok(value)
    }
}

fn core_to_public_value(value: CoreValue) -> Result<Value> {
    match value {
        CoreValue::Bool(value) => Ok(Value::Boolean(value)),
        CoreValue::String(value) => Ok(Value::Text(value)),
        CoreValue::U32(value) => Ok(Value::Integer(i32::try_from(value).map_err(|_| {
            JazzError::Query(format!("U32 value {value} is outside INTEGER range"))
        })?)),
        CoreValue::I32(value) => Ok(Value::Integer(value)),
        CoreValue::I64(value) => Ok(Value::BigInt(value)),
        CoreValue::U64(value) | CoreValue::U48(value) => Ok(Value::Timestamp(value)),
        CoreValue::F64(value) => Ok(Value::Double(value)),
        CoreValue::Uuid(value) => Ok(Value::Uuid(ObjectId::from_uuid(value))),
        CoreValue::Bytes(value) => Ok(Value::Bytea(value)),
        CoreValue::Nullable(None) => Ok(Value::Null),
        CoreValue::Nullable(Some(value)) => core_to_public_value(*value),
        CoreValue::Array(values) => values
            .into_iter()
            .map(core_to_public_value)
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        CoreValue::Tuple(values) => Ok(Value::Row {
            id: None,
            values: values
                .into_iter()
                .map(core_to_public_value)
                .collect::<Result<Vec<_>>>()?,
        }),
        CoreValue::Record(record) => {
            let identity_index = ["row_uuid", "id"]
                .into_iter()
                .find_map(|name| record.descriptor().field_index(name));
            let id = identity_index.and_then(|index| match record.get_idx(index) {
                Ok(CoreValue::Uuid(id)) => Some(ObjectId::from_uuid(id)),
                _ => None,
            });
            // `row_uuid` only keys a nested row; an `id` field is data.
            let hidden = identity_index.filter(|index| {
                record.descriptor().fields()[*index].name.as_deref() == Some("row_uuid")
            });
            let values = record
                .to_values()
                .map_err(|error| JazzError::Query(error.to_string()))?
                .into_iter()
                .enumerate()
                .filter(|(index, _)| Some(*index) != hidden)
                .map(|(_, value)| core_to_public_value(value))
                .collect::<Result<Vec<_>>>()?;
            Ok(Value::Row { id, values })
        }
        other => Err(JazzError::Query(format!(
            "client does not support value {other:?}"
        ))),
    }
}

fn core_to_public_value_for_column_type(
    value: CoreValue,
    column_type: &ColumnType,
) -> Result<Value> {
    match (value, column_type) {
        (CoreValue::Nullable(None), _) => Ok(Value::Null),
        (CoreValue::Nullable(Some(value)), column_type) => {
            core_to_public_value_for_column_type(*value, column_type)
        }
        (CoreValue::I32(value), ColumnType::Integer) => Ok(Value::Integer(value)),
        (CoreValue::I64(value), ColumnType::Integer) => i32::try_from(value)
            .map(Value::Integer)
            .map_err(|_| JazzError::Query(format!("I64 value {value} is outside INTEGER range"))),
        (CoreValue::I64(value), ColumnType::BigInt) => Ok(Value::BigInt(value)),
        (CoreValue::Array(values), ColumnType::Array { element }) => values
            .into_iter()
            .map(|value| core_to_public_value_for_column_type(value, element))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        (value, _) => core_to_public_value(value),
    }
}

// ---- results ----------------------------------------------------------------

/// Decodes engine rows into public results for the caller's query.
struct Decoder<'a> {
    schema: &'a Schema,
    query: &'a Query,
    /// Where a delivered row's provenance is read when the query does not
    /// project it.
    db: Option<&'a Db>,
}

impl Decoder<'_> {
    fn table_schema(&self, table: &str) -> Result<&TableSchema> {
        self.schema
            .get(&TableName::new(
                history::history_source_table(table).unwrap_or(table),
            ))
            .ok_or_else(|| JazzError::Query(format!("unknown table {table}")))
    }

    /// The query's output fields in order, with their declared types where
    /// a column declares one.
    fn fields(&self) -> Result<Vec<(String, Option<ColumnType>)>> {
        let query = self.query;
        let table_schema = self.table_schema(&query.table)?;
        let column_type = |schema: &TableSchema, name: &str| {
            schema
                .columns
                .column(name)
                .map(|column| column.column_type.clone())
        };
        if let Some(aggregate) = &query.aggregate {
            let mut fields: Vec<_> = aggregate
                .group_by
                .iter()
                .map(|name| (name.clone(), column_type(table_schema, name)))
                .collect();
            fields.extend(
                aggregate
                    .aggregates
                    .iter()
                    .map(|output| (output.alias.clone(), None)),
            );
            return Ok(fields);
        }
        if let Some(flat_join) = &query.flat_join {
            let mut sources = vec![(
                flat_join
                    .root_alias
                    .clone()
                    .unwrap_or_else(|| query.table.clone()),
                query.table.clone(),
            )];
            sources.extend(flat_join.sources.iter().map(|source| {
                (
                    source.alias.clone().unwrap_or_else(|| source.table.clone()),
                    source.table.clone(),
                )
            }));
            let mut fields = Vec::new();
            for (source, table) in sources {
                let source_schema = self.table_schema(&table)?;
                for name in projected(&Query::from(table.as_str()), source_schema) {
                    let ty = column_type(source_schema, &name);
                    fields.push((format!("{source}.{name}"), ty));
                }
            }
            return Ok(fields);
        }
        let mut fields: Vec<_> = projected(query, table_schema)
            .into_iter()
            .map(|name| {
                let ty = column_type(table_schema, &name);
                (name, ty)
            })
            .collect();
        fields.extend(
            query
                .array_subqueries
                .iter()
                .map(|subquery| (subquery.column_name.clone(), None)),
        );
        Ok(fields)
    }

    fn key(&self, row: &CurrentRow) -> Result<ResultKey> {
        let id = ObjectId::from_uuid(row.row_uuid().0);
        if self.query.aggregate.is_none()
            && self.query.flat_join.is_none()
            && history::is_history_table(&self.query.table)
        {
            let Some(CoreValue::Tuple(version)) = cell(row, history::TRANSACTION_ID_COLUMN) else {
                return Err(JazzError::Query(
                    "history result is missing its transaction identity".into(),
                ));
            };
            let [CoreValue::U64(time), CoreValue::Uuid(node)] = version.as_slice() else {
                return Err(JazzError::Query(
                    "invalid history transaction identity".into(),
                ));
            };
            return Ok(ResultKey::from_occurrence(OutputOccurrenceId::history(
                id,
                crate::time::TxTime(*time),
                NodeUuid(*node),
            )));
        }
        Ok(row.occurrence_key())
    }

    fn values(&self, row: &CurrentRow) -> Result<Vec<QueryResultField>> {
        self.fields()?
            .into_iter()
            .map(|(name, ty)| {
                let value = if name == "id" && self.query.aggregate.is_none() {
                    Value::Uuid(ObjectId::from_uuid(row.row_uuid().0))
                } else {
                    let value = cell(row, &name).ok_or_else(|| {
                        JazzError::Query(format!("row is missing projected field {name}"))
                    })?;
                    match &ty {
                        Some(ty) => core_to_public_value_for_column_type(value, ty)?,
                        None => core_to_public_value(value)?,
                    }
                };
                Ok(QueryResultField { name, value })
            })
            .collect()
    }

    fn results(&self, rows: &[CurrentRow]) -> Result<Vec<QueryResult>> {
        rows.iter()
            .map(|row| Ok(QueryResult::new(self.key(row)?, self.values(row)?)))
            .collect()
    }

    /// A delivered row's provenance: its projected magic columns, or the
    /// stored row's stamps.
    fn provenance(&self, row: &CurrentRow) -> Result<crate::model::metadata::RowProvenance> {
        let projected = provenance(row);
        let Some(db) = self.db.filter(|_| projected.created_by == "jazz:unknown") else {
            return Ok(projected);
        };
        Ok(
            match db
                .row_provenance(row.table(), row.row_uuid())
                .map_err(|error| JazzError::Query(error.to_string()))?
            {
                Some((created_at, created_by, updated_at, updated_by)) => {
                    crate::model::metadata::RowProvenance {
                        created_by,
                        created_at,
                        updated_by,
                        updated_at,
                    }
                }
                None => projected,
            },
        )
    }

    fn subscription_row(&self, row: &SubscriptionOutputRow) -> Result<Row> {
        let record = crate::binding_codec::row_record(&row.row)
            .map_err(|error| JazzError::Query(format!("encode subscription row: {error}")))?;
        let public = Row::new(
            self.key(&row.row)?,
            record.raw().to_vec(),
            TransactionId([0; 16]),
            self.provenance(&row.row)?,
        );
        #[cfg(feature = "testing")]
        let public = public.with_fields(self.values(&row.row)?);
        Ok(public)
    }
}

/// The columns a read of `table` projects, in order.
fn projected(query: &Query, table: &TableSchema) -> Vec<String> {
    if let Some(columns) = &query.select {
        return columns.clone();
    }
    let mut columns: Vec<String> = table
        .columns
        .columns
        .iter()
        .map(|column| column.name.as_str().to_owned())
        .collect();
    if history::is_history_table(&query.table) {
        columns.extend(
            history::HistoryMetadataColumn::ALL
                .iter()
                .map(|column| column.name().to_owned()),
        );
    }
    columns
}

/// A projected cell by name; an unwritten cell reads as null.
fn cell(row: &CurrentRow, name: &str) -> Option<CoreValue> {
    row.cells()
        .find(|(cell, _)| *cell == name)
        .map(|(_, value)| value.cloned().unwrap_or(CoreValue::Nullable(None)))
}

/// Row provenance from projected magic columns, when the read projects them.
fn provenance(row: &CurrentRow) -> crate::model::metadata::RowProvenance {
    let unwrap = |value: Option<CoreValue>| match value {
        Some(CoreValue::Nullable(Some(value))) => Some(*value),
        other => other,
    };
    let timestamp = |name| match unwrap(cell(row, name)) {
        Some(CoreValue::U64(at)) => Some(at),
        _ => None,
    };
    let author = |name| match unwrap(cell(row, name)) {
        Some(CoreValue::String(author)) => Some(author),
        _ => None,
    };
    let created_at = timestamp("$createdAt").unwrap_or(0);
    let created_by = author("$createdBy").unwrap_or_else(|| "jazz:unknown".to_owned());
    crate::model::metadata::RowProvenance {
        updated_at: timestamp("$updatedAt").unwrap_or(created_at),
        updated_by: author("$updatedBy").unwrap_or_else(|| created_by.clone()),
        created_by,
        created_at,
    }
}
