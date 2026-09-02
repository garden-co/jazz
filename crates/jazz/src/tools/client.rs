//! Thin Rust client facade over `crate::db`.

#[cfg(test)]
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::task::{ArcWake, waker};

use crate::db::{
    Db as CoreDb, DbConfig as CoreDbConfig, DbIdentity as CoreDbIdentity, Error as CoreDbError,
    ErrorCode as CoreDbErrorCode, ExclusiveTxOps, LocalUpdates as CoreLocalUpdates,
    PeerConnection as CorePeerConnection, Propagation as CorePropagation, ReadOpts as CoreReadOpts,
    SubscriptionEvent as CoreSubscriptionEvent, SubscriptionOutputRow as CoreSubscriptionOutputRow,
    TickScheduler, TickUrgency, Transport as CoreTransport, WireTransportAdapter,
};
use crate::groove::records::{
    BorrowedRecord, OwnedRecord, Value as CoreValue, ValueType as CoreValueType,
};
use crate::groove::storage::{BoxedStorage as CoreStorage, MemoryStorage as CoreMemoryStorage};
use crate::ids::{
    AuthorSubject as CoreAuthorSubject, NodeUuid as CoreNodeUuid, RowUuid as CoreRowUuid,
};
use crate::protocol::ReadViewSpec as CoreReadViewSpec;
use crate::query::{Aggregate as CoreAggregate, AggregateFunction as CoreAggregateFunction, Query};
use crate::storage_codec_profile::epoch_1_storage_codec_profile;
use crate::tools::OpenTransactionId;
use crate::tools::native_transport_connector::{NativeTransportConnector, NativeTransportRequest};
use crate::tools::public_api::types::{
    OrderedAdded, OrderedRemoved, OrderedUpdated, QueryResultField,
};
use crate::tools::public_schema::TableName;
use crate::tools::public_schema::{ColumnType, Session, TableSchema, Value, WriteContext};
use crate::tools::public_schema::{OrderedRowDelta, QueryResult, Row};
use crate::tools::public_schema::{Schema, validate_json_value};
#[cfg(feature = "testing")]
use crate::tools::sync::ClientId;
use crate::tools::sync::{DurabilityTier, ReadTier};
use crate::tools::transaction::TransactionId;
use crate::tools::websocket_prelude_auth::AuthConfig as WsAuthConfig;
use crate::tx::{
    DurabilityTier as CoreDurabilityTier, Fate as CoreFate, RejectionReason as CoreRejectionReason,
    TxId as CoreTxId,
};
use base64::Engine;
use futures::lock::Mutex as LocalMutex;
use serde::{Deserialize, Deserializer};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::tools::{
    AppContext, ClientStorage, JazzError, ObjectId, OutputOccurrenceId, Result, ResultKey,
    SubscriptionHandle, SubscriptionRejectReason, SubscriptionServerFailureCode,
    SubscriptionStream, SubscriptionStreamItem,
};

type CoreClientDb = CoreDb<CoreStorage>;
type BackendConnection = Rc<LocalMutex<CorePeerConnection<CoreStorage>>>;

const MAX_TICK_DRIVER_RECOVERY_ATTEMPTS: u32 = 12;
const TICK_DRIVER_RETRY_BASE_DELAY: Duration = Duration::from_millis(50);
const TICK_DRIVER_RETRY_MAX_DELAY: Duration = Duration::from_secs(2);

struct StackSafeFuture<F> {
    inner: Pin<Box<F>>,
}

impl<F> StackSafeFuture<F> {
    fn new(inner: F) -> Self {
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl<F: Future> Future for StackSafeFuture<F> {
    type Output = F::Output;

    fn poll(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        stacker::maybe_grow(4 * 1024 * 1024, 8 * 1024 * 1024, || {
            self.inner.as_mut().poll(context)
        })
    }
}

type StorageBundle = CoreStorage;

#[derive(Debug, Deserialize)]
struct UnverifiedJwtClaims {
    iss: String,
    sub: String,
    #[serde(default)]
    claims: JwtClaimsPayload,
}

/// Keeps a missing application-claims field distinct from a supplied JSON
/// value, including JSON `null`.
#[derive(Debug, Default)]
enum JwtClaimsPayload {
    #[default]
    Absent,
    Present(serde_json::Value),
}

impl<'de> Deserialize<'de> for JwtClaimsPayload {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_json::Value::deserialize(deserializer).map(Self::Present)
    }
}

/// Jazz client for building applications.
///
/// Combines local storage with server sync.
pub struct JazzClient {
    /// Session inferred from client auth context for user-scoped operations.
    default_session: Option<Session>,
    /// Write metadata applied to mutations issued through this client.
    write_context: Option<WriteContext>,
    /// Shared core database handle backing the public client facade.
    db: Rc<ClientDb>,
}

impl Clone for JazzClient {
    fn clone(&self) -> Self {
        Self {
            default_session: self.default_session.clone(),
            write_context: self.write_context.clone(),
            db: self.db.clone(),
        }
    }
}

struct ClientDb {
    inner: Rc<RefCell<ClientDbInner>>,
    query_decoder: PublicQueryDecoder,
}

#[derive(Clone)]
struct PublicQueryDecoder {
    schema: Rc<Schema>,
}

struct ClientDbInner {
    // This is deliberately removed as the first step of shutdown. All public
    // JazzClient facades share this inner state, so a retained clone cannot
    // keep the local storage handle alive or resume work after shutdown.
    db: Option<Backend>,
    identity: CoreDbIdentity,
    connect_config: Option<ConnectConfig>,
    scheduler: Rc<TickSchedulerImpl>,
    upstream: Option<BackendConnection>,
    write_map: HashMap<TransactionId, CoreTxId>,
    row_tables: HashMap<ObjectId, String>,
    transactions: HashMap<OpenTransactionId, ExclusiveTransactionState>,
    closed_transactions: HashMap<OpenTransactionId, ClosedTransactionState>,
    tick_driver_error: Option<String>,
    tick_driver_error_notify: Arc<tokio::sync::Notify>,
    subscription_forwarders: HashMap<u64, SubscriptionForwarder>,
    next_subscription_forwarder: u64,
    shutdown_state: ShutdownState,
    shutdown_notify: Arc<tokio::sync::Notify>,
}

/// A public subscription admission or forwarding task owned by the facade.
///
/// Shutdown closes admission through `cancellation` and waits for `completion`
/// before releasing the core database. This keeps a retained public stream from
/// retaining a core stream (and therefore persistent storage) past shutdown.
struct SubscriptionForwarder {
    cancellation: oneshot::Sender<()>,
    completion: oneshot::Receiver<()>,
}

/// Completes a forwarding admission even when its async future returns early.
struct SubscriptionForwarderCompletion(Option<oneshot::Sender<()>>);

impl SubscriptionForwarderCompletion {
    fn new(sender: oneshot::Sender<()>) -> Self {
        Self(Some(sender))
    }
}

impl Drop for SubscriptionForwarderCompletion {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[derive(Debug)]
enum ShutdownState {
    Open,
    Closing,
    Closed,
    Failed(String),
}

/// Completes a shared terminal shutdown even when its caller is cancelled.
///
/// Cancellation cannot safely resume a `Db::close` future after its
/// finalization admission has begun.  In that case the backend is dropped and
/// every shared client facade remains terminal rather than being allowed to
/// reconnect against a partially closed core.
struct ShutdownCompletion {
    inner: Rc<RefCell<ClientDbInner>>,
    backend: Backend,
    finished: bool,
}

impl ShutdownCompletion {
    fn new(inner: Rc<RefCell<ClientDbInner>>, backend: Backend) -> Self {
        Self {
            inner,
            backend,
            finished: false,
        }
    }

    fn finish(&mut self, result: std::result::Result<(), String>) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        inner.shutdown_state = match result {
            Ok(()) => ShutdownState::Closed,
            Err(error) => ShutdownState::Failed(error),
        };
        inner.shutdown_notify.notify_waiters();
        self.finished = true;
        match &inner.shutdown_state {
            ShutdownState::Closed => Ok(()),
            ShutdownState::Failed(error) => Err(JazzError::Connection(format!(
                "client shutdown failed: {error}"
            ))),
            ShutdownState::Open | ShutdownState::Closing => unreachable!("shutdown completed"),
        }
    }
}

impl Drop for ShutdownCompletion {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut inner = self.inner.borrow_mut();
        inner.shutdown_state = ShutdownState::Failed(
            "shutdown was cancelled before the storage close completed".to_string(),
        );
        inner.shutdown_notify.notify_waiters();
        // `backend` drops with this guard, releasing storage resources without
        // advertising an unfinished context as usable.
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TickDriverErrorClass {
    Retry,
    Reconnect,
    Fatal,
}

fn classify_tick_driver_error(error: &CoreDbError) -> TickDriverErrorClass {
    match error.code {
        CoreDbErrorCode::Backpressure => TickDriverErrorClass::Retry,
        CoreDbErrorCode::Protocol if error.message == "websocket pump is closed" => {
            TickDriverErrorClass::Reconnect
        }
        _ => TickDriverErrorClass::Fatal,
    }
}

fn tick_driver_retry_delay(attempt: u32) -> Duration {
    let multiplier = 1u32 << attempt.saturating_sub(1).min(5);
    TICK_DRIVER_RETRY_BASE_DELAY
        .checked_mul(multiplier)
        .unwrap_or(TICK_DRIVER_RETRY_MAX_DELAY)
        .min(TICK_DRIVER_RETRY_MAX_DELAY)
}

async fn recover_tick_driver_error(
    inner: &Rc<RefCell<ClientDbInner>>,
    scheduler: &TickSchedulerImpl,
    class: TickDriverErrorClass,
    error: &CoreDbError,
    attempts: &mut u32,
) -> bool {
    *attempts = attempts.saturating_add(1);
    if *attempts > MAX_TICK_DRIVER_RECOVERY_ATTEMPTS {
        inner.borrow_mut().record_tick_driver_failure(format!(
            "recovery exhausted after {MAX_TICK_DRIVER_RECOVERY_ATTEMPTS} attempts for {error}"
        ));
        return false;
    }

    #[cfg(feature = "sync-autopsy")]
    crate::db::sync_autopsy::record(format!(
        "client tick driver retrying {class:?} error attempt {attempts}: {error}"
    ));
    tokio::time::sleep(tick_driver_retry_delay(*attempts)).await;

    if class == TickDriverErrorClass::Reconnect {
        inner.borrow_mut().disconnect_upstream();
        if let Err(_reconnect_error) = ClientDbInner::reconnect_upstream(inner).await {
            #[cfg(feature = "sync-autopsy")]
            crate::db::sync_autopsy::record(format!(
                "client tick driver reconnect attempt {attempts} failed: {_reconnect_error}"
            ));
        }
    }
    scheduler.wake(TickUrgency::Immediate);
    true
}

#[derive(Clone)]
struct ConnectConfig {
    server_url: String,
    app_id: crate::tools::AppId,
    auth: WsAuthConfig,
    connector: Arc<dyn NativeTransportConnector>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClosedTransactionState {
    Committed,
    RolledBack,
}

#[derive(Clone)]
struct Backend(Rc<CoreClientDb>);

#[cfg(test)]
thread_local! {
    static COMPLETED_BACKEND_CLOSES: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn completed_backend_close_count() -> usize {
    COMPLETED_BACKEND_CLOSES.with(Cell::get)
}

impl Backend {
    async fn open(
        schema: crate::schema::JazzSchema,
        storage: StorageBundle,
        identity: CoreDbIdentity,
    ) -> Result<Self> {
        Ok(Self(Rc::new(
            StackSafeFuture::new(CoreDb::open(CoreDbConfig::new(schema, storage, identity)))
                .await
                .map_err(|error| JazzError::Connection(error.to_string()))?,
        )))
    }

    fn set_tick_scheduler(&self, scheduler: Rc<TickSchedulerImpl>) {
        self.0.set_tick_scheduler(Some(scheduler));
    }

    async fn close(&self) -> Result<()> {
        StackSafeFuture::new(self.0.close())
            .await
            .map_err(|error| JazzError::Connection(error.to_string()))?;
        #[cfg(test)]
        COMPLETED_BACKEND_CLOSES.with(|count| count.set(count.get() + 1));
        Ok(())
    }

    async fn connect_upstream(&self, transport: Box<dyn CoreTransport>) -> BackendConnection {
        StackSafeFuture::new(self.0.connect_upstream(transport)).await
    }

    fn detach_connection(&self, connection: &BackendConnection) -> bool {
        self.0.detach_connection(connection)
    }

    fn set_identity_claims(&self, identity: CoreAuthorSubject, claims: HashMap<String, CoreValue>) {
        self.0
            .set_identity_claims(identity, claims.into_iter().collect());
    }

    async fn tick(&self) -> std::result::Result<(), CoreDbError> {
        StackSafeFuture::new(self.0.tick()).await
    }

    fn insert(
        &self,
        table: &str,
        cells: crate::db::RowCells,
    ) -> std::result::Result<(CoreRowUuid, CoreTxId), CoreDbError> {
        let write = crate::db::block_on(self.0.insert(table, cells, Default::default()))?;
        Ok((write.row_uuid(), write.mergeable_tx_id()))
    }

    fn insert_for_identity(
        &self,
        identity: CoreAuthorSubject,
        table: &str,
        cells: crate::db::RowCells,
    ) -> std::result::Result<(CoreRowUuid, CoreTxId), CoreDbError> {
        let write = crate::db::block_on(self.0.insert(
            table,
            cells,
            crate::db::InsertOptions {
                identity: crate::db::WriteIdentity::Session(identity),
                ..Default::default()
            },
        ))?;
        Ok((write.row_uuid(), write.mergeable_tx_id()))
    }

    fn insert_with_id(
        &self,
        table: &str,
        row_id: CoreRowUuid,
        cells: crate::db::RowCells,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        Ok(crate::db::block_on(self.0.insert(
            table,
            cells,
            crate::db::InsertOptions {
                row_id: Some(row_id),
                ..Default::default()
            },
        ))?
        .mergeable_tx_id())
    }

    fn insert_with_id_for_identity(
        &self,
        identity: CoreAuthorSubject,
        table: &str,
        row_id: CoreRowUuid,
        cells: crate::db::RowCells,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        Ok(crate::db::block_on(self.0.insert(
            table,
            cells,
            crate::db::InsertOptions {
                row_id: Some(row_id),
                identity: crate::db::WriteIdentity::Session(identity),
                ..Default::default()
            },
        ))?
        .mergeable_tx_id())
    }

    fn upsert(
        &self,
        table: &str,
        row_id: CoreRowUuid,
        cells: crate::db::RowCells,
        updated_at_ms: Option<u64>,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        Ok(crate::db::block_on(self.0.upsert(
            table,
            row_id,
            cells,
            crate::db::UpsertOptions {
                updated_at_ms,
                ..Default::default()
            },
        ))?
        .mergeable_tx_id())
    }

    fn upsert_for_identity(
        &self,
        identity: CoreAuthorSubject,
        table: &str,
        row_id: CoreRowUuid,
        cells: crate::db::RowCells,
        updated_at_ms: Option<u64>,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        Ok(crate::db::block_on(self.0.upsert(
            table,
            row_id,
            cells,
            crate::db::UpsertOptions {
                identity: crate::db::WriteIdentity::Session(identity),
                updated_at_ms,
                ..Default::default()
            },
        ))?
        .mergeable_tx_id())
    }

    fn update(
        &self,
        table: &str,
        row_id: CoreRowUuid,
        cells: crate::db::RowCells,
        updated_at_ms: Option<u64>,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        Ok(crate::db::block_on(self.0.update(
            table,
            row_id,
            cells,
            crate::db::UpdateOptions {
                updated_at_ms,
                ..Default::default()
            },
        ))?
        .mergeable_tx_id())
    }

    fn delete_for_identity(
        &self,
        identity: CoreAuthorSubject,
        table: &str,
        row_id: CoreRowUuid,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        Ok(crate::db::block_on(self.0.delete(
            table,
            row_id,
            crate::db::DeleteOptions {
                identity: crate::db::WriteIdentity::Session(identity),
                ..Default::default()
            },
        ))?
        .mergeable_tx_id())
    }

    fn delete(
        &self,
        table: &str,
        row_id: CoreRowUuid,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        Ok(
            crate::db::block_on(self.0.delete(table, row_id, Default::default()))?
                .mergeable_tx_id(),
        )
    }

    fn prepare_query(
        &self,
        query: &crate::query::Query,
    ) -> std::result::Result<crate::db::PreparedQuery, CoreDbError> {
        self.0.prepare_query_for_open_schema(query)
    }

    fn row_provenance(
        &self,
        row: &crate::node::CurrentRow,
    ) -> std::result::Result<Option<crate::node::RowProvenance>, CoreDbError> {
        self.0.row_provenance(row)
    }

    async fn row_provenance_for_subscription(
        &self,
        row: &crate::node::CurrentRow,
    ) -> std::result::Result<Option<crate::node::RowProvenance>, CoreDbError> {
        StackSafeFuture::new(self.0.row_provenance_async(row)).await
    }

    async fn all(
        &self,
        prepared: &crate::db::PreparedQuery,
        opts: CoreReadOpts,
    ) -> std::result::Result<Vec<crate::node::CurrentRow>, CoreDbError> {
        StackSafeFuture::new(self.0.all(prepared, opts)).await
    }

    async fn transaction_all_for_identity(
        &self,
        tx_id: OpenTransactionId,
        prepared: &crate::db::PreparedQuery,
        author: CoreAuthorSubject,
        opts: CoreReadOpts,
    ) -> std::result::Result<Vec<crate::node::CurrentRow>, CoreDbError> {
        StackSafeFuture::new(
            self.0
                .transaction_all_for_identity(tx_id, prepared, author, opts),
        )
        .await
    }

    async fn subscribe(
        &self,
        prepared: &crate::db::PreparedQuery,
        opts: CoreReadOpts,
    ) -> std::result::Result<crate::db::SubscriptionStream, CoreDbError> {
        StackSafeFuture::new(self.0.subscribe(prepared, opts)).await
    }

    fn write_state(
        &self,
        tx_id: CoreTxId,
    ) -> std::result::Result<crate::db::WriteState, CoreDbError> {
        self.0.write_state(tx_id)
    }

    async fn next_write_state_change(&self, tx_id: CoreTxId) {
        StackSafeFuture::new(self.0.next_write_state_change(tx_id)).await;
    }

    fn begin_exclusive(&self, id: OpenTransactionId) -> std::result::Result<(), CoreDbError> {
        crate::db::block_on(self.0.begin_exclusive(id))
    }

    fn begin_exclusive_for_identity(
        &self,
        id: OpenTransactionId,
        author: CoreAuthorSubject,
    ) -> std::result::Result<(), CoreDbError> {
        crate::db::block_on(self.0.begin_exclusive_for_identity(id, author))
    }

    fn exclusive_write(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row_id: CoreRowUuid,
        cells: crate::db::RowCells,
    ) -> std::result::Result<(), CoreDbError> {
        crate::db::block_on(self.0.exclusive_tx_ref(tx_id).insert(
            table,
            cells,
            crate::db::InsertOptions {
                row_id: Some(row_id),
                ..Default::default()
            },
        ))
        .map(|_| ())
    }

    fn exclusive_update(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row_id: CoreRowUuid,
        cells: crate::db::RowCells,
    ) -> std::result::Result<(), CoreDbError> {
        crate::db::block_on(self.0.exclusive_tx_ref(tx_id).update(
            table,
            row_id,
            cells,
            Default::default(),
        ))
    }

    fn exclusive_delete(
        &self,
        tx_id: OpenTransactionId,
        table: &str,
        row_id: CoreRowUuid,
    ) -> std::result::Result<(), CoreDbError> {
        crate::db::block_on(self.0.exclusive_tx_ref(tx_id).delete(
            table,
            row_id,
            Default::default(),
        ))
    }

    fn commit_exclusive_handle(
        &self,
        tx_id: OpenTransactionId,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        crate::db::block_on(self.0.commit_exclusive_handle(tx_id))
    }

    fn commit_exclusive_handle_for_identity(
        &self,
        tx_id: OpenTransactionId,
        author: CoreAuthorSubject,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        crate::db::block_on(self.0.commit_exclusive_handle_for_identity(tx_id, author))
    }
}

struct ExclusiveTransactionState {
    author: Option<CoreAuthorSubject>,
    writes: Vec<ExclusiveTransactionWrite>,
}

struct ExclusiveTransactionWrite {
    table: String,
    row_id: ObjectId,
}

#[derive(Default)]
struct TickSchedulerImpl {
    state: Arc<TickState>,
}

#[derive(Default)]
struct TickState {
    immediate: AtomicBool,
    deferred: AtomicBool,
    after_current_turn: AtomicBool,
    delayed: AtomicBool,
    notify: tokio::sync::Notify,
}

/// Thread-safe wake bridge retained by cold Groove storage futures.
///
/// It does not poll the database itself. It only records one Immediate owner
/// turn when an actually pending operation becomes ready.
struct QueryRuntimeWake {
    state: Arc<TickState>,
}

impl ArcWake for QueryRuntimeWake {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.state.immediate.store(true, Ordering::Release);
        arc_self.state.notify.notify_one();
    }
}

impl TickSchedulerImpl {
    fn take(&self) -> Option<TickUrgency> {
        if self.state.immediate.swap(false, Ordering::AcqRel) {
            self.state.deferred.store(false, Ordering::Release);
            self.state
                .after_current_turn
                .store(false, Ordering::Release);
            Some(TickUrgency::Immediate)
        } else if self.state.after_current_turn.swap(false, Ordering::AcqRel) {
            Some(TickUrgency::AfterCurrentTurn)
        } else if self.state.deferred.swap(false, Ordering::AcqRel) {
            Some(TickUrgency::Deferred)
        } else {
            None
        }
    }

    fn wake(&self, urgency: TickUrgency) {
        match urgency {
            TickUrgency::Immediate => self.state.immediate.store(true, Ordering::Release),
            TickUrgency::Deferred => self.state.deferred.store(true, Ordering::Release),
            TickUrgency::AfterCurrentTurn => {
                self.state.after_current_turn.store(true, Ordering::Release)
            }
        }
        self.state.notify.notify_one();
    }

    fn wake_handle(&self) -> Arc<TickState> {
        Arc::clone(&self.state)
    }

    fn wake_after(&self, delay_ms: u64) {
        // One delayed wake is enough to service all currently rate-limited
        // uploads. The protocol has no receiver-supplied retry-after, so every
        // caller uses the same bounded default admission window.
        if self.state.delayed.swap(true, Ordering::AcqRel) {
            return;
        }
        let state = Arc::clone(&self.state);
        tokio::task::spawn_local(async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            state.delayed.store(false, Ordering::Release);
            state.deferred.store(true, Ordering::Release);
            state.notify.notify_one();
        });
    }
}

impl TickScheduler for TickSchedulerImpl {
    fn schedule_tick(&self, urgency: TickUrgency) {
        self.wake(urgency);
    }

    fn schedule_tick_after(&self, delay_ms: u64) {
        self.wake_after(delay_ms);
    }

    fn query_runtime_waker(&self) -> Option<std::task::Waker> {
        Some(waker(Arc::new(QueryRuntimeWake {
            state: Arc::clone(&self.state),
        })))
    }
}

impl ClientDb {
    async fn open(
        schema: crate::schema::JazzSchema,
        public_schema: Schema,
        storage: StorageBundle,
        identity: CoreDbIdentity,
        server_url: Option<String>,
        app_id: crate::tools::AppId,
        auth: Option<WsAuthConfig>,
        connector: Option<Arc<dyn NativeTransportConnector>>,
    ) -> Result<Rc<Self>> {
        let scheduler = Rc::new(TickSchedulerImpl::default());
        let has_upstream = server_url.is_some();
        let inner = ClientDbInner::open(
            schema,
            storage,
            identity,
            server_url,
            app_id,
            auth,
            connector,
            Rc::clone(&scheduler),
        )
        .await?;
        let inner = Rc::new(RefCell::new(inner));
        if has_upstream {
            Self::spawn_local_tick_driver(Rc::downgrade(&inner), Rc::clone(&scheduler));
        }
        Ok(Rc::new(Self {
            inner,
            query_decoder: PublicQueryDecoder {
                schema: Rc::new(public_schema),
            },
        }))
    }

    async fn close(&self) -> Result<()> {
        let (backend, forwarders) = {
            let mut inner = self.inner.borrow_mut();
            match inner.shutdown_state {
                ShutdownState::Open => {
                    inner.disconnect_upstream();
                    inner.connect_config = None;
                    inner.tick_driver_error = Some("client shut down".to_string());
                    inner.tick_driver_error_notify.notify_waiters();
                    inner.shutdown_state = ShutdownState::Closing;
                    let backend = inner.db.take().ok_or_else(ClientDbInner::shutdown_error)?;
                    let forwarders = std::mem::take(&mut inner.subscription_forwarders);
                    (backend, forwarders)
                }
                ShutdownState::Closing | ShutdownState::Closed => {
                    drop(inner);
                    return self.wait_for_shutdown().await;
                }
                ShutdownState::Failed(ref error) => {
                    return Err(JazzError::Connection(format!(
                        "client shutdown previously failed: {error}"
                    )));
                }
            }
        };
        // Install the cancellation guard before the first await: forwarder
        // draining is part of shutdown, and cancellation there must still
        // leave every retained facade terminal and wake concurrent shutdowns.
        let mut completion = ShutdownCompletion::new(Rc::clone(&self.inner), backend);
        let mut completions = Vec::with_capacity(forwarders.len());
        for SubscriptionForwarder {
            cancellation,
            completion,
        } in forwarders.into_values()
        {
            let _ = cancellation.send(());
            completions.push(completion);
        }
        for completion in completions {
            let _ = completion.await;
        }
        let result = completion
            .backend
            .close()
            .await
            .map_err(|error| error.to_string());
        completion.finish(result)
    }

    async fn wait_for_shutdown(&self) -> Result<()> {
        loop {
            let notified = {
                let inner = self.inner.borrow();
                match &inner.shutdown_state {
                    ShutdownState::Closed => return Ok(()),
                    ShutdownState::Failed(error) => {
                        return Err(JazzError::Connection(format!(
                            "client shutdown failed: {error}"
                        )));
                    }
                    ShutdownState::Open => {
                        return Err(ClientDbInner::shutdown_error());
                    }
                    ShutdownState::Closing => Arc::clone(&inner.shutdown_notify).notified_owned(),
                }
            };
            notified.await;
        }
    }

    async fn query_rows(
        &self,
        query: crate::query::Query,
        opts: CoreReadOpts,
        table: String,
        wait_for_coverage: bool,
    ) -> Result<Vec<crate::node::CurrentRow>> {
        self.ensure_tick_driver_running()?;
        ClientDbInner::handle_query(&self.inner, query, opts, table, wait_for_coverage).await
    }

    async fn query_transaction_rows(
        &self,
        query: crate::query::Query,
        opts: CoreReadOpts,
        transaction_id: OpenTransactionId,
        table: String,
        author: CoreAuthorSubject,
    ) -> Result<Vec<crate::node::CurrentRow>> {
        let prepared = {
            let inner = self.inner.borrow();
            inner
                .backend()?
                .prepare_query(&query)
                .map_err(|error| JazzError::Query(error.to_string()))?
        };
        let backend = {
            let inner = self.inner.borrow();
            inner.ensure_transaction_open(transaction_id)?;
            inner.backend_clone()?
        };
        let rows = backend
            .transaction_all_for_identity(transaction_id, &prepared, author, opts)
            .await
            .map_err(|error| JazzError::Query(error.to_string()))?;
        self.inner.borrow_mut().remember_rows(&table, &rows);
        Ok(rows)
    }

    async fn subscribe(
        &self,
        query: crate::query::Query,
        opts: CoreReadOpts,
        table: String,
        tx: mpsc::UnboundedSender<SubscriptionStreamItem>,
        cancellation: oneshot::Receiver<()>,
    ) -> Result<()> {
        self.ensure_tick_driver_running()?;
        ClientDbInner::handle_subscribe(
            &self.inner,
            self.query_decoder.clone(),
            query,
            opts,
            table,
            tx,
            cancellation,
        )
        .await
    }

    fn insert(
        &self,
        table: String,
        row_id: Option<Uuid>,
        cells: crate::db::RowCells,
        identity: Option<CoreAuthorSubject>,
    ) -> Result<(ObjectId, CoreTxId)> {
        let mut inner = self.inner.borrow_mut();
        let (row_uuid, tx_id) = match row_id {
            Some(uuid) => {
                let row_uuid = CoreRowUuid(uuid);
                let tx_id = match identity {
                    Some(identity) => inner
                        .backend()?
                        .insert_with_id_for_identity(identity, &table, row_uuid, cells),
                    None => inner.backend()?.insert_with_id(&table, row_uuid, cells),
                }
                .map_err(|error| JazzError::Write(error.to_string()))?;
                (row_uuid, tx_id)
            }
            None => {
                if let Some(identity) = identity {
                    inner
                        .backend()?
                        .insert_for_identity(identity, &table, cells)
                        .map_err(|error| JazzError::Write(error.to_string()))?
                } else {
                    inner
                        .backend()?
                        .insert(&table, cells)
                        .map_err(|error| JazzError::Write(error.to_string()))?
                }
            }
        };
        JazzClient::check_core_write_not_rejected(inner.backend()?, tx_id)?;
        let object_id = ObjectId::from_uuid(row_uuid.0);
        inner.remember_write(object_id, &table, tx_id);
        Ok((object_id, tx_id))
    }

    fn stage_insert(
        &self,
        transaction_id: OpenTransactionId,
        table: String,
        row_id: Option<Uuid>,
        cells: crate::db::RowCells,
    ) -> Result<ObjectId> {
        let mut inner = self.inner.borrow_mut();
        let row_id = ObjectId::from_uuid(row_id.unwrap_or_else(Uuid::now_v7));
        inner.ensure_transaction_open(transaction_id)?;
        let tx_id = transaction_id;
        inner
            .backend()?
            .exclusive_write(tx_id, &table, CoreRowUuid(*row_id.uuid()), cells.clone())
            .map_err(|error| JazzError::Write(error.to_string()))?;
        let tx = inner
            .transactions
            .get_mut(&transaction_id)
            .expect("transaction open checked above");
        tx.writes.push(ExclusiveTransactionWrite {
            table: table.clone(),
            row_id,
        });
        inner.row_tables.insert(row_id, table);
        Ok(row_id)
    }

    fn upsert(
        &self,
        table: String,
        row_id: Uuid,
        cells: crate::db::RowCells,
        identity: Option<CoreAuthorSubject>,
        updated_at_ms: Option<u64>,
    ) -> Result<CoreTxId> {
        let mut inner = self.inner.borrow_mut();
        let write = match identity {
            Some(identity) => inner.backend()?.upsert_for_identity(
                identity,
                &table,
                CoreRowUuid(row_id),
                cells,
                updated_at_ms,
            ),
            None => inner
                .backend()?
                .upsert(&table, CoreRowUuid(row_id), cells, updated_at_ms),
        }
        .map_err(|error| JazzError::Write(error.to_string()))?;
        JazzClient::check_core_write_not_rejected(inner.backend()?, write)?;
        let object_id = ObjectId::from_uuid(row_id);
        inner.remember_write(object_id, &table, write);
        let tx_id = write;
        Ok(tx_id)
    }

    fn stage_upsert(
        &self,
        transaction_id: OpenTransactionId,
        table: String,
        row_id: Uuid,
        cells: crate::db::RowCells,
    ) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let object_id = ObjectId::from_uuid(row_id);
        inner.ensure_transaction_open(transaction_id)?;
        let tx_id = transaction_id;
        inner
            .backend()?
            .exclusive_write(tx_id, &table, CoreRowUuid(row_id), cells.clone())
            .map_err(|error| JazzError::Write(error.to_string()))?;
        let tx = inner
            .transactions
            .get_mut(&transaction_id)
            .expect("transaction open checked above");
        tx.writes.push(ExclusiveTransactionWrite {
            table: table.clone(),
            row_id: object_id,
        });
        inner.row_tables.insert(object_id, table);
        Ok(())
    }

    fn update(
        &self,
        row_id: ObjectId,
        cells: crate::db::RowCells,
        identity: Option<CoreAuthorSubject>,
        updated_at_ms: Option<u64>,
    ) -> Result<CoreTxId> {
        let mut inner = self.inner.borrow_mut();
        let table = inner.row_tables.get(&row_id).cloned().ok_or_else(|| {
            JazzError::Write("update requires a row created or observed by this client".to_string())
        })?;
        let write = match identity {
            Some(identity) => inner.backend()?.upsert_for_identity(
                identity,
                &table,
                CoreRowUuid(*row_id.uuid()),
                cells,
                updated_at_ms,
            ),
            None => {
                inner
                    .backend()?
                    .update(&table, CoreRowUuid(*row_id.uuid()), cells, updated_at_ms)
            }
        }
        .map_err(|error| JazzError::Write(error.to_string()))?;
        JazzClient::check_core_write_not_rejected(inner.backend()?, write)?;
        inner.remember_write(row_id, &table, write);
        let tx_id = write;
        Ok(tx_id)
    }

    fn stage_update(
        &self,
        transaction_id: OpenTransactionId,
        row_id: ObjectId,
        cells: crate::db::RowCells,
    ) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let table = inner.row_tables.get(&row_id).cloned().ok_or_else(|| {
            JazzError::Write("update requires a row created or observed by this client".to_string())
        })?;
        inner.ensure_transaction_open(transaction_id)?;
        let tx_id = transaction_id;
        inner
            .backend()?
            .exclusive_update(tx_id, &table, CoreRowUuid(*row_id.uuid()), cells.clone())
            .map_err(|error| JazzError::Write(error.to_string()))?;
        let tx = inner
            .transactions
            .get_mut(&transaction_id)
            .expect("transaction open checked above");
        tx.writes.push(ExclusiveTransactionWrite { table, row_id });
        Ok(())
    }

    fn delete(&self, row_id: ObjectId, identity: Option<CoreAuthorSubject>) -> Result<CoreTxId> {
        let mut inner = self.inner.borrow_mut();
        let table = inner.row_tables.get(&row_id).cloned().ok_or_else(|| {
            JazzError::Write("delete requires a row created or observed by this client".to_string())
        })?;
        let write = match identity {
            Some(identity) => {
                inner
                    .backend()?
                    .delete_for_identity(identity, &table, CoreRowUuid(*row_id.uuid()))
            }
            None => inner.backend()?.delete(&table, CoreRowUuid(*row_id.uuid())),
        }
        .map_err(|error| JazzError::Write(error.to_string()))?;
        JazzClient::check_core_write_not_rejected(inner.backend()?, write)?;
        inner.remember_write(row_id, &table, write);
        let tx_id = write;
        Ok(tx_id)
    }

    fn stage_delete(&self, transaction_id: OpenTransactionId, row_id: ObjectId) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let table = inner.row_tables.get(&row_id).cloned().ok_or_else(|| {
            JazzError::Write("delete requires a row created or observed by this client".to_string())
        })?;
        inner.ensure_transaction_open(transaction_id)?;
        let tx_id = transaction_id;
        inner
            .backend()?
            .exclusive_delete(tx_id, &table, CoreRowUuid(*row_id.uuid()))
            .map_err(|error| JazzError::Write(error.to_string()))?;
        let tx = inner
            .transactions
            .get_mut(&transaction_id)
            .expect("transaction open checked above");
        tx.writes.push(ExclusiveTransactionWrite { table, row_id });
        Ok(())
    }

    fn begin_transaction(&self, author: Option<CoreAuthorSubject>) -> Result<OpenTransactionId> {
        let mut inner = self.inner.borrow_mut();
        let mut transaction_id = OpenTransactionId::new();
        while inner.transactions.contains_key(&transaction_id)
            || inner.closed_transactions.contains_key(&transaction_id)
        {
            transaction_id = OpenTransactionId::new();
        }
        match author {
            Some(author) => inner
                .backend()?
                .begin_exclusive_for_identity(transaction_id, author),
            None => inner.backend()?.begin_exclusive(transaction_id),
        }
        .map_err(|error| JazzError::Write(error.to_string()))?;
        inner.transactions.insert(
            transaction_id,
            ExclusiveTransactionState {
                author,
                writes: Vec::new(),
            },
        );
        Ok(transaction_id)
    }

    fn commit_transaction(&self, transaction_id: OpenTransactionId) -> Result<TransactionId> {
        let mut inner = self.inner.borrow_mut();
        inner.ensure_transaction_open(transaction_id)?;
        if inner
            .transactions
            .get(&transaction_id)
            .expect("transaction open checked above")
            .writes
            .is_empty()
        {
            return Err(JazzError::Write(
                "transaction cannot commit without writes".to_string(),
            ));
        }
        let state = inner
            .transactions
            .remove(&transaction_id)
            .expect("transaction open checked above");
        let tx_id = match state.author {
            Some(author) => inner
                .backend()?
                .commit_exclusive_handle_for_identity(transaction_id, author),
            None => inner.backend()?.commit_exclusive_handle(transaction_id),
        }
        .map_err(|error| JazzError::Write(error.to_string()))?;
        let committed_id = core_batch_id(tx_id);
        inner.write_map.insert(committed_id, tx_id);
        for write in state.writes {
            inner.row_tables.insert(write.row_id, write.table);
        }
        inner
            .closed_transactions
            .insert(transaction_id, ClosedTransactionState::Committed);
        Ok(committed_id)
    }

    fn rollback_transaction(&self, transaction_id: OpenTransactionId) -> Result<bool> {
        let mut inner = self.inner.borrow_mut();
        inner.backend()?;
        inner.ensure_transaction_open(transaction_id)?;
        let removed = inner.transactions.remove(&transaction_id).is_some();
        if removed {
            inner
                .closed_transactions
                .insert(transaction_id, ClosedTransactionState::RolledBack);
        }
        Ok(removed)
    }

    async fn wait_for_transaction(
        &self,
        transaction_id: TransactionId,
        tier: DurabilityTier,
    ) -> Result<()> {
        self.ensure_tick_driver_running()?;
        ClientDbInner::handle_wait_for_transaction(
            &self.inner,
            transaction_id,
            tier,
            Duration::from_secs(25),
        )
        .await
    }

    #[cfg(feature = "testing")]
    async fn reconnect_upstream(&self) -> Result<bool> {
        ClientDbInner::reconnect_upstream(&self.inner).await
    }

    #[cfg(feature = "testing")]
    fn disconnect_upstream(&self) -> bool {
        self.inner.borrow_mut().disconnect_upstream()
    }

    fn ensure_tick_driver_running(&self) -> Result<()> {
        self.inner.borrow().ensure_tick_driver_running()
    }

    fn spawn_local_tick_driver(
        inner: Weak<RefCell<ClientDbInner>>,
        scheduler: Rc<TickSchedulerImpl>,
    ) {
        let state = scheduler.wake_handle();
        tokio::task::spawn_local(async move {
            let mut recovery_attempts = 0;
            loop {
                state.notify.notified().await;
                while let Some(urgency) = scheduler.take() {
                    let Some(inner) = inner.upgrade() else {
                        return;
                    };
                    if urgency == TickUrgency::Deferred {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    } else if urgency == TickUrgency::AfterCurrentTurn {
                        tokio::task::yield_now().await;
                    }
                    let backend = match inner.borrow().backend_clone() {
                        Ok(db) => db,
                        Err(_) => return,
                    };
                    let tick_result = backend.tick().await;
                    match tick_result {
                        Ok(()) => recovery_attempts = 0,
                        Err(error) => {
                            let class = classify_tick_driver_error(&error);
                            let should_exit = if class == TickDriverErrorClass::Fatal {
                                inner
                                    .borrow_mut()
                                    .record_tick_driver_failure(error.to_string());
                                true
                            } else {
                                !recover_tick_driver_error(
                                    &inner,
                                    &scheduler,
                                    class,
                                    &error,
                                    &mut recovery_attempts,
                                )
                                .await
                            };
                            if should_exit {
                                #[cfg(feature = "sync-autopsy")]
                                crate::db::sync_autopsy::record(format!(
                                    "client tick driver exited after db.tick error: {error}"
                                ));
                                return;
                            }
                        }
                    }
                }
            }
        });
    }
}

impl ClientDbInner {
    fn shutdown_error() -> JazzError {
        JazzError::Connection("client is shut down".to_string())
    }

    fn backend(&self) -> Result<&Backend> {
        self.db.as_ref().ok_or_else(Self::shutdown_error)
    }

    fn backend_clone(&self) -> Result<Backend> {
        Ok(self.backend()?.clone())
    }

    fn disconnect_upstream(&mut self) -> bool {
        let Some(connection) = self.upstream.take() else {
            return false;
        };
        self.db
            .as_ref()
            .is_some_and(|db| db.detach_connection(&connection))
    }

    fn ensure_tick_driver_running(&self) -> Result<()> {
        if !matches!(self.shutdown_state, ShutdownState::Open) {
            return Err(Self::shutdown_error());
        }
        match &self.tick_driver_error {
            Some(error) => Err(JazzError::Sync(format!(
                "client tick driver stopped: {error}"
            ))),
            None => Ok(()),
        }
    }

    fn admit_subscription(
        &mut self,
    ) -> Result<(oneshot::Receiver<()>, SubscriptionForwarderCompletion)> {
        self.ensure_tick_driver_running()?;
        self.subscription_forwarders.retain(|_, forwarder| {
            matches!(
                forwarder.completion.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            )
        });
        let id = self.next_subscription_forwarder;
        self.next_subscription_forwarder = self.next_subscription_forwarder.wrapping_add(1);
        let (cancellation, cancellation_rx) = oneshot::channel();
        let (completion, completion_rx) = oneshot::channel();
        self.subscription_forwarders.insert(
            id,
            SubscriptionForwarder {
                cancellation,
                completion: completion_rx,
            },
        );
        Ok((
            cancellation_rx,
            SubscriptionForwarderCompletion::new(completion),
        ))
    }

    fn record_tick_driver_failure(&mut self, error: String) {
        self.tick_driver_error = Some(error);
        self.tick_driver_error_notify.notify_waiters();
    }

    async fn open(
        schema: crate::schema::JazzSchema,
        storage: StorageBundle,
        identity: CoreDbIdentity,
        server_url: Option<String>,
        app_id: crate::tools::AppId,
        auth: Option<WsAuthConfig>,
        connector: Option<Arc<dyn NativeTransportConnector>>,
        scheduler: Rc<TickSchedulerImpl>,
    ) -> Result<Self> {
        let db = Backend::open(schema, storage, identity).await?;
        db.set_tick_scheduler(scheduler.clone());
        let connect_config = if let Some(server_url) = server_url {
            let auth = auth.ok_or_else(|| {
                JazzError::Connection("server connection missing auth config".to_string())
            })?;
            Some(ConnectConfig {
                server_url,
                app_id,
                auth,
                connector: connector.ok_or_else(|| {
                    JazzError::Connection(
                        "server connection missing native transport connector".to_owned(),
                    )
                })?,
            })
        } else {
            None
        };
        let mut inner = Self {
            db: Some(db),
            identity,
            connect_config,
            scheduler,
            upstream: None,
            write_map: HashMap::new(),
            row_tables: HashMap::new(),
            transactions: HashMap::new(),
            closed_transactions: HashMap::new(),
            tick_driver_error: None,
            tick_driver_error_notify: Arc::new(tokio::sync::Notify::new()),
            subscription_forwarders: HashMap::new(),
            next_subscription_forwarder: 0,
            shutdown_state: ShutdownState::Open,
            shutdown_notify: Arc::new(tokio::sync::Notify::new()),
        };
        inner.connect_upstream_transport().await?;
        Ok(inner)
    }

    async fn reconnect_upstream(inner: &Rc<RefCell<Self>>) -> Result<bool> {
        let (db, identity, scheduler, config) = {
            let inner = inner.borrow();
            if !matches!(inner.shutdown_state, ShutdownState::Open) {
                return Err(Self::shutdown_error());
            }
            if inner.upstream.is_some() {
                return Ok(false);
            }
            let Some(config) = inner.connect_config.clone() else {
                return Ok(false);
            };
            (
                inner.backend_clone()?,
                inner.identity,
                Rc::clone(&inner.scheduler),
                config,
            )
        };
        let connection = Self::connect_with_config(&db, identity, scheduler, config).await?;
        let mut inner = inner.borrow_mut();
        if !matches!(inner.shutdown_state, ShutdownState::Open) || inner.upstream.is_some() {
            db.detach_connection(&connection);
            return Ok(false);
        }
        inner.upstream = Some(connection);
        Ok(true)
    }

    async fn connect_upstream_transport(&mut self) -> Result<()> {
        if self.upstream.is_some() {
            return Ok(());
        }
        let Some(config) = self.connect_config.clone() else {
            return Ok(());
        };
        self.upstream = Some(
            Self::connect_with_config(
                self.backend()?,
                self.identity,
                Rc::clone(&self.scheduler),
                config,
            )
            .await?,
        );
        Ok(())
    }

    async fn connect_with_config(
        db: &Backend,
        identity: CoreDbIdentity,
        scheduler: Rc<TickSchedulerImpl>,
        config: ConnectConfig,
    ) -> Result<BackendConnection> {
        let wake = scheduler.wake_handle();
        let connected = config
            .connector
            .connect(NativeTransportRequest {
                server_url: config.server_url,
                app_id: config.app_id,
                peer_identity: identity.author,
                auth: config.auth,
                wake: Arc::new(move || {
                    wake.immediate.store(true, Ordering::Release);
                    wake.notify.notify_one();
                }),
            })
            .await
            .map_err(|error| JazzError::Connection(error.to_string()))?;
        Ok(db
            .connect_upstream(Box::new(
                WireTransportAdapter::new_with_session_context_and_delegated_sessions(
                    connected.transport,
                    connected.protocol_version,
                    connected.features,
                    None,
                    connected.session_context,
                    connected.permits_delegated_sessions,
                ),
            ))
            .await)
    }

    fn ensure_transaction_open(&self, transaction_id: OpenTransactionId) -> Result<()> {
        if self.transactions.contains_key(&transaction_id) {
            return Ok(());
        }
        if let Some(state) = self.closed_transactions.get(&transaction_id) {
            return Err(JazzError::Write(Self::closed_transaction_message(
                transaction_id,
                *state,
            )));
        }
        Err(JazzError::Write(format!(
            "transaction {transaction_id} is not open"
        )))
    }

    fn closed_transaction_message(
        transaction_id: OpenTransactionId,
        state: ClosedTransactionState,
    ) -> String {
        match state {
            ClosedTransactionState::Committed => {
                format!("transaction {transaction_id} already committed")
            }
            ClosedTransactionState::RolledBack => {
                format!("transaction {transaction_id} completed or was never opened")
            }
        }
    }

    async fn handle_query(
        inner: &Rc<RefCell<Self>>,
        query: crate::query::Query,
        opts: CoreReadOpts,
        table: String,
        wait_for_coverage: bool,
    ) -> Result<Vec<crate::node::CurrentRow>> {
        let (db, prepared) = {
            let inner = inner.borrow();
            (
                inner.backend_clone()?,
                inner
                    .backend()?
                    .prepare_query(&query)
                    .map_err(|error| JazzError::Query(error.to_string()))?,
            )
        };
        let rows = if wait_for_coverage {
            Self::read_remote_one_shot_from_subscription(&db, &prepared, opts).await?
        } else {
            db.all(&prepared, opts)
                .await
                .map_err(|error| JazzError::Query(error.to_string()))?
        };
        inner.borrow_mut().remember_rows(&table, &rows);
        Ok(rows)
    }

    /// Evaluate one strict remote usage site through a short-lived local
    /// maintained subscription. The server supplies only its policy-scoped
    /// source closure; the subscription's receiver-local Groove graph is the
    /// sole producer of the rows returned here.
    ///
    /// `SubscriptionStream::Drop` queues the same finalization if this future
    /// is cancelled while waiting. On ordinary success, rejection, or a closed
    /// stream we explicitly await `close` so its exact coverage owner is
    /// retired before returning to the caller.
    async fn read_remote_one_shot_from_subscription(
        db: &Backend,
        prepared: &crate::db::PreparedQuery,
        opts: CoreReadOpts,
    ) -> Result<Vec<crate::node::CurrentRow>> {
        let mut stream = db
            .subscribe(prepared, opts)
            .await
            .map_err(|error| JazzError::Query(error.to_string()))?;
        let outcome = async {
            loop {
                let event = stream.next_event().await.ok_or_else(|| {
                    JazzError::Query(
                        "remote one-shot subscription closed before settlement".to_owned(),
                    )
                })?;
                if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                    match &event {
                        CoreSubscriptionEvent::Delta {
                            reset,
                            publishable,
                            added,
                            updated,
                            removed,
                            terminal_operations,
                            settled,
                            tier,
                        } => eprintln!(
                            "JAZZ_COVERED_INPUT_TRACE stage=remote_one_shot_event reset={reset} publishable={publishable} added={} updated={} removed={} terminal_ops={} settled={settled} tier={tier:?}",
                            added.len(), updated.len(), removed.len(), terminal_operations.len(),
                        ),
                        CoreSubscriptionEvent::Rejected { reason } => eprintln!(
                            "JAZZ_COVERED_INPUT_TRACE stage=remote_one_shot_rejected reason={reason:?}"
                        ),
                        CoreSubscriptionEvent::Closed => eprintln!(
                            "JAZZ_COVERED_INPUT_TRACE stage=remote_one_shot_closed"
                        ),
                    }
                }
                match event {
                    CoreSubscriptionEvent::Delta { settled: true, .. } => {
                        let snapshot = stream
                            .settled_receiver_local_snapshot()
                            .map_err(|error| {
                                if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                                    eprintln!(
                                        "JAZZ_COVERED_INPUT_TRACE stage=remote_one_shot_snapshot_error error={error}"
                                    );
                                }
                                JazzError::Query(error.to_string())
                            })?;
                        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                            eprintln!(
                                "JAZZ_COVERED_INPUT_TRACE stage=remote_one_shot_settled roots={} rows={}",
                                snapshot.root_count,
                                snapshot.rows.len(),
                            );
                        }
                        return Ok(snapshot
                            .rows
                            .into_iter()
                            .take(snapshot.root_count)
                            .collect());
                    }
                    CoreSubscriptionEvent::Delta { settled: false, .. } => {}
                    CoreSubscriptionEvent::Rejected { reason } => {
                        return Err(JazzError::Query(format!(
                            "remote one-shot subscription rejected: {reason:?}"
                        )));
                    }
                    CoreSubscriptionEvent::Closed => {
                        return Err(JazzError::Query(
                            "remote one-shot subscription closed before settlement".to_owned(),
                        ));
                    }
                }
            }
        }
        .await;
        let finalization = stream
            .close()
            .await
            .map_err(|error| JazzError::Query(error.to_string()));
        match (outcome, finalization) {
            (Ok(rows), Ok(())) => Ok(rows),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    async fn handle_subscribe(
        inner: &Rc<RefCell<Self>>,
        query_decoder: PublicQueryDecoder,
        query: crate::query::Query,
        opts: CoreReadOpts,
        table: String,
        tx: mpsc::UnboundedSender<SubscriptionStreamItem>,
        mut cancellation: oneshot::Receiver<()>,
    ) -> Result<()> {
        // Register before cloning the backend or awaiting core admission. A
        // concurrent shutdown can therefore cancel and await this path even
        // when core subscription setup is still in flight.
        let (mut shutdown_cancellation, completion) = inner.borrow_mut().admit_subscription()?;
        let (db, prepared) = {
            let inner = inner.borrow();
            let prepared = inner
                .backend()?
                .prepare_query(&query)
                .map_err(|error| JazzError::Query(error.to_string()))?;
            (inner.backend_clone()?, prepared)
        };
        let stream = tokio::select! {
            biased;
            _ = &mut shutdown_cancellation => return Err(ClientDbInner::shutdown_error()),
            stream = db.subscribe(&prepared, opts) => stream
                .map_err(|error| JazzError::Query(error.to_string()))?,
        };
        let inner = Rc::clone(inner);
        tokio::task::spawn_local(async move {
            let _completion = completion;
            let mut stream = stream;
            let mut current_rows: Vec<CoreSubscriptionOutputRow> = Vec::new();
            loop {
                let Some(event) = (tokio::select! {
                    biased;
                    _ = &mut cancellation => None,
                    _ = &mut shutdown_cancellation => None,
                    event = stream.next_event() => event,
                }) else {
                    break;
                };
                match event {
                    CoreSubscriptionEvent::Delta {
                        reset,
                        added,
                        updated,
                        removed,
                        settled,
                        ..
                    } => {
                        // A reset carries the complete replacement snapshot.
                        // Core may omit explicit removals because the reset bit
                        // already makes absence authoritative, but the public
                        // facade exposes semantic deltas. Recover those absent
                        // occurrences before classifying retained snapshot rows
                        // as updates.
                        let mut removed = removed;
                        if reset {
                            let replacement_ids = added
                                .iter()
                                .chain(&updated)
                                .map(|row| row.occurrence_id.clone())
                                .collect::<std::collections::BTreeSet<_>>();
                            let explicitly_removed = removed
                                .iter()
                                .map(|row| row.occurrence_id.clone())
                                .collect::<std::collections::BTreeSet<_>>();
                            removed.extend(
                                reset_absent_row_indices(
                                    &current_rows,
                                    &replacement_ids,
                                    &explicitly_removed,
                                    |row| &row.occurrence_id,
                                )
                                .into_iter()
                                .map(|index| {
                                    let row = &current_rows[index];
                                    crate::db::RemovedRow {
                                        table: table.clone(),
                                        row_uuid: row.row.row_uuid(),
                                        occurrence_id: row.occurrence_id.clone(),
                                        index: row.index,
                                    }
                                }),
                            );
                        }
                        // A local aggregate snapshot may retract while the
                        // relay concurrently publishes its replacement.  The
                        // relay correctly calls that replacement an update,
                        // but it is an add relative to this facade's already
                        // retracted snapshot. Normalize at this boundary so a
                        // public stream never emits an update for an unknown
                        // row.
                        let removed_occurrences = removed
                            .iter()
                            .map(|row| row.occurrence_id.clone())
                            .collect::<std::collections::BTreeSet<_>>();
                        let surviving_rows = surviving_subscription_rows(
                            &current_rows,
                            &removed_occurrences,
                            |row| &row.occurrence_id,
                            |row| row.index,
                        );
                        let previous_rows_by_occurrence = current_rows
                            .iter()
                            .map(|row| (row.occurrence_id.clone(), row))
                            .collect::<std::collections::BTreeMap<_, _>>();
                        let (effective_added, mut effective_updated) =
                            normalize_subscription_updates(
                                surviving_rows,
                                added,
                                updated,
                                |row| &row.occurrence_id,
                                |row, previous_index| {
                                    row.previous_index.get_or_insert(previous_index);
                                },
                            );
                        retain_changed_subscription_updates(
                            &mut effective_updated,
                            &previous_rows_by_occurrence,
                            |row| &row.occurrence_id,
                            |current, row| {
                                current.index == row.index
                                    && current.row.subscription_equivalent(&row.row)
                            },
                        );
                        let change_delta = query_decoder
                            .core_subscription_change_delta(
                                &db,
                                &query,
                                &current_rows,
                                &effective_added,
                                &effective_updated,
                                &removed,
                            )
                            .await;
                        PublicQueryDecoder::apply_core_subscription_rows(
                            &mut current_rows,
                            &effective_added,
                            &effective_updated,
                            &removed,
                        );
                        let rows_for_cache = current_rows
                            .iter()
                            .map(|row| row.row.clone())
                            .collect::<Vec<_>>();
                        inner.borrow_mut().remember_rows(&table, &rows_for_cache);
                        let delta = change_delta;
                        let Ok(delta) = delta else {
                            break;
                        };
                        let mut delta = delta;
                        delta.pending = !settled;
                        let _ = tx.send(SubscriptionStreamItem::Delta(delta));
                    }
                    CoreSubscriptionEvent::Rejected { reason } => {
                        let reason = match reason {
                            crate::protocol::SubscribeRejectReason::UnsupportedShapeCapability {
                                detail,
                            } => SubscriptionRejectReason::UnsupportedShapeCapability { detail },
                            crate::protocol::SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission => {
                                SubscriptionRejectReason::ShapeRegistrationPendingCatalogueAdmission
                            }
                            crate::protocol::SubscribeRejectReason::ServerFailure { code } => {
                                SubscriptionRejectReason::ServerFailure {
                                    code: match code {
                                        crate::protocol::SubscribeServerFailureCode::TableNotFound => {
                                            SubscriptionServerFailureCode::TableNotFound
                                        }
                                        crate::protocol::SubscribeServerFailureCode::SchemaResolution => {
                                            SubscriptionServerFailureCode::SchemaResolution
                                        }
                                        crate::protocol::SubscribeServerFailureCode::QueryValidation => {
                                            SubscriptionServerFailureCode::QueryValidation
                                        }
                                        crate::protocol::SubscribeServerFailureCode::QueryLowering => {
                                            SubscriptionServerFailureCode::QueryLowering
                                        }
                                        crate::protocol::SubscribeServerFailureCode::PolicyEvaluation => {
                                            SubscriptionServerFailureCode::PolicyEvaluation
                                        }
                                        crate::protocol::SubscribeServerFailureCode::Internal => {
                                            SubscriptionServerFailureCode::Internal
                                        }
                                    },
                                }
                            }
                        };
                        let _ = tx.send(SubscriptionStreamItem::Rejected { reason });
                    }
                    CoreSubscriptionEvent::Closed => break,
                }
            }
        });
        Ok(())
    }

    async fn handle_wait_for_transaction(
        inner: &Rc<RefCell<Self>>,
        transaction_id: TransactionId,
        tier: DurabilityTier,
        timeout: Duration,
    ) -> Result<()> {
        let desired = core_write_tier(tier);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            inner.borrow().ensure_tick_driver_running()?;
            let tx_id = {
                let borrowed = inner.borrow();
                if let Some(tx_id) = borrowed.write_map.get(&transaction_id).copied() {
                    tx_id
                } else {
                    return Err(JazzError::Sync(format!(
                        "unknown transaction {transaction_id}"
                    )));
                }
            };
            let state = inner
                .borrow()
                .backend()?
                .write_state(tx_id)
                .map_err(|error| JazzError::Sync(error.to_string()))?;
            if let CoreFate::Rejected(reason) = &state.fate {
                return Err(JazzError::Sync(transaction_rejected_before_tier_message(
                    tier, reason,
                )));
            }
            if crate::db::transaction_satisfies_wait(
                &state.fate,
                state.global_time,
                state.durability,
                desired,
            ) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(JazzError::Sync(format!(
                    "timed out waiting for transaction to reach {tier:?}"
                )));
            }
            let (db, tick_driver_error_notify) = {
                let inner = inner.borrow();
                (
                    inner.backend_clone()?,
                    Arc::clone(&inner.tick_driver_error_notify),
                )
            };
            let tick_driver_failure = tick_driver_error_notify.notified();
            tokio::select! {
                state_change = tokio::time::timeout_at(deadline, db.next_write_state_change(tx_id)) => {
                    if state_change.is_err() {
                        return Err(JazzError::Sync(format!(
                            "timed out waiting for transaction to reach {tier:?}"
                        )));
                    }
                }
                _ = tick_driver_failure => inner.borrow().ensure_tick_driver_running()?,
            }
        }
    }

    fn remember_write(&mut self, row_id: ObjectId, table: &str, tx_id: CoreTxId) {
        self.write_map.insert(core_batch_id(tx_id), tx_id);
        self.row_tables.insert(row_id, table.to_string());
    }

    fn remember_rows(&mut self, table: &str, rows: &[crate::node::CurrentRow]) {
        for row in rows {
            self.row_tables
                .insert(ObjectId::from_uuid(row.row_uuid().0), table.to_string());
        }
    }
}

/// Convert updates against members absent after this delta's removals into
/// additions. A relay replacement may cross a locally retracted aggregate
/// member, so public streams may never observe that as an update.
fn normalize_subscription_updates<T>(
    surviving: std::collections::BTreeMap<OutputOccurrenceId, usize>,
    added: Vec<T>,
    updated: Vec<T>,
    occurrence_id: impl Fn(&T) -> &OutputOccurrenceId,
    mut retain_previous_index: impl FnMut(&mut T, usize),
) -> (Vec<T>, Vec<T>) {
    let mut effective_added = Vec::new();
    let mut effective_updated = Vec::new();
    for mut row in added {
        if let Some(previous_index) = surviving.get(occurrence_id(&row)).copied() {
            retain_previous_index(&mut row, previous_index);
            effective_updated.push(row);
        } else {
            effective_added.push(row);
        }
    }
    for row in updated {
        if surviving.contains_key(occurrence_id(&row)) {
            effective_updated.push(row);
        } else {
            effective_added.push(row);
        }
    }
    (effective_added, effective_updated)
}

fn reset_absent_row_indices<T, K: Ord>(
    current: &[T],
    replacement_ids: &std::collections::BTreeSet<K>,
    explicitly_removed: &std::collections::BTreeSet<K>,
    occurrence_id: impl Fn(&T) -> &K,
) -> Vec<usize> {
    current
        .iter()
        .enumerate()
        .filter_map(|(index, row)| {
            let id = occurrence_id(row);
            (!replacement_ids.contains(id) && !explicitly_removed.contains(id)).then_some(index)
        })
        .collect()
}

fn retain_changed_subscription_updates<T, K: Ord, P>(
    updates: &mut Vec<T>,
    previous_by_occurrence: &std::collections::BTreeMap<K, P>,
    occurrence_id: impl Fn(&T) -> &K,
    unchanged: impl Fn(&P, &T) -> bool,
) {
    updates.retain(|row| {
        previous_by_occurrence
            .get(occurrence_id(row))
            .is_none_or(|previous| !unchanged(previous, row))
    });
}

fn surviving_subscription_rows<T, K: Ord + Clone>(
    current: &[T],
    removed_occurrences: &std::collections::BTreeSet<K>,
    occurrence_id: impl Fn(&T) -> &K,
    index: impl Fn(&T) -> usize,
) -> std::collections::BTreeMap<K, usize> {
    current
        .iter()
        .filter(|row| !removed_occurrences.contains(occurrence_id(row)))
        .map(|row| (occurrence_id(row).clone(), index(row)))
        .collect()
}

/// Transaction-scoped Jazz client handle.
///
/// Mutations issued through this handle are staged in the transaction returned
/// by [`JazzClient::begin_transaction`]. The handle dereferences to the scoped
/// [`JazzClient`] so regular client methods can be used directly.
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

fn session_from_unverified_jwt(token: &str) -> Option<Session> {
    let payload = token.split('.').nth(1)?;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))
        .ok()?;
    let claims: UnverifiedJwtClaims = serde_json::from_slice(&payload).ok()?;
    let user_id = claims.sub.as_str();
    if !crate::tools::identity::principal_is_nonempty(user_id) {
        return None;
    }

    let auth_mode = match claims.iss.as_str() {
        CoreAuthorSubject::LOCAL_FIRST_ISSUER => {
            crate::tools::public_api::session::AuthMode::LocalFirst
        }
        CoreAuthorSubject::ANONYMOUS_ISSUER => {
            crate::tools::public_api::session::AuthMode::Anonymous
        }
        _ => crate::tools::public_api::session::AuthMode::External,
    };
    Some(Session {
        issuer: claims.iss.clone(),
        user_id: user_id.to_string(),
        claims: match claims.claims {
            JwtClaimsPayload::Absent => serde_json::Value::Object(serde_json::Map::new()),
            JwtClaimsPayload::Present(claims) => claims,
        },
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
}

fn core_identity(
    context: &AppContext,
    default_session: Option<&Session>,
) -> Result<CoreDbIdentity> {
    let node_uuid = context
        .client_id
        .map(|id| id.0)
        .unwrap_or_else(Uuid::now_v7);
    let author = match default_session {
        Some(session) => core_author_from_session(session)?,
        // A backend/admin credential is an internal authority context, not an
        // unauthenticated end-user session.  Its connection identity must use
        // the canonical system subject so trusted writes can receive their
        // fate even when a per-write session supplies a distinct permission
        // subject.
        None if context.backend_secret.is_some() || context.admin_secret.is_some() => {
            CoreAuthorSubject::SYSTEM
        }
        None => CoreAuthorSubject::reserved(
            CoreAuthorSubject::ANONYMOUS_ISSUER,
            &node_uuid.to_string(),
        )?,
    };
    Ok(CoreDbIdentity {
        node: CoreNodeUuid(node_uuid),
        author,
    })
}

fn core_author_from_session(session: &Session) -> Result<CoreAuthorSubject> {
    Ok(session.author_subject()?)
}

async fn core_storage(
    schema: &crate::schema::JazzSchema,
    context: &AppContext,
) -> Result<StorageBundle> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    match context.storage {
        ClientStorage::Memory => Ok(CoreStorage::new(
            CoreMemoryStorage::new(&refs).expect("valid memory storage families"),
        )),
        ClientStorage::Persistent => {
            let factory = context.storage_factory.as_ref().ok_or_else(|| {
                JazzError::Connection(
                    "persistent client storage requires a target-shell storage factory".to_string(),
                )
            })?;
            factory
                .open(
                    context.data_dir.join("jazz-core.rocksdb"),
                    column_families,
                    epoch_1_storage_codec_profile()
                        .map_err(|error| JazzError::Connection(error.to_string()))?,
                )
                .await
                .map_err(|error| JazzError::Connection(error.to_string()))
        }
    }
}

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

fn json_claim_to_core_value(value: serde_json::Value) -> Result<CoreValue> {
    match value {
        serde_json::Value::Null => Ok(CoreValue::Nullable(None)),
        serde_json::Value::Bool(value) => Ok(CoreValue::Bool(value)),
        serde_json::Value::String(value) => Ok(CoreValue::String(value)),
        serde_json::Value::Number(value) => {
            crate::tools::policy_claims::json_number_to_policy_claim(
                value,
                crate::tools::policy_claims::NumericClaimOrigin::ExactJson,
            )
            .map_err(JazzError::Connection)
        }
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(json_claim_to_core_value)
            .collect::<Result<Vec<_>>>()
            .map(CoreValue::Array),
        serde_json::Value::Object(_) => Err(JazzError::Connection(
            "nested JWT claim objects are not supported by core policy claims yet".to_string(),
        )),
    }
}

fn session_claims_to_core_claims(session: &Session) -> Result<HashMap<String, CoreValue>> {
    let serde_json::Value::Object(claims) = session.claims.clone() else {
        return Err(JazzError::Connection(
            "JWT claims payload must be a JSON object".to_string(),
        ));
    };
    let mut core_claims = HashMap::new();
    for (name, value) in claims {
        core_claims.insert(
            crate::query::provider_claim_key(&name),
            json_claim_to_core_value(value)?,
        );
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
    core_claims.insert(
        "user".to_owned(),
        CoreValue::String(session.author_subject()?.canonical().to_owned()),
    );
    Ok(core_claims)
}

fn core_to_public_value(value: CoreValue) -> Result<Value> {
    match value {
        CoreValue::Bool(value) => Ok(Value::Boolean(value)),
        CoreValue::String(value) => Ok(Value::Text(value)),
        CoreValue::U32(value) => Ok(Value::Integer(i32::try_from(value).map_err(|_| {
            JazzError::Query(format!("core U32 value {value} is outside INTEGER range"))
        })?)),
        CoreValue::I32(value) => Ok(Value::Integer(value)),
        CoreValue::I64(value) => Ok(Value::BigInt(value)),
        CoreValue::U64(value) => Ok(Value::Timestamp(value)),
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
        CoreValue::Record(record) => {
            let identity_index = ["row_uuid", "id"]
                .into_iter()
                .find_map(|name| record.descriptor().field_index(name))
                .filter(|index| {
                    record.descriptor().fields()[*index].name.as_deref() == Some("row_uuid")
                });
            let id = identity_index.and_then(|index| match record.get_idx(index) {
                Ok(CoreValue::Uuid(id)) => Some(ObjectId::from_uuid(id)),
                _ => None,
            });
            let values = record
                .to_values()
                .map_err(|error| JazzError::Query(error.to_string()))?
                .into_iter()
                .enumerate()
                .filter(|(index, _)| Some(*index) != identity_index)
                .map(|(_, value)| core_to_public_value(value))
                .collect::<Result<Vec<_>>>()?;
            Ok(Value::Row { id, values })
        }
        other => Err(JazzError::Query(format!(
            "client does not support core value {other:?}"
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
                JazzError::Write(format!(
                    "BIGINT value {value} is outside INTEGER range for core write"
                ))
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

fn public_to_core_value_for_column(
    value: Value,
    column: &crate::tools::public_schema::ColumnDescriptor,
) -> Result<CoreValue> {
    validate_json_value(&value, &column.column_type, column.name_str())
        .map_err(JazzError::Write)?;
    let value = public_to_core_value_for_column_type(value, &column.column_type)?;
    if column.nullable && !matches!(value, CoreValue::Nullable(_)) {
        Ok(CoreValue::Nullable(Some(Box::new(value))))
    } else {
        Ok(value)
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
        (CoreValue::I64(value), ColumnType::Integer) => {
            i32::try_from(value).map(Value::Integer).map_err(|_| {
                JazzError::Query(format!("core I64 value {value} is outside INTEGER range"))
            })
        }
        (CoreValue::I64(value), ColumnType::BigInt) => Ok(Value::BigInt(value)),
        (CoreValue::Array(values), ColumnType::Array { element }) => values
            .into_iter()
            .map(|value| core_to_public_value_for_column_type(value, element))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        (value, _) => core_to_public_value(value),
    }
}

fn auth_mode_claim_value(auth_mode: crate::tools::public_api::session::AuthMode) -> &'static str {
    match auth_mode {
        crate::tools::public_api::session::AuthMode::External => "external",
        crate::tools::public_api::session::AuthMode::LocalFirst => "local-first",
        crate::tools::public_api::session::AuthMode::Anonymous => "anonymous",
    }
}

fn core_row_provenance_to_public(
    provenance: crate::node::RowProvenance,
) -> crate::tools::metadata::RowProvenance {
    crate::tools::metadata::RowProvenance {
        created_by: provenance.created_by.canonical().to_owned(),
        created_at: provenance.created_at,
        updated_by: provenance.updated_by.canonical().to_owned(),
        updated_at: provenance.updated_at,
    }
}

/// Re-encode a subscription row for the public Jazz-client boundary.
///
/// Current rows already expose public provenance in Unix milliseconds. Packed
/// HLC values remain internal version and transaction-ordering state.
fn public_subscription_record(row: &crate::node::CurrentRow) -> Result<Vec<u8>> {
    let (descriptor, raw) = row.encoded_record();
    let mut values = BorrowedRecord::new(raw, descriptor)
        .to_values()
        .map_err(|error| JazzError::Query(format!("invalid subscription row: {error}")))?;
    normalize_public_subscription_record_values(descriptor, &mut values)?;
    descriptor
        .create(&values)
        .map_err(|error| JazzError::Query(format!("encode public subscription row: {error}")))
}

fn normalize_public_subscription_record_values(
    descriptor: &crate::groove::records::RecordDescriptor,
    values: &mut [CoreValue],
) -> Result<()> {
    if descriptor.fields().len() != values.len() {
        return Err(JazzError::Query(
            "subscription record value count does not match its descriptor".to_owned(),
        ));
    }
    for (field, value) in descriptor.fields().iter().zip(values) {
        normalize_public_subscription_value(field.name.as_deref(), &field.value_type, value)?;
    }
    Ok(())
}

fn normalize_public_subscription_value(
    field_name: Option<&str>,
    value_type: &CoreValueType,
    value: &mut CoreValue,
) -> Result<()> {
    match (value_type, value) {
        (CoreValueType::Nullable(inner), CoreValue::Nullable(Some(value))) => {
            normalize_public_subscription_value(field_name, inner, value)
        }
        (CoreValueType::Nullable(_), CoreValue::Nullable(None)) => Ok(()),
        (CoreValueType::Array(element), CoreValue::Array(values)) => {
            for value in values {
                normalize_public_subscription_value(None, element, value)?;
            }
            Ok(())
        }
        (CoreValueType::Record(descriptor), CoreValue::Record(record)) => {
            if record.descriptor() != descriptor.as_ref() {
                return Err(JazzError::Query(
                    "subscription nested record does not match its descriptor".to_owned(),
                ));
            }
            let mut values = record.to_values().map_err(|error| {
                JazzError::Query(format!("invalid nested subscription record: {error}"))
            })?;
            normalize_public_subscription_record_values(descriptor, &mut values)?;
            let raw = descriptor.create(&values).map_err(|error| {
                JazzError::Query(format!("encode nested public subscription record: {error}"))
            })?;
            *record = OwnedRecord::new(raw, (**descriptor).clone());
            Ok(())
        }
        (CoreValueType::Tuple(members), CoreValue::Tuple(values)) => {
            if members.len() != values.len() {
                return Err(JazzError::Query(
                    "subscription tuple value count does not match its descriptor".to_owned(),
                ));
            }
            for (member, value) in members.iter().zip(values) {
                normalize_public_subscription_value(None, member, value)?;
            }
            Ok(())
        }
        (CoreValueType::U64, CoreValue::U64(_timestamp))
            if matches!(field_name, Some("$createdAt" | "$updatedAt")) =>
        {
            Ok(())
        }
        (_, _) if matches!(field_name, Some("$createdAt" | "$updatedAt")) => {
            Err(JazzError::Query(format!(
                "subscription provenance field {} is not a u64 timestamp",
                field_name.expect("matched provenance field")
            )))
        }
        _ => Ok(()),
    }
}

fn aggregate_output_column_type(
    output: &CoreAggregate,
    table_schema: &TableSchema,
    table: &str,
) -> Result<Option<ColumnType>> {
    match output.function {
        CoreAggregateFunction::Count => Ok(Some(ColumnType::Timestamp)),
        CoreAggregateFunction::Avg => Ok(Some(ColumnType::Double)),
        CoreAggregateFunction::Sum | CoreAggregateFunction::Min | CoreAggregateFunction::Max => {
            let column = output
                .column
                .as_deref()
                .expect("non-count aggregate has an input column");
            let idx = table_schema.columns.column_index(column).ok_or_else(|| {
                JazzError::Query(format!(
                    "unknown aggregate column {column} on table {table}"
                ))
            })?;
            Ok(Some(table_schema.columns.columns[idx].column_type.clone()))
        }
    }
}

fn aggregate_public_values(
    query: &Query,
    table_schema: &TableSchema,
    row: &crate::node::CurrentRow,
) -> Result<Vec<Value>> {
    let Some(aggregate) = &query.aggregate else {
        return Ok(Vec::new());
    };
    // Aggregate collector records are normalized to the same `CurrentRow`
    // representation as incremental aggregate updates before they reach this
    // boundary.  Aggregate aliases remain in their compiler-reserved logical
    // namespace, nested inside the ordinary physical `user_` cell namespace.
    let mut columns: Vec<(String, String, Option<ColumnType>)> = Vec::new();
    if let Some(group_by) = &aggregate.group_by {
        let idx = table_schema.columns.column_index(group_by).ok_or_else(|| {
            JazzError::Query(format!(
                "unknown group_by column {group_by} on table {}",
                query.table.as_str()
            ))
        })?;
        columns.push((
            group_by.clone(),
            crate::node::query_engine::user_column_field(group_by),
            Some(table_schema.columns.columns[idx].column_type.clone()),
        ));
    }
    // The public API preserves the output order requested by the caller.
    // It resolves those names from the compiler-owned aggregate record, whose
    // internal order may be canonicalized for shape sharing.
    for output in &aggregate.aggregates {
        let public_name = output.alias.clone();
        columns.push((
            public_name.clone(),
            crate::node::query_engine::aggregate_output_app_field(&public_name),
            aggregate_output_column_type(output, table_schema, query.table.as_str())?,
        ));
    }
    let (descriptor, raw) = row.encoded_record();
    let borrowed = crate::groove::records::BorrowedRecord::new(raw, descriptor);
    columns
        .into_iter()
        .map(|(public_column, physical_column, column_type)| {
            let idx = descriptor.field_index(&physical_column).ok_or_else(|| {
                if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                    eprintln!(
                        "JAZZ_COVERED_INPUT_TRACE stage=aggregate_field_missing wanted={physical_column} descriptor_fields={:?}",
                        descriptor
                            .fields()
                            .iter()
                            .map(|field| field.name.as_deref())
                            .collect::<Vec<_>>(),
                    );
                }
                JazzError::Query(format!(
                    "aggregate row missing column {public_column} (physical field {physical_column})"
                ))
            })?;
            let value = borrowed
                .get_idx(idx)
                .map_err(|error| JazzError::Query(error.to_string()))?;
            if let Some(column_type) = column_type {
                core_to_public_value_for_column_type(value, &column_type)
            } else {
                core_to_public_value(value)
            }
        })
        .collect()
}

fn core_batch_id(tx_id: CoreTxId) -> TransactionId {
    TransactionId::from_committed_tx(tx_id)
}

fn core_write_tier(tier: DurabilityTier) -> CoreDurabilityTier {
    match tier {
        DurabilityTier::Local => CoreDurabilityTier::Local,
        DurabilityTier::EdgeServer => CoreDurabilityTier::Edge,
        DurabilityTier::GlobalServer => CoreDurabilityTier::Global,
    }
}

fn core_legacy_read_tier(tier: DurabilityTier) -> CoreDurabilityTier {
    match tier {
        DurabilityTier::Local => CoreDurabilityTier::Local,
        DurabilityTier::EdgeServer | DurabilityTier::GlobalServer => CoreDurabilityTier::Global,
    }
}

fn core_rejection_reason_label(reason: &CoreRejectionReason) -> String {
    match reason {
        CoreRejectionReason::ClientClockTooFarAhead => "client_clock_too_far_ahead".to_owned(),
        CoreRejectionReason::AuthorizationDenied => "authorization_denied".to_owned(),
        CoreRejectionReason::ExclusiveConflict => "transaction_conflict".to_owned(),
        CoreRejectionReason::CausalityViolation => "causality_violation".to_owned(),
        CoreRejectionReason::Cascade { root } => format!("cascade:{root:?}"),
        CoreRejectionReason::MalformedCommit(reason) => format!("malformed_commit:{reason}"),
    }
}

fn transaction_rejected_before_tier_message(
    tier: DurabilityTier,
    reason: &CoreRejectionReason,
) -> String {
    format!(
        "transaction was rejected before reaching {tier:?} durability: {}",
        core_rejection_reason_label(reason)
    )
}

impl JazzClient {
    fn write_identity(&self) -> Result<Option<CoreAuthorSubject>> {
        let session = self
            .write_context
            .as_ref()
            .and_then(|context| context.session())
            .or(self.default_session.as_ref());
        let Some(session) = session else {
            return Ok(None);
        };
        let identity = core_author_from_session(session)?;
        // Explicit backend session scopes supply the policy subject at write
        // time, after the shared client has already opened. Register their
        // raw session claims under that canonical subject before evaluating
        // local policy. In particular, `user_id` remains the JWT subject so a
        // UUID policy columns can coerce it; the separate reserved `author`
        // claim carries canonical provenance identity.
        let claims = session_claims_to_core_claims(session)?;
        self.db
            .inner
            .borrow()
            .backend()?
            .set_identity_claims(identity, claims);
        Ok(Some(identity))
    }

    /// Validate a public write-context physical-millisecond timestamp.
    /// Core mints its packed HLC representation when it constructs provenance.
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
        const MAX_PHYSICAL_MS: u64 = crate::time::HLC_MAX_PHYSICAL_MS;
        if updated_at > MAX_PHYSICAL_MS {
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

    fn check_core_write_not_rejected(db: &Backend, tx_id: CoreTxId) -> Result<()> {
        let state = db
            .write_state(tx_id)
            .map_err(|error| JazzError::Write(error.to_string()))?;
        if let CoreFate::Rejected(reason) = state.fate {
            return Err(JazzError::Write(format!("core write rejected: {reason:?}")));
        }
        Ok(())
    }
    fn core_read_opts(durability_tier: Option<DurabilityTier>) -> CoreReadOpts {
        CoreReadOpts {
            tier: durability_tier
                .map(core_legacy_read_tier)
                .unwrap_or(CoreDurabilityTier::Local),
            local_updates: CoreLocalUpdates::Immediate,
            propagation: CorePropagation::Full,
            include_deleted: false,
            read_view: CoreReadViewSpec::default(),
        }
    }

    fn core_read_opts_for_read_tier(tier: ReadTier) -> CoreReadOpts {
        let mut opts = Self::core_read_opts(Some(tier.legacy_durability_tier()));
        opts.local_updates = match tier {
            ReadTier::Remote => CoreLocalUpdates::Deferred,
            ReadTier::LocalFirst | ReadTier::RemoteIfPossible => CoreLocalUpdates::Immediate,
        };
        opts
    }
}

impl PublicQueryDecoder {
    fn core_rows_to_public(
        &self,
        db: &Backend,
        query: &Query,
        rows: Vec<crate::node::CurrentRow>,
    ) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        let table = query.table.as_str();
        let schema = self.schema.as_ref();
        let table_schema = schema
            .get(&TableName::new(table))
            .ok_or_else(|| JazzError::Query(format!("unknown table {table}")))?;
        if query.aggregate.is_some() {
            return rows
                .into_iter()
                .map(|row| {
                    let row_id = ObjectId::from_uuid(row.row_uuid().0);
                    let values = aggregate_public_values(query, table_schema, &row)?;
                    Ok((row_id, values))
                })
                .collect();
        }
        if let Some(flat_join) = &query.flat_join {
            let mut output_columns = Vec::new();
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
            for (source, table) in sources {
                let table_schema = schema
                    .get(&TableName::new(&table))
                    .ok_or_else(|| JazzError::Query(format!("unknown flat join table {table}")))?;
                for column in &table_schema.columns.columns {
                    output_columns.push((
                        format!("{source}.{}", column.name),
                        column.column_type.clone(),
                    ));
                }
            }
            return rows
                .into_iter()
                .map(|row| {
                    let row_id = ObjectId::from_uuid(row.row_uuid().0);
                    let values = output_columns
                        .iter()
                        .map(|(field, ty)| {
                            row.raw_field(field)
                                .ok_or_else(|| {
                                    JazzError::Query(format!("flat join row missing {field}"))
                                })
                                .and_then(|value| core_to_public_value_for_column_type(value, ty))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok((row_id, values))
                })
                .collect();
        }
        let columns = query.select.clone().unwrap_or_else(|| {
            table_schema
                .columns
                .columns
                .iter()
                .map(|column| column.name.as_str().to_string())
                .collect()
        });
        let rows = rows
            .into_iter()
            .map(|row| {
                let core_row_id = row.row_uuid();
                let row_id = ObjectId::from_uuid(core_row_id.0);
                let values = columns
                    .iter()
                    .map(|column| {
                        if let Some(value) =
                            self.core_magic_value(db, table, core_row_id, &row, column)?
                        {
                            return Ok(value);
                        }
                        let position =
                            table_schema.columns.column_index(column).ok_or_else(|| {
                                JazzError::Query(format!(
                                    "unknown column {column} on table {table}"
                                ))
                            })?;
                        let physical_column = crate::node::query_engine::user_column_field(column);
                        let value = row
                            .raw_field(&physical_column)
                            .or_else(|| row.raw_field(column))
                            .ok_or_else(|| {
                                JazzError::Query(format!("row missing column {column}"))
                            })?;
                        let column_schema = &table_schema.columns.columns[position];
                        if matches!(value, CoreValue::Nullable(None)) && !column_schema.nullable {
                            return Err(JazzError::Query(format!(
                                "row missing projected value for column {column}"
                            )));
                        }
                        core_to_public_value_for_column_type(value, &column_schema.column_type)
                    })
                    .chain(query.array_subqueries.iter().map(|subquery| {
                        row.raw_field(&subquery.column_name)
                            .ok_or_else(|| {
                                JazzError::Query(format!(
                                    "row missing array relation {}",
                                    subquery.column_name
                                ))
                            })
                            .and_then(core_to_public_value)
                    }))
                    .collect::<Result<Vec<_>>>()?;
                Ok((row_id, values))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(rows)
    }

    fn core_rows_to_query_results(
        &self,
        db: &Backend,
        query: &Query,
        rows: Vec<crate::node::CurrentRow>,
    ) -> Result<Vec<QueryResult>> {
        let keys = rows
            .iter()
            .map(|row| ResultKey::from_occurrence(crate::db::subscription_row_occurrence_id(row)))
            .collect::<Vec<_>>();
        let names = self.query_result_column_names(query)?;
        let values = self.core_rows_to_public(db, query, rows)?;
        keys.into_iter()
            .zip(values)
            .map(|(key, (_, values))| {
                if names.len() != values.len() {
                    return Err(JazzError::Query(format!(
                        "query result descriptor has {} fields but row has {} values",
                        names.len(),
                        values.len()
                    )));
                }
                let fields = names
                    .iter()
                    .cloned()
                    .zip(values)
                    .map(|(name, value)| QueryResultField { name, value })
                    .collect();
                Ok(QueryResult::new(key, fields))
            })
            .collect()
    }

    fn query_result_column_names(&self, query: &Query) -> Result<Vec<String>> {
        let schema = self.schema.as_ref();
        let table = query.table.as_str();
        let table_schema = schema
            .get(&TableName::new(table))
            .ok_or_else(|| JazzError::Query(format!("unknown table {table}")))?;
        if let Some(aggregate) = &query.aggregate {
            let mut names = aggregate.group_by.iter().cloned().collect::<Vec<_>>();
            names.extend(
                aggregate
                    .aggregates
                    .iter()
                    .map(|output| output.alias.clone()),
            );
            return Ok(names);
        }
        if let Some(flat_join) = &query.flat_join {
            let mut names = Vec::new();
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
            for (source, table) in sources {
                let source_schema = schema
                    .get(&TableName::new(&table))
                    .ok_or_else(|| JazzError::Query(format!("unknown flat join table {table}")))?;
                names.extend(
                    source_schema
                        .columns
                        .columns
                        .iter()
                        .map(|column| format!("{source}.{}", column.name)),
                );
            }
            return Ok(names);
        }
        let mut names = query.select.clone().unwrap_or_else(|| {
            table_schema
                .columns
                .columns
                .iter()
                .map(|column| column.name.as_str().to_owned())
                .collect()
        });
        names.extend(
            query
                .array_subqueries
                .iter()
                .map(|subquery| subquery.column_name.clone()),
        );
        Ok(names)
    }
}

impl PublicQueryDecoder {
    async fn core_subscription_row_to_public(
        &self,
        db: &Backend,
        query: &Query,
        row: &CoreSubscriptionOutputRow,
    ) -> Result<Row> {
        #[cfg(not(feature = "testing"))]
        let _ = query;
        let encoded = public_subscription_record(&row.row)?;
        let provenance = db
            .row_provenance_for_subscription(&row.row)
            .await
            .map_err(|error| JazzError::Query(error.to_string()))?
            .map(core_row_provenance_to_public)
            .unwrap_or_else(|| {
                crate::tools::metadata::RowProvenance::for_insert("jazz:unknown", 0)
            });
        let public = Row::new(
            ResultKey::from_occurrence(row.occurrence_id.clone()),
            encoded.to_vec(),
            TransactionId([0; 16]),
            provenance,
        );
        #[cfg(feature = "testing")]
        let public = {
            let fields = match self.core_rows_to_query_results(db, query, vec![row.row.clone()]) {
                Ok(mut results) => results
                    .pop()
                    .map(|result| result.fields)
                    .unwrap_or_default(),
                Err(error) => {
                    if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                        eprintln!(
                            "JAZZ_COVERED_INPUT_TRACE stage=subscription_public_fields_error error={error}"
                        );
                    }
                    Vec::new()
                }
            };
            public.with_fields(fields)
        };
        Ok(public)
    }

    fn apply_core_subscription_rows(
        current_rows: &mut Vec<CoreSubscriptionOutputRow>,
        added_rows: &[CoreSubscriptionOutputRow],
        updated_rows: &[CoreSubscriptionOutputRow],
        removed_rows: &[crate::db::RemovedRow],
    ) {
        let changed = added_rows
            .iter()
            .chain(updated_rows)
            .map(|row| row.occurrence_id.clone())
            .chain(removed_rows.iter().map(|row| row.occurrence_id.clone()))
            .collect::<BTreeSet<_>>();
        current_rows.retain(|row| !changed.contains(&row.occurrence_id));
        let mut placements = added_rows
            .iter()
            .chain(updated_rows)
            .cloned()
            .collect::<Vec<_>>();
        placements.sort_by_key(|row| row.index);
        for row in placements {
            let index = row.index.min(current_rows.len());
            current_rows.insert(index, row);
        }
    }

    async fn core_subscription_change_delta(
        &self,
        db: &Backend,
        query: &Query,
        previous_rows: &[CoreSubscriptionOutputRow],
        added_rows: &[CoreSubscriptionOutputRow],
        updated_rows: &[CoreSubscriptionOutputRow],
        removed_rows: &[crate::db::RemovedRow],
    ) -> Result<OrderedRowDelta> {
        let mut added = Vec::with_capacity(added_rows.len());
        for row in added_rows {
            let public = self.core_subscription_row_to_public(db, query, row).await?;
            added.push(OrderedAdded {
                id: public.id.clone(),
                index: row.index,
                row: public,
            });
        }
        let mut updated = Vec::with_capacity(updated_rows.len());
        for row in updated_rows {
            let public = self.core_subscription_row_to_public(db, query, row).await?;
            let content_changed = previous_rows
                .iter()
                .find(|previous| previous.occurrence_id == row.occurrence_id)
                .is_none_or(|previous| !previous.row.subscription_equivalent(&row.row));
            updated.push(OrderedUpdated {
                id: public.id.clone(),
                old_index: row.previous_index.unwrap_or(row.index),
                new_index: row.index,
                row: content_changed.then_some(public),
            });
        }
        let removed = removed_rows
            .iter()
            .map(|row| OrderedRemoved {
                id: ResultKey::from_occurrence(row.occurrence_id.clone()),
                index: row.index,
            })
            .collect();
        Ok(OrderedRowDelta {
            added,
            removed,
            updated,
            pending: false,
        })
    }
}

impl PublicQueryDecoder {
    fn core_magic_value(
        &self,
        db: &Backend,
        table: &str,
        _row_id: CoreRowUuid,
        row: &crate::node::CurrentRow,
        column: &str,
    ) -> Result<Option<Value>> {
        let value = match column {
            "$canRead" => {
                return Err(JazzError::Query(format!(
                    "permission introspection column {column} requires unified policy lowering"
                )));
            }
            "$createdAt" | "$updatedAt" | "$createdBy" | "$updatedBy" => {
                let provenance = db
                    .row_provenance(row)
                    .map_err(|error| JazzError::Query(error.to_string()))?;
                let Some(provenance) = provenance else {
                    return Err(JazzError::Query(format!(
                        "row missing provenance for magic column {column} on table {table}"
                    )));
                };
                match column {
                    "$createdAt" => Value::Timestamp(provenance.created_at),
                    "$updatedAt" => Value::Timestamp(provenance.updated_at),
                    "$createdBy" => Value::Text(provenance.created_by.canonical().to_owned()),
                    "$updatedBy" => Value::Text(provenance.updated_by.canonical().to_owned()),
                    _ => unreachable!("matched provenance magic column"),
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

impl JazzClient {
    fn core_cells(
        &self,
        table: &str,
        values: HashMap<String, Value>,
    ) -> Result<crate::db::RowCells> {
        let schema = self.schema()?;
        let table_schema = schema
            .get(&TableName::new(table))
            .ok_or_else(|| JazzError::Write(format!("unknown table {table}")))?;
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
    fn core_ordered_values(
        &self,
        table: &str,
        values: &HashMap<String, Value>,
    ) -> Result<Vec<Value>> {
        let schema = self.schema()?;
        let table_schema = schema
            .get(&TableName::new(table))
            .ok_or_else(|| JazzError::Write(format!("unknown table {table}")))?;
        table_schema
            .columns
            .columns
            .iter()
            .map(|column| {
                values
                    .get(column.name.as_str())
                    .cloned()
                    .or_else(|| column.default.clone())
                    .ok_or_else(|| {
                        JazzError::Write(format!(
                            "core insert missing required column {}",
                            column.name.as_str()
                        ))
                    })
            })
            .collect()
    }
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
        let default_session = default_session_from_context(&context);
        let has_server = !context.server_url.is_empty();
        {
            let public_schema_convert = crate::schema::JazzSchema::new(&context.schema)
                .map_err(|error| JazzError::Schema(error.to_string()))?;
            let identity = core_identity(&context, default_session.as_ref())?;
            let storage = core_storage(&public_schema_convert, &context).await?;
            let auth = has_server.then(|| WsAuthConfig {
                jwt_token: if context.backend_secret.is_some() {
                    None
                } else {
                    context.jwt_token.clone()
                },
                backend_secret: context.backend_secret.clone(),
                admin_secret: context.admin_secret.clone(),
                backend_session: None,
            });
            let db = ClientDb::open(
                public_schema_convert,
                context.schema.clone(),
                storage,
                identity,
                has_server.then(|| context.server_url.clone()),
                context.app_id,
                auth,
                connector,
            )
            .await
            .map_err(|error| JazzError::Connection(error.to_string()))?;
            if let Some(session) = default_session.as_ref() {
                let claims = session_claims_to_core_claims(session)?;
                db.inner
                    .borrow()
                    .backend()?
                    .set_identity_claims(identity.author, claims);
            }
            let client = Self {
                default_session,
                write_context: None,
                db,
            };
            Ok(client)
        }
    }

    /// Subscribe to a query.
    ///
    /// Returns a stream of row deltas as the data changes.
    pub async fn subscribe(&self, query: Query) -> Result<SubscriptionStream> {
        self.subscribe_with_read_tier(query, ReadTier::Remote).await
    }

    /// Subscribe using a product-level read tier.
    ///
    /// `RemoteIfPossible` keeps a strict remote initial gate in the native Rust
    /// facade because it has no public explicit-disconnect state; host bindings
    /// can lower it to local only after their caller explicitly disconnects.
    pub async fn subscribe_with_read_tier(
        &self,
        query: Query,
        tier: ReadTier,
    ) -> Result<SubscriptionStream> {
        self.subscribe_with_opts(query, Self::core_read_opts_for_read_tier(tier))
            .await
    }

    /// Subscribe to a query with explicit core read options.
    pub async fn subscribe_with_opts(
        &self,
        query: Query,
        opts: CoreReadOpts,
    ) -> Result<SubscriptionStream> {
        let table = query.table.clone();
        let (tx, rx) = mpsc::unbounded_channel::<SubscriptionStreamItem>();
        let (cancellation, cancellation_rx) = oneshot::channel();
        self.db
            .subscribe(query, opts, table, tx, cancellation_rx)
            .await?;
        Ok(SubscriptionStream::new(rx, cancellation))
    }

    /// One-shot query using a product-level read tier.
    ///
    /// Returns the current results as `Vec<(ObjectId, Vec<Value>)>`.
    pub async fn query_with_read_tier(
        &self,
        query: Query,
        tier: ReadTier,
    ) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        self.query_with_opts(query, Self::core_read_opts_for_read_tier(tier))
            .await
    }

    /// One-shot query, optionally waiting for a legacy durability tier.
    ///
    /// Returns the current results as `Vec<(ObjectId, Vec<Value>)>`.
    #[deprecated(
        note = "read APIs should use query_with_read_tier(query, ReadTier); DurabilityTier remains supported for write waits"
    )]
    pub async fn query(
        &self,
        query: Query,
        durability_tier: Option<DurabilityTier>,
    ) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        self.query_with_opts(query, Self::core_read_opts(durability_tier))
            .await
    }

    /// Execute a row-id query using the canonical core read options.
    pub async fn query_with_opts(
        &self,
        query: Query,
        opts: CoreReadOpts,
    ) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        if query.flat_join.is_some() {
            return Err(JazzError::Query(
                "joined results require query_results(), which returns stable ResultKey values"
                    .to_owned(),
            ));
        }
        let results = self.query_results_with_opts(query, opts).await?;
        results
            .into_iter()
            .map(|result| {
                let row_id = result.key.row_id().ok_or_else(|| {
                    JazzError::Query(
                        "joined result cannot be represented by the legacy row-id query API"
                            .to_owned(),
                    )
                })?;
                Ok((row_id, result.into_values()))
            })
            .collect()
    }

    /// One-shot query with stable result keys using a product-level read tier.
    pub async fn query_results_with_read_tier(
        &self,
        query: Query,
        tier: ReadTier,
    ) -> Result<Vec<QueryResult>> {
        self.query_results_with_opts(query, Self::core_read_opts_for_read_tier(tier))
            .await
    }

    /// One-shot query with stable keys using a legacy durability tier.
    #[deprecated(
        note = "read APIs should use query_results_with_read_tier(query, ReadTier); DurabilityTier remains supported for write waits"
    )]
    pub async fn query_results(
        &self,
        query: Query,
        durability_tier: Option<DurabilityTier>,
    ) -> Result<Vec<QueryResult>> {
        self.query_results_with_opts(query, Self::core_read_opts(durability_tier))
            .await
    }

    /// Execute a query using the canonical core read options.
    pub async fn query_results_with_opts(
        &self,
        query: Query,
        opts: CoreReadOpts,
    ) -> Result<Vec<QueryResult>> {
        let table = query.table.clone();
        let rows = if let Some(transaction_id) = self
            .write_context
            .as_ref()
            .and_then(|ctx| ctx.transaction_id)
        {
            let author = self
                .write_identity()?
                .unwrap_or_else(|| self.db.inner.borrow().identity.author);
            self.db
                .query_transaction_rows(query.clone(), opts, transaction_id, table, author)
                .await?
        } else {
            // A product `Remote` read lowers to the legacy Edge tier. Both
            // Edge and Global are strict remote one-shots: they must own a
            // fresh coverage lifetime and return only after the receiver's
            // local maintained graph has settled that exact coverage.
            let wait_for_coverage = opts.tier >= CoreDurabilityTier::Edge;
            self.db
                .query_rows(query.clone(), opts, table, wait_for_coverage)
                .await?
        };
        let db = self.db.inner.borrow().backend_clone()?;
        self.db
            .query_decoder
            .core_rows_to_query_results(&db, &query, rows)
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
        {
            self.reject_updated_at_override("inserts")?;
            let row_values = self.core_ordered_values(table, &values)?;
            let cells = self.core_cells(table, values)?;
            if let Some(transaction_id) = self
                .write_context
                .as_ref()
                .and_then(|ctx| ctx.transaction_id)
            {
                let row_id = self.db.stage_insert(
                    transaction_id,
                    table.to_string(),
                    object_id.into(),
                    cells,
                )?;
                Ok((row_id, row_values, None))
            } else {
                let (row_id, tx_id) = self.db.insert(
                    table.to_string(),
                    object_id.into(),
                    cells,
                    self.write_identity()?,
                )?;
                let transaction_id = core_batch_id(tx_id);
                Ok((row_id, row_values, Some(transaction_id)))
            }
        }
    }

    /// Create or update a row using a caller-supplied UUID.
    pub fn upsert(
        &self,
        table: &str,
        object_id: Uuid,
        values: HashMap<String, Value>,
    ) -> Result<Option<TransactionId>> {
        {
            let cells = self.core_cells(table, values)?;
            let updated_at = self.write_updated_at()?;
            if let Some(transaction_id) = self
                .write_context
                .as_ref()
                .and_then(|ctx| ctx.transaction_id)
            {
                self.db
                    .stage_upsert(transaction_id, table.to_string(), object_id, cells)?;
                Ok(None)
            } else {
                let tx_id = self.db.upsert(
                    table.to_string(),
                    object_id,
                    cells,
                    self.write_identity()?,
                    updated_at,
                )?;
                Ok(Some(core_batch_id(tx_id)))
            }
        }
    }

    /// Update a row.
    pub fn update(
        &self,
        object_id: ObjectId,
        updates: Vec<(String, Value)>,
    ) -> Result<Option<TransactionId>> {
        {
            let table = self
                .db
                .inner
                .borrow()
                .row_tables
                .get(&object_id)
                .cloned()
                .ok_or_else(|| {
                    JazzError::Write(
                        "update requires a row created or observed by this client".to_string(),
                    )
                })?;
            let cells = self.core_cells(&table, updates.into_iter().collect())?;
            let updated_at = self.write_updated_at()?;
            if let Some(transaction_id) = self
                .write_context
                .as_ref()
                .and_then(|ctx| ctx.transaction_id)
            {
                self.db.stage_update(transaction_id, object_id, cells)?;
                Ok(None)
            } else {
                let tx_id = self
                    .db
                    .update(object_id, cells, self.write_identity()?, updated_at)?;
                Ok(Some(core_batch_id(tx_id)))
            }
        }
    }

    /// Delete a row.
    pub fn delete(&self, object_id: ObjectId) -> Result<Option<TransactionId>> {
        {
            self.reject_updated_at_override("deletes")?;
            if let Some(transaction_id) = self
                .write_context
                .as_ref()
                .and_then(|ctx| ctx.transaction_id)
            {
                self.db.stage_delete(transaction_id, object_id)?;
                Ok(None)
            } else {
                let tx_id = self.db.delete(object_id, self.write_identity()?)?;
                Ok(Some(core_batch_id(tx_id)))
            }
        }
    }

    /// Begin a transaction and return a transaction-scoped client handle.
    ///
    /// Mutations issued through the returned handle are staged locally and are
    /// not visible to ordinary reads until the transaction is committed and
    /// accepted by the authority.
    pub fn begin_transaction(&self) -> Result<JazzTransaction> {
        let author = self.write_identity()?;
        let transaction_id = self.db.begin_transaction(author)?;
        // Keep an explicit session/attribution context when adding the
        // transaction id. In particular, a backend connection is SYSTEM by
        // default, but `for_session(..).begin_transaction()` must continue to
        // evaluate and author as that session throughout staging and commit.
        let write_context = self
            .write_context
            .clone()
            .unwrap_or_default()
            .with_transaction_id(transaction_id);
        let client = self.with_write_context(write_context);
        Ok(JazzTransaction {
            transaction_id,
            client,
        })
    }

    /// Commit an open transaction by transaction id.
    pub fn commit_transaction(&self, transaction_id: OpenTransactionId) -> Result<TransactionId> {
        self.db.commit_transaction(transaction_id)
    }

    /// Roll back an open transaction by transaction id.
    ///
    /// Returns whether a local batch record existed for the transaction.
    pub fn rollback_transaction(&self, transaction_id: OpenTransactionId) -> Result<bool> {
        self.db.rollback_transaction(transaction_id)
    }

    pub async fn wait_for_transaction(
        &self,
        transaction_id: TransactionId,
        tier: DurabilityTier,
    ) -> Result<()> {
        self.db.wait_for_transaction(transaction_id, tier).await
    }

    /// Unsubscribe from a subscription.
    pub async fn unsubscribe(&self, _handle: SubscriptionHandle) -> Result<()> {
        Ok(())
    }

    /// Get the current schema.
    pub fn schema(&self) -> Result<Schema> {
        Ok(self.db.query_decoder.schema.as_ref().clone())
    }

    /// Check if connected to server.
    pub fn is_connected(&self) -> bool {
        self.db.inner.borrow().upstream.is_some()
    }

    /// Create a client that uses the given write context for mutations.
    pub fn with_write_context(&self, write_context: WriteContext) -> JazzClient {
        JazzClient {
            default_session: self.default_session.clone(),
            write_context: Some(write_context),
            db: self.db.clone(),
        }
    }

    /// Create a session-scoped client for backend operations.
    pub fn for_session(&self, session: Session) -> JazzClient {
        self.with_write_context(WriteContext::from_session(session))
    }

    /// Shutdown this shared client context and release its resources.
    ///
    /// Every [`JazzClient`] clone for this context becomes unusable once
    /// shutdown starts. Concurrent shutdown calls wait for the same close. If
    /// the shutdown future is cancelled, the context remains terminal and its
    /// storage handle is released, but callers must not treat it as a clean
    /// close.
    pub async fn shutdown(self) -> Result<()> {
        self.db.close().await
    }
}

#[cfg(feature = "testing")]
impl JazzClient {
    pub fn client_id(&self) -> Option<ClientId> {
        None
    }

    pub async fn test_client(schema: Schema) -> crate::tools::JazzClient {
        let context = crate::tools::AppContext::test(schema);
        crate::tools::JazzClient::connect(context)
            .await
            .expect("connect local JazzClient")
    }

    pub(crate) fn disconnect_upstream_for_test(&self) -> bool {
        self.db.disconnect_upstream()
    }

    /// Wait for a durability tier using an exact timeout rather than the
    /// load-tolerant test multiplier.
    pub async fn wait_for_transaction_with_timeout_for_test(
        &self,
        transaction_id: TransactionId,
        tier: DurabilityTier,
        timeout: Duration,
    ) -> Result<()> {
        self.db.ensure_tick_driver_running()?;
        ClientDbInner::handle_wait_for_transaction(&self.db.inner, transaction_id, tier, timeout)
            .await
    }

    pub(crate) async fn reconnect_upstream_for_test(&self) -> Result<bool> {
        self.db.reconnect_upstream().await
    }
}

#[cfg(any(test, feature = "testing"))]
impl Drop for JazzClient {
    /// This is a simplified and synchronous implementation of `JazzClient.shutdown`
    /// that is good-enough for tests (so that we don't require an explicit
    /// `JazzClient.shutdown` at the end of each test case)
    fn drop(&mut self) {
        let _ = self;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::groove::storage::{Error as StorageError, StorageFactory, StorageFuture};
    use crate::ids::NodeUuid;
    use crate::tools::AppId;
    use crate::tools::public_schema::Schema;
    use crate::tools::{ClientStorage, ColumnType, SchemaBuilder, TableSchema};
    use serde_json::json;
    use tempfile::TempDir;

    #[derive(Debug)]
    struct YieldingStorageFactory;

    impl StorageFactory for YieldingStorageFactory {
        fn open(
            &self,
            _path: std::path::PathBuf,
            column_families: Vec<String>,
            _codec_profile: crate::groove::storage::StorageCodecProfile,
        ) -> StorageFuture<'_, std::result::Result<CoreStorage, StorageError>> {
            Box::pin(async move {
                // A target storage factory may need an executor turn before it
                // can hand a boxed store back to the facade.
                tokio::task::yield_now().await;
                let refs = column_families
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                Ok(CoreStorage::new(CoreMemoryStorage::new(&refs)?))
            })
        }
    }

    #[test]
    fn query_runtime_waker_enqueues_one_immediate_owner_turn() {
        let scheduler = TickSchedulerImpl::default();
        assert_eq!(scheduler.take(), None);
        let waker = scheduler
            .query_runtime_waker()
            .expect("client scheduler provides cold-query wake bridge");
        waker.wake_by_ref();
        assert_eq!(scheduler.take(), Some(TickUrgency::Immediate));
        assert_eq!(scheduler.take(), None, "waking does not create a hot loop");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn persistent_storage_open_yields_without_sync_polling() {
        let temp_dir = TempDir::new().expect("temp client dir");
        let mut context = make_offline_context_with_storage(
            AppId::from_name("yielding-storage-factory"),
            temp_dir.path().to_path_buf(),
            declared_todo_schema(),
            ClientStorage::Persistent,
        );
        context.storage_factory = Some(Arc::new(YieldingStorageFactory));

        let client = tokio::time::timeout(Duration::from_secs(1), JazzClient::connect(context))
            .await
            .expect("storage factory must resume through the async connect owner turn")
            .expect("connect yielding persistent storage");
        client
            .shutdown()
            .await
            .expect("close yielding persistent storage");
    }

    /// This binding-boundary lowering is asserted directly because its internal
    /// overlay bit is not independently observable without conflating it with
    /// remote transport timing. Write durability remains independent.
    #[test]
    fn read_tier_lowers_without_changing_write_durability() {
        assert_eq!(
            ReadTier::LocalFirst.legacy_durability_tier(),
            DurabilityTier::Local
        );
        assert_eq!(
            ReadTier::Remote.legacy_durability_tier(),
            DurabilityTier::EdgeServer
        );
        assert_eq!(
            ReadTier::RemoteIfPossible.legacy_durability_tier(),
            DurabilityTier::EdgeServer,
            "the native facade has no explicit offline boundary"
        );
        assert_eq!(
            JazzClient::core_read_opts_for_read_tier(ReadTier::LocalFirst).local_updates,
            CoreLocalUpdates::Immediate
        );
        assert_eq!(
            JazzClient::core_read_opts_for_read_tier(ReadTier::Remote).local_updates,
            CoreLocalUpdates::Deferred
        );
        assert_eq!(
            JazzClient::core_read_opts_for_read_tier(ReadTier::RemoteIfPossible).local_updates,
            CoreLocalUpdates::Immediate
        );
        assert_eq!(
            core_legacy_read_tier(DurabilityTier::Local),
            CoreDurabilityTier::Local
        );
        assert_eq!(
            core_legacy_read_tier(DurabilityTier::EdgeServer),
            CoreDurabilityTier::Global,
            "legacy EdgeServer reads retain the ordinary settled remote view"
        );
        assert_eq!(
            core_write_tier(DurabilityTier::Local),
            CoreDurabilityTier::Local
        );
        assert_eq!(
            core_write_tier(DurabilityTier::EdgeServer),
            CoreDurabilityTier::Edge
        );
        assert_eq!(
            core_write_tier(DurabilityTier::GlobalServer),
            CoreDurabilityTier::Global
        );
    }

    fn declared_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean),
            )
            .build()
    }

    fn make_offline_context(
        app_id: AppId,
        data_dir: std::path::PathBuf,
        schema: Schema,
    ) -> AppContext {
        AppContext {
            app_id,
            client_id: None,
            schema,
            server_url: String::new(),
            data_dir,
            storage: ClientStorage::default(),
            storage_factory: Some(std::sync::Arc::new(
                jazz_storage_rocksdb::RocksDbStorageFactory,
            )),
            jwt_token: None,
            backend_secret: None,
            admin_secret: None,
        }
    }

    fn make_offline_context_with_storage(
        app_id: AppId,
        data_dir: std::path::PathBuf,
        schema: Schema,
        storage: ClientStorage,
    ) -> AppContext {
        let mut context = AppContext {
            storage,
            ..make_offline_context(app_id, data_dir, schema)
        };
        if storage == ClientStorage::Persistent {
            context.storage_factory = Some(std::sync::Arc::new(
                jazz_storage_rocksdb::RocksDbStorageFactory,
            ));
        }
        context
    }

    fn make_test_jwt(sub: &str, claims: serde_json::Value) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"alg":"none","typ":"JWT"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({
                "iss": "https://issuer.example",
                "sub": sub,
                "claims": claims,
            }))
            .expect("serialize jwt payload"),
        );
        format!("{header}.{payload}.sig")
    }

    fn make_test_jwt_without_claims(sub: &str) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"alg":"none","typ":"JWT"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({
                "iss": "https://issuer.example",
                "sub": sub,
            }))
            .expect("serialize jwt payload"),
        );
        format!("{header}.{payload}.sig")
    }
    #[test]
    fn core_integer_bridge_uses_signed_value_domain() {
        let core_value =
            public_to_core_value(Value::Integer(-1)).expect("negative i32 should encode for core");

        assert_eq!(core_value, CoreValue::I32(-1));
        assert_eq!(
            core_to_public_value_for_column_type(core_value, &ColumnType::Integer)
                .expect("decode signed i32"),
            Value::Integer(-1)
        );
        assert_eq!(
            public_to_core_value(Value::Integer(0)).expect("encode zero"),
            CoreValue::I32(0)
        );
    }

    #[test]
    fn client_session_claim_numbers_match_admission_classification() {
        assert_eq!(
            json_claim_to_core_value(json!(7)).unwrap(),
            CoreValue::U64(7)
        );
        assert_eq!(
            json_claim_to_core_value(json!(-7)).unwrap(),
            CoreValue::I64(-7)
        );
        assert_eq!(
            json_claim_to_core_value(json!(9_007_199_254_740_992_u64)).unwrap(),
            CoreValue::U64(9_007_199_254_740_992)
        );
        assert!(json_claim_to_core_value(json!({ "role": "admin" })).is_err());
    }

    #[test]
    fn client_session_preserves_provider_subject_and_adds_logical_user_identity() {
        let session = Session::new(CoreAuthorSubject::LOCAL_FIRST_ISSUER, "trusted-user")
            .with_auth_mode(crate::tools::public_api::session::AuthMode::LocalFirst)
            .with_claims(json!({
                "sub": "spoofed-subject",
                "user_id": "spoofed-user",
                "user": "provider-user",
                "authMode": "external",
            }));

        let claims = session_claims_to_core_claims(&session).unwrap();
        assert_eq!(
            claims.get(&crate::query::provider_claim_key("sub")),
            Some(&CoreValue::String("trusted-user".to_owned()))
        );
        assert_eq!(
            claims.get(&crate::query::provider_claim_key("user_id")),
            Some(&CoreValue::String("spoofed-user".to_owned()))
        );
        assert_eq!(
            claims.get(&crate::query::provider_claim_key("user")),
            Some(&CoreValue::String("provider-user".to_owned()))
        );
        assert_eq!(
            claims.get("authMode"),
            Some(&CoreValue::String("local-first".to_owned()))
        );
        assert_eq!(
            claims.get("user"),
            Some(&CoreValue::String(
                CoreAuthorSubject::reserved(CoreAuthorSubject::LOCAL_FIRST_ISSUER, "trusted-user")
                    .unwrap()
                    .canonical()
                    .to_owned()
            ))
        );
    }

    // This narrow internal test is necessary because the wire crossing is an
    // impossible state to inject through the public client API: it requires a
    // local aggregate retraction racing a relay replacement. The public
    // aggregate subscription tests cover ordinary delivery around it.
    #[test]
    fn aggregate_replacement_for_absent_member_is_normalized_to_an_add() {
        let held = OutputOccurrenceId::single_source(ObjectId::from_uuid(Uuid::from_u128(1)));
        let crossed = OutputOccurrenceId::single_source(ObjectId::from_uuid(Uuid::from_u128(2)));
        let (added, updated) = normalize_subscription_updates(
            std::collections::BTreeMap::from([(held.clone(), 7)]),
            Vec::<(OutputOccurrenceId, Option<usize>)>::new(),
            vec![(crossed.clone(), None)],
            |row| &row.0,
            |row, previous_index| row.1 = Some(previous_index),
        );

        assert_eq!(added, vec![(crossed, None)]);
        assert!(updated.is_empty());

        let (added, updated) = normalize_subscription_updates(
            std::collections::BTreeMap::from([(held.clone(), 7)]),
            Vec::<(OutputOccurrenceId, Option<usize>)>::new(),
            vec![(held.clone(), Some(7))],
            |row| &row.0,
            |row, previous_index| row.1 = Some(previous_index),
        );
        assert!(added.is_empty());
        assert_eq!(updated, vec![(held.clone(), Some(7))]);

        let (added, updated) = normalize_subscription_updates(
            std::collections::BTreeMap::from([(held.clone(), 7)]),
            vec![(held.clone(), None)],
            Vec::<(OutputOccurrenceId, Option<usize>)>::new(),
            |row| &row.0,
            |row, previous_index| row.1 = Some(previous_index),
        );
        assert!(added.is_empty());
        assert_eq!(updated, vec![(held, Some(7))]);
    }

    #[test]
    fn reset_snapshot_preserves_retained_updates_and_recovers_absent_removals() {
        #[derive(Clone, Debug, PartialEq, Eq)]
        struct SnapshotRow {
            id: OutputOccurrenceId,
            payload: &'static str,
            index: usize,
            previous_index: Option<usize>,
        }

        let a = OutputOccurrenceId::single_source(ObjectId::from_uuid(Uuid::from_u128(1)));
        let b = OutputOccurrenceId::single_source(ObjectId::from_uuid(Uuid::from_u128(2)));
        let c = OutputOccurrenceId::single_source(ObjectId::from_uuid(Uuid::from_u128(3)));
        let current = vec![
            SnapshotRow {
                id: a.clone(),
                payload: "old A",
                index: 0,
                previous_index: None,
            },
            SnapshotRow {
                id: b.clone(),
                payload: "old B",
                index: 1,
                previous_index: None,
            },
        ];
        let replacement = vec![
            SnapshotRow {
                id: c.clone(),
                payload: "new C",
                index: 0,
                previous_index: None,
            },
            SnapshotRow {
                id: a.clone(),
                payload: "new A",
                index: 1,
                previous_index: None,
            },
        ];
        let replacement_ids = replacement.iter().map(|row| row.id.clone()).collect();
        let removed_indices = reset_absent_row_indices(
            &current,
            &replacement_ids,
            &std::collections::BTreeSet::new(),
            |row| &row.id,
        );
        assert_eq!(removed_indices, vec![1]);
        assert_eq!(current[removed_indices[0]].id, b);
        assert_eq!(current[removed_indices[0]].payload, "old B");

        let surviving = std::collections::BTreeMap::from([(a.clone(), 0)]);
        let (added, updated) = normalize_subscription_updates(
            surviving,
            replacement,
            Vec::new(),
            |row| &row.id,
            |row, previous_index| row.previous_index = Some(previous_index),
        );
        assert_eq!(added.len(), 1);
        assert_eq!(
            (&added[0].id, added[0].payload, added[0].index),
            (&c, "new C", 0)
        );
        assert_eq!(updated.len(), 1);
        assert_eq!(
            (
                &updated[0].id,
                updated[0].payload,
                updated[0].previous_index,
                updated[0].index,
            ),
            (&a, "new A", Some(0), 1)
        );
        assert_eq!(added.len() + updated.len() + removed_indices.len(), 3);
    }

    #[test]
    fn semantic_update_filter_is_indexed_and_preserves_real_changes() {
        #[derive(Clone, Debug, PartialEq, Eq)]
        struct TestRow {
            id: u8,
            index: usize,
            payload: &'static str,
            provenance: u8,
        }

        let previous = TestRow {
            id: b'A',
            index: 0,
            payload: "same",
            provenance: 1,
        };
        let previous_by_occurrence = std::collections::BTreeMap::from([(b'A', &previous)]);
        let exact = previous.clone();
        let moved = TestRow {
            index: 1,
            ..previous.clone()
        };
        let content_changed = TestRow {
            payload: "changed",
            ..previous.clone()
        };
        let provenance_changed = TestRow {
            provenance: 2,
            ..previous.clone()
        };
        let unknown_first = TestRow {
            id: b'B',
            ..previous.clone()
        };
        let unknown_second = unknown_first.clone();
        let mut updates = vec![
            exact,
            moved.clone(),
            content_changed.clone(),
            provenance_changed.clone(),
            unknown_first.clone(),
            unknown_second.clone(),
        ];

        retain_changed_subscription_updates(
            &mut updates,
            &previous_by_occurrence,
            |row| &row.id,
            |old, new| {
                old.index == new.index
                    && old.payload == new.payload
                    && old.provenance == new.provenance
            },
        );

        assert_eq!(
            updates,
            vec![
                moved,
                content_changed,
                provenance_changed,
                unknown_first,
                unknown_second,
            ]
        );
        assert_eq!(
            updates.iter().filter(|row| row.id == b'B').count(),
            2,
            "filtering semantic no-ops must not deduplicate distinct update entries"
        );
    }

    #[test]
    fn reset_survivor_classification_scales_by_index_lookup() {
        static COMPARISONS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

        #[derive(Clone, Debug, PartialEq, Eq)]
        struct CountingKey(u32);

        impl PartialOrd for CountingKey {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for CountingKey {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                COMPARISONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.0.cmp(&other.0)
            }
        }

        const ROWS: usize = 1_024;
        let current = (0..ROWS)
            .map(|index| (CountingKey(index as u32), index))
            .collect::<Vec<_>>();
        let removed = current
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        COMPARISONS.store(0, std::sync::atomic::Ordering::Relaxed);

        let surviving = surviving_subscription_rows(&current, &removed, |row| &row.0, |row| row.1);
        let comparisons = COMPARISONS.load(std::sync::atomic::Ordering::Relaxed);

        assert!(surviving.is_empty());
        assert!(
            comparisons < ROWS * 64,
            "reset survivor classification regressed from indexed lookup: {comparisons} comparisons for {ROWS} rows"
        );
    }

    #[test]
    fn default_session_from_context_uses_jwt_claims_for_user_clients() {
        let app_id = AppId::from_name("client-jwt-session");
        let mut context = make_offline_context(
            app_id,
            TempDir::new().expect("tempdir").keep(),
            declared_todo_schema(),
        );
        context.jwt_token = Some(make_test_jwt("alice", json!({ "join_code": "secret-123" })));

        let session = default_session_from_context(&context).expect("derive session from jwt");
        assert_eq!(session.user_id, "alice");
        assert_eq!(session.claims["join_code"], "secret-123");
    }

    // These internal tests are necessary because the distinction is made while
    // decoding the unverified JWT, before a client connection can expose it.
    #[test]
    fn session_from_unverified_jwt_defaults_absent_claims_to_an_empty_object() {
        let session = session_from_unverified_jwt(&make_test_jwt_without_claims("alice"))
            .expect("derive session from jwt without application claims");

        assert_eq!(session.claims, json!({}));
        assert!(session_claims_to_core_claims(&session).is_ok());
    }

    #[test]
    fn session_from_unverified_jwt_preserves_explicit_non_object_claims() {
        let session = session_from_unverified_jwt(&make_test_jwt("alice", json!(null)))
            .expect("derive session from jwt with explicit null claims");

        let error = session_claims_to_core_claims(&session)
            .expect_err("explicit null application claims must be rejected");
        assert!(
            matches!(error, JazzError::Connection(message) if message == "JWT claims payload must be a JSON object")
        );
    }

    #[test]
    fn default_session_from_context_skips_backend_capable_clients() {
        let app_id = AppId::from_name("client-backend-session");
        let mut context = make_offline_context(
            app_id,
            TempDir::new().expect("tempdir").keep(),
            declared_todo_schema(),
        );
        context.jwt_token = Some(make_test_jwt("alice", json!({ "role": "user" })));
        context.backend_secret = Some("backend-secret".to_string());

        assert!(
            default_session_from_context(&context).is_none(),
            "backend/admin clients should keep using explicit session scopes"
        );
    }

    #[test]
    fn backend_context_uses_system_connection_author_for_explicit_session_writes() {
        let temp_dir = TempDir::new().expect("temp dir");
        let mut context = make_offline_context(
            AppId::from_name("backend-system-connection-author"),
            temp_dir.path().to_path_buf(),
            declared_todo_schema(),
        );
        context.backend_secret = Some("backend-secret".to_owned());

        let identity = core_identity(&context, default_session_from_context(&context).as_ref())
            .expect("derive backend identity");
        assert_eq!(identity.author, CoreAuthorSubject::SYSTEM);
    }

    /// A strict remote one-shot must not return Alice's ambient local row while
    /// offline: it waits for its transient subscription's exact authority
    /// closure. Dropping that waiting caller retires the owned coverage.
    ///
    /// alice ──local write──► offline local store
    /// alice ──Remote read──► transient subscription ──wait──► authority
    #[tokio::test(flavor = "current_thread")]
    async fn strict_remote_one_shot_uses_transient_subscription_not_ambient_all() {
        let client = JazzClient::connect(make_offline_context(
            AppId::from_name("strict-remote-one-shot-subscription"),
            TempDir::new().expect("tempdir").keep(),
            declared_todo_schema(),
        ))
        .await
        .expect("connect offline client");
        client
            .upsert(
                "todos",
                Uuid::from_u128(0x5151),
                HashMap::from([
                    ("title".to_owned(), Value::Text("local only".to_owned())),
                    ("completed".to_owned(), Value::Boolean(false)),
                ]),
            )
            .expect("write local row");

        let mut query =
            Box::pin(client.query_with_read_tier(Query::from("todos"), ReadTier::Remote));
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        assert!(
            matches!(query.as_mut().poll(&mut context), std::task::Poll::Pending),
            "strict remote must wait rather than silently reading local storage"
        );

        let backend = client
            .db
            .inner
            .borrow()
            .backend_clone()
            .expect("client is open");
        assert!(
            backend.0.query_coverage_attachment_counts_for_test().0 > 0,
            "the pending read must own transient upstream coverage"
        );

        drop(query);
        backend
            .0
            .tick()
            .await
            .expect("drain dropped one-shot cleanup");

        assert_eq!(
            backend.0.query_coverage_attachment_counts_for_test(),
            (0, 0),
            "cancelling the remote one-shot must release its coverage owner"
        );
    }

    // This is an internal fault-injection test because a real fatal tick error
    // means corrupt local state; creating that through the public API would
    // require deliberately corrupting a storage backend. The assertion itself
    // is public: a normal `JazzClient::query` must report the stopped driver.
    #[tokio::test(flavor = "current_thread")]
    async fn fatal_tick_driver_failure_is_reported_to_callers() {
        let client = JazzClient::connect(make_offline_context(
            AppId::from_name("fatal-tick-driver-error"),
            TempDir::new().expect("tempdir").keep(),
            declared_todo_schema(),
        ))
        .await
        .expect("connect offline client");
        let error = CoreDbError {
            code: CoreDbErrorCode::Storage,
            message: "simulated local storage corruption".to_string(),
        };

        assert_eq!(
            classify_tick_driver_error(&error),
            TickDriverErrorClass::Fatal,
            "storage faults must not enter the retry loop"
        );
        client
            .db
            .inner
            .borrow_mut()
            .record_tick_driver_failure(error.to_string());

        let error = client
            .query_with_read_tier(Query::from("todos"), ReadTier::LocalFirst)
            .await
            .expect_err("a stopped tick driver must be visible to the caller");
        assert!(
            matches!(error, JazzError::Sync(ref message) if message.contains("client tick driver stopped") && message.contains("local storage corruption")),
            "unexpected fatal tick-driver error: {error}"
        );
    }

    #[tokio::test]
    async fn offline_persistent_client_rehydrates_rows_from_core_storage() {
        let data_dir = TempDir::new().expect("temp client dir");
        let app_id = AppId::from_name("client-core-row-rehydrate");
        let context = make_offline_context_with_storage(
            app_id,
            data_dir.path().to_path_buf(),
            declared_todo_schema(),
            ClientStorage::Persistent,
        );

        let client = JazzClient::connect(context.clone())
            .await
            .expect("connect offline persistent client");
        let (row_id, _values, transaction_id) = client
            .insert(
                "todos",
                crate::row_input!("title" => "rehydrated", "completed" => false),
            )
            .expect("insert offline persistent row");
        client
            .wait_for_transaction(
                transaction_id.expect("ordinary mutation commits immediately"),
                DurabilityTier::Local,
            )
            .await
            .expect("wait for local durability");
        client.shutdown().await.expect("gracefully close client");

        let restarted = JazzClient::connect(context)
            .await
            .expect("reconnect offline persistent client");
        let rows = restarted
            .query_with_read_tier(Query::from("todos"), ReadTier::LocalFirst)
            .await
            .expect("query rehydrated rows");

        assert_eq!(
            rows,
            vec![(
                row_id,
                vec![Value::Text("rehydrated".to_string()), Value::Boolean(false)]
            )]
        );
    }

    /// A retained public subscription must become terminal during shutdown so
    /// that alice can reopen the same persistent client directory immediately.
    ///
    /// ```text
    /// alice ──subscribe──► local core stream
    /// alice ──shutdown───► cancel forwarder ──► close RocksDB
    /// alice ──reopen─────► same directory
    /// ```
    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_terminates_retained_subscription_before_persistent_reopen() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let data_dir = TempDir::new().expect("temp client dir");
                let app_id = AppId::from_name("retained-subscription-persistent-reopen");
                let context = make_offline_context_with_storage(
                    app_id,
                    data_dir.path().to_path_buf(),
                    declared_todo_schema(),
                    ClientStorage::Persistent,
                );
                let client = JazzClient::connect(context.clone())
                    .await
                    .expect("connect offline client");
                let mut subscription = client
                    .subscribe_with_read_tier(Query::from("todos"), ReadTier::LocalFirst)
                    .await
                    .expect("subscribe before shutdown");

                client
                    .shutdown()
                    .await
                    .expect("close retained subscription");

                tokio::time::timeout(Duration::from_secs(1), async {
                    while subscription.next().await.is_some() {}
                })
                .await
                .expect("retained public stream must close after shutdown");

                let restarted = JazzClient::connect(context)
                    .await
                    .expect("reopen persistent directory after retained stream shutdown");
                restarted
                    .shutdown()
                    .await
                    .expect("close reopened persistent client");
            })
            .await;
    }

    /// A failed public subscription admission must complete its facade
    /// lifecycle before alice shuts down and reopens persistent storage.
    ///
    /// ```text
    /// alice ──subscribe missing table──► admission error
    /// alice ──shutdown─────────────────► close RocksDB
    /// alice ──reopen───────────────────► same directory
    /// ```
    #[tokio::test(flavor = "current_thread")]
    async fn failed_subscription_admission_is_terminal_before_persistent_reopen() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let data_dir = TempDir::new().expect("temp client dir");
                let app_id = AppId::from_name("failed-subscription-admission-reopen");
                let context = make_offline_context_with_storage(
                    app_id,
                    data_dir.path().to_path_buf(),
                    declared_todo_schema(),
                    ClientStorage::Persistent,
                );
                let client = JazzClient::connect(context.clone())
                    .await
                    .expect("connect offline client");

                let error = match client
                    .subscribe_with_read_tier(Query::from("missing"), ReadTier::LocalFirst)
                    .await
                {
                    Ok(_) => panic!("missing-table subscription must fail"),
                    Err(error) => error,
                };
                assert!(
                    matches!(error, JazzError::Query(_)),
                    "unexpected missing-table subscription error: {error}"
                );

                client
                    .shutdown()
                    .await
                    .expect("close failed admission client");
                let restarted = JazzClient::connect(context)
                    .await
                    .expect("reopen persistent directory after failed admission");
                restarted
                    .shutdown()
                    .await
                    .expect("close reopened persistent client");
            })
            .await;
    }

    /// Shutdown must cancel and wait for the admission interval between the
    /// facade accepting alice's subscription and core creating its stream.
    ///
    /// This is a narrow internal receipt because the public offline adapter
    /// completes core admission synchronously; the registry is the exact
    /// observable-to-shutdown boundary that makes the concurrent interleaving
    /// deterministic without mocking storage or transport.
    ///
    /// ```text
    /// alice ──admit subscription──► core subscribe is in flight
    /// alice ──shutdown────────────► cancellation ──► wait for admission exit
    /// ```
    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_cancels_and_waits_for_in_flight_subscription_admission() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let data_dir = TempDir::new().expect("temp client dir");
                let app_id = AppId::from_name("in-flight-subscription-admission-shutdown");
                let context = make_offline_context_with_storage(
                    app_id,
                    data_dir.path().to_path_buf(),
                    declared_todo_schema(),
                    ClientStorage::Persistent,
                );
                let client = JazzClient::connect(context.clone())
                    .await
                    .expect("connect offline client");
                let (mut cancellation, completion) = client
                    .db
                    .inner
                    .borrow_mut()
                    .admit_subscription()
                    .expect("admit in-flight subscription");

                let shutdown = tokio::task::spawn_local(client.shutdown());
                tokio::time::timeout(Duration::from_secs(1), &mut cancellation)
                    .await
                    .expect("shutdown must cancel in-flight admission")
                    .expect("shutdown cancellation sender must stay alive");
                assert!(
                    !shutdown.is_finished(),
                    "shutdown must wait for the in-flight admission completion"
                );

                drop(completion);
                shutdown
                    .await
                    .expect("shutdown task must not panic")
                    .expect("shutdown after admitted subscription");

                let restarted = JazzClient::connect(context)
                    .await
                    .expect("reopen persistent directory after in-flight admission");
                restarted
                    .shutdown()
                    .await
                    .expect("close reopened persistent client");
            })
            .await;
    }

    /// Cancelling shutdown while it drains alice's in-flight admission must
    /// leave retained facades terminal rather than stranded in `Closing`.
    ///
    /// This is a narrow internal receipt for task cancellation during the
    /// drain await; public adapters cannot deterministically suspend that
    /// interval without mocking transport or storage.
    ///
    /// ```text
    /// alice ──admit subscription──► shutdown waits for admission exit
    /// alice ──abort shutdown──────► retained clone observes terminal failure
    /// alice ──reopen─────────────► same directory
    /// ```
    #[tokio::test(flavor = "current_thread")]
    async fn aborting_shutdown_during_subscription_drain_notifies_retained_clone() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let data_dir = TempDir::new().expect("temp client dir");
                let app_id = AppId::from_name("abort-subscription-drain-shutdown");
                let context = make_offline_context_with_storage(
                    app_id,
                    data_dir.path().to_path_buf(),
                    declared_todo_schema(),
                    ClientStorage::Persistent,
                );
                let client = JazzClient::connect(context.clone())
                    .await
                    .expect("connect offline client");
                let retained_clone = client.clone();
                let (mut cancellation, admission_completion) = client
                    .db
                    .inner
                    .borrow_mut()
                    .admit_subscription()
                    .expect("admit in-flight subscription");

                let shutdown = tokio::task::spawn_local(client.shutdown());
                tokio::time::timeout(Duration::from_secs(1), &mut cancellation)
                    .await
                    .expect("shutdown must cancel in-flight admission")
                    .expect("shutdown cancellation sender must stay alive");
                assert!(
                    !shutdown.is_finished(),
                    "shutdown must still be draining the in-flight admission"
                );

                shutdown.abort();
                let cancellation = shutdown
                    .await
                    .expect_err("aborted shutdown task must not complete normally");
                assert!(
                    cancellation.is_cancelled(),
                    "shutdown task must report task cancellation: {cancellation}"
                );
                drop(admission_completion);

                let error = tokio::time::timeout(Duration::from_secs(1), retained_clone.shutdown())
                    .await
                    .expect("retained clone shutdown must not hang after cancellation")
                    .expect_err("retained clone must observe terminal shutdown failure");
                assert!(
                    matches!(error, JazzError::Connection(ref message) if message.contains("shutdown previously failed")),
                    "unexpected retained-clone shutdown error: {error}"
                );

                let restarted = JazzClient::connect(context)
                    .await
                    .expect("reopen persistent directory after cancelled shutdown");
                restarted
                    .shutdown()
                    .await
                    .expect("close reopened persistent client");
            })
            .await;
    }

    // The successful core close boundary is not visible through the public
    // facade: RocksDB can release its lock when the final Rc drops even if a
    // regression bypasses `Db::close`. Keep this narrow internal receipt in
    // addition to the black-box retained-clone/reopen integration test.
    #[tokio::test]
    async fn shutdown_closes_the_shared_backend_once_and_retires_clones() {
        let client = JazzClient::connect(make_offline_context(
            AppId::from_name("shared-terminal-shutdown"),
            TempDir::new().expect("tempdir").keep(),
            declared_todo_schema(),
        ))
        .await
        .expect("connect offline client");
        let retained_clone = client.clone();
        let second_shutdown = client.clone();
        let close_count_before = completed_backend_close_count();

        let (first, second) = tokio::join!(client.shutdown(), second_shutdown.shutdown());
        first.expect("first shutdown succeeds");
        second.expect("concurrent shutdown waits for the shared close");
        assert_eq!(
            completed_backend_close_count(),
            close_count_before + 1,
            "shutdown must complete the core close boundary exactly once"
        );

        let error = retained_clone
            .query_with_read_tier(Query::from("todos"), ReadTier::LocalFirst)
            .await
            .expect_err("retained clone must not operate after shared shutdown");
        assert!(
            matches!(error, JazzError::Connection(ref message) if message == "client is shut down"),
            "unexpected retained-clone error: {error}"
        );
        retained_clone
            .shutdown()
            .await
            .expect("completed shutdown remains idempotent for retained clones");
    }

    #[tokio::test]
    async fn offline_memory_client_does_not_create_core_rocksdb_dir() {
        let data_dir = TempDir::new().expect("temp client dir");
        let app_id = AppId::from_name("client-core-memory");
        let context = make_offline_context_with_storage(
            app_id,
            data_dir.path().to_path_buf(),
            declared_todo_schema(),
            ClientStorage::Memory,
        );

        let client = JazzClient::connect(context)
            .await
            .expect("connect offline memory client");
        let (_row_id, _values, transaction_id) = client
            .insert(
                "todos",
                crate::row_input!("title" => "memory", "completed" => false),
            )
            .expect("insert offline memory row");
        client
            .wait_for_transaction(
                transaction_id.expect("ordinary mutation commits immediately"),
                DurabilityTier::Local,
            )
            .await
            .expect("wait for local durability");
        drop(client);

        assert!(
            !data_dir.path().join("jazz-core.rocksdb").exists(),
            "memory storage should not create a RocksDB data directory"
        );
    }

    #[tokio::test]
    async fn transaction_wait_errors_use_transaction_vocabulary() {
        let client = JazzClient::connect(make_offline_context(
            AppId::from_name("transaction-wait-error-vocabulary"),
            TempDir::new().expect("temp client dir").keep(),
            declared_todo_schema(),
        ))
        .await
        .expect("connect offline client");
        let unknown = TransactionId::from_committed_tx(CoreTxId::new(
            crate::time::TxTime::from(42),
            NodeUuid::from_bytes([0x42; 16]),
        ));
        let unknown_error = client
            .wait_for_transaction_with_timeout_for_test(
                unknown,
                DurabilityTier::EdgeServer,
                Duration::ZERO,
            )
            .await
            .expect_err("unknown transaction must fail");
        assert!(
            matches!(unknown_error, JazzError::Sync(ref message) if message == &format!("unknown transaction {unknown}")),
            "unexpected unknown-transaction error: {unknown_error}"
        );

        let (_row_id, _values, transaction_id) = client
            .insert(
                "todos",
                crate::row_input!("title" => "pending", "completed" => false),
            )
            .expect("insert offline row");
        let transaction_id = transaction_id.expect("ordinary mutation commits immediately");
        let timeout_error = client
            .wait_for_transaction_with_timeout_for_test(
                transaction_id,
                DurabilityTier::EdgeServer,
                Duration::ZERO,
            )
            .await
            .expect_err("offline transaction cannot reach edge");
        assert!(
            matches!(timeout_error, JazzError::Sync(ref message) if message == "timed out waiting for transaction to reach EdgeServer"),
            "unexpected transaction timeout error: {timeout_error}"
        );
        assert_eq!(
            transaction_rejected_before_tier_message(
                DurabilityTier::EdgeServer,
                &CoreRejectionReason::AuthorizationDenied,
            ),
            "transaction was rejected before reaching EdgeServer durability: authorization_denied",
        );
    }

    #[test]
    fn public_provenance_uses_unix_milliseconds_without_touching_other_timestamps() {
        use crate::groove::records::ValueType;
        use crate::time::TxTime;

        let physical_ms = 1_777_777_777_777;
        let created = TxTime::new(physical_ms, 17);
        let provenance = core_row_provenance_to_public(crate::node::RowProvenance {
            created_by: CoreAuthorSubject::SYSTEM,
            created_at: physical_ms,
            updated_by: CoreAuthorSubject::SYSTEM,
            updated_at: physical_ms + 1,
        });
        assert_eq!(provenance.created_at, physical_ms);
        assert_eq!(provenance.updated_at, physical_ms + 1);

        let descriptor = crate::groove::records::RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("user_occurred_at", ValueType::U64),
            ("$createdAt", ValueType::U64),
            ("$updatedAt", ValueType::U64),
            ("tx_time", ValueType::U64),
        ]);
        let row_id = Uuid::from_u128(1);
        let raw = descriptor
            .create(&[
                CoreValue::Uuid(row_id),
                CoreValue::U64(42),
                CoreValue::U64(physical_ms),
                CoreValue::U64(physical_ms + 1),
                CoreValue::U64(created.0),
            ])
            .expect("encode current row");
        let row = crate::node::CurrentRow::new("todos", OwnedRecord::new(raw, descriptor.clone()));
        let encoded = public_subscription_record(&row).expect("encode public row");
        let values = BorrowedRecord::new(&encoded, &descriptor)
            .to_values()
            .expect("decode public row");
        assert_eq!(values[1], CoreValue::U64(42));
        assert_eq!(values[2], CoreValue::U64(physical_ms));
        assert_eq!(values[3], CoreValue::U64(physical_ms + 1));
        assert_eq!(values[4], CoreValue::U64(created.0));
    }

    #[tokio::test]
    async fn transaction_scoped_timestamp_override_is_rejected() {
        let client = JazzClient::test_client(declared_todo_schema()).await;
        let transaction = client.begin_transaction().expect("open transaction");
        let scoped = transaction.client().with_write_context(
            WriteContext::default()
                .with_transaction_id(transaction.transaction_id())
                .with_updated_at(1_700_000_000_001),
        );
        let error = scoped
            .upsert(
                "todos",
                Uuid::from_u128(7),
                HashMap::from([
                    ("title".to_owned(), Value::Text("no staging".to_owned())),
                    ("completed".to_owned(), Value::Boolean(false)),
                ]),
            )
            .expect_err("a staged write cannot silently discard updated_at");
        assert!(
            error
                .to_string()
                .contains("updated_at is not supported for transaction-scoped writes")
        );
        transaction.rollback().expect("rollback empty transaction");
    }

    #[tokio::test]
    async fn raw_core_provenance_predicates_and_public_results_use_ms() {
        use crate::query::{Query, col, gte, lit};

        let updated_at_ms = 1_777_777_777_777;
        let client = JazzClient::test_client(declared_todo_schema()).await;
        let writer =
            client.with_write_context(WriteContext::default().with_updated_at(updated_at_ms));
        writer
            .upsert(
                "todos",
                Uuid::from_u128(9),
                HashMap::from([
                    (
                        "title".to_owned(),
                        Value::Text("timestamp receipt".to_owned()),
                    ),
                    ("completed".to_owned(), Value::Boolean(false)),
                ]),
            )
            .expect("write with deterministic physical timestamp");
        let query = Query::from("todos")
            .filter(gte(col("$updatedAt"), lit(updated_at_ms)))
            .select(["$updatedAt"]);
        let results = client
            .query_results(query, Some(DurabilityTier::Local))
            .await
            .expect("query with physical-ms provenance predicate");
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].fields,
            vec![QueryResultField {
                name: "$updatedAt".to_owned(),
                value: Value::Timestamp(updated_at_ms),
            }]
        );
    }
}
