//! JazzClient implementation.

use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crate::batch_fate::BatchMode;
use crate::query_manager::manager::LocalUpdates as AlphaLocalUpdates;
use crate::query_manager::query::Query;
use crate::query_manager::session::{Session, WriteContext};
#[cfg(feature = "test-utils")]
use crate::query_manager::types::RowPolicyMode;
use crate::query_manager::types::{OrderedRowDelta, Schema, TableName, Value};
use crate::row_histories::BatchId;
use crate::runtime_core::ReadDurabilityOptions;
use crate::runtime_tokio::{SubscriptionHandle as RuntimeSubHandle, TokioRuntime};
use crate::schema_manager::{SchemaManager, rehydrate_schema_manager_from_catalogue};
use crate::server::direct_client::DirectCoreWebSocketTransport;
use crate::server::direct_schema::convert_alpha_schema;
#[cfg(all(feature = "sqlite", not(feature = "rocksdb")))]
use crate::storage::SqliteStorage;
use crate::storage::{MemoryStorage, Storage};
#[cfg(feature = "rocksdb")]
use crate::storage::{RocksDBStorage, StorageError};
use crate::sync_manager::{ClientId, DurabilityTier, OutboxEntry, SyncManager};
use crate::transport_auth::AuthConfig as WsAuthConfig;
use base64::Engine;
use jazz::db::{
    Db as CoreDb, DbConfig as CoreDbConfig, DbIdentity as CoreDbIdentity,
    LocalUpdates as CoreLocalUpdates, PeerConnection as CorePeerConnection,
    Propagation as CorePropagation, ReadOpts as CoreReadOpts,
    SubscriptionEvent as CoreSubscriptionEvent, WireTransportAdapter,
};
use jazz::groove::records::Value as CoreValue;
use jazz::groove::storage::MemoryStorage as CoreMemoryStorage;
use jazz::ids::{AuthorId as CoreAuthorId, NodeUuid as CoreNodeUuid, RowUuid as CoreRowUuid};
use jazz::tx::{DurabilityTier as CoreDurabilityTier, Fate as CoreFate, TxId as CoreTxId};
use serde::Deserialize;
use tokio::sync::{RwLock, mpsc};
use uuid::Uuid;

use crate::{
    AppContext, ClientStorage, JazzError, ObjectId, Result, SubscriptionHandle, SubscriptionStream,
};

type DynStorage = Box<dyn Storage + Send>;
type ClientRuntime = TokioRuntime<DynStorage>;
type DirectCoreDb = CoreDb<CoreMemoryStorage>;
type DirectCoreConnection = Rc<std::cell::RefCell<CorePeerConnection<CoreMemoryStorage>>>;

#[derive(Debug, Deserialize)]
struct UnverifiedJwtClaims {
    sub: String,
    #[serde(default)]
    claims: serde_json::Value,
}

/// Jazz client for building applications.
///
/// Combines local storage with server sync.
pub struct JazzClient {
    /// Session inferred from client auth context for user-scoped operations.
    default_session: Option<Session>,
    /// Write metadata applied to mutations issued through this client.
    write_context: Option<WriteContext>,
    /// Private engine backing the public client facade.
    engine: ClientEngine,
    /// Whether a server URL was provided at construction time.
    has_server: bool,
    /// Active subscriptions (metadata).
    subscriptions: Arc<RwLock<HashMap<SubscriptionHandle, SubscriptionState>>>,
    /// Next subscription handle ID.
    next_handle: Arc<std::sync::atomic::AtomicU64>,
}

impl Clone for JazzClient {
    fn clone(&self) -> Self {
        Self {
            default_session: self.default_session.clone(),
            write_context: self.write_context.clone(),
            engine: self.engine.clone(),
            has_server: self.has_server,
            subscriptions: Arc::clone(&self.subscriptions),
            next_handle: Arc::clone(&self.next_handle),
        }
    }
}

enum ClientEngine {
    Legacy {
        runtime: ClientRuntime,
    },
    DirectCore {
        engine: Arc<DirectCoreEngine>,
        alpha_schema: Schema,
    },
}

impl Clone for ClientEngine {
    fn clone(&self) -> Self {
        match self {
            Self::Legacy { runtime } => Self::Legacy {
                runtime: runtime.clone(),
            },
            Self::DirectCore {
                engine,
                alpha_schema,
            } => Self::DirectCore {
                engine: Arc::clone(engine),
                alpha_schema: alpha_schema.clone(),
            },
        }
    }
}

struct DirectCoreEngine {
    commands: mpsc::UnboundedSender<DirectCoreCommand>,
    owner_thread: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

struct DirectCoreInner {
    db: Rc<DirectCoreDb>,
    connection: DirectCoreConnection,
    write_map: HashMap<BatchId, CoreTxId>,
    row_tables: HashMap<ObjectId, String>,
}

enum DirectCoreCommand {
    WaitForTick {
        reply: std::sync::mpsc::Sender<Result<()>>,
    },
    Query {
        query: jazz::query::Query,
        opts: CoreReadOpts,
        table: String,
        wait_for_coverage: bool,
        reply: std::sync::mpsc::Sender<Result<Vec<jazz::node::CurrentRow>>>,
    },
    Subscribe {
        query: jazz::query::Query,
        opts: CoreReadOpts,
        table: String,
        tx: mpsc::UnboundedSender<OrderedRowDelta>,
        reply: std::sync::mpsc::Sender<Result<()>>,
    },
    Insert {
        table: String,
        row_id: Option<Uuid>,
        cells: jazz::db::RowCells,
        reply: std::sync::mpsc::Sender<Result<(ObjectId, CoreTxId)>>,
    },
    Upsert {
        table: String,
        row_id: Uuid,
        cells: jazz::db::RowCells,
        reply: std::sync::mpsc::Sender<Result<CoreTxId>>,
    },
    Update {
        row_id: ObjectId,
        cells: jazz::db::RowCells,
        reply: std::sync::mpsc::Sender<Result<CoreTxId>>,
    },
    Delete {
        row_id: ObjectId,
        reply: std::sync::mpsc::Sender<Result<CoreTxId>>,
    },
    WaitForBatch {
        batch_id: BatchId,
        tier: DurabilityTier,
        reply: std::sync::mpsc::Sender<Result<()>>,
    },
    Shutdown,
}

impl DirectCoreEngine {
    async fn start(
        schema: jazz::schema::JazzSchema,
        identity: CoreDbIdentity,
        server_url: String,
        app_id: crate::schema_manager::AppId,
        auth: WsAuthConfig,
    ) -> Result<Arc<Self>> {
        let (commands, command_rx) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let owner_thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("create direct core owner runtime");
            let local = tokio::task::LocalSet::new();
            local.block_on(&runtime, async move {
                let result =
                    DirectCoreInner::open(schema, identity, server_url, app_id, auth).await;
                match result {
                    Ok(inner) => {
                        let inner = Rc::new(std::cell::RefCell::new(inner));
                        DirectCoreInner::spawn_tick(Rc::clone(&inner));
                        let _ = ready_tx.send(Ok(()));
                        DirectCoreInner::run_commands(inner, command_rx).await;
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                    }
                }
            });
        });
        tokio::task::spawn_blocking(move || {
            ready_rx
                .recv()
                .map_err(|_| JazzError::Connection("direct core owner thread exited".to_string()))?
        })
        .await
        .map_err(|error| {
            JazzError::Connection(format!("direct core start task failed: {error}"))
        })??;
        Ok(Arc::new(Self {
            commands,
            owner_thread: std::sync::Mutex::new(Some(owner_thread)),
        }))
    }

    fn request<R: Send + 'static>(
        &self,
        build: impl FnOnce(std::sync::mpsc::Sender<Result<R>>) -> DirectCoreCommand,
    ) -> Result<R> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.commands
            .send(build(reply))
            .map_err(|_| JazzError::Sync("direct core owner thread stopped".to_string()))?;
        rx.recv()
            .map_err(|_| JazzError::Sync("direct core owner thread stopped".to_string()))?
    }

    async fn request_async<R: Send + 'static>(
        self: &Arc<Self>,
        build: impl FnOnce(std::sync::mpsc::Sender<Result<R>>) -> DirectCoreCommand + Send + 'static,
    ) -> Result<R> {
        let engine = Arc::clone(self);
        tokio::task::spawn_blocking(move || engine.request(build))
            .await
            .map_err(|error| JazzError::Sync(format!("direct core request task failed: {error}")))?
    }

    async fn wait_for_tick(&self) -> Result<()> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.commands
            .send(DirectCoreCommand::WaitForTick { reply })
            .map_err(|_| JazzError::Sync("direct core owner thread stopped".to_string()))?;
        tokio::task::spawn_blocking(move || {
            rx.recv()
                .map_err(|_| JazzError::Sync("direct core owner thread stopped".to_string()))?
        })
        .await
        .map_err(|error| JazzError::Sync(format!("direct core request task failed: {error}")))?
    }

    async fn query_rows(
        self: &Arc<Self>,
        query: jazz::query::Query,
        opts: CoreReadOpts,
        table: String,
        wait_for_coverage: bool,
    ) -> Result<Vec<jazz::node::CurrentRow>> {
        self.request_async(move |reply| DirectCoreCommand::Query {
            query,
            opts,
            table,
            wait_for_coverage,
            reply,
        })
        .await
    }

    async fn subscribe(
        self: &Arc<Self>,
        query: jazz::query::Query,
        opts: CoreReadOpts,
        table: String,
        tx: mpsc::UnboundedSender<OrderedRowDelta>,
    ) -> Result<()> {
        self.request_async(move |reply| DirectCoreCommand::Subscribe {
            query,
            opts,
            table,
            tx,
            reply,
        })
        .await
    }

    fn insert(
        &self,
        table: String,
        row_id: Option<Uuid>,
        cells: jazz::db::RowCells,
    ) -> Result<(ObjectId, CoreTxId)> {
        self.request(|reply| DirectCoreCommand::Insert {
            table,
            row_id,
            cells,
            reply,
        })
    }

    fn upsert(&self, table: String, row_id: Uuid, cells: jazz::db::RowCells) -> Result<CoreTxId> {
        self.request(|reply| DirectCoreCommand::Upsert {
            table,
            row_id,
            cells,
            reply,
        })
    }

    fn update(&self, row_id: ObjectId, cells: jazz::db::RowCells) -> Result<CoreTxId> {
        self.request(|reply| DirectCoreCommand::Update {
            row_id,
            cells,
            reply,
        })
    }

    fn delete(&self, row_id: ObjectId) -> Result<CoreTxId> {
        self.request(|reply| DirectCoreCommand::Delete { row_id, reply })
    }

    async fn wait_for_batch(
        self: &Arc<Self>,
        batch_id: BatchId,
        tier: DurabilityTier,
    ) -> Result<()> {
        self.request_async(move |reply| DirectCoreCommand::WaitForBatch {
            batch_id,
            tier,
            reply,
        })
        .await
    }
}

impl DirectCoreInner {
    async fn open(
        schema: jazz::schema::JazzSchema,
        identity: CoreDbIdentity,
        server_url: String,
        app_id: crate::schema_manager::AppId,
        auth: WsAuthConfig,
    ) -> Result<Self> {
        let db = Rc::new(
            CoreDb::open(CoreDbConfig::new(
                schema.clone(),
                direct_core_storage(&schema),
                identity,
            ))
            .await
            .map_err(|error| JazzError::Connection(error.to_string()))?,
        );
        let transport =
            DirectCoreWebSocketTransport::connect(&server_url, app_id, identity.author, auth)
                .await
                .map_err(|error| JazzError::Connection(error.to_string()))?;
        let connection = db.connect_upstream(Box::new(WireTransportAdapter::current(transport)));
        Ok(Self {
            db,
            connection,
            write_map: HashMap::new(),
            row_tables: HashMap::new(),
        })
    }

    fn spawn_tick(inner: Rc<std::cell::RefCell<Self>>) {
        tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(10));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if inner.borrow().connection.borrow_mut().tick().is_err() {
                    break;
                }
            }
        });
    }

    async fn run_commands(
        inner: Rc<std::cell::RefCell<Self>>,
        mut commands: mpsc::UnboundedReceiver<DirectCoreCommand>,
    ) {
        while let Some(command) = commands.recv().await {
            match command {
                DirectCoreCommand::WaitForTick { reply } => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    let _ = reply.send(Ok(()));
                }
                DirectCoreCommand::Query {
                    query,
                    opts,
                    table,
                    wait_for_coverage,
                    reply,
                } => {
                    let result =
                        Self::handle_query(&inner, query, opts, table, wait_for_coverage).await;
                    let _ = reply.send(result);
                }
                DirectCoreCommand::Subscribe {
                    query,
                    opts,
                    table,
                    tx,
                    reply,
                } => {
                    let result = Self::handle_subscribe(&inner, query, opts, table, tx).await;
                    let _ = reply.send(result);
                }
                DirectCoreCommand::Insert {
                    table,
                    row_id,
                    cells,
                    reply,
                } => {
                    let result = {
                        let mut inner = inner.borrow_mut();
                        let write = match row_id {
                            Some(uuid) => inner.db.insert_with_id(&table, CoreRowUuid(uuid), cells),
                            None => inner.db.insert(&table, cells),
                        }
                        .map_err(|error| JazzError::Write(error.to_string()))
                        .and_then(|write| {
                            JazzClient::check_direct_write_not_rejected(
                                &inner.db,
                                write.mergeable_tx_id(),
                            )?;
                            Ok(write)
                        });
                        write.map(|write| {
                            let object_id = ObjectId::from_uuid(write.row_uuid().0);
                            inner.remember_write(object_id, &table, write.mergeable_tx_id());
                            (object_id, write.mergeable_tx_id())
                        })
                    };
                    let _ = reply.send(result);
                }
                DirectCoreCommand::Upsert {
                    table,
                    row_id,
                    cells,
                    reply,
                } => {
                    let result = {
                        let mut inner = inner.borrow_mut();
                        inner
                            .db
                            .upsert(&table, CoreRowUuid(row_id), cells)
                            .map_err(|error| JazzError::Write(error.to_string()))
                            .and_then(|write| {
                                JazzClient::check_direct_write_not_rejected(
                                    &inner.db,
                                    write.mergeable_tx_id(),
                                )?;
                                let object_id = ObjectId::from_uuid(row_id);
                                inner.remember_write(object_id, &table, write.mergeable_tx_id());
                                Ok(write.mergeable_tx_id())
                            })
                    };
                    let _ = reply.send(result);
                }
                DirectCoreCommand::Update {
                    row_id,
                    cells,
                    reply,
                } => {
                    let result = {
                        let mut inner = inner.borrow_mut();
                        let table = inner.row_tables.get(&row_id).cloned().ok_or_else(|| {
                            JazzError::Write(
                                "direct core update requires a row created or observed by this client"
                                    .to_string(),
                            )
                        });
                        table.and_then(|table| {
                            let write = inner
                                .db
                                .update(&table, CoreRowUuid(*row_id.uuid()), cells)
                                .map_err(|error| JazzError::Write(error.to_string()))?;
                            JazzClient::check_direct_write_not_rejected(
                                &inner.db,
                                write.mergeable_tx_id(),
                            )?;
                            inner.remember_write(row_id, &table, write.mergeable_tx_id());
                            Ok(write.mergeable_tx_id())
                        })
                    };
                    let _ = reply.send(result);
                }
                DirectCoreCommand::Delete { row_id, reply } => {
                    let result = {
                        let mut inner = inner.borrow_mut();
                        let table = inner.row_tables.get(&row_id).cloned().ok_or_else(|| {
                            JazzError::Write(
                                "direct core delete requires a row created or observed by this client"
                                    .to_string(),
                            )
                        });
                        table.and_then(|table| {
                            let write = inner
                                .db
                                .delete(&table, CoreRowUuid(*row_id.uuid()))
                                .map_err(|error| JazzError::Write(error.to_string()))?;
                            JazzClient::check_direct_write_not_rejected(
                                &inner.db,
                                write.mergeable_tx_id(),
                            )?;
                            inner.remember_write(row_id, &table, write.mergeable_tx_id());
                            Ok(write.mergeable_tx_id())
                        })
                    };
                    let _ = reply.send(result);
                }
                DirectCoreCommand::WaitForBatch {
                    batch_id,
                    tier,
                    reply,
                } => {
                    let result = Self::handle_wait_for_batch(&inner, batch_id, tier).await;
                    let _ = reply.send(result);
                }
                DirectCoreCommand::Shutdown => break,
            }
        }
    }

    async fn handle_query(
        inner: &Rc<std::cell::RefCell<Self>>,
        query: jazz::query::Query,
        opts: CoreReadOpts,
        table: String,
        wait_for_coverage: bool,
    ) -> Result<Vec<jazz::node::CurrentRow>> {
        let prepared = {
            let inner = inner.borrow();
            inner
                .db
                .prepare_query(&query)
                .map_err(|error| JazzError::Query(error.to_string()))?
        };
        if wait_for_coverage {
            inner.borrow().db.propagate_query_with_opts(&prepared, opts);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while !inner.borrow().db.query_is_covered(&prepared)
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            if !inner.borrow().db.query_is_covered(&prepared) {
                return Err(JazzError::Query(
                    "timed out waiting for direct core query coverage".to_string(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let rows = {
            let inner = inner.borrow();
            inner
                .db
                .all(&prepared, opts)
                .await
                .map_err(|error| JazzError::Query(error.to_string()))?
        };
        inner.borrow_mut().remember_rows(&table, &rows);
        Ok(rows)
    }

    async fn handle_subscribe(
        inner: &Rc<std::cell::RefCell<Self>>,
        query: jazz::query::Query,
        opts: CoreReadOpts,
        table: String,
        tx: mpsc::UnboundedSender<OrderedRowDelta>,
    ) -> Result<()> {
        let stream = {
            let inner = inner.borrow();
            let prepared = inner
                .db
                .prepare_query(&query)
                .map_err(|error| JazzError::Query(error.to_string()))?;
            inner
                .db
                .subscribe(&prepared, opts)
                .await
                .map_err(|error| JazzError::Query(error.to_string()))?
        };
        let inner = Rc::clone(inner);
        tokio::task::spawn_local(async move {
            let mut stream = stream;
            while let Some(event) = stream.next_event().await {
                match event {
                    CoreSubscriptionEvent::Opened { current, .. }
                    | CoreSubscriptionEvent::Reset { current, .. } => {
                        inner.borrow_mut().remember_rows(&table, &current);
                        let _ = tx.send(OrderedRowDelta::default());
                    }
                    CoreSubscriptionEvent::Delta { .. } => {
                        let _ = tx.send(OrderedRowDelta::default());
                    }
                    CoreSubscriptionEvent::Closed => break,
                }
            }
        });
        Ok(())
    }

    async fn handle_wait_for_batch(
        inner: &Rc<std::cell::RefCell<Self>>,
        batch_id: BatchId,
        tier: DurabilityTier,
    ) -> Result<()> {
        let tx_id = inner
            .borrow()
            .write_map
            .get(&batch_id)
            .copied()
            .ok_or_else(|| JazzError::Sync(format!("unknown direct core batch {batch_id}")))?;
        let desired = core_tier(tier);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
        loop {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let state = inner
                .borrow()
                .db
                .write_state(tx_id)
                .map_err(|error| JazzError::Sync(error.to_string()))?;
            if matches!(state.fate, CoreFate::Rejected(_)) {
                return Err(JazzError::Sync(format!(
                    "batch was rejected before reaching {tier:?} durability"
                )));
            }
            if state.durability >= desired {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(JazzError::Sync(format!(
                    "timed out waiting for direct core batch to reach {tier:?}"
                )));
            }
        }
    }

    fn remember_write(&mut self, row_id: ObjectId, table: &str, tx_id: CoreTxId) {
        self.write_map.insert(direct_batch_id(tx_id), tx_id);
        self.row_tables.insert(row_id, table.to_string());
    }

    fn remember_rows(&mut self, table: &str, rows: &[jazz::node::CurrentRow]) {
        for row in rows {
            self.row_tables
                .insert(ObjectId::from_uuid(row.row_uuid().0), table.to_string());
        }
    }
}

impl Drop for DirectCoreEngine {
    fn drop(&mut self) {
        let _ = self.commands.send(DirectCoreCommand::Shutdown);
        if let Ok(mut owner_thread) = self.owner_thread.lock() {
            if let Some(owner_thread) = owner_thread.take() {
                let _ = owner_thread.join();
            }
        }
    }
}

/// Transaction-scoped Jazz client handle.
///
/// Mutations issued through this handle are staged in the transaction returned
/// by [`JazzClient::begin_transaction`]. The handle dereferences to the scoped
/// [`JazzClient`] so regular client methods can be used directly.
pub struct JazzTransaction {
    batch_id: BatchId,
    client: JazzClient,
}

impl JazzTransaction {
    /// Logical batch id backing this transaction.
    pub fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    /// The transaction-scoped client.
    pub fn client(&self) -> &JazzClient {
        &self.client
    }

    /// Commit this transaction.
    ///
    /// Returns the transaction batch id so callers can wait for durability with
    /// [`JazzClient::wait_for_batch`] if needed.
    pub fn commit(self) -> Result<BatchId> {
        self.client.commit_transaction(self.batch_id)?;
        Ok(self.batch_id)
    }

    /// Roll back this transaction locally.
    pub fn rollback(self) -> Result<bool> {
        self.client.rollback_transaction(self.batch_id)
    }
}

impl Deref for JazzTransaction {
    type Target = JazzClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

/// State for an active subscription.
struct SubscriptionState {
    runtime_handle: RuntimeSubHandle,
}

fn build_client_schema_manager<S: Storage + ?Sized>(
    storage: &S,
    context: &AppContext,
) -> Result<SchemaManager> {
    let sync_manager = SyncManager::new();
    let mut schema_manager = SchemaManager::new(
        sync_manager,
        context.schema.clone(),
        context.app_id,
        "client",
        "main",
    )
    .map_err(|e| JazzError::Schema(format!("{:?}", e)))?;

    rehydrate_schema_manager_from_catalogue(&mut schema_manager, storage, context.app_id)
        .map_err(JazzError::Storage)?;

    Ok(schema_manager)
}

#[cfg(feature = "test-utils")]
fn build_client_schema_manager_with_policy_mode<S: Storage + ?Sized>(
    storage: &S,
    context: &AppContext,
    row_policy_mode: RowPolicyMode,
) -> Result<SchemaManager> {
    let sync_manager = SyncManager::new();
    let mut schema_manager = SchemaManager::new_with_policy_mode(
        sync_manager,
        context.schema.clone(),
        context.app_id,
        "client",
        "main",
        row_policy_mode,
    )
    .map_err(|e| JazzError::Schema(format!("{:?}", e)))?;

    rehydrate_schema_manager_from_catalogue(&mut schema_manager, storage, context.app_id)
        .map_err(JazzError::Storage)?;

    Ok(schema_manager)
}

fn session_from_unverified_jwt(token: &str) -> Option<Session> {
    let payload = token.split('.').nth(1)?;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))
        .ok()?;
    let claims: UnverifiedJwtClaims = serde_json::from_slice(&payload).ok()?;
    let user_id = claims.sub.trim();
    if user_id.is_empty() {
        return None;
    }

    Some(Session {
        user_id: user_id.to_string(),
        claims: claims.claims,
        ..Session::new(user_id)
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

fn direct_core_identity(context: &AppContext, default_session: Option<&Session>) -> CoreDbIdentity {
    let node_uuid = context
        .client_id
        .map(|id| id.0)
        .unwrap_or_else(Uuid::now_v7);
    let author_uuid = default_session
        .map(|session| Uuid::new_v5(&Uuid::NAMESPACE_URL, session.user_id.as_bytes()))
        .unwrap_or(node_uuid);
    CoreDbIdentity {
        node: CoreNodeUuid(node_uuid),
        author: CoreAuthorId(author_uuid),
    }
}

fn direct_core_storage(schema: &jazz::schema::JazzSchema) -> CoreMemoryStorage {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    CoreMemoryStorage::new(&refs)
}

fn alpha_to_core_value(value: Value) -> Result<CoreValue> {
    match value {
        Value::Boolean(value) => Ok(CoreValue::Bool(value)),
        Value::Text(value) => Ok(CoreValue::String(value)),
        Value::Integer(value) => u64::try_from(value).map(CoreValue::U64).map_err(|_| {
            JazzError::Write("negative INTEGER values are not supported by direct core".to_string())
        }),
        Value::BigInt(value) => u64::try_from(value).map(CoreValue::U64).map_err(|_| {
            JazzError::Write("negative BIGINT values are not supported by direct core".to_string())
        }),
        Value::Double(value) => Ok(CoreValue::F64(value)),
        Value::Timestamp(value) => Ok(CoreValue::U64(value)),
        Value::Uuid(value) => Ok(CoreValue::Uuid(*value.uuid())),
        Value::Bytea(value) => Ok(CoreValue::Bytes(value)),
        Value::Null => Ok(CoreValue::Nullable(None)),
        Value::Array(values) => values
            .into_iter()
            .map(alpha_to_core_value)
            .collect::<Result<Vec<_>>>()
            .map(CoreValue::Array),
        other => Err(JazzError::Write(format!(
            "direct core client does not support alpha value {other:?}"
        ))),
    }
}

fn core_to_alpha_value(value: CoreValue) -> Result<Value> {
    match value {
        CoreValue::Bool(value) => Ok(Value::Boolean(value)),
        CoreValue::String(value) => Ok(Value::Text(value)),
        CoreValue::U64(value) => Ok(Value::Timestamp(value)),
        CoreValue::F64(value) => Ok(Value::Double(value)),
        CoreValue::Uuid(value) => Ok(Value::Uuid(ObjectId::from_uuid(value))),
        CoreValue::Bytes(value) => Ok(Value::Bytea(value)),
        CoreValue::Nullable(None) => Ok(Value::Null),
        CoreValue::Nullable(Some(value)) => core_to_alpha_value(*value),
        CoreValue::Array(values) => values
            .into_iter()
            .map(core_to_alpha_value)
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        other => Err(JazzError::Query(format!(
            "direct core client does not support core value {other:?}"
        ))),
    }
}

fn direct_batch_id(tx_id: CoreTxId) -> BatchId {
    let mut bytes = *tx_id.node.0.as_bytes();
    bytes[..8].copy_from_slice(&tx_id.time.0.to_be_bytes());
    BatchId(bytes)
}

fn core_tier(tier: DurabilityTier) -> CoreDurabilityTier {
    match tier {
        DurabilityTier::Local => CoreDurabilityTier::Local,
        DurabilityTier::EdgeServer | DurabilityTier::GlobalServer => CoreDurabilityTier::Global,
    }
}

#[cfg(test)]
async fn wait_for_initial_transport_handshake(
    runtime: &ClientRuntime,
    timeout_after: Duration,
) -> Result<()> {
    let connected = tokio::time::timeout(timeout_after, runtime.transport_wait_until_connected())
        .await
        .map_err(|_| {
            JazzError::Connection(
                "timed out waiting for WebSocket handshake to complete".to_string(),
            )
        })?;
    if !connected {
        return Err(JazzError::Connection(
            "transport closed before WebSocket handshake completed".to_string(),
        ));
    }
    // The watch signal means the transport queued `Connected`; drain the
    // scheduled tick so `connect()` returns with the server registered.
    runtime.flush().await.map_err(|e| {
        JazzError::Connection(format!("failed to apply initial WebSocket handshake: {e}"))
    })?;
    Ok(())
}

impl JazzClient {
    fn check_direct_write_not_rejected(db: &DirectCoreDb, tx_id: CoreTxId) -> Result<()> {
        let state = db
            .write_state(tx_id)
            .map_err(|error| JazzError::Write(error.to_string()))?;
        if let CoreFate::Rejected(reason) = state.fate {
            return Err(JazzError::Write(format!(
                "direct core write rejected: {reason:?}"
            )));
        }
        Ok(())
    }

    fn direct_read_opts(durability_tier: Option<DurabilityTier>) -> CoreReadOpts {
        CoreReadOpts {
            tier: durability_tier
                .map(core_tier)
                .unwrap_or(CoreDurabilityTier::Local),
            local_updates: CoreLocalUpdates::Immediate,
            propagation: CorePropagation::Full,
            include_deleted: false,
        }
    }

    fn direct_core_query(&self, query: &Query) -> Result<jazz::query::Query> {
        if query.disjuncts.len() != 1
            || !query.disjuncts[0].conditions.is_empty()
            || !query.joins.is_empty()
            || !query.array_subqueries.is_empty()
            || query.recursive.is_some()
            || !query.order_by.is_empty()
            || query.limit.is_some()
            || query.offset != 0
            || query.include_deleted
            || query.result_element_index.is_some()
        {
            return Err(JazzError::Query(
                "direct core JazzClient currently supports simple table queries only".to_string(),
            ));
        }
        let mut core_query = jazz::query::Query::from(query.table.as_str());
        if let Some(columns) = query.select_columns.clone() {
            core_query = core_query.select(columns);
        }
        Ok(core_query)
    }

    fn direct_rows_to_alpha(
        &self,
        query: &Query,
        rows: Vec<jazz::node::CurrentRow>,
    ) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        let table = query.table.as_str();
        let schema = self.schema()?;
        let table_schema = schema
            .get(&TableName::new(table))
            .ok_or_else(|| JazzError::Query(format!("unknown table {table}")))?;
        let columns = query.select_columns.clone().unwrap_or_else(|| {
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
                let row_id = ObjectId::from_uuid(row.row_uuid().0);
                let values = columns
                    .iter()
                    .map(|column| {
                        let position =
                            table_schema.columns.column_index(column).ok_or_else(|| {
                                JazzError::Query(format!(
                                    "unknown column {column} on table {table}"
                                ))
                            })?;
                        row.cell_at(position)
                            .ok_or_else(|| {
                                JazzError::Query(format!("direct core row missing column {column}"))
                            })
                            .and_then(core_to_alpha_value)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((row_id, values))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(rows)
    }

    fn direct_cells(values: HashMap<String, Value>) -> Result<jazz::db::RowCells> {
        values
            .into_iter()
            .map(|(name, value)| Ok((name, alpha_to_core_value(value)?)))
            .collect()
    }

    fn direct_ordered_values(
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
                values.get(column.name.as_str()).cloned().ok_or_else(|| {
                    JazzError::Write(format!(
                        "direct core insert missing required column {}",
                        column.name.as_str()
                    ))
                })
            })
            .collect()
    }

    fn read_session(&self) -> Option<Session> {
        self.write_context
            .as_ref()
            .and_then(|context| context.session.clone())
            .or_else(|| self.default_session.clone())
    }

    fn write_context_for_batch(&self, batch_id: BatchId, batch_mode: BatchMode) -> WriteContext {
        self.write_context
            .clone()
            .unwrap_or_default()
            .with_batch_mode(batch_mode)
            .with_batch_id(batch_id)
    }

    /// Connect to Jazz with the given configuration.
    ///
    /// This will:
    /// 1. Open local storage
    /// 2. Initialize the runtime
    /// 3. Connect to the server over WebSocket (if URL provided)
    /// 4. Wait for the initial WS handshake to complete
    pub async fn connect(context: AppContext) -> Result<Self> {
        Self::connect_with_schema_manager(context, build_client_schema_manager).await
    }

    async fn connect_with_schema_manager(
        context: AppContext,
        build_schema_manager: impl FnOnce(&DynStorage, &AppContext) -> Result<SchemaManager>,
    ) -> Result<Self> {
        let default_session = default_session_from_context(&context);
        // Loaded for its side effect of persisting the client-id file on disk;
        // the wire ClientId is assigned by `TransportManager::create` at connect
        // time and is exposed via `runtime.transport_client_id()`.
        let _client_id = match context.storage {
            ClientStorage::Persistent => load_or_create_persistent_client_id(&context)?,
            ClientStorage::Memory => context.client_id.unwrap_or_default(),
        };

        let storage: DynStorage = match context.storage {
            ClientStorage::Persistent => open_persistent_storage(&context.data_dir).await?,
            ClientStorage::Memory => Box::new(MemoryStorage::new()),
        };

        let schema_manager = build_schema_manager(&storage, &context)?;

        // Create runtime. The sync callback is a no-op — the WS TransportManager
        // drives the outbox directly via its own channel.
        let runtime = TokioRuntime::new(schema_manager, storage, move |_entry: OutboxEntry| {});

        // Attach the tracer to the runtime so all outbox/inbox traffic is
        // recorded under the participant name.
        if let Some((ref tracer, ref name)) = context.sync_tracer {
            runtime.set_sync_tracer(tracer.clone(), name.clone());
        }

        // Persist schema to catalogue for server sync
        runtime
            .persist_schema()
            .map_err(|e| JazzError::Storage(e.to_string()))?;

        let has_server = !context.server_url.is_empty();

        if has_server {
            if !matches!(context.storage, ClientStorage::Memory) {
                return Err(JazzError::Connection(
                    "direct core JazzClient currently supports server-backed memory clients only"
                        .to_string(),
                ));
            }
            let core_schema = convert_alpha_schema(&context.schema)
                .map_err(|error| JazzError::Schema(error.to_string()))?;
            let identity = direct_core_identity(&context, default_session.as_ref());
            let auth = WsAuthConfig {
                jwt_token: if context.backend_secret.is_some() {
                    None
                } else {
                    context.jwt_token.clone()
                },
                backend_secret: context.backend_secret.clone(),
                admin_secret: context.admin_secret.clone(),
                backend_session: None,
            };
            let direct_engine = DirectCoreEngine::start(
                core_schema,
                identity,
                context.server_url.clone(),
                context.app_id,
                auth,
            )
            .await
            .map_err(|error| JazzError::Connection(error.to_string()))?;
            let engine = ClientEngine::DirectCore {
                engine: direct_engine,
                alpha_schema: context.schema.clone(),
            };
            let client = Self {
                default_session,
                write_context: None,
                engine,
                has_server,
                subscriptions: Arc::new(RwLock::new(HashMap::new())),
                next_handle: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            };
            if let ClientEngine::DirectCore { engine, .. } = &client.engine {
                engine.wait_for_tick().await?;
            }
            return Ok(client);
        }

        Ok(Self {
            default_session,
            write_context: None,
            engine: ClientEngine::Legacy { runtime },
            has_server,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            next_handle: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        })
    }

    #[cfg(feature = "test-utils")]
    pub async fn connect_with_row_policy_mode(
        context: AppContext,
        row_policy_mode: RowPolicyMode,
    ) -> Result<Self> {
        Self::connect_with_schema_manager(context, |storage, context| {
            build_client_schema_manager_with_policy_mode(storage, context, row_policy_mode)
        })
        .await
    }

    /// Subscribe to a query.
    ///
    /// Returns a stream of row deltas as the data changes.
    pub async fn subscribe(&self, query: Query) -> Result<SubscriptionStream> {
        let handle = SubscriptionHandle(
            self.next_handle
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );

        // Create channel for this subscription's deltas.
        // tx is moved directly into the callback so the delta is never dropped due
        // to the race where immediate_tick fires the callback before we can insert
        // tx into a shared map.
        let (tx, rx) = mpsc::unbounded_channel::<OrderedRowDelta>();

        if let ClientEngine::DirectCore { engine, .. } = &self.engine {
            let core_query = self.direct_core_query(&query)?;
            engine
                .subscribe(
                    core_query,
                    Self::direct_read_opts(Some(DurabilityTier::EdgeServer)),
                    query.table.as_str().to_string(),
                    tx,
                )
                .await?;
            return Ok(SubscriptionStream::new(rx));
        }

        let ClientEngine::Legacy { runtime } = &self.engine else {
            unreachable!("direct core handled above");
        };
        // Register with runtime using callback pattern
        // The callback bridges runtime updates to the channel
        let runtime_handle = runtime
            .subscribe(
                query.clone(),
                move |delta| {
                    // Route delta to the subscription stream without dropping
                    // updates when the consumer falls briefly behind.
                    let _ = tx.send(delta.ordered_delta);
                },
                self.write_context
                    .as_ref()
                    .and_then(|context| context.session.clone())
                    .or_else(|| self.default_session.clone()),
            )
            .map_err(|e| JazzError::Query(e.to_string()))?;

        // Track subscription metadata
        {
            let mut subs = self.subscriptions.write().await;
            subs.insert(handle, SubscriptionState { runtime_handle });
        }

        Ok(SubscriptionStream::new(rx))
    }

    /// One-shot query, optionally waiting for a durability tier.
    ///
    /// Returns the current results as `Vec<(ObjectId, Vec<Value>)>`.
    pub async fn query(
        &self,
        query: Query,
        durability_tier: Option<DurabilityTier>,
    ) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        if let ClientEngine::DirectCore { engine, .. } = &self.engine {
            let opts = Self::direct_read_opts(durability_tier);
            let rows = engine
                .query_rows(
                    self.direct_core_query(&query)?,
                    opts,
                    query.table.as_str().to_string(),
                    durability_tier.is_some(),
                )
                .await?;
            return self.direct_rows_to_alpha(&query, rows);
        }

        let ClientEngine::Legacy { runtime } = &self.engine else {
            unreachable!("direct core handled above");
        };
        let future = runtime
            .query(
                query,
                self.read_session(),
                ReadDurabilityOptions {
                    tier: durability_tier,
                    local_updates: AlphaLocalUpdates::Immediate,
                },
                self.write_context.as_ref().and_then(WriteContext::batch_id),
            )
            .map_err(|e| JazzError::Query(e.to_string()))?;
        future
            .await
            .map_err(|e| JazzError::Query(format!("{:?}", e)))
    }

    /// Create a new row in a table.
    pub fn insert(
        &self,
        table: &str,
        values: HashMap<String, Value>,
    ) -> Result<(ObjectId, Vec<Value>, BatchId)> {
        self.insert_with_id(table, Option::<Uuid>::None, values)
    }

    /// Create a new row in a table using a caller-supplied UUID.
    pub fn insert_with_id(
        &self,
        table: &str,
        object_id: impl Into<Option<Uuid>>,
        values: HashMap<String, Value>,
    ) -> Result<(ObjectId, Vec<Value>, BatchId)> {
        if let ClientEngine::DirectCore { engine, .. } = &self.engine {
            let row_values = self.direct_ordered_values(table, &values)?;
            let cells = Self::direct_cells(values)?;
            let (row_id, tx_id) = engine.insert(table.to_string(), object_id.into(), cells)?;
            let batch_id = direct_batch_id(tx_id);
            return Ok((row_id, row_values, batch_id));
        }

        let ClientEngine::Legacy { runtime } = &self.engine else {
            unreachable!("direct core handled above");
        };
        let (object_id, row_values, batch_id) = runtime
            .insert_with_id(
                table,
                values,
                object_id.into().map(ObjectId::from_uuid),
                self.write_context.as_ref(),
            )
            .map_err(|e| JazzError::Write(e.to_string()))?;
        Ok((object_id, row_values, batch_id))
    }

    /// Create or update a row using a caller-supplied UUID.
    pub fn upsert(
        &self,
        table: &str,
        object_id: Uuid,
        values: HashMap<String, Value>,
    ) -> Result<BatchId> {
        if let ClientEngine::DirectCore { engine, .. } = &self.engine {
            let cells = Self::direct_cells(values)?;
            let tx_id = engine.upsert(table.to_string(), object_id, cells)?;
            return Ok(direct_batch_id(tx_id));
        }
        let ClientEngine::Legacy { runtime } = &self.engine else {
            unreachable!("direct core handled above");
        };
        runtime
            .upsert(
                table,
                ObjectId::from_uuid(object_id),
                values,
                self.write_context.as_ref(),
            )
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Update a row.
    pub fn update(&self, object_id: ObjectId, updates: Vec<(String, Value)>) -> Result<BatchId> {
        if let ClientEngine::DirectCore { engine, .. } = &self.engine {
            let cells = Self::direct_cells(updates.into_iter().collect())?;
            let tx_id = engine.update(object_id, cells)?;
            return Ok(direct_batch_id(tx_id));
        }
        let ClientEngine::Legacy { runtime } = &self.engine else {
            unreachable!("direct core handled above");
        };
        runtime
            .update(object_id, updates, self.write_context.as_ref())
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Delete a row.
    pub fn delete(&self, object_id: ObjectId) -> Result<BatchId> {
        if let ClientEngine::DirectCore { engine, .. } = &self.engine {
            let tx_id = engine.delete(object_id)?;
            return Ok(direct_batch_id(tx_id));
        }
        let ClientEngine::Legacy { runtime } = &self.engine else {
            unreachable!("direct core handled above");
        };
        runtime
            .delete(object_id, self.write_context.as_ref())
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Begin a transaction and return a transaction-scoped client handle.
    ///
    /// Mutations issued through the returned handle are staged locally and are
    /// not visible to ordinary reads until the transaction is committed and
    /// accepted by the authority.
    pub fn begin_transaction(&self) -> Result<JazzTransaction> {
        if matches!(self.engine, ClientEngine::DirectCore { .. }) {
            return Err(JazzError::Write(
                "direct core JazzClient transactions are not implemented yet".to_string(),
            ));
        }
        let ClientEngine::Legacy { runtime } = &self.engine else {
            unreachable!("direct core handled above");
        };
        let batch_id = runtime
            .begin_batch(BatchMode::Transactional)
            .map_err(|e| JazzError::Write(e.to_string()))?;
        let client = self
            .with_write_context(self.write_context_for_batch(batch_id, BatchMode::Transactional));
        Ok(JazzTransaction { batch_id, client })
    }

    /// Commit an open transaction by batch id.
    pub fn commit_transaction(&self, batch_id: BatchId) -> Result<()> {
        if matches!(self.engine, ClientEngine::DirectCore { .. }) {
            return Err(JazzError::Write(
                "direct core JazzClient transactions are not implemented yet".to_string(),
            ));
        }
        let ClientEngine::Legacy { runtime } = &self.engine else {
            unreachable!("direct core handled above");
        };
        runtime
            .commit_batch(batch_id)
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Roll back an open transaction by batch id.
    ///
    /// Returns whether a local batch record existed for the transaction.
    pub fn rollback_transaction(&self, batch_id: BatchId) -> Result<bool> {
        if matches!(self.engine, ClientEngine::DirectCore { .. }) {
            return Err(JazzError::Write(
                "direct core JazzClient transactions are not implemented yet".to_string(),
            ));
        }
        let ClientEngine::Legacy { runtime } = &self.engine else {
            unreachable!("direct core handled above");
        };
        runtime
            .rollback_batch(batch_id)
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    pub async fn wait_for_batch(&self, batch_id: BatchId, tier: DurabilityTier) -> Result<()> {
        if let ClientEngine::DirectCore { engine, .. } = &self.engine {
            return engine.wait_for_batch(batch_id, tier).await;
        }
        let ClientEngine::Legacy { runtime } = &self.engine else {
            unreachable!("direct core handled above");
        };
        let receiver = runtime
            .wait_for_batch(batch_id, tier)
            .map_err(|e| JazzError::Sync(e.to_string()))?;
        wait_for_batch_write(receiver, tier).await
    }

    /// Unsubscribe from a subscription.
    pub async fn unsubscribe(&self, handle: SubscriptionHandle) -> Result<()> {
        let mut subs = self.subscriptions.write().await;
        if let Some(state) = subs.remove(&handle) {
            if let ClientEngine::Legacy { runtime } = &self.engine {
                let _ = runtime.unsubscribe(state.runtime_handle);
            }
        }
        Ok(())
    }

    /// Get the current schema.
    pub fn schema(&self) -> Result<crate::query_manager::types::Schema> {
        let ClientEngine::Legacy { runtime } = &self.engine else {
            if let ClientEngine::DirectCore { alpha_schema, .. } = &self.engine {
                return Ok(alpha_schema.clone());
            }
            unreachable!("unknown client engine");
        };
        runtime
            .current_schema()
            .map_err(|e| JazzError::Query(e.to_string()))
    }

    /// Check if connected to server.
    pub fn is_connected(&self) -> bool {
        match &self.engine {
            ClientEngine::Legacy { runtime } => {
                self.has_server && runtime.transport_ever_connected()
            }
            ClientEngine::DirectCore { .. } => false,
        }
    }

    /// Create a client that uses the given write context for mutations.
    pub fn with_write_context(&self, write_context: WriteContext) -> JazzClient {
        JazzClient {
            default_session: self.default_session.clone(),
            write_context: Some(write_context),
            engine: self.engine.clone(),
            has_server: self.has_server,
            subscriptions: Arc::clone(&self.subscriptions),
            next_handle: Arc::clone(&self.next_handle),
        }
    }

    /// Create a session-scoped client for backend operations.
    pub fn for_session(&self, session: Session) -> JazzClient {
        self.with_write_context(WriteContext::from_session(session))
    }

    /// Shutdown the client and release resources.
    pub async fn shutdown(self) -> Result<()> {
        let ClientEngine::Legacy { runtime } = &self.engine else {
            return Ok(());
        };
        // Disconnect from server (drops the TransportHandle; manager task exits cleanly)
        if self.has_server {
            runtime.disconnect();
        }

        // Flush pending operations
        let runtime_flush_result = runtime
            .flush()
            .await
            .map_err(|e| JazzError::Connection(e.to_string()));

        // Flush storage state to disk for persistence
        let storage_result = runtime
            .with_storage(|storage| {
                let flush_result = storage.flush();
                let flush_wal_result = storage.flush_wal();
                let close_result = storage.close();

                flush_result?;
                flush_wal_result?;
                close_result
            })
            .map_err(|e| JazzError::Storage(e.to_string()))
            .and_then(|result| result.map_err(|e| JazzError::Storage(e.to_string())));

        runtime_flush_result?;
        storage_result?;

        Ok(())
    }
}

#[cfg(feature = "test-utils")]
impl JazzClient {
    pub fn client_id(&self) -> Option<ClientId> {
        match &self.engine {
            ClientEngine::Legacy { runtime } => runtime.transport_client_id(),
            ClientEngine::DirectCore { .. } => None,
        }
    }

    pub async fn test_client(schema: Schema) -> crate::JazzClient {
        let context = crate::AppContext::test(schema);
        crate::JazzClient::connect(context)
            .await
            .expect("connect local JazzClient")
    }

    pub async fn permissive_test_client(schema: Schema) -> crate::JazzClient {
        crate::JazzClient::connect_with_row_policy_mode(
            crate::AppContext::test(schema),
            RowPolicyMode::PermissiveLocal,
        )
        .await
        .expect("connect permissive local JazzClient")
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl Drop for JazzClient {
    /// This is a simplified and synchronous implementation of `JazzClient.shutdown`
    /// that is good-enough for tests (so that we don't require an explicit
    /// `JazzClient.shutdown` at the end of each test case)
    fn drop(&mut self) {
        if Arc::strong_count(&self.next_handle) > 1 {
            return;
        }

        if let ClientEngine::Legacy { runtime } = &self.engine {
            if self.has_server {
                runtime.disconnect();
            }

            let _ = runtime.with_storage(|storage| {
                let _ = storage.flush();
                let _ = storage.flush_wal();
                let _ = storage.close();
            });
        }
    }
}

async fn wait_for_batch_write(
    receiver: futures::channel::oneshot::Receiver<crate::runtime_core::PersistedWriteAck>,
    tier: DurabilityTier,
) -> Result<()> {
    receiver
        .await
        .map_err(|_| {
            JazzError::Sync(format!(
                "batch was cancelled before reaching {tier:?} durability"
            ))
        })?
        .map_err(|rejection| {
            JazzError::Sync(format!(
                "batch was rejected before reaching {tier:?} durability ({}): {}",
                rejection.code, rejection.reason
            ))
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::policy::PolicyExpr;
    use crate::query_manager::types::{Schema, SchemaHash, TableName, TablePolicies};
    use crate::runtime_core::{NoopScheduler, RuntimeCore};
    use crate::schema_manager::AppId;
    #[cfg(feature = "rocksdb")]
    use crate::storage::RocksDBStorage;
    use crate::{ColumnType, SchemaBuilder, TableSchema};
    use serde_json::json;
    use tempfile::TempDir;

    fn declared_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean),
            )
            .build()
    }

    fn learned_runtime_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean)
                    .nullable_column("description", ColumnType::Text),
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
            jwt_token: None,
            backend_secret: None,
            admin_secret: None,
            sync_tracer: None,
        }
    }

    fn make_test_jwt(sub: &str, claims: serde_json::Value) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"alg":"none","typ":"JWT"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({
                "sub": sub,
                "claims": claims,
            }))
            .expect("serialize jwt payload"),
        );
        format!("{header}.{payload}.sig")
    }

    #[cfg(feature = "rocksdb")]
    fn seed_rehydrated_client_storage(
        data_dir: &std::path::Path,
        app_id: AppId,
        publish_permissions: bool,
    ) -> (SchemaHash, SchemaHash) {
        std::fs::create_dir_all(data_dir).expect("create seeded client data dir");

        #[cfg(feature = "rocksdb")]
        let storage = {
            let db_path = data_dir.join("jazz.rocksdb");
            RocksDBStorage::open(&db_path, 64 * 1024 * 1024).expect("open seeded client storage")
        };
        let bundled_schema = declared_todo_schema();
        let learned_schema = learned_runtime_todo_schema();
        let bundled_hash = SchemaHash::compute(&bundled_schema);
        let learned_hash = SchemaHash::compute(&learned_schema);

        let schema_manager = SchemaManager::new(
            SyncManager::new(),
            learned_schema.clone(),
            app_id,
            "seed",
            "main",
        )
        .expect("seed schema manager");
        let mut runtime = RuntimeCore::new(schema_manager, storage, NoopScheduler);
        runtime.persist_schema();
        runtime.publish_schema(bundled_schema.clone());
        let lens = runtime
            .schema_manager()
            .generate_lens(&bundled_schema, &learned_schema);
        assert!(!lens.is_draft(), "seed lens should be publishable");
        runtime.publish_lens(&lens).expect("persist learned lens");

        if publish_permissions {
            runtime
                .publish_permissions_bundle(
                    learned_hash,
                    HashMap::from([(
                        TableName::new("todos"),
                        TablePolicies::new().with_select(PolicyExpr::True),
                    )]),
                    None,
                )
                .expect("seed permissions bundle");
        }

        let storage = runtime.into_storage();
        storage.flush().expect("flush seeded client storage");
        storage.close().expect("close seeded client storage");

        (bundled_hash, learned_hash)
    }

    #[cfg(feature = "rocksdb")]
    fn expected_client_catalogue_hash(context: &AppContext) -> String {
        #[cfg(feature = "rocksdb")]
        let storage = {
            let db_path = context.data_dir.join("jazz.rocksdb");
            RocksDBStorage::open(&db_path, 64 * 1024 * 1024).expect("open seeded client storage")
        };
        let schema_manager = build_client_schema_manager(&storage, context)
            .expect("rehydrate client schema manager");
        let catalogue_hash = schema_manager.catalogue_state_hash();
        storage.close().expect("close seeded client storage");
        catalogue_hash
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn seeded_client_storage_persists_learned_schema_and_lens() {
        let data_dir = TempDir::new().expect("temp client dir");
        let app_id = AppId::from_name("client-seeded-storage");
        let (_bundled_hash, learned_hash) =
            seed_rehydrated_client_storage(data_dir.path(), app_id, false);

        let db_path = data_dir.path().join("jazz.rocksdb");
        let storage =
            RocksDBStorage::open(&db_path, 64 * 1024 * 1024).expect("open seeded client storage");

        let entries = storage
            .scan_catalogue_entries()
            .expect("scan seeded catalogue entries");
        let learned_object_id = learned_hash.to_object_id();
        assert!(
            entries
                .iter()
                .any(|entry| entry.object_id == learned_object_id),
            "seeded storage should persist the learned schema object"
        );
        assert!(
            entries.iter().any(|entry| entry.object_type()
                == Some(crate::metadata::ObjectType::CatalogueLens.as_str())),
            "seeded storage should persist at least one learned lens"
        );

        storage.close().expect("close seeded client storage");
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn boxed_client_storage_rehydrates_learned_schema_from_catalogue() {
        let data_dir = TempDir::new().expect("temp client dir");
        let app_id = AppId::from_name("client-boxed-rehydrate");
        let (_bundled_hash, learned_hash) =
            seed_rehydrated_client_storage(data_dir.path(), app_id, false);
        let context = make_offline_context(
            app_id,
            data_dir.path().to_path_buf(),
            declared_todo_schema(),
        );

        let concrete_storage = {
            let db_path = data_dir.path().join("jazz.rocksdb");
            RocksDBStorage::open(&db_path, 64 * 1024 * 1024)
                .expect("open seeded client storage concretely")
        };
        let concrete_manager = build_client_schema_manager(&concrete_storage, &context)
            .expect("rehydrate schema manager from concrete storage");
        assert!(
            concrete_manager
                .known_schema_hashes()
                .contains(&learned_hash),
            "concrete storage rehydrate should learn the newer schema"
        );
        concrete_storage
            .close()
            .expect("close seeded client storage");

        let boxed_storage = open_persistent_storage(data_dir.path())
            .await
            .expect("open boxed client storage");
        let boxed_manager = build_client_schema_manager(boxed_storage.as_ref(), &context)
            .expect("rehydrate schema manager from boxed storage");
        assert!(
            boxed_manager.known_schema_hashes().contains(&learned_hash),
            "boxed client storage rehydrate should learn the newer schema"
        );
        boxed_storage.close().expect("close boxed client storage");
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

    #[tokio::test]
    async fn initial_transport_handshake_wait_errors_when_transport_is_absent() {
        let app_id = AppId::from_name("client-missing-transport");
        let context = make_offline_context(
            app_id,
            TempDir::new().expect("tempdir").keep(),
            declared_todo_schema(),
        );
        let storage: DynStorage = Box::new(MemoryStorage::new());
        let schema_manager =
            build_client_schema_manager(storage.as_ref(), &context).expect("schema manager");
        let runtime = TokioRuntime::new(schema_manager, storage, |_entry: OutboxEntry| {});

        let result = wait_for_initial_transport_handshake(&runtime, Duration::from_secs(1)).await;

        match result {
            Err(JazzError::Connection(message)) => assert_eq!(
                message,
                "transport closed before WebSocket handshake completed"
            ),
            other => panic!("expected connection error for missing transport, got {other:?}"),
        }
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn client_rehydrates_learned_lens_from_local_catalogue_on_restart() {
        let data_dir = TempDir::new().expect("temp client dir");
        let app_id = AppId::from_name("client-rehydrate-lens");
        let (_bundled_hash, learned_hash) =
            seed_rehydrated_client_storage(data_dir.path(), app_id, false);
        let context = make_offline_context(
            app_id,
            data_dir.path().to_path_buf(),
            declared_todo_schema(),
        );

        let client = JazzClient::connect(context).await.expect("connect client");

        let has_learned_schema = client
            .runtime
            .known_schema_hashes()
            .expect("read known schema hashes")
            .contains(&learned_hash);
        assert!(
            has_learned_schema,
            "client should restore newer learned schema"
        );

        let lens_path_len = client
            .runtime
            .with_schema_manager(|manager| manager.lens_path(&learned_hash).map(|path| path.len()))
            .expect("read client schema manager")
            .expect("lens path to bundled schema");
        assert_eq!(
            lens_path_len, 1,
            "client should restore learned migration lens"
        );

        client.shutdown().await.expect("shutdown client");
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn client_rehydrates_permissions_head_and_bundle_from_local_catalogue_on_restart() {
        let data_dir = TempDir::new().expect("temp client dir");
        let app_id = AppId::from_name("client-rehydrate-permissions");
        let (_bundled_hash, learned_hash) =
            seed_rehydrated_client_storage(data_dir.path(), app_id, true);
        let context = make_offline_context(
            app_id,
            data_dir.path().to_path_buf(),
            declared_todo_schema(),
        );
        let expected_catalogue_hash = expected_client_catalogue_hash(&context);

        let client = JazzClient::connect(context).await.expect("connect client");

        let actual_catalogue_hash = client
            .runtime
            .catalogue_state_hash()
            .expect("read client catalogue hash");
        assert_eq!(
            actual_catalogue_hash, expected_catalogue_hash,
            "client should restore learned permissions head and bundle before any network sync"
        );

        let lens_path_exists = client
            .runtime
            .with_schema_manager(|manager| manager.lens_path(&learned_hash).is_ok())
            .expect("read client schema manager");
        assert!(
            lens_path_exists,
            "permissions rehydrate should preserve the target schema's learned lens context"
        );

        client.shutdown().await.expect("shutdown client");
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn open_persistent_storage_retries_on_lock_contention() {
        let data_dir = TempDir::new().expect("temp dir");
        std::fs::create_dir_all(data_dir.path()).unwrap();

        let db_path = data_dir.path().join("jazz.rocksdb");
        // Hold the DB open so the next open hits a lock error.
        let _holder =
            RocksDBStorage::open(&db_path, 64 * 1024 * 1024).expect("first open should succeed");

        // Spawn a task that drops the holder after a short delay, unblocking the retry.
        let holder_handle = tokio::task::spawn_blocking({
            let holder = _holder;
            move || {
                std::thread::sleep(Duration::from_millis(150));
                drop(holder);
            }
        });

        // open_persistent_storage retries up to 100 times at 25ms intervals.
        // The holder is released after ~150ms, so this should succeed within a few retries.
        let storage = open_persistent_storage(data_dir.path()).await;
        assert!(
            storage.is_ok(),
            "should succeed after lock is released: {:?}",
            storage.err()
        );

        holder_handle.await.expect("holder task should complete");
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn open_persistent_storage_fails_on_non_lock_error() {
        // Point at a file (not a directory) so RocksDB gets a non-lock IO error.
        let data_dir = TempDir::new().expect("temp dir");
        let db_path = data_dir.path().join("jazz.rocksdb");
        // Create a regular file where rocksdb expects a directory.
        std::fs::write(&db_path, b"not a database").unwrap();

        let result = open_persistent_storage(data_dir.path()).await;
        assert!(
            result.is_err(),
            "non-lock errors should not be retried and should fail immediately"
        );
    }
}

fn load_or_create_persistent_client_id(context: &AppContext) -> Result<ClientId> {
    std::fs::create_dir_all(&context.data_dir)?;

    let client_id_path = context.data_dir.join("client_id");
    let client_id = if client_id_path.exists() {
        let id_str = std::fs::read_to_string(&client_id_path)?;
        ClientId::parse(id_str.trim()).unwrap_or_else(|| {
            let id = context.client_id.unwrap_or_default();
            let _ = std::fs::write(&client_id_path, id.to_string());
            id
        })
    } else if let Some(id) = context.client_id {
        std::fs::write(&client_id_path, id.to_string())?;
        id
    } else {
        let id = ClientId::new();
        std::fs::write(&client_id_path, id.to_string())?;
        id
    };

    Ok(client_id)
}

async fn open_persistent_storage(data_dir: &std::path::Path) -> Result<DynStorage> {
    #[cfg(feature = "rocksdb")]
    {
        Ok(Box::new(open_rocksdb_storage(data_dir).await?))
    }
    #[cfg(all(feature = "sqlite", not(feature = "rocksdb")))]
    {
        std::fs::create_dir_all(data_dir)?;
        let db_path = data_dir.join("jazz.sqlite");
        SqliteStorage::open(&db_path)
            .map(|s| Box::new(s) as DynStorage)
            .map_err(|e| {
                JazzError::Connection(format!(
                    "failed to open sqlite storage '{}': {e:?}",
                    db_path.display()
                ))
            })
    }
    #[cfg(not(any(feature = "rocksdb", feature = "sqlite")))]
    {
        tracing::warn!("no persistent storage backend enabled, falling back to MemoryStorage");
        Ok(Box::new(MemoryStorage::new()))
    }
}

#[cfg(feature = "rocksdb")]
async fn open_rocksdb_storage(data_dir: &std::path::Path) -> Result<RocksDBStorage> {
    const MAX_ATTEMPTS: usize = 100;
    const RETRY_DELAY_MS: u64 = 25;

    std::fs::create_dir_all(data_dir)?;

    let db_path = data_dir.join("jazz.rocksdb");
    let mut opened = None;
    let mut last_err = None;

    for attempt in 0..MAX_ATTEMPTS {
        match RocksDBStorage::open(&db_path, 64 * 1024 * 1024) {
            Ok(storage) => {
                opened = Some(storage);
                break;
            }
            Err(err) => {
                let is_lock_error = matches!(
                    &err,
                    StorageError::IoError(msg)
                        if msg.contains("lock") || msg.contains("Lock") || msg.contains("busy")
                );
                if !is_lock_error || attempt + 1 == MAX_ATTEMPTS {
                    last_err = Some(err);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
            }
        }
    }

    opened.ok_or_else(|| {
        JazzError::Connection(format!(
            "failed to open rocksdb storage '{}': {:?}",
            db_path.display(),
            last_err
        ))
    })
}
