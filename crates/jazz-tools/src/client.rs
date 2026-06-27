//! JazzClient implementation.

use std::collections::HashMap;
use std::ops::Deref;
#[cfg(feature = "client-engine")]
use std::rc::Rc;
use std::sync::Arc;
#[cfg(feature = "client-engine")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(any(feature = "client-engine", feature = "rocksdb"))]
use std::time::Duration;

#[cfg(feature = "test-utils")]
use crate::query_manager::types::RowPolicyMode;
#[cfg(feature = "client-engine")]
use crate::schema_api::OrderedRowDelta;
#[cfg(any(feature = "client-engine", feature = "test-utils"))]
use crate::schema_api::Schema;
#[cfg(feature = "client-engine")]
use crate::schema_api::TableName;
use crate::schema_api::{Query, Session, Value, WriteContext};
#[cfg(feature = "client-engine")]
use crate::server::schema_convert::convert_public_schema;
#[cfg(feature = "client-engine")]
use crate::server::websocket_client::WebSocketTransport;
#[cfg(feature = "test-utils")]
use crate::sync::ClientId;
#[cfg(feature = "client-engine")]
use crate::sync::DurabilityTier;
use crate::transaction::BatchId;
#[cfg(feature = "client-engine")]
use crate::transport_auth::AuthConfig as WsAuthConfig;
use base64::Engine;
#[cfg(feature = "client-engine")]
use jazz::db::{
    Db as CoreDb, DbConfig as CoreDbConfig, DbIdentity as CoreDbIdentity, Error as CoreDbError,
    LocalUpdates as CoreLocalUpdates, Propagation as CorePropagation, ReadOpts as CoreReadOpts,
    SubscriptionEvent as CoreSubscriptionEvent, TickScheduler, TickUrgency,
    Transport as CoreTransport, WireTransportAdapter,
};
#[cfg(feature = "client-engine")]
use jazz::groove::records::Value as CoreValue;
#[cfg(feature = "client-engine")]
use jazz::groove::storage::MemoryStorage as CoreMemoryStorage;
#[cfg(all(feature = "client-engine", feature = "rocksdb"))]
use jazz::groove::storage::RocksDbStorage as CoreRocksDbStorage;
#[cfg(feature = "client-engine")]
use jazz::ids::{AuthorId as CoreAuthorId, NodeUuid as CoreNodeUuid, RowUuid as CoreRowUuid};
#[cfg(feature = "client-engine")]
use jazz::tx::{
    DeletionEvent as CoreDeletionEvent, DurabilityTier as CoreDurabilityTier, Fate as CoreFate,
    TxId as CoreTxId,
};
use serde::Deserialize;
use tokio::sync::RwLock;
#[cfg(feature = "client-engine")]
use tokio::sync::mpsc;
use uuid::Uuid;

#[cfg(all(feature = "client-engine", feature = "rocksdb"))]
use crate::ClientStorage;
use crate::{AppContext, JazzError, ObjectId, Result, SubscriptionHandle, SubscriptionStream};

#[cfg(feature = "client-engine")]
type CoreMemoryDb = CoreDb<CoreMemoryStorage>;
#[cfg(all(feature = "client-engine", feature = "rocksdb"))]
type CoreRocksDb = CoreDb<CoreRocksDbStorage>;

#[cfg(feature = "client-engine")]
enum StorageBundle {
    Memory(CoreMemoryStorage),
    #[cfg(feature = "rocksdb")]
    RocksDb(CoreRocksDbStorage),
}

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
    /// Core engine backing the public client facade.
    #[cfg(feature = "client-engine")]
    engine: Rc<ClientEngine>,
    /// Public schema retained for the current public API surface.
    #[cfg(feature = "client-engine")]
    public_schema: Schema,
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
            #[cfg(feature = "client-engine")]
            engine: self.engine.clone(),
            #[cfg(feature = "client-engine")]
            public_schema: self.public_schema.clone(),
            has_server: self.has_server,
            subscriptions: Arc::clone(&self.subscriptions),
            next_handle: Arc::clone(&self.next_handle),
        }
    }
}

#[cfg(feature = "client-engine")]
struct ClientEngine {
    inner: Rc<std::cell::RefCell<ClientEngineInner>>,
}

#[cfg(feature = "client-engine")]
struct ClientEngineInner {
    db: Backend,
    write_map: HashMap<BatchId, CoreTxId>,
    row_tables: HashMap<ObjectId, String>,
    transactions: HashMap<BatchId, DirectTransactionState>,
}

#[cfg(feature = "client-engine")]
enum Backend {
    Memory(Rc<CoreMemoryDb>),
    #[cfg(feature = "rocksdb")]
    RocksDb(Rc<CoreRocksDb>),
}

#[cfg(feature = "client-engine")]
impl Clone for Backend {
    fn clone(&self) -> Self {
        match self {
            Self::Memory(db) => Self::Memory(Rc::clone(db)),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => Self::RocksDb(Rc::clone(db)),
        }
    }
}

#[cfg(feature = "client-engine")]
impl Backend {
    async fn open(
        schema: jazz::schema::JazzSchema,
        storage: StorageBundle,
        identity: CoreDbIdentity,
    ) -> Result<Self> {
        match storage {
            StorageBundle::Memory(storage) => Ok(Self::Memory(Rc::new(
                CoreDb::open(CoreDbConfig::new(schema, storage, identity))
                    .await
                    .map_err(|error| JazzError::Connection(error.to_string()))?,
            ))),
            #[cfg(feature = "rocksdb")]
            StorageBundle::RocksDb(storage) => Ok(Self::RocksDb(Rc::new(
                CoreDb::open(CoreDbConfig::new(schema, storage, identity))
                    .await
                    .map_err(|error| JazzError::Connection(error.to_string()))?,
            ))),
        }
    }

    fn set_tick_scheduler(&self, scheduler: Rc<TickSchedulerImpl>) {
        match self {
            Self::Memory(db) => db.set_tick_scheduler(Some(scheduler)),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.set_tick_scheduler(Some(scheduler)),
        }
    }

    fn connect_upstream(&self, transport: Box<dyn CoreTransport>) {
        match self {
            Self::Memory(db) => {
                db.connect_upstream(transport);
            }
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => {
                db.connect_upstream(transport);
            }
        }
    }

    fn tick(&self) -> std::result::Result<(), CoreDbError> {
        match self {
            Self::Memory(db) => db.tick(),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.tick(),
        }
    }

    fn insert(
        &self,
        table: &str,
        cells: jazz::db::RowCells,
    ) -> std::result::Result<(CoreRowUuid, CoreTxId), CoreDbError> {
        match self {
            Self::Memory(db) => {
                let write = db.insert(table, cells)?;
                Ok((write.row_uuid(), write.mergeable_tx_id()))
            }
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => {
                let write = db.insert(table, cells)?;
                Ok((write.row_uuid(), write.mergeable_tx_id()))
            }
        }
    }

    fn insert_with_id(
        &self,
        table: &str,
        row_id: CoreRowUuid,
        cells: jazz::db::RowCells,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        match self {
            Self::Memory(db) => Ok(db.insert_with_id(table, row_id, cells)?.mergeable_tx_id()),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => Ok(db.insert_with_id(table, row_id, cells)?.mergeable_tx_id()),
        }
    }

    fn upsert(
        &self,
        table: &str,
        row_id: CoreRowUuid,
        cells: jazz::db::RowCells,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        match self {
            Self::Memory(db) => Ok(db.upsert(table, row_id, cells)?.mergeable_tx_id()),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => Ok(db.upsert(table, row_id, cells)?.mergeable_tx_id()),
        }
    }

    fn update(
        &self,
        table: &str,
        row_id: CoreRowUuid,
        cells: jazz::db::RowCells,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        match self {
            Self::Memory(db) => Ok(db.update(table, row_id, cells)?.mergeable_tx_id()),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => Ok(db.update(table, row_id, cells)?.mergeable_tx_id()),
        }
    }

    fn delete(
        &self,
        table: &str,
        row_id: CoreRowUuid,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        match self {
            Self::Memory(db) => Ok(db.delete(table, row_id)?.mergeable_tx_id()),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => Ok(db.delete(table, row_id)?.mergeable_tx_id()),
        }
    }

    fn commit_writes(
        &self,
        writes: Vec<DirectTransactionWrite>,
    ) -> std::result::Result<(CoreTxId, Vec<(ObjectId, String)>), CoreDbError> {
        match self {
            Self::Memory(db) => commit_core_writes(db, writes),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => commit_core_writes(db, writes),
        }
    }

    fn prepare_query(
        &self,
        query: &jazz::query::Query,
    ) -> std::result::Result<jazz::db::PreparedQuery, CoreDbError> {
        match self {
            Self::Memory(db) => db.prepare_query(query),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.prepare_query(query),
        }
    }

    fn query_is_covered(&self, prepared: &jazz::db::PreparedQuery) -> bool {
        match self {
            Self::Memory(db) => db.query_is_covered(prepared),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.query_is_covered(prepared),
        }
    }

    async fn all(
        &self,
        prepared: &jazz::db::PreparedQuery,
        opts: CoreReadOpts,
    ) -> std::result::Result<Vec<jazz::node::CurrentRow>, CoreDbError> {
        match self {
            Self::Memory(db) => db.all(prepared, opts).await,
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.all(prepared, opts).await,
        }
    }

    async fn subscribe(
        &self,
        prepared: &jazz::db::PreparedQuery,
        opts: CoreReadOpts,
    ) -> std::result::Result<jazz::db::SubscriptionStream, CoreDbError> {
        match self {
            Self::Memory(db) => db.subscribe(prepared, opts).await,
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.subscribe(prepared, opts).await,
        }
    }

    fn write_state(
        &self,
        tx_id: CoreTxId,
    ) -> std::result::Result<jazz::db::WriteState, CoreDbError> {
        match self {
            Self::Memory(db) => db.write_state(tx_id),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.write_state(tx_id),
        }
    }

    async fn next_write_state_change(&self, tx_id: CoreTxId) {
        match self {
            Self::Memory(db) => db.next_write_state_change(tx_id).await,
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.next_write_state_change(tx_id).await,
        }
    }
}

#[cfg(feature = "client-engine")]
fn commit_core_writes<S>(
    db: &CoreDb<S>,
    writes: Vec<DirectTransactionWrite>,
) -> std::result::Result<(CoreTxId, Vec<(ObjectId, String)>), CoreDbError>
where
    S: jazz::groove::storage::OrderedKvStorage + jazz::groove::storage::ReopenableStorage + 'static,
{
    let mut tx = db.mergeable_tx();
    let mut touched_rows = Vec::new();
    for write in writes {
        match write.deletion {
            Some(CoreDeletionEvent::Deleted) => {
                tx.delete(&write.table, CoreRowUuid(*write.row_id.uuid()))?
            }
            Some(CoreDeletionEvent::Restored) => {
                tx.restore(&write.table, CoreRowUuid(*write.row_id.uuid()), write.cells)?
            }
            None => tx.update(&write.table, CoreRowUuid(*write.row_id.uuid()), write.cells)?,
        }
        touched_rows.push((write.row_id, write.table));
    }
    let tx_id = tx.commit()?;
    Ok((tx_id, touched_rows))
}

#[cfg(feature = "client-engine")]
struct DirectTransactionState {
    writes: Vec<DirectTransactionWrite>,
}

#[cfg(feature = "client-engine")]
struct DirectTransactionWrite {
    table: String,
    row_id: ObjectId,
    cells: jazz::db::RowCells,
    deletion: Option<CoreDeletionEvent>,
}

#[derive(Default)]
#[cfg(feature = "client-engine")]
struct TickSchedulerImpl {
    state: Arc<TickState>,
}

#[derive(Default)]
#[cfg(feature = "client-engine")]
struct TickState {
    immediate: AtomicBool,
    deferred: AtomicBool,
    notify: tokio::sync::Notify,
}

#[cfg(feature = "client-engine")]
impl TickSchedulerImpl {
    fn take(&self) -> Option<TickUrgency> {
        if self.state.immediate.swap(false, Ordering::AcqRel) {
            self.state.deferred.store(false, Ordering::Release);
            Some(TickUrgency::Immediate)
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
        }
        self.state.notify.notify_one();
    }

    fn wake_handle(&self) -> Arc<TickState> {
        Arc::clone(&self.state)
    }
}

#[cfg(feature = "client-engine")]
impl TickScheduler for TickSchedulerImpl {
    fn schedule_tick(&self, urgency: TickUrgency) {
        self.wake(urgency);
    }
}

#[cfg(feature = "client-engine")]
impl ClientEngine {
    async fn start(
        schema: jazz::schema::JazzSchema,
        storage: StorageBundle,
        identity: CoreDbIdentity,
        server_url: Option<String>,
        app_id: crate::schema_manager::AppId,
        auth: Option<WsAuthConfig>,
    ) -> Result<Rc<Self>> {
        let scheduler = Rc::new(TickSchedulerImpl::default());
        let has_upstream = server_url.is_some();
        let inner = ClientEngineInner::open(
            schema,
            storage,
            identity,
            server_url,
            app_id,
            auth,
            Rc::clone(&scheduler),
        )
        .await?;
        let inner = Rc::new(std::cell::RefCell::new(inner));
        if has_upstream {
            Self::spawn_local_tick_driver(Rc::clone(&inner), Rc::clone(&scheduler));
        }
        Ok(Rc::new(Self { inner }))
    }

    async fn query_rows(
        &self,
        query: jazz::query::Query,
        opts: CoreReadOpts,
        table: String,
        wait_for_coverage: bool,
    ) -> Result<Vec<jazz::node::CurrentRow>> {
        ClientEngineInner::handle_query(&self.inner, query, opts, table, wait_for_coverage).await
    }

    async fn subscribe(
        &self,
        query: jazz::query::Query,
        opts: CoreReadOpts,
        table: String,
        tx: mpsc::UnboundedSender<OrderedRowDelta>,
    ) -> Result<()> {
        ClientEngineInner::handle_subscribe(&self.inner, query, opts, table, tx).await
    }

    fn insert(
        &self,
        table: String,
        row_id: Option<Uuid>,
        cells: jazz::db::RowCells,
    ) -> Result<(ObjectId, CoreTxId)> {
        let mut inner = self.inner.borrow_mut();
        let (row_uuid, tx_id) = match row_id {
            Some(uuid) => {
                let row_uuid = CoreRowUuid(uuid);
                let tx_id = inner
                    .db
                    .insert_with_id(&table, row_uuid, cells)
                    .map_err(|error| JazzError::Write(error.to_string()))?;
                (row_uuid, tx_id)
            }
            None => inner
                .db
                .insert(&table, cells)
                .map_err(|error| JazzError::Write(error.to_string()))?,
        };
        JazzClient::check_core_write_not_rejected(&inner.db, tx_id)?;
        let object_id = ObjectId::from_uuid(row_uuid.0);
        inner.remember_write(object_id, &table, tx_id);
        Ok((object_id, tx_id))
    }

    fn stage_insert(
        &self,
        batch_id: BatchId,
        table: String,
        row_id: Option<Uuid>,
        cells: jazz::db::RowCells,
    ) -> Result<ObjectId> {
        let mut inner = self.inner.borrow_mut();
        let row_id = ObjectId::from_uuid(row_id.unwrap_or_else(Uuid::now_v7));
        let tx = inner
            .transactions
            .get_mut(&batch_id)
            .ok_or_else(|| JazzError::Write(format!("transaction {batch_id} is not open")))?;
        tx.writes.push(DirectTransactionWrite {
            table: table.clone(),
            row_id,
            cells,
            deletion: None,
        });
        inner.row_tables.insert(row_id, table);
        Ok(row_id)
    }

    fn upsert(&self, table: String, row_id: Uuid, cells: jazz::db::RowCells) -> Result<CoreTxId> {
        let mut inner = self.inner.borrow_mut();
        let write = inner
            .db
            .upsert(&table, CoreRowUuid(row_id), cells)
            .map_err(|error| JazzError::Write(error.to_string()))?;
        JazzClient::check_core_write_not_rejected(&inner.db, write)?;
        let object_id = ObjectId::from_uuid(row_id);
        inner.remember_write(object_id, &table, write);
        let tx_id = write;
        Ok(tx_id)
    }

    fn stage_upsert(
        &self,
        batch_id: BatchId,
        table: String,
        row_id: Uuid,
        cells: jazz::db::RowCells,
    ) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let object_id = ObjectId::from_uuid(row_id);
        let tx = inner
            .transactions
            .get_mut(&batch_id)
            .ok_or_else(|| JazzError::Write(format!("transaction {batch_id} is not open")))?;
        tx.writes.push(DirectTransactionWrite {
            table: table.clone(),
            row_id: object_id,
            cells,
            deletion: None,
        });
        inner.row_tables.insert(object_id, table);
        Ok(())
    }

    fn update(&self, row_id: ObjectId, cells: jazz::db::RowCells) -> Result<CoreTxId> {
        let mut inner = self.inner.borrow_mut();
        let table = inner.row_tables.get(&row_id).cloned().ok_or_else(|| {
            JazzError::Write("update requires a row created or observed by this client".to_string())
        })?;
        let write = inner
            .db
            .update(&table, CoreRowUuid(*row_id.uuid()), cells)
            .map_err(|error| JazzError::Write(error.to_string()))?;
        JazzClient::check_core_write_not_rejected(&inner.db, write)?;
        inner.remember_write(row_id, &table, write);
        let tx_id = write;
        Ok(tx_id)
    }

    fn stage_update(
        &self,
        batch_id: BatchId,
        row_id: ObjectId,
        cells: jazz::db::RowCells,
    ) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let table = inner.row_tables.get(&row_id).cloned().ok_or_else(|| {
            JazzError::Write("update requires a row created or observed by this client".to_string())
        })?;
        let tx = inner
            .transactions
            .get_mut(&batch_id)
            .ok_or_else(|| JazzError::Write(format!("transaction {batch_id} is not open")))?;
        tx.writes.push(DirectTransactionWrite {
            table,
            row_id,
            cells,
            deletion: None,
        });
        Ok(())
    }

    fn delete(&self, row_id: ObjectId) -> Result<CoreTxId> {
        let mut inner = self.inner.borrow_mut();
        let table = inner.row_tables.get(&row_id).cloned().ok_or_else(|| {
            JazzError::Write("delete requires a row created or observed by this client".to_string())
        })?;
        let write = inner
            .db
            .delete(&table, CoreRowUuid(*row_id.uuid()))
            .map_err(|error| JazzError::Write(error.to_string()))?;
        JazzClient::check_core_write_not_rejected(&inner.db, write)?;
        inner.remember_write(row_id, &table, write);
        let tx_id = write;
        Ok(tx_id)
    }

    fn stage_delete(&self, batch_id: BatchId, row_id: ObjectId) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let table = inner.row_tables.get(&row_id).cloned().ok_or_else(|| {
            JazzError::Write("delete requires a row created or observed by this client".to_string())
        })?;
        let tx = inner
            .transactions
            .get_mut(&batch_id)
            .ok_or_else(|| JazzError::Write(format!("transaction {batch_id} is not open")))?;
        tx.writes.push(DirectTransactionWrite {
            table,
            row_id,
            cells: jazz::db::RowCells::new(),
            deletion: Some(CoreDeletionEvent::Deleted),
        });
        Ok(())
    }

    fn begin_transaction(&self) -> Result<BatchId> {
        let mut inner = self.inner.borrow_mut();
        let mut batch_id = BatchId::new();
        while inner.transactions.contains_key(&batch_id) || inner.write_map.contains_key(&batch_id)
        {
            batch_id = BatchId::new();
        }
        inner
            .transactions
            .insert(batch_id, DirectTransactionState { writes: Vec::new() });
        Ok(batch_id)
    }

    fn commit_transaction(&self, batch_id: BatchId) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let state = inner
            .transactions
            .remove(&batch_id)
            .ok_or_else(|| JazzError::Write(format!("transaction {batch_id} is not open")))?;
        if state.writes.is_empty() {
            return Err(JazzError::Write(
                "transaction cannot commit without writes".to_string(),
            ));
        }
        let (tx_id, touched_rows) = inner
            .db
            .commit_writes(state.writes)
            .map_err(|error| JazzError::Write(error.to_string()))?;
        JazzClient::check_core_write_not_rejected(&inner.db, tx_id)?;
        inner.write_map.insert(batch_id, tx_id);
        inner.write_map.insert(core_batch_id(tx_id), tx_id);
        for (row_id, table) in touched_rows {
            inner.row_tables.insert(row_id, table);
        }
        Ok(())
    }

    fn rollback_transaction(&self, batch_id: BatchId) -> Result<bool> {
        let mut inner = self.inner.borrow_mut();
        Ok(inner.transactions.remove(&batch_id).is_some())
    }

    async fn wait_for_batch(&self, batch_id: BatchId, tier: DurabilityTier) -> Result<()> {
        ClientEngineInner::handle_wait_for_batch(&self.inner, batch_id, tier).await
    }

    fn spawn_local_tick_driver(
        inner: Rc<std::cell::RefCell<ClientEngineInner>>,
        scheduler: Rc<TickSchedulerImpl>,
    ) {
        let state = scheduler.wake_handle();
        tokio::task::spawn_local(async move {
            loop {
                state.notify.notified().await;
                while let Some(urgency) = scheduler.take() {
                    if urgency == TickUrgency::Deferred {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    if inner.borrow().db.tick().is_err() {
                        return;
                    }
                }
            }
        });
    }
}

#[cfg(feature = "client-engine")]
impl ClientEngineInner {
    async fn open(
        schema: jazz::schema::JazzSchema,
        storage: StorageBundle,
        identity: CoreDbIdentity,
        server_url: Option<String>,
        app_id: crate::schema_manager::AppId,
        auth: Option<WsAuthConfig>,
        scheduler: Rc<TickSchedulerImpl>,
    ) -> Result<Self> {
        let db = Backend::open(schema, storage, identity).await?;
        db.set_tick_scheduler(scheduler.clone());
        if let Some(server_url) = server_url {
            let auth = auth.ok_or_else(|| {
                JazzError::Connection("server connection missing auth config".to_string())
            })?;
            let wake = scheduler.wake_handle();
            let transport = WebSocketTransport::connect_with_wake(
                &server_url,
                app_id,
                identity.author,
                auth,
                Arc::new(move || {
                    wake.immediate.store(true, Ordering::Release);
                    wake.notify.notify_one();
                }),
            )
            .await
            .map_err(|error| JazzError::Connection(error.to_string()))?;
            db.connect_upstream(Box::new(WireTransportAdapter::current(transport)));
        }
        Ok(Self {
            db,
            write_map: HashMap::new(),
            row_tables: HashMap::new(),
            transactions: HashMap::new(),
        })
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
            Self::wait_for_query_coverage(inner, &prepared, opts).await?;
        }
        let (db, prepared) = {
            let inner = inner.borrow();
            (inner.db.clone(), prepared)
        };
        let rows = db
            .all(&prepared, opts)
            .await
            .map_err(|error| JazzError::Query(error.to_string()))?;
        inner.borrow_mut().remember_rows(&table, &rows);
        Ok(rows)
    }

    async fn wait_for_query_coverage(
        inner: &Rc<std::cell::RefCell<Self>>,
        prepared: &jazz::db::PreparedQuery,
        opts: CoreReadOpts,
    ) -> Result<()> {
        if inner.borrow().db.query_is_covered(prepared) {
            return Ok(());
        }
        let mut stream = {
            let inner = inner.borrow();
            inner
                .db
                .subscribe(prepared, opts)
                .await
                .map_err(|error| JazzError::Query(error.to_string()))?
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if inner.borrow().db.query_is_covered(prepared) {
                return Ok(());
            }
            let event = tokio::time::timeout_at(deadline, stream.next_event())
                .await
                .map_err(|_| {
                    JazzError::Query("timed out waiting for query coverage".to_string())
                })?;
            match event {
                Some(CoreSubscriptionEvent::Opened { settled, .. })
                | Some(CoreSubscriptionEvent::Reset { settled, .. })
                | Some(CoreSubscriptionEvent::Delta { settled, .. })
                    if settled =>
                {
                    return Ok(());
                }
                Some(CoreSubscriptionEvent::Closed) | None => {
                    return Err(JazzError::Query(
                        "query coverage subscription closed before settling".to_string(),
                    ));
                }
                Some(_) => {}
            }
        }
    }

    async fn handle_subscribe(
        inner: &Rc<std::cell::RefCell<Self>>,
        query: jazz::query::Query,
        opts: CoreReadOpts,
        table: String,
        tx: mpsc::UnboundedSender<OrderedRowDelta>,
    ) -> Result<()> {
        let (db, prepared) = {
            let inner = inner.borrow();
            let prepared = inner
                .db
                .prepare_query(&query)
                .map_err(|error| JazzError::Query(error.to_string()))?;
            (inner.db.clone(), prepared)
        };
        let stream = db
            .subscribe(&prepared, opts)
            .await
            .map_err(|error| JazzError::Query(error.to_string()))?;
        let inner = Rc::clone(inner);
        tokio::task::spawn_local(async move {
            let mut stream = stream;
            loop {
                match stream.next_event().await {
                    Some(CoreSubscriptionEvent::Opened { current, .. })
                    | Some(CoreSubscriptionEvent::Reset { current, .. }) => {
                        inner.borrow_mut().remember_rows(&table, &current);
                        let _ = tx.send(OrderedRowDelta::default());
                    }
                    Some(CoreSubscriptionEvent::Delta { .. }) => {
                        let _ = tx.send(OrderedRowDelta::default());
                    }
                    Some(CoreSubscriptionEvent::Closed) | None => break,
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
        let desired = core_tier(tier);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
        loop {
            let tx_id = {
                let borrowed = inner.borrow();
                if let Some(tx_id) = borrowed.write_map.get(&batch_id).copied() {
                    tx_id
                } else if borrowed.transactions.contains_key(&batch_id) {
                    drop(borrowed);
                    if tokio::time::Instant::now() >= deadline {
                        return Err(JazzError::Sync(format!(
                            "timed out waiting for batch {batch_id}"
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                } else {
                    return Err(JazzError::Sync(format!("unknown batch {batch_id}")));
                }
            };
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
                    "timed out waiting for batch to reach {tier:?}"
                )));
            }
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
            let db = inner.borrow().db.clone();
            if tokio::time::timeout_at(deadline, db.next_write_state_change(tx_id))
                .await
                .is_err()
            {
                return Err(JazzError::Sync(format!(
                    "timed out waiting for batch to reach {tier:?}"
                )));
            }
        }
    }

    fn remember_write(&mut self, row_id: ObjectId, table: &str, tx_id: CoreTxId) {
        self.write_map.insert(core_batch_id(tx_id), tx_id);
        self.row_tables.insert(row_id, table.to_string());
    }

    fn remember_rows(&mut self, table: &str, rows: &[jazz::node::CurrentRow]) {
        for row in rows {
            self.row_tables
                .insert(ObjectId::from_uuid(row.row_uuid().0), table.to_string());
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
struct SubscriptionState;

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

#[cfg(feature = "client-engine")]
fn core_identity(context: &AppContext, default_session: Option<&Session>) -> CoreDbIdentity {
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

#[cfg(feature = "client-engine")]
fn core_storage(schema: &jazz::schema::JazzSchema, context: &AppContext) -> Result<StorageBundle> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    #[cfg(feature = "rocksdb")]
    {
        match context.storage {
            ClientStorage::Memory => Ok(StorageBundle::Memory(CoreMemoryStorage::new(&refs))),
            ClientStorage::Persistent => {
                std::fs::create_dir_all(&context.data_dir)?;
                let db_path = context.data_dir.join("jazz-core.rocksdb");
                let storage = CoreRocksDbStorage::open(&db_path, &refs)
                    .map_err(|error| JazzError::Connection(error.to_string()))?;
                Ok(StorageBundle::RocksDb(storage))
            }
        }
    }
    #[cfg(not(feature = "rocksdb"))]
    {
        let _ = context;
        Ok(StorageBundle::Memory(CoreMemoryStorage::new(&refs)))
    }
}

#[cfg(feature = "client-engine")]
fn public_to_core_value(value: Value) -> Result<CoreValue> {
    match value {
        Value::Boolean(value) => Ok(CoreValue::Bool(value)),
        Value::Text(value) => Ok(CoreValue::String(value)),
        Value::Integer(value) => Ok(CoreValue::U32(encode_signed_i32_for_core(value))),
        Value::BigInt(value) => u64::try_from(value).map(CoreValue::U64).map_err(|_| {
            JazzError::Write("negative BIGINT values are not supported by core".to_string())
        }),
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

#[cfg(feature = "client-engine")]
fn encode_signed_i32_for_core(value: i32) -> u32 {
    u32::from_ne_bytes(value.to_ne_bytes()) ^ 0x8000_0000
}

#[cfg(feature = "client-engine")]
fn decode_signed_i32_from_core(value: u32) -> i32 {
    i32::from_ne_bytes((value ^ 0x8000_0000).to_ne_bytes())
}

#[cfg(feature = "client-engine")]
fn core_to_public_value(value: CoreValue) -> Result<Value> {
    match value {
        CoreValue::Bool(value) => Ok(Value::Boolean(value)),
        CoreValue::String(value) => Ok(Value::Text(value)),
        CoreValue::U32(value) => Ok(Value::Integer(decode_signed_i32_from_core(value))),
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
        other => Err(JazzError::Query(format!(
            "client does not support engine value {other:?}"
        ))),
    }
}

#[cfg(feature = "client-engine")]
fn core_batch_id(tx_id: CoreTxId) -> BatchId {
    let mut bytes = *tx_id.node.0.as_bytes();
    bytes[..8].copy_from_slice(&tx_id.time.0.to_be_bytes());
    BatchId(bytes)
}

#[cfg(feature = "client-engine")]
fn core_tier(tier: DurabilityTier) -> CoreDurabilityTier {
    match tier {
        DurabilityTier::Local => CoreDurabilityTier::Local,
        DurabilityTier::EdgeServer | DurabilityTier::GlobalServer => CoreDurabilityTier::Global,
    }
}

impl JazzClient {
    #[cfg(feature = "client-engine")]
    fn check_core_write_not_rejected(db: &Backend, tx_id: CoreTxId) -> Result<()> {
        let state = db
            .write_state(tx_id)
            .map_err(|error| JazzError::Write(error.to_string()))?;
        if let CoreFate::Rejected(reason) = state.fate {
            return Err(JazzError::Write(format!("core write rejected: {reason:?}")));
        }
        Ok(())
    }

    #[cfg(feature = "client-engine")]
    fn core_read_opts(durability_tier: Option<DurabilityTier>) -> CoreReadOpts {
        CoreReadOpts {
            tier: durability_tier
                .map(core_tier)
                .unwrap_or(CoreDurabilityTier::Local),
            local_updates: CoreLocalUpdates::Immediate,
            propagation: CorePropagation::Full,
            include_deleted: false,
        }
    }

    #[cfg(feature = "client-engine")]
    fn core_query(&self, query: &Query) -> Result<jazz::query::Query> {
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
                "JazzClient currently supports simple table queries only".to_string(),
            ));
        }
        let mut core_query = jazz::query::Query::from(query.table.as_str());
        if let Some(columns) = query.select_columns.clone() {
            core_query = core_query.select(columns);
        }
        Ok(core_query)
    }

    #[cfg(feature = "client-engine")]
    fn core_rows_to_public(
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
                            .ok_or_else(|| JazzError::Query(format!("row missing column {column}")))
                            .and_then(core_to_public_value)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((row_id, values))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(rows)
    }

    #[cfg(feature = "client-engine")]
    fn core_cells(values: HashMap<String, Value>) -> Result<jazz::db::RowCells> {
        values
            .into_iter()
            .map(|(name, value)| Ok((name, public_to_core_value(value)?)))
            .collect()
    }

    #[cfg(feature = "client-engine")]
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
                values.get(column.name.as_str()).cloned().ok_or_else(|| {
                    JazzError::Write(format!(
                        "core insert missing required column {}",
                        column.name.as_str()
                    ))
                })
            })
            .collect()
    }

    #[cfg(feature = "client-engine")]
    fn apply_core_transaction_overlay(
        &self,
        query: &Query,
        batch_id: BatchId,
        rows: &mut Vec<(ObjectId, Vec<Value>)>,
    ) -> Result<()> {
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

        let inner = self.engine.inner.borrow();
        let tx = inner
            .transactions
            .get(&batch_id)
            .ok_or_else(|| JazzError::Query(format!("transaction {batch_id} is not open")))?;

        for write in tx.writes.iter().filter(|write| write.table == table) {
            if write.deletion == Some(CoreDeletionEvent::Deleted) {
                rows.retain(|(row_id, _)| *row_id != write.row_id);
                continue;
            }

            let existing_position = rows.iter().position(|(row_id, _)| *row_id == write.row_id);
            let mut values = existing_position
                .map(|position| rows[position].1.clone())
                .unwrap_or_else(|| vec![Value::Null; columns.len()]);

            for (column, value) in &write.cells {
                if let Some(position) = columns.iter().position(|candidate| candidate == column) {
                    values[position] = core_to_public_value(value.clone())?;
                }
            }

            if let Some(position) = existing_position {
                rows[position].1 = values;
            } else {
                rows.push((write.row_id, values));
            }
        }

        Ok(())
    }

    /// Connect to Jazz with the given configuration.
    ///
    /// This will:
    /// 1. Open local storage
    /// 2. Initialize the runtime
    /// 3. Connect to the server over WebSocket (if URL provided)
    /// 4. Wait for the initial WS handshake to complete
    pub async fn connect(context: AppContext) -> Result<Self> {
        Self::connect_inner(context).await
    }

    async fn connect_inner(context: AppContext) -> Result<Self> {
        let default_session = default_session_from_context(&context);
        let has_server = !context.server_url.is_empty();

        #[cfg(feature = "client-engine")]
        {
            let schema_convert = convert_public_schema(&context.schema)
                .map_err(|error| JazzError::Schema(error.to_string()))?;
            let identity = core_identity(&context, default_session.as_ref());
            let storage = core_storage(&schema_convert, &context)?;
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
            let core_engine = ClientEngine::start(
                schema_convert,
                storage,
                identity,
                has_server.then(|| context.server_url.clone()),
                context.app_id,
                auth,
            )
            .await
            .map_err(|error| JazzError::Connection(error.to_string()))?;
            let client = Self {
                default_session,
                write_context: None,
                engine: core_engine,
                public_schema: context.schema.clone(),
                has_server,
                subscriptions: Arc::new(RwLock::new(HashMap::new())),
                next_handle: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            };
            Ok(client)
        }
    }

    #[cfg(feature = "test-utils")]
    pub async fn connect_with_row_policy_mode(
        context: AppContext,
        _row_policy_mode: RowPolicyMode,
    ) -> Result<Self> {
        Self::connect_inner(context).await
    }

    /// Subscribe to a query.
    ///
    /// Returns a stream of row deltas as the data changes.
    pub async fn subscribe(&self, query: Query) -> Result<SubscriptionStream> {
        #[cfg(feature = "client-engine")]
        {
            let _handle = SubscriptionHandle(
                self.next_handle
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            );
            let (tx, rx) = mpsc::unbounded_channel::<OrderedRowDelta>();
            let core_query = self.core_query(&query)?;
            self.engine
                .subscribe(
                    core_query,
                    Self::core_read_opts(Some(DurabilityTier::EdgeServer)),
                    query.table.as_str().to_string(),
                    tx,
                )
                .await?;
            Ok(SubscriptionStream::new(rx))
        }
    }

    /// One-shot query, optionally waiting for a durability tier.
    ///
    /// Returns the current results as `Vec<(ObjectId, Vec<Value>)>`.
    pub async fn query(
        &self,
        query: Query,
        durability_tier: Option<DurabilityTier>,
    ) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        #[cfg(feature = "client-engine")]
        {
            let opts = Self::core_read_opts(durability_tier);
            let rows = self
                .engine
                .query_rows(
                    self.core_query(&query)?,
                    opts,
                    query.table.as_str().to_string(),
                    durability_tier.is_some(),
                )
                .await?;
            let mut rows = self.core_rows_to_public(&query, rows)?;
            if let Some(batch_id) = self.write_context.as_ref().and_then(|ctx| ctx.batch_id) {
                self.apply_core_transaction_overlay(&query, batch_id, &mut rows)?;
            }
            Ok(rows)
        }
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
        #[cfg(feature = "client-engine")]
        {
            let row_values = self.core_ordered_values(table, &values)?;
            let cells = Self::core_cells(values)?;
            if let Some(batch_id) = self.write_context.as_ref().and_then(|ctx| ctx.batch_id) {
                let row_id = self.engine.stage_insert(
                    batch_id,
                    table.to_string(),
                    object_id.into(),
                    cells,
                )?;
                Ok((row_id, row_values, batch_id))
            } else {
                let (row_id, tx_id) =
                    self.engine
                        .insert(table.to_string(), object_id.into(), cells)?;
                let batch_id = core_batch_id(tx_id);
                Ok((row_id, row_values, batch_id))
            }
        }
    }

    /// Create or update a row using a caller-supplied UUID.
    pub fn upsert(
        &self,
        table: &str,
        object_id: Uuid,
        values: HashMap<String, Value>,
    ) -> Result<BatchId> {
        #[cfg(feature = "client-engine")]
        {
            let cells = Self::core_cells(values)?;
            if let Some(batch_id) = self.write_context.as_ref().and_then(|ctx| ctx.batch_id) {
                self.engine
                    .stage_upsert(batch_id, table.to_string(), object_id, cells)?;
                Ok(batch_id)
            } else {
                let tx_id = self.engine.upsert(table.to_string(), object_id, cells)?;
                Ok(core_batch_id(tx_id))
            }
        }
    }

    /// Update a row.
    pub fn update(&self, object_id: ObjectId, updates: Vec<(String, Value)>) -> Result<BatchId> {
        #[cfg(feature = "client-engine")]
        {
            let cells = Self::core_cells(updates.into_iter().collect())?;
            if let Some(batch_id) = self.write_context.as_ref().and_then(|ctx| ctx.batch_id) {
                self.engine.stage_update(batch_id, object_id, cells)?;
                Ok(batch_id)
            } else {
                let tx_id = self.engine.update(object_id, cells)?;
                Ok(core_batch_id(tx_id))
            }
        }
    }

    /// Delete a row.
    pub fn delete(&self, object_id: ObjectId) -> Result<BatchId> {
        #[cfg(feature = "client-engine")]
        {
            if let Some(batch_id) = self.write_context.as_ref().and_then(|ctx| ctx.batch_id) {
                self.engine.stage_delete(batch_id, object_id)?;
                Ok(batch_id)
            } else {
                let tx_id = self.engine.delete(object_id)?;
                Ok(core_batch_id(tx_id))
            }
        }
    }

    /// Begin a transaction and return a transaction-scoped client handle.
    ///
    /// Mutations issued through the returned handle are staged locally and are
    /// not visible to ordinary reads until the transaction is committed and
    /// accepted by the authority.
    pub fn begin_transaction(&self) -> Result<JazzTransaction> {
        #[cfg(feature = "client-engine")]
        {
            let batch_id = self.engine.begin_transaction()?;
            let client = self.with_write_context(WriteContext::default().with_batch_id(batch_id));
            Ok(JazzTransaction { batch_id, client })
        }
    }

    /// Commit an open transaction by batch id.
    pub fn commit_transaction(&self, batch_id: BatchId) -> Result<()> {
        #[cfg(feature = "client-engine")]
        {
            self.engine.commit_transaction(batch_id)
        }
    }

    /// Roll back an open transaction by batch id.
    ///
    /// Returns whether a local batch record existed for the transaction.
    pub fn rollback_transaction(&self, batch_id: BatchId) -> Result<bool> {
        #[cfg(feature = "client-engine")]
        {
            self.engine.rollback_transaction(batch_id)
        }
    }

    pub async fn wait_for_batch(&self, batch_id: BatchId, tier: DurabilityTier) -> Result<()> {
        #[cfg(feature = "client-engine")]
        {
            self.engine.wait_for_batch(batch_id, tier).await
        }
    }

    /// Unsubscribe from a subscription.
    pub async fn unsubscribe(&self, handle: SubscriptionHandle) -> Result<()> {
        let mut subs = self.subscriptions.write().await;
        let _ = subs.remove(&handle);
        Ok(())
    }

    /// Get the current schema.
    pub fn schema(&self) -> Result<Schema> {
        #[cfg(feature = "client-engine")]
        {
            Ok(self.public_schema.clone())
        }
    }

    /// Check if connected to server.
    pub fn is_connected(&self) -> bool {
        false
    }

    /// Create a client that uses the given write context for mutations.
    pub fn with_write_context(&self, write_context: WriteContext) -> JazzClient {
        JazzClient {
            default_session: self.default_session.clone(),
            write_context: Some(write_context),
            #[cfg(feature = "client-engine")]
            engine: self.engine.clone(),
            #[cfg(feature = "client-engine")]
            public_schema: self.public_schema.clone(),
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
        Ok(())
    }
}

#[cfg(feature = "test-utils")]
impl JazzClient {
    pub fn client_id(&self) -> Option<ClientId> {
        None
    }

    #[cfg(feature = "client-engine")]
    pub fn local_driver_active(&self) -> bool {
        true
    }

    pub async fn test_client(schema: Schema) -> crate::JazzClient {
        let context = crate::AppContext::test(schema);
        crate::JazzClient::connect(context)
            .await
            .expect("connect local JazzClient")
    }

    #[cfg(feature = "client-engine")]
    pub async fn connect_with_local_driver(context: AppContext) -> Result<Self> {
        Self::connect_inner(context).await
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
        let _ = self;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_api::Schema;
    use crate::schema_manager::AppId;
    use crate::{ClientStorage, ColumnType, SchemaBuilder, TableSchema};
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
        }
    }

    fn make_offline_context_with_storage(
        app_id: AppId,
        data_dir: std::path::PathBuf,
        schema: Schema,
        storage: ClientStorage,
    ) -> AppContext {
        AppContext {
            storage,
            ..make_offline_context(app_id, data_dir, schema)
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

    #[cfg(feature = "client-engine")]
    #[test]
    fn core_integer_bridge_preserves_signed_i32_bits() {
        let core_value =
            public_to_core_value(Value::Integer(-1)).expect("negative i32 should encode for core");

        assert_eq!(core_value, CoreValue::U32(0x7fff_ffff));
        assert_eq!(
            core_to_public_value(core_value).expect("decode signed i32"),
            Value::Integer(-1)
        );
        assert_eq!(
            public_to_core_value(Value::Integer(0)).expect("encode zero"),
            CoreValue::U32(0x8000_0000)
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

    #[cfg(feature = "client-engine")]
    #[tokio::test]
    async fn core_engine_transaction_stages_and_commits() {
        let public_schema = declared_todo_schema();
        let schema_convert = convert_public_schema(&public_schema).expect("convert schema");
        let storage = core_storage(
            &schema_convert,
            &make_offline_context(
                AppId::from_name("core-transaction-test"),
                TempDir::new().expect("tempdir").keep(),
                public_schema.clone(),
            ),
        )
        .expect("open engine storage");
        let engine = ClientEngine::start(
            schema_convert.clone(),
            storage,
            CoreDbIdentity {
                node: CoreNodeUuid::from_bytes([0x11; 16]),
                author: CoreAuthorId::from_bytes([0xa1; 16]),
            },
            None,
            AppId::from_name("core-transaction-test"),
            None,
        )
        .await
        .expect("open client engine");

        let batch_id = engine.begin_transaction().expect("begin transaction");
        let row_id = engine
            .stage_insert(
                batch_id,
                "todos".to_string(),
                None,
                jazz::row! {
                    title: "staged",
                    completed: false,
                },
            )
            .expect("stage insert");
        assert!(
            engine.inner.borrow().write_map.get(&batch_id).is_none(),
            "staged transaction should not be committed yet",
        );

        engine
            .commit_transaction(batch_id)
            .expect("commit transaction");
        engine
            .wait_for_batch(batch_id, DurabilityTier::Local)
            .await
            .expect("wait for committed transaction");

        let rows = engine
            .query_rows(
                jazz::query::Query::from("todos"),
                CoreReadOpts::default(),
                "todos".to_string(),
                false,
            )
            .await
            .expect("query committed rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(ObjectId::from_uuid(rows[0].row_uuid().0), row_id);
    }

    #[cfg(feature = "rocksdb")]
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
        let (row_id, _values, batch_id) = client
            .insert(
                "todos",
                crate::row_input!("title" => "rehydrated", "completed" => false),
            )
            .expect("insert offline persistent row");
        client
            .wait_for_batch(batch_id, DurabilityTier::Local)
            .await
            .expect("wait for local durability");
        drop(client);

        let restarted = JazzClient::connect(context)
            .await
            .expect("reconnect offline persistent client");
        let rows = restarted
            .query(Query::new("todos"), Some(DurabilityTier::Local))
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

    #[cfg(feature = "rocksdb")]
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
        let (_row_id, _values, batch_id) = client
            .insert(
                "todos",
                crate::row_input!("title" => "memory", "completed" => false),
            )
            .expect("insert offline memory row");
        client
            .wait_for_batch(batch_id, DurabilityTier::Local)
            .await
            .expect("wait for local durability");
        drop(client);

        assert!(
            !data_dir.path().join("jazz-core.rocksdb").exists(),
            "memory storage should not create a RocksDB data directory"
        );
    }
}
