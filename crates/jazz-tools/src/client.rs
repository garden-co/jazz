//! Thin Rust client facade over `jazz::db`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::public_api::query::{
    Condition as PublicCondition, SortDirection as PublicSortDirection,
};
use crate::public_api::types::{OrderedAdded, OrderedRemoved, OrderedUpdated};
use crate::public_schema::Schema;
use crate::public_schema::TableName;
use crate::public_schema::{
    ColumnType, LargeValueHandle, Query, Session, TableSchema, Value, WriteContext,
};
use crate::public_schema::{OrderedRowDelta, Row};
use crate::server::core_websocket_transport::WebSocketTransport;
use crate::server::public_schema_convert::convert_public_schema;
#[cfg(feature = "test-utils")]
use crate::sync::ClientId;
use crate::sync::DurabilityTier;
use crate::transaction::BatchId;
use crate::websocket_prelude_auth::AuthConfig as WsAuthConfig;
use base64::Engine;
use jazz::db::{
    Db as CoreDb, DbConfig as CoreDbConfig, DbIdentity as CoreDbIdentity, Error as CoreDbError,
    LocalUpdates as CoreLocalUpdates, PeerConnection as CorePeerConnection,
    Propagation as CorePropagation, ReadOpts as CoreReadOpts,
    SubscriptionEvent as CoreSubscriptionEvent, TextEdit as CoreTextEdit, TickScheduler,
    TickUrgency, Transport as CoreTransport, WireTransportAdapter,
};
use jazz::groove::records::Value as CoreValue;
use jazz::groove::storage::MemoryStorage as CoreMemoryStorage;
#[cfg(feature = "rocksdb")]
use jazz::groove::storage::RocksDbStorage as CoreRocksDbStorage;
use jazz::ids::{AuthorId as CoreAuthorId, NodeUuid as CoreNodeUuid, RowUuid as CoreRowUuid};
use jazz::node::OpenTxId as CoreOpenTxId;
use jazz::tx::{
    DeletionEvent as CoreDeletionEvent, DurabilityTier as CoreDurabilityTier, Fate as CoreFate,
    RejectionReason as CoreRejectionReason, TxId as CoreTxId,
};
use serde::Deserialize;
use tokio::sync::mpsc;
use uuid::Uuid;

#[cfg(feature = "rocksdb")]
use crate::ClientStorage;
use crate::{AppContext, JazzError, ObjectId, Result, SubscriptionHandle, SubscriptionStream};

type CoreMemoryDb = CoreDb<CoreMemoryStorage>;
#[cfg(feature = "rocksdb")]
type CoreRocksDb = CoreDb<CoreRocksDbStorage>;

enum BackendConnection {
    Memory(Rc<RefCell<CorePeerConnection<CoreMemoryStorage>>>),
    #[cfg(feature = "rocksdb")]
    RocksDb(Rc<RefCell<CorePeerConnection<CoreRocksDbStorage>>>),
}

const QUERY_COVERAGE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_TEST_WAIT_TIMEOUT_MULTIPLIER: u32 = 8;
const LARGE_VALUE_HANDLE_MAGIC: &[u8] = b"JLVH1";

fn load_tolerant_test_timeout(timeout: Duration) -> Duration {
    let multiplier = std::env::var("JAZZ_TOOLS_TEST_WAIT_TIMEOUT_MULTIPLIER")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_TEST_WAIT_TIMEOUT_MULTIPLIER);
    timeout.checked_mul(multiplier).unwrap_or(timeout)
}

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
    /// Shared core database handle backing the public client facade.
    db: Rc<ClientDb>,
    /// Public schema retained for the current public API surface.
    public_schema: Schema,
}

impl Clone for JazzClient {
    fn clone(&self) -> Self {
        Self {
            default_session: self.default_session.clone(),
            write_context: self.write_context.clone(),
            db: self.db.clone(),
            public_schema: self.public_schema.clone(),
        }
    }
}

struct ClientDb {
    inner: Rc<RefCell<ClientDbInner>>,
}

struct ClientDbInner {
    db: Backend,
    identity: CoreDbIdentity,
    connect_config: Option<ConnectConfig>,
    scheduler: Rc<TickSchedulerImpl>,
    upstream: Option<BackendConnection>,
    write_map: HashMap<BatchId, CoreTxId>,
    row_tables: HashMap<ObjectId, String>,
    transactions: HashMap<BatchId, ExclusiveTransactionState>,
    closed_transactions: HashMap<BatchId, ClosedTransactionState>,
}

#[derive(Clone)]
struct ConnectConfig {
    server_url: String,
    app_id: crate::AppId,
    auth: WsAuthConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClosedTransactionState {
    Committed,
    RolledBack,
}

enum Backend {
    Memory(Rc<CoreMemoryDb>),
    #[cfg(feature = "rocksdb")]
    RocksDb(Rc<CoreRocksDb>),
}

impl Clone for Backend {
    fn clone(&self) -> Self {
        match self {
            Self::Memory(db) => Self::Memory(Rc::clone(db)),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => Self::RocksDb(Rc::clone(db)),
        }
    }
}

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

    fn connect_upstream(&self, transport: Box<dyn CoreTransport>) -> BackendConnection {
        match self {
            Self::Memory(db) => BackendConnection::Memory(db.connect_upstream(transport)),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => BackendConnection::RocksDb(db.connect_upstream(transport)),
        }
    }

    fn detach_connection(&self, connection: &BackendConnection) -> bool {
        match (self, connection) {
            (Self::Memory(db), BackendConnection::Memory(connection)) => {
                db.detach_connection(connection)
            }
            #[cfg(feature = "rocksdb")]
            (Self::RocksDb(db), BackendConnection::RocksDb(connection)) => {
                db.detach_connection(connection)
            }
            #[allow(unreachable_patterns)]
            _ => false,
        }
    }

    fn set_identity_claims(&self, identity: CoreAuthorId, claims: HashMap<String, CoreValue>) {
        match self {
            Self::Memory(db) => db.set_identity_claims(identity, claims.into_iter().collect()),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.set_identity_claims(identity, claims.into_iter().collect()),
        }
    }

    fn tick(&self) -> std::result::Result<(), CoreDbError> {
        match self {
            Self::Memory(db) => db.tick(),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.tick(),
        }
    }

    fn hydrate_large_value_handle(
        &self,
        handle: &[u8],
    ) -> std::result::Result<Vec<u8>, CoreDbError> {
        match self {
            Self::Memory(db) => db.hydrate_large_value_handle(handle),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.hydrate_large_value_handle(handle),
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

    fn insert_for_identity(
        &self,
        identity: CoreAuthorId,
        table: &str,
        cells: jazz::db::RowCells,
    ) -> std::result::Result<(CoreRowUuid, CoreTxId), CoreDbError> {
        match self {
            Self::Memory(db) => {
                let write = db.insert_for_identity(identity, table, cells)?;
                Ok((write.row_uuid(), write.mergeable_tx_id()))
            }
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => {
                let write = db.insert_for_identity(identity, table, cells)?;
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

    fn insert_with_id_for_identity(
        &self,
        identity: CoreAuthorId,
        table: &str,
        row_id: CoreRowUuid,
        cells: jazz::db::RowCells,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        match self {
            Self::Memory(db) => Ok(db
                .insert_with_id_for_identity(identity, table, row_id, cells)?
                .mergeable_tx_id()),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => Ok(db
                .insert_with_id_for_identity(identity, table, row_id, cells)?
                .mergeable_tx_id()),
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

    fn upsert_for_identity(
        &self,
        identity: CoreAuthorId,
        table: &str,
        row_id: CoreRowUuid,
        cells: jazz::db::RowCells,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        match self {
            Self::Memory(db) => Ok(db
                .upsert_for_identity(identity, table, row_id, cells)?
                .mergeable_tx_id()),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => Ok(db
                .upsert_for_identity(identity, table, row_id, cells)?
                .mergeable_tx_id()),
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

    fn edit_text(
        &self,
        table: &str,
        row_id: CoreRowUuid,
        column: &str,
        edit: CoreTextEdit,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        match self {
            Self::Memory(db) => Ok(db.edit_text(table, row_id, column, edit)?.mergeable_tx_id()),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => Ok(db.edit_text(table, row_id, column, edit)?.mergeable_tx_id()),
        }
    }

    fn delete_for_identity(
        &self,
        identity: CoreAuthorId,
        table: &str,
        row_id: CoreRowUuid,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        match self {
            Self::Memory(db) => Ok(db
                .delete_for_identity(identity, table, row_id)?
                .mergeable_tx_id()),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => Ok(db
                .delete_for_identity(identity, table, row_id)?
                .mergeable_tx_id()),
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

    fn attach_query(
        &self,
        prepared: &jazz::db::PreparedQuery,
        opts: CoreReadOpts,
    ) -> std::result::Result<jazz::db::QueryAttachment, CoreDbError> {
        match self {
            Self::Memory(db) => db.attach_query_with_opts(prepared, opts),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.attach_query_with_opts(prepared, opts),
        }
    }

    fn query_attachment_is_covered(&self, attachment: &jazz::db::QueryAttachment) -> bool {
        match self {
            Self::Memory(db) => db.query_attachment_is_covered(attachment),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.query_attachment_is_covered(attachment),
        }
    }

    fn detach_query(&self, attachment: jazz::db::QueryAttachment) {
        match self {
            Self::Memory(db) => db.detach_query(attachment),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.detach_query(attachment),
        }
    }

    fn row_provenance(
        &self,
        row: &jazz::node::CurrentRow,
    ) -> std::result::Result<Option<jazz::node::RowProvenance>, CoreDbError> {
        match self {
            Self::Memory(db) => db.row_provenance(row),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.row_provenance(row),
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

    fn begin_exclusive(&self) -> std::result::Result<CoreOpenTxId, CoreDbError> {
        match self {
            Self::Memory(db) => db.begin_exclusive(),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.begin_exclusive(),
        }
    }

    fn exclusive_write(
        &self,
        tx_id: CoreOpenTxId,
        table: &str,
        row_id: CoreRowUuid,
        cells: jazz::db::RowCells,
    ) -> std::result::Result<(), CoreDbError> {
        match self {
            Self::Memory(db) => db.exclusive_write(tx_id, table, row_id, cells),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.exclusive_write(tx_id, table, row_id, cells),
        }
    }

    fn exclusive_update(
        &self,
        tx_id: CoreOpenTxId,
        table: &str,
        row_id: CoreRowUuid,
        cells: jazz::db::RowCells,
    ) -> std::result::Result<(), CoreDbError> {
        match self {
            Self::Memory(db) => db.exclusive_update(tx_id, table, row_id, cells),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.exclusive_update(tx_id, table, row_id, cells),
        }
    }

    fn exclusive_delete(
        &self,
        tx_id: CoreOpenTxId,
        table: &str,
        row_id: CoreRowUuid,
    ) -> std::result::Result<(), CoreDbError> {
        match self {
            Self::Memory(db) => db.exclusive_delete(tx_id, table, row_id),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.exclusive_delete(tx_id, table, row_id),
        }
    }

    fn commit_exclusive_handle(
        &self,
        tx_id: CoreOpenTxId,
    ) -> std::result::Result<CoreTxId, CoreDbError> {
        match self {
            Self::Memory(db) => db.commit_exclusive_handle(tx_id),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.commit_exclusive_handle(tx_id),
        }
    }
}

struct ExclusiveTransactionState {
    tx_id: CoreOpenTxId,
    writes: Vec<ExclusiveTransactionWrite>,
}

struct ExclusiveTransactionWrite {
    table: String,
    row_id: ObjectId,
    cells: jazz::db::RowCells,
    deletion: Option<CoreDeletionEvent>,
}

#[derive(Default)]
struct TickSchedulerImpl {
    state: Arc<TickState>,
}

#[derive(Default)]
struct TickState {
    immediate: AtomicBool,
    deferred: AtomicBool,
    notify: tokio::sync::Notify,
}

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

impl TickScheduler for TickSchedulerImpl {
    fn schedule_tick(&self, urgency: TickUrgency) {
        self.wake(urgency);
    }
}

impl ClientDb {
    async fn open(
        schema: jazz::schema::JazzSchema,
        storage: StorageBundle,
        identity: CoreDbIdentity,
        server_url: Option<String>,
        app_id: crate::AppId,
        auth: Option<WsAuthConfig>,
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
            Rc::clone(&scheduler),
        )
        .await?;
        let inner = Rc::new(RefCell::new(inner));
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
        ClientDbInner::handle_query(&self.inner, query, opts, table, wait_for_coverage).await
    }

    async fn subscribe(
        &self,
        query: jazz::query::Query,
        opts: CoreReadOpts,
        table: String,
        tx: mpsc::UnboundedSender<OrderedRowDelta>,
    ) -> Result<()> {
        ClientDbInner::handle_subscribe(&self.inner, query, opts, table, tx).await
    }

    fn insert(
        &self,
        table: String,
        row_id: Option<Uuid>,
        cells: jazz::db::RowCells,
        identity: Option<CoreAuthorId>,
    ) -> Result<(ObjectId, CoreTxId)> {
        let mut inner = self.inner.borrow_mut();
        let (row_uuid, tx_id) = match row_id {
            Some(uuid) => {
                let row_uuid = CoreRowUuid(uuid);
                let tx_id = match identity {
                    Some(identity) => inner
                        .db
                        .insert_with_id_for_identity(identity, &table, row_uuid, cells),
                    None => inner.db.insert_with_id(&table, row_uuid, cells),
                }
                .map_err(|error| JazzError::Write(error.to_string()))?;
                (row_uuid, tx_id)
            }
            None => {
                if let Some(identity) = identity {
                    inner
                        .db
                        .insert_for_identity(identity, &table, cells)
                        .map_err(|error| JazzError::Write(error.to_string()))?
                } else {
                    inner
                        .db
                        .insert(&table, cells)
                        .map_err(|error| JazzError::Write(error.to_string()))?
                }
            }
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
        inner.ensure_transaction_open(batch_id)?;
        let tx_id = inner
            .transactions
            .get(&batch_id)
            .expect("transaction open checked above")
            .tx_id;
        inner
            .db
            .exclusive_write(tx_id, &table, CoreRowUuid(*row_id.uuid()), cells.clone())
            .map_err(|error| JazzError::Write(error.to_string()))?;
        let tx = inner
            .transactions
            .get_mut(&batch_id)
            .expect("transaction open checked above");
        tx.writes.push(ExclusiveTransactionWrite {
            table: table.clone(),
            row_id,
            cells,
            deletion: None,
        });
        inner.row_tables.insert(row_id, table);
        Ok(row_id)
    }

    fn upsert(
        &self,
        table: String,
        row_id: Uuid,
        cells: jazz::db::RowCells,
        identity: Option<CoreAuthorId>,
    ) -> Result<CoreTxId> {
        let mut inner = self.inner.borrow_mut();
        let write = match identity {
            Some(identity) => {
                inner
                    .db
                    .upsert_for_identity(identity, &table, CoreRowUuid(row_id), cells)
            }
            None => inner.db.upsert(&table, CoreRowUuid(row_id), cells),
        }
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
        inner.ensure_transaction_open(batch_id)?;
        let tx_id = inner
            .transactions
            .get(&batch_id)
            .expect("transaction open checked above")
            .tx_id;
        inner
            .db
            .exclusive_write(tx_id, &table, CoreRowUuid(row_id), cells.clone())
            .map_err(|error| JazzError::Write(error.to_string()))?;
        let tx = inner
            .transactions
            .get_mut(&batch_id)
            .expect("transaction open checked above");
        tx.writes.push(ExclusiveTransactionWrite {
            table: table.clone(),
            row_id: object_id,
            cells,
            deletion: None,
        });
        inner.row_tables.insert(object_id, table);
        Ok(())
    }

    fn update(
        &self,
        row_id: ObjectId,
        cells: jazz::db::RowCells,
        identity: Option<CoreAuthorId>,
    ) -> Result<CoreTxId> {
        let mut inner = self.inner.borrow_mut();
        let table = inner.row_tables.get(&row_id).cloned().ok_or_else(|| {
            JazzError::Write("update requires a row created or observed by this client".to_string())
        })?;
        let write = match identity {
            Some(identity) => {
                inner
                    .db
                    .upsert_for_identity(identity, &table, CoreRowUuid(*row_id.uuid()), cells)
            }
            None => inner.db.update(&table, CoreRowUuid(*row_id.uuid()), cells),
        }
        .map_err(|error| JazzError::Write(error.to_string()))?;
        JazzClient::check_core_write_not_rejected(&inner.db, write)?;
        inner.remember_write(row_id, &table, write);
        let tx_id = write;
        Ok(tx_id)
    }

    fn edit_text(&self, row_id: ObjectId, column: &str, edit: CoreTextEdit) -> Result<CoreTxId> {
        let mut inner = self.inner.borrow_mut();
        let table = inner.row_tables.get(&row_id).cloned().ok_or_else(|| {
            JazzError::Write(
                "text edit requires a row created or observed by this client".to_string(),
            )
        })?;
        let tx_id = inner
            .db
            .edit_text(&table, CoreRowUuid(*row_id.uuid()), column, edit)
            .map_err(|error| JazzError::Write(error.to_string()))?;
        inner.remember_write(row_id, &table, tx_id);
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
        inner.ensure_transaction_open(batch_id)?;
        let tx_id = inner
            .transactions
            .get(&batch_id)
            .expect("transaction open checked above")
            .tx_id;
        inner
            .db
            .exclusive_update(tx_id, &table, CoreRowUuid(*row_id.uuid()), cells.clone())
            .map_err(|error| JazzError::Write(error.to_string()))?;
        let tx = inner
            .transactions
            .get_mut(&batch_id)
            .expect("transaction open checked above");
        tx.writes.push(ExclusiveTransactionWrite {
            table,
            row_id,
            cells,
            deletion: None,
        });
        Ok(())
    }

    fn delete(&self, row_id: ObjectId, identity: Option<CoreAuthorId>) -> Result<CoreTxId> {
        let mut inner = self.inner.borrow_mut();
        let table = inner.row_tables.get(&row_id).cloned().ok_or_else(|| {
            JazzError::Write("delete requires a row created or observed by this client".to_string())
        })?;
        let write = match identity {
            Some(identity) => {
                inner
                    .db
                    .delete_for_identity(identity, &table, CoreRowUuid(*row_id.uuid()))
            }
            None => inner.db.delete(&table, CoreRowUuid(*row_id.uuid())),
        }
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
        inner.ensure_transaction_open(batch_id)?;
        let tx_id = inner
            .transactions
            .get(&batch_id)
            .expect("transaction open checked above")
            .tx_id;
        inner
            .db
            .exclusive_delete(tx_id, &table, CoreRowUuid(*row_id.uuid()))
            .map_err(|error| JazzError::Write(error.to_string()))?;
        let tx = inner
            .transactions
            .get_mut(&batch_id)
            .expect("transaction open checked above");
        tx.writes.push(ExclusiveTransactionWrite {
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
        while inner.transactions.contains_key(&batch_id)
            || inner.closed_transactions.contains_key(&batch_id)
            || inner.write_map.contains_key(&batch_id)
        {
            batch_id = BatchId::new();
        }
        let tx_id = inner
            .db
            .begin_exclusive()
            .map_err(|error| JazzError::Write(error.to_string()))?;
        inner.transactions.insert(
            batch_id,
            ExclusiveTransactionState {
                tx_id,
                writes: Vec::new(),
            },
        );
        Ok(batch_id)
    }

    fn hydrate_large_value_handle(&self, handle: &LargeValueHandle) -> Result<Vec<u8>> {
        self.inner
            .borrow()
            .db
            .hydrate_large_value_handle(handle.as_bytes())
            .map_err(|error| JazzError::Query(error.to_string()))
    }

    fn commit_transaction(&self, batch_id: BatchId) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        inner.ensure_transaction_open(batch_id)?;
        if inner
            .transactions
            .get(&batch_id)
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
            .remove(&batch_id)
            .expect("transaction open checked above");
        let tx_id = inner
            .db
            .commit_exclusive_handle(state.tx_id)
            .map_err(|error| JazzError::Write(error.to_string()))?;
        inner.write_map.insert(batch_id, tx_id);
        inner.write_map.insert(core_batch_id(tx_id), tx_id);
        for write in state.writes {
            inner.row_tables.insert(write.row_id, write.table);
        }
        inner
            .closed_transactions
            .insert(batch_id, ClosedTransactionState::Committed);
        Ok(())
    }

    fn rollback_transaction(&self, batch_id: BatchId) -> Result<bool> {
        let mut inner = self.inner.borrow_mut();
        inner.ensure_transaction_open(batch_id)?;
        let removed = inner.transactions.remove(&batch_id).is_some();
        if removed {
            inner
                .closed_transactions
                .insert(batch_id, ClosedTransactionState::RolledBack);
        }
        Ok(removed)
    }

    async fn wait_for_batch(&self, batch_id: BatchId, tier: DurabilityTier) -> Result<()> {
        ClientDbInner::handle_wait_for_batch(&self.inner, batch_id, tier).await
    }

    fn disconnect_upstream(&self) -> bool {
        let mut inner = self.inner.borrow_mut();
        let Some(connection) = inner.upstream.take() else {
            return false;
        };
        inner.db.detach_connection(&connection)
    }

    async fn reconnect_upstream(&self) -> Result<bool> {
        ClientDbInner::reconnect_upstream(&self.inner).await
    }

    fn spawn_local_tick_driver(
        inner: Rc<RefCell<ClientDbInner>>,
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
                    if let Err(error) = inner.borrow().db.tick() {
                        #[cfg(feature = "sync-autopsy")]
                        jazz::db::sync_autopsy::record(format!(
                            "client tick driver exited after db.tick error: {error}"
                        ));
                        return;
                    }
                }
            }
        });
    }
}

impl ClientDbInner {
    async fn open(
        schema: jazz::schema::JazzSchema,
        storage: StorageBundle,
        identity: CoreDbIdentity,
        server_url: Option<String>,
        app_id: crate::AppId,
        auth: Option<WsAuthConfig>,
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
            })
        } else {
            None
        };
        let mut inner = Self {
            db,
            identity,
            connect_config,
            scheduler,
            upstream: None,
            write_map: HashMap::new(),
            row_tables: HashMap::new(),
            transactions: HashMap::new(),
            closed_transactions: HashMap::new(),
        };
        inner.connect_upstream_transport().await?;
        Ok(inner)
    }

    async fn reconnect_upstream(inner: &Rc<RefCell<Self>>) -> Result<bool> {
        let (db, identity, scheduler, config) = {
            let inner = inner.borrow();
            if inner.upstream.is_some() {
                return Ok(false);
            }
            let Some(config) = inner.connect_config.clone() else {
                return Ok(false);
            };
            (
                inner.db.clone(),
                inner.identity,
                Rc::clone(&inner.scheduler),
                config,
            )
        };
        let connection = Self::connect_with_config(&db, identity, scheduler, config).await?;
        let mut inner = inner.borrow_mut();
        if inner.upstream.is_some() {
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
            Self::connect_with_config(&self.db, self.identity, Rc::clone(&self.scheduler), config)
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
        let transport = WebSocketTransport::connect_with_wake(
            &config.server_url,
            config.app_id,
            identity.author,
            config.auth,
            Arc::new(move || {
                wake.immediate.store(true, Ordering::Release);
                wake.notify.notify_one();
            }),
        )
        .await
        .map_err(|error| JazzError::Connection(error.to_string()))?;
        Ok(db.connect_upstream(Box::new(WireTransportAdapter::current(transport))))
    }

    fn ensure_transaction_open(&self, batch_id: BatchId) -> Result<()> {
        if self.transactions.contains_key(&batch_id) {
            return Ok(());
        }
        if let Some(state) = self.closed_transactions.get(&batch_id) {
            return Err(JazzError::Write(Self::closed_transaction_message(
                batch_id, *state,
            )));
        }
        Err(JazzError::Write(format!(
            "transaction {batch_id} is not open"
        )))
    }

    fn closed_transaction_message(batch_id: BatchId, state: ClosedTransactionState) -> String {
        match state {
            ClosedTransactionState::Committed => {
                format!("transaction {batch_id} already committed")
            }
            ClosedTransactionState::RolledBack => {
                format!("transaction {batch_id} completed or was never opened")
            }
        }
    }

    async fn handle_query(
        inner: &Rc<RefCell<Self>>,
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
        let attachment = if wait_for_coverage {
            let attachment = inner
                .borrow()
                .db
                .attach_query(&prepared, opts.clone())
                .map_err(|error| JazzError::Query(error.to_string()))?;
            Self::wait_for_query_coverage(inner, &attachment).await?;
            Some(attachment)
        } else {
            None
        };
        let (db, prepared) = {
            let inner = inner.borrow();
            (inner.db.clone(), prepared)
        };
        let rows = db
            .all(&prepared, opts)
            .await
            .map_err(|error| JazzError::Query(error.to_string()))?;
        if let Some(attachment) = attachment {
            db.detach_query(attachment);
        }
        inner.borrow_mut().remember_rows(&table, &rows);
        Ok(rows)
    }

    async fn wait_for_query_coverage(
        inner: &Rc<RefCell<Self>>,
        attachment: &jazz::db::QueryAttachment,
    ) -> Result<()> {
        if inner.borrow().db.query_attachment_is_covered(attachment) {
            return Ok(());
        }
        let deadline =
            tokio::time::Instant::now() + load_tolerant_test_timeout(QUERY_COVERAGE_TIMEOUT);
        loop {
            if inner.borrow().db.query_attachment_is_covered(attachment) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(JazzError::Query(
                    "timed out waiting for query coverage".to_string(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn handle_subscribe(
        inner: &Rc<RefCell<Self>>,
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
            .subscribe(&prepared, opts.clone())
            .await
            .map_err(|error| JazzError::Query(error.to_string()))?;
        let inner = Rc::clone(inner);
        tokio::task::spawn_local(async move {
            let mut stream = stream;
            let mut current_rows: Vec<jazz::node::CurrentRow> = Vec::new();
            while let Some(CoreSubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                ..
            }) = stream.next_event().await
            {
                let previous_rows = current_rows.clone();
                let previous_row_ids: Vec<ObjectId> = previous_rows
                    .iter()
                    .map(|row| ObjectId::from_uuid(row.row_uuid().0))
                    .collect();
                let next_rows = db.all(&prepared, opts.clone()).await;
                let Ok(next_rows) = next_rows else {
                    break;
                };
                current_rows = next_rows;
                inner.borrow_mut().remember_rows(&table, &current_rows);
                let delta = if reset {
                    JazzClient::core_subscription_reset_delta(&db, &previous_row_ids, &current_rows)
                } else {
                    JazzClient::core_subscription_change_delta(
                        &db,
                        &previous_rows,
                        &current_rows,
                        &added,
                        &updated,
                        &removed,
                    )
                };
                let Ok(delta) = delta else {
                    break;
                };
                let _ = tx.send(delta);
            }
        });
        Ok(())
    }

    async fn handle_wait_for_batch(
        inner: &Rc<RefCell<Self>>,
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
            if let CoreFate::Rejected(reason) = state.fate {
                return Err(JazzError::Sync(format!(
                    "batch was rejected before reaching {tier:?} durability: {}",
                    core_rejection_reason_label(&reason)
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

fn core_identity(context: &AppContext, default_session: Option<&Session>) -> CoreDbIdentity {
    let node_uuid = context
        .client_id
        .map(|id| id.0)
        .unwrap_or_else(Uuid::now_v7);
    let author_uuid = default_session
        .map(|session| {
            Uuid::parse_str(session.user_id.trim())
                .unwrap_or_else(|_| Uuid::new_v5(&Uuid::NAMESPACE_URL, session.user_id.as_bytes()))
        })
        .unwrap_or(node_uuid);
    CoreDbIdentity {
        node: CoreNodeUuid(node_uuid),
        author: CoreAuthorId(author_uuid),
    }
}

fn core_author_from_principal(principal: &str) -> CoreAuthorId {
    CoreAuthorId(
        Uuid::parse_str(principal.trim())
            .unwrap_or_else(|_| Uuid::new_v5(&Uuid::NAMESPACE_URL, principal.as_bytes())),
    )
}

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

fn public_to_core_value(value: Value) -> Result<CoreValue> {
    match value {
        Value::Boolean(value) => Ok(CoreValue::Bool(value)),
        Value::Text(value) => Ok(CoreValue::String(value)),
        Value::Integer(value) => Ok(CoreValue::U32(encode_signed_i32_for_core(value))),
        Value::BigInt(value) => Ok(CoreValue::I64(value)),
        Value::Double(value) => Ok(CoreValue::F64(value)),
        Value::Timestamp(value) => Ok(CoreValue::U64(value)),
        Value::Uuid(value) => Ok(CoreValue::Uuid(*value.uuid())),
        Value::Bytea(value) => Ok(CoreValue::Bytes(value)),
        Value::LargeValue(_) => Err(JazzError::Write(
            "large-value handles are read-only query results; write Bytea content instead"
                .to_string(),
        )),
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
            if let Some(value) = value.as_u64() {
                u32::try_from(value)
                    .map(CoreValue::U32)
                    .or(Ok(CoreValue::U64(value)))
            } else if let Some(value) = value.as_i64() {
                i32::try_from(value)
                    .map(|value| CoreValue::U32(encode_signed_i32_for_core(value)))
                    .or(Ok(CoreValue::I64(value)))
            } else if let Some(value) = value.as_f64() {
                Ok(CoreValue::F64(value))
            } else {
                Err(JazzError::Connection(
                    "JWT claim number is not representable".to_string(),
                ))
            }
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
    core_claims.insert("sub".to_owned(), CoreValue::String(session.user_id.clone()));
    core_claims.insert(
        "user_id".to_owned(),
        CoreValue::String(session.user_id.clone()),
    );
    core_claims.insert(
        "authMode".to_owned(),
        CoreValue::String(auth_mode_claim_value(session.auth_mode).to_owned()),
    );
    for (name, value) in claims {
        core_claims.insert(name, json_claim_to_core_value(value)?);
    }
    Ok(core_claims)
}

fn auth_mode_claim_value(auth_mode: crate::public_api::session::AuthMode) -> &'static str {
    match auth_mode {
        crate::public_api::session::AuthMode::External => "external",
        crate::public_api::session::AuthMode::LocalFirst => "local-first",
        crate::public_api::session::AuthMode::Anonymous => "anonymous",
    }
}

fn core_row_provenance_to_public(
    provenance: jazz::node::RowProvenance,
) -> crate::metadata::RowProvenance {
    crate::metadata::RowProvenance {
        created_by: provenance.created_by.0.to_string(),
        created_at: provenance.created_at.physical_ms(),
        updated_by: provenance.updated_by.0.to_string(),
        updated_at: provenance.updated_at.physical_ms(),
    }
}

fn encode_signed_i32_for_core(value: i32) -> u32 {
    u32::from_ne_bytes(value.to_ne_bytes()) ^ 0x8000_0000
}

fn decode_signed_i32_from_core(value: u32) -> i32 {
    i32::from_ne_bytes((value ^ 0x8000_0000).to_ne_bytes())
}

fn core_to_public_value(value: CoreValue) -> Result<Value> {
    match value {
        CoreValue::Bool(value) => Ok(Value::Boolean(value)),
        CoreValue::String(value) => Ok(Value::Text(value)),
        CoreValue::U32(value) => Ok(Value::Integer(decode_signed_i32_from_core(value))),
        CoreValue::I64(value) => Ok(Value::BigInt(value)),
        CoreValue::U64(value) => Ok(Value::Timestamp(value)),
        CoreValue::F64(value) => Ok(Value::Double(value)),
        CoreValue::Uuid(value) => Ok(Value::Uuid(ObjectId::from_uuid(value))),
        CoreValue::Bytes(value) if value.starts_with(LARGE_VALUE_HANDLE_MAGIC) => {
            Ok(Value::LargeValue(LargeValueHandle::from_bytes(value)))
        }
        CoreValue::Bytes(value) => Ok(Value::Bytea(value)),
        CoreValue::Nullable(None) => Ok(Value::Null),
        CoreValue::Nullable(Some(value)) => core_to_public_value(*value),
        CoreValue::Array(values) => values
            .into_iter()
            .map(core_to_public_value)
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        other => Err(JazzError::Query(format!(
            "client does not support core value {other:?}"
        ))),
    }
}

fn public_to_core_literal_for_column(value: &Value, column_type: &ColumnType) -> Result<CoreValue> {
    match (value, column_type) {
        (Value::Integer(value), ColumnType::BigInt) => Ok(CoreValue::I64(i64::from(*value))),
        (Value::BigInt(value), ColumnType::BigInt) => Ok(CoreValue::I64(*value)),
        (Value::BigInt(value), ColumnType::Integer) => i32::try_from(*value)
            .map(|value| CoreValue::U32(encode_signed_i32_for_core(value)))
            .map_err(|_| {
                JazzError::Query(format!(
                    "BIGINT literal {value} is outside INTEGER range for core query"
                ))
            }),
        _ => public_to_core_value(value.clone()),
    }
}

fn core_literal_operand(value: &Value, column_type: &ColumnType) -> Result<jazz::query::Operand> {
    public_to_core_literal_for_column(value, column_type).map(jazz::query::Operand::Literal)
}

fn core_query_condition(
    condition: &PublicCondition,
    table_schema: &TableSchema,
) -> Result<Vec<jazz::query::Predicate>> {
    let column = condition.column();
    let column_schema = table_schema
        .columns
        .columns
        .iter()
        .find(|schema| schema.name.as_str() == column)
        .ok_or_else(|| JazzError::Query(format!("unknown column {column}")))?;
    let column_operand = || jazz::query::Operand::Column(column.to_owned());
    let literal_operand = |value: &Value| core_literal_operand(value, &column_schema.column_type);

    let predicate = match condition {
        PublicCondition::Eq { value, .. } if value.is_null() => {
            jazz::query::is_null(column_operand())
        }
        PublicCondition::Ne { value, .. } if value.is_null() => {
            jazz::query::not(jazz::query::is_null(column_operand()))
        }
        PublicCondition::Eq { value, .. } => {
            jazz::query::eq(column_operand(), literal_operand(value)?)
        }
        PublicCondition::Ne { value, .. } => {
            jazz::query::ne(column_operand(), literal_operand(value)?)
        }
        PublicCondition::Lt { value, .. } => {
            jazz::query::lt(column_operand(), literal_operand(value)?)
        }
        PublicCondition::Le { value, .. } => {
            jazz::query::lte(column_operand(), literal_operand(value)?)
        }
        PublicCondition::Gt { value, .. } => {
            jazz::query::gt(column_operand(), literal_operand(value)?)
        }
        PublicCondition::Ge { value, .. } => {
            jazz::query::gte(column_operand(), literal_operand(value)?)
        }
        PublicCondition::Contains { value, .. } => {
            jazz::query::contains(column_operand(), literal_operand(value)?)
        }
        PublicCondition::IsNull { .. } => jazz::query::is_null(column_operand()),
        PublicCondition::IsNotNull { .. } => {
            jazz::query::not(jazz::query::is_null(column_operand()))
        }
        PublicCondition::Between { min, max, .. } => {
            return Ok(vec![
                jazz::query::gte(column_operand(), literal_operand(min)?),
                jazz::query::lte(column_operand(), literal_operand(max)?),
            ]);
        }
    };
    Ok(vec![predicate])
}

fn aggregate_public_values(query: &Query, row: &jazz::node::CurrentRow) -> Result<Vec<Value>> {
    let Some(aggregate) = &query.aggregate else {
        return Ok(Vec::new());
    };
    let mut columns = Vec::new();
    if let Some(group_by) = &aggregate.group_by {
        columns.push(group_by.clone());
    }
    columns.extend(
        aggregate
            .outputs
            .iter()
            .map(|output| match output.function {
                crate::public_api::query::AggregateFunction::Count => "count".to_owned(),
                crate::public_api::query::AggregateFunction::Sum => {
                    format!(
                        "sum_{}",
                        output
                            .column
                            .as_deref()
                            .expect("sum aggregate has an input column")
                    )
                }
            }),
    );
    let (descriptor, raw) = row.encoded_record();
    let borrowed = jazz::groove::records::BorrowedRecord::new(raw, descriptor);
    columns
        .into_iter()
        .map(|column| {
            let idx = descriptor.field_index(&column).ok_or_else(|| {
                JazzError::Query(format!("aggregate row missing column {column}"))
            })?;
            let value = borrowed
                .get_idx(idx)
                .map_err(|error| JazzError::Query(error.to_string()))?;
            core_to_public_value(value)
        })
        .collect()
}

fn core_batch_id(tx_id: CoreTxId) -> BatchId {
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

impl JazzClient {
    fn write_identity(&self) -> Option<CoreAuthorId> {
        self.write_context
            .as_ref()
            .and_then(|context| context.session())
            .or(self.default_session.as_ref())
            .map(|session| core_author_from_principal(session.get_user_id()))
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
                .map(core_tier)
                .unwrap_or(CoreDurabilityTier::Local),
            local_updates: CoreLocalUpdates::Immediate,
            propagation: CorePropagation::Full,
            include_deleted: false,
            ..CoreReadOpts::default()
        }
    }
    fn core_query(&self, query: &Query) -> Result<jazz::query::Query> {
        if query.disjuncts.len() != 1
            || !query.joins.is_empty()
            || !query.array_subqueries.is_empty()
            || query.recursive.is_some()
            || query.include_deleted
            || query.result_element_index.is_some()
            || (query.aggregate.is_some() && query.select_columns.is_some())
        {
            return Err(JazzError::Query(
                "JazzClient currently supports simple table queries only".to_string(),
            ));
        }
        let mut core_query = jazz::query::Query::from(query.table.as_str());
        let schema = self.schema()?;
        let table_schema = schema
            .get(&TableName::new(query.table.as_str()))
            .ok_or_else(|| JazzError::Query(format!("unknown table {}", query.table.as_str())))?;
        for condition in &query.disjuncts[0].conditions {
            for predicate in core_query_condition(condition, table_schema)? {
                core_query = core_query.filter(predicate);
            }
        }
        if let Some(aggregate) = &query.aggregate {
            let outputs = aggregate
                .outputs
                .iter()
                .map(|output| match output.function {
                    crate::public_api::query::AggregateFunction::Count => {
                        jazz::query::Aggregate::count()
                    }
                    crate::public_api::query::AggregateFunction::Sum => {
                        jazz::query::Aggregate::sum(
                            output
                                .column
                                .as_deref()
                                .expect("sum aggregate has an input column"),
                        )
                    }
                });
            core_query = core_query.aggregate(outputs);
            if let Some(group_by) = &aggregate.group_by {
                core_query = core_query.group_by(group_by.clone());
            }
        } else if let Some(columns) = query.select_columns.clone() {
            core_query = core_query.select(columns);
        }
        for (column, direction) in &query.order_by {
            let direction = match direction {
                PublicSortDirection::Ascending => jazz::query::OrderDirection::Asc,
                PublicSortDirection::Descending => jazz::query::OrderDirection::Desc,
            };
            core_query = core_query.order_by(column.clone(), direction);
        }
        if let Some(limit) = query.limit {
            core_query = core_query.limit(limit);
        }
        if query.offset != 0 {
            core_query = core_query.offset(query.offset);
        }
        Ok(core_query)
    }
    fn core_rows_to_public(
        &self,
        query: &Query,
        rows: Vec<jazz::node::CurrentRow>,
    ) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        if query.aggregate.is_some() {
            return rows
                .into_iter()
                .map(|row| {
                    let row_id = ObjectId::from_uuid(row.row_uuid().0);
                    let values = aggregate_public_values(query, &row)?;
                    Ok((row_id, values))
                })
                .collect();
        }
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
                let core_row_id = row.row_uuid();
                let row_id = ObjectId::from_uuid(core_row_id.0);
                let values = columns
                    .iter()
                    .map(|column| {
                        if let Some(value) =
                            self.core_magic_value(table, core_row_id, &row, column)?
                        {
                            return Ok(value);
                        }
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

    fn core_subscription_row_to_public(db: &Backend, row: &jazz::node::CurrentRow) -> Result<Row> {
        let (_, encoded) = row.encoded_record();
        let provenance = db
            .row_provenance(row)
            .map_err(|error| JazzError::Query(error.to_string()))?
            .map(core_row_provenance_to_public)
            .unwrap_or_else(|| crate::metadata::RowProvenance::for_insert("jazz:unknown", 0));
        Ok(Row::new(
            ObjectId::from_uuid(row.row_uuid().0),
            encoded.to_vec(),
            BatchId([0; 16]),
            provenance,
        ))
    }

    fn core_subscription_snapshot_delta(
        db: &Backend,
        rows: &[jazz::node::CurrentRow],
    ) -> Result<OrderedRowDelta> {
        let added = rows
            .iter()
            .enumerate()
            .map(|(index, row)| {
                let public = Self::core_subscription_row_to_public(db, row)?;
                Ok(OrderedAdded {
                    id: public.id,
                    index,
                    row: public,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(OrderedRowDelta {
            added,
            ..OrderedRowDelta::default()
        })
    }

    fn core_subscription_reset_delta(
        db: &Backend,
        previous_rows: &[ObjectId],
        rows: &[jazz::node::CurrentRow],
    ) -> Result<OrderedRowDelta> {
        let removed = previous_rows
            .iter()
            .copied()
            .enumerate()
            .map(|(index, id)| OrderedRemoved { id, index })
            .collect();
        let mut delta = Self::core_subscription_snapshot_delta(db, rows)?;
        delta.removed = removed;
        Ok(delta)
    }

    fn core_subscription_change_delta(
        db: &Backend,
        previous_rows: &[jazz::node::CurrentRow],
        current_rows: &[jazz::node::CurrentRow],
        added_rows: &[jazz::node::CurrentRow],
        updated_rows: &[jazz::node::CurrentRow],
        removed_rows: &[jazz::db::RemovedRow],
    ) -> Result<OrderedRowDelta> {
        let current_index_of = |id: ObjectId| {
            current_rows
                .iter()
                .position(|row| ObjectId::from_uuid(row.row_uuid().0) == id)
                .unwrap_or(0)
        };
        let previous_index_of = |id: ObjectId| {
            previous_rows
                .iter()
                .position(|row| ObjectId::from_uuid(row.row_uuid().0) == id)
                .unwrap_or(0)
        };
        let added = added_rows
            .iter()
            .map(|row| {
                let public = Self::core_subscription_row_to_public(db, row)?;
                Ok(OrderedAdded {
                    id: public.id,
                    index: current_index_of(public.id),
                    row: public,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let updated = updated_rows
            .iter()
            .map(|row| {
                let public = Self::core_subscription_row_to_public(db, row)?;
                Ok(OrderedUpdated {
                    id: public.id,
                    old_index: previous_index_of(public.id),
                    new_index: current_index_of(public.id),
                    row: Some(public),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let removed = removed_rows
            .iter()
            .map(|row| {
                let id = ObjectId::from_uuid(row.row_uuid.0);
                OrderedRemoved {
                    id,
                    index: previous_index_of(id),
                }
            })
            .collect();
        Ok(OrderedRowDelta {
            added,
            removed,
            updated,
            pending: false,
        })
    }

    fn core_magic_value(
        &self,
        table: &str,
        _row_id: CoreRowUuid,
        row: &jazz::node::CurrentRow,
        column: &str,
    ) -> Result<Option<Value>> {
        let value = match column {
            "$canRead" => {
                return Err(JazzError::Query(format!(
                    "permission introspection column {column} requires unified policy lowering"
                )));
            }
            "$createdAt" | "$updatedAt" | "$createdBy" | "$updatedBy" => {
                let provenance = self
                    .db
                    .inner
                    .borrow()
                    .db
                    .row_provenance(row)
                    .map_err(|error| JazzError::Query(error.to_string()))?;
                let Some(provenance) = provenance else {
                    return Err(JazzError::Query(format!(
                        "row missing provenance for magic column {column} on table {table}"
                    )));
                };
                match column {
                    "$createdAt" => Value::Timestamp(provenance.created_at.physical_ms()),
                    "$updatedAt" => Value::Timestamp(provenance.updated_at.physical_ms()),
                    "$createdBy" => Value::Text(provenance.created_by.0.to_string()),
                    "$updatedBy" => Value::Text(provenance.updated_by.0.to_string()),
                    _ => unreachable!("matched provenance magic column"),
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
    fn core_cells(values: HashMap<String, Value>) -> Result<jazz::db::RowCells> {
        values
            .into_iter()
            .map(|(name, value)| Ok((name, public_to_core_value(value)?)))
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

        let inner = self.db.inner.borrow();
        let tx = inner.transactions.get(&batch_id).ok_or_else(|| {
            let message = inner
                .closed_transactions
                .get(&batch_id)
                .copied()
                .map(|state| ClientDbInner::closed_transaction_message(batch_id, state))
                .unwrap_or_else(|| format!("transaction {batch_id} is not open"));
            JazzError::Query(message)
        })?;

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
    pub async fn connect(context: AppContext) -> Result<Self> {
        Self::connect_inner(context).await
    }

    async fn connect_inner(context: AppContext) -> Result<Self> {
        let default_session = default_session_from_context(&context);
        let has_server = !context.server_url.is_empty();
        {
            let public_schema_convert = convert_public_schema(&context.schema)
                .map_err(|error| JazzError::Schema(error.to_string()))?;
            let identity = core_identity(&context, default_session.as_ref());
            let storage = core_storage(&public_schema_convert, &context)?;
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
                storage,
                identity,
                has_server.then(|| context.server_url.clone()),
                context.app_id,
                auth,
            )
            .await
            .map_err(|error| JazzError::Connection(error.to_string()))?;
            if let Some(session) = default_session.as_ref() {
                let claims = session_claims_to_core_claims(session)?;
                db.inner
                    .borrow()
                    .db
                    .set_identity_claims(identity.author, claims);
            }
            let client = Self {
                default_session,
                write_context: None,
                db,
                public_schema: context.schema.clone(),
            };
            Ok(client)
        }
    }

    /// Subscribe to a query.
    ///
    /// Returns a stream of row deltas as the data changes.
    pub async fn subscribe(&self, query: Query) -> Result<SubscriptionStream> {
        {
            let (tx, rx) = mpsc::unbounded_channel::<OrderedRowDelta>();
            let core_query = self.core_query(&query)?;
            self.db
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
        {
            let opts = Self::core_read_opts(durability_tier);
            let rows = self
                .db
                .query_rows(
                    self.core_query(&query)?,
                    opts,
                    query.table.as_str().to_string(),
                    matches!(
                        durability_tier,
                        Some(DurabilityTier::EdgeServer | DurabilityTier::GlobalServer)
                    ),
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
        {
            let row_values = self.core_ordered_values(table, &values)?;
            let cells = Self::core_cells(values)?;
            if let Some(batch_id) = self.write_context.as_ref().and_then(|ctx| ctx.batch_id) {
                let row_id =
                    self.db
                        .stage_insert(batch_id, table.to_string(), object_id.into(), cells)?;
                Ok((row_id, row_values, batch_id))
            } else {
                let (row_id, tx_id) = self.db.insert(
                    table.to_string(),
                    object_id.into(),
                    cells,
                    self.write_identity(),
                )?;
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
        {
            let cells = Self::core_cells(values)?;
            if let Some(batch_id) = self.write_context.as_ref().and_then(|ctx| ctx.batch_id) {
                self.db
                    .stage_upsert(batch_id, table.to_string(), object_id, cells)?;
                Ok(batch_id)
            } else {
                let tx_id =
                    self.db
                        .upsert(table.to_string(), object_id, cells, self.write_identity())?;
                Ok(core_batch_id(tx_id))
            }
        }
    }

    /// Update a row.
    pub fn update(&self, object_id: ObjectId, updates: Vec<(String, Value)>) -> Result<BatchId> {
        {
            let cells = Self::core_cells(updates.into_iter().collect())?;
            if let Some(batch_id) = self.write_context.as_ref().and_then(|ctx| ctx.batch_id) {
                self.db.stage_update(batch_id, object_id, cells)?;
                Ok(batch_id)
            } else {
                let tx_id = self.db.update(object_id, cells, self.write_identity())?;
                Ok(core_batch_id(tx_id))
            }
        }
    }

    /// Apply explicit byte-position edits to a text-document column.
    pub fn edit_text(
        &self,
        object_id: ObjectId,
        column: &str,
        edit: CoreTextEdit,
    ) -> Result<BatchId> {
        if self
            .write_context
            .as_ref()
            .and_then(|ctx| ctx.batch_id)
            .is_some()
        {
            return Err(JazzError::Write(
                "text edits are not supported inside exclusive transactions".to_string(),
            ));
        }
        let tx_id = self.db.edit_text(object_id, column, edit)?;
        Ok(core_batch_id(tx_id))
    }

    /// Delete a row.
    pub fn delete(&self, object_id: ObjectId) -> Result<BatchId> {
        {
            if let Some(batch_id) = self.write_context.as_ref().and_then(|ctx| ctx.batch_id) {
                self.db.stage_delete(batch_id, object_id)?;
                Ok(batch_id)
            } else {
                let tx_id = self.db.delete(object_id, self.write_identity())?;
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
        {
            let batch_id = self.db.begin_transaction()?;
            let client = self.with_write_context(WriteContext::default().with_batch_id(batch_id));
            Ok(JazzTransaction { batch_id, client })
        }
    }

    /// Commit an open transaction by batch id.
    pub fn commit_transaction(&self, batch_id: BatchId) -> Result<()> {
        self.db.commit_transaction(batch_id)
    }

    /// Roll back an open transaction by batch id.
    ///
    /// Returns whether a local batch record existed for the transaction.
    pub fn rollback_transaction(&self, batch_id: BatchId) -> Result<bool> {
        self.db.rollback_transaction(batch_id)
    }

    pub async fn wait_for_batch(&self, batch_id: BatchId, tier: DurabilityTier) -> Result<()> {
        self.db.wait_for_batch(batch_id, tier).await
    }

    /// Fetch and materialize the bytes behind a large-value handle returned by a query.
    pub async fn hydrate_large_value(&self, handle: &LargeValueHandle) -> Result<Vec<u8>> {
        let deadline = tokio::time::Instant::now() + QUERY_COVERAGE_TIMEOUT;
        loop {
            match self.db.hydrate_large_value_handle(handle) {
                Ok(bytes) => return Ok(bytes),
                Err(error) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(error);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Unsubscribe from a subscription.
    pub async fn unsubscribe(&self, _handle: SubscriptionHandle) -> Result<()> {
        Ok(())
    }

    /// Get the current schema.
    pub fn schema(&self) -> Result<Schema> {
        Ok(self.public_schema.clone())
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
            public_schema: self.public_schema.clone(),
        }
    }

    /// Create a session-scoped client for backend operations.
    pub fn for_session(&self, session: Session) -> JazzClient {
        self.with_write_context(WriteContext::from_session(session))
    }

    /// Shutdown the client and release resources.
    pub async fn shutdown(self) -> Result<()> {
        self.db.disconnect_upstream();
        Ok(())
    }
}

#[cfg(feature = "test-utils")]
impl JazzClient {
    pub fn client_id(&self) -> Option<ClientId> {
        None
    }

    pub async fn test_client(schema: Schema) -> crate::JazzClient {
        let context = crate::AppContext::test(schema);
        crate::JazzClient::connect(context)
            .await
            .expect("connect local JazzClient")
    }

    pub(crate) fn disconnect_upstream_for_test(&self) -> bool {
        self.db.disconnect_upstream()
    }

    pub(crate) async fn reconnect_upstream_for_test(&self) -> Result<bool> {
        self.db.reconnect_upstream().await
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
    use crate::AppId;
    use crate::public_schema::Schema;
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
