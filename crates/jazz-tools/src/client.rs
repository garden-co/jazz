//! Thin Rust client facade over `jazz::db`.

use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::public_schema::OrderedRowDelta;
use crate::public_schema::Schema;
use crate::public_schema::TableName;
use crate::public_schema::{Query, Session, Value, WriteContext};
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
    LocalUpdates as CoreLocalUpdates, Propagation as CorePropagation, ReadOpts as CoreReadOpts,
    SubscriptionEvent as CoreSubscriptionEvent, TickScheduler, TickUrgency,
    Transport as CoreTransport, WireTransportAdapter,
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
    inner: Rc<std::cell::RefCell<ClientDbInner>>,
}

struct ClientDbInner {
    db: Backend,
    write_map: HashMap<BatchId, CoreTxId>,
    row_tables: HashMap<ObjectId, String>,
    transactions: HashMap<BatchId, ExclusiveTransactionState>,
    closed_transactions: HashMap<BatchId, ClosedTransactionState>,
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
    ) -> jazz::db::QueryAttachment {
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

    fn can_read(&self, table: &str, row: CoreRowUuid) -> std::result::Result<bool, CoreDbError> {
        match self {
            Self::Memory(db) => db.can_read(table, row),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.can_read(table, row),
        }
    }

    fn can_update(&self, table: &str, row: CoreRowUuid) -> std::result::Result<bool, CoreDbError> {
        match self {
            Self::Memory(db) => db.can_update(table, row),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.can_update(table, row),
        }
    }

    fn can_delete(&self, table: &str, row: CoreRowUuid) -> std::result::Result<bool, CoreDbError> {
        match self {
            Self::Memory(db) => db.can_delete(table, row),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(db) => db.can_delete(table, row),
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

    fn spawn_local_tick_driver(
        inner: Rc<std::cell::RefCell<ClientDbInner>>,
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
            closed_transactions: HashMap::new(),
        })
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
        let attachment = if wait_for_coverage {
            let attachment = inner.borrow().db.attach_query(&prepared, opts);
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
        inner: &Rc<std::cell::RefCell<Self>>,
        attachment: &jazz::db::QueryAttachment,
    ) -> Result<()> {
        if inner.borrow().db.query_attachment_is_covered(attachment) {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
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
                        inner.borrow_mut().remember_rows(&table, &current.rows);
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
            "client does not support core value {other:?}"
        ))),
    }
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
        }
    }
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

    fn core_magic_value(
        &self,
        table: &str,
        row_id: CoreRowUuid,
        row: &jazz::node::CurrentRow,
        column: &str,
    ) -> Result<Option<Value>> {
        let value = match column {
            "$canRead" => Value::Boolean(
                self.db
                    .inner
                    .borrow()
                    .db
                    .can_read(table, row_id)
                    .map_err(|error| JazzError::Query(error.to_string()))?,
            ),
            "$canEdit" => Value::Boolean(
                self.db
                    .inner
                    .borrow()
                    .db
                    .can_update(table, row_id)
                    .map_err(|error| JazzError::Query(error.to_string()))?,
            ),
            "$canDelete" => Value::Boolean(
                self.db
                    .inner
                    .borrow()
                    .db
                    .can_delete(table, row_id)
                    .map_err(|error| JazzError::Query(error.to_string()))?,
            ),
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
                values.get(column.name.as_str()).cloned().ok_or_else(|| {
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
        false
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
