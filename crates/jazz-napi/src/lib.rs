//! jazz-napi — Native Node.js bindings for Jazz.
//!
//! Provides Node.js bindings for the Jazz core database, server helpers, and
//! local-first identity utilities.
//!
//! # Architecture
//!
//! - `NapiDb` exposes the Jazz database directly over an
//!   encoded-row boundary for the TypeScript client packages.
//! - `JazzServer` exposes the Rust server process used by integration tests
//!   and Node deployments.
//! - Local-first JWT helpers stay here as package-level native utilities.
//!
//! # Allocator
//!
//! This crate uses `mimalloc-safe` (napi-rs–maintained mimalloc fork) as Rust's
//! `#[global_allocator]`. It does NOT override the host process's `malloc`/`free` —
//! Node.js / V8 keep their own allocator. The two coexist safely as long as
//! memory crosses the FFI boundary **by copy**, which is what napi-rs does today
//! for Vec/String/Buffer returns.
//!
//! Footgun: never `Vec::leak` / `Box::into_raw` an allocation across FFI and let
//! the host call `free()` on it — that mixes allocators and corrupts the heap.
//! If a future zero-copy shim is added, hand the host a Rust-defined finalizer
//! callback that frees through mimalloc instead.

#[global_allocator]
static GLOBAL: mimalloc_safe::MiMalloc = mimalloc_safe::MiMalloc;

use napi::bindgen_prelude::*;
use napi::sys;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::Mutex;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jazz::db::{
    ConnectionSessionContext as CoreConnectionSessionContext, Db as CoreDb,
    DbConfig as CoreDbConfig, DbIdentity as CoreDbIdentity, ExclusiveTxOps,
    InitialSyncFlushCadence as CoreInitialSyncFlushCadence, LocalUpdates as CoreLocalUpdates,
    MergeableTxOps, MutationErrorCallback as CoreMutationErrorCallback,
    PeerConnection as CorePeerConnection, PreparedQuery as PreparedQueryInner,
    Propagation as CorePropagation, QueryAttachment as CoreQueryAttachment,
    ReadOpts as CoreReadOpts, RowCells as CoreRowCells, SeededRowIdSource as CoreSeededRowIdSource,
    SubscriptionEvent as CoreSubscriptionEvent, SubscriptionStream,
    TickScheduler as CoreTickScheduler, TickUrgency as CoreTickUrgency,
    WireTransportAdapter as CoreWireTransportAdapter, WriteHandle, block_on as core_block_on,
};
use jazz::groove::records::{
    BorrowedRecord as CoreBorrowedRecord, RecordDescriptor, Value as CoreValue,
};
use jazz::groove::storage::{
    MemoryStorage as CoreMemoryStorage, OrderedKvStorage as CoreOrderedKvStorage,
    ReopenableStorage as CoreReopenableStorage, RocksDbStorage as CoreRocksDbStorage,
};
use jazz::ids::{AuthorId as CoreAuthorId, NodeUuid as CoreNodeUuid, RowUuid as CoreRowUuid};
use jazz::query::{
    Query as CoreQuery, RelationExpr as CoreRelationExpr, RelationQuery as CoreRelationQuery,
};
use jazz::schema::JazzSchema;
use jazz::tools::OpenBatchId as CoreOpenBatchId;
use jazz::tools::identity;
use jazz::tools::middleware::AuthConfig;
use jazz::tools::server::{
    JazzServer as CoreJazzServer, ServerBuilder, ServerDataDir, StorageBackend,
    TestJwtIssuer as JazzTestJwtIssuer, TestJwtOptions,
};
use jazz::tools::{AppId, BatchId};
use jazz::tx::{DurabilityTier as CoreDurabilityTier, TxId};
use jazz::wire::{
    TransportError, WireAuthorityEndpoint as CoreWireAuthorityEndpoint,
    WireTransport as CoreWireTransport,
};

#[derive(Clone, Debug, Deserialize)]
struct CoreOpenDbConfig {
    identity: CoreOpenDbIdentity,
    row_id_seed: Option<u64>,
    history_complete: bool,
    initial_sync_flush_every: Option<u32>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct CoreOpenDbIdentity {
    node: CoreNodeUuid,
    author: CoreAuthorId,
}

impl From<CoreOpenDbIdentity> for CoreDbIdentity {
    fn from(identity: CoreOpenDbIdentity) -> Self {
        Self {
            node: identity.node,
            author: identity.author,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize)]
struct WriteResult {
    row_id: CoreRowUuid,
    tx_id: TxId,
}

type NapiDbInner = Rc<RefCell<Option<NapiDbInnerStorage>>>;

enum NapiDbInnerStorage {
    Memory(Rc<CoreDb<CoreMemoryStorage>>),
    Persistent(Rc<CoreDb<CoreRocksDbStorage>>),
}

enum NapiWrite {
    Memory {
        db: Rc<CoreDb<CoreMemoryStorage>>,
        tx_id: TxId,
    },
    Persistent {
        db: Rc<CoreDb<CoreRocksDbStorage>>,
        tx_id: TxId,
    },
}

#[derive(Clone, Default)]
struct WireQueues {
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
}

struct NapiWireTransport {
    queues: WireQueues,
}

struct NapiTickScheduler {
    callback: ThreadsafeFunction<String, ()>,
}

impl CoreTickScheduler for NapiTickScheduler {
    fn schedule_tick(&self, urgency: CoreTickUrgency) {
        let urgency = match urgency {
            CoreTickUrgency::Immediate => "immediate",
            CoreTickUrgency::Deferred => "deferred",
        };
        let _ = self.callback.call(
            Ok(urgency.to_string()),
            ThreadsafeFunctionCallMode::NonBlocking,
        );
    }
}

impl CoreWireTransport for NapiWireTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> std::result::Result<(), TransportError> {
        self.queues.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.queues.inbound.borrow_mut().pop_front()
    }
}

#[napi(js_name = "PreparedQuery")]
pub struct PreparedQuery {
    inner: PreparedQueryInner,
}

#[napi(js_name = "QueryAttachment")]
pub struct QueryAttachment {
    inner: CoreQueryAttachment,
}

#[napi(js_name = "Write")]
pub struct Write {
    payload: Vec<u8>,
    batch_id: BatchId,
    inner: Option<NapiWrite>,
}

#[napi(js_name = "Transport")]
pub struct Transport {
    inner: NapiTransportInner,
    queues: WireQueues,
}

#[napi(js_name = "Subscription")]
pub struct Subscription {
    inner: Option<NapiSubscription>,
    /// Layout definitions are installed atomically with the first event that
    /// references them.  Later terminal operations only carry this stable id.
    published_terminal_layouts: HashSet<String>,
}

#[napi(object)]
pub struct SubscriptionDeltaEvent {
    #[napi(js_name = "type", ts_type = "'delta'")]
    pub event_type: String,
    pub reset: bool,
    pub delta: Uint8Array,
    #[napi(js_name = "terminalOperations")]
    pub terminal_operations: Vec<SubscriptionTerminalOperation>,
    #[napi(js_name = "terminalLayouts")]
    pub terminal_layouts: Vec<SubscriptionTerminalLayout>,
    pub settled: bool,
    #[napi(ts_type = "'None' | 'Local' | 'Edge' | 'Global'")]
    pub tier: String,
}

#[napi(object)]
pub struct SubscriptionRejectedEvent {
    #[napi(js_name = "type", ts_type = "'rejected'")]
    pub event_type: String,
    pub reason: SubscriptionRejectionReason,
}

#[napi(object)]
pub struct SubscriptionClosedEvent {
    #[napi(js_name = "type", ts_type = "'closed'")]
    pub event_type: String,
}

#[napi(object)]
pub struct SubscriptionUnsupportedShapeCapabilityReason {
    #[napi(js_name = "type", ts_type = "'UnsupportedShapeCapability'")]
    pub reason_type: String,
    pub detail: String,
}

#[napi(object)]
pub struct SubscriptionShapeRegistrationPendingReason {
    #[napi(
        js_name = "type",
        ts_type = "'ShapeRegistrationPendingCatalogueAdmission'"
    )]
    pub reason_type: String,
}

#[napi(object)]
pub struct SubscriptionServerFailureReason {
    #[napi(js_name = "type", ts_type = "'ServerFailure'")]
    pub reason_type: String,
    #[napi(
        ts_type = "'TableNotFound' | 'SchemaResolution' | 'QueryValidation' | 'QueryLowering' | 'PolicyEvaluation' | 'Internal'"
    )]
    pub code: String,
}

#[napi(object)]
pub struct SubscriptionTerminalOperation {
    #[napi(js_name = "rootLayoutId")]
    pub root_layout_id: String,
    #[napi(js_name = "root_key")]
    pub root_key: Vec<u32>,
    pub path: Vec<SubscriptionTerminalPathSegment>,
    pub edit: SubscriptionTerminalEdit,
}

/// Immutable producer-owned root record contract.  The descriptor and public
/// slots are published once per NAPI subscription, before an operation may
/// reference `id`; TypeScript never has to infer a CurrentRow/layout family.
#[napi(object)]
pub struct SubscriptionTerminalLayout {
    pub id: String,
    #[napi(js_name = "rootDescriptor")]
    pub root_descriptor: Vec<u32>,
    #[napi(js_name = "rootKeySlot")]
    pub root_key_slot: f64,
    #[napi(js_name = "rootKeyFieldName")]
    pub root_key_field_name: String,
    #[napi(js_name = "publicFields")]
    pub public_fields: Vec<SubscriptionTerminalPublicField>,
    pub carrier: String,
}

#[napi(object)]
pub struct SubscriptionTerminalPublicField {
    pub name: String,
    #[napi(js_name = "descriptorFieldName")]
    pub descriptor_field_name: String,
    pub slot: f64,
    pub carrier: String,
}

#[napi(object)]
pub struct SubscriptionTerminalCollectionPathSegment {
    #[napi(js_name = "Collection")]
    pub collection: String,
}

#[napi(object)]
pub struct SubscriptionTerminalKeyPathSegment {
    #[napi(js_name = "Key")]
    pub key: Vec<u32>,
}

#[napi(object)]
pub struct SubscriptionTerminalInsertEdit {
    #[napi(js_name = "Insert")]
    pub insert: SubscriptionTerminalInsert,
}

#[napi(object)]
pub struct SubscriptionTerminalInsert {
    pub index: f64,
    pub key: Vec<u32>,
    pub value: Vec<u32>,
}

#[napi(object)]
pub struct SubscriptionTerminalUpdateEdit {
    #[napi(js_name = "Update")]
    pub update: SubscriptionTerminalUpdate,
}

#[napi(object)]
pub struct SubscriptionTerminalUpdate {
    pub key: Vec<u32>,
    pub value: Vec<u32>,
}

#[napi(object)]
pub struct SubscriptionTerminalRemoveEdit {
    #[napi(js_name = "Remove")]
    pub remove: SubscriptionTerminalRemove,
}

#[napi(object)]
pub struct SubscriptionTerminalRemove {
    pub key: Vec<u32>,
}

#[napi(object)]
pub struct SubscriptionTerminalMoveEdit {
    #[napi(js_name = "Move")]
    pub move_edit: SubscriptionTerminalMove,
}

#[napi(object)]
pub struct SubscriptionTerminalMove {
    pub key: Vec<u32>,
    pub index: f64,
}

#[napi]
pub type SubscriptionEvent =
    Either3<SubscriptionDeltaEvent, SubscriptionRejectedEvent, SubscriptionClosedEvent>;

#[napi]
pub type SubscriptionRejectionReason = Either3<
    SubscriptionUnsupportedShapeCapabilityReason,
    SubscriptionShapeRegistrationPendingReason,
    SubscriptionServerFailureReason,
>;

#[napi]
pub type SubscriptionTerminalPathSegment =
    Either<SubscriptionTerminalCollectionPathSegment, SubscriptionTerminalKeyPathSegment>;

#[napi]
pub type SubscriptionTerminalEdit = Either4<
    SubscriptionTerminalInsertEdit,
    SubscriptionTerminalUpdateEdit,
    SubscriptionTerminalRemoveEdit,
    SubscriptionTerminalMoveEdit,
>;

enum NapiTransportInner {
    Memory {
        db: Rc<CoreDb<CoreMemoryStorage>>,
        connection: Option<Rc<RefCell<CorePeerConnection<CoreMemoryStorage>>>>,
    },
    Persistent {
        db: Rc<CoreDb<CoreRocksDbStorage>>,
        connection: Option<Rc<RefCell<CorePeerConnection<CoreRocksDbStorage>>>>,
    },
    Closed,
}

enum NapiSubscription {
    Memory(SubscriptionStream),
    Persistent(SubscriptionStream),
}

#[napi(js_name = "Tx")]
pub struct Tx {
    db: NapiDbInnerStorage,
    kind: NapiTxKind,
    open_tx: Option<CoreOpenBatchId>,
    owns_lifetime: bool,
}

#[derive(Clone, Copy)]
enum NapiTxKind {
    Mergeable,
    Exclusive,
}

macro_rules! with_napi_mergeable_tx {
    ($transaction:expr, |$tx:ident| $operation:expr) => {{
        let open_tx = $transaction.open_tx()?;
        match &$transaction.db {
            NapiDbInnerStorage::Memory(db) => {
                let $tx = db.mergeable_tx_ref(open_tx);
                $operation
            }
            NapiDbInnerStorage::Persistent(db) => {
                let $tx = db.mergeable_tx_ref(open_tx);
                $operation
            }
        }
        .map_err(|error: jazz::db::Error| napi::Error::from_reason(error.to_string()))
    }};
}

macro_rules! with_napi_exclusive_tx {
    ($transaction:expr, |$tx:ident| $operation:expr) => {{
        let open_tx = $transaction.open_tx()?;
        match &$transaction.db {
            NapiDbInnerStorage::Memory(db) => {
                let $tx = db.exclusive_tx_ref(open_tx);
                $operation
            }
            NapiDbInnerStorage::Persistent(db) => {
                let $tx = db.exclusive_tx_ref(open_tx);
                $operation
            }
        }
        .map_err(|error: jazz::db::Error| napi::Error::from_reason(error.to_string()))
    }};
}

impl Write {
    fn wait_promise(
        &self,
        env: Env,
        tier: CoreDurabilityTier,
    ) -> napi::Result<PromiseRaw<'static, ()>> {
        let Some(write) = &self.inner else {
            return Err(napi::Error::from_reason("write state is unavailable"));
        };
        let mut deferred = std::ptr::null_mut();
        let mut promise = std::ptr::null_mut();
        let env = env.raw();
        let status = unsafe { sys::napi_create_promise(env, &mut deferred, &mut promise) };
        if status != sys::Status::napi_ok {
            return Err(napi::Error::from_reason(
                "failed to create transaction wait promise",
            ));
        }
        let callback = move |result: std::result::Result<TxId, jazz::db::Error>| {
            finish_wait_promise(env, deferred, result);
        };
        match write {
            NapiWrite::Memory { db, tx_id } => {
                db.wait_for_transaction_with(*tx_id, tier, callback);
            }
            NapiWrite::Persistent { db, tx_id } => {
                db.wait_for_transaction_with(*tx_id, tier, callback);
            }
        }
        Ok(PromiseRaw::new(env, promise))
    }
}

#[napi]
impl Write {
    #[napi(getter, js_name = "batchId")]
    pub fn batch_id(&self) -> String {
        self.batch_id.to_string()
    }

    #[napi(getter)]
    pub fn payload(&self) -> Uint8Array {
        Uint8Array::new(self.payload.clone())
    }

    #[napi(js_name = "writeState")]
    pub fn write_state(&self) -> napi::Result<serde_json::Value> {
        let Some(write) = &self.inner else {
            return Err(napi::Error::from_reason("write state is unavailable"));
        };
        let state = match write {
            NapiWrite::Memory { db, tx_id } => db.write_state(*tx_id),
            NapiWrite::Persistent { db, tx_id } => db.write_state(*tx_id),
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(core_write_state_to_json(&state))
    }

    #[napi]
    pub fn wait(&self, env: Env, tier: String) -> napi::Result<PromiseRaw<'static, ()>> {
        self.wait_promise(env, core_durability_tier_from_str(&tier)?)
    }

    #[napi]
    pub fn close(&mut self) -> bool {
        self.inner.take().is_some()
    }
}

#[napi]
impl Transport {
    #[napi(js_name = "sendWireFrame")]
    pub fn send_wire_frame(&self, frame: Uint8Array) {
        self.queues.inbound.borrow_mut().push_back(frame.to_vec());
    }

    #[napi(js_name = "sendWireFrames")]
    pub fn send_wire_frames(&self, frames: Vec<Uint8Array>) {
        let mut inbound = self.queues.inbound.borrow_mut();
        for frame in frames {
            inbound.push_back(frame.to_vec());
        }
    }

    #[napi(js_name = "recvWireFrames")]
    pub fn recv_wire_frames(&self) -> Vec<Uint8Array> {
        let mut frames = Vec::new();
        let mut outbound = self.queues.outbound.borrow_mut();
        while let Some(frame) = outbound.pop_front() {
            frames.push(Uint8Array::new(frame));
        }
        frames
    }

    #[napi]
    pub fn tick(&self) -> napi::Result<u32> {
        match &self.inner {
            NapiTransportInner::Memory { connection, .. } => core_tick_connection(connection),
            NapiTransportInner::Persistent { connection, .. } => core_tick_connection(connection),
            NapiTransportInner::Closed => Ok(0),
        }
    }

    #[napi]
    pub fn close(&mut self) -> bool {
        match std::mem::replace(&mut self.inner, NapiTransportInner::Closed) {
            NapiTransportInner::Memory { db, connection } => {
                let Some(connection) = connection else {
                    return false;
                };
                db.detach_connection(&connection)
            }
            NapiTransportInner::Persistent { db, connection } => {
                let Some(connection) = connection else {
                    return false;
                };
                db.detach_connection(&connection)
            }
            NapiTransportInner::Closed => false,
        }
    }
}

#[napi]
impl Subscription {
    #[napi(js_name = "readAll")]
    pub fn read_all(&mut self) -> napi::Result<Vec<SubscriptionEvent>> {
        let subscription = self
            .inner
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("subscription is closed"))?;
        let mut events = Vec::new();
        loop {
            let event = match subscription {
                NapiSubscription::Memory(stream) => stream.try_next_event(),
                NapiSubscription::Persistent(stream) => stream.try_next_event(),
            };
            let Some(event) = event else {
                break;
            };
            events.push(core_subscription_event_to_napi(
                &event,
                &mut self.published_terminal_layouts,
            )?);
        }
        Ok(events)
    }

    #[napi]
    pub fn drain(&mut self) -> napi::Result<Vec<SubscriptionEvent>> {
        self.read_all()
    }

    #[napi]
    pub fn close(&mut self) -> bool {
        self.inner.take().is_some()
    }
}

#[napi]
impl Tx {
    #[napi(js_name = "insertWithIdEncoded")]
    pub fn insert_with_id_encoded(
        &mut self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<()> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let now_ms = updated_at_ms.map(|value| value as u64);
        match self.kind {
            NapiTxKind::Mergeable => with_napi_mergeable_tx!(self, |tx| match now_ms {
                Some(now_ms) => tx.insert_with_id_at_ms(&table, row_id, cells, now_ms),
                None => tx.insert_with_id(&table, row_id, cells),
            }),
            NapiTxKind::Exclusive => {
                with_napi_exclusive_tx!(self, |tx| tx.insert_with_id(&table, row_id, cells))
            }
        }
    }

    #[napi(js_name = "updateEncoded")]
    pub fn update_encoded(
        &mut self,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<()> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let patch = decode_core_cells(&patch)?;
        let now_ms = updated_at_ms.map(|value| value as u64);
        match self.kind {
            NapiTxKind::Mergeable => with_napi_mergeable_tx!(self, |tx| match now_ms {
                Some(now_ms) => tx.update_at_ms(&table, row_id, patch, now_ms),
                None => tx.update(&table, row_id, patch),
            }),
            NapiTxKind::Exclusive => {
                with_napi_exclusive_tx!(self, |tx| tx.update(&table, row_id, patch))
            }
        }
    }

    #[napi(js_name = "upsertEncoded")]
    pub fn upsert_encoded(
        &mut self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<()> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let now_ms = updated_at_ms.map(|value| value as u64);
        match self.kind {
            NapiTxKind::Mergeable => with_napi_mergeable_tx!(self, |tx| match now_ms {
                Some(now_ms) => tx.update_at_ms(&table, row_id, cells, now_ms),
                None => tx.update(&table, row_id, cells),
            }),
            NapiTxKind::Exclusive => {
                with_napi_exclusive_tx!(self, |tx| tx.update(&table, row_id, cells))
            }
        }
    }

    #[napi(js_name = "delete")]
    pub fn delete_encoded(
        &mut self,
        table: String,
        row_id: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<()> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        match self.kind {
            NapiTxKind::Mergeable => match updated_at_ms.map(|value| value as u64) {
                Some(now_ms) => {
                    with_napi_mergeable_tx!(self, |tx| tx.delete_at_ms(&table, row_id, now_ms))
                }
                None => with_napi_mergeable_tx!(self, |tx| tx.delete(&table, row_id)),
            },
            NapiTxKind::Exclusive => {
                with_napi_exclusive_tx!(self, |tx| tx.delete(&table, row_id))
            }
        }
    }

    #[napi(js_name = "restoreEncoded")]
    pub fn restore_encoded(
        &mut self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<()> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let now_ms = updated_at_ms.map(|value| value as u64);
        match self.kind {
            NapiTxKind::Mergeable => with_napi_mergeable_tx!(self, |tx| match now_ms {
                Some(now_ms) => tx.restore_at_ms(&table, row_id, cells, now_ms),
                None => tx.restore(&table, row_id, cells),
            }),
            NapiTxKind::Exclusive => {
                with_napi_exclusive_tx!(self, |tx| tx.restore(&table, row_id, cells))
            }
        }
    }

    #[napi]
    pub fn commit(&mut self) -> napi::Result<Write> {
        if !self.owns_lifetime {
            return Err(napi::Error::from_reason(
                "attached transaction views cannot commit the owner-wide batch",
            ));
        }
        let open_tx = self.open_tx()?;
        let write = match &self.db {
            NapiDbInnerStorage::Memory(db) => core_commit_tx_memory(db, open_tx),
            NapiDbInnerStorage::Persistent(db) => core_commit_tx_persistent(db, open_tx),
        }?;
        self.open_tx.take();
        Ok(write)
    }

    #[napi]
    pub fn rollback(&mut self) -> napi::Result<()> {
        if !self.owns_lifetime {
            return Err(napi::Error::from_reason(
                "attached transaction views cannot roll back the owner-wide batch",
            ));
        }
        let open_tx = self.open_tx()?;
        self.abandon(open_tx)?;
        self.open_tx.take();
        Ok(())
    }
}

impl Tx {
    fn open_tx(&self) -> napi::Result<CoreOpenBatchId> {
        self.open_tx
            .ok_or_else(|| napi::Error::from_reason("transaction is already closed"))
    }

    fn abandon(&self, open_tx: CoreOpenBatchId) -> napi::Result<()> {
        match &self.db {
            NapiDbInnerStorage::Memory(db) => db.abandon_transaction_handle(open_tx),
            NapiDbInnerStorage::Persistent(db) => db.abandon_transaction_handle(open_tx),
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))
    }
}

impl Drop for Tx {
    fn drop(&mut self) {
        if !self.owns_lifetime {
            return;
        }
        let Some(open_tx) = self.open_tx.take() else {
            return;
        };
        let _ = self.abandon(open_tx);
    }
}

#[napi(js_name = "NapiDb")]
pub struct NapiDb {
    inner: NapiDbInner,
    owns_runtime: bool,
}

#[napi]
impl NapiDb {
    #[napi(factory, js_name = "openMemory")]
    pub fn open_memory(schema: Uint8Array, config: Uint8Array) -> napi::Result<Self> {
        let (schema, config) = decode_core_open_args(&schema, &config)?;
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let db = open_core_db(schema, CoreMemoryStorage::new(&refs), config)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(NapiDbInnerStorage::Memory(Rc::new(db))))),
            owns_runtime: true,
        })
    }

    #[napi(factory, js_name = "openPersistent")]
    pub fn open_persistent(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
    ) -> napi::Result<Self> {
        let (schema, config) = decode_core_open_args(&schema, &config)?;
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = CoreRocksDbStorage::open(data_path, &refs)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        let db = open_core_db(schema, storage, config)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(NapiDbInnerStorage::Persistent(Rc::new(
                db,
            ))))),
            owns_runtime: true,
        })
    }

    /// Register and return a typed view backed by this same runtime owner.
    #[napi(js_name = "registerSchema")]
    pub fn register_schema(&self, schema: Uint8Array) -> napi::Result<Self> {
        let schema: JazzSchema = postcard::from_bytes(&schema)
            .map_err(|error| napi::Error::from_reason(format!("decode schema: {error}")))?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let view = match db {
            NapiDbInnerStorage::Memory(db) => NapiDbInnerStorage::Memory(Rc::new(
                db.register_schema_view(schema)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            )),
            NapiDbInnerStorage::Persistent(db) => NapiDbInnerStorage::Persistent(Rc::new(
                db.register_schema_view(schema)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            )),
        };
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(view))),
            owns_runtime: false,
        })
    }

    /// Attach a schema view to an owner-wide mergeable batch without opening,
    /// committing, or abandoning that batch.
    #[napi(js_name = "attachMergeableTx")]
    pub fn attach_mergeable_tx(&self, open_batch_id: String) -> napi::Result<Tx> {
        let open_batch_id = open_batch_id
            .parse::<CoreOpenBatchId>()
            .map_err(napi::Error::from_reason)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        Ok(Tx {
            db: match db {
                NapiDbInnerStorage::Memory(db) => NapiDbInnerStorage::Memory(Rc::clone(db)),
                NapiDbInnerStorage::Persistent(db) => NapiDbInnerStorage::Persistent(Rc::clone(db)),
            },
            kind: NapiTxKind::Mergeable,
            open_tx: Some(open_batch_id),
            owns_lifetime: false,
        })
    }

    /// Attach a schema view to an existing owner-wide exclusive batch.
    #[napi(js_name = "attachExclusiveTx")]
    pub fn attach_exclusive_tx(&self, open_batch_id: String) -> napi::Result<Tx> {
        let open_batch_id = open_batch_id
            .parse::<CoreOpenBatchId>()
            .map_err(napi::Error::from_reason)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        Ok(Tx {
            db: match db {
                NapiDbInnerStorage::Memory(db) => NapiDbInnerStorage::Memory(Rc::clone(db)),
                NapiDbInnerStorage::Persistent(db) => NapiDbInnerStorage::Persistent(Rc::clone(db)),
            },
            kind: NapiTxKind::Exclusive,
            open_tx: Some(open_batch_id),
            owns_lifetime: false,
        })
    }

    /// Begin one owner-wide batch without creating an owning per-schema Tx.
    #[napi(js_name = "beginTransaction")]
    pub fn begin_transaction(
        &self,
        open_batch_id: String,
        kind: String,
        author: Option<Uint8Array>,
    ) -> napi::Result<()> {
        let open_batch_id = open_batch_id
            .parse::<CoreOpenBatchId>()
            .map_err(napi::Error::from_reason)?;
        let author = author
            .as_deref()
            .map(core_author_id_from_bytes)
            .transpose()?;
        if kind != "mergeable" && kind != "exclusive" {
            return Err(napi::Error::from_reason(format!(
                "unknown batch kind {kind}"
            )));
        }
        if kind == "exclusive" && author.is_some() {
            return Err(napi::Error::from_reason(
                "exclusive batches do not accept an identity override",
            ));
        }
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        macro_rules! begin {
            ($db:expr) => {
                if kind == "mergeable" {
                    match author {
                        Some(author) => $db.begin_mergeable_for_identity(open_batch_id, author),
                        None => $db.begin_mergeable(open_batch_id),
                    }
                } else {
                    $db.begin_exclusive(open_batch_id)
                }
            };
        }
        match db {
            NapiDbInnerStorage::Memory(db) => begin!(db),
            NapiDbInnerStorage::Persistent(db) => begin!(db),
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    /// Commit an owner-wide batch by id and optional kind.
    #[napi(js_name = "commitTransaction")]
    pub fn commit_transaction(
        &self,
        open_batch_id: String,
        kind: Option<String>,
    ) -> napi::Result<Write> {
        let open_batch_id = open_batch_id
            .parse::<CoreOpenBatchId>()
            .map_err(napi::Error::from_reason)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match (db, kind.as_deref().unwrap_or("mergeable")) {
            (NapiDbInnerStorage::Memory(db), "mergeable") => {
                core_commit_tx_memory(db, open_batch_id)
            }
            (NapiDbInnerStorage::Persistent(db), "mergeable") => {
                core_commit_tx_persistent(db, open_batch_id)
            }
            (NapiDbInnerStorage::Memory(db), "exclusive") => {
                core_commit_exclusive_tx_memory(db, open_batch_id)
            }
            (NapiDbInnerStorage::Persistent(db), "exclusive") => {
                core_commit_exclusive_tx_persistent(db, open_batch_id)
            }
            (_, kind) => Err(napi::Error::from_reason(format!(
                "unknown batch kind {kind}"
            ))),
        }
    }

    /// Roll back an owner-wide open batch by id.
    #[napi(js_name = "rollbackTransaction")]
    pub fn rollback_transaction(&self, open_batch_id: String) -> napi::Result<()> {
        let open_batch_id = open_batch_id
            .parse::<CoreOpenBatchId>()
            .map_err(napi::Error::from_reason)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => db.abandon_transaction_handle(open_batch_id),
            NapiDbInnerStorage::Persistent(db) => db.abandon_transaction_handle(open_batch_id),
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi(js_name = "setTickScheduler")]
    pub fn set_tick_scheduler(&self, callback: ThreadsafeFunction<String, ()>) -> napi::Result<()> {
        let scheduler = Rc::new(NapiTickScheduler { callback });
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => db.set_tick_scheduler(Some(scheduler)),
            NapiDbInnerStorage::Persistent(db) => db.set_tick_scheduler(Some(scheduler)),
        }
        Ok(())
    }

    #[napi(
        js_name = "onMutationError",
        ts_args_type = "callback: (event: any) => void"
    )]
    pub fn on_mutation_error(
        &self,
        callback: ThreadsafeFunction<JsonValue, ()>,
    ) -> napi::Result<()> {
        let callback: CoreMutationErrorCallback = Rc::new(move |event| {
            let Ok(event) = serde_json::to_value(event) else {
                return;
            };
            let _ = callback.call(Ok(event), ThreadsafeFunctionCallMode::NonBlocking);
        });
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => db.on_mutation_error(Rc::clone(&callback)),
            NapiDbInnerStorage::Persistent(db) => db.on_mutation_error(Rc::clone(&callback)),
        }
        Ok(())
    }

    #[napi(js_name = "prepareQuery")]
    pub fn prepare_query(&self, query: Uint8Array) -> napi::Result<PreparedQuery> {
        let query: CoreQuery = postcard::from_bytes(&query)
            .map_err(|error| napi::Error::from_reason(format!("decode query: {error}")))?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let inner = match db {
            NapiDbInnerStorage::Memory(db) => db.prepare_query(&query),
            NapiDbInnerStorage::Persistent(db) => db.prepare_query(&query),
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(PreparedQuery { inner })
    }

    #[napi]
    pub fn all(
        &self,
        query: &PreparedQuery,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
    ) -> napi::Result<Uint8Array> {
        let opts = core_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let rows = match db {
            NapiDbInnerStorage::Memory(db) => core_block_on(db.all(&query.inner, opts)),
            NapiDbInnerStorage::Persistent(db) => core_block_on(db.all(&query.inner, opts)),
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        encode_core_rows(&rows)
            .map(Uint8Array::new)
            .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi(js_name = "setIdentityClaims")]
    pub fn set_identity_claims(
        &self,
        author: Uint8Array,
        #[napi(ts_arg_type = "Record<string, unknown> | undefined | null")] claims: Option<
            JsonValue,
        >,
    ) -> napi::Result<()> {
        let author = core_author_id_from_bytes(&author)?;
        let claims = core_claims_from_json(author, claims)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => db.set_identity_claims(author, claims),
            NapiDbInnerStorage::Persistent(db) => db.set_identity_claims(author, claims),
        }
        Ok(())
    }

    #[napi(js_name = "allForIdentity")]
    pub fn all_for_identity(
        &self,
        query: &PreparedQuery,
        author: Uint8Array,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
    ) -> napi::Result<Uint8Array> {
        let author = core_author_id_from_bytes(&author)?;
        let opts = core_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let rows = match db {
            NapiDbInnerStorage::Memory(db) => {
                core_block_on(db.all_for_identity(&query.inner, opts, author))
            }
            NapiDbInnerStorage::Persistent(db) => {
                core_block_on(db.all_for_identity(&query.inner, opts, author))
            }
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        encode_core_rows(&rows)
            .map(Uint8Array::new)
            .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi(js_name = "allRelationSnapshot")]
    pub fn all_relation_snapshot(
        &self,
        query: &PreparedQuery,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
    ) -> napi::Result<Uint8Array> {
        let opts = core_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let snapshot = match db {
            NapiDbInnerStorage::Memory(db) => {
                core_block_on(db.all_relation_snapshot(&query.inner, opts))
            }
            NapiDbInnerStorage::Persistent(db) => {
                core_block_on(db.all_relation_snapshot(&query.inner, opts))
            }
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        encode_core_relation_snapshot(&snapshot)
            .map(Uint8Array::new)
            .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi(js_name = "allRelationSnapshotForIdentity")]
    pub fn all_relation_snapshot_for_identity(
        &self,
        query: &PreparedQuery,
        author: Uint8Array,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
    ) -> napi::Result<Uint8Array> {
        let author = core_author_id_from_bytes(&author)?;
        let opts = core_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let snapshot = match db {
            NapiDbInnerStorage::Memory(db) => {
                core_block_on(db.all_relation_snapshot_for_identity(&query.inner, opts, author))
            }
            NapiDbInnerStorage::Persistent(db) => {
                core_block_on(db.all_relation_snapshot_for_identity(&query.inner, opts, author))
            }
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        encode_core_relation_snapshot(&snapshot)
            .map(Uint8Array::new)
            .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi(js_name = "allRelationQuery")]
    pub fn all_relation_query(
        &self,
        query_json: String,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
    ) -> napi::Result<Uint8Array> {
        let query = core_relation_query_from_json(&query_json)?;
        let opts = core_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let snapshot = match db {
            NapiDbInnerStorage::Memory(db) => core_block_on(db.all_relation_query(&query, opts)),
            NapiDbInnerStorage::Persistent(db) => {
                core_block_on(db.all_relation_query(&query, opts))
            }
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        encode_core_rows(&snapshot.rows)
            .map(Uint8Array::new)
            .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi(js_name = "allRelationQueryForIdentity")]
    pub fn all_relation_query_for_identity(
        &self,
        query_json: String,
        author: Uint8Array,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
    ) -> napi::Result<Uint8Array> {
        let query = core_relation_query_from_json(&query_json)?;
        let author = core_author_id_from_bytes(&author)?;
        let opts = core_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let snapshot = match db {
            NapiDbInnerStorage::Memory(db) => {
                core_block_on(db.all_relation_query_for_identity(&query, opts, author))
            }
            NapiDbInnerStorage::Persistent(db) => {
                core_block_on(db.all_relation_query_for_identity(&query, opts, author))
            }
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        encode_core_rows(&snapshot.rows)
            .map(Uint8Array::new)
            .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi(js_name = "localCurrentRow")]
    pub fn local_current_row(&self, table: String, row_id: Uint8Array) -> napi::Result<Uint8Array> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let row = match db {
            NapiDbInnerStorage::Memory(db) => db.local_current_row(&table, row_id),
            NapiDbInnerStorage::Persistent(db) => db.local_current_row(&table, row_id),
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        let rows = row.into_iter().collect::<Vec<_>>();
        encode_core_rows(&rows)
            .map(Uint8Array::new)
            .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi(js_name = "attachQuery")]
    pub fn attach_query(
        &self,
        query: &PreparedQuery,
        opts: Option<serde_json::Value>,
    ) -> napi::Result<QueryAttachment> {
        let opts = core_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let inner = match db {
            NapiDbInnerStorage::Memory(db) => db.attach_query_with_opts(&query.inner, opts),
            NapiDbInnerStorage::Persistent(db) => db.attach_query_with_opts(&query.inner, opts),
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(QueryAttachment { inner })
    }

    #[napi(js_name = "attachQueryForIdentity")]
    pub fn attach_query_for_identity(
        &self,
        query: &PreparedQuery,
        author: Uint8Array,
        opts: Option<serde_json::Value>,
    ) -> napi::Result<QueryAttachment> {
        let author = core_author_id_from_bytes(&author)?;
        let opts = core_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let inner = match db {
            NapiDbInnerStorage::Memory(db) => {
                db.attach_query_with_opts_for_identity(&query.inner, opts, author)
            }
            NapiDbInnerStorage::Persistent(db) => {
                db.attach_query_with_opts_for_identity(&query.inner, opts, author)
            }
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(QueryAttachment { inner })
    }

    #[napi(js_name = "queryAttachmentIsCovered")]
    pub fn query_attachment_is_covered(&self, attachment: &QueryAttachment) -> napi::Result<bool> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        Ok(match db {
            NapiDbInnerStorage::Memory(db) => db.query_attachment_is_covered(&attachment.inner),
            NapiDbInnerStorage::Persistent(db) => db.query_attachment_is_covered(&attachment.inner),
        })
    }

    #[napi(js_name = "detachQuery")]
    pub fn detach_query(&self, attachment: &QueryAttachment) -> napi::Result<()> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => db.detach_query(attachment.inner.clone()),
            NapiDbInnerStorage::Persistent(db) => db.detach_query(attachment.inner.clone()),
        }
        Ok(())
    }

    #[napi]
    pub fn subscribe(
        &self,
        query: &PreparedQuery,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
    ) -> napi::Result<Subscription> {
        let opts = core_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let inner = match db {
            NapiDbInnerStorage::Memory(db) => NapiSubscription::Memory(
                core_block_on(db.subscribe(&query.inner, opts))
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => NapiSubscription::Persistent(
                core_block_on(db.subscribe(&query.inner, opts))
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        };
        Ok(Subscription {
            inner: Some(inner),
            published_terminal_layouts: HashSet::new(),
        })
    }

    #[napi(js_name = "subscribeForIdentity")]
    pub fn subscribe_for_identity(
        &self,
        query: &PreparedQuery,
        author: Uint8Array,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
    ) -> napi::Result<Subscription> {
        let author = core_author_id_from_bytes(&author)?;
        let opts = core_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let inner = match db {
            NapiDbInnerStorage::Memory(db) => NapiSubscription::Memory(
                core_block_on(db.subscribe_for_identity(&query.inner, opts, author))
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => NapiSubscription::Persistent(
                core_block_on(db.subscribe_for_identity(&query.inner, opts, author))
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        };
        Ok(Subscription {
            inner: Some(inner),
            published_terminal_layouts: HashSet::new(),
        })
    }

    #[napi(js_name = "subscribeRelationQuery")]
    pub fn subscribe_relation_query(
        &self,
        query_json: String,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
    ) -> napi::Result<Subscription> {
        let query = core_relation_query_from_json(&query_json)?;
        let opts = core_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let inner = match db {
            NapiDbInnerStorage::Memory(db) => NapiSubscription::Memory(
                core_block_on(db.subscribe_relation_query(&query, opts))
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => NapiSubscription::Persistent(
                core_block_on(db.subscribe_relation_query(&query, opts))
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        };
        Ok(Subscription {
            inner: Some(inner),
            published_terminal_layouts: HashSet::new(),
        })
    }

    #[napi(js_name = "subscribeRelationQueryForIdentity")]
    pub fn subscribe_relation_query_for_identity(
        &self,
        query_json: String,
        author: Uint8Array,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
    ) -> napi::Result<Subscription> {
        let query = core_relation_query_from_json(&query_json)?;
        let author = core_author_id_from_bytes(&author)?;
        let opts = core_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let inner = match db {
            NapiDbInnerStorage::Memory(db) => NapiSubscription::Memory(
                core_block_on(db.subscribe_relation_query_for_identity(&query, opts, author))
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => NapiSubscription::Persistent(
                core_block_on(db.subscribe_relation_query_for_identity(&query, opts, author))
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        };
        Ok(Subscription {
            inner: Some(inner),
            published_terminal_layouts: HashSet::new(),
        })
    }

    #[napi(js_name = "insertWithIdEncoded")]
    pub fn insert_with_id_encoded(
        &self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let updated_at_ms = updated_at_ms.map(|value| value as u64);
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.insert_with_id_at_ms(&table, row_id, cells, now_ms),
                    None => db.insert_with_id(&table, row_id, cells),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.insert_with_id_at_ms(&table, row_id, cells, now_ms),
                    None => db.insert_with_id(&table, row_id, cells),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        }
    }

    #[napi(js_name = "insertWithIdEncodedForIdentity")]
    pub fn insert_with_id_encoded_for_identity(
        &self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        author: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let author = core_author_id_from_bytes(&author)?;
        let updated_at_ms = updated_at_ms.map(|value| value as u64);
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => {
                        db.insert_with_id_for_identity_at_ms(author, &table, row_id, cells, now_ms)
                    }
                    None => db.insert_with_id_for_identity(author, &table, row_id, cells),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => {
                        db.insert_with_id_for_identity_at_ms(author, &table, row_id, cells, now_ms)
                    }
                    None => db.insert_with_id_for_identity(author, &table, row_id, cells),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        }
    }

    #[napi(js_name = "updateEncoded")]
    pub fn update_encoded(
        &self,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let patch = decode_core_cells(&patch)?;
        let updated_at_ms = updated_at_ms.map(|value| value as u64);
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.update_at_ms(&table, row_id, patch, now_ms),
                    None => db.update(&table, row_id, patch),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.update_at_ms(&table, row_id, patch, now_ms),
                    None => db.update(&table, row_id, patch),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        }
    }

    #[napi(js_name = "updateEncodedForIdentity")]
    pub fn update_encoded_for_identity(
        &self,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        author: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let patch = decode_core_cells(&patch)?;
        let author = core_author_id_from_bytes(&author)?;
        let updated_at_ms = updated_at_ms.map(|value| value as u64);
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => {
                        db.update_for_identity_at_ms(author, &table, row_id, patch, now_ms)
                    }
                    None => db.update_for_identity(author, &table, row_id, patch),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => {
                        db.update_for_identity_at_ms(author, &table, row_id, patch, now_ms)
                    }
                    None => db.update_for_identity(author, &table, row_id, patch),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        }
    }

    #[napi(js_name = "upsertEncoded")]
    pub fn upsert_encoded(
        &self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let updated_at_ms = updated_at_ms.map(|value| value as u64);
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.upsert_at_ms(&table, row_id, cells, now_ms),
                    None => db.upsert(&table, row_id, cells),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.upsert_at_ms(&table, row_id, cells, now_ms),
                    None => db.upsert(&table, row_id, cells),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        }
    }

    #[napi(js_name = "upsertEncodedForIdentity")]
    pub fn upsert_encoded_for_identity(
        &self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        author: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let author = core_author_id_from_bytes(&author)?;
        let updated_at_ms = updated_at_ms.map(|value| value as u64);
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => {
                        db.upsert_for_identity_at_ms(author, &table, row_id, cells, now_ms)
                    }
                    None => db.upsert_for_identity(author, &table, row_id, cells),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => {
                        db.upsert_for_identity_at_ms(author, &table, row_id, cells, now_ms)
                    }
                    None => db.upsert_for_identity(author, &table, row_id, cells),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        }
    }

    #[napi(js_name = "delete")]
    pub fn delete_encoded(
        &self,
        table: String,
        row_id: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let updated_at_ms = updated_at_ms.map(|value| value as u64);
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.delete_at_ms(&table, row_id, now_ms),
                    None => db.delete(&table, row_id),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.delete_at_ms(&table, row_id, now_ms),
                    None => db.delete(&table, row_id),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        }
    }

    #[napi(js_name = "deleteForIdentity")]
    pub fn delete_for_identity(
        &self,
        table: String,
        row_id: Uint8Array,
        author: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let author = core_author_id_from_bytes(&author)?;
        let updated_at_ms = updated_at_ms.map(|value| value as u64);
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.delete_for_identity_at_ms(author, &table, row_id, now_ms),
                    None => db.delete_for_identity(author, &table, row_id),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.delete_for_identity_at_ms(author, &table, row_id, now_ms),
                    None => db.delete_for_identity(author, &table, row_id),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        }
    }

    #[napi(js_name = "restoreEncoded")]
    pub fn restore_encoded(
        &self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let updated_at_ms = updated_at_ms.map(|value| value as u64);
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.restore_at_ms(&table, row_id, cells, now_ms),
                    None => db.restore(&table, row_id, cells),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.restore_at_ms(&table, row_id, cells, now_ms),
                    None => db.restore(&table, row_id, cells),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        }
    }

    #[napi(js_name = "restoreEncodedForIdentity")]
    pub fn restore_encoded_for_identity(
        &self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        author: Uint8Array,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let author = core_author_id_from_bytes(&author)?;
        let updated_at_ms = updated_at_ms.map(|value| value as u64);
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => {
                        db.restore_for_identity_at_ms(author, &table, row_id, cells, now_ms)
                    }
                    None => db.restore_for_identity(author, &table, row_id, cells),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => {
                        db.restore_for_identity_at_ms(author, &table, row_id, cells, now_ms)
                    }
                    None => db.restore_for_identity(author, &table, row_id, cells),
                }
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        }
    }

    #[napi]
    pub fn tick(&self) -> napi::Result<()> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => db.tick(),
            NapiDbInnerStorage::Persistent(db) => db.tick(),
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi(js_name = "setNonDurableClient")]
    pub fn set_non_durable_client(&self) -> napi::Result<()> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => db.set_non_durable_client(),
            NapiDbInnerStorage::Persistent(db) => db.set_non_durable_client(),
        }
        Ok(())
    }

    #[napi(js_name = "connectUpstream")]
    pub fn connect_upstream(&self) -> napi::Result<Transport> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let queues = WireQueues::default();
        // The JS WebSocket carrier has no authenticated endpoint context for
        // scoped receipt/view frames. Keep this upstream transport aligned
        // with its authority-unbound Hello until such a context is plumbed.
        let transport = Box::new(CoreWireTransportAdapter::new(
            NapiWireTransport {
                queues: queues.clone(),
            },
            jazz::wire::WIRE_PROTOCOL_VERSION,
            jazz::wire::current_wire_features()
                & !(jazz::wire::FEATURE_AUTHORIZATION_SCOPE_RECEIPTS
                    | jazz::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS),
            None,
        ));
        let inner = match db {
            NapiDbInnerStorage::Memory(db) => NapiTransportInner::Memory {
                db: Rc::clone(db),
                connection: Some(db.connect_upstream(transport)),
            },
            NapiDbInnerStorage::Persistent(db) => NapiTransportInner::Persistent {
                db: Rc::clone(db),
                connection: Some(db.connect_upstream(transport)),
            },
        };
        Ok(Transport { inner, queues })
    }

    #[napi(js_name = "connectUpstreamWithSession")]
    pub fn connect_upstream_with_session(
        &self,
        protocol_version: u16,
        features: u32,
        remote_node: Buffer,
        remote_epoch: BigInt,
        local_node: Buffer,
        local_epoch: BigInt,
    ) -> napi::Result<Transport> {
        let remote_node: [u8; 16] = remote_node.as_ref().try_into().map_err(|_| {
            napi::Error::from_reason("server hello authority node must be 16 bytes")
        })?;
        let local_node: [u8; 16] = local_node
            .as_ref()
            .try_into()
            .map_err(|_| napi::Error::from_reason("local peer identity must be 16 bytes"))?;
        let remote_epoch =
            authority_epoch_from_bigint(remote_epoch, "server hello authority epoch")?;
        let local_epoch = authority_epoch_from_bigint(local_epoch, "local connection epoch")?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let queues = WireQueues::default();
        let session_context = CoreConnectionSessionContext {
            local: CoreWireAuthorityEndpoint {
                node: CoreNodeUuid::from_bytes(local_node),
                epoch: local_epoch,
            },
            remote: CoreWireAuthorityEndpoint {
                node: CoreNodeUuid::from_bytes(remote_node),
                epoch: remote_epoch,
            },
            link_identity: CoreAuthorId::from_bytes(local_node),
            negotiated_features: features as u64,
        };
        let transport = Box::new(CoreWireTransportAdapter::new_with_session_context(
            NapiWireTransport {
                queues: queues.clone(),
            },
            protocol_version,
            features as u64,
            None,
            Some(session_context),
        ));
        let inner = match db {
            NapiDbInnerStorage::Memory(db) => NapiTransportInner::Memory {
                db: Rc::clone(db),
                connection: Some(db.connect_upstream(transport)),
            },
            NapiDbInnerStorage::Persistent(db) => NapiTransportInner::Persistent {
                db: Rc::clone(db),
                connection: Some(db.connect_upstream(transport)),
            },
        };
        Ok(Transport { inner, queues })
    }

    #[napi(js_name = "mergeableTx")]
    pub fn mergeable_tx(&self, open_batch_id: String) -> napi::Result<Tx> {
        let open_batch_id = open_batch_id
            .parse::<CoreOpenBatchId>()
            .map_err(napi::Error::from_reason)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => Ok(Tx {
                db: NapiDbInnerStorage::Memory(Rc::clone(db)),
                kind: NapiTxKind::Mergeable,
                open_tx: Some({
                    db.begin_mergeable(open_batch_id)
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                    open_batch_id
                }),
                owns_lifetime: true,
            }),
            NapiDbInnerStorage::Persistent(db) => Ok(Tx {
                db: NapiDbInnerStorage::Persistent(Rc::clone(db)),
                kind: NapiTxKind::Mergeable,
                open_tx: Some({
                    db.begin_mergeable(open_batch_id)
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                    open_batch_id
                }),
                owns_lifetime: true,
            }),
        }
    }

    #[napi(js_name = "mergeableTxForIdentity")]
    pub fn mergeable_tx_for_identity(
        &self,
        open_batch_id: String,
        author: Uint8Array,
    ) -> napi::Result<Tx> {
        let open_batch_id = open_batch_id
            .parse::<CoreOpenBatchId>()
            .map_err(napi::Error::from_reason)?;
        let author = core_author_id_from_bytes(&author)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => Ok(Tx {
                db: NapiDbInnerStorage::Memory(Rc::clone(db)),
                kind: NapiTxKind::Mergeable,
                open_tx: Some({
                    db.begin_mergeable_for_identity(open_batch_id, author)
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                    open_batch_id
                }),
                owns_lifetime: true,
            }),
            NapiDbInnerStorage::Persistent(db) => Ok(Tx {
                db: NapiDbInnerStorage::Persistent(Rc::clone(db)),
                kind: NapiTxKind::Mergeable,
                open_tx: Some({
                    db.begin_mergeable_for_identity(open_batch_id, author)
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                    open_batch_id
                }),
                owns_lifetime: true,
            }),
        }
    }

    #[napi]
    pub fn close(&self) -> napi::Result<()> {
        let inner = self.inner.borrow_mut().take();
        if !self.owns_runtime {
            return Ok(());
        }
        if let Some(inner) = inner {
            match inner {
                NapiDbInnerStorage::Memory(db) => db
                    .close()
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
                NapiDbInnerStorage::Persistent(db) => db
                    .close()
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            }
        }
        Ok(())
    }
}

fn authority_epoch_from_bigint(value: BigInt, label: &str) -> napi::Result<u64> {
    let (negative, epoch, lossless) = value.get_u64();
    if negative || !lossless {
        return Err(napi::Error::from_reason(format!(
            "{label} must be an unsigned 64-bit integer"
        )));
    }
    Ok(epoch)
}

fn decode_core_open_args(
    schema: &[u8],
    config: &[u8],
) -> napi::Result<(JazzSchema, CoreOpenDbConfig)> {
    let schema: JazzSchema = postcard::from_bytes(schema)
        .map_err(|error| napi::Error::from_reason(format!("decode schema: {error}")))?;
    let config: CoreOpenDbConfig = postcard::from_bytes(config)
        .map_err(|error| napi::Error::from_reason(format!("decode open config: {error}")))?;
    Ok((schema, config))
}

fn open_core_db<S>(
    schema: JazzSchema,
    storage: S,
    config: CoreOpenDbConfig,
) -> std::result::Result<CoreDb<S>, jazz::db::Error>
where
    S: CoreOrderedKvStorage + CoreReopenableStorage + 'static,
{
    let mut db_config = CoreDbConfig::new(schema, storage, config.identity.into());
    if let Some(seed) = config.row_id_seed {
        db_config = db_config.with_id_source(CoreSeededRowIdSource::new(seed));
    }
    let initial_sync_flush_every = config.initial_sync_flush_every;
    if config.history_complete {
        let db = core_block_on(CoreDb::open_history_complete(db_config))?;
        configure_initial_sync_flush_cadence(&db, initial_sync_flush_every)?;
        Ok(db)
    } else {
        let db = core_block_on(CoreDb::open(db_config))?;
        configure_initial_sync_flush_cadence(&db, initial_sync_flush_every)?;
        Ok(db)
    }
}

fn configure_initial_sync_flush_cadence<S>(
    db: &CoreDb<S>,
    every: Option<u32>,
) -> std::result::Result<(), jazz::db::Error>
where
    S: CoreOrderedKvStorage + CoreReopenableStorage + 'static,
{
    let Some(every) = every else {
        return Ok(());
    };
    let Some(every) = std::num::NonZeroUsize::new(every as usize) else {
        return Ok(());
    };
    db.set_initial_sync_flush_cadence(CoreInitialSyncFlushCadence::every(every))
}

fn decode_core_cells(bytes: &[u8]) -> napi::Result<CoreRowCells> {
    let (descriptor, raw): (RecordDescriptor, Vec<u8>) = postcard::from_bytes(bytes)
        .map_err(|error| napi::Error::from_reason(format!("decode cells: {error}")))?;
    let record = CoreBorrowedRecord::new(&raw, &descriptor);
    let values = record
        .to_values()
        .map_err(|error| napi::Error::from_reason(format!("decode cell record: {error}")))?;
    let mut cells = CoreRowCells::new();
    for (field, value) in descriptor.fields().iter().zip(values) {
        let Some(name) = &field.name else {
            return Err(napi::Error::from_reason(
                "encoded cells must use named fields",
            ));
        };
        cells.insert(name.clone(), value);
    }
    Ok(cells)
}

fn core_row_uuid_from_bytes(bytes: &[u8]) -> napi::Result<CoreRowUuid> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| napi::Error::from_reason("row id must be 16 bytes"))?;
    Ok(CoreRowUuid::from_bytes(bytes))
}

fn core_author_id_from_bytes(bytes: &[u8]) -> napi::Result<CoreAuthorId> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| napi::Error::from_reason("author id must be 16 bytes"))?;
    Ok(CoreAuthorId::from_bytes(bytes))
}

fn core_write_memory(
    db: Rc<CoreDb<CoreMemoryStorage>>,
    write: WriteHandle<CoreMemoryStorage>,
) -> napi::Result<Write> {
    let tx_id = write.mergeable_tx_id();
    let result = WriteResult {
        row_id: write.row_uuid(),
        tx_id,
    };
    Ok(Write {
        payload: postcard::to_allocvec(&result)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?,
        batch_id: BatchId::from_committed_tx(tx_id),
        inner: Some(NapiWrite::Memory { db, tx_id }),
    })
}

fn core_write_persistent(
    db: Rc<CoreDb<CoreRocksDbStorage>>,
    write: WriteHandle<CoreRocksDbStorage>,
) -> napi::Result<Write> {
    let tx_id = write.mergeable_tx_id();
    let result = WriteResult {
        row_id: write.row_uuid(),
        tx_id,
    };
    Ok(Write {
        payload: postcard::to_allocvec(&result)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?,
        batch_id: BatchId::from_committed_tx(tx_id),
        inner: Some(NapiWrite::Persistent { db, tx_id }),
    })
}

fn core_claims_from_json(
    author: CoreAuthorId,
    claims: Option<JsonValue>,
) -> napi::Result<BTreeMap<String, CoreValue>> {
    let mut claims = match claims {
        None | Some(JsonValue::Null) => BTreeMap::new(),
        Some(JsonValue::Object(map)) => map
            .into_iter()
            .map(|(key, value)| Ok((key, core_claim_value_from_json(value)?)))
            .collect::<napi::Result<BTreeMap<_, _>>>()?,
        Some(_) => {
            return Err(napi::Error::from_reason(
                "identity claims must be an object",
            ));
        }
    };
    let subject = author.0.to_string();
    claims
        .entry("subject".to_owned())
        .or_insert_with(|| CoreValue::String(subject.clone()));
    claims
        .entry("sub".to_owned())
        .or_insert_with(|| CoreValue::String(subject.clone()));
    claims
        .entry("user_id".to_owned())
        .or_insert_with(|| CoreValue::String(subject));
    Ok(claims)
}

fn core_claim_value_from_json(value: JsonValue) -> napi::Result<CoreValue> {
    Ok(match value {
        JsonValue::Null => CoreValue::Nullable(None),
        JsonValue::Bool(value) => CoreValue::Bool(value),
        JsonValue::Number(value) => jazz::tools::policy_claims::json_number_to_policy_claim(
            value,
            jazz::tools::policy_claims::NumericClaimOrigin::JavaScript,
        )
        .map_err(napi::Error::from_reason)?,
        JsonValue::String(value) => CoreValue::String(value),
        JsonValue::Array(values) => CoreValue::Array(
            values
                .into_iter()
                .map(core_claim_value_from_json)
                .collect::<napi::Result<Vec<_>>>()?,
        ),
        JsonValue::Object(_) => {
            return Err(napi::Error::from_reason(
                "nested object claims are not supported",
            ));
        }
    })
}

fn core_tx_write(tx_id: TxId, inner: Option<NapiWrite>) -> napi::Result<Write> {
    let result = WriteResult {
        row_id: CoreRowUuid::from_bytes([0; 16]),
        tx_id,
    };
    Ok(Write {
        payload: postcard::to_allocvec(&result)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?,
        batch_id: BatchId::from_committed_tx(tx_id),
        inner,
    })
}

fn core_tick_connection<S>(
    connection: &Option<Rc<RefCell<CorePeerConnection<S>>>>,
) -> napi::Result<u32>
where
    S: CoreOrderedKvStorage + CoreReopenableStorage + 'static,
{
    let Some(connection) = connection else {
        return Ok(0);
    };
    let stats = connection
        .borrow_mut()
        .tick()
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    Ok(stats.subscription_events as u32)
}

fn core_write_state_to_json(state: &jazz::db::WriteState) -> serde_json::Value {
    serde_json::to_value(state).unwrap_or_else(|_| serde_json::json!({}))
}

fn resolve_raw_promise(env: sys::napi_env, deferred: sys::napi_deferred) {
    let mut undefined = std::ptr::null_mut();
    let status = unsafe { sys::napi_get_undefined(env, &mut undefined) };
    if status == sys::Status::napi_ok {
        let _ = unsafe { sys::napi_resolve_deferred(env, deferred, undefined) };
    }
}

fn finish_wait_promise(
    env: sys::napi_env,
    deferred: sys::napi_deferred,
    result: std::result::Result<TxId, jazz::db::Error>,
) {
    let Err(error) = result else {
        resolve_raw_promise(env, deferred);
        return;
    };
    let message = error.to_string();
    let mut js_message = std::ptr::null_mut();
    let status = unsafe {
        sys::napi_create_string_utf8(
            env,
            message.as_ptr().cast(),
            message.len() as isize,
            &mut js_message,
        )
    };
    if status != sys::Status::napi_ok {
        return;
    }
    let mut js_error = std::ptr::null_mut();
    let status =
        unsafe { sys::napi_create_error(env, std::ptr::null_mut(), js_message, &mut js_error) };
    let rejection = if status == sys::Status::napi_ok {
        js_error
    } else {
        js_message
    };
    let _ = unsafe { sys::napi_reject_deferred(env, deferred, rejection) };
}

fn core_commit_tx<S>(db: &CoreDb<S>, open_tx: CoreOpenBatchId) -> napi::Result<TxId>
where
    S: CoreOrderedKvStorage + CoreReopenableStorage + 'static,
{
    db.commit_mergeable_handle(open_tx)
        .map_err(|error| napi::Error::from_reason(error.to_string()))
}

fn core_commit_tx_memory(
    db: &Rc<CoreDb<CoreMemoryStorage>>,
    open_tx: CoreOpenBatchId,
) -> napi::Result<Write> {
    let tx_id = core_commit_tx(db, open_tx)?;
    core_tx_write(
        tx_id,
        Some(NapiWrite::Memory {
            db: Rc::clone(db),
            tx_id,
        }),
    )
}

fn core_commit_tx_persistent(
    db: &Rc<CoreDb<CoreRocksDbStorage>>,
    open_tx: CoreOpenBatchId,
) -> napi::Result<Write> {
    let tx_id = core_commit_tx(db, open_tx)?;
    core_tx_write(
        tx_id,
        Some(NapiWrite::Persistent {
            db: Rc::clone(db),
            tx_id,
        }),
    )
}

fn core_commit_exclusive_tx_memory(
    db: &Rc<CoreDb<CoreMemoryStorage>>,
    open_tx: CoreOpenBatchId,
) -> napi::Result<Write> {
    let tx_id = db
        .commit_exclusive_handle(open_tx)
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    core_tx_write(
        tx_id,
        Some(NapiWrite::Memory {
            db: Rc::clone(db),
            tx_id,
        }),
    )
}

fn core_commit_exclusive_tx_persistent(
    db: &Rc<CoreDb<CoreRocksDbStorage>>,
    open_tx: CoreOpenBatchId,
) -> napi::Result<Write> {
    let tx_id = db
        .commit_exclusive_handle(open_tx)
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    core_tx_write(
        tx_id,
        Some(NapiWrite::Persistent {
            db: Rc::clone(db),
            tx_id,
        }),
    )
}

fn core_read_opts_from_json(value: Option<JsonValue>) -> napi::Result<CoreReadOpts> {
    let mut opts = CoreReadOpts::default();
    let Some(value) = value else {
        return Ok(opts);
    };
    if value.is_null() {
        return Ok(opts);
    }
    if let Some(tier) = optional_json_string_prop(&value, "tier")? {
        opts.tier = core_durability_tier_from_str(&tier)?;
    }
    if let Some(local_updates) = optional_json_string_prop(&value, "local_updates")? {
        opts.local_updates = match local_updates.as_str() {
            "Immediate" | "immediate" => CoreLocalUpdates::Immediate,
            "Deferred" | "deferred" => CoreLocalUpdates::Deferred,
            other => {
                return Err(napi::Error::from_reason(format!(
                    "unknown local_updates {other}"
                )));
            }
        };
    }
    if optional_json_bool_prop(&value, "propagate")? == Some(false) {
        opts.propagation = CorePropagation::LocalOnly;
    }
    if let Some(propagation) = optional_json_string_prop(&value, "propagation")? {
        opts.propagation = match propagation.as_str() {
            "Full" | "full" => CorePropagation::Full,
            "LocalOnly" | "local_only" | "localOnly" | "local-only" => CorePropagation::LocalOnly,
            other => {
                return Err(napi::Error::from_reason(format!(
                    "unknown propagation {other}"
                )));
            }
        };
    }
    if let Some(include_deleted) = optional_json_bool_prop(&value, "include_deleted")? {
        opts.include_deleted = include_deleted;
    }
    if value
        .get("read_view")
        .or_else(|| value.get("readView"))
        .filter(|read_view| !read_view.is_null())
        .is_some()
    {
        return Err(napi::Error::from_reason(
            "non-default read_view is not supported yet",
        ));
    }
    Ok(opts)
}

fn core_durability_tier_from_str(tier: &str) -> napi::Result<CoreDurabilityTier> {
    match tier {
        "None" | "none" => Ok(CoreDurabilityTier::None),
        "Local" | "local" => Ok(CoreDurabilityTier::Local),
        "Edge" | "edge" => Ok(CoreDurabilityTier::Edge),
        "Global" | "global" => Ok(CoreDurabilityTier::Global),
        other => Err(napi::Error::from_reason(format!(
            "unknown durability tier {other}"
        ))),
    }
}

fn optional_json_string_prop(value: &JsonValue, name: &str) -> napi::Result<Option<String>> {
    match value.get(name) {
        Some(JsonValue::String(value)) => Ok(Some(value.clone())),
        Some(JsonValue::Null) | None => Ok(None),
        Some(_) => Err(napi::Error::from_reason(format!("{name} must be a string"))),
    }
}

fn optional_json_bool_prop(value: &JsonValue, name: &str) -> napi::Result<Option<bool>> {
    match value.get(name) {
        Some(JsonValue::Bool(value)) => Ok(Some(*value)),
        Some(JsonValue::Null) | None => Ok(None),
        Some(_) => Err(napi::Error::from_reason(format!(
            "{name} must be a boolean"
        ))),
    }
}

fn encode_core_rows(
    rows: &[jazz::node::CurrentRow],
) -> std::result::Result<Vec<u8>, postcard::Error> {
    jazz::binding_codec::encode_rows(rows)
}

fn encode_core_relation_snapshot(
    snapshot: &jazz::node::RelationSnapshot,
) -> std::result::Result<Vec<u8>, postcard::Error> {
    jazz::binding_codec::encode_relation_snapshot(snapshot)
}

fn encode_core_subscription_delta<'a>(
    added: &'a [jazz::db::SubscriptionOutputRow],
    updated: &'a [jazz::db::SubscriptionOutputRow],
    removed: &[jazz::db::RemovedRow],
) -> std::result::Result<Vec<u8>, postcard::Error> {
    jazz::binding_codec::encode_subscription_delta(added, updated, removed)
}

fn core_subscription_event_to_napi(
    event: &CoreSubscriptionEvent,
    published_terminal_layouts: &mut HashSet<String>,
) -> napi::Result<SubscriptionEvent> {
    match event {
        CoreSubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            terminal_operations,
            terminal_layout,
            settled,
            tier,
            ..
        } => {
            let added = terminal_operations.is_empty().then_some(added.as_slice());
            let updated = terminal_operations.is_empty().then_some(updated.as_slice());
            let empty_removed = Vec::new();
            let delta = encode_core_subscription_delta(
                added.unwrap_or_default(),
                updated.unwrap_or_default(),
                if terminal_operations.is_empty() {
                    removed
                } else {
                    &empty_removed
                },
            )
            .map_err(|error| napi::Error::from_reason(error.to_string()))?;
            let (terminal_layouts, terminal_operations) = if terminal_operations.is_empty() {
                (Vec::new(), Vec::new())
            } else {
                let terminal_layout = terminal_layout.as_ref().ok_or_else(|| {
                    napi::Error::from_reason(
                        "terminal operation arrived without a prepared root layout".to_owned(),
                    )
                })?;
                if terminal_operations
                    .iter()
                    .any(|operation| operation.root_descriptor != terminal_layout.root_descriptor)
                {
                    return Err(napi::Error::from_reason(
                        "terminal operation descriptor disagrees with its prepared root layout"
                            .to_owned(),
                    ));
                }
                let terminal_layouts = published_terminal_layouts
                    .insert(terminal_layout.id.clone())
                    .then(|| core_terminal_layout_to_napi(terminal_layout))
                    .transpose()?
                    .into_iter()
                    .collect();
                let terminal_operations = terminal_operations
                    .iter()
                    .map(|operation| {
                        core_terminal_operation_to_napi(operation, terminal_layout.id.clone())
                    })
                    .collect::<std::result::Result<_, _>>()?;
                (terminal_layouts, terminal_operations)
            };
            Ok(Either3::A(SubscriptionDeltaEvent {
                event_type: "delta".to_string(),
                reset: *reset,
                delta: Uint8Array::new(delta),
                terminal_operations,
                terminal_layouts,
                settled: *settled,
                tier: format!("{tier:?}"),
            }))
        }
        CoreSubscriptionEvent::Rejected { reason } => {
            let reason = match reason {
                jazz::protocol::SubscribeRejectReason::UnsupportedShapeCapability { detail } => {
                    Either3::A(SubscriptionUnsupportedShapeCapabilityReason {
                        reason_type: "UnsupportedShapeCapability".to_string(),
                        detail: detail.clone(),
                    })
                }
                // Transient: the shape is awaiting catalogue admission and may
                // yet be served. Surfaced distinctly so a caller cannot mistake
                // it for an unsupported capability, which is permanent — that
                // conflation is the bug this variant was introduced to fix.
                jazz::protocol::SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission => {
                    Either3::B(SubscriptionShapeRegistrationPendingReason {
                        reason_type: "ShapeRegistrationPendingCatalogueAdmission".to_string(),
                    })
                }
                jazz::protocol::SubscribeRejectReason::ServerFailure { code } => {
                    Either3::C(SubscriptionServerFailureReason {
                        reason_type: "ServerFailure".to_string(),
                        code: format!("{code:?}"),
                    })
                }
            };
            Ok(Either3::B(SubscriptionRejectedEvent {
                event_type: "rejected".to_string(),
                reason,
            }))
        }
        CoreSubscriptionEvent::Closed => Ok(Either3::C(SubscriptionClosedEvent {
            event_type: "closed".to_string(),
        })),
    }
}

mod test_fixture_export {
    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    #[napi_derive::napi(js_name = "__testSubscriptionEvents", skip_typescript)]
    pub fn subscription_events() -> napi::Result<Vec<super::SubscriptionEvent>> {
        use jazz::db::SubscriptionEvent;
        use jazz::protocol::{SubscribeRejectReason, SubscribeServerFailureCode};

        [
            SubscriptionEvent::Rejected {
                reason: SubscribeRejectReason::UnsupportedShapeCapability {
                    detail: "fixture unsupported shape".to_owned(),
                },
            },
            SubscriptionEvent::Rejected {
                reason: SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission,
            },
            SubscriptionEvent::Rejected {
                reason: SubscribeRejectReason::ServerFailure {
                    code: SubscribeServerFailureCode::QueryValidation,
                },
            },
            SubscriptionEvent::Closed,
        ]
        .iter()
        .scan(std::collections::HashSet::new(), |layouts, event| {
            Some(super::core_subscription_event_to_napi(event, layouts))
        })
        .collect()
    }
}

/// Convert terminal edits without serde_json so binary subscription deltas keep
/// their typed-array representation. Root descriptors retain the upstream
/// postcard encoding; ordered keys and edit payloads retain their number-array
/// representation for the existing TypeScript terminal consumer.
fn core_terminal_operation_to_napi(
    operation: &jazz::groove::ivm::TerminalOperation,
    root_layout_id: String,
) -> napi::Result<SubscriptionTerminalOperation> {
    use jazz::groove::ivm::{TerminalEdit, TerminalPathSegment};

    let path = operation
        .path
        .iter()
        .map(|segment| match segment {
            TerminalPathSegment::Collection(collection) => {
                Either::A(SubscriptionTerminalCollectionPathSegment {
                    collection: collection.clone(),
                })
            }
            TerminalPathSegment::Key(key) => Either::B(SubscriptionTerminalKeyPathSegment {
                key: terminal_bytes_to_numbers(key),
            }),
        })
        .collect();
    let edit = match &operation.edit {
        TerminalEdit::Insert { index, key, value } => Either4::A(SubscriptionTerminalInsertEdit {
            insert: SubscriptionTerminalInsert {
                index: *index as f64,
                key: terminal_bytes_to_numbers(key),
                value: terminal_bytes_to_numbers(value),
            },
        }),
        TerminalEdit::Update { key, value } => Either4::B(SubscriptionTerminalUpdateEdit {
            update: SubscriptionTerminalUpdate {
                key: terminal_bytes_to_numbers(key),
                value: terminal_bytes_to_numbers(value),
            },
        }),
        TerminalEdit::Remove { key } => Either4::C(SubscriptionTerminalRemoveEdit {
            remove: SubscriptionTerminalRemove {
                key: terminal_bytes_to_numbers(key),
            },
        }),
        TerminalEdit::Move { key, index } => Either4::D(SubscriptionTerminalMoveEdit {
            move_edit: SubscriptionTerminalMove {
                key: terminal_bytes_to_numbers(key),
                index: *index as f64,
            },
        }),
    };

    Ok(SubscriptionTerminalOperation {
        root_layout_id,
        root_key: terminal_bytes_to_numbers(&operation.root_key),
        path,
        edit,
    })
}

fn core_terminal_layout_to_napi(
    layout: &jazz::db::TerminalRootLayout,
) -> napi::Result<SubscriptionTerminalLayout> {
    let root_descriptor = postcard::to_allocvec(&layout.root_descriptor)
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    Ok(SubscriptionTerminalLayout {
        id: layout.id.clone(),
        root_descriptor: terminal_bytes_to_numbers(&root_descriptor),
        root_key_slot: layout.root_key_slot as f64,
        root_key_field_name: layout.root_key_field_name.clone(),
        public_fields: layout
            .public_fields
            .iter()
            .map(|field| SubscriptionTerminalPublicField {
                name: field.name.clone(),
                descriptor_field_name: field.descriptor_field_name.clone(),
                slot: field.slot as f64,
                carrier: match field.carrier {
                    jazz::db::TerminalRootCarrier::CurrentRow => "CurrentRow".to_owned(),
                    jazz::db::TerminalRootCarrier::Logical => "Logical".to_owned(),
                },
            })
            .collect(),
        carrier: match layout.carrier {
            jazz::db::TerminalRootCarrier::CurrentRow => "CurrentRow".to_owned(),
            jazz::db::TerminalRootCarrier::Logical => "Logical".to_owned(),
        },
    })
}

fn terminal_bytes_to_numbers(bytes: &[u8]) -> Vec<u32> {
    bytes.iter().copied().map(u32::from).collect()
}

fn core_relation_query_from_json(query_json: &str) -> napi::Result<CoreRelationQuery> {
    let value: serde_json::Value = serde_json::from_str(query_json)
        .map_err(|err| napi::Error::from_reason(format!("decode query json: {err}")))?;
    let relation_ir = value
        .get("relation_ir")
        .ok_or_else(|| napi::Error::from_reason("relation query json is missing relation_ir"))?
        .clone();
    let rel: CoreRelationExpr = serde_json::from_value(relation_ir)
        .map_err(|err| napi::Error::from_reason(format!("decode relation_ir: {err}")))?;
    Ok(CoreRelationQuery { rel })
}

// ============================================================================
// TestJwtIssuer
// ============================================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JazzServerStartOptions {
    app_id: String,
    port: Option<u16>,
    data_dir: Option<String>,
    in_memory: Option<bool>,
    jwks_url: Option<String>,
    backend_secret: String,
    admin_secret: String,
    upstream_url: Option<String>,
    allow_local_first_auth: Option<bool>,
    telemetry_collector_url: Option<String>,
    schema: Option<Vec<u8>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestJwtForUserOptions {
    expires_in_seconds: Option<u64>,
    issuer: Option<String>,
}

fn parse_jazz_server_start_options(options: JsonValue) -> napi::Result<JazzServerStartOptions> {
    serde_json::from_value(options)
        .map_err(|error| napi::Error::from_reason(format!("Invalid JazzServer options: {error}")))
}

fn init_jazz_server_telemetry(collector_url: Option<&str>) {
    let Some(collector_url) = collector_url else {
        return;
    };
    jazz_otel::init_process_tracing_with_endpoint_once(
        "jazz-server",
        collector_url,
        &["jazz_tools=trace", "tower_http=debug"],
    );
}

#[napi]
pub struct TestJwtIssuer {
    inner: Mutex<Option<JazzTestJwtIssuer>>,
    jwks_url: String,
}

#[napi]
impl TestJwtIssuer {
    #[napi(factory, ts_return_type = "Promise<TestJwtIssuer>")]
    pub async fn start() -> napi::Result<Self> {
        let issuer = JazzTestJwtIssuer::start().await;
        let jwks_url = issuer.endpoint();
        Ok(Self {
            inner: Mutex::new(Some(issuer)),
            jwks_url,
        })
    }

    #[napi(getter, js_name = "jwksUrl")]
    pub fn jwks_url(&self) -> String {
        self.jwks_url.clone()
    }

    #[napi(js_name = "jwtForUser")]
    pub fn jwt_for_user(
        &self,
        user_id: String,
        #[napi(ts_arg_type = "Record<string, unknown> | undefined")] claims: Option<JsonValue>,
        #[napi(ts_arg_type = "{ expiresInSeconds?: number; issuer?: string } | undefined")]
        options: Option<JsonValue>,
    ) -> napi::Result<String> {
        let claims = claims.unwrap_or_else(|| serde_json::json!({ "role": "user" }));
        let options = match options {
            None | Some(JsonValue::Null) => TestJwtForUserOptions::default(),
            Some(value) => {
                serde_json::from_value::<TestJwtForUserOptions>(value).map_err(|error| {
                    napi::Error::from_reason(format!("Invalid JWT options: {error}"))
                })?
            }
        };
        let expires_in_seconds = options.expires_in_seconds.unwrap_or(3600);

        Ok(JazzTestJwtIssuer::jwt_for_user_with_options(
            &user_id,
            claims,
            TestJwtOptions {
                expires_in: Duration::from_secs(expires_in_seconds),
                issuer: options.issuer,
            },
        ))
    }

    #[napi]
    pub async fn stop(&self) -> napi::Result<()> {
        self.inner
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?
            .take();
        Ok(())
    }
}

// ============================================================================
// JazzServer
// ============================================================================

#[napi]
pub struct JazzServer {
    inner: Mutex<Option<JazzServerInner>>,
}

enum JazzServerInner {
    Core(CoreJazzServer),
}

#[napi]
impl JazzServer {
    #[napi(factory, ts_return_type = "Promise<JazzServer>")]
    pub async fn start(
        #[napi(
            ts_arg_type = "{ appId: string; backendSecret: string; adminSecret: string; port?: number; dataDir?: string; inMemory?: boolean; jwksUrl?: string; allowLocalFirstAuth?: boolean; upstreamUrl?: string; telemetryCollectorUrl?: string; schema?: Buffer | Uint8Array | number[] }"
        )]
        options: JsonValue,
    ) -> napi::Result<Self> {
        let mut opts = parse_jazz_server_start_options(options)?;
        init_jazz_server_telemetry(opts.telemetry_collector_url.as_deref());

        let core_server_shell_schema = opts
            .schema
            .take()
            .map(|schema_bytes| {
                postcard::from_bytes::<JazzSchema>(&schema_bytes).map_err(|error| {
                    napi::Error::from_reason(format!("Invalid Jazz schema bytes: {error}"))
                })
            })
            .transpose()?;

        let app_id =
            AppId::from_string(&opts.app_id).unwrap_or_else(|_| AppId::from_name(&opts.app_id));

        let auth_config = AuthConfig {
            jwks_url: opts.jwks_url,
            allow_local_first_auth: opts.allow_local_first_auth.unwrap_or(true),
            backend_secret: Some(opts.backend_secret.clone()),
            admin_secret: Some(opts.admin_secret.clone()),
            ..Default::default()
        };

        let in_memory = opts.in_memory.unwrap_or(false);
        let data_dir = if in_memory {
            String::new()
        } else {
            opts.data_dir.unwrap_or_else(|| "./data".to_string())
        };

        let mut server_builder = ServerBuilder::new(app_id)
            .with_auth_config(auth_config)
            .with_native_transport_connector(std::sync::Arc::new(
                jazz_native_transport::NativeWebSocketConnector,
            ));
        if let Some(schema) = core_server_shell_schema {
            server_builder = server_builder.with_core_server_shell_schema(schema);
        }
        if let Some(upstream_url) = opts.upstream_url.clone() {
            server_builder = server_builder.with_upstream_url(upstream_url);
        }

        if in_memory {
            server_builder = server_builder.with_storage(StorageBackend::InMemory);
        } else {
            #[cfg(feature = "rocksdb")]
            {
                server_builder = server_builder.with_storage(StorageBackend::RocksDb {
                    path: data_dir.clone().into(),
                });
            }
            #[cfg(not(feature = "rocksdb"))]
            {
                return Err(napi::Error::from_reason(
                    "persistent JazzServer storage requires the rocksdb feature; use inMemory for ephemeral servers"
                        .to_string(),
                ));
            }
        }

        let built = server_builder
            .build()
            .await
            .map_err(napi::Error::from_reason)?;

        let data_dir_path = std::path::PathBuf::from(&data_dir);

        let server = CoreJazzServer::from_built(
            built,
            opts.port,
            app_id,
            ServerDataDir::from_path(data_dir_path),
            opts.admin_secret.clone(),
            opts.backend_secret.clone(),
        )
        .await;

        Ok(Self {
            inner: Mutex::new(Some(JazzServerInner::Core(server))),
        })
    }

    #[napi(getter, js_name = "appId")]
    pub fn app_id(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Core(server) => server.app_id().to_string(),
        })
    }

    #[napi(getter)]
    pub fn url(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Core(server) => server.base_url(),
        })
    }

    #[napi(getter)]
    pub fn port(&self) -> napi::Result<u16> {
        self.with_server(|server| match server {
            JazzServerInner::Core(server) => server.port(),
        })
    }

    #[napi(getter, js_name = "dataDir")]
    pub fn data_dir(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Core(server) => server.data_dir().to_string_lossy().into_owned(),
        })
    }

    #[napi(getter, js_name = "backendSecret")]
    pub fn backend_secret(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Core(server) => server.backend_secret().to_string(),
        })
    }

    #[napi(getter, js_name = "adminSecret")]
    pub fn admin_secret(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Core(server) => server.admin_secret().to_string(),
        })
    }

    #[napi]
    pub async fn stop(&self) -> napi::Result<()> {
        let server = self
            .inner
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?
            .take();

        if let Some(server) = server {
            match server {
                JazzServerInner::Core(server) => server.shutdown().await,
            }
        }

        Ok(())
    }

    fn with_server<T>(&self, f: impl FnOnce(&JazzServerInner) -> T) -> napi::Result<T> {
        let server = self
            .inner
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let server = server
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("JazzServer has been stopped"))?;
        Ok(f(server))
    }
}

// ============================================================================
// Module-level utility functions
// ============================================================================

// ============================================================================
// Identity crypto utilities
// ============================================================================

fn decode_seed_napi(seed_b64: &str) -> napi::Result<[u8; 32]> {
    let bytes = URL_SAFE_NO_PAD
        .decode(seed_b64)
        .map_err(|e| napi::Error::from_reason(format!("seed base64 decode error: {e}")))?;
    bytes
        .try_into()
        .map_err(|_| napi::Error::from_reason("seed must be exactly 32 bytes"))
}

#[napi(js_name = "mintLocalFirstToken")]
pub fn mint_local_first_token(
    seed_b64: String,
    audience: String,
    ttl_seconds: u32,
) -> napi::Result<String> {
    let seed = decode_seed_napi(&seed_b64)?;
    identity::mint_jazz_self_signed_token(
        &seed,
        identity::LOCAL_FIRST_ISSUER,
        &audience,
        ttl_seconds as u64,
    )
    .map_err(napi::Error::from_reason)
}

#[napi(object)]
pub struct VerifyTokenResult {
    pub ok: bool,
    pub id: String,
    pub error: Option<String>,
}

#[napi(js_name = "verifyLocalFirstIdentityProof")]
pub fn verify_local_first_identity_proof_napi(
    token: Option<String>,
    expected_audience: String,
) -> VerifyTokenResult {
    let token = match token {
        Some(t) if !t.is_empty() => t,
        _ => {
            return VerifyTokenResult {
                ok: false,
                id: String::new(),
                error: Some("proofToken is required".to_string()),
            };
        }
    };
    match identity::verify_jazz_self_signed_proof(&token, &expected_audience) {
        Ok(verified) => VerifyTokenResult {
            ok: true,
            id: verified.user_id,
            error: None,
        },
        Err(e) => VerifyTokenResult {
            ok: false,
            id: String::new(),
            error: Some(e),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet};
    use std::rc::Rc;

    use crate::{
        NapiDbInnerStorage, NapiTxKind, Tx, authority_epoch_from_bigint, core_block_on,
        core_claim_value_from_json, core_read_opts_from_json, core_subscription_event_to_napi,
        encode_core_subscription_delta, terminal_bytes_to_numbers,
    };
    use jazz::db::{
        Db as CoreDb, DbConfig as CoreDbConfig, DbIdentity as CoreDbIdentity, ExclusiveTxOps,
        MergeableTxOps, Propagation as CorePropagation, SubscriptionEvent as CoreSubscriptionEvent,
        TerminalRootCarrier, TerminalRootLayout, TerminalRootPublicField,
    };
    use jazz::groove::ivm::{TerminalEdit, TerminalOperation, TerminalPathSegment};
    use jazz::groove::records::Value as CoreValue;
    use jazz::groove::records::{RecordDescriptor, ValueType};
    use jazz::groove::schema::ColumnType as GrooveColumnType;
    use jazz::groove::storage::MemoryStorage as CoreMemoryStorage;
    use jazz::ids::{AuthorId as CoreAuthorId, NodeUuid as CoreNodeUuid, RowUuid as CoreRowUuid};
    use jazz::schema::{
        ColumnSchema as CoreColumnSchema, JazzSchema, Policy, TableSchema as CoreTableSchema,
    };
    use jazz::tools::OpenBatchId as CoreOpenBatchId;
    use jazz::tools::{ColumnType, Schema, SchemaBuilder, TableName, TableSchema, Value};
    use jazz::tx::DurabilityTier;
    use napi::bindgen_prelude::{BigInt, Either, Either3, Either4};
    use serde_json::json;

    #[test]
    fn javascript_numeric_claims_preserve_safe_integers_and_fail_closed_when_lossy() {
        assert_eq!(
            core_claim_value_from_json(json!(7)).unwrap(),
            CoreValue::U64(7)
        );
        assert_eq!(
            core_claim_value_from_json(json!(-7)).unwrap(),
            CoreValue::I64(-7)
        );
        assert_eq!(
            core_claim_value_from_json(serde_json::Value::Number(
                serde_json::Number::from_f64(7.0).unwrap()
            ))
            .unwrap(),
            CoreValue::U64(7),
            "JS-number deserialization must agree with integer JSON"
        );
        assert_eq!(
            core_claim_value_from_json(json!(7.5)).unwrap(),
            CoreValue::F64(7.5)
        );
        assert_eq!(
            core_claim_value_from_json(json!(9_007_199_254_740_992_u64)).unwrap(),
            CoreValue::F64(9_007_199_254_740_992.0),
            "integers beyond Number.MAX_SAFE_INTEGER must not participate in integer policy matching"
        );
        assert_eq!(
            core_claim_value_from_json(json!(-9_007_199_254_740_992_i64)).unwrap(),
            CoreValue::F64(-9_007_199_254_740_992.0)
        );
    }

    #[test]
    fn authority_epoch_bigint_rejects_lossy_values_and_preserves_u64() {
        let above_u32 = BigInt {
            sign_bit: false,
            words: vec![u32::MAX as u64 + 1],
        };
        let near_safe_integer = BigInt {
            sign_bit: false,
            words: vec![9_007_199_254_740_993],
        };
        let maximum_u64 = BigInt {
            sign_bit: false,
            words: vec![u64::MAX],
        };
        assert_eq!(
            authority_epoch_from_bigint(above_u32, "authority").unwrap(),
            u32::MAX as u64 + 1
        );
        assert_eq!(
            authority_epoch_from_bigint(near_safe_integer, "authority").unwrap(),
            9_007_199_254_740_993
        );
        assert_eq!(
            authority_epoch_from_bigint(maximum_u64, "authority").unwrap(),
            u64::MAX
        );
        assert!(
            authority_epoch_from_bigint(
                BigInt {
                    sign_bit: true,
                    words: vec![1],
                },
                "authority",
            )
            .is_err()
        );
        assert!(
            authority_epoch_from_bigint(
                BigInt {
                    sign_bit: false,
                    words: vec![0, 1],
                },
                "authority",
            )
            .is_err()
        );
    }

    #[test]
    fn native_delta_preserves_typed_union_occurrence_keys() {
        #[derive(serde::Deserialize)]
        struct DecodedRemoved {
            #[allow(dead_code)]
            table: String,
            #[allow(dead_code)]
            row_id: CoreRowUuid,
        }
        #[derive(serde::Deserialize)]
        struct DecodedDelta {
            added: Vec<serde::de::IgnoredAny>,
            updated: Vec<serde::de::IgnoredAny>,
            removed: Vec<DecodedRemoved>,
            added_occurrence_keys: Vec<jazz::tools::ResultKey>,
            updated_occurrence_keys: Vec<jazz::tools::ResultKey>,
            removed_occurrence_keys: Vec<jazz::tools::ResultKey>,
        }
        let root = jazz::tools::ObjectId::from_uuid(uuid::Uuid::from_bytes([1; 16]));
        let joined = jazz::tools::ObjectId::from_uuid(uuid::Uuid::from_bytes([2; 16]));
        let occurrence = |label: &str| {
            jazz::tools::ResultKey::from_union_occurrence(root, [joined], [(0, label.to_owned())])
                .unwrap()
        };
        let removed = ["direct", "inherited"].map(|label| {
            jazz::db::RemovedRow::from_result_key(
                "todos".to_owned(),
                CoreRowUuid::from_bytes([1; 16]),
                occurrence(label),
            )
        });
        let bytes = encode_core_subscription_delta(&[], &[], &removed).unwrap();
        let decoded: DecodedDelta = postcard::from_bytes(&bytes).unwrap();
        assert!(decoded.added.is_empty() && decoded.updated.is_empty());
        assert_eq!(decoded.removed.len(), 2);
        assert!(decoded.added_occurrence_keys.is_empty());
        assert!(decoded.updated_occurrence_keys.is_empty());
        assert_ne!(
            decoded.removed_occurrence_keys[0],
            decoded.removed_occurrence_keys[1]
        );
    }

    #[test]
    fn schema_json_roundtrip_preserves_enum_fk_and_defaults() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("files").column("name", ColumnType::Text))
            .table(
                TableSchema::builder("todos")
                    .column_with_default("done", ColumnType::Boolean, Value::Boolean(false))
                    .column(
                        "status",
                        ColumnType::Enum {
                            variants: vec!["done".to_string(), "todo".to_string()],
                        },
                    )
                    .fk_column("image", "files"),
            )
            .build();

        let encoded = serde_json::to_string(&schema).expect("serialize schema");
        let decoded: Schema = serde_json::from_str(&encoded).expect("deserialize schema");

        let status = decoded
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .column("status")
            .unwrap();
        assert_eq!(
            status.column_type,
            ColumnType::Enum {
                variants: vec!["done".to_string(), "todo".to_string()]
            }
        );

        let image = decoded
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .column("image")
            .unwrap();
        assert_eq!(image.references, Some(TableName::new("files")));

        let done = decoded
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .column("done")
            .unwrap();
        assert_eq!(done.default, Some(Value::Boolean(false)));
    }

    #[test]
    fn core_read_opts_accept_public_local_only_spelling() {
        let opts = core_read_opts_from_json(Some(json!({ "propagation": "local-only" })))
            .expect("parse read opts");

        assert_eq!(opts.propagation, CorePropagation::LocalOnly);
    }

    #[test]
    fn subscription_payload_exposes_only_terminal_rows() {
        let payload = core_subscription_event_to_napi(
            &CoreSubscriptionEvent::Delta {
                reset: false,
                publishable: true,
                added: Vec::new(),
                updated: Vec::new(),
                removed: Vec::new(),
                terminal_operations: Vec::new(),
                terminal_layout: None,
                settled: true,
                tier: DurabilityTier::Local,
            },
            &mut HashSet::new(),
        )
        .expect("encode terminal delta");

        let Either3::A(payload) = payload else {
            panic!("expected delta payload");
        };
        assert!(!payload.delta.is_empty());
        assert!(payload.terminal_operations.is_empty());
        assert_eq!(payload.tier, "Local");
    }

    #[test]
    fn subscription_payload_preserves_typed_terminal_operations_and_descriptor() {
        let descriptor = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            (
                "user_title",
                ValueType::Nullable(Box::new(ValueType::String)),
            ),
        ]);
        let expected_descriptor = postcard::to_allocvec(&descriptor).unwrap();
        let operations = vec![
            TerminalOperation {
                root_descriptor: descriptor,
                root_key: vec![0, 255],
                path: vec![
                    TerminalPathSegment::Collection("children".to_owned()),
                    TerminalPathSegment::Key(vec![1, 254]),
                ],
                edit: TerminalEdit::Insert {
                    index: 3,
                    key: vec![2, 253],
                    value: (0_u8..=u8::MAX).collect(),
                },
            },
            TerminalOperation {
                root_descriptor: descriptor,
                root_key: vec![4],
                path: Vec::new(),
                edit: TerminalEdit::Update {
                    key: vec![5],
                    value: vec![6],
                },
            },
            TerminalOperation {
                root_descriptor: descriptor,
                root_key: vec![7],
                path: Vec::new(),
                edit: TerminalEdit::Remove { key: vec![8] },
            },
            TerminalOperation {
                root_descriptor: descriptor,
                root_key: vec![9],
                path: Vec::new(),
                edit: TerminalEdit::Move {
                    key: vec![10],
                    index: 11,
                },
            },
        ];
        let terminal_layout = TerminalRootLayout {
            id: "test-terminal-layout".to_owned(),
            root_descriptor: descriptor,
            root_key_slot: 0,
            root_key_field_name: "row_uuid".to_owned(),
            public_fields: vec![TerminalRootPublicField {
                name: "title".to_owned(),
                descriptor_field_name: "user_title".to_owned(),
                slot: 1,
                carrier: TerminalRootCarrier::CurrentRow,
            }],
            carrier: TerminalRootCarrier::CurrentRow,
        };
        let payload = core_subscription_event_to_napi(
            &CoreSubscriptionEvent::Delta {
                reset: false,
                publishable: true,
                added: Vec::new(),
                updated: Vec::new(),
                removed: Vec::new(),
                terminal_operations: operations,
                terminal_layout: Some(terminal_layout),
                settled: false,
                tier: DurabilityTier::Edge,
            },
            &mut HashSet::new(),
        )
        .expect("encode terminal operations");

        let Either3::A(payload) = payload else {
            panic!("expected delta payload");
        };
        assert_eq!(payload.tier, "Edge");
        assert_eq!(payload.terminal_operations.len(), 4);
        let insert = &payload.terminal_operations[0];
        assert_eq!(insert.root_layout_id, "test-terminal-layout");
        assert_eq!(
            payload.terminal_layouts[0].root_descriptor,
            terminal_bytes_to_numbers(&expected_descriptor)
        );
        assert_eq!(insert.root_key, vec![0, 255]);
        assert!(matches!(
            insert.path.as_slice(),
            [Either::A(collection), Either::B(key)]
                if collection.collection == "children" && key.key == vec![1, 254]
        ));
        assert!(matches!(
            &insert.edit,
            Either4::A(edit)
                if edit.insert.index == 3.0
                    && edit.insert.key == vec![2, 253]
                    && edit.insert.value == (0_u32..=u8::MAX.into()).collect::<Vec<_>>()
        ));
        assert!(matches!(
            &payload.terminal_operations[1].edit,
            Either4::B(edit) if edit.update.key == vec![5] && edit.update.value == vec![6]
        ));
        assert!(matches!(
            &payload.terminal_operations[2].edit,
            Either4::C(edit) if edit.remove.key == vec![8]
        ));
        assert!(matches!(
            &payload.terminal_operations[3].edit,
            Either4::D(edit) if edit.move_edit.key == vec![10] && edit.move_edit.index == 11.0
        ));
    }

    #[test]
    fn subscription_payload_preserves_rejection_and_closed_variants() {
        use jazz::protocol::{SubscribeRejectReason, SubscribeServerFailureCode};

        let unsupported = core_subscription_event_to_napi(
            &CoreSubscriptionEvent::Rejected {
                reason: SubscribeRejectReason::UnsupportedShapeCapability {
                    detail: "unsupported maintained shape".to_owned(),
                },
            },
            &mut HashSet::new(),
        )
        .expect("encode unsupported rejection");
        assert!(matches!(
            unsupported,
            Either3::B(crate::SubscriptionRejectedEvent {
                event_type,
                reason: Either3::A(crate::SubscriptionUnsupportedShapeCapabilityReason {
                    reason_type,
                    detail,
                }),
            }) if event_type == "rejected"
                && reason_type == "UnsupportedShapeCapability"
                && detail == "unsupported maintained shape"
        ));

        let pending = core_subscription_event_to_napi(
            &CoreSubscriptionEvent::Rejected {
                reason: SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission,
            },
            &mut HashSet::new(),
        )
        .expect("encode pending rejection");
        assert!(matches!(
            pending,
            Either3::B(crate::SubscriptionRejectedEvent {
                event_type,
                reason: Either3::B(crate::SubscriptionShapeRegistrationPendingReason {
                    reason_type,
                }),
            }) if event_type == "rejected"
                && reason_type == "ShapeRegistrationPendingCatalogueAdmission"
        ));

        let server_failure = core_subscription_event_to_napi(
            &CoreSubscriptionEvent::Rejected {
                reason: SubscribeRejectReason::ServerFailure {
                    code: SubscribeServerFailureCode::QueryValidation,
                },
            },
            &mut HashSet::new(),
        )
        .expect("encode server rejection");
        assert!(matches!(
            server_failure,
            Either3::B(crate::SubscriptionRejectedEvent {
                event_type,
                reason: Either3::C(crate::SubscriptionServerFailureReason {
                    reason_type,
                    code,
                }),
            }) if event_type == "rejected"
                && reason_type == "ServerFailure"
                && code == "QueryValidation"
        ));

        let closed =
            core_subscription_event_to_napi(&CoreSubscriptionEvent::Closed, &mut HashSet::new())
                .expect("encode closed event");
        assert!(matches!(
            closed,
            Either3::C(crate::SubscriptionClosedEvent { event_type }) if event_type == "closed"
        ));
    }

    #[test]
    #[cfg(debug_assertions)]
    fn debug_subscription_event_fixture_covers_rejection_and_closed_variants() {
        let events = crate::test_fixture_export::subscription_events()
            .expect("encode debug subscription event fixture");

        assert_eq!(events.len(), 4);
        assert!(matches!(events[0], Either3::B(_)));
        assert!(matches!(events[1], Either3::B(_)));
        assert!(matches!(events[2], Either3::B(_)));
        assert!(matches!(events[3], Either3::C(_)));
    }
    /// A short-lived NAPI schema attachment must not own or abandon the
    /// owner-wide OpenBatch lifetime when its JS wrapper is collected.
    #[test]
    fn attached_tx_drop_preserves_owner_batch() {
        let schema = JazzSchema::new([CoreTableSchema::new(
            "items",
            [CoreColumnSchema::new("label", GrooveColumnType::String)],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public())]);
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let owner = Rc::new(
            core_block_on(CoreDb::open(CoreDbConfig::new(
                schema.clone(),
                CoreMemoryStorage::new(&refs),
                CoreDbIdentity {
                    node: CoreNodeUuid::from_bytes([0x44; 16]),
                    author: CoreAuthorId::from_bytes([0xa4; 16]),
                },
            )))
            .unwrap(),
        );
        let view = Rc::new(owner.register_schema_view(schema).unwrap());
        let batch = CoreOpenBatchId::new();
        owner.begin_mergeable(batch).unwrap();
        drop(Tx {
            db: NapiDbInnerStorage::Memory(Rc::clone(&view)),
            kind: NapiTxKind::Mergeable,
            open_tx: Some(batch),
            owns_lifetime: false,
        });
        view.mergeable_tx_ref(batch)
            .insert_with_id(
                "items",
                CoreRowUuid::from_bytes([1; 16]),
                BTreeMap::from([("label".to_owned(), CoreValue::String("kept".to_owned()))]),
            )
            .unwrap();
        owner.commit_mergeable_handle(batch).unwrap();

        let exclusive = CoreOpenBatchId::new();
        owner.begin_exclusive(exclusive).unwrap();
        drop(Tx {
            db: NapiDbInnerStorage::Memory(Rc::clone(&view)),
            kind: NapiTxKind::Exclusive,
            open_tx: Some(exclusive),
            owns_lifetime: false,
        });
        view.exclusive_tx_ref(exclusive)
            .insert_with_id(
                "items",
                CoreRowUuid::from_bytes([2; 16]),
                BTreeMap::from([(
                    "label".to_owned(),
                    CoreValue::String("exclusive-kept".to_owned()),
                )]),
            )
            .unwrap();
        owner.commit_exclusive_handle(exclusive).unwrap();
    }
}
