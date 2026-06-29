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
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jazz::db::{
    Db as CoreDb, DbConfig as CoreDbConfig, DbIdentity as CoreDbIdentity,
    LocalUpdates as CoreLocalUpdates, PeerConnection as CorePeerConnection,
    PreparedQuery as PreparedQueryInner, Propagation as CorePropagation,
    QueryAttachment as CoreQueryAttachment, ReadOpts as CoreReadOpts, RowCells as CoreRowCells,
    SeededRowIdSource as CoreSeededRowIdSource, SubscriptionEvent, SubscriptionStream,
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
use jazz::query::Query as CoreQuery;
use jazz::schema::JazzSchema;
use jazz::tx::{DurabilityTier as CoreDurabilityTier, Fate as CoreFate, TxId};
use jazz::wire::{TransportError, WireTransport as CoreWireTransport};
use jazz_tools::AppId;
use jazz_tools::identity;
use jazz_tools::middleware::AuthConfig;
use jazz_tools::server::{
    JazzServer as CoreJazzServer, ServerBuilder, ServerDataDir, StorageBackend,
    TestJwtIssuer as JazzTestJwtIssuer, TestJwtOptions,
};

#[derive(Clone, Debug, Deserialize)]
struct CoreOpenDbConfig {
    identity: CoreOpenDbIdentity,
    row_id_seed: Option<u64>,
    history_complete: bool,
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
struct CoreRowBatch<'a> {
    table: &'a str,
    descriptor: RecordDescriptor,
    rows: Vec<CoreRow<'a>>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct CoreRow<'a> {
    row_id: CoreRowUuid,
    deleted: bool,
    raw: &'a [u8],
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

enum NapiTxWrite {
    Insert {
        table: String,
        row_id: CoreRowUuid,
        cells: CoreRowCells,
    },
    Update {
        table: String,
        row_id: CoreRowUuid,
        patch: CoreRowCells,
    },
    Upsert {
        table: String,
        row_id: CoreRowUuid,
        cells: CoreRowCells,
    },
    Delete {
        table: String,
        row_id: CoreRowUuid,
    },
    Restore {
        table: String,
        row_id: CoreRowUuid,
        cells: CoreRowCells,
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
}

enum NapiTransportInner {
    Memory {
        db: Rc<CoreDb<CoreMemoryStorage>>,
        connection: Option<Rc<RefCell<CorePeerConnection<CoreMemoryStorage>>>>,
    },
    Persistent {
        db: Rc<CoreDb<CoreRocksDbStorage>>,
        connection: Option<Rc<RefCell<CorePeerConnection<CoreRocksDbStorage>>>>,
    },
}

enum NapiSubscription {
    Memory(SubscriptionStream),
    Persistent(SubscriptionStream),
}

#[napi(js_name = "Tx")]
pub struct Tx {
    db: NapiDbInnerStorage,
    identity: Option<CoreAuthorId>,
    writes: Option<Vec<NapiTxWrite>>,
}

#[napi]
impl Write {
    #[napi(getter)]
    pub fn payload(&self) -> Uint8Array {
        Uint8Array::new(self.payload.clone())
    }

    #[napi]
    pub fn wait(&self, tier: String) -> napi::Result<()> {
        let tier = core_durability_tier_from_str(&tier)?;
        if let Some(write) = &self.inner {
            match write {
                NapiWrite::Memory { db, tx_id } => core_wait_for_tx(db, *tx_id, tier)?,
                NapiWrite::Persistent { db, tx_id } => core_wait_for_tx(db, *tx_id, tier)?,
            }
        }
        Ok(())
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

    #[napi(js_name = "nextWriteStateChange")]
    pub fn next_write_state_change(&self, env: Env) -> napi::Result<PromiseRaw<'static, ()>> {
        let Some(write) = &self.inner else {
            return Err(napi::Error::from_reason("write state is unavailable"));
        };
        let mut deferred = std::ptr::null_mut();
        let mut promise = std::ptr::null_mut();
        let env = env.raw();
        let status = unsafe { sys::napi_create_promise(env, &mut deferred, &mut promise) };
        if status != sys::Status::napi_ok {
            return Err(napi::Error::from_reason(
                "failed to create write-state promise",
            ));
        }
        match write {
            NapiWrite::Memory { db, tx_id } => {
                db.on_next_write_state_change(*tx_id, move || {
                    resolve_raw_promise(env, deferred);
                });
            }
            NapiWrite::Persistent { db, tx_id } => {
                db.on_next_write_state_change(*tx_id, move || {
                    resolve_raw_promise(env, deferred);
                });
            }
        }
        Ok(PromiseRaw::new(env, promise))
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
        }
    }

    #[napi]
    pub fn close(&mut self) -> bool {
        match &mut self.inner {
            NapiTransportInner::Memory { db, connection } => {
                let Some(connection) = connection.take() else {
                    return false;
                };
                db.detach_connection(&connection)
            }
            NapiTransportInner::Persistent { db, connection } => {
                let Some(connection) = connection.take() else {
                    return false;
                };
                db.detach_connection(&connection)
            }
        }
    }
}

#[napi]
impl Subscription {
    #[napi(js_name = "readAll")]
    pub fn read_all(&mut self) -> napi::Result<Vec<serde_json::Value>> {
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
            events.push(core_subscription_event_to_json(&event)?);
        }
        Ok(events)
    }

    #[napi]
    pub fn drain(&mut self) -> napi::Result<Vec<serde_json::Value>> {
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
    ) -> napi::Result<()> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        self.pending_writes()?.push(NapiTxWrite::Insert {
            table,
            row_id,
            cells,
        });
        Ok(())
    }

    #[napi(js_name = "updateEncoded")]
    pub fn update_encoded(
        &mut self,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
    ) -> napi::Result<()> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let patch = decode_core_cells(&patch)?;
        self.pending_writes()?.push(NapiTxWrite::Update {
            table,
            row_id,
            patch,
        });
        Ok(())
    }

    #[napi(js_name = "upsertEncoded")]
    pub fn upsert_encoded(
        &mut self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
    ) -> napi::Result<()> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        self.pending_writes()?.push(NapiTxWrite::Upsert {
            table,
            row_id,
            cells,
        });
        Ok(())
    }

    #[napi(js_name = "delete")]
    pub fn delete_encoded(&mut self, table: String, row_id: Uint8Array) -> napi::Result<()> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        self.pending_writes()?
            .push(NapiTxWrite::Delete { table, row_id });
        Ok(())
    }

    #[napi(js_name = "restoreEncoded")]
    pub fn restore_encoded(
        &mut self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
    ) -> napi::Result<()> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        self.pending_writes()?.push(NapiTxWrite::Restore {
            table,
            row_id,
            cells,
        });
        Ok(())
    }

    #[napi]
    pub fn commit(&mut self) -> napi::Result<Write> {
        let writes = self
            .writes
            .take()
            .ok_or_else(|| napi::Error::from_reason("transaction is already closed"))?;
        match &self.db {
            NapiDbInnerStorage::Memory(db) => core_commit_tx_memory(db, self.identity, writes),
            NapiDbInnerStorage::Persistent(db) => {
                core_commit_tx_persistent(db, self.identity, writes)
            }
        }
    }

    #[napi]
    pub fn rollback(&mut self) -> napi::Result<()> {
        self.writes
            .take()
            .ok_or_else(|| napi::Error::from_reason("transaction is already closed"))?;
        Ok(())
    }
}

impl Tx {
    fn pending_writes(&mut self) -> napi::Result<&mut Vec<NapiTxWrite>> {
        self.writes
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("transaction is already closed"))
    }
}

#[napi(js_name = "NapiDb")]
pub struct NapiDb {
    inner: NapiDbInner,
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
        })
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
        };
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
        Ok(Subscription { inner: Some(inner) })
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
        Ok(Subscription { inner: Some(inner) })
    }

    #[napi(js_name = "insertWithIdEncoded")]
    pub fn insert_with_id_encoded(
        &self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                db.insert_with_id(&table, row_id, cells)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                db.insert_with_id(&table, row_id, cells)
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
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let author = core_author_id_from_bytes(&author)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                db.insert_with_id_for_identity(author, &table, row_id, cells)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                db.insert_with_id_for_identity(author, &table, row_id, cells)
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
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let patch = decode_core_cells(&patch)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                db.update(&table, row_id, patch)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                db.update(&table, row_id, patch)
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
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let patch = decode_core_cells(&patch)?;
        let author = core_author_id_from_bytes(&author)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                db.update_for_identity(author, &table, row_id, patch)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                db.update_for_identity(author, &table, row_id, patch)
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
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                db.upsert(&table, row_id, cells)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                db.upsert(&table, row_id, cells)
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
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let author = core_author_id_from_bytes(&author)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                db.upsert_for_identity(author, &table, row_id, cells)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                db.upsert_for_identity(author, &table, row_id, cells)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        }
    }

    #[napi(js_name = "delete")]
    pub fn delete_encoded(&self, table: String, row_id: Uint8Array) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                db.delete(&table, row_id)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                db.delete(&table, row_id)
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
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let author = core_author_id_from_bytes(&author)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                db.delete_for_identity(author, &table, row_id)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                db.delete_for_identity(author, &table, row_id)
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
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                db.restore(&table, row_id, cells)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                db.restore(&table, row_id, cells)
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
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let author = core_author_id_from_bytes(&author)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                db.restore_for_identity(author, &table, row_id, cells)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                db.restore_for_identity(author, &table, row_id, cells)
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

    #[napi(js_name = "connectUpstream")]
    pub fn connect_upstream(&self) -> napi::Result<Transport> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let queues = WireQueues::default();
        let transport = Box::new(CoreWireTransportAdapter::current(NapiWireTransport {
            queues: queues.clone(),
        }));
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
    pub fn mergeable_tx(&self) -> napi::Result<Tx> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        Ok(Tx {
            db: match db {
                NapiDbInnerStorage::Memory(db) => NapiDbInnerStorage::Memory(Rc::clone(db)),
                NapiDbInnerStorage::Persistent(db) => NapiDbInnerStorage::Persistent(Rc::clone(db)),
            },
            identity: None,
            writes: Some(Vec::new()),
        })
    }

    #[napi(js_name = "mergeableTxForIdentity")]
    pub fn mergeable_tx_for_identity(&self, author: Uint8Array) -> napi::Result<Tx> {
        let author = core_author_id_from_bytes(&author)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        Ok(Tx {
            db: match db {
                NapiDbInnerStorage::Memory(db) => NapiDbInnerStorage::Memory(Rc::clone(db)),
                NapiDbInnerStorage::Persistent(db) => NapiDbInnerStorage::Persistent(Rc::clone(db)),
            },
            identity: Some(author),
            writes: Some(Vec::new()),
        })
    }

    #[napi]
    pub fn close(&self) {
        self.inner.borrow_mut().take();
    }
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
    if config.history_complete {
        core_block_on(CoreDb::open_history_complete(db_config))
    } else {
        core_block_on(CoreDb::open(db_config))
    }
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
        JsonValue::Number(value) => {
            if let Some(value) = value.as_u64() {
                CoreValue::U64(value)
            } else if let Some(value) = value.as_f64() {
                CoreValue::F64(value)
            } else {
                return Err(napi::Error::from_reason("unsupported numeric claim value"));
            }
        }
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

fn core_wait_for_tx<S>(db: &CoreDb<S>, tx_id: TxId, tier: CoreDurabilityTier) -> napi::Result<()>
where
    S: CoreOrderedKvStorage + CoreReopenableStorage + 'static,
{
    if tier <= CoreDurabilityTier::Local {
        return Ok(());
    }
    let state = db
        .write_state(tx_id)
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    match state.fate {
        CoreFate::Rejected(reason) => {
            return Err(napi::Error::from_reason(format!(
                "transaction was rejected: {reason:?}"
            )));
        }
        CoreFate::Pending if tier >= CoreDurabilityTier::Edge => {
            return Err(napi::Error::from_reason(format!(
                "transaction has not been accepted at requested tier {tier:?}"
            )));
        }
        CoreFate::Pending | CoreFate::Accepted => {}
    }
    if state.durability >= tier {
        return Ok(());
    }
    Err(napi::Error::from_reason(format!(
        "transaction has not reached requested tier {tier:?}"
    )))
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

fn core_commit_tx<S>(
    db: &CoreDb<S>,
    identity: Option<CoreAuthorId>,
    writes: Vec<NapiTxWrite>,
) -> napi::Result<TxId>
where
    S: CoreOrderedKvStorage + CoreReopenableStorage + 'static,
{
    let mut tx = if let Some(identity) = identity {
        db.mergeable_tx_for_identity(identity)
    } else {
        db.mergeable_tx()
    };
    for write in writes {
        match write {
            NapiTxWrite::Insert {
                table,
                row_id,
                cells,
            } => tx
                .insert_with_id(&table, row_id, cells)
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            NapiTxWrite::Update {
                table,
                row_id,
                patch,
            } => tx
                .update(&table, row_id, patch)
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            NapiTxWrite::Upsert {
                table,
                row_id,
                cells,
            } => tx
                .update(&table, row_id, cells)
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            NapiTxWrite::Delete { table, row_id } => tx
                .delete(&table, row_id)
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            NapiTxWrite::Restore {
                table,
                row_id,
                cells,
            } => tx
                .restore(&table, row_id, cells)
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
        }
    }
    tx.commit()
        .map_err(|error| napi::Error::from_reason(error.to_string()))
}

fn core_commit_tx_memory(
    db: &Rc<CoreDb<CoreMemoryStorage>>,
    identity: Option<CoreAuthorId>,
    writes: Vec<NapiTxWrite>,
) -> napi::Result<Write> {
    let tx_id = core_commit_tx(db, identity, writes)?;
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
    identity: Option<CoreAuthorId>,
    writes: Vec<NapiTxWrite>,
) -> napi::Result<Write> {
    let tx_id = core_commit_tx(db, identity, writes)?;
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
    postcard::to_allocvec(&core_row_batches(rows))
}

fn core_row_batches(rows: &[jazz::node::CurrentRow]) -> Vec<CoreRowBatch<'_>> {
    let mut batches: Vec<CoreRowBatch<'_>> = Vec::new();
    for row in rows {
        let (descriptor, raw) = row.encoded_record();
        match batches.last_mut() {
            Some(batch) if batch.table == row.table() && batch.descriptor == *descriptor => {
                batch.rows.push(core_row(row, raw));
            }
            _ => batches.push(CoreRowBatch {
                table: row.table(),
                descriptor: *descriptor,
                rows: vec![core_row(row, raw)],
            }),
        }
    }
    batches
}

fn core_row<'a>(row: &jazz::node::CurrentRow, raw: &'a [u8]) -> CoreRow<'a> {
    CoreRow {
        row_id: row.row_uuid(),
        deleted: row.is_deleted(),
        raw,
    }
}

fn core_subscription_event_to_json(event: &SubscriptionEvent) -> napi::Result<serde_json::Value> {
    match event {
        SubscriptionEvent::Opened {
            current,
            settled,
            tier,
        }
        | SubscriptionEvent::Reset {
            current,
            settled,
            tier,
        } => {
            let rows = encode_core_rows(current)
                .map_err(|error| napi::Error::from_reason(error.to_string()))?;
            Ok(serde_json::json!({
                "type": "snapshot",
                "rows": rows,
                "settled": settled,
                "tier": format!("{tier:?}"),
            }))
        }
        SubscriptionEvent::Delta {
            current,
            settled,
            tier,
            ..
        } => {
            let rows = encode_core_rows(current)
                .map_err(|error| napi::Error::from_reason(error.to_string()))?;
            Ok(serde_json::json!({
                "type": "snapshot",
                "rows": rows,
                "settled": settled,
                "tier": format!("{tier:?}"),
            }))
        }
        SubscriptionEvent::Closed => Ok(serde_json::json!({ "type": "closed" })),
    }
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

static JAZZ_SERVER_OTEL_PROVIDER: OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    OnceLock::new();
static JAZZ_SERVER_TELEMETRY_INIT: OnceLock<()> = OnceLock::new();

fn init_jazz_server_telemetry(collector_url: Option<&str>) {
    let Some(collector_url) = collector_url else {
        return;
    };

    JAZZ_SERVER_TELEMETRY_INIT.get_or_init(|| {
        use tracing_subscriber::layer::SubscriberExt as _;

        let endpoint = jazz_tools::otel::normalize_otlp_traces_endpoint(collector_url);
        let provider =
            jazz_tools::otel::init_tracer_provider_with_endpoint("jazz-server", Some(&endpoint));
        let otel_layer = jazz_tools::otel::layer(&provider);
        let filter = tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("jazz_tools=trace".parse().expect("valid tracing directive"))
            .add_directive("tower_http=debug".parse().expect("valid tracing directive"));

        if tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(filter).with(otel_layer),
        )
        .is_ok()
        {
            let _ = JAZZ_SERVER_OTEL_PROVIDER.set(provider);
        }
    });
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

        let mut server_builder = ServerBuilder::new(app_id).with_auth_config(auth_config);
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
    use crate::core_read_opts_from_json;
    use jazz::db::Propagation as CorePropagation;
    use jazz_tools::{ColumnType, Schema, SchemaBuilder, TableName, TableSchema, Value};
    use serde_json::json;

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
}
