//! jazz-napi — Native Node.js bindings for Jazz.
//!
//! Provides Node.js bindings for the Jazz core database, server helpers, and
//! local-first identity utilities.
//!
//! # Architecture
//!
//! - `NapiDirectDb` exposes the vendored Jazz core DB directly over an
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
    Db as DirectDb, DbConfig as DirectDbConfig, DbIdentity as DirectDbIdentity,
    LocalUpdates as DirectLocalUpdates, PeerConnection as DirectPeerConnection,
    PreparedQuery as DirectPreparedQueryInner, Propagation as DirectPropagation,
    ReadOpts as DirectReadOpts, RemovedRow as DirectRemovedRowInner, RowCells as DirectRowCells,
    SeededRowIdSource as DirectSeededRowIdSource, SubscriptionEvent as DirectSubscriptionEvent,
    SubscriptionStream as DirectSubscriptionStream,
    WireTransportAdapter as DirectWireTransportAdapter, WriteHandle as DirectWriteHandle,
    block_on as direct_block_on,
};
use jazz::groove::records::{
    BorrowedRecord as DirectBorrowedRecord, RecordDescriptor, Value as DirectValue,
};
use jazz::groove::storage::{
    MemoryStorage as DirectMemoryStorage, OrderedKvStorage as DirectOrderedKvStorage,
    ReopenableStorage as DirectReopenableStorage, RocksDbStorage as DirectRocksDbStorage,
};
use jazz::ids::{AuthorId as DirectAuthorId, NodeUuid as DirectNodeUuid, RowUuid as DirectRowUuid};
use jazz::query::Query as DirectQuery;
use jazz::schema::JazzSchema;
use jazz::tx::{DurabilityTier as DirectDurabilityTier, Fate as DirectFate, TxId as DirectTxId};
use jazz::wire::{TransportError as DirectTransportError, WireTransport as DirectWireTransport};
use jazz_tools::AppId;
use jazz_tools::identity;
use jazz_tools::middleware::AuthConfig;
use jazz_tools::server::{
    JazzServer as CoreJazzServer, ServerBuilder, ServerDataDir, StorageBackend,
    TestJwtIssuer as JazzTestJwtIssuer, TestJwtOptions,
};

#[derive(Clone, Debug, Deserialize)]
struct DirectOpenDbConfig {
    identity: DirectOpenDbIdentity,
    row_id_seed: Option<u64>,
    history_complete: bool,
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct DirectOpenDbIdentity {
    node: DirectNodeUuid,
    author: DirectAuthorId,
}

impl From<DirectOpenDbIdentity> for DirectDbIdentity {
    fn from(identity: DirectOpenDbIdentity) -> Self {
        Self {
            node: identity.node,
            author: identity.author,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize)]
struct DirectRowBatch<'a> {
    table: &'a str,
    descriptor: RecordDescriptor,
    rows: Vec<DirectRow<'a>>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct DirectRow<'a> {
    row_id: DirectRowUuid,
    deleted: bool,
    raw: &'a [u8],
}

#[derive(Clone, Debug, serde::Serialize)]
struct DirectWriteResult {
    row_id: DirectRowUuid,
    tx_id: DirectTxId,
}

#[derive(Clone, Debug, serde::Serialize)]
struct DirectRemovedRow<'a> {
    table: &'a str,
    row_id: DirectRowUuid,
}

#[derive(Clone, Debug, serde::Serialize)]
struct DirectSubscriptionDelta<'a> {
    added: Vec<DirectRowBatch<'a>>,
    updated: Vec<DirectRowBatch<'a>>,
    removed: Vec<DirectRemovedRow<'a>>,
}

type DirectNapiDbInner = Rc<RefCell<Option<DirectNapiDb>>>;

enum DirectNapiDb {
    Memory(Rc<DirectDb<DirectMemoryStorage>>),
    Persistent(Rc<DirectDb<DirectRocksDbStorage>>),
}

enum DirectNapiWrite {
    Memory(DirectWriteHandle<DirectMemoryStorage>),
    Persistent(DirectWriteHandle<DirectRocksDbStorage>),
    MemoryTx {
        db: Rc<DirectDb<DirectMemoryStorage>>,
        tx_id: DirectTxId,
    },
    PersistentTx {
        db: Rc<DirectDb<DirectRocksDbStorage>>,
        tx_id: DirectTxId,
    },
}

enum DirectNapiTxWrite {
    Insert {
        table: String,
        row_id: DirectRowUuid,
        cells: DirectRowCells,
    },
    Update {
        table: String,
        row_id: DirectRowUuid,
        patch: DirectRowCells,
    },
    Upsert {
        table: String,
        row_id: DirectRowUuid,
        cells: DirectRowCells,
    },
    Delete {
        table: String,
        row_id: DirectRowUuid,
    },
    Restore {
        table: String,
        row_id: DirectRowUuid,
        cells: DirectRowCells,
    },
}

#[derive(Clone, Default)]
struct DirectWireQueues {
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
}

struct NapiWireTransport {
    queues: DirectWireQueues,
}

impl DirectWireTransport for NapiWireTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> std::result::Result<(), DirectTransportError> {
        self.queues.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.queues.inbound.borrow_mut().pop_front()
    }
}

#[napi(js_name = "DirectPreparedQuery")]
pub struct NapiDirectPreparedQuery {
    inner: DirectPreparedQueryInner,
}

#[napi(js_name = "DirectWrite")]
pub struct NapiDirectWrite {
    payload: Vec<u8>,
    inner: Option<DirectNapiWrite>,
}

#[napi(js_name = "DirectTransport")]
pub struct NapiDirectTransport {
    inner: DirectNapiTransportInner,
    queues: DirectWireQueues,
}

#[napi(js_name = "DirectSubscription")]
pub struct NapiDirectSubscription {
    inner: Option<DirectNapiSubscription>,
}

enum DirectNapiTransportInner {
    Memory {
        db: Rc<DirectDb<DirectMemoryStorage>>,
        connection: Option<Rc<RefCell<DirectPeerConnection<DirectMemoryStorage>>>>,
    },
    Persistent {
        db: Rc<DirectDb<DirectRocksDbStorage>>,
        connection: Option<Rc<RefCell<DirectPeerConnection<DirectRocksDbStorage>>>>,
    },
}

enum DirectNapiSubscription {
    Memory(DirectSubscriptionStream),
    Persistent(DirectSubscriptionStream),
}

#[napi(js_name = "DirectTx")]
pub struct NapiDirectTx {
    db: DirectNapiDb,
    writes: Option<Vec<DirectNapiTxWrite>>,
}

#[napi]
impl NapiDirectWrite {
    #[napi(getter)]
    pub fn payload(&self) -> Uint8Array {
        Uint8Array::new(self.payload.clone())
    }

    #[napi]
    pub fn wait(&self, tier: String) -> napi::Result<()> {
        let tier = direct_durability_tier_from_str(&tier)?;
        if let Some(write) = &self.inner {
            match write {
                DirectNapiWrite::Memory(write) => {
                    direct_block_on(write.wait(tier))
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                }
                DirectNapiWrite::Persistent(write) => {
                    direct_block_on(write.wait(tier))
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                }
                DirectNapiWrite::MemoryTx { db, tx_id } => direct_wait_for_tx(db, *tx_id, tier)?,
                DirectNapiWrite::PersistentTx { db, tx_id } => {
                    direct_wait_for_tx(db, *tx_id, tier)?
                }
            }
        }
        Ok(())
    }
}

#[napi]
impl NapiDirectTransport {
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
            DirectNapiTransportInner::Memory { connection, .. } => {
                direct_tick_connection(connection)
            }
            DirectNapiTransportInner::Persistent { connection, .. } => {
                direct_tick_connection(connection)
            }
        }
    }

    #[napi]
    pub fn close(&mut self) -> bool {
        match &mut self.inner {
            DirectNapiTransportInner::Memory { db, connection } => {
                let Some(connection) = connection.take() else {
                    return false;
                };
                db.detach_connection(&connection)
            }
            DirectNapiTransportInner::Persistent { db, connection } => {
                let Some(connection) = connection.take() else {
                    return false;
                };
                db.detach_connection(&connection)
            }
        }
    }
}

#[napi]
impl NapiDirectSubscription {
    #[napi(js_name = "readAll")]
    pub fn read_all(&mut self) -> napi::Result<Vec<serde_json::Value>> {
        let subscription = self
            .inner
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("subscription is closed"))?;
        let mut events = Vec::new();
        loop {
            let event = match subscription {
                DirectNapiSubscription::Memory(stream) => stream.try_next_event(),
                DirectNapiSubscription::Persistent(stream) => stream.try_next_event(),
            };
            let Some(event) = event else {
                break;
            };
            events.push(direct_subscription_event_to_json(&event)?);
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
impl NapiDirectTx {
    #[napi(js_name = "insertWithIdEncoded")]
    pub fn insert_with_id_encoded(
        &mut self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
    ) -> napi::Result<()> {
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        let cells = decode_direct_cells(&cells)?;
        self.pending_writes()?.push(DirectNapiTxWrite::Insert {
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
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        let patch = decode_direct_cells(&patch)?;
        self.pending_writes()?.push(DirectNapiTxWrite::Update {
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
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        let cells = decode_direct_cells(&cells)?;
        self.pending_writes()?.push(DirectNapiTxWrite::Upsert {
            table,
            row_id,
            cells,
        });
        Ok(())
    }

    #[napi(js_name = "delete")]
    pub fn delete_encoded(&mut self, table: String, row_id: Uint8Array) -> napi::Result<()> {
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        self.pending_writes()?
            .push(DirectNapiTxWrite::Delete { table, row_id });
        Ok(())
    }

    #[napi(js_name = "restoreEncoded")]
    pub fn restore_encoded(
        &mut self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
    ) -> napi::Result<()> {
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        let cells = decode_direct_cells(&cells)?;
        self.pending_writes()?.push(DirectNapiTxWrite::Restore {
            table,
            row_id,
            cells,
        });
        Ok(())
    }

    #[napi]
    pub fn commit(&mut self) -> napi::Result<NapiDirectWrite> {
        let writes = self
            .writes
            .take()
            .ok_or_else(|| napi::Error::from_reason("transaction is already closed"))?;
        match &self.db {
            DirectNapiDb::Memory(db) => direct_commit_tx_memory(db, writes),
            DirectNapiDb::Persistent(db) => direct_commit_tx_persistent(db, writes),
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

impl NapiDirectTx {
    fn pending_writes(&mut self) -> napi::Result<&mut Vec<DirectNapiTxWrite>> {
        self.writes
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("transaction is already closed"))
    }
}

#[napi(js_name = "NapiDirectDb")]
pub struct NapiDirectDb {
    inner: DirectNapiDbInner,
}

#[napi]
impl NapiDirectDb {
    #[napi(factory, js_name = "openMemory")]
    pub fn open_memory(schema: Uint8Array, config: Uint8Array) -> napi::Result<Self> {
        let (schema, config) = decode_direct_open_args(&schema, &config)?;
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let db = open_direct_db(schema, DirectMemoryStorage::new(&refs), config)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(DirectNapiDb::Memory(Rc::new(db))))),
        })
    }

    #[napi(factory, js_name = "openPersistent")]
    pub fn open_persistent(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
    ) -> napi::Result<Self> {
        let (schema, config) = decode_direct_open_args(&schema, &config)?;
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = DirectRocksDbStorage::open(data_path, &refs)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        let db = open_direct_db(schema, storage, config)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(DirectNapiDb::Persistent(Rc::new(db))))),
        })
    }

    #[napi(js_name = "prepareQuery")]
    pub fn prepare_query(&self, query: Uint8Array) -> napi::Result<NapiDirectPreparedQuery> {
        let query: DirectQuery = postcard::from_bytes(&query)
            .map_err(|error| napi::Error::from_reason(format!("decode query: {error}")))?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        let inner = match db {
            DirectNapiDb::Memory(db) => db.prepare_query(&query),
            DirectNapiDb::Persistent(db) => db.prepare_query(&query),
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(NapiDirectPreparedQuery { inner })
    }

    #[napi]
    pub fn all(
        &self,
        query: &NapiDirectPreparedQuery,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
    ) -> napi::Result<Uint8Array> {
        let opts = direct_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        let rows = match db {
            DirectNapiDb::Memory(db) => direct_block_on(db.all(&query.inner, opts)),
            DirectNapiDb::Persistent(db) => direct_block_on(db.all(&query.inner, opts)),
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        encode_direct_rows(&rows)
            .map(Uint8Array::new)
            .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi(js_name = "allForIdentity")]
    pub fn all_for_identity(
        &self,
        query: &NapiDirectPreparedQuery,
        author: Uint8Array,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
    ) -> napi::Result<Uint8Array> {
        let author = direct_author_id_from_bytes(&author)?;
        let opts = direct_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        let rows = match db {
            DirectNapiDb::Memory(db) => {
                direct_set_identity_claims(db, author);
                direct_block_on(db.all_for_identity(&query.inner, opts, author))
            }
            DirectNapiDb::Persistent(db) => {
                direct_set_identity_claims(db, author);
                direct_block_on(db.all_for_identity(&query.inner, opts, author))
            }
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        encode_direct_rows(&rows)
            .map(Uint8Array::new)
            .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi(js_name = "propagateQuery")]
    pub fn propagate_query(
        &self,
        query: &NapiDirectPreparedQuery,
        opts: Option<serde_json::Value>,
    ) -> napi::Result<()> {
        let opts = direct_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        match db {
            DirectNapiDb::Memory(db) => db.propagate_query_with_opts(&query.inner, opts),
            DirectNapiDb::Persistent(db) => db.propagate_query_with_opts(&query.inner, opts),
        }
        Ok(())
    }

    #[napi(js_name = "queryIsCovered")]
    pub fn query_is_covered(&self, query: &NapiDirectPreparedQuery) -> napi::Result<bool> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        Ok(match db {
            DirectNapiDb::Memory(db) => db.query_is_covered(&query.inner),
            DirectNapiDb::Persistent(db) => db.query_is_covered(&query.inner),
        })
    }

    #[napi]
    pub fn subscribe(
        &self,
        query: &NapiDirectPreparedQuery,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
    ) -> napi::Result<NapiDirectSubscription> {
        let opts = direct_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        let inner = match db {
            DirectNapiDb::Memory(db) => DirectNapiSubscription::Memory(
                direct_block_on(db.subscribe(&query.inner, opts))
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            DirectNapiDb::Persistent(db) => DirectNapiSubscription::Persistent(
                direct_block_on(db.subscribe(&query.inner, opts))
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        };
        Ok(NapiDirectSubscription { inner: Some(inner) })
    }

    #[napi(js_name = "subscribeForIdentity")]
    pub fn subscribe_for_identity(
        &self,
        query: &NapiDirectPreparedQuery,
        author: Uint8Array,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
    ) -> napi::Result<NapiDirectSubscription> {
        let author = direct_author_id_from_bytes(&author)?;
        let opts = direct_read_opts_from_json(opts)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        let inner = match db {
            DirectNapiDb::Memory(db) => {
                direct_set_identity_claims(db, author);
                DirectNapiSubscription::Memory(
                    direct_block_on(db.subscribe_for_identity(&query.inner, opts, author))
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?,
                )
            }
            DirectNapiDb::Persistent(db) => {
                direct_set_identity_claims(db, author);
                DirectNapiSubscription::Persistent(
                    direct_block_on(db.subscribe_for_identity(&query.inner, opts, author))
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?,
                )
            }
        };
        Ok(NapiDirectSubscription { inner: Some(inner) })
    }

    #[napi(js_name = "insertWithIdEncoded")]
    pub fn insert_with_id_encoded(
        &self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
    ) -> napi::Result<NapiDirectWrite> {
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        let cells = decode_direct_cells(&cells)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        match db {
            DirectNapiDb::Memory(db) => direct_write_memory(
                db.insert_with_id(&table, row_id, cells)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            DirectNapiDb::Persistent(db) => direct_write_persistent(
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
    ) -> napi::Result<NapiDirectWrite> {
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        let cells = decode_direct_cells(&cells)?;
        let author = direct_author_id_from_bytes(&author)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        match db {
            DirectNapiDb::Memory(db) => {
                direct_set_identity_claims(db, author);
                direct_write_memory(
                    db.insert_with_id_for_identity(author, &table, row_id, cells)
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?,
                )
            }
            DirectNapiDb::Persistent(db) => {
                direct_set_identity_claims(db, author);
                direct_write_persistent(
                    db.insert_with_id_for_identity(author, &table, row_id, cells)
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?,
                )
            }
        }
    }

    #[napi(js_name = "updateEncoded")]
    pub fn update_encoded(
        &self,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
    ) -> napi::Result<NapiDirectWrite> {
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        let patch = decode_direct_cells(&patch)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        match db {
            DirectNapiDb::Memory(db) => direct_write_memory(
                db.update(&table, row_id, patch)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            DirectNapiDb::Persistent(db) => direct_write_persistent(
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
    ) -> napi::Result<NapiDirectWrite> {
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        let patch = decode_direct_cells(&patch)?;
        let author = direct_author_id_from_bytes(&author)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        match db {
            DirectNapiDb::Memory(db) => {
                direct_set_identity_claims(db, author);
                direct_write_memory(
                    db.update_for_identity(author, &table, row_id, patch)
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?,
                )
            }
            DirectNapiDb::Persistent(db) => {
                direct_set_identity_claims(db, author);
                direct_write_persistent(
                    db.update_for_identity(author, &table, row_id, patch)
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?,
                )
            }
        }
    }

    #[napi(js_name = "upsertEncoded")]
    pub fn upsert_encoded(
        &self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
    ) -> napi::Result<NapiDirectWrite> {
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        let cells = decode_direct_cells(&cells)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        match db {
            DirectNapiDb::Memory(db) => direct_write_memory(
                db.upsert(&table, row_id, cells)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            DirectNapiDb::Persistent(db) => direct_write_persistent(
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
    ) -> napi::Result<NapiDirectWrite> {
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        let cells = decode_direct_cells(&cells)?;
        let author = direct_author_id_from_bytes(&author)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        match db {
            DirectNapiDb::Memory(db) => {
                direct_set_identity_claims(db, author);
                direct_write_memory(
                    db.upsert_for_identity(author, &table, row_id, cells)
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?,
                )
            }
            DirectNapiDb::Persistent(db) => {
                direct_set_identity_claims(db, author);
                direct_write_persistent(
                    db.upsert_for_identity(author, &table, row_id, cells)
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?,
                )
            }
        }
    }

    #[napi(js_name = "delete")]
    pub fn delete_encoded(
        &self,
        table: String,
        row_id: Uint8Array,
    ) -> napi::Result<NapiDirectWrite> {
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        match db {
            DirectNapiDb::Memory(db) => direct_write_memory(
                db.delete(&table, row_id)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            DirectNapiDb::Persistent(db) => direct_write_persistent(
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
    ) -> napi::Result<NapiDirectWrite> {
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        let author = direct_author_id_from_bytes(&author)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        match db {
            DirectNapiDb::Memory(db) => {
                direct_set_identity_claims(db, author);
                direct_write_memory(
                    db.delete_for_identity(author, &table, row_id)
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?,
                )
            }
            DirectNapiDb::Persistent(db) => {
                direct_set_identity_claims(db, author);
                direct_write_persistent(
                    db.delete_for_identity(author, &table, row_id)
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?,
                )
            }
        }
    }

    #[napi(js_name = "restoreEncoded")]
    pub fn restore_encoded(
        &self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
    ) -> napi::Result<NapiDirectWrite> {
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        let cells = decode_direct_cells(&cells)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        match db {
            DirectNapiDb::Memory(db) => direct_write_memory(
                db.restore(&table, row_id, cells)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            DirectNapiDb::Persistent(db) => direct_write_persistent(
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
    ) -> napi::Result<NapiDirectWrite> {
        let row_id = direct_row_uuid_from_bytes(&row_id)?;
        let cells = decode_direct_cells(&cells)?;
        let author = direct_author_id_from_bytes(&author)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        match db {
            DirectNapiDb::Memory(db) => {
                direct_set_identity_claims(db, author);
                direct_write_memory(
                    db.restore_for_identity(author, &table, row_id, cells)
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?,
                )
            }
            DirectNapiDb::Persistent(db) => {
                direct_set_identity_claims(db, author);
                direct_write_persistent(
                    db.restore_for_identity(author, &table, row_id, cells)
                        .map_err(|error| napi::Error::from_reason(error.to_string()))?,
                )
            }
        }
    }

    #[napi]
    pub fn tick(&self) -> napi::Result<()> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        match db {
            DirectNapiDb::Memory(db) => db.tick(),
            DirectNapiDb::Persistent(db) => db.tick(),
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi(js_name = "connectUpstream")]
    pub fn connect_upstream(&self) -> napi::Result<NapiDirectTransport> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        let queues = DirectWireQueues::default();
        let transport = Box::new(DirectWireTransportAdapter::current(NapiWireTransport {
            queues: queues.clone(),
        }));
        let inner = match db {
            DirectNapiDb::Memory(db) => DirectNapiTransportInner::Memory {
                db: Rc::clone(db),
                connection: Some(db.connect_upstream(transport)),
            },
            DirectNapiDb::Persistent(db) => DirectNapiTransportInner::Persistent {
                db: Rc::clone(db),
                connection: Some(db.connect_upstream(transport)),
            },
        };
        Ok(NapiDirectTransport { inner, queues })
    }

    #[napi(js_name = "mergeableTx")]
    pub fn mergeable_tx(&self) -> napi::Result<NapiDirectTx> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("direct DB is closed"))?;
        Ok(NapiDirectTx {
            db: match db {
                DirectNapiDb::Memory(db) => DirectNapiDb::Memory(Rc::clone(db)),
                DirectNapiDb::Persistent(db) => DirectNapiDb::Persistent(Rc::clone(db)),
            },
            writes: Some(Vec::new()),
        })
    }

    #[napi]
    pub fn close(&self) {
        self.inner.borrow_mut().take();
    }
}

fn decode_direct_open_args(
    schema: &[u8],
    config: &[u8],
) -> napi::Result<(JazzSchema, DirectOpenDbConfig)> {
    let schema: JazzSchema = postcard::from_bytes(schema)
        .map_err(|error| napi::Error::from_reason(format!("decode schema: {error}")))?;
    let config: DirectOpenDbConfig = postcard::from_bytes(config)
        .map_err(|error| napi::Error::from_reason(format!("decode open config: {error}")))?;
    Ok((schema, config))
}

fn open_direct_db<S>(
    schema: JazzSchema,
    storage: S,
    config: DirectOpenDbConfig,
) -> std::result::Result<DirectDb<S>, jazz::db::Error>
where
    S: DirectOrderedKvStorage + DirectReopenableStorage + 'static,
{
    let mut db_config = DirectDbConfig::new(schema, storage, config.identity.into());
    if let Some(seed) = config.row_id_seed {
        db_config = db_config.with_id_source(DirectSeededRowIdSource::new(seed));
    }
    if config.history_complete {
        direct_block_on(DirectDb::open_history_complete(db_config))
    } else {
        direct_block_on(DirectDb::open(db_config))
    }
}

fn decode_direct_cells(bytes: &[u8]) -> napi::Result<DirectRowCells> {
    let (descriptor, raw): (RecordDescriptor, Vec<u8>) = postcard::from_bytes(bytes)
        .map_err(|error| napi::Error::from_reason(format!("decode cells: {error}")))?;
    let record = DirectBorrowedRecord::new(&raw, &descriptor);
    let values = record
        .to_values()
        .map_err(|error| napi::Error::from_reason(format!("decode cell record: {error}")))?;
    let mut cells = DirectRowCells::new();
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

fn direct_row_uuid_from_bytes(bytes: &[u8]) -> napi::Result<DirectRowUuid> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| napi::Error::from_reason("row id must be 16 bytes"))?;
    Ok(DirectRowUuid::from_bytes(bytes))
}

fn direct_author_id_from_bytes(bytes: &[u8]) -> napi::Result<DirectAuthorId> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| napi::Error::from_reason("author id must be 16 bytes"))?;
    Ok(DirectAuthorId::from_bytes(bytes))
}

fn direct_write_memory(
    write: DirectWriteHandle<DirectMemoryStorage>,
) -> napi::Result<NapiDirectWrite> {
    let result = DirectWriteResult {
        row_id: write.row_uuid(),
        tx_id: write.mergeable_tx_id(),
    };
    Ok(NapiDirectWrite {
        payload: postcard::to_allocvec(&result)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?,
        inner: Some(DirectNapiWrite::Memory(write)),
    })
}

fn direct_write_persistent(
    write: DirectWriteHandle<DirectRocksDbStorage>,
) -> napi::Result<NapiDirectWrite> {
    let result = DirectWriteResult {
        row_id: write.row_uuid(),
        tx_id: write.mergeable_tx_id(),
    };
    Ok(NapiDirectWrite {
        payload: postcard::to_allocvec(&result)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?,
        inner: Some(DirectNapiWrite::Persistent(write)),
    })
}

fn direct_set_identity_claims<S>(db: &DirectDb<S>, author: DirectAuthorId)
where
    S: DirectOrderedKvStorage + DirectReopenableStorage + 'static,
{
    let subject = author.0.to_string();
    db.set_identity_claims(
        author,
        BTreeMap::from([
            ("subject".to_owned(), DirectValue::String(subject.clone())),
            ("sub".to_owned(), DirectValue::String(subject.clone())),
            ("user_id".to_owned(), DirectValue::String(subject)),
        ]),
    );
}

fn direct_tx_write(
    tx_id: DirectTxId,
    inner: Option<DirectNapiWrite>,
) -> napi::Result<NapiDirectWrite> {
    let result = DirectWriteResult {
        row_id: DirectRowUuid::from_bytes([0; 16]),
        tx_id,
    };
    Ok(NapiDirectWrite {
        payload: postcard::to_allocvec(&result)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?,
        inner,
    })
}

fn direct_tick_connection<S>(
    connection: &Option<Rc<RefCell<DirectPeerConnection<S>>>>,
) -> napi::Result<u32>
where
    S: DirectOrderedKvStorage + DirectReopenableStorage + 'static,
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

fn direct_wait_for_tx<S>(
    db: &DirectDb<S>,
    tx_id: DirectTxId,
    tier: DirectDurabilityTier,
) -> napi::Result<()>
where
    S: DirectOrderedKvStorage + DirectReopenableStorage + 'static,
{
    if tier <= DirectDurabilityTier::Local {
        return Ok(());
    }
    let state = db
        .write_state(tx_id)
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    match state.fate {
        DirectFate::Rejected(reason) => {
            return Err(napi::Error::from_reason(format!(
                "transaction was rejected: {reason:?}"
            )));
        }
        DirectFate::Pending if tier >= DirectDurabilityTier::Edge => {
            return Err(napi::Error::from_reason(format!(
                "transaction has not been accepted at requested tier {tier:?}"
            )));
        }
        DirectFate::Pending | DirectFate::Accepted => {}
    }
    if state.durability >= tier {
        return Ok(());
    }
    Err(napi::Error::from_reason(format!(
        "transaction has not reached requested tier {tier:?}"
    )))
}

fn direct_commit_tx<S>(db: &DirectDb<S>, writes: Vec<DirectNapiTxWrite>) -> napi::Result<DirectTxId>
where
    S: DirectOrderedKvStorage + DirectReopenableStorage + 'static,
{
    let mut tx = db.mergeable_tx();
    for write in writes {
        match write {
            DirectNapiTxWrite::Insert {
                table,
                row_id,
                cells,
            } => tx
                .insert_with_id(&table, row_id, cells)
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            DirectNapiTxWrite::Update {
                table,
                row_id,
                patch,
            } => tx
                .update(&table, row_id, patch)
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            DirectNapiTxWrite::Upsert {
                table,
                row_id,
                cells,
            } => tx
                .update(&table, row_id, cells)
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            DirectNapiTxWrite::Delete { table, row_id } => tx
                .delete(&table, row_id)
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            DirectNapiTxWrite::Restore {
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

fn direct_commit_tx_memory(
    db: &Rc<DirectDb<DirectMemoryStorage>>,
    writes: Vec<DirectNapiTxWrite>,
) -> napi::Result<NapiDirectWrite> {
    let tx_id = direct_commit_tx(db, writes)?;
    direct_tx_write(
        tx_id,
        Some(DirectNapiWrite::MemoryTx {
            db: Rc::clone(db),
            tx_id,
        }),
    )
}

fn direct_commit_tx_persistent(
    db: &Rc<DirectDb<DirectRocksDbStorage>>,
    writes: Vec<DirectNapiTxWrite>,
) -> napi::Result<NapiDirectWrite> {
    let tx_id = direct_commit_tx(db, writes)?;
    direct_tx_write(
        tx_id,
        Some(DirectNapiWrite::PersistentTx {
            db: Rc::clone(db),
            tx_id,
        }),
    )
}

fn direct_read_opts_from_json(value: Option<JsonValue>) -> napi::Result<DirectReadOpts> {
    let mut opts = DirectReadOpts::default();
    let Some(value) = value else {
        return Ok(opts);
    };
    if value.is_null() {
        return Ok(opts);
    }
    if let Some(tier) = optional_json_string_prop(&value, "tier")? {
        opts.tier = direct_durability_tier_from_str(&tier)?;
    }
    if let Some(local_updates) = optional_json_string_prop(&value, "local_updates")? {
        opts.local_updates = match local_updates.as_str() {
            "Immediate" | "immediate" => DirectLocalUpdates::Immediate,
            "Deferred" | "deferred" => DirectLocalUpdates::Deferred,
            other => {
                return Err(napi::Error::from_reason(format!(
                    "unknown local_updates {other}"
                )));
            }
        };
    }
    if let Some(propagation) = optional_json_string_prop(&value, "propagation")? {
        opts.propagation = match propagation.as_str() {
            "Full" | "full" => DirectPropagation::Full,
            "LocalOnly" | "local_only" | "localOnly" => DirectPropagation::LocalOnly,
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

fn direct_durability_tier_from_str(tier: &str) -> napi::Result<DirectDurabilityTier> {
    match tier {
        "None" | "none" => Ok(DirectDurabilityTier::None),
        "Local" | "local" => Ok(DirectDurabilityTier::Local),
        "Edge" | "edge" => Ok(DirectDurabilityTier::Edge),
        "Global" | "global" => Ok(DirectDurabilityTier::Global),
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

fn encode_direct_rows(
    rows: &[jazz::node::CurrentRow],
) -> std::result::Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&direct_row_batches(rows))
}

fn direct_row_batches(rows: &[jazz::node::CurrentRow]) -> Vec<DirectRowBatch<'_>> {
    let mut batches: Vec<DirectRowBatch<'_>> = Vec::new();
    for row in rows {
        let (descriptor, raw) = row.encoded_record();
        match batches.last_mut() {
            Some(batch) if batch.table == row.table() && batch.descriptor == *descriptor => {
                batch.rows.push(direct_row(row, raw));
            }
            _ => batches.push(DirectRowBatch {
                table: row.table(),
                descriptor: *descriptor,
                rows: vec![direct_row(row, raw)],
            }),
        }
    }
    batches
}

fn direct_row<'a>(row: &jazz::node::CurrentRow, raw: &'a [u8]) -> DirectRow<'a> {
    DirectRow {
        row_id: row.row_uuid(),
        deleted: row.is_deleted(),
        raw,
    }
}

fn direct_removed_rows(rows: &[DirectRemovedRowInner]) -> Vec<DirectRemovedRow<'_>> {
    rows.iter()
        .map(|row| DirectRemovedRow {
            table: row.table.as_str(),
            row_id: row.row_uuid,
        })
        .collect()
}

fn encode_direct_subscription_delta(
    added: &[jazz::node::CurrentRow],
    updated: &[jazz::node::CurrentRow],
    removed: &[DirectRemovedRowInner],
) -> std::result::Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&DirectSubscriptionDelta {
        added: direct_row_batches(added),
        updated: direct_row_batches(updated),
        removed: direct_removed_rows(removed),
    })
}

fn direct_subscription_event_to_json(
    event: &DirectSubscriptionEvent,
) -> napi::Result<serde_json::Value> {
    match event {
        DirectSubscriptionEvent::Opened { current, .. }
        | DirectSubscriptionEvent::Reset { current, .. } => {
            let rows = encode_direct_rows(current)
                .map_err(|error| napi::Error::from_reason(error.to_string()))?;
            Ok(serde_json::json!({ "type": "snapshot", "rows": rows }))
        }
        DirectSubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        } => {
            let delta = encode_direct_subscription_delta(added, updated, removed)
                .map_err(|error| napi::Error::from_reason(error.to_string()))?;
            Ok(serde_json::json!({ "type": "delta", "delta": delta }))
        }
        DirectSubscriptionEvent::Closed => Ok(serde_json::json!({ "type": "closed" })),
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
    Alpha(CoreJazzServer),
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

        let core_server_schema = opts
            .schema
            .take()
            .map(|schema_bytes| {
                postcard::from_bytes::<JazzSchema>(&schema_bytes).map_err(|error| {
                    napi::Error::from_reason(format!("Invalid direct Jazz schema bytes: {error}"))
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
        if let Some(schema) = core_server_schema {
            server_builder = server_builder.with_core_server_schema(schema);
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
                server_builder = server_builder.with_storage(StorageBackend::Sqlite {
                    path: data_dir.clone().into(),
                });
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
            inner: Mutex::new(Some(JazzServerInner::Alpha(server))),
        })
    }

    #[napi(getter, js_name = "appId")]
    pub fn app_id(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Alpha(server) => server.app_id().to_string(),
        })
    }

    #[napi(getter)]
    pub fn url(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Alpha(server) => server.base_url(),
        })
    }

    #[napi(getter)]
    pub fn port(&self) -> napi::Result<u16> {
        self.with_server(|server| match server {
            JazzServerInner::Alpha(server) => server.port(),
        })
    }

    #[napi(getter, js_name = "dataDir")]
    pub fn data_dir(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Alpha(server) => server.data_dir().to_string_lossy().into_owned(),
        })
    }

    #[napi(getter, js_name = "backendSecret")]
    pub fn backend_secret(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Alpha(server) => server.backend_secret().to_string(),
        })
    }

    #[napi(getter, js_name = "adminSecret")]
    pub fn admin_secret(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Alpha(server) => server.admin_secret().to_string(),
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
                JazzServerInner::Alpha(server) => server.shutdown().await,
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
    use jazz_tools::{ColumnType, Schema, SchemaBuilder, TableName, TableSchema, Value};

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
}
