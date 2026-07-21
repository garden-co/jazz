use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::rc::Rc;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use futures_util::{Stream, StreamExt};
use jazz::db::{
    block_on, Db, DbConfig, DbIdentity, LocalUpdates, PeerConnection, PreparedQuery, Propagation,
    QueryAttachment, ReadOpts, RowCells, SeededRowIdSource, SubscriptionEvent, TickScheduler,
    TickUrgency, WireTransportAdapter, WriteHandle,
};
use jazz::groove::records::{BorrowedRecord, RecordDescriptor, Value};
#[cfg(target_arch = "wasm32")]
use jazz::groove::storage::OpfsStorage;
use jazz::groove::storage::{MemoryStorage, OrderedKvStorage, ReopenableStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::OpenTxId;
use jazz::query::{Query, RelationExpr, RelationQuery};
use jazz::schema::JazzSchema;
use jazz::tx::{DurabilityTier, TxId};
use jazz::wire::{TransportError, WireTransport};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::future_to_promise;

mod identity;

#[cfg(feature = "bench-probes")]
pub mod bench_probes;

#[cfg(all(target_arch = "wasm32", not(target_feature = "atomics")))]
#[global_allocator]
static TALC: talc::wasm::WasmDynamicTalc = talc::wasm::new_wasm_dynamic_allocator();

/// Initialize the WASM module.
///
/// Sets up panic hook for better error messages in the browser console.
#[wasm_bindgen(start)]
pub fn init() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

/// Generate a new UUID v7 (time-ordered).
///
/// Useful when a caller wants the default generated row-id shape.
#[wasm_bindgen(js_name = generateId)]
pub fn generate_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// Get the current timestamp in microseconds since Unix epoch.
#[wasm_bindgen(js_name = currentTimestamp)]
pub fn current_timestamp() -> u64 {
    use web_time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

#[cfg(feature = "bench-probes")]
#[wasm_bindgen(js_name = benchProbeArithmeticHash)]
pub fn bench_probe_arithmetic_hash(iterations: u32) -> u64 {
    bench_probes::arithmetic_hash(iterations)
}

#[cfg(feature = "bench-probes")]
#[wasm_bindgen(js_name = benchProbeDynDispatch)]
pub fn bench_probe_dyn_dispatch(iterations: u32) -> u64 {
    bench_probes::dyn_dispatch(iterations)
}

#[cfg(feature = "bench-probes")]
#[wasm_bindgen(js_name = benchProbeRefCellBorrow)]
pub fn bench_probe_refcell_borrow(iterations: u32) -> u64 {
    bench_probes::refcell_borrow(iterations)
}

#[cfg(feature = "bench-probes")]
#[wasm_bindgen(js_name = benchProbeAllocChurn)]
pub fn bench_probe_alloc_churn(iterations: u32) -> u64 {
    bench_probes::alloc_churn(iterations)
}

#[cfg(feature = "bench-probes")]
#[wasm_bindgen(js_name = benchProbeRandomAccessMemory)]
pub fn bench_probe_random_access_memory(iterations: u32, entries: u32) -> u64 {
    bench_probes::random_access_memory(iterations, entries)
}

fn decode_seed(seed_b64: &str) -> Result<[u8; 32], JsValue> {
    let bytes = URL_SAFE_NO_PAD
        .decode(seed_b64)
        .map_err(|e| JsValue::from_str(&format!("seed base64 decode error: {e}")))?;
    bytes
        .try_into()
        .map_err(|_| JsValue::from_str("seed must be exactly 32 bytes"))
}

/// Mint a local-first identity JWT from a base64url-encoded 32-byte seed.
#[wasm_bindgen(js_name = mintLocalFirstToken)]
pub fn mint_local_first_token(
    seed_b64: String,
    audience: String,
    ttl_seconds: u32,
    now_seconds: u64,
) -> Result<String, JsValue> {
    let seed = decode_seed(&seed_b64)?;
    identity::mint_jazz_self_signed_token_at(
        &seed,
        identity::LOCAL_FIRST_ISSUER,
        &audience,
        ttl_seconds as u64,
        now_seconds,
    )
    .map_err(|e| JsValue::from_str(&e))
}

/// Derive a stable local-first user id from a base64url-encoded 32-byte seed.
#[wasm_bindgen(js_name = deriveUserId)]
pub fn derive_user_id(seed_b64: String) -> Result<String, JsValue> {
    let seed = decode_seed(&seed_b64)?;
    Ok(identity::derive_user_id(&seed).to_string())
}

/// Mint an anonymous identity JWT from a base64url-encoded 32-byte seed.
#[wasm_bindgen(js_name = mintAnonymousToken)]
pub fn mint_anonymous_token(
    seed_b64: String,
    audience: String,
    ttl_seconds: u32,
    now_seconds: u64,
) -> Result<String, JsValue> {
    let seed = decode_seed(&seed_b64)?;
    identity::mint_jazz_self_signed_token_at(
        &seed,
        identity::ANONYMOUS_ISSUER,
        &audience,
        ttl_seconds as u64,
        now_seconds,
    )
    .map_err(|e| JsValue::from_str(&e))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct WasmOpenDbConfig {
    identity: WasmDbIdentity,
    row_id_seed: Option<u64>,
    history_complete: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct WasmDbIdentity {
    node: NodeUuid,
    author: AuthorId,
}

impl From<WasmDbIdentity> for DbIdentity {
    fn from(identity: WasmDbIdentity) -> Self {
        Self {
            node: identity.node,
            author: identity.author,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct WasmRowBatch<'a> {
    table: &'a str,
    descriptor: RecordDescriptor,
    rows: Vec<WasmRow<'a>>,
}

#[derive(Clone, Debug, Serialize)]
struct WasmRow<'a> {
    row_id: RowUuid,
    deleted: bool,
    raw: &'a [u8],
}

#[derive(Clone, Debug, Serialize)]
struct WasmRelationSnapshot<'a> {
    cursor: u64,
    root_count: u64,
    rows: Vec<WasmRowBatch<'a>>,
    edges: Vec<WasmRelationEdge>,
}

#[derive(Clone, Debug, Serialize)]
struct WasmSubscriptionDelta<'a> {
    added: Vec<WasmRowBatch<'a>>,
    updated: Vec<WasmRowBatch<'a>>,
    removed: Vec<WasmRemovedRow>,
}

#[derive(Clone, Debug, Serialize)]
struct WasmRelationSubscriptionDelta<'a> {
    base_cursor: Option<u64>,
    cursor: u64,
    added: Vec<WasmRowBatch<'a>>,
    updated: Vec<WasmRowBatch<'a>>,
    removed: Vec<WasmRemovedRow>,
    added_edges: Vec<WasmRelationEdge>,
    removed_edges: Vec<WasmRelationEdge>,
}

#[derive(Clone, Debug, Serialize)]
struct WasmRelationEdge {
    source_table: String,
    source_row_id: RowUuid,
    relation: String,
    target_table: String,
    target_row_id: RowUuid,
}

#[derive(Clone, Debug, Serialize)]
struct WasmRemovedRow {
    table: String,
    row_id: RowUuid,
}

#[derive(Clone, Debug, Serialize)]
pub struct WasmWriteResult {
    row_id: RowUuid,
    tx_id: jazz::tx::TxId,
}

#[wasm_bindgen]
pub struct WasmPreparedQuery {
    inner: PreparedQuery,
}

#[wasm_bindgen(js_name = QueryAttachment)]
pub struct WasmQueryAttachment {
    inner: QueryAttachment,
}

#[wasm_bindgen]
pub struct WasmWrite {
    payload: Vec<u8>,
    inner: Option<WasmWriteInner>,
}

enum WasmWriteInner {
    MemoryTx {
        db: Rc<Db<MemoryStorage>>,
        tx_id: TxId,
    },
    #[cfg(target_arch = "wasm32")]
    BrowserTx {
        db: Rc<Db<OpfsStorage>>,
        tx_id: TxId,
    },
}

#[wasm_bindgen]
impl WasmWrite {
    #[wasm_bindgen(getter, js_name = payload)]
    pub fn payload(&self) -> Vec<u8> {
        self.payload.clone()
    }

    #[wasm_bindgen(js_name = writeState)]
    pub fn write_state(&self) -> Result<JsValue, JsValue> {
        match &self.inner {
            Some(WasmWriteInner::MemoryTx { db, tx_id }) => {
                write_state_to_js(db.write_state(*tx_id).map_err(to_js_error)?)
            }
            #[cfg(target_arch = "wasm32")]
            Some(WasmWriteInner::BrowserTx { db, tx_id }) => {
                write_state_to_js(db.write_state(*tx_id).map_err(to_js_error)?)
            }
            None => Err(JsValue::from_str("write state is unavailable")),
        }
    }

    #[wasm_bindgen(js_name = wait)]
    pub fn wait(&self, tier: String) -> Result<(), JsValue> {
        let tier = durability_tier_from_str(&tier)?;
        match &self.inner {
            Some(WasmWriteInner::MemoryTx { db, tx_id }) => {
                wait_for_tx(db, *tx_id, tier)?;
            }
            #[cfg(target_arch = "wasm32")]
            Some(WasmWriteInner::BrowserTx { db, tx_id }) => {
                wait_for_tx(db, *tx_id, tier)?;
            }
            None => return Err(JsValue::from_str("write wait is unavailable")),
        }
        Ok(())
    }

    #[wasm_bindgen(js_name = nextWriteStateChange)]
    pub fn next_write_state_change(&self) -> Result<js_sys::Promise, JsValue> {
        match &self.inner {
            Some(WasmWriteInner::MemoryTx { db, tx_id }) => {
                let db = Rc::clone(db);
                let tx_id = *tx_id;
                Ok(future_to_promise(async move {
                    db.next_write_state_change(tx_id).await;
                    Ok(JsValue::UNDEFINED)
                }))
            }
            #[cfg(target_arch = "wasm32")]
            Some(WasmWriteInner::BrowserTx { db, tx_id }) => {
                let db = Rc::clone(db);
                let tx_id = *tx_id;
                Ok(future_to_promise(async move {
                    db.next_write_state_change(tx_id).await;
                    Ok(JsValue::UNDEFINED)
                }))
            }
            None => Err(JsValue::from_str("write state is unavailable")),
        }
    }

    #[wasm_bindgen]
    pub fn close(&mut self) -> bool {
        self.inner.take().is_some()
    }
}

#[wasm_bindgen]
pub struct WasmDb {
    inner: WasmDbInner,
}

enum WasmDbInner {
    Memory(Rc<Db<MemoryStorage>>),
    #[cfg(target_arch = "wasm32")]
    Browser(Rc<Db<OpfsStorage>>),
    Closed,
}

impl Clone for WasmDbInner {
    fn clone(&self) -> Self {
        match self {
            Self::Memory(db) => Self::Memory(Rc::clone(db)),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => Self::Browser(Rc::clone(db)),
            Self::Closed => Self::Closed,
        }
    }
}

#[wasm_bindgen]
pub struct WasmTransport {
    inner: WasmTransportInner,
    queues: WasmWireQueues,
}

enum WasmTransportInner {
    Memory {
        db: Rc<Db<MemoryStorage>>,
        connection: Option<Rc<RefCell<PeerConnection<MemoryStorage>>>>,
    },
    #[cfg(target_arch = "wasm32")]
    Browser {
        db: Rc<Db<OpfsStorage>>,
        connection: Option<Rc<RefCell<PeerConnection<OpfsStorage>>>>,
    },
}

impl WasmTransportInner {
    fn tick(&self) -> Result<u32, JsValue> {
        match self {
            Self::Memory { connection, .. } => tick_connection(connection),
            #[cfg(target_arch = "wasm32")]
            Self::Browser { connection, .. } => tick_connection(connection),
        }
    }

    fn close(&mut self) -> bool {
        match self {
            Self::Memory { db, connection } => {
                let Some(connection) = connection.take() else {
                    return false;
                };
                db.detach_connection(&connection)
            }
            #[cfg(target_arch = "wasm32")]
            Self::Browser { db, connection } => {
                let Some(connection) = connection.take() else {
                    return false;
                };
                db.detach_connection(&connection)
            }
        }
    }
}

#[derive(Clone, Default)]
struct WasmWireQueues {
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
}

struct WasmWireTransport {
    queues: WasmWireQueues,
}

struct WasmTickScheduler {
    callback: js_sys::Function,
}

impl TickScheduler for WasmTickScheduler {
    fn schedule_tick(&self, urgency: TickUrgency) {
        let urgency = match urgency {
            TickUrgency::Immediate => "immediate",
            TickUrgency::Deferred => "deferred",
        };
        let _ = self
            .callback
            .call1(&JsValue::NULL, &JsValue::from_str(urgency));
    }
}

impl WireTransport for WasmWireTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.queues.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.queues.inbound.borrow_mut().pop_front()
    }
}

impl WasmDbInner {
    fn prepare_query(&self, query: &Query) -> Result<PreparedQuery, jazz::db::Error> {
        match self {
            Self::Memory(db) => db.prepare_query(query),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.prepare_query(query),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn all(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        match self {
            Self::Memory(db) => block_on(db.all(query, opts)),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => block_on(db.all(query, opts)),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn all_for_identity(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        match self {
            Self::Memory(db) => block_on(db.all_for_identity(query, opts, author)),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => block_on(db.all_for_identity(query, opts, author)),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn begin_exclusive(&self) -> Result<OpenTxId, jazz::db::Error> {
        match self {
            Self::Memory(db) => db.begin_exclusive(),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.begin_exclusive(),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn exclusive_all_for_identity(
        &self,
        tx_id: OpenTxId,
        query: &PreparedQuery,
        author: AuthorId,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        match self {
            Self::Memory(db) => db.exclusive_all_for_identity(tx_id, query, author),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.exclusive_all_for_identity(tx_id, query, author),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn exclusive_all(
        &self,
        tx_id: OpenTxId,
        query: &PreparedQuery,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        match self {
            Self::Memory(db) => db.exclusive_all(tx_id, query),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.exclusive_all(tx_id, query),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn abandon_exclusive(&self, tx_id: OpenTxId) -> Result<(), jazz::db::Error> {
        match self {
            Self::Memory(db) => db.abandon_exclusive_handle(tx_id),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.abandon_exclusive_handle(tx_id),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn exclusive_write(
        &self,
        tx_id: OpenTxId,
        table: &str,
        row_id: RowUuid,
        cells: RowCells,
    ) -> Result<(), jazz::db::Error> {
        match self {
            Self::Memory(db) => db.exclusive_write(tx_id, table, row_id, cells),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.exclusive_write(tx_id, table, row_id, cells),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn exclusive_update(
        &self,
        tx_id: OpenTxId,
        table: &str,
        row_id: RowUuid,
        patch: RowCells,
    ) -> Result<(), jazz::db::Error> {
        match self {
            Self::Memory(db) => db.exclusive_update(tx_id, table, row_id, patch),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.exclusive_update(tx_id, table, row_id, patch),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn exclusive_delete(
        &self,
        tx_id: OpenTxId,
        table: &str,
        row_id: RowUuid,
    ) -> Result<(), jazz::db::Error> {
        match self {
            Self::Memory(db) => db.exclusive_delete(tx_id, table, row_id),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.exclusive_delete(tx_id, table, row_id),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn commit_exclusive(&self, tx_id: OpenTxId) -> Result<TxId, jazz::db::Error> {
        match self {
            Self::Memory(db) => db.commit_exclusive_handle(tx_id),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.commit_exclusive_handle(tx_id),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn all_relation_snapshot(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<jazz::node::RelationSnapshot, jazz::db::Error> {
        match self {
            Self::Memory(db) => block_on(db.all_relation_snapshot(query, opts)),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => block_on(db.all_relation_snapshot(query, opts)),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn all_relation_snapshot_for_identity(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<jazz::node::RelationSnapshot, jazz::db::Error> {
        match self {
            Self::Memory(db) => {
                block_on(db.all_relation_snapshot_for_identity(query, opts, author))
            }
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => {
                block_on(db.all_relation_snapshot_for_identity(query, opts, author))
            }
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn all_relation_query(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<jazz::node::RelationSnapshot, jazz::db::Error> {
        match self {
            Self::Memory(db) => block_on(db.all_relation_query(query, opts)),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => block_on(db.all_relation_query(query, opts)),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn all_relation_query_for_identity(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<jazz::node::RelationSnapshot, jazz::db::Error> {
        match self {
            Self::Memory(db) => block_on(db.all_relation_query_for_identity(query, opts, author)),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => block_on(db.all_relation_query_for_identity(query, opts, author)),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn set_identity_claims(&self, author: AuthorId, claims: BTreeMap<String, Value>) {
        match self {
            Self::Memory(db) => db.set_identity_claims(author, claims),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.set_identity_claims(author, claims),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn subscribe(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Pin<Box<dyn Stream<Item = SubscriptionEvent> + 'static>>, jazz::db::Error> {
        match self {
            Self::Memory(db) => block_on(db.subscribe(query, opts))
                .map(|stream| Box::pin(stream) as Pin<Box<dyn Stream<Item = SubscriptionEvent>>>),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => block_on(db.subscribe(query, opts))
                .map(|stream| Box::pin(stream) as Pin<Box<dyn Stream<Item = SubscriptionEvent>>>),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn subscribe_for_identity(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<Pin<Box<dyn Stream<Item = SubscriptionEvent> + 'static>>, jazz::db::Error> {
        match self {
            Self::Memory(db) => block_on(db.subscribe_for_identity(query, opts, author))
                .map(|stream| Box::pin(stream) as Pin<Box<dyn Stream<Item = SubscriptionEvent>>>),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => block_on(db.subscribe_for_identity(query, opts, author))
                .map(|stream| Box::pin(stream) as Pin<Box<dyn Stream<Item = SubscriptionEvent>>>),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn subscribe_relation_query(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<Pin<Box<dyn Stream<Item = SubscriptionEvent> + 'static>>, jazz::db::Error> {
        match self {
            Self::Memory(db) => block_on(db.subscribe_relation_query(query, opts))
                .map(|stream| Box::pin(stream) as Pin<Box<dyn Stream<Item = SubscriptionEvent>>>),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => block_on(db.subscribe_relation_query(query, opts))
                .map(|stream| Box::pin(stream) as Pin<Box<dyn Stream<Item = SubscriptionEvent>>>),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn subscribe_relation_query_for_identity(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<Pin<Box<dyn Stream<Item = SubscriptionEvent> + 'static>>, jazz::db::Error> {
        match self {
            Self::Memory(db) => block_on(
                db.subscribe_relation_query_for_identity(query, opts, author),
            )
            .map(|stream| Box::pin(stream) as Pin<Box<dyn Stream<Item = SubscriptionEvent>>>),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => block_on(
                db.subscribe_relation_query_for_identity(query, opts, author),
            )
            .map(|stream| Box::pin(stream) as Pin<Box<dyn Stream<Item = SubscriptionEvent>>>),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn attach_query(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<QueryAttachment, jazz::db::Error> {
        match self {
            Self::Memory(db) => db.attach_query_with_opts(query, opts),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.attach_query_with_opts(query, opts),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn attach_query_for_identity(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<QueryAttachment, jazz::db::Error> {
        match self {
            Self::Memory(db) => db.attach_query_with_opts_for_identity(query, opts, author),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.attach_query_with_opts_for_identity(query, opts, author),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn query_attachment_is_covered(&self, attachment: &QueryAttachment) -> bool {
        match self {
            Self::Memory(db) => db.query_attachment_is_covered(attachment),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.query_attachment_is_covered(attachment),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn detach_query(&self, attachment: QueryAttachment) {
        match self {
            Self::Memory(db) => db.detach_query(attachment),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.detach_query(attachment),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn set_tick_scheduler(&self, callback: js_sys::Function) {
        let scheduler = Rc::new(WasmTickScheduler { callback });
        match self {
            Self::Memory(db) => db.set_tick_scheduler(Some(scheduler)),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.set_tick_scheduler(Some(scheduler)),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn insert(&self, table: &str, cells: RowCells) -> Result<WasmWrite, JsValue> {
        match self {
            Self::Memory(db) => {
                wasm_write_memory(Rc::clone(db), db.insert(table, cells).map_err(to_js_error)?)
            }
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => {
                wasm_write_browser(Rc::clone(db), db.insert(table, cells).map_err(to_js_error)?)
            }
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn insert_with_id(
        &self,
        table: &str,
        row_id: RowUuid,
        cells: RowCells,
        updated_at_ms: Option<u64>,
    ) -> Result<WasmWrite, JsValue> {
        match self {
            Self::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.insert_with_id_at_ms(table, row_id, cells, now_ms),
                    None => db.insert_with_id(table, row_id, cells),
                }
                .map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.insert_with_id_at_ms(table, row_id, cells, now_ms),
                    None => db.insert_with_id(table, row_id, cells),
                }
                .map_err(to_js_error)?,
            ),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn insert_with_id_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row_id: RowUuid,
        cells: RowCells,
        updated_at_ms: Option<u64>,
    ) -> Result<WasmWrite, JsValue> {
        match self {
            Self::Memory(db) => {
                set_identity_claims(db, identity);
                wasm_write_memory(
                    Rc::clone(db),
                    match updated_at_ms {
                        Some(now_ms) => db.insert_with_id_for_identity_at_ms(
                            identity, table, row_id, cells, now_ms,
                        ),
                        None => db.insert_with_id_for_identity(identity, table, row_id, cells),
                    }
                    .map_err(to_js_error)?,
                )
            }
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => {
                set_identity_claims(db, identity);
                wasm_write_browser(
                    Rc::clone(db),
                    match updated_at_ms {
                        Some(now_ms) => db.insert_with_id_for_identity_at_ms(
                            identity, table, row_id, cells, now_ms,
                        ),
                        None => db.insert_with_id_for_identity(identity, table, row_id, cells),
                    }
                    .map_err(to_js_error)?,
                )
            }
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn update(
        &self,
        table: &str,
        row_id: RowUuid,
        patch: RowCells,
        updated_at_ms: Option<u64>,
    ) -> Result<WasmWrite, JsValue> {
        match self {
            Self::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.update_at_ms(table, row_id, patch, now_ms),
                    None => db.update(table, row_id, patch),
                }
                .map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.update_at_ms(table, row_id, patch, now_ms),
                    None => db.update(table, row_id, patch),
                }
                .map_err(to_js_error)?,
            ),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn update_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row_id: RowUuid,
        patch: RowCells,
        updated_at_ms: Option<u64>,
    ) -> Result<WasmWrite, JsValue> {
        match self {
            Self::Memory(db) => {
                set_identity_claims(db, identity);
                wasm_write_memory(
                    Rc::clone(db),
                    match updated_at_ms {
                        Some(now_ms) => {
                            db.update_for_identity_at_ms(identity, table, row_id, patch, now_ms)
                        }
                        None => db.update_for_identity(identity, table, row_id, patch),
                    }
                    .map_err(to_js_error)?,
                )
            }
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => {
                set_identity_claims(db, identity);
                wasm_write_browser(
                    Rc::clone(db),
                    match updated_at_ms {
                        Some(now_ms) => {
                            db.update_for_identity_at_ms(identity, table, row_id, patch, now_ms)
                        }
                        None => db.update_for_identity(identity, table, row_id, patch),
                    }
                    .map_err(to_js_error)?,
                )
            }
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn upsert(
        &self,
        table: &str,
        row_id: RowUuid,
        cells: RowCells,
        updated_at_ms: Option<u64>,
    ) -> Result<WasmWrite, JsValue> {
        match self {
            Self::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.upsert_at_ms(table, row_id, cells, now_ms),
                    None => db.upsert(table, row_id, cells),
                }
                .map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.upsert_at_ms(table, row_id, cells, now_ms),
                    None => db.upsert(table, row_id, cells),
                }
                .map_err(to_js_error)?,
            ),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn upsert_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row_id: RowUuid,
        cells: RowCells,
        updated_at_ms: Option<u64>,
    ) -> Result<WasmWrite, JsValue> {
        match self {
            Self::Memory(db) => {
                set_identity_claims(db, identity);
                wasm_write_memory(
                    Rc::clone(db),
                    match updated_at_ms {
                        Some(now_ms) => {
                            db.upsert_for_identity_at_ms(identity, table, row_id, cells, now_ms)
                        }
                        None => db.upsert_for_identity(identity, table, row_id, cells),
                    }
                    .map_err(to_js_error)?,
                )
            }
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => {
                set_identity_claims(db, identity);
                wasm_write_browser(
                    Rc::clone(db),
                    match updated_at_ms {
                        Some(now_ms) => {
                            db.upsert_for_identity_at_ms(identity, table, row_id, cells, now_ms)
                        }
                        None => db.upsert_for_identity(identity, table, row_id, cells),
                    }
                    .map_err(to_js_error)?,
                )
            }
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn delete(
        &self,
        table: &str,
        row_id: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<WasmWrite, JsValue> {
        match self {
            Self::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                match now_ms {
                    Some(now_ms) => db.delete_at_ms(table, row_id, now_ms),
                    None => db.delete(table, row_id),
                }
                .map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                match now_ms {
                    Some(now_ms) => db.delete_at_ms(table, row_id, now_ms),
                    None => db.delete(table, row_id),
                }
                .map_err(to_js_error)?,
            ),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn delete_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row_id: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<WasmWrite, JsValue> {
        match self {
            Self::Memory(db) => {
                set_identity_claims(db, identity);
                wasm_write_memory(
                    Rc::clone(db),
                    match now_ms {
                        Some(now_ms) => {
                            db.delete_for_identity_at_ms(identity, table, row_id, now_ms)
                        }
                        None => db.delete_for_identity(identity, table, row_id),
                    }
                    .map_err(to_js_error)?,
                )
            }
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => {
                set_identity_claims(db, identity);
                wasm_write_browser(
                    Rc::clone(db),
                    match now_ms {
                        Some(now_ms) => {
                            db.delete_for_identity_at_ms(identity, table, row_id, now_ms)
                        }
                        None => db.delete_for_identity(identity, table, row_id),
                    }
                    .map_err(to_js_error)?,
                )
            }
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn restore(
        &self,
        table: &str,
        row_id: RowUuid,
        cells: RowCells,
        updated_at_ms: Option<u64>,
    ) -> Result<WasmWrite, JsValue> {
        match self {
            Self::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.restore_at_ms(table, row_id, cells, now_ms),
                    None => db.restore(table, row_id, cells),
                }
                .map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                match updated_at_ms {
                    Some(now_ms) => db.restore_at_ms(table, row_id, cells, now_ms),
                    None => db.restore(table, row_id, cells),
                }
                .map_err(to_js_error)?,
            ),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn restore_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row_id: RowUuid,
        cells: RowCells,
        updated_at_ms: Option<u64>,
    ) -> Result<WasmWrite, JsValue> {
        match self {
            Self::Memory(db) => {
                set_identity_claims(db, identity);
                wasm_write_memory(
                    Rc::clone(db),
                    match updated_at_ms {
                        Some(now_ms) => {
                            db.restore_for_identity_at_ms(identity, table, row_id, cells, now_ms)
                        }
                        None => db.restore_for_identity(identity, table, row_id, cells),
                    }
                    .map_err(to_js_error)?,
                )
            }
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => {
                set_identity_claims(db, identity);
                wasm_write_browser(
                    Rc::clone(db),
                    match updated_at_ms {
                        Some(now_ms) => {
                            db.restore_for_identity_at_ms(identity, table, row_id, cells, now_ms)
                        }
                        None => db.restore_for_identity(identity, table, row_id, cells),
                    }
                    .map_err(to_js_error)?,
                )
            }
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn tick(&self) -> Result<(), jazz::db::Error> {
        match self {
            Self::Memory(db) => db.tick(),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.tick(),
            Self::Closed => panic!("WasmDb is closed"),
        }
    }
}

enum WasmTxWrite {
    Insert {
        table: String,
        row_id: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    },
    Update {
        table: String,
        row_id: RowUuid,
        patch: RowCells,
        now_ms: Option<u64>,
    },
    Delete {
        table: String,
        row_id: RowUuid,
        now_ms: Option<u64>,
    },
    Restore {
        table: String,
        row_id: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    },
}

#[wasm_bindgen]
pub struct WasmTx {
    db: WasmDbInner,
    kind: WasmTxKind,
    writes: Option<Vec<WasmTxWrite>>,
    open_tx: Option<OpenTxId>,
}

#[derive(Clone, Copy)]
enum WasmTxKind {
    Mergeable { author: Option<AuthorId> },
    Exclusive,
}

#[wasm_bindgen]
impl WasmDb {
    #[wasm_bindgen(js_name = openMemory)]
    pub fn open_memory(schema: Vec<u8>, config: Vec<u8>) -> Result<WasmDb, JsValue> {
        console_error_panic_hook::set_once();
        let (schema, config) = decode_open_args(&schema, &config)?;
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let db = open_db(schema, MemoryStorage::new(&refs), config).map_err(to_js_error)?;
        Ok(Self {
            inner: WasmDbInner::Memory(Rc::new(db)),
        })
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = openBrowser)]
    pub async fn open_browser(
        namespace: String,
        schema: Vec<u8>,
        config: Vec<u8>,
    ) -> Result<WasmDb, JsValue> {
        console_error_panic_hook::set_once();
        let (schema, config) = decode_open_args(&schema, &config)?;
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = OpfsStorage::open(&namespace, &refs)
            .await
            .map_err(to_js_error)?;
        let db = open_db(schema, storage, config).map_err(to_js_error)?;
        Ok(Self {
            inner: WasmDbInner::Browser(Rc::new(db)),
        })
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = destroyBrowserStorage)]
    pub async fn destroy_browser_storage(namespace: String) -> Result<(), JsValue> {
        OpfsStorage::destroy(&namespace).await.map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = prepareQuery)]
    pub fn prepare_query(&self, query: Vec<u8>) -> Result<WasmPreparedQuery, JsValue> {
        let query: Query = postcard::from_bytes(&query)
            .map_err(|err| to_js_error(format!("decode query: {err}")))?;
        Ok(WasmPreparedQuery {
            inner: self.inner.prepare_query(&query).map_err(to_js_error)?,
        })
    }

    #[wasm_bindgen(js_name = all)]
    pub fn all(&self, query: &WasmPreparedQuery, opts: JsValue) -> Result<Vec<u8>, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let rows = self.inner.all(&query.inner, opts).map_err(to_js_error)?;
        encode_rows(&rows).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = one)]
    pub fn one(&self, query: &WasmPreparedQuery, opts: JsValue) -> Result<Vec<u8>, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let mut rows = self.inner.all(&query.inner, opts).map_err(to_js_error)?;
        rows.truncate(1);
        encode_rows(&rows).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = allInTransaction)]
    pub fn all_in_transaction(
        &self,
        query: &WasmPreparedQuery,
        tx: &WasmTx,
        opts: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let _opts = read_opts_from_js(opts)?;
        let tx_id = tx.open_tx_for_read()?;
        let rows = self
            .inner
            .exclusive_all(tx_id, &query.inner)
            .map_err(to_js_error)?;
        encode_rows(&rows).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = allInTransactionForIdentity)]
    pub fn all_in_transaction_for_identity(
        &self,
        query: &WasmPreparedQuery,
        tx: &WasmTx,
        author: Vec<u8>,
        opts: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let _opts = read_opts_from_js(opts)?;
        let author = author_id_from_bytes(&author)?;
        let tx_id = tx.open_tx_for_read()?;
        let rows = self
            .inner
            .exclusive_all_for_identity(tx_id, &query.inner, author)
            .map_err(to_js_error)?;
        encode_rows(&rows).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = oneInTransaction)]
    pub fn one_in_transaction(
        &self,
        query: &WasmPreparedQuery,
        tx: &WasmTx,
        opts: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let mut rows = read_rows_for_transaction(&self.inner, query, tx, None, opts)?;
        rows.truncate(1);
        encode_rows(&rows).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = oneInTransactionForIdentity)]
    pub fn one_in_transaction_for_identity(
        &self,
        query: &WasmPreparedQuery,
        tx: &WasmTx,
        author: Vec<u8>,
        opts: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let author = author_id_from_bytes(&author)?;
        let mut rows = read_rows_for_transaction(&self.inner, query, tx, Some(author), opts)?;
        rows.truncate(1);
        encode_rows(&rows).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = setIdentityClaims)]
    pub fn set_identity_claims(&self, author: Vec<u8>, claims: JsValue) -> Result<(), JsValue> {
        let author = author_id_from_bytes(&author)?;
        let claims = claims_from_js(author, claims)?;
        self.inner.set_identity_claims(author, claims);
        Ok(())
    }

    #[wasm_bindgen(js_name = allForIdentity)]
    pub fn all_for_identity(
        &self,
        query: &WasmPreparedQuery,
        author: Vec<u8>,
        opts: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let author = author_id_from_bytes(&author)?;
        let rows = self
            .inner
            .all_for_identity(&query.inner, opts, author)
            .map_err(to_js_error)?;
        encode_rows(&rows).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = allRelationQuery)]
    pub fn all_relation_query(
        &self,
        query_json: String,
        opts: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let query = relation_query_from_json(&query_json)?;
        let snapshot = self
            .inner
            .all_relation_query(&query, opts)
            .map_err(to_js_error)?;
        encode_rows(&snapshot.rows).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = allRelationQueryForIdentity)]
    pub fn all_relation_query_for_identity(
        &self,
        query_json: String,
        author: Vec<u8>,
        opts: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let author = author_id_from_bytes(&author)?;
        let query = relation_query_from_json(&query_json)?;
        let snapshot = self
            .inner
            .all_relation_query_for_identity(&query, opts, author)
            .map_err(to_js_error)?;
        encode_rows(&snapshot.rows).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = allRelationSnapshot)]
    pub fn all_relation_snapshot(
        &self,
        query: &WasmPreparedQuery,
        opts: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let snapshot = self
            .inner
            .all_relation_snapshot(&query.inner, opts)
            .map_err(to_js_error)?;
        encode_relation_snapshot(&snapshot).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = allRelationSnapshotForIdentity)]
    pub fn all_relation_snapshot_for_identity(
        &self,
        query: &WasmPreparedQuery,
        author: Vec<u8>,
        opts: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let author = author_id_from_bytes(&author)?;
        let snapshot = self
            .inner
            .all_relation_snapshot_for_identity(&query.inner, opts, author)
            .map_err(to_js_error)?;
        encode_relation_snapshot(&snapshot).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = subscribe)]
    pub fn subscribe(&self, query: &WasmPreparedQuery, opts: JsValue) -> Result<JsValue, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let stream = self
            .inner
            .subscribe(&query.inner, opts)
            .map_err(to_js_error)?;
        readable_stream_from_stream(stream.map(subscription_chunk_to_js))
    }

    #[wasm_bindgen(js_name = subscribeForIdentity)]
    pub fn subscribe_for_identity(
        &self,
        query: &WasmPreparedQuery,
        author: Vec<u8>,
        opts: JsValue,
    ) -> Result<JsValue, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let author = author_id_from_bytes(&author)?;
        let stream = self
            .inner
            .subscribe_for_identity(&query.inner, opts, author)
            .map_err(to_js_error)?;
        readable_stream_from_stream(stream.map(subscription_chunk_to_js))
    }

    #[wasm_bindgen(js_name = subscribeRelationQuery)]
    pub fn subscribe_relation_query(
        &self,
        query_json: String,
        opts: JsValue,
    ) -> Result<JsValue, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let query = relation_query_from_json(&query_json)?;
        let stream = self
            .inner
            .subscribe_relation_query(&query, opts)
            .map_err(to_js_error)?;
        readable_stream_from_stream(stream.map(subscription_chunk_to_js))
    }

    #[wasm_bindgen(js_name = subscribeRelationQueryForIdentity)]
    pub fn subscribe_relation_query_for_identity(
        &self,
        query_json: String,
        author: Vec<u8>,
        opts: JsValue,
    ) -> Result<JsValue, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let author = author_id_from_bytes(&author)?;
        let query = relation_query_from_json(&query_json)?;
        let stream = self
            .inner
            .subscribe_relation_query_for_identity(&query, opts, author)
            .map_err(to_js_error)?;
        readable_stream_from_stream(stream.map(subscription_chunk_to_js))
    }

    #[wasm_bindgen(js_name = attachQuery)]
    pub fn attach_query(
        &self,
        query: &WasmPreparedQuery,
        opts: JsValue,
    ) -> Result<WasmQueryAttachment, JsValue> {
        let opts = read_opts_from_js(opts)?;
        Ok(WasmQueryAttachment {
            inner: self
                .inner
                .attach_query(&query.inner, opts)
                .map_err(to_js_error)?,
        })
    }

    #[wasm_bindgen(js_name = attachQueryForIdentity)]
    pub fn attach_query_for_identity(
        &self,
        query: &WasmPreparedQuery,
        author: Vec<u8>,
        opts: JsValue,
    ) -> Result<WasmQueryAttachment, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let author = author_id_from_bytes(&author)?;
        Ok(WasmQueryAttachment {
            inner: self
                .inner
                .attach_query_for_identity(&query.inner, opts, author)
                .map_err(to_js_error)?,
        })
    }

    #[wasm_bindgen(js_name = queryAttachmentIsCovered)]
    pub fn query_attachment_is_covered(&self, attachment: &WasmQueryAttachment) -> bool {
        self.inner.query_attachment_is_covered(&attachment.inner)
    }

    #[wasm_bindgen(js_name = detachQuery)]
    pub fn detach_query(&self, attachment: &WasmQueryAttachment) {
        self.inner.detach_query(attachment.inner.clone());
    }

    #[wasm_bindgen(js_name = setTickScheduler)]
    pub fn set_tick_scheduler(&self, callback: js_sys::Function) {
        self.inner.set_tick_scheduler(callback);
    }

    #[wasm_bindgen(js_name = insertEncoded)]
    pub fn insert_encoded(&self, table: String, cells: Vec<u8>) -> Result<WasmWrite, JsValue> {
        let cells = decode_cells(&cells)?;
        self.inner.insert(&table, cells)
    }

    #[wasm_bindgen(js_name = canInsertEncoded)]
    pub fn can_insert_encoded(&self, table: String, cells: Vec<u8>) -> Result<bool, JsValue> {
        let cells = decode_cells(&cells)?;
        match &self.inner {
            WasmDbInner::Memory(db) => db.can_insert(&table, cells).map_err(to_js_error),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => db.can_insert(&table, cells).map_err(to_js_error),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = canInsertEncodedForIdentity)]
    pub fn can_insert_encoded_for_identity(
        &self,
        table: String,
        cells: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<bool, JsValue> {
        let cells = decode_cells(&cells)?;
        let author = author_id_from_bytes(&author)?;
        match &self.inner {
            WasmDbInner::Memory(db) => db
                .can_insert_for_identity(&table, cells, author)
                .map_err(to_js_error),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => db
                .can_insert_for_identity(&table, cells, author)
                .map_err(to_js_error),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = canReadForIdentity)]
    pub fn can_read_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<bool, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let author = author_id_from_bytes(&author)?;
        match &self.inner {
            WasmDbInner::Memory(db) => db
                .can_read_for_identity(&table, row_id, author)
                .map_err(to_js_error),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => db
                .can_read_for_identity(&table, row_id, author)
                .map_err(to_js_error),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = insertWithIdEncoded)]
    pub fn insert_with_id_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        self.inner.insert_with_id(
            &table,
            row_id,
            cells,
            updated_at_ms.map(|value| value as u64),
        )
    }

    #[wasm_bindgen(js_name = insertWithIdEncodedForIdentity)]
    pub fn insert_with_id_encoded_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let author = author_id_from_bytes(&author)?;
        self.inner.insert_with_id_for_identity(
            author,
            &table,
            row_id,
            cells,
            updated_at_ms.map(|value| value as u64),
        )
    }

    #[wasm_bindgen(js_name = updateEncoded)]
    pub fn update_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let patch = decode_cells(&patch)?;
        self.inner.update(
            &table,
            row_id,
            patch,
            updated_at_ms.map(|value| value as u64),
        )
    }

    #[wasm_bindgen(js_name = updateEncodedForIdentity)]
    pub fn update_encoded_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        author: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let patch = decode_cells(&patch)?;
        let author = author_id_from_bytes(&author)?;
        self.inner.update_for_identity(
            author,
            &table,
            row_id,
            patch,
            updated_at_ms.map(|value| value as u64),
        )
    }

    #[wasm_bindgen(js_name = canUpdateEncodedForIdentity)]
    pub fn can_update_encoded_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        _patch: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<bool, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let author = author_id_from_bytes(&author)?;
        match &self.inner {
            WasmDbInner::Memory(db) => db
                .can_update_for_identity(&table, row_id, author)
                .map_err(to_js_error),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => db
                .can_update_for_identity(&table, row_id, author)
                .map_err(to_js_error),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = canDeleteForIdentity)]
    pub fn can_delete_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<bool, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let author = author_id_from_bytes(&author)?;
        match &self.inner {
            WasmDbInner::Memory(db) => db
                .can_delete_for_identity(&table, row_id, author)
                .map_err(to_js_error),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => db
                .can_delete_for_identity(&table, row_id, author)
                .map_err(to_js_error),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = upsertEncoded)]
    pub fn upsert_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        self.inner.upsert(
            &table,
            row_id,
            cells,
            updated_at_ms.map(|value| value as u64),
        )
    }

    #[wasm_bindgen(js_name = upsertEncodedForIdentity)]
    pub fn upsert_encoded_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let author = author_id_from_bytes(&author)?;
        self.inner.upsert_for_identity(
            author,
            &table,
            row_id,
            cells,
            updated_at_ms.map(|value| value as u64),
        )
    }

    #[wasm_bindgen(js_name = delete)]
    pub fn delete(
        &self,
        table: String,
        row_id: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        self.inner
            .delete(&table, row_id, updated_at_ms.map(|value| value as u64))
    }

    #[wasm_bindgen(js_name = deleteForIdentity)]
    pub fn delete_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        author: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let author = author_id_from_bytes(&author)?;
        self.inner.delete_for_identity(
            author,
            &table,
            row_id,
            updated_at_ms.map(|value| value as u64),
        )
    }

    #[wasm_bindgen(js_name = restoreEncoded)]
    pub fn restore_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        self.inner.restore(
            &table,
            row_id,
            cells,
            updated_at_ms.map(|value| value as u64),
        )
    }

    #[wasm_bindgen(js_name = restoreEncodedForIdentity)]
    pub fn restore_encoded_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let author = author_id_from_bytes(&author)?;
        self.inner.restore_for_identity(
            author,
            &table,
            row_id,
            cells,
            updated_at_ms.map(|value| value as u64),
        )
    }

    #[wasm_bindgen(js_name = tick)]
    pub fn tick(&self) -> Result<(), JsValue> {
        self.inner.tick().map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = connectUpstream)]
    pub fn connect_upstream(&self) -> Result<WasmTransport, JsValue> {
        let queues = WasmWireQueues::default();
        let transport = Box::new(WireTransportAdapter::current(WasmWireTransport {
            queues: queues.clone(),
        }));
        let inner = match &self.inner {
            WasmDbInner::Memory(db) => WasmTransportInner::Memory {
                db: Rc::clone(db),
                connection: Some(db.connect_upstream(transport)),
            },
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => WasmTransportInner::Browser {
                db: Rc::clone(db),
                connection: Some(db.connect_upstream(transport)),
            },
            WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
        };
        Ok(WasmTransport { inner, queues })
    }

    #[wasm_bindgen(js_name = acceptSubscriber)]
    pub fn accept_subscriber(&self, identity: Vec<u8>) -> Result<WasmTransport, JsValue> {
        let identity = author_id_from_bytes(&identity)?;
        let queues = WasmWireQueues::default();
        let transport = Box::new(WireTransportAdapter::current(WasmWireTransport {
            queues: queues.clone(),
        }));
        let inner = match &self.inner {
            WasmDbInner::Memory(db) => WasmTransportInner::Memory {
                db: Rc::clone(db),
                connection: Some(db.accept_subscriber(transport, identity)),
            },
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => WasmTransportInner::Browser {
                db: Rc::clone(db),
                connection: Some(db.accept_subscriber(transport, identity)),
            },
            WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
        };
        Ok(WasmTransport { inner, queues })
    }

    #[wasm_bindgen(js_name = mergeableTx)]
    pub fn mergeable_tx(&self) -> Result<WasmTx, JsValue> {
        Ok(WasmTx {
            db: self.inner.clone(),
            kind: WasmTxKind::Mergeable { author: None },
            writes: Some(Vec::new()),
            open_tx: Some(self.inner.begin_exclusive().map_err(to_js_error)?),
        })
    }

    #[wasm_bindgen(js_name = mergeableTxForIdentity)]
    pub fn mergeable_tx_for_identity(&self, author: Vec<u8>) -> Result<WasmTx, JsValue> {
        Ok(WasmTx {
            db: self.inner.clone(),
            kind: WasmTxKind::Mergeable {
                author: Some(author_id_from_bytes(&author)?),
            },
            writes: Some(Vec::new()),
            open_tx: Some(self.inner.begin_exclusive().map_err(to_js_error)?),
        })
    }

    #[wasm_bindgen(js_name = exclusiveTx)]
    pub fn exclusive_tx(&self) -> Result<WasmTx, JsValue> {
        Ok(WasmTx {
            db: self.inner.clone(),
            kind: WasmTxKind::Exclusive,
            writes: Some(Vec::new()),
            open_tx: Some(self.inner.begin_exclusive().map_err(to_js_error)?),
        })
    }

    #[wasm_bindgen(js_name = close)]
    pub fn close(&mut self) -> Result<bool, JsValue> {
        let inner = std::mem::replace(&mut self.inner, WasmDbInner::Closed);
        match inner {
            WasmDbInner::Memory(db) => {
                db.close().map_err(to_js_error)?;
                Ok(true)
            }
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => {
                db.close().map_err(to_js_error)?;
                Ok(true)
            }
            WasmDbInner::Closed => Ok(false),
        }
    }
}

#[wasm_bindgen]
impl WasmTransport {
    #[wasm_bindgen(js_name = sendWireFrame)]
    pub fn send_wire_frame(&self, frame: Vec<u8>) {
        self.queues.inbound.borrow_mut().push_back(frame);
    }

    #[wasm_bindgen(js_name = sendWireFrames)]
    pub fn send_wire_frames(&self, frames: js_sys::Array) {
        let mut inbound = self.queues.inbound.borrow_mut();
        for frame in frames.iter() {
            inbound.push_back(js_sys::Uint8Array::new(&frame).to_vec());
        }
    }

    #[wasm_bindgen(js_name = recvWireFrames)]
    pub fn recv_wire_frames(&self) -> js_sys::Array {
        let frames = js_sys::Array::new();
        let mut outbound = self.queues.outbound.borrow_mut();
        while let Some(frame) = outbound.pop_front() {
            frames.push(&js_sys::Uint8Array::from(frame.as_slice()).into());
        }
        frames
    }

    #[wasm_bindgen(js_name = tick)]
    pub fn tick(&self) -> Result<u32, JsValue> {
        self.inner.tick()
    }

    #[wasm_bindgen(js_name = close)]
    pub fn close(&mut self) -> bool {
        self.inner.close()
    }
}

#[wasm_bindgen]
impl WasmTx {
    #[wasm_bindgen(js_name = insertWithIdEncoded)]
    pub fn insert_with_id_encoded(
        &mut self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<(), JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let now_ms = updated_at_ms.map(|value| value as u64);
        let open_tx = self.open_tx_for_read()?;
        self.db
            .exclusive_write(open_tx, &table, row_id, cells.clone())
            .map_err(to_js_error)?;
        self.pending_writes()?.push(WasmTxWrite::Insert {
            table,
            row_id,
            cells,
            now_ms,
        });
        Ok(())
    }

    #[wasm_bindgen(js_name = updateEncoded)]
    pub fn update_encoded(
        &mut self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<(), JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let patch = decode_cells(&patch)?;
        let now_ms = updated_at_ms.map(|value| value as u64);
        let open_tx = self.open_tx_for_read()?;
        self.db
            .exclusive_update(open_tx, &table, row_id, patch.clone())
            .map_err(to_js_error)?;
        self.pending_writes()?.push(WasmTxWrite::Update {
            table,
            row_id,
            patch,
            now_ms,
        });
        Ok(())
    }

    #[wasm_bindgen(js_name = upsertEncoded)]
    pub fn upsert_encoded(
        &mut self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<(), JsValue> {
        self.insert_with_id_encoded(table, row_id, cells, updated_at_ms)
    }

    #[wasm_bindgen(js_name = delete)]
    pub fn delete(
        &mut self,
        table: String,
        row_id: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<(), JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let open_tx = self.open_tx_for_read()?;
        self.db
            .exclusive_delete(open_tx, &table, row_id)
            .map_err(to_js_error)?;
        self.pending_writes()?.push(WasmTxWrite::Delete {
            table,
            row_id,
            now_ms: updated_at_ms.map(|value| value as u64),
        });
        Ok(())
    }

    #[wasm_bindgen(js_name = restoreEncoded)]
    pub fn restore_encoded(
        &mut self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms: Option<f64>,
    ) -> Result<(), JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let now_ms = updated_at_ms.map(|value| value as u64);
        let open_tx = self.open_tx_for_read()?;
        self.db
            .exclusive_write(open_tx, &table, row_id, cells.clone())
            .map_err(to_js_error)?;
        self.pending_writes()?.push(WasmTxWrite::Restore {
            table,
            row_id,
            cells,
            now_ms,
        });
        Ok(())
    }

    #[wasm_bindgen(js_name = commit)]
    pub fn commit(&mut self) -> Result<WasmWrite, JsValue> {
        let writes = self
            .writes
            .take()
            .ok_or_else(|| JsValue::from_str("transaction is already closed"))?;
        let open_tx = self
            .open_tx
            .take()
            .ok_or_else(|| JsValue::from_str("transaction is already closed"))?;
        match (&self.db, self.kind) {
            (WasmDbInner::Memory(db), WasmTxKind::Mergeable { author }) => {
                let result = commit_wasm_tx_memory(db, author, writes);
                self.db.abandon_exclusive(open_tx).map_err(to_js_error)?;
                result
            }
            (WasmDbInner::Memory(db), WasmTxKind::Exclusive) => {
                let _ = writes;
                let tx_id = self.db.commit_exclusive(open_tx).map_err(to_js_error)?;
                wasm_tx_write(
                    tx_id,
                    Some(WasmWriteInner::MemoryTx {
                        db: Rc::clone(db),
                        tx_id,
                    }),
                )
            }
            #[cfg(target_arch = "wasm32")]
            (WasmDbInner::Browser(db), WasmTxKind::Mergeable { author }) => {
                let result = commit_wasm_tx_browser(db, author, writes);
                self.db.abandon_exclusive(open_tx).map_err(to_js_error)?;
                result
            }
            #[cfg(target_arch = "wasm32")]
            (WasmDbInner::Browser(db), WasmTxKind::Exclusive) => {
                let _ = writes;
                let tx_id = self.db.commit_exclusive(open_tx).map_err(to_js_error)?;
                wasm_tx_write(
                    tx_id,
                    Some(WasmWriteInner::BrowserTx {
                        db: Rc::clone(db),
                        tx_id,
                    }),
                )
            }
            (WasmDbInner::Closed, _) => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = rollback)]
    pub fn rollback(&mut self) -> Result<(), JsValue> {
        self.writes
            .take()
            .ok_or_else(|| JsValue::from_str("transaction is already closed"))?;
        let open_tx = self
            .open_tx
            .take()
            .ok_or_else(|| JsValue::from_str("transaction is already closed"))?;
        self.db.abandon_exclusive(open_tx).map_err(to_js_error)?;
        Ok(())
    }

    fn pending_writes(&mut self) -> Result<&mut Vec<WasmTxWrite>, JsValue> {
        self.writes
            .as_mut()
            .ok_or_else(|| JsValue::from_str("transaction is already closed"))
    }

    fn open_tx_for_read(&self) -> Result<OpenTxId, JsValue> {
        self.open_tx
            .ok_or_else(|| JsValue::from_str("transaction is already closed"))
    }
}

fn read_rows_for_transaction(
    db: &WasmDbInner,
    query: &WasmPreparedQuery,
    tx: &WasmTx,
    author: Option<AuthorId>,
    opts: JsValue,
) -> Result<Vec<jazz::node::CurrentRow>, JsValue> {
    let _opts = read_opts_from_js(opts)?;
    let tx_id = tx.open_tx_for_read()?;
    match author {
        Some(author) => db
            .exclusive_all_for_identity(tx_id, &query.inner, author)
            .map_err(to_js_error),
        None => db.exclusive_all(tx_id, &query.inner).map_err(to_js_error),
    }
}

fn decode_cells(bytes: &[u8]) -> Result<RowCells, JsValue> {
    let (descriptor, raw): (RecordDescriptor, Vec<u8>) =
        postcard::from_bytes(bytes).map_err(|err| to_js_error(format!("decode cells: {err}")))?;
    let record = BorrowedRecord::new(&raw, &descriptor);
    let values = record
        .to_values()
        .map_err(|err| to_js_error(format!("decode cell record: {err}")))?;
    let mut cells = RowCells::new();
    for (field, value) in descriptor.fields().iter().zip(values) {
        let Some(name) = &field.name else {
            return Err(JsValue::from_str("encoded cells must use named fields"));
        };
        cells.insert(name.clone(), value);
    }
    Ok(cells)
}

fn decode_open_args(
    schema: &[u8],
    config: &[u8],
) -> Result<(JazzSchema, WasmOpenDbConfig), JsValue> {
    let schema: JazzSchema =
        postcard::from_bytes(schema).map_err(|err| to_js_error(format!("decode schema: {err}")))?;
    let config: WasmOpenDbConfig = postcard::from_bytes(config)
        .map_err(|err| to_js_error(format!("decode open config: {err}")))?;
    Ok((schema, config))
}

fn relation_query_from_json(query_json: &str) -> Result<RelationQuery, JsValue> {
    let value: serde_json::Value = serde_json::from_str(query_json)
        .map_err(|err| to_js_error(format!("decode query json: {err}")))?;
    let relation_ir = value
        .get("relation_ir")
        .ok_or_else(|| to_js_error("relation query json is missing relation_ir"))?
        .clone();
    let rel: RelationExpr = serde_json::from_value(relation_ir)
        .map_err(|err| to_js_error(format!("decode relation_ir: {err}")))?;
    Ok(RelationQuery { rel })
}

fn open_db<S>(
    schema: JazzSchema,
    storage: S,
    config: WasmOpenDbConfig,
) -> Result<Db<S>, jazz::db::Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let mut db_config = DbConfig::new(schema, storage, config.identity.into());
    if let Some(seed) = config.row_id_seed {
        db_config = db_config.with_id_source(SeededRowIdSource::new(seed));
    }
    if config.history_complete {
        block_on(Db::open_history_complete(db_config))
    } else {
        block_on(Db::open(db_config))
    }
}

fn tick_connection<S>(connection: &Option<Rc<RefCell<PeerConnection<S>>>>) -> Result<u32, JsValue>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let Some(connection) = connection else {
        return Ok(0);
    };
    let stats = connection.borrow_mut().tick().map_err(to_js_error)?;
    Ok(stats.subscription_events as u32)
}

fn wait_for_tx<S>(db: &Db<S>, tx_id: TxId, tier: DurabilityTier) -> Result<(), JsValue>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    if tier <= DurabilityTier::Local {
        return Ok(());
    }
    let state = db.write_state(tx_id).map_err(to_js_error)?;
    if state.durability >= tier {
        return Ok(());
    }
    Err(JsValue::from_str(&format!(
        "transaction has not reached requested tier {tier:?}"
    )))
}

fn commit_wasm_tx<S>(
    db: &Db<S>,
    author: Option<AuthorId>,
    writes: Vec<WasmTxWrite>,
) -> Result<TxId, JsValue>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let mut tx = match author {
        Some(author) => db.mergeable_tx_for_identity(author),
        None => db.mergeable_tx(),
    };
    for write in writes {
        match write {
            WasmTxWrite::Insert {
                table,
                row_id,
                cells,
                now_ms,
            } => match now_ms {
                Some(now_ms) => tx.insert_with_id_at_ms(&table, row_id, cells, now_ms),
                None => tx.insert_with_id(&table, row_id, cells),
            }
            .map_err(to_js_error)?,
            WasmTxWrite::Update {
                table,
                row_id,
                patch,
                now_ms,
            } => match now_ms {
                Some(now_ms) => tx.update_at_ms(&table, row_id, patch, now_ms),
                None => tx.update(&table, row_id, patch),
            }
            .map_err(to_js_error)?,
            WasmTxWrite::Delete {
                table,
                row_id,
                now_ms,
            } => match now_ms {
                Some(now_ms) => tx.delete_at_ms(&table, row_id, now_ms),
                None => tx.delete(&table, row_id),
            }
            .map_err(to_js_error)?,
            WasmTxWrite::Restore {
                table,
                row_id,
                cells,
                now_ms,
            } => match now_ms {
                Some(now_ms) => tx.restore_at_ms(&table, row_id, cells, now_ms),
                None => tx.restore(&table, row_id, cells),
            }
            .map_err(to_js_error)?,
        }
    }
    tx.commit().map_err(to_js_error)
}

fn commit_wasm_tx_memory(
    db: &Rc<Db<MemoryStorage>>,
    author: Option<AuthorId>,
    writes: Vec<WasmTxWrite>,
) -> Result<WasmWrite, JsValue> {
    let tx_id = commit_wasm_tx(db, author, writes)?;
    wasm_tx_write(
        tx_id,
        Some(WasmWriteInner::MemoryTx {
            db: Rc::clone(db),
            tx_id,
        }),
    )
}

#[cfg(target_arch = "wasm32")]
fn commit_wasm_tx_browser(
    db: &Rc<Db<OpfsStorage>>,
    author: Option<AuthorId>,
    writes: Vec<WasmTxWrite>,
) -> Result<WasmWrite, JsValue> {
    let tx_id = commit_wasm_tx(db, author, writes)?;
    wasm_tx_write(
        tx_id,
        Some(WasmWriteInner::BrowserTx {
            db: Rc::clone(db),
            tx_id,
        }),
    )
}

fn row_uuid_from_bytes(bytes: &[u8]) -> Result<RowUuid, JsValue> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| JsValue::from_str("row id must be 16 bytes"))?;
    Ok(RowUuid::from_bytes(bytes))
}

fn author_id_from_bytes(bytes: &[u8]) -> Result<AuthorId, JsValue> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| JsValue::from_str("author id must be 16 bytes"))?;
    Ok(AuthorId::from_bytes(bytes))
}

fn set_identity_claims<S>(db: &Db<S>, author: AuthorId)
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let subject = author.0.to_string();
    db.set_identity_claims(
        author,
        BTreeMap::from([
            ("subject".to_owned(), Value::String(subject.clone())),
            ("sub".to_owned(), Value::String(subject.clone())),
            ("user_id".to_owned(), Value::String(subject)),
        ]),
    );
}

fn claims_from_js(author: AuthorId, claims: JsValue) -> Result<BTreeMap<String, Value>, JsValue> {
    let raw: serde_json::Value = serde_wasm_bindgen::from_value(claims).map_err(to_js_error)?;
    let mut claims = match raw {
        serde_json::Value::Null => BTreeMap::new(),
        serde_json::Value::Object(map) => map
            .into_iter()
            .map(|(key, value)| Ok((key, claim_value_from_json(value)?)))
            .collect::<Result<BTreeMap<_, _>, JsValue>>()?,
        _ => return Err(JsValue::from_str("identity claims must be an object")),
    };
    let subject = author.0.to_string();
    claims
        .entry("subject".to_owned())
        .or_insert_with(|| Value::String(subject.clone()));
    claims
        .entry("sub".to_owned())
        .or_insert_with(|| Value::String(subject.clone()));
    claims
        .entry("user_id".to_owned())
        .or_insert_with(|| Value::String(subject));
    Ok(claims)
}

fn claim_value_from_json(value: serde_json::Value) -> Result<Value, JsValue> {
    Ok(match value {
        serde_json::Value::Null => Value::Nullable(None),
        serde_json::Value::Bool(value) => Value::Bool(value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                Value::U64(value)
            } else if let Some(value) = value.as_f64() {
                Value::F64(value)
            } else {
                return Err(JsValue::from_str("unsupported numeric claim value"));
            }
        }
        serde_json::Value::String(value) => Value::String(value),
        serde_json::Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(claim_value_from_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        serde_json::Value::Object(_) => {
            return Err(JsValue::from_str("nested object claims are not supported"));
        }
    })
}

fn wasm_write_memory(
    db: Rc<Db<MemoryStorage>>,
    write: WriteHandle<MemoryStorage>,
) -> Result<WasmWrite, JsValue> {
    let tx_id = write.mergeable_tx_id();
    let result = WasmWriteResult {
        row_id: write.row_uuid(),
        tx_id,
    };
    Ok(WasmWrite {
        payload: postcard::to_allocvec(&result).map_err(to_js_error)?,
        inner: Some(WasmWriteInner::MemoryTx { db, tx_id }),
    })
}

#[cfg(target_arch = "wasm32")]
fn wasm_write_browser(
    db: Rc<Db<OpfsStorage>>,
    write: WriteHandle<OpfsStorage>,
) -> Result<WasmWrite, JsValue> {
    let tx_id = write.mergeable_tx_id();
    let result = WasmWriteResult {
        row_id: write.row_uuid(),
        tx_id,
    };
    Ok(WasmWrite {
        payload: postcard::to_allocvec(&result).map_err(to_js_error)?,
        inner: Some(WasmWriteInner::BrowserTx { db, tx_id }),
    })
}

fn wasm_tx_write(tx_id: TxId, inner: Option<WasmWriteInner>) -> Result<WasmWrite, JsValue> {
    let result = WasmWriteResult {
        row_id: RowUuid::from_bytes([0; 16]),
        tx_id,
    };
    Ok(WasmWrite {
        payload: postcard::to_allocvec(&result).map_err(to_js_error)?,
        inner,
    })
}

fn read_opts_from_js(value: JsValue) -> Result<ReadOpts, JsValue> {
    let mut opts = ReadOpts::default();
    if value.is_undefined() || value.is_null() {
        return Ok(opts);
    }
    reject_unsupported_non_default_read_view(&value)?;
    if let Some(tier) = optional_string_prop(&value, "tier")? {
        opts.tier = durability_tier_from_str(&tier)?;
    }
    if let Some(local_updates) = optional_string_prop(&value, "local_updates")? {
        opts.local_updates = match local_updates.as_str() {
            "Immediate" | "immediate" => LocalUpdates::Immediate,
            "Deferred" | "deferred" => LocalUpdates::Deferred,
            other => return Err(JsValue::from_str(&format!("unknown local_updates {other}"))),
        };
    }
    if optional_bool_prop(&value, "propagate")? == Some(false) {
        opts.propagation = Propagation::LocalOnly;
    }
    if let Some(propagation) = optional_string_prop(&value, "propagation")? {
        opts.propagation = match propagation.as_str() {
            "Full" | "full" => Propagation::Full,
            "LocalOnly" | "local_only" | "localOnly" => Propagation::LocalOnly,
            other => return Err(JsValue::from_str(&format!("unknown propagation {other}"))),
        };
    }
    if let Some(include_deleted) = optional_bool_prop(&value, "include_deleted")? {
        opts.include_deleted = include_deleted;
    }
    Ok(opts)
}

fn reject_unsupported_non_default_read_view(value: &JsValue) -> Result<(), JsValue> {
    for name in ["read_view", "readView"] {
        let prop = js_sys::Reflect::get(value, &JsValue::from_str(name))?;
        if !prop.is_undefined() && !prop.is_null() {
            return Err(JsValue::from_str(
                "non-default read_view is not supported yet",
            ));
        }
    }
    Ok(())
}

fn durability_tier_from_str(tier: &str) -> Result<DurabilityTier, JsValue> {
    match tier {
        "None" | "none" => Ok(DurabilityTier::None),
        "Local" | "local" => Ok(DurabilityTier::Local),
        "Edge" | "edge" => Ok(DurabilityTier::Edge),
        "Global" | "global" => Ok(DurabilityTier::Global),
        other => Err(JsValue::from_str(&format!(
            "unknown durability tier {other}"
        ))),
    }
}

fn write_state_to_js(state: jazz::db::WriteState) -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(&state).map_err(to_js_error)
}

fn optional_string_prop(value: &JsValue, name: &str) -> Result<Option<String>, JsValue> {
    let prop = js_sys::Reflect::get(value, &JsValue::from_str(name))?;
    if prop.is_undefined() || prop.is_null() {
        return Ok(None);
    }
    prop.as_string()
        .map(Some)
        .ok_or_else(|| JsValue::from_str(&format!("{name} must be a string")))
}

fn optional_bool_prop(value: &JsValue, name: &str) -> Result<Option<bool>, JsValue> {
    let prop = js_sys::Reflect::get(value, &JsValue::from_str(name))?;
    if prop.is_undefined() || prop.is_null() {
        return Ok(None);
    }
    prop.as_bool()
        .map(Some)
        .ok_or_else(|| JsValue::from_str(&format!("{name} must be a boolean")))
}

fn encode_rows(rows: &[jazz::node::CurrentRow]) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&row_batches(rows))
}

fn encode_relation_snapshot(
    snapshot: &jazz::node::RelationSnapshot,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&WasmRelationSnapshot {
        cursor: 0,
        root_count: snapshot.root_count as u64,
        rows: row_batches(&snapshot.rows),
        edges: snapshot.edges.iter().map(wasm_relation_edge).collect(),
    })
}

fn encode_subscription_delta<'a>(
    added: &'a [jazz::node::CurrentRow],
    updated: &'a [jazz::node::CurrentRow],
    removed: &[jazz::db::RemovedRow],
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&WasmSubscriptionDelta {
        added: row_batches(added),
        updated: row_batches(updated),
        removed: removed
            .iter()
            .map(|row| WasmRemovedRow {
                table: row.table.clone(),
                row_id: row.row_uuid,
            })
            .collect(),
    })
}

fn encode_relation_subscription_delta<'a>(
    added: &'a [jazz::node::CurrentRow],
    updated: &'a [jazz::node::CurrentRow],
    removed: &[jazz::db::RemovedRow],
    added_related: &'a [jazz::node::CurrentRow],
    added_edges: &[jazz::node::RelationEdge],
    removed_edges: &[jazz::db::RemovedRelationEdge],
) -> Result<Vec<u8>, postcard::Error> {
    let mut relation_added = Vec::with_capacity(added.len() + added_related.len());
    relation_added.extend_from_slice(added);
    relation_added.extend_from_slice(added_related);
    postcard::to_allocvec(&WasmRelationSubscriptionDelta {
        base_cursor: None,
        cursor: 0,
        added: row_batches(&relation_added),
        updated: row_batches(updated),
        removed: removed
            .iter()
            .map(|row| WasmRemovedRow {
                table: row.table.clone(),
                row_id: row.row_uuid,
            })
            .collect(),
        added_edges: added_edges.iter().map(wasm_relation_edge).collect(),
        removed_edges: removed_edges.iter().map(wasm_relation_edge).collect(),
    })
}

fn row_batches(rows: &[jazz::node::CurrentRow]) -> Vec<WasmRowBatch<'_>> {
    let mut batches: Vec<WasmRowBatch<'_>> = Vec::new();
    for row in rows {
        let (descriptor, raw) = row.encoded_record();
        match batches.last_mut() {
            Some(batch) if batch.table == row.table() && batch.descriptor == *descriptor => {
                batch.rows.push(wasm_row(row, raw));
            }
            _ => batches.push(WasmRowBatch {
                table: row.table(),
                descriptor: *descriptor,
                rows: vec![wasm_row(row, raw)],
            }),
        }
    }
    batches
}

fn wasm_relation_edge(edge: &jazz::node::RelationEdge) -> WasmRelationEdge {
    WasmRelationEdge {
        source_table: edge.source_table.clone(),
        source_row_id: edge.source_row,
        relation: edge.relation.clone(),
        target_table: edge.target_table.clone(),
        target_row_id: edge.target_row,
    }
}

fn wasm_row<'a>(row: &jazz::node::CurrentRow, raw: &'a [u8]) -> WasmRow<'a> {
    WasmRow {
        row_id: row.row_uuid(),
        deleted: row.is_deleted(),
        raw,
    }
}

fn subscription_chunk_to_js(event: SubscriptionEvent) -> Result<JsValue, JsValue> {
    let object = js_sys::Object::new();
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            added_related,
            added_edges,
            removed_edges,
            settled,
            tier,
        } => {
            let delta =
                encode_subscription_delta(&added, &updated, &removed).map_err(to_js_error)?;
            let relation_delta = encode_relation_subscription_delta(
                &added,
                &updated,
                &removed,
                &added_related,
                &added_edges,
                &removed_edges,
            )
            .map_err(to_js_error)?;
            set_prop(&object, "type", JsValue::from_str("delta"))?;
            set_prop(
                &object,
                "delta",
                js_sys::Uint8Array::from(delta.as_slice()).into(),
            )?;
            set_prop(
                &object,
                "relation_delta",
                js_sys::Uint8Array::from(relation_delta.as_slice()).into(),
            )?;
            set_prop(&object, "reset", JsValue::from_bool(reset))?;
            set_prop(&object, "settled", JsValue::from_bool(settled))?;
            set_prop(&object, "tier", JsValue::from_str(&format!("{tier:?}")))?;
        }
        SubscriptionEvent::Closed => {
            set_prop(&object, "type", JsValue::from_str("closed"))?;
        }
    };
    Ok(object.into())
}

fn set_prop(object: &js_sys::Object, name: &str, value: JsValue) -> Result<(), JsValue> {
    js_sys::Reflect::set(object, &JsValue::from_str(name), &value).map(|_| ())
}

type JsResultStream = dyn Stream<Item = Result<JsValue, JsValue>>;

fn readable_stream_from_stream<St>(stream: St) -> Result<JsValue, JsValue>
where
    St: Stream<Item = Result<JsValue, JsValue>> + 'static,
{
    let stream: Pin<Box<JsResultStream>> = Box::pin(stream);
    let state = std::rc::Rc::new(std::cell::RefCell::new(Some(stream)));
    let source = js_sys::Object::new();

    let pull_state = std::rc::Rc::clone(&state);
    let pull = Closure::<dyn FnMut(JsValue) -> js_sys::Promise>::new(move |controller| {
        let pull_state = std::rc::Rc::clone(&pull_state);
        future_to_promise(async move {
            let Some(mut stream) = pull_state.borrow_mut().take() else {
                return Err(JsValue::from_str(
                    "subscription stream pull already in progress",
                ));
            };
            let next = stream.next().await;
            match next {
                Some(Ok(chunk)) => {
                    *pull_state.borrow_mut() = Some(stream);
                    call_controller_method(&controller, "enqueue", Some(&chunk))?;
                }
                Some(Err(error)) => {
                    call_controller_method(&controller, "error", Some(&error))?;
                    return Err(error);
                }
                None => {
                    call_controller_method(&controller, "close", None)?;
                }
            }
            Ok(JsValue::undefined())
        })
    });
    js_sys::Reflect::set(&source, &JsValue::from_str("pull"), pull.as_ref())?;
    pull.forget();

    let cancel_state = std::rc::Rc::clone(&state);
    let cancel = Closure::<dyn FnMut()>::new(move || {
        cancel_state.borrow_mut().take();
    });
    js_sys::Reflect::set(&source, &JsValue::from_str("cancel"), cancel.as_ref())?;
    cancel.forget();

    let strategy = js_sys::Object::new();
    js_sys::Reflect::set(
        &strategy,
        &JsValue::from_str("highWaterMark"),
        &JsValue::from_f64(0.0),
    )?;
    let args = js_sys::Array::new();
    args.push(&source);
    args.push(&strategy);
    let constructor =
        js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("ReadableStream"))?
            .dyn_into::<js_sys::Function>()?;
    js_sys::Reflect::construct(&constructor, &args)
}

fn call_controller_method(
    controller: &JsValue,
    method: &str,
    arg: Option<&JsValue>,
) -> Result<(), JsValue> {
    let function = js_sys::Reflect::get(controller, &JsValue::from_str(method))?
        .dyn_into::<js_sys::Function>()?;
    match arg {
        Some(arg) => function.call1(controller, arg)?,
        None => function.call0(controller)?,
    };
    Ok(())
}

fn to_js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}
