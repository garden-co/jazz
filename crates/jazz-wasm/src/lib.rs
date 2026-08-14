use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::pin::Pin;
use std::rc::Rc;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use futures_util::{Stream, StreamExt};
use jazz::db::{
    block_on, ConnectionSessionContext, Db, DbConfig, DbIdentity, ExclusiveTxOps,
    InitialSyncFlushCadence, LocalUpdates, MergeableTxOps, MutationErrorCallback, PeerConnection,
    PermissionAdvice, PreparedQuery, Propagation, QueryAttachment, ReadOpts, RowCells,
    SeededRowIdSource, SubscriptionEvent, TickScheduler, TickUrgency, WireTransportAdapter,
    WriteHandle,
};
use jazz::groove::records::{BorrowedRecord, RecordDescriptor, Value};
#[cfg(target_arch = "wasm32")]
use jazz::groove::storage::OpfsStorage;
use jazz::groove::storage::{MemoryStorage, OrderedKvStorage, ReopenableStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::protocol::PermissionAdviceAction;
use jazz::query::{Query, RelationExpr, RelationQuery};
use jazz::schema::JazzSchema;
use jazz::tools::{BatchId, OpenBatchId};
use jazz::tx::{DurabilityTier, TxId};
use jazz::wire::{TransportError, WireAuthorityEndpoint, WireTransport};
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

fn advice_string(advice: PermissionAdvice) -> String {
    match advice {
        PermissionAdvice::Allowed => "allowed",
        PermissionAdvice::Denied => "denied",
        PermissionAdvice::Unknown => "unknown",
    }
    .to_owned()
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
    initial_sync_flush_every: Option<u32>,
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
    batch_id: BatchId,
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
    #[wasm_bindgen(getter, js_name = batchId)]
    pub fn batch_id(&self) -> String {
        self.batch_id.to_string()
    }

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
    pub fn wait(&self, tier: String) -> Result<js_sys::Promise, JsValue> {
        let tier = durability_tier_from_str(&tier)?;
        match &self.inner {
            Some(WasmWriteInner::MemoryTx { db, tx_id }) => {
                Ok(wait_promise(db.as_ref(), *tx_id, tier))
            }
            #[cfg(target_arch = "wasm32")]
            Some(WasmWriteInner::BrowserTx { db, tx_id }) => {
                Ok(wait_promise(db.as_ref(), *tx_id, tier))
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
    owns_runtime: bool,
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
    subscriber_identity: Option<AuthorId>,
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

macro_rules! with_wasm_db {
    ($inner:expr, |$db:ident| $body:expr) => {
        match $inner {
            WasmDbInner::Memory($db) => $body,
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser($db) => $body,
            WasmDbInner::Closed => panic!("WasmDb is closed"),
        }
    };
}

#[wasm_bindgen]
pub struct WasmPermissionAdviceRequest {
    promise: js_sys::Promise,
    cancel: Box<dyn Fn()>,
}

#[wasm_bindgen]
impl WasmPermissionAdviceRequest {
    #[wasm_bindgen(getter)]
    pub fn promise(&self) -> js_sys::Promise {
        self.promise.clone()
    }

    pub fn cancel(&self) {
        (self.cancel)();
    }
}

impl WasmDbInner {
    fn request_permission_advice(
        &self,
        action: PermissionAdviceAction,
    ) -> Result<WasmPermissionAdviceRequest, JsValue> {
        macro_rules! request {
            ($db:expr) => {{
                let db = Rc::clone($db);
                let future = db.request_permission_advice(action);
                let request_id = future.request_id();
                let cancel_db = Rc::clone(&db);
                WasmPermissionAdviceRequest {
                    promise: future_to_promise(async move {
                        Ok(JsValue::from_str(&advice_string(future.await)))
                    }),
                    cancel: Box::new(move || {
                        cancel_db.cancel_permission_advice_request(request_id)
                    }),
                }
            }};
        }
        Ok(match self {
            WasmDbInner::Memory(db) => request!(db),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => request!(db),
            WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
        })
    }

    fn register_schema_view(&self, schema: JazzSchema) -> Result<Self, String> {
        match self {
            Self::Memory(db) => Ok(Self::Memory(Rc::new(
                db.register_schema_view(schema)
                    .map_err(|error| error.to_string())?,
            ))),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => Ok(Self::Browser(Rc::new(
                db.register_schema_view(schema)
                    .map_err(|error| error.to_string())?,
            ))),
            Self::Closed => Err("WasmDb is closed".to_owned()),
        }
    }

    fn prepare_query(&self, query: &Query) -> Result<PreparedQuery, jazz::db::Error> {
        with_wasm_db!(self, |db| db.prepare_query(query))
    }

    fn all(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(db.all(query, opts)))
    }

    fn all_for_identity(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(
            db.all_for_identity(query, opts, author)
        ))
    }

    fn begin_exclusive(&self, id: OpenBatchId) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| db.begin_exclusive(id))
    }

    fn begin_mergeable(
        &self,
        id: OpenBatchId,
        author: Option<AuthorId>,
    ) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| match author {
            Some(author) => db.begin_mergeable_for_identity(id, author),
            None => db.begin_mergeable(id),
        })
    }

    fn exclusive_all_for_identity(
        &self,
        tx_id: OpenBatchId,
        query: &PreparedQuery,
        author: AuthorId,
        opts: ReadOpts,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        with_wasm_db!(self, |db| db
            .exclusive_tx_ref(tx_id)
            .all_prepared_for_identity_with_opts(query, author, opts))
    }

    fn exclusive_all(
        &self,
        tx_id: OpenBatchId,
        query: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        with_wasm_db!(self, |db| db
            .exclusive_tx_ref(tx_id)
            .all_prepared_with_opts(query, opts))
    }

    fn mergeable_all_for_identity(
        &self,
        tx_id: OpenBatchId,
        query: &PreparedQuery,
        author: AuthorId,
        opts: ReadOpts,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        with_wasm_db!(self, |db| db
            .mergeable_tx_ref(tx_id)
            .all_prepared_for_identity_with_opts(query, author, opts))
    }

    fn mergeable_all(
        &self,
        tx_id: OpenBatchId,
        query: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        with_wasm_db!(self, |db| db
            .mergeable_tx_ref(tx_id)
            .all_prepared_with_opts(query, opts))
    }

    fn abandon_transaction(&self, tx_id: OpenBatchId) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| db.abandon_transaction_handle(tx_id))
    }

    fn mergeable_insert(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row_id: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| match now_ms {
            Some(now_ms) => db
                .mergeable_tx_ref(tx_id)
                .insert_with_id_at_ms(table, row_id, cells, now_ms),
            None => db
                .mergeable_tx_ref(tx_id)
                .insert_with_id(table, row_id, cells),
        })
    }

    fn mergeable_update(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row_id: RowUuid,
        patch: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| match now_ms {
            Some(now_ms) => db
                .mergeable_tx_ref(tx_id)
                .update_at_ms(table, row_id, patch, now_ms),
            None => db.mergeable_tx_ref(tx_id).update(table, row_id, patch),
        })
    }

    fn mergeable_delete(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row_id: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| match now_ms {
            Some(now_ms) => db
                .mergeable_tx_ref(tx_id)
                .delete_at_ms(table, row_id, now_ms),
            None => db.mergeable_tx_ref(tx_id).delete(table, row_id),
        })
    }

    fn mergeable_restore(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row_id: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| match now_ms {
            Some(now_ms) => db
                .mergeable_tx_ref(tx_id)
                .restore_at_ms(table, row_id, cells, now_ms),
            None => db.mergeable_tx_ref(tx_id).restore(table, row_id, cells),
        })
    }

    fn exclusive_write(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row_id: RowUuid,
        cells: RowCells,
    ) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| db
            .exclusive_tx_ref(tx_id)
            .insert_with_id(table, row_id, cells))
    }

    fn exclusive_update(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row_id: RowUuid,
        patch: RowCells,
    ) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| db
            .exclusive_tx_ref(tx_id)
            .update(table, row_id, patch))
    }

    fn exclusive_delete(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row_id: RowUuid,
    ) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| db.exclusive_tx_ref(tx_id).delete(table, row_id))
    }

    fn exclusive_restore(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row_id: RowUuid,
        cells: RowCells,
    ) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| db
            .exclusive_tx_ref(tx_id)
            .restore(table, row_id, cells))
    }

    fn commit_exclusive(&self, tx_id: OpenBatchId) -> Result<TxId, jazz::db::Error> {
        with_wasm_db!(self, |db| db.commit_exclusive_handle(tx_id))
    }

    fn commit_mergeable(&self, tx_id: OpenBatchId) -> Result<TxId, jazz::db::Error> {
        with_wasm_db!(self, |db| db.commit_mergeable_handle(tx_id))
    }

    fn all_relation_snapshot(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<jazz::node::RelationSnapshot, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(db.all_relation_snapshot(query, opts)))
    }

    fn all_relation_snapshot_for_identity(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<jazz::node::RelationSnapshot, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(
            db.all_relation_snapshot_for_identity(query, opts, author)
        ))
    }

    fn all_relation_query(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<jazz::node::RelationSnapshot, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(db.all_relation_query(query, opts)))
    }

    fn all_relation_query_for_identity(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<jazz::node::RelationSnapshot, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(
            db.all_relation_query_for_identity(query, opts, author)
        ))
    }

    fn set_identity_claims(&self, author: AuthorId, claims: BTreeMap<String, Value>) {
        with_wasm_db!(self, |db| db.set_identity_claims(author, claims))
    }

    fn subscribe(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Pin<Box<dyn Stream<Item = SubscriptionEvent> + 'static>>, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(db.subscribe(query, opts)).map(
            |stream| Box::pin(stream) as Pin<Box<dyn Stream<Item = SubscriptionEvent>>>
        ))
    }

    fn subscribe_for_identity(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<Pin<Box<dyn Stream<Item = SubscriptionEvent> + 'static>>, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(
            db.subscribe_for_identity(query, opts, author)
        )
        .map(
            |stream| Box::pin(stream) as Pin<Box<dyn Stream<Item = SubscriptionEvent>>>
        ))
    }

    fn subscribe_relation_query(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<Pin<Box<dyn Stream<Item = SubscriptionEvent> + 'static>>, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(
            db.subscribe_relation_query(query, opts)
        )
        .map(
            |stream| Box::pin(stream) as Pin<Box<dyn Stream<Item = SubscriptionEvent>>>
        ))
    }

    fn subscribe_relation_query_for_identity(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<Pin<Box<dyn Stream<Item = SubscriptionEvent> + 'static>>, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(
            db.subscribe_relation_query_for_identity(query, opts, author),
        )
        .map(
            |stream| Box::pin(stream) as Pin<Box<dyn Stream<Item = SubscriptionEvent>>>
        ))
    }

    fn attach_query(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<QueryAttachment, jazz::db::Error> {
        with_wasm_db!(self, |db| db.attach_query_with_opts(query, opts))
    }

    fn attach_query_for_identity(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<QueryAttachment, jazz::db::Error> {
        with_wasm_db!(self, |db| db
            .attach_query_with_opts_for_identity(query, opts, author))
    }

    fn query_attachment_is_covered(&self, attachment: &QueryAttachment) -> bool {
        with_wasm_db!(self, |db| db.query_attachment_is_covered(attachment))
    }

    fn detach_query(&self, attachment: QueryAttachment) {
        with_wasm_db!(self, |db| db.detach_query(attachment))
    }

    fn set_tick_scheduler(&self, callback: js_sys::Function) {
        let scheduler = Rc::new(WasmTickScheduler { callback });
        with_wasm_db!(self, |db| db.set_tick_scheduler(Some(scheduler)))
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
        with_wasm_db!(self, |db| db.tick())
    }
}

#[wasm_bindgen]
pub struct WasmTx {
    db: WasmDbInner,
    kind: WasmTxKind,
    open_tx: Option<OpenBatchId>,
    owns_lifetime: bool,
}

impl Drop for WasmTx {
    fn drop(&mut self) {
        if !self.owns_lifetime {
            return;
        }
        let Some(open_tx) = self.open_tx.take() else {
            return;
        };
        let _ = self.db.abandon_transaction(open_tx);
    }
}

#[derive(Clone, Copy)]
enum WasmTxKind {
    Mergeable,
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
            owns_runtime: true,
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
            owns_runtime: true,
        })
    }

    /// Register a typed schema view backed by this same runtime owner.
    #[wasm_bindgen(js_name = registerSchema)]
    pub fn register_schema(&self, schema: Vec<u8>) -> Result<WasmDb, JsValue> {
        let schema: JazzSchema = postcard::from_bytes(&schema)
            .map_err(|error| to_js_error(format!("decode schema: {error}")))?;
        Ok(Self {
            inner: self
                .inner
                .register_schema_view(schema)
                .map_err(to_js_error)?,
            owns_runtime: false,
        })
    }

    /// Attach this typed view to an existing owner-wide mergeable batch.
    #[wasm_bindgen(js_name = attachMergeableTx)]
    pub fn attach_mergeable_tx(&self, open_batch_id: String) -> Result<WasmTx, JsValue> {
        let open_batch_id = open_batch_id
            .parse::<OpenBatchId>()
            .map_err(|error| JsValue::from_str(&error))?;
        Ok(WasmTx {
            db: self.inner.clone(),
            kind: WasmTxKind::Mergeable,
            open_tx: Some(open_batch_id),
            owns_lifetime: false,
        })
    }

    /// Attach this typed view to an existing owner-wide exclusive batch.
    #[wasm_bindgen(js_name = attachExclusiveTx)]
    pub fn attach_exclusive_tx(&self, open_batch_id: String) -> Result<WasmTx, JsValue> {
        let open_batch_id = open_batch_id
            .parse::<OpenBatchId>()
            .map_err(|error| JsValue::from_str(&error))?;
        Ok(WasmTx {
            db: self.inner.clone(),
            kind: WasmTxKind::Exclusive,
            open_tx: Some(open_batch_id),
            owns_lifetime: false,
        })
    }

    /// Begin one owner-wide batch without creating an owning per-schema Tx.
    #[wasm_bindgen(js_name = beginTransaction)]
    pub fn begin_transaction(
        &self,
        open_batch_id: String,
        kind: String,
        author: Option<Vec<u8>>,
    ) -> Result<(), JsValue> {
        let open_batch_id = open_batch_id
            .parse::<OpenBatchId>()
            .map_err(|error| JsValue::from_str(&error))?;
        let author = author.as_deref().map(author_id_from_bytes).transpose()?;
        match kind.as_str() {
            "mergeable" => self
                .inner
                .begin_mergeable(open_batch_id, author)
                .map_err(to_js_error),
            "exclusive" if author.is_none() => self
                .inner
                .begin_exclusive(open_batch_id)
                .map_err(to_js_error),
            "exclusive" => Err(JsValue::from_str(
                "exclusive batches do not accept an identity override",
            )),
            _ => Err(JsValue::from_str(&format!("unknown batch kind {kind}"))),
        }
    }

    /// Commit an owner-wide mergeable batch by id.
    #[wasm_bindgen(js_name = commitTransaction)]
    pub fn commit_transaction(
        &self,
        open_batch_id: String,
        kind: Option<String>,
    ) -> Result<WasmWrite, JsValue> {
        let open_batch_id = open_batch_id
            .parse::<OpenBatchId>()
            .map_err(|error| JsValue::from_str(&error))?;
        let tx_id = match kind.as_deref().unwrap_or("mergeable") {
            "mergeable" => self.inner.commit_mergeable(open_batch_id),
            "exclusive" => self.inner.commit_exclusive(open_batch_id),
            kind => return Err(JsValue::from_str(&format!("unknown batch kind {kind}"))),
        }
        .map_err(to_js_error)?;
        match &self.inner {
            WasmDbInner::Memory(db) => wasm_tx_write(
                tx_id,
                Some(WasmWriteInner::MemoryTx {
                    db: Rc::clone(db),
                    tx_id,
                }),
            ),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => wasm_tx_write(
                tx_id,
                Some(WasmWriteInner::BrowserTx {
                    db: Rc::clone(db),
                    tx_id,
                }),
            ),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    /// Roll back an owner-wide open batch by id.
    #[wasm_bindgen(js_name = rollbackTransaction)]
    pub fn rollback_transaction(&self, open_batch_id: String) -> Result<(), JsValue> {
        let open_batch_id = open_batch_id
            .parse::<OpenBatchId>()
            .map_err(|error| JsValue::from_str(&error))?;
        self.inner
            .abandon_transaction(open_batch_id)
            .map_err(to_js_error)
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
        let opts = read_opts_from_js(opts)?;
        let tx_id = tx.open_tx_for_read()?;
        let rows = match tx.kind {
            WasmTxKind::Mergeable => self.inner.mergeable_all(tx_id, &query.inner, opts),
            WasmTxKind::Exclusive => self.inner.exclusive_all(tx_id, &query.inner, opts),
        }
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
        let opts = read_opts_from_js(opts)?;
        let author = author_id_from_bytes(&author)?;
        let tx_id = tx.open_tx_for_read()?;
        let rows = match tx.kind {
            WasmTxKind::Mergeable => {
                self.inner
                    .mergeable_all_for_identity(tx_id, &query.inner, author, opts)
            }
            WasmTxKind::Exclusive => {
                self.inner
                    .exclusive_all_for_identity(tx_id, &query.inner, author, opts)
            }
        }
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
        subscription_stream_to_js(stream)
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
        subscription_stream_to_js(stream)
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
        subscription_stream_to_js(stream)
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
        subscription_stream_to_js(stream)
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

    /// Register a callback for rejected writes that no active wait consumed.
    #[wasm_bindgen(js_name = onMutationError)]
    pub fn on_mutation_error(&self, callback: js_sys::Function) {
        let callback: MutationErrorCallback = Rc::new(move |event| {
            let Ok(value) = serde_wasm_bindgen::to_value(event) else {
                return;
            };
            let _ = callback.call1(&JsValue::UNDEFINED, &value);
        });
        match &self.inner {
            WasmDbInner::Memory(db) => db.on_mutation_error(Rc::clone(&callback)),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => db.on_mutation_error(Rc::clone(&callback)),
            WasmDbInner::Closed => {}
        }
    }

    #[wasm_bindgen(js_name = insertEncoded)]
    pub fn insert_encoded(&self, table: String, cells: Vec<u8>) -> Result<WasmWrite, JsValue> {
        let cells = decode_cells(&cells)?;
        self.inner.insert(&table, cells)
    }

    #[wasm_bindgen(js_name = canInsertEncoded)]
    pub fn can_insert_encoded(&self, table: String, cells: Vec<u8>) -> Result<String, JsValue> {
        let cells = decode_cells(&cells)?;
        match &self.inner {
            WasmDbInner::Memory(db) => db
                .can_insert(&table, cells)
                .map(advice_string)
                .map_err(to_js_error),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => db
                .can_insert(&table, cells)
                .map(advice_string)
                .map_err(to_js_error),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = requestInsertPermissionAdviceEncoded)]
    pub fn request_insert_permission_advice_encoded(
        &self,
        table: String,
        cells: Vec<u8>,
    ) -> Result<WasmPermissionAdviceRequest, JsValue> {
        self.inner
            .request_permission_advice(PermissionAdviceAction::Insert {
                table,
                cells: decode_cells(&cells)?,
            })
    }

    #[wasm_bindgen(js_name = requestReadPermissionAdvice)]
    pub fn request_read_permission_advice(
        &self,
        table: String,
        row_id: Vec<u8>,
    ) -> Result<WasmPermissionAdviceRequest, JsValue> {
        self.inner
            .request_permission_advice(PermissionAdviceAction::Read {
                table,
                row: row_uuid_from_bytes(&row_id)?,
            })
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

    #[wasm_bindgen(js_name = requestUpdatePermissionAdviceEncoded)]
    pub fn request_update_permission_advice_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
    ) -> Result<WasmPermissionAdviceRequest, JsValue> {
        self.inner
            .request_permission_advice(PermissionAdviceAction::Update {
                table,
                row: row_uuid_from_bytes(&row_id)?,
                patch: decode_cells(&patch)?,
            })
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

    #[wasm_bindgen(js_name = requestDeletePermissionAdvice)]
    pub fn request_delete_permission_advice(
        &self,
        table: String,
        row_id: Vec<u8>,
    ) -> Result<WasmPermissionAdviceRequest, JsValue> {
        self.inner
            .request_permission_advice(PermissionAdviceAction::Delete {
                table,
                row: row_uuid_from_bytes(&row_id)?,
            })
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

    /// Configure this runtime as the optimistic in-memory side of a browser
    /// client/worker pair. Must be called before application writes begin.
    #[wasm_bindgen(js_name = setNonDurableClient)]
    pub fn set_non_durable_client(&self) -> Result<(), JsValue> {
        match &self.inner {
            WasmDbInner::Memory(db) => db.set_non_durable_client(),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => db.set_non_durable_client(),
            WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
        }
        Ok(())
    }

    #[wasm_bindgen(js_name = connectUpstream)]
    pub fn connect_upstream(&self) -> Result<WasmTransport, JsValue> {
        let queues = WasmWireQueues::default();
        // Browser WebSocket carriers negotiate ordinary sync only. They do not
        // receive the authenticated endpoint context required for scoped
        // receipt/view frames, so their transport must not self-advertise
        // those features before the carrier can bind that context.
        let transport = Box::new(WireTransportAdapter::new(
            WasmWireTransport {
                queues: queues.clone(),
            },
            jazz::wire::WIRE_PROTOCOL_VERSION,
            jazz::wire::current_wire_features()
                & !(jazz::wire::FEATURE_AUTHORIZATION_SCOPE_RECEIPTS
                    | jazz::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS),
            None,
        ));
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
        Ok(WasmTransport {
            inner,
            queues,
            subscriber_identity: None,
        })
    }

    /// Connect after the browser carrier has accepted the server Hello. The
    /// browser never asserts an authority endpoint itself; this context binds
    /// the authority advertised by the authenticated server response.
    #[wasm_bindgen(js_name = connectUpstreamWithSession)]
    pub fn connect_upstream_with_session(
        &self,
        protocol_version: u16,
        features: u32,
        remote_node: Vec<u8>,
        remote_epoch: u64,
        local_node: Vec<u8>,
        local_epoch: u64,
    ) -> Result<WasmTransport, JsValue> {
        let remote_node: [u8; 16] = remote_node
            .try_into()
            .map_err(|_| JsValue::from_str("server hello authority node must be 16 bytes"))?;
        let local_node: [u8; 16] = local_node
            .try_into()
            .map_err(|_| JsValue::from_str("local peer identity must be 16 bytes"))?;
        let queues = WasmWireQueues::default();
        let session_context = ConnectionSessionContext {
            local: WireAuthorityEndpoint {
                node: NodeUuid::from_bytes(local_node),
                epoch: local_epoch,
            },
            remote: WireAuthorityEndpoint {
                node: NodeUuid::from_bytes(remote_node),
                epoch: remote_epoch,
            },
            link_identity: AuthorId::from_bytes(local_node),
            negotiated_features: features as u64,
        };
        let transport = Box::new(WireTransportAdapter::new_with_session_context(
            WasmWireTransport {
                queues: queues.clone(),
            },
            protocol_version,
            features as u64,
            None,
            Some(session_context),
        ));
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
        Ok(WasmTransport {
            inner,
            queues,
            subscriber_identity: None,
        })
    }

    #[wasm_bindgen(js_name = acceptSubscriber)]
    pub fn accept_subscriber(
        &self,
        identity: Vec<u8>,
        claims: JsValue,
    ) -> Result<WasmTransport, JsValue> {
        let identity = author_id_from_bytes(&identity)?;
        let claims = claims_from_js(identity, claims)?;
        let queues = WasmWireQueues::default();
        // Like the JS-owned upstream carrier, this binding-local transport has
        // no authenticated endpoint context for scoped receipt/view frames.
        let transport = Box::new(WireTransportAdapter::new(
            WasmWireTransport {
                queues: queues.clone(),
            },
            jazz::wire::WIRE_PROTOCOL_VERSION,
            jazz::wire::current_wire_features()
                & !(jazz::wire::FEATURE_AUTHORIZATION_SCOPE_RECEIPTS
                    | jazz::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS),
            None,
        ));
        let inner = match &self.inner {
            WasmDbInner::Memory(db) => WasmTransportInner::Memory {
                db: Rc::clone(db),
                connection: Some(db.accept_subscriber_with_claims(
                    transport,
                    identity,
                    claims.clone(),
                )),
            },
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => WasmTransportInner::Browser {
                db: Rc::clone(db),
                connection: Some(db.accept_subscriber_with_claims(transport, identity, claims)),
            },
            WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
        };
        Ok(WasmTransport {
            inner,
            queues,
            subscriber_identity: Some(identity),
        })
    }

    #[wasm_bindgen(js_name = mergeableTx)]
    pub fn mergeable_tx(&self, open_batch_id: String) -> Result<WasmTx, JsValue> {
        let open_batch_id = open_batch_id
            .parse::<OpenBatchId>()
            .map_err(|error| JsValue::from_str(&error))?;
        self.inner
            .begin_mergeable(open_batch_id, None)
            .map_err(to_js_error)?;
        Ok(WasmTx {
            db: self.inner.clone(),
            kind: WasmTxKind::Mergeable,
            open_tx: Some(open_batch_id),
            owns_lifetime: true,
        })
    }

    #[wasm_bindgen(js_name = mergeableTxForIdentity)]
    pub fn mergeable_tx_for_identity(
        &self,
        open_batch_id: String,
        author: Vec<u8>,
    ) -> Result<WasmTx, JsValue> {
        let open_batch_id = open_batch_id
            .parse::<OpenBatchId>()
            .map_err(|error| JsValue::from_str(&error))?;
        let author = author_id_from_bytes(&author)?;
        self.inner
            .begin_mergeable(open_batch_id, Some(author))
            .map_err(to_js_error)?;
        Ok(WasmTx {
            db: self.inner.clone(),
            kind: WasmTxKind::Mergeable,
            open_tx: Some(open_batch_id),
            owns_lifetime: true,
        })
    }

    #[wasm_bindgen(js_name = exclusiveTx)]
    pub fn exclusive_tx(&self, open_batch_id: String) -> Result<WasmTx, JsValue> {
        let open_batch_id = open_batch_id
            .parse::<OpenBatchId>()
            .map_err(|error| JsValue::from_str(&error))?;
        self.inner
            .begin_exclusive(open_batch_id)
            .map_err(to_js_error)?;
        Ok(WasmTx {
            db: self.inner.clone(),
            kind: WasmTxKind::Exclusive,
            open_tx: Some(open_batch_id),
            owns_lifetime: true,
        })
    }

    #[wasm_bindgen(js_name = close)]
    pub fn close(&mut self) -> Result<bool, JsValue> {
        let inner = std::mem::replace(&mut self.inner, WasmDbInner::Closed);
        if !self.owns_runtime {
            return Ok(!matches!(inner, WasmDbInner::Closed));
        }
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
    #[wasm_bindgen(js_name = updateAuthenticatedClaims)]
    pub fn update_authenticated_claims(&self, claims: JsValue) -> Result<(), JsValue> {
        let identity = self
            .subscriber_identity
            .ok_or_else(|| JsValue::from_str("transport is not a subscriber link"))?;
        let claims = claims_from_js(identity, claims)?;
        match &self.inner {
            WasmTransportInner::Memory { connection, .. } => connection
                .as_ref()
                .ok_or_else(|| JsValue::from_str("subscriber transport is closed"))?
                .borrow_mut()
                .update_authenticated_session_claims(claims),
            #[cfg(target_arch = "wasm32")]
            WasmTransportInner::Browser { connection, .. } => connection
                .as_ref()
                .ok_or_else(|| JsValue::from_str("subscriber transport is closed"))?
                .borrow_mut()
                .update_authenticated_session_claims(claims),
        }
        Ok(())
    }

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
        match self.kind {
            WasmTxKind::Mergeable => self
                .db
                .mergeable_insert(open_tx, &table, row_id, cells, now_ms),
            WasmTxKind::Exclusive => self.db.exclusive_write(open_tx, &table, row_id, cells),
        }
        .map_err(to_js_error)?;
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
        match self.kind {
            WasmTxKind::Mergeable => self
                .db
                .mergeable_update(open_tx, &table, row_id, patch, now_ms),
            WasmTxKind::Exclusive => self.db.exclusive_update(open_tx, &table, row_id, patch),
        }
        .map_err(to_js_error)?;
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
        match self.kind {
            WasmTxKind::Mergeable => self.db.mergeable_delete(
                open_tx,
                &table,
                row_id,
                updated_at_ms.map(|value| value as u64),
            ),
            WasmTxKind::Exclusive => self.db.exclusive_delete(open_tx, &table, row_id),
        }
        .map_err(to_js_error)?;
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
        match self.kind {
            WasmTxKind::Mergeable => self
                .db
                .mergeable_restore(open_tx, &table, row_id, cells, now_ms),
            WasmTxKind::Exclusive => self.db.exclusive_restore(open_tx, &table, row_id, cells),
        }
        .map_err(to_js_error)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = commit)]
    pub fn commit(&mut self) -> Result<WasmWrite, JsValue> {
        let open_tx = self.open_tx_for_read()?;
        let write = match (&self.db, self.kind) {
            (WasmDbInner::Memory(db), WasmTxKind::Mergeable) => {
                let tx_id = self.db.commit_mergeable(open_tx).map_err(to_js_error)?;
                wasm_tx_write(
                    tx_id,
                    Some(WasmWriteInner::MemoryTx {
                        db: Rc::clone(db),
                        tx_id,
                    }),
                )
            }
            (WasmDbInner::Memory(db), WasmTxKind::Exclusive) => {
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
            (WasmDbInner::Browser(db), WasmTxKind::Mergeable) => {
                let tx_id = self.db.commit_mergeable(open_tx).map_err(to_js_error)?;
                wasm_tx_write(
                    tx_id,
                    Some(WasmWriteInner::BrowserTx {
                        db: Rc::clone(db),
                        tx_id,
                    }),
                )
            }
            #[cfg(target_arch = "wasm32")]
            (WasmDbInner::Browser(db), WasmTxKind::Exclusive) => {
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
        }?;
        self.open_tx.take();
        Ok(write)
    }

    #[wasm_bindgen(js_name = rollback)]
    pub fn rollback(&mut self) -> Result<(), JsValue> {
        let open_tx = self.open_tx_for_read()?;
        self.db.abandon_transaction(open_tx).map_err(to_js_error)?;
        self.open_tx.take();
        Ok(())
    }

    fn open_tx_for_read(&self) -> Result<OpenBatchId, JsValue> {
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
    let opts = read_opts_from_js(opts)?;
    let tx_id = tx.open_tx_for_read()?;
    match (tx.kind, author) {
        (WasmTxKind::Mergeable, Some(author)) => db
            .mergeable_all_for_identity(tx_id, &query.inner, author, opts)
            .map_err(to_js_error),
        (WasmTxKind::Mergeable, None) => db
            .mergeable_all(tx_id, &query.inner, opts)
            .map_err(to_js_error),
        (WasmTxKind::Exclusive, Some(author)) => db
            .exclusive_all_for_identity(tx_id, &query.inner, author, opts)
            .map_err(to_js_error),
        (WasmTxKind::Exclusive, None) => db
            .exclusive_all(tx_id, &query.inner, opts)
            .map_err(to_js_error),
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
    let initial_sync_flush_every = config.initial_sync_flush_every;
    if config.history_complete {
        let db = block_on(Db::open_history_complete(db_config))?;
        configure_initial_sync_flush_cadence(&db, initial_sync_flush_every)?;
        Ok(db)
    } else {
        let db = block_on(Db::open(db_config))?;
        configure_initial_sync_flush_cadence(&db, initial_sync_flush_every)?;
        Ok(db)
    }
}

fn configure_initial_sync_flush_cadence<S>(
    db: &Db<S>,
    every: Option<u32>,
) -> Result<(), jazz::db::Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let Some(every) = every else {
        return Ok(());
    };
    let Some(every) = std::num::NonZeroUsize::new(every as usize) else {
        return Ok(());
    };
    db.set_initial_sync_flush_cadence(InitialSyncFlushCadence::every(every))
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

fn wait_promise<S>(db: &Db<S>, tx_id: TxId, tier: DurabilityTier) -> js_sys::Promise
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    js_sys::Promise::new(&mut |resolve, reject| {
        db.wait_for_transaction_with(tx_id, tier, move |result| match result {
            Ok(_) => {
                let _ = resolve.call0(&JsValue::UNDEFINED);
            }
            Err(error) => {
                let _ = reject.call1(&JsValue::UNDEFINED, &to_js_error(error));
            }
        });
    })
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
            jazz::tools::policy_claims::json_number_to_policy_claim(
                value,
                jazz::tools::policy_claims::NumericClaimOrigin::JavaScript,
            )
            .map_err(to_js_error)?
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
        batch_id: BatchId::from_committed_tx(tx_id),
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
        batch_id: BatchId::from_committed_tx(tx_id),
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
        batch_id: BatchId::from_committed_tx(tx_id),
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
    jazz::binding_codec::encode_rows(rows)
}

fn encode_relation_snapshot(
    snapshot: &jazz::node::RelationSnapshot,
) -> Result<Vec<u8>, postcard::Error> {
    jazz::binding_codec::encode_relation_snapshot(snapshot)
}

fn encode_subscription_delta<'a>(
    added: &'a [jazz::db::SubscriptionOutputRow],
    updated: &'a [jazz::db::SubscriptionOutputRow],
    removed: &[jazz::db::RemovedRow],
) -> Result<Vec<u8>, postcard::Error> {
    jazz::binding_codec::encode_subscription_delta(added, updated, removed)
}

fn subscription_stream_to_js(
    stream: impl Stream<Item = SubscriptionEvent> + 'static,
) -> Result<JsValue, JsValue> {
    readable_stream_from_stream(stream.scan(HashSet::new(), |layouts, event| {
        std::future::ready(Some(subscription_chunk_to_js(event, layouts)))
    }))
}

fn subscription_chunk_to_js(
    event: SubscriptionEvent,
    published_terminal_layouts: &mut HashSet<String>,
) -> Result<JsValue, JsValue> {
    let object = js_sys::Object::new();
    match event {
        SubscriptionEvent::Delta {
            reset,
            publishable,
            added,
            updated,
            removed,
            terminal_operations,
            terminal_layout,
            settled,
            tier,
            ..
        } => {
            let (added, updated, removed) = if terminal_operations.is_empty() {
                (added, updated, removed)
            } else {
                (Vec::new(), Vec::new(), Vec::new())
            };
            let delta =
                encode_subscription_delta(&added, &updated, &removed).map_err(to_js_error)?;
            if let Some(layout) = terminal_layout.as_ref() {
                if terminal_operations
                    .iter()
                    .any(|operation| operation.root_descriptor != layout.root_descriptor)
                {
                    return Err(JsValue::from_str(
                        "terminal operation descriptor disagrees with its prepared root layout",
                    ));
                }
            }
            let terminal_layout_id = if terminal_operations.is_empty() {
                ""
            } else {
                terminal_layout
                    .as_ref()
                    .ok_or_else(|| {
                        JsValue::from_str(
                            "terminal operation arrived without a prepared root layout",
                        )
                    })?
                    .id
                    .as_str()
            };
            set_prop(&object, "type", JsValue::from_str("delta"))?;
            set_prop(
                &object,
                "delta",
                js_sys::Uint8Array::from(delta.as_slice()).into(),
            )?;
            set_prop(
                &object,
                "terminalOperations",
                jazz::binding_codec::terminal_operations_to_json(
                    &terminal_operations,
                    terminal_layout_id,
                )
                .map_err(to_js_error)?
                .serialize(&serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true))
                .map_err(to_js_error)?,
            )?;
            let terminal_layouts = if terminal_operations.is_empty() {
                Vec::new()
            } else {
                let layout = terminal_layout.as_ref().ok_or_else(|| {
                    JsValue::from_str("terminal operation arrived without a prepared root layout")
                })?;
                published_terminal_layouts
                    .insert(layout.id.clone())
                    .then(|| {
                        jazz::binding_codec::terminal_layout_to_json(layout).map_err(to_js_error)
                    })
                    .transpose()?
                    .into_iter()
                    .collect()
            };
            set_prop(
                &object,
                "terminalLayouts",
                serde_json::Value::Array(terminal_layouts)
                    .serialize(
                        &serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true),
                    )
                    .map_err(to_js_error)?,
            )?;
            set_prop(&object, "reset", JsValue::from_bool(reset))?;
            set_prop(&object, "publishable", JsValue::from_bool(publishable))?;
            set_prop(&object, "settled", JsValue::from_bool(settled))?;
            set_prop(&object, "tier", JsValue::from_str(&format!("{tier:?}")))?;
        }
        SubscriptionEvent::Closed => {
            set_prop(&object, "type", JsValue::from_str("closed"))?;
        }
        SubscriptionEvent::Rejected { reason } => {
            let reason_object = js_sys::Object::new();
            match reason {
                jazz::protocol::SubscribeRejectReason::UnsupportedShapeCapability { detail } => {
                    set_prop(
                        &reason_object,
                        "type",
                        JsValue::from_str("UnsupportedShapeCapability"),
                    )?;
                    set_prop(&reason_object, "detail", JsValue::from_str(&detail))?;
                }
                // Transient: the shape is awaiting catalogue admission and may
                // yet be served. Surfaced distinctly so a caller cannot mistake
                // it for an unsupported capability, which is permanent — that
                // conflation is the bug this variant was introduced to fix.
                jazz::protocol::SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission => {
                    set_prop(
                        &reason_object,
                        "type",
                        JsValue::from_str("ShapeRegistrationPendingCatalogueAdmission"),
                    )?;
                }
                jazz::protocol::SubscribeRejectReason::ServerFailure { code } => {
                    set_prop(&reason_object, "type", JsValue::from_str("ServerFailure"))?;
                    set_prop(
                        &reason_object,
                        "code",
                        JsValue::from_str(&format!("{code:?}")),
                    )?;
                }
            }
            set_prop(&object, "type", JsValue::from_str("rejected"))?;
            set_prop(&object, "reason", reason_object.into())?;
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

#[cfg(test)]
mod dynamic_schema_view_tests {
    use super::*;
    use jazz::db::{DbConfig, DbIdentity, ExclusiveTxOps};
    use jazz::groove::schema::ColumnType;
    use jazz::schema::{ColumnSchema, Policy, TableSchema};

    #[test]
    fn javascript_numeric_claims_preserve_safe_integers_and_fail_closed_when_lossy() {
        assert_eq!(
            claim_value_from_json(serde_json::json!(7)).unwrap(),
            Value::U64(7)
        );
        assert_eq!(
            claim_value_from_json(serde_json::json!(-7)).unwrap(),
            Value::I64(-7)
        );
        assert_eq!(
            claim_value_from_json(serde_json::Value::Number(
                serde_json::Number::from_f64(7.0).unwrap()
            ))
            .unwrap(),
            Value::U64(7),
            "WASM's f64 JS-number path must agree with integer JSON"
        );
        assert_eq!(
            claim_value_from_json(serde_json::json!(7.5)).unwrap(),
            Value::F64(7.5)
        );
        assert_eq!(
            claim_value_from_json(serde_json::json!(9_007_199_254_740_992_u64)).unwrap(),
            Value::F64(9_007_199_254_740_992.0),
            "integers beyond Number.MAX_SAFE_INTEGER must not participate in integer policy matching"
        );
        assert_eq!(
            claim_value_from_json(serde_json::json!(-9_007_199_254_740_992_i64)).unwrap(),
            Value::F64(-9_007_199_254_740_992.0)
        );
    }

    #[test]
    fn wasm_delta_preserves_typed_union_occurrence_keys() {
        #[derive(serde::Deserialize)]
        struct DecodedRemoved {
            #[allow(dead_code)]
            table: String,
            #[allow(dead_code)]
            row_id: RowUuid,
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
                RowUuid::from_bytes([1; 16]),
                occurrence(label),
            )
        });
        let bytes = encode_subscription_delta(&[], &[], &removed).unwrap();
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
    /// A short-lived WASM schema attachment must not abandon its owner's open
    /// batch when the JavaScript wrapper is collected.
    #[test]
    fn attached_tx_drop_preserves_owner_batch() {
        let schema = JazzSchema::new([TableSchema::new(
            "items",
            [ColumnSchema::new("label", ColumnType::String)],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public())]);
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let owner = Rc::new(
            block_on(Db::open(DbConfig::new(
                schema.clone(),
                MemoryStorage::new(&refs),
                DbIdentity {
                    node: jazz::ids::NodeUuid::from_bytes([0x45; 16]),
                    author: AuthorId::from_bytes([0xa5; 16]),
                },
            )))
            .unwrap(),
        );
        let view = Rc::new(owner.register_schema_view(schema).unwrap());
        let batch = OpenBatchId::new();
        owner.begin_mergeable(batch).unwrap();
        drop(WasmTx {
            db: WasmDbInner::Memory(Rc::clone(&view)),
            kind: WasmTxKind::Mergeable,
            open_tx: Some(batch),
            owns_lifetime: false,
        });
        view.mergeable_tx_ref(batch)
            .insert_with_id(
                "items",
                RowUuid::from_bytes([1; 16]),
                BTreeMap::from([("label".to_owned(), Value::String("kept".to_owned()))]),
            )
            .unwrap();
        let prepared = view.prepare_query(&view.table("items")).unwrap();
        let rows = WasmDbInner::Memory(Rc::clone(&view))
            .mergeable_all(batch, &prepared, ReadOpts::default())
            .unwrap();
        assert_eq!(rows.len(), 1, "the attached view reads staged rows");
        owner.commit_mergeable_handle(batch).unwrap();

        let exclusive = OpenBatchId::new();
        owner.begin_exclusive(exclusive).unwrap();
        drop(WasmTx {
            db: WasmDbInner::Memory(Rc::clone(&view)),
            kind: WasmTxKind::Exclusive,
            open_tx: Some(exclusive),
            owns_lifetime: false,
        });
        view.exclusive_tx_ref(exclusive)
            .insert_with_id(
                "items",
                RowUuid::from_bytes([2; 16]),
                BTreeMap::from([(
                    "label".to_owned(),
                    Value::String("exclusive-kept".to_owned()),
                )]),
            )
            .unwrap();
        owner.commit_exclusive_handle(exclusive).unwrap();
    }
}
