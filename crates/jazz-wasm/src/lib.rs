use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::future::{AbortHandle, Abortable};
use futures_util::lock::Mutex as LocalMutex;
use futures_util::stream;
use futures_util::task::{waker, ArcWake};
use futures_util::{Stream, StreamExt};
#[cfg(target_arch = "wasm32")]
use idb_tree::IndexedDbPageStore;
use jazz::db::{
    block_on, ConnectionSessionContext, Db, DbConfig, DbIdentity, ExclusiveTxOps,
    InitialSyncFlushCadence, LargeValueUpdate, LocalUpdates, MergeableTxOps, MutationErrorCallback,
    PeerConnection, PermissionAdvice, PreparedQuery, Propagation, QueryAttachment, ReadOpts,
    RowCells, SeededRowIdSource, StreamingMutationKind, StreamingValueUpload, SubscriptionEvent,
    TickScheduler, TickUrgency, WireTransportAdapter, WriteHandle,
};
use jazz::groove::records::{BorrowedRecord, RecordDescriptor, Value};
#[cfg(target_arch = "wasm32")]
use jazz::groove::storage::IdbStorage;
use jazz::groove::storage::{MemoryStorage, OrderedKvStorage, ReopenableStorage};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{BranchSelector, BranchViewBase, PermissionAdviceAction, ReadViewSpec};
use jazz::query::{Query, RelationExpr, RelationQuery};
use jazz::schema::JazzSchema;
use jazz::tools::{OpenTransactionId, TransactionId};
use jazz::tx::{DurabilityTier, TxId};
use jazz::wire::{TransportError, WireAuthorityEndpoint, WireTransport};
use serde::{Deserialize, Serialize};

#[cfg(target_arch = "wasm32")]
type BrowserStorage = IdbStorage<IndexedDbPageStore>;
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

/// Exact build/ABI fingerprint for this generated WASM artifact.
#[wasm_bindgen(js_name = nativeArtifactFingerprint)]
pub fn native_artifact_fingerprint() -> String {
    option_env!("JAZZ_NATIVE_ARTIFACT_FINGERPRINT")
        .unwrap_or("missing-build-fingerprint")
        .to_owned()
}

/// Test-only bridge for executing the Rust-owned v1 binding corpus through the
/// generated WASM artifact. The JavaScript test still uses the production
/// decoder for every returned postcard payload.
#[wasm_bindgen(js_name = __testBindingCodecGoldenFixture)]
pub fn test_binding_codec_golden_fixture() -> String {
    jazz::binding_codec::BINDING_CODEC_GOLDEN_FIXTURE.to_owned()
}

/// Generate a new UUID v7 (time-ordered).
///
/// Useful when a caller wants the default generated row-id shape.
#[wasm_bindgen(js_name = generateId)]
pub fn generate_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// Get the current timestamp in milliseconds since Unix epoch.
#[wasm_bindgen(js_name = currentTimestamp)]
pub fn current_timestamp() -> u64 {
    use web_time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
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
    author: AuthorSubject,
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
    row_id: RowUuid,
    tx_id: TransactionId,
    inner: Option<WasmWriteInner>,
}

struct WasmStreamingMutationState {
    db: WasmDbInner,
    upload: StreamingValueUpload,
    mutation: StreamingMutationKind,
    table: String,
    row_id: RowUuid,
    cells: RowCells,
    column: String,
    identity: Option<AuthorSubject>,
    attribution: Option<AuthorSubject>,
    updated_at_ms: Option<u64>,
    head: Option<BranchSelector>,
    base: Option<BranchViewBase>,
}

#[wasm_bindgen(js_name = StreamingMutation)]
pub struct WasmStreamingMutation {
    state: Rc<RefCell<Option<WasmStreamingMutationState>>>,
}

#[wasm_bindgen]
impl WasmStreamingMutation {
    pub fn push(&self, chunk: Vec<u8>) -> js_sys::Promise {
        let state_cell = Rc::clone(&self.state);
        future_to_promise(async move {
            let mut state = state_cell
                .borrow_mut()
                .take()
                .ok_or_else(|| JsValue::from_str("streaming mutation is closed"))?;
            let result = match &state.db {
                WasmDbInner::Memory(db) => {
                    db.push_streaming_value_upload(&mut state.upload, &chunk)
                        .await
                }
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => {
                    db.push_streaming_value_upload(&mut state.upload, &chunk)
                        .await
                }
                WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
            };
            result.map_err(to_js_error)?;
            *state_cell.borrow_mut() = Some(state);
            Ok(JsValue::UNDEFINED)
        })
    }

    pub fn finish(&self) -> js_sys::Promise {
        let state_cell = Rc::clone(&self.state);
        future_to_promise(async move {
            let state = state_cell
                .borrow_mut()
                .take()
                .ok_or_else(|| JsValue::from_str("streaming mutation is closed"))?;
            let write = match &state.db {
                WasmDbInner::Memory(db) => wasm_write_memory(
                    Rc::clone(db),
                    db.finish_streaming_value_upload(
                        state.upload,
                        state.mutation,
                        &state.table,
                        state.row_id,
                        state.cells,
                        &state.column,
                        state.identity,
                        state.updated_at_ms,
                        state.head,
                        state.base,
                        state.attribution,
                    )
                    .await
                    .map_err(to_js_error)?,
                ),
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => wasm_write_browser(
                    Rc::clone(db),
                    db.finish_streaming_value_upload(
                        state.upload,
                        state.mutation,
                        &state.table,
                        state.row_id,
                        state.cells,
                        &state.column,
                        state.identity,
                        state.updated_at_ms,
                        state.head,
                        state.base,
                        state.attribution,
                    )
                    .await
                    .map_err(to_js_error)?,
                ),
                WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
            }?;
            Ok(write.into())
        })
    }

    pub fn abort(&self) -> js_sys::Promise {
        let state_cell = Rc::clone(&self.state);
        future_to_promise(async move {
            let Some(state) = state_cell.borrow_mut().take() else {
                return Ok(JsValue::FALSE);
            };
            match &state.db {
                WasmDbInner::Memory(db) => db.abort_streaming_value_upload(state.upload).await,
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => db.abort_streaming_value_upload(state.upload).await,
                WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
            }
            .map_err(to_js_error)?;
            Ok(JsValue::TRUE)
        })
    }
}

enum WasmWriteInner {
    MemoryTx {
        db: Rc<Db<MemoryStorage>>,
        tx_id: TxId,
    },
    #[cfg(target_arch = "wasm32")]
    BrowserTx {
        db: Rc<Db<BrowserStorage>>,
        tx_id: TxId,
    },
}

#[wasm_bindgen]
impl WasmWrite {
    #[wasm_bindgen(getter, js_name = txId)]
    pub fn tx_id(&self) -> String {
        self.tx_id.to_string()
    }

    #[wasm_bindgen(getter, js_name = payload)]
    pub fn payload(&self) -> Vec<u8> {
        self.payload.clone()
    }

    #[wasm_bindgen(getter, js_name = rowId)]
    pub fn row_id(&self) -> Vec<u8> {
        self.row_id.to_bytes()
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
    // This is set only by the explicit backend-open ABI.  Attributed writes
    // are otherwise a privilege-escalation surface, because their author is
    // provenance while admission remains the runtime's SYSTEM identity.
    trusted_backend: bool,
}

enum WasmDbInner {
    Memory(Rc<Db<MemoryStorage>>),
    #[cfg(target_arch = "wasm32")]
    Browser(Rc<Db<BrowserStorage>>),
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

impl WasmDbInner {
    fn shares_runtime_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Memory(left), Self::Memory(right)) => left.shares_runtime_with(right),
            #[cfg(target_arch = "wasm32")]
            (Self::Browser(left), Self::Browser(right)) => left.shares_runtime_with(right),
            _ => false,
        }
    }

    async fn hydrate_relation_snapshot_for_binding(
        &self,
        snapshot: &mut jazz::node::RelationSnapshot,
    ) -> Result<(), jazz::db::Error> {
        match self {
            Self::Memory(db) => db.hydrate_relation_snapshot_for_binding(snapshot).await,
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.hydrate_relation_snapshot_for_binding(snapshot).await,
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    async fn hydrate_subscription_event_for_binding(
        &self,
        event: &mut SubscriptionEvent,
    ) -> Result<(), jazz::db::BindingHydrationError> {
        match self {
            Self::Memory(db) => {
                db.hydrate_subscription_event_for_binding_outcome(event)
                    .await
            }
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => {
                db.hydrate_subscription_event_for_binding_outcome(event)
                    .await
            }
            Self::Closed => panic!("WasmDb is closed"),
        }
    }
}

#[wasm_bindgen]
pub struct WasmTransport {
    inner: WasmTransportInner,
    queues: WasmWireQueues,
    auxiliary_pump: jazz::db::PeerIoPump,
    protocol_version: u16,
    features: u64,
    subscriber_identity: Option<AuthorSubject>,
}

enum WasmTransportInner {
    Memory {
        db: Rc<Db<MemoryStorage>>,
        connection: Option<Rc<LocalMutex<PeerConnection<MemoryStorage>>>>,
    },
    #[cfg(target_arch = "wasm32")]
    Browser {
        db: Rc<Db<BrowserStorage>>,
        connection: Option<Rc<LocalMutex<PeerConnection<BrowserStorage>>>>,
    },
}

impl Clone for WasmTransportInner {
    fn clone(&self) -> Self {
        match self {
            Self::Memory { db, connection } => Self::Memory {
                db: Rc::clone(db),
                connection: connection.clone(),
            },
            #[cfg(target_arch = "wasm32")]
            Self::Browser { db, connection } => Self::Browser {
                db: Rc::clone(db),
                connection: connection.clone(),
            },
        }
    }
}

impl WasmTransportInner {
    fn auxiliary_pump(&self) -> jazz::db::PeerIoPump {
        match self {
            Self::Memory { connection, .. } => jazz::db::block_on(async {
                connection
                    .as_ref()
                    .expect("new transport has a connection")
                    .lock()
                    .await
                    .io_pump()
            }),
            #[cfg(target_arch = "wasm32")]
            Self::Browser { connection, .. } => jazz::db::block_on(async {
                connection
                    .as_ref()
                    .expect("new transport has a connection")
                    .lock()
                    .await
                    .io_pump()
            }),
        }
    }

    async fn tick(self) -> Result<u32, JsValue> {
        match self {
            Self::Memory { connection, .. } => tick_connection(&connection).await,
            #[cfg(target_arch = "wasm32")]
            Self::Browser { connection, .. } => tick_connection(&connection).await,
        }
    }

    async fn update_authenticated_claims(
        self,
        claims: BTreeMap<String, Value>,
    ) -> Result<(), JsValue> {
        match self {
            Self::Memory { connection, .. } => connection
                .ok_or_else(|| JsValue::from_str("subscriber transport is closed"))?
                .lock()
                .await
                .update_authenticated_session_claims(claims),
            #[cfg(target_arch = "wasm32")]
            Self::Browser { connection, .. } => connection
                .ok_or_else(|| JsValue::from_str("subscriber transport is closed"))?
                .lock()
                .await
                .update_authenticated_session_claims(claims),
        }
        Ok(())
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
    outbound_scheduler: Rc<RefCell<Option<js_sys::Function>>>,
}

struct WasmWireTransport {
    queues: WasmWireQueues,
}

struct WasmTickScheduler {
    callback: js_sys::Function,
    progress_wake: UnboundedSender<()>,
    progress_wake_pending: Arc<AtomicBool>,
}

/// `Waker` itself must be Send + Sync, while a JS callback is deliberately
/// thread-affine. Keep only a thread-safe channel in the waker and forward it
/// back to the WASM local task before touching JS.
struct WasmQueryRuntimeWake {
    sender: UnboundedSender<()>,
    pending: Arc<AtomicBool>,
}

impl ArcWake for WasmQueryRuntimeWake {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        if !arc_self.pending.swap(true, Ordering::AcqRel) {
            let _ = arc_self.sender.unbounded_send(());
        }
    }
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

    fn schedule_tick_after(&self, delay_ms: u64) {
        let _ = self.callback.call1(
            &JsValue::NULL,
            &JsValue::from_str(&format!("after:{delay_ms}")),
        );
    }

    fn query_runtime_waker(&self) -> Option<Waker> {
        Some(waker(Arc::new(WasmQueryRuntimeWake {
            sender: self.progress_wake.clone(),
            pending: Arc::clone(&self.progress_wake_pending),
        })))
    }
}

impl WireTransport for WasmWireTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.queues.outbound.borrow_mut().push_back(frame);
        let scheduler = self.queues.outbound_scheduler.borrow().clone();
        if let Some(scheduler) = scheduler {
            let _ = scheduler.call0(&JsValue::NULL);
        }
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
                block_on(db.register_schema_view(schema)).map_err(|error| error.to_string())?,
            ))),
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => Ok(Self::Browser(Rc::new(
                block_on(db.register_schema_view(schema)).map_err(|error| error.to_string())?,
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
        author: AuthorSubject,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(
            db.all_for_identity(query, opts, author)
        ))
    }

    async fn all_async(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        match self {
            Self::Memory(db) => db.all(query, opts).await,
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.all(query, opts).await,
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    async fn all_for_identity_async(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorSubject,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        match self {
            Self::Memory(db) => db.all_for_identity(query, opts, author).await,
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.all_for_identity(query, opts, author).await,
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    async fn hydrate_rows_for_binding(
        &self,
        rows: &mut [jazz::node::CurrentRow],
    ) -> Result<(), jazz::db::Error> {
        match self {
            Self::Memory(db) => db.hydrate_rows_for_binding(rows).await,
            #[cfg(target_arch = "wasm32")]
            Self::Browser(db) => db.hydrate_rows_for_binding(rows).await,
            Self::Closed => panic!("WasmDb is closed"),
        }
    }

    fn begin_exclusive(
        &self,
        id: OpenTransactionId,
        author: Option<AuthorSubject>,
    ) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| match author {
            Some(author) => block_on(db.begin_exclusive_for_identity(id, author)),
            None => block_on(db.begin_exclusive(id)),
        })
    }

    fn begin_mergeable(
        &self,
        id: OpenTransactionId,
        author: Option<AuthorSubject>,
    ) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| match author {
            Some(author) => block_on(db.begin_mergeable_for_identity(id, author)),
            None => block_on(db.begin_mergeable(id)),
        })
    }

    fn exclusive_all_for_identity(
        &self,
        tx_id: OpenTransactionId,
        query: &PreparedQuery,
        author: AuthorSubject,
        opts: ReadOpts,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(
            db.exclusive_tx_ref(tx_id)
                .all_prepared_for_identity_with_opts(query, author, opts)
        ))
    }

    fn exclusive_all(
        &self,
        tx_id: OpenTransactionId,
        query: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(
            db.exclusive_tx_ref(tx_id)
                .all_prepared_with_opts(query, opts)
        ))
    }

    fn mergeable_all_for_identity(
        &self,
        tx_id: OpenTransactionId,
        query: &PreparedQuery,
        author: AuthorSubject,
        opts: ReadOpts,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(
            db.mergeable_tx_ref(tx_id)
                .all_prepared_for_identity_with_opts(query, author, opts)
        ))
    }

    fn mergeable_all(
        &self,
        tx_id: OpenTransactionId,
        query: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<jazz::node::CurrentRow>, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(
            db.mergeable_tx_ref(tx_id)
                .all_prepared_with_opts(query, opts)
        ))
    }

    fn abandon_transaction(&self, tx_id: OpenTransactionId) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| db.abandon_transaction_handle(tx_id))
    }

    fn commit_exclusive(&self, tx_id: OpenTransactionId) -> Result<TxId, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(db.commit_exclusive_handle(tx_id)))
    }

    fn commit_mergeable(&self, tx_id: OpenTransactionId) -> Result<TxId, jazz::db::Error> {
        with_wasm_db!(self, |db| block_on(db.commit_mergeable_handle(tx_id)))
    }

    async fn all_relation_snapshot(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<jazz::node::RelationSnapshot, jazz::db::Error> {
        with_wasm_db!(self, |db| db.all_relation_snapshot(query, opts).await)
    }

    async fn all_relation_snapshot_for_identity(
        &self,
        query: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorSubject,
    ) -> Result<jazz::node::RelationSnapshot, jazz::db::Error> {
        with_wasm_db!(self, |db| db
            .all_relation_snapshot_for_identity(query, opts, author)
            .await)
    }

    async fn all_relation_query(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<jazz::node::RelationSnapshot, jazz::db::Error> {
        with_wasm_db!(self, |db| db.all_relation_query(query, opts).await)
    }

    async fn all_relation_query_for_identity(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorSubject,
    ) -> Result<jazz::node::RelationSnapshot, jazz::db::Error> {
        with_wasm_db!(self, |db| db
            .all_relation_query_for_identity(query, opts, author)
            .await)
    }

    fn set_identity_claims(&self, author: AuthorSubject, claims: BTreeMap<String, Value>) {
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
        author: AuthorSubject,
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
        author: AuthorSubject,
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
        author: AuthorSubject,
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
        let (progress_wake, mut progress_events) = unbounded();
        let progress_wake_pending = Arc::new(AtomicBool::new(false));
        let progress_callback = callback.clone();
        let progress_pending = Arc::clone(&progress_wake_pending);
        wasm_bindgen_futures::spawn_local(async move {
            while progress_events.next().await.is_some() {
                progress_pending.store(false, Ordering::Release);
                let _ = progress_callback.call1(&JsValue::NULL, &JsValue::from_str("immediate"));
            }
        });
        let scheduler = Rc::new(WasmTickScheduler {
            callback,
            progress_wake,
            progress_wake_pending,
        });
        with_wasm_db!(self, |db| db.set_tick_scheduler(Some(scheduler)))
    }

    async fn tick(&self) -> Result<(), jazz::db::Error> {
        with_wasm_db!(self, |db| db.tick().await)
    }
}

#[wasm_bindgen]
pub struct WasmTx {
    db: WasmDbInner,
    kind: WasmTxKind,
    open_tx: Option<OpenTransactionId>,
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
    fn require_trusted_backend(&self) -> Result<(), JsValue> {
        self.trusted_backend.then_some(()).ok_or_else(|| {
            JsValue::from_str("backend attribution requires an explicit backend runtime")
        })
    }

    #[wasm_bindgen(js_name = insertEncoded)]
    pub fn insert_encoded_with_options(
        &self,
        table: String,
        cells: Vec<u8>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let cells = decode_cells(&cells)?;
        let options = insert_options_from_js(options)?;
        match &self.inner {
            WasmDbInner::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                block_on(db.insert(&table, cells, options)).map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                block_on(db.insert(&table, cells, options)).map_err(to_js_error)?,
            ),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = updateEncoded)]
    pub fn update_encoded_with_options(
        &self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let patch = decode_cells(&patch)?;
        let options = update_options_from_js(options)?;
        match &self.inner {
            WasmDbInner::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                block_on(db.update(&table, row_id, patch, options)).map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                block_on(db.update(&table, row_id, patch, options)).map_err(to_js_error)?,
            ),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = updateLargeValuesEncoded)]
    pub fn update_large_values_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        mutations: JsValue,
        updated_at_ms: Option<f64>,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let patch = decode_cells(&patch)?;
        let mutations: Vec<LargeValueUpdate> =
            serde_wasm_bindgen::from_value(mutations).map_err(|error| {
                JsValue::from_str(&format!("invalid partial-value update descriptor: {error}"))
            })?;
        match &self.inner {
            WasmDbInner::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                match updated_at_ms
                    .map(|value| checked_js_u64(value, "updatedAtMs"))
                    .transpose()?
                {
                    Some(now_ms) => block_on(db.update_with_large_value_mutations_at_ms(
                        &table, row_id, patch, mutations, now_ms,
                    )),
                    None => block_on(
                        db.update_with_large_value_mutations(&table, row_id, patch, mutations),
                    ),
                }
                .map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                match updated_at_ms
                    .map(|value| checked_js_u64(value, "updatedAtMs"))
                    .transpose()?
                {
                    Some(now_ms) => block_on(db.update_with_large_value_mutations_at_ms(
                        &table, row_id, patch, mutations, now_ms,
                    )),
                    None => block_on(
                        db.update_with_large_value_mutations(&table, row_id, patch, mutations),
                    ),
                }
                .map_err(to_js_error)?,
            ),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = upsertEncoded)]
    pub fn upsert_encoded_with_options(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let options = upsert_options_from_js(options)?;
        match &self.inner {
            WasmDbInner::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                block_on(db.upsert(&table, row_id, cells, options)).map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                block_on(db.upsert(&table, row_id, cells, options)).map_err(to_js_error)?,
            ),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = deleteEncoded)]
    pub fn delete_encoded_with_options(
        &self,
        table: String,
        row_id: Vec<u8>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let options = delete_options_from_js(options)?;
        match &self.inner {
            WasmDbInner::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                block_on(db.delete(&table, row_id, options)).map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                block_on(db.delete(&table, row_id, options)).map_err(to_js_error)?,
            ),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = restoreEncoded)]
    pub fn restore_encoded_with_options(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let options = restore_options_from_js(options)?;
        match &self.inner {
            WasmDbInner::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                block_on(db.restore(&table, row_id, Some(cells), options)).map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                block_on(db.restore(&table, row_id, Some(cells), options)).map_err(to_js_error)?,
            ),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    /// Backend-only root insert. Admission remains SYSTEM while `author` is
    /// retained as row provenance; the public raw open cannot enable this.
    #[wasm_bindgen(js_name = insertWithIdEncodedAttributed)]
    pub fn insert_with_id_encoded_attributed(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<WasmWrite, JsValue> {
        self.require_trusted_backend()?;
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let author = author_id_from_bytes(&author)?;
        match &self.inner {
            WasmDbInner::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                block_on(db.insert_with_id_attributed(author, &table, row_id, cells))
                    .map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                block_on(db.insert_with_id_attributed(author, &table, row_id, cells))
                    .map_err(to_js_error)?,
            ),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = updateEncodedAttributed)]
    pub fn update_encoded_attributed(
        &self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<WasmWrite, JsValue> {
        self.require_trusted_backend()?;
        let row_id = row_uuid_from_bytes(&row_id)?;
        let patch = decode_cells(&patch)?;
        let author = author_id_from_bytes(&author)?;
        match &self.inner {
            WasmDbInner::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                block_on(db.update_attributed(author, &table, row_id, patch))
                    .map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                block_on(db.update_attributed(author, &table, row_id, patch))
                    .map_err(to_js_error)?,
            ),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = upsertEncodedAttributed)]
    pub fn upsert_encoded_attributed(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<WasmWrite, JsValue> {
        self.require_trusted_backend()?;
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let author = author_id_from_bytes(&author)?;
        match &self.inner {
            WasmDbInner::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                block_on(db.upsert_attributed(author, &table, row_id, cells))
                    .map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                block_on(db.upsert_attributed(author, &table, row_id, cells))
                    .map_err(to_js_error)?,
            ),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = deleteAttributed)]
    pub fn delete_attributed(
        &self,
        table: String,
        row_id: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<WasmWrite, JsValue> {
        self.require_trusted_backend()?;
        let row_id = row_uuid_from_bytes(&row_id)?;
        let author = author_id_from_bytes(&author)?;
        match &self.inner {
            WasmDbInner::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                block_on(db.delete_attributed(author, &table, row_id)).map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                block_on(db.delete_attributed(author, &table, row_id)).map_err(to_js_error)?,
            ),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = restoreEncodedAttributed)]
    pub fn restore_encoded_attributed(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<WasmWrite, JsValue> {
        self.require_trusted_backend()?;
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let author = author_id_from_bytes(&author)?;
        match &self.inner {
            WasmDbInner::Memory(db) => wasm_write_memory(
                Rc::clone(db),
                block_on(db.restore_attributed(author, &table, row_id, cells))
                    .map_err(to_js_error)?,
            ),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => wasm_write_browser(
                Rc::clone(db),
                block_on(db.restore_attributed(author, &table, row_id, cells))
                    .map_err(to_js_error)?,
            ),
            WasmDbInner::Closed => Err(JsValue::from_str("WasmDb is closed")),
        }
    }

    #[wasm_bindgen(js_name = openMemory)]
    pub fn open_memory(schema: Vec<u8>, config: Vec<u8>) -> Result<WasmDb, JsValue> {
        console_error_panic_hook::set_once();
        let (schema, config) = decode_open_args(&schema, &config)?;
        validate_untrusted_open_author(&config)?;
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let db = block_on(open_db(
            schema,
            MemoryStorage::new(&refs).expect("valid memory storage families"),
            config,
        ))
        .map_err(to_js_error)?;
        db.set_deferred_local_persistence(true);
        Ok(Self {
            inner: WasmDbInner::Memory(Rc::new(db)),
            owns_runtime: true,
            trusted_backend: false,
        })
    }

    /// Open a deliberate backend runtime. This remains a separate ABI from
    /// `openMemory`: the public raw-open configuration can never select the
    /// privileged system author.
    #[wasm_bindgen(js_name = openMemoryAsBackend)]
    pub fn open_memory_as_backend(schema: Vec<u8>, config: Vec<u8>) -> Result<WasmDb, JsValue> {
        console_error_panic_hook::set_once();
        let (schema, config) = decode_open_args(&schema, &config)?;
        let identity = backend_open_identity(&config)?;
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let db = block_on(open_backend_db(
            schema,
            MemoryStorage::new(&refs).expect("valid memory storage families"),
            config,
            identity,
        ))
        .map_err(to_js_error)?;
        db.set_deferred_local_persistence(true);
        Ok(Self {
            inner: WasmDbInner::Memory(Rc::new(db)),
            owns_runtime: true,
            trusted_backend: true,
        })
    }

    /// Open with a verified Jazz self-signed client identity. This deliberately
    /// stays separate from `openMemory`: untrusted open config bytes can never
    /// select a Jazz-reserved identity.
    #[wasm_bindgen(js_name = openMemoryWithSelfSignedProof)]
    pub fn open_memory_with_self_signed_proof(
        schema: Vec<u8>,
        config: Vec<u8>,
        token: String,
        app_id: String,
        claimed_author: String,
    ) -> Result<WasmDb, JsValue> {
        console_error_panic_hook::set_once();
        let (schema, mut config) = decode_open_args(&schema, &config)?;
        config.identity.author =
            verify_self_signed_runtime_author(&token, &app_id, &claimed_author)?;
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let db = block_on(open_db(
            schema,
            MemoryStorage::new(&refs).expect("valid memory storage families"),
            config,
        ))
        .map_err(to_js_error)?;
        db.set_deferred_local_persistence(true);
        Ok(Self {
            inner: WasmDbInner::Memory(Rc::new(db)),
            owns_runtime: true,
            trusted_backend: false,
        })
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = openBrowser)]
    pub async fn open_browser(
        page_store: JsValue,
        schema: Vec<u8>,
        config: Vec<u8>,
    ) -> Result<WasmDb, JsValue> {
        console_error_panic_hook::set_once();
        let (schema, config) = decode_open_args(&schema, &config)?;
        validate_untrusted_open_author(&config)?;
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = BrowserStorage::open(IndexedDbPageStore::from_js(page_store), &refs)
            .await
            .map_err(to_js_error)?;
        let db = open_db(schema, storage, config)
            .await
            .map_err(to_js_error)?;
        db.restore_browser_relay_pending_uploads()
            .map_err(to_js_error)?;
        db.set_deferred_local_persistence(true);
        Ok(Self {
            inner: WasmDbInner::Browser(Rc::new(db)),
            owns_runtime: true,
            trusted_backend: false,
        })
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = openBrowserWithSelfSignedProof)]
    pub async fn open_browser_with_self_signed_proof(
        page_store: JsValue,
        schema: Vec<u8>,
        config: Vec<u8>,
        token: String,
        app_id: String,
        claimed_author: String,
    ) -> Result<WasmDb, JsValue> {
        console_error_panic_hook::set_once();
        let (schema, mut config) = decode_open_args(&schema, &config)?;
        config.identity.author =
            verify_self_signed_runtime_author(&token, &app_id, &claimed_author)?;
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = BrowserStorage::open(IndexedDbPageStore::from_js(page_store), &refs)
            .await
            .map_err(to_js_error)?;
        let db = open_db(schema, storage, config)
            .await
            .map_err(to_js_error)?;
        db.restore_browser_relay_pending_uploads()
            .map_err(to_js_error)?;
        db.set_deferred_local_persistence(true);
        Ok(Self {
            inner: WasmDbInner::Browser(Rc::new(db)),
            owns_runtime: true,
            trusted_backend: false,
        })
    }

    /// Register a typed schema view backed by this same runtime owner.
    #[wasm_bindgen(js_name = registerSchema)]
    pub fn register_schema(&self, schema: Vec<u8>) -> Result<WasmDb, JsValue> {
        let schema = decode_public_schema(&schema)?;
        Ok(Self {
            inner: self
                .inner
                .register_schema_view(schema)
                .map_err(to_js_error)?,
            owns_runtime: false,
            trusted_backend: self.trusted_backend,
        })
    }

    /// Attach this typed view to an existing owner-wide mergeable transaction.
    #[wasm_bindgen(js_name = attachMergeableTx)]
    pub fn attach_mergeable_tx(&self, open_transaction_id: String) -> Result<WasmTx, JsValue> {
        let open_transaction_id = open_transaction_id
            .parse::<OpenTransactionId>()
            .map_err(|error| JsValue::from_str(&error))?;
        Ok(WasmTx {
            db: self.inner.clone(),
            kind: WasmTxKind::Mergeable,
            open_tx: Some(open_transaction_id),
            owns_lifetime: false,
        })
    }

    /// Attach this typed view to an existing owner-wide exclusive transaction.
    #[wasm_bindgen(js_name = attachExclusiveTx)]
    pub fn attach_exclusive_tx(&self, open_transaction_id: String) -> Result<WasmTx, JsValue> {
        let open_transaction_id = open_transaction_id
            .parse::<OpenTransactionId>()
            .map_err(|error| JsValue::from_str(&error))?;
        Ok(WasmTx {
            db: self.inner.clone(),
            kind: WasmTxKind::Exclusive,
            open_tx: Some(open_transaction_id),
            owns_lifetime: false,
        })
    }

    /// Begin one owner-wide transaction without creating an owning per-schema Tx.
    #[wasm_bindgen(js_name = beginTransaction)]
    pub fn begin_transaction(
        &self,
        open_transaction_id: String,
        kind: String,
        author: Option<Vec<u8>>,
    ) -> Result<(), JsValue> {
        let open_transaction_id = open_transaction_id
            .parse::<OpenTransactionId>()
            .map_err(|error| JsValue::from_str(&error))?;
        let author = author.as_deref().map(author_id_from_bytes).transpose()?;
        match kind.as_str() {
            "mergeable" => self
                .inner
                .begin_mergeable(open_transaction_id, author)
                .map_err(to_js_error),
            "exclusive" => self
                .inner
                .begin_exclusive(open_transaction_id, author)
                .map_err(to_js_error),
            _ => Err(JsValue::from_str(&unknown_transaction_kind_message(&kind))),
        }
    }

    /// Begin the only supported attributed transaction shape.  It is distinct
    /// from `beginTransaction` so an older binding fails closed rather than
    /// silently converting external provenance into SYSTEM authorship.
    #[wasm_bindgen(js_name = beginTransactionAttributed)]
    pub fn begin_transaction_attributed(
        &self,
        open_transaction_id: String,
        attribution: Vec<u8>,
    ) -> Result<(), JsValue> {
        self.require_trusted_backend()?;
        let open_transaction_id = open_transaction_id
            .parse::<OpenTransactionId>()
            .map_err(|error| JsValue::from_str(&error))?;
        let attribution = author_id_from_bytes(&attribution)?;
        match &self.inner {
            WasmDbInner::Memory(db) => {
                block_on(db.begin_mergeable_attributed(open_transaction_id, attribution))
            }
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => {
                block_on(db.begin_mergeable_attributed(open_transaction_id, attribution))
            }
            WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
        }
        .map_err(to_js_error)
    }

    /// Commit an owner-wide mergeable transaction by id.
    #[wasm_bindgen(js_name = commitTransaction)]
    pub fn commit_transaction(
        &self,
        open_transaction_id: String,
        kind: Option<String>,
    ) -> Result<WasmWrite, JsValue> {
        let open_transaction_id = open_transaction_id
            .parse::<OpenTransactionId>()
            .map_err(|error| JsValue::from_str(&error))?;
        let tx_id = match kind.as_deref().unwrap_or("mergeable") {
            "mergeable" => self.inner.commit_mergeable(open_transaction_id),
            "exclusive" => self.inner.commit_exclusive(open_transaction_id),
            kind => return Err(JsValue::from_str(&unknown_transaction_kind_message(kind))),
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

    /// Roll back an owner-wide open transaction by id.
    #[wasm_bindgen(js_name = rollbackTransaction)]
    pub fn rollback_transaction(&self, open_transaction_id: String) -> Result<(), JsValue> {
        let open_transaction_id = open_transaction_id
            .parse::<OpenTransactionId>()
            .map_err(|error| JsValue::from_str(&error))?;
        self.inner
            .abandon_transaction(open_transaction_id)
            .map_err(to_js_error)
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
        encode_synchronous_rows(&rows)
    }

    /// Asynchronous ordinary read. Unlike the legacy synchronous entry point,
    /// this can suspend to hydrate indirect large values without blocking the
    /// browser event loop that drives the owning peer transport.
    #[wasm_bindgen(js_name = allAsync)]
    pub fn all_async(
        &self,
        query: &WasmPreparedQuery,
        opts: JsValue,
    ) -> Result<js_sys::Promise, JsValue> {
        let inner = self.inner.clone();
        let query = query.inner.clone();
        let opts = read_opts_from_js(opts)?;
        Ok(future_to_promise(async move {
            let mut rows = inner.all_async(&query, opts).await.map_err(to_js_error)?;
            inner
                .hydrate_rows_for_binding(&mut rows)
                .await
                .map_err(to_js_error)?;
            bytes_to_js(encode_rows(&rows).map_err(to_js_error)?)
        }))
    }

    #[wasm_bindgen(js_name = one)]
    pub fn one(&self, query: &WasmPreparedQuery, opts: JsValue) -> Result<Vec<u8>, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let mut rows = self.inner.all(&query.inner, opts).map_err(to_js_error)?;
        rows.truncate(1);
        encode_synchronous_rows(&rows)
    }

    #[wasm_bindgen(js_name = allInTransaction)]
    pub fn all_in_transaction(
        &self,
        query: &WasmPreparedQuery,
        tx: &WasmTx,
        opts: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        ensure_transaction_runtime(&self.inner, tx)?;
        let opts = read_opts_from_js(opts)?;
        let tx_id = tx.open_tx_for_read()?;
        let rows = match tx.kind {
            WasmTxKind::Mergeable => self.inner.mergeable_all(tx_id, &query.inner, opts),
            WasmTxKind::Exclusive => self.inner.exclusive_all(tx_id, &query.inner, opts),
        }
        .map_err(to_js_error)?;
        encode_synchronous_rows(&rows)
    }

    #[wasm_bindgen(js_name = allInTransactionForIdentity)]
    pub fn all_in_transaction_for_identity(
        &self,
        query: &WasmPreparedQuery,
        tx: &WasmTx,
        author: Vec<u8>,
        opts: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        ensure_transaction_runtime(&self.inner, tx)?;
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
        encode_synchronous_rows(&rows)
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
        encode_synchronous_rows(&rows)
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
        encode_synchronous_rows(&rows)
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
        encode_synchronous_rows(&rows)
    }

    /// Identity-scoped asynchronous ordinary read; see [`Self::all_async`].
    #[wasm_bindgen(js_name = allForIdentityAsync)]
    pub fn all_for_identity_async(
        &self,
        query: &WasmPreparedQuery,
        author: Vec<u8>,
        opts: JsValue,
    ) -> Result<js_sys::Promise, JsValue> {
        let inner = self.inner.clone();
        let query = query.inner.clone();
        let author = author_id_from_bytes(&author)?;
        let opts = read_opts_from_js(opts)?;
        Ok(future_to_promise(async move {
            let mut rows = inner
                .all_for_identity_async(&query, opts, author)
                .await
                .map_err(to_js_error)?;
            inner
                .hydrate_rows_for_binding(&mut rows)
                .await
                .map_err(to_js_error)?;
            bytes_to_js(encode_rows(&rows).map_err(to_js_error)?)
        }))
    }

    #[wasm_bindgen(js_name = allRelationQuery)]
    pub fn all_relation_query(
        &self,
        query_json: String,
        opts: JsValue,
    ) -> Result<js_sys::Promise, JsValue> {
        let inner = self.inner.clone();
        let opts = read_opts_from_js(opts)?;
        let query = relation_query_from_json(&query_json)?;
        Ok(future_to_promise(async move {
            let mut snapshot = inner
                .all_relation_query(&query, opts)
                .await
                .map_err(to_js_error)?;
            inner
                .hydrate_relation_snapshot_for_binding(&mut snapshot)
                .await
                .map_err(to_js_error)?;
            bytes_to_js(encode_rows(&snapshot.rows).map_err(to_js_error)?)
        }))
    }

    #[wasm_bindgen(js_name = allRelationQueryForIdentity)]
    pub fn all_relation_query_for_identity(
        &self,
        query_json: String,
        author: Vec<u8>,
        opts: JsValue,
    ) -> Result<js_sys::Promise, JsValue> {
        let inner = self.inner.clone();
        let opts = read_opts_from_js(opts)?;
        let author = author_id_from_bytes(&author)?;
        let query = relation_query_from_json(&query_json)?;
        Ok(future_to_promise(async move {
            let mut snapshot = inner
                .all_relation_query_for_identity(&query, opts, author)
                .await
                .map_err(to_js_error)?;
            inner
                .hydrate_relation_snapshot_for_binding(&mut snapshot)
                .await
                .map_err(to_js_error)?;
            bytes_to_js(encode_rows(&snapshot.rows).map_err(to_js_error)?)
        }))
    }

    #[wasm_bindgen(js_name = allRelationSnapshot)]
    pub fn all_relation_snapshot(
        &self,
        query: &WasmPreparedQuery,
        opts: JsValue,
    ) -> Result<js_sys::Promise, JsValue> {
        let inner = self.inner.clone();
        let opts = read_opts_from_js(opts)?;
        let query = query.inner.clone();
        Ok(future_to_promise(async move {
            let mut snapshot = inner
                .all_relation_snapshot(&query, opts)
                .await
                .map_err(to_js_error)?;
            inner
                .hydrate_relation_snapshot_for_binding(&mut snapshot)
                .await
                .map_err(to_js_error)?;
            bytes_to_js(encode_relation_snapshot(&snapshot).map_err(to_js_error)?)
        }))
    }

    #[wasm_bindgen(js_name = allRelationSnapshotForIdentity)]
    pub fn all_relation_snapshot_for_identity(
        &self,
        query: &WasmPreparedQuery,
        author: Vec<u8>,
        opts: JsValue,
    ) -> Result<js_sys::Promise, JsValue> {
        let inner = self.inner.clone();
        let opts = read_opts_from_js(opts)?;
        let author = author_id_from_bytes(&author)?;
        let query = query.inner.clone();
        Ok(future_to_promise(async move {
            let mut snapshot = inner
                .all_relation_snapshot_for_identity(&query, opts, author)
                .await
                .map_err(to_js_error)?;
            inner
                .hydrate_relation_snapshot_for_binding(&mut snapshot)
                .await
                .map_err(to_js_error)?;
            bytes_to_js(encode_relation_snapshot(&snapshot).map_err(to_js_error)?)
        }))
    }

    #[wasm_bindgen(js_name = subscribe)]
    pub fn subscribe(&self, query: &WasmPreparedQuery, opts: JsValue) -> Result<JsValue, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let stream = self
            .inner
            .subscribe(&query.inner, opts)
            .map_err(to_js_error)?;
        subscription_stream_to_js(self.inner.clone(), stream)
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
        subscription_stream_to_js(self.inner.clone(), stream)
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
        subscription_stream_to_js(self.inner.clone(), stream)
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
        subscription_stream_to_js(self.inner.clone(), stream)
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

    #[wasm_bindgen(js_name = tick)]
    pub fn tick(&self) -> js_sys::Promise {
        let inner = self.inner.clone();
        future_to_promise(async move {
            inner.tick().await.map_err(to_js_error)?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Configure Jazz-owned upload ingress and unpublished-tree expiry limits.
    #[wasm_bindgen(js_name = setLargeValueStagingPolicy)]
    pub fn set_large_value_staging_policy(
        &self,
        incoming_bytes_per_window: f64,
        window_ms: f64,
        max_age_ms: Option<f64>,
    ) -> Result<(), JsValue> {
        let incoming_bytes_per_window =
            checked_js_u64(incoming_bytes_per_window, "incomingBytesPerWindow")?;
        let window_ms = checked_js_u64(window_ms, "windowMs")?;
        if window_ms < 1 {
            return Err(JsValue::from_str("windowMs must be at least 1"));
        }
        let max_age_ms = max_age_ms
            .map(|value| checked_js_u64(value, "maxAgeMs"))
            .transpose()?
            .unwrap_or(jazz::node::LargeValueStagingPolicy::default().max_age_ms);
        let policy = jazz::node::LargeValueStagingPolicy {
            incoming_bytes_per_window,
            window_ms,
            max_age_ms,
        };
        match &self.inner {
            WasmDbInner::Memory(db) => db.set_large_value_staging_policy(policy),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => db.set_large_value_staging_policy(policy),
            WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
        }
        Ok(())
    }

    /// Run one idempotent expiry pass; browser hosts normally call this from a timer.
    #[wasm_bindgen(js_name = evictExpiredStagedLargeValues)]
    pub fn evict_expired_staged_large_values(&self) -> js_sys::Promise {
        let inner = self.inner.clone();
        future_to_promise(async move {
            let evicted = match &inner {
                WasmDbInner::Memory(db) => db.evict_expired_staged_large_values().await,
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => db.evict_expired_staged_large_values().await,
                WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
            }
            .map_err(to_js_error)?;
            Ok(JsValue::from_f64(evicted as f64))
        })
    }

    #[wasm_bindgen(js_name = beginStreamingMutationEncoded)]
    #[allow(clippy::too_many_arguments)]
    pub fn begin_streaming_mutation_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        column: String,
        mutation: Option<String>,
        author: Option<Vec<u8>>,
        updated_at_ms: Option<f64>,
        head: Option<JsValue>,
        base: Option<JsValue>,
    ) -> Result<WasmStreamingMutation, JsValue> {
        self.begin_streaming_mutation_inner(
            table,
            row_id,
            cells,
            column,
            mutation,
            author,
            None,
            updated_at_ms,
            head,
            base,
        )
    }

    /// Trusted-backend streaming counterpart.  SYSTEM remains the admission
    /// identity; `attribution` is only final row provenance.  Branch streaming
    /// remains intentionally unsupported until its state model is designed.
    #[wasm_bindgen(js_name = beginStreamingMutationAttributedEncoded)]
    #[allow(clippy::too_many_arguments)]
    pub fn begin_streaming_mutation_attributed_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        column: String,
        mutation: Option<String>,
        author: Option<Vec<u8>>,
        attribution: Vec<u8>,
        updated_at_ms: Option<f64>,
        head: Option<JsValue>,
        base: Option<JsValue>,
    ) -> Result<WasmStreamingMutation, JsValue> {
        self.require_trusted_backend()?;
        if author.is_some() {
            return Err(JsValue::from_str(
                "backend-attributed streaming mutations cannot override backend admission identity",
            ));
        }
        if head
            .as_ref()
            .is_some_and(|value| !value.is_null() && !value.is_undefined())
            || base
                .as_ref()
                .is_some_and(|value| !value.is_null() && !value.is_undefined())
        {
            return Err(JsValue::from_str(
                "backend-attributed streaming mutations do not support branch writes",
            ));
        }
        let attribution = author_id_from_bytes(&attribution)?;
        self.begin_streaming_mutation_inner(
            table,
            row_id,
            cells,
            column,
            mutation,
            None,
            Some(attribution),
            updated_at_ms,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn begin_streaming_mutation_inner(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        column: String,
        mutation: Option<String>,
        author: Option<Vec<u8>>,
        attribution: Option<AuthorSubject>,
        updated_at_ms: Option<f64>,
        head: Option<JsValue>,
        base: Option<JsValue>,
    ) -> Result<WasmStreamingMutation, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let mutation = match mutation.as_deref().unwrap_or("insert") {
            "insert" => StreamingMutationKind::Insert,
            "update" => StreamingMutationKind::Update,
            "upsert" => StreamingMutationKind::Upsert,
            _ => return Err(JsValue::from_str("unknown streaming mutation kind")),
        };
        let identity = author.as_deref().map(author_id_from_bytes).transpose()?;
        let updated_at_ms = updated_at_ms
            .map(|value| checked_js_u64(value, "updatedAtMs"))
            .transpose()?;
        let head = head
            .filter(|value| !value.is_null() && !value.is_undefined())
            .map(|value| serde_wasm_bindgen::from_value(value).map_err(to_js_error))
            .transpose()?;
        let base = base
            .filter(|value| !value.is_null() && !value.is_undefined())
            .map(|value| serde_wasm_bindgen::from_value(value).map_err(to_js_error))
            .transpose()?;
        if base.is_some() && head.is_none() {
            return Err(JsValue::from_str(
                "streaming mutation base requires a branch head",
            ));
        }
        let upload = match &self.inner {
            WasmDbInner::Memory(db) => db.begin_streaming_value_upload(&table, &cells, &column),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => db.begin_streaming_value_upload(&table, &cells, &column),
            WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
        }
        .map_err(to_js_error)?;
        Ok(WasmStreamingMutation {
            state: Rc::new(RefCell::new(Some(WasmStreamingMutationState {
                db: self.inner.clone(),
                upload,
                mutation,
                table,
                row_id,
                cells,
                column,
                identity,
                attribution,
                updated_at_ms,
                head,
                base,
            }))),
        })
    }

    #[wasm_bindgen(js_name = readValueRange)]
    pub fn read_value_range(
        &self,
        table: String,
        row_id: Vec<u8>,
        column: String,
        start: f64,
        end: f64,
    ) -> Result<js_sys::Promise, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let range = checked_js_u64_range(start, end)?;
        let inner = self.inner.clone();
        Ok(future_to_promise(async move {
            let bytes = match &inner {
                WasmDbInner::Memory(db) => {
                    db.read_value_range(&table, row_id, &column, range).await
                }
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => {
                    db.read_value_range(&table, row_id, &column, range).await
                }
                WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
            }
            .map_err(to_js_error)?;
            Ok(js_sys::Uint8Array::from(bytes.as_slice()).into())
        }))
    }

    #[wasm_bindgen(js_name = readTextUtf16Range)]
    pub fn read_text_utf16_range(
        &self,
        table: String,
        row_id: Vec<u8>,
        column: String,
        start: f64,
        end: f64,
    ) -> Result<js_sys::Promise, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let range = checked_js_u64_range(start, end)?;
        let inner = self.inner.clone();
        Ok(future_to_promise(async move {
            let text = match &inner {
                WasmDbInner::Memory(db) => {
                    db.read_text_utf16_range(&table, row_id, &column, range)
                        .await
                }
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => {
                    db.read_text_utf16_range(&table, row_id, &column, range)
                        .await
                }
                WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
            }
            .map_err(to_js_error)?;
            Ok(JsValue::from_str(&text))
        }))
    }

    #[wasm_bindgen(js_name = readJsonPointer)]
    pub fn read_json_pointer(
        &self,
        table: String,
        row_id: Vec<u8>,
        column: String,
        pointer: String,
    ) -> Result<js_sys::Promise, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let inner = self.inner.clone();
        Ok(future_to_promise(async move {
            let value = match &inner {
                WasmDbInner::Memory(db) => {
                    db.read_json_pointer(&table, row_id, &column, &pointer)
                        .await
                }
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => {
                    db.read_json_pointer(&table, row_id, &column, &pointer)
                        .await
                }
                WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
            }
            .map_err(to_js_error)?;
            serde_wasm_bindgen::to_value(&value)
                .map_err(|error| JsValue::from_str(&error.to_string()))
        }))
    }

    #[wasm_bindgen(js_name = appendValue)]
    pub fn append_value(
        &self,
        table: String,
        row_id: Vec<u8>,
        column: String,
        bytes: Vec<u8>,
    ) -> Result<js_sys::Promise, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let inner = self.inner.clone();
        Ok(future_to_promise(async move {
            let write = match &inner {
                WasmDbInner::Memory(db) => wasm_write_memory(
                    Rc::clone(db),
                    db.append_value(&table, row_id, &column, bytes)
                        .await
                        .map_err(to_js_error)?,
                ),
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => wasm_write_browser(
                    Rc::clone(db),
                    db.append_value(&table, row_id, &column, bytes)
                        .await
                        .map_err(to_js_error)?,
                ),
                WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
            }?;
            Ok(write.into())
        }))
    }

    #[wasm_bindgen(js_name = spliceValue)]
    pub fn splice_value(
        &self,
        table: String,
        row_id: Vec<u8>,
        column: String,
        offset: f64,
        delete_length: f64,
        insert: Vec<u8>,
    ) -> Result<js_sys::Promise, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let offset = checked_js_u64(offset, "offset")?;
        let delete_length = checked_js_u64(delete_length, "deleteLength")?;
        let inner = self.inner.clone();
        Ok(future_to_promise(async move {
            let write = match &inner {
                WasmDbInner::Memory(db) => wasm_write_memory(
                    Rc::clone(db),
                    db.splice_value(&table, row_id, &column, offset, delete_length, insert)
                        .await
                        .map_err(to_js_error)?,
                ),
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => wasm_write_browser(
                    Rc::clone(db),
                    db.splice_value(&table, row_id, &column, offset, delete_length, insert)
                        .await
                        .map_err(to_js_error)?,
                ),
                WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
            }?;
            Ok(write.into())
        }))
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

    #[wasm_bindgen(js_name = setRelayAuthoritySessionOwner)]
    pub fn set_relay_authority_session_owner(&self) -> Result<(), JsValue> {
        match &self.inner {
            WasmDbInner::Memory(db) => db.set_relay_authority_session_owner(),
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => db.set_relay_authority_session_owner(),
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
                connection: Some(jazz::db::block_on(db.connect_upstream(transport))),
            },
            #[cfg(target_arch = "wasm32")]
            WasmDbInner::Browser(db) => WasmTransportInner::Browser {
                db: Rc::clone(db),
                connection: Some(jazz::db::block_on(db.connect_upstream(transport))),
            },
            WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
        };
        let auxiliary_pump = inner.auxiliary_pump();
        Ok(WasmTransport {
            inner,
            queues,
            auxiliary_pump,
            protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
            features: jazz::wire::current_wire_features()
                & !(jazz::wire::FEATURE_AUTHORIZATION_SCOPE_RECEIPTS
                    | jazz::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS),
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
    ) -> Result<js_sys::Promise, JsValue> {
        let db_inner = self.inner.clone();
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
            link_identity: AuthorSubject::for_test_bytes(local_node),
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
        Ok(future_to_promise(async move {
            let inner = match &db_inner {
                WasmDbInner::Memory(db) => WasmTransportInner::Memory {
                    db: Rc::clone(db),
                    connection: Some(db.connect_upstream(transport).await),
                },
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => WasmTransportInner::Browser {
                    db: Rc::clone(db),
                    connection: Some(db.connect_upstream(transport).await),
                },
                WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
            };
            let auxiliary_pump = inner.auxiliary_pump();
            Ok(WasmTransport {
                inner,
                queues,
                auxiliary_pump,
                protocol_version,
                features: features as u64,
                subscriber_identity: None,
            }
            .into())
        }))
    }

    #[wasm_bindgen(js_name = acceptSubscriber)]
    pub fn accept_subscriber(
        &self,
        identity: Vec<u8>,
        claims: JsValue,
    ) -> Result<WasmTransport, JsValue> {
        let identity = author_id_from_bytes(&identity)?;
        let claims = claims_from_js(identity, claims)?;
        self.accept_subscriber_with_admitted_identity(identity, claims)
    }

    /// Attach a browser-local follower for a verified first-party identity.
    ///
    /// This is deliberately separate from [`Self::accept_subscriber`]: raw
    /// serialized identities must keep rejecting Jazz-reserved issuers, while
    /// a local-first or anonymous browser worker can prove its own identity
    /// with the same signed capability that opened the worker database.
    #[wasm_bindgen(js_name = acceptSubscriberWithSelfSignedProof)]
    pub fn accept_subscriber_with_self_signed_proof(
        &self,
        claims: JsValue,
        token: String,
        app_id: String,
        claimed_author: String,
    ) -> Result<WasmTransport, JsValue> {
        let identity = verify_self_signed_runtime_author(&token, &app_id, &claimed_author)?;
        let claims = claims_from_js(identity, claims)?;
        self.accept_subscriber_with_admitted_identity(identity, claims)
    }

    fn accept_subscriber_with_admitted_identity(
        &self,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) -> Result<WasmTransport, JsValue> {
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
        let auxiliary_pump = inner.auxiliary_pump();
        Ok(WasmTransport {
            inner,
            queues,
            auxiliary_pump,
            protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
            features: jazz::wire::current_wire_features()
                & !(jazz::wire::FEATURE_AUTHORIZATION_SCOPE_RECEIPTS
                    | jazz::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS),
            subscriber_identity: Some(identity),
        })
    }

    #[wasm_bindgen(js_name = mergeableTx)]
    pub fn mergeable_tx(&self, open_transaction_id: String) -> Result<WasmTx, JsValue> {
        let open_transaction_id = open_transaction_id
            .parse::<OpenTransactionId>()
            .map_err(|error| JsValue::from_str(&error))?;
        self.inner
            .begin_mergeable(open_transaction_id, None)
            .map_err(to_js_error)?;
        Ok(WasmTx {
            db: self.inner.clone(),
            kind: WasmTxKind::Mergeable,
            open_tx: Some(open_transaction_id),
            owns_lifetime: true,
        })
    }

    #[wasm_bindgen(js_name = mergeableTxForIdentity)]
    pub fn mergeable_tx_for_identity(
        &self,
        open_transaction_id: String,
        author: Vec<u8>,
    ) -> Result<WasmTx, JsValue> {
        let open_transaction_id = open_transaction_id
            .parse::<OpenTransactionId>()
            .map_err(|error| JsValue::from_str(&error))?;
        let author = author_id_from_bytes(&author)?;
        self.inner
            .begin_mergeable(open_transaction_id, Some(author))
            .map_err(to_js_error)?;
        Ok(WasmTx {
            db: self.inner.clone(),
            kind: WasmTxKind::Mergeable,
            open_tx: Some(open_transaction_id),
            owns_lifetime: true,
        })
    }

    #[wasm_bindgen(js_name = exclusiveTx)]
    pub fn exclusive_tx(&self, open_transaction_id: String) -> Result<WasmTx, JsValue> {
        let open_transaction_id = open_transaction_id
            .parse::<OpenTransactionId>()
            .map_err(|error| JsValue::from_str(&error))?;
        self.inner
            .begin_exclusive(open_transaction_id, None)
            .map_err(to_js_error)?;
        Ok(WasmTx {
            db: self.inner.clone(),
            kind: WasmTxKind::Exclusive,
            open_tx: Some(open_transaction_id),
            owns_lifetime: true,
        })
    }

    #[wasm_bindgen(js_name = close)]
    pub fn close(&mut self) -> js_sys::Promise {
        let inner = std::mem::replace(&mut self.inner, WasmDbInner::Closed);
        let owns_runtime = self.owns_runtime;
        future_to_promise(async move {
            if !owns_runtime {
                return Ok(JsValue::from_bool(!matches!(inner, WasmDbInner::Closed)));
            }
            let closed = match inner {
                WasmDbInner::Memory(db) => {
                    db.close().await.map_err(to_js_error)?;
                    true
                }
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => {
                    db.close().await.map_err(to_js_error)?;
                    true
                }
                WasmDbInner::Closed => false,
            };
            Ok(JsValue::from_bool(closed))
        })
    }
}

#[wasm_bindgen]
impl WasmTransport {
    #[wasm_bindgen(js_name = updateAuthenticatedClaims)]
    pub fn update_authenticated_claims(&self, claims: JsValue) -> Result<js_sys::Promise, JsValue> {
        let identity = self
            .subscriber_identity
            .ok_or_else(|| JsValue::from_str("transport is not a subscriber link"))?;
        let claims = claims_from_js(identity, claims)?;
        let inner = self.inner.clone();
        Ok(future_to_promise(async move {
            inner.update_authenticated_claims(claims).await?;
            Ok(JsValue::UNDEFINED)
        }))
    }

    /// Route one socket frame through the chunk lane before semantic delivery.
    /// Returns the original frame when it belongs to the ordinary Jazz lane.
    #[wasm_bindgen(js_name = routeAuxiliaryWireFrame)]
    pub fn route_auxiliary_wire_frame(&self, frame: Vec<u8>) -> js_sys::Promise {
        let pump = self.auxiliary_pump.clone();
        let features = self.features;
        future_to_promise(async move {
            match pump
                .route_incoming_wire_frame(frame, features)
                .await
                .map_err(|error| JsValue::from_str(&error))?
            {
                Some(frame) => Ok(js_sys::Uint8Array::from(frame.as_slice()).into()),
                None => Ok(JsValue::UNDEFINED),
            }
        })
    }

    /// Drain a bounded FIFO batch of complete auxiliary wire frames
    /// independently of semantic ticks. Browser bindings choose a small
    /// MessagePort batch so a burst of legal chunk responses cannot become one
    /// giant structured-clone allocation.
    #[wasm_bindgen(js_name = recvAuxiliaryWireFrames)]
    pub fn recv_auxiliary_wire_frames(
        &self,
        max_frames: Option<u32>,
        max_bytes: Option<u32>,
    ) -> Result<js_sys::Array, JsValue> {
        let max_frames = max_frames.unwrap_or(1).max(1) as usize;
        let max_bytes = max_bytes
            .unwrap_or(jazz::protocol_limits::MAX_WIRE_FRAME_BYTES as u32)
            .max(1) as usize;
        let frames = js_sys::Array::new();
        for frame in self
            .auxiliary_pump
            .take_outbound_wire_frames(
                self.protocol_version,
                self.features,
                None,
                max_frames,
                max_bytes,
            )
            .map_err(|error| JsValue::from_str(&error))?
        {
            frames.push(&js_sys::Uint8Array::from(frame.as_slice()).into());
        }
        Ok(frames)
    }

    /// Resolve when the independently driven chunk lane has socket output.
    #[wasm_bindgen(js_name = auxiliaryOutboundReady)]
    pub fn auxiliary_outbound_ready(&self) -> js_sys::Promise {
        let pump = self.auxiliary_pump.clone();
        future_to_promise(async move {
            pump.outbound_ready().await;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Drain this transport's bounded, redacted chunk-relay flight recorder.
    /// This is intentionally diagnostics-only: capabilities and full content
    /// hashes never cross the JS boundary.
    #[wasm_bindgen(js_name = takeAuxiliaryTrace)]
    pub fn take_auxiliary_trace(&self) -> js_sys::Array {
        let entries = js_sys::Array::new();
        for trace in self.auxiliary_pump.take_trace() {
            let entry = js_sys::Object::new();
            let set = |name: &str, value: JsValue| {
                let _ = js_sys::Reflect::set(&entry, &JsValue::from_str(name), &value);
            };
            set("event", JsValue::from_str(trace.event));
            set("role", JsValue::from_str(trace.role));
            // IDs are opaque diagnostic correlation keys. Keep the full u64
            // rather than lossy-converting it to a JavaScript number.
            set(
                "connection",
                JsValue::from_str(&trace.connection.to_string()),
            );
            set(
                "requestId",
                JsValue::from_str(&trace.request_id.to_string()),
            );
            set(
                "remainingHops",
                JsValue::from_f64(trace.remaining_hops as f64),
            );
            set("objectHash", JsValue::from_str(&trace.object_hash));
            set(
                "locatorFingerprint",
                JsValue::from_str(&trace.locator_fingerprint),
            );
            if let Some(response) = trace.response {
                set("response", JsValue::from_str(response));
            }
            if let Some(storage_error) = trace.storage_error {
                set("storageError", JsValue::from_str(storage_error));
            }
            entries.push(&entry);
        }
        entries
    }

    /// Enable this transport's bounded redacted auxiliary flight recorder.
    #[wasm_bindgen(js_name = setAuxiliaryTraceEnabled)]
    pub fn set_auxiliary_trace_enabled(&self, enabled: bool) {
        self.auxiliary_pump.set_trace_enabled(enabled);
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

    #[wasm_bindgen(js_name = setOutboundScheduler)]
    pub fn set_outbound_scheduler(&self, callback: js_sys::Function) {
        *self.queues.outbound_scheduler.borrow_mut() = Some(callback);
    }

    #[wasm_bindgen(js_name = clearOutboundScheduler)]
    pub fn clear_outbound_scheduler(&self) {
        self.queues.outbound_scheduler.borrow_mut().take();
    }

    #[wasm_bindgen(js_name = tick)]
    pub fn tick(&self) -> js_sys::Promise {
        let inner = self.inner.clone();
        future_to_promise(async move {
            let work = inner.tick().await?;
            Ok(JsValue::from_f64(work as f64))
        })
    }

    #[wasm_bindgen(js_name = close)]
    pub fn close(&mut self) -> bool {
        self.auxiliary_pump.disconnect();
        self.inner.close()
    }
}

#[wasm_bindgen]
impl WasmTx {
    #[wasm_bindgen(js_name = insertEncoded)]
    pub fn insert_encoded_with_options(
        &mut self,
        table: String,
        cells: Vec<u8>,
        options: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let cells = decode_cells(&cells)?;
        let options = insert_options_from_js(options)?;
        let open_tx = self.open_tx_for_read()?;
        let row = with_wasm_db!(&self.db, |db| match self.kind {
            WasmTxKind::Mergeable =>
                block_on(db.mergeable_tx_ref(open_tx).insert(&table, cells, options,)),
            WasmTxKind::Exclusive =>
                block_on(db.exclusive_tx_ref(open_tx).insert(&table, cells, options,)),
        })
        .map_err(to_js_error)?;
        Ok(row.to_bytes())
    }

    #[wasm_bindgen(js_name = updateEncoded)]
    pub fn update_encoded_with_options(
        &mut self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        options: JsValue,
    ) -> Result<(), JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let patch = decode_cells(&patch)?;
        let options = update_options_from_js(options)?;
        let open_tx = self.open_tx_for_read()?;
        with_wasm_db!(&self.db, |db| match self.kind {
            WasmTxKind::Mergeable => block_on(
                db.mergeable_tx_ref(open_tx)
                    .update(&table, row_id, patch, options,)
            ),
            WasmTxKind::Exclusive => block_on(
                db.exclusive_tx_ref(open_tx)
                    .update(&table, row_id, patch, options,)
            ),
        })
        .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = upsertEncoded)]
    pub fn upsert_encoded_with_options(
        &mut self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        options: JsValue,
    ) -> Result<(), JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let options = upsert_options_from_js(options)?;
        let open_tx = self.open_tx_for_read()?;
        with_wasm_db!(&self.db, |db| match self.kind {
            WasmTxKind::Mergeable => block_on(
                db.mergeable_tx_ref(open_tx)
                    .upsert(&table, row_id, cells, options,)
            ),
            WasmTxKind::Exclusive => block_on(
                db.exclusive_tx_ref(open_tx)
                    .upsert(&table, row_id, cells, options,)
            ),
        })
        .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = deleteEncoded)]
    pub fn delete_encoded_with_options(
        &mut self,
        table: String,
        row_id: Vec<u8>,
        options: JsValue,
    ) -> Result<(), JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let options = delete_options_from_js(options)?;
        let open_tx = self.open_tx_for_read()?;
        with_wasm_db!(&self.db, |db| match self.kind {
            WasmTxKind::Mergeable => {
                block_on(db.mergeable_tx_ref(open_tx).delete(&table, row_id, options))
            }
            WasmTxKind::Exclusive => {
                block_on(db.exclusive_tx_ref(open_tx).delete(&table, row_id, options))
            }
        })
        .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = restoreEncoded)]
    pub fn restore_encoded_with_options(
        &mut self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        options: JsValue,
    ) -> Result<(), JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let options = restore_options_from_js(options)?;
        let open_tx = self.open_tx_for_read()?;
        with_wasm_db!(&self.db, |db| match self.kind {
            WasmTxKind::Mergeable =>
                block_on(
                    db.mergeable_tx_ref(open_tx)
                        .restore(&table, row_id, Some(cells), options,)
                ),
            WasmTxKind::Exclusive =>
                block_on(
                    db.exclusive_tx_ref(open_tx)
                        .restore(&table, row_id, Some(cells), options,)
                ),
        })
        .map_err(to_js_error)
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

    fn open_tx_for_read(&self) -> Result<OpenTransactionId, JsValue> {
        self.open_tx
            .ok_or_else(|| JsValue::from_str("transaction is already closed"))
    }
}

fn read_rows_for_transaction(
    db: &WasmDbInner,
    query: &WasmPreparedQuery,
    tx: &WasmTx,
    author: Option<AuthorSubject>,
    opts: JsValue,
) -> Result<Vec<jazz::node::CurrentRow>, JsValue> {
    ensure_transaction_runtime(db, tx)?;
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

fn ensure_transaction_runtime(db: &WasmDbInner, tx: &WasmTx) -> Result<(), JsValue> {
    if db.shares_runtime_with(&tx.db) {
        Ok(())
    } else {
        Err(JsValue::from_str(
            "transaction belongs to a different database runtime",
        ))
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

fn write_option(options: &JsValue, name: &str) -> Result<Option<JsValue>, JsValue> {
    if options.is_null() || options.is_undefined() {
        return Ok(None);
    }
    let value = js_sys::Reflect::get(options, &JsValue::from_str(name))?;
    Ok((!value.is_null() && !value.is_undefined()).then_some(value))
}

fn write_identity_option(options: &JsValue) -> Result<jazz::db::WriteIdentity, JsValue> {
    write_option(options, "author")?
        .map(|author| {
            author_id_from_bytes(&js_sys::Uint8Array::new(&author).to_vec())
                .map(jazz::db::WriteIdentity::Session)
        })
        .transpose()
        .map(|identity| identity.unwrap_or_default())
}

fn write_timestamp_option(options: &JsValue) -> Result<Option<u64>, JsValue> {
    write_option(options, "updatedAtMs")?
        .map(|value| {
            value
                .as_f64()
                .ok_or_else(|| JsValue::from_str("updatedAtMs must be a number"))
                .and_then(|value| checked_js_u64(value, "updatedAtMs"))
        })
        .transpose()
}

fn insert_options_from_js(options: JsValue) -> Result<jazz::db::InsertOptions, JsValue> {
    Ok(jazz::db::InsertOptions {
        row_id: write_option(&options, "rowId")?
            .map(|row_id| row_uuid_from_bytes(&js_sys::Uint8Array::new(&row_id).to_vec()))
            .transpose()?,
        identity: write_identity_option(&options)?,
        target: write_option(&options, "branch")?
            .map(|branch| {
                serde_wasm_bindgen::from_value(branch)
                    .map(jazz::db::ExactWriteTarget::Branch)
                    .map_err(to_js_error)
            })
            .transpose()?
            .unwrap_or_default(),
        updated_at_ms: write_timestamp_option(&options)?,
    })
}

fn update_options_from_js(options: JsValue) -> Result<jazz::db::UpdateOptions, JsValue> {
    let head = write_option(&options, "head")?;
    let base = write_option(&options, "base")?;
    let target = match head {
        Some(head) => jazz::db::WriteTarget::BranchView {
            head: serde_wasm_bindgen::from_value(head).map_err(to_js_error)?,
            base: base
                .map(|base| serde_wasm_bindgen::from_value(base).map_err(to_js_error))
                .transpose()?,
        },
        None if base.is_none() => Default::default(),
        None => {
            return Err(JsValue::from_str(
                "branch view base requires a head selector",
            ))
        }
    };
    Ok(jazz::db::UpdateOptions {
        identity: write_identity_option(&options)?,
        target,
        updated_at_ms: write_timestamp_option(&options)?,
    })
}

fn upsert_options_from_js(options: JsValue) -> Result<jazz::db::UpsertOptions, JsValue> {
    Ok(jazz::db::UpsertOptions {
        identity: write_identity_option(&options)?,
        target: write_option(&options, "branch")?
            .map(|branch| {
                serde_wasm_bindgen::from_value(branch)
                    .map(jazz::db::ExactWriteTarget::Branch)
                    .map_err(to_js_error)
            })
            .transpose()?
            .unwrap_or_default(),
        updated_at_ms: write_timestamp_option(&options)?,
    })
}

fn delete_options_from_js(options: JsValue) -> Result<jazz::db::DeleteOptions, JsValue> {
    let options = update_options_from_js(options)?;
    Ok(jazz::db::DeleteOptions {
        identity: options.identity,
        target: options.target,
        updated_at_ms: options.updated_at_ms,
    })
}

fn restore_options_from_js(options: JsValue) -> Result<jazz::db::RestoreOptions, JsValue> {
    let options = upsert_options_from_js(options)?;
    Ok(jazz::db::RestoreOptions {
        identity: options.identity,
        target: options.target,
        updated_at_ms: options.updated_at_ms,
    })
}

fn decode_open_args(
    schema: &[u8],
    config: &[u8],
) -> Result<(JazzSchema, WasmOpenDbConfig), JsValue> {
    let schema = decode_public_schema(schema)?;
    let config: WasmOpenDbConfig = postcard::from_bytes(config)
        .map_err(|err| to_js_error(format!("decode open config: {err}")))?;
    Ok((schema, config))
}

fn decode_public_schema(schema: &[u8]) -> Result<JazzSchema, JsValue> {
    jazz::tools::public_schema_convert::decode_public_schema_json(schema).map_err(to_js_error)
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

async fn open_db<S>(
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
        let db = Db::open_history_complete(db_config).await?;
        configure_initial_sync_flush_cadence(&db, initial_sync_flush_every)?;
        Ok(db)
    } else {
        let db = Db::open(db_config).await?;
        configure_initial_sync_flush_cadence(&db, initial_sync_flush_every)?;
        Ok(db)
    }
}

async fn open_backend_db<S>(
    schema: JazzSchema,
    storage: S,
    config: WasmOpenDbConfig,
    identity: DbIdentity,
) -> Result<Db<S>, jazz::db::Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let mut db_config = DbConfig::new(schema, storage, identity);
    if let Some(seed) = config.row_id_seed {
        db_config = db_config.with_id_source(SeededRowIdSource::new(seed));
    }
    let initial_sync_flush_every = config.initial_sync_flush_every;
    // SAFETY: this function is called solely by the explicit, non-raw backend
    // open ABI above, after it has validated the caller-controlled envelope.
    let db = if config.history_complete {
        unsafe { Db::open_history_complete_with_backend_attribution(db_config).await? }
    } else {
        unsafe { Db::open_with_backend_attribution(db_config).await? }
    };
    configure_initial_sync_flush_cadence(&db, initial_sync_flush_every)?;
    Ok(db)
}

fn validate_untrusted_open_author(config: &WasmOpenDbConfig) -> Result<(), JsValue> {
    validate_untrusted_open_author_core(config)
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

fn backend_open_identity(config: &WasmOpenDbConfig) -> Result<DbIdentity, JsValue> {
    backend_open_identity_core(config).map_err(|error| JsValue::from_str(&error.to_string()))
}

fn backend_open_identity_core(
    config: &WasmOpenDbConfig,
) -> Result<DbIdentity, jazz::ids::AuthorSubjectError> {
    // Validate every caller-controlled field at the ordinary fail-closed
    // ingress before this explicit, intentional backend ABI derives SYSTEM.
    validate_untrusted_open_author_core(config)?;
    Ok(DbIdentity {
        node: config.identity.node,
        author: AuthorSubject::SYSTEM,
    })
}

fn validate_untrusted_open_author_core(
    config: &WasmOpenDbConfig,
) -> Result<(), jazz::ids::AuthorSubjectError> {
    AuthorSubject::from_untrusted_canonical(config.identity.author.canonical()).map(|_| ())
}

fn verify_self_signed_runtime_author(
    token: &str,
    app_id: &str,
    claimed_author: &str,
) -> Result<AuthorSubject, JsValue> {
    verify_self_signed_runtime_author_core(token, app_id, claimed_author)
        .map_err(|error| JsValue::from_str(&error))
}

fn verify_self_signed_runtime_author_core(
    token: &str,
    app_id: &str,
    claimed_author: &str,
) -> Result<AuthorSubject, String> {
    // `std::time::SystemTime` panics under wasm32. The verifier's explicit
    // clock form keeps proof expiry validation intact while using the browser-
    // safe clock already used by this binding.
    let now_seconds = web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs();
    verify_self_signed_runtime_author_at(token, app_id, claimed_author, now_seconds)
}

// This stays internal because it is a binding boundary receipt: the public
// WASM API obtains time from the browser above, while this form lets the
// binding prove that it passes that clock into Jazz's normal proof verifier.
fn verify_self_signed_runtime_author_at(
    token: &str,
    app_id: &str,
    claimed_author: &str,
    now_seconds: u64,
) -> Result<AuthorSubject, String> {
    jazz::tools::identity::verify_client_runtime_author_at(
        token,
        app_id,
        claimed_author,
        now_seconds,
    )
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

async fn tick_connection<S>(
    connection: &Option<Rc<LocalMutex<PeerConnection<S>>>>,
) -> Result<u32, JsValue>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let Some(connection) = connection else {
        return Ok(0);
    };
    let stats = connection.lock().await.tick().await.map_err(to_js_error)?;
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

fn checked_js_u64(value: f64, name: &str) -> Result<u64, JsValue> {
    checked_js_safe_u64(value)
        .ok_or_else(|| JsValue::from_str(&format!("{name} must be a nonnegative safe integer")))
}

fn checked_js_safe_u64(value: f64) -> Option<u64> {
    (value.is_finite()
        && value >= 0.0
        && value.fract() == 0.0
        && value <= jazz::tools::policy_claims::MAX_SAFE_JS_INTEGER as f64)
        .then_some(value as u64)
}

fn checked_js_u64_range(start: f64, end: f64) -> Result<std::ops::Range<u64>, JsValue> {
    let start = checked_js_u64(start, "start")?;
    let end = checked_js_u64(end, "end")?;
    if start > end {
        return Err(JsValue::from_str("start must not exceed end"));
    }
    Ok(start..end)
}

fn author_id_from_bytes(bytes: &[u8]) -> Result<AuthorSubject, JsValue> {
    let canonical = std::str::from_utf8(bytes)
        .map_err(|_| JsValue::from_str("author subject must be canonical UTF-8 JSON"))?;
    AuthorSubject::from_untrusted_canonical(canonical)
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

fn claims_from_js(
    author: AuthorSubject,
    claims: JsValue,
) -> Result<BTreeMap<String, Value>, JsValue> {
    let raw: serde_json::Value = serde_wasm_bindgen::from_value(claims).map_err(to_js_error)?;
    let claims = match raw {
        serde_json::Value::Null => BTreeMap::new(),
        serde_json::Value::Object(map) => map
            .into_iter()
            .map(|(key, value)| Ok((key, claim_value_from_json(value)?)))
            .collect::<Result<BTreeMap<_, _>, JsValue>>()?,
        _ => return Err(JsValue::from_str("identity claims must be an object")),
    };
    Ok(admit_binding_claims(author, claims))
}

/// Admit raw provider claims received through a binding-local transport.
///
/// The transport identity is the authority for the reserved policy fields and
/// for `session.claims.iss`/`session.claims.sub`; callers may not replace
/// those values by supplying similarly named provider claims. All other
/// provider values remain below the collision-proof `session.claims` prefix.
fn admit_binding_claims(
    author: AuthorSubject,
    claims: BTreeMap<String, Value>,
) -> BTreeMap<String, Value> {
    let (issuer, subject): (String, String) = serde_json::from_str(author.canonical())
        .expect("author subjects always have canonical issuer/subject JSON");
    let mut admitted = claims
        .into_iter()
        .map(|(name, value)| (jazz::query::provider_claim_key(&name), value))
        .collect::<BTreeMap<_, _>>();
    admitted.insert(
        jazz::query::provider_claim_key("iss"),
        Value::String(issuer.clone()),
    );
    admitted.insert(
        jazz::query::provider_claim_key("sub"),
        Value::String(subject),
    );
    admitted.insert(
        "user".to_owned(),
        Value::String(author.canonical().to_owned()),
    );
    admitted.insert(
        "authMode".to_owned(),
        Value::String(auth_mode_for_author(&issuer).to_owned()),
    );
    admitted
}

fn auth_mode_for_author(issuer: &str) -> &'static str {
    match issuer {
        AuthorSubject::LOCAL_FIRST_ISSUER => "local-first",
        AuthorSubject::ANONYMOUS_ISSUER => "anonymous",
        _ => "external",
    }
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
        row_id: result.row_id,
        tx_id: TransactionId::from_committed_tx(tx_id),
        inner: Some(WasmWriteInner::MemoryTx { db, tx_id }),
    })
}

#[cfg(target_arch = "wasm32")]
fn wasm_write_browser(
    db: Rc<Db<BrowserStorage>>,
    write: WriteHandle<BrowserStorage>,
) -> Result<WasmWrite, JsValue> {
    let tx_id = write.mergeable_tx_id();
    let result = WasmWriteResult {
        row_id: write.row_uuid(),
        tx_id,
    };
    Ok(WasmWrite {
        payload: postcard::to_allocvec(&result).map_err(to_js_error)?,
        row_id: result.row_id,
        tx_id: TransactionId::from_committed_tx(tx_id),
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
        row_id: result.row_id,
        tx_id: TransactionId::from_committed_tx(tx_id),
        inner,
    })
}

fn read_opts_from_js(value: JsValue) -> Result<ReadOpts, JsValue> {
    let mut opts = ReadOpts::default();
    if value.is_undefined() || value.is_null() {
        return Ok(opts);
    }
    for name in ["read_view", "readView"] {
        let prop = js_sys::Reflect::get(&value, &JsValue::from_str(name))?;
        if !prop.is_undefined() && !prop.is_null() {
            opts.read_view = serde_wasm_bindgen::from_value::<ReadViewSpec>(prop)
                .map_err(|error| JsValue::from_str(&format!("invalid read_view: {error}")))?;
            break;
        }
    }
    if let Some(tier) = optional_string_prop(&value, "tier")? {
        opts.tier = read_tier_from_str(&tier)?;
    }
    if let Some(local_updates) = optional_string_prop(&value, "local_updates")? {
        opts.local_updates = match local_updates.as_str() {
            "Immediate" | "immediate" => LocalUpdates::Immediate,
            "Deferred" | "deferred" => LocalUpdates::Deferred,
            other => return Err(JsValue::from_str(&format!("unknown local_updates {other}"))),
        };
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

/// Read-only binding lowering. Write waits keep `durability_tier_from_str`, so
/// a product read choice can never change write-settlement semantics.
fn read_tier_from_str(tier: &str) -> Result<DurabilityTier, JsValue> {
    match tier {
        "local-first" | "LocalFirst" => Ok(DurabilityTier::Local),
        // The host connection manager applies the explicit-offline decision
        // before invoking this ABI. A direct WASM caller therefore gets the
        // strict remote behavior for RemoteIfPossible.
        "remote" | "Remote" | "remote-if-possible" | "RemoteIfPossible" => Ok(DurabilityTier::Edge),
        _ => durability_tier_from_str(tier),
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

/// Synchronous WASM reads cannot suspend for a missing immutable chunk. Until
/// their API is made asynchronous, fail at the binding boundary instead of
/// handing a physical `Value::Large` tag to JavaScript's logical row decoder.
/// Relation reads and subscriptions already use the async materialization path.
fn encode_synchronous_rows(rows: &[jazz::node::CurrentRow]) -> Result<Vec<u8>, JsValue> {
    for row in rows {
        let (descriptor, raw) = row.encoded_record();
        let values = descriptor.bind(raw).to_values().map_err(to_js_error)?;
        if values.iter().any(value_contains_indirect_scalar) {
            return Err(JsValue::from_str(
                "synchronous WASM all/transaction reads cannot materialize a large value; use an async relation read or subscription instead",
            ));
        }
    }
    encode_rows(rows).map_err(to_js_error)
}

fn value_contains_indirect_scalar(value: &Value) -> bool {
    match value {
        Value::Large(_) => true,
        Value::Nullable(Some(value)) => value_contains_indirect_scalar(value),
        _ => false,
    }
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
    db: WasmDbInner,
    stream: impl Stream<Item = SubscriptionEvent> + 'static,
) -> Result<JsValue, JsValue> {
    let state = (
        db,
        Box::pin(stream) as Pin<Box<dyn Stream<Item = SubscriptionEvent>>>,
    );
    readable_stream_from_stream(stream::unfold(state, |(db, mut source)| async move {
        let mut event = match source.next().await {
            Some(event) => event,
            None => return None,
        };
        let mut retry_attempt = 0;
        loop {
            match db.hydrate_subscription_event_for_binding(&mut event).await {
                Ok(()) => break,
                Err(jazz::db::BindingHydrationError::RetryableChunkUnavailable {
                    retry_after_ms,
                }) => {
                    // Keep this event ahead of the source stream. A fulfilled
                    // ReadableStream pull without enqueueing does not cause a
                    // new pull at HWM 0, so wait for a real delayed wake before
                    // retrying rather than returning an empty chunk or spinning.
                    let delay_ms = subscription_retry_delay_ms(retry_attempt, retry_after_ms);
                    if let Err(error) = wait_for_subscription_retry(delay_ms).await {
                        return Some((Err(error), (db, source)));
                    }
                    retry_attempt = retry_attempt.saturating_add(1);
                }
                Err(jazz::db::BindingHydrationError::Error(error)) => {
                    // Do not retain a fatal event: surfacing the stream error
                    // drops SubscriptionStream and runs its cleanup guard.
                    return Some((Err(to_js_error(error)), (db, source)));
                }
            }
        }
        Some((subscription_chunk_to_js(event), (db, source)))
    }))
}

const INITIAL_SUBSCRIPTION_RETRY_MS: u32 = 25;
const MAX_SUBSCRIPTION_RETRY_MS: u32 = 1_000;
// Browsers and Node clamp a `setTimeout` delay above signed i32::MAX down to
// a near-immediate timer. Keep each segment below that host ceiling so an
// untrusted peer cannot turn a long retry instruction into a hot loop.
const MAX_JS_TIMEOUT_MS: u32 = i32::MAX as u32;

fn local_retry_delay_ms(attempt: u8) -> u32 {
    INITIAL_SUBSCRIPTION_RETRY_MS
        .saturating_mul(1_u32 << attempt.min(6))
        .min(MAX_SUBSCRIPTION_RETRY_MS)
}

/// The peer's retry hint is a minimum, not a suggestion to cap. Local backoff
/// only protects against immediate repeated failures when the peer supplies a
/// shorter delay (or zero).
fn subscription_retry_delay_ms(attempt: u8, peer_retry_after_ms: u32) -> u32 {
    local_retry_delay_ms(attempt).max(peer_retry_after_ms)
}

async fn wait_for_subscription_retry(delay_ms: u32) -> Result<(), JsValue> {
    for segment_ms in subscription_retry_timer_segments(delay_ms) {
        SubscriptionRetryTimer::new(segment_ms)?.await;
    }
    Ok(())
}

fn subscription_retry_timer_segments(mut delay_ms: u32) -> Vec<u32> {
    let mut segments = Vec::new();
    while delay_ms > 0 {
        let segment_ms = delay_ms.min(MAX_JS_TIMEOUT_MS);
        segments.push(segment_ms);
        delay_ms -= segment_ms;
    }
    segments
}

struct SubscriptionRetryTimerState {
    fired: std::cell::Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

/// One host timer whose lifetime is owned by the Rust future. Dropping an
/// in-flight subscription pull clears the JavaScript timer immediately instead
/// of leaving a callback live until its (possibly multi-day) deadline.
struct SubscriptionRetryTimer {
    state: Rc<SubscriptionRetryTimerState>,
    timer_global: JsValue,
    clear_timeout: js_sys::Function,
    timeout_handle: JsValue,
    _callback: Closure<dyn FnMut()>,
}

impl SubscriptionRetryTimer {
    fn new(delay_ms: u32) -> Result<Self, JsValue> {
        let timer_global = js_sys::global();
        let set_timeout = js_sys::Reflect::get(&timer_global, &JsValue::from_str("setTimeout"))
            .and_then(|value| value.dyn_into::<js_sys::Function>())?;
        let clear_timeout = js_sys::Reflect::get(&timer_global, &JsValue::from_str("clearTimeout"))
            .and_then(|value| value.dyn_into::<js_sys::Function>())?;
        Self::with_functions(delay_ms, timer_global.into(), set_timeout, clear_timeout)
    }

    fn with_functions(
        delay_ms: u32,
        timer_global: JsValue,
        set_timeout: js_sys::Function,
        clear_timeout: js_sys::Function,
    ) -> Result<Self, JsValue> {
        let state = Rc::new(SubscriptionRetryTimerState {
            fired: std::cell::Cell::new(false),
            waker: RefCell::new(None),
        });
        let callback_state = Rc::clone(&state);
        let callback = Closure::<dyn FnMut()>::new(move || {
            callback_state.fired.set(true);
            if let Some(waker) = callback_state.waker.borrow_mut().take() {
                waker.wake();
            }
        });
        let timeout_handle = set_timeout.call2(
            &timer_global,
            callback.as_ref(),
            &JsValue::from_f64(f64::from(delay_ms)),
        )?;
        Ok(Self {
            state,
            timer_global,
            clear_timeout,
            timeout_handle,
            _callback: callback,
        })
    }
}

impl Future for SubscriptionRetryTimer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        if self.state.fired.get() {
            return Poll::Ready(());
        }
        *self.state.waker.borrow_mut() = Some(context.waker().clone());
        if self.state.fired.get() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for SubscriptionRetryTimer {
    fn drop(&mut self) {
        if !self.state.fired.get() {
            let _ = self
                .clear_timeout
                .call1(&self.timer_global, &self.timeout_handle);
        }
    }
}

fn subscription_chunk_to_js(event: SubscriptionEvent) -> Result<JsValue, JsValue> {
    let object = js_sys::Object::new();
    match event {
        SubscriptionEvent::Delta {
            reset,
            publishable,
            added,
            updated,
            removed,
            terminal_operations,
            settled,
            tier,
            ..
        } => {
            let delta =
                encode_subscription_delta(&added, &updated, &removed).map_err(to_js_error)?;
            if terminal_operations
                .iter()
                .any(|operation| operation.path.is_empty())
            {
                return Err(JsValue::from_str(
                    "native producer emitted a root terminal operation",
                ));
            }
            set_prop(&object, "type", JsValue::from_str("delta"))?;
            set_prop(
                &object,
                "delta",
                js_sys::Uint8Array::from(delta.as_slice()).into(),
            )?;
            set_prop(
                &object,
                "terminalOperations",
                jazz::binding_codec::terminal_operations_to_json(&terminal_operations)
                    .map_err(to_js_error)?
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
    let cancelled = std::rc::Rc::new(std::cell::Cell::new(false));
    let active_abort = std::rc::Rc::new(std::cell::RefCell::new(None::<AbortHandle>));
    let source = js_sys::Object::new();

    let pull_state = std::rc::Rc::clone(&state);
    let pull_cancelled = std::rc::Rc::clone(&cancelled);
    let pull_abort = std::rc::Rc::clone(&active_abort);
    let pull = Closure::<dyn FnMut(JsValue) -> js_sys::Promise>::new(move |controller| {
        let pull_state = std::rc::Rc::clone(&pull_state);
        let pull_cancelled = std::rc::Rc::clone(&pull_cancelled);
        let pull_abort = std::rc::Rc::clone(&pull_abort);
        future_to_promise(async move {
            if pull_cancelled.get() {
                return Ok(JsValue::undefined());
            }
            let Some(mut stream) = pull_state.borrow_mut().take() else {
                return Err(JsValue::from_str(
                    "subscription stream pull already in progress",
                ));
            };
            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            *pull_abort.borrow_mut() = Some(abort_handle);
            let next = Abortable::new(stream.next(), abort_registration).await;
            pull_abort.borrow_mut().take();
            if pull_cancelled.get() {
                // Do not restore the stream after cancellation: dropping it
                // runs the subscription cleanup guard even during a retry wait.
                return Ok(JsValue::undefined());
            }
            let next = next.map_err(|_| JsValue::from_str("subscription pull was aborted"))?;
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
    let cancel_flag = std::rc::Rc::clone(&cancelled);
    let cancel_abort = std::rc::Rc::clone(&active_abort);
    let cancel = Closure::<dyn FnMut()>::new(move || {
        cancel_flag.set(true);
        if let Some(handle) = cancel_abort.borrow_mut().take() {
            handle.abort();
        }
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

fn bytes_to_js(bytes: Vec<u8>) -> Result<JsValue, JsValue> {
    Ok(js_sys::Uint8Array::from(bytes.as_slice()).into())
}

fn unknown_transaction_kind_message(kind: &str) -> String {
    format!("unknown transaction kind {kind}")
}

#[cfg(test)]
mod dynamic_schema_view_tests {
    use super::*;
    use jazz::db::{DbConfig, DbIdentity, ExclusiveTxOps};
    use jazz::tools::public_schema::{
        ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchema,
    };

    /// Every ordinary write option shares `write_timestamp_option`, so this
    /// boundary test protects insert, update, upsert, delete, and restore from
    /// JavaScript's lossy number-to-u64 coercions.
    #[test]
    fn write_timestamp_requires_a_nonnegative_safe_integer_millisecond_value() {
        assert_eq!(
            checked_js_safe_u64(1_704_067_200_123.0),
            Some(1_704_067_200_123),
        );
        for invalid in [-1.0, 1.5, f64::NAN, f64::INFINITY, 9_007_199_254_740_992.0] {
            assert!(
                checked_js_safe_u64(invalid).is_none(),
                "invalid updatedAtMs {invalid:?} must fail before a write"
            );
        }
    }

    /// The JavaScript-facing parsers share the checked timestamp conversion
    /// for every ordinary write shape, including delete and restore delegates.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn ordinary_write_options_reject_lossy_updated_at_milliseconds_before_mutation() {
        fn options(updated_at_ms: f64) -> JsValue {
            let options = js_sys::Object::new();
            js_sys::Reflect::set(
                &options,
                &JsValue::from_str("updatedAtMs"),
                &JsValue::from_f64(updated_at_ms),
            )
            .expect("setting ordinary write options succeeds");
            options.into()
        }

        for invalid in [-1.0, 1.5, f64::NAN, f64::INFINITY, 9_007_199_254_740_992.0] {
            assert!(insert_options_from_js(options(invalid)).is_err());
            assert!(update_options_from_js(options(invalid)).is_err());
            assert!(upsert_options_from_js(options(invalid)).is_err());
            assert!(delete_options_from_js(options(invalid)).is_err());
            assert!(restore_options_from_js(options(invalid)).is_err());
        }
    }

    /// Binding read choices lower to the existing core tiers.
    #[test]
    fn read_tier_names_lower_to_existing_core_tiers() {
        assert_eq!(
            read_tier_from_str("local-first").expect("local-first read tier"),
            DurabilityTier::Local
        );
        assert_eq!(
            read_tier_from_str("remote-if-possible").expect("strict remote read tier"),
            DurabilityTier::Edge
        );
        assert_eq!(
            durability_tier_from_str("local").expect("legacy write tier"),
            DurabilityTier::Local,
            "the write parser remains the separate legacy durability boundary"
        );
    }

    #[test]
    fn subscription_chunk_retry_uses_a_bounded_nonzero_backoff() {
        assert_eq!(local_retry_delay_ms(0), 25);
        assert_eq!(local_retry_delay_ms(1), 50);
        assert_eq!(local_retry_delay_ms(5), 800);
        assert_eq!(local_retry_delay_ms(u8::MAX), 1_000);
        assert_eq!(
            subscription_retry_delay_ms(0, 10_000),
            10_000,
            "a peer retry minimum must never be capped downward"
        );
    }

    #[test]
    fn subscription_retry_timer_segments_cover_the_full_u32_delay() {
        assert_eq!(
            subscription_retry_timer_segments(u32::MAX),
            vec![MAX_JS_TIMEOUT_MS, MAX_JS_TIMEOUT_MS, 1],
            "every host timer segment stays safe while the total delay is exact"
        );
    }

    #[test]
    fn canceling_a_pending_subscription_retry_drops_it_without_waiting() {
        let (abort, registration) = AbortHandle::new_pair();
        let wait = Abortable::new(futures_util::future::pending::<()>(), registration);
        abort.abort();
        assert!(matches!(block_on(wait), Err(_)));
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn canceling_an_actual_maximum_subscription_timer_drops_it_without_waiting() {
        let (abort, registration) = AbortHandle::new_pair();
        let mut wait = Box::pin(Abortable::new(
            wait_for_subscription_retry(u32::MAX),
            registration,
        ));
        let mut context = Context::from_waker(std::task::Waker::noop());
        assert!(matches!(wait.as_mut().poll(&mut context), Poll::Pending));
        abort.abort();
        assert!(matches!(
            wait.as_mut().poll(&mut context),
            Poll::Ready(Err(_))
        ));
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn subscription_retry_timer_clears_live_handles_and_skips_fired_ones() {
        fn timer_functions(
            callback: Rc<RefCell<Option<js_sys::Function>>>,
            clears: Rc<std::cell::Cell<u32>>,
        ) -> (
            Closure<dyn FnMut(JsValue, JsValue) -> JsValue>,
            Closure<dyn FnMut(JsValue)>,
        ) {
            let set_timeout = Closure::<dyn FnMut(JsValue, JsValue) -> JsValue>::new(
                move |callback_value: JsValue, _delay: JsValue| {
                    *callback.borrow_mut() = Some(callback_value.unchecked_into());
                    JsValue::from_f64(91.0)
                },
            );
            let clear_timeout = Closure::<dyn FnMut(JsValue)>::new(move |handle: JsValue| {
                assert_eq!(handle.as_f64(), Some(91.0));
                clears.set(clears.get() + 1);
            });
            (set_timeout, clear_timeout)
        }

        let callback = Rc::new(RefCell::new(None));
        let clears = Rc::new(std::cell::Cell::new(0));
        let (set_timeout, clear_timeout) =
            timer_functions(Rc::clone(&callback), Rc::clone(&clears));
        let timer = SubscriptionRetryTimer::with_functions(
            MAX_JS_TIMEOUT_MS,
            JsValue::UNDEFINED,
            set_timeout
                .as_ref()
                .unchecked_ref::<js_sys::Function>()
                .clone(),
            clear_timeout
                .as_ref()
                .unchecked_ref::<js_sys::Function>()
                .clone(),
        )
        .unwrap();
        drop(timer);
        assert_eq!(
            clears.get(),
            1,
            "cancelling the retry clears its host timer"
        );

        let callback = Rc::new(RefCell::new(None));
        let clears = Rc::new(std::cell::Cell::new(0));
        let (set_timeout, clear_timeout) =
            timer_functions(Rc::clone(&callback), Rc::clone(&clears));
        let mut timer = Box::pin(
            SubscriptionRetryTimer::with_functions(
                1,
                JsValue::UNDEFINED,
                set_timeout
                    .as_ref()
                    .unchecked_ref::<js_sys::Function>()
                    .clone(),
                clear_timeout
                    .as_ref()
                    .unchecked_ref::<js_sys::Function>()
                    .clone(),
            )
            .unwrap(),
        );
        let mut context = Context::from_waker(std::task::Waker::noop());
        assert!(matches!(timer.as_mut().poll(&mut context), Poll::Pending));
        callback
            .borrow()
            .as_ref()
            .expect("setTimeout captured its callback")
            .call0(&JsValue::UNDEFINED)
            .unwrap();
        assert!(matches!(timer.as_mut().poll(&mut context), Poll::Ready(())));
        drop(timer);
        assert_eq!(clears.get(), 0, "a fired timer has no live handle to clear");
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn removed_propagate_option_does_not_select_local_only() {
        let value = js_sys::Object::new();
        js_sys::Reflect::set(&value, &JsValue::from_str("propagate"), &JsValue::FALSE)
            .expect("set legacy option");

        let opts = read_opts_from_js(value.into()).expect("parse read options");

        assert_eq!(opts.propagation, Propagation::Full);
    }

    /// The host-visible transport boundary must honor both parts of its
    /// auxiliary drain budget. This lives here rather than in the core pump
    /// receipts because wasm-bindgen's optional-number ABI is part of the
    /// browser worker contract: a stale generated package can otherwise hide
    /// the count/byte arguments behind TypeScript mocks.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    async fn wasm_auxiliary_drain_bounds_count_and_bytes_with_fifo_remainder() {
        let schema = JazzSchema::new(&SchemaBuilder::new().build())
            .expect("empty public schema compiles for transport receipt");
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let db = Rc::new(
            Db::open(DbConfig::new(
                schema,
                MemoryStorage::new(&refs).expect("valid memory storage families"),
                DbIdentity {
                    node: jazz::ids::NodeUuid::from_bytes([0x63; 16]),
                    author: AuthorSubject::for_test_bytes([0xc3; 16]),
                },
            ))
            .await
            .expect("open memory-backed wasm transport"),
        );
        let binding = WasmDb {
            inner: WasmDbInner::Memory(db),
            owns_runtime: false,
            trusted_backend: false,
        };
        let subscriber = AuthorSubject::from_canonical(
            &serde_json::to_string(&("https://wasm.test", "subscriber")).unwrap(),
        )
        .unwrap();
        let mut transport = binding
            .accept_subscriber(subscriber.canonical().as_bytes().to_vec(), JsValue::NULL)
            .expect("accept a real wasm subscriber transport");

        // Encode against the exact binding-local negotiation surface. The
        // subscriber transport intentionally omits authorization-scope
        // extensions, whose feature-gated enum layout must not leak into this
        // auxiliary frame.
        transport.features &= !(jazz::wire::FEATURE_PAYLOAD_LZ4 | jazz::wire::FEATURE_PAYLOAD_ZSTD);
        let features = transport.features;
        let request = |request_id| jazz::protocol::ChunkRequestEntry {
            request_id,
            locator: jazz::groove::large_values::Locator::random(),
            expected_hash: [request_id as u8; 32],
            // A hop-exhausted request has a deterministic immediate result,
            // so the receipt exercises the binding drain without another
            // transport or a semantic tick.
            remaining_hops: 0,
        };
        let incoming =
            jazz::protocol::SyncMessage::ChunkRequestBatch(jazz::protocol::ChunkRequestBatch {
                requests: (1..=5).map(request).collect(),
            });
        transport
            .auxiliary_pump
            .route_incoming(incoming)
            .await
            .expect("route actual auxiliary request through the binding pump");

        let one_response =
            jazz::protocol::SyncMessage::ChunkResponseBatch(jazz::protocol::ChunkResponseBatch {
                responses: vec![jazz::protocol::ChunkResponseEntry {
                    request_id: 1,
                    result: jazz::protocol::ChunkResponse::Unavailable,
                }],
            });
        let one_response_bytes =
            jazz::wire::encode_sync_message_for_features(&one_response, features)
                .expect("encode expected auxiliary response");
        let one_response_frame = jazz::wire::encode_frame(&jazz::wire::WireFrame::Message(
            jazz::wire::WireEnvelope::new(
                jazz::wire::WIRE_PROTOCOL_VERSION,
                features,
                one_response_bytes,
            ),
        ))
        .expect("frame expected auxiliary response");
        let byte_budget = one_response_frame.len() as u32;

        let decode_ids = |frames: js_sys::Array| {
            frames
                .iter()
                .map(|frame| {
                    let bytes = js_sys::Uint8Array::new(&frame).to_vec();
                    assert!(
                        bytes.len() <= byte_budget as usize,
                        "each actual WASM result stays within the requested byte budget"
                    );
                    let jazz::wire::WireFrame::Message(envelope) = jazz::wire::decode_frame(&bytes)
                        .expect("decode actual wasm response frame")
                    else {
                        panic!("auxiliary output remains a complete message frame");
                    };
                    let response =
                        jazz::wire::decode_sync_message_for_features(&envelope.payload, features)
                            .expect("decode actual wasm auxiliary response");
                    let jazz::protocol::SyncMessage::ChunkResponseBatch(batch) = response else {
                        panic!("subscriber sends a chunk response on the auxiliary lane");
                    };
                    assert_eq!(batch.responses.len(), 1);
                    batch.responses[0].request_id
                })
                .collect::<Vec<_>>()
        };

        let first = transport
            .recv_auxiliary_wire_frames(Some(8), Some(byte_budget))
            .expect("drain byte-bounded real wasm response batch");
        assert_eq!(first.length(), 1, "byte budget admits only one frame");
        assert_eq!(decode_ids(first), vec![1]);

        let second = transport
            .recv_auxiliary_wire_frames(Some(2), Some(byte_budget * 2))
            .expect("drain count-bounded real wasm response batch");
        assert_eq!(second.length(), 2, "count budget admits exactly two frames");
        assert_eq!(decode_ids(second), vec![2, 3]);

        let tail = transport
            .recv_auxiliary_wire_frames(Some(8), Some(byte_budget * 8))
            .expect("drain retained real wasm response remainder");
        assert_eq!(decode_ids(tail), vec![4, 5], "remainder stays FIFO");
        assert_eq!(
            transport
                .recv_auxiliary_wire_frames(Some(1), Some(byte_budget))
                .expect("empty auxiliary drain")
                .length(),
            0,
            "all queued frames are eventually drained"
        );
    }

    #[test]
    fn transaction_binding_diagnostics_use_transaction_vocabulary() {
        assert_eq!(
            unknown_transaction_kind_message("invalid"),
            "unknown transaction kind invalid"
        );
    }

    #[test]
    fn public_wasm_open_config_rejects_every_reserved_author() {
        for issuer in [
            AuthorSubject::SYSTEM_ISSUER,
            AuthorSubject::LOCAL_FIRST_ISSUER,
            AuthorSubject::ANONYMOUS_ISSUER,
            AuthorSubject::STATIC_BEARER_ISSUER,
        ] {
            let author = if issuer == AuthorSubject::SYSTEM_ISSUER {
                AuthorSubject::SYSTEM
            } else {
                AuthorSubject::from_canonical(&serde_json::to_string(&(issuer, "caller")).unwrap())
                    .unwrap()
            };
            let config = WasmOpenDbConfig {
                identity: WasmDbIdentity {
                    node: NodeUuid::from_bytes([0x7a; 16]),
                    author,
                },
                row_id_seed: None,
                history_complete: false,
                initial_sync_flush_every: None,
            };
            assert!(
                validate_untrusted_open_author_core(&config).is_err(),
                "ordinary WasmDb.openMemory must reject reserved issuer {issuer}"
            );
        }
    }

    #[test]
    fn explicit_wasm_backend_open_derives_system_after_raw_validation() {
        let config = WasmOpenDbConfig {
            identity: WasmDbIdentity {
                node: NodeUuid::from_bytes([0x6b; 16]),
                author: AuthorSubject::from_canonical(
                    &serde_json::to_string(&("https://issuer.test", "backend")).unwrap(),
                )
                .unwrap(),
            },
            row_id_seed: None,
            history_complete: false,
            initial_sync_flush_every: None,
        };
        assert_eq!(
            backend_open_identity_core(&config).unwrap().author,
            AuthorSubject::SYSTEM
        );

        let reserved = WasmOpenDbConfig {
            identity: WasmDbIdentity {
                node: config.identity.node,
                author: AuthorSubject::SYSTEM,
            },
            ..config
        };
        assert!(backend_open_identity_core(&reserved).is_err());
    }

    #[test]
    fn wasm_self_signed_open_verifier_binds_exact_proof_author() {
        let seed = [0x51; 32];
        let app_id = "wasm-proof-test";
        let token = jazz::tools::identity::mint_jazz_self_signed_token(
            &seed,
            AuthorSubject::LOCAL_FIRST_ISSUER,
            app_id,
            60,
        )
        .unwrap();
        let verified =
            jazz::tools::identity::verify_jazz_self_signed_proof(&token, app_id).unwrap();
        let claimed = AuthorSubject::from_canonical(
            &serde_json::to_string(&(verified.issuer, verified.user_id)).unwrap(),
        )
        .unwrap();
        // The ordinary browser-worker subscriber ABI receives a serialized
        // identity from an untrusted port. A reserved author must therefore
        // remain impossible there even when a genuine proof for that author
        // exists elsewhere in the worker.
        assert!(AuthorSubject::from_untrusted_canonical(claimed.canonical()).is_err());
        assert_eq!(
            verify_self_signed_runtime_author_core(&token, app_id, claimed.canonical()).unwrap(),
            claimed
        );
        assert!(
            verify_self_signed_runtime_author_core(&token, "wrong-app", claimed.canonical())
                .is_err()
        );
        assert!(verify_self_signed_runtime_author_core(
            &token,
            app_id,
            AuthorSubject::SYSTEM_CANONICAL
        )
        .is_err());
        let mut bad_signature = token.into_bytes();
        let last = bad_signature.len() - 1;
        bad_signature[last] ^= 1;
        assert!(verify_self_signed_runtime_author_core(
            std::str::from_utf8(&bad_signature).unwrap(),
            app_id,
            claimed.canonical(),
        )
        .is_err());

        // This binding-level receipt is intentionally not a public database
        // test: it proves that the WASM boundary supplies its browser clock
        // to the same expiry validation as native bindings.
        let expired = jazz::tools::identity::mint_jazz_self_signed_token_at(
            &seed,
            AuthorSubject::LOCAL_FIRST_ISSUER,
            app_id,
            1,
            1_000_000,
        )
        .unwrap();
        assert!(verify_self_signed_runtime_author_at(
            &expired,
            app_id,
            claimed.canonical(),
            1_000_100,
        )
        .is_err());
    }

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
    fn binding_claim_admission_derives_identity_and_shadows_identity_named_provider_claims() {
        let author = AuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
        let claims = admit_binding_claims(
            author,
            BTreeMap::from([
                ("user".to_owned(), Value::String("forged-user".to_owned())),
                (
                    "authMode".to_owned(),
                    Value::String("local-first".to_owned()),
                ),
                ("iss".to_owned(), Value::String("forged-issuer".to_owned())),
                ("sub".to_owned(), Value::String("forged-subject".to_owned())),
                ("role".to_owned(), Value::String("editor".to_owned())),
            ]),
        );

        assert_eq!(
            claims.get("user"),
            Some(&Value::String(author.canonical().to_owned())),
            "session.user must come from the admitted transport identity"
        );
        assert_eq!(
            claims.get("authMode"),
            Some(&Value::String("external".to_owned())),
            "session.authMode must be derived rather than provider controlled"
        );
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("iss")),
            Some(&Value::String("https://issuer.example".to_owned())),
            "session.claims.iss must agree with session.user"
        );
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("sub")),
            Some(&Value::String("alice".to_owned())),
            "session.claims.sub must agree with session.user"
        );
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("user")),
            Some(&Value::String("forged-user".to_owned())),
            "provider user remains available only below session.claims"
        );
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("authMode")),
            Some(&Value::String("local-first".to_owned())),
            "provider authMode remains available only below session.claims"
        );
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("role")),
            Some(&Value::String("editor".to_owned()))
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
    /// transaction when the JavaScript wrapper is collected.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn attached_tx_drop_preserves_owner_batch() {
        let source = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("label", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build();
        let schema = jazz::schema::JazzSchema::new(&source)
            .expect("WASM transaction fixture public schema compiles");
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let owner = Rc::new(
            block_on(Db::open(DbConfig::new(
                schema.clone(),
                MemoryStorage::new(&refs).expect("valid memory storage families"),
                DbIdentity {
                    node: jazz::ids::NodeUuid::from_bytes([0x45; 16]),
                    author: AuthorSubject::for_test_bytes([0xa5; 16]),
                },
            )))
            .unwrap(),
        );
        let view = Rc::new(block_on(owner.register_schema_view(schema.clone())).unwrap());
        let batch = OpenTransactionId::new();
        block_on(owner.begin_mergeable(batch)).unwrap();
        drop(WasmTx {
            db: WasmDbInner::Memory(Rc::clone(&view)),
            kind: WasmTxKind::Mergeable,
            open_tx: Some(batch),
            owns_lifetime: false,
        });
        block_on(view.mergeable_tx_ref(batch).insert(
            "items",
            BTreeMap::from([("label".to_owned(), Value::String("kept".to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(RowUuid::from_bytes([1; 16])),
                ..Default::default()
            },
        ))
        .unwrap();
        let prepared = view.prepare_query(&view.table("items")).unwrap();
        let rows = WasmDbInner::Memory(Rc::clone(&view))
            .mergeable_all(batch, &prepared, ReadOpts::default())
            .unwrap();
        assert_eq!(rows.len(), 1, "the attached view reads staged rows");
        block_on(owner.commit_mergeable_handle(batch)).unwrap();

        let exclusive = OpenTransactionId::new();
        block_on(owner.begin_exclusive(exclusive)).unwrap();
        drop(WasmTx {
            db: WasmDbInner::Memory(Rc::clone(&view)),
            kind: WasmTxKind::Exclusive,
            open_tx: Some(exclusive),
            owns_lifetime: false,
        });
        block_on(view.exclusive_tx_ref(exclusive).insert(
            "items",
            BTreeMap::from([(
                "label".to_owned(),
                Value::String("exclusive-kept".to_owned()),
            )]),
            jazz::db::InsertOptions {
                row_id: Some(RowUuid::from_bytes([2; 16])),
                ..Default::default()
            },
        ))
        .unwrap();
        block_on(owner.commit_exclusive_handle(exclusive)).unwrap();

        // The public WASM transaction surface binds Alice at begin. A later request
        // cannot switch the transaction-local authorization subject to Bob.
        // JsValue construction requires an actual wasm runtime.
        #[cfg(target_arch = "wasm32")]
        {
            let binding = WasmDb {
                inner: WasmDbInner::Memory(Rc::clone(&owner)),
                owns_runtime: false,
                trusted_backend: false,
            };
            let alice = AuthorSubject::for_test_bytes([0xa7; 16]);
            let bob = AuthorSubject::for_test_bytes([0xb7; 16]);
            let bound = OpenTransactionId::new();
            binding
                .begin_transaction(
                    bound.to_string(),
                    "exclusive".to_owned(),
                    Some(alice.canonical().as_bytes().to_vec()),
                )
                .unwrap();
            let tx = binding.attach_exclusive_tx(bound.to_string()).unwrap();
            let query = WasmPreparedQuery {
                inner: owner.prepare_query(&owner.table("items")).unwrap(),
            };
            assert!(
                binding
                    .all_in_transaction_for_identity(
                        &query,
                        &tx,
                        alice.canonical().as_bytes().to_vec(),
                        JsValue::NULL
                    )
                    .is_ok(),
                "planted positive: Alice retains the bound capability"
            );
            let view_binding = WasmDb {
                inner: WasmDbInner::Memory(Rc::clone(&view)),
                owns_runtime: false,
                trusted_backend: false,
            };
            let view_query = WasmPreparedQuery {
                inner: view.prepare_query(&view.table("items")).unwrap(),
            };
            assert!(view_binding
                .all_in_transaction(&view_query, &tx, JsValue::NULL)
                .is_ok());
            assert!(view_binding
                .all_in_transaction_for_identity(
                    &view_query,
                    &tx,
                    alice.canonical().as_bytes().to_vec(),
                    JsValue::NULL,
                )
                .is_ok());
            assert!(view_binding
                .one_in_transaction(&view_query, &tx, JsValue::NULL)
                .is_ok());
            assert!(
                view_binding
                    .one_in_transaction_for_identity(
                        &view_query,
                        &tx,
                        alice.canonical().as_bytes().to_vec(),
                        JsValue::NULL,
                    )
                    .is_ok(),
                "registered schema facades share all owner transaction read overloads"
            );

            let other_owner = Rc::new(
                block_on(Db::open(DbConfig::new(
                    schema.clone(),
                    MemoryStorage::new(&refs).expect("valid memory storage families"),
                    DbIdentity {
                        node: jazz::ids::NodeUuid::from_bytes([0x47; 16]),
                        author: alice,
                    },
                )))
                .unwrap(),
            );
            let other_binding = WasmDb {
                inner: WasmDbInner::Memory(Rc::clone(&other_owner)),
                owns_runtime: false,
                trusted_backend: false,
            };
            other_binding
                .begin_transaction(
                    bound.to_string(),
                    "exclusive".to_owned(),
                    Some(alice.canonical().as_bytes().to_vec()),
                )
                .unwrap();
            block_on(other_owner.exclusive_tx_ref(bound).insert(
                "items",
                BTreeMap::from([(
                    "label".to_owned(),
                    Value::String("receiver-secret".to_owned()),
                )]),
                jazz::db::InsertOptions {
                    row_id: Some(RowUuid::from_bytes([3; 16])),
                    ..Default::default()
                },
            ))
            .unwrap();
            let other_query = WasmPreparedQuery {
                inner: other_owner
                    .prepare_query(&other_owner.table("items"))
                    .unwrap(),
            };
            let assert_foreign = |result: Result<Vec<u8>, JsValue>| {
                assert!(result
                    .unwrap_err()
                    .as_string()
                    .is_some_and(|message| { message.contains("different database runtime") }));
            };
            assert_foreign(other_binding.all_in_transaction(&other_query, &tx, JsValue::NULL));
            assert_foreign(other_binding.all_in_transaction_for_identity(
                &other_query,
                &tx,
                alice.canonical().as_bytes().to_vec(),
                JsValue::NULL,
            ));
            assert_foreign(other_binding.one_in_transaction(&other_query, &tx, JsValue::NULL));
            assert_foreign(other_binding.one_in_transaction_for_identity(
                &other_query,
                &tx,
                alice.canonical().as_bytes().to_vec(),
                JsValue::NULL,
            ));
            let error = binding
                .all_in_transaction_for_identity(
                    &query,
                    &tx,
                    bob.canonical().as_bytes().to_vec(),
                    JsValue::NULL,
                )
                .unwrap_err();
            assert!(error
                .as_string()
                .is_some_and(|message| message.contains("bound identity")));
            binding
                .commit_transaction(bound.to_string(), Some("exclusive".to_owned()))
                .unwrap();
            other_binding
                .rollback_transaction(bound.to_string())
                .unwrap();
        }
    }
}
