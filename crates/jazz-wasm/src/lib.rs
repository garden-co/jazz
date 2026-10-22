//! WebAssembly bindings for Jazz on SQLite (`jazz-db`).
//!
//! One bundle serves both browser roles: a worker that owns a durable SQLite
//! file (on IndexedDB or OPFS, see [`browser`]) and main-thread in-memory
//! databases that sync through it. The exported names and byte codecs are the
//! ones the Groove engine's binding had, so the TypeScript runtime keeps its
//! shape. Engine features `jazz-db` does not have yet fail with an explicit
//! "not supported on SQLite yet" error instead of a stand-in.
//!
//! `jazz-db` runs every operation to completion synchronously. The only
//! things that wait are reads and write fates that need the server; they
//! complete on a later tick, so every tick wakes them (see [`Progress`]).

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use futures_util::future::{AbortHandle, Abortable};
use jazz::db::{
    block_on, Db, DbConfig, DbIdentity, Error, ErrorCode, FirstLoad, LargeValueStagingPolicy,
    LargeValueUpdate, ReadOpts, RemoteLinkHint, SeededRowIdSource, SerializedReadResult,
    StreamingMutationKind, StreamingValueUpload, SubscriptionEvent, SubscriptionStream,
    TransportError, WireTransport, WireTransportAdapter, WriteHandle,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{
    BranchSelector, BranchViewBase, PermissionAdvice, PermissionAdviceAction, ReadViewSpec,
};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::time::TxTime;
use jazz::tools::{OpenTransactionId, TransactionId};
use jazz::tx::{DurabilityTier, TxId};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::future_to_promise;

#[cfg(target_arch = "wasm32")]
mod browser;
#[cfg(all(target_arch = "wasm32", feature = "browser-bench"))]
pub mod browser_bench;
mod identity;

#[cfg(feature = "bench-probes")]
pub mod bench_probes;

#[cfg(all(target_arch = "wasm32", not(target_feature = "atomics")))]
#[global_allocator]
static TALC: talc::wasm::WasmDynamicTalc = talc::wasm::new_wasm_dynamic_allocator();

type Cells = BTreeMap<String, Value>;

/// Validate a schema and its embedded permissions without opening a database.
#[wasm_bindgen(js_name = validateSchema)]
pub fn validate_schema(schema: Vec<u8>) -> Result<(), JsValue> {
    decode_public_schema(&schema).map(|_| ())
}

/// Initialize the WASM module.
///
/// Sets up the panic hook for better error messages in the browser console.
/// Tracing is initialized by the first owning database open, after callers
/// have selected `globalThis.__JAZZ_WASM_LOG_LEVEL`.
#[wasm_bindgen(start)]
pub fn init() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

static WASM_TRACING_INITIALIZED: AtomicBool = AtomicBool::new(false);

fn initialize_wasm_tracing() {
    if WASM_TRACING_INITIALIZED.swap(true, Ordering::AcqRel) {
        return;
    }
    let max_level = configured_wasm_log_level();
    let config = wasm_tracing::WasmLayerConfig::new()
        .with_max_level(max_level)
        .with_console_group_spans();
    let _ = wasm_tracing::set_as_global_default_with_config(config);
}

fn configured_wasm_log_level() -> tracing::Level {
    #[cfg(target_arch = "wasm32")]
    {
        let value = js_sys::Reflect::get(
            &js_sys::global(),
            &JsValue::from_str("__JAZZ_WASM_LOG_LEVEL"),
        )
        .ok()
        .and_then(|value| value.as_string());
        return match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
            Some("error") => tracing::Level::ERROR,
            Some("info") => tracing::Level::INFO,
            Some("debug") => tracing::Level::DEBUG,
            Some("trace") => tracing::Level::TRACE,
            _ => tracing::Level::WARN,
        };
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        tracing::Level::WARN
    }
}

/// Enable or disable buffering of tracing entries for JavaScript drains.
#[wasm_bindgen(js_name = setTraceEntryCollectionEnabled)]
pub fn set_trace_entry_collection_enabled(enabled: bool) {
    wasm_tracing::set_trace_entry_collection_enabled(enabled);
}

/// Drain buffered tracing entries as a JavaScript array.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = drainTraceEntries)]
pub fn drain_trace_entries() -> JsValue {
    wasm_tracing::drain_trace_entries()
}

/// Register a callback invoked when buffered tracing entries are ready.
/// Returns an unsubscribe function.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = subscribeTraceEntries)]
pub fn subscribe_trace_entries(callback: js_sys::Function) -> js_sys::Function {
    wasm_tracing::subscribe_trace_entries(callback)
}

/// Install the JSON Schema validator for JSON columns.
///
/// The browser build leaves the Rust validator out to keep the binary small.
/// `compile` takes a declared schema as JSON text and returns a check that
/// takes a value as JSON text and returns why it does not match, or
/// `undefined`. `compile` throws when the schema itself is invalid; a check
/// that throws is reported as the validator failing, not as a mismatch.
/// Until this is called, JSON column schemas are reported as unavailable.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[wasm_bindgen(js_name = setJsonSchemaValidator)]
pub fn set_json_schema_validator(compile: js_sys::Function) {
    jazz::model::json_schema::install_host_validator(Box::new(JsJsonSchemaValidator { compile }));
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
struct JsJsonSchemaValidator {
    compile: js_sys::Function,
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
struct JsCompiledJsonSchema {
    check: js_sys::Function,
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl jazz::model::json_schema::HostJsonSchemaValidator for JsJsonSchemaValidator {
    fn compile(
        &self,
        schema_json: &str,
    ) -> Result<Box<dyn jazz::model::json_schema::HostCompiledJsonSchema>, String> {
        let check = self
            .compile
            .call1(&JsValue::NULL, &JsValue::from_str(schema_json))
            .map_err(|error| js_error_message(&error))?;
        let check = check
            .dyn_into::<js_sys::Function>()
            .map_err(|_| "the JSON Schema validator did not return a function".to_owned())?;
        Ok(Box::new(JsCompiledJsonSchema { check }))
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl jazz::model::json_schema::HostCompiledJsonSchema for JsCompiledJsonSchema {
    fn validate(&self, instance_json: &str) -> Result<Option<String>, String> {
        let outcome = self
            .check
            .call1(&JsValue::NULL, &JsValue::from_str(instance_json))
            .map_err(|error| js_error_message(&error))?;
        Ok(outcome.as_string())
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn js_error_message(error: &JsValue) -> String {
    error
        .dyn_ref::<js_sys::Error>()
        .map(|error| String::from(error.message()))
        .or_else(|| error.as_string())
        .unwrap_or_else(|| format!("{error:?}"))
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

/// Test-only execution of a frozen v5 frame batch through the
/// generated WASM artifact. This reaches the production Rust frame, feature,
/// compression, and semantic-payload decoders; it is not a browser transport
/// API.
#[wasm_bindgen(js_name = __testValidateWireFrameBatchCorpus)]
pub fn test_validate_wire_frame_batch_corpus(
    frame: Vec<u8>,
    negotiated_features: String,
) -> Result<(), JsValue> {
    let negotiated_features = negotiated_features
        .parse()
        .map_err(|_| JsValue::from_str("test wire corpus features must be a u64 decimal"))?;
    jazz::wire::decode_websocket_frame_batch(&frame)
        .map_err(|error| error.to_string())
        .and_then(|frames| {
            jazz::wire::validate_frames_for_artifact_corpus(&frames, negotiated_features)
        })
        .map_err(|error| error.into())
}

/// Test-only feature inventory paired with [`test_validate_wire_frame_batch_corpus`].
#[wasm_bindgen(js_name = __testWireFrameCorpusFeatures)]
pub fn test_wire_frame_corpus_features() -> String {
    jazz::wire::current_wire_features().to_string()
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
    /// A Groove persistence knob: how many initial-sync batches to buffer
    /// before a flush. SQLite commits each applied batch, so it is accepted
    /// and has nothing to configure.
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
    tx_id: TxId,
}

/// The error a binding call fails with for an engine feature `jazz-db` does
/// not have yet.
fn not_supported(what: &str) -> JsValue {
    core_error_to_js(&Error::new(
        ErrorCode::Query,
        format!("{what} is not supported on SQLite yet"),
    ))
}

// ---- progress --------------------------------------------------------------

/// Wakers of futures waiting on sync progress: a server answer, a write's
/// fate, a subscription's next event. `jazz-db` does not register wakers; its
/// waits complete when a later tick or write changes their state. So every
/// tick and every local write wakes all of them to poll again.
#[derive(Clone, Default)]
struct Progress(Rc<RefCell<Vec<Waker>>>);

impl Progress {
    fn park(&self, context: &Context<'_>) {
        let mut wakers = self.0.borrow_mut();
        if !wakers.iter().any(|waker| waker.will_wake(context.waker())) {
            wakers.push(context.waker().clone());
        }
    }

    fn wake_all(&self) {
        let wakers = std::mem::take(&mut *self.0.borrow_mut());
        for waker in wakers {
            waker.wake();
        }
    }

    /// `future`, parking its waker here whenever it is pending.
    fn parked<F: Future>(&self, future: F) -> Parked<F> {
        Parked {
            future: Box::pin(future),
            progress: self.clone(),
        }
    }
}

struct Parked<F: Future> {
    future: Pin<Box<F>>,
    progress: Progress,
}

impl<F: Future> Future for Parked<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        match self.future.as_mut().poll(context) {
            Poll::Ready(output) => Poll::Ready(output),
            Poll::Pending => {
                self.progress.park(context);
                Poll::Pending
            }
        }
    }
}

/// Host hooks shared by a runtime and its schema views.
#[derive(Default)]
struct Host {
    progress: Progress,
    tick_scheduler: RefCell<Option<js_sys::Function>>,
    /// Kept for the host's lifecycle; `jazz-db` has no rejected-write
    /// notification to forward yet (rejections reach `WasmWrite.wait`).
    mutation_errors: RefCell<Option<js_sys::Function>>,
    #[cfg(target_arch = "wasm32")]
    store: RefCell<Option<browser::Store>>,
}

impl Host {
    /// Ask the host for a tick: there is sync work to send or apply.
    fn schedule_tick(&self) {
        let scheduler = self.tick_scheduler.borrow().clone();
        if let Some(scheduler) = scheduler {
            let _ = scheduler.call1(&JsValue::NULL, &JsValue::from_str("immediate"));
        }
    }

    /// After a local write: subscriptions have new events, and the write
    /// has to go upstream.
    fn after_write(&self) {
        self.progress.wake_all();
        self.schedule_tick();
    }
}

// ---- pending operations ------------------------------------------------------

type WasmReadFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>, JsValue>> + 'static>>;

#[wasm_bindgen(js_name = PendingNativeRead)]
pub struct WasmPendingNativeRead {
    future: Rc<RefCell<Option<WasmReadFuture>>>,
}

impl WasmPendingNativeRead {
    fn new(future: WasmReadFuture) -> Self {
        Self {
            future: Rc::new(RefCell::new(Some(future))),
        }
    }

    fn poll_once(&self) -> Result<Option<Vec<u8>>, JsValue> {
        let Some(mut future) = self.future.borrow_mut().take() else {
            return Err(JsValue::from_str("native pending read is already complete"));
        };
        let mut context = Context::from_waker(Waker::noop());
        match Pin::new(&mut future).poll(&mut context) {
            Poll::Ready(result) => result.map(Some),
            Poll::Pending => {
                *self.future.borrow_mut() = Some(future);
                Ok(None)
            }
        }
    }
}

#[wasm_bindgen(js_class = PendingNativeRead)]
impl WasmPendingNativeRead {
    pub fn poll(&self) -> Result<JsValue, JsValue> {
        match self.poll_once()? {
            Some(bytes) => bytes_to_js(bytes),
            None => Ok(JsValue::NULL),
        }
    }

    pub fn cancel(&self) {
        self.future.borrow_mut().take();
    }
}

/// A pending JavaScript callback wake: it calls `callback` from a fresh task
/// instead of inside the waking call.
fn callback_waker(callback: js_sys::Function) -> Waker {
    struct CallbackWake(js_sys::Function);
    // Wasm is single-threaded; the waker never leaves this thread.
    unsafe impl Send for CallbackWake {}
    unsafe impl Sync for CallbackWake {}
    impl std::task::Wake for CallbackWake {
        fn wake(self: std::sync::Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            let callback = self.0.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let _ = callback.call0(&JsValue::NULL);
            });
        }
    }
    Waker::from(std::sync::Arc::new(CallbackWake(callback)))
}

type PendingWasmOperation<T> = RefCell<Option<Pin<Box<dyn Future<Output = Result<T, JsValue>>>>>>;

#[wasm_bindgen]
pub struct WasmPendingSubscription {
    future: PendingWasmOperation<JsValue>,
    wake: RefCell<Option<Waker>>,
}

impl WasmPendingSubscription {
    fn poll_once(&self) -> Result<Option<JsValue>, JsValue> {
        let Some(mut future) = self.future.borrow_mut().take() else {
            return Err(to_js_error("native operation is complete or cancelled"));
        };
        let wake = self
            .wake
            .borrow()
            .clone()
            .unwrap_or_else(|| Waker::noop().clone());
        let mut context = Context::from_waker(&wake);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result.map(Some),
            Poll::Pending => {
                *self.future.borrow_mut() = Some(future);
                Ok(None)
            }
        }
    }
}

#[wasm_bindgen]
impl WasmPendingSubscription {
    #[wasm_bindgen(js_name = setWake)]
    pub fn set_wake(&self, callback: js_sys::Function) {
        *self.wake.borrow_mut() = Some(callback_waker(callback));
    }

    pub fn poll(&self) -> Result<Option<JsValue>, JsValue> {
        self.poll_once()
    }

    pub fn cancel(&self) {
        self.future.borrow_mut().take();
        self.wake.borrow_mut().take();
    }
}

/// One permission preflight sent to the upstream authority.
#[wasm_bindgen]
pub struct WasmPermissionAdviceRequest {
    promise: js_sys::Promise,
    db: WasmDbInner,
    id: u64,
}

#[wasm_bindgen]
impl WasmPermissionAdviceRequest {
    #[wasm_bindgen(getter)]
    pub fn promise(&self) -> js_sys::Promise {
        self.promise.clone()
    }

    /// Resolve the promise as `"unknown"`; a late answer is ignored.
    pub fn cancel(&self) {
        self.db.db.cancel_permission_advice_request(self.id);
        self.db.host.progress.wake_all();
    }
}

fn advice_string(advice: PermissionAdvice) -> &'static str {
    match advice {
        PermissionAdvice::Allowed => "allowed",
        PermissionAdvice::Denied => "denied",
        PermissionAdvice::Unknown => "unknown",
    }
}

impl WasmDbInner {
    fn request_permission_advice(&self, action: PermissionAdviceAction) -> WasmPermissionAdviceRequest {
        let id = self.db.request_permission_advice(action);
        // The request goes out on the next tick.
        self.host.after_write();
        let db = self.clone();
        let promise = future_to_promise(async move {
            let advice = db.host.progress.parked(db.db.permission_advice(id)).await;
            Ok(JsValue::from_str(advice_string(advice)))
        });
        WasmPermissionAdviceRequest {
            promise,
            db: self.clone(),
            id,
        }
    }
}

// ---- writes ------------------------------------------------------------------

#[wasm_bindgen]
pub struct WasmWrite {
    payload: Vec<u8>,
    row_id: RowUuid,
    tx_id: TransactionId,
    inner: Option<WasmWriteInner>,
}

struct WasmWriteInner {
    db: WasmDbInner,
    seq: u64,
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
        Err(not_supported("write state snapshots"))
    }

    /// Resolve once the write reached `tier`: at once for `none` and
    /// `local` (the commit is in the store when the write call returns), on
    /// the server's acceptance for `global`, rejecting if it refused.
    #[wasm_bindgen(js_name = wait)]
    pub fn wait(
        &self,
        tier: String,
        _observe_only: Option<bool>,
    ) -> Result<js_sys::Promise, JsValue> {
        let tier = durability_tier_from_str(&tier)?;
        let Some(WasmWriteInner { db, seq }) = &self.inner else {
            return Err(JsValue::from_str("write state is unavailable"));
        };
        let (db, seq) = (db.clone(), *seq);
        Ok(future_to_promise(async move {
            let fate = db.db.wait_for_transaction(seq, tier);
            db.host.progress.parked(fate).await.map_err(to_js_error)?;
            Ok(JsValue::UNDEFINED)
        }))
    }

    #[wasm_bindgen]
    pub fn close(&mut self) -> bool {
        self.inner.take().is_some()
    }
}

fn wasm_write(db: &WasmDbInner, write: WriteHandle) -> Result<WasmWrite, JsValue> {
    let seq = write.mergeable_tx_id();
    let result = WasmWriteResult {
        row_id: write.row_uuid(),
        tx_id: TxId::new(TxTime(seq), db.node),
    };
    Ok(WasmWrite {
        payload: postcard::to_allocvec(&result).map_err(to_js_error)?,
        row_id: result.row_id,
        tx_id: TransactionId::from_committed_tx(result.tx_id),
        inner: Some(WasmWriteInner {
            db: db.clone(),
            seq,
        }),
    })
}

struct WasmStreamingMutationState {
    db: WasmDbInner,
    upload: StreamingValueUpload,
    mutation: StreamingMutationKind,
    table: String,
    row_id: RowUuid,
    cells: Cells,
    column: String,
    identity: jazz::db::WriteIdentity,
    updated_at_ms: Option<u64>,
    head: Option<BranchSelector>,
    base: Option<BranchViewBase>,
}

/// A large value uploaded in chunks, then published as one row mutation.
/// `jazz-db` applies each push synchronously, so there is never a push in
/// flight when another call arrives.
#[wasm_bindgen(js_name = StreamingMutation)]
pub struct WasmStreamingMutation {
    state: Rc<RefCell<Option<Box<WasmStreamingMutationState>>>>,
}

fn streaming_mutation_closed() -> JsValue {
    JsValue::from_str("streaming mutation is closed")
}

#[wasm_bindgen]
impl WasmStreamingMutation {
    pub fn push(&self, chunk: Vec<u8>) -> js_sys::Promise {
        let mut slot = self.state.borrow_mut();
        let Some(state) = slot.as_mut() else {
            return js_sys::Promise::reject(&streaming_mutation_closed());
        };
        let result = block_on(
            state
                .db
                .db
                .push_streaming_value_upload(&mut state.upload, &chunk),
        );
        match result {
            Ok(()) => js_sys::Promise::resolve(&JsValue::UNDEFINED),
            Err(error) => {
                slot.take();
                js_sys::Promise::reject(&to_js_error(error))
            }
        }
    }

    pub fn finish(&self) -> js_sys::Promise {
        let Some(state) = self.state.borrow_mut().take() else {
            return js_sys::Promise::reject(&streaming_mutation_closed());
        };
        let state = *state;
        let result = block_on(state.db.db.finish_streaming_value_upload(
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
        ))
        .map_err(to_js_error)
        .and_then(|write| {
            state.db.host.after_write();
            wasm_write(&state.db, write)
        });
        match result {
            Ok(write) => js_sys::Promise::resolve(&JsValue::from(write)),
            Err(error) => js_sys::Promise::reject(&error),
        }
    }

    pub fn abort(&self) -> js_sys::Promise {
        let Some(state) = self.state.borrow_mut().take() else {
            return js_sys::Promise::resolve(&JsValue::FALSE);
        };
        match block_on(state.db.db.abort_streaming_value_upload(state.upload)) {
            Ok(()) => js_sys::Promise::resolve(&JsValue::TRUE),
            Err(error) => js_sys::Promise::reject(&to_js_error(error)),
        }
    }
}

// ---- the database --------------------------------------------------------------

#[wasm_bindgen]
pub struct WasmDb {
    // Taken at close admission: every later call fails before touching the
    // engine.
    inner: Rc<RefCell<Option<WasmDbInner>>>,
    owns_runtime: bool,
    // This is set only by the explicit backend-open ABI. Attributed writes
    // are otherwise a privilege-escalation surface, because their author is
    // provenance while admission remains the runtime's SYSTEM identity.
    trusted_backend: bool,
}

/// An open database (or one of its schema views) and its host hooks.
#[derive(Clone)]
struct WasmDbInner {
    db: Rc<Db>,
    host: Rc<Host>,
    node: NodeUuid,
    author: AuthorSubject,
}

impl WasmDbInner {
    fn new(db: Db, identity: DbIdentity) -> Self {
        Self {
            db: Rc::new(db),
            host: Rc::default(),
            node: identity.node,
            author: identity.author,
        }
    }

    fn write(&self, write: Result<WriteHandle, Error>) -> Result<WasmWrite, JsValue> {
        let write = write.map_err(to_js_error)?;
        self.host.after_write();
        wasm_write(self, write)
    }

    fn staged<T>(&self, staged: Result<T, Error>) -> Result<(), JsValue> {
        staged.map(drop).map_err(to_js_error)
    }

    async fn tick(&self) -> Result<(), Error> {
        let result = self.db.tick().await;
        self.host.progress.wake_all();
        result
    }
}

fn parse_open_tx(id: &str) -> Result<OpenTransactionId, JsValue> {
    id.parse::<OpenTransactionId>()
        .map_err(|error| JsValue::from_str(&error))
}

#[wasm_bindgen]
impl WasmDb {
    fn open_inner(&self) -> Result<WasmDbInner, JsValue> {
        self.inner
            .borrow()
            .clone()
            .ok_or_else(|| JsValue::from_str("WasmDb is closed"))
    }

    /// Exact wire capabilities compiled into this WASM artifact.
    #[wasm_bindgen(js_name = wireFeatures)]
    pub fn wire_features(&self) -> u32 {
        jazz::wire::current_wire_features() as u32
    }

    fn require_trusted_backend(&self) -> Result<(), JsValue> {
        self.trusted_backend.then_some(()).ok_or_else(|| {
            JsValue::from_str("backend attribution requires an explicit backend runtime")
        })
    }

    fn read_author(&self, author: Option<Vec<u8>>) -> Result<Option<AuthorSubject>, JsValue> {
        match author {
            Some(author) => Ok(Some(author_id_from_bytes(&author)?)),
            None if self.trusted_backend => Ok(Some(AuthorSubject::SYSTEM)),
            None => Ok(None),
        }
    }

    fn checked_identity(
        &self,
        identity: jazz::db::WriteIdentity,
    ) -> Result<jazz::db::WriteIdentity, JsValue> {
        if matches!(identity, jazz::db::WriteIdentity::Attribution(_)) {
            self.require_trusted_backend()?;
        }
        Ok(identity)
    }

    #[wasm_bindgen(js_name = insert)]
    pub fn insert_with_options(
        &self,
        table: String,
        cells: Vec<u8>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let cells = decode_cells(&cells)?;
        let mut options = insert_options_from_js(options)?;
        options.identity = self.checked_identity(options.identity)?;
        let inner = self.open_inner()?;
        inner.write(block_on(inner.db.insert(&table, cells, options)))
    }

    #[wasm_bindgen(js_name = insertInTransaction)]
    pub fn insert_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        cells: Vec<u8>,
        options: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let id = parse_open_tx(&open_transaction_id)?;
        let cells = decode_cells(&cells)?;
        let options = insert_options_from_js(options)?;
        let inner = self.open_inner()?;
        let row = block_on(inner.db.mergeable_tx_ref(id).insert(&table, cells, options))
            .map_err(to_js_error)?;
        Ok(row.row_uuid().to_bytes().to_vec())
    }

    #[wasm_bindgen(js_name = update)]
    pub fn update_with_options(
        &self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let patch = decode_cells(&patch)?;
        let mut options = update_options_from_js(options)?;
        options.identity = self.checked_identity(options.identity)?;
        let inner = self.open_inner()?;
        inner.write(block_on(inner.db.update(&table, row_id, patch, options)))
    }

    #[wasm_bindgen(js_name = updateInTransaction)]
    pub fn update_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        options: JsValue,
    ) -> Result<(), JsValue> {
        let id = parse_open_tx(&open_transaction_id)?;
        let row_id = row_uuid_from_bytes(&row_id)?;
        let patch = decode_cells(&patch)?;
        let options = update_options_from_js(options)?;
        let inner = self.open_inner()?;
        inner.staged(block_on(
            inner
                .db
                .mergeable_tx_ref(id)
                .update(&table, row_id, patch, options),
        ))
    }

    /// Typed partial-value updates staged inside an open transaction.
    #[wasm_bindgen(js_name = updateLargeValuesInTransaction)]
    pub fn update_large_values_in_transaction(
        &self,
        _open_transaction_id: String,
        _table: String,
        _row_id: Vec<u8>,
        _patch: Vec<u8>,
        _mutations: JsValue,
        _updated_at_ms: Option<f64>,
    ) -> Result<(), JsValue> {
        Err(not_supported("partial large-value updates inside a transaction"))
    }

    #[wasm_bindgen(js_name = updateLargeValues)]
    pub fn update_large_values(
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
        let updated_at_ms = updated_at_ms
            .map(|value| checked_js_u64(value, "updatedAtMs"))
            .transpose()?;
        let inner = self.open_inner()?;
        let options = jazz::db::UpdateOptions {
            updated_at_ms,
            ..Default::default()
        };
        inner.write(block_on(inner.db.update_with_large_value_mutations(
            &table, row_id, patch, mutations, options,
        )))
    }

    #[wasm_bindgen(js_name = upsert)]
    pub fn upsert_with_options(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let mut options = upsert_options_from_js(options)?;
        options.identity = self.checked_identity(options.identity)?;
        let inner = self.open_inner()?;
        inner.write(block_on(inner.db.upsert(&table, row_id, cells, options)))
    }

    #[wasm_bindgen(js_name = upsertInTransaction)]
    pub fn upsert_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        options: JsValue,
    ) -> Result<(), JsValue> {
        let id = parse_open_tx(&open_transaction_id)?;
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let options = upsert_options_from_js(options)?;
        let inner = self.open_inner()?;
        inner.staged(block_on(
            inner
                .db
                .mergeable_tx_ref(id)
                .upsert(&table, row_id, cells, options),
        ))
    }

    #[wasm_bindgen(js_name = delete)]
    pub fn delete_with_options(
        &self,
        table: String,
        row_id: Vec<u8>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let mut options = delete_options_from_js(options)?;
        options.identity = self.checked_identity(options.identity)?;
        let inner = self.open_inner()?;
        inner.write(block_on(inner.db.delete(&table, row_id, options)))
    }

    #[wasm_bindgen(js_name = deleteInTransaction)]
    pub fn delete_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Vec<u8>,
        options: JsValue,
    ) -> Result<(), JsValue> {
        let id = parse_open_tx(&open_transaction_id)?;
        let row_id = row_uuid_from_bytes(&row_id)?;
        let options = delete_options_from_js(options)?;
        let inner = self.open_inner()?;
        inner.staged(block_on(
            inner.db.mergeable_tx_ref(id).delete(&table, row_id, options),
        ))
    }

    #[wasm_bindgen(js_name = restore)]
    pub fn restore_with_options(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let mut options = restore_options_from_js(options)?;
        options.identity = self.checked_identity(options.identity)?;
        let inner = self.open_inner()?;
        inner.write(block_on(
            inner.db.restore(&table, row_id, Some(cells), options),
        ))
    }

    #[wasm_bindgen(js_name = restoreInTransaction)]
    pub fn restore_in_transaction(
        &self,
        _open_transaction_id: String,
        _table: String,
        _row_id: Vec<u8>,
        _cells: Vec<u8>,
        _options: JsValue,
    ) -> Result<(), JsValue> {
        Err(not_supported("restores inside a transaction"))
    }

    /// A fresh root runtime owner over `inner`.
    fn runtime_owner(inner: WasmDbInner, trusted_backend: bool) -> Self {
        Self {
            inner: Rc::new(RefCell::new(Some(inner))),
            owns_runtime: true,
            trusted_backend,
        }
    }

    #[wasm_bindgen(js_name = openMemory)]
    pub fn open_memory(schema: Vec<u8>, config: Vec<u8>) -> Result<WasmDb, JsValue> {
        initialize_wasm_tracing();
        let (schema, config) = decode_open_args(&schema, &config)?;
        validate_untrusted_open_author(&config)?;
        let identity = config.identity.into();
        let db = block_on(open_db(db_config(schema, &config, identity), &config))
            .map_err(to_js_error)?;
        Ok(Self::runtime_owner(WasmDbInner::new(db, identity), false))
    }

    /// Open a deliberate backend runtime. This remains a separate ABI from
    /// `openMemory`: the public raw-open configuration can never select the
    /// privileged system author.
    #[wasm_bindgen(js_name = openMemoryAsBackend)]
    pub fn open_memory_as_backend(schema: Vec<u8>, config: Vec<u8>) -> Result<WasmDb, JsValue> {
        initialize_wasm_tracing();
        let (schema, config) = decode_open_args(&schema, &config)?;
        let identity = backend_open_identity(&config)?;
        let db = block_on(open_db(db_config(schema, &config, identity), &config))
            .map_err(to_js_error)?;
        Ok(Self::runtime_owner(WasmDbInner::new(db, identity), true))
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
        initialize_wasm_tracing();
        let (schema, mut config) = decode_open_args(&schema, &config)?;
        config.identity.author =
            verify_self_signed_runtime_author(&token, &app_id, &claimed_author)?;
        let identity = config.identity.into();
        let db = block_on(open_db(db_config(schema, &config, identity), &config))
            .map_err(to_js_error)?;
        Ok(Self::runtime_owner(WasmDbInner::new(db, identity), false))
    }

    /// Open the durable store a browser worker owns. `storage` selects the
    /// SQLite VFS: `{ backend: "indexeddb" | "opfs", path?: string }`
    /// (IndexedDB when omitted; see [`browser`]). The default file is named
    /// after `storage_owner`, the worker's admitted ownership marker.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = openBrowser)]
    pub async fn open_browser(
        storage: JsValue,
        schema: Vec<u8>,
        config: Vec<u8>,
        storage_owner: String,
    ) -> Result<WasmDb, JsValue> {
        initialize_wasm_tracing();
        let (schema, config) = decode_open_args(&schema, &config)?;
        validate_untrusted_open_author(&config)?;
        Self::open_browser_runtime(storage, schema, config, storage_owner).await
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = openBrowserWithSelfSignedProof)]
    pub async fn open_browser_with_self_signed_proof(
        storage: JsValue,
        schema: Vec<u8>,
        config: Vec<u8>,
        token: String,
        app_id: String,
        claimed_author: String,
        storage_owner: String,
    ) -> Result<WasmDb, JsValue> {
        initialize_wasm_tracing();
        let (schema, mut config) = decode_open_args(&schema, &config)?;
        config.identity.author =
            verify_self_signed_runtime_author(&token, &app_id, &claimed_author)?;
        Self::open_browser_runtime(storage, schema, config, storage_owner).await
    }

    #[cfg(target_arch = "wasm32")]
    async fn open_browser_runtime(
        storage: JsValue,
        schema: JazzSchema,
        config: WasmOpenDbConfig,
        storage_owner: String,
    ) -> Result<WasmDb, JsValue> {
        // Every SQLite store keeps full history, so `history_complete`
        // needs nothing extra.
        let spec = browser::StoreSpec::from_js(&storage, &storage_owner)?;
        let identity = config.identity.into();
        let (db, store) = browser::open(&spec, db_config(schema, &config, identity)).await?;
        let inner = WasmDbInner::new(db, identity);
        *inner.host.store.borrow_mut() = Some(store);
        Ok(Self::runtime_owner(inner, false))
    }

    /// Delete a durable browser store (`{ backend, path }`, as for
    /// `openBrowser`). No database may have it open.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = destroyBrowserStorage)]
    pub async fn destroy_browser_storage(storage: JsValue, storage_owner: String) -> Result<(), JsValue> {
        let spec = browser::StoreSpec::from_js(&storage, &storage_owner)?;
        browser::destroy(&spec).await
    }

    /// Wait until every commit so far is in the browser store's backing
    /// storage. A no-op for in-memory and OPFS databases, whose commits are
    /// written through.
    #[wasm_bindgen(js_name = flush)]
    pub fn flush(&self) -> js_sys::Promise {
        let inner = match self.open_inner() {
            Ok(inner) => inner,
            Err(error) => return js_sys::Promise::reject(&error),
        };
        future_to_promise(async move {
            #[cfg(target_arch = "wasm32")]
            {
                let store = inner.host.store.borrow().clone();
                if let Some(store) = store {
                    store.flush().await?;
                }
            }
            let _ = inner;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Register a typed schema view backed by this same runtime owner.
    #[wasm_bindgen(js_name = registerSchema)]
    pub fn register_schema(&self, schema: Vec<u8>) -> Result<WasmDb, JsValue> {
        let schema = decode_public_schema(&schema)?;
        let inner = self.open_inner()?;
        let view = block_on(inner.db.register_schema_view(schema)).map_err(to_js_error)?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(WasmDbInner {
                db: Rc::new(view),
                ..inner
            }))),
            owns_runtime: false,
            trusted_backend: self.trusted_backend,
        })
    }

    /// Begin one owner-wide transaction.
    #[wasm_bindgen(js_name = beginTransaction)]
    pub fn begin_transaction(
        &self,
        open_transaction_id: String,
        kind: String,
        author: Option<Vec<u8>>,
        attribution: Option<Vec<u8>>,
    ) -> Result<(), JsValue> {
        let id = parse_open_tx(&open_transaction_id)?;
        if author.is_some() && attribution.is_some() {
            return Err(JsValue::from_str(
                "transaction identity cannot contain both author and attribution",
            ));
        }
        if author.is_some() || attribution.is_some() {
            return Err(not_supported("transactions bound to another identity"));
        }
        let inner = self.open_inner()?;
        match kind.as_str() {
            "mergeable" => block_on(inner.db.begin_mergeable(id)).map_err(to_js_error),
            "exclusive" | "serializable" => block_on(inner.db.begin_serializable(id)).map_err(to_js_error),
            _ => Err(JsValue::from_str(&unknown_transaction_kind_message(&kind))),
        }
    }

    /// Commit an owner-wide transaction by id.
    #[wasm_bindgen(js_name = commitTransaction)]
    pub fn commit_transaction(
        &self,
        open_transaction_id: String,
        kind: Option<String>,
    ) -> Result<WasmWrite, JsValue> {
        let id = parse_open_tx(&open_transaction_id)?;
        let inner = self.open_inner()?;
        let receipt = match kind.as_deref().unwrap_or("mergeable") {
            "mergeable" => block_on(inner.db.commit_mergeable_handle(id)),
            "exclusive" | "serializable" => block_on(inner.db.commit_serializable_handle(id)),
            kind => return Err(JsValue::from_str(&unknown_transaction_kind_message(kind))),
        }
        .map_err(to_js_error)?;
        inner.host.after_write();
        let tx_id = TxId::new(TxTime(receipt.0), inner.node);
        Ok(WasmWrite {
            payload: postcard::to_allocvec(&WasmWriteResult {
                row_id: RowUuid::from_bytes([0; 16]),
                tx_id,
            })
            .map_err(to_js_error)?,
            row_id: RowUuid::from_bytes([0; 16]),
            tx_id: TransactionId::from_committed_tx(tx_id),
            inner: Some(WasmWriteInner {
                db: inner,
                seq: receipt.0,
            }),
        })
    }

    /// Roll back an owner-wide open transaction by id.
    #[wasm_bindgen(js_name = rollbackTransaction)]
    pub fn rollback_transaction(&self, open_transaction_id: String) -> Result<(), JsValue> {
        let id = parse_open_tx(&open_transaction_id)?;
        self.open_inner()?
            .db
            .abandon_transaction_handle(id)
            .map_err(to_js_error)
    }

    /// Exact local state for the write-merge bridge: the row as stored,
    /// deleted or not. Write authorization remains at the mutation boundary;
    /// this is not a query.
    #[wasm_bindgen(js_name = localCurrentRow)]
    pub fn local_current_row(&self, table: String, row_id: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        let row_id = row_uuid_from_bytes(&row_id)?;
        let inner = self.open_inner()?;
        let query = jazz::query::Query::from(table.as_str()).filter(jazz::query::eq(
            jazz::query::col("id"),
            jazz::query::lit(Value::Uuid(row_id.0)),
        ));
        let prepared = inner.db.prepare_query(&query).map_err(to_js_error)?;
        let opts = ReadOpts {
            tier: jazz::db::ReadTier::LocalOnly,
            include_deleted: true,
            ..ReadOpts::default()
        };
        let rows = block_on(inner.db.all(&prepared, opts)).map_err(to_js_error)?;
        jazz::binding_codec::encode_rows(&rows).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = all)]
    pub fn all(
        &self,
        query: Vec<u8>,
        opts: JsValue,
        open_transaction_id: Option<String>,
        author: Option<Vec<u8>>,
        claims: JsValue,
    ) -> Result<JsValue, JsValue> {
        let inner = self.open_inner()?;
        let opts = read_opts_from_js(opts)?;
        let has_explicit_author = author.is_some();
        let author = self.read_author(author)?;
        let admission = if has_explicit_author {
            author
                .map(|author| Ok::<_, JsValue>((author, claims_from_js(author, claims)?)))
                .transpose()?
        } else {
            None
        };
        let open_tx = open_transaction_id
            .as_deref()
            .map(parse_open_tx)
            .transpose()?;
        let progress = inner.host.progress.clone();
        let future = Box::pin(progress.parked(async move {
            let result = inner
                .db
                .all_serialized_query(
                    &query,
                    opts,
                    open_tx,
                    admission,
                    author,
                    false,
                    || false,
                    |_| {},
                )
                .await
                .map_err(to_js_error)?;
            match result {
                SerializedReadResult::Rows(rows) => jazz::binding_codec::encode_rows(&rows),
                SerializedReadResult::Relation(snapshot) => {
                    jazz::binding_codec::encode_relation_snapshot(&snapshot)
                }
            }
            .map_err(to_js_error)
        }));
        wasm_read_or_pending(future)
    }

    /// Bind correlation claims to this already-open client's own identity.
    /// This does not grant a different author or enable serving reads.
    #[wasm_bindgen(js_name = setSessionClaims)]
    pub fn set_session_claims(&self, claims: JsValue) -> Result<JsValue, JsValue> {
        let inner = self.open_inner()?;
        let claims = claims_from_js(inner.author, claims)?;
        inner.db.set_identity_claims(inner.author, claims);
        inner.host.progress.wake_all();
        Ok(JsValue::UNDEFINED)
    }

    #[wasm_bindgen(js_name = setIdentityClaims)]
    pub fn set_identity_claims(&self, author: Vec<u8>, claims: JsValue) -> Result<(), JsValue> {
        let author = author_id_from_bytes(&author)?;
        let claims = claims_from_js(author, claims)?;
        let inner = self.open_inner()?;
        inner.db.set_identity_claims(author, claims);
        inner.host.progress.wake_all();
        Ok(())
    }

    #[wasm_bindgen(js_name = subscribe)]
    pub fn subscribe(
        &self,
        query: Vec<u8>,
        opts: JsValue,
        author: Option<Vec<u8>>,
        claims: JsValue,
    ) -> Result<JsValue, JsValue> {
        let opts = read_opts_from_js(opts)?;
        let has_explicit_author = author.is_some();
        let author = self.read_author(author)?;
        let claims = match (has_explicit_author, author) {
            (true, Some(author)) => Some(claims_from_js(author, claims)?),
            _ => None,
        };
        let inner = self.open_inner()?;
        let query: jazz::query::Query = postcard::from_bytes(&query)
            .map_err(|error| to_js_error(format!("undecodable query: {error}")))?;
        let mut prepared = inner.db.prepare_query(&query).map_err(to_js_error)?;
        let stream = match author {
            Some(author) => {
                if let Some(claims) = claims {
                    prepared = prepared.with_identity_claims(author, claims);
                }
                block_on(inner.db.subscribe_for_identity(&prepared, opts, author))
            }
            None => block_on(inner.db.subscribe(&prepared, opts)),
        }
        .map_err(to_js_error)?;
        // A remote attachment goes upstream on the next tick.
        inner.host.schedule_tick();
        subscription_stream_to_js(inner, stream)
    }

    /// The host's tick callback: called with `"immediate"` whenever there
    /// is sync work (a local write to upload, frames to apply).
    #[wasm_bindgen(js_name = setTickScheduler)]
    pub fn set_tick_scheduler(&self, callback: js_sys::Function) {
        if let Ok(inner) = self.open_inner() {
            *inner.host.tick_scheduler.borrow_mut() = Some(callback);
        }
    }

    /// Register a callback for rejected writes that no active wait consumed.
    #[wasm_bindgen(js_name = onMutationError)]
    pub fn on_mutation_error(&self, callback: js_sys::Function) {
        if let Ok(inner) = self.open_inner() {
            *inner.host.mutation_errors.borrow_mut() = Some(callback);
        }
    }

    /// A replica's own policy evidence may be incomplete, so it never
    /// decides locally; ask the authority with `requestInsertPermissionAdvice`.
    #[wasm_bindgen(js_name = canInsert)]
    pub fn can_insert(&self, _table: String, _cells: Vec<u8>) -> Result<String, JsValue> {
        self.open_inner()?;
        Ok(advice_string(PermissionAdvice::Unknown).to_owned())
    }

    #[wasm_bindgen(js_name = requestInsertPermissionAdvice)]
    pub fn request_insert_permission_advice(
        &self,
        table: String,
        cells: Vec<u8>,
    ) -> Result<WasmPermissionAdviceRequest, JsValue> {
        let cells = decode_cells(&cells)?;
        let action = PermissionAdviceAction::Insert { table, cells };
        Ok(self.open_inner()?.request_permission_advice(action))
    }

    #[wasm_bindgen(js_name = requestReadPermissionAdvice)]
    pub fn request_read_permission_advice(
        &self,
        table: String,
        row_id: Vec<u8>,
    ) -> Result<WasmPermissionAdviceRequest, JsValue> {
        let row = row_uuid_from_bytes(&row_id)?;
        let action = PermissionAdviceAction::Read { table, row };
        Ok(self.open_inner()?.request_permission_advice(action))
    }

    #[wasm_bindgen(js_name = requestUpdatePermissionAdvice)]
    pub fn request_update_permission_advice(
        &self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
    ) -> Result<WasmPermissionAdviceRequest, JsValue> {
        let row = row_uuid_from_bytes(&row_id)?;
        let patch = decode_cells(&patch)?;
        let action = PermissionAdviceAction::Update { table, row, patch };
        Ok(self.open_inner()?.request_permission_advice(action))
    }

    #[wasm_bindgen(js_name = requestDeletePermissionAdvice)]
    pub fn request_delete_permission_advice(
        &self,
        table: String,
        row_id: Vec<u8>,
    ) -> Result<WasmPermissionAdviceRequest, JsValue> {
        let row = row_uuid_from_bytes(&row_id)?;
        let action = PermissionAdviceAction::Delete { table, row };
        Ok(self.open_inner()?.request_permission_advice(action))
    }

    #[wasm_bindgen(js_name = captureSnapshot)]
    pub fn capture_snapshot(&self) -> Result<JsValue, JsValue> {
        let inner = self.open_inner()?;
        let progress = inner.host.progress.clone();
        let future = Box::pin(progress.parked(async move {
            let snapshot = inner.db.capture_snapshot().await.map_err(to_js_error)?;
            Ok(snapshot.to_persisted().into_bytes())
        }));
        wasm_read_or_pending(future)
    }

    /// Resolve once every local write so far reached `tier`.
    #[wasm_bindgen(js_name = waitForPendingWrites)]
    pub fn wait_for_pending_writes(&self, tier: String) -> js_sys::Promise {
        let inner = match self.open_inner() {
            Ok(inner) => inner,
            Err(error) => return js_sys::Promise::reject(&error),
        };
        future_to_promise(async move {
            match durability_tier_from_str(&tier)? {
                DurabilityTier::None => {}
                DurabilityTier::Local => {
                    #[cfg(target_arch = "wasm32")]
                    {
                        let store = inner.host.store.borrow().clone();
                        if let Some(store) = store {
                            store.flush().await?;
                        }
                    }
                }
                DurabilityTier::Global => {
                    let db = inner.db.clone();
                    let pending = db.wait_for_pending_writes(DurabilityTier::Global);
                    inner.host.progress.parked(pending).await.map_err(to_js_error)?;
                }
            }
            Ok(JsValue::UNDEFINED)
        })
    }

    #[wasm_bindgen(js_name = tick)]
    pub fn tick(&self) -> js_sys::Promise {
        let inner = match self.open_inner() {
            Ok(inner) => inner,
            Err(error) => return js_sys::Promise::reject(&error),
        };
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
            .unwrap_or(LargeValueStagingPolicy::default().max_age_ms);
        let policy = LargeValueStagingPolicy {
            incoming_bytes_per_window,
            window_ms,
            max_age_ms,
        };
        self.open_inner()?.db.set_large_value_staging_policy(policy);
        Ok(())
    }

    /// Run one idempotent expiry pass; browser hosts normally call this from a timer.
    #[wasm_bindgen(js_name = evictExpiredStagedLargeValues)]
    pub fn evict_expired_staged_large_values(&self) -> js_sys::Promise {
        let inner = match self.open_inner() {
            Ok(inner) => inner,
            Err(error) => return js_sys::Promise::reject(&error),
        };
        future_to_promise(async move {
            let evicted = inner
                .db
                .evict_expired_staged_large_values()
                .await
                .map_err(to_js_error)?;
            Ok(JsValue::from_f64(evicted as f64))
        })
    }

    #[wasm_bindgen(js_name = beginStreamingMutation)]
    #[allow(clippy::too_many_arguments)]
    pub fn begin_streaming_mutation(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        column: String,
        mutation: Option<String>,
        author: Option<Vec<u8>>,
        attribution: Option<Vec<u8>>,
        updated_at_ms: Option<f64>,
        head: Option<JsValue>,
        base: Option<JsValue>,
    ) -> Result<WasmStreamingMutation, JsValue> {
        let identity = match (author, attribution) {
            (Some(_), Some(_)) => {
                return Err(JsValue::from_str(
                    "streaming mutation identity cannot contain both author and attribution",
                ));
            }
            (Some(author), None) => {
                jazz::db::WriteIdentity::Session(author_id_from_bytes(&author)?)
            }
            (None, Some(attribution)) => {
                self.require_trusted_backend()?;
                jazz::db::WriteIdentity::Attribution(author_id_from_bytes(&attribution)?)
            }
            (None, None) => jazz::db::WriteIdentity::Database,
        };
        let row_id = row_uuid_from_bytes(&row_id)?;
        let cells = decode_cells(&cells)?;
        let mutation = match mutation.as_deref().unwrap_or("insert") {
            "insert" => StreamingMutationKind::Insert,
            "update" => StreamingMutationKind::Update,
            "upsert" => StreamingMutationKind::Upsert,
            _ => return Err(JsValue::from_str("unknown streaming mutation kind")),
        };
        let updated_at_ms = updated_at_ms
            .map(|value| checked_js_u64(value, "updatedAtMs"))
            .transpose()?;
        let head = present(head)
            .map(|value| serde_wasm_bindgen::from_value(value).map_err(to_js_error))
            .transpose()?;
        let base = present(base)
            .map(|value| serde_wasm_bindgen::from_value(value).map_err(to_js_error))
            .transpose()?;
        if base.is_some() && head.is_none() {
            return Err(JsValue::from_str(
                "streaming mutation base requires a branch head",
            ));
        }
        if matches!(identity, jazz::db::WriteIdentity::Attribution(_)) && head.is_some() {
            return Err(JsValue::from_str(
                "backend-attributed streaming mutations do not support branch writes",
            ));
        }
        let inner = self.open_inner()?;
        let upload = inner
            .db
            .begin_streaming_value_upload(&table, &cells, &column)
            .map_err(to_js_error)?;
        Ok(WasmStreamingMutation {
            state: Rc::new(RefCell::new(Some(Box::new(WasmStreamingMutationState {
                db: inner,
                upload,
                mutation,
                table,
                row_id,
                cells,
                column,
                identity,
                updated_at_ms,
                head,
                base,
            })))),
        })
    }

    /// The optimistic in-memory side of a browser client/worker pair needs
    /// the relay tier, which is not built on SQLite yet.
    #[wasm_bindgen(js_name = setNonDurableClient)]
    pub fn set_non_durable_client(&self) -> Result<(), JsValue> {
        Err(not_supported("a non-durable client over a relaying worker"))
    }

    /// Report what the host knows about the path to the authoritative server
    /// (`"none" | "attempting" | "live" | "failed"`; the TypeScript names
    /// `"connecting" | "connected" | "unavailable"` are accepted as aliases).
    #[wasm_bindgen(js_name = setRemoteLinkHint)]
    pub fn set_remote_link_hint(&self, state: String) -> Result<(), JsValue> {
        let hint = RemoteLinkHint::from_host_str(&state)
            .ok_or_else(|| JsValue::from_str(&format!("unknown remote link state {state}")))?;
        let inner = self.open_inner()?;
        inner.db.set_remote_link_hint(hint);
        inner.host.progress.wake_all();
        Ok(())
    }

    /// The newest transaction time this database handed out under its
    /// node (a `TxTime` is the commit seq). A leased node's next holder
    /// seeds it through `seedForegroundTxTimeHighWater`.
    #[wasm_bindgen(js_name = foregroundTxTimeHighWater)]
    pub fn foreground_tx_time_high_water(&self) -> Result<js_sys::Promise, JsValue> {
        let high_water = self.open_inner()?.db.seq_high_water();
        Ok(js_sys::Promise::resolve(&JsValue::from(high_water)))
    }

    /// Continue after a previous holder of this node: no transaction time
    /// at or below `high_water` is reused.
    #[wasm_bindgen(js_name = seedForegroundTxTimeHighWater)]
    pub fn seed_foreground_tx_time_high_water(&self, high_water: u64) -> Result<(), JsValue> {
        self.open_inner()?.db.seed_seq_floor(high_water);
        Ok(())
    }

    /// Connect to the server over byte frames the host carries: each frame
    /// is one postcard `SyncMessage`.
    #[wasm_bindgen(js_name = connectUpstream)]
    pub fn connect_upstream(&self) -> Result<js_sys::Promise, JsValue> {
        let inner = self.open_inner()?;
        let queues = WasmWireQueues::default();
        let transport = Box::new(WireTransportAdapter::current(WasmWireTransport {
            queues: queues.clone(),
        }));
        let upstream = block_on(inner.db.connect_upstream(transport));
        // Resubscriptions and unsettled commits go out on the next tick.
        inner.host.schedule_tick();
        Ok(js_sys::Promise::resolve(&JsValue::from(WasmTransport {
            db: inner,
            queues,
            link: Link::Upstream(Some(upstream)),
        })))
    }

    /// Connect after the carrier accepted the server Hello. The session
    /// context (authority endpoints, negotiated features) has no counterpart
    /// on SQLite's sync, so this is [`Self::connect_upstream`].
    #[wasm_bindgen(js_name = connectUpstreamWithSession)]
    pub fn connect_upstream_with_session(
        &self,
        _protocol_version: u16,
        _features: u32,
        remote_node: Vec<u8>,
        _remote_epoch: u64,
        local_node: Vec<u8>,
        _local_epoch: u64,
    ) -> Result<js_sys::Promise, JsValue> {
        let _: [u8; 16] = remote_node
            .try_into()
            .map_err(|_| JsValue::from_str("server hello authority node must be 16 bytes"))?;
        let _: [u8; 16] = local_node
            .try_into()
            .map_err(|_| JsValue::from_str("local peer identity must be 16 bytes"))?;
        self.connect_upstream()
    }

    /// Serve a subscriber (a tab's in-memory database) as `identity`.
    #[wasm_bindgen(js_name = acceptSubscriber)]
    pub fn accept_subscriber(
        &self,
        identity: Vec<u8>,
        claims: JsValue,
        _local_epoch: Option<u64>,
    ) -> Result<js_sys::Promise, JsValue> {
        let identity = author_id_from_bytes(&identity)?;
        let claims = claims_from_js(identity, claims)?;
        Ok(js_sys::Promise::resolve(&JsValue::from(
            self.accept_admitted(identity, claims)?,
        )))
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
        _local_epoch: Option<u64>,
    ) -> Result<js_sys::Promise, JsValue> {
        let identity = verify_self_signed_runtime_author(&token, &app_id, &claimed_author)?;
        let claims = claims_from_js(identity, claims)?;
        Ok(js_sys::Promise::resolve(&JsValue::from(
            self.accept_admitted(identity, claims)?,
        )))
    }

    fn accept_admitted(
        &self,
        identity: AuthorSubject,
        claims: Cells,
    ) -> Result<WasmTransport, JsValue> {
        let inner = self.open_inner()?;
        inner.db.set_identity_claims(identity, claims);
        let queues = WasmWireQueues::default();
        let transport = Box::new(WireTransportAdapter::current(WasmWireTransport {
            queues: queues.clone(),
        }));
        let subscriber = inner.db.accept_subscriber(transport, identity);
        Ok(WasmTransport {
            db: inner,
            queues,
            link: Link::Subscriber {
                id: Some(subscriber),
                identity,
            },
        })
    }

    #[wasm_bindgen(js_name = close)]
    pub fn close(&self) -> js_sys::Promise {
        // A close failure still consumes the binding: retrying a partially
        // failed close would re-enter an indeterminate runtime.
        let Some(inner) = self.inner.borrow_mut().take() else {
            return js_sys::Promise::resolve(&JsValue::from_bool(false));
        };
        let owns_runtime = self.owns_runtime;
        future_to_promise(async move {
            if owns_runtime {
                inner.db.close().await.map_err(to_js_error)?;
                #[cfg(target_arch = "wasm32")]
                {
                    let store = inner.host.store.borrow_mut().take();
                    if let Some(store) = store {
                        store.flush().await?;
                    }
                }
            }
            Ok(JsValue::TRUE)
        })
    }
}

// ---- transports ------------------------------------------------------------------

#[derive(Clone, Default)]
struct WasmWireQueues {
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    outbound_scheduler: Rc<RefCell<Option<js_sys::Function>>>,
}

struct WasmWireTransport {
    queues: WasmWireQueues,
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

enum Link {
    Upstream(Option<jazz::db::Upstream>),
    Subscriber {
        id: Option<jazz::db::SubscriberId>,
        identity: AuthorSubject,
    },
}

/// One sync link, carried by the host as byte frames. Frames are applied
/// and produced by the database's tick.
#[wasm_bindgen]
pub struct WasmTransport {
    db: WasmDbInner,
    queues: WasmWireQueues,
    link: Link,
}

#[wasm_bindgen]
impl WasmTransport {
    #[wasm_bindgen(js_name = updateAuthenticatedClaims)]
    pub fn update_authenticated_claims(&self, claims: JsValue) -> Result<js_sys::Promise, JsValue> {
        let Link::Subscriber { identity, .. } = self.link else {
            return Err(JsValue::from_str("transport is not a subscriber link"));
        };
        let claims = claims_from_js(identity, claims)?;
        self.db.db.set_identity_claims(identity, claims);
        self.db.host.progress.wake_all();
        Ok(js_sys::Promise::resolve(&JsValue::UNDEFINED))
    }

    /// SQLite's sync has no auxiliary chunk lane: large-value chunks travel
    /// as ordinary sync messages. Every frame belongs to the ordinary lane.
    #[wasm_bindgen(js_name = routeAuxiliaryWireFrame)]
    pub fn route_auxiliary_wire_frame(&self, frame: Vec<u8>) -> js_sys::Promise {
        js_sys::Promise::resolve(&JsValue::from(js_sys::Uint8Array::from(frame.as_slice())))
    }

    #[wasm_bindgen(js_name = recvAuxiliaryWireFrames)]
    pub fn recv_auxiliary_wire_frames(
        &self,
        _max_frames: Option<u32>,
        _max_bytes: Option<u32>,
    ) -> js_sys::Array {
        js_sys::Array::new()
    }

    #[wasm_bindgen(js_name = auxiliaryReceiveTimeoutMs)]
    pub fn auxiliary_receive_timeout_ms(&self) -> Option<u32> {
        None
    }

    #[wasm_bindgen(js_name = expireAuxiliaryReceive)]
    pub fn expire_auxiliary_receive(&self) {}

    /// Never resolves: there is no auxiliary lane to produce output.
    #[wasm_bindgen(js_name = auxiliaryOutboundReady)]
    pub fn auxiliary_outbound_ready(&self) -> js_sys::Promise {
        js_sys::Promise::new(&mut |_, _| {})
    }

    #[wasm_bindgen(js_name = takeAuxiliaryTrace)]
    pub fn take_auxiliary_trace(&self) -> js_sys::Array {
        js_sys::Array::new()
    }

    #[wasm_bindgen(js_name = setAuxiliaryTraceEnabled)]
    pub fn set_auxiliary_trace_enabled(&self, _enabled: bool) {}

    #[wasm_bindgen(js_name = sendWireFrame)]
    pub fn send_wire_frame(&self, frame: Vec<u8>) {
        self.queues.inbound.borrow_mut().push_back(frame);
        self.db.host.schedule_tick();
    }

    #[wasm_bindgen(js_name = sendWireFrames)]
    pub fn send_wire_frames(&self, frames: js_sys::Array) {
        {
            let mut inbound = self.queues.inbound.borrow_mut();
            for frame in frames.iter() {
                inbound.push_back(js_sys::Uint8Array::new(&frame).to_vec());
            }
        }
        self.db.host.schedule_tick();
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

    /// Tick the database this link belongs to: apply received frames, send
    /// what is due. Resolves with the number of frames still to send.
    #[wasm_bindgen(js_name = tick)]
    pub fn tick(&self) -> js_sys::Promise {
        let db = self.db.clone();
        let outbound = self.queues.outbound.clone();
        future_to_promise(async move {
            db.tick().await.map_err(to_js_error)?;
            Ok(JsValue::from_f64(outbound.borrow().len() as f64))
        })
    }

    #[wasm_bindgen(js_name = close)]
    pub fn close(&mut self) -> bool {
        match &mut self.link {
            Link::Upstream(upstream) => upstream
                .take()
                .is_some_and(|upstream| self.db.db.detach_connection(&upstream)),
            Link::Subscriber { id, .. } => match id.take() {
                Some(id) => {
                    self.db.db.disconnect_subscriber(id);
                    true
                }
                None => false,
            },
        }
    }
}

// ---- argument decoding -----------------------------------------------------------

fn wasm_read_or_pending(future: WasmReadFuture) -> Result<JsValue, JsValue> {
    let pending = WasmPendingNativeRead::new(future);
    match pending.poll_once()? {
        Some(bytes) => bytes_to_js(bytes),
        None => Ok(pending.into()),
    }
}

fn present(value: Option<JsValue>) -> Option<JsValue> {
    value.filter(|value| !value.is_null() && !value.is_undefined())
}

fn decode_cells(bytes: &[u8]) -> Result<Cells, JsValue> {
    jazz::binding_codec::decode_named_cells(bytes)
        .map_err(|error| to_js_error(format!("decode cells: {error}")))
}

fn write_option(options: &JsValue, name: &str) -> Result<Option<JsValue>, JsValue> {
    if options.is_null() || options.is_undefined() {
        return Ok(None);
    }
    let value = js_sys::Reflect::get(options, &JsValue::from_str(name))?;
    Ok((!value.is_null() && !value.is_undefined()).then_some(value))
}

/// Whether a JavaScript write-options object *contains* a property.
///
/// This deliberately differs from [`write_option`]: for ordinary optional
/// fields, `undefined` and `null` mean "no value".  Removed fields must be
/// rejected by presence, though, so untyped callers cannot smuggle the old
/// `{ branch: undefined }` shape through to a root-target upsert.
fn has_write_option(options: &JsValue, name: &str) -> Result<bool, JsValue> {
    if options.is_null() || options.is_undefined() {
        return Ok(false);
    }
    js_sys::Reflect::has(options, &JsValue::from_str(name))
}

fn write_identity_option(options: &JsValue) -> Result<jazz::db::WriteIdentity, JsValue> {
    let author = write_option(options, "author")?;
    let attribution = write_option(options, "attribution")?;
    match (author, attribution) {
        (Some(_), Some(_)) => Err(JsValue::from_str(
            "write identity cannot contain both author and attribution",
        )),
        (Some(author), None) => author_id_from_bytes(&js_sys::Uint8Array::new(&author).to_vec())
            .map(jazz::db::WriteIdentity::Session),
        (None, Some(attribution)) => {
            author_id_from_bytes(&js_sys::Uint8Array::new(&attribution).to_vec())
                .map(jazz::db::WriteIdentity::Attribution)
        }
        (None, None) => Ok(jazz::db::WriteIdentity::Database),
    }
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

fn branch_view_target(options: &JsValue) -> Result<jazz::db::WriteTarget, JsValue> {
    let head = write_option(options, "head")?;
    let base = write_option(options, "base")?;
    match (head, base) {
        (Some(head), base) => Ok(jazz::db::WriteTarget::BranchView {
            head: serde_wasm_bindgen::from_value(head).map_err(to_js_error)?,
            base: base
                .map(|base| serde_wasm_bindgen::from_value(base).map_err(to_js_error))
                .transpose()?,
        }),
        (None, None) => Ok(Default::default()),
        (None, Some(_)) => Err(JsValue::from_str(
            "branch view base requires a head selector",
        )),
    }
}

fn update_options_from_js(options: JsValue) -> Result<jazz::db::UpdateOptions, JsValue> {
    Ok(jazz::db::UpdateOptions {
        identity: write_identity_option(&options)?,
        target: branch_view_target(&options)?,
        updated_at_ms: write_timestamp_option(&options)?,
    })
}

fn upsert_options_from_js(options: JsValue) -> Result<jazz::db::UpsertOptions, JsValue> {
    if has_write_option(&options, "branch")? {
        return Err(JsValue::from_str(
            "upsert option `branch` is not supported; use `head` (and optional `base`) for a branch view",
        ));
    }
    Ok(jazz::db::UpsertOptions {
        identity: write_identity_option(&options)?,
        target: branch_view_target(&options)?,
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
    Ok(jazz::db::RestoreOptions {
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

fn db_config(schema: JazzSchema, config: &WasmOpenDbConfig, identity: DbIdentity) -> DbConfig {
    let db_config = DbConfig::new(schema, identity);
    match config.row_id_seed {
        Some(seed) => db_config.with_id_source(SeededRowIdSource::new(seed)),
        None => db_config,
    }
}

async fn open_db(db_config: DbConfig, config: &WasmOpenDbConfig) -> Result<Db, Error> {
    if config.history_complete {
        Db::open_history_complete(db_config).await
    } else {
        Db::open(db_config).await
    }
}

fn validate_untrusted_open_author(config: &WasmOpenDbConfig) -> Result<(), JsValue> {
    validate_untrusted_open_author_core(config)
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

fn backend_open_identity(config: &WasmOpenDbConfig) -> Result<DbIdentity, JsValue> {
    // Validate every caller-controlled field at the ordinary fail-closed
    // ingress before this explicit, intentional backend ABI derives SYSTEM.
    validate_untrusted_open_author(config)?;
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
    // `std::time::SystemTime` panics under wasm32. The verifier's explicit
    // clock form keeps proof expiry validation intact while using the browser-
    // safe clock already used by this binding.
    let now_seconds = web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_err(|error| JsValue::from_str(&error.to_string()))?
        .as_secs();
    jazz::tools::identity::verify_client_runtime_author_at(
        token,
        app_id,
        claimed_author,
        now_seconds,
    )
    .map_err(|error| JsValue::from_str(&error))
}

fn row_uuid_from_bytes(bytes: &[u8]) -> Result<RowUuid, JsValue> {
    jazz::binding_args::row_uuid(bytes).map_err(|error| JsValue::from_str(&error))
}

fn checked_js_u64(value: f64, name: &str) -> Result<u64, JsValue> {
    jazz::binding_args::checked_u64(value, name).map_err(|error| JsValue::from_str(&error))
}

fn author_id_from_bytes(bytes: &[u8]) -> Result<AuthorSubject, JsValue> {
    jazz::binding_args::author_subject(bytes).map_err(|error| JsValue::from_str(&error))
}

fn claims_from_js(author: AuthorSubject, claims: JsValue) -> Result<Cells, JsValue> {
    let raw: serde_json::Value = serde_wasm_bindgen::from_value(claims).map_err(to_js_error)?;
    jazz::binding_args::policy_claims(&author, raw).map_err(|error| JsValue::from_str(&error))
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
    if let Some(include_deleted) = optional_bool_prop(&value, "include_deleted")? {
        opts.include_deleted = include_deleted;
    }
    if let Some(timeout_ms) = optional_wait_ms_prop(&value, "first_load_remote_wait_ms")? {
        if opts.tier == jazz::db::ReadTier::LocalFirst {
            opts.first_load = FirstLoad::WaitForRemote { timeout_ms };
        }
    }
    Ok(opts)
}

fn optional_wait_ms_prop(value: &JsValue, name: &str) -> Result<Option<u64>, JsValue> {
    let prop = js_sys::Reflect::get(value, &JsValue::from_str(name))?;
    if prop.is_undefined() || prop.is_null() {
        return Ok(None);
    }
    match prop.as_f64() {
        Some(ms) if ms.is_finite() && ms >= 0.0 => Ok(Some(ms.floor() as u64)),
        _ => Err(JsValue::from_str(&format!(
            "{name} must be a non-negative number of milliseconds"
        ))),
    }
}

fn durability_tier_from_str(tier: &str) -> Result<DurabilityTier, JsValue> {
    jazz::binding_args::durability_tier(tier).map_err(|error| JsValue::from_str(&error))
}

fn read_tier_from_str(tier: &str) -> Result<jazz::db::ReadTier, JsValue> {
    jazz::binding_args::read_tier(tier).map_err(|error| JsValue::from_str(&error))
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

// ---- subscriptions -----------------------------------------------------------------

/// The stream's next event, waiting for a write or tick when there is none.
struct NextEvent<'a> {
    stream: &'a mut SubscriptionStream,
    progress: &'a Progress,
}

impl Future for NextEvent<'_> {
    type Output = SubscriptionEvent;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<SubscriptionEvent> {
        match self.stream.try_next_event() {
            Some(event) => Poll::Ready(event),
            None => {
                self.progress.park(context);
                Poll::Pending
            }
        }
    }
}

fn subscription_stream_to_js(
    db: WasmDbInner,
    stream: SubscriptionStream,
) -> Result<JsValue, JsValue> {
    let source = futures_util::stream::unfold(Some((db, stream)), |state| async move {
        let (db, mut stream) = state?;
        let event = NextEvent {
            stream: &mut stream,
            progress: &db.host.progress,
        }
        .await;
        let closed = matches!(event, SubscriptionEvent::Closed);
        let chunk = subscription_chunk_to_js(event);
        Some((chunk, (!closed).then_some((db, stream))))
    });
    readable_stream_from_stream(source)
}

fn subscription_chunk_to_js(event: SubscriptionEvent) -> Result<JsValue, JsValue> {
    let object = js_sys::Object::new();
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            settled,
            tier,
            ..
        } => {
            let delta = jazz::binding_codec::encode_subscription_delta(&added, &updated, &removed)
                .map_err(to_js_error)?;
            set_prop(&object, "type", JsValue::from_str("delta"))?;
            set_prop(
                &object,
                "delta",
                js_sys::Uint8Array::from(delta.as_slice()).into(),
            )?;
            set_prop(&object, "terminalOperations", js_sys::Array::new().into())?;
            set_prop(&object, "reset", JsValue::from_bool(reset))?;
            set_prop(&object, "settled", JsValue::from_bool(settled))?;
            set_prop(&object, "tier", JsValue::from_str(&format!("{tier:?}")))?;
        }
        SubscriptionEvent::Closed => {
            set_prop(&object, "type", JsValue::from_str("closed"))?;
        }
        SubscriptionEvent::Rejected { reason } => {
            let reason_object = js_sys::Object::new();
            set_prop(&reason_object, "type", JsValue::from_str("ServerFailure"))?;
            set_prop(&reason_object, "detail", JsValue::from_str(&reason))?;
            set_prop(&object, "type", JsValue::from_str("rejected"))?;
            set_prop(&object, "reason", reason_object.into())?;
        }
    };
    Ok(object.into())
}

fn set_prop(object: &js_sys::Object, name: &str, value: JsValue) -> Result<(), JsValue> {
    js_sys::Reflect::set(object, &JsValue::from_str(name), &value).map(|_| ())
}

type JsResultStream = dyn futures_util::Stream<Item = Result<JsValue, JsValue>>;

fn readable_stream_from_stream<St>(stream: St) -> Result<JsValue, JsValue>
where
    St: futures_util::Stream<Item = Result<JsValue, JsValue>> + 'static,
{
    use futures_util::StreamExt;

    let stream: Pin<Box<JsResultStream>> = Box::pin(stream);
    let state = Rc::new(RefCell::new(Some(stream)));
    let cancelled = Rc::new(std::cell::Cell::new(false));
    let active_abort = Rc::new(RefCell::new(None::<AbortHandle>));
    let source = js_sys::Object::new();

    let pull_state = Rc::clone(&state);
    let pull_cancelled = Rc::clone(&cancelled);
    let pull_abort = Rc::clone(&active_abort);
    let pull = Closure::<dyn FnMut(JsValue) -> js_sys::Promise>::new(move |controller| {
        let pull_state = Rc::clone(&pull_state);
        let pull_cancelled = Rc::clone(&pull_cancelled);
        let pull_abort = Rc::clone(&pull_abort);
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
                // ends the subscription.
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

    let cancel_state = Rc::clone(&state);
    let cancel_flag = Rc::clone(&cancelled);
    let cancel_abort = Rc::clone(&active_abort);
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

// ---- errors --------------------------------------------------------------------------

/// Convert a Rust error into the value a binding rejects or throws with.
///
/// A core [`Error`] becomes a real JavaScript `Error` whose `message` is the
/// unchanged `Display` text and whose `code` is the stable
/// [`ErrorCode::as_str`] string, so callers classify it without matching the
/// message. Every other error keeps its historical bare-string form.
fn to_js_error(error: impl std::fmt::Display + 'static) -> JsValue {
    let any = &error as &dyn std::any::Any;
    if let Some(core) = any.downcast_ref::<Error>() {
        return core_error_to_js(core);
    }
    JsValue::from_str(&error.to_string())
}

fn core_error_to_js(error: &Error) -> JsValue {
    let js_error = js_sys::Error::new(&error.to_string());
    // Setting a data property on a fresh ordinary object cannot fail.
    let _ = js_sys::Reflect::set(
        &js_error,
        &JsValue::from_str("code"),
        &JsValue::from_str(error.code.as_str()),
    );
    js_error.into()
}

fn bytes_to_js(bytes: Vec<u8>) -> Result<JsValue, JsValue> {
    Ok(js_sys::Uint8Array::from(bytes.as_slice()).into())
}

fn unknown_transaction_kind_message(kind: &str) -> String {
    format!("unknown transaction kind {kind}")
}

#[cfg(test)]
mod tests {
    /// Every ordinary write option shares `write_timestamp_option`, so this
    /// boundary test protects insert, update, upsert, delete, and restore from
    /// JavaScript's lossy number-to-u64 coercions.
    #[test]
    fn write_timestamp_requires_a_nonnegative_safe_integer_millisecond_value() {
        assert_eq!(
            jazz::binding_args::safe_u64(1_704_067_200_123.0),
            Some(1_704_067_200_123),
        );
        for invalid in [-1.0, 1.5, f64::NAN, f64::INFINITY, 9_007_199_254_740_992.0] {
            assert!(
                jazz::binding_args::safe_u64(invalid).is_none(),
                "invalid updatedAtMs {invalid:?} must fail before a write"
            );
        }
    }
}
