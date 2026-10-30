//! WebAssembly bindings for the Jazz database (`jazz-db`).
//!
//! One bundle serves both browser roles: a worker that owns a durable SQLite
//! file (on IndexedDB or OPFS, see [`browser`]) and main-thread in-memory
//! databases that sync through it. Calls for features Jazz does not support
//! yet fail with an explicit "not supported yet" error.
//!
//! `jazz-db` runs every operation to completion synchronously. The only
//! things that wait are reads and write fates that need the server; they
//! complete on a later tick, so every tick wakes them (see [`Progress`]).

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use futures_util::future::{AbortHandle, Abortable};
use jazz::binding_host::{
    self, Binding, EventChunk, HostError, HostObject, HostResult, OpenAs, Write,
};
use jazz::db::{block_on, Error, LargeValueUpdate, SubscriptionEvent, SubscriptionStream};
use jazz::ids::AuthorSubject;
use jazz::tx::DurabilityTier;
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

/// Validate a schema and its embedded permissions without opening a database.
#[wasm_bindgen(js_name = validateSchema)]
pub fn validate_schema(schema: Vec<u8>) -> Result<(), JsValue> {
    binding_host::decode_schema(&schema)
        .map(|_| ())
        .map_err(host_error)
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
        match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
            Some("error") => tracing::Level::ERROR,
            Some("info") => tracing::Level::INFO,
            Some("debug") => tracing::Level::DEBUG,
            Some("trace") => tracing::Level::TRACE,
            _ => tracing::Level::WARN,
        }
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

/// Test-only: read one pinned wire corpus batch through the generated WASM
/// artifact, as `reader` (`"server-opening"`, `"server"` or `"client"`)
/// would, with the production frame and sync message decoders. Not a
/// browser transport API.
#[wasm_bindgen(js_name = __testValidateWireFrameBatchCorpus)]
pub fn test_validate_wire_frame_batch_corpus(
    batch: Vec<u8>,
    reader: String,
    negotiated_features: String,
) -> Result<(), JsValue> {
    let negotiated_features = negotiated_features
        .parse()
        .map_err(|_| JsValue::from_str("test wire corpus features must be a u64 decimal"))?;
    let reader = match reader.as_str() {
        "server-opening" => jazz::db::CorpusReader::ServerOpening,
        "server" => jazz::db::CorpusReader::Server,
        "client" => jazz::db::CorpusReader::Client,
        other => {
            return Err(JsValue::from_str(&format!(
                "unknown wire corpus reader {other:?}"
            )));
        }
    };
    jazz::db::validate_frame_batch_for_corpus(&batch, reader, negotiated_features)
        .map_err(|error| JsValue::from_str(&error))
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

// ---- host values -----------------------------------------------------------

/// A JavaScript options object (or `undefined`).
struct JsOptions(JsValue);

impl HostObject for JsOptions {
    type Value = JsValue;

    fn get(&self, name: &str) -> HostResult<Option<JsValue>> {
        if self.0.is_null() || self.0.is_undefined() {
            return Ok(None);
        }
        let value = js_sys::Reflect::get(&self.0, &JsValue::from_str(name))
            .map_err(|error| HostError::Argument(js_error_message(&error)))?;
        Ok((!value.is_null() && !value.is_undefined()).then_some(value))
    }

    fn has(&self, name: &str) -> HostResult<bool> {
        if self.0.is_null() || self.0.is_undefined() {
            return Ok(false);
        }
        js_sys::Reflect::has(&self.0, &JsValue::from_str(name))
            .map_err(|error| HostError::Argument(js_error_message(&error)))
    }

    fn bytes(value: &JsValue) -> HostResult<Vec<u8>> {
        Ok(js_sys::Uint8Array::new(value).to_vec())
    }

    fn number(value: &JsValue) -> Option<f64> {
        value.as_f64()
    }

    fn string(value: &JsValue) -> Option<String> {
        value.as_string()
    }

    fn boolean(value: &JsValue) -> Option<bool> {
        value.as_bool()
    }

    fn decode<T: serde::de::DeserializeOwned>(value: JsValue) -> HostResult<T> {
        serde_wasm_bindgen::from_value(value)
            .map_err(|error| HostError::Argument(error.to_string()))
    }
}

fn options(value: JsValue) -> JsOptions {
    JsOptions(value)
}

/// A JSON value from JavaScript (claims), `None` when `undefined`.
fn json(value: JsValue) -> Result<Option<serde_json::Value>, JsValue> {
    if value.is_undefined() {
        return Ok(None);
    }
    serde_wasm_bindgen::from_value(value)
        .map(Some)
        .map_err(to_js_error)
}

fn json_or_null(value: JsValue) -> Result<serde_json::Value, JsValue> {
    Ok(json(value)?.unwrap_or(serde_json::Value::Null))
}

/// The error a binding call fails with for an engine feature the SQLite
/// engine does not have yet.
fn not_supported(what: &str) -> JsValue {
    host_error(binding_host::not_supported(what))
}

// ---- host hooks ------------------------------------------------------------

/// The browser's half of the hooks: callbacks and the durable store.
#[derive(Default)]
struct Host {
    tick_scheduler: RefCell<Option<js_sys::Function>>,
    /// Told of a write rejected at apply time that no wait claimed.
    mutation_errors: RefCell<Option<js_sys::Function>>,
    #[cfg(target_arch = "wasm32")]
    store: RefCell<Option<browser::Store>>,
}

impl Host {
    /// Resolve once every commit so far is in the browser store's backing
    /// storage (a no-op without one).
    async fn flush(&self) -> Result<(), JsValue> {
        #[cfg(target_arch = "wasm32")]
        {
            let store = self.store.borrow().clone();
            if let Some(store) = store {
                store.flush().await?;
            }
        }
        Ok(())
    }
}

/// A bound database (or one of its schema views) and the browser hooks.
#[derive(Clone)]
struct WasmDbInner {
    binding: Binding,
    host: Rc<Host>,
}

impl WasmDbInner {
    fn new(binding: Binding) -> Self {
        let host = Rc::new(Host::default());
        let scheduler = Rc::clone(&host);
        *binding.hooks.schedule_tick.borrow_mut() = Some(Box::new(move || {
            let scheduler = scheduler.tick_scheduler.borrow().clone();
            if let Some(scheduler) = scheduler {
                let _ = scheduler.call1(&JsValue::NULL, &JsValue::from_str("immediate"));
            }
        }));
        let reporter = Rc::clone(&host);
        *binding.hooks.rejected.borrow_mut() =
            Some(Box::new(move |tx_id, error, serializable, claimed| {
                // The event is handed over on the next task; a listener
                // registered now takes it.
                let listening = reporter.mutation_errors.borrow().is_some();
                if listening {
                    report_later(
                        &reporter,
                        &tx_id.to_string(),
                        error,
                        serializable,
                        true,
                        claimed,
                    );
                }
                listening
            }));
        Self { binding, host }
    }

    fn write(&self, write: HostResult<Write>) -> Result<WasmWrite, JsValue> {
        let write = write.map_err(host_error)?;
        let claimed = Rc::new(std::cell::Cell::new(false));
        if let Some(error) = &write.rejected {
            self.report_unclaimed(&write, error, Rc::clone(&claimed));
        }
        self.binding.hooks.track(&write, &claimed);
        Ok(WasmWrite {
            inner: Some(self.clone()),
            write,
            claimed,
        })
    }

    /// Tell the mutation-error callback of a rejected write unless a wait
    /// claims it first: the caller gets until the current task ends (its
    /// promise chain included) to call `wait`.
    fn report_unclaimed(&self, write: &Write, error: &Error, claimed: Rc<std::cell::Cell<bool>>) {
        report_later(
            &self.host,
            &write.tx_id.to_string(),
            error,
            write.serializable,
            false,
            Some(claimed),
        );
    }
}

/// Tell the mutation-error callback of transaction `tx`'s rejection (by
/// the authority, if `upstream`) on a later task, unless a wait set
/// `claimed` by then.
fn report_later(
    host: &Rc<Host>,
    tx: &str,
    error: &Error,
    serializable: bool,
    upstream: bool,
    claimed: Option<Rc<std::cell::Cell<bool>>>,
) {
    let host = Rc::clone(host);
    let event = mutation_error_event(tx, error, serializable, upstream);
    let report = Closure::once_into_js(move || {
        if claimed.is_some_and(|claimed| claimed.get()) {
            return;
        }
        let callback = host.mutation_errors.borrow().clone();
        if let (Some(callback), Ok(event)) = (callback, event) {
            let _ = callback.call1(&JsValue::NULL, &event);
        }
    });
    let set_timeout = js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("setTimeout"))
        .ok()
        .and_then(|f| f.dyn_into::<js_sys::Function>().ok());
    if let Some(set_timeout) = set_timeout {
        let _ = set_timeout.call2(&JsValue::NULL, &report, &JsValue::from_f64(0.0));
    }
}

/// A `MutationErrorEvent` for transaction `tx` rejected with `error` (by
/// the authority, if `upstream`).
fn mutation_error_event(
    tx: &str,
    error: &Error,
    serializable: bool,
    upstream: bool,
) -> Result<JsValue, JsValue> {
    let (code, reason) = binding_host::mutation_error_parts(error, upstream);
    let rejected = js_sys::Object::new();
    set_prop(&rejected, "kind", JsValue::from_str("rejected"))?;
    set_prop(&rejected, "transactionId", JsValue::from_str(tx))?;
    set_prop(&rejected, "code", JsValue::from_str(code))?;
    set_prop(&rejected, "reason", JsValue::from_str(&reason))?;
    let transaction = js_sys::Object::new();
    set_prop(&transaction, "transactionId", JsValue::from_str(tx))?;
    set_prop(
        &transaction,
        "kind",
        JsValue::from_str(binding_host::transaction_kind(serializable)),
    )?;
    set_prop(&transaction, "sealed", JsValue::TRUE)?;
    set_prop(&transaction, "latestSettlement", rejected.into())?;
    let event = js_sys::Object::new();
    set_prop(&event, "code", JsValue::from_str(code))?;
    set_prop(&event, "reason", JsValue::from_str(&reason))?;
    set_prop(&event, "transaction", transaction.into())?;
    Ok(event.into())
}

fn host_promise<T: 'static>(
    future: binding_host::HostFuture<T>,
    to_js: impl FnOnce(T) -> JsValue + 'static,
) -> js_sys::Promise {
    future_to_promise(async move { future.await.map(to_js).map_err(host_error) })
}

// ---- pending operations ------------------------------------------------------

/// The shared upstream link machine (`jazz::link`), which decides when the
/// runtime's WebSocket connects, backs off and reports an outage. The runtime
/// owns the socket and the timer and steps the machine with JSON documents.
#[wasm_bindgen(js_name = UpstreamLink)]
pub struct WasmUpstreamLink {
    link: RefCell<jazz::link::host::HostLink>,
}

#[wasm_bindgen(js_class = UpstreamLink)]
impl WasmUpstreamLink {
    /// `seed` drives the retry jitter.
    #[wasm_bindgen(constructor)]
    pub fn new(seed: f64) -> Self {
        Self {
            link: RefCell::new(jazz::link::host::HostLink::new(seed as u64)),
        }
    }

    /// Apply one input observed at `now_ms`; returns the actions, events and
    /// status as JSON.
    pub fn handle(&self, now_ms: f64, input: &str) -> Result<String, JsValue> {
        self.link
            .borrow_mut()
            .handle_json(now_ms, input)
            .map_err(|error| JsValue::from_str(&error))
    }
}

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
        self.db.binding.cancel_permission_advice(self.id);
    }
}

impl WasmDbInner {
    fn request_permission_advice(
        &self,
        action: HostResult<jazz::protocol::PermissionAdviceAction>,
    ) -> Result<WasmPermissionAdviceRequest, JsValue> {
        let action = action.map_err(host_error)?;
        let (id, advice) = self
            .binding
            .request_permission_advice(action, None, None)
            .map_err(host_error)?;
        Ok(WasmPermissionAdviceRequest {
            promise: host_promise(advice, JsValue::from_str),
            db: self.clone(),
            id,
        })
    }
}

// ---- writes ------------------------------------------------------------------

#[wasm_bindgen]
pub struct WasmWrite {
    write: Write,
    inner: Option<WasmDbInner>,
    /// Whether a wait took over reporting the write's rejection.
    claimed: Rc<std::cell::Cell<bool>>,
}

#[wasm_bindgen]
impl WasmWrite {
    #[wasm_bindgen(getter, js_name = txId)]
    pub fn tx_id(&self) -> String {
        self.write.tx_id.to_string()
    }

    #[wasm_bindgen(getter, js_name = payload)]
    pub fn payload(&self) -> Vec<u8> {
        self.write.payload.clone()
    }

    #[wasm_bindgen(getter, js_name = rowId)]
    pub fn row_id(&self) -> Vec<u8> {
        self.write.row_id.to_bytes()
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
        observe_only: Option<bool>,
    ) -> Result<js_sys::Promise, JsValue> {
        let Some(db) = &self.inner else {
            return Err(JsValue::from_str("write state is unavailable"));
        };
        let fate = db
            .binding
            .wait_for_write(&self.write, &tier)
            .map_err(host_error)?;
        if !observe_only.unwrap_or(false) {
            self.claimed.set(true);
        }
        Ok(host_promise(fate, |()| JsValue::UNDEFINED))
    }

    #[wasm_bindgen]
    pub fn close(&mut self) -> bool {
        self.inner.take().is_some()
    }
}

/// A large value uploaded in chunks, then published as one row mutation.
#[wasm_bindgen(js_name = StreamingMutation)]
pub struct WasmStreamingMutation {
    db: WasmDbInner,
    mutation: RefCell<binding_host::StreamingMutation>,
}

#[wasm_bindgen]
impl WasmStreamingMutation {
    pub fn push(&self, chunk: Vec<u8>) -> js_sys::Promise {
        match self.mutation.borrow_mut().push(&chunk) {
            Ok(()) => js_sys::Promise::resolve(&JsValue::UNDEFINED),
            Err(error) => js_sys::Promise::reject(&host_error(error)),
        }
    }

    pub fn finish(&self) -> js_sys::Promise {
        let write = self.mutation.borrow_mut().finish();
        match self.db.write(write) {
            Ok(write) => js_sys::Promise::resolve(&JsValue::from(write)),
            Err(error) => js_sys::Promise::reject(&error),
        }
    }

    pub fn abort(&self) -> js_sys::Promise {
        match self.mutation.borrow_mut().abort() {
            Ok(aborted) => js_sys::Promise::resolve(&JsValue::from_bool(aborted)),
            Err(error) => js_sys::Promise::reject(&host_error(error)),
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
}

#[wasm_bindgen]
impl WasmDb {
    fn open_inner(&self) -> Result<WasmDbInner, JsValue> {
        self.inner
            .borrow()
            .clone()
            .ok_or_else(|| JsValue::from_str("WasmDb is closed"))
    }

    fn binding(&self) -> Result<Binding, JsValue> {
        Ok(self.open_inner()?.binding)
    }

    /// Exact wire capabilities compiled into this WASM artifact.
    #[wasm_bindgen(js_name = wireFeatures)]
    pub fn wire_features(&self) -> u32 {
        jazz::wire::current_wire_features() as u32
    }

    #[wasm_bindgen(js_name = insert)]
    pub fn insert_with_options(
        &self,
        table: String,
        cells: Vec<u8>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let inner = self.open_inner()?;
        inner.write(
            inner
                .binding
                .insert(&table, &cells, &self::options(options)),
        )
    }

    #[wasm_bindgen(js_name = insertInTransaction)]
    pub fn insert_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        cells: Vec<u8>,
        options: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let row = self
            .binding()?
            .insert_in_transaction(
                &open_transaction_id,
                &table,
                &cells,
                &self::options(options),
            )
            .map_err(host_error)?;
        Ok(row.to_bytes().to_vec())
    }

    #[wasm_bindgen(js_name = update)]
    pub fn update_with_options(
        &self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let inner = self.open_inner()?;
        inner.write(
            inner
                .binding
                .update(&table, &row_id, &patch, &self::options(options)),
        )
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
        self.binding()?
            .update_in_transaction(
                &open_transaction_id,
                &table,
                &row_id,
                &patch,
                &self::options(options),
            )
            .map_err(host_error)
    }

    /// Typed partial-value updates staged inside an open transaction.
    #[wasm_bindgen(js_name = updateLargeValuesInTransaction)]
    pub fn update_large_values_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        mutations: JsValue,
        updated_at_ms: Option<f64>,
    ) -> Result<(), JsValue> {
        let mutations = large_value_updates(mutations)?;
        self.binding()?
            .update_large_values_in_transaction(
                &open_transaction_id,
                &table,
                &row_id,
                &patch,
                mutations,
                updated_at_ms,
            )
            .map_err(host_error)
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
        let mutations = large_value_updates(mutations)?;
        let inner = self.open_inner()?;
        inner.write(inner.binding.update_large_values(
            &table,
            &row_id,
            &patch,
            mutations,
            updated_at_ms,
        ))
    }

    #[wasm_bindgen(js_name = upsert)]
    pub fn upsert_with_options(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let inner = self.open_inner()?;
        inner.write(
            inner
                .binding
                .upsert(&table, &row_id, &cells, &self::options(options)),
        )
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
        self.binding()?
            .upsert_in_transaction(
                &open_transaction_id,
                &table,
                &row_id,
                &cells,
                &self::options(options),
            )
            .map_err(host_error)
    }

    #[wasm_bindgen(js_name = delete)]
    pub fn delete_with_options(
        &self,
        table: String,
        row_id: Vec<u8>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let inner = self.open_inner()?;
        inner.write(
            inner
                .binding
                .delete(&table, &row_id, &self::options(options)),
        )
    }

    #[wasm_bindgen(js_name = deleteInTransaction)]
    pub fn delete_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Vec<u8>,
        options: JsValue,
    ) -> Result<(), JsValue> {
        self.binding()?
            .delete_in_transaction(
                &open_transaction_id,
                &table,
                &row_id,
                &self::options(options),
            )
            .map_err(host_error)
    }

    #[wasm_bindgen(js_name = restore)]
    pub fn restore_with_options(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Option<Vec<u8>>,
        options: JsValue,
    ) -> Result<WasmWrite, JsValue> {
        let inner = self.open_inner()?;
        inner.write(inner.binding.restore(
            &table,
            &row_id,
            cells.as_deref(),
            &self::options(options),
        ))
    }

    #[wasm_bindgen(js_name = restoreInTransaction)]
    pub fn restore_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Vec<u8>,
        cells: Option<Vec<u8>>,
        options: JsValue,
    ) -> Result<(), JsValue> {
        self.binding()?
            .restore_in_transaction(
                &open_transaction_id,
                &table,
                &row_id,
                cells.as_deref(),
                &self::options(options),
            )
            .map_err(host_error)
    }

    /// A fresh root runtime owner over `binding`.
    fn runtime_owner(binding: Binding) -> Self {
        Self {
            inner: Rc::new(RefCell::new(Some(WasmDbInner::new(binding)))),
            owns_runtime: true,
        }
    }

    fn open_memory_as(schema: &[u8], config: &[u8], how: OpenAs<'_>) -> Result<WasmDb, JsValue> {
        initialize_wasm_tracing();
        let (schema, mut config) = binding_host::decode_open(schema, config).map_err(host_error)?;
        let (identity, trusted) =
            binding_host::open_identity(&mut config, how).map_err(host_error)?;
        let db_config = jazz::tools::deployment::published::with_client_migrations(
            binding_host::db_config(schema, &config, identity),
            config.migrations.as_deref(),
        )
        .map_err(|error| host_error(binding_host::HostError::Argument(error)))?;
        let db = block_on(binding_host::open_memory(db_config)).map_err(to_js_error)?;
        Ok(Self::runtime_owner(Binding::new(db, identity, trusted)))
    }

    #[wasm_bindgen(js_name = openMemory)]
    pub fn open_memory(schema: Vec<u8>, config: Vec<u8>) -> Result<WasmDb, JsValue> {
        Self::open_memory_as(&schema, &config, OpenAs::Untrusted)
    }

    /// Open a deliberate backend runtime. This remains a separate ABI from
    /// `openMemory`: the public raw-open configuration can never select the
    /// privileged system author.
    #[wasm_bindgen(js_name = openMemoryAsBackend)]
    pub fn open_memory_as_backend(schema: Vec<u8>, config: Vec<u8>) -> Result<WasmDb, JsValue> {
        Self::open_memory_as(&schema, &config, OpenAs::Backend)
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
        Self::open_memory_as(
            &schema,
            &config,
            OpenAs::SelfSigned {
                token: &token,
                app_id: &app_id,
                claimed_author: &claimed_author,
            },
        )
    }

    /// Open the durable store a browser worker owns. `storage` selects the
    /// SQLite VFS: `{ backend: "indexeddb" | "opfs", path?: string }`
    /// (IndexedDB when omitted; see [`browser`]). The default file is named
    /// after `database`, the physical database name the worker admitted.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = openBrowser)]
    pub async fn open_browser(
        storage: JsValue,
        schema: Vec<u8>,
        config: Vec<u8>,
        database: String,
    ) -> Result<WasmDb, JsValue> {
        Self::open_browser_as(storage, schema, config, database, OpenAs::Untrusted).await
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
        database: String,
    ) -> Result<WasmDb, JsValue> {
        let how = OpenAs::SelfSigned {
            token: &token,
            app_id: &app_id,
            claimed_author: &claimed_author,
        };
        Self::open_browser_as(storage, schema, config, database, how).await
    }

    #[cfg(target_arch = "wasm32")]
    async fn open_browser_as(
        storage: JsValue,
        schema: Vec<u8>,
        config: Vec<u8>,
        database: String,
        how: OpenAs<'_>,
    ) -> Result<WasmDb, JsValue> {
        initialize_wasm_tracing();
        let (schema, mut config) =
            binding_host::decode_open(&schema, &config).map_err(host_error)?;
        let (identity, trusted) =
            binding_host::open_identity(&mut config, how).map_err(host_error)?;
        // Every SQLite store keeps full history, so `history_complete`
        // needs nothing extra.
        let spec = browser::StoreSpec::from_js(&storage, &database)?;
        let db_config = jazz::tools::deployment::published::with_client_migrations(
            binding_host::db_config(schema, &config, identity),
            config.migrations.as_deref(),
        )
        .map_err(|error| host_error(binding_host::HostError::Argument(error)))?;
        let (db, store) = browser::open(&spec, db_config).await?;
        let owner = Self::runtime_owner(Binding::new(db, identity, trusted));
        *owner.open_inner()?.host.store.borrow_mut() = Some(store);
        Ok(owner)
    }

    /// Delete a durable browser store (`{ backend, path }`, as for
    /// `openBrowser`). No database may have it open.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = destroyBrowserStorage)]
    pub async fn destroy_browser_storage(
        storage: JsValue,
        database: String,
    ) -> Result<(), JsValue> {
        let spec = browser::StoreSpec::from_js(&storage, &database)?;
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
            inner.host.flush().await?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Register a typed schema view backed by this same runtime owner, of
    /// an app shipping `migrations` (its bundle as JSON).
    #[wasm_bindgen(js_name = registerSchema)]
    pub fn register_schema(
        &self,
        schema: Vec<u8>,
        migrations: Option<Vec<u8>>,
    ) -> Result<WasmDb, JsValue> {
        let inner = self.open_inner()?;
        let migrations =
            jazz::tools::deployment::published::lowered_client_migrations(migrations.as_deref())
                .map_err(|error| host_error(binding_host::HostError::Argument(error)))?;
        let binding = inner
            .binding
            .register_schema_shipping(&schema, migrations)
            .map_err(host_error)?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(WasmDbInner { binding, ..inner }))),
            owns_runtime: false,
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
        self.binding()?
            .begin_transaction(
                &open_transaction_id,
                &kind,
                author.as_deref(),
                attribution.as_deref(),
            )
            .map_err(host_error)
    }

    /// Commit an owner-wide transaction by id.
    #[wasm_bindgen(js_name = commitTransaction)]
    pub fn commit_transaction(
        &self,
        open_transaction_id: String,
        kind: Option<String>,
    ) -> Result<WasmWrite, JsValue> {
        let inner = self.open_inner()?;
        inner.write(
            inner
                .binding
                .commit_transaction(&open_transaction_id, kind.as_deref()),
        )
    }

    /// Roll back an owner-wide open transaction by id.
    #[wasm_bindgen(js_name = rollbackTransaction)]
    pub fn rollback_transaction(&self, open_transaction_id: String) -> Result<(), JsValue> {
        self.binding()?
            .rollback_transaction(&open_transaction_id)
            .map_err(host_error)
    }

    /// Exact local state for the write-merge bridge: the row as stored,
    /// deleted or not. Write authorization remains at the mutation boundary;
    /// this is not a query.
    #[wasm_bindgen(js_name = localCurrentRow)]
    pub fn local_current_row(&self, table: String, row_id: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        self.binding()?
            .local_current_row(&table, &row_id)
            .map_err(host_error)
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
        let read = self
            .binding()?
            .all(
                query,
                &options(opts),
                open_transaction_id.as_deref(),
                author.as_deref(),
                json(claims)?,
            )
            .map_err(host_error)?;
        wasm_read_or_pending(Box::pin(async move { read.await.map_err(host_error) }))
    }

    /// Bind correlation claims to this already-open client's own identity.
    /// This does not grant a different author or enable serving reads.
    #[wasm_bindgen(js_name = setSessionClaims)]
    pub fn set_session_claims(&self, claims: JsValue) -> Result<JsValue, JsValue> {
        self.binding()?
            .set_session_claims(json_or_null(claims)?)
            .map_err(host_error)?;
        Ok(JsValue::UNDEFINED)
    }

    #[wasm_bindgen(js_name = setIdentityClaims)]
    pub fn set_identity_claims(&self, author: Vec<u8>, claims: JsValue) -> Result<(), JsValue> {
        self.binding()?
            .set_identity_claims(&author, json_or_null(claims)?)
            .map_err(host_error)
    }

    #[wasm_bindgen(js_name = subscribe)]
    pub fn subscribe(
        &self,
        query: Vec<u8>,
        opts: JsValue,
        author: Option<Vec<u8>>,
        claims: JsValue,
    ) -> Result<JsValue, JsValue> {
        let inner = self.open_inner()?;
        let stream = inner
            .binding
            .subscribe(&query, &options(opts), author.as_deref(), json(claims)?)
            .map_err(host_error)?;
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
            // Refusals kept while nobody listened reach this callback now.
            inner.binding.redeliver_rejections();
        }
    }

    /// A replica's own policy evidence may be incomplete, so it never
    /// decides locally; ask the authority with `requestInsertPermissionAdvice`.
    #[wasm_bindgen(js_name = canInsert)]
    pub fn can_insert(&self, _table: String, _cells: Vec<u8>) -> Result<String, JsValue> {
        self.open_inner()?;
        Ok(binding_host::advice_str(jazz::protocol::PermissionAdvice::Unknown).to_owned())
    }

    #[wasm_bindgen(js_name = requestInsertPermissionAdvice)]
    pub fn request_insert_permission_advice(
        &self,
        table: String,
        cells: Vec<u8>,
    ) -> Result<WasmPermissionAdviceRequest, JsValue> {
        self.open_inner()?
            .request_permission_advice(binding_host::advice::insert(table, &cells))
    }

    #[wasm_bindgen(js_name = requestReadPermissionAdvice)]
    pub fn request_read_permission_advice(
        &self,
        table: String,
        row_id: Vec<u8>,
    ) -> Result<WasmPermissionAdviceRequest, JsValue> {
        self.open_inner()?
            .request_permission_advice(binding_host::advice::read(table, &row_id))
    }

    #[wasm_bindgen(js_name = requestUpdatePermissionAdvice)]
    pub fn request_update_permission_advice(
        &self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
    ) -> Result<WasmPermissionAdviceRequest, JsValue> {
        self.open_inner()?
            .request_permission_advice(binding_host::advice::update(table, &row_id, &patch))
    }

    #[wasm_bindgen(js_name = requestDeletePermissionAdvice)]
    pub fn request_delete_permission_advice(
        &self,
        table: String,
        row_id: Vec<u8>,
    ) -> Result<WasmPermissionAdviceRequest, JsValue> {
        self.open_inner()?
            .request_permission_advice(binding_host::advice::delete(table, &row_id))
    }

    #[wasm_bindgen(js_name = captureSnapshot)]
    pub fn capture_snapshot(&self) -> Result<JsValue, JsValue> {
        let capture = self.binding()?.capture_snapshot();
        wasm_read_or_pending(Box::pin(async move { capture.await.map_err(host_error) }))
    }

    /// Resolve once every local write so far reached `tier`. Settling
    /// `cancelled` stops a wait that is still pending and rejects it, so a
    /// caller that gives up does not leave the core waiter parked.
    #[wasm_bindgen(js_name = waitForPendingWrites)]
    pub fn wait_for_pending_writes(
        &self,
        tier: String,
        cancelled: Option<js_sys::Promise>,
    ) -> js_sys::Promise {
        let inner = match self.open_inner() {
            Ok(inner) => inner,
            Err(error) => return js_sys::Promise::reject(&error),
        };
        future_to_promise(async move {
            let wait = async move {
                match durability_tier_from_str(&tier)? {
                    DurabilityTier::None => {}
                    DurabilityTier::Local => inner.host.flush().await?,
                    DurabilityTier::Global => inner
                        .binding
                        .wait_for_pending_writes_global()
                        .await
                        .map_err(host_error)?,
                }
                Ok(JsValue::UNDEFINED)
            };
            let Some(cancelled) = cancelled else {
                return wait.await;
            };
            let cancelled = wasm_bindgen_futures::JsFuture::from(cancelled);
            match futures_util::future::select(Box::pin(wait), cancelled).await {
                futures_util::future::Either::Left((settled, _)) => settled,
                futures_util::future::Either::Right(_) => {
                    Err(JsValue::from_str("waitForPendingWrites was cancelled"))
                }
            }
        })
    }

    #[wasm_bindgen(js_name = tick)]
    pub fn tick(&self) -> js_sys::Promise {
        let binding = match self.binding() {
            Ok(binding) => binding,
            Err(error) => return js_sys::Promise::reject(&error),
        };
        future_to_promise(async move {
            binding.tick().await.map_err(host_error)?;
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
        self.binding()?
            .set_large_value_staging_policy(incoming_bytes_per_window, window_ms, max_age_ms)
            .map_err(host_error)
    }

    /// Run one idempotent expiry pass; browser hosts normally call this from a timer.
    #[wasm_bindgen(js_name = evictExpiredStagedLargeValues)]
    pub fn evict_expired_staged_large_values(&self) -> js_sys::Promise {
        let binding = match self.binding() {
            Ok(binding) => binding,
            Err(error) => return js_sys::Promise::reject(&error),
        };
        future_to_promise(async move {
            let evicted = binding
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
        let head = present(head)
            .map(|value| serde_wasm_bindgen::from_value(value).map_err(to_js_error))
            .transpose()?;
        let base = present(base)
            .map(|value| serde_wasm_bindgen::from_value(value).map_err(to_js_error))
            .transpose()?;
        let inner = self.open_inner()?;
        let mutation = inner
            .binding
            .begin_streaming_mutation(
                table,
                &row_id,
                &cells,
                column,
                mutation.as_deref(),
                author.as_deref(),
                attribution.as_deref(),
                updated_at_ms,
                head,
                base,
            )
            .map_err(host_error)?;
        Ok(WasmStreamingMutation {
            db: inner,
            mutation: RefCell::new(mutation),
        })
    }

    /// How many local commits still wait for the server's verdict.
    #[wasm_bindgen(js_name = pendingUploadCount)]
    pub fn pending_upload_count(&self) -> Result<u32, JsValue> {
        Ok(self.binding()?.pending_upload_count() as u32)
    }

    /// The optimistic in-memory side of a browser client/worker pair: its
    /// subscriptions open with the worker's answer.
    #[wasm_bindgen(js_name = setNonDurableClient)]
    pub fn set_non_durable_client(&self) -> Result<(), JsValue> {
        self.binding()?.set_non_durable_client();
        Ok(())
    }

    /// Report what the host knows about the path to the authoritative server
    /// (`"none" | "attempting" | "live" | "failed"`; the TypeScript names
    /// `"connecting" | "connected" | "unavailable"` are accepted as aliases).
    #[wasm_bindgen(js_name = setRemoteLinkHint)]
    pub fn set_remote_link_hint(&self, state: String) -> Result<(), JsValue> {
        self.binding()?
            .set_remote_link_hint(&state)
            .map_err(host_error)
    }

    /// A fresh upstream link machine for this runtime's server connection;
    /// `seed` drives its retry jitter.
    #[wasm_bindgen(js_name = createUpstreamLink)]
    pub fn create_upstream_link(&self, seed: f64) -> WasmUpstreamLink {
        WasmUpstreamLink::new(seed)
    }

    /// The slow-query report due now: one line per slow query shape,
    /// naming `app`: the first at once, then at most every 10 seconds. The caller
    /// writes each line with `console.warn`.
    #[wasm_bindgen(js_name = takeSlowQueryReport)]
    pub fn take_slow_query_report(&self, app: Option<String>) -> Result<Vec<String>, JsValue> {
        Ok(self.binding()?.take_slow_query_report(app.as_deref()))
    }

    /// Report query runs of at least `ms` milliseconds (default 50); 0
    /// turns the slow-query log off.
    #[wasm_bindgen(js_name = setSlowQueryThresholdMs)]
    pub fn set_slow_query_threshold_ms(&self, ms: f64) -> Result<(), JsValue> {
        self.binding()?
            .set_slow_query_threshold_ms(ms)
            .map_err(host_error)
    }

    /// Connect to the server over byte frames the host carries: each frame
    /// is one postcard `SyncMessage`.
    #[wasm_bindgen(js_name = connectUpstream)]
    pub fn connect_upstream(&self) -> Result<js_sys::Promise, JsValue> {
        let link = self.binding()?.connect_upstream();
        Ok(js_sys::Promise::resolve(&JsValue::from(
            WasmTransport::new(link),
        )))
    }

    /// Connect after the carrier accepted the server Hello. Negotiated
    /// features must be compiled in; the rest of the session context
    /// (authority endpoints, epochs) has no counterpart on SQLite's sync, so
    /// this is otherwise [`Self::connect_upstream`].
    #[wasm_bindgen(js_name = connectUpstreamWithSession)]
    pub fn connect_upstream_with_session(
        &self,
        _protocol_version: u16,
        features: u32,
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
        let unsupported = u64::from(features) & !jazz::wire::current_wire_features();
        if unsupported != 0 {
            return Err(JsValue::from_str(&format!(
                "server negotiated wire features {features:#x}, but this WASM binding was not compiled with {unsupported:#x}"
            )));
        }
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
        self.accept_admitted(identity, claims)
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
        let identity = binding_host::verify_self_signed(&token, &app_id, &claimed_author)
            .map_err(host_error)?;
        self.accept_admitted(identity, claims)
    }

    fn accept_admitted(
        &self,
        identity: AuthorSubject,
        claims: JsValue,
    ) -> Result<js_sys::Promise, JsValue> {
        let claims = binding_host::claims(identity, json_or_null(claims)?).map_err(host_error)?;
        let link = self
            .binding()?
            .accept_subscriber(identity, claims)
            .map_err(host_error)?;
        Ok(js_sys::Promise::resolve(&JsValue::from(
            WasmTransport::new(link),
        )))
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
                inner.binding.close().await.map_err(host_error)?;
                inner.host.flush().await?;
                #[cfg(target_arch = "wasm32")]
                inner.host.store.borrow_mut().take();
            }
            Ok(JsValue::TRUE)
        })
    }
}

// ---- transports ------------------------------------------------------------------

/// One sync link, carried by the host as byte frames. Frames are applied
/// and produced by the database's tick.
#[wasm_bindgen]
pub struct WasmTransport {
    link: binding_host::Link,
}

impl WasmTransport {
    fn new(link: binding_host::Link) -> Self {
        Self { link }
    }
}

#[wasm_bindgen]
impl WasmTransport {
    #[wasm_bindgen(js_name = updateAuthenticatedClaims)]
    pub fn update_authenticated_claims(&self, claims: JsValue) -> Result<js_sys::Promise, JsValue> {
        self.link
            .update_claims(json_or_null(claims)?)
            .map_err(host_error)?;
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
        self.link.receive([frame]);
    }

    #[wasm_bindgen(js_name = sendWireFrames)]
    pub fn send_wire_frames(&self, frames: js_sys::Array) {
        self.link.receive(
            frames
                .iter()
                .map(|frame| js_sys::Uint8Array::new(&frame).to_vec()),
        );
    }

    #[wasm_bindgen(js_name = recvWireFrames)]
    pub fn recv_wire_frames(&self) -> js_sys::Array {
        let frames = js_sys::Array::new();
        for frame in self.link.take_outbound() {
            frames.push(&js_sys::Uint8Array::from(frame.as_slice()).into());
        }
        frames
    }

    #[wasm_bindgen(js_name = setOutboundScheduler)]
    pub fn set_outbound_scheduler(&self, callback: js_sys::Function) {
        self.link.set_outbound_ready(Some(Box::new(move || {
            let _ = callback.call0(&JsValue::NULL);
        })));
    }

    #[wasm_bindgen(js_name = clearOutboundScheduler)]
    pub fn clear_outbound_scheduler(&self) {
        self.link.set_outbound_ready(None);
    }

    /// Tick the database this link belongs to: apply received frames, send
    /// what is due. Resolves with the number of frames still to send.
    #[wasm_bindgen(js_name = tick)]
    pub fn tick(&self) -> js_sys::Promise {
        let binding = self.link.binding().clone();
        let pending = self.link.outbound_counter();
        future_to_promise(async move {
            binding.tick().await.map_err(host_error)?;
            Ok(JsValue::from_f64(pending() as f64))
        })
    }

    #[wasm_bindgen(js_name = close)]
    pub fn close(&mut self) -> bool {
        self.link.close()
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

fn large_value_updates(mutations: JsValue) -> Result<Vec<LargeValueUpdate>, JsValue> {
    serde_wasm_bindgen::from_value(mutations).map_err(|error| {
        JsValue::from_str(&format!("invalid partial-value update descriptor: {error}"))
    })
}

fn present(value: Option<JsValue>) -> Option<JsValue> {
    value.filter(|value| !value.is_null() && !value.is_undefined())
}

fn author_id_from_bytes(bytes: &[u8]) -> Result<AuthorSubject, JsValue> {
    jazz::binding_args::author_subject(bytes).map_err(|error| JsValue::from_str(&error))
}

fn durability_tier_from_str(tier: &str) -> Result<DurabilityTier, JsValue> {
    jazz::binding_args::durability_tier(tier).map_err(|error| JsValue::from_str(&error))
}

// ---- subscriptions -----------------------------------------------------------------

fn subscription_stream_to_js(
    db: WasmDbInner,
    stream: SubscriptionStream,
) -> Result<JsValue, JsValue> {
    let source = futures_util::stream::unfold(Some((db, stream)), |state| async move {
        let (db, mut stream) = state?;
        let event = db.binding.next_event(&mut stream).await;
        let closed = matches!(event, SubscriptionEvent::Closed);
        let chunk = binding_host::event_chunk(event)
            .map_err(host_error)
            .and_then(event_chunk_to_js);
        Some((chunk, (!closed).then_some((db, stream))))
    });
    readable_stream_from_stream(source)
}

fn event_chunk_to_js(chunk: EventChunk) -> Result<JsValue, JsValue> {
    let object = js_sys::Object::new();
    match chunk {
        EventChunk::Delta {
            delta,
            reset,
            settled,
            tier,
        } => {
            set_prop(&object, "type", JsValue::from_str("delta"))?;
            set_prop(
                &object,
                "delta",
                js_sys::Uint8Array::from(delta.as_slice()).into(),
            )?;
            set_prop(&object, "terminalOperations", js_sys::Array::new().into())?;
            set_prop(&object, "reset", JsValue::from_bool(reset))?;
            set_prop(&object, "settled", JsValue::from_bool(settled))?;
            set_prop(&object, "tier", JsValue::from_str(&tier))?;
        }
        EventChunk::Closed => {
            set_prop(&object, "type", JsValue::from_str("closed"))?;
        }
        EventChunk::Rejected { detail } => {
            let reason = js_sys::Object::new();
            set_prop(&reason, "type", JsValue::from_str("ServerFailure"))?;
            set_prop(&reason, "detail", JsValue::from_str(&detail))?;
            set_prop(&object, "type", JsValue::from_str("rejected"))?;
            set_prop(&object, "reason", reason.into())?;
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
/// unchanged `Display` text, whose `code` is the stable [`ErrorCode::as_str`]
/// string and, when it has one, whose `reason` is the stable `Reason::as_str`
/// name, so callers classify it without matching the message. Every other error keeps its historical bare-string form.
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
    if let Some(reason) = error.reason {
        let _ = js_sys::Reflect::set(
            &js_error,
            &JsValue::from_str("reason"),
            &JsValue::from_str(reason.as_str()),
        );
    }
    js_error.into()
}

/// The value a shared-layer error rejects or throws with: an engine error
/// keeps its stable code, an argument error its historical bare string.
fn host_error(error: HostError) -> JsValue {
    match error {
        HostError::Engine(error) => core_error_to_js(&error),
        HostError::Argument(message) => JsValue::from_str(&message),
    }
}

fn bytes_to_js(bytes: Vec<u8>) -> Result<JsValue, JsValue> {
    Ok(js_sys::Uint8Array::from(bytes.as_slice()).into())
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
