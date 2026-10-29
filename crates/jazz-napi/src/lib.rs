//! jazz-napi — Native Node.js bindings for the Jazz database (`jazz-db`).
//!
//! Provides Node.js bindings for the Jazz database, server helpers, and
//! local-first identity utilities.
//!
//! # Architecture
//!
//! - `NapiDb` exposes the database over the encoded-row boundary the
//!   TypeScript client packages share with the WASM binding. Both delegate to
//!   [`jazz::binding_host`]; this crate only adapts it to Node: options
//!   objects, pollable pending reads, write promises and the tick callback.
//!   Unsupported features fail with an explicit "not supported yet" error.
//! - `JazzServer` exposes the Rust server process used by integration tests
//!   and Node deployments.
//! - Local-first JWT helpers stay here as package-level native utilities.
//!
//! # Thread affinity
//!
//! A database is `Rc`/thread-affine and NAPI's worker-pool promises need
//! `Send`, so nothing here runs off the JavaScript thread. `jazz-db` runs
//! every operation to completion synchronously; only reads and write fates
//! that need the server wait, and they complete on a later tick. Reads hand
//! JavaScript a pollable object the host drives after transport progress;
//! write waits are promises this crate settles after every call that can make
//! progress (see [`settle_promises`]).
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

mod e2ee_crypto;

#[cfg(feature = "rn-test-bridge")]
mod rn_test_bridge;

#[global_allocator]
static GLOBAL: mimalloc_safe::MiMalloc = mimalloc_safe::MiMalloc;

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures::FutureExt;
use jazz::binding_host::{self, Binding, EventChunk, HostError, HostFuture, HostResult, OpenAs};
use jazz::db::{Error as CoreError, FileOptions, LargeValueUpdate, SubscriptionEvent as CoreEvent};
use jazz::db::{SubscriptionStream, block_on};
use jazz::identity;
use jazz::tools::AppId;
use jazz::tx::DurabilityTier;
use jazz_server::{
    AuthConfig, JazzServer as CoreJazzServer, ServerBuilder, ServerDataDir, StorageBackend,
    TEST_JWT_AUDIENCE, TEST_JWT_ISSUER, TestJwtIssuer as JazzTestJwtIssuer, TestJwtOptions,
};
use napi::bindgen_prelude::*;
use napi::sys;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use serde::Deserialize;

/// Any JSON-compatible value crossing the native JavaScript boundary.
///
/// Keep this alias exported through napi-rs rather than relying on its Rust
/// import name: exported methods use `JsonValue` throughout their generated
/// declarations, so the package must define that name for TypeScript
/// consumers.
#[napi]
pub type JsonValue = serde_json::Value;

/// Validate a schema and its embedded permissions without opening a database.
#[napi(js_name = "validateSchema")]
pub fn validate_schema(schema: Uint8Array) -> js::Result<()> {
    binding_host::decode_schema(&schema).map_err(host_error)?;
    Ok(())
}

/// Exact build/ABI fingerprint for the generated native artifact.
#[napi]
pub fn native_artifact_fingerprint() -> String {
    option_env!("JAZZ_NATIVE_ARTIFACT_FINGERPRINT")
        .unwrap_or("missing-build-fingerprint")
        .to_owned()
}

/// Test-only bridge for executing the Rust-owned v1 binding corpus through the
/// generated NAPI artifact. It intentionally returns the frozen corpus rather
/// than a TypeScript reimplementation of its byte layouts.
#[napi(js_name = "__testBindingCodecGoldenFixture", skip_typescript)]
pub fn test_binding_codec_golden_fixture() -> String {
    jazz::binding_codec::BINDING_CODEC_GOLDEN_FIXTURE.to_owned()
}

/// Test-only: read one pinned wire corpus batch through the generated NAPI
/// artifact, as `reader` (`"server-opening"`, `"server"` or `"client"`)
/// would, with the production frame and sync message decoders. Not a host
/// wire API.
#[napi(js_name = "__testValidateWireFrameBatchCorpus", skip_typescript)]
pub fn test_validate_wire_frame_batch_corpus(
    batch: Buffer,
    reader: String,
    negotiated_features: String,
) -> Result<()> {
    let negotiated_features = negotiated_features
        .parse()
        .map_err(|_| napi::Error::from_reason("test wire corpus features must be a u64 decimal"))?;
    let reader = corpus_reader(&reader).map_err(napi::Error::from_reason)?;
    jazz::db::validate_frame_batch_for_corpus(&batch, reader, negotiated_features)
        .map_err(napi::Error::from_reason)
}

fn corpus_reader(name: &str) -> std::result::Result<jazz::db::CorpusReader, String> {
    match name {
        "server-opening" => Ok(jazz::db::CorpusReader::ServerOpening),
        "server" => Ok(jazz::db::CorpusReader::Server),
        "client" => Ok(jazz::db::CorpusReader::Client),
        other => Err(format!("unknown wire corpus reader {other:?}")),
    }
}

/// Test-only: every core `Reason` name, so the TypeScript reason list is
/// checked against the engine's rather than copied from it.
#[napi(js_name = "__testCoreErrorReasonNames", skip_typescript)]
pub fn test_core_error_reason_names() -> Vec<String> {
    jazz::db::Reason::ALL
        .into_iter()
        .map(|reason| reason.as_str().to_owned())
        .collect()
}

/// Test-only feature inventory paired with [`test_validate_wire_frame_batch_corpus`].
#[napi(js_name = "__testWireFrameCorpusFeatures", skip_typescript)]
pub fn test_wire_frame_corpus_features() -> String {
    jazz::wire::current_wire_features().to_string()
}

// ---- options -------------------------------------------------------------------------

#[napi(object)]
pub struct InsertOptions {
    pub row_id: Option<Uint8Array>,
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub branch: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
}

#[napi(object)]
pub struct UpdateOptions {
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub head: Option<JsonValue>,
    pub base: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
}

#[napi(object)]
pub struct UpsertOptions {
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub head: Option<JsonValue>,
    pub base: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
}

#[napi(object)]
pub struct DeleteOptions {
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub head: Option<JsonValue>,
    pub base: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
}

#[napi(object)]
pub struct RestoreOptions {
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub branch: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
}

/// Write options as the JSON object [`binding_host`] reads: byte arrays
/// become arrays of numbers, absent fields stay absent.
#[derive(Default)]
struct JsonOptions(serde_json::Map<String, serde_json::Value>);

impl JsonOptions {
    fn bytes(mut self, name: &str, value: Option<Uint8Array>) -> Self {
        if let Some(value) = value {
            self.0.insert(name.to_owned(), value.to_vec().into());
        }
        self
    }

    fn json(mut self, name: &str, value: Option<JsonValue>) -> Self {
        if let Some(value) = value {
            self.0.insert(name.to_owned(), value);
        }
        self
    }

    fn number(mut self, name: &str, value: Option<f64>) -> Self {
        if let Some(value) = value {
            self.0.insert(name.to_owned(), value.into());
        }
        self
    }

    fn done(self) -> serde_json::Value {
        serde_json::Value::Object(self.0)
    }
}

fn insert_options(options: Option<InsertOptions>) -> serde_json::Value {
    let Some(options) = options else {
        return serde_json::Value::Null;
    };
    JsonOptions::default()
        .bytes("rowId", options.row_id)
        .bytes("author", options.author)
        .bytes("attribution", options.attribution)
        .json("branch", options.branch)
        .number("updatedAtMs", options.updated_at_ms)
        .done()
}

fn update_options(options: Option<UpdateOptions>) -> serde_json::Value {
    let Some(options) = options else {
        return serde_json::Value::Null;
    };
    JsonOptions::default()
        .bytes("author", options.author)
        .bytes("attribution", options.attribution)
        .json("head", options.head)
        .json("base", options.base)
        .number("updatedAtMs", options.updated_at_ms)
        .done()
}

fn delete_options(options: Option<DeleteOptions>) -> serde_json::Value {
    update_options(options.map(|options| UpdateOptions {
        author: options.author,
        attribution: options.attribution,
        head: options.head,
        base: options.base,
        updated_at_ms: options.updated_at_ms,
    }))
}

fn restore_options(options: Option<RestoreOptions>) -> serde_json::Value {
    insert_options(options.map(|options| InsertOptions {
        row_id: None,
        author: options.author,
        attribution: options.attribution,
        branch: options.branch,
        updated_at_ms: options.updated_at_ms,
    }))
}

/// Upsert options from the raw JavaScript object. `#[napi(object)]` reads an
/// absent, `undefined` and `null` field alike, but the removed `branch` field
/// must be rejected by its presence, so an untyped caller cannot silently
/// write to the root. It is checked with `has`, never read.
fn upsert_options(options: Option<Unknown<'_>>) -> napi::Result<serde_json::Value> {
    let Some(options) = options else {
        return Ok(serde_json::Value::Null);
    };
    match options.get_type()? {
        ValueType::Undefined | ValueType::Null => return Ok(serde_json::Value::Null),
        ValueType::Object => {}
        _ => return Err(napi::Error::from_reason("upsert options must be an object")),
    }
    let object = Object::from_raw(options.value().env, options.value().value);
    let mut json = JsonOptions::default()
        .bytes("author", object.get_named_property_unchecked("author")?)
        .bytes(
            "attribution",
            object.get_named_property_unchecked("attribution")?,
        )
        .json("head", object.get_named_property_unchecked("head")?)
        .json("base", object.get_named_property_unchecked("base")?)
        .number(
            "updatedAtMs",
            object.get_named_property_unchecked("updatedAtMs")?,
        );
    if object.has_named_property("branch")? {
        json.0.insert("branch".to_owned(), serde_json::Value::Null);
    }
    Ok(json.done())
}

fn large_value_updates(mutations: JsonValue) -> napi::Result<Vec<LargeValueUpdate>> {
    serde_json::from_value(mutations).map_err(|error| {
        napi::Error::from_reason(format!("invalid partial-value update descriptor: {error}"))
    })
}

/// A structured option (a branch selector or base), `None` when absent.
fn structured<T: serde::de::DeserializeOwned>(
    value: Option<JsonValue>,
    name: &str,
) -> napi::Result<Option<T>> {
    value
        .filter(|value| !value.is_null())
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| napi::Error::from_reason(format!("invalid {name}: {error}")))
}

fn json_or_null(value: Option<JsonValue>) -> serde_json::Value {
    value.unwrap_or(serde_json::Value::Null)
}

// ---- errors --------------------------------------------------------------------------

/// Prefix of the private `cause` that carries a core error code, and after
/// [`CORE_ERROR_REASON_SEPARATOR`] its reason name, from where the core error
/// is converted to the exported method that throws it or the promise it
/// rejects. It never reaches JavaScript: [`BindingError`] strips it.
const CORE_ERROR_CODE_CARRIER: &str = "\u{0}jazz-core-error-code:";
const CORE_ERROR_REASON_SEPARATOR: char = '\u{0}';

/// Convert a core error into a `napi::Error` with its unchanged `Display`
/// text, recording its stable [`jazz::db::ErrorCode::as_str`] code and
/// `Reason::as_str` name. They ride inside the `napi::Error` until an
/// exported method returns it as a [`BindingError`], which throws the code as
/// the JavaScript `error.code`, or a promise rejects with it, which also sets
/// `error.reason` (see [`js_error_value`]).
fn napi_core_error(error: &CoreError) -> napi::Error {
    let mut napi_error = napi::Error::from_reason(error.to_string());
    let mut carrier = format!("{CORE_ERROR_CODE_CARRIER}{}", error.code.as_str());
    if let Some(reason) = error.reason {
        carrier.push(CORE_ERROR_REASON_SEPARATOR);
        carrier.push_str(reason.as_str());
    }
    napi_error.set_cause(napi::Error::from_reason(carrier));
    napi_error
}

/// The core code and reason name `error` carries, if it is a core error.
fn carried_core(error: &napi::Error) -> Option<(&str, Option<&str>)> {
    let carried = error
        .cause
        .as_ref()?
        .reason
        .strip_prefix(CORE_ERROR_CODE_CARRIER)?;
    Some(match carried.split_once(CORE_ERROR_REASON_SEPARATOR) {
        Some((code, reason)) => (code, Some(reason)),
        None => (carried, None),
    })
}

/// The error a shared-layer failure throws: an engine error keeps its stable
/// code, an argument error is a plain `GenericFailure`.
fn host_error(error: HostError) -> napi::Error {
    match error {
        HostError::Engine(error) => napi_core_error(&error),
        HostError::Argument(message) => napi::Error::from_reason(message),
    }
}

fn not_supported(what: &str) -> napi::Error {
    host_error(binding_host::not_supported(what))
}

/// The JavaScript `error.code` of an error thrown by an exported method.
///
/// napi-rs throws an `Error<S>` with `code` set to `S::as_ref()`. A core error
/// uses its stable core code (for example `not_observed`); every other error
/// keeps the napi status name napi-rs has always used (`GenericFailure`).
pub struct JsErrorCode(String);

impl AsRef<str> for JsErrorCode {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// The error type of exported methods: a `napi::Error` whose JavaScript
/// `code` is the core error code when there is one.
pub struct BindingError(napi::Error<JsErrorCode>);

impl From<napi::Error> for BindingError {
    fn from(mut error: napi::Error) -> Self {
        // A thrown error carries only the code: napi-rs creates it from a
        // code and a message, with no way to add properties.
        let carried = carried_core(&error).map(|(code, _)| code.to_owned());
        let (code, cause) = match carried {
            Some(code) => (code, None),
            None => (error.status.as_ref().to_owned(), error.cause.take()),
        };
        let mut coded = napi::Error::new(JsErrorCode(code), std::mem::take(&mut error.reason));
        coded.cause = cause;
        Self(coded)
    }
}

impl From<BindingError> for JsError<JsErrorCode> {
    fn from(error: BindingError) -> Self {
        JsError::from(error.0)
    }
}

impl std::ops::Deref for BindingError {
    type Target = napi::Error<JsErrorCode>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for BindingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// `js::Result` is the return type of exported methods that can throw a core
/// error. napi-derive recognises a return type whose last path segment is
/// `Result`, so the alias keeps that name.
mod js {
    pub type Result<T> = std::result::Result<T, super::BindingError>;
}

fn closed() -> napi::Error {
    napi::Error::from_reason("database is closed")
}

// ---- promises --------------------------------------------------------------------------

/// A JavaScript promise waiting on a binding future.
struct PendingPromise {
    env: sys::napi_env,
    deferred: sys::napi_deferred,
    future: HostFuture<()>,
}

thread_local! {
    /// Promises whose futures were pending when last polled. Each database
    /// is owned by one JavaScript thread, and so are its promises.
    static PENDING_PROMISES: RefCell<Vec<PendingPromise>> = const { RefCell::new(Vec::new()) };
}

/// A promise settled by `future`: at once when it is ready, otherwise by a
/// later [`settle_promises`].
fn host_promise(env: &Env, future: HostFuture<()>) -> napi::Result<PromiseRaw<'static, ()>> {
    let env = env.raw();
    let mut deferred = std::ptr::null_mut();
    let mut promise = std::ptr::null_mut();
    let status = unsafe { sys::napi_create_promise(env, &mut deferred, &mut promise) };
    if status != sys::Status::napi_ok {
        return Err(napi::Error::from_reason(
            "failed to create a write wait promise",
        ));
    }
    let mut pending = PendingPromise {
        env,
        deferred,
        future,
    };
    if !pending.poll() {
        PENDING_PROMISES.with(|promises| promises.borrow_mut().push(pending));
    }
    Ok(PromiseRaw::new(env, promise))
}

impl PendingPromise {
    /// Settle the promise if its future is ready; `false` while pending.
    fn poll(&mut self) -> bool {
        let mut context = Context::from_waker(Waker::noop());
        let result = match self.future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => return false,
        };
        let (env, deferred) = (self.env, self.deferred);
        match result {
            Ok(()) => {
                let mut undefined = std::ptr::null_mut();
                if unsafe { sys::napi_get_undefined(env, &mut undefined) } == sys::Status::napi_ok {
                    unsafe { sys::napi_resolve_deferred(env, deferred, undefined) };
                }
            }
            Err(error) => {
                let rejection = js_error_value(env, &host_error(error));
                unsafe { sys::napi_reject_deferred(env, deferred, rejection) };
            }
        }
        true
    }
}

/// A JavaScript `Error` with `error`'s message, code and, for a core error
/// that has one, its reason name as `error.reason` (see [`BindingError`]).
fn js_error_value(env: sys::napi_env, error: &napi::Error) -> sys::napi_value {
    let carried = carried_core(error);
    let code = carried.map_or_else(|| error.status.as_ref(), |(code, _)| code);
    let string = |text: &str| {
        let mut value = std::ptr::null_mut();
        unsafe {
            sys::napi_create_string_utf8(env, text.as_ptr().cast(), text.len() as isize, &mut value)
        };
        value
    };
    let message = string(&error.reason);
    let mut js_error = std::ptr::null_mut();
    // `napi_create_error` sets the JavaScript `error.code` from its code.
    let status = unsafe { sys::napi_create_error(env, string(code), message, &mut js_error) };
    if status != sys::Status::napi_ok {
        return message;
    }
    if let Some((_, Some(reason))) = carried {
        // Setting a data property on a fresh error object cannot fail.
        unsafe { sys::napi_set_named_property(env, js_error, c"reason".as_ptr(), string(reason)) };
    }
    js_error
}

/// Settle every promise whose wait completed. Waits complete only when a
/// tick, a local write or a claims change made progress, so every call that
/// can do so ends here.
fn settle_promises() {
    let mut pending = PENDING_PROMISES.with(|promises| std::mem::take(&mut *promises.borrow_mut()));
    pending.retain_mut(|promise| !promise.poll());
    PENDING_PROMISES.with(|promises| {
        let mut promises = promises.borrow_mut();
        pending.append(&mut promises);
        *promises = pending;
    });
}

// ---- host hooks ------------------------------------------------------------------

/// Node's half of the hooks: the tick and mutation-error callbacks.
#[derive(Default)]
struct Host {
    tick_scheduler: RefCell<Option<ThreadsafeFunction<String, ()>>>,
    /// A tick is requested and not yet delivered: further requests coalesce.
    tick_requested: Arc<AtomicBool>,
    /// Told of a write rejected at apply time that no wait claimed.
    mutation_errors: RefCell<Option<FunctionRef<JsonValue, ()>>>,
    /// The same callback, told of an upload the authority refused that no
    /// wait claimed (it arrives during a tick, outside any JS call).
    upstream_mutation_errors: RefCell<Option<MutationErrorCallback>>,
}

type MutationErrorCallback =
    ThreadsafeFunction<serde_json::Value, (), serde_json::Value, napi::Status, false, true>;

impl Host {
    fn request_tick(&self) {
        let scheduler = self.tick_scheduler.borrow();
        let Some(scheduler) = scheduler.as_ref() else {
            return;
        };
        if self.tick_requested.swap(true, Ordering::AcqRel) {
            return;
        }
        let status = scheduler.call(
            Ok("immediate".to_owned()),
            ThreadsafeFunctionCallMode::NonBlocking,
        );
        if status != napi::Status::Ok {
            self.tick_requested.store(false, Ordering::Release);
        }
    }
}

/// A bound database (or one of its schema views) and the Node hooks.
#[derive(Clone)]
struct Inner {
    binding: Binding,
    host: Rc<Host>,
}

impl Inner {
    fn new(binding: Binding) -> Self {
        let host = Rc::new(Host::default());
        let scheduler = Rc::clone(&host);
        *binding.hooks.schedule_tick.borrow_mut() =
            Some(Box::new(move || scheduler.request_tick()));
        let reporter = Rc::clone(&host);
        *binding.hooks.rejected.borrow_mut() =
            Some(Box::new(move |tx_id, error, serializable, _| {
                let callbacks = reporter.upstream_mutation_errors.borrow();
                let Some(callback) = callbacks.as_ref() else {
                    return false;
                };
                let event = mutation_error_event(&tx_id.to_string(), error, serializable, true);
                // A call the queue dropped did not reach JS: keep the
                // refusal for the next delivery.
                callback.call(event, ThreadsafeFunctionCallMode::NonBlocking) == napi::Status::Ok
            }));
        Self { binding, host }
    }

    /// A write's receipt. A rejected write is reported to the
    /// mutation-error callback unless a wait claims it first.
    fn write(&self, env: &Env, write: HostResult<binding_host::Write>) -> napi::Result<Write> {
        let write = write.map_err(host_error)?;
        settle_promises();
        let claimed = Rc::new(Cell::new(false));
        if let Some(error) = &write.rejected {
            self.report_unclaimed(env, &write, error, Rc::clone(&claimed))?;
        }
        self.binding.hooks.track(&write, &claimed);
        Ok(Write {
            inner: Some(self.clone()),
            write,
            claimed,
        })
    }

    /// Tell the mutation-error callback of a rejected write unless a wait
    /// claims it first: the caller gets until the current task ends (its
    /// promise chain included) to call `wait`.
    fn report_unclaimed(
        &self,
        env: &Env,
        write: &binding_host::Write,
        error: &CoreError,
        claimed: Rc<Cell<bool>>,
    ) -> napi::Result<()> {
        let host = Rc::clone(&self.host);
        let event =
            mutation_error_event(&write.tx_id.to_string(), error, write.serializable, false);
        let report = env.create_function_from_closure("reportMutationError", move |context| {
            if claimed.get() {
                return Ok(());
            }
            if let Some(callback) = host.mutation_errors.borrow().as_ref() {
                callback.borrow_back(context.env)?.call(event.clone())?;
            }
            Ok(())
        })?;
        let set_timeout: SetTimeout = env
            .get_global()?
            .get_named_property_unchecked("setTimeout")?;
        set_timeout.call(FnArgs {
            data: (report, 0.0),
        })?;
        Ok(())
    }
}

/// `globalThis.setTimeout(handler, delay)`.
type SetTimeout<'a> = Function<'a, FnArgs<(Function<'a, (), ()>, f64)>, Unknown<'a>>;

/// A `MutationErrorEvent` for transaction `tx` rejected with `error` (by
/// the authority, if `upstream`).
fn mutation_error_event(
    tx: &str,
    error: &CoreError,
    serializable: bool,
    upstream: bool,
) -> serde_json::Value {
    let (code, reason) = binding_host::mutation_error_parts(error, upstream);
    serde_json::json!({
        "code": code,
        "reason": reason,
        "transaction": {
            "transactionId": tx,
            "kind": binding_host::transaction_kind(serializable),
            "sealed": true,
            "latestSettlement": {
                "kind": "rejected",
                "transactionId": tx,
                "code": code,
                "reason": reason,
            },
        },
    })
}

// ---- pending operations ------------------------------------------------------------

type ReadFuture = Pin<Box<dyn Future<Output = HostResult<Vec<u8>>>>>;

/// The shared upstream link machine (`jazz::link`), which decides when the
/// runtime's WebSocket connects, backs off and reports an outage. The runtime
/// owns the socket and the timer and steps the machine with JSON documents.
#[napi(js_name = "UpstreamLink")]
pub struct NapiUpstreamLink {
    link: RefCell<jazz::link::host::HostLink>,
}

#[napi]
impl NapiUpstreamLink {
    /// `seed` drives the retry jitter.
    #[napi(constructor)]
    pub fn new(seed: f64) -> Self {
        Self {
            link: RefCell::new(jazz::link::host::HostLink::new(seed as u64)),
        }
    }

    /// Apply one input observed at `now_ms`; returns the actions, events and
    /// status as JSON.
    #[napi]
    pub fn handle(&self, now_ms: f64, input: String) -> napi::Result<String> {
        self.link
            .borrow_mut()
            .handle_json(now_ms, &input)
            .map_err(napi::Error::from_reason)
    }
}

/// A JavaScript-thread-owned read waiting for the server. NAPI promises
/// execute on a Send worker pool, whereas a Jazz database is deliberately
/// `Rc`/thread-affine. The adapter drives this object after its peer
/// transport makes progress instead of blocking Node.
#[napi]
pub struct PendingNativeRead {
    future: RefCell<Option<ReadFuture>>,
}

impl PendingNativeRead {
    fn poll_once(&self) -> napi::Result<Option<Uint8Array>> {
        let Some(mut future) = self.future.borrow_mut().take() else {
            return Err(napi::Error::from_reason(
                "native pending read is already complete",
            ));
        };
        let mut context = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result.map(|bytes| Some(bytes.into())).map_err(host_error),
            Poll::Pending => {
                *self.future.borrow_mut() = Some(future);
                Ok(None)
            }
        }
    }
}

#[napi]
impl PendingNativeRead {
    #[napi]
    pub fn poll(&self) -> js::Result<Option<Uint8Array>> {
        Ok(self.poll_once()?)
    }

    #[napi]
    pub fn cancel(&self) {
        self.future.borrow_mut().take();
    }
}

fn read_or_pending(future: ReadFuture) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
    let pending = PendingNativeRead {
        future: RefCell::new(Some(future)),
    };
    Ok(match pending.poll_once()? {
        Some(bytes) => Either::A(bytes),
        None => Either::B(pending),
    })
}

/// A JavaScript-thread-owned permission preflight waiting for the upstream
/// authority's answer. Like pending reads, it stays on the owning JavaScript
/// thread.
#[napi]
pub struct PendingNativePermissionAdvice {
    future: RefCell<Option<HostFuture<&'static str>>>,
    binding: Binding,
    id: u64,
}

impl PendingNativePermissionAdvice {
    fn poll_once(&self) -> napi::Result<Option<String>> {
        let Some(mut future) = self.future.borrow_mut().take() else {
            return Err(napi::Error::from_reason(
                "native pending permission advice is already complete",
            ));
        };
        let mut context = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result
                .map(|advice| Some(advice.to_owned()))
                .map_err(host_error),
            Poll::Pending => {
                *self.future.borrow_mut() = Some(future);
                Ok(None)
            }
        }
    }
}

#[napi]
impl PendingNativePermissionAdvice {
    #[napi]
    pub fn poll(&self) -> js::Result<Option<String>> {
        Ok(self.poll_once()?)
    }

    /// Stop waiting; a late answer is ignored.
    #[napi]
    pub fn cancel(&self) {
        if self.future.borrow_mut().take().is_some() {
            self.binding.cancel_permission_advice(self.id);
        }
    }
}

// ---- writes ------------------------------------------------------------------------

#[napi(js_name = "Write")]
pub struct Write {
    write: binding_host::Write,
    inner: Option<Inner>,
    /// Whether a wait took over reporting the write's rejection.
    claimed: Rc<Cell<bool>>,
}

#[napi]
impl Write {
    #[napi(getter, js_name = "txId")]
    pub fn tx_id(&self) -> String {
        self.write.tx_id.to_string()
    }

    #[napi(getter)]
    pub fn payload(&self) -> Uint8Array {
        self.write.payload.clone().into()
    }

    #[napi(getter, js_name = "rowId")]
    pub fn row_id(&self) -> Uint8Array {
        self.write.row_id.to_bytes().to_vec().into()
    }

    #[napi(js_name = "writeState")]
    pub fn write_state(&self) -> js::Result<JsonValue> {
        Err(not_supported("write state snapshots").into())
    }

    /// Resolve once the write reached `tier`: at once for `none` and
    /// `local` (the commit is in the store when the write call returns), on
    /// the server's acceptance for `global`, rejecting if it refused.
    #[napi]
    pub fn wait(
        &self,
        env: Env,
        tier: String,
        observe_only: Option<bool>,
    ) -> js::Result<PromiseRaw<'static, ()>> {
        let Some(inner) = &self.inner else {
            return Err(napi::Error::from_reason("write state is unavailable").into());
        };
        let fate = inner
            .binding
            .wait_for_write(&self.write, &tier)
            .map_err(host_error)?;
        if !observe_only.unwrap_or(false) {
            self.claimed.set(true);
        }
        Ok(host_promise(&env, fate)?)
    }

    #[napi]
    pub fn close(&mut self) -> bool {
        self.inner.take().is_some()
    }
}

/// A large value uploaded in chunks, then published as one row mutation.
#[napi(js_name = "StreamingMutation")]
pub struct StreamingMutation {
    inner: Inner,
    mutation: binding_host::StreamingMutation,
}

#[napi]
impl StreamingMutation {
    #[napi]
    pub fn push(&mut self, chunk: Uint8Array) -> js::Result<()> {
        Ok(self.mutation.push(&chunk).map_err(host_error)?)
    }

    #[napi]
    pub fn finish(&mut self, env: Env) -> js::Result<Write> {
        let write = self.mutation.finish();
        Ok(self.inner.write(&env, write)?)
    }

    #[napi]
    pub fn abort(&mut self) -> js::Result<bool> {
        Ok(self.mutation.abort().map_err(host_error)?)
    }
}

// ---- subscriptions ----------------------------------------------------------------

#[napi(object)]
pub struct SubscriptionDeltaEvent {
    #[napi(js_name = "type", ts_type = "'delta'")]
    pub event_type: String,
    pub reset: bool,
    pub delta: Uint8Array,
    /// Always empty: SQLite subscriptions deliver whole-row deltas.
    #[napi(js_name = "terminalOperations", ts_type = "Array<never>")]
    pub terminal_operations: Vec<JsonValue>,
    pub settled: bool,
    #[napi(ts_type = "'None' | 'Local' | 'Global'")]
    pub tier: String,
}

#[napi(object)]
pub struct SubscriptionRejectedEvent {
    #[napi(js_name = "type", ts_type = "'rejected'")]
    pub event_type: String,
    pub reason: SubscriptionServerFailureReason,
}

#[napi(object)]
pub struct SubscriptionClosedEvent {
    #[napi(js_name = "type", ts_type = "'closed'")]
    pub event_type: String,
}

/// Why the server refused a subscription.
#[napi(object)]
pub struct SubscriptionServerFailureReason {
    #[napi(js_name = "type", ts_type = "'ServerFailure'")]
    pub reason_type: String,
    /// Stable, client-safe failure class.
    #[napi(
        ts_type = "'TableNotFound' | 'SchemaResolution' | 'QueryValidation' | 'QueryLowering' | 'PolicyEvaluation' | 'Internal' | 'QueryResultProtocol' | 'StrandedStore'"
    )]
    pub code: String,
}

#[napi]
pub type SubscriptionEvent =
    Either3<SubscriptionDeltaEvent, SubscriptionRejectedEvent, SubscriptionClosedEvent>;

#[napi(js_name = "Subscription")]
pub struct Subscription {
    stream: Option<SubscriptionStream>,
}

fn subscription_event(event: CoreEvent) -> napi::Result<SubscriptionEvent> {
    Ok(
        match binding_host::event_chunk(event).map_err(host_error)? {
            EventChunk::Delta {
                delta,
                reset,
                settled,
                tier,
            } => Either3::A(SubscriptionDeltaEvent {
                event_type: "delta".to_owned(),
                reset,
                delta: delta.into(),
                terminal_operations: Vec::new(),
                settled,
                tier,
            }),
            EventChunk::Rejected { code } => Either3::B(SubscriptionRejectedEvent {
                event_type: "rejected".to_owned(),
                reason: SubscriptionServerFailureReason {
                    reason_type: "ServerFailure".to_owned(),
                    code: code.as_str().to_owned(),
                },
            }),
            EventChunk::Closed => Either3::C(SubscriptionClosedEvent {
                event_type: "closed".to_owned(),
            }),
        },
    )
}

#[napi]
impl Subscription {
    /// Every event available now; empty until the next tick or local write
    /// has something to deliver.
    #[napi(js_name = "readAll")]
    pub fn read_all(&mut self) -> js::Result<Vec<SubscriptionEvent>> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("subscription is closed"))?;
        let mut events = Vec::new();
        while let Some(event) = stream.try_next_event() {
            let closed = matches!(event, CoreEvent::Closed);
            events.push(subscription_event(event)?);
            if closed {
                self.stream = None;
                break;
            }
        }
        Ok(events)
    }

    #[napi]
    pub fn drain(&mut self) -> js::Result<Vec<SubscriptionEvent>> {
        self.read_all()
    }

    #[napi]
    pub fn close(&mut self) -> bool {
        self.stream.take().is_some()
    }
}

// ---- transports ------------------------------------------------------------------

/// One sync link, carried by the host as byte frames. Frames are applied
/// and produced by the database's tick.
#[napi(js_name = "Transport")]
pub struct Transport {
    link: binding_host::Link,
}

#[napi]
impl Transport {
    /// SQLite's sync has no auxiliary chunk lane: large-value chunks travel
    /// as ordinary sync messages, so every frame belongs to the ordinary lane.
    #[napi(js_name = "routeAuxiliaryWireFrame")]
    pub fn route_auxiliary_wire_frame(&self, frame: Uint8Array) -> Option<Uint8Array> {
        Some(frame)
    }

    #[napi(js_name = "recvAuxiliaryWireFrames")]
    pub fn recv_auxiliary_wire_frames(&self) -> Vec<Uint8Array> {
        Vec::new()
    }

    #[napi(js_name = "auxiliaryReceiveTimeoutMs")]
    pub fn auxiliary_receive_timeout_ms(&self) -> Option<u32> {
        None
    }

    #[napi(js_name = "expireAuxiliaryReceive")]
    pub fn expire_auxiliary_receive(&self) {}

    #[napi(js_name = "auxiliaryOutboundReady")]
    pub fn auxiliary_outbound_ready(&self) -> bool {
        false
    }

    #[napi(js_name = "sendWireFrame")]
    pub fn send_wire_frame(&self, frame: Uint8Array) {
        self.link.receive([frame.to_vec()]);
    }

    #[napi(js_name = "sendWireFrames")]
    pub fn send_wire_frames(&self, frames: Vec<Uint8Array>) {
        self.link.receive(frames.iter().map(|frame| frame.to_vec()));
    }

    #[napi(js_name = "recvWireFrames")]
    pub fn recv_wire_frames(&self) -> Vec<Uint8Array> {
        self.link
            .take_outbound()
            .into_iter()
            .map(Uint8Array::from)
            .collect()
    }

    /// Tick the database this link belongs to: apply received frames, send
    /// what is due. Returns the number of frames waiting to be sent.
    #[napi]
    pub fn tick(&self) -> js::Result<u32> {
        let result = block_on(self.link.binding().tick());
        settle_promises();
        result.map_err(host_error)?;
        Ok(self.link.outbound_len() as u32)
    }

    #[napi]
    pub fn close(&mut self) -> bool {
        self.link.close()
    }
}

// ---- the database --------------------------------------------------------------

#[napi(js_name = "NapiDb")]
pub struct NapiDb {
    // Taken at close: every later call fails before touching the engine.
    inner: Rc<RefCell<Option<Inner>>>,
    owns_runtime: bool,
}

impl Drop for NapiDb {
    fn drop(&mut self) {
        // Release the tick callback so it no longer keeps Node's event loop
        // alive once JavaScript dropped an unclosed database.
        if self.owns_runtime
            && let Some(inner) = self.inner.borrow_mut().take()
        {
            inner.host.tick_scheduler.borrow_mut().take();
        }
    }
}

impl NapiDb {
    fn open_inner(&self) -> napi::Result<Inner> {
        self.inner.borrow().clone().ok_or_else(closed)
    }

    fn binding(&self) -> napi::Result<Binding> {
        Ok(self.open_inner()?.binding)
    }

    fn runtime_owner(binding: Binding) -> Self {
        Self {
            inner: Rc::new(RefCell::new(Some(Inner::new(binding)))),
            owns_runtime: true,
        }
    }

    fn open_as(
        schema: &[u8],
        config: &[u8],
        data_path: Option<&str>,
        how: OpenAs<'_>,
    ) -> napi::Result<Self> {
        let (schema, mut config) = binding_host::decode_open(schema, config).map_err(host_error)?;
        let (identity, trusted) =
            binding_host::open_identity(&mut config, how).map_err(host_error)?;
        let db_config = binding_host::db_config(schema, &config, identity);
        let db = match data_path {
            None => block_on(binding_host::open_memory(db_config)),
            Some(data_path) => block_on(binding_host::open_file(
                db_config,
                &store_file(data_path)?,
                FileOptions::default(),
            )),
        }
        .map_err(|error| napi_core_error(&error))?;
        Ok(Self::runtime_owner(Binding::new(db, identity, trusted)))
    }

    fn request_permission_advice(
        &self,
        action: HostResult<jazz::protocol::PermissionAdviceAction>,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Either<String, PendingNativePermissionAdvice>> {
        let binding = self.binding()?;
        let action = action.map_err(host_error)?;
        let (id, future) = binding
            .request_permission_advice(action, author.as_deref(), claims)
            .map_err(host_error)?;
        let pending = PendingNativePermissionAdvice {
            future: RefCell::new(Some(future)),
            binding,
            id,
        };
        Ok(match pending.poll_once()? {
            Some(advice) => Either::A(advice),
            None => Either::B(pending),
        })
    }

    fn transport(&self, link: binding_host::Link) -> Transport {
        Transport { link }
    }
}

/// The SQLite file of a persistent store in directory `data_path`.
///
/// A directory that holds a store from before storage format 1 is refused
/// with the `unsupported_storage_codecs` code rather than opened empty next to
/// its data. `jazz.sqlite.native-root.v1` admits the regular SQLite database
/// and sidecar files plus unrelated regular files (such as `.DS_Store` or
/// notes), which are left alone. It refuses symlinks, directories and other
/// non-regular entries of any name, and regular files named like another
/// SQLite database or RocksDB store. Validate before SQLite opens or creates
/// any file.
fn store_file(data_path: &str) -> napi::Result<std::path::PathBuf> {
    let dir = std::path::Path::new(data_path);
    jazz::format::refuse_groove_store(dir).map_err(|error| host_error(HostError::Engine(error)))?;
    std::fs::create_dir_all(dir)
        .map_err(|error| napi::Error::from_reason(format!("create {data_path}: {error}")))?;
    let io_error = |error: std::io::Error| {
        napi::Error::from_reason(format!("inspect native database root {data_path}: {error}"))
    };
    for entry in std::fs::read_dir(dir).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        // DirEntry::file_type does not follow symlinks, so a link of any name
        // (which could redirect the database or a sidecar into another root)
        // is refused here, as is any directory.
        let regular = entry.file_type().map_err(io_error)?.is_file();
        if !regular || names_foreign_store(&entry.file_name().to_string_lossy()) {
            return Err(napi_core_error(&CoreError::new(
                jazz::db::ErrorCode::UnsupportedStorageCodecs,
                "native database root uses an unsupported storage layout; expected jazz.sqlite.native-root.v1",
            )));
        }
    }
    Ok(dir.join("jazz.sqlite"))
}

/// Whether a regular file named `name` in a native database root looks like
/// part of a store other than `jazz.sqlite`: another SQLite database or
/// sidecar, or a RocksDB (Groove) file. The match is by name only, so a copy
/// such as `jazz.sqlite.bak` is a harmless stray.
fn names_foreign_store(name: &str) -> bool {
    if matches!(
        name,
        "jazz.sqlite" | "jazz.sqlite-wal" | "jazz.sqlite-shm" | "jazz.sqlite-journal"
    ) {
        return false;
    }
    // SQLite databases and their sidecars, and RocksDB table files.
    const FOREIGN_SUFFIXES: [&str; 8] = [
        ".sqlite", ".sqlite3", ".db", ".db3", "-wal", "-shm", "-journal", ".sst",
    ];
    const ROCKSDB_NAMES: [&str; 3] = ["CURRENT", "IDENTITY", "LOCK"];
    const ROCKSDB_PREFIXES: [&str; 2] = ["MANIFEST-", "OPTIONS-"];
    let lower = name.to_ascii_lowercase();
    FOREIGN_SUFFIXES
        .iter()
        .any(|suffix| lower.ends_with(suffix))
        || ROCKSDB_NAMES.contains(&name)
        || ROCKSDB_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
}

#[napi]
impl NapiDb {
    /// Exact wire capabilities compiled into this native binding.
    ///
    /// The TypeScript WebSocket carrier uses this for its Hello instead of an
    /// independent feature list, so a package cannot advertise a codec this
    /// native artifact cannot decode.
    #[napi(js_name = "wireFeatures")]
    pub fn wire_features(&self) -> u32 {
        jazz::wire::current_wire_features() as u32
    }

    #[napi(js_name = "requestInsertPermissionAdvice")]
    pub fn request_insert_permission_advice(
        &self,
        table: String,
        cells: Uint8Array,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Either<String, PendingNativePermissionAdvice>> {
        self.request_permission_advice(binding_host::advice::insert(table, &cells), author, claims)
    }

    #[napi(js_name = "requestReadPermissionAdvice")]
    pub fn request_read_permission_advice(
        &self,
        table: String,
        row_id: Uint8Array,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Either<String, PendingNativePermissionAdvice>> {
        self.request_permission_advice(binding_host::advice::read(table, &row_id), author, claims)
    }

    #[napi(js_name = "requestUpdatePermissionAdvice")]
    pub fn request_update_permission_advice(
        &self,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Either<String, PendingNativePermissionAdvice>> {
        self.request_permission_advice(
            binding_host::advice::update(table, &row_id, &patch),
            author,
            claims,
        )
    }

    #[napi(js_name = "requestDeletePermissionAdvice")]
    pub fn request_delete_permission_advice(
        &self,
        table: String,
        row_id: Uint8Array,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Either<String, PendingNativePermissionAdvice>> {
        self.request_permission_advice(binding_host::advice::delete(table, &row_id), author, claims)
    }

    /// Admit a verified local-first account for this backend runtime only.
    #[napi]
    pub fn admit_local_first_session(
        &self,
        token: String,
        app_id: String,
        claimed_author: String,
    ) -> js::Result<()> {
        Ok(self
            .binding()?
            .admit_local_first_session(&token, &app_id, &claimed_author)
            .map_err(host_error)?)
    }

    #[napi(js_name = "insert")]
    pub fn insert_with_options(
        &self,
        env: Env,
        table: String,
        cells: Uint8Array,
        options: Option<InsertOptions>,
    ) -> js::Result<Write> {
        let inner = self.open_inner()?;
        let write = inner
            .binding
            .insert(&table, &cells, &insert_options(options));
        Ok(inner.write(&env, write)?)
    }

    #[napi(js_name = "insertInTransaction")]
    pub fn insert_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        cells: Uint8Array,
        options: Option<InsertOptions>,
    ) -> js::Result<Uint8Array> {
        let row = self
            .binding()?
            .insert_in_transaction(
                &open_transaction_id,
                &table,
                &cells,
                &insert_options(options),
            )
            .map_err(host_error)?;
        Ok(row.to_bytes().to_vec().into())
    }

    #[napi(js_name = "update")]
    pub fn update_with_options(
        &self,
        env: Env,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        options: Option<UpdateOptions>,
    ) -> js::Result<Write> {
        let inner = self.open_inner()?;
        let write = inner
            .binding
            .update(&table, &row_id, &patch, &update_options(options));
        Ok(inner.write(&env, write)?)
    }

    #[napi(js_name = "updateInTransaction")]
    pub fn update_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        options: Option<UpdateOptions>,
    ) -> js::Result<()> {
        Ok(self
            .binding()?
            .update_in_transaction(
                &open_transaction_id,
                &table,
                &row_id,
                &patch,
                &update_options(options),
            )
            .map_err(host_error)?)
    }

    /// Binding-only entrypoint for typed partial-value updates staged inside
    /// an open transaction. The transaction's bound identity authors them.
    #[napi(js_name = "updateLargeValuesInTransaction")]
    pub fn update_large_values_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        mutations: JsonValue,
        updated_at_ms: Option<f64>,
    ) -> js::Result<()> {
        let mutations = large_value_updates(mutations)?;
        Ok(self
            .binding()?
            .update_large_values_in_transaction(
                &open_transaction_id,
                &table,
                &row_id,
                &patch,
                mutations,
                updated_at_ms,
            )
            .map_err(host_error)?)
    }

    /// Binding-only entrypoint for typed partial-value updates. The public
    /// TypeScript API validates column-kind-specific descriptors before they
    /// reach this encoded boundary.
    #[napi(js_name = "updateLargeValues")]
    pub fn update_large_values(
        &self,
        env: Env,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        mutations: JsonValue,
        updated_at_ms: Option<f64>,
    ) -> js::Result<Write> {
        let mutations = large_value_updates(mutations)?;
        let inner = self.open_inner()?;
        let write =
            inner
                .binding
                .update_large_values(&table, &row_id, &patch, mutations, updated_at_ms);
        Ok(inner.write(&env, write)?)
    }

    #[napi(js_name = "upsert")]
    pub fn upsert_with_options(
        &self,
        env: Env,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        #[napi(ts_arg_type = "UpsertOptions | undefined | null")] options: Option<Unknown<'_>>,
    ) -> js::Result<Write> {
        let options = upsert_options(options)?;
        let inner = self.open_inner()?;
        let write = inner.binding.upsert(&table, &row_id, &cells, &options);
        Ok(inner.write(&env, write)?)
    }

    #[napi(js_name = "upsertInTransaction")]
    pub fn upsert_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        #[napi(ts_arg_type = "UpsertOptions | undefined | null")] options: Option<Unknown<'_>>,
    ) -> js::Result<()> {
        let options = upsert_options(options)?;
        Ok(self
            .binding()?
            .upsert_in_transaction(&open_transaction_id, &table, &row_id, &cells, &options)
            .map_err(host_error)?)
    }

    #[napi(js_name = "delete")]
    pub fn delete_with_options(
        &self,
        env: Env,
        table: String,
        row_id: Uint8Array,
        options: Option<DeleteOptions>,
    ) -> js::Result<Write> {
        let inner = self.open_inner()?;
        let write = inner
            .binding
            .delete(&table, &row_id, &delete_options(options));
        Ok(inner.write(&env, write)?)
    }

    #[napi(js_name = "deleteInTransaction")]
    pub fn delete_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Uint8Array,
        options: Option<DeleteOptions>,
    ) -> js::Result<()> {
        Ok(self
            .binding()?
            .delete_in_transaction(
                &open_transaction_id,
                &table,
                &row_id,
                &delete_options(options),
            )
            .map_err(host_error)?)
    }

    #[napi(js_name = "restore")]
    pub fn restore_with_options(
        &self,
        env: Env,
        table: String,
        row_id: Uint8Array,
        cells: Option<Uint8Array>,
        options: Option<RestoreOptions>,
    ) -> js::Result<Write> {
        let inner = self.open_inner()?;
        let write =
            inner
                .binding
                .restore(&table, &row_id, cells.as_deref(), &restore_options(options));
        Ok(inner.write(&env, write)?)
    }

    #[napi(js_name = "restoreInTransaction")]
    pub fn restore_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Uint8Array,
        cells: Option<Uint8Array>,
        options: Option<RestoreOptions>,
    ) -> js::Result<()> {
        Ok(self
            .binding()?
            .restore_in_transaction(
                &open_transaction_id,
                &table,
                &row_id,
                cells.as_deref(),
                &restore_options(options),
            )
            .map_err(host_error)?)
    }

    #[napi(js_name = "beginStreamingMutation")]
    #[allow(clippy::too_many_arguments)]
    pub fn begin_streaming_mutation(
        &self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        column: String,
        mutation: Option<String>,
        author: Option<Uint8Array>,
        attribution: Option<Uint8Array>,
        updated_at_ms: Option<f64>,
        head: Option<JsonValue>,
        base: Option<JsonValue>,
    ) -> js::Result<StreamingMutation> {
        let head = structured(head, "head")?;
        let base = structured(base, "base")?;
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
        Ok(StreamingMutation { inner, mutation })
    }

    #[napi(factory, js_name = "openMemory")]
    pub fn open_memory(schema: Uint8Array, config: Uint8Array) -> js::Result<Self> {
        Ok(Self::open_as(&schema, &config, None, OpenAs::Untrusted)?)
    }

    /// Open a deliberate backend runtime. Unlike the public raw-open entrypoint,
    /// this explicit ABI derives the canonical system author.
    #[napi(factory, js_name = "openMemoryAsBackend")]
    pub fn open_memory_as_backend(schema: Uint8Array, config: Uint8Array) -> js::Result<Self> {
        Ok(Self::open_as(&schema, &config, None, OpenAs::Backend)?)
    }

    /// Open with a verified Jazz self-signed client identity. This is a
    /// separate ABI entrypoint deliberately: a new client cannot accidentally
    /// hand proof bytes to an old constructor, and an old client cannot enter
    /// the proof-bearing path.
    #[napi(factory, js_name = "openMemoryWithSelfSignedProof")]
    pub fn open_memory_with_self_signed_proof(
        schema: Uint8Array,
        config: Uint8Array,
        token: String,
        app_id: String,
        claimed_author: String,
    ) -> js::Result<Self> {
        let how = OpenAs::SelfSigned {
            token: &token,
            app_id: &app_id,
            claimed_author: &claimed_author,
        };
        Ok(Self::open_as(&schema, &config, None, how)?)
    }

    /// Open the durable store in directory `data_path` (one SQLite file).
    #[napi(factory, js_name = "openPersistent")]
    pub fn open_persistent(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
    ) -> js::Result<Self> {
        Ok(Self::open_as(
            &schema,
            &config,
            Some(&data_path),
            OpenAs::Untrusted,
        )?)
    }

    /// Open a deliberate persistent backend runtime. This is intentionally a
    /// distinct ABI from the public raw-open entrypoint.
    #[napi(factory, js_name = "openPersistentAsBackend")]
    pub fn open_persistent_as_backend(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
    ) -> js::Result<Self> {
        Ok(Self::open_as(
            &schema,
            &config,
            Some(&data_path),
            OpenAs::Backend,
        )?)
    }

    #[napi(factory, js_name = "openPersistentWithSelfSignedProof")]
    pub fn open_persistent_with_self_signed_proof(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
        token: String,
        app_id: String,
        claimed_author: String,
    ) -> js::Result<Self> {
        let how = OpenAs::SelfSigned {
            token: &token,
            app_id: &app_id,
            claimed_author: &claimed_author,
        };
        Ok(Self::open_as(&schema, &config, Some(&data_path), how)?)
    }

    /// Register and return a typed view backed by this same runtime owner.
    #[napi(js_name = "registerSchema")]
    pub fn register_schema(&self, schema: Uint8Array) -> js::Result<Self> {
        let inner = self.open_inner()?;
        let binding = inner.binding.register_schema(&schema).map_err(host_error)?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(Inner { binding, ..inner }))),
            owns_runtime: false,
        })
    }

    /// Begin one owner-wide transaction.
    #[napi(js_name = "beginTransaction")]
    pub fn begin_transaction(
        &self,
        open_transaction_id: String,
        kind: String,
        author: Option<Uint8Array>,
        attribution: Option<Uint8Array>,
    ) -> js::Result<()> {
        Ok(self
            .binding()?
            .begin_transaction(
                &open_transaction_id,
                &kind,
                author.as_deref(),
                attribution.as_deref(),
            )
            .map_err(host_error)?)
    }

    /// Commit an owner-wide transaction by id and optional kind.
    #[napi(js_name = "commitTransaction")]
    pub fn commit_transaction(
        &self,
        env: Env,
        open_transaction_id: String,
        kind: Option<String>,
    ) -> js::Result<Write> {
        let inner = self.open_inner()?;
        let write = inner
            .binding
            .commit_transaction(&open_transaction_id, kind.as_deref());
        Ok(inner.write(&env, write)?)
    }

    /// Roll back an owner-wide open transaction by id.
    #[napi(js_name = "rollbackTransaction")]
    pub fn rollback_transaction(&self, open_transaction_id: String) -> js::Result<()> {
        Ok(self
            .binding()?
            .rollback_transaction(&open_transaction_id)
            .map_err(host_error)?)
    }

    /// The host's tick callback: called with `"immediate"` whenever there
    /// is sync work (a local write to upload, frames to apply).
    #[napi(
        js_name = "setTickScheduler",
        ts_args_type = "callback: ((err: Error | null, arg: string) => void)"
    )]
    pub fn set_tick_scheduler(&self, callback: Function<'_, String, ()>) -> js::Result<()> {
        let inner = self.open_inner()?;
        let requested = Arc::clone(&inner.host.tick_requested);
        let callback = callback
            .build_threadsafe_function::<String>()
            .callee_handled::<true>()
            .build_callback(move |context| {
                // Clear before calling JS so reentrant progress can request
                // the next turn without losing its wake.
                requested.store(false, Ordering::Release);
                Ok(context.value)
            })?;
        *inner.host.tick_scheduler.borrow_mut() = Some(callback);
        Ok(())
    }

    /// Register a callback for rejected writes that no active wait consumed.
    #[napi(
        js_name = "onMutationError",
        ts_args_type = "callback: (event: any) => void"
    )]
    pub fn on_mutation_error(&self, callback: Function<'_, JsonValue, ()>) -> js::Result<()> {
        let inner = self.open_inner()?;
        *inner.host.mutation_errors.borrow_mut() = Some(callback.create_ref()?);
        let upstream = callback
            .build_threadsafe_function::<serde_json::Value>()
            .callee_handled::<false>()
            .weak::<true>()
            .build()?;
        *inner.host.upstream_mutation_errors.borrow_mut() = Some(upstream);
        // Refusals kept while nobody listened reach this callback now.
        inner.binding.redeliver_rejections();
        Ok(())
    }

    /// Decode, prepare, and execute one read inside Rust. Query-plan ownership
    /// never crosses the language boundary.
    #[napi]
    pub fn all(
        &self,
        query: Uint8Array,
        #[napi(
            ts_arg_type = "{ tier?: string; include_deleted?: boolean; sync?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
        open_transaction_id: Option<String>,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        let read = self
            .binding()?
            .all(
                query.to_vec(),
                &json_or_null(opts),
                open_transaction_id.as_deref(),
                author.as_deref(),
                claims,
            )
            .map_err(host_error)?;
        read_or_pending(read)
    }

    /// Bind receipt-correlation claims to this client's own admitted identity.
    #[napi(js_name = "setSessionClaims")]
    pub fn set_session_claims(
        &self,
        #[napi(ts_arg_type = "Record<string, unknown> | undefined | null")] claims: Option<
            JsonValue,
        >,
    ) -> js::Result<()> {
        self.binding()?
            .set_session_claims(json_or_null(claims))
            .map_err(host_error)?;
        settle_promises();
        Ok(())
    }

    /// Set the claims reads and writes as `author` are checked with.
    #[napi(js_name = "setIdentityClaims")]
    pub fn set_identity_claims(
        &self,
        author: Uint8Array,
        #[napi(ts_arg_type = "Record<string, unknown> | undefined | null")] claims: Option<
            JsonValue,
        >,
    ) -> js::Result<()> {
        self.binding()?
            .set_identity_claims(&author, json_or_null(claims))
            .map_err(host_error)?;
        settle_promises();
        Ok(())
    }

    /// Exact local state for the write-merge bridge: the row as stored,
    /// deleted or not. Write authorization remains at the mutation boundary;
    /// this is not a query.
    #[napi(js_name = "localCurrentRow")]
    pub fn local_current_row(&self, table: String, row_id: Uint8Array) -> js::Result<Uint8Array> {
        Ok(self
            .binding()?
            .local_current_row(&table, &row_id)
            .map_err(host_error)?
            .into())
    }

    #[napi]
    pub fn subscribe(
        &self,
        query: Uint8Array,
        #[napi(ts_arg_type = "{ tier?: string; include_deleted?: boolean } | undefined | null")]
        opts: Option<JsonValue>,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Subscription> {
        let stream = self
            .binding()?
            .subscribe(&query, &json_or_null(opts), author.as_deref(), claims)
            .map_err(host_error)?;
        Ok(Subscription {
            stream: Some(stream),
        })
    }

    /// Apply received frames and send what is due on every link.
    #[napi]
    pub fn tick(&self) -> js::Result<()> {
        let result = block_on(self.binding()?.tick());
        settle_promises();
        Ok(result.map_err(host_error)?)
    }

    /// Configure Jazz-owned upload ingress and unpublished-tree expiry limits.
    #[napi(js_name = "setLargeValueStagingPolicy")]
    pub fn set_large_value_staging_policy(
        &self,
        incoming_bytes_per_window: f64,
        window_ms: f64,
        max_age_ms: Option<f64>,
    ) -> js::Result<()> {
        Ok(self
            .binding()?
            .set_large_value_staging_policy(incoming_bytes_per_window, window_ms, max_age_ms)
            .map_err(host_error)?)
    }

    /// Run one idempotent expiry pass; native hosts normally call this on a timer.
    #[napi(js_name = "evictExpiredStagedLargeValues")]
    pub fn evict_expired_staged_large_values(&self) -> js::Result<u32> {
        let evicted = block_on(self.binding()?.db.evict_expired_staged_large_values())
            .map_err(|error| napi_core_error(&error))?;
        Ok(evicted as u32)
    }

    /// How many local commits still wait for the server's verdict.
    #[napi(js_name = "pendingUploadCount")]
    pub fn pending_upload_count(&self) -> js::Result<u32> {
        Ok(self.binding()?.pending_upload_count() as u32)
    }

    /// The optimistic in-memory side of a client/worker pair: its
    /// subscriptions open with the worker's answer.
    #[napi(js_name = "setNonDurableClient")]
    pub fn set_non_durable_client(&self) -> js::Result<()> {
        self.binding()?.set_non_durable_client();
        Ok(())
    }

    /// Report what the host knows about the path to the authoritative server
    /// (`"none" | "attempting" | "live" | "failed"`; the TypeScript names
    /// `"connecting" | "connected" | "unavailable"` are accepted as aliases).
    /// Drives only local-first reads that wait for the server on first load.
    #[napi(js_name = "setRemoteLinkHint")]
    pub fn set_remote_link_hint(&self, state: String) -> js::Result<()> {
        self.binding()?
            .set_remote_link_hint(&state)
            .map_err(host_error)?;
        settle_promises();
        Ok(())
    }

    /// A fresh upstream link machine for this runtime's server connection;
    /// `seed` drives its retry jitter.
    #[napi(js_name = "createUpstreamLink")]
    pub fn create_upstream_link(&self, seed: f64) -> NapiUpstreamLink {
        NapiUpstreamLink::new(seed)
    }

    /// The slow-query report due now: one line per slow query shape,
    /// naming `app`: the first at once, then at most every 10 seconds. The caller
    /// writes each line with `console.warn`.
    #[napi(js_name = "takeSlowQueryReport")]
    pub fn take_slow_query_report(&self, app: Option<String>) -> js::Result<Vec<String>> {
        Ok(self.binding()?.take_slow_query_report(app.as_deref()))
    }

    /// Report query runs of at least `ms` milliseconds (default 50); 0
    /// turns the slow-query log off.
    #[napi(js_name = "setSlowQueryThresholdMs")]
    pub fn set_slow_query_threshold_ms(&self, ms: f64) -> js::Result<()> {
        self.binding()?
            .set_slow_query_threshold_ms(ms)
            .map_err(host_error)?;
        Ok(())
    }

    /// Connect to the server over byte frames the host carries: each frame
    /// is one postcard `SyncMessage`.
    #[napi(js_name = "connectUpstream")]
    pub fn connect_upstream(&self) -> js::Result<Transport> {
        Ok(self.transport(self.binding()?.connect_upstream()))
    }

    /// Connect after the carrier accepted the server Hello. Negotiated
    /// features must be compiled in; the rest of the session context
    /// (authority endpoints, epochs) has no counterpart on SQLite's sync, so
    /// this is otherwise [`Self::connect_upstream`].
    #[napi(js_name = "connectUpstreamWithSession")]
    pub fn connect_upstream_with_session(
        &self,
        _protocol_version: u32,
        features: u32,
        remote_node: Buffer,
        _remote_epoch: BigInt,
        local_node: Buffer,
        _local_epoch: BigInt,
    ) -> js::Result<Transport> {
        if remote_node.len() != 16 {
            return Err(
                napi::Error::from_reason("server hello authority node must be 16 bytes").into(),
            );
        }
        if local_node.len() != 16 {
            return Err(napi::Error::from_reason("local peer identity must be 16 bytes").into());
        }
        let unsupported = u64::from(features) & !jazz::wire::current_wire_features();
        if unsupported != 0 {
            return Err(napi::Error::from_reason(format!(
                "server negotiated wire features {features:#x}, but this native binding was not compiled with {unsupported:#x}"
            ))
            .into());
        }
        self.connect_upstream()
    }

    /// The newest transaction time this database handed out under its node
    /// (a `TxTime` is the commit seq). A leased node's next holder seeds it
    /// through `seedForegroundTxTimeHighWater`.
    #[napi(js_name = "foregroundTxTimeHighWater")]
    pub fn foreground_tx_time_high_water(&self) -> js::Result<BigInt> {
        Ok(BigInt::from(self.binding()?.db.seq_high_water()))
    }

    /// Continue after a previous holder of this node: no transaction time at
    /// or below `high_water` is reused.
    #[napi(js_name = "seedForegroundTxTimeHighWater")]
    pub fn seed_foreground_tx_time_high_water(&self, high_water: BigInt) -> js::Result<()> {
        let (negative, value, lossless) = high_water.get_u64();
        if negative || !lossless {
            return Err(napi::Error::from_reason(
                "foreground transaction time high water must be an unsigned 64-bit integer",
            )
            .into());
        }
        self.binding()?.db.seed_seq_floor(value);
        Ok(())
    }

    /// Capture a snapshot reference, as its persisted string's bytes.
    #[napi(js_name = "captureSnapshot")]
    pub fn capture_snapshot(&self) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        read_or_pending(self.binding()?.capture_snapshot())
    }

    /// Complete once every local write so far reached `tier`. Commits are in
    /// the SQLite file when their write call returns, so only `global` waits.
    #[napi(js_name = "waitForPendingWrites")]
    pub fn wait_for_pending_writes(
        &self,
        tier: String,
    ) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        let binding = self.binding()?;
        let tier = jazz::binding_args::durability_tier(&tier).map_err(napi::Error::from_reason)?;
        match tier {
            DurabilityTier::None | DurabilityTier::Local => Ok(Either::A(Vec::new().into())),
            DurabilityTier::Global => {
                let wait = binding.wait_for_pending_writes_global();
                read_or_pending(Box::pin(wait.map(|result| result.map(|()| Vec::new()))))
            }
        }
    }

    /// Close the database (a schema view leaves its owner open). The
    /// package's `close()` wraps this pollable form.
    #[napi(js_name = "__closePollable", skip_typescript)]
    pub fn close(&self) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        // A close failure still consumes the binding: retrying a partially
        // failed close would re-enter an indeterminate runtime.
        let Some(inner) = self.inner.borrow_mut().take() else {
            return Ok(Either::A(Vec::new().into()));
        };
        if self.owns_runtime {
            inner.host.tick_scheduler.borrow_mut().take();
            inner.host.mutation_errors.borrow_mut().take();
            inner.host.upstream_mutation_errors.borrow_mut().take();
            block_on(inner.binding.close()).map_err(host_error)?;
        }
        Ok(Either::A(Vec::new().into()))
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
    host: Option<String>,
    data_dir: Option<String>,
    in_memory: Option<bool>,
    jwks_url: Option<String>,
    jwt_issuer: Option<String>,
    jwt_audience: Option<String>,
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

    #[napi(getter)]
    pub fn issuer(&self) -> String {
        TEST_JWT_ISSUER.to_owned()
    }

    #[napi(getter)]
    pub fn audience(&self) -> String {
        TEST_JWT_AUDIENCE.to_owned()
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
                // Keep the server test helper's ordinary external-session
                // default. `None` is reserved for tests that explicitly
                // exercise an issuer-less bearer, not the NAPI omission case.
                issuer: options.issuer.or_else(|| Some(TEST_JWT_ISSUER.to_owned())),
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
    lifecycle: Arc<JazzServerLifecycle>,
}

struct JazzServerLifecycle {
    state: Mutex<JazzServerState>,
    changed: tokio::sync::watch::Sender<()>,
}

enum JazzServerState {
    Running(Box<CoreJazzServer>),
    Stopping,
    Stopped(std::result::Result<(), String>),
}

#[napi]
impl JazzServer {
    #[napi(factory, ts_return_type = "Promise<JazzServer>")]
    pub async fn start(
        #[napi(
            ts_arg_type = "{ appId: string; backendSecret: string; adminSecret: string; port?: number; host?: string; dataDir?: string; inMemory?: boolean; jwksUrl?: string; jwtIssuer?: string; jwtAudience?: string; allowLocalFirstAuth?: boolean; telemetryCollectorUrl?: string; schema?: Buffer | Uint8Array | number[] }"
        )]
        options: JsonValue,
    ) -> napi::Result<Self> {
        let mut opts = parse_jazz_server_start_options(options)?;
        init_jazz_server_telemetry(opts.telemetry_collector_url.as_deref());

        let core_server_shell_schema = opts
            .schema
            .take()
            .map(|schema_bytes| binding_host::decode_schema(&schema_bytes))
            .transpose()
            .map_err(host_error)?;

        let app_id =
            AppId::from_string(&opts.app_id).unwrap_or_else(|_| AppId::from_name(&opts.app_id));

        let auth_config = AuthConfig {
            jwks_url: opts.jwks_url,
            jwt_issuer: opts.jwt_issuer,
            jwt_audience: opts.jwt_audience,
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
        if opts.upstream_url.is_some() {
            return Err(napi::Error::from_reason(
                "server edges are no longer supported; remove upstreamUrl and connect clients directly to Core",
            ));
        }
        server_builder = server_builder.with_storage(if in_memory {
            StorageBackend::InMemory
        } else {
            StorageBackend::Persistent {
                path: data_dir.clone().into(),
            }
        });

        let built = server_builder
            .build()
            .await
            .map_err(napi::Error::from_reason)?;

        let server = CoreJazzServer::from_built(
            built,
            opts.port,
            opts.host,
            app_id,
            ServerDataDir::from_path(std::path::PathBuf::from(&data_dir)),
            opts.admin_secret.clone(),
            opts.backend_secret.clone(),
        )
        .await
        .map_err(napi::Error::from_reason)?;

        let (changed, _) = tokio::sync::watch::channel(());
        Ok(Self {
            lifecycle: Arc::new(JazzServerLifecycle {
                state: Mutex::new(JazzServerState::Running(Box::new(server))),
                changed,
            }),
        })
    }

    #[napi(getter, js_name = "appId")]
    pub fn app_id(&self) -> napi::Result<String> {
        self.with_server(|server| server.app_id().to_string())
    }

    #[napi(getter)]
    pub fn url(&self) -> napi::Result<String> {
        self.with_server(|server| server.base_url())
    }

    #[napi(getter)]
    pub fn port(&self) -> napi::Result<u16> {
        self.with_server(|server| server.port())
    }

    #[napi(getter, js_name = "dataDir")]
    pub fn data_dir(&self) -> napi::Result<String> {
        self.with_server(|server| server.data_dir().to_string_lossy().into_owned())
    }

    #[napi(getter, js_name = "backendSecret")]
    pub fn backend_secret(&self) -> napi::Result<String> {
        self.with_server(|server| server.backend_secret().to_string())
    }

    #[napi(getter, js_name = "adminSecret")]
    pub fn admin_secret(&self) -> napi::Result<String> {
        self.with_server(|server| server.admin_secret().to_string())
    }

    #[napi]
    pub async fn stop(&self) -> napi::Result<()> {
        let mut changes = self.lifecycle.changed.subscribe();
        let shutdown_owner = {
            let mut state = self
                .lifecycle
                .state
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            match &*state {
                JazzServerState::Running(_) => {
                    let JazzServerState::Running(server) =
                        std::mem::replace(&mut *state, JazzServerState::Stopping)
                    else {
                        unreachable!("running state changed while locked")
                    };
                    Some(server)
                }
                JazzServerState::Stopping | JazzServerState::Stopped(_) => None,
            }
        };

        if let Some(server) = shutdown_owner {
            let lifecycle = Arc::clone(&self.lifecycle);
            tokio::spawn(async move {
                let result = std::panic::AssertUnwindSafe(shutdown_jazz_server(*server))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err("JazzServer shutdown task panicked".to_owned()));
                let mut state = lifecycle
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *state = JazzServerState::Stopped(result);
                drop(state);
                lifecycle.changed.send_replace(());
            });
        }

        loop {
            let result = {
                let state = self
                    .lifecycle
                    .state
                    .lock()
                    .map_err(|_| napi::Error::from_reason("lock"))?;
                match &*state {
                    JazzServerState::Running(_) => {
                        return Err(napi::Error::from_reason(
                            "JazzServer shutdown state returned to running",
                        ));
                    }
                    JazzServerState::Stopping => None,
                    JazzServerState::Stopped(result) => Some(result.clone()),
                }
            };
            if let Some(result) = result {
                return result.map_err(napi::Error::from_reason);
            }
            changes.changed().await.map_err(|_| {
                napi::Error::from_reason("JazzServer shutdown state closed unexpectedly")
            })?;
        }
    }

    fn with_server<T>(&self, f: impl FnOnce(&CoreJazzServer) -> T) -> napi::Result<T> {
        let state = self
            .lifecycle
            .state
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        match &*state {
            JazzServerState::Running(server) => Ok(f(server.as_ref())),
            JazzServerState::Stopping => Err(napi::Error::from_reason("JazzServer is stopping")),
            JazzServerState::Stopped(_) => {
                Err(napi::Error::from_reason("JazzServer has been stopped"))
            }
        }
    }
}

async fn shutdown_jazz_server(server: CoreJazzServer) -> std::result::Result<(), String> {
    let phase = server.shutdown().await;
    if phase == jazz_server::ShutdownPhase::StorageClosed {
        Ok(())
    } else {
        Err(format!("JazzServer shutdown failed during phase {phase:?}"))
    }
}

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
mod native_root_tests {
    use super::names_foreign_store;

    #[test]
    fn jazz_sqlite_files_and_unrelated_strays_are_admitted() {
        for name in [
            "jazz.sqlite",
            "jazz.sqlite-wal",
            "jazz.sqlite-shm",
            "jazz.sqlite-journal",
            ".DS_Store",
            "notes.txt",
            "jazz.sqlite.bak",
            "application.data",
        ] {
            assert!(!names_foreign_store(name), "{name} should be admitted");
        }
    }

    #[test]
    fn other_sqlite_and_rocksdb_files_are_refused() {
        for name in [
            "other.sqlite",
            "other.SQLITE3",
            "app.db",
            "app.db3",
            "app.db-wal",
            "app.db-shm",
            "app.db-journal",
            "jazz.sqlite.bak-wal",
            "CURRENT",
            "LOCK",
            "IDENTITY",
            "MANIFEST-000001",
            "OPTIONS-000005",
            "000003.sst",
        ] {
            assert!(names_foreign_store(name), "{name} should be refused");
        }
    }
}
