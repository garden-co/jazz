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
mod lifecycle;
mod native_storage;
use jazz::db::DbIdentity as CoreDbIdentity;
use jazz::ids::AuthorSubject as CoreAuthorSubject;
use jazz::schema::JazzSchema;
use jazz::tools::OpenTransactionId as CoreOpenTransactionId;
use native_storage::NativeStorage;
type CoreOpenDbConfig = binding_host::OpenConfig;
struct CoreSelfSignedClientProof {
    token: String,
    app_id: String,
    claimed_author: String,
}

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

#[napi(js_name = "__testValidateSqliteSyncFrameCorpus", skip_typescript)]
pub fn test_validate_sqlite_sync_frame_corpus(frame: Buffer) -> Result<()> {
    jazz::db::decode_message(&frame)
        .map(drop)
        .map_err(napi_error)
}

fn corpus_reader(name: &str) -> std::result::Result<jazz::db::CorpusReader, String> {
    match name {
        "server-opening" => Ok(jazz::db::CorpusReader::ServerOpening),
        "server" => Ok(jazz::db::CorpusReader::Server),
        "client" => Ok(jazz::db::CorpusReader::Client),
        other => Err(format!("unknown wire corpus reader {other:?}")),
    }
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

    fn number(mut self, name: &str, value: Option<f64>) -> napi::Result<Self> {
        if let Some(value) = value {
            jazz::binding_args::checked_u64(value, name).map_err(napi_error)?;
            self.0.insert(name.to_owned(), value.into());
        }
        Ok(self)
    }

    fn done(self) -> serde_json::Value {
        serde_json::Value::Object(self.0)
    }
}

fn insert_options(options: Option<InsertOptions>) -> napi::Result<serde_json::Value> {
    let Some(options) = options else {
        return Ok(serde_json::Value::Null);
    };
    Ok(JsonOptions::default()
        .bytes("rowId", options.row_id)
        .bytes("author", options.author)
        .bytes("attribution", options.attribution)
        .json("branch", options.branch)
        .number("updatedAtMs", options.updated_at_ms)?
        .done())
}

fn update_options(options: Option<UpdateOptions>) -> napi::Result<serde_json::Value> {
    let Some(options) = options else {
        return Ok(serde_json::Value::Null);
    };
    Ok(JsonOptions::default()
        .bytes("author", options.author)
        .bytes("attribution", options.attribution)
        .json("head", options.head)
        .json("base", options.base)
        .number("updatedAtMs", options.updated_at_ms)?
        .done())
}

fn delete_options(options: Option<DeleteOptions>) -> napi::Result<serde_json::Value> {
    update_options(options.map(|options| UpdateOptions {
        author: options.author,
        attribution: options.attribution,
        head: options.head,
        base: options.base,
        updated_at_ms: options.updated_at_ms,
    }))
}

fn restore_options(options: Option<RestoreOptions>) -> napi::Result<serde_json::Value> {
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
        )?;
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

/// Prefix of the private `cause` that carries a core error code from where the
/// core error is converted to the exported method that throws it. It never
/// reaches JavaScript: [`BindingError`] strips it.
const CORE_ERROR_CODE_CARRIER: &str = "\u{0}jazz-core-error-code:";

/// Convert a core error into a `napi::Error` with its unchanged `Display`
/// text, recording its stable [`jazz::db::ErrorCode::as_str`] code. The code
/// rides inside the `napi::Error` until an exported method returns it as a
/// [`BindingError`], which throws it as the JavaScript `error.code`.
fn napi_core_error(error: &CoreError) -> napi::Error {
    let mut napi_error = napi::Error::from_reason(error.to_string());
    napi_error.set_cause(napi::Error::from_reason(format!(
        "{CORE_ERROR_CODE_CARRIER}{}",
        error.code.as_str()
    )));
    napi_error
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
        let carried = error.cause.as_ref().and_then(|cause| {
            cause
                .reason
                .strip_prefix(CORE_ERROR_CODE_CARRIER)
                .map(str::to_owned)
        });
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

/// A JavaScript `Error` with `error`'s message and code (see
/// [`BindingError`]).
fn js_error_value(env: sys::napi_env, error: &napi::Error) -> sys::napi_value {
    let code = error
        .cause
        .as_ref()
        .and_then(|cause| cause.reason.strip_prefix(CORE_ERROR_CODE_CARRIER))
        .unwrap_or_else(|| error.status.as_ref());
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
    if status == sys::Status::napi_ok {
        js_error
    } else {
        message
    }
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
    /// Released only after the core retires every persistent owner reference.
    file_lock: RefCell<Option<Rc<std::fs::File>>>,
    closed: Cell<bool>,
    closing: Cell<bool>,
    next_resource: Cell<u64>,
    views: RefCell<std::collections::BTreeMap<u64, std::rc::Weak<lifecycle::View>>>,
    initialization_views:
        RefCell<std::collections::BTreeMap<String, std::rc::Weak<lifecycle::View>>>,
    cleanup_failures: RefCell<lifecycle::CleanupFailures>,
    cleanup_drain: futures::lock::Mutex<()>,
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
    view: Rc<lifecycle::View>,
    live: Rc<Cell<bool>>,
    binding: Binding,
    host: Rc<Host>,
}

impl Inner {
    fn ensure_open(&self) -> napi::Result<()> {
        if self.host.closed.get() || self.host.closing.get() || !self.live.get() {
            Err(closed())
        } else {
            Ok(())
        }
    }
    fn new(binding: Binding) -> Self {
        let host = Rc::new(Host::default());
        let scheduler = Rc::clone(&host);
        *binding.hooks.schedule_tick.borrow_mut() =
            Some(Box::new(move || scheduler.request_tick()));
        let reporter = Rc::clone(&host);
        *binding.hooks.rejected.borrow_mut() =
            Some(Box::new(move |tx_id, error, serializable, _| {
                if let Some(callback) = reporter.upstream_mutation_errors.borrow().as_ref() {
                    let event = mutation_error_event(&tx_id.to_string(), error, serializable, true);
                    callback.call(event, ThreadsafeFunctionCallMode::NonBlocking);
                }
            }));
        let view = lifecycle::View::new(&host);
        Self {
            binding,
            host,
            live: view.live.clone(),
            view,
        }
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

/// A JavaScript-thread-owned read waiting for the server. NAPI promises
/// execute on a Send worker pool, whereas a Jazz database is deliberately
/// `Rc`/thread-affine. The adapter drives this object after its peer
/// transport makes progress instead of blocking Node.
#[napi]
pub struct PendingNativeRead {
    future: lifecycle::Resource<ReadFuture>,
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
            Poll::Ready(result) => {
                self.future.complete();
                result.map(|bytes| Some(bytes.into())).map_err(host_error)
            }
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
        self.future.cancel();
    }
}

fn read_or_pending(future: ReadFuture) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
    read_for_view(None, future)
}

fn read_for_view(
    view: Option<&Rc<lifecycle::View>>,
    future: ReadFuture,
) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
    let pending = PendingNativeRead {
        future: lifecycle::Resource::new(view, future),
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
    future: lifecycle::Resource<HostFuture<&'static str>>,
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
            Poll::Ready(result) => {
                self.future.complete();
                result
                    .map(|advice| Some(advice.to_owned()))
                    .map_err(host_error)
            }
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
        self.future.cancel();
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
    upload: Option<lifecycle::UploadHandle>,
}
#[napi]
impl StreamingMutation {
    #[napi]
    pub fn push(&mut self, chunk: Uint8Array) -> js::Result<()> {
        self.upload.as_ref().ok_or_else(closed)?.push(&chunk)?;
        Ok(())
    }
    #[napi]
    pub fn finish(&mut self, env: Env) -> js::Result<Write> {
        let upload = self.upload.as_ref().ok_or_else(closed)?;
        let write = upload.finish()?;
        let receipt = upload.inner.write(&env, Ok(write));
        self.upload.take();
        Ok(receipt?)
    }
    #[napi]
    pub fn stage(&mut self) -> js::Result<StagedStreamingMutation> {
        self.upload.as_ref().ok_or_else(closed)?.stage()?;
        Ok(StagedStreamingMutation {
            upload: self.upload.take(),
        })
    }
    #[napi]
    pub fn abort(&mut self) -> js::Result<bool> {
        Ok(self
            .upload
            .take()
            .map(|upload| upload.abort())
            .transpose()?
            .unwrap_or(false))
    }
}

/// A single-use capability that remains bound to its originating schema facade.
#[napi(js_name = "StagedStreamingMutation")]
pub struct StagedStreamingMutation {
    upload: Option<lifecycle::UploadHandle>,
}
#[napi]
impl StagedStreamingMutation {
    #[napi]
    pub fn attach(&mut self, transaction: String) -> js::Result<()> {
        self.upload
            .take()
            .ok_or_else(closed)?
            .attach(&transaction)?;
        Ok(())
    }
    #[napi]
    pub fn abort(&mut self) -> js::Result<bool> {
        Ok(self
            .upload
            .take()
            .map(|upload| upload.abort())
            .transpose()?
            .unwrap_or(false))
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
    /// The server's reason.
    pub code: String,
}

#[napi]
pub type SubscriptionEvent =
    Either3<SubscriptionDeltaEvent, SubscriptionRejectedEvent, SubscriptionClosedEvent>;

#[napi(js_name = "Subscription")]
pub struct Subscription {
    stream: lifecycle::Resource<SubscriptionStream>,
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
            EventChunk::Rejected { detail } => Either3::B(SubscriptionRejectedEvent {
                event_type: "rejected".to_owned(),
                reason: SubscriptionServerFailureReason {
                    reason_type: "ServerFailure".to_owned(),
                    code: detail,
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
        let mut resource = self.stream.borrow_mut();
        let stream = resource
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("subscription is closed"))?;
        let mut events = Vec::new();
        while let Some(event) = stream.try_next_event() {
            let closed = matches!(event, CoreEvent::Closed);
            events.push(subscription_event(event)?);
            if closed {
                resource.take();
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
        self.stream.borrow_mut().take().is_some()
    }
}

// Debug artifact fixture uses the same event converter as live subscriptions.
#[cfg(debug_assertions)]
mod test_fixture_export {
    #[napi_derive::napi(js_name = "__testSubscriptionEvents", skip_typescript)]
    pub fn subscription_events() -> napi::Result<Vec<super::SubscriptionEvent>> {
        [
            super::CoreEvent::Rejected {
                reason: "fixture SQLite subscription refusal".into(),
            },
            super::CoreEvent::Closed,
        ]
        .into_iter()
        .map(super::subscription_event)
        .collect()
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
    pub fn send_wire_frame(&self, frame: Uint8Array) -> js::Result<()> {
        jazz::sync_codec::decode_sync_message(&frame).map_err(|error| napi_core_error(&error))?;
        self.link.receive([frame.to_vec()]);
        Ok(())
    }

    #[napi(js_name = "sendWireFrames")]
    pub fn send_wire_frames(&self, frames: Vec<Uint8Array>) -> js::Result<()> {
        // Reject the complete batch before queueing even its valid prefix.
        for frame in &frames {
            jazz::sync_codec::decode_sync_message(frame)
                .map_err(|error| napi_core_error(&error))?;
        }
        self.link.receive(frames.iter().map(|frame| frame.to_vec()));
        Ok(())
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
    // Retained for repeated close results; admission is fenced by the view/host.
    inner: Rc<RefCell<Option<Inner>>>,
    owns_runtime: bool,
}

impl Drop for NapiDb {
    fn drop(&mut self) {
        let Some(inner) = self.inner.borrow_mut().take() else {
            return;
        };
        inner.view.retire();
        inner.binding.hooks.schedule_tick();
        if self.owns_runtime {
            // GC cannot await storage cleanup. Retained sibling views/resources
            // keep the owner lease until they are retired; explicit close drains
            // it deterministically. Do not leave an event-loop callback alive.
            inner.host.tick_scheduler.borrow_mut().take();
        }
    }
}

mod account_owner;
mod initialization;

impl NapiDb {
    fn open_inner(&self) -> napi::Result<Inner> {
        let inner = self.inner.borrow().clone().ok_or_else(closed)?;
        if inner.host.closed.get() || inner.host.closing.get() || !inner.live.get() {
            return Err(closed());
        }
        Ok(inner)
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
        cached: Option<&[u8]>,
    ) -> std::result::Result<Self, PersistentOpenError> {
        let (schema, mut config) = binding_host::decode_open(schema, config).map_err(host_error)?;
        let private_owner = data_path.is_some() && matches!(how, OpenAs::SelfSigned { .. });
        let (identity, trusted) =
            binding_host::open_identity(&mut config, how).map_err(host_error)?;
        if cached.is_some() && config.history_complete {
            return Err(napi::Error::from_reason(
                "cached catalogue cannot establish history completeness",
            )
            .into());
        }
        let db_config = binding_host::db_config(schema, &config, identity);
        let (db, lock) = match data_path {
            None => (
                block_on(binding_host::open_memory(db_config))
                    .map_err(|error| napi_core_error(&error))?,
                None,
            ),
            Some(root) => {
                let NativeStorage::File { path, lock } = native_storage::persistent_storage(root)?;
                let db = block_on(binding_host::open_file(
                    db_config,
                    &path,
                    FileOptions {
                        synchronous_full: true,
                        ..Default::default()
                    },
                ))
                .map_err(|error| napi_core_error(&error))?;
                (db, Some(lock))
            }
        };
        if let Some(capture) = cached {
            // SAFETY: the host loaded this app-scoped authenticated capture
            // before publishing the runtime or accepting any client input.
            unsafe { db.install_cached_catalogue(capture) }
                .map_err(|error| napi_core_error(&error))?;
        }
        if private_owner {
            db.set_scope_isolated_storage_owner()
                .map_err(|error| napi_core_error(&error))?;
        }
        let runtime = Self::runtime_owner(Binding::new(db, identity, trusted));
        *runtime.open_inner()?.host.file_lock.borrow_mut() = lock;
        Ok(runtime)
    }

    fn request_permission_advice(
        &self,
        action: HostResult<jazz::protocol::PermissionAdviceAction>,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Either<String, PendingNativePermissionAdvice>> {
        let inner = self.open_inner()?;
        let binding = inner.binding.clone();
        let action = action.map_err(host_error)?;
        let (id, future) = binding
            .request_permission_advice(action, author.as_deref(), claims)
            .map_err(host_error)?;
        let pending = PendingNativePermissionAdvice {
            future: lifecycle::Resource::new(Some(&inner.view), future)
                .on_cancel(move || binding.cancel_permission_advice(id)),
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
            .insert(&table, &cells, &insert_options(options)?);
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
                &insert_options(options)?,
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
            .update(&table, &row_id, &patch, &update_options(options)?);
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
                &update_options(options)?,
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
            .delete(&table, &row_id, &delete_options(options)?);
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
                &delete_options(options)?,
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
        let write = inner.binding.restore(
            &table,
            &row_id,
            cells.as_deref(),
            &restore_options(options)?,
        );
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
                &restore_options(options)?,
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
        Ok(StreamingMutation {
            upload: Some(lifecycle::UploadHandle::new(inner, mutation)),
        })
    }

    #[napi(factory, js_name = "openMemory")]
    pub fn open_memory(
        schema: Uint8Array,
        config: Uint8Array,
        cached_catalogue: Option<Uint8Array>,
    ) -> js::Result<Self> {
        Ok(Self::open_as(
            &schema,
            &config,
            None,
            OpenAs::Untrusted,
            cached_catalogue.as_deref(),
        )
        .map_err(PersistentOpenError::into_napi)?)
    }

    /// Open a deliberate backend runtime. Unlike the public raw-open entrypoint,
    /// this explicit ABI derives the canonical system author.
    #[napi(factory, js_name = "openMemoryAsBackend")]
    pub fn open_memory_as_backend(schema: Uint8Array, config: Uint8Array) -> js::Result<Self> {
        Ok(Self::open_as(&schema, &config, None, OpenAs::Backend, None)
            .map_err(PersistentOpenError::into_napi)?)
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
        cached_catalogue: Option<Uint8Array>,
    ) -> js::Result<Self> {
        let how = OpenAs::SelfSigned {
            token: &token,
            app_id: &app_id,
            claimed_author: &claimed_author,
        };
        Ok(
            Self::open_as(&schema, &config, None, how, cached_catalogue.as_deref())
                .map_err(PersistentOpenError::into_napi)?,
        )
    }

    /// Open the durable store in directory `data_path` (one SQLite file).
    #[napi(factory, js_name = "openPersistent")]
    pub fn open_persistent(
        env: Env,
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
        cached_catalogue: Option<Uint8Array>,
    ) -> js::Result<Self> {
        Self::open_as(
            &schema,
            &config,
            Some(&data_path),
            OpenAs::Untrusted,
            cached_catalogue.as_deref(),
        )
        .map_err(|error| error.throw(&env))
    }

    /// Open a deliberate persistent backend runtime. This is intentionally a
    /// distinct ABI from the public raw-open entrypoint.
    #[napi(factory, js_name = "openPersistentAsBackend")]
    pub fn open_persistent_as_backend(
        env: Env,
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
    ) -> js::Result<Self> {
        Self::open_as(&schema, &config, Some(&data_path), OpenAs::Backend, None)
            .map_err(|error| error.throw(&env))
    }

    // Keep the positional JavaScript ABI aligned with the other persistent opens.
    #[napi(factory, js_name = "openPersistentWithSelfSignedProof")]
    #[allow(clippy::too_many_arguments)]
    pub fn open_persistent_with_self_signed_proof(
        env: Env,
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
        token: String,
        app_id: String,
        claimed_author: String,
        cached_catalogue: Option<Uint8Array>,
    ) -> js::Result<Self> {
        let how = OpenAs::SelfSigned {
            token: &token,
            app_id: &app_id,
            claimed_author: &claimed_author,
        };
        Self::open_as(
            &schema,
            &config,
            Some(&data_path),
            how,
            cached_catalogue.as_deref(),
        )
        .map_err(|error| error.throw(&env))
    }

    #[napi(js_name = "tableIdentity")]
    pub fn table_identity(
        &self,
        table: String,
    ) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        Ok(Either::A(
            self.binding()?
                .table_identity(&table)
                .map_err(host_error)?
                .into(),
        ))
    }
    #[napi(js_name = "columnIdentity")]
    pub fn column_identity(
        &self,
        table: String,
        column: String,
    ) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        Ok(Either::A(
            self.binding()?
                .column_identity(&table, &column)
                .map_err(host_error)?
                .into(),
        ))
    }
    #[napi(js_name = "coverCatalogue")]
    pub fn cover_catalogue(
        &self,
        table: String,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        let inner = self.open_inner()?;
        read_for_view(
            Some(&inner.view),
            inner
                .binding
                .cover_catalogue(table, author.as_deref(), claims)
                .map_err(host_error)?,
        )
    }
    #[napi(js_name = "allSettlementMetadata")]
    pub fn all_settlement_metadata(
        &self,
        query: Uint8Array,
        opts: Option<JsonValue>,
        open_transaction_id: String,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
        include_rows: Option<bool>,
    ) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        let inner = self.open_inner()?;
        read_for_view(
            Some(&inner.view),
            inner
                .binding
                .all_settlement_metadata(
                    query.to_vec(),
                    &json_or_null(opts),
                    &open_transaction_id,
                    author.as_deref(),
                    claims,
                    include_rows.unwrap_or(false),
                )
                .map_err(host_error)?,
        )
    }
    #[napi(js_name = "setPrivateOwnerClient")]
    pub fn set_private_owner_client(&self) -> js::Result<()> {
        self.binding()?
            .set_private_owner_client()
            .map_err(host_error)?;
        Ok(())
    }

    /// Register and return a typed view backed by this same runtime owner.
    #[napi(js_name = "registerSchema")]
    pub fn register_schema(&self, schema: Uint8Array) -> js::Result<Self> {
        let inner = self.open_inner()?;
        let binding = inner.binding.register_schema(&schema).map_err(host_error)?;
        let view = lifecycle::View::new(&inner.host);
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(Inner {
                binding,
                live: view.live.clone(),
                view,
                ..inner
            }))),
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
        let inner = self.open_inner()?;
        let read = inner
            .binding
            .all(
                query.to_vec(),
                &json_or_null(opts),
                open_transaction_id.as_deref(),
                author.as_deref(),
                claims,
            )
            .map_err(host_error)?;
        read_for_view(Some(&inner.view), read)
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
        let inner = self.open_inner()?;
        let stream = inner
            .binding
            .subscribe(&query, &json_or_null(opts), author.as_deref(), claims)
            .map_err(host_error)?;
        Ok(Subscription {
            stream: lifecycle::Resource::new(Some(&inner.view), stream),
        })
    }

    /// Apply received frames and send what is due on every link.
    #[napi]
    pub fn tick(&self) -> js::Result<()> {
        // Deferred close keeps the progress route while application entrypoints
        // remain fenced. Only retirement makes ticks unavailable.
        let inner = self.inner.borrow().clone().ok_or_else(closed)?;
        if inner.host.closed.get() {
            return Err(closed().into());
        }
        let Some(_drain) = inner.host.cleanup_drain.try_lock() else {
            return Ok(());
        };
        let result = block_on(inner.binding.tick());
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
        remote_epoch: BigInt,
        local_node: Buffer,
        local_epoch: BigInt,
    ) -> js::Result<Transport> {
        if remote_node.len() != 16 {
            return Err(
                napi::Error::from_reason("server hello authority node must be 16 bytes").into(),
            );
        }
        if local_node.len() != 16 {
            return Err(napi::Error::from_reason("local peer identity must be 16 bytes").into());
        }
        authority_epoch_from_bigint(remote_epoch, "authority")?;
        authority_epoch_from_bigint(local_epoch, "local")?;
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
        let inner = self.open_inner()?;
        read_for_view(Some(&inner.view), inner.binding.capture_snapshot())
    }

    /// Complete once every local write so far reached `tier`. Commits are in
    /// the SQLite file when their write call returns, so only `global` waits.
    #[napi(js_name = "waitForPendingWrites")]
    pub fn wait_for_pending_writes(
        &self,
        tier: String,
    ) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        let inner = self.open_inner()?;
        let tier = jazz::binding_args::durability_tier(&tier).map_err(napi::Error::from_reason)?;
        match tier {
            DurabilityTier::None | DurabilityTier::Local => Ok(Either::A(Vec::new().into())),
            DurabilityTier::Global => {
                let wait = inner.binding.wait_for_pending_writes_global();
                read_for_view(
                    Some(&inner.view),
                    Box::pin(wait.map(|result| result.map(|()| Vec::new()))),
                )
            }
        }
    }

    /// Close the database (a schema view leaves its owner open). The
    /// package's `close()` wraps this pollable form.
    #[napi(js_name = "__closePollable", skip_typescript)]
    pub fn close(&self) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        let Some(inner) = self.inner.borrow().clone() else {
            return Ok(Either::A(Vec::new().into()));
        };
        read_or_pending(lifecycle::close(inner, self.owns_runtime))
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

/// Why a persistent open failed, before it is thrown in JavaScript.
#[derive(Debug)]
pub(crate) enum PersistentOpenError {
    /// The store's root was written in a storage format this build does not
    /// read (#3728).
    UnsupportedStorageCodecs(jazz::db::Error),
    Other(BindingError),
}

impl From<BindingError> for PersistentOpenError {
    fn from(error: BindingError) -> Self {
        Self::Other(error)
    }
}

impl From<napi::Error> for PersistentOpenError {
    fn from(error: napi::Error) -> Self {
        Self::Other(BindingError::from(error))
    }
}

impl PersistentOpenError {
    /// Throw a storage-format refusal as a JavaScript `Error` whose
    /// `message` is the core display text, whose `code` is
    /// `"unsupported_storage_codecs"`, and whose `format` names the SQLite
    /// native-root layout. Other failures are returned unchanged.
    ///
    /// napi-rs builds a thrown error from only a code and a message, so this
    /// throws the complete object itself and returns the pending-exception
    /// status, which the generated wrapper leaves in place.
    fn throw(self, env: &Env) -> BindingError {
        let error = match self {
            Self::UnsupportedStorageCodecs(error) => error,
            Self::Other(error) => return error,
        };
        let thrown = env
            .create_error(napi::Error::new(
                napi::Status::GenericFailure,
                error.to_string(),
            ))
            .and_then(|mut object| {
                object.set_named_property("code", error.code.as_str())?;
                object.set_named_property("format", "jazz.sqlite.native-root.v1")?;
                env.throw(object)
            });
        match thrown {
            Ok(()) => BindingError::from(napi::Error::from_status(napi::Status::PendingException)),
            Err(_) => BindingError::from(napi_core_error(&error)),
        }
    }
}

impl PersistentOpenError {
    fn into_napi(self) -> napi::Error {
        match self {
            Self::UnsupportedStorageCodecs(error) => napi_core_error(&error),
            Self::Other(error) => {
                let mut value = napi::Error::from_reason(error.0.reason);
                value.set_cause(napi::Error::from_reason(format!(
                    "{CORE_ERROR_CODE_CARRIER}{}",
                    error.0.status.as_ref()
                )));
                value
            }
        }
    }
}
fn napi_error(error: impl std::fmt::Display) -> napi::Error {
    napi::Error::from_reason(error.to_string())
}
fn decode_core_open_args(
    schema: &[u8],
    config: &[u8],
) -> napi::Result<(JazzSchema, CoreOpenDbConfig)> {
    binding_host::decode_open(schema, config).map_err(host_error)
}
fn core_open_identity(
    config: &CoreOpenDbConfig,
    proof: Option<&CoreSelfSignedClientProof>,
) -> napi::Result<CoreDbIdentity> {
    let how = match proof {
        None => OpenAs::Untrusted,
        Some(proof) => OpenAs::SelfSigned {
            token: &proof.token,
            app_id: &proof.app_id,
            claimed_author: &proof.claimed_author,
        },
    };
    binding_host::open_identity(&mut config.clone(), how)
        .map(|(id, _)| id)
        .map_err(host_error)
}

fn authority_epoch_from_bigint(epoch: BigInt, name: &str) -> napi::Result<u64> {
    let (negative, value, lossless) = epoch.get_u64();
    if negative || !lossless {
        return Err(napi::Error::from_reason(format!(
            "{name} epoch must be an unsigned 64-bit integer"
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    mod account_owner;
    mod initialization;
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use futures::channel::oneshot;

    use self::host_fixture::*;
    use super::*;
    pub(super) mod host_fixture;

    /// Contract: a core error keeps its display text and gains its stable code
    /// as the thrown JavaScript `error.code`; any other error keeps napi-rs's
    /// `GenericFailure`. White-box because throwing needs a live Node
    /// environment; `native-error-code.test.ts` covers the thrown object.
    #[test]
    fn binding_errors_carry_the_core_code_and_keep_the_message() {
        let core = jazz::db::Error::new(jazz::db::ErrorCode::NotObserved, "not resident");
        let thrown = crate::BindingError::from(crate::napi_core_error(&core));
        assert_eq!(thrown.status.as_ref(), "not_observed");
        assert_eq!(thrown.reason, "NotObserved: not resident");
        assert!(
            thrown.cause.is_none(),
            "the code carrier never reaches JavaScript"
        );

        let other = crate::BindingError::from(crate::napi_error("database is closed"));
        assert_eq!(other.status.as_ref(), "GenericFailure");
        assert_eq!(other.reason, "database is closed");
        assert!(other.cause.is_none());
    }

    /// Alice's failed SQLite cleanup keeps its lease and tick route; retry preserves committed rows and discards the abandoned upload.
    #[test]
    fn deferred_upload_close_preserves_native_tick_and_retry() {
        // White-box storage fixture: SQLite's real writer lock suspends the
        // cleanup pass without adding a production failure-injection path.
        let dir = tempfile::tempdir().unwrap();
        let source = SchemaBuilder::new()
            .table(TableSchema::builder("files").column("body", ColumnType::Text))
            .build();
        let schema = serde_json::to_vec(&source).unwrap();
        let config = encode_persistent_open_config(CoreAuthorSubject::for_test_bytes([0xf6; 16]));
        let open = || {
            NapiDb::try_open_persistent(
                dir.path().to_string_lossy().into_owned(),
                schema.clone().into(),
                config.clone().into(),
                None,
            )
        };
        let db = open().unwrap();
        let begin = |row: u8| {
            db.begin_streaming_mutation(
                "files".into(),
                vec![row; 16].into(),
                empty_cells(),
                "body".into(),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap()
        };
        let mut published = begin(0x31);
        published.push(b"committed file".to_vec().into()).unwrap();
        let write = published.finish_test().unwrap();
        let binding = db.binding().unwrap();
        block_on(binding.wait_for_write(&write, "local").unwrap()).unwrap();
        let original = db
            .local_current_row("files".into(), vec![0x31; 16].into())
            .unwrap();
        let mut abandoned = begin(0x32);
        abandoned
            .push("unpublished".repeat(8000).into_bytes().into())
            .unwrap();
        let lock = rusqlite::Connection::open(dir.path().join("jazz.sqlite")).unwrap();
        lock.execute_batch("BEGIN IMMEDIATE").unwrap();
        let failure = match db.close() {
            Err(error) => error.reason.clone(),
            Ok(Either::A(_)) => panic!("locked cleanup must fail close"),
            Ok(Either::B(pending)) => pending
                .poll()
                .err()
                .expect("locked cleanup fails")
                .reason
                .clone(),
        };
        assert!(failure.contains("database is locked"), "{failure}");
        assert!(open().is_err(), "failed close retains the root lease");
        assert!(abandoned.push(vec![1].into()).is_err());
        assert!(
            db.begin_transaction(
                CoreOpenTransactionId::new().to_string(),
                "serializable".into(),
                None,
                None
            )
            .is_err()
        );
        lock.execute_batch("ROLLBACK").unwrap();
        db.tick().expect("failed close retains the tick route");
        block_on(binding.wait_for_write(&write, "local").unwrap()).unwrap();
        finish_close(&db);
        let reopened = open().unwrap();
        assert_eq!(
            reopened
                .local_current_row("files".into(), vec![0x31; 16].into())
                .unwrap()
                .as_ref(),
            original.as_ref()
        );
        assert_eq!(
            reopened
                .local_current_row("files".into(), vec![0x32; 16].into())
                .unwrap()
                .as_ref(),
            &[0]
        );
        finish_close(&reopened);
    }

    fn encode_persistent_open_config(author: CoreAuthorSubject) -> Vec<u8> {
        #[derive(serde::Serialize)]
        struct EncodedIdentity {
            node: CoreNodeUuid,
            author: CoreAuthorSubject,
        }
        #[derive(serde::Serialize)]
        struct EncodedConfig {
            identity: EncodedIdentity,
            row_id_seed: Option<u64>,
            history_complete: bool,
            initial_sync_flush_every: Option<u32>,
            backend_credential: Option<String>,
        }
        postcard::to_allocvec(&EncodedConfig {
            identity: EncodedIdentity {
                node: CoreNodeUuid::from_bytes([0x71; 16]),
                author,
            },
            row_id_seed: None,
            history_complete: false,
            initial_sync_flush_every: None,
            backend_credential: None,
        })
        .expect("valid NAPI open fixture")
    }

    /// Plant the adapter's generic profile first. Each public persistent NAPI
    /// opener must reject it rather than silently treating a Jazz root as a
    /// Groove-only one. This is intentionally an internal manifest-admission
    /// receipt: a user-visible API cannot create the invalid physical root.
    fn plant_groove_only_persistent_root(path: &std::path::Path, _schema_bytes: &[u8]) {
        // The SQLite adapter rejects foreign roots before opening storage.
        // Immutable Groove filenames suffice; no removed RocksDB dependency.
        std::fs::create_dir_all(path).unwrap();
        std::fs::write(path.join("CURRENT"), b"MANIFEST-000001\n").unwrap();
        std::fs::write(path.join("MANIFEST-000001"), b"foreign Groove fixture").unwrap();
    }

    #[test]
    fn every_public_persistent_open_rejects_a_planted_groove_only_manifest() {
        let schema = br#"{"tables":{}}"#;
        let external_author =
            CoreAuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
        let config = encode_persistent_open_config(external_author);

        let ordinary_dir = tempfile::tempdir().unwrap();
        let ordinary_path = ordinary_dir.path().join("ordinary.rocksdb");
        plant_groove_only_persistent_root(&ordinary_path, schema);
        assert!(
            NapiDb::try_open_persistent(
                ordinary_path.to_string_lossy().into_owned(),
                Uint8Array::from(schema.to_vec()),
                Uint8Array::from(config.clone()),
                None,
            )
            .is_err(),
            "openPersistent must reject a manifest that omits Jazz codecs"
        );

        let backend_dir = tempfile::tempdir().unwrap();
        let backend_path = backend_dir.path().join("backend.rocksdb");
        plant_groove_only_persistent_root(&backend_path, schema);
        assert!(
            NapiDb::try_open_persistent_as_backend(
                backend_path.to_string_lossy().into_owned(),
                Uint8Array::from(schema.to_vec()),
                Uint8Array::from(config.clone()),
            )
            .is_err(),
            "openPersistentAsBackend must reject a manifest that omits Jazz codecs"
        );

        let proof_dir = tempfile::tempdir().unwrap();
        let proof_path = proof_dir.path().join("proof.rocksdb");
        plant_groove_only_persistent_root(&proof_path, schema);
        let token = jazz::tools::identity::mint_jazz_self_signed_token(
            &[0x72; 32],
            jazz::tools::identity::LOCAL_FIRST_ISSUER,
            "codec-profile-test",
            60,
        )
        .unwrap();
        let verified =
            jazz::tools::identity::verify_jazz_self_signed_proof(&token, "codec-profile-test")
                .unwrap();
        let claimed_author =
            serde_json::to_string(&(jazz::tools::identity::LOCAL_FIRST_ISSUER, verified.user_id))
                .unwrap();
        assert!(
            NapiDb::try_open_persistent_with_self_signed_proof(
                proof_path.to_string_lossy().into_owned(),
                Uint8Array::from(schema.to_vec()),
                Uint8Array::from(config),
                token,
                "codec-profile-test".to_owned(),
                claimed_author,
                None,
            )
            .is_err(),
            "openPersistentWithSelfSignedProof must reject a manifest that omits Jazz codecs"
        );
        for root in [&ordinary_path, &backend_path, &proof_path] {
            assert_eq!(
                std::fs::read(root.join("CURRENT")).unwrap(),
                b"MANIFEST-000001\n"
            );
            assert_eq!(
                std::fs::read(root.join("MANIFEST-000001")).unwrap(),
                b"foreign Groove fixture"
            );
        }
    }

    #[test]
    fn pending_native_read_retains_a_suspended_future_until_the_next_js_turn() {
        let (sender, receiver) = oneshot::channel::<Uint8Array>();
        let pending = pending_read(Box::pin(async move {
            receiver
                .await
                .map_err(|_| napi::Error::from_reason("planned sender drop"))
        }));

        assert!(
            pending.poll_once().unwrap().is_none(),
            "planted suspension is retained"
        );
        assert!(
            sender.send(Uint8Array::new(vec![7, 11])).is_ok(),
            "complete the retained future"
        );
        assert_eq!(pending.poll_once().unwrap().unwrap().to_vec(), vec![7, 11]);
        assert!(
            pending.poll_once().is_err(),
            "completed reads cannot be replayed or double-encoded"
        );
    }

    /// Alice can cancel a suspended read without blocking other local work or retaining its resources.
    #[test]
    fn cancelled_read_releases_held_state_and_does_not_block_the_owner() {
        // White-box cancellation check: a held guard is an observable stand-in
        // for a read's resource. The SQLite host no longer takes an owner lock.
        let state = Rc::new(futures::lock::Mutex::new(()));
        let held = state.clone();
        let pending = pending_read(Box::pin(async move {
            let _guard = held.lock().await;
            futures::future::pending::<()>().await;
            Ok(vec![3].into())
        }));
        assert!(pending.poll_once().unwrap().is_none());
        assert!(state.try_lock().is_none());
        let db = fixture();
        db.insert_test("items".into(), label_cells("available"), None)
            .unwrap();
        assert_eq!(labels(&db), ["available"]);
        pending.cancel();
        assert!(state.try_lock().is_some());
        assert!(pending.poll_once().is_err());
    }

    #[test]
    fn remote_subscription_remains_pending_between_host_turns() {
        let db = fixture();
        let query = postcard::to_allocvec(&jazz::query::Query::from("items")).unwrap();
        let mut subscription = db
            .subscribe(query.into(), Some(json!({"tier":"remote"})), None, None)
            .unwrap();
        for _ in 0..3 {
            for event in subscription.read_all().unwrap() {
                if let Either3::A(delta) = event {
                    assert!(
                        !delta.settled,
                        "an offline replica cannot claim remote coverage"
                    );
                    let (added, updated) =
                        jazz::binding_codec::decode_subscription_delta_rows(&delta.delta).unwrap();
                    assert!(added.is_empty() && updated.is_empty());
                } else {
                    panic!("absence of authority is pending, not a terminal event");
                }
            }
            db.tick().unwrap();
        }
        assert!(subscription.close());
        assert!(!subscription.close());
    }

    /// Alice receives every inserted row once across coalesced SQLite delivery turns.
    #[test]
    fn subscription_keeps_all_rows_across_host_delivery_turns() {
        let db = fixture();
        let query = postcard::to_allocvec(&jazz::query::Query::from("items")).unwrap();
        let mut subscription = db
            .subscribe(
                query.into(),
                Some(json!({"tier":"local-first"})),
                None,
                None,
            )
            .unwrap();
        subscription.read_all().unwrap();
        db.insert_test("items".into(), label_cells("first"), None)
            .unwrap();
        db.insert_test("items".into(), label_cells("second"), None)
            .unwrap();
        let mut received = delta_labels(subscription.read_all().unwrap());
        received.sort();
        assert_eq!(received, ["first", "second"]);
        assert!(subscription.read_all().unwrap().is_empty());
        db.insert_test("items".into(), label_cells("third"), None)
            .unwrap();
        assert_eq!(delta_labels(subscription.read_all().unwrap()), ["third"]);
        assert_eq!(labels(&db), ["first", "second", "third"]);
    }

    /// Binding read choices lower without widening the write durability parser.
    #[test]
    fn read_tier_names_are_separate_from_durability() {
        assert_eq!(
            core_read_tier_from_str("local-first").expect("local-first read tier"),
            jazz::db::ReadTier::LocalFirst
        );
        assert_eq!(
            core_read_tier_from_str("remote").expect("strict remote read tier"),
            jazz::db::ReadTier::Remote
        );
        for name in [
            "remote-if-possible",
            "RemoteIfPossible",
            "local-first-unless-empty",
            "LocalFirstUnlessEmpty",
            "core",
            "Core",
            "local",
            "global",
            "none",
            "invalid-tier",
        ] {
            assert_eq!(
                core_read_tier_from_str(name).unwrap_err().reason,
                format!("unknown read tier {name}"),
            );
        }
        assert!(
            host_fixture::core_durability_tier_from_str("remote").is_err(),
            "write waits must not accept read-only tier names"
        );
    }

    #[test]
    fn transaction_binding_diagnostics_use_transaction_vocabulary() {
        assert_eq!(
            unknown_transaction_kind_message("invalid"),
            "unknown transaction kind invalid"
        );
    }

    async fn jazz_server_binding(
        built: jazz_server::BuiltServer,
        app_id: jazz::tools::AppId,
    ) -> JazzServer {
        let server = jazz_server::JazzServer::from_built(
            built,
            None,
            None,
            app_id,
            jazz_server::ServerDataDir::in_memory(),
            "napi-stop-test-admin".to_owned(),
            "napi-stop-test-backend".to_owned(),
        )
        .await
        .expect("start NAPI test server");
        server_binding(server)
    }

    #[tokio::test]
    async fn jazz_server_stop_shares_success_with_concurrent_and_later_callers() {
        let app_id = jazz::tools::AppId::from_name("napi-stop-success");
        let built = jazz_server::ServerBuilder::new(app_id)
            .with_storage(jazz_server::StorageBackend::InMemory)
            .build()
            .await
            .expect("build NAPI test server");
        let server = jazz_server_binding(built, app_id).await;

        let (first, second) = tokio::join!(server.stop(), server.stop());
        assert!(first.is_ok(), "shutdown owner succeeds: {first:?}");
        assert!(
            second.is_ok(),
            "concurrent waiter shares success: {second:?}"
        );
        assert!(
            server.stop().await.is_ok(),
            "later caller replays terminal success"
        );
    }

    #[tokio::test]
    async fn jazz_server_stop_finalization_outlives_the_initiating_future() {
        let app_id = jazz::tools::AppId::from_name("napi-stop-cancelled-initiator");
        let built = jazz_server::ServerBuilder::new(app_id)
            .with_storage(jazz_server::StorageBackend::InMemory)
            .build()
            .await
            .expect("build NAPI test server");
        let server = jazz_server_binding(built, app_id).await;

        // The first poll synchronously transfers Running -> Stopping and
        // launches the independently owned finalizer. Dropping this future
        // models cancellation of the Promise which initiated `stop`.
        let mut initiator = Box::pin(server.stop());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(
            initiator.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(initiator);

        assert!(
            server.stop().await.is_ok(),
            "waiter completes the finalization detached from its initiator"
        );
        assert!(
            server.stop().await.is_ok(),
            "later caller replays the same completed outcome"
        );
    }

    #[tokio::test]
    async fn jazz_server_stop_shares_failure_with_concurrent_and_later_callers() {
        let app_id = jazz::tools::AppId::from_name("napi-stop-failure");
        let built = jazz_server::ServerBuilder::new(app_id)
            .with_storage(jazz_server::StorageBackend::InMemory)
            .with_shutdown_timeout(Duration::from_millis(1))
            .build()
            .await
            .expect("build NAPI test server");
        let active_request = built
            .state
            .shutdown
            .try_enter_app_request()
            .expect("hold request through shutdown timeout");
        let server = jazz_server_binding(built, app_id).await;

        let (first, second) = tokio::join!(server.stop(), server.stop());
        let first = first
            .expect_err("shutdown owner reports failure")
            .reason
            .clone();
        let second = second
            .expect_err("concurrent waiter reports failure")
            .reason
            .clone();
        assert_eq!(second, first);
        assert_eq!(
            first, "JazzServer shutdown failed during phase Failed",
            "binding preserves the embedded host's terminal failure"
        );
        assert_eq!(
            server
                .stop()
                .await
                .expect_err("later caller replays failure")
                .reason
                .clone(),
            first
        );
        drop(active_request);
    }
    use jazz::account_registry::AccountId;
    use jazz::db::{DbIdentity as CoreDbIdentity, SubscriptionEvent as CoreSubscriptionEvent};
    use jazz::ids::{
        AuthorSubject as CoreAuthorSubject, NodeUuid as CoreNodeUuid, RowAuthor as CoreRowAuthor,
        RowUuid as CoreRowUuid,
    };
    use jazz::protocol::ReadViewSpec as CoreReadViewSpec;
    use jazz::records::Value as CoreValue;
    use jazz::records::{RecordDescriptor, ValueType};
    use jazz::tools::OpenTransactionId as CoreOpenTransactionId;
    use jazz::tools::{
        ColumnType, PolicyExpr, Schema, SchemaBuilder, TableName, TablePolicies, TableSchema, Value,
    };
    use jazz::tx::DurabilityTier;
    use napi::bindgen_prelude::Uint8Array;
    use napi::bindgen_prelude::{BigInt, Either, Either3};
    use serde_json::json;
    use std::cell::RefCell;

    /// An initialized raw streaming handle must relinquish its durable
    /// retainer when JavaScript drops it. The zero-age maintenance pass is
    /// deliberately run only after a binding tick, so this distinguishes
    /// asynchronous Drop cleanup from the TTL backstop.
    #[test]
    fn dropped_initialized_stream_is_cleaned_by_the_binding_tick() {
        let source = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("payload", ColumnType::Bytea)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build();
        let schema = serde_json::to_vec(&source).expect("encode streaming schema");
        let author = CoreAuthorSubject::for_test_bytes([0xd1; 16]);
        let config = encode_persistent_open_config(author);
        let db = NapiDb::open_memory(Uint8Array::from(schema), Uint8Array::from(config), None)
            .expect("open streaming fixture");
        let descriptor = RecordDescriptor::new(Vec::<(String, ValueType)>::new());
        let raw = descriptor.create(&[]).expect("encode streaming cells");
        let cells = postcard::to_allocvec(&(descriptor, raw)).expect("encode streaming cells");
        let mut stream = db
            .begin_streaming_mutation(
                "items".to_owned(),
                Uint8Array::from(vec![0xd2; 16]),
                Uint8Array::from(cells.clone()),
                "payload".to_owned(),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("begin streaming mutation");
        stream
            .push(Uint8Array::from(b"orphaned".to_vec()))
            .expect("stage streaming bytes");
        drop(stream);
        std::thread::sleep(Duration::from_millis(2));

        db.set_large_value_staging_policy(9_000_000_000_000_000.0, 1.0, Some(0.0))
            .expect("set immediate expiry backstop");
        db.tick().expect("binding tick");
        assert_eq!(
            db.evict_expired_staged_large_values()
                .expect("maintenance pass"),
            0,
            "Drop cleanup must win before the TTL backstop"
        );
        let mut retained = db
            .begin_streaming_mutation(
                "items".to_owned(),
                Uint8Array::from(vec![0xd3; 16]),
                Uint8Array::from(cells),
                "payload".to_owned(),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("begin retained streaming mutation");
        retained
            .push(Uint8Array::from(b"closed-with-owner".to_vec()))
            .expect("stage retained streaming bytes");
        assert!(
            matches!(db.close().expect("close owner"), Either::A(_)),
            "in-memory owner close completes after cleanup drain"
        );
        assert!(retained.push(Uint8Array::from(b"late".to_vec())).is_err());
        assert!(!retained.abort().expect("closed abort is idempotent"));
        assert!(retained.finish_test().is_err());
        assert!(
            matches!(db.close().expect("repeat owner close"), Either::A(_)),
            "repeated close replays the completed result"
        );
    }

    #[test]
    fn schema_view_lifecycle_retires_only_view_streams() {
        let source = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("payload", ColumnType::Bytea)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build();
        let schema = serde_json::to_vec(&source).expect("encode view schema");
        let config = encode_persistent_open_config(CoreAuthorSubject::for_test_bytes([0xd4; 16]));
        let owner = NapiDb::open_memory(
            Uint8Array::from(schema.clone()),
            Uint8Array::from(config),
            None,
        )
        .expect("open view fixture");
        let view = owner
            .register_schema(Uint8Array::from(schema.clone()))
            .expect("register schema view");
        let descriptor = RecordDescriptor::new(Vec::<(String, ValueType)>::new());
        let raw = descriptor.create(&[]).expect("encode view cells");
        let cells = postcard::to_allocvec(&(descriptor, raw)).expect("encode view cells");
        let mut view_stream = view
            .begin_streaming_mutation(
                "items".to_owned(),
                Uint8Array::from(vec![0xd5; 16]),
                Uint8Array::from(cells.clone()),
                "payload".to_owned(),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("begin view stream");
        view_stream
            .push(Uint8Array::from(b"view".to_vec()))
            .expect("stage view stream");
        let mut owner_stream = owner
            .begin_streaming_mutation(
                "items".to_owned(),
                Uint8Array::from(vec![0xd6; 16]),
                Uint8Array::from(cells.clone()),
                "payload".to_owned(),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("begin owner stream");
        owner_stream
            .push(Uint8Array::from(b"owner".to_vec()))
            .expect("stage owner stream");

        drop(view);
        assert!(
            view_stream
                .push(Uint8Array::from(b"late".to_vec()))
                .is_err()
        );
        assert!(!view_stream.abort().expect("dropped view abort"));
        owner_stream
            .push(Uint8Array::from(b"still-open".to_vec()))
            .expect("view drop must preserve sibling stream admission");
        assert!(owner_stream.abort().expect("abort owner stream"));

        let closed_view = owner
            .register_schema(Uint8Array::from(schema.clone()))
            .expect("register explicitly closed view");
        let mut closed_stream = closed_view
            .begin_streaming_mutation(
                "items".to_owned(),
                Uint8Array::from(vec![0xd9; 16]),
                Uint8Array::from(cells.clone()),
                "payload".to_owned(),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("begin explicitly closed view stream");
        closed_stream
            .push(Uint8Array::from(b"closed-view".to_vec()))
            .expect("stage explicitly closed view stream");
        assert!(
            matches!(
                closed_view.close().expect("close schema view"),
                Either::A(_)
            ),
            "explicit view close drains its admitted stream"
        );
        assert!(
            closed_stream
                .push(Uint8Array::from(b"late".to_vec()))
                .is_err()
        );
        assert!(!closed_stream.abort().expect("explicitly closed view abort"));
        assert!(
            matches!(
                closed_view.close().expect("repeat close schema view"),
                Either::A(_)
            ),
            "repeated view close replays its result"
        );

        owner
            .set_large_value_staging_policy(1.0, 60_000.0, None)
            .expect("set restrictive ingress policy");
        let mut failed_stream = owner
            .begin_streaming_mutation(
                "items".to_owned(),
                Uint8Array::from(vec![0xd7; 16]),
                Uint8Array::from(cells),
                "payload".to_owned(),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("begin failed stream");
        let push_error = failed_stream
            .push(Uint8Array::from(vec![
                0xd8;
                jazz::large_values::LEAF_MAX_BYTES + 1
            ]))
            .expect_err("ingress rejection must be reported");
        assert!(push_error.reason.contains("rate limit"));
        assert!(
            failed_stream
                .push(Uint8Array::from(b"retry".to_vec()))
                .is_err(),
            "a push failure transitions the stream to cleanup-only"
        );
        assert!(
            failed_stream.finish_test().is_err(),
            "cleanup-only streams cannot publish"
        );
        assert!(failed_stream.abort().expect("abort failed stream"));

        assert!(
            matches!(owner.close().expect("close owner"), Either::A(_)),
            "owner close remains usable after view drop and push failure"
        );
    }

    /// Alice's two view closes wait for the shared drain while fencing their own writes and leaving the owner usable.
    #[test]
    fn concurrent_cleanup_drains_share_the_host_gate_and_retire_views() {
        // White-box because this holds the existing production drain mutex;
        // no synthetic queue or test-only runtime gate is installed.
        let owner = fixture();
        let schema = fixture_schema();
        let first = owner.register_schema(schema.clone().into()).unwrap();
        let second = owner.register_schema(schema.into()).unwrap();
        let inner = owner.open_inner().unwrap();
        let lock = block_on(inner.host.cleanup_drain.lock());
        let first_close = match first.close().unwrap() {
            Either::B(pending) => pending,
            Either::A(_) => panic!("drain locked"),
        };
        let second_close = match second.close().unwrap() {
            Either::B(pending) => pending,
            Either::A(_) => panic!("drain locked"),
        };
        assert!(
            first
                .insert_test("items".into(), label_cells("closed"), None)
                .is_err()
        );
        assert!(
            second
                .insert_test("items".into(), label_cells("closed"), None)
                .is_err()
        );
        owner.tick().unwrap();
        assert!(first_close.poll().unwrap().is_none());
        assert!(second_close.poll().unwrap().is_none());
        drop(lock);
        assert!(first_close.poll().unwrap().is_some());
        assert!(second_close.poll().unwrap().is_some());
        owner
            .insert_test("items".into(), label_cells("owner remains live"), None)
            .unwrap();
        assert_eq!(labels(&owner), ["owner remains live"]);
    }

    /// SQLite admits direct writes synchronously, in call order. Each NAPI
    /// receipt refers to its actual committed version and remains waitable after
    /// later writes; there is no deferred storage queue or placeholder identity.
    #[test]
    fn direct_sqlite_mutations_keep_call_order_and_distinct_receipts() {
        let owner = fixture();
        let db = owner.binding().unwrap();
        let inserted = db
            .insert("items", &label_cells("before"), &JsonValue::Null)
            .unwrap();
        let mut writes = Vec::new();
        for label in ["first", "second"] {
            let write = db
                .update(
                    "items",
                    inserted.row_id.0.as_bytes(),
                    &label_cells(label),
                    &JsonValue::Null,
                )
                .unwrap();
            assert!(write.seq > inserted.seq);
            assert_eq!(
                db.write_state(&write).unwrap().durability,
                DurabilityTier::Local
            );
            writes.push(write);
        }
        assert_ne!(writes[0].tx_id, writes[1].tx_id);
        for write in &writes {
            block_on(db.wait_for_write(write, "local").unwrap()).unwrap();
        }
        assert_eq!(labels(&owner), ["second"]);
    }

    /// An empty SQLite update reuses the row's current committed version.
    /// NAPI exposes that exact transaction and wait result without minting a
    /// queued request identity or creating a global no-op alias.
    #[test]
    fn napi_noop_receipt_reuses_the_existing_sqlite_transaction() {
        let owner = fixture();
        let db = owner.binding().unwrap();
        let initial = db
            .insert("items", &label_cells("before"), &JsonValue::Null)
            .unwrap();
        let high_water = db.db.seq_high_water();
        let unchanged = db
            .update(
                "items",
                initial.row_id.0.as_bytes(),
                &empty_cells(),
                &JsonValue::Null,
            )
            .unwrap();
        assert_eq!(unchanged.seq, initial.seq);
        assert_eq!(unchanged.tx_id, initial.tx_id);
        assert_eq!(
            db.db.seq_high_water(),
            high_water,
            "no placeholder transaction was allocated"
        );
        block_on(db.wait_for_write(&unchanged, "local").unwrap()).unwrap();
        assert_eq!(
            db.write_state(&unchanged).unwrap(),
            db.write_state(&initial).unwrap()
        );
        assert_eq!(labels(&owner), ["before"]);
    }

    /// The sync NAPI mutation ABI makes one resident admission turn before it
    /// returns a `Write`. An operation that fails in that turn must not hand
    /// callers a seemingly committed receipt, and ordinary/partial-value
    /// entrypoints must preserve the same boundary.
    #[test]
    fn direct_insert_and_large_value_update_surface_memory_first_turn_failures() {
        let source = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("label", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                    ),
            )
            .build();
        let schema = serde_json::to_vec(&source).expect("serialize NAPI policy fixture");
        let author = CoreAuthorSubject::for_test_bytes([0xc4; 16]);
        let db = NapiDb::open_memory(
            Uint8Array::from(schema),
            Uint8Array::from(encode_persistent_open_config(author)),
            None,
        )
        .expect("open memory NAPI fixture");
        let label_cells = |value: &str| {
            let descriptor = RecordDescriptor::new([("label", ValueType::String)]);
            let raw = descriptor
                .create(&[CoreValue::String(value.to_owned())])
                .expect("encode label fixture cell");
            Uint8Array::from(
                jazz::binding_codec::encode_named_cells(&jazz::records::OwnedRecord::new(
                    raw, descriptor,
                ))
                .expect("encode named NAPI cells"),
            )
        };

        let insert_error =
            match db.insert_test("missing_table".to_owned(), label_cells("before"), None) {
                Ok(_) => panic!("ordinary insert must surface its first-turn failure"),
                Err(error) => error,
            };
        assert!(
            insert_error.reason.contains("missing_table"),
            "ordinary insert retains its core failure diagnostic: {}",
            insert_error.reason
        );

        let large_update_error = match db.update_large_test(
            "missing_table".to_owned(),
            Uint8Array::from(vec![0xc5; 16]),
            label_cells("after"),
            json!([]),
            None,
        ) {
            Ok(_) => panic!("partial-value entrypoint must surface its first-turn failure"),
            Err(error) => error,
        };
        assert!(
            large_update_error.reason.contains("missing_table"),
            "partial-value update retains its core failure diagnostic: {}",
            large_update_error.reason
        );
    }

    #[test]
    fn identity_claim_ingress_namespaces_provider_values_and_derives_reserved_fields() {
        let author = CoreAuthorSubject::authenticated("https://issuer.example", "alice")
            .unwrap()
            .with_account(AccountId(uuid::Uuid::from_bytes([0xa2; 16])));
        let claims = host_fixture::core_claims_from_json(
            author,
            Some(json!({
                "user": "forged-user",
                "iss": "forged-issuer",
                "sub": "provider-subject",
                "custom": "provider-value",
                "authMode": "local-first",
            })),
        )
        .expect("NAPI claims are scalar provider data");

        assert_eq!(
            claims.get("user"),
            Some(
                &CoreRowAuthor::from_persisted_subject(author)
                    .expect("admitted row author")
                    .to_value()
            ),
            "session.user must come from the supplied structured author"
        );
        assert_eq!(
            claims.get("authMode"),
            Some(&CoreValue::String("external".to_owned())),
            "the public NAPI ingress derives external auth mode"
        );
        for (name, value) in [
            ("user", "forged-user"),
            ("custom", "provider-value"),
            ("authMode", "local-first"),
        ] {
            assert_eq!(
                claims.get(&jazz::query::provider_claim_key(name)),
                Some(&CoreValue::String(value.to_owned())),
                "raw provider {name} stays below session.claims"
            );
        }
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("iss")),
            Some(&CoreValue::String("https://issuer.example".to_owned())),
            "session.claims.iss must agree with the admitted author rather than a supplied claim"
        );
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("sub")),
            Some(&CoreValue::String("alice".to_owned())),
            "session.claims.sub must agree with the admitted author rather than a supplied claim"
        );
    }

    #[test]
    fn identity_claim_ingress_preserves_nested_json_and_scalar_prototype_names() {
        let author = CoreAuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
        let claims = host_fixture::core_claims_from_json(
            author,
            Some(json!({
                "profile": { "handler_only": true },
                "mixed": ["editor", { "nested": true }],
                "__proto__": "safe",
                "constructor": "also-safe",
            })),
        )
        .expect("recursive metadata must not reject NAPI admission");

        assert!(claims.contains_key(&jazz::query::provider_claim_key("profile")));
        assert!(claims.contains_key(&jazz::query::provider_claim_key("mixed")));
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("__proto__")),
            Some(&CoreValue::String("safe".to_owned()))
        );
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("constructor")),
            Some(&CoreValue::String("also-safe".to_owned()))
        );
    }

    #[test]
    fn identity_claim_ingress_derives_first_party_auth_mode_from_verified_author() {
        let account = AccountId(uuid::Uuid::from_bytes([0xa3; 16]));
        let author = CoreAuthorSubject::from_canonical(
            &serde_json::to_string(&(
                account.0.to_string(),
                CoreAuthorSubject::LOCAL_FIRST_ISSUER,
                "alice",
            ))
            .expect("serialize canonical admitted first-party author"),
        )
        .expect("parse canonical admitted first-party author");
        let claims =
            host_fixture::core_claims_from_json(author, Some(json!({ "authMode": "external" })))
                .expect("NAPI claims are scalar provider data");

        assert_eq!(
            claims.get("user"),
            Some(
                &CoreRowAuthor::from_persisted_subject(author)
                    .expect("admitted row author")
                    .to_value()
            )
        );
        assert_eq!(
            claims.get("authMode"),
            Some(&CoreValue::String("local-first".to_owned())),
            "a provider claim must not override the mode verified by the native open ABI"
        );
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("authMode")),
            Some(&CoreValue::String("external".to_owned()))
        );
    }

    #[test]
    fn public_author_ingress_requires_a_verified_self_signed_open_proof() {
        // This exercises the binding's raw postcard configuration boundary
        // directly. A normal DB operation cannot construct an invalid native
        // open envelope, so this internal receipt is required to prove that
        // reserved identity selection stays closed at that ingress.
        let external = br#"["https://issuer.example","alice"]"#;
        assert_eq!(
            core_author_id_from_bytes(external).unwrap().canonical(),
            std::str::from_utf8(external).unwrap()
        );
        for issuer in [
            CoreAuthorSubject::SYSTEM_ISSUER,
            CoreAuthorSubject::LOCAL_FIRST_ISSUER,
            CoreAuthorSubject::STATIC_BEARER_ISSUER,
            CoreAuthorSubject::ANONYMOUS_ISSUER,
        ] {
            let canonical = serde_json::to_vec(&(issuer, "caller")).unwrap();
            assert!(
                core_author_id_from_bytes(&canonical).is_err(),
                "issuer {issuer}"
            );
        }

        #[derive(serde::Serialize)]
        struct EncodedIdentity {
            node: CoreNodeUuid,
            author: CoreAuthorSubject,
        }
        #[derive(serde::Serialize)]
        struct EncodedConfig {
            identity: EncodedIdentity,
            row_id_seed: Option<u64>,
            history_complete: bool,
            initial_sync_flush_every: Option<u32>,
            backend_credential: Option<String>,
        }
        let encode_config = |author, backend_credential| {
            postcard::to_allocvec(&EncodedConfig {
                identity: EncodedIdentity {
                    node: CoreNodeUuid::from_bytes([7; 16]),
                    author,
                },
                row_id_seed: None,
                history_complete: false,
                initial_sync_flush_every: None,
                backend_credential,
            })
            .unwrap()
        };
        for issuer in [
            CoreAuthorSubject::SYSTEM_ISSUER,
            CoreAuthorSubject::LOCAL_FIRST_ISSUER,
            CoreAuthorSubject::STATIC_BEARER_ISSUER,
            CoreAuthorSubject::ANONYMOUS_ISSUER,
        ] {
            let author = if issuer == CoreAuthorSubject::SYSTEM_ISSUER {
                CoreAuthorSubject::SYSTEM
            } else {
                CoreAuthorSubject::from_canonical(
                    &serde_json::to_string(&(issuer, "caller")).unwrap(),
                )
                .unwrap()
            };
            let bytes = encode_config(author, None);
            assert!(
                NapiDb::open_memory(fixture_schema().into(), bytes.clone().into(), None).is_err()
                    && NapiDb::open_memory_as_backend(fixture_schema().into(), bytes.into())
                        .is_err(),
                "both raw open paths must reject {issuer} without a proof"
            );
        }

        let external_author =
            CoreAuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
        let bytes = encode_config(external_author, None);
        let external_config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
        assert_eq!(
            core_open_identity(&external_config, None).unwrap().author,
            external_author,
            "an old TS caller uses only this ordinary raw-open path"
        );
        assert_ne!(
            core_open_identity(&external_config, None).unwrap().author,
            CoreAuthorSubject::SYSTEM,
            "the ordinary raw-open path cannot become SYSTEM"
        );
        assert_eq!(
            core_open_backend_identity(&external_config).unwrap().author,
            CoreAuthorSubject::SYSTEM,
            "only the separate backend-open path may intentionally derive SYSTEM"
        );
        NapiDb::open_memory_as_backend(
            Uint8Array::from(br#"{"tables":{}}"#.to_vec()),
            Uint8Array::from(encode_config(external_author, None)),
        )
        .expect(
            "an explicit local backend open derives SYSTEM without requiring upstream verification",
        );

        for credential in ["arbitrary-backend-secret", "malformed.backend.credential"] {
            let bytes = encode_config(external_author, Some(credential.to_owned()));
            assert!(
                decode_core_open_args(&fixture_schema(), &bytes).is_err(),
                "the shared ingress must reject {credential:?} before any open"
            );
            assert!(NapiDb::open_memory_as_backend(fixture_schema().into(), bytes.into()).is_err());
        }

        let memory_credential =
            encode_config(external_author, Some("arbitrary-backend-secret".to_owned()));
        assert!(
            NapiDb::open_memory(
                Uint8Array::from(b"{}".to_vec()),
                Uint8Array::from(memory_credential),
                None,
            )
            .is_err(),
            "openMemory must reject an unverified backend credential before opening the DB"
        );

        let persistent_path = std::env::temp_dir().join(format!(
            "jazz-unverified-backend-open-{}",
            std::process::id()
        ));
        assert!(
            !persistent_path.exists(),
            "test path must be fresh: {}",
            persistent_path.display()
        );
        let persistent_credential = encode_config(
            external_author,
            Some("malformed.backend.credential".to_owned()),
        );
        let result = NapiDb::try_open_persistent(
            persistent_path.to_string_lossy().into_owned(),
            Uint8Array::from(b"{}".to_vec()),
            Uint8Array::from(persistent_credential),
            None,
        );
        let created_storage = persistent_path.exists();
        if created_storage {
            std::fs::remove_dir_all(&persistent_path).unwrap();
        }
        assert!(
            result.is_err(),
            "openPersistent must reject an unverified backend credential"
        );
        assert!(
            !created_storage,
            "openPersistent must reject an unverified backend credential before creating storage"
        );

        for issuer in [
            jazz::tools::identity::LOCAL_FIRST_ISSUER,
            jazz::tools::identity::ANONYMOUS_ISSUER,
        ] {
            let token = jazz::tools::identity::mint_jazz_self_signed_token(
                &[issuer.len() as u8; 32],
                issuer,
                "proof-app",
                60,
            )
            .unwrap();
            let verified =
                jazz::tools::identity::verify_jazz_self_signed_proof(&token, "proof-app").unwrap();
            let claimed_author = serde_json::to_string(&(issuer, verified.user_id)).unwrap();
            let bytes = encode_config(external_author, None);
            let config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
            let proof = CoreSelfSignedClientProof {
                token,
                app_id: "proof-app".to_owned(),
                claimed_author: claimed_author.clone(),
            };
            assert_eq!(
                core_open_identity(&config, Some(&proof))
                    .unwrap()
                    .author
                    .canonical(),
                claimed_author,
                "the separate proof constructor derives {issuer}, never backend/SYSTEM identity"
            );
            let bytes = encode_config(external_author, Some("misdecode-me".to_owned()));
            let config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
            assert_eq!(
                core_open_identity(&config, Some(&proof))
                    .unwrap()
                    .author
                    .canonical(),
                claimed_author,
                "a proof-bearing constructor never treats a config field as backend authority"
            );
        }

        let token = jazz::tools::identity::mint_jazz_self_signed_token(
            &[9; 32],
            jazz::tools::identity::LOCAL_FIRST_ISSUER,
            "proof-app",
            60,
        )
        .unwrap();
        let verified =
            jazz::tools::identity::verify_jazz_self_signed_proof(&token, "proof-app").unwrap();
        let claimed_author =
            serde_json::to_string(&(jazz::tools::identity::LOCAL_FIRST_ISSUER, verified.user_id))
                .unwrap();
        let bytes = encode_config(external_author, None);
        let config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
        let (signing_input, signature_b64) = token.rsplit_once('.').unwrap();
        let mut signature = URL_SAFE_NO_PAD.decode(signature_b64).unwrap();
        assert_eq!(signature.len(), 64);
        signature[0] ^= 1;
        let proof = CoreSelfSignedClientProof {
            token: format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(signature)),
            app_id: "proof-app".to_owned(),
            claimed_author: claimed_author.clone(),
        };
        assert!(
            core_open_identity(&config, Some(&proof)).is_err(),
            "same-length, valid-base64 signature tampering must fail"
        );
        let persistent_path = std::env::temp_dir().join(format!(
            "jazz-invalid-self-signed-open-{}",
            std::process::id()
        ));
        assert!(
            !persistent_path.exists(),
            "test path must be fresh: {}",
            persistent_path.display()
        );
        let result = NapiDb::try_open_persistent_with_self_signed_proof(
            persistent_path.to_string_lossy().into_owned(),
            Uint8Array::from(b"{}".to_vec()),
            Uint8Array::from(encode_config(external_author, None)),
            proof.token.clone(),
            proof.app_id.clone(),
            proof.claimed_author.clone(),
            None,
        );
        let created_storage = persistent_path.exists();
        if created_storage {
            std::fs::remove_dir_all(&persistent_path).unwrap();
        }
        assert!(
            result.is_err(),
            "invalid proof must reject the persistent open"
        );
        assert!(
            !created_storage,
            "invalid proof must be rejected before RocksDB creates its data directory"
        );

        let bytes = encode_config(external_author, None);
        let config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
        let proof = CoreSelfSignedClientProof {
            token: token.clone(),
            app_id: "wrong-app".to_owned(),
            claimed_author: claimed_author.clone(),
        };
        assert!(
            core_open_identity(&config, Some(&proof)).is_err(),
            "wrong audience must fail"
        );

        let bytes = encode_config(external_author, None);
        let config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
        let proof = CoreSelfSignedClientProof {
            token,
            app_id: "proof-app".to_owned(),
            claimed_author: r#"["urn:jazz:local-first","another-subject"]"#.to_owned(),
        };
        assert!(
            core_open_identity(&config, Some(&proof)).is_err(),
            "claimed author must match the signed issuer and subject"
        );

        let expired = jazz::tools::identity::mint_jazz_self_signed_token_at(
            &[10; 32],
            jazz::tools::identity::LOCAL_FIRST_ISSUER,
            "proof-app",
            1,
            0,
        )
        .unwrap();
        let bytes = encode_config(external_author, None);
        let config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
        let proof = CoreSelfSignedClientProof {
            token: expired,
            app_id: "proof-app".to_owned(),
            claimed_author,
        };
        assert!(
            core_open_identity(&config, Some(&proof)).is_err(),
            "expired proof must fail"
        );
    }

    /// The native binding mints backend attribution only from its distinct
    /// backend-open constructor. A backend can credit Alice while retaining
    /// SYSTEM admission; raw opens, transaction identity overrides, and branch
    /// streaming combinations must all fail before a write is staged.
    ///
    /// ```text
    /// openMemoryAsBackend ──attribution=alice──► accepted NAPI write
    /// openMemory / raw session ──attribution=alice──► rejected at binding edge
    /// ```
    #[test]
    fn native_backend_attribution_capability_is_explicit_and_non_mixable() {
        #[derive(serde::Serialize)]
        struct EncodedIdentity {
            node: CoreNodeUuid,
            author: CoreAuthorSubject,
        }
        #[derive(serde::Serialize)]
        struct EncodedConfig {
            identity: EncodedIdentity,
            row_id_seed: Option<u64>,
            history_complete: bool,
            initial_sync_flush_every: Option<u32>,
            backend_credential: Option<String>,
        }

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
        let schema = serde_json::to_vec(&source).unwrap();
        let config = postcard::to_allocvec(&EncodedConfig {
            identity: EncodedIdentity {
                node: CoreNodeUuid::from_bytes([0xb3; 16]),
                author: CoreAuthorSubject::for_test_bytes([0xb3; 16]),
            },
            row_id_seed: Some(0xb3),
            history_complete: false,
            initial_sync_flush_every: None,
            backend_credential: None,
        })
        .unwrap();
        let alice = CoreAuthorSubject::for_test_bytes([0xa1; 16]);
        let alice_bytes = alice.canonical().as_bytes().to_vec();
        let descriptor = RecordDescriptor::new([("label", ValueType::String)]);
        let raw = descriptor
            .create(&[CoreValue::String("credited to alice".to_owned())])
            .unwrap();
        let cells = jazz::binding_codec::encode_named_cells(&jazz::records::OwnedRecord::new(
            raw, descriptor,
        ))
        .unwrap();

        let backend = NapiDb::open_memory_as_backend(
            Uint8Array::from(schema.clone()),
            Uint8Array::from(config.clone()),
        )
        .unwrap();
        let write = backend
            .insert_test(
                "items".to_owned(),
                Uint8Array::from(cells.clone()),
                Some(InsertOptions {
                    row_id: Some(Uint8Array::from(vec![0xb4; 16])),
                    author: None,
                    attribution: Some(Uint8Array::from(alice_bytes.clone())),
                    branch: None,
                    updated_at_ms: None,
                }),
            )
            .expect("the explicit backend constructor mints attribution capability");
        assert_eq!(write.row_id, CoreRowUuid::from_bytes([0xb4; 16]));

        let ordinary =
            NapiDb::open_memory(Uint8Array::from(schema), Uint8Array::from(config), None).unwrap();
        let err = match ordinary.insert_test(
            "items".to_owned(),
            Uint8Array::from(cells),
            Some(InsertOptions {
                row_id: Some(Uint8Array::from(vec![0xb4; 16])),
                author: None,
                attribution: Some(Uint8Array::from(alice_bytes.clone())),
                branch: None,
                updated_at_ms: None,
            }),
        ) {
            Ok(_) => panic!("a raw NAPI open must never gain attribution authority"),
            Err(err) => err,
        };
        assert!(err.reason.contains("explicit backend runtime"));

        let err = ordinary
            .begin_transaction(
                CoreOpenTransactionId::new().to_string(),
                "mergeable".to_owned(),
                None,
                Some(Uint8Array::from(alice_bytes.clone())),
            )
            .expect_err("ordinary transactions cannot claim backend attribution");
        assert!(err.reason.contains("explicit backend runtime"));

        let err = match backend.begin_streaming_mutation(
            "items".to_owned(),
            Uint8Array::from(vec![0xb5; 16]),
            Uint8Array::from(Vec::new()),
            "label".to_owned(),
            None,
            Some(Uint8Array::from(alice_bytes)),
            Some(Uint8Array::from(alice.canonical().as_bytes().to_vec())),
            None,
            None,
            None,
        ) {
            Ok(_) => panic!("streaming attribution cannot override SYSTEM admission"),
            Err(err) => err,
        };
        assert!(
            err.reason
                .contains("cannot contain both author and attribution")
        );

        let err = match backend.begin_streaming_mutation(
            "items".to_owned(),
            Uint8Array::from(vec![0xb6; 16]),
            empty_cells(),
            "label".to_owned(),
            None,
            None,
            Some(Uint8Array::from(alice.canonical().as_bytes().to_vec())),
            None,
            Some(
                serde_json::to_value(jazz::protocol::BranchSelector::new([(
                    "branch",
                    CoreValue::String("draft".into()),
                )]))
                .unwrap(),
            ),
            None,
        ) {
            Ok(_) => panic!("attributed streaming branch writes must fail closed"),
            Err(err) => err,
        };
        assert!(err.reason.contains("do not support branch writes"));
    }

    #[test]
    fn javascript_numeric_claims_preserve_safe_integers_and_fail_closed_when_lossy() {
        assert_eq!(
            core_claim_value_from_json(json!(7)).unwrap().unwrap(),
            CoreValue::U64(7)
        );
        assert_eq!(
            core_claim_value_from_json(json!(-7)).unwrap().unwrap(),
            CoreValue::I64(-7)
        );
        assert_eq!(
            core_claim_value_from_json(serde_json::Value::Number(
                serde_json::Number::from_f64(7.0).unwrap()
            ))
            .unwrap()
            .unwrap(),
            CoreValue::U64(7),
            "JS-number deserialization must agree with integer JSON"
        );
        assert_eq!(
            core_claim_value_from_json(json!(7.5)).unwrap().unwrap(),
            CoreValue::F64(7.5)
        );
        assert_eq!(
            core_claim_value_from_json(json!(9_007_199_254_740_992_u64))
                .unwrap()
                .unwrap(),
            CoreValue::F64(9_007_199_254_740_992.0),
            "integers beyond Number.MAX_SAFE_INTEGER must not participate in integer policy matching"
        );
        assert_eq!(
            core_claim_value_from_json(json!(-9_007_199_254_740_992_i64))
                .unwrap()
                .unwrap(),
            CoreValue::F64(-9_007_199_254_740_992.0)
        );
    }

    #[test]
    fn javascript_u64_boundaries_reject_lossy_or_invalid_numbers() {
        assert_eq!(jazz::binding_args::checked_u64(42.0, "value").unwrap(), 42);
        for value in [
            -1.0,
            f64::NAN,
            f64::INFINITY,
            1.5,
            (jazz::tools::policy_claims::MAX_SAFE_JS_INTEGER + 1) as f64,
        ] {
            assert!(
                jazz::binding_args::checked_u64(value, "value").is_err(),
                "{value:?}"
            );
        }
    }

    #[test]
    fn write_option_timestamps_reject_lossy_javascript_numbers() {
        assert!(
            core_insert_options(Some(InsertOptions {
                row_id: None,
                author: None,
                attribution: None,
                branch: None,
                updated_at_ms: Some(1.5),
            }))
            .is_err()
        );
        assert!(
            core_update_options(Some(UpdateOptions {
                author: None,
                attribution: None,
                head: None,
                base: None,
                updated_at_ms: Some(f64::NAN),
            }))
            .is_err()
        );
        assert!(binding_host::upsert_options(&json!({"updatedAtMs":-1.0})).is_err());
        assert!(
            core_restore_options(Some(RestoreOptions {
                author: None,
                attribution: None,
                branch: None,
                updated_at_ms: Some((jazz::tools::policy_claims::MAX_SAFE_JS_INTEGER + 1) as f64),
            }))
            .is_err()
        );
    }

    #[test]
    fn javascript_upsert_rejects_removed_branch_property_by_presence() {
        let error = binding_host::upsert_options(&json!({"branch":null})).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("option `branch` is not supported; use `head`")
        );
        let head = serde_json::to_value(jazz::protocol::BranchSelector::new([(
            "branch",
            CoreValue::String("draft".into()),
        )]))
        .unwrap();
        let parsed = binding_host::upsert_options(&json!({"head":head})).unwrap();
        assert!(matches!(
            parsed.target,
            jazz::db::WriteTarget::BranchView { .. }
        ));
    }

    // The capability is binding-local, so exercise native ABI ingress directly.
    #[test]
    fn native_local_first_session_admission_is_exact_and_runtime_scoped() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("items").column("label", ColumnType::Text))
            .build();
        let schema_bytes = || Uint8Array::from(serde_json::to_vec(&schema).unwrap());
        let config = || {
            Uint8Array::from(encode_persistent_open_config(
                CoreAuthorSubject::for_test_bytes([0x92; 16]),
            ))
        };
        let backend = NapiDb::open_memory_as_backend(schema_bytes(), config()).unwrap();
        let other = NapiDb::open_memory_as_backend(schema_bytes(), config()).unwrap();
        let ordinary = NapiDb::open_memory(schema_bytes(), config(), None).unwrap();
        let app_id = "native-session-admission";
        let token = jazz::tools::identity::mint_jazz_self_signed_token(
            &[0x93; 32],
            jazz::tools::identity::LOCAL_FIRST_ISSUER,
            app_id,
            60,
        )
        .unwrap();
        let verified =
            jazz::tools::identity::verify_jazz_self_signed_proof(&token, app_id).unwrap();
        let app = jazz::tools::AppId::from_name(app_id);
        let account =
            jazz::account_registry::local_first_account_id(*app.uuid(), &verified.user_id);
        let author = CoreAuthorSubject::from_canonical(
            &serde_json::to_string(&(jazz::tools::identity::LOCAL_FIRST_ISSUER, &verified.user_id))
                .unwrap(),
        )
        .unwrap()
        .with_account(account);
        let canonical = author.canonical().to_owned();
        let bytes = || Uint8Array::from(canonical.as_bytes().to_vec());
        assert!(backend.set_identity_claims(bytes(), None).is_err());
        assert!(
            ordinary
                .admit_local_first_session(token.clone(), app_id.into(), canonical.clone())
                .is_err()
        );
        assert!(
            backend
                .admit_local_first_session(token.clone(), "wrong-app".into(), canonical.clone())
                .is_err()
        );
        assert!(backend.set_identity_claims(bytes(), None).is_err());
        let wrong_account = author.with_account(jazz::account_registry::local_first_account_id(
            *app.uuid(),
            "wrong-user",
        ));
        assert!(
            backend
                .admit_local_first_session(
                    token.clone(),
                    app_id.into(),
                    wrong_account.canonical().into()
                )
                .is_err()
        );
        for claimed in [
            serde_json::to_string(&(jazz::tools::identity::LOCAL_FIRST_ISSUER, &verified.user_id))
                .unwrap(),
            CoreAuthorSubject::SYSTEM.canonical().to_owned(),
            serde_json::to_string(&(jazz::tools::identity::ANONYMOUS_ISSUER, &verified.user_id))
                .unwrap(),
        ] {
            assert!(
                backend
                    .admit_local_first_session(token.clone(), app_id.into(), claimed)
                    .is_err()
            );
        }
        backend
            .admit_local_first_session(token.clone(), app_id.into(), canonical.clone())
            .unwrap();
        backend.set_identity_claims(bytes(), None).unwrap();
        assert!(other.set_identity_claims(bytes(), None).is_err());
        assert!(ordinary.set_identity_claims(bytes(), None).is_err());
        assert!(
            backend
                .admit_local_first_session(
                    "forged.token.proof".into(),
                    app_id.into(),
                    canonical.clone()
                )
                .is_err()
        );
        backend.set_identity_claims(bytes(), None).unwrap();
        let batch = CoreOpenTransactionId::new().to_string();
        backend
            .begin_transaction(batch.clone(), "mergeable".into(), None, Some(bytes()))
            .unwrap();
        let descriptor = RecordDescriptor::new([("label", ValueType::String)]);
        let raw = descriptor
            .create(&[CoreValue::String("admitted Tx".to_owned())])
            .unwrap();
        let cells = jazz::binding_codec::encode_named_cells(&jazz::records::OwnedRecord::new(
            raw, descriptor,
        ))
        .unwrap();
        backend
            .insert_in_transaction(
                batch.clone(),
                "items".into(),
                Uint8Array::from(cells),
                Some(InsertOptions {
                    row_id: None,
                    author: None,
                    attribution: None,
                    branch: None,
                    updated_at_ms: None,
                }),
            )
            .unwrap();
        backend
            .commit_test(batch, Some("mergeable".into()))
            .unwrap();
        // A schema view shares the exact owner's admission map.
        let view = backend.register_schema(schema_bytes()).unwrap();
        view.set_identity_claims(bytes(), None).unwrap();
        view.close().unwrap();
        backend.close().unwrap();
        assert!(
            backend
                .admit_local_first_session(token, app_id.into(), canonical)
                .is_err()
        );
        other.close().unwrap();
        ordinary.close().unwrap();
    }

    #[test]
    fn native_node_clock_handoff_preserves_u64_and_rejects_lossy_inputs() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("items").column("label", ColumnType::Text))
            .build();
        let open = || {
            NapiDb::open_memory_as_backend(
                Uint8Array::from(serde_json::to_vec(&schema).unwrap()),
                Uint8Array::from(encode_persistent_open_config(
                    CoreAuthorSubject::for_test_bytes([0x91; 16]),
                )),
            )
            .unwrap()
        };
        let first = open();
        first
            .seed_foreground_tx_time_high_water(BigInt::from(u64::MAX - 1))
            .unwrap();
        let high_water = first.foreground_tx_time_high_water().unwrap();
        assert_eq!(high_water.get_u64(), (false, u64::MAX - 1, true));
        first.close().unwrap();
        let second = open();
        second
            .seed_foreground_tx_time_high_water(high_water)
            .unwrap();
        second
            .seed_foreground_tx_time_high_water(BigInt::from(1_u64))
            .unwrap();
        assert_eq!(
            second.foreground_tx_time_high_water().unwrap().get_u64(),
            (false, u64::MAX - 1, true)
        );
        for invalid in [
            BigInt {
                sign_bit: true,
                words: vec![1],
            },
            BigInt {
                sign_bit: false,
                words: vec![0, 1],
            },
        ] {
            assert!(second.seed_foreground_tx_time_high_water(invalid).is_err());
        }
        assert_eq!(
            second.foreground_tx_time_high_water().unwrap().get_u64(),
            (false, u64::MAX - 1, true)
        );
        second.close().unwrap();
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
            added_indices: Vec<u64>,
            updated_previous_indices: Vec<u64>,
            updated_indices: Vec<u64>,
            removed_indices: Vec<u64>,
        }
        let root = jazz::tools::ObjectId::from_uuid(uuid::Uuid::from_bytes([1; 16]));
        let joined = jazz::tools::ObjectId::from_uuid(uuid::Uuid::from_bytes([2; 16]));
        let occurrence = |label: &str| {
            jazz::tools::ResultKey::from_union_occurrence(root, [joined], [(0, label.to_owned())])
                .unwrap()
        };
        let removed = ["direct", "inherited"].map(|label| jazz::db::RemovedRow {
            table: "todos".to_owned(),
            row_uuid: CoreRowUuid::from_bytes([1; 16]),
            joined: vec![jazz_db::values::JoinedRow {
                row: CoreRowUuid::from_bytes([2; 16]),
                version: None,
            }],
            version: None,
            union_arm: Some(label.into()),
            index: 0,
        });
        let binding_host::EventChunk::Delta { delta: bytes, .. } =
            binding_host::event_chunk(CoreSubscriptionEvent::Delta {
                reset: false,
                publishable: true,
                added: vec![],
                updated: vec![],
                removed: removed.into(),
                settled: true,
                tier: DurabilityTier::Local,
            })
            .unwrap()
        else {
            panic!("expected delta")
        };
        let decoded: DecodedDelta = postcard::from_bytes(&bytes).unwrap();
        assert!(decoded.added.is_empty() && decoded.updated.is_empty());
        assert_eq!(decoded.removed.len(), 2);
        assert!(
            decoded.added_indices.is_empty()
                && decoded.updated_indices.is_empty()
                && decoded.updated_previous_indices.is_empty()
        );
        assert_eq!(decoded.removed_indices.len(), 2);
        assert!(decoded.added_occurrence_keys.is_empty());
        assert!(decoded.updated_occurrence_keys.is_empty());
        assert_eq!(
            decoded.removed_occurrence_keys,
            [occurrence("direct"), occurrence("inherited")]
        );
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
        let opts = core_read_opts_from_json(Some(json!({ "tier": "local-only" })))
            .expect("parse read opts");

        assert_eq!(opts.tier, jazz::db::ReadTier::LocalOnly);
    }

    #[test]
    fn core_read_opts_ignore_removed_propagate_field() {
        let opts =
            core_read_opts_from_json(Some(json!({ "propagate": false }))).expect("parse read opts");

        assert_eq!(opts.tier, jazz::db::ReadTier::LocalFirst);
    }

    #[test]
    fn core_read_opts_accept_branch_view() {
        let expected = CoreReadViewSpec::branch_view(
            jazz::protocol::BranchSelector::new([(
                "branch",
                CoreValue::Uuid(uuid::Uuid::from_bytes([0x42; 16])),
            )]),
            None,
        );
        let opts = core_read_opts_from_json(Some(json!({
            "read_view": serde_json::to_value(&expected).unwrap()
        })))
        .expect("parse branch read view");

        assert_eq!(opts.read_view, expected);
    }

    #[test]
    fn subscription_payload_exposes_only_terminal_rows() {
        let payload = core_subscription_event_to_napi(&CoreSubscriptionEvent::Delta {
            reset: false,
            publishable: true,
            added: Vec::new(),
            updated: Vec::new(),
            removed: Vec::new(),
            settled: true,
            tier: DurabilityTier::Local,
        })
        .expect("encode terminal delta");

        let Either3::A(payload) = payload else {
            panic!("expected delta payload");
        };
        assert!(!payload.delta.is_empty());
        assert!(payload.terminal_operations.is_empty());
        assert_eq!(payload.tier, "Local");
    }

    /// Alice observes child insert, update, reorder and removal through whole-parent deltas, preserving every payload byte.
    #[test]
    fn subscription_payload_preserves_nested_changes_as_whole_rows() {
        use jazz::query::{ArraySubquery, OrderDirection, Query};
        let policies = || {
            TablePolicies::new()
                .with_select(PolicyExpr::True)
                .with_insert(PolicyExpr::True)
                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                .with_delete(PolicyExpr::True)
        };
        let source = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("label", ColumnType::Text)
                    .policies(policies()),
            )
            .table(
                TableSchema::builder("children")
                    .column("parent", ColumnType::Uuid)
                    .column("rank", ColumnType::Text)
                    .column("payload", ColumnType::Bytea)
                    .policies(policies()),
            )
            .build();
        let owner = NapiDb::open_memory(
            serde_json::to_vec(&source).unwrap().into(),
            encode_persistent_open_config(CoreAuthorSubject::for_test_bytes([0x66; 16])).into(),
            None,
        )
        .unwrap();
        let db = owner.binding().unwrap();
        let parent = db
            .insert("items", &label_cells("parent"), &JsonValue::Null)
            .unwrap();
        let query = Query::from("items").array_subquery(
            ArraySubquery::new("children", "children", "parent", "id")
                .order_by("rank", OrderDirection::Asc),
        );
        let mut stream = owner
            .subscribe(
                postcard::to_allocvec(&query).unwrap().into(),
                Some(json!({"tier":"local-first"})),
                None,
                None,
            )
            .unwrap();
        stream.read_all().unwrap();
        let cells = |rank: &str, payload: Vec<u8>| {
            let descriptor = RecordDescriptor::new([
                ("parent", ValueType::Uuid),
                ("rank", ValueType::String),
                ("payload", ValueType::Bytes),
            ]);
            let raw = descriptor
                .create(&[
                    CoreValue::Uuid(parent.row_id.0),
                    CoreValue::String(rank.into()),
                    CoreValue::Bytes(payload),
                ])
                .unwrap();
            jazz::binding_codec::encode_named_cells(&jazz::records::OwnedRecord::new(
                raw, descriptor,
            ))
            .unwrap()
        };
        let children = |events: Vec<SubscriptionEvent>| {
            let mut latest = None;
            for event in events {
                let Either3::A(delta) = event else {
                    panic!("unexpected terminal event")
                };
                assert!(delta.terminal_operations.is_empty());
                let (added, updated) =
                    jazz::binding_codec::decode_subscription_delta_rows(&delta.delta).unwrap();
                for batch in added.into_iter().chain(updated) {
                    for row in batch.rows {
                        assert_eq!(row.row_id, parent.row_id);
                        let CoreValue::Array(nested) = &row.cells["children"] else {
                            panic!("children array")
                        };
                        latest = Some(
                            nested
                                .iter()
                                .map(|child| {
                                    let CoreValue::Record(record) = child else {
                                        panic!("child record")
                                    };
                                    let fields = record.descriptor().fields();
                                    let values = record.to_values().unwrap();
                                    let slot = fields
                                        .iter()
                                        .position(|f| f.name.as_deref() == Some("payload"))
                                        .unwrap();
                                    let CoreValue::Bytes(bytes) = &values[slot] else {
                                        panic!("child bytes")
                                    };
                                    bytes.clone()
                                })
                                .collect::<Vec<_>>(),
                        );
                    }
                }
            }
            latest.expect("changed child must deliver its parent")
        };
        let all_bytes = (0..=255u8).collect::<Vec<_>>();
        let first = db
            .insert("children", &cells("a", all_bytes.clone()), &JsonValue::Null)
            .unwrap();
        assert_eq!(children(stream.read_all().unwrap()), [all_bytes.clone()]);
        let second = db
            .insert("children", &cells("b", vec![7]), &JsonValue::Null)
            .unwrap();
        assert_eq!(
            children(stream.read_all().unwrap()),
            [all_bytes.clone(), vec![7]]
        );
        db.update(
            "children",
            first.row_id.0.as_bytes(),
            &cells("a", vec![6]),
            &JsonValue::Null,
        )
        .unwrap();
        assert_eq!(children(stream.read_all().unwrap()), [vec![6], vec![7]]);
        db.update(
            "children",
            second.row_id.0.as_bytes(),
            &cells("0", vec![7]),
            &JsonValue::Null,
        )
        .unwrap();
        assert_eq!(children(stream.read_all().unwrap()), [vec![7], vec![6]]);
        db.delete("children", first.row_id.0.as_bytes(), &JsonValue::Null)
            .unwrap();
        assert_eq!(children(stream.read_all().unwrap()), [vec![7]]);
        assert!(stream.read_all().unwrap().is_empty());
    }

    #[test]
    fn subscription_payload_preserves_rejection_and_closed_variants() {
        let rejected = core_subscription_event_to_napi(&CoreSubscriptionEvent::Rejected {
            reason: "SQLite subscription refusal".to_owned(),
        })
        .expect("encode SQLite rejection");
        assert!(matches!(rejected,
            Either3::B(crate::SubscriptionRejectedEvent {
                event_type,
                reason: crate::SubscriptionServerFailureReason { reason_type, code },
            }) if event_type == "rejected" && reason_type == "ServerFailure"
                && code == "SQLite subscription refusal"
        ));

        let closed = core_subscription_event_to_napi(&CoreSubscriptionEvent::Closed)
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

        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], Either3::B(_)));
        assert!(matches!(events[1], Either3::C(_)));
    }
    /// A NAPI schema view can address its owner's open transaction by id.
    #[test]
    fn schema_view_uses_owner_transaction_id() {
        let owner = fixture();
        let view = owner.register_schema(fixture_schema().into()).unwrap();
        for (kind, label) in [("mergeable", "kept"), ("serializable", "serializable-kept")] {
            let id = CoreOpenTransactionId::new().to_string();
            owner
                .begin_transaction(id.clone(), kind.into(), None, None)
                .unwrap();
            view.insert_in_transaction(id.clone(), "items".into(), label_cells(label), None)
                .unwrap();
            owner.commit_test(id, Some(kind.into())).unwrap();
        }
        assert_eq!(labels(&view), ["kept", "serializable-kept"]);
        let alice = CoreAuthorSubject::for_test_bytes([0xa6; 16]);
        let id = CoreOpenTransactionId::new().to_string();
        owner
            .begin_transaction(
                id.clone(),
                "serializable".into(),
                Some(alice.canonical().as_bytes().to_vec().into()),
                None,
            )
            .unwrap();
        let query = postcard::to_allocvec(&jazz::query::Query::from("items")).unwrap();
        for db in [&owner, &view] {
            assert!(
                db.all(
                    query.clone().into(),
                    Some(json!({"tier":"local-only"})),
                    Some(id.clone()),
                    None,
                    None
                )
                .is_ok()
            );
        }
        view.commit_test(id, Some("serializable".into())).unwrap();
    }
}
