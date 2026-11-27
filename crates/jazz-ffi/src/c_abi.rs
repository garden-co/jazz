//! The C ABI declared in `include/jazz_ffi.h`, for Go (cgo) and C.
//!
//! Strings are UTF-8 and NUL-terminated. Every string a function returns,
//! and every message it stores in `*err`, belongs to the caller, who frees
//! it with [`jazz_string_free`]. On failure a function returns NULL and, if
//! `err` is not NULL, stores `{"kind":"refused"|"io"|"closed","message":…}`
//! in `*err`. Strings passed to a subscription's callback are only valid
//! during the call.

use std::cell::Cell;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use serde_json::{Value as Json, json};
use zeroize::{Zeroize, Zeroizing};

use crate::JazzError;
use crate::core::{Core, Event, Opts, SubscriptionCore, parse_tx, write_tier};
use crate::error::{FfiError, refused};

/// An open app (opaque to C).
pub struct JazzHandle(Core);

/// A live subscription (opaque to C).
pub struct JazzSubscription(SubscriptionCore);

/// A subscription's callback: `kind` is `"delta"` (with the delta JSON) or
/// `"error"` (with the error JSON, after which no callback comes).
pub type JazzCallback =
    Option<unsafe extern "C" fn(ctx: *mut c_void, kind: *const c_char, json: *const c_char)>;

fn error_json(error: &JazzError) -> String {
    let mut json = json!({ "kind": error.kind(), "message": error.to_string() });
    match error {
        JazzError::Encryption { code, .. } => json["code"] = json!(code),
        JazzError::NoAccount { user_id, .. } => json["userId"] = json!(user_id),
        JazzError::Unsynced { pending, .. } => json["pending"] = json!(pending),
        JazzError::NoUser { status, .. } => json["status"] = json!(status),
        JazzError::Interrupted {
            interruption,
            queued,
            ..
        } => {
            json["reason"] = json!(interruption);
            json["queued"] = json!(queued);
        }
        JazzError::BundleMismatch { own, served, .. } => {
            json["own"] = json!(own);
            json["served"] = json!(served);
        }
        _ => {}
    }
    json.to_string()
}

/// The `"call"` (from `callBegin`) a call's args name, if any.
fn call_id(args: &Json) -> Result<Option<u64>, FfiError> {
    match args.get("call").filter(|call| !call.is_null()) {
        None => Ok(None),
        Some(call) => call
            .as_str()
            .and_then(|call| call.parse().ok())
            .map(Some)
            .ok_or_else(|| refused("args_json.call must be a call from callBegin")),
    }
}

fn into_c(text: String) -> *mut c_char {
    // Interior NULs cannot occur in JSON we serialize (they're escaped);
    // replace them defensively rather than fail.
    CString::new(text)
        .unwrap_or_else(|error| {
            let mut bytes = error.into_vec();
            bytes.retain(|b| *b != 0);
            CString::new(bytes).unwrap_or_default()
        })
        .into_raw()
}

/// Runs `call`, turning errors and panics into NULL plus `*err`, named as
/// the contract version `call` sets (1 until it knows better).
fn guarded<T>(
    err: *mut *mut c_char,
    null: T,
    call: impl FnOnce(&Cell<u64>) -> Result<T, FfiError>,
) -> T {
    let contract = Cell::new(1);
    if !err.is_null() {
        // SAFETY: the caller passes NULL or a valid `char**`.
        unsafe { *err = ptr::null_mut() };
    }
    let failure = match catch_unwind(AssertUnwindSafe(|| call(&contract))) {
        Ok(Ok(value)) => return value,
        Ok(Err(error)) => JazzError::from(error).for_contract(contract.get()),
        Err(_) => JazzError::Io {
            reason: "the call panicked".into(),
        },
    };
    if !err.is_null() {
        // SAFETY: as above.
        unsafe { *err = into_c(error_json(&failure)) };
    }
    null
}

/// # Safety
/// `text` is NULL or a NUL-terminated string valid for the call.
unsafe fn text<'a>(text: *const c_char, what: &str) -> Result<&'a str, FfiError> {
    if text.is_null() {
        return Err(refused(format!("{what} is NULL")));
    }
    // SAFETY: per the function's contract.
    unsafe { CStr::from_ptr(text) }
        .to_str()
        .map_err(|_| refused(format!("{what} is not UTF-8")))
}

/// # Safety
/// `handle` is NULL or a handle not yet freed.
unsafe fn handle<'a>(handle: *const JazzHandle) -> Result<&'a Core, FfiError> {
    // SAFETY: per the function's contract.
    unsafe { handle.as_ref() }
        .map(|handle| &handle.0)
        .ok_or_else(|| refused("the handle is NULL"))
}

/// Opens an app from its config JSON (see the header).
///
/// # Safety
/// `config_json` is a NUL-terminated string; `err` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_open(
    config_json: *const c_char,
    err: *mut *mut c_char,
) -> *mut JazzHandle {
    // SAFETY: per the function's contract.
    unsafe { open_host(config_json, None, None, err) }
}

/// Calls `method` (`all`, `one`, `insert`, `update`, `delete`, `account`,
/// `txBegin`, `txCommit`, `txRollback`) with its arguments as JSON:
/// `{query}`, `{query}`, `{table,row}`, `{table,id,patch}`, `{table,id}`,
/// `{}`, `{serializable?}`, `{tx,tier?}`, `{tx}`. Reads and writes also take
/// `"tx"` (run inside that transaction) and `"tier"`. Returns its result as
/// JSON: rows, a row or `null`, the inserted row, `null`, `null`, the
/// account id or `null`, `{tx}`, `null`, `null`.
///
/// # Safety
/// `handle` is not freed (it may be closed); the strings are
/// NUL-terminated; `err` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_call(
    handle_ptr: *const JazzHandle,
    method: *const c_char,
    args_json: *const c_char,
    err: *mut *mut c_char,
) -> *mut c_char {
    guarded(err, ptr::null_mut(), |contract| {
        // SAFETY: per the function's contract.
        let (core, method, args) = unsafe {
            (
                handle(handle_ptr)?,
                text(method, "method")?,
                text(args_json, "args_json")?,
            )
        };
        contract.set(core.contract);
        let args = if args.trim().is_empty() {
            Json::Object(Default::default())
        } else {
            crate::convert::parse(args, "args_json")?
        };
        // Beside any other call, and on a closed handle too.
        if method == "close" {
            core.close_within(close_timeout(&args)?)?;
            return Ok(into_c("null".into()));
        }
        // A call's control: cancelling and ending it run beside any call,
        // and on a closed handle too.
        match method {
            "cancel" => {
                let id = call_id(&args)?.ok_or_else(|| refused("cancel needs \"call\""))?;
                core.control(id)?.cancel();
                return Ok(into_c("null".into()));
            }
            "callEnd" => {
                let id = call_id(&args)?.ok_or_else(|| refused("callEnd needs \"call\""))?;
                core.call_end(id);
                return Ok(into_c("null".into()));
            }
            "callBegin" => {
                let id = core.call_begin(close_timeout(&args)?)?;
                return Ok(into_c(json!({ "call": id.to_string() }).to_string()));
            }
            _ => {}
        }
        // The call obeys its control, if it names one.
        let control = call_id(&args)?.map(|id| core.control(id)).transpose()?;
        let _control = crate::core::Controlled::new(control);
        core.ensure_open()?;
        let field = |name: &str| -> Result<&Json, FfiError> {
            args.get(name)
                .ok_or_else(|| refused(format!("{method} needs \"{name}\" in args_json")))
        };
        let string = |name: &str| -> Result<String, FfiError> {
            field(name)?
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| refused(format!("args_json.{name} must be a string")))
        };
        let opts = |read: bool| Opts::parse(args.get("tx"), args.get("tier"), read);
        let result = match method {
            "all" => core.all(&field("query")?.to_string(), &opts(true)?)?,
            "one" => core
                .one(&field("query")?.to_string(), &opts(true)?)?
                .unwrap_or_else(|| "null".into()),
            "insert" => {
                core.insert(&string("table")?, &field("row")?.to_string(), &opts(false)?)?
            }
            "update" => {
                core.update(
                    &string("table")?,
                    &string("id")?,
                    &field("patch")?.to_string(),
                    &opts(false)?,
                )?;
                "null".into()
            }
            "delete" => {
                core.delete(&string("table")?, &string("id")?, &opts(false)?)?;
                "null".into()
            }
            "edit" => {
                let patch = args
                    .get("patch")
                    .filter(|p| !p.is_null())
                    .map(Json::to_string);
                core.edit(
                    &string("table")?,
                    &string("id")?,
                    patch.as_deref(),
                    &field("edits")?.to_string(),
                    &opts(false)?,
                )?;
                "null".into()
            }
            // Go's shape: splices over the whole current value.
            "textEdit" => {
                let edits = json!([{
                    "kind": "splice",
                    "column": string("column")?,
                    "units": args.get("units").cloned().unwrap_or(json!("utf8")),
                    "splices": field("edits")?
                        .as_array()
                        .ok_or_else(|| refused("args_json.edits must be an array"))?
                        .iter()
                        .map(|e| json!({ "at": e.get("pos"), "delete": e.get("delete"), "insert": e.get("insert") }))
                        .collect::<Vec<_>>(),
                }]);
                core.edit(
                    &string("table")?,
                    &string("id")?,
                    None,
                    &edits.to_string(),
                    &opts(false)?,
                )?;
                "null".into()
            }
            "editText" => {
                let edits = json!([{
                    "kind": "text",
                    "column": string("column")?,
                    "text": string("text")?,
                    "editedFrom": string("editedFrom")?,
                }]);
                core.edit(
                    &string("table")?,
                    &string("id")?,
                    None,
                    &edits.to_string(),
                    &opts(false)?,
                )?;
                "null".into()
            }
            "can" => {
                let row = args
                    .get("row")
                    .filter(|r| !r.is_null())
                    .map(Json::to_string);
                let id = args.get("id").and_then(Json::as_str);
                Json::from(core.can(&string("table")?, &string("op")?, id, row.as_deref())?)
                    .to_string()
            }
            "session" => core.session_state()?.unwrap_or_else(|| "null".into()),
            "syncStatus" => core.sync_status()?,
            "refresh" => {
                core.refresh()?;
                "null".into()
            }
            "link" => core.link(&string("bearer")?)?,
            "bundle" => Json::from(core.bundle()?).to_string(),
            "signOut" => {
                core.sign_out()?;
                "null".into()
            }
            "disconnect" => {
                core.disconnect()?;
                "null".into()
            }
            "reconnect" => {
                core.reconnect()?;
                "null".into()
            }
            "deleteClientStorage" => {
                core.delete_client_storage()?;
                "null".into()
            }
            "upsert" => core.upsert(
                &string("table")?,
                &string("id")?,
                &field("row")?.to_string(),
                &opts(false)?,
            )?,
            "restore" => {
                let patch = args
                    .get("patch")
                    .filter(|p| !p.is_null())
                    .map(Json::to_string);
                core.restore(
                    &string("table")?,
                    &string("id")?,
                    patch.as_deref(),
                    &opts(false)?,
                )?
            }
            "uploadBegin" => {
                // jazz-app-support streams outside transactions only: an upload
                // that named one would escape it.
                if args.get("tx").is_some_and(|tx| !tx.is_null()) {
                    return Err(crate::error::unsupported(
                        "streaming uploads inside a transaction",
                    ));
                }
                let row = args
                    .get("row")
                    .filter(|r| !r.is_null())
                    .map(Json::to_string);
                let kind = args.get("kind").and_then(Json::as_str).unwrap_or("insert");
                json!({ "upload": core.upload_begin(
                    &string("table")?,
                    &string("column")?,
                    kind,
                    args.get("id").and_then(Json::as_str),
                    row.as_deref(),
                )? })
                .to_string()
            }
            "uploadPush" => {
                use base64::Engine as _;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(string("bytes")?)
                    .map_err(|_| refused("args_json.bytes must be base64"))?;
                core.upload_push(parse_tx(&string("upload")?)?, bytes)?;
                "null".into()
            }
            "uploadFinish" => {
                if args.get("tx").is_some_and(|tx| !tx.is_null()) {
                    return Err(crate::error::unsupported(
                        "streaming uploads inside a transaction",
                    ));
                }
                core.upload_finish(parse_tx(&string("upload")?)?, &opts(false)?)?
            }
            "uploadAbort" => {
                core.upload_abort(parse_tx(&string("upload")?)?)?;
                "null".into()
            }
            "txBegin" => {
                let serializable = match args.get("serializable") {
                    None | Some(Json::Null) => true,
                    Some(Json::Bool(serializable)) => *serializable,
                    Some(_) => return Err(refused("args_json.serializable must be a boolean")),
                };
                json!({ "tx": core.tx_begin(serializable)? }).to_string()
            }
            "txCommit" => {
                let tier = match args.get("tier").filter(|t| !t.is_null()) {
                    Some(tier) => {
                        Some(write_tier(tier.as_str().ok_or_else(|| {
                            refused("args_json.tier must be a string")
                        })?)?)
                    }
                    None => None,
                };
                core.tx_commit(parse_tx(&string("tx")?)?, tier)?;
                "null".into()
            }
            "waitForSync" => {
                let tier = args.get("tier").and_then(Json::as_str).ok_or_else(|| {
                    refused("waitForSync needs \"tier\": \"local\" or \"global\"")
                })?;
                core.wait_for_sync(write_tier(tier)?)?;
                "null".into()
            }
            "txRollback" => {
                core.tx_rollback(parse_tx(&string("tx")?)?)?;
                "null".into()
            }
            "memberAdd" => core.member_add(
                &string("table")?,
                &string("id")?,
                &string("accountId")?,
                Opts::parse(None, args.get("tier"), false)?.write_tier,
            )?,
            "memberList" => core.member_list(&string("table")?, &string("id")?)?,
            "memberRemove" => core
                .member_remove(&string("table")?, &string("id")?, &string("accountId")?)?
                .to_string(),
            "encryptionCall" => core.encryption_call(
                &string("op")?,
                &args.get("args").map(Json::to_string).unwrap_or_default(),
            )?,
            "account" => Json::from(core.account()?).to_string(),
            "backendClient" => Json::from(core.backend_client()?).to_string(),
            other => return Err(refused(format!("unknown method {other:?}"))),
        };
        Ok(into_c(result))
    })
}

/// `"close"`'s `{"timeoutMs"?}`.
fn close_timeout(args: &Json) -> Result<Option<std::time::Duration>, FfiError> {
    match args.get("timeoutMs").filter(|timeout| !timeout.is_null()) {
        None => Ok(None),
        Some(timeout) => timeout
            .as_u64()
            .map(|ms| Some(std::time::Duration::from_millis(ms)))
            .ok_or_else(|| {
                refused("args_json.timeoutMs must be a non-negative whole number of milliseconds")
            }),
    }
}

struct Context(*mut c_void);
// SAFETY: the caller of `jazz_subscribe` promises `ctx` may be used from
// another thread (the header says so).
unsafe impl Send for Context {}

/// Subscribes to a query; `cb` is called on a thread of the subscription's
/// own until `jazz_unsubscribe`.
///
/// # Safety
/// `handle` is not freed; `query_json` is
/// NUL-terminated; `cb` and `ctx` stay valid, from any thread, until
/// `jazz_unsubscribe` returns; `err` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_subscribe(
    handle_ptr: *const JazzHandle,
    query_json: *const c_char,
    cb: JazzCallback,
    ctx: *mut c_void,
    err: *mut *mut c_char,
) -> *mut JazzSubscription {
    guarded(err, ptr::null_mut(), |contract| {
        // SAFETY: per the function's contract.
        let (core, query) = unsafe { (handle(handle_ptr)?, text(query_json, "query_json")?) };
        contract.set(core.contract);
        core.ensure_open()?;
        let version = core.contract;
        // `{ "query": Q, "tier"? }`, or the query itself.
        let parsed = crate::convert::parse(query, "query_json")?;
        let (query, opts) = match parsed.get("query") {
            Some(inner) if parsed.get("table").is_none() => (
                inner.to_string(),
                Opts::parse(None, parsed.get("tier"), true)?,
            ),
            _ => (query.to_owned(), Opts::default()),
        };
        let cb = cb.ok_or_else(|| refused("the callback is NULL"))?;
        let ctx = Context(ctx);
        let subscription = core.subscribe(
            &query,
            &opts,
            Box::new(move |event| {
                let ctx = &ctx;
                let (kind, json) = match event {
                    Event::Delta(json) => (c"delta", json),
                    Event::Error(error) => (c"error", error_json(&error.for_contract(version))),
                };
                let json = CString::new(json).unwrap_or_default();
                // SAFETY: per `jazz_subscribe`'s contract; the strings live
                // for the call.
                unsafe { cb(ctx.0, kind.as_ptr(), json.as_ptr()) };
            }),
        )?;
        Ok(Box::into_raw(Box::new(JazzSubscription(subscription))))
    })
}

/// Ends and frees a subscription. When it returns, no callback is running
/// or will start, so `cb` and `ctx` may be freed; called from inside the
/// callback, it returns without waiting for that callback. A subscription
/// owns what it needs: it may be freed before or after `jazz_close`.
///
/// # Safety
/// `subscription` is NULL or from `jazz_subscribe`, and not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_unsubscribe(subscription: *mut JazzSubscription) {
    if subscription.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: per the function's contract.
        let subscription = unsafe { Box::from_raw(subscription) };
        subscription.0.cancel_and_wait();
    }));
}

/// Closes the app and frees the handle. Each subscription still open gets a
/// last `"error"` callback with kind `"closed"`; free them with
/// `jazz_unsubscribe` (before or after this call). No other call on this
/// handle may run concurrently with, or follow, this one.
///
/// # Safety
/// `handle` is NULL or from `jazz_open`, and not yet closed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_close(handle: *mut JazzHandle) {
    if handle.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: per the function's contract.
        let handle = unsafe { Box::from_raw(handle) };
        let _ = handle.0.close();
        handle.0.before_free();
    }));
}

/// [`jazz_close`], first waiting up to `timeout_ms` for the writes the
/// client holds to reach the server. Returns 0; or -1 with `*err` set when
/// writes were left unsent (memory storage, kind `"unsynced"` with
/// `"pending"` under contract 2) or closing failed. The handle is closed and
/// freed either way; after a `"close"` call it only frees. `timeout_ms` 0
/// waits for no writes, but is not instant: local work already started
/// finishes, and stream sessions get up to 2 s to end (no hard limit).
///
/// # Safety
/// As [`jazz_close`]; `err` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_close_within(
    handle: *mut JazzHandle,
    timeout_ms: u64,
    err: *mut *mut c_char,
) -> c_int {
    guarded(err, -1, |contract| {
        if handle.is_null() {
            return Ok(0);
        }
        // SAFETY: per the function's contract.
        let handle = unsafe { Box::from_raw(handle) };
        contract.set(handle.0.contract);
        let closed = handle
            .0
            .close_within(Some(std::time::Duration::from_millis(timeout_ms)));
        handle.0.before_free();
        closed?;
        Ok(0)
    })
}

/// On a trusted backend client, a new handle acting as `user_id`, with
/// `claims_json` (a JSON object, or NULL for `{}`) as the claims its
/// permissions see. Free it with `jazz_close`, which leaves `handle` open;
/// once `handle` closes, its calls fail with kind `"closed"`. It never
/// creates an account: kind `"noAccount"` (with `"userId"`, contract 2)
/// when the user has none yet, `"permission"` when the main backend refuses
/// the identity, `"refused"` on a scoped client, a `jazz_for_user` handle
/// or any other app.
///
/// # Safety
/// `handle` comes from `jazz_open_user` and is not closed; the strings are
/// NUL-terminated (`claims_json` may be NULL); `err` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_for_user(
    handle_ptr: *const JazzHandle,
    user_id: *const c_char,
    claims_json: *const c_char,
    err: *mut *mut c_char,
) -> *mut JazzHandle {
    guarded(err, ptr::null_mut(), |contract| {
        // SAFETY: per the function's contract.
        let core = unsafe { handle(handle_ptr) }?;
        contract.set(core.contract);
        core.ensure_open()?;
        // SAFETY: as above.
        let user_id = unsafe { text(user_id, "user_id") }?;
        let claims = if claims_json.is_null() {
            ""
        } else {
            // SAFETY: as above.
            unsafe { text(claims_json, "claims_json") }?
        };
        Ok(Box::into_raw(Box::new(JazzHandle(
            core.for_user(user_id, claims)?,
        ))))
    })
}

/// A user-following session (opaque to C).
pub struct JazzSession(crate::session::SessionCore);

/// # Safety
/// `session` is NULL or a session from `jazz_session_open`, not yet freed.
unsafe fn session<'a>(
    session: *const JazzSession,
) -> Result<&'a crate::session::SessionCore, FfiError> {
    // SAFETY: per the function's contract.
    unsafe { session.as_ref() }
        .map(|session| &session.0)
        .ok_or_else(|| refused("the session is NULL"))
}

/// Opens a user-following session (see the header). `token_cb` (NULL
/// without `"auth": {"bearer": true}`) gives the app's bearer.
///
/// # Safety
/// `config_json` is NUL-terminated; `token_cb` and `ctx` stay valid, from
/// any thread, until `jazz_session_close` returns; `err` is NULL or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_session_open(
    config_json: *const c_char,
    token_cb: JazzTokenCallback,
    ctx: *mut c_void,
    err: *mut *mut c_char,
) -> *mut JazzSession {
    guarded(err, ptr::null_mut(), |contract| {
        // SAFETY: per the function's contract.
        let config = unsafe { text(config_json, "config_json") }?;
        if let Some(version) = serde_json::from_str::<Json>(config)
            .ok()
            .and_then(|c| c.get("contract").and_then(Json::as_u64))
        {
            contract.set(version);
        }
        let (tokens, gate) = match token_cb {
            Some(cb) => {
                let (host, gate) = host_tokens(cb, ctx);
                let tokens: std::sync::Arc<dyn jazz_app_support::TokenProvider> =
                    std::sync::Arc::new(host);
                (Some(tokens), Some(gate))
            }
            None => (None, None),
        };
        let core = crate::session::SessionCore::open(config, tokens)?;
        if let Some(gate) = gate {
            core.on_close(gate);
        }
        Ok(Box::into_raw(Box::new(JazzSession(core))))
    })
}

/// Calls a session's `method` with `args_json` (see the header); returns
/// its result as JSON.
///
/// # Safety
/// `session` is not freed (it may be closed); the strings
/// are NUL-terminated; `err` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_session_call(
    session_ptr: *const JazzSession,
    method: *const c_char,
    args_json: *const c_char,
    err: *mut *mut c_char,
) -> *mut c_char {
    guarded(err, ptr::null_mut(), |contract| {
        // SAFETY: per the function's contract.
        let core = unsafe { session(session_ptr) }?;
        contract.set(core.contract);
        // SAFETY: as above.
        let (method, args) = unsafe { (text(method, "method")?, text(args_json, "args_json")?) };
        let args = if args.trim().is_empty() {
            Json::Object(Default::default())
        } else {
            crate::convert::parse(args, "args_json")?
        };
        // A call's control, as on a handle.
        match method {
            "cancel" => {
                let id = call_id(&args)?.ok_or_else(|| refused("cancel needs \"call\""))?;
                core.control(id)?.cancel();
                return Ok(into_c("null".into()));
            }
            "callEnd" => {
                let id = call_id(&args)?.ok_or_else(|| refused("callEnd needs \"call\""))?;
                core.call_end(id);
                return Ok(into_c("null".into()));
            }
            "callBegin" => {
                let id = core.call_begin(close_timeout(&args)?)?;
                return Ok(into_c(json!({ "call": id.to_string() }).to_string()));
            }
            _ => {}
        }
        let control = call_id(&args)?.map(|id| core.control(id)).transpose()?;
        let _control = crate::core::Controlled::new(control);
        Ok(into_c(core.call(method, &args)?.to_string()))
    })
}

/// A handle acting for the user the session has signed in now (kind
/// `"noUser"`, with `"status"`, while there is none). Free it with
/// `jazz_close`, which leaves the session open; once the session's user
/// changes, its calls fail with kind `"replaced"` (contract 2; `"closed"`
/// otherwise): fetch the new one when the snapshot's `clientEpoch` changes.
///
/// # Safety
/// `session` comes from `jazz_session_open` and is not closed; `err` is
/// NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_session_jazz(
    session_ptr: *const JazzSession,
    err: *mut *mut c_char,
) -> *mut JazzHandle {
    guarded(err, ptr::null_mut(), |contract| {
        // SAFETY: per the function's contract.
        let core = unsafe { session(session_ptr) }?;
        contract.set(core.contract);
        Ok(Box::into_raw(Box::new(JazzHandle(core.jazz()?))))
    })
}

/// Calls `cb` with kind `"session"` and each snapshot's JSON, the current
/// one first, on a thread of its own, until `jazz_unwatch`; when the
/// session closes, a last call with kind `"error"` and
/// `{"kind":"closed",…}`.
///
/// # Safety
/// `session` comes from `jazz_session_open` and is not closed; `cb` and
/// `ctx` stay valid, from any thread, until `jazz_unwatch` returns; `err`
/// is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_session_watch(
    session_ptr: *const JazzSession,
    cb: JazzCallback,
    ctx: *mut c_void,
    err: *mut *mut c_char,
) -> *mut JazzWatch {
    guarded(err, ptr::null_mut(), |contract| {
        // SAFETY: per the function's contract.
        let core = unsafe { session(session_ptr) }?;
        contract.set(core.contract);
        let cb = cb.ok_or_else(|| refused("the callback is NULL"))?;
        let ctx = Context(ctx);
        let watch = core.watch(Box::new(move |state| {
            let ctx = &ctx;
            let (kind, json) = match state {
                Some(state) => (c"session", state),
                None => (c"error", error_json(&JazzError::Closed)),
            };
            let json = CString::new(json).unwrap_or_default();
            // SAFETY: per `jazz_session_watch`'s contract.
            unsafe { cb(ctx.0, kind.as_ptr(), json.as_ptr()) };
        }))?;
        Ok(Box::into_raw(Box::new(JazzWatch(watch))))
    })
}

/// Closes the session and frees it: every handle it gave fails with kind
/// `"closed"` (free them with `jazz_close`, before or after), every watch
/// gets its last callback, and no token callback runs once it returns.
///
/// # Safety
/// `session` is NULL or from `jazz_session_open`, not yet closed; no other
/// call on it runs concurrently with or follows this one.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_session_close(session: *mut JazzSession) {
    if session.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: per the function's contract.
        let session = unsafe { Box::from_raw(session) };
        let _ = session.0.close();
        session.0.before_free();
    }));
}

/// Frees a string a function of this API returned or stored in `*err`.
///
/// # Safety
/// `text` is NULL or such a string, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_string_free(text: *mut c_char) {
    if !text.is_null() {
        // SAFETY: per the function's contract.
        drop(unsafe { CString::from_raw(text) });
    }
}

/// A host's token callback: called on a thread of its own (it may block)
/// with `reason` (`"connect"`, `"renew"`, `"expired"`, `"refresh"`). Returns
/// 0 with `*token` set to the token, a `malloc`ed string (Go's `C.CString`)
/// jazz frees, or to NULL when no one is signed in (the app stops); or
/// non-zero when the fetch failed (a network blip): the session stays and
/// asks again, and `*token`, if set, is the failure's message (freed too).
pub type JazzTokenCallback = Option<
    unsafe extern "C" fn(ctx: *mut c_void, reason: *const c_char, token: *mut *mut c_char) -> i32,
>;

/// A private store's callback (see the header): `request` is JSON text;
/// returns 0 with `*payload` the result as JSON text, or non-zero with
/// `*payload` the error (`conflict`, `wiped` or a message); `*payload` is a
/// `malloc`ed string jazz zeroes and frees.
pub type JazzStoreCallback = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        request: *const c_char,
        payload: *mut *mut c_char,
    ) -> i32,
>;

unsafe extern "C" {
    fn free(pointer: *mut c_void);
}

type Callback = unsafe extern "C" fn(*mut c_void, *const c_char, *mut *mut c_char) -> i32;
type CallbackGate = crate::gate::Gate<(Callback, CallbackContext)>;

#[derive(Clone, Copy)]
struct CallbackContext(*mut c_void);
// SAFETY: the host promises its context is valid on the callback's thread
// while the gate is open (see the header).
unsafe impl Send for CallbackContext {}
unsafe impl Sync for CallbackContext {}

fn call_host(
    gate: &CallbackGate,
    input: &str,
    wipe_answer: bool,
) -> Option<(bool, Option<String>)> {
    gate.call(|&(cb, ctx)| {
        let mut text = Zeroizing::new(Vec::with_capacity(input.len() + 1));
        if !input.as_bytes().contains(&0) {
            text.extend_from_slice(input.as_bytes());
        }
        text.push(0);
        let mut payload = ptr::null_mut();
        // SAFETY: the gate holds a live callback and context, and text ends in NUL.
        let succeeded = unsafe { cb(ctx.0, text.as_ptr().cast(), &mut payload) } == 0;
        let answer = if payload.is_null() {
            None
        } else {
            // SAFETY: the callback returns a NUL-terminated malloc allocation.
            unsafe {
                let answer = CStr::from_ptr(payload);
                let read = answer.to_string_lossy().into_owned();
                if wipe_answer {
                    std::slice::from_raw_parts_mut(payload.cast::<u8>(), answer.to_bytes().len())
                        .zeroize();
                }
                free(payload.cast());
                Some(read)
            }
        };
        (succeeded, answer)
    })
}

/// Opens an app as an end user who signs in with the app's bearer
/// (`"auth": {"bearer": true}`): `token_cb` gives it.
///
/// # Safety
/// `config_json` is NUL-terminated; `token_cb` and `ctx` stay valid, from
/// any thread, until `jazz_close` returns; `err` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_open_user(
    config_json: *const c_char,
    token_cb: JazzTokenCallback,
    ctx: *mut c_void,
    err: *mut *mut c_char,
) -> *mut JazzHandle {
    // SAFETY: per the function's contract.
    unsafe { open_host(config_json, Some((token_cb, ctx)), None, err) }
}

/// A C host's token callback as a token provider, behind its handle's gate.
fn host_tokens(
    cb: Callback,
    ctx: *mut c_void,
) -> (crate::core::HostTokens, std::sync::Arc<CallbackGate>) {
    let gate = std::sync::Arc::new(CallbackGate::new((cb, CallbackContext(ctx))));
    let calling = gate.clone();
    let host = crate::core::HostTokens(std::sync::Arc::new(move |reason: &str| {
        match call_host(&calling, reason, false) {
            Some((true, answer)) => Ok(answer),
            Some((false, answer)) => {
                Err(answer.unwrap_or_else(|| "the token callback failed".into()))
            }
            None => Err("the app is closed".into()),
        }
    }));
    (host, gate)
}

/// Opens an app that declares end-to-end encryption as an end user (see the
/// header). `token_cb` is NULL unless `"auth": {"bearer": true}`.
///
/// # Safety
/// `config_json` is NUL-terminated; both callbacks and their contexts stay
/// valid, from any thread, until `jazz_close` returns; `err` is NULL or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_open_encrypted(
    config_json: *const c_char,
    token_cb: JazzTokenCallback,
    token_ctx: *mut c_void,
    store_cb: JazzStoreCallback,
    store_ctx: *mut c_void,
    err: *mut *mut c_char,
) -> *mut JazzHandle {
    // SAFETY: per the function's contract.
    unsafe {
        open_host(
            config_json,
            token_cb.map(|cb| (Some(cb), token_ctx)),
            Some((store_cb, store_ctx)),
            err,
        )
    }
}

/// # Safety
/// The config and error slot are valid for the call, and callbacks and their
/// contexts stay valid until the handle closes.
unsafe fn open_host(
    config_json: *const c_char,
    tokens: Option<(JazzTokenCallback, *mut c_void)>,
    store: Option<(JazzStoreCallback, *mut c_void)>,
    err: *mut *mut c_char,
) -> *mut JazzHandle {
    guarded(err, ptr::null_mut(), |contract| {
        // SAFETY: per the function's contract.
        let config = unsafe { text(config_json, "config_json") }?;
        contract.set(crate::core::open_contract(config));
        let store = store
            .map(|(cb, ctx)| {
                let cb = cb.ok_or_else(|| refused("the store callback is NULL"))?;
                let gate = std::sync::Arc::new(CallbackGate::new((cb, CallbackContext(ctx))));
                let calling = gate.clone();
                let config = jazz_app_support::EncryptionConfig::store_calls(move |request| {
                    let request = Zeroizing::new(request);
                    call_host(&calling, &request, true)
                        .map(|(ok, answer)| (ok, answer.unwrap_or_default()))
                        .unwrap_or_else(|| (false, "the app is closed".to_owned()))
                });
                Ok::<_, FfiError>((config, gate))
            })
            .transpose()?;
        let tokens = tokens
            .map(|(cb, ctx)| {
                let cb = cb.ok_or_else(|| refused("the token callback is NULL"))?;
                let (host, gate) = host_tokens(cb, ctx);
                let host: std::sync::Arc<dyn jazz_app_support::TokenProvider> =
                    std::sync::Arc::new(host);
                Ok::<_, FfiError>((host, gate))
            })
            .transpose()?;
        let core = Core::open_with(
            config,
            tokens.as_ref().map(|(host, _)| host.clone()),
            store.as_ref().map(|(calls, _)| calls.clone()),
        )?;
        if let Some((_, gate)) = tokens {
            core.on_close(gate);
        }
        if let Some((_, gate)) = store {
            core.on_close(gate);
        }
        Ok(Box::into_raw(Box::new(JazzHandle(core))))
    })
}

/// A watch of session or sync states (opaque to C).
pub struct JazzWatch(crate::core::Watch);

/// Calls `cb` with kind `"session"` or `"sync"` and each state's JSON, the
/// current one first, on a thread of its own, until `jazz_unwatch`.
///
/// # Safety
/// `handle` comes from `jazz_open`/`jazz_open_user` and is not closed;
/// `what` is NUL-terminated; `cb` and `ctx` stay valid, from any thread,
/// until `jazz_unwatch` returns; `err` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_watch(
    handle_ptr: *const JazzHandle,
    what: *const c_char,
    cb: JazzCallback,
    ctx: *mut c_void,
    err: *mut *mut c_char,
) -> *mut JazzWatch {
    guarded(err, ptr::null_mut(), |contract| {
        // SAFETY: per the function's contract.
        let (core, what) = unsafe { (handle(handle_ptr)?, text(what, "what")?) };
        contract.set(core.contract);
        core.ensure_open()?;
        let cb = cb.ok_or_else(|| refused("the callback is NULL"))?;
        let ctx = Context(ctx);
        let kind = CString::new(what).unwrap_or_default();
        let watch = core.watch(
            what,
            Box::new(move |state| {
                let ctx = &ctx;
                // The app closed: a last "error" callback, kind "closed".
                let (kind, json) = match state {
                    Some(state) => (kind.as_c_str(), state),
                    None => (c"error", error_json(&JazzError::Closed)),
                };
                let json = CString::new(json).unwrap_or_default();
                // SAFETY: per `jazz_watch`'s contract.
                unsafe { cb(ctx.0, kind.as_ptr(), json.as_ptr()) };
            }),
        )?;
        Ok(Box::into_raw(Box::new(JazzWatch(watch))))
    })
}

/// Ends and frees a watch: when it returns no callback is running (unless
/// called from one).
///
/// # Safety
/// `watch` is NULL or from `jazz_watch`, and not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_unwatch(watch: *mut JazzWatch) {
    if watch.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: per the function's contract.
        let watch = unsafe { Box::from_raw(watch) };
        watch.0.cancel();
    }));
}

/// A new device account's key as `jazz-auth-v1:…` text, for `"auth":
/// {"deviceKey": …}`; free it with `jazz_string_free`.
///
/// # Safety
/// `err` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_generate_device_key(err: *mut *mut c_char) -> *mut c_char {
    guarded(err, ptr::null_mut(), |_| {
        Ok(into_c(crate::core::generate_device_key()?))
    })
}
