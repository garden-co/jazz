//! The C ABI declared in `include/jazz_ffi.h`, for Go (cgo) and C.
//!
//! Strings are UTF-8 and NUL-terminated. Every string a function returns,
//! and every message it stores in `*err`, belongs to the caller, who frees
//! it with [`jazz_string_free`]. On failure a function returns NULL and, if
//! `err` is not NULL, stores `{"kind":"refused"|"io"|"closed","message":…}`
//! in `*err`. Strings passed to a subscription's callback are only valid
//! during the call.

use std::cell::Cell;
use std::ffi::{CStr, CString, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use serde_json::{Value as Json, json};

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
    json!({ "kind": error.kind(), "message": error.to_string() }).to_string()
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
/// `handle` is NULL or a handle from `jazz_open` not yet closed.
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
    guarded(err, ptr::null_mut(), |contract| {
        // SAFETY: per the function's contract.
        let config = unsafe { text(config_json, "config_json") }?;
        if let Some(version) = serde_json::from_str::<Json>(config)
            .ok()
            .and_then(|c| c.get("contract").and_then(Json::as_u64))
        {
            contract.set(version);
        }
        Ok(Box::into_raw(Box::new(JazzHandle(Core::open(config)?))))
    })
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
/// `handle` comes from `jazz_open` and is not closed; the strings are
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
            "account" => Json::from(core.account()?).to_string(),
            other => return Err(refused(format!("unknown method {other:?}"))),
        };
        Ok(into_c(result))
    })
}

struct Context(*mut c_void);
// SAFETY: the caller of `jazz_subscribe` promises `ctx` may be used from
// another thread (the header says so).
unsafe impl Send for Context {}

/// Subscribes to a query; `cb` is called on a thread of the subscription's
/// own until `jazz_unsubscribe`.
///
/// # Safety
/// `handle` comes from `jazz_open` and is not closed; `query_json` is
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
