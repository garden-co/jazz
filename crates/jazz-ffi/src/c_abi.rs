//! The C ABI declared in `include/jazz_ffi.h`, for Go (cgo) and C.
//!
//! Strings are UTF-8 and NUL-terminated. Every string a function returns,
//! and every message it stores in `*err`, belongs to the caller, who frees
//! it with [`jazz_string_free`]. On failure a function returns NULL and, if
//! `err` is not NULL, stores `{"kind":"refused"|"io"|"closed","message":…}`
//! in `*err`. Strings passed to a subscription's callback are only valid
//! during the call.

use std::ffi::{CStr, CString, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use serde_json::{Value as Json, json};

use crate::JazzError;
use crate::core::{Core, Event, SubscriptionCore};
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
    let kind = match error {
        JazzError::Refused { .. } => "refused",
        JazzError::Io { .. } => "io",
        JazzError::Closed => "closed",
    };
    json!({ "kind": kind, "message": error.to_string() }).to_string()
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

/// Runs `call`, turning errors and panics into NULL plus `*err`.
fn guarded<T>(err: *mut *mut c_char, null: T, call: impl FnOnce() -> Result<T, FfiError>) -> T {
    if !err.is_null() {
        // SAFETY: the caller passes NULL or a valid `char**`.
        unsafe { *err = ptr::null_mut() };
    }
    let failure = match catch_unwind(AssertUnwindSafe(call)) {
        Ok(Ok(value)) => return value,
        Ok(Err(error)) => JazzError::from(error),
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
    guarded(err, ptr::null_mut(), || {
        // SAFETY: per the function's contract.
        let config = unsafe { text(config_json, "config_json") }?;
        Ok(Box::into_raw(Box::new(JazzHandle(Core::open(config)?))))
    })
}

/// Calls `method` (`all`, `one`, `insert`, `update`, `delete`, `account`)
/// with its arguments as JSON: `{query}`, `{query}`, `{table,row}`,
/// `{table,id,patch}`, `{table,id}`, `{}`. Returns its result as JSON:
/// rows, a row or `null`, the inserted row, `null`, `null`, the account id
/// or `null`.
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
    guarded(err, ptr::null_mut(), || {
        // SAFETY: per the function's contract.
        let (core, method, args) = unsafe {
            (
                handle(handle_ptr)?,
                text(method, "method")?,
                text(args_json, "args_json")?,
            )
        };
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
        let result = match method {
            "all" => core.all(&field("query")?.to_string())?,
            "one" => core
                .one(&field("query")?.to_string())?
                .unwrap_or_else(|| "null".into()),
            "insert" => core.insert(&string("table")?, &field("row")?.to_string())?,
            "update" => {
                core.update(
                    &string("table")?,
                    &string("id")?,
                    &field("patch")?.to_string(),
                )?;
                "null".into()
            }
            "delete" => {
                core.delete(&string("table")?, &string("id")?)?;
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
    guarded(err, ptr::null_mut(), || {
        // SAFETY: per the function's contract.
        let (core, query) = unsafe { (handle(handle_ptr)?, text(query_json, "query_json")?) };
        let cb = cb.ok_or_else(|| refused("the callback is NULL"))?;
        let ctx = Context(ctx);
        let subscription = core.subscribe(
            query,
            Box::new(move |event| {
                let ctx = &ctx;
                let (kind, json) = match event {
                    Event::Delta(json) => (c"delta", json),
                    Event::Error(error) => (c"error", error_json(&error)),
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
