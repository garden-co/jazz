//! The C ABI Go calls, exercised from Rust.

mod common;

use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use common::*;
use jazz_ffi::c_abi::*;
use serde_json::{Value, json};

fn c(text: &str) -> CString {
    CString::new(text).unwrap()
}

/// Takes ownership of a returned string.
fn owned(text: *mut c_char) -> String {
    assert!(!text.is_null());
    let copy = unsafe { CStr::from_ptr(text) }.to_str().unwrap().to_owned();
    unsafe { jazz_string_free(text) };
    copy
}

fn open(config: &str) -> Result<*mut JazzHandle, Value> {
    let mut err = ptr::null_mut();
    let handle = unsafe { jazz_open(c(config).as_ptr(), &mut err) };
    if handle.is_null() {
        Err(parse(&owned(err)))
    } else {
        assert!(err.is_null());
        Ok(handle)
    }
}

fn call(handle: *mut JazzHandle, method: &str, args: Value) -> Result<Value, Value> {
    let mut err = ptr::null_mut();
    let out = unsafe {
        jazz_call(
            handle,
            c(method).as_ptr(),
            c(&args.to_string()).as_ptr(),
            &mut err,
        )
    };
    if out.is_null() {
        Err(parse(&owned(err)))
    } else {
        assert!(err.is_null());
        Ok(parse(&owned(out)))
    }
}

#[derive(Default)]
struct Events(Mutex<Vec<(String, Value)>>);

unsafe extern "C" fn record(ctx: *mut c_void, kind: *const c_char, json: *const c_char) {
    let events = unsafe { &*(ctx as *const Events) };
    let kind = unsafe { CStr::from_ptr(kind) }.to_str().unwrap().to_owned();
    let json = parse(unsafe { CStr::from_ptr(json) }.to_str().unwrap());
    events.0.lock().unwrap().push((kind, json));
}

fn wait_for(events: &Events, count: usize) -> Vec<(String, Value)> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let seen = events.0.lock().unwrap().clone();
        if seen.len() >= count || Instant::now() > deadline {
            return seen;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Go's path: a local client through `jazz_call`, with a subscription whose
/// C callback sees the reset delta, then each write, and nothing after
/// `jazz_unsubscribe`.
#[test]
fn a_client_through_the_c_abi() {
    let h = open(&client_config()).unwrap();
    assert_eq!(call(h, "account", json!({})).unwrap(), Value::Null);
    let show = call(
        h,
        "insert",
        json!({ "table": "shows", "row": parse(&show(nil_uuid())) }),
    )
    .unwrap();
    let show_id = show["id"].as_str().unwrap().to_owned();

    let events = Box::new(Events::default());
    let mut err = ptr::null_mut();
    let sub = unsafe {
        jazz_subscribe(
            h,
            c(&tasks_of(&show_id)).as_ptr(),
            Some(record),
            &*events as *const Events as *mut c_void,
            &mut err,
        )
    };
    assert!(!sub.is_null() && err.is_null());
    let reset = wait_for(&events, 1);
    assert_eq!(reset[0].0, "delta");
    assert_eq!(reset[0].1["reset"], true);
    assert_eq!(reset[0].1["added"], json!([]));

    let rig = call(
        h,
        "insert",
        json!({ "table": "tasks", "row": parse(&task(&show_id, "Rig", 1.0)) }),
    )
    .unwrap();
    let id = rig["id"].as_str().unwrap().to_owned();
    assert_eq!(
        call(
            h,
            "update",
            json!({ "table": "tasks", "id": id, "patch": { "status": "doing" } })
        )
        .unwrap(),
        Value::Null
    );
    let one = call(h, "one", json!({ "query": { "table": "tasks", "where": [{ "column": "id", "op": "eq", "value": id }] } })).unwrap();
    assert_eq!(one["status"], "doing");
    assert_eq!(
        titles(&call(h, "all", json!({ "query": parse(&tasks_of(&show_id)) })).unwrap()),
        ["Rig"]
    );
    assert_eq!(
        call(h, "delete", json!({ "table": "tasks", "id": id })).unwrap(),
        Value::Null
    );
    assert_eq!(
        call(h, "one", json!({ "query": { "table": "tasks" } })).unwrap(),
        Value::Null
    );

    let seen = wait_for(&events, 4);
    let kinds: Vec<_> = seen
        .iter()
        .map(|(kind, delta)| (kind.as_str(), delta["reset"].as_bool()))
        .collect();
    assert_eq!(
        kinds[..4],
        [
            ("delta", Some(true)),
            ("delta", Some(false)),
            ("delta", Some(false)),
            ("delta", Some(false))
        ]
    );
    assert_eq!(seen[3].1["removed"], json!([id]));

    unsafe { jazz_unsubscribe(sub) };
    let before = events.0.lock().unwrap().len();
    call(
        h,
        "insert",
        json!({ "table": "tasks", "row": parse(&task(&show_id, "unseen", 2.0)) }),
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(events.0.lock().unwrap().len(), before);

    unsafe { jazz_close(h) };
}

/// Failures store `{kind, message}` JSON in `*err`; NULL handles, methods and
/// callbacks are refused rather than dereferenced.
#[test]
fn errors_come_back_as_json_and_null_is_refused() {
    assert_eq!(open("{").unwrap_err()["kind"], "refused");
    let h = open(&client_config()).unwrap();
    let error = call(
        h,
        "all",
        json!({ "query": { "table": "shows", "include": [{ "relation": "tasks", "query": { "offset": 1 } }] } }),
    )
    .unwrap_err();
    assert_eq!(error["kind"], "refused");
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .starts_with("not supported yet")
    );
    assert_eq!(call(h, "nope", json!({})).unwrap_err()["kind"], "refused");
    assert_eq!(
        call(h, "insert", json!({ "table": "tasks" })).unwrap_err()["kind"],
        "refused"
    );

    // NULL arguments are refused, not dereferenced; NULL err is allowed.
    let mut err = ptr::null_mut();
    assert!(
        unsafe { jazz_call(ptr::null_mut(), c"all".as_ptr(), c"{}".as_ptr(), &mut err) }.is_null()
    );
    assert_eq!(parse(&owned(err))["message"], "the handle is NULL");
    assert!(unsafe { jazz_call(h, ptr::null(), c"{}".as_ptr(), ptr::null_mut()) }.is_null());
    let mut err = ptr::null_mut();
    assert!(
        unsafe {
            jazz_subscribe(
                h,
                c"{\"table\":\"tasks\"}".as_ptr(),
                None,
                ptr::null_mut(),
                &mut err,
            )
        }
        .is_null()
    );
    assert_eq!(parse(&owned(err))["kind"], "refused");
    unsafe {
        jazz_unsubscribe(ptr::null_mut());
        jazz_string_free(ptr::null_mut());
        jazz_close(h);
        jazz_close(ptr::null_mut());
    }
}

/// Every returned string is owned by the caller and freed exactly once with
/// `jazz_string_free`.
#[test]
fn returned_strings_are_the_callers_to_free() {
    let h = open(&client_config()).unwrap();
    // Many calls, each result freed once by `owned`: under a leak or
    // double-free checker (Miri, ASan) this is where ownership shows.
    for i in 0..50 {
        let row = call(
            h,
            "insert",
            json!({ "table": "shows", "row": parse(&show(nil_uuid())) }),
        )
        .unwrap();
        assert!(row["id"].is_string(), "{i}");
        call(h, "all", json!({ "query": { "table": "shows" } })).unwrap();
    }
    let mut err = ptr::null_mut();
    let out = unsafe {
        jazz_call(
            h,
            c"all".as_ptr(),
            c"{\"query\":{\"table\":\"shows\"}}".as_ptr(),
            &mut err,
        )
    };
    assert!(err.is_null());
    let text = unsafe { CStr::from_ptr(out) }.to_bytes().len();
    assert!(text > 0);
    unsafe { jazz_string_free(out) };
    unsafe { jazz_close(h) };
}

/// carol's backend, through the C ABI, reads her account, creates a show
/// she chiefs and reads its tasks under StagePlan's permissions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backend_through_the_c_abi() {
    let app_id = jazz_app_support::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let config = backend_config(&server, &app_id.to_string(), "carol");
    let result = tokio::task::spawn_blocking(move || {
        let h = open(&config).unwrap();
        let bundle = call(h, "bundle", json!({})).unwrap();
        assert!(bundle.as_str().is_some_and(|b| !b.is_empty()), "{bundle}");
        let account = call(h, "account", json!({})).unwrap();
        let account = account.as_str().expect("carol's account").to_owned();
        let show = call(
            h,
            "insert",
            json!({ "table": "shows", "row": parse(&show(&account)) }),
        )
        .unwrap();
        let show_id = show["id"].as_str().unwrap();
        call(
            h,
            "insert",
            json!({ "table": "tasks", "row": parse(&task(show_id, "Doors", 1.0)) }),
        )
        .unwrap();
        assert_eq!(
            titles(&call(h, "all", json!({ "query": parse(&tasks_of(show_id)) })).unwrap()),
            ["Doors"]
        );
        unsafe { jazz_close(h) };
    })
    .await;
    server.shutdown().await;
    result.unwrap();
}

#[derive(Default)]
struct Slow {
    entered: std::sync::atomic::AtomicUsize,
    finished: std::sync::atomic::AtomicUsize,
}

unsafe extern "C" fn slow(ctx: *mut c_void, _kind: *const c_char, _json: *const c_char) {
    use std::sync::atomic::Ordering::SeqCst;
    let slow = unsafe { &*(ctx as *const Slow) };
    slow.entered.fetch_add(1, SeqCst);
    std::thread::sleep(Duration::from_millis(200));
    slow.finished.fetch_add(1, SeqCst);
}

/// `jazz_unsubscribe` returns only once a callback already running has
/// returned, so the host (Go's `cgo.Handle.Delete`) may free `ctx` right
/// after:
///
/// ```
/// delivery thread ──cb enters──────(sleeps)──────cb returns──┐
/// host ──────────────────jazz_unsubscribe (waits)────────────┴─► free(ctx)
/// ```
#[test]
fn unsubscribe_waits_for_a_running_callback() {
    use std::sync::atomic::Ordering::SeqCst;
    let h = open(&client_config()).unwrap();
    let slow_ctx = Box::new(Slow::default());
    let mut err = ptr::null_mut();
    let sub = unsafe {
        jazz_subscribe(
            h,
            c"{\"table\":\"shows\"}".as_ptr(),
            Some(slow),
            &*slow_ctx as *const Slow as *mut c_void,
            &mut err,
        )
    };
    assert!(!sub.is_null());
    let deadline = Instant::now() + Duration::from_secs(5);
    while slow_ctx.entered.load(SeqCst) == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(slow_ctx.entered.load(SeqCst), 1);
    unsafe { jazz_unsubscribe(sub) };
    assert_eq!(
        slow_ctx.finished.load(SeqCst),
        slow_ctx.entered.load(SeqCst)
    );
    drop(slow_ctx);
    unsafe { jazz_close(h) };
}

struct SelfFreeing {
    subscription: Mutex<*mut JazzSubscription>,
    calls: std::sync::atomic::AtomicUsize,
}

unsafe extern "C" fn unsubscribe_self(ctx: *mut c_void, _: *const c_char, _: *const c_char) {
    let this = unsafe { &*(ctx as *const SelfFreeing) };
    this.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let sub = std::mem::replace(&mut *this.subscription.lock().unwrap(), ptr::null_mut());
    unsafe { jazz_unsubscribe(sub) };
}

/// A callback may free its own subscription: `jazz_unsubscribe` from the
/// delivery thread doesn't wait for itself.
#[test]
fn a_callback_may_unsubscribe_itself() {
    use std::sync::atomic::Ordering::SeqCst;
    let h = open(&client_config()).unwrap();
    let this = Box::new(SelfFreeing {
        subscription: Mutex::new(ptr::null_mut()),
        calls: Default::default(),
    });
    // Held until the subscription is stored, so the callback finds it.
    let mut guard = this.subscription.lock().unwrap();
    let mut err = ptr::null_mut();
    let sub = unsafe {
        jazz_subscribe(
            h,
            c"{\"table\":\"shows\"}".as_ptr(),
            Some(unsubscribe_self),
            &*this as *const SelfFreeing as *mut c_void,
            &mut err,
        )
    };
    assert!(!sub.is_null());
    *guard = sub;
    drop(guard);
    let deadline = Instant::now() + Duration::from_secs(5);
    while this.calls.load(SeqCst) == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    call(
        h,
        "insert",
        json!({ "table": "shows", "row": parse(&show(nil_uuid())) }),
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(this.calls.load(SeqCst), 1);
    assert!(this.subscription.lock().unwrap().is_null());
    unsafe { jazz_close(h) };
}

/// Closing the app ends each open subscription with a last `"error"` of
/// kind `"closed"`; the subscription is still freed safely afterwards.
#[test]
fn close_ends_subscriptions_and_unsubscribe_may_follow() {
    let h = open(&client_config()).unwrap();
    let events = Box::new(Events::default());
    let mut err = ptr::null_mut();
    let sub = unsafe {
        jazz_subscribe(
            h,
            c"{\"table\":\"shows\"}".as_ptr(),
            Some(record),
            &*events as *const Events as *mut c_void,
            &mut err,
        )
    };
    assert!(!sub.is_null());
    wait_for(&events, 1);
    unsafe { jazz_close(h) };
    let seen = wait_for(&events, 2);
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert_eq!(seen[1].0, "error");
    assert_eq!(seen[1].1["kind"], "closed");
    unsafe { jazz_unsubscribe(sub) };
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(events.0.lock().unwrap().len(), 2);
}
