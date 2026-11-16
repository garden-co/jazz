//! jazz-ffi v2 end users through the C ABI (Go's path): `jazz_open_user`
//! with a C token callback, `jazz_generate_device_key`, the session calls of
//! `jazz_call`, `jazz_watch` / `jazz_unwatch`, and the token callback's
//! lifetime against `jazz_close`.

mod common;
#[allow(dead_code)]
#[path = "../../jazz-app/tests/relay/mod.rs"]
mod relay;

use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use common::*;
use jazz_ffi::c_abi::*;
use serde_json::{Value, json};

unsafe extern "C" {
    fn malloc(size: usize) -> *mut c_void;
}

/// A `malloc`ed C string, as a C or Go host hands one over.
fn c_owned(text: &str) -> *mut c_char {
    let bytes = text.as_bytes();
    unsafe {
        let out = malloc(bytes.len() + 1) as *mut u8;
        ptr::copy_nonoverlapping(bytes.as_ptr(), out, bytes.len());
        *out.add(bytes.len()) = 0;
        out as *mut c_char
    }
}

/// What a host's token callback does, and what it saw.
struct Host {
    /// Answer `refresh` with a failure whose message is this (as a failing
    /// Go provider would), instead of a token.
    fail_refresh_with: Option<&'static str>,
    /// How long each call takes.
    slow: Duration,
    calls: AtomicUsize,
    running: AtomicBool,
    closed: AtomicBool,
    /// A call that started after `jazz_close` returned.
    after_close: AtomicBool,
    reasons: Mutex<Vec<String>>,
}

impl Host {
    fn new(fail_refresh_with: Option<&'static str>, slow: Duration) -> Box<Self> {
        Box::new(Self {
            fail_refresh_with,
            slow,
            calls: AtomicUsize::new(0),
            running: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            after_close: AtomicBool::new(false),
            reasons: Mutex::new(Vec::new()),
        })
    }
}

unsafe extern "C" fn token_cb(
    ctx: *mut c_void,
    reason: *const c_char,
    token: *mut *mut c_char,
) -> i32 {
    let host = unsafe { &*(ctx as *const Host) };
    if host.closed.load(Ordering::SeqCst) {
        host.after_close.store(true, Ordering::SeqCst);
    }
    host.running.store(true, Ordering::SeqCst);
    host.calls.fetch_add(1, Ordering::SeqCst);
    let reason = unsafe { CStr::from_ptr(reason) }
        .to_str()
        .unwrap()
        .to_owned();
    host.reasons.lock().unwrap().push(reason.clone());
    std::thread::sleep(host.slow);
    host.running.store(false, Ordering::SeqCst);
    if reason == "refresh"
        && let Some(message) = host.fail_refresh_with
    {
        unsafe { *token = c_owned(message) };
        return 1;
    }
    unsafe { *token = c_owned("token-alice") };
    0
}

fn c(text: &str) -> CString {
    CString::new(text).unwrap()
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
    let take = |text: *mut c_char| {
        let copy = unsafe { CStr::from_ptr(text) }.to_str().unwrap().to_owned();
        unsafe { jazz_string_free(text) };
        parse(&copy)
    };
    if out.is_null() {
        Err(take(err))
    } else {
        Ok(take(out))
    }
}

struct Seen(Mutex<Vec<(String, Value)>>);

unsafe extern "C" fn watch_cb(ctx: *mut c_void, kind: *const c_char, json: *const c_char) {
    let seen = unsafe { &*(ctx as *const Seen) };
    let kind = unsafe { CStr::from_ptr(kind) }.to_str().unwrap().to_owned();
    let json = parse(unsafe { CStr::from_ptr(json) }.to_str().unwrap());
    seen.0.lock().unwrap().push((kind, json));
}

struct App {
    server: jazz_server::JazzServer,
    backend: jazz_app_support::Jazz,
    _relay: relay::Relay,
    config: Box<dyn Fn(Value) -> String + Send + Sync>,
}

async fn app() -> App {
    let app_id = jazz_app_support::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let files = jazz_app_support::AppFiles::new(STAGE_PLAN_APP.as_bytes().to_vec(), None)
        .with_permissions(STAGE_PLAN_PERMISSIONS.as_bytes().to_vec());
    let backend = jazz_app_support::Jazz::open(
        files,
        jazz_app_support::Config::new(app_id)
            .server(server.base_url())
            .backend(jazz_app_support::Backend::new(
                server.backend_secret(),
                server.admin_secret(),
                "stage-plan",
            )),
    )
    .await
    .unwrap();
    let relay = relay::start(
        &server.base_url(),
        app_id,
        server.backend_secret(),
        server.admin_secret(),
        backend.bundle().unwrap(),
    )
    .await;
    let (id, endpoint) = (app_id.to_string(), relay.endpoint.clone());
    App {
        server,
        backend,
        _relay: relay,
        config: Box::new(move |auth| {
            json!({ "app": STAGE_PLAN_APP, "appId": id, "serverUrl": endpoint, "auth": auth, "contract": 2 })
                .to_string()
        }),
    }
}

impl App {
    async fn stop(self) {
        self.backend.shutdown().await.unwrap();
        self.server.shutdown().await;
    }
}

/// Go's sign-in path: a device key it generated, and a bearer from its C
/// token callback, with the session calls, a watch, and a refresh whose
/// fetch fails: the failure is not a token (its message never becomes the
/// bearer) and not a sign-out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_hosts_sign_in_and_follow_their_session() {
    let app = app().await;
    let device = (app.config)(json!({ "deviceKey": "PLACEHOLDER" }));
    let bearer = (app.config)(json!({ "bearer": true }));
    let result = tokio::task::spawn_blocking(move || unsafe {
        let mut err = ptr::null_mut();
        let key = jazz_generate_device_key(&mut err);
        assert!(!key.is_null());
        let key_text = CStr::from_ptr(key).to_str().unwrap().to_owned();
        jazz_string_free(key);
        assert!(key_text.starts_with("jazz-auth-v1:"));
        let device = device.replace("PLACEHOLDER", &key_text);
        let carol = jazz_open(c(&device).as_ptr(), &mut err);
        assert!(
            !carol.is_null(),
            "{}",
            CStr::from_ptr(err).to_str().unwrap()
        );
        let state = call(carol, "session", json!({})).unwrap();
        assert_eq!(state["status"], "signedIn", "{state}");
        assert_eq!(state["user"]["kind"], "device");
        assert!(
            call(carol, "syncStatus", json!({}))
                .unwrap()
                .get("link")
                .is_some()
        );
        jazz_close(carol);

        // The fetch for refresh fails with "token-bob" as its message: were
        // that taken for a token, alice would become bob.
        let host = Host::new(Some("token-bob"), Duration::ZERO);
        let alice = jazz_open_user(
            c(&bearer).as_ptr(),
            Some(token_cb),
            &*host as *const Host as *mut c_void,
            &mut err,
        );
        assert!(
            !alice.is_null(),
            "{}",
            CStr::from_ptr(err).to_str().unwrap()
        );
        assert_eq!(
            host.reasons.lock().unwrap().first().map(String::as_str),
            Some("connect")
        );
        let seen = Box::new(Seen(Mutex::new(Vec::new())));
        let watch = jazz_watch(
            alice,
            c"session".as_ptr(),
            Some(watch_cb),
            &*seen as *const Seen as *mut c_void,
            &mut err,
        );
        assert!(!watch.is_null());
        let refreshed = call(alice, "refresh", json!({}));
        assert!(refreshed.is_err(), "{refreshed:?}");
        let state = call(alice, "session", json!({})).unwrap();
        assert_eq!(state["status"], "signedIn", "{state}");
        assert_eq!(state["user"]["subject"], "alice", "{state}");
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            seen.0
                .lock()
                .unwrap()
                .iter()
                .all(|(kind, s)| kind == "session" && s["user"]["subject"] != "bob")
        );
        assert_eq!(seen.0.lock().unwrap()[0].1["status"], "signedIn");
        jazz_unwatch(watch);
        assert_eq!(call(alice, "signOut", json!({})).unwrap(), Value::Null);
        assert!(call(alice, "all", json!({ "query": { "table": "shows" } })).is_err());
        jazz_close(alice);
    })
    .await;
    app.stop().await;
    result.unwrap();
}

/// After `signOut` (which ends the app) the token callback is never called
/// again either, though a fetch was in progress and close follows later.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_token_callback_runs_after_sign_out() {
    let app = app().await;
    let bearer = (app.config)(json!({ "bearer": true }));
    let result = tokio::task::spawn_blocking(move || unsafe {
        let host = Host::new(Some("offline"), Duration::from_millis(800));
        let mut err = ptr::null_mut();
        let alice = jazz_open_user(
            c(&bearer).as_ptr(),
            Some(token_cb),
            &*host as *const Host as *mut c_void,
            &mut err,
        );
        assert!(!alice.is_null());
        // A failed refresh: the session retries on its own, with a slow
        // fetch; sign out and close while that retry is in flight.
        let _ = call(alice, "refresh", json!({}));
        let asked = host.calls.load(Ordering::SeqCst);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !(host.calls.load(Ordering::SeqCst) > asked && host.running.load(Ordering::SeqCst)) {
            assert!(std::time::Instant::now() < deadline, "no retry came");
            std::thread::sleep(Duration::from_millis(5));
        }
        let _ = call(alice, "signOut", json!({}));
        jazz_close(alice);
        assert!(
            !host.running.load(Ordering::SeqCst),
            "close returned while the callback ran"
        );
        host.closed.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(1500));
        assert!(
            !host.after_close.load(Ordering::SeqCst),
            "the callback ran after close"
        );
        drop(host);
    })
    .await;
    app.stop().await;
    result.unwrap();
}

/// When `jazz_close` returns, the token callback is not running and is never
/// called again: a host may free its ctx (Go: delete its cgo.Handle) then.
/// Here a slow fetch is in progress when close is called.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_token_callback_runs_after_close() {
    let app = app().await;
    let bearer = (app.config)(json!({ "bearer": true }));
    let result = tokio::task::spawn_blocking(move || unsafe {
        let host = Host::new(None, Duration::from_millis(400));
        let ctx = &*host as *const Host as usize;
        let mut err = ptr::null_mut();
        let alice = jazz_open_user(
            c(&bearer).as_ptr(),
            Some(token_cb),
            ctx as *mut c_void,
            &mut err,
        );
        assert!(
            !alice.is_null(),
            "{}",
            CStr::from_ptr(err).to_str().unwrap()
        );
        let handle = alice as usize;
        // A refresh whose fetch is still running when close is called.
        let refreshing = std::thread::spawn(move || {
            let _ = call(handle as *mut JazzHandle, "refresh", json!({}));
        });
        while !host.running.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(5));
        }
        jazz_close(alice);
        assert!(
            !host.running.load(Ordering::SeqCst),
            "close returned while the callback ran"
        );
        host.closed.store(true, Ordering::SeqCst);
        refreshing.join().unwrap();
        // The session's own renewals would come now, if any could.
        std::thread::sleep(Duration::from_millis(1500));
        assert!(
            !host.after_close.load(Ordering::SeqCst),
            "the callback ran after close"
        );
        drop(host);
    })
    .await;
    app.stop().await;
    result.unwrap();
}

/// Through the C ABI: a device account links to the app's sign-in (the
/// session becomes that user's), a bearer the backend rejects is a refusal
/// (not I/O) and leaves the session as it was, and a backend's `bundle`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_hosts_link_and_read_a_backends_bundle() {
    let app = app().await;
    let device = (app.config)(json!({ "deviceKey": "PLACEHOLDER" }));
    let bundle = app.backend.bundle().unwrap().to_owned();
    let result = tokio::task::spawn_blocking(move || unsafe {
        let mut err = ptr::null_mut();
        let key = jazz_generate_device_key(&mut err);
        let key_text = CStr::from_ptr(key).to_str().unwrap().to_owned();
        jazz_string_free(key);
        let carol = jazz_open(
            c(&device.replace("PLACEHOLDER", &key_text)).as_ptr(),
            &mut err,
        );
        assert!(!carol.is_null());
        let rejected = call(carol, "link", json!({ "bearer": "not-a-token" })).unwrap_err();
        assert_eq!(rejected["kind"], "refused", "{rejected}");
        assert_eq!(
            call(carol, "session", json!({})).unwrap()["user"]["kind"],
            "device"
        );
        let user = call(carol, "link", json!({ "bearer": "token-carol" })).unwrap();
        assert_eq!(user["kind"], "backend", "{user}");
        assert_eq!(
            call(carol, "session", json!({})).unwrap()["user"]["subject"],
            "carol"
        );
        assert_eq!(call(carol, "bundle", json!({})).unwrap(), Value::Null);
        jazz_close(carol);

        // Another device linking the same (already linked) user: the backend
        // refuses it, a refusal (not I/O).
        let key = jazz_generate_device_key(&mut err);
        let other_key = CStr::from_ptr(key).to_str().unwrap().to_owned();
        jazz_string_free(key);
        let other = jazz_open(
            c(&device.replace("PLACEHOLDER", &other_key)).as_ptr(),
            &mut err,
        );
        assert!(!other.is_null());
        let conflict = call(other, "link", json!({ "bearer": "token-carol" })).unwrap_err();
        assert_eq!(conflict["kind"], "refused", "{conflict}");
        assert_eq!(
            call(other, "session", json!({})).unwrap()["user"]["kind"],
            "device"
        );
        jazz_close(other);
    })
    .await;
    let _ = bundle;
    app.stop().await;
    result.unwrap();
}

/// Concurrent fetches: `jazz_close` called from inside one token callback
/// waits for the others still running (not for itself), so the host may
/// free `ctx` when that callback returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_from_a_token_callback_waits_for_the_other_fetches() {
    let app = app().await;
    let bearer = (app.config)(json!({ "bearer": true }));
    let result = tokio::task::spawn_blocking(move || unsafe {
        let host = Host::new(None, Duration::from_millis(600));
        let mut err = ptr::null_mut();
        let alice = jazz_open_user(
            c(&bearer).as_ptr(),
            Some(token_cb),
            &*host as *const Host as *mut c_void,
            &mut err,
        );
        assert!(!alice.is_null());
        // Several refreshes at once: their fetches overlap.
        let handle = alice as usize;
        let refreshes: Vec<_> = (0..4)
            .map(|_| {
                std::thread::spawn(move || {
                    let _ = call(handle as *mut JazzHandle, "refresh", json!({}));
                })
            })
            .collect();
        while host.calls.load(Ordering::SeqCst) < 2 {
            std::thread::sleep(Duration::from_millis(5));
        }
        jazz_close(alice);
        assert!(
            !host.running.load(Ordering::SeqCst),
            "close returned while fetches ran"
        );
        host.closed.store(true, Ordering::SeqCst);
        for refresh in refreshes {
            refresh.join().unwrap();
        }
        std::thread::sleep(Duration::from_millis(500));
        assert!(!host.after_close.load(Ordering::SeqCst));
        drop(host);
    })
    .await;
    app.stop().await;
    result.unwrap();
}
