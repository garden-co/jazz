//! Closing without freeing (`"close"` in `jazz_call` and
//! `jazz_session_call`), as a C host runs it: close, join every call in
//! flight, then free. Every wait has a watchdog: a hang is a failure, not a
//! stuck test run.

mod common;
#[allow(dead_code)]
#[path = "../../jazz-app/tests/relay/mod.rs"]
mod relay;

use std::ffi::{CStr, CString, c_char, c_void};
use std::io::Read;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::{Duration, Instant};

use common::*;
use jazz_ffi::c_abi::*;
use serde_json::{Value, json};

/// What a close, and what it ends, may take: the waits on the server end
/// at once, so only local work and the stream sessions' 2 s are left. Well
/// short of the 30 s job drain, which would be the bug.
const PROMPT: Duration = Duration::from_secs(8);

unsafe extern "C" {
    fn malloc(size: usize) -> *mut c_void;
}

fn c(text: &str) -> CString {
    CString::new(text).unwrap()
}

fn take(text: *mut c_char) -> Value {
    assert!(!text.is_null());
    let copy = unsafe { CStr::from_ptr(text) }.to_str().unwrap().to_owned();
    unsafe { jazz_string_free(text) };
    parse(&copy)
}

fn result(out: *mut c_char, err: *mut c_char) -> Result<Value, Value> {
    if out.is_null() {
        Err(take(err))
    } else {
        Ok(take(out))
    }
}

fn kind(result: &Result<Value, Value>) -> &str {
    match result {
        Ok(_) => "ok",
        Err(error) => error["kind"].as_str().unwrap_or_default(),
    }
}

/// A handle as a host passes it between its threads.
#[derive(Clone, Copy)]
struct Handle(*mut JazzHandle);
// SAFETY: calls on one handle may come from any threads (header).
unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}

impl Handle {
    fn call(self, method: &str, args: Value) -> Result<Value, Value> {
        let (method, args) = (c(method), c(&args.to_string()));
        let mut err = ptr::null_mut();
        let out = unsafe { jazz_call(self.0, method.as_ptr(), args.as_ptr(), &mut err) };
        result(out, err)
    }

    fn spawn(self, method: &'static str, args: Value) -> Running<Result<Value, Value>> {
        Running::start(move || self.call(method, args))
    }

    /// Only after every call on it was joined.
    fn free(self) {
        Running::start(move || unsafe { jazz_close({ self }.0) }).within(PROMPT, "jazz_close");
    }

    fn for_user(self, user: &str) -> Result<Handle, Value> {
        let mut err = ptr::null_mut();
        let out = unsafe { jazz_for_user(self.0, c(user).as_ptr(), ptr::null(), &mut err) };
        if out.is_null() {
            Err(take(err))
        } else {
            Ok(Handle(out))
        }
    }
}

#[derive(Clone, Copy)]
struct Session(*mut jazz_ffi::c_abi::JazzSession);
// SAFETY: as for `Handle`.
unsafe impl Send for Session {}
unsafe impl Sync for Session {}

impl Session {
    fn open(config: &str, host: Option<&'static Host>) -> Session {
        let mut err = ptr::null_mut();
        let (callback, ctx): (JazzTokenCallback, *mut c_void) = match host {
            Some(host) => (Some(token_cb), host.ctx()),
            None => (None, ptr::null_mut()),
        };
        let session = unsafe { jazz_session_open(c(config).as_ptr(), callback, ctx, &mut err) };
        assert!(!session.is_null(), "{}", take(err));
        Session(session)
    }

    fn call(self, method: &str, args: Value) -> Result<Value, Value> {
        let (method, args) = (c(method), c(&args.to_string()));
        let mut err = ptr::null_mut();
        let out = unsafe { jazz_session_call(self.0, method.as_ptr(), args.as_ptr(), &mut err) };
        result(out, err)
    }

    fn spawn(self, method: &'static str) -> Running<Result<Value, Value>> {
        Running::start(move || self.call(method, json!({})))
    }

    fn jazz(self) -> Result<Handle, Value> {
        let mut err = ptr::null_mut();
        let out = unsafe { jazz_session_jazz(self.0, &mut err) };
        if out.is_null() {
            Err(take(err))
        } else {
            Ok(Handle(out))
        }
    }

    /// Only after every call on it was joined.
    fn free(self) {
        Running::start(move || unsafe { jazz_session_close({ self }.0) })
            .within(PROMPT, "jazz_session_close");
    }
}

/// A call on a thread of its own, awaited only with a watchdog.
struct Running<T> {
    answer: mpsc::Receiver<T>,
}

impl<T: Send + 'static> Running<T> {
    fn start(call: impl FnOnce() -> T + Send + 'static) -> Self {
        let (sender, answer) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = sender.send(call());
        });
        Self { answer }
    }

    fn within(&self, limit: Duration, what: &str) -> T {
        self.answer
            .recv_timeout(limit)
            .unwrap_or_else(|error| panic!("{what} did not return within {limit:?}: {error}"))
    }

    fn still_running(&self, what: &str) {
        assert!(
            matches!(
                self.answer.recv_timeout(Duration::from_millis(400)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "{what} returned early"
        );
    }
}

fn until(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !done() {
        assert!(Instant::now() < deadline, "never: {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// What the token callback does besides giving its token.
#[derive(Clone, Copy)]
enum Then {
    Nothing,
    /// Blocks until the test releases it.
    Stall,
    /// Closes this handle from inside the callback.
    CloseHandle(Handle),
    /// Closes this handle once the test releases it.
    StallThenCloseHandle(Handle),
    /// Signs this handle out once the test releases it.
    StallThenSignOut(Handle),
    /// Closes this session from inside the callback.
    CloseSession(Session),
}

/// A host's token callback and ctx. Leaked until `free_ctx`: a test that
/// fails may leave a callback running.
struct Host {
    token: &'static str,
    then: Mutex<Then>,
    released: (Mutex<bool>, Condvar),
    running: AtomicUsize,
    closed_inside: Mutex<Vec<Result<Value, Value>>>,
    /// How long the callback goes on after closing from inside.
    linger: Mutex<Duration>,
    freed: AtomicBool,
    after_free: AtomicBool,
}

impl Host {
    fn new(token: &'static str) -> &'static Self {
        Box::leak(Box::new(Self {
            token,
            then: Mutex::new(Then::Nothing),
            released: (Mutex::new(false), Condvar::new()),
            running: AtomicUsize::new(0),
            closed_inside: Mutex::default(),
            linger: Mutex::default(),
            freed: AtomicBool::new(false),
            after_free: AtomicBool::new(false),
        }))
    }

    fn ctx(&'static self) -> *mut c_void {
        self as *const Self as *mut c_void
    }

    fn then(&self, then: Then) {
        *self.then.lock().unwrap() = then;
    }

    fn release(&self) {
        *self.released.0.lock().unwrap() = true;
        self.released.1.notify_all();
    }

    fn wait_released(&self) {
        let mut released = self.released.0.lock().unwrap();
        while !*released {
            released = self.released.1.wait(released).unwrap();
        }
    }

    fn idle(&self) -> bool {
        self.running.load(Ordering::SeqCst) == 0
    }

    /// The host may free its ctx now: nothing runs, nothing will.
    fn assert_freeable(&self) {
        assert!(
            self.idle(),
            "a token callback was running when free returned"
        );
        self.freed.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(300));
        assert!(
            !self.after_free.load(Ordering::SeqCst),
            "a token callback ran after free"
        );
    }
}

/// The host frees its ctx, once free returned: for a sanitizer to see any
/// later use.
fn free_ctx(host: *const Host) {
    // SAFETY: leaked from a `Box` by `Host::new`, and not used again: the
    // handle it was given to is freed.
    drop(unsafe { Box::from_raw(host.cast_mut()) });
}

unsafe extern "C" fn token_cb(
    ctx: *mut c_void,
    _reason: *const c_char,
    token: *mut *mut c_char,
) -> i32 {
    let host = unsafe { &*(ctx as *const Host) };
    if host.freed.load(Ordering::SeqCst) {
        host.after_free.store(true, Ordering::SeqCst);
    }
    host.running.fetch_add(1, Ordering::SeqCst);
    let then = *host.then.lock().unwrap();
    let closed = match then {
        Then::Nothing => None,
        Then::Stall => {
            host.wait_released();
            None
        }
        Then::CloseHandle(handle) => Some(handle.call("close", json!({}))),
        Then::StallThenCloseHandle(handle) => {
            host.wait_released();
            Some(handle.call("close", json!({})))
        }
        Then::StallThenSignOut(handle) => {
            host.wait_released();
            Some(handle.call("signOut", json!({})))
        }
        Then::CloseSession(session) => Some(session.call("close", json!({}))),
    };
    if let Some(closed) = closed {
        host.closed_inside.lock().unwrap().push(closed);
        let linger = *host.linger.lock().unwrap();
        std::thread::sleep(linger);
    }
    let text = c(host.token);
    let text = text.as_bytes_with_nul();
    unsafe {
        let out = malloc(text.len()) as *mut u8;
        ptr::copy_nonoverlapping(text.as_ptr(), out, text.len());
        *token = out as *mut c_char;
    }
    host.running.fetch_sub(1, Ordering::SeqCst);
    0
}

/// A main backend (the relay stand-in) on a runtime that a hung call can't
/// hold up.
struct App {
    server: Option<jazz_server::JazzServer>,
    backend: Option<jazz_app_support::Jazz>,
    relay: Option<relay::Relay>,
    runtime: Option<tokio::runtime::Runtime>,
    app_id: String,
}

impl App {
    fn start() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let app_id = jazz_app_support::AppId::random();
        let (server, backend, relay) = runtime.block_on(async {
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
            (server, backend, relay)
        });
        Self {
            server: Some(server),
            backend: Some(backend),
            relay: Some(relay),
            runtime: Some(runtime),
            app_id: app_id.to_string(),
        }
    }

    fn relay(&self) -> &relay::Relay {
        self.relay.as_ref().unwrap()
    }

    fn open(&self, auth: Value, host: &'static Host) -> Handle {
        let mut config = json!({ "app": STAGE_PLAN_APP, "appId": self.app_id,
            "serverUrl": self.relay().endpoint, "auth": auth, "contract": 2 });
        if config["auth"]["credential"] == true {
            config["permissions"] = json!(STAGE_PLAN_PERMISSIONS);
        }
        let mut err = ptr::null_mut();
        let handle = unsafe {
            jazz_open_user(
                c(&config.to_string()).as_ptr(),
                Some(token_cb),
                host.ctx(),
                &mut err,
            )
        };
        assert!(!handle.is_null(), "{}", take(err));
        Handle(handle)
    }

    fn machine(&self) -> (Handle, &'static Host) {
        let host = Host::new("machine-trusted");
        (self.open(json!({ "credential": true }), host), host)
    }

    /// Signs alice in once, so that she has an account to act as.
    fn register_alice(&self) {
        let alice = self.open(json!({ "bearer": true }), Host::new("token-alice"));
        alice.free();
    }
}

impl Drop for App {
    fn drop(&mut self) {
        let runtime = self.runtime.take().unwrap();
        {
            let _entered = runtime.enter();
            drop(self.relay.take());
            drop(self.backend.take());
            drop(self.server.take());
        }
        runtime.shutdown_background();
    }
}

/// A server that refuses connections.
fn unreachable() -> String {
    let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let at = free.local_addr().unwrap();
    format!("http://{at}/jazz")
}

/// A server that takes a request and never answers it; counts the HTTP
/// requests for the accounts API (the session's own).
fn stalling() -> (String, Arc<AtomicUsize>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/jazz", listener.local_addr().unwrap());
    let asked = Arc::new(AtomicUsize::new(0));
    let counting = asked.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let counting = counting.clone();
            std::thread::spawn(move || {
                let mut head = Vec::new();
                let mut chunk = [0; 1024];
                let mut counted = false;
                while let Ok(read) = stream.read(&mut chunk) {
                    if read == 0 {
                        return;
                    }
                    head.extend_from_slice(&chunk[..read]);
                    if !counted && String::from_utf8_lossy(&head).contains("/accounts/backend-") {
                        counted = true;
                        counting.fetch_add(1, Ordering::SeqCst);
                    }
                }
            });
        }
    });
    (endpoint, asked)
}

fn device(endpoint: &str) -> Handle {
    let mut err = ptr::null_mut();
    let key = unsafe { jazz_generate_device_key(&mut err) };
    assert!(!key.is_null());
    let text = unsafe { CStr::from_ptr(key) }.to_str().unwrap().to_owned();
    unsafe { jazz_string_free(key) };
    let config = json!({ "app": STAGE_PLAN_APP, "appId": jazz_app_support::AppId::random().to_string(),
        "serverUrl": endpoint, "auth": { "deviceKey": text }, "contract": 2 });
    let handle = unsafe { jazz_open(c(&config.to_string()).as_ptr(), &mut err) };
    assert!(!handle.is_null(), "{}", take(err));
    Handle(handle)
}

fn guest_session(endpoint: &str, host: Option<&'static Host>) -> Session {
    let mut config = json!({ "app": STAGE_PLAN_APP, "appId": jazz_app_support::AppId::random().to_string(),
        "serverUrl": endpoint, "initial": "device", "contract": 2 });
    if host.is_some() {
        config["auth"] = json!({ "bearer": true });
    }
    Session::open(&config.to_string(), host)
}

fn write_show(handle: Handle) {
    let chief = handle.call("account", json!({})).unwrap();
    let chief = chief.as_str().unwrap_or(nil_uuid());
    handle
        .call(
            "insert",
            json!({ "table": "shows", "row": parse(&show(chief)) }),
        )
        .unwrap();
}

/// A waitForSync that only the server (or close) can end.
fn pending_wait(handle: Handle) -> Running<Result<Value, Value>> {
    write_show(handle);
    let waiting = handle.spawn("waitForSync", json!({ "tier": "global" }));
    waiting.still_running("waitForSync");
    waiting
}

/// The hang jazz-go and the Kotlin/Native spike hit: close ends the pending
/// wait, so the host can join it and only then free.
#[test]
fn close_ends_a_root_handles_wait() {
    let app = device(&unreachable());
    let waiting = pending_wait(app);
    let closing = app.spawn("close", json!({}));
    assert_eq!(closing.within(PROMPT, "close"), Ok(Value::Null));
    assert_eq!(kind(&waiting.within(PROMPT, "waitForSync")), "closed");
    app.free();
}

/// The drain is the close's bound: the job it would otherwise wait 30 s
/// for ends with it.
#[test]
fn a_drains_timeout_bounds_close_with_a_wait_pending() {
    let app = device(&unreachable());
    let waiting = pending_wait(app);
    let started = Instant::now();
    let closed = app
        .spawn("close", json!({ "timeoutMs": 1_000 }))
        .within(PROMPT, "close");
    let took = started.elapsed();
    assert_eq!(kind(&closed), "unsynced", "{closed:?}");
    assert!(
        took >= Duration::from_millis(900) && took < Duration::from_secs(3),
        "close took {took:?}"
    );
    assert_eq!(kind(&waiting.within(PROMPT, "waitForSync")), "closed");
    app.free();
}

/// What can finish inside the drain does.
#[test]
fn a_wait_that_finishes_inside_the_drain_succeeds() {
    let app = App::start();
    let alice = app.open(json!({ "bearer": true }), Host::new("token-alice"));
    app.relay().set_unreachable(true);
    let waiting = pending_wait(alice);
    let closing = alice.spawn("close", json!({ "timeoutMs": 30_000 }));
    closing.still_running("a drain with the backend unreachable");
    app.relay().set_unreachable(false);
    let limit = Duration::from_secs(45);
    assert_eq!(waiting.within(limit, "waitForSync"), Ok(Value::Null));
    assert_eq!(closing.within(limit, "close"), Ok(Value::Null));
    alice.free();
}

/// Every wait on the server, not only waitForSync.
#[test]
fn close_ends_every_kind_of_wait_on_the_server() {
    let app = device(&unreachable());
    let chief = app.call("account", json!({})).unwrap();
    let chief = chief.as_str().unwrap();
    let tx = app.call("txBegin", json!({})).unwrap()["tx"].clone();
    app.call(
        "insert",
        json!({ "table": "shows", "row": parse(&show(chief)), "tx": tx }),
    )
    .unwrap();
    let waits = [
        (
            "a remote read",
            app.spawn(
                "all",
                json!({ "query": { "table": "shows" }, "tier": "remote" }),
            ),
        ),
        (
            "a global write",
            app.spawn(
                "insert",
                json!({ "table": "shows", "row": parse(&show(chief)), "tier": "global" }),
            ),
        ),
        (
            "a global commit",
            app.spawn("txCommit", json!({ "tx": tx, "tier": "global" })),
        ),
    ];
    for (what, wait) in &waits {
        wait.still_running(what);
    }
    assert_eq!(
        app.spawn("close", json!({})).within(PROMPT, "close"),
        Ok(Value::Null)
    );
    for (what, wait) in waits {
        let ended = wait.within(PROMPT, what);
        assert_eq!(kind(&ended), "closed", "{what}: {ended:?}");
    }
    app.free();
}

/// A for_user handle shares the parent's client, which lives on: its close
/// wakes its own waits at once, and leaves nothing to hold up the parent.
#[test]
fn close_ends_a_for_user_handles_wait_at_once() {
    let app = App::start();
    app.register_alice();
    let (job, host) = app.machine();
    let alice = job.for_user("alice").unwrap();
    app.relay().set_unreachable(true);
    let waiting = pending_wait(alice);
    assert_eq!(
        alice.spawn("close", json!({})).within(PROMPT, "close"),
        Ok(Value::Null)
    );
    assert_eq!(kind(&waiting.within(PROMPT, "waitForSync")), "closed");
    alice.free();
    assert!(job.call("account", json!({})).is_ok(), "the parent goes on");
    assert_eq!(
        job.spawn("close", json!({}))
            .within(PROMPT, "the parent's close"),
        Ok(Value::Null)
    );
    job.free();
    host.assert_freeable();
    free_ctx(host);
}

/// A wait abandoned by a for_user handle's close doesn't take the writes it
/// was checking with it: the parent's next waitForSync still reports the
/// refusal.
#[test]
fn an_abandoned_wait_leaves_its_checks_to_the_next() {
    let app = App::start();
    app.register_alice();
    let (job, _) = app.machine();
    let alice = job.for_user("alice").unwrap();
    app.relay().set_unreachable(true);
    // Alice may not make someone else the chief: the server refuses it.
    alice
        .call(
            "insert",
            json!({ "table": "shows", "row": parse(&show(nil_uuid())) }),
        )
        .unwrap();
    let waiting = alice.spawn("waitForSync", json!({ "tier": "global" }));
    waiting.still_running("waitForSync");
    alice
        .spawn("close", json!({}))
        .within(PROMPT, "close")
        .unwrap();
    assert_eq!(kind(&waiting.within(PROMPT, "waitForSync")), "closed");
    alice.free();
    app.relay().set_unreachable(false);
    let checked = job
        .spawn("waitForSync", json!({ "tier": "global" }))
        .within(Duration::from_secs(45), "the parent's waitForSync");
    assert_eq!(kind(&checked), "permission", "{checked:?}");
    job.spawn("close", json!({}))
        .within(PROMPT, "close")
        .unwrap();
    job.free();
}

/// The same for a global write, which waitForSync never had to check.
#[test]
fn an_abandoned_global_write_is_checked_by_the_next_wait() {
    let app = App::start();
    app.register_alice();
    let (job, _) = app.machine();
    let alice = job.for_user("alice").unwrap();
    app.relay().set_unreachable(true);
    let writing = alice.spawn(
        "insert",
        json!({ "table": "shows", "row": parse(&show(nil_uuid())), "tier": "global" }),
    );
    writing.still_running("a global write");
    alice
        .spawn("close", json!({}))
        .within(PROMPT, "close")
        .unwrap();
    assert_eq!(kind(&writing.within(PROMPT, "the global write")), "closed");
    alice.free();
    app.relay().set_unreachable(false);
    let checked = job
        .spawn("waitForSync", json!({ "tier": "global" }))
        .within(Duration::from_secs(45), "the parent's waitForSync");
    assert_eq!(kind(&checked), "permission", "{checked:?}");
    job.spawn("close", json!({}))
        .within(PROMPT, "close")
        .unwrap();
    job.free();
}

/// A session's handle closed on its own: the session and its client go on.
#[test]
fn close_ends_a_session_handles_wait_at_once() {
    let session = guest_session(&unreachable(), None);
    let guest = session.jazz().unwrap();
    let waiting = pending_wait(guest);
    assert_eq!(
        guest.spawn("close", json!({})).within(PROMPT, "close"),
        Ok(Value::Null)
    );
    assert_eq!(kind(&waiting.within(PROMPT, "waitForSync")), "closed");
    guest.free();
    assert!(
        session.jazz().map(Handle::free).is_ok(),
        "the session goes on"
    );
    assert_eq!(
        session.spawn("close").within(PROMPT, "close"),
        Ok(Value::Null)
    );
    session.free();
}

/// A `link` on a session's handle is refused (the session links its own
/// device account), at once and without a request, so nothing of it is
/// left for close to end; the handle stays open until closed.
#[test]
fn a_session_handles_link_is_refused_and_close_still_closes_it() {
    let session = guest_session(&unreachable(), None);
    let guest = session.jazz().unwrap();
    let linked = guest
        .spawn("link", json!({ "bearer": "token-alice" }))
        .within(PROMPT, "link");
    assert_eq!(kind(&linked), "notSupported", "{linked:?}");
    assert!(guest.call("account", json!({})).is_ok(), "still open");
    assert_eq!(
        guest.spawn("close", json!({})).within(PROMPT, "close"),
        Ok(Value::Null)
    );
    assert_eq!(kind(&guest.call("account", json!({}))), "closed");
    guest.free();
    assert_eq!(
        session.spawn("close").within(PROMPT, "close"),
        Ok(Value::Null)
    );
    session.free();
}

/// Closing the session ends what waits on its handles, as `closed` (not
/// `replaced`).
#[test]
fn session_close_ends_its_handles_waits() {
    let session = guest_session(&unreachable(), None);
    let guest = session.jazz().unwrap();
    let waiting = pending_wait(guest);
    assert_eq!(
        session.spawn("close").within(PROMPT, "close"),
        Ok(Value::Null)
    );
    assert_eq!(kind(&waiting.within(PROMPT, "waitForSync")), "closed");
    assert_eq!(kind(&guest.call("account", json!({}))), "closed");
    guest.free();
    session.free();
}

/// `replaced` only while the session goes on: signing out.
#[test]
fn sign_out_replaces_a_pending_wait() {
    let session = guest_session(&unreachable(), None);
    let guest = session.jazz().unwrap();
    let waiting = pending_wait(guest);
    session.spawn("signOut").within(PROMPT, "signOut").unwrap();
    assert_eq!(kind(&waiting.within(PROMPT, "waitForSync")), "replaced");
    assert_eq!(kind(&guest.call("account", json!({}))), "replaced");
    // Closed wins once both hold.
    session.spawn("close").within(PROMPT, "close").unwrap();
    assert_eq!(kind(&guest.call("account", json!({}))), "closed");
    guest.free();
    session.free();
}

/// `replaced` only while the session goes on: a change of user.
#[test]
fn a_change_of_user_replaces_a_pending_wait() {
    let session = guest_session(&unreachable(), None);
    let guest = session.jazz().unwrap();
    let waiting = pending_wait(guest);
    session
        .spawn("createDeviceAccount")
        .within(PROMPT, "createDeviceAccount")
        .unwrap();
    assert_eq!(kind(&waiting.within(PROMPT, "waitForSync")), "replaced");
    session.spawn("close").within(PROMPT, "close").unwrap();
    guest.free();
    session.free();
}

/// A refresh stuck in the token callback: close ends it (and one queued
/// behind it) as `closed` while the callback still runs, and returns once
/// the callback has.
#[test]
fn session_close_ends_an_operation_stuck_in_the_token_callback() {
    let host = Host::new("token-alice");
    let session = guest_session(&unreachable(), Some(host));
    host.then(Then::Stall);
    let refreshing = session.spawn("refresh");
    until("the callback", || !host.idle());
    let queued = session.spawn("createDeviceAccount");
    queued.still_running("an operation behind the refresh");
    let closing = session.spawn("close");
    assert_eq!(kind(&refreshing.within(PROMPT, "refresh")), "closed");
    assert_eq!(
        kind(&queued.within(PROMPT, "the queued operation")),
        "closed"
    );
    closing.still_running("close, with a token callback running,");
    host.release();
    assert_eq!(closing.within(PROMPT, "close"), Ok(Value::Null));
    assert_eq!(kind(&session.call("snapshot", json!({}))), "closed");
    assert_eq!(kind(&session.jazz().map(|_| Value::Null)), "closed");
    session.free();
    host.assert_freeable();
    free_ctx(host);
}

/// A refresh stuck on the backend's HTTP answer (30 s of it).
#[test]
fn session_close_ends_an_operation_stuck_on_http() {
    let (endpoint, asked) = stalling();
    let host = Host::new("token-alice");
    let session = guest_session(&endpoint, Some(host));
    let refreshing = session.spawn("refresh");
    until("the backend's request", || asked.load(Ordering::SeqCst) > 0);
    assert_eq!(
        session.spawn("close").within(PROMPT, "close"),
        Ok(Value::Null)
    );
    assert_eq!(kind(&refreshing.within(PROMPT, "refresh")), "closed");
    session.free();
    host.assert_freeable();
    free_ctx(host);
}

/// A token callback that never returns on its own holds up close, as
/// documented, but not the calls in flight: they end with the client.
#[test]
fn a_stuck_token_callback_holds_up_close_but_not_the_calls() {
    let app = App::start();
    let host = Host::new("token-alice");
    let alice = app.open(json!({ "bearer": true }), host);
    host.then(Then::Stall);
    let refreshing = alice.spawn("refresh", json!({}));
    until("the callback", || !host.idle());
    let closing = alice.spawn("close", json!({}));
    assert_eq!(kind(&refreshing.within(PROMPT, "refresh")), "closed");
    closing.still_running("close, with a token callback running,");
    host.release();
    assert_eq!(closing.within(PROMPT, "close"), Ok(Value::Null));
    alice.free();
    host.assert_freeable();
    free_ctx(host);
}

#[test]
fn close_from_inside_the_token_callback() {
    let app = App::start();
    let host = Host::new("token-alice");
    let alice = app.open(json!({ "bearer": true }), host);
    host.then(Then::CloseHandle(alice));
    let refreshed = alice.spawn("refresh", json!({})).within(PROMPT, "refresh");
    assert_eq!(kind(&refreshed), "closed", "{refreshed:?}");
    until("close inside the callback", || {
        !host.closed_inside.lock().unwrap().is_empty() && host.idle()
    });
    assert_eq!(*host.closed_inside.lock().unwrap(), [Ok(Value::Null)]);
    alice.free();
    host.assert_freeable();
    free_ctx(host);
}

#[test]
fn session_close_from_inside_the_token_callback() {
    let host = Host::new("token-alice");
    let session = guest_session(&unreachable(), Some(host));
    host.then(Then::CloseSession(session));
    let refreshed = session.spawn("refresh").within(PROMPT, "refresh");
    assert_eq!(kind(&refreshed), "closed", "{refreshed:?}");
    until("close inside the callback", || {
        !host.closed_inside.lock().unwrap().is_empty() && host.idle()
    });
    assert_eq!(*host.closed_inside.lock().unwrap(), [Ok(Value::Null)]);
    session.free();
    host.assert_freeable();
    free_ctx(host);
}

/// Close waits for the callback, and the callback closes: the second close
/// must not wait for the first.
#[test]
fn close_from_the_callback_that_a_close_is_waiting_for() {
    let app = App::start();
    let host = Host::new("token-alice");
    let alice = app.open(json!({ "bearer": true }), host);
    host.then(Then::StallThenCloseHandle(alice));
    let refreshing = alice.spawn("refresh", json!({}));
    until("the callback", || !host.idle());
    let closing = alice.spawn("close", json!({}));
    closing.still_running("close, with a token callback running,");
    host.release();
    assert_eq!(closing.within(PROMPT, "close"), Ok(Value::Null));
    assert_eq!(kind(&refreshing.within(PROMPT, "refresh")), "closed");
    assert_eq!(*host.closed_inside.lock().unwrap(), [Ok(Value::Null)]);
    alice.free();
    host.assert_freeable();
    free_ctx(host);
}

/// Two token callbacks running at once (two for_user fetches), both
/// closing: neither waits for the other.
#[test]
fn two_callbacks_closing_at_once() {
    let app = App::start();
    app.register_alice();
    let (job, host) = app.machine();
    host.then(Then::StallThenCloseHandle(job));
    let acting: Vec<_> = (0..2)
        .map(|_| Running::start(move || job.for_user("alice")))
        .collect();
    until("both callbacks", || {
        host.running.load(Ordering::SeqCst) == 2
    });
    host.release();
    for acting in acting {
        match acting.within(PROMPT, "for_user") {
            Ok(handle) => handle.free(),
            Err(error) => assert_eq!(error["kind"], "closed", "{error}"),
        }
    }
    until("both closes", || {
        host.closed_inside.lock().unwrap().len() == 2 && host.idle()
    });
    job.free();
    host.assert_freeable();
    free_ctx(host);
}

/// The same for a handle, whose ctx may go after `jazz_close`.
#[test]
fn freeing_a_handle_waits_for_the_callback_that_closed() {
    let app = App::start();
    let host = Host::new("token-alice");
    let alice = app.open(json!({ "bearer": true }), host);
    host.then(Then::CloseHandle(alice));
    *host.linger.lock().unwrap() = Duration::from_secs(1);
    alice
        .spawn("refresh", json!({}))
        .within(PROMPT, "refresh")
        .ok();
    until("close inside the callback", || {
        !host.closed_inside.lock().unwrap().is_empty()
    });
    alice.free();
    host.assert_freeable();
    free_ctx(host);
}

/// Freeing waits for the callback that closed, so ctx can go right after.
#[test]
fn free_waits_for_the_callback_that_closed() {
    let host = Host::new("token-alice");
    let session = guest_session(&unreachable(), Some(host));
    host.then(Then::CloseSession(session));
    *host.linger.lock().unwrap() = Duration::from_secs(1);
    session.spawn("refresh").within(PROMPT, "refresh").ok();
    until("close inside the callback", || {
        !host.closed_inside.lock().unwrap().is_empty()
    });
    // No waiting for the callback's own return: free does that.
    session.free();
    host.assert_freeable();
    free_ctx(host);
}

#[test]
fn close_twice_at_once_and_again() {
    let app = device(&unreachable());
    write_show(app);
    let closes: Vec<_> = (0..4).map(|_| app.spawn("close", json!({}))).collect();
    for close in closes {
        assert_eq!(close.within(PROMPT, "close"), Ok(Value::Null));
    }
    assert_eq!(app.call("close", json!({})), Ok(Value::Null));
    app.free();

    let session = guest_session(&unreachable(), None);
    let closes: Vec<_> = (0..4).map(|_| session.spawn("close")).collect();
    for close in closes {
        assert_eq!(close.within(PROMPT, "close"), Ok(Value::Null));
    }
    assert_eq!(session.call("close", json!({})), Ok(Value::Null));
    session.free();
}

/// A drain's failure is the close's, and free after it only frees.
#[test]
fn close_returns_unsynced_then_free_is_clean() {
    let app = device(&unreachable());
    write_show(app);
    let closed = app
        .spawn("close", json!({ "timeoutMs": 700 }))
        .within(PROMPT, "close");
    let error = closed.expect_err("writes left unsent");
    assert_eq!(error["kind"], "unsynced", "{error}");
    assert_eq!(error["pending"], 1, "{error}");
    assert_eq!(app.call("close", json!({})), Ok(Value::Null));
    let mut err = ptr::null_mut();
    assert_eq!(unsafe { jazz_close_within(app.0, 5_000, &mut err) }, 0);
    assert!(err.is_null());
}

/// A second close during a drain returns at once and leaves the drain its
/// token callback, which reconnecting needs.
#[test]
fn close_during_a_drain_leaves_the_drain_alone() {
    let app = App::start();
    let (job, host) = app.machine();
    app.relay().refuse("machine-trusted", "token expired");
    app.relay().kick();
    std::thread::sleep(Duration::from_millis(300));
    job.call(
        "insert",
        json!({ "table": "shows", "row": parse(&show(nil_uuid())) }),
    )
    .unwrap();
    let draining = job.spawn("close", json!({ "timeoutMs": 15_000 }));
    draining.still_running("a drain with the backend refusing");
    assert_eq!(
        job.spawn("close", json!({}))
            .within(PROMPT, "the second close"),
        Ok(Value::Null)
    );
    assert_eq!(kind(&job.call("account", json!({}))), "closed");
    app.relay().refusing.lock().unwrap().clear();
    app.relay().kick();
    assert_eq!(
        draining.within(Duration::from_secs(25), "the drain"),
        Ok(Value::Null)
    );
    job.free();
    host.assert_freeable();
    free_ctx(host);
}

#[test]
fn close_refuses_a_bad_timeout_and_stays_open() {
    let mut err = ptr::null_mut();
    let app = Handle(unsafe { jazz_open(c(&client_config()).as_ptr(), &mut err) });
    for bad in [json!(-1), json!(1.5), json!("10")] {
        assert_eq!(
            kind(&app.call("close", json!({ "timeoutMs": bad }))),
            "refused"
        );
    }
    assert!(app.call("account", json!({})).is_ok());
    assert_eq!(
        app.call("close", json!({ "timeoutMs": null })),
        Ok(Value::Null)
    );
    app.free();
}

/// Whatever a call's arguments, after close it fails `closed`.
#[test]
fn every_later_call_fails_closed() {
    let session = guest_session(&unreachable(), None);
    let guest = session.jazz().unwrap();
    let tx = guest.call("txBegin", json!({})).unwrap()["tx"].clone();
    guest
        .spawn("close", json!({}))
        .within(PROMPT, "close")
        .unwrap();
    for (method, args) in [
        ("account", json!({})),
        ("txCommit", json!({ "tx": tx })),
        ("uploadPush", json!({ "upload": "1", "bytes": "" })),
        ("insert", json!({ "table": "nope", "row": {} })),
        ("waitForSync", json!({ "tier": "global" })),
        ("signOut", json!({})),
    ] {
        assert_eq!(kind(&guest.call(method, args)), "closed", "{method}");
    }
    guest.free();
    session.spawn("close").within(PROMPT, "close").unwrap();
    session.free();
}

/// txBegin and uploadBegin against close, on a root and on a derived
/// handle: each gives an id or `closed`, and nothing crashes or hangs.
/// (That none of them leaks is `core::tests`': a leak is invisible here.)
#[test]
fn begins_racing_close_give_an_id_or_closed() {
    let session = guest_session(&unreachable(), None);
    for round in 0..40u64 {
        let root = round % 8 == 0;
        let handle = if root {
            let mut err = ptr::null_mut();
            Handle(unsafe { jazz_open(c(&client_config()).as_ptr(), &mut err) })
        } else {
            session.jazz().unwrap()
        };
        let mut row = parse(&show(nil_uuid()));
        row.as_object_mut().unwrap().remove("name");
        let begins = [
            handle.spawn("txBegin", json!({})),
            handle.spawn(
                "uploadBegin",
                json!({ "table": "shows", "column": "name", "row": row }),
            ),
        ];
        std::thread::sleep(Duration::from_micros(round * 20));
        assert_eq!(
            handle.spawn("close", json!({})).within(PROMPT, "close"),
            Ok(Value::Null)
        );
        for begin in begins {
            let begun = begin.within(PROMPT, "begin");
            assert!(matches!(kind(&begun), "ok" | "closed"), "{begun:?}");
        }
        assert_eq!(kind(&handle.call("txBegin", json!({}))), "closed");
        handle.free();
    }
    session.spawn("close").within(PROMPT, "close").unwrap();
    session.free();
}

/// for_user answering as the parent closes: `closed`, or a handle that is.
#[test]
fn for_user_as_the_parent_closes() {
    let app = App::start();
    app.register_alice();
    let (job, host) = app.machine();
    host.then(Then::Stall);
    let acting = Running::start(move || job.for_user("alice"));
    until("for_user's credential fetch", || !host.idle());
    let closing = job.spawn("close", json!({}));
    std::thread::sleep(Duration::from_millis(200));
    host.release();
    assert_eq!(closing.within(PROMPT, "close"), Ok(Value::Null));
    match acting.within(PROMPT, "for_user") {
        Ok(alice) => {
            assert_eq!(kind(&alice.call("account", json!({}))), "closed");
            alice.free();
        }
        Err(error) => assert_eq!(error["kind"], "closed", "{error}"),
    }
    job.free();
    host.assert_freeable();
    free_ctx(host);
}

/// `jazz_session_jazz` as the session closes: `closed`, or a handle that is.
#[test]
fn session_jazz_as_the_session_closes() {
    for round in 0..12u64 {
        let session = guest_session(&unreachable(), None);
        let fetching = Running::start(move || session.jazz());
        std::thread::sleep(Duration::from_micros(round * 50));
        assert_eq!(
            session.spawn("close").within(PROMPT, "close"),
            Ok(Value::Null)
        );
        match fetching.within(PROMPT, "jazz_session_jazz") {
            Ok(handle) => {
                assert_eq!(kind(&handle.call("account", json!({}))), "closed");
                handle.free();
            }
            Err(error) => assert_eq!(error["kind"], "closed", "{error}"),
        }
        session.free();
    }
}

/// The same with a sign-out from the callback a close waits for: the
/// sign-out stops a client that is already stopping, and neither waits for
/// the other (nor for the store's release, which needs neither).
#[test]
fn sign_out_from_the_callback_that_a_close_is_waiting_for() {
    let app = App::start();
    let host = Host::new("token-alice");
    let alice = app.open(json!({ "bearer": true }), host);
    host.then(Then::StallThenSignOut(alice));
    let refreshing = alice.spawn("refresh", json!({}));
    until("the callback", || !host.idle());
    let closing = alice.spawn("close", json!({}));
    closing.still_running("close, with a token callback running,");
    host.release();
    assert_eq!(closing.within(PROMPT, "close"), Ok(Value::Null));
    let _ = refreshing.within(PROMPT, "refresh");
    let signed_out = host.closed_inside.lock().unwrap().clone();
    assert_eq!(signed_out.len(), 1, "{signed_out:?}");
    assert!(
        signed_out[0].is_ok() || kind(&signed_out[0]) == "closed",
        "{signed_out:?}"
    );
    alice.free();
    host.assert_freeable();
    free_ctx(host);
}

// Per-call control: `callBegin` / `"call"` / `cancel` / `callEnd` in the C
// ABI, `CallControl` and `controlled` over UniFFI.

/// What a cancel may take to end a call's wait.
const AT_ONCE: Duration = Duration::from_millis(100);

fn call_begin(handle: Handle, args: Value) -> Value {
    handle.call("callBegin", args).unwrap()["call"].clone()
}

/// `method` with `call`'s control, still waiting, then cancelled: what it
/// failed with, within [`AT_ONCE`] of the cancel.
fn cancelled(handle: Handle, method: &'static str, mut args: Value) -> Value {
    let call = call_begin(handle, json!({}));
    args["call"] = call.clone();
    let running = handle.spawn(method, args);
    running.still_running(method);
    let cancelled = Instant::now();
    handle.call("cancel", json!({ "call": call })).unwrap();
    let ended = running.within(PROMPT, method);
    let took = cancelled.elapsed();
    assert!(took < AT_ONCE, "{method} took {took:?} after the cancel");
    handle.call("callEnd", json!({ "call": call })).unwrap();
    ended.expect_err(method)
}

fn interrupted(error: &Value, reason: &str, queued: bool) {
    assert_eq!(error["kind"], "interrupted", "{error}");
    assert_eq!(error["reason"], reason, "{error}");
    assert_eq!(error["queued"], queued, "{error}");
}

/// A call whose control ended before it started does nothing.
#[test]
fn c_a_call_cancelled_before_it_starts_does_nothing() {
    let app = device(&unreachable());
    let chief = app.call("account", json!({})).unwrap();
    let call = call_begin(app, json!({}));
    app.call("cancel", json!({ "call": call })).unwrap();
    let written = app.call(
        "insert",
        json!({ "table": "shows", "row": parse(&show(chief.as_str().unwrap())), "call": call }),
    );
    interrupted(&written.expect_err("insert"), "cancelled", false);
    let rows = app
        .call("all", json!({ "query": { "table": "shows" } }))
        .unwrap();
    assert_eq!(rows, json!([]), "the insert did nothing");
    app.call("callEnd", json!({ "call": call })).unwrap();
    app.spawn("close", json!({}))
        .within(PROMPT, "close")
        .unwrap();
    app.free();
}

/// A cancel ends every kind of wait on the server at once: `queued` when
/// the call committed a write.
#[test]
fn c_cancel_ends_each_wait_at_once() {
    let app = device(&unreachable());
    write_show(app);
    let chief = app.call("account", json!({})).unwrap();
    let chief = chief.as_str().unwrap();
    let synced = cancelled(app, "waitForSync", json!({ "tier": "global" }));
    interrupted(&synced, "cancelled", false);
    let read = cancelled(
        app,
        "all",
        json!({ "query": { "table": "shows" }, "tier": "remote" }),
    );
    interrupted(&read, "cancelled", false);
    let written = cancelled(
        app,
        "insert",
        json!({ "table": "shows", "row": parse(&show(chief)), "tier": "global" }),
    );
    interrupted(&written, "cancelled", true);
    let tx = app.call("txBegin", json!({})).unwrap()["tx"].clone();
    app.call(
        "insert",
        json!({ "table": "shows", "row": parse(&show(chief)), "tx": tx }),
    )
    .unwrap();
    let committed = cancelled(app, "txCommit", json!({ "tx": tx, "tier": "global" }));
    interrupted(&committed, "cancelled", true);
    // What they committed is still there, and the handle goes on.
    let rows = app
        .call("all", json!({ "query": { "table": "shows" } }))
        .unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 3, "{rows}");
    app.spawn("close", json!({}))
        .within(PROMPT, "close")
        .unwrap();
    app.free();
}

/// A timeout fires on time while the server takes requests and never
/// answers.
#[test]
fn c_a_timeout_fires_on_time() {
    let (endpoint, _) = stalling();
    let app = device(&endpoint);
    write_show(app);
    let call = call_begin(app, json!({ "timeoutMs": 300 }));
    let started = Instant::now();
    let synced = app
        .spawn("waitForSync", json!({ "tier": "global", "call": call }))
        .within(PROMPT, "waitForSync");
    let took = started.elapsed();
    interrupted(&synced.expect_err("waitForSync"), "timeout", false);
    assert!(
        took >= Duration::from_millis(300) && took < Duration::from_millis(400),
        "took {took:?}"
    );
    app.spawn("close", json!({}))
        .within(PROMPT, "close")
        .unwrap();
    app.free();
}

/// A refusal that arrives after its call was cancelled is still reported:
/// the next waitForSync reports it.
#[test]
fn c_a_refusal_after_a_cancel_is_still_reported() {
    let app = App::start();
    let alice = app.open(json!({ "bearer": true }), Host::new("token-alice"));
    app.relay().set_unreachable(true);
    // Alice may not make someone else the chief: the server refuses it.
    let written = cancelled(
        alice,
        "insert",
        json!({ "table": "shows", "row": parse(&show(nil_uuid())), "tier": "global" }),
    );
    interrupted(&written, "cancelled", true);
    app.relay().set_unreachable(false);
    let checked = alice
        .spawn("waitForSync", json!({ "tier": "global" }))
        .within(Duration::from_secs(45), "waitForSync");
    assert_eq!(kind(&checked), "permission", "{checked:?}");
    alice
        .spawn("close", json!({}))
        .within(PROMPT, "close")
        .unwrap();
    alice.free();
}

/// Controls work on a derived (for_user) handle, and leave its parent be.
#[test]
fn c_controls_on_a_derived_handle() {
    let app = App::start();
    app.register_alice();
    let (job, host) = app.machine();
    let alice = job.for_user("alice").unwrap();
    app.relay().set_unreachable(true);
    write_show(alice);
    let synced = cancelled(alice, "waitForSync", json!({ "tier": "global" }));
    interrupted(&synced, "cancelled", false);
    assert!(job.call("account", json!({})).is_ok(), "the parent goes on");
    alice
        .spawn("close", json!({}))
        .within(PROMPT, "close")
        .unwrap();
    alice.free();
    job.spawn("close", json!({}))
        .within(PROMPT, "close")
        .unwrap();
    job.free();
    host.assert_freeable();
    free_ctx(host);
}

/// A control the host never ended is freed with its handle; one used after
/// the close fails `closed`, and cancelling it is harmless.
#[test]
fn c_free_with_a_control_still_live() {
    let app = device(&unreachable());
    let call = call_begin(app, json!({ "timeoutMs": 60_000 }));
    let unknown = app.call("cancel", json!({ "call": "1" }));
    assert_eq!(kind(&unknown), "refused", "{unknown:?}");
    app.spawn("close", json!({}))
        .within(PROMPT, "close")
        .unwrap();
    app.call("cancel", json!({ "call": call })).unwrap();
    let after = app.call(
        "all",
        json!({ "query": { "table": "shows" }, "call": call }),
    );
    assert_eq!(kind(&after), "closed", "{after:?}");
    app.free();
}

mod uniffi_control {
    use super::*;
    use jazz_ffi::{CallControl, JazzError, JazzHandle, StateListener};

    fn device_handle(endpoint: &str) -> Arc<JazzHandle> {
        let key = jazz_ffi::generate_device_key().unwrap();
        JazzHandle::open(
            json!({ "app": STAGE_PLAN_APP, "appId": jazz_app_support::AppId::random().to_string(),
                "serverUrl": endpoint, "auth": { "deviceKey": key }, "contract": 2 })
            .to_string(),
        )
        .unwrap()
    }

    fn interrupted<T: std::fmt::Debug>(result: Result<T, JazzError>, reason: &str, queued: bool) {
        match result {
            Err(JazzError::Interrupted {
                interruption,
                queued: was_queued,
                ..
            }) => assert_eq!((interruption.as_str(), was_queued), (reason, queued)),
            other => panic!("expected interrupted ({reason}, queued {queued}): {other:?}"),
        }
    }

    /// Runs `call` on a view with a fresh control, cancels it once it
    /// waits, and returns its result within [`AT_ONCE`] of the cancel.
    fn cancelled<T: Send + 'static>(
        jazz: &Arc<JazzHandle>,
        call: impl FnOnce(Arc<JazzHandle>) -> Result<T, JazzError> + Send + 'static,
    ) -> Result<T, JazzError> {
        let control = CallControl::new(None);
        let view = jazz.controlled(control.clone());
        let running = Running::start(move || call(view));
        running.still_running("the call");
        let cancelled = Instant::now();
        control.cancel();
        let ended = running.within(PROMPT, "the call");
        assert!(
            cancelled.elapsed() < AT_ONCE,
            "took {:?}",
            cancelled.elapsed()
        );
        ended
    }

    fn a_show(jazz: &JazzHandle) -> String {
        show(&jazz.account().unwrap().unwrap())
    }

    #[test]
    fn a_call_cancelled_before_it_starts_does_nothing() {
        let jazz = device_handle(&unreachable());
        let control = CallControl::new(None);
        control.cancel();
        let view = jazz.controlled(control);
        interrupted(
            view.insert("shows".into(), a_show(&jazz)),
            "cancelled",
            false,
        );
        assert_eq!(
            parse(&jazz.all(r#"{"table":"shows"}"#.into()).unwrap()),
            json!([])
        );
        jazz.shutdown().unwrap();
    }

    #[test]
    fn cancel_ends_each_wait_at_once() {
        let jazz = device_handle(&unreachable());
        jazz.insert("shows".into(), a_show(&jazz)).unwrap();
        interrupted(
            cancelled(&jazz, |view| view.wait_for_sync("global".into())),
            "cancelled",
            false,
        );
        interrupted(
            cancelled(&jazz, |view| {
                view.all_at(r#"{"table":"shows"}"#.into(), "remote".into())
            }),
            "cancelled",
            false,
        );
        let row = a_show(&jazz);
        interrupted(
            cancelled(&jazz, move |view| {
                view.insert_at("shows".into(), row, "global".into())
            }),
            "cancelled",
            true,
        );
        let row = a_show(&jazz);
        interrupted(
            cancelled(&jazz, move |view| {
                let tx = view.begin()?;
                tx.insert("shows".into(), row)?;
                tx.commit(Some("global".into()))
            }),
            "cancelled",
            true,
        );
        jazz.shutdown().unwrap();
    }

    #[test]
    fn a_timeout_fires_on_time() {
        let (endpoint, _) = stalling();
        let jazz = device_handle(&endpoint);
        jazz.insert("shows".into(), a_show(&jazz)).unwrap();
        let view = jazz.controlled(CallControl::new(Some(300)));
        let started = Instant::now();
        interrupted(view.wait_for_sync("global".into()), "timeout", false);
        let took = started.elapsed();
        assert!(
            took >= Duration::from_millis(300) && took < Duration::from_millis(400),
            "took {took:?}"
        );
        jazz.shutdown().unwrap();
    }

    struct Errors(mpsc::Sender<String>);
    impl StateListener for Errors {
        fn on_change(&self, state_json: String) {
            let _ = self.0.send(state_json);
        }
    }

    /// The refusal of a write whose call was cancelled reaches mutation
    /// errors and the next waitForSync.
    #[test]
    fn a_refusal_after_a_cancel_is_still_reported() {
        struct Alice;
        impl jazz_ffi::TokenProvider for Alice {
            fn token(&self, _: String) -> Result<Option<String>, JazzError> {
                Ok(Some("token-alice".into()))
            }
        }
        let app = App::start();
        let jazz = JazzHandle::open_with_tokens(
            json!({ "app": STAGE_PLAN_APP, "appId": app.app_id,
                "serverUrl": app.relay().endpoint, "auth": { "bearer": true }, "contract": 2 })
            .to_string(),
            Arc::new(Alice),
        )
        .unwrap();
        let (sender, errors) = mpsc::channel();
        let _watch = jazz
            .watch_mutation_errors(Arc::new(Errors(sender)))
            .unwrap();
        app.relay().set_unreachable(true);
        interrupted(
            cancelled(&jazz, |view| {
                view.insert_at("shows".into(), show(nil_uuid()), "global".into())
            }),
            "cancelled",
            true,
        );
        app.relay().set_unreachable(false);
        let error = errors
            .recv_timeout(Duration::from_secs(45))
            .expect("a mutation error");
        assert!(
            error.contains("permission") || error.contains("denied"),
            "{error}"
        );
        assert!(
            matches!(
                jazz.wait_for_sync("global".into()),
                Err(JazzError::Permission { .. })
            ),
            "waitForSync reports it"
        );
        jazz.shutdown().unwrap();
    }

    #[test]
    fn controls_on_a_derived_handle_and_a_freed_handle() {
        let jazz = device_handle(&unreachable());
        jazz.insert("shows".into(), a_show(&jazz)).unwrap();
        let control = CallControl::new(None);
        // A view of a view: the latest control rules.
        let view = jazz
            .controlled(CallControl::new(None))
            .controlled(control.clone());
        let waiting = Running::start({
            let view = view.clone();
            move || view.wait_for_sync("global".into())
        });
        waiting.still_running("waitForSync");
        control.cancel();
        interrupted(waiting.within(PROMPT, "waitForSync"), "cancelled", false);
        // Freeing the view leaves the app open.
        drop(view);
        assert!(jazz.account().is_ok());
        // Freeing the app with a control still live, then cancelling it.
        let live = CallControl::new(Some(60_000));
        let view = jazz.controlled(live.clone());
        jazz.shutdown().unwrap();
        drop(jazz);
        assert!(matches!(view.account(), Err(JazzError::Closed)));
        drop(view);
        live.cancel();
    }
}

/// A session's operations obey a control too: a refresh stuck on the
/// backend's HTTP answer ends at once when cancelled, and one cancelled
/// before it starts does nothing. Over C.
#[test]
fn c_cancel_ends_a_session_operation_at_once() {
    let (endpoint, asked) = stalling();
    let host = Host::new("token-alice");
    let session = guest_session(&endpoint, Some(host));
    let call = session.call("callBegin", json!({})).unwrap()["call"].clone();
    let refreshing = Running::start({
        let call = call.clone();
        move || session.call("refresh", json!({ "call": call }))
    });
    until("the backend's request", || asked.load(Ordering::SeqCst) > 0);
    let cancelled = Instant::now();
    session.call("cancel", json!({ "call": call })).unwrap();
    let ended = refreshing.within(PROMPT, "refresh");
    assert!(
        cancelled.elapsed() < AT_ONCE,
        "took {:?}",
        cancelled.elapsed()
    );
    interrupted(&ended.expect_err("refresh"), "cancelled", false);
    // Cancelled already: the next does nothing.
    let again = session.call("refresh", json!({ "call": call }));
    interrupted(&again.expect_err("refresh"), "cancelled", false);
    session.call("callEnd", json!({ "call": call })).unwrap();
    // The session goes on.
    assert!(session.call("snapshot", json!({})).is_ok());
    let timed = session
        .call("callBegin", json!({ "timeoutMs": 300 }))
        .unwrap()["call"]
        .clone();
    let started = Instant::now();
    let ended = session.call("refresh", json!({ "call": timed }));
    let took = started.elapsed();
    interrupted(&ended.expect_err("refresh"), "timeout", false);
    assert!(
        took >= Duration::from_millis(300) && took < Duration::from_millis(400),
        "took {took:?}"
    );
    assert_eq!(
        session.spawn("close").within(PROMPT, "close"),
        Ok(Value::Null)
    );
    session.free();
    host.assert_freeable();
    free_ctx(host);
}

mod uniffi_session_control {
    use super::*;
    use jazz_ffi::{CallControl, JazzError, Session, TokenProvider};

    struct Alice;
    impl TokenProvider for Alice {
        fn token(&self, _: String) -> Result<Option<String>, JazzError> {
            Ok(Some("token-alice".into()))
        }
    }

    #[test]
    fn cancel_ends_a_session_operation_at_once() {
        let (endpoint, asked) = stalling();
        let session = Session::open(
            json!({ "app": STAGE_PLAN_APP, "appId": jazz_app_support::AppId::random().to_string(),
                "serverUrl": endpoint, "initial": "device", "auth": { "bearer": true }, "contract": 2 })
            .to_string(),
            Some(Arc::new(Alice)),
        )
        .unwrap();
        let control = CallControl::new(None);
        let view = session.controlled(control.clone());
        let refreshing = Running::start({
            let view = view.clone();
            move || view.refresh()
        });
        until("the backend's request", || asked.load(Ordering::SeqCst) > 0);
        let cancelled = Instant::now();
        control.cancel();
        let ended = refreshing.within(PROMPT, "refresh");
        assert!(
            cancelled.elapsed() < AT_ONCE,
            "took {:?}",
            cancelled.elapsed()
        );
        assert!(
            matches!(
                &ended,
                Err(JazzError::Interrupted { interruption, queued: false, .. })
                    if interruption == "cancelled"
            ),
            "{ended:?}"
        );
        assert!(matches!(view.refresh(), Err(JazzError::Interrupted { .. })));
        drop(view);
        assert!(session.snapshot().is_ok(), "the session goes on");
        session.shutdown().unwrap();
    }
}
