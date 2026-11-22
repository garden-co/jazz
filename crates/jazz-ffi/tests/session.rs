//! jazz-ffi sessions (jazz-app's `JazzSession`): following the app's
//! bearer from user to user, device accounts, signing out, and the handle
//! a session gives for its user, through UniFFI and the C ABI, against
//! jazz-app's `relay/` stand-in for `createJazz`.

mod common;
#[allow(dead_code)]
#[path = "../../jazz-app/tests/relay/mod.rs"]
mod relay;

use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use common::*;
use jazz_ffi::{JazzError, Session, StateListener, TokenProvider};
use serde_json::{Value, json};

struct App {
    _server: jazz_server::JazzServer,
    backend: jazz_app_support::Jazz,
    relay: relay::Relay,
    app_id: jazz_app_support::AppId,
}

impl App {
    async fn start() -> Self {
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
        Self {
            _server: server,
            backend,
            relay,
            app_id,
        }
    }

    fn config(&self, extra: Value) -> String {
        let mut config = json!({ "app": STAGE_PLAN_APP, "appId": self.app_id.to_string(),
            "serverUrl": self.relay.endpoint, "contract": 2 });
        for (key, value) in extra.as_object().unwrap() {
            config[key] = value.clone();
        }
        config.to_string()
    }
}

/// The app's bearer, switchable: whoever it names is signed in.
struct Bearer(Mutex<Option<String>>);
impl TokenProvider for Bearer {
    fn token(&self, _reason: String) -> Result<Option<String>, JazzError> {
        Ok(self.0.lock().unwrap().clone())
    }
}

struct States(Mutex<mpsc::Sender<Value>>);
impl StateListener for States {
    fn on_change(&self, state_json: String) {
        let _ = self.0.lock().unwrap().send(parse(&state_json));
    }
}

/// The session's snapshot once `done` holds.
fn snapshot_until(session: &Session, done: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let snapshot = parse(&session.snapshot().unwrap());
        if done(&snapshot) {
            return snapshot;
        }
        assert!(Instant::now() < deadline, "{snapshot}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Watched states up to and including the first for which `done` holds.
/// A watch delivers the latest snapshot, so a state the session passes
/// through quickly may be skipped: wait for it here before moving on.
fn watched_until(states: &mpsc::Receiver<Value>, done: impl Fn(&Value) -> bool) -> Vec<Value> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut seen = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match states.recv_timeout(left) {
            Ok(state) => {
                let found = done(&state);
                seen.push(state);
                if found {
                    return seen;
                }
            }
            Err(error) => panic!("watch: {error}; saw {seen:?}"),
        }
    }
}

/// The session follows the bearer: alice, then bob (a new client, the old
/// handle `Replaced`), then no one (`NoUser`); the watch sees each and a
/// last `closed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_follows_the_bearer_from_user_to_user() {
    let app = App::start().await;
    let config = app.config(json!({ "auth": { "bearer": true } }));
    tokio::task::spawn_blocking(move || {
        let bearer = Arc::new(Bearer(Mutex::new(Some("token-alice".into()))));
        let session = Session::open(config, Some(bearer.clone())).unwrap();
        let (sender, states) = mpsc::channel();
        let watch = session.watch(Arc::new(States(Mutex::new(sender)))).unwrap();
        let alice = snapshot_until(&session, |s| s["status"] == "ready");
        assert_eq!(alice["user"]["subject"], "alice", "{alice}");
        let epoch = alice["clientEpoch"].as_u64().unwrap();
        let is_ready = |subject: &'static str| {
            move |s: &Value| s["status"] == "ready" && s["user"]["subject"] == subject
        };
        let mut seen = watched_until(&states, is_ready("alice"));

        let as_alice = session.jazz().unwrap();
        let account = as_alice.account().unwrap().expect("alice's account");
        as_alice.insert("shows".into(), show(&account)).unwrap();
        as_alice.wait_for_sync("global".into()).unwrap();

        // The app signs bob in instead.
        *bearer.0.lock().unwrap() = Some("token-bob".into());
        session.refresh().unwrap();
        let bob = snapshot_until(&session, |s| {
            s["status"] == "ready" && s["user"]["subject"] == "bob"
        });
        assert!(bob["clientEpoch"].as_u64().unwrap() > epoch, "{bob}");
        seen.extend(watched_until(&states, is_ready("bob")));
        // Alice's handle was replaced; bob's is fresh and sees none of hers.
        let replaced = as_alice.all(json!({ "table": "shows" }).to_string());
        assert!(
            matches!(replaced, Err(JazzError::Replaced { .. })),
            "{replaced:?}"
        );
        let as_bob = session.jazz().unwrap();
        assert_ne!(as_bob.account().unwrap(), Some(account));
        as_alice.shutdown().unwrap();

        // Signed out: no user, and no client to fetch.
        session.sign_out().unwrap();
        let out = snapshot_until(&session, |s| s["status"] == "signed-out");
        assert!(out.get("user").is_none_or(Value::is_null), "{out}");
        match session.jazz() {
            Err(JazzError::NoUser { status, .. }) => assert_eq!(status, "signed-out"),
            other => panic!("no user: {:?}", other.map(|_| ())),
        }
        assert!(matches!(
            as_bob.all(json!({ "table": "shows" }).to_string()),
            Err(JazzError::Replaced { .. })
        ));

        session.shutdown().unwrap();
        assert!(matches!(session.snapshot(), Err(JazzError::Closed)));
        // The watch saw alice, bob (each waited for above), then a last
        // closed. (A watch delivers the latest snapshot: signing out and
        // closing in quick succession may skip the one between.)
        seen.extend(watched_until(&states, |s| s["kind"] == "closed"));
        let subjects: Vec<_> = seen
            .iter()
            .filter(|s| s["status"] == "ready")
            .map(|s| s["user"]["subject"].clone())
            .collect();
        assert!(
            subjects.contains(&json!("alice")) && subjects.contains(&json!("bob")),
            "{seen:?}"
        );
        watch.cancel();
    })
    .await
    .unwrap();
    app.backend.shutdown().await.unwrap();
}

/// A guest: a device account the session makes, whose key restores it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_keeps_a_device_account() {
    let app = App::start().await;
    let config = app.config(json!({ "initial": "signed-out" }));
    tokio::task::spawn_blocking(move || {
        let session = Session::open(config.clone(), None).unwrap();
        snapshot_until(&session, |s| s["status"] == "signed-out");
        session.create_device_account().unwrap();
        let guest = snapshot_until(&session, |s| s["status"] == "ready");
        assert_eq!(guest["user"]["kind"], "device", "{guest}");
        let key = session.export_device_key().unwrap();
        assert!(key.starts_with("jazz-auth-v1:"), "{key}");
        let jazz = session.jazz().unwrap();
        let account = jazz.account().unwrap().unwrap();
        jazz.insert("shows".into(), show(&account)).unwrap();
        jazz.wait_for_sync("global".into()).unwrap();
        session.shutdown().unwrap();
        assert!(matches!(
            jazz.all(json!({ "table": "shows" }).to_string()),
            Err(JazzError::Closed)
        ));

        // Another session restores the same account from its key.
        let again = Session::open(config, None).unwrap();
        again.restore_device_account(key).unwrap();
        snapshot_until(&again, |s| s["status"] == "ready");
        let restored = again.jazz().unwrap();
        assert_eq!(restored.account().unwrap(), Some(account));
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let shows = parse(
                &restored
                    .all_at(json!({ "table": "shows" }).to_string(), "remote".into())
                    .unwrap(),
            );
            if shows.as_array().unwrap().len() == 1 {
                break;
            }
            assert!(Instant::now() < deadline, "{shows}");
            std::thread::sleep(Duration::from_millis(50));
        }
        again.shutdown().unwrap();
    })
    .await
    .unwrap();
    app.backend.shutdown().await.unwrap();
}

/// A guest whose user signs in with the app (no account yet) is linked:
/// the same account, reopened on the bearer (a new client: the guest's
/// handle is `Replaced`). With `linkGuest: false` it waits for
/// `link_device_account`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_guest_is_linked_when_its_user_signs_in() {
    let app = App::start().await;
    for link_guest in [true, false] {
        let config = app.config(json!({ "auth": { "bearer": true }, "initial": "device",
            "linkGuest": link_guest }));
        tokio::task::spawn_blocking(move || {
            let bearer = Arc::new(Bearer(Mutex::new(None)));
            let session = Session::open(config, Some(bearer.clone())).unwrap();
            let guest = snapshot_until(&session, |s| s["status"] == "ready");
            assert_eq!(guest["user"]["kind"], "device", "{guest}");
            let as_guest = session.jazz().unwrap();
            let account = as_guest.account().unwrap().unwrap();

            // The user signs up with the app: a new user, no account yet.
            let user = format!("token-carol{}", if link_guest { "" } else { "-later" });
            *bearer.0.lock().unwrap() = Some(user);
            session.refresh().unwrap();
            if !link_guest {
                let still = parse(&session.snapshot().unwrap());
                assert_eq!(still["user"]["kind"], "device", "{still}");
                session.link_device_account().unwrap();
            }
            let linked = snapshot_until(&session, |s| {
                s["status"] == "ready" && s["user"]["kind"] == "backend"
            });
            assert!(
                linked["clientEpoch"].as_u64() > guest["clientEpoch"].as_u64(),
                "{linked}"
            );
            assert!(matches!(
                as_guest.account(),
                Err(JazzError::Replaced { .. })
            ));
            assert_eq!(session.jazz().unwrap().account().unwrap(), Some(account));
            session.shutdown().unwrap();
        })
        .await
        .unwrap();
    }
    app.backend.shutdown().await.unwrap();
}

// --- The C ABI ---

use jazz_ffi::c_abi::{
    jazz_call, jazz_close, jazz_session_call, jazz_session_close, jazz_session_jazz,
    jazz_session_open, jazz_session_watch, jazz_string_free, jazz_unwatch,
};

unsafe extern "C" {
    fn malloc(size: usize) -> *mut c_void;
}

unsafe extern "C" fn alice_cb(
    _ctx: *mut c_void,
    _reason: *const c_char,
    token: *mut *mut c_char,
) -> i32 {
    let text = b"token-alice";
    unsafe {
        let out = malloc(text.len() + 1) as *mut u8;
        ptr::copy_nonoverlapping(text.as_ptr(), out, text.len());
        *out.add(text.len()) = 0;
        *token = out as *mut c_char;
    }
    0
}

unsafe extern "C" fn seen_cb(ctx: *mut c_void, kind: *const c_char, json: *const c_char) {
    let seen = unsafe { &*(ctx as *const Mutex<Vec<(String, Value)>>) };
    let (kind, json) = unsafe {
        (
            CStr::from_ptr(kind).to_str().unwrap().to_owned(),
            parse(CStr::from_ptr(json).to_str().unwrap()),
        )
    };
    seen.lock().unwrap().push((kind, json));
}

fn c(text: &str) -> CString {
    CString::new(text).unwrap()
}

fn take(text: *mut c_char) -> Value {
    let copy = unsafe { CStr::from_ptr(text) }.to_str().unwrap().to_owned();
    unsafe { jazz_string_free(text) };
    parse(&copy)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_c_host_follows_a_session() {
    let app = App::start().await;
    let config = app.config(json!({ "auth": { "bearer": true } }));
    let bad = app.config(json!({}));
    tokio::task::spawn_blocking(move || unsafe {
        let seen: &'static Mutex<Vec<(String, Value)>> =
            Box::leak(Box::new(Mutex::new(Vec::new())));
        let mut err = ptr::null_mut();
        // "backend" (the default) follows a bearer: it needs one.
        assert!(jazz_session_open(c(&bad).as_ptr(), None, ptr::null_mut(), &mut err).is_null());
        assert_eq!(take(err)["kind"], "refused");

        let session = jazz_session_open(
            c(&config).as_ptr(),
            Some(alice_cb),
            ptr::null_mut(),
            &mut err,
        );
        assert!(!session.is_null(), "{}", take(err));
        let watch = jazz_session_watch(
            session,
            Some(seen_cb),
            seen as *const _ as *mut c_void,
            &mut err,
        );
        assert!(!watch.is_null());
        let call = |method: &str| {
            let mut err = ptr::null_mut();
            let out = jazz_session_call(session, c(method).as_ptr(), c("{}").as_ptr(), &mut err);
            if out.is_null() {
                Err(take(err))
            } else {
                Ok(take(out))
            }
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while call("snapshot").unwrap()["status"] != "ready" {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(20));
        }
        let handle = jazz_session_jazz(session, &mut err);
        assert!(!handle.is_null(), "{}", take(err));
        let account = {
            let out = jazz_call(handle, c("account").as_ptr(), c("{}").as_ptr(), &mut err);
            take(out)
        };
        assert!(account.is_string());
        call("signOut").unwrap();
        let none = jazz_session_jazz(session, &mut err);
        assert!(none.is_null());
        let error = take(err);
        assert_eq!(
            (error["kind"].as_str(), error["status"].as_str()),
            (Some("noUser"), Some("signed-out")),
            "{error}"
        );
        let out = jazz_call(handle, c("account").as_ptr(), c("{}").as_ptr(), &mut err);
        assert!(out.is_null());
        assert_eq!(take(err)["kind"], "replaced");
        assert_eq!(call("nope").unwrap_err()["kind"], "refused");
        jazz_close(handle);
        jazz_session_close(session);
        let deadline = Instant::now() + Duration::from_secs(5);
        while seen
            .lock()
            .unwrap()
            .last()
            .is_none_or(|(kind, _)| kind != "error")
        {
            assert!(Instant::now() < deadline, "no closed");
            std::thread::sleep(Duration::from_millis(10));
        }
        let seen = seen.lock().unwrap();
        assert!(
            seen.iter()
                .any(|(kind, s)| kind == "session" && s["status"] == "ready")
        );
        assert_eq!(seen.last().unwrap().1["kind"], "closed");
        jazz_unwatch(watch);
    })
    .await
    .unwrap();
    app.backend.shutdown().await.unwrap();
}

/// What a slow host token callback saw: whether it is running, and whether
/// it ran after the session closed.
struct SlowHost {
    running: std::sync::atomic::AtomicBool,
    closed: std::sync::atomic::AtomicBool,
    after_close: std::sync::atomic::AtomicBool,
}

unsafe extern "C" fn slow_cb(
    ctx: *mut c_void,
    reason: *const c_char,
    token: *mut *mut c_char,
) -> i32 {
    use std::sync::atomic::Ordering;
    let host = unsafe { &*(ctx as *const SlowHost) };
    if host.closed.load(Ordering::SeqCst) {
        host.after_close.store(true, Ordering::SeqCst);
    }
    host.running.store(true, Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(400));
    host.running.store(false, Ordering::SeqCst);
    unsafe { alice_cb(ctx, reason, token) }
}

/// When `jazz_session_close` returns, the token callback is not running and
/// is never called again, though the client was renewing its bearer on its
/// own (the backend refused it as expired): a host may free its ctx then.
/// No other call on the session runs meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_token_callback_runs_after_the_session_closes() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let app = App::start().await;
    let config = app.config(json!({ "auth": { "bearer": true } }));
    let host: &'static SlowHost = Box::leak(Box::new(SlowHost {
        running: AtomicBool::new(false),
        closed: AtomicBool::new(false),
        after_close: AtomicBool::new(false),
    }));
    let session = tokio::task::spawn_blocking(move || unsafe {
        let mut err = ptr::null_mut();
        let session = jazz_session_open(
            c(&config).as_ptr(),
            Some(slow_cb),
            host as *const SlowHost as *mut c_void,
            &mut err,
        );
        assert!(!session.is_null(), "{}", take(err));
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let mut err = ptr::null_mut();
            let out =
                jazz_session_call(session, c("snapshot").as_ptr(), c("{}").as_ptr(), &mut err);
            if take(out)["status"] == "ready" {
                break;
            }
            assert!(Instant::now() < deadline, "not ready");
            std::thread::sleep(Duration::from_millis(20));
        }
        session as usize
    })
    .await
    .unwrap();
    // The backend refuses alice's bearer as expired: the client asks the
    // host for another, on its own, again and again.
    app.relay.refuse("token-alice", "token expired");
    app.relay.kick();
    tokio::task::spawn_blocking(move || unsafe {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !host.running.load(Ordering::SeqCst) {
            assert!(Instant::now() < deadline, "no renewal");
            std::thread::sleep(Duration::from_millis(5));
        }
        jazz_session_close(session as *mut _);
        assert!(
            !host.running.load(Ordering::SeqCst),
            "close returned while the callback ran"
        );
        host.closed.store(true, Ordering::SeqCst);
        // The client's renewals would come now, if any could.
        std::thread::sleep(Duration::from_millis(1500));
        assert!(
            !host.after_close.load(Ordering::SeqCst),
            "the callback ran after close"
        );
    })
    .await
    .unwrap();
    app.backend.shutdown().await.unwrap();
}

/// A host's device session (Swift and Go open through this): a kept
/// key is never replaced by opening, whether the URL or the key is wrong.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opening_never_replaces_a_kept_device_key() {
    let app = App::start().await;
    let dir = std::env::temp_dir().join(format!("jazz-ffi-keep-key-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.join("device.key");
    let device = json!({ "initial": "device", "storage": "persistent",
        "dataDir": dir.to_str().unwrap() });
    let good = app.config(device.clone());
    let mut bad_url = device;
    bad_url["serverUrl"] = json!("http://192.0.2.1/jazz");
    let bad_url = app.config(bad_url);
    tokio::task::spawn_blocking(move || {
        let session = Session::open(good.clone(), None).unwrap();
        let guest = snapshot_until(&session, |s| s["status"] == "ready");
        let account = guest["user"]["account"].clone();
        session.shutdown().unwrap();
        let key = std::fs::read(&path).unwrap();

        let session = Session::open(bad_url, None).unwrap();
        assert!(
            std::fs::read(&path).unwrap() == key,
            "the wrong URL replaced the key"
        );
        let failed = parse(&session.snapshot().unwrap());
        assert_eq!(failed["status"], "error", "{failed}");
        assert_eq!(failed["error"]["kind"], "config", "{failed}");
        session.shutdown().unwrap();

        let quoted = [&b"\""[..], &key, &b"\""[..]].concat();
        std::fs::write(&path, &quoted).unwrap();
        let session = Session::open(good.clone(), None).unwrap();
        assert!(
            std::fs::read(&path).unwrap() == quoted,
            "the unparsable key was replaced"
        );
        let failed = parse(&session.snapshot().unwrap());
        assert_eq!(failed["status"], "error", "{failed}");
        session.shutdown().unwrap();

        std::fs::write(&path, &key).unwrap();
        let session = Session::open(good, None).unwrap();
        let mended = snapshot_until(&session, |s| s["status"] == "ready");
        assert_eq!(mended["user"]["account"], account, "{mended}");
        session.shutdown().unwrap();
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
