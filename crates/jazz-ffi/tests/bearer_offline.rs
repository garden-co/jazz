//! jazz-ffi v2: a user signed in with the app's bearer opens their
//! persistent store again with no network, through UniFFI
//! (`JazzHandle::open_with_tokens`) and the C ABI (`jazz_open_user`), as
//! jazz-app-support's `end_user` tests do in Rust.

mod common;
#[allow(dead_code)]
#[path = "../../jazz-app/tests/relay/mod.rs"]
mod relay;

use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;
use std::sync::Arc;

use common::*;
use jazz_ffi::c_abi::{jazz_call, jazz_close, jazz_open_user, jazz_string_free};
use jazz_ffi::{JazzHandle, TokenProvider};
use serde_json::{Value, json};

struct App {
    server: jazz_server::JazzServer,
    backend: jazz_app_support::Jazz,
    relay: Arc<relay::Relay>,
    app_id: String,
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
    App {
        server,
        backend,
        relay: Arc::new(relay),
        app_id: app_id.to_string(),
    }
}

impl App {
    fn config(&self, dir: &std::path::Path) -> String {
        json!({
            "app": STAGE_PLAN_APP,
            "appId": self.app_id,
            "serverUrl": self.relay.endpoint,
            "auth": { "bearer": true },
            "storage": "persistent",
            "dataDir": dir.to_str().unwrap(),
            "contract": 2,
        })
        .to_string()
    }

    async fn stop(self) {
        self.backend.shutdown().await.unwrap();
        self.server.shutdown().await;
    }
}

/// A store dir of its own, removed when dropped.
struct Dir(std::path::PathBuf);
impl Dir {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "jazz-ffi-{name}-{}-{}",
            std::process::id(),
            jazz_app_support::AppId::random()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Jwt(String);
impl TokenProvider for Jwt {
    fn token(&self, _reason: String) -> Result<Option<String>, jazz_ffi::JazzError> {
        Ok(Some(self.0.clone()))
    }
}

fn names(rows: &Value) -> usize {
    rows.as_array().expect("rows").len()
}

/// UniFFI: alice signs in once and closes; with the server unreachable she
/// opens her store again and reads it, a user who never signed in here is
/// refused, and once the server is back her offline write uploads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bearers_user_opens_offline_over_uniffi() {
    let app = app().await;
    let dir = Dir::new("uniffi-offline");
    let config = app.config(dir.path());
    let relay = app.relay.clone();
    let result = tokio::task::spawn_blocking(move || {
        let open = |user: &str, n| {
            JazzHandle::open_with_tokens(config.clone(), Arc::new(Jwt(relay::jwt(user, n))))
        };
        let alice = open("alice", 0).expect("alice signs in");
        let state = parse(&alice.session_state().unwrap().unwrap());
        let account = state["user"]["account"].as_str().unwrap().to_owned();
        alice.insert("shows".into(), show(&account)).unwrap();
        alice.wait_for_sync("global".into()).unwrap();
        alice.shutdown().unwrap();

        relay.set_unreachable(true);
        let alice = open("alice", 1).expect("alice opens offline");
        let state = parse(&alice.session_state().unwrap().unwrap());
        assert_eq!(state["status"], "signedIn", "{state}");
        assert_eq!(state["user"]["account"], account.as_str(), "{state}");
        let shows = json!({ "table": "shows" }).to_string();
        assert_eq!(names(&parse(&alice.all(shows.clone()).unwrap())), 1);
        alice.insert("shows".into(), show(&account)).unwrap();

        let bob = open("bob", 0);
        let Err(jazz_ffi::JazzError::Io { reason }) = &bob else {
            panic!("bob is refused offline: {:?}", bob.map(|_| ()));
        };
        assert!(
            reason.contains("opening offline needs an earlier sign-in"),
            "{reason}"
        );

        relay.set_unreachable(false);
        alice
            .wait_for_sync("global".into())
            .expect("the offline write uploads once the server signs her in");
        assert_eq!(names(&parse(&alice.all(shows).unwrap())), 2);
        alice.shutdown().unwrap();
    })
    .await;
    app.stop().await;
    result.unwrap();
}

fn c(text: &str) -> CString {
    CString::new(text).unwrap()
}

/// The C host's token callback: the JWT its context holds.
unsafe extern "C" fn token_cb(
    ctx: *mut c_void,
    _reason: *const c_char,
    token: *mut *mut c_char,
) -> i32 {
    let jwt = unsafe { &*(ctx as *const String) };
    unsafe extern "C" {
        fn malloc(size: usize) -> *mut c_void;
    }
    unsafe {
        let out = malloc(jwt.len() + 1) as *mut u8;
        ptr::copy_nonoverlapping(jwt.as_ptr(), out, jwt.len());
        *out.add(jwt.len()) = 0;
        *token = out as *mut c_char;
    }
    0
}

fn call(
    handle: *mut jazz_ffi::c_abi::JazzHandle,
    method: &str,
    args: Value,
) -> Result<Value, String> {
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
        copy
    };
    if out.is_null() {
        Err(take(err))
    } else {
        Ok(parse(&take(out)))
    }
}

/// The C ABI: `jazz_open_user` (Go's path) opens offline the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bearers_user_opens_offline_over_the_c_abi() {
    let app = app().await;
    let dir = Dir::new("c-offline");
    let config = app.config(dir.path());
    let relay = app.relay.clone();
    let result = tokio::task::spawn_blocking(move || unsafe {
        let open = |jwt: &String| {
            let mut err = ptr::null_mut();
            let handle = jazz_open_user(
                c(&config).as_ptr(),
                Some(token_cb),
                jwt as *const String as *mut c_void,
                &mut err,
            );
            if handle.is_null() {
                let message = CStr::from_ptr(err).to_str().unwrap().to_owned();
                jazz_string_free(err);
                Err(message)
            } else {
                Ok(handle)
            }
        };
        let (first, again, bob) = (
            relay::jwt("alice", 0),
            relay::jwt("alice", 1),
            relay::jwt("bob", 0),
        );
        let alice = open(&first).expect("alice signs in");
        let account = call(alice, "session", json!({})).unwrap()["user"]["account"]
            .as_str()
            .unwrap()
            .to_owned();
        let row = parse(&show(&account));
        call(alice, "insert", json!({ "table": "shows", "row": row })).unwrap();
        call(alice, "waitForSync", json!({ "tier": "global" })).unwrap();
        jazz_close(alice);

        relay.set_unreachable(true);
        let alice = open(&again).expect("alice opens offline");
        let state = call(alice, "session", json!({})).unwrap();
        assert_eq!(state["status"], "signedIn", "{state}");
        assert_eq!(state["user"]["account"], account.as_str(), "{state}");
        let rows = call(alice, "all", json!({ "query": { "table": "shows" } })).unwrap();
        assert_eq!(names(&rows), 1);
        call(alice, "insert", json!({ "table": "shows", "row": row })).unwrap();

        let refused = open(&bob).unwrap_err();
        assert!(
            refused.contains("opening offline needs an earlier sign-in"),
            "{refused}"
        );

        relay.set_unreachable(false);
        call(alice, "waitForSync", json!({ "tier": "global" }))
            .expect("the offline write uploads once the server signs her in");
        jazz_close(alice);
        // The token callback's context outlives the handles that use it.
        drop((first, again, bob));
    })
    .await;
    app.stop().await;
    result.unwrap();
}
