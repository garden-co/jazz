//! jazz-ffi: end-to-end encryption for a host language. A device opens an
//! app that declares encryption with a private store it supplies (here a
//! store answering the request contract the way a Swift, Kotlin or Go store
//! does), and reads and writes encrypted columns as plain values.
//!
//! The app's Jazz endpoint is jazz-app-support's `relay/` stand-in; the core
//! is an in-process server whose backend holds no keys.

mod common;
#[path = "../../jazz-app/tests/fixtures/encrypted/omitted_columns.rs"]
mod omitted_columns;
#[allow(dead_code)]
#[path = "../../jazz-app/tests/relay/mod.rs"]
mod relay;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use common::parse;
use jazz_ffi::{JazzError, JazzHandle, PrivateStore, StoreReply, generate_device_key};
use serde_json::{Value, json};

const DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../jazz-app/tests/fixtures/encrypted"
);

/// One scope of a host's store: its incarnation and its documents (bytes and revision).
type Scope = (u64, HashMap<String, (Vec<u8>, u64)>);

/// A host's private store: keys and revisions in memory, answering the
/// request contract (`op`, bytes as base64, numbers as decimal text). A real
/// host keeps these in a file protected by its platform's key store.
#[derive(Default)]
struct HostStore {
    scopes: Mutex<HashMap<String, Scope>>,
    calls: Mutex<Vec<String>>,
    lose_recovery_ack: AtomicBool,
}

impl HostStore {
    fn answer(&self, request: &Value) -> Result<Value, String> {
        let text = |name: &str| request[name].as_str().ok_or(format!("no {name}"));
        let number =
            |name: &str| -> Result<u64, String> { text(name)?.parse().map_err(|_| "bad".into()) };
        let bytes = |name: &str| -> Result<Vec<u8>, String> {
            STANDARD.decode(text(name)?).map_err(|e| e.to_string())
        };
        let mut scopes = self.scopes.lock().unwrap();
        match text("op")? {
            "openScope" => {
                let next = scopes.len() as u64 + 1;
                let scope = scopes.entry(text("scope")?.to_owned()).or_insert((
                    // An incarnation that is never zero.
                    next * 7919,
                    HashMap::new(),
                ));
                Ok(json!(scope.0.to_string()))
            }
            "load" => Ok(scopes
                .get(text("scope")?)
                .and_then(|scope| scope.1.get(text("doc").ok()?))
                .map_or(Value::Null, |(bytes, revision)| {
                    json!({ "bytes": STANDARD.encode(bytes), "revision": revision.to_string() })
                })),
            "save" => {
                let Some(scope) = scopes.get_mut(text("scope")?) else {
                    return Err("wiped".into());
                };
                if scope.0 != number("incarnation")? {
                    return Err("wiped".into());
                }
                let doc = text("doc")?.to_owned();
                let expected = match request["expected"].as_str() {
                    Some(_) => Some(number("expected")?),
                    None => None,
                };
                let held = scope.1.get(&doc).map(|(_, revision)| *revision);
                if held != expected {
                    return Err("conflict".into());
                }
                let revision = held.unwrap_or(0) + 1;
                scope.1.insert(doc, (bytes("bytes")?, revision));
                Ok(json!(revision.to_string()))
            }
            "list" => Ok(json!(
                scopes
                    .get(text("scope")?)
                    .map(|scope| scope.1.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default()
            )),
            "delete" => {
                scopes.remove(text("scope")?);
                Ok(Value::Null)
            }
            // A real host protects the secret with its platform's key store.
            "protect" => Ok(json!(text("secret")?)),
            "unprotect" => Ok(json!(text("blob")?)),
            other => Err(format!("unknown op {other}")),
        }
    }
}

impl PrivateStore for HostStore {
    fn call(&self, request: String) -> StoreReply {
        let parsed = parse(&request);
        self.calls
            .lock()
            .unwrap()
            .push(parsed["op"].as_str().unwrap_or_default().to_owned());
        match self.answer(&parsed) {
            Ok(_)
                if parsed["op"] == "save"
                    && parsed["doc"] == "device"
                    && parsed["expected"].is_string()
                    && self.lose_recovery_ack.swap(false, Ordering::SeqCst) =>
            {
                StoreReply {
                    ok: false,
                    payload: "lost acknowledgement of the recovery nonce save".to_owned(),
                }
            }
            Ok(value) => StoreReply {
                ok: true,
                payload: value.to_string(),
            },
            Err(message) => StoreReply {
                ok: false,
                payload: message,
            },
        }
    }
}

fn app_file() -> String {
    std::fs::read_to_string(format!("{DIR}/jazz.app.json")).unwrap()
}

struct App {
    app_id: jazz_app_support::AppId,
    server: jazz_server::JazzServer,
    backend: jazz_app_support::Jazz,
    relay: relay::Relay,
}

/// A server, the app's backend (which holds no keys) and its endpoint.
async fn start() -> App {
    start_with(jazz_app_support::AppFiles::read(DIR).unwrap()).await
}

async fn start_with(files: jazz_app_support::AppFiles) -> App {
    let app_id = jazz_app_support::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let backend = jazz_app_support::Jazz::open(
        files,
        jazz_app_support::Config::new(app_id)
            .server(server.base_url())
            .backend(jazz_app_support::Backend::new(
                server.backend_secret(),
                server.admin_secret(),
                "service",
            )),
    )
    .await
    .expect("an encrypted app opens as a backend");
    let relay = relay::start(
        &server.base_url(),
        app_id,
        server.backend_secret(),
        server.admin_secret(),
        backend.bundle().unwrap(),
    )
    .await;
    App {
        app_id,
        server,
        backend,
        relay,
    }
}

impl App {
    /// A device's config: a new device account.
    fn config(&self) -> String {
        self.config_for(&app_file())
    }

    fn config_for(&self, app_file: &str) -> String {
        json!({
            "app": app_file, "appId": self.app_id.to_string(),
            "serverUrl": self.relay.endpoint,
            "auth": { "deviceKey": generate_device_key().unwrap() },
            "environment": "test", "contract": 2,
        })
        .to_string()
    }

    async fn stop(self) {
        self.backend.shutdown().await.unwrap();
        self.server.shutdown().await;
    }
}

/// The first member row is the creator's, in the transaction that
/// makes the space row.
fn make_project(alice: &Arc<JazzHandle>, alice_account: &str) -> (String, String) {
    let projects = alice.table_identity("projects".into()).unwrap();
    let tx = alice.clone().begin().unwrap();
    let project = parse(
        &tx.insert(
            "projects".into(),
            json!({ "name": "plans", "ownerId": alice_account }).to_string(),
        )
        .unwrap(),
    )["id"]
        .as_str()
        .unwrap()
        .to_owned();
    tx.insert(
        "__e2ee_members".into(),
        json!({ "scopeId": projects, "identifier": project, "accountId": alice_account })
            .to_string(),
    )
    .unwrap();
    tx.commit(Some("global".into())).unwrap();
    (project, projects)
}

/// Contract: a host-language app opens an app that declares encryption with
/// its own private store; two members share a space and read each other's
/// encrypted rows as plain values through the JSON handle; the encryption
/// calls (`ready`, `keysDeliver`) work; opening without a store is refused,
/// naming it.
///
/// ```text
/// alice (host store) ──project + member (one transaction)──► server
/// alice ──encrypted task (waits for the space's key)──► server
/// alice ──keysDeliver──► bob (host store) reads it ──reply──► alice reads it
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_app_shares_an_encrypted_space_with_a_store_it_supplies() {
    let app = start().await;
    let (alice_config, bob_config, no_store) = (app.config(), app.config(), app.config());

    let result = tokio::task::spawn_blocking(move || {
        // Without a store the open names what is missing.
        match JazzHandle::open(no_store) {
            Err(JazzError::Refused { reason }) => {
                assert!(
                    reason.contains("declares end-to-end encryption"),
                    "{reason}"
                );
                assert!(reason.contains("Config::encryption"), "{reason}");
            }
            Err(other) => panic!("expected a refusal, got {other}"),
            Ok(_) => panic!("an encrypted app opened without a store"),
        }

        let alice_store = Arc::new(HostStore::default());
        let alice = JazzHandle::open_with_store(alice_config, alice_store.clone(), None)
            .expect("alice opens with her store");
        let bob = JazzHandle::open_with_store(bob_config, Arc::new(HostStore::default()), None)
            .expect("bob opens with his store");
        assert_eq!(alice.encryption_ready(30_000).unwrap(), "active");
        assert_eq!(bob.encryption_ready(30_000).unwrap(), "active");
        assert!(
            alice_store
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|op| op == "save"),
            "her keys went to her own store"
        );

        let (alice_account, bob_account) = (
            alice.account().unwrap().unwrap(),
            bob.account().unwrap().unwrap(),
        );
        let (project, projects) = make_project(&alice, &alice_account);
        alice
            .insert_at(
                "__e2ee_members".into(),
                json!({ "scopeId": projects, "identifier": project, "accountId": bob_account })
                    .to_string(),
                "global".into(),
            )
            .unwrap();

        // The first write of the space waits for its key to be made.
        let inserted = parse(
            &alice
                .insert_at(
                    "tasks".into(),
                    json!({ "projectId": project, "title": "alice's secret" }).to_string(),
                    "global".into(),
                )
                .unwrap(),
        );
        assert_eq!(
            inserted["title"], "alice's secret",
            "the row an insert returns has the plaintext, not the sealed bytes: {inserted}"
        );
        alice
            .encryption_call("keysDeliver".into(), "{}".into())
            .unwrap();

        let tasks = json!({ "table": "tasks" }).to_string();
        let seen = |handle: &JazzHandle, count: usize| {
            let end = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                let read = handle.all_at(tasks.clone(), "remote".into());
                let rows = parse(read.as_deref().unwrap_or("[]"));
                let mut titles: Vec<String> = rows
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(|row| row["title"].as_str().map(str::to_owned))
                    .collect();
                if titles.len() >= count {
                    titles.sort();
                    return titles;
                }
                assert!(
                    std::time::Instant::now() < end,
                    "timed out: {rows} (last read: {read:?})"
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        };
        assert_eq!(seen(&bob, 1), ["alice's secret"]);
        bob.insert_at(
            "tasks".into(),
            json!({ "projectId": project, "title": "bob's reply" }).to_string(),
            "global".into(),
        )
        .unwrap();
        assert_eq!(seen(&alice, 2), ["alice's secret", "bob's reply"]);

        // An equality filter on the encrypted column (with the space's own
        // equality, which a scalable read needs) finds the row by its
        // plaintext, and the row has no token column.
        let by_title = |title: &str| {
            parse(
                &alice
                    .all_at(
                        json!({ "table": "tasks", "where": [
                            { "column": "projectId", "op": "eq", "value": project },
                            { "column": "title", "op": "eq", "value": title }
                        ] })
                        .to_string(),
                        "remote".into(),
                    )
                    .unwrap(),
            )
        };
        let found = by_title("bob's reply");
        assert_eq!(found.as_array().unwrap().len(), 1, "{found}");
        assert_eq!(found[0]["title"], "bob's reply");
        assert!(found[0].get("__e2ee_eq_title").is_none(), "{found}");
        assert!(by_title("nobody's").as_array().unwrap().is_empty());

        // An encryption call that names nothing is refused with its code.
        match alice.encryption_call("noSuchCall".into(), "{}".into()) {
            Err(JazzError::Refused { reason }) => {
                assert!(reason.contains("encryption"), "{reason}")
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        let state = parse(&alice.encryption_state().unwrap());
        assert_eq!(state["status"], "active", "{state}");
        alice.shutdown().unwrap();
        bob.shutdown().unwrap();
    })
    .await;
    app.stop().await;
    result.unwrap();
}

/// Alice's FFI insert returns the logical defaults and nulls for encrypted
/// fields she leaves out, matching the same row read through the FFI.
/// An implicit read includes the ordinary column whose name looks like a
/// token for the unindexed field and omits the actual token column.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ffi_insert_returns_omitted_encrypted_values() {
    let files = omitted_columns::app();
    let app = start_with(jazz_app_support::AppFiles::new(
        files.app.clone(),
        Some(files.permissions.into_bytes()),
    ))
    .await;
    let config = app.config_for(&files.app);
    let result = tokio::task::spawn_blocking(move || {
        let alice =
            JazzHandle::open_with_store(config, Arc::new(HostStore::default()), None).unwrap();
        assert_eq!(alice.encryption_ready(30_000).unwrap(), "active");
        let (project, _) = make_project(&alice, &alice.account().unwrap().unwrap());
        let inserted = parse(
            &alice
                .insert_at(
                    "tasks".into(),
                    json!({ "projectId": project, "__e2ee_eq_privateNote": "alice's note" })
                        .to_string(),
                    "global".into(),
                )
                .unwrap(),
        );
        assert_eq!(inserted["title"], "untitled");
        assert_eq!(inserted.get("privateNote"), Some(&Value::Null));
        assert_eq!(inserted["__e2ee_eq_privateNote"], "alice's note");
        assert!(inserted.get("__e2ee_eq_title").is_none());
        let rows = alice.all(json!({ "table": "tasks" }).to_string()).unwrap();
        assert_eq!(parse(&rows), json!([inserted]));
        let explicit = json!({ "table": "tasks", "select": ["__e2ee_eq_privateNote"] });
        let error = alice.all(explicit.to_string()).unwrap_err();
        assert!(
            error.to_string().contains("Reading reserved column"),
            "{error}"
        );
        alice.shutdown().unwrap();
    })
    .await;
    app.stop().await;
    result.unwrap();
}

/// Reopening a private store after a lost acknowledgement of the spent
/// nonce enrolls a fresh device and continues recovery with the same key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_continues_after_reopening_with_a_stranded_nonce() {
    let app = start().await;
    let config = app.config();
    let result = tokio::task::spawn_blocking(move || {
        let laptop =
            JazzHandle::open_with_store(config.clone(), Arc::new(HostStore::default()), None)
                .unwrap();
        assert_eq!(laptop.encryption_ready(30_000).unwrap(), "active");
        let store = Arc::new(HostStore {
            lose_recovery_ack: AtomicBool::new(true),
            ..Default::default()
        });
        let phone = JazzHandle::open_with_store(config.clone(), store.clone(), None).unwrap();
        let error = phone.encryption_ready(15_000).unwrap_err();
        assert!(
            matches!(error, JazzError::Io { reason } if reason.contains("lost acknowledgement")),
        );
        let stranded = phone
            .encryption_call("deviceId".into(), "{}".into())
            .unwrap();
        phone.shutdown().unwrap();

        let reopened = JazzHandle::open_with_store(config, store, None).unwrap();
        assert_eq!(reopened.encryption_ready(15_000).unwrap(), "active");
        let recovered = reopened
            .encryption_call("deviceId".into(), "{}".into())
            .unwrap();
        assert_ne!(
            recovered, stranded,
            "the spent nonce belongs to the old device"
        );

        reopened.shutdown().unwrap();
        laptop.shutdown().unwrap();
    })
    .await;
    app.stop().await;
    result.unwrap();
}

/// Contract: a device account whose account-secret root was retired still
/// starts: an active device reopened on its store is active (the retired
/// root is not registered again), and a new device with the same secret
/// stays awaiting approval, answering well before its deadline instead of
/// waiting for a recovery that cannot come.
///
/// ```text
/// laptop ──ready: registers the secret's root──► retire it ──► close
/// laptop (same store) ──reopen──► active
/// phone (same secret, new store) ──ready──► awaiting-approval, at once
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_device_account_starts_after_its_secrets_root_is_retired() {
    let app = start().await;
    let config = app.config();
    let result = tokio::task::spawn_blocking(move || {
        let store = Arc::new(HostStore::default());
        let laptop = JazzHandle::open_with_store(config.clone(), store.clone(), None).unwrap();
        assert_eq!(laptop.encryption_ready(30_000).unwrap(), "active");
        // The verified account predates the root its start registered: read
        // it again until the root is there.
        let started = std::time::Instant::now();
        let roots = loop {
            laptop.encryption_call("ready".into(), "{}".into()).unwrap();
            let roots = parse(
                &laptop
                    .encryption_call("recoveryList".into(), "{}".into())
                    .unwrap(),
            );
            if !roots.as_array().unwrap().is_empty() {
                break roots;
            }
            assert!(
                started.elapsed() < Duration::from_secs(15),
                "no root registered"
            );
            std::thread::sleep(Duration::from_millis(50));
        };
        assert_eq!(roots.as_array().unwrap().len(), 1, "{roots}");
        laptop
            .encryption_call(
                "recoveryRetire".into(),
                json!({ "rootId": roots[0]["id"] }).to_string(),
            )
            .unwrap();
        laptop.shutdown().unwrap();

        let reopened = JazzHandle::open_with_store(config.clone(), store, None).unwrap();
        assert_eq!(reopened.encryption_ready(15_000).unwrap(), "active");

        let phone =
            JazzHandle::open_with_store(config, Arc::new(HostStore::default()), None).unwrap();
        let started = std::time::Instant::now();
        assert_eq!(phone.encryption_ready(30_000).unwrap(), "awaiting-approval");
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "it did not wait out its deadline: {:?}",
            started.elapsed()
        );

        phone.shutdown().unwrap();
        reopened.shutdown().unwrap();
    })
    .await;
    app.stop().await;
    result.unwrap();
}

unsafe extern "C" {
    fn strdup(text: *const std::ffi::c_char) -> *mut std::ffi::c_char;
}

/// The C store callback of a host (Go's): `ctx` is its store.
unsafe extern "C" fn c_store(
    ctx: *mut std::ffi::c_void,
    request: *const std::ffi::c_char,
    payload: *mut *mut std::ffi::c_char,
) -> i32 {
    // SAFETY: the test passes a live `HostStore` as `ctx`, and the request is
    // NUL-terminated; `strdup` makes the `malloc`ed string jazz frees.
    unsafe {
        let store = &*(ctx as *const HostStore);
        let request = std::ffi::CStr::from_ptr(request).to_str().unwrap();
        let reply = store.call(request.to_owned());
        let text = std::ffi::CString::new(reply.payload).unwrap();
        *payload = strdup(text.as_ptr());
        if reply.ok { 0 } else { 1 }
    }
}

/// Contract: the C ABI opens an encrypted app with a store callback (a NULL
/// one is refused), and `jazz_call` reaches the encryption operations by name.
///
/// ```text
/// Go host ──jazz_open_encrypted(store callback)──► ready ► state ► deviceId
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_c_abi_opens_an_encrypted_app_with_a_store_callback() {
    use std::ffi::{CStr, CString, c_char};
    let app = start().await;
    let config = app.config();
    let result = tokio::task::spawn_blocking(move || {
        let store = Arc::new(HostStore::default());
        let config = CString::new(config).unwrap();
        let call = |handle: *const jazz_ffi::c_abi::JazzHandle, method: &str, args: Value| {
            let (method, args) = (
                CString::new(method).unwrap(),
                CString::new(args.to_string()).unwrap(),
            );
            let mut err: *mut c_char = std::ptr::null_mut();
            // SAFETY: a live handle and NUL-terminated strings.
            let out = unsafe {
                jazz_ffi::c_abi::jazz_call(handle, method.as_ptr(), args.as_ptr(), &mut err)
            };
            assert!(!out.is_null(), "{:?}", unsafe { CStr::from_ptr(err) });
            // SAFETY: a NUL-terminated string jazz allocated.
            let text = unsafe { CStr::from_ptr(out) }.to_str().unwrap().to_owned();
            unsafe { jazz_ffi::c_abi::jazz_string_free(out) };
            parse(&text)
        };
        // SAFETY: valid strings, a live store and a writable error slot.
        unsafe {
            let mut err: *mut c_char = std::ptr::null_mut();
            let none = jazz_ffi::c_abi::jazz_open_encrypted(
                config.as_ptr(),
                None,
                std::ptr::null_mut(),
                None,
                std::ptr::null_mut(),
                &mut err,
            );
            assert!(none.is_null(), "a NULL store callback is refused");
            jazz_ffi::c_abi::jazz_string_free(err);
            let mut err: *mut c_char = std::ptr::null_mut();
            let handle = jazz_ffi::c_abi::jazz_open_encrypted(
                config.as_ptr(),
                None,
                std::ptr::null_mut(),
                Some(c_store),
                Arc::as_ptr(&store) as *mut std::ffi::c_void,
                &mut err,
            );
            assert!(!handle.is_null(), "{:?}", CStr::from_ptr(err));
            assert_eq!(
                call(handle, "encryptionReady", json!({ "timeoutMs": 30000 })),
                "active"
            );
            assert_eq!(
                call(handle, "encryptionState", json!({}))["status"],
                "active"
            );
            assert!(call(handle, "encryptionCall", json!({ "op": "deviceId" })).is_string());
            assert!(store.calls.lock().unwrap().iter().any(|op| op == "save"));
            // A negative readiness timeout is refused, not read as the default.
            let (method, args) = (c"encryptionReady", c"{\"timeoutMs\": -1}");
            let mut err: *mut c_char = std::ptr::null_mut();
            let out = jazz_ffi::c_abi::jazz_call(handle, method.as_ptr(), args.as_ptr(), &mut err);
            assert!(out.is_null(), "a negative timeout is refused");
            assert!(
                CStr::from_ptr(err).to_str().unwrap().contains("timeoutMs"),
                "{:?}",
                CStr::from_ptr(err)
            );
            jazz_ffi::c_abi::jazz_string_free(err);
            jazz_ffi::c_abi::jazz_close(handle);
        }
    })
    .await;
    app.stop().await;
    result.unwrap();
}
