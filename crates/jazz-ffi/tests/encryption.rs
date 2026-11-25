//! jazz-ffi: end-to-end encryption for a host language. A device opens an
//! app that declares encryption with a private store it supplies (here a
//! store answering the request contract the way a Swift, Kotlin or Go store
//! does), and reads and writes encrypted columns as plain values.
//!
//! The app's Jazz endpoint is jazz-app-support's `relay/` stand-in; the core
//! is an in-process server whose backend holds no keys.

mod common;
#[path = "../../jazz-app/tests/relay/encrypted_app.rs"]
mod encrypted_app;
#[path = "../../jazz-app/tests/fixtures/encrypted/omitted_columns.rs"]
mod omitted_columns;
#[allow(dead_code)]
#[path = "../../jazz-app/tests/relay/mod.rs"]
mod relay;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::parse;
use encrypted_app::EncryptedApp as App;
use jazz_e2ee_client::testing::answer_memory_store;
use jazz_e2ee_client::{MemoryStore, PrivateStore as _, StoreError};
use jazz_ffi::{
    JazzError, JazzHandle, PrivateStore, PrivateStoreError, ScopeDocument, StoredDocument,
    generate_device_key,
};
use serde_json::{Value, json};

const DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../jazz-app/tests/fixtures/encrypted"
);

#[derive(Debug, Default, PartialEq, Eq)]
struct Reads {
    single: usize,
    list: usize,
    bulk: usize,
}

/// A host-language store that records reads and saves.
#[derive(Default)]
struct HostStore {
    memory: MemoryStore,
    saves: Mutex<usize>,
    reads: Mutex<Reads>,
    wipe_during_bulk: AtomicBool,
}

fn answer<T>(answer: Result<T, StoreError>) -> Result<T, PrivateStoreError> {
    answer.map_err(|error| match error {
        StoreError::Conflict => PrivateStoreError::Conflict,
        StoreError::Wiped => PrivateStoreError::Wiped,
        StoreError::Failed(reason) => PrivateStoreError::Failed { reason },
    })
}

impl PrivateStore for HostStore {
    fn open_scope(&self, scope: String) -> Result<u64, PrivateStoreError> {
        answer(futures::executor::block_on(self.memory.open_scope(&scope)))
    }

    fn load(
        &self,
        scope: String,
        doc: String,
    ) -> Result<Option<StoredDocument>, PrivateStoreError> {
        self.reads.lock().unwrap().single += 1;
        answer(futures::executor::block_on(self.memory.load(&scope, &doc))).map(|result| {
            result.map(|document| StoredDocument {
                bytes: document.bytes,
                revision: document.revision,
            })
        })
    }

    fn load_all(
        &self,
        scope: String,
        incarnation: u64,
    ) -> Result<Vec<ScopeDocument>, PrivateStoreError> {
        self.reads.lock().unwrap().bulk += 1;
        if self.wipe_during_bulk.load(Ordering::Relaxed) {
            self.delete(scope.clone())?;
        }
        answer(self.memory.snapshot(&scope, incarnation)).map(|documents| {
            documents
                .into_iter()
                .map(|(doc, stored)| ScopeDocument {
                    doc,
                    bytes: stored.bytes,
                    revision: stored.revision,
                })
                .collect()
        })
    }

    fn save(
        &self,
        scope: String,
        incarnation: u64,
        doc: String,
        bytes: zeroize::Zeroizing<Vec<u8>>,
        expected: Option<u64>,
    ) -> Result<u64, PrivateStoreError> {
        *self.saves.lock().unwrap() += 1;
        answer(futures::executor::block_on(self.memory.save(
            &scope,
            incarnation,
            &doc,
            &bytes,
            expected,
        )))
    }

    fn list(&self, scope: String) -> Result<Vec<String>, PrivateStoreError> {
        self.reads.lock().unwrap().list += 1;
        answer(futures::executor::block_on(self.memory.list(&scope)))
    }

    fn delete(&self, scope: String) -> Result<(), PrivateStoreError> {
        answer(futures::executor::block_on(self.memory.delete(&scope)))
    }

    fn protect(&self, secret: zeroize::Zeroizing<Vec<u8>>) -> Result<Vec<u8>, PrivateStoreError> {
        answer(futures::executor::block_on(self.memory.protect(&secret)))
    }

    fn unprotect(&self, blob: Vec<u8>) -> Result<Vec<u8>, PrivateStoreError> {
        answer(futures::executor::block_on(self.memory.unprotect(&blob)))
            .map(|secret| secret.to_vec())
    }
}

fn app_file() -> String {
    std::fs::read_to_string(format!("{DIR}/jazz.app.json")).unwrap()
}

async fn start() -> App {
    App::start_with(jazz_app_support::AppFiles::read(DIR).unwrap()).await
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
}

fn wait_until_ready(handle: &JazzHandle, timeout_ms: u64) -> Result<Value, JazzError> {
    handle
        .encryption_call(
            "waitUntilReady".into(),
            json!({ "timeoutMs": timeout_ms }).to_string(),
        )
        .map(|result| parse(&result))
}

/// Alice reopens with 1,000 floor documents. The UniFFI adapter fetches the
/// whole scope in one callback and loads her previously stored device.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_returning_host_reads_its_scope_in_one_callback() {
    let app = start().await;
    let config = app.config();
    let result = tokio::task::spawn_blocking(move || {
        let store = Arc::new(HostStore::default());
        let alice = JazzHandle::open_with_store(config.clone(), store.clone(), None).unwrap();
        assert_eq!(wait_until_ready(&alice, 30_000).unwrap(), "active");
        let device = alice
            .encryption_call("deviceId".into(), "{}".into())
            .unwrap();
        alice.shutdown().unwrap();

        let scope = store.memory.scopes().pop().unwrap();
        let floors = floors_in(&store, &scope);
        let original = futures::executor::block_on(store.memory.load(&scope, &floors[0]))
            .unwrap()
            .unwrap();
        // The host treats these as opaque files. Reusing a record under
        // other names gives Rust floor documents it must read and reject.
        for _ in floors.len()..1_000 {
            let name = format!("floor-{}", uuid::Uuid::new_v4());
            store
                .memory
                .overwrite(&scope, &name, original.bytes.clone());
        }
        assert_eq!(floors_in(&store, &scope).len(), 1_000);
        *store.reads.lock().unwrap() = Reads::default();

        let reopened = JazzHandle::open_with_store(config, store.clone(), None).unwrap();
        assert_eq!(
            *store.reads.lock().unwrap(),
            Reads {
                single: 0,
                list: 0,
                bulk: 1
            }
        );
        assert_eq!(
            reopened
                .encryption_call("deviceId".into(), "{}".into())
                .unwrap(),
            device
        );
        reopened.shutdown().unwrap();
    })
    .await;
    app.stop().await;
    result.unwrap();
}

/// A wipe after Alice opens the scope invalidates its snapshot. Continued
/// wipes exhaust the startup retries and surface the wiped error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scope_wiped_during_the_callback_fails_startup_as_wiped() {
    let app = start().await;
    let config = app.config();
    let result = tokio::task::spawn_blocking(move || {
        let store = Arc::new(HostStore::default());
        store.wipe_during_bulk.store(true, Ordering::Relaxed);
        let error = match JazzHandle::open_with_store(config, store.clone(), None) {
            Ok(_) => panic!("opened a scope wiped during each snapshot"),
            Err(error) => error,
        };
        assert!(matches!(error, JazzError::Encryption { code, reason, .. }
            if code == "store" && reason.contains("wiped")));
        assert!(store.reads.lock().unwrap().bulk > 1);
        assert_eq!(*store.saves.lock().unwrap(), 0);
    })
    .await;
    app.stop().await;
    result.unwrap();
}

/// Alice reopens from a snapshot with a floor whose signature Mallory spoiled.
/// Jazz reports the floor and keeps the authentic device and space keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tampered_floor_does_not_discard_other_snapshot_documents() {
    let app = start().await;
    let config = app.config();
    let result = tokio::task::spawn_blocking(move || {
        let store = Arc::new(HostStore::default());
        let alice = JazzHandle::open_with_store(config.clone(), store.clone(), None).unwrap();
        assert_eq!(wait_until_ready(&alice, 30_000).unwrap(), "active");
        let account = alice.account().unwrap().unwrap();
        let (project, _) = make_project(&alice, &account);
        alice
            .insert_at(
                "tasks".into(),
                json!({ "projectId": project, "title": "plans" }).to_string(),
                "global".into(),
            )
            .unwrap();
        let device = alice
            .encryption_call("deviceId".into(), "{}".into())
            .unwrap();
        alice.shutdown().unwrap();

        let scope = store.memory.scopes().pop().unwrap();
        let name = format!("floor-{account}");
        let stored = futures::executor::block_on(store.memory.load(&scope, &name))
            .unwrap()
            .unwrap();
        let mut altered: Value = serde_json::from_slice(&stored.bytes).unwrap();
        let cut = altered["through"].as_str().unwrap();
        altered["through"] = if cut == "0" { json!("1") } else { json!("0") };
        store
            .memory
            .overwrite(&scope, &name, serde_json::to_vec(&altered).unwrap());

        let reopened = JazzHandle::open_with_store(config, store, None).unwrap();
        assert_eq!(
            reopened
                .encryption_call("deviceId".into(), "{}".into())
                .unwrap(),
            device
        );
        let reports = parse(
            &reopened
                .encryption_call("reports".into(), "{}".into())
                .unwrap(),
        );
        let skipped: Vec<_> = reports
            .as_array()
            .unwrap()
            .iter()
            .filter(|report| report["type"] == "skipped-record")
            .map(|report| &report["document"])
            .collect();
        assert_eq!(skipped, [&json!(name)]);
        assert_eq!(wait_until_ready(&reopened, 30_000).unwrap(), "active");
        let query = json!({ "table": "tasks" }).to_string();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let rows = parse(&reopened.all_at(query.clone(), "remote".into()).unwrap());
            if rows[0]["title"] == "plans" {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "{rows}");
            std::thread::sleep(Duration::from_millis(25));
        }
        reopened.shutdown().unwrap();
    })
    .await;
    app.stop().await;
    result.unwrap();
}

fn floors_in(store: &HostStore, scope: &str) -> Vec<String> {
    futures::executor::block_on(store.memory.list(scope))
        .unwrap()
        .into_iter()
        .filter(|name| name.starts_with("floor-"))
        .collect()
}

/// The first member row is the creator's, in the transaction that
/// makes the space row.
fn make_project(alice: &Arc<JazzHandle>, alice_account: &str) -> (String, String) {
    let project = jazz_app_support::ObjectId::new().to_string();
    let address = parse(
        &alice
            .encryption_call(
                "spaceRow".into(),
                json!({ "table": "projects", "id": project }).to_string(),
            )
            .unwrap(),
    );
    let projects = address["scopeId"].as_str().unwrap().to_owned();
    let tx = alice.clone().begin().unwrap();
    tx.insert(
        "projects".into(),
        json!({ "id": project, "name": "plans", "ownerId": alice_account }).to_string(),
    )
    .unwrap();
    tx.insert(
        "__e2ee_members".into(),
        json!({ "scopeId": projects, "identifier": address["identifier"], "accountId": alice_account })
            .to_string(),
    )
    .unwrap();
    tx.commit(Some("global".into())).unwrap();
    (project, projects)
}

/// Contract: a host-language app opens an app that declares encryption with
/// its own private store; two members share a space and read each other's
/// encrypted rows as plain values through the JSON handle; the encryption
/// calls (`waitUntilReady`, `keysDeliver`) work; opening without a store is refused,
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
        assert_eq!(wait_until_ready(&alice, 30_000).unwrap(), "active");
        assert_eq!(wait_until_ready(&bob, 30_000).unwrap(), "active");
        assert!(
            *alice_store.saves.lock().unwrap() > 0,
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

        // The generic native operation reaches the account's reset policy.
        assert!(
            matches!(alice.encryption_call("resetAccount".into(), "{}".into()),
            Err(JazzError::Encryption { code, io: false, .. }) if code == "account-reset-not-allowed")
        );
        // The verified account can predate the root its start registered:
        // read it again until the root is there.
        let started = std::time::Instant::now();
        let roots = loop {
            alice.encryption_call("ready".into(), "{}".into()).unwrap();
            let roots = parse(&alice.encryption_call("recoveryList".into(), "{}".into()).unwrap());
            if !roots.as_array().unwrap().is_empty() {
                break roots;
            }
            assert!(
                started.elapsed() < std::time::Duration::from_secs(15),
                "no root registered"
            );
            std::thread::sleep(Duration::from_millis(50));
        };
        assert_eq!(roots.as_array().unwrap().len(), 1, "{roots}");
        let refusal = alice.encryption_call(
            "recoveryRetire".into(),
            json!({ "rootId": roots[0]["id"] }).to_string(),
        ).unwrap_err();
        assert!(matches!(refusal, JazzError::Encryption { code, reason, io: false }
            if code == "device-account-root-required" && reason.contains("prevent recovery")));
        // An encryption call that names nothing is refused with its code,
        // and its message names the operation.
        match alice.encryption_call("noSuchCall".into(), "{}".into()) {
            Err(JazzError::Encryption {
                code,
                reason,
                io: false,
            }) => {
                assert_eq!(code, "error");
                assert_eq!(
                    reason,
                    "encryption (error): unknown encryption operation `noSuchCall`"
                );
            }
            other => panic!("expected a coded refusal, got {other:?}"),
        }
        let devices = parse(
            &alice
                .encryption_call("devices".into(), "{}".into())
                .unwrap(),
        );
        assert_eq!(devices[0]["removalRequested"], false);
        let state = parse(&alice.encryption_call("state".into(), "{}".into()).unwrap());
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
    let app = App::start_with(jazz_app_support::AppFiles::new(
        files.app.clone(),
        Some(files.permissions.into_bytes()),
    ))
    .await;
    let config = app.config_for(&files.app);
    let result = tokio::task::spawn_blocking(move || {
        let alice =
            JazzHandle::open_with_store(config, Arc::new(HostStore::default()), None).unwrap();
        assert_eq!(wait_until_ready(&alice, 30_000).unwrap(), "active");
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
        if parse(request)["op"] == "save" {
            *store.saves.lock().unwrap() += 1;
        }
        let (ok, answer) = answer_memory_store(&store.memory, request.to_owned());
        let text = std::ffi::CString::new(answer).unwrap();
        *payload = strdup(text.as_ptr());
        if ok { 0 } else { 1 }
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
                call(
                    handle,
                    "encryptionCall",
                    json!({ "op": "waitUntilReady", "args": { "timeoutMs": 30000 } })
                ),
                "active"
            );
            assert_eq!(
                call(handle, "encryptionCall", json!({ "op": "state" }))["status"],
                "active"
            );
            assert!(call(handle, "encryptionCall", json!({ "op": "deviceId" })).is_string());
            let reports = call(handle, "encryptionCall", json!({ "op": "reports" }));
            assert!(reports.as_array().unwrap().iter().all(Value::is_object));
            assert_eq!(
                call(handle, "encryptionCall", json!({ "op": "reports" })),
                json!([])
            );
            assert!(*store.saves.lock().unwrap() > 0);

            let mut err: *mut c_char = std::ptr::null_mut();
            let args = c"{\"op\":\"noSuchCall\"}";
            let out = jazz_ffi::c_abi::jazz_call(
                handle,
                c"encryptionCall".as_ptr(),
                args.as_ptr(),
                &mut err,
            );
            assert!(out.is_null());
            assert!(!err.is_null());
            let failure = parse(CStr::from_ptr(err).to_str().unwrap());
            jazz_ffi::c_abi::jazz_string_free(err);
            assert_eq!(failure["kind"], "refused");
            assert_eq!(failure["code"], "error");

            let mut err = std::ptr::null_mut();
            let reset = jazz_ffi::c_abi::jazz_call(
                handle,
                c"encryptionCall".as_ptr(),
                c"{\"op\":\"resetAccount\"}".as_ptr(),
                &mut err,
            );
            assert!(reset.is_null() && !err.is_null());
            let failure = parse(CStr::from_ptr(err).to_str().unwrap());
            assert_eq!(failure["kind"], "refused");
            assert_eq!(failure["code"], "account-reset-not-allowed");
            jazz_ffi::c_abi::jazz_string_free(err);
            // A negative readiness timeout is refused, not read as the default.
            let (method, args) = (
                c"encryptionCall",
                c"{\"op\":\"waitUntilReady\",\"args\":{\"timeoutMs\":-1}}",
            );
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

/// Contract: closing a handle ends a host's `waitUntilReady` (Go's, Swift's
/// and Kotlin's) at once with kind "closed", while the device waits for an
/// unreachable server: the close doesn't wait out the wait's timeout.
///
/// ```text
/// relay unreachable ─► host ──waitUntilReady(60 s)──► waiting
/// host ──shutdown──► waitUntilReady: closed
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_ends_a_wait_until_ready() {
    let app = start().await;
    app.relay.set_unreachable(true);
    let config = app.config();
    let result = tokio::task::spawn_blocking(move || {
        let handle =
            JazzHandle::open_with_store(config, Arc::new(HostStore::default()), None).unwrap();
        let (sent, waited) = std::sync::mpsc::channel();
        std::thread::spawn({
            let handle = handle.clone();
            move || {
                let _ = sent.send(wait_until_ready(&handle, 60_000));
            }
        });
        // The wait is on the client's thread before the close.
        std::thread::sleep(Duration::from_millis(300));
        let closing = std::time::Instant::now();
        handle.shutdown().unwrap();
        let waited = waited
            .recv_timeout(Duration::from_secs(60))
            .expect("the wait ends");
        assert!(matches!(waited, Err(JazzError::Closed)), "{waited:?}");
        assert!(
            closing.elapsed() < Duration::from_secs(5),
            "the close waited {:?} for readiness",
            closing.elapsed()
        );
    })
    .await;
    app.stop().await;
    result.unwrap();
}

/// A C host's store that closes its own handle from inside a request, once
/// armed with it.
#[derive(Default)]
struct ClosingStore {
    store: HostStore,
    handle: std::sync::atomic::AtomicPtr<jazz_ffi::c_abi::JazzHandle>,
    /// What its "close" answered: the result, or the error JSON.
    closed: Mutex<Option<Result<String, String>>>,
    /// The close's arguments (`{}` unless set).
    close_args: Mutex<Option<std::ffi::CString>>,
}

/// [`c_store`], calling "close" on the armed handle first.
unsafe extern "C" fn c_closing_store(
    ctx: *mut std::ffi::c_void,
    request: *const std::ffi::c_char,
    payload: *mut *mut std::ffi::c_char,
) -> i32 {
    use std::ffi::CStr;
    // SAFETY: the test passes a live `ClosingStore` as `ctx`; the handle it
    // is armed with is live, and closing doesn't free it.
    unsafe {
        let closing = &*(ctx as *const ClosingStore);
        let handle = closing
            .handle
            .swap(std::ptr::null_mut(), std::sync::atomic::Ordering::SeqCst);
        if !handle.is_null() {
            let mut err = std::ptr::null_mut();
            let args = closing.close_args.lock().unwrap().clone();
            let args = args.as_deref().unwrap_or(c"{}");
            let out =
                jazz_ffi::c_abi::jazz_call(handle, c"close".as_ptr(), args.as_ptr(), &mut err);
            let answer = if out.is_null() {
                let error = CStr::from_ptr(err).to_str().unwrap().to_owned();
                jazz_ffi::c_abi::jazz_string_free(err);
                Err(error)
            } else {
                let result = CStr::from_ptr(out).to_str().unwrap().to_owned();
                jazz_ffi::c_abi::jazz_string_free(out);
                Ok(result)
            };
            *closing.closed.lock().unwrap() = Some(answer);
        }
        c_store(
            std::ptr::from_ref(&closing.store) as *mut _,
            request,
            payload,
        )
    }
}

/// Contract: a C host's store callback, which runs on the client's thread,
/// may call "close" on its own handle: it returns at once without waiting
/// for that thread (no deadlock), the request it is answering still gets
/// its answer, the handle's calls fail "closed" from then on, and the
/// handle frees from another thread.
///
/// ```text
/// host ──prepareSpace──► client thread ──save──► store callback ──"close"──► null
///                                                       ◄── answer
/// host ──encryptionCall──► closed ► jazz_close
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_store_callback_closes_its_own_handle() {
    use std::ffi::{CStr, CString, c_char};
    let app = start().await;
    let config = app.config();
    let result = tokio::task::spawn_blocking(move || {
        let closing = Arc::new(ClosingStore::default());
        let config = CString::new(config).unwrap();
        /// A handle a test thread may call into.
        #[derive(Clone, Copy)]
        struct Handle(*mut jazz_ffi::c_abi::JazzHandle);
        // SAFETY: the C ABI takes calls on a handle from any thread.
        unsafe impl Send for Handle {}
        let call = |handle: Handle, method: &CStr, args: Value| {
            let args = CString::new(args.to_string()).unwrap();
            let mut err: *mut c_char = std::ptr::null_mut();
            // SAFETY: a live handle and NUL-terminated strings.
            unsafe {
                let out =
                    jazz_ffi::c_abi::jazz_call(handle.0, method.as_ptr(), args.as_ptr(), &mut err);
                let (text, ok) = if out.is_null() {
                    (err, false)
                } else {
                    (out, true)
                };
                let read = parse(CStr::from_ptr(text).to_str().unwrap());
                jazz_ffi::c_abi::jazz_string_free(text);
                if ok { Ok(read) } else { Err(read) }
            }
        };
        // SAFETY: valid strings, a live store and a writable error slot.
        let handle = unsafe {
            let mut err: *mut c_char = std::ptr::null_mut();
            let handle = jazz_ffi::c_abi::jazz_open_encrypted(
                config.as_ptr(),
                None,
                std::ptr::null_mut(),
                Some(c_closing_store),
                Arc::as_ptr(&closing) as *mut std::ffi::c_void,
                &mut err,
            );
            assert!(!handle.is_null(), "{:?}", CStr::from_ptr(err));
            Handle(handle)
        };
        let encryption = move |op: &str, args: Value| {
            call(handle, c"encryptionCall", json!({ "op": op, "args": args }))
        };
        assert_eq!(
            encryption("waitUntilReady", json!({ "timeoutMs": 30000 })).unwrap(),
            "active"
        );
        let id = jazz_app_support::ObjectId::new().to_string();
        let space = encryption("spaceRow", json!({ "table": "projects", "id": id })).unwrap();
        closing
            .handle
            .store(handle.0, std::sync::atomic::Ordering::SeqCst);
        // Preparing the space saves its key: the store callback closes.
        let (sent, prepared) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = sent.send(encryption("prepareSpace", space));
        });
        let prepared = prepared
            .recv_timeout(Duration::from_secs(30))
            .expect("the call the store callback closed under ends: no deadlock");
        let closed = closing.closed.lock().unwrap().clone();
        assert_eq!(
            closed,
            Some(Ok("null".to_owned())),
            "the callback's close returns at once"
        );
        if let Err(failure) = &prepared {
            assert_eq!(failure["kind"], "closed", "{failure}");
        }
        let after = encryption("state", json!({})).unwrap_err();
        assert_eq!(after["kind"], "closed", "{after}");
        let (sent, freed) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let handle = handle;
            // SAFETY: no other call on the handle runs or follows.
            unsafe { jazz_ffi::c_abi::jazz_close(handle.0) };
            let _ = sent.send(());
        });
        freed
            .recv_timeout(Duration::from_secs(30))
            .expect("the closed handle frees");
    })
    .await;
    app.stop().await;
    result.unwrap();
}

/// A C handle a test thread may call into.
#[derive(Clone, Copy)]
struct CHandle(*mut jazz_ffi::c_abi::JazzHandle);
// SAFETY: the C ABI takes calls on a handle from any thread.
unsafe impl Send for CHandle {}

/// `jazz_call`: the result's JSON, or the error's.
fn c_call(handle: CHandle, method: &std::ffi::CStr, args: Value) -> Result<Value, Value> {
    use std::ffi::{CStr, CString};
    let args = CString::new(args.to_string()).unwrap();
    let mut err = std::ptr::null_mut();
    // SAFETY: a live handle and NUL-terminated strings.
    unsafe {
        let out = jazz_ffi::c_abi::jazz_call(handle.0, method.as_ptr(), args.as_ptr(), &mut err);
        let (text, ok) = if out.is_null() {
            (err, false)
        } else {
            (out, true)
        };
        let read = parse(CStr::from_ptr(text).to_str().unwrap());
        jazz_ffi::c_abi::jazz_string_free(text);
        if ok { Ok(read) } else { Err(read) }
    }
}

/// A device opened over a [`ClosingStore`] (not armed yet), ready, with a
/// space to prepare.
fn closing_device(config: String) -> (CHandle, Arc<ClosingStore>, Value) {
    use std::ffi::{CStr, CString};
    let closing = Arc::new(ClosingStore::default());
    let config = CString::new(config).unwrap();
    // SAFETY: valid strings, a live store and a writable error slot.
    let handle = unsafe {
        let mut err = std::ptr::null_mut();
        let handle = jazz_ffi::c_abi::jazz_open_encrypted(
            config.as_ptr(),
            None,
            std::ptr::null_mut(),
            Some(c_closing_store),
            Arc::as_ptr(&closing) as *mut std::ffi::c_void,
            &mut err,
        );
        assert!(!handle.is_null(), "{:?}", CStr::from_ptr(err));
        CHandle(handle)
    };
    let encryption = |op: &str, args: Value| {
        c_call(handle, c"encryptionCall", json!({ "op": op, "args": args }))
    };
    assert_eq!(
        encryption("waitUntilReady", json!({ "timeoutMs": 30000 })).unwrap(),
        "active"
    );
    let id = jazz_app_support::ObjectId::new().to_string();
    let space = encryption("spaceRow", json!({ "table": "projects", "id": id })).unwrap();
    (handle, closing, space)
}

/// Arms `closing` to close with `args`, prepares `space` (whose key the
/// store saves: its callback closes), and frees the handle: what the
/// callback's close answered.
fn close_from_the_store(
    handle: CHandle,
    closing: &ClosingStore,
    space: Value,
    args: Value,
) -> Result<String, String> {
    *closing.close_args.lock().unwrap() = Some(std::ffi::CString::new(args.to_string()).unwrap());
    closing
        .handle
        .store(handle.0, std::sync::atomic::Ordering::SeqCst);
    let (sent, prepared) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = sent.send(c_call(
            handle,
            c"encryptionCall",
            json!({ "op": "prepareSpace", "args": space }),
        ));
    });
    let _ = prepared
        .recv_timeout(Duration::from_secs(30))
        .expect("the call the store callback closed under ends");
    let closed = closing
        .closed
        .lock()
        .unwrap()
        .take()
        .expect("the store callback closed");
    // SAFETY: the call above ended; nothing else uses the handle.
    unsafe { jazz_ffi::c_abi::jazz_close(handle.0) };
    closed
}

/// Contract: a close from inside the store callback can't wait for writes
/// (it ignores `timeoutMs`), so the writes it was asked for and loses (a
/// store in memory, with a server) fail it `unsynced` with `pending`, at
/// once, as a close that waited would at its timeout. Without unsent
/// writes, or without a `timeoutMs` (which accepts the loss, as any close
/// does), it answers null.
///
/// ```text
/// note (relay unreachable) ─► store callback ──"close" {timeoutMs}──► unsynced, pending 1
/// no writes                ─► store callback ──"close" {timeoutMs}──► null
/// note (relay unreachable) ─► store callback ──"close" {}──────────► null
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_store_callbacks_close_reports_the_writes_it_loses() {
    let app = start().await;
    let timeout = json!({ "timeoutMs": 10_000 });
    for (unsent, args) in [(true, timeout.clone()), (false, timeout), (true, json!({}))] {
        let config = app.config();
        let (handle, closing, space) = tokio::task::spawn_blocking(move || closing_device(config))
            .await
            .unwrap();
        if unsent {
            app.relay.set_unreachable(true);
            tokio::task::spawn_blocking(move || {
                c_call(
                    handle,
                    c"insert",
                    json!({ "table": "notes", "row": { "text": "unsent" } }),
                )
                .unwrap();
            })
            .await
            .unwrap();
        }
        let started = std::time::Instant::now();
        let closed = tokio::task::spawn_blocking({
            let args = args.clone();
            move || close_from_the_store(handle, &closing, space, args)
        })
        .await
        .unwrap();
        app.relay.set_unreachable(false);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the close didn't wait: {:?}",
            started.elapsed()
        );
        match (unsent, args.get("timeoutMs").is_some()) {
            (true, true) => {
                let error = parse(&closed.expect_err("lost writes fail the close"));
                assert_eq!(error["kind"], "unsynced", "{error}");
                assert_eq!(error["pending"], 1, "{error}");
            }
            _ => assert_eq!(closed, Ok("null".to_owned()), "unsent {unsent}, {args}"),
        }
    }
    app.stop().await;
}

/// Alice's host receives a refusal when her device has no key for the project,
/// whether she edits a task directly or stages the write in a transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn key_refusal_on_transaction_write_and_edit() {
    let app = start().await;
    let config = app.config();
    tokio::task::spawn_blocking(move || {
        let alice =
            JazzHandle::open_with_store(config, Arc::new(HostStore::default()), None).unwrap();
        wait_until_ready(&alice, 30_000).unwrap();
        let project = jazz_app_support::ObjectId::new().to_string();
        let task = jazz_app_support::ObjectId::new().to_string();
        let patch = json!({ "projectId": project, "title": "private" }).to_string();
        let edit = || {
            alice.edit(
                "tasks".into(),
                task.clone(),
                Some(patch.clone()),
                "[]".into(),
            )
        };
        // Before a root has been attempted, the original NotObserved edit
        // error is reported as a missing row.
        assert_eq!(
            edit().unwrap_err(),
            JazzError::Refused {
                reason: format!("configuration: no tasks row {task}"),
            }
        );

        let space = alice
            .encryption_call(
                "spaceRow".into(),
                json!({ "table": "projects", "id": project }).to_string(),
            )
            .unwrap();
        assert!(matches!(
            alice.insert_at("tasks".into(), patch.clone(), "global".into()),
            Err(JazzError::Permission { .. })
        ));
        assert_eq!(
            alice
                .encryption_call("prepareSpace".into(), space.clone())
                .unwrap(),
            "false"
        );
        let tx = alice.clone().begin().unwrap();
        let staged = tx
            .edit(
                "tasks".into(),
                task.clone(),
                Some(patch.clone()),
                "[]".into(),
            )
            .unwrap_err();
        tx.rollback().unwrap();
        let address = parse(&space);
        let expected = JazzError::Refused {
            reason: format!(
                "write rejected (write_rejected): space {}/{} has no valid root",
                address["scopeId"].as_str().unwrap(),
                project
            ),
        };
        assert_eq!(staged, expected);
        assert_eq!(edit().unwrap_err(), expected);
        alice.shutdown().unwrap();
    })
    .await
    .unwrap();
    app.stop().await;
}

/// Alice adds Bob through the UniFFI handle, sees his membership row, and
/// removes him while retaining her own creator membership.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_uniffi_handle_adds_lists_and_removes_space_members() {
    let app = start().await;
    let (alice_config, bob_config) = (app.config(), app.config());
    tokio::task::spawn_blocking(move || {
        let alice = JazzHandle::open_with_store(alice_config, Arc::new(HostStore::default()), None)
            .unwrap();
        let bob =
            JazzHandle::open_with_store(bob_config, Arc::new(HostStore::default()), None).unwrap();
        assert_eq!(wait_until_ready(&alice, 30_000).unwrap(), "active");
        assert_eq!(wait_until_ready(&bob, 30_000).unwrap(), "active");
        let alice_id = alice.account().unwrap().unwrap();
        let bob_id = bob.account().unwrap().unwrap();
        let project = parse(
            &alice
                .insert_at(
                    "projects".into(),
                    json!({ "name": "plans", "ownerId": alice_id }).to_string(),
                    "global".into(),
                )
                .unwrap(),
        );
        let id = project["id"].as_str().unwrap().to_owned();
        let members = || parse(&alice.member_list("projects".into(), id.clone()).unwrap());
        let creator = members()[0].clone();
        assert_eq!(creator["accountId"], alice_id);

        let admitted = parse(
            &alice
                .member_add(
                    "projects".into(),
                    id.clone(),
                    bob_id.clone(),
                    Some("global".into()),
                )
                .unwrap(),
        );
        assert_eq!(admitted["scopeId"], creator["scopeId"]);
        assert_eq!(admitted["identifier"], id);
        assert_eq!(admitted["accountId"], bob_id);
        assert_eq!(members().as_array().unwrap().len(), 2);
        assert!(members().as_array().unwrap().contains(&admitted));

        assert_eq!(
            alice
                .member_remove("projects".into(), id.clone(), bob_id.clone())
                .unwrap(),
            1
        );
        assert_eq!(
            alice
                .member_remove("projects".into(), id.clone(), bob_id)
                .unwrap(),
            0
        );
        assert_eq!(members(), json!([creator]));
    })
    .await
    .unwrap();
}
