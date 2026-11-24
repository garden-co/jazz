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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::parse;
use encrypted_app::EncryptedApp as App;
use jazz_e2ee_client::testing::answer_memory_store;
use jazz_e2ee_client::{MemoryStore, PrivateStore as _, StoreError};
use jazz_ffi::{
    JazzError, JazzHandle, PrivateStore, PrivateStoreError, StoredDocument, generate_device_key,
};
use serde_json::{Value, json};

const DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../jazz-app/tests/fixtures/encrypted"
);

/// A host-language store that records saves.
#[derive(Default)]
struct HostStore {
    memory: MemoryStore,
    saves: Mutex<usize>,
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
        answer(futures::executor::block_on(self.memory.load(&scope, &doc))).map(|result| {
            result.map(|document| StoredDocument {
                bytes: document.bytes,
                revision: document.revision,
            })
        })
    }

    fn save(
        &self,
        scope: String,
        incarnation: u64,
        doc: String,
        bytes: Vec<u8>,
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
        answer(futures::executor::block_on(self.memory.list(&scope)))
    }

    fn delete(&self, scope: String) -> Result<(), PrivateStoreError> {
        answer(futures::executor::block_on(self.memory.delete(&scope)))
    }

    fn protect(&self, secret: Vec<u8>) -> Result<Vec<u8>, PrivateStoreError> {
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
        alice.encryption_call("ready".into(), "{}".into()).unwrap();
        let roots = parse(&alice.encryption_call("recoveryList".into(), "{}".into()).unwrap());
        assert_eq!(roots.as_array().unwrap().len(), 1);
        let refusal = alice.encryption_call(
            "recoveryRetire".into(),
            json!({ "rootId": roots[0]["id"] }).to_string(),
        ).unwrap_err();
        assert!(matches!(refusal, JazzError::Encryption { code, reason, io: false }
            if code == "device-account-root-required" && reason.contains("prevent recovery")));
        // An encryption call that names nothing is refused with its code.
        match alice.encryption_call("noSuchCall".into(), "{}".into()) {
            Err(JazzError::Encryption {
                code, io: false, ..
            }) => assert_eq!(code, "error"),
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
