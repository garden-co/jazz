//! jazz-ffi v2: backend clients, machines syncing through the app's main
//! backend with their own credential (`"auth": { "credential": true }`),
//! against jazz-app's `relay/` stand-in for `createJazz` with
//! `backendClients`. Trusted (`machine-trusted`) or scoped (`token-agent`),
//! as the main backend decides; `for_user` on a trusted one; closing with a
//! timeout. Through UniFFI and the C ABI.

mod common;
#[allow(dead_code)]
#[path = "../../jazz-app/tests/relay/mod.rs"]
mod relay;

use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use jazz_ffi::{JazzError, JazzHandle, TokenProvider};
use serde_json::{Value, json};

struct Machine(&'static str);
impl TokenProvider for Machine {
    fn token(&self, _reason: String) -> Result<Option<String>, JazzError> {
        Ok(Some(self.0.to_owned()))
    }
}

struct App {
    server: jazz_server::JazzServer,
    backend: jazz_app::Jazz,
    relay: relay::Relay,
    app_id: jazz_app::AppId,
}

impl App {
    async fn start() -> Self {
        let app_id = jazz_app::AppId::random();
        let server = jazz_server::JazzServer::builder()
            .with_app_id(app_id)
            .start()
            .await
            .unwrap();
        let files = jazz_app::AppFiles::new(STAGE_PLAN_APP.as_bytes().to_vec(), None)
            .with_permissions(STAGE_PLAN_PERMISSIONS.as_bytes().to_vec());
        let backend = jazz_app::Jazz::open(
            files,
            jazz_app::Config::new(app_id)
                .server(server.base_url())
                .backend(jazz_app::Backend::new(
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
            server,
            backend,
            relay,
            app_id,
        }
    }

    /// A backend client's config (with the app's permissions bundle, as
    /// TS's createJazzClient takes it), contract 2.
    fn config(&self, auth: Value) -> String {
        let mut config = parse(&self.user_config(auth));
        config["permissions"] = json!(STAGE_PLAN_PERMISSIONS);
        config.to_string()
    }

    /// An end user's config, contract 2.
    fn user_config(&self, auth: Value) -> String {
        json!({ "app": STAGE_PLAN_APP, "appId": self.app_id.to_string(),
                "serverUrl": self.relay.endpoint, "auth": auth, "contract": 2 })
        .to_string()
    }

    async fn stop(self) {
        self.backend.shutdown().await.unwrap();
        drop(self.relay);
        drop(self.server);
    }
}

/// A trusted machine acts as the system, and for a user only once that user
/// has an account; a scoped one acts as its user and has no `for_user`; a
/// credential trusted neither way doesn't open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backend_clients_open_as_the_main_backend_trusts_them() {
    let app = App::start().await;
    let credential = app.config(json!({ "credential": true }));
    let alice_config = app.user_config(json!({ "bearer": true }));
    tokio::task::spawn_blocking(move || {
        let job =
            JazzHandle::open_with_tokens(credential.clone(), Arc::new(Machine("machine-trusted")))
                .expect("open as a trusted backend client");
        assert_eq!(job.backend_client().unwrap().as_deref(), Some("trusted"));
        let state = parse(&job.session_state().unwrap().expect("a session"));
        assert_eq!(state["status"], "signedIn", "{state}");
        assert_eq!(state["user"]["kind"], "system", "{state}");
        assert_eq!(job.account().unwrap(), None);

        // Alice has no account yet: none to act as, and none is made.
        match job.for_user("alice".into(), String::new()) {
            Err(JazzError::NoAccount { user_id, .. }) => assert_eq!(user_id, "alice"),
            other => panic!("no account: {:?}", other.map(|_| ())),
        }
        assert!(matches!(
            job.for_user(String::new(), String::new()),
            Err(JazzError::Refused { .. })
        ));
        assert!(matches!(
            job.for_user("alice".into(), "[1]".into()),
            Err(JazzError::Refused { .. })
        ));

        // Once she has signed in, the job acts as her, under her permissions.
        let alice =
            JazzHandle::open_with_tokens(alice_config, Arc::new(Machine("token-alice"))).unwrap();
        let alice_account = alice.account().unwrap().expect("alice's account");
        // From a plain thread with no Tokio runtime, as Swift, Kotlin or Go
        // call it.
        let as_alice = std::thread::scope(|scope| {
            scope
                .spawn(|| job.for_user("alice".into(), json!({ "org": "acme" }).to_string()))
                .join()
                .unwrap()
        })
        .expect("act for alice");
        assert_eq!(
            as_alice.account().unwrap().as_deref(),
            Some(alice_account.as_str())
        );
        as_alice
            .insert("shows".into(), show(&alice_account))
            .unwrap();
        as_alice.wait_for_sync("global".into()).unwrap();
        // She reads it back as herself, by the app's permissions.
        let hers = parse(&as_alice.all(shows()).unwrap());
        assert_eq!(hers.as_array().map(Vec::len), Some(1), "{hers}");
        // A show chiefed by someone else is refused under her permissions.
        let theirs = as_alice.insert_at("shows".into(), show(nil_uuid()), "global".into());
        assert!(
            matches!(theirs, Err(JazzError::Permission { .. })),
            "{theirs:?}"
        );
        // A handle acting for a user acts for no one else.
        assert!(matches!(
            as_alice.for_user("bob".into(), String::new()),
            Err(JazzError::Refused { .. })
        ));
        // Closing it leaves the job open.
        as_alice.shutdown().unwrap();
        assert!(matches!(as_alice.all(shows()), Err(JazzError::Closed)));
        // The job's own client still runs: it reads and writes.
        job.insert("shows".into(), show(nil_uuid())).unwrap();
        assert!(!titles_of(&job.all(shows()).unwrap()).is_empty());

        let still_alice = job.for_user("alice".into(), String::new()).unwrap();
        job.shutdown_within(10_000).expect("drained");
        assert!(matches!(job.all(shows()), Err(JazzError::Closed)));
        // The parent's close closes its for_user handles: every call, even
        // one answered without the client, fails Closed.
        assert!(matches!(still_alice.account(), Err(JazzError::Closed)));
        assert!(matches!(still_alice.all(shows()), Err(JazzError::Closed)));
        still_alice.shutdown().unwrap();
        alice.shutdown().unwrap();

        // A scoped machine acts as its user.
        let agent =
            JazzHandle::open_with_tokens(credential.clone(), Arc::new(Machine("token-agent")))
                .expect("open as a scoped backend client");
        assert_eq!(agent.backend_client().unwrap().as_deref(), Some("scoped"));
        let state = parse(&agent.session_state().unwrap().unwrap());
        assert_eq!(state["user"]["subject"], "agent", "{state}");
        assert!(matches!(
            agent.for_user("alice".into(), String::new()),
            Err(JazzError::Refused { .. })
        ));
        agent.shutdown_within(10_000).unwrap();

        // Trusted neither way.
        let untrusted =
            JazzHandle::open_with_tokens(credential.clone(), Arc::new(Machine("not-a-credential")));
        assert!(
            matches!(untrusted, Err(JazzError::Refused { .. })),
            "{:?}",
            untrusted.map(|_| ())
        );
        // { "credential": true } stands alone.
        let mut mixed = parse(&credential);
        mixed["auth"]["bearer"] = json!(true);
        assert!(matches!(
            JazzHandle::open_with_tokens(mixed.to_string(), Arc::new(Machine("machine-trusted"))),
            Err(JazzError::Refused { .. })
        ));
        let mut secret = parse(&credential);
        secret["backendSecret"] = json!("s");
        assert!(matches!(
            JazzHandle::open_with_tokens(secret.to_string(), Arc::new(Machine("machine-trusted"))),
            Err(JazzError::Refused { .. })
        ));
        // And the app's permissions, as createJazzClient takes them.
        let mut bare = parse(&credential);
        bare.as_object_mut().unwrap().remove("permissions");
        assert!(matches!(
            JazzHandle::open_with_tokens(bare.to_string(), Arc::new(Machine("machine-trusted"))),
            Err(JazzError::Refused { .. })
        ));
        // A credential needs a provider.
        assert!(matches!(
            JazzHandle::open(credential),
            Err(JazzError::Refused { .. })
        ));
    })
    .await
    .unwrap();
    app.stop().await;
}

fn titles_of(rows: &str) -> Vec<Value> {
    parse(rows).as_array().cloned().unwrap_or_default()
}

fn shows() -> String {
    json!({ "table": "shows" }).to_string()
}

/// Closing with a timeout reports the writes a memory store loses; zero
/// closes at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_with_a_timeout_reports_lost_writes() {
    let app = App::start().await;
    let credential = app.config(json!({ "credential": true }));
    let contract_1 = {
        let mut config = parse(&credential);
        config["contract"] = json!(1);
        config.to_string()
    };
    let refusing = app.relay.refusing.clone();
    let relay = &app.relay;
    let job = {
        let credential = credential.clone();
        tokio::task::spawn_blocking(move || {
            JazzHandle::open_with_tokens(credential, Arc::new(Machine("machine-trusted"))).unwrap()
        })
        .await
        .unwrap()
    };
    relay.refuse("machine-trusted", "account revoked");
    relay.kick();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let job = tokio::task::spawn_blocking(move || {
        job.insert("shows".into(), show(nil_uuid())).unwrap();
        match job.shutdown_within(500) {
            Err(JazzError::Unsynced { pending, .. }) => assert_eq!(pending, 1),
            other => panic!("unsynced: {other:?}"),
        }
        assert!(matches!(job.all(shows()), Err(JazzError::Closed)));
    });
    job.await.unwrap();

    // Contract 1 hosts see it as I/O.
    refusing.lock().unwrap().clear();
    let old = tokio::task::spawn_blocking(move || {
        JazzHandle::open_with_tokens(contract_1, Arc::new(Machine("machine-trusted"))).unwrap()
    })
    .await
    .unwrap();
    relay.refuse("machine-trusted", "account revoked");
    relay.kick();
    tokio::time::sleep(Duration::from_millis(300)).await;
    tokio::task::spawn_blocking(move || {
        old.insert("shows".into(), show(nil_uuid())).unwrap();
        assert!(matches!(
            old.shutdown_within(500),
            Err(JazzError::Io { .. })
        ));
    })
    .await
    .unwrap();

    // Zero closes at once, accepting the loss.
    refusing.lock().unwrap().clear();
    let quick = tokio::task::spawn_blocking(move || {
        JazzHandle::open_with_tokens(credential, Arc::new(Machine("machine-trusted"))).unwrap()
    })
    .await
    .unwrap();
    relay.refuse("machine-trusted", "account revoked");
    relay.kick();
    tokio::time::sleep(Duration::from_millis(300)).await;
    tokio::task::spawn_blocking(move || {
        quick.insert("shows".into(), show(nil_uuid())).unwrap();
        quick.shutdown_within(0).unwrap();
    })
    .await
    .unwrap();
    app.stop().await;
}

// --- The C ABI (Go's path) ---

use jazz_ffi::c_abi::{
    jazz_call, jazz_close, jazz_close_within, jazz_for_user, jazz_open_user, jazz_string_free,
};

unsafe extern "C" {
    fn malloc(size: usize) -> *mut c_void;
}

/// Gives the token in `ctx` (a `&'static str`), `malloc`ed as a C host does.
unsafe extern "C" fn machine_cb(
    ctx: *mut c_void,
    _reason: *const c_char,
    token: *mut *mut c_char,
) -> i32 {
    let text = unsafe { &*(ctx as *const &'static str) }.as_bytes();
    unsafe {
        let out = malloc(text.len() + 1) as *mut u8;
        ptr::copy_nonoverlapping(text.as_ptr(), out, text.len());
        *out.add(text.len()) = 0;
        *token = out as *mut c_char;
    }
    0
}

fn c(text: &str) -> CString {
    CString::new(text).unwrap()
}

fn take(text: *mut c_char) -> Value {
    let copy = unsafe { CStr::from_ptr(text) }.to_str().unwrap().to_owned();
    unsafe { jazz_string_free(text) };
    parse(&copy)
}

fn call(
    handle: *mut jazz_ffi::c_abi::JazzHandle,
    method: &str,
    args: Value,
) -> Result<Value, Value> {
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
        Err(take(err))
    } else {
        Ok(take(out))
    }
}

static TRUSTED: &str = "machine-trusted";
static ALICE: &str = "token-alice";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_c_host_opens_a_trusted_backend_client_and_acts_for_a_user() {
    let app = App::start().await;
    let credential = app.config(json!({ "credential": true }));
    let alice_config = app.user_config(json!({ "bearer": true }));
    tokio::task::spawn_blocking(move || unsafe {
        let mut err = ptr::null_mut();
        let job = jazz_open_user(
            c(&credential).as_ptr(),
            Some(machine_cb),
            &TRUSTED as *const &str as *mut c_void,
            &mut err,
        );
        assert!(!job.is_null(), "{:?}", (!err.is_null()).then(|| take(err)));
        assert_eq!(call(job, "backendClient", json!({})), Ok(json!("trusted")));

        // No account yet: kind noAccount, with the user's id.
        let none = jazz_for_user(job, c("alice").as_ptr(), ptr::null(), &mut err);
        assert!(none.is_null());
        let error = take(err);
        assert_eq!(error["kind"], "noAccount", "{error}");
        assert_eq!(error["userId"], "alice", "{error}");

        let alice = jazz_open_user(
            c(&alice_config).as_ptr(),
            Some(machine_cb),
            &ALICE as *const &str as *mut c_void,
            &mut err,
        );
        assert!(!alice.is_null());
        let account = call(alice, "account", json!({})).unwrap();
        let as_alice = jazz_for_user(
            job,
            c("alice").as_ptr(),
            c(r#"{"org":"acme"}"#).as_ptr(),
            &mut err,
        );
        assert!(
            !as_alice.is_null(),
            "{:?}",
            (!err.is_null()).then(|| take(err))
        );
        assert_eq!(call(as_alice, "account", json!({})), Ok(account.clone()));
        // The trusted client it came from.
        assert_eq!(
            call(as_alice, "backendClient", json!({})),
            Ok(json!("trusted"))
        );
        call(
            as_alice,
            "insert",
            json!({ "table": "shows", "row": parse(&show(account.as_str().unwrap())) }),
        )
        .unwrap();
        // A transaction id works only on the handle that began it: a user's
        // handle can't write in the system's transaction, nor the system in
        // the user's.
        let job_tx = call(job, "txBegin", json!({})).unwrap()["tx"].clone();
        let alice_tx = call(as_alice, "txBegin", json!({})).unwrap()["tx"].clone();
        let row = parse(&show(nil_uuid()));
        for (handle, tx) in [(as_alice, &job_tx), (job, &alice_tx)] {
            let escaped = call(
                handle,
                "insert",
                json!({ "table": "shows", "row": row, "tx": tx }),
            );
            assert_eq!(escaped.unwrap_err()["kind"], "refused");
            for method in ["txCommit", "txRollback"] {
                let escaped = call(handle, method, json!({ "tx": tx }));
                assert_eq!(escaped.unwrap_err()["kind"], "refused", "{method}");
            }
        }
        // Each still works on its own handle; alice's is left open, for her
        // handle's close to roll back.
        call(
            as_alice,
            "insert",
            json!({ "table": "shows",
            "row": parse(&show(account.as_str().unwrap())), "tx": alice_tx }),
        )
        .unwrap();
        call(job, "txRollback", json!({ "tx": job_tx })).unwrap();
        // An upload as alice reads back her row, as she sees it.
        let mut fields = parse(&show(account.as_str().unwrap()));
        fields.as_object_mut().unwrap().remove("name");
        let upload = call(
            as_alice,
            "uploadBegin",
            json!({ "table": "shows", "column": "name", "row": fields }),
        )
        .unwrap()["upload"]
            .clone();
        call(
            as_alice,
            "uploadPush",
            json!({ "upload": upload, "bytes": "R2FsYQ==" }),
        )
        .unwrap();
        let uploaded = call(as_alice, "uploadFinish", json!({ "upload": upload }))
            .expect("the uploaded row, read back as alice");
        assert_eq!(uploaded["name"], "Gala", "{uploaded}");
        // Written once: nothing for a host to retry.
        let galas = call(
            as_alice,
            "all",
            json!({ "query": { "table": "shows",
            "where": [{ "column": "name", "op": "eq", "value": "Gala" }] } }),
        )
        .unwrap();
        assert_eq!(galas.as_array().map(Vec::len), Some(1), "{galas}");
        // So does an upsert.
        let id = uploaded["id"].as_str().unwrap();
        let upserted = call(
            as_alice,
            "upsert",
            json!({ "table": "shows", "id": id, "row": { "venue": "Side Hall" } }),
        )
        .expect("the upserted row, read back as alice");
        assert_eq!(upserted["venue"], "Side Hall", "{upserted}");
        // An upload id works only on the handle that began it.
        let mut row = parse(&show(nil_uuid()));
        row.as_object_mut().unwrap().remove("name");
        let job_upload = call(
            job,
            "uploadBegin",
            json!({ "table": "shows", "column": "name", "row": row }),
        )
        .unwrap()["upload"]
            .clone();
        for (method, args) in [
            (
                "uploadPush",
                json!({ "upload": job_upload, "bytes": "R2FsYQ==" }),
            ),
            ("uploadFinish", json!({ "upload": job_upload })),
            ("uploadAbort", json!({ "upload": job_upload })),
        ] {
            let escaped = call(as_alice, method, args);
            assert_eq!(escaped.unwrap_err()["kind"], "refused", "{method}");
        }
        call(job, "uploadAbort", json!({ "upload": job_upload })).unwrap();
        // A scoped user's handle can't act for others.
        let bob = jazz_for_user(as_alice, c("bob").as_ptr(), ptr::null(), &mut err);
        assert!(bob.is_null());
        assert_eq!(take(err)["kind"], "refused");
        // The job's own transaction, open across her handle's close.
        let job_open = call(job, "txBegin", json!({})).unwrap()["tx"].clone();
        call(
            job,
            "insert",
            json!({ "table": "shows", "row": parse(&show(nil_uuid())), "tx": job_open }),
        )
        .unwrap();
        jazz_close(as_alice);
        // It survives: closing her handle ends only her handle's own.
        call(job, "txCommit", json!({ "tx": job_open })).expect("the job's transaction");
        // Her handle's close rolled back her open transaction: the job can't
        // reach it either way.
        assert_eq!(
            call(job, "txCommit", json!({ "tx": alice_tx })).unwrap_err()["kind"],
            "refused"
        );
        // The job goes on, then drains and is freed.
        assert!(
            !call(job, "all", json!({ "query": { "table": "shows" } }))
                .unwrap()
                .as_array()
                .unwrap()
                .is_empty()
        );
        let closed = jazz_close_within(job, 10_000, &mut err);
        assert!(err.is_null(), "{}", take(err));
        assert_eq!(closed, 0);
        jazz_close(alice);
        // NULL is closed at once.
        assert_eq!(jazz_close_within(ptr::null_mut(), 0, &mut err), 0);
    })
    .await
    .unwrap();
    app.stop().await;
}

/// Counts its calls; gives `machine-trusted`.
unsafe extern "C" fn counting_cb(
    ctx: *mut c_void,
    reason: *const c_char,
    token: *mut *mut c_char,
) -> i32 {
    let calls = unsafe { &*(ctx as *const std::sync::atomic::AtomicUsize) };
    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let trusted = &TRUSTED as *const &str as *mut c_void;
    unsafe { machine_cb(trusted, reason, token) }
}

/// While `jazz_close_within` drains, the client may reconnect, and the
/// token callback still answers it; it stops once the call returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_token_callback_answers_while_closing_drains() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let app = App::start().await;
    let credential = app.config(json!({ "credential": true }));
    let calls: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));
    struct Handle(*mut jazz_ffi::c_abi::JazzHandle);
    unsafe impl Send for Handle {}
    let job = tokio::task::spawn_blocking(move || unsafe {
        let mut err = ptr::null_mut();
        let job = jazz_open_user(
            c(&credential).as_ptr(),
            Some(counting_cb),
            calls as *const AtomicUsize as *mut c_void,
            &mut err,
        );
        assert!(!job.is_null());
        Handle(job)
    })
    .await
    .unwrap();
    // The main backend stops admitting the machine: a write waits here.
    app.relay.refuse(TRUSTED, "token expired");
    app.relay.kick();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let job = tokio::task::spawn_blocking(move || {
        let job = job;
        call(
            job.0,
            "insert",
            json!({ "table": "shows", "row": parse(&show(nil_uuid())) }),
        )
        .unwrap();
        job
    })
    .await
    .unwrap();
    let before = calls.load(Ordering::SeqCst);
    let closing = tokio::task::spawn_blocking(move || unsafe {
        let job = job;
        let mut err = ptr::null_mut();
        let closed = jazz_close_within(job.0, 15_000, &mut err);
        (closed, (!err.is_null()).then(|| take(err)))
    });
    // Admitted again while it drains: reconnecting asks for a token.
    tokio::time::sleep(Duration::from_millis(500)).await;
    app.relay.refusing.lock().unwrap().clear();
    app.relay.kick();
    let (closed, error) = closing.await.unwrap();
    assert_eq!((closed, error), (0, None), "drained through a fresh token");
    assert!(
        calls.load(Ordering::SeqCst) > before,
        "asked while draining"
    );
    let after = calls.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        after,
        "never after it returns"
    );
    app.stop().await;
}

/// A trusted client whose permissions bundle isn't the one the main backend
/// serves opens, but acts for no user: kind "bundleMismatch", with both
/// bundle ids.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trusted_client_with_other_permissions_acts_for_no_user() {
    let app = App::start().await;
    let mut laxer: Value = serde_json::from_str(STAGE_PLAN_PERMISSIONS).unwrap();
    laxer["tables"]["activity"]["read"] = json!("true");
    let mut config = parse(&app.config(json!({ "credential": true })));
    config["permissions"] = json!(laxer.to_string());
    let config = config.to_string();
    let served = app.backend.bundle().unwrap().to_owned();
    tokio::task::spawn_blocking(move || unsafe {
        let mut err = ptr::null_mut();
        let job = jazz_open_user(
            c(&config).as_ptr(),
            Some(machine_cb),
            &TRUSTED as *const &str as *mut c_void,
            &mut err,
        );
        assert!(!job.is_null(), "it opens: only acting for users is refused");
        let none = jazz_for_user(job, c("alice").as_ptr(), ptr::null(), &mut err);
        assert!(none.is_null());
        let error = take(err);
        assert_eq!(error["kind"], "bundleMismatch", "{error}");
        assert_eq!(error["served"], served.as_str(), "{error}");
        assert!(
            error["own"].is_string() && error["own"] != error["served"],
            "{error}"
        );
        jazz_close(job);
    })
    .await
    .unwrap();
    app.stop().await;
}
