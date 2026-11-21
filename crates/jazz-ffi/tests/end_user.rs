//! jazz-ffi v2: end users — a device account or the app's bearer, syncing
//! through the app's Jazz endpoint (jazz-app-support's `relay/` stand-in for
//! `createJazz`), with session and sync states.

mod common;
#[allow(dead_code)]
#[path = "../../jazz-app/tests/relay/mod.rs"]
mod relay;

use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use common::*;
use jazz_ffi::{JazzHandle, StateListener, TokenProvider, generate_device_key};
use serde_json::{Value, json};

struct Tokens(Mutex<Vec<String>>, &'static str);
impl TokenProvider for Tokens {
    fn token(&self, reason: String) -> Result<Option<String>, jazz_ffi::JazzError> {
        self.0.lock().unwrap().push(reason.clone());
        // A refresh that fails (a network blip) must not sign alice out.
        // A provider that panics is a failed fetch, not a sign-out.
        if reason == "refresh" && self.1 == "panicky" {
            panic!("the provider broke");
        }
        if reason == "refresh" && self.1 == "flaky" {
            return Err(jazz_ffi::JazzError::Io {
                reason: "offline".into(),
            });
        }
        Ok(Some(format!(
            "token-{}",
            if self.1 == "flaky" { "alice" } else { self.1 }
        )))
    }
}

struct States(Mutex<mpsc::Sender<Value>>);
impl StateListener for States {
    fn on_change(&self, state_json: String) {
        let _ = self.0.lock().unwrap().send(parse(&state_json));
    }
}

/// carol signs up with a new device account and alice signs in with the
/// app's bearer, each through the app's endpoint under StagePlan's
/// permissions: each is signed in as herself, writes a show she chiefs and
/// sees it reach the server; alice's session is watched, refreshed (her
/// provider is asked for a fresh token) and signed out.
///
/// ```
/// carol (device key) ─┐
///                     ├─► app endpoint (relay) ──► core
/// alice (bearer)  ────┘          └─ authenticate("token-alice")
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn end_users_sign_in_and_sync_through_the_app() {
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
    let (app, endpoint) = (app_id.to_string(), relay.endpoint.clone());
    let config = move |auth: Value| {
        json!({ "app": STAGE_PLAN_APP, "appId": app, "serverUrl": endpoint,
                "auth": auth, "contract": 2 })
        .to_string()
    };
    let (carol, alice) = (
        config(json!({ "deviceKey": generate_device_key().unwrap() })),
        config(json!({ "bearer": true })),
    );

    let result = tokio::task::spawn_blocking(move || {
        let carol = JazzHandle::open(carol).unwrap();
        let state = parse(&carol.session_state().unwrap().expect("a session"));
        assert_eq!(state["status"], "signedIn", "{state}");
        assert_eq!(state["user"]["kind"], "device");
        let account = state["user"]["account"].as_str().unwrap().to_owned();
        carol.insert("shows".into(), show(&account)).unwrap();
        carol.wait_for_sync("global".into()).unwrap();
        let status = parse(&carol.sync_status().unwrap());
        assert!(status.get("link").is_some(), "{status}");
        assert!(status.get("pendingUploads").is_some(), "{status}");

        // carol links her device account to her account with the app's
        // sign-in: the session becomes that user's, on her device account.
        // From a plain thread with no tokio runtime, as Swift or Go call it.
        let user = std::thread::scope(|scope| {
            scope.spawn(|| carol.link("token-carol".into())).join().unwrap()
        });
        let user = parse(&user.unwrap());
        assert_eq!(user["kind"], "backend", "{user}");
        assert_eq!(user["account"], account);
        let state = parse(&carol.session_state().unwrap().unwrap());
        assert_eq!(state["user"]["subject"], "carol", "{state}");

        let asked = Arc::new(Tokens(Mutex::new(Vec::new()), "alice"));
        let alice_config = alice.clone();
        let alice = JazzHandle::open_with_tokens(alice, asked.clone()).unwrap();
        let state = parse(&alice.session_state().unwrap().unwrap());
        assert_eq!(state["status"], "signedIn", "{state}");
        assert_eq!(state["user"]["subject"], "alice");
        assert_eq!(asked.0.lock().unwrap().first().map(String::as_str), Some("connect"));
        let (sender, states) = mpsc::channel();
        let watch = alice.watch_session(Arc::new(States(Mutex::new(sender)))).unwrap();
        assert_eq!(states.recv_timeout(Duration::from_secs(5)).unwrap()["status"], "signedIn");
        let account = state["user"]["account"].as_str().unwrap().to_owned();
        alice.insert_at("shows".into(), show(&account), "global".into()).unwrap();

        alice.refresh().unwrap();
        assert!(asked.0.lock().unwrap().iter().any(|reason| reason == "refresh"));
        // A provider whose refresh fails (a network blip) keeps alice
        // signed in: the failure is the call's, not a sign-out.
        let flaky = JazzHandle::open_with_tokens(
            alice_config.clone(),
            Arc::new(Tokens(Mutex::new(Vec::new()), "flaky")),
        )
        .unwrap();
        assert!(flaky.refresh().is_err());
        let state = parse(&flaky.session_state().unwrap().unwrap());
        assert_eq!(state["status"], "signedIn", "{state}");
        flaky.all(json!({ "table": "shows" }).to_string()).unwrap();
        flaky.shutdown().unwrap();
        let panicky = JazzHandle::open_with_tokens(
            alice_config.clone(),
            Arc::new(Tokens(Mutex::new(Vec::new()), "panicky")),
        )
        .unwrap();
        assert!(panicky.refresh().is_err());
        let state = parse(&panicky.session_state().unwrap().unwrap());
        assert_eq!(state["status"], "signedIn", "{state}");
        panicky.shutdown().unwrap();

        alice.sign_out().unwrap();
        // The watch reports the sign-out. A shared CI host can take more
        // than a couple of seconds to deliver it, so allow ten.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut last = Value::Null;
        while let Ok(state) =
            states.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
        {
            last = state;
            if last["status"] == "signedOut" {
                break;
            }
        }
        assert_eq!(last["status"], "signedOut", "{last}");
        watch.cancel();
        assert!(alice.all(json!({ "table": "shows" }).to_string()).is_err());
        carol.shutdown().unwrap();

        // Bearer auth without a provider, and a server with neither auth
        // nor permissions, are refused by name.
        let no_provider = JazzHandle::open(config(json!({ "bearer": true })));
        assert!(format!("{no_provider:?}").contains("token provider"));
        let neither = JazzHandle::open(
            json!({ "app": STAGE_PLAN_APP, "appId": nil_uuid(), "serverUrl": "http://localhost:1" }).to_string(),
        );
        assert!(format!("{neither:?}").contains("\\\"auth\\\""), "{neither:?}");
    })
    .await;
    backend.shutdown().await.unwrap();
    server.shutdown().await;
    result.unwrap();
}
