//! Refusals that cross a derived handle's close, on the client's thread.

#[allow(dead_code)]
#[path = "../../jazz-app/tests/relay/mod.rs"]
mod relay;

use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use super::*;

const APP: &str = include_str!("../../../examples/stage-plan/apps/react-localfirst/jazz.app.json");
const PERMISSIONS: &str =
    include_str!("../../../examples/stage-plan/apps/react-localfirst/jazz.permissions.json");

/// A backend client and a handle of it acting for a user whose one write
/// the server has refused, as the client already knows.
struct Refused {
    job: Arc<Core>,
    user: Arc<Core>,
    _relay: relay::Relay,
    _server: jazz_server::JazzServer,
    _backend: Jazz,
    _runtime: tokio::runtime::Runtime,
}

impl Refused {
    fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let app_id = AppId::random();
        let (server, backend, relay) = runtime.block_on(async {
            let server = jazz_server::JazzServer::builder()
                .with_app_id(app_id)
                .start()
                .await
                .unwrap();
            let files = AppFiles::new(APP.as_bytes().to_vec(), None)
                .with_permissions(PERMISSIONS.as_bytes().to_vec());
            let backend = Jazz::open(
                files,
                jazz_app_support::Config::new(app_id)
                    .server(server.base_url())
                    .backend(jazz_app_support::Backend::new(
                        server.backend_secret(),
                        server.admin_secret(),
                        "svc",
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
        let open = |token: &'static str, auth: Json, permissions: Option<&str>| {
            let mut config = serde_json::json!({ "app": APP, "appId": app_id.to_string(),
                "serverUrl": relay.endpoint, "auth": auth, "contract": 2 });
            if let Some(permissions) = permissions {
                config["permissions"] = permissions.into();
            }
            let tokens =
                move |_: jazz_app_support::TokenReason| async move { Ok(Some(token.to_owned())) };
            Core::open_with_tokens(&config.to_string(), Some(Arc::new(tokens))).unwrap()
        };
        // Signed in once, so she has an account to act as.
        open("token-alice", serde_json::json!({ "bearer": true }), None)
            .close()
            .unwrap();
        let job = open(
            "machine-trusted",
            serde_json::json!({ "credential": true }),
            Some(PERMISSIONS),
        );
        let user = job.for_user("alice", "").unwrap();
        // She may not make someone else the chief: the server refuses it.
        let show = serde_json::json!({ "name": "Spring Gala", "venue": "Main Hall",
            "date": "2026-04-01", "doors": "19:00",
            "chiefAccount": "00000000-0000-0000-0000-000000000000" });
        user.insert("shows", &show.to_string(), &Opts::default())
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        while convert::parse(&job.sync_status().unwrap(), "the status").unwrap()["rejected"] != 1 {
            assert!(Instant::now() < deadline, "the write is never refused");
            std::thread::sleep(Duration::from_millis(20));
        }
        Self {
            job: Arc::new(job),
            user: Arc::new(user),
            _relay: relay,
            _server: server,
            _backend: backend,
            _runtime: runtime,
        }
    }
}

/// The refusal is there when the handle closes, but the client's thread is
/// busy: neither the call nor the parent's next waitForSync may lose it.
#[test]
fn a_refusal_ready_as_a_derived_handle_closes_is_not_lost() {
    let app = Refused::new();
    let (release, released) = mpsc::channel::<()>();
    let (holding, held) = mpsc::channel::<()>();
    let jazz = app.job.jazz.clone();
    let holder = std::thread::spawn(move || {
        let _ = block_on(jazz.with_client(move |_| async move {
            let _ = holding.send(());
            let _ = released.recv_timeout(Duration::from_secs(10));
            Ok(())
        }));
    });
    held.recv().unwrap();
    let user = app.user.clone();
    let waiting = std::thread::spawn(move || user.wait_for_sync(DurabilityTier::GlobalServer));
    std::thread::sleep(Duration::from_millis(200));
    let user = app.user.clone();
    let closing = std::thread::spawn(move || user.close());
    std::thread::sleep(Duration::from_millis(200));
    release.send(()).unwrap();
    holder.join().unwrap();
    closing.join().unwrap().unwrap();

    let waited = waiting.join().unwrap();
    let checked = app.job.wait_for_sync(DurabilityTier::GlobalServer);
    assert!(
        matches!(checked, Err(FfiError::Permission(_))),
        "the parent's check: {checked:?}; the call: {waited:?}"
    );
    assert!(
        matches!(waited, Err(FfiError::Permission(_))),
        "the call had its answer: {waited:?}"
    );
    app.job.close().unwrap();
}

/// The same for a global write's confirmation.
#[test]
fn a_global_writes_refusal_is_checked_again_once_closed() {
    let app = Refused::new();
    let writes = block_on(app.job.jazz.with_client(|_| async {
        Ok(UNSYNCED.with(|unsynced| std::mem::take(&mut *unsynced.borrow_mut())))
    }))
    .unwrap();
    assert_eq!(writes.len(), 1);
    app.user.open.closed.close();
    let write = writes[0];
    let settled = block_on(app.user.jazz.with_client(move |client| async move {
        settle(&client, Some(write), Some(DurabilityTier::GlobalServer)).await
    }));
    assert!(settled.is_err(), "{settled:?}");
    let checked = app.job.wait_for_sync(DurabilityTier::GlobalServer);
    assert!(
        matches!(checked, Err(FfiError::Permission(_))),
        "{checked:?}"
    );
    app.job.close().unwrap();
}
