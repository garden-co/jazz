//! An end user's client: signed in with a device account or the app's
//! bearer, syncing through the app's Jazz endpoint (a stand-in for
//! `createJazz`, in `relay/`) with an in-process core and the app's backend.

#[path = "bindings/todos.rs"]
mod bindings;
mod relay;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bindings::{NewTodo, Todo, todos};
use futures::StreamExt;
use jazz_app::{
    AppFiles, AppId, Auth, Backend, Config, DeviceKey, Error, Jazz, Link, ReadTier, SessionState,
    Storage, TokenReason, UserKind,
};
use jazz_server::JazzServer;

/// The app's files with its permissions, as a backend client opens them.
fn files() -> AppFiles {
    AppFiles::read(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/todos")).unwrap()
}

fn client_files() -> AppFiles {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/todos");
    AppFiles::new(std::fs::read(format!("{dir}/jazz.app.json")).unwrap(), None)
}

struct App {
    app_id: AppId,
    server: JazzServer,
    backend: Jazz,
    relay: relay::Relay,
}

impl App {
    async fn start() -> Self {
        Self::start_for(AppId::random()).await
    }

    /// A fresh server, store and backend for `app_id`: a new history, as a
    /// recreated server store has.
    async fn start_for(app_id: AppId) -> Self {
        let server = JazzServer::builder()
            .with_app_id(app_id)
            .start()
            .await
            .expect("start a Jazz server");
        let permissions = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/todos/jazz.permissions.json"
        ))
        .unwrap();
        let backend = bindings::open(
            Config::new(app_id).server(server.base_url()).backend(
                Backend::new(
                    server.backend_secret(),
                    server.admin_secret(),
                    "todo-service",
                )
                .permissions(permissions),
            ),
        )
        .await
        .expect("open the app's backend");
        let relay = relay::start(
            &server.base_url(),
            app_id,
            server.backend_secret(),
            server.admin_secret(),
            backend.bundle().unwrap(),
        )
        .await;
        Self {
            app_id,
            server,
            backend,
            relay,
        }
    }

    fn user(&self, auth: Auth) -> Config {
        Config::new(self.app_id)
            .server(&self.relay.endpoint)
            .user(auth)
    }

    async fn stop(self) {
        self.backend.shutdown().await.unwrap();
        self.server.shutdown().await;
    }
}

/// Every todo `jazz` reads from the server, once there are `count`.
async fn remote_todos(jazz: &Jazz, count: usize) -> Vec<Todo> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = jazz
                .all_with(todos::query(), ReadTier::Remote)
                .await
                .unwrap();
            if rows.len() >= count {
                break rows;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the rows reach the server")
}

async fn connected(jazz: &Jazz) {
    let mut changes = jazz.sync_changes();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(status) = changes.next().await {
            if status.link == Link::Connected {
                return;
            }
        }
    })
    .await
    .expect("the client connects");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_device_account_syncs_across_its_devices() {
    let app = App::start().await;
    let key = DeviceKey::generate().unwrap();
    let dir = tempfile::tempdir().unwrap();

    let phone = Jazz::open(
        client_files(),
        app.user(Auth::Device(key.clone()))
            .storage(Storage::Dir(dir.path().into())),
    )
    .await
    .expect("open as a device account");
    let session = phone.session().expect("an end user's session").clone();
    let SessionState::SignedIn { user } = session.state() else {
        panic!("signed in: {:?}", session.state());
    };
    assert_eq!(user.kind, UserKind::Device);
    assert_eq!(user.account, key.account(app.app_id));
    assert_eq!(user.subject, key.user_id());
    assert_eq!(phone.account(), Some(user.account));
    connected(&phone).await;

    let written = phone
        .insert(NewTodo::new("From the phone", false))
        .await
        .unwrap();

    // The same key, read back as the TypeScript client keeps it, on another
    // device: the same account, and the phone's row.
    let laptop_key: DeviceKey = key.to_string().parse().unwrap();
    let laptop = Jazz::open(client_files(), app.user(Auth::Device(laptop_key)))
        .await
        .unwrap();
    assert_eq!(laptop.account(), phone.account());
    assert_eq!(remote_todos(&laptop, 1).await, vec![written.clone()]);
    // And the app's backend sees it, by its device account.
    assert_eq!(remote_todos(&app.backend, 1).await, vec![written]);
    let admitted = app.relay.admitted.lock().unwrap().clone();
    assert!(admitted.len() >= 2, "{admitted:?}");
    assert!(admitted.iter().all(|who| who.starts_with("device:")));

    // Signing out stops the client; its store stays for the next sign-in.
    let mut states = session.changes();
    phone.sign_out().await.unwrap();
    assert_eq!(session.state(), SessionState::SignedOut);
    let last = tokio::time::timeout(Duration::from_secs(5), async {
        let mut last = None;
        while let Some(state) = states.next().await {
            last = Some(state);
        }
        last
    })
    .await
    .expect("the session's changes end with the client");
    assert_eq!(last, Some(SessionState::SignedOut));
    assert!(matches!(
        phone.all(todos::query()).await,
        Err(Error::Stopped)
    ));

    let reopened = Jazz::open(
        client_files(),
        app.user(Auth::Device(key))
            .storage(Storage::Dir(dir.path().into())),
    )
    .await
    .unwrap();
    assert_eq!(reopened.all(todos::query()).await.unwrap().len(), 1);

    reopened.shutdown().await.unwrap();
    laptop.shutdown().await.unwrap();
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bearer_signs_in_the_user_the_backend_names() {
    let app = App::start().await;
    let token = Arc::new(Mutex::new(Some("token-alice".to_owned())));
    let fresh = Arc::new(AtomicUsize::new(0));
    let provider = {
        let (token, fresh) = (token.clone(), fresh.clone());
        move |reason: TokenReason| {
            let token = token.lock().unwrap().clone();
            if reason == TokenReason::Refresh {
                fresh.fetch_add(1, Ordering::SeqCst);
            }
            async move { Ok(token) }
        }
    };
    let alice = Jazz::open(client_files(), app.user(Auth::bearer(provider)))
        .await
        .expect("open as the bearer's user");
    let session = alice.session().unwrap().clone();
    let SessionState::SignedIn { user } = session.state() else {
        panic!("signed in: {:?}", session.state());
    };
    assert_eq!(user.kind, UserKind::Backend);
    assert_eq!(
        (user.issuer.as_str(), user.subject.as_str()),
        ("jazz:backend", "alice")
    );
    connected(&alice).await;

    let first = alice.insert(NewTodo::new("Alice's", false)).await.unwrap();
    assert_eq!(remote_todos(&app.backend, 1).await, vec![first]);

    // A refresh asks the provider for a fresh token and reconnects with it.
    session.refresh().await.unwrap();
    assert_eq!(fresh.load(Ordering::SeqCst), 1);
    connected(&alice).await;
    alice
        .insert(NewTodo::new("After the refresh", false))
        .await
        .unwrap();
    assert_eq!(remote_todos(&app.backend, 2).await.len(), 2);
    assert!(
        app.relay
            .admitted
            .lock()
            .unwrap()
            .iter()
            .filter(|who| *who == "user:alice")
            .count()
            >= 2
    );

    // The app signs Alice out: the provider names no one. The client stops,
    // so nothing syncs as her any more.
    *token.lock().unwrap() = None;
    session.refresh().await.unwrap();
    assert_eq!(session.state(), SessionState::SignedOut);
    assert!(matches!(
        alice.insert(NewTodo::new("After signing out", false)).await,
        Err(Error::Stopped)
    ));
    assert!(matches!(
        alice.shutdown().await,
        Err(Error::Stopped) | Ok(())
    ));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(remote_todos(&app.backend, 2).await.len(), 2);

    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_end_user_needs_someone_signed_in_and_the_apps_endpoint() {
    let app = App::start().await;
    let nobody = Jazz::open(
        client_files(),
        app.user(Auth::bearer(|_| async { Ok(None) })),
    )
    .await;
    assert!(matches!(nobody, Err(Error::SignedOut)), "{nobody:?}");

    let unknown = Jazz::open(
        client_files(),
        app.user(Auth::bearer(|_| async {
            Ok(Some("not-a-token".to_owned()))
        })),
    )
    .await;
    assert!(matches!(unknown, Err(Error::SignedOut)), "{unknown:?}");

    // The core is not the app's endpoint: it has no backend sign-in.
    let at_core = Jazz::open(
        client_files(),
        Config::new(app.app_id)
            .server(app.server.base_url())
            .user(Auth::bearer(|_| async {
                Ok(Some("token-alice".to_owned()))
            })),
    )
    .await;
    assert!(matches!(at_core, Err(Error::Server(_))), "{at_core:?}");

    let plain = Jazz::open(
        client_files(),
        Config::new(app.app_id)
            .server("http://todos.example/jazz")
            .user(Auth::Device(DeviceKey::generate().unwrap())),
    )
    .await;
    assert!(matches!(plain, Err(Error::Config(_))), "{plain:?}");
    let plain_socket = Jazz::open(
        client_files(),
        Config::new(app.app_id)
            .server("ws://todos.example/jazz")
            .user(Auth::Device(DeviceKey::generate().unwrap())),
    )
    .await;
    assert!(
        matches!(plain_socket, Err(Error::Config(_))),
        "{plain_socket:?}"
    );

    let local = Jazz::open(
        client_files(),
        Config::new(app.app_id).user(Auth::Device(DeviceKey::generate().unwrap())),
    )
    .await;
    assert!(matches!(local, Err(Error::Config(_))), "{local:?}");
    app.stop().await;
}

/// The status once `done` holds for it.
async fn sync_until(
    jazz: &Jazz,
    done: impl Fn(&jazz_app::SyncStatus) -> bool,
) -> jazz_app::SyncStatus {
    let mut changes = jazz.sync_changes();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(status) = changes.next().await {
            if done(&status) {
                return status;
            }
        }
        panic!("the status stream ended");
    })
    .await
    .expect("the sync status gets there")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_status_counts_uploads_until_the_server_has_them() {
    let app = App::start().await;
    let jazz = Jazz::open(
        client_files(),
        app.user(Auth::Device(DeviceKey::generate().unwrap())),
    )
    .await
    .unwrap();
    jazz.insert(NewTodo::new("Uploaded", false)).await.unwrap();
    let synced = sync_until(&jazz, |status| status.synced).await;
    assert_eq!(synced.link, Link::Connected);
    assert_eq!(synced.pending_uploads, 0);
    assert_eq!(synced.oldest_pending_at, None);
    let json = serde_json::to_value(&synced).unwrap();
    assert_eq!(json["link"], serde_json::json!({ "state": "connected" }));
    assert_eq!(json["pendingUploads"], 0);
    assert_eq!(json["synced"], true);
    jazz.shutdown().await.unwrap();

    // Nobody answers at this endpoint: the client opens offline, and its
    // writes wait.
    let unused = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/jazz", unused.local_addr().unwrap());
    drop(unused);
    let offline = Jazz::open(
        client_files(),
        Config::new(app.app_id)
            .server(endpoint)
            .user(Auth::Device(DeviceKey::generate().unwrap())),
    )
    .await
    .expect("an unreachable server only means offline");
    offline
        .insert(NewTodo::new("Waiting", false))
        .await
        .unwrap();
    let waiting = sync_until(&offline, |status| status.pending_uploads == 1).await;
    assert!(!waiting.synced);
    assert!(waiting.oldest_pending_at.is_some());
    assert_ne!(waiting.link, Link::Connected);
    offline.shutdown().await.unwrap();
    app.stop().await;
}

/// The session's state once `done` holds for it.
async fn session_until(
    session: &jazz_app::Session,
    done: impl Fn(&SessionState) -> bool,
) -> SessionState {
    let mut changes = session.changes();
    tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(state) = changes.next().await {
            if done(&state) {
                return state;
            }
        }
        panic!("the session's changes ended");
    })
    .await
    .expect("the session gets there")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_bearer_is_renewed_expires_and_recovers_but_a_revoked_one_is_not() {
    use jazz_app::AuthFailure;
    let app = App::start().await;
    let token = Arc::new(Mutex::new("token-alice".to_owned()));
    let asked = Arc::new(Mutex::new(Vec::new()));
    let provider = {
        let (token, asked) = (token.clone(), asked.clone());
        move |reason: TokenReason| {
            asked.lock().unwrap().push(reason);
            let token = token.lock().unwrap().clone();
            async move { Ok(Some(token)) }
        }
    };
    let alice = Jazz::open(client_files(), app.user(Auth::bearer(provider)))
        .await
        .unwrap();
    let session = alice.session().unwrap().clone();
    connected(&alice).await;

    // The backend refuses her bearer: the session asks for a new one at
    // once, and reads expired when that one is refused too.
    app.relay.refuse("token-alice", "token expired");
    app.relay.kick();
    let expired = session_until(&session, |state| {
        matches!(state, SessionState::Expired { .. })
    })
    .await;
    assert!(matches!(
        expired,
        SessionState::Expired {
            reason: AuthFailure::Expired,
            ..
        }
    ));
    assert!(asked.lock().unwrap().contains(&TokenReason::Expired));

    // Her auth gets a new token: a renewal retried with backoff picks it up.
    *token.lock().unwrap() = "token-alice.2".to_owned();
    session_until(&session, |state| {
        matches!(state, SessionState::SignedIn { .. })
    })
    .await;
    alice
        .insert(NewTodo::new("Back again", false))
        .await
        .unwrap();
    assert_eq!(remote_todos(&app.backend, 1).await.len(), 1);

    // A revoked account no new token fixes: it is not asked for one.
    app.relay.refuse("token-alice.2", "account revoked");
    let before = asked.lock().unwrap().len();
    app.relay.kick();
    let revoked = session_until(&session, |state| {
        matches!(state, SessionState::Expired { .. })
    })
    .await;
    assert!(matches!(
        revoked,
        SessionState::Expired {
            reason: AuthFailure::Revoked,
            ..
        }
    ));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(asked.lock().unwrap().len(), before);

    alice.shutdown().await.unwrap();
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_provider_does_not_hold_up_signing_out() {
    let app = App::start().await;
    let slow = Arc::new(AtomicUsize::new(0));
    let provider = {
        let slow = slow.clone();
        move |reason: TokenReason| {
            let wait = reason == TokenReason::Refresh;
            if wait {
                slow.fetch_add(1, Ordering::SeqCst);
            }
            async move {
                if wait {
                    // An interactive sign-in nobody completes.
                    std::future::pending::<()>().await;
                }
                Ok(Some("token-dana".to_owned()))
            }
        }
    };
    let dana = Jazz::open(client_files(), app.user(Auth::bearer(provider)))
        .await
        .unwrap();
    let session = dana.session().unwrap().clone();
    let refreshing = tokio::spawn({
        let session = session.clone();
        async move { session.refresh().await }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while slow.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), dana.sign_out())
        .await
        .expect("signing out does not wait for the provider")
        .unwrap();
    assert_eq!(session.state(), SessionState::SignedOut);
    // The refresh that waited ends with the session.
    assert!(
        tokio::time::timeout(Duration::from_secs(5), refreshing)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    app.stop().await;
}

fn machine(token: &'static str) -> impl jazz_app::TokenProvider + 'static {
    move |_: TokenReason| async move { Ok(Some(token.to_owned())) }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trusted_backend_client_acts_as_the_system_and_for_users_with_accounts() {
    use jazz_app::BackendClientKind;
    let app = App::start().await;
    let job = Jazz::open(
        files(),
        Config::new(app.app_id)
            .server(&app.relay.endpoint)
            .credential(machine("machine-trusted")),
    )
    .await
    .expect("open as a trusted backend client");
    assert_eq!(job.backend_client(), Some(BackendClientKind::Trusted));
    let Some(SessionState::SignedIn { user }) = job.session().map(|s| s.state()) else {
        panic!("signed in as the system");
    };
    assert_eq!(user.kind, UserKind::System);
    assert_eq!(job.account(), None);
    connected(&job).await;
    let imported = job.insert(NewTodo::new("Imported", false)).await.unwrap();
    assert_eq!(remote_todos(&app.backend, 1).await, vec![imported]);
    assert!(
        app.relay
            .admitted
            .lock()
            .unwrap()
            .contains(&"trusted".to_owned())
    );

    // Alice has not signed in yet: no account to act as, and none is made.
    let none = job.for_user("alice", serde_json::Value::Null).await;
    assert!(
        matches!(&none, Err(Error::NoAccount(user)) if user == "alice"),
        "{none:?}"
    );

    // Once she has, the job acts as her: her identity, her permissions.
    let alice = Jazz::open(
        client_files(),
        app.user(Auth::bearer(|_| async {
            Ok(Some("token-alice".to_owned()))
        })),
    )
    .await
    .unwrap();
    let as_alice = job
        .for_user("alice", serde_json::json!({ "org": "acme" }))
        .await
        .expect("act for alice");
    assert!(as_alice.acts_for_user());
    // From any executor, not only Tokio's: the requests run on the client's.
    let elsewhere = {
        let job = job.clone();
        std::thread::spawn(move || {
            futures::executor::block_on(job.for_user("alice", serde_json::Value::Null))
        })
        .join()
        .expect("no panic outside a Tokio runtime")
    };
    assert!(elsewhere.is_ok(), "{elsewhere:?}");
    as_alice
        .insert(NewTodo::new("Your weekly digest", false))
        .await
        .unwrap();
    assert_eq!(remote_todos(&alice, 2).await.len(), 2);
    // Closing drains what users' handles wrote too, not only the job's own.
    let digest = as_alice
        .insert(NewTodo::new("Another digest", false))
        .await
        .unwrap();
    // A handle acting for a user acts for no one else.
    assert!(matches!(
        as_alice.for_user("bob", serde_json::Value::Null).await,
        Err(Error::Config(_))
    ));

    job.close(jazz_app::CLOSE_TIMEOUT).await.expect("drained");
    assert!(remote_todos(&alice, 3).await.contains(&digest));
    assert!(matches!(job.all(todos::query()).await, Err(Error::Stopped)));
    alice.shutdown().await.unwrap();
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scoped_backend_client_acts_as_its_user_and_an_untrusted_one_is_refused() {
    use jazz_app::BackendClientKind;
    let app = App::start().await;
    let agent = Jazz::open(
        files(),
        Config::new(app.app_id)
            .server(&app.relay.endpoint)
            .credential(machine("token-agent")),
    )
    .await
    .expect("open as a scoped backend client");
    assert_eq!(agent.backend_client(), Some(BackendClientKind::Scoped));
    let Some(SessionState::SignedIn { user }) = agent.session().map(|s| s.state()) else {
        panic!("signed in as its user");
    };
    assert_eq!(
        (user.kind, user.subject.as_str()),
        (UserKind::Backend, "agent")
    );
    assert!(agent.account().is_some());
    assert!(matches!(
        agent.for_user("alice", serde_json::Value::Null).await,
        Err(Error::Config(_))
    ));
    agent
        .insert(NewTodo::new("By the agent", false))
        .await
        .unwrap();
    assert_eq!(remote_todos(&app.backend, 1).await.len(), 1);
    agent.close(jazz_app::CLOSE_TIMEOUT).await.unwrap();

    let untrusted = Jazz::open(
        files(),
        Config::new(app.app_id)
            .server(&app.relay.endpoint)
            .credential(machine("not-a-credential")),
    )
    .await;
    assert!(matches!(untrusted, Err(Error::Refused(_))), "{untrusted:?}");

    // Not a backend client at all: no for_user.
    let device = Jazz::open(
        client_files(),
        app.user(Auth::Device(DeviceKey::generate().unwrap())),
    )
    .await
    .unwrap();
    assert_eq!(device.backend_client(), None);
    assert!(matches!(
        device.for_user("alice", serde_json::Value::Null).await,
        Err(Error::Config(_))
    ));
    device.shutdown().await.unwrap();
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_reports_writes_a_memory_store_loses() {
    let app = App::start().await;
    let open = || {
        Jazz::open(
            files(),
            Config::new(app.app_id)
                .server(&app.relay.endpoint)
                .credential(machine("machine-trusted")),
        )
    };
    // The main backend stops admitting the machine: its writes stay here.
    let job = open().await.unwrap();
    connected(&job).await;
    app.relay.refuse("machine-trusted", "account revoked");
    app.relay.kick();
    tokio::time::sleep(Duration::from_millis(300)).await;
    job.insert(NewTodo::new("Never arrives", false))
        .await
        .unwrap();
    let lost = job.close(Duration::from_millis(500)).await;
    assert!(
        matches!(lost, Err(Error::Unsynced { pending: 1 })),
        "{lost:?}"
    );

    // A zero timeout closes at once, accepting the loss.
    app.relay.refusing.lock().unwrap().clear();
    let quick = open().await.unwrap();
    app.relay.refuse("machine-trusted", "account revoked");
    app.relay.kick();
    tokio::time::sleep(Duration::from_millis(300)).await;
    quick
        .insert(NewTodo::new("Also lost", false))
        .await
        .unwrap();
    quick.close(Duration::ZERO).await.unwrap();
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_status_reports_rejections_and_link_failures_as_typescript_does() {
    // No server: local-only, and synced.
    let local = Jazz::open(client_files(), Config::new(AppId::random()))
        .await
        .unwrap();
    let json = serde_json::to_value(local.sync_status()).unwrap();
    assert_eq!(json["link"], serde_json::json!({ "state": "local-only" }));
    assert_eq!(json["synced"], true);
    assert_eq!(json["lastError"], serde_json::Value::Null);
    local.shutdown().await.unwrap();

    let app = App::start().await;
    let token = Arc::new(Mutex::new("token-erin".to_owned()));
    let provider = {
        let token = token.clone();
        move |_: TokenReason| {
            let token = token.lock().unwrap().clone();
            async move { Ok(Some(token)) }
        }
    };
    let erin = Jazz::open(client_files(), app.user(Auth::bearer(provider)))
        .await
        .unwrap();
    connected(&erin).await;

    // Projects may be inserted, not updated: the server rejects the update.
    let project = erin
        .insert(bindings::NewProject::new("Plans"))
        .await
        .unwrap();
    erin.update(
        project.id,
        bindings::ProjectUpdate {
            name: Some("Other plans".into()),
        },
    )
    .await
    .unwrap();
    let rejected = sync_until(&erin, |status| status.rejected == 1).await;
    let Some(jazz_app::SyncError::Rejected { code, .. }) = &rejected.last_error else {
        panic!("a rejection: {rejected:?}");
    };
    assert!(!code.is_empty());
    let json = serde_json::to_value(&rejected).unwrap();
    assert_eq!(json["lastError"]["kind"], "rejected");

    // The backend refuses her bearer for good: the link is offline for auth,
    // and that failure, being later, is the last error.
    app.relay.refuse("token-erin", "account revoked");
    app.relay.kick();
    let refused = sync_until(&erin, |status| {
        matches!(
            status.link,
            Link::Offline {
                reason: jazz_app::Offline::Auth,
                ..
            }
        )
    })
    .await;
    let json = serde_json::to_value(&refused).unwrap();
    assert_eq!(json["link"]["state"], "offline");
    assert_eq!(json["link"]["reason"], "auth");
    assert_eq!(json["link"]["retrying"], false);
    assert_eq!(json["link"]["auth"], "revoked");
    assert_eq!(json["lastError"]["kind"], "auth");
    assert!(
        json["lastError"]["message"]
            .as_str()
            .unwrap()
            .contains("revoked")
    );
    assert!(!refused.synced);

    erin.shutdown().await.unwrap();
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_device_account_links_to_the_users_provider_account() {
    let app = App::start().await;
    let key = DeviceKey::generate().unwrap();
    let device = Jazz::open(client_files(), app.user(Auth::Device(key.clone())))
        .await
        .unwrap();
    connected(&device).await;
    let written = device
        .insert(NewTodo::new("Before signing up", false))
        .await
        .unwrap();
    let session = device.session().unwrap().clone();

    // Carol signs up with the app's own auth, and keeps her device's data,
    // from any executor (the request runs on the client's runtime).
    let carol = std::thread::spawn({
        let session = session.clone();
        move || futures::executor::block_on(session.link("token-carol"))
    })
    .join()
    .expect("no panic outside a Tokio runtime")
    .expect("link the device account");
    assert_eq!(carol.account, key.account(app.app_id));
    assert_eq!(carol.kind, UserKind::Backend);
    assert_eq!(carol.subject, "carol");
    assert_eq!(
        session.state(),
        SessionState::SignedIn {
            user: carol.clone()
        }
    );
    // Linking needs someone signed in.
    assert!(matches!(
        session.link("not-a-token").await,
        Err(Error::SignedOut)
    ));

    // Dave already has an account of his own: the backend refuses to link
    // him to another device's, for good, not as a server failure.
    let dave = Jazz::open(
        client_files(),
        app.user(Auth::bearer(|_| async {
            Ok(Some("token-dave".to_owned()))
        })),
    )
    .await
    .unwrap();
    let other = Jazz::open(
        client_files(),
        app.user(Auth::Device(DeviceKey::generate().unwrap())),
    )
    .await
    .unwrap();
    let refused = other.session().unwrap().link("token-dave").await;
    assert!(matches!(refused, Err(Error::Refused(_))), "{refused:?}");
    other.shutdown().await.unwrap();
    dave.shutdown().await.unwrap();

    // Signed in with her bearer elsewhere, she reaches the same account.
    let laptop = Jazz::open(
        client_files(),
        app.user(Auth::bearer(|_| async {
            Ok(Some("token-carol".to_owned()))
        })),
    )
    .await
    .unwrap();
    assert_eq!(laptop.account(), Some(carol.account));
    assert_eq!(remote_todos(&laptop, 1).await, vec![written]);

    // A bearer's session has no device key to link.
    let bearer_session = laptop.session().unwrap();
    assert!(matches!(
        bearer_session.link("token-carol").await,
        Err(Error::Config(_))
    ));

    laptop.shutdown().await.unwrap();
    device.shutdown().await.unwrap();
    app.stop().await;
}

/// Reads that name no tier read as TypeScript's do: a backend client from
/// the server, an end user's first load waiting for it, so neither shows an
/// empty cache of what the server already has.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_without_a_tier_see_what_the_server_has() {
    let app = App::start().await;
    let written = app
        .backend
        .insert(NewTodo::new("By the main backend", false))
        .await
        .unwrap();
    app.backend.wait_for_sync().await.unwrap();

    let agent = Jazz::open(
        files(),
        Config::new(app.app_id)
            .server(&app.relay.endpoint)
            .credential(machine("token-agent")),
    )
    .await
    .unwrap();
    assert_eq!(
        agent.all(todos::query()).await.unwrap(),
        vec![written.clone()]
    );
    // Options that name no tier read the same way.
    assert_eq!(
        agent
            .all_with_options(todos::query(), jazz_app::ReadOptions::default())
            .await
            .unwrap(),
        vec![written.clone()]
    );
    let mut changes = agent.subscribe(todos::query()).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(10), changes.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let added: Vec<_> = first.added.into_iter().map(|added| added.row).collect();
    assert_eq!(added, vec![written.clone()]);

    let user = Jazz::open(
        client_files(),
        app.user(Auth::Device(DeviceKey::generate().unwrap())),
    )
    .await
    .unwrap();
    assert_eq!(user.all(todos::query()).await.unwrap(), vec![written]);

    // Without a server, reads stay local.
    let local = Jazz::open(client_files(), Config::new(AppId::random()))
        .await
        .unwrap();
    assert_eq!(local.all(todos::query()).await.unwrap(), vec![]);

    drop(changes);
    agent.shutdown().await.unwrap();
    user.shutdown().await.unwrap();
    local.shutdown().await.unwrap();
    app.stop().await;
}

/// A server serving another history than the client's store (its store was
/// recreated): the client stops syncing with it and says so, as TypeScript
/// does (`authority-changed`), and its local data stays usable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_with_another_history_is_reported_as_authority_changed() {
    let first = App::start().await;
    let app_id = first.app_id;
    let key = DeviceKey::generate().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let open = |app: &App| {
        Jazz::open(
            client_files(),
            app.user(Auth::Device(key.clone()))
                .storage(Storage::Dir(dir.path().into())),
        )
    };
    let phone = open(&first).await.unwrap();
    let kept = phone
        .insert(NewTodo::new("Synced once", false))
        .await
        .unwrap();
    phone.wait_for_sync().await.unwrap();
    phone.shutdown().await.unwrap();
    first.stop().await;

    // The same app's server, on a new store.
    let second = App::start_for(app_id).await;
    let phone = open(&second)
        .await
        .expect("an authority change still opens");
    let stopped = sync_until(&phone, |status| matches!(status.link, Link::Offline { .. })).await;
    assert_eq!(
        stopped.link,
        Link::Offline {
            reason: jazz_app::Offline::AuthorityChanged,
            retrying: false,
            auth: None,
        },
        "{stopped:?}"
    );
    assert!(matches!(
        stopped.last_error,
        Some(jazz_app::SyncError::AuthorityChanged { .. })
    ));
    let json = serde_json::to_value(&stopped).unwrap();
    assert_eq!(json["link"]["reason"], "authority-changed");
    assert_eq!(json["lastError"]["kind"], "authority-changed");

    // Reconnecting tries the server again (as TS's `db.reconnect()`), and
    // finds it serving another history still.
    tokio::time::timeout(Duration::from_secs(10), phone.reconnect())
        .await
        .expect("reconnect returns after an authority change")
        .unwrap();
    let again = sync_until(&phone, |status| {
        matches!(
            status.link,
            Link::Offline {
                reason: jazz_app::Offline::AuthorityChanged,
                ..
            }
        )
    })
    .await;
    assert!(!again.synced);

    // Local data stays usable, and nothing is reset.
    assert_eq!(
        phone
            .all_with(todos::query(), ReadTier::LocalOnly)
            .await
            .unwrap(),
        vec![kept]
    );
    phone
        .insert(NewTodo::new("Written after", false))
        .await
        .unwrap();

    phone.shutdown().await.unwrap();
    second.stop().await;
}
