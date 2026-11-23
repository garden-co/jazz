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
use jazz_app_support::{
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
    done: impl Fn(&jazz_app_support::SyncStatus) -> bool,
) -> jazz_app_support::SyncStatus {
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
    session: &jazz_app_support::Session,
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
    use jazz_app_support::AuthFailure;
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

fn machine(token: &'static str) -> impl jazz_app_support::TokenProvider + 'static {
    move |_: TokenReason| async move { Ok(Some(token.to_owned())) }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trusted_backend_client_acts_as_the_system_and_for_users_with_accounts() {
    use jazz_app_support::BackendClientKind;
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
    // Its link and store are the job's: a user's handle doesn't manage them.
    match as_alice.disconnect().await {
        Err(Error::Unsupported(message)) => assert!(message.contains("(for_user)"), "{message}"),
        other => panic!("disconnect on a user's handle: {other:?}"),
    }

    job.close(jazz_app_support::CLOSE_TIMEOUT)
        .await
        .expect("drained");
    assert!(remote_todos(&alice, 3).await.contains(&digest));
    assert!(matches!(job.all(todos::query()).await, Err(Error::Stopped)));
    alice.shutdown().await.unwrap();
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scoped_backend_client_acts_as_its_user_and_an_untrusted_one_is_refused() {
    use jazz_app_support::BackendClientKind;
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
    agent.close(jazz_app_support::CLOSE_TIMEOUT).await.unwrap();

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
    let Some(jazz_app_support::SyncError::Rejected { code, .. }) = &rejected.last_error else {
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
                reason: jazz_app_support::Offline::Auth,
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
            .all_with_options(todos::query(), jazz_app_support::ReadOptions::default())
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
            reason: jazz_app_support::Offline::AuthorityChanged,
            retrying: false,
            auth: None,
        },
        "{stopped:?}"
    );
    assert!(matches!(
        stopped.last_error,
        Some(jazz_app_support::SyncError::AuthorityChanged { .. })
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
                reason: jazz_app_support::Offline::AuthorityChanged,
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

/// While the server is unreachable, a backend client's default (`Remote`)
/// read and subscription wait for it, as TypeScript's do: they neither fail
/// nor answer from the local store. Once it is back, they answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_reads_wait_for_an_unreachable_server() {
    let app = App::start().await;
    let agent = Jazz::open(
        files(),
        Config::new(app.app_id)
            .server(&app.relay.endpoint)
            .credential(machine("token-agent")),
    )
    .await
    .unwrap();
    connected(&agent).await;
    let written = app
        .backend
        .insert(NewTodo::new("While the agent was away", false))
        .await
        .unwrap();
    app.backend.wait_for_sync().await.unwrap();

    app.relay.set_unreachable(true);
    let read = tokio::spawn({
        let agent = agent.clone();
        async move { agent.all(todos::query()).await }
    });
    let mut changes = agent.subscribe(todos::query()).await.unwrap();
    let first = tokio::spawn(async move {
        let first = changes.next().await;
        (first, changes)
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!read.is_finished(), "the read waits for the server");
    assert!(
        !first.is_finished(),
        "the subscription waits for the server"
    );

    app.relay.set_unreachable(false);
    let rows = tokio::time::timeout(Duration::from_secs(30), read)
        .await
        .expect("the read answers once the server is back")
        .unwrap()
        .unwrap();
    assert_eq!(rows, vec![written.clone()]);
    let (first, changes) = tokio::time::timeout(Duration::from_secs(30), first)
        .await
        .expect("the subscription answers once the server is back")
        .unwrap();
    let added: Vec<_> = first
        .unwrap()
        .unwrap()
        .added
        .into_iter()
        .map(|added| added.row)
        .collect();
    assert_eq!(added, vec![written]);

    drop(changes);
    agent.shutdown().await.unwrap();
    app.stop().await;
}

/// Titles of the todos `jazz` holds locally.
async fn local_titles(jazz: &Jazz) -> Vec<String> {
    let mut titles: Vec<String> = jazz
        .all_with(todos::query(), ReadTier::LocalOnly)
        .await
        .unwrap()
        .into_iter()
        .map(|todo| todo.title)
        .collect();
    titles.sort();
    titles
}

/// Titles of every todo on the server, once it has `count`.
async fn server_titles(app: &App, count: usize) -> Vec<String> {
    let mut titles: Vec<String> = remote_todos(&app.backend, count)
        .await
        .into_iter()
        .map(|todo| todo.title)
        .collect();
    titles.sort();
    titles
}

/// Two end users on one device, one after the other, with the same
/// `Storage::Dir`: each has a store of their own under it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_user_on_a_dir_has_a_store_of_their_own() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let alice = DeviceKey::generate().unwrap();
    let as_ = |key: &DeviceKey| {
        app.user(Auth::Device(key.clone()))
            .storage(Storage::Dir(dir.path().into()))
    };

    // Alice syncs one write and leaves one unsent, then closes the app.
    let first = Jazz::open(client_files(), as_(&alice)).await.unwrap();
    connected(&first).await;
    first
        .insert(NewTodo::new("alice synced", false))
        .await
        .unwrap();
    first.wait_for_sync().await.unwrap();
    app.relay.set_unreachable(true);
    first
        .insert(NewTodo::new("alice unsent", false))
        .await
        .unwrap();
    first.shutdown().await.unwrap();
    app.relay.set_unreachable(false);

    // Bob, on the same dir, sees none of it and uploads only his own.
    let bob = DeviceKey::generate().unwrap();
    let second = Jazz::open(client_files(), as_(&bob)).await.unwrap();
    assert_eq!(local_titles(&second).await, Vec::<String>::new());
    second.insert(NewTodo::new("bob", false)).await.unwrap();
    second.wait_for_sync().await.expect("bob's own write syncs");
    assert_eq!(server_titles(&app, 2).await, ["alice synced", "bob"]);
    second.shutdown().await.unwrap();

    // Alice again: her store, and her unsent write goes up as hers.
    let again = Jazz::open(client_files(), as_(&alice)).await.unwrap();
    assert_eq!(local_titles(&again).await, ["alice synced", "alice unsent"]);
    again.wait_for_sync().await.unwrap();
    assert_eq!(
        server_titles(&app, 3).await,
        ["alice synced", "alice unsent", "bob"]
    );

    // Each account's store deletes alone.
    again.delete_client_storage().await.unwrap();
    let after = Jazz::open(client_files(), as_(&bob)).await.unwrap();
    assert_eq!(local_titles(&after).await, ["bob"]);
    after.shutdown().await.unwrap();
    app.stop().await;
}

/// The same with the app's own sign-in: one user signs out, the next signs
/// in on the same dir.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signing_out_and_in_as_someone_else_opens_their_store() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let as_ = |user: &'static str| {
        app.user(Auth::bearer(move |_| async move {
            Ok(Some(format!("token-{user}")))
        }))
        .storage(Storage::Dir(dir.path().into()))
    };
    let alice = Jazz::open(client_files(), as_("alice")).await.unwrap();
    connected(&alice).await;
    app.relay.set_unreachable(true);
    alice
        .insert(NewTodo::new("alice unsent", false))
        .await
        .unwrap();
    alice.sign_out().await.unwrap();
    app.relay.set_unreachable(false);

    let bob = Jazz::open(client_files(), as_("bob")).await.unwrap();
    assert_eq!(local_titles(&bob).await, Vec::<String>::new());
    bob.insert(NewTodo::new("bob", false)).await.unwrap();
    bob.wait_for_sync().await.expect("bob's own write syncs");
    assert_eq!(server_titles(&app, 1).await, ["bob"]);
    bob.shutdown().await.unwrap();

    let alice = Jazz::open(client_files(), as_("alice")).await.unwrap();
    alice.wait_for_sync().await.unwrap();
    assert_eq!(server_titles(&app, 2).await, ["alice unsent", "bob"]);
    alice.shutdown().await.unwrap();
    app.stop().await;
}

/// Scoped backend clients are users too: each has its own store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scoped_backend_clients_on_a_dir_have_stores_of_their_own() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let open = |token: &'static str| {
        Jazz::open(
            files(),
            Config::new(app.app_id)
                .server(&app.relay.endpoint)
                .credential(machine(token))
                .storage(Storage::Dir(dir.path().into())),
        )
    };
    let agent = open("token-agent").await.unwrap();
    agent.insert(NewTodo::new("agent", false)).await.unwrap();
    agent.wait_for_sync().await.unwrap();
    agent.shutdown().await.unwrap();
    let other = open("token-other-agent").await.unwrap();
    assert_eq!(local_titles(&other).await, Vec::<String>::new());
    other.shutdown().await.unwrap();
    app.stop().await;
}

/// A device account linked to the app's sign-in keeps its account, and so
/// its store: signing in with the bearer later opens it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_linked_device_account_keeps_its_store() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let key = DeviceKey::generate().unwrap();
    let device = Jazz::open(
        client_files(),
        app.user(Auth::Device(key))
            .storage(Storage::Dir(dir.path().into())),
    )
    .await
    .unwrap();
    connected(&device).await;
    device.insert(NewTodo::new("erin's", false)).await.unwrap();
    device.session().unwrap().link("token-erin").await.unwrap();
    device.shutdown().await.unwrap();

    app.relay.set_unreachable(true);
    let erin = Jazz::open(
        client_files(),
        app.user(Auth::bearer(|_| async {
            Ok(Some("token-erin".to_owned()))
        }))
        .storage(Storage::Dir(dir.path().into())),
    )
    .await;
    app.relay.set_unreachable(false);
    // Signing in needs the backend: reachable again, the same store.
    let erin = match erin {
        Ok(erin) => erin,
        Err(_) => Jazz::open(
            client_files(),
            app.user(Auth::bearer(|_| async {
                Ok(Some("token-erin".to_owned()))
            }))
            .storage(Storage::Dir(dir.path().into())),
        )
        .await
        .unwrap(),
    };
    assert_eq!(local_titles(&erin).await, ["erin's"]);
    erin.shutdown().await.unwrap();
    app.stop().await;
}

/// A store an earlier version kept at the dir itself is left as it was:
/// end users open stores of their own beside it, and an app without a
/// user refuses it when a signed-in user wrote it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_store_kept_at_the_dir_itself_is_left_alone() {
    let app = App::start().await;
    let made = tempfile::tempdir().unwrap();
    let key = DeviceKey::generate().unwrap();
    // A user's store, moved to where an earlier version kept it.
    let user = Jazz::open(
        client_files(),
        app.user(Auth::Device(key.clone()))
            .storage(Storage::Dir(made.path().into())),
    )
    .await
    .unwrap();
    connected(&user).await;
    user.insert(NewTodo::new("from before", false))
        .await
        .unwrap();
    user.wait_for_sync().await.unwrap();
    user.shutdown().await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = std::fs::read_dir(made.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.join("jazz.sqlite").exists())
        .unwrap_or_else(|| made.path().to_owned());
    for entry in std::fs::read_dir(&store).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            std::fs::copy(&path, dir.path().join(path.file_name().unwrap())).unwrap();
        }
    }
    let before = std::fs::read(dir.path().join("jazz.sqlite")).unwrap();

    // An app with no user on that dir: refused, the store untouched.
    let local = Jazz::open(
        client_files(),
        Config::new(app.app_id).storage(Storage::Dir(dir.path().into())),
    )
    .await;
    match local {
        Err(Error::Refused(message)) => assert!(message.contains("signed-in user"), "{message}"),
        other => panic!(
            "opened a signed-in user's store without them: {:?}",
            other.err()
        ),
    }

    // A user on that dir gets a store of their own; this one stays.
    let again = Jazz::open(
        client_files(),
        app.user(Auth::Device(key))
            .storage(Storage::Dir(dir.path().into())),
    )
    .await
    .unwrap();
    again.shutdown().await.unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("jazz.sqlite")).unwrap(),
        before
    );

    // A store with only an app's own writes opens as before.
    let own = tempfile::tempdir().unwrap();
    let config = || Config::new(app.app_id).storage(Storage::Dir(own.path().into()));
    let local = Jazz::open(client_files(), config()).await.unwrap();
    local.insert(NewTodo::new("local", false)).await.unwrap();
    local.shutdown().await.unwrap();
    let local = Jazz::open(client_files(), config()).await.unwrap();
    assert_eq!(local_titles(&local).await, ["local"]);
    local.shutdown().await.unwrap();
    app.stop().await;
}

// ---- opening offline with a bearer ------------------------------------------

/// A bearer whose provider always gives `token`.
fn bearer_of(token: String) -> Auth {
    Auth::bearer(move |_| {
        let token = token.clone();
        async move { Ok(Some(token)) }
    })
}

/// An endpoint nothing listens at: connecting is refused.
async fn dead_endpoint() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/jazz", listener.local_addr().unwrap());
    drop(listener);
    endpoint
}

/// Alice, signed in once with her JWT on `dir`: one write synced, one left
/// unsent. Returns her account.
async fn alice_signed_in_once(app: &App, dir: &std::path::Path) -> jazz_app_support::AccountId {
    let alice = Jazz::open(
        client_files(),
        app.user(bearer_of(relay::jwt("alice", 0)))
            .storage(Storage::Dir(dir.into())),
    )
    .await
    .expect("alice signs in");
    connected(&alice).await;
    alice.insert(NewTodo::new("synced", false)).await.unwrap();
    alice.wait_for_sync().await.unwrap();
    app.relay.set_unreachable(true);
    alice.insert(NewTodo::new("unsent", false)).await.unwrap();
    let account = alice.account().unwrap();
    alice.shutdown().await.unwrap();
    account
}

fn admitted_as(app: &App, who: &str) -> usize {
    app.relay
        .admitted
        .lock()
        .unwrap()
        .iter()
        .filter(|admitted| *admitted == who)
        .count()
}

fn offline_open_refused(result: Result<Jazz, Error>) -> String {
    match result {
        Err(Error::Server(message)) => {
            assert!(
                message.contains("opening offline needs an earlier sign-in"),
                "{message}"
            );
            message
        }
        Err(error) => panic!("refused as offline open: {error:?}"),
        Ok(_) => panic!("refused as offline open, but it opened"),
    }
}

/// Written, closed, and with the server gone, a bearer's user opens their
/// store again: their rows and writes are there, nothing reaches the server
/// until it signs them in again, and then their queued writes upload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bearers_user_opens_their_store_offline() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let account = alice_signed_in_once(&app, dir.path()).await;
    let admitted = admitted_as(&app, "user:alice");

    // No server listening at all: the store opens, as hers.
    let gone = Jazz::open(
        client_files(),
        Config::new(app.app_id)
            .server(dead_endpoint().await)
            .user(bearer_of(relay::jwt("alice", 1)))
            .storage(Storage::Dir(dir.path().into())),
    )
    .await
    .expect("opens offline with no server listening");
    assert_eq!(gone.account(), Some(account));
    assert_eq!(local_titles(&gone).await, ["synced", "unsent"]);
    gone.shutdown().await.unwrap();

    // The app's server drops every connection: the same.
    let alice = Jazz::open(
        client_files(),
        app.user(bearer_of(relay::jwt("alice", 2)))
            .storage(Storage::Dir(dir.path().into())),
    )
    .await
    .expect("opens offline");
    let session = alice.session().unwrap().clone();
    let SessionState::SignedIn { user } = session.state() else {
        panic!("signed in: {:?}", session.state());
    };
    assert_eq!(
        (user.account, user.issuer.as_str(), user.subject.as_str()),
        (account, "jazz:backend", "alice")
    );
    assert_eq!(local_titles(&alice).await, ["synced", "unsent"]);
    alice.insert(NewTodo::new("offline", false)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(admitted_as(&app, "user:alice"), admitted);

    // The server is back: it signs her in again, and her writes upload.
    app.relay.set_unreachable(false);
    alice
        .wait_for_sync()
        .await
        .expect("her queued writes upload");
    assert_eq!(
        server_titles(&app, 3).await,
        ["offline", "synced", "unsent"]
    );
    assert_eq!(admitted_as(&app, "user:alice"), admitted + 1);
    assert!(matches!(session.state(), SessionState::SignedIn { .. }));
    alice.shutdown().await.unwrap();
    app.stop().await;
}

/// Offline, another user's bearer opens only what that user signed in to
/// on this device: their own store, or nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offline_another_users_bearer_opens_only_their_own_store() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let on_dir = |token: String| {
        app.user(bearer_of(token))
            .storage(Storage::Dir(dir.path().into()))
    };
    let alice = alice_signed_in_once(&app, dir.path()).await;

    // Bob never signed in here.
    offline_open_refused(Jazz::open(client_files(), on_dir(relay::jwt("bob", 0))).await);
    // Nor did a token naming Alice from another issuer, nor an opaque one.
    offline_open_refused(
        Jazz::open(
            client_files(),
            on_dir(relay::jwt_from("https://other.test", "alice", 0)),
        )
        .await,
    );
    offline_open_refused(Jazz::open(client_files(), on_dir("token-alice".into())).await);

    // Bob signs in once, online; offline, his bearer opens his store.
    app.relay.set_unreachable(false);
    let bob = Jazz::open(client_files(), on_dir(relay::jwt("bob", 0)))
        .await
        .unwrap();
    connected(&bob).await;
    bob.insert(NewTodo::new("bob's", false)).await.unwrap();
    bob.wait_for_sync().await.unwrap();
    let bob_account = bob.account().unwrap();
    bob.shutdown().await.unwrap();
    app.relay.set_unreachable(true);
    let bob = Jazz::open(client_files(), on_dir(relay::jwt("bob", 1)))
        .await
        .expect("bob opens offline");
    assert_eq!(bob.account(), Some(bob_account));
    assert_ne!(bob.account(), Some(alice));
    assert_eq!(local_titles(&bob).await, ["bob's"]);
    bob.shutdown().await.unwrap();
    app.stop().await;
}

/// Offline open is for a server that can't be reached: one that answers
/// (failing, or naming no one) fails the open as before, remembered or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_that_answers_is_not_offline() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    alice_signed_in_once(&app, dir.path()).await;
    app.relay.set_unreachable(false);
    let open = || {
        Jazz::open(
            client_files(),
            app.user(bearer_of(relay::jwt("alice", 1)))
                .storage(Storage::Dir(dir.path().into())),
        )
    };

    app.relay
        .links
        .lock()
        .unwrap()
        .fail_sign_in
        .insert("alice".into());
    match open().await {
        Err(Error::Server(message)) => {
            assert!(message.contains("503"), "{message}");
            assert!(!message.contains("offline"), "{message}");
        }
        other => panic!("a 503 fails the open: {:?}", other.map(|_| ())),
    }

    app.relay
        .links
        .lock()
        .unwrap()
        .no_one
        .insert("alice".into());
    assert!(matches!(open().await, Err(Error::SignedOut)));
    app.stop().await;
}

/// A remembered account whose store was deleted is not opened offline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remembered_account_whose_store_is_gone_does_not_open_offline() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let account = alice_signed_in_once(&app, dir.path()).await;
    std::fs::remove_dir_all(dir.path().join(account.0.to_string())).unwrap();
    offline_open_refused(
        Jazz::open(
            client_files(),
            app.user(bearer_of(relay::jwt("alice", 1)))
                .storage(Storage::Dir(dir.path().into())),
        )
        .await,
    );
    assert!(!dir.path().join(account.0.to_string()).exists());
    app.stop().await;
}

/// Opened offline, then the server signs the bearer in as another account:
/// the client stops and the session reads signed out, having uploaded
/// nothing. A server refusing the bearer does the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offline_then_another_account_or_a_refusal_signs_out_with_nothing_uploaded() {
    for refusal in [false, true] {
        let app = App::start().await;
        let dir = tempfile::tempdir().unwrap();
        alice_signed_in_once(&app, dir.path()).await;
        let admitted = admitted_as(&app, "user:alice");
        let alice = Jazz::open(
            client_files(),
            app.user(bearer_of(relay::jwt("alice", 1)))
                .storage(Storage::Dir(dir.path().into())),
        )
        .await
        .expect("opens offline");
        alice.insert(NewTodo::new("offline", false)).await.unwrap();
        let session = alice.session().unwrap().clone();
        {
            let mut links = app.relay.links.lock().unwrap();
            if refusal {
                links.no_one.insert("alice".into());
            } else {
                links.sign_in_as.insert("alice".into(), "mallory".into());
            }
        }
        app.relay.set_unreachable(false);
        session_until(&session, |state| *state == SessionState::SignedOut).await;
        assert!(matches!(
            alice.insert(NewTodo::new("after", false)).await,
            Err(Error::Stopped)
        ));
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            admitted_as(&app, "user:alice"),
            admitted,
            "refusal: {refusal}"
        );
        assert_eq!(
            server_titles(&app, 1).await,
            ["synced"],
            "refusal: {refusal}"
        );
        let _ = alice.shutdown().await;
        app.stop().await;
    }
}
