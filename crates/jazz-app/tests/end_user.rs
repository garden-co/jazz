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
        let app_id = AppId::random();
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

    // The app signs Alice out: the provider names no one.
    *token.lock().unwrap() = None;
    session.refresh().await.unwrap();
    assert_eq!(session.state(), SessionState::SignedOut);

    alice.shutdown().await.unwrap();
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

    let local = Jazz::open(
        client_files(),
        Config::new(app.app_id).user(Auth::Device(DeviceKey::generate().unwrap())),
    )
    .await;
    assert!(matches!(local, Err(Error::Config(_))), "{local:?}");
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

    // Carol signs up with the app's own auth, and keeps her device's data.
    let carol = session
        .link("token-carol")
        .await
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
