//! A user-following session ([`JazzSession`]) against the app's Jazz
//! endpoint (the `createJazz` stand-in in `relay/`): it follows the user
//! the app's bearer names, replaces the client when they change, and signs
//! in device accounts.

#[path = "bindings/todos.rs"]
mod bindings;
#[allow(dead_code)]
mod relay;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bindings::{NewTodo, todos};
use futures::StreamExt;
use jazz_app::{
    AppFiles, AppId, Backend, Config, Error, Initial, Jazz, JazzSession, ReadTier, SessionConfig,
    SessionStatus, Storage, TokenReason, UserKind,
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
            .unwrap();
        let permissions = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/todos/jazz.permissions.json"
        ))
        .unwrap();
        let backend = bindings::open(
            Config::new(app_id).server(server.base_url()).backend(
                Backend::new(server.backend_secret(), server.admin_secret(), "svc")
                    .permissions(permissions),
            ),
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
            app_id,
            server,
            backend,
            relay,
        }
    }

    /// A session whose bearer is whatever `token` holds.
    fn session(&self, token: &Arc<Mutex<Option<String>>>) -> SessionConfig {
        let token = token.clone();
        SessionConfig::new(self.app_id)
            .server(&self.relay.endpoint)
            .bearer(move |_: TokenReason| {
                let token = token.lock().unwrap().clone();
                async move { Ok(token) }
            })
    }

    async fn stop(self) {
        self.backend.shutdown().await.unwrap();
        self.server.shutdown().await;
    }
}

fn bearer(value: Option<&str>) -> Arc<Mutex<Option<String>>> {
    Arc::new(Mutex::new(value.map(str::to_owned)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_follows_the_user_the_bearer_names() {
    let app = App::start().await;
    let token = bearer(Some("token-alice"));
    let session = JazzSession::open(client_files(), app.session(&token))
        .await
        .unwrap();
    let snapshot = session.snapshot();
    assert_eq!(snapshot.status, SessionStatus::Ready);
    let alice = snapshot.user.clone().unwrap();
    assert_eq!(
        (alice.kind, alice.subject.as_str()),
        (UserKind::Backend, "alice")
    );
    let epoch = snapshot.client_epoch;
    let as_alice = session.jazz().unwrap();
    as_alice
        .insert(NewTodo::new("Alice's", false))
        .await
        .unwrap();

    // The app's auth says the same user: the same client goes on.
    session.refresh().await.unwrap();
    assert_eq!(session.snapshot().client_epoch, epoch);
    assert_eq!(session.snapshot().user, Some(alice.clone()));
    as_alice.all(todos::query()).await.unwrap();

    let mut alices = as_alice.subscribe(todos::query()).await.unwrap();
    alices.next().await.unwrap().unwrap();

    // Another user: the old client is replaced, the new one is theirs.
    *token.lock().unwrap() = Some("token-bob".to_owned());
    session.refresh().await.unwrap();
    let bob = session.snapshot();
    assert_eq!(bob.status, SessionStatus::Ready);
    assert_eq!(bob.user.as_ref().unwrap().subject, "bob");
    assert!(bob.client_epoch > epoch);
    assert!(matches!(
        as_alice.all(todos::query()).await,
        Err(Error::Replaced)
    ));
    // Its subscriptions end too.
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(item) = alices.next().await {
            if item.is_err() {
                break;
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "the replaced client's subscription ends");
    let as_bob = session.jazz().unwrap();
    assert_eq!(as_bob.account(), Some(bob.user.unwrap().account));

    // No one: signed out, and the client is gone.
    *token.lock().unwrap() = None;
    session.refresh().await.unwrap();
    assert_eq!(session.snapshot().status, SessionStatus::SignedOut);
    assert!(matches!(
        session.jazz(),
        Err(Error::NoUser {
            status: SessionStatus::SignedOut
        })
    ));
    assert!(matches!(
        as_bob.all(todos::query()).await,
        Err(Error::Replaced)
    ));
    let json = serde_json::to_value(session.snapshot()).unwrap();
    assert_eq!(json["status"], "signedOut");
    assert!(json["clientEpoch"].is_u64());

    session.close().await.unwrap();
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn device_accounts_are_created_reopened_restored_and_linked() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let token = bearer(None);
    let config = app
        .session(&token)
        .storage(Storage::Dir(dir.path().into()))
        .initial(Initial::SignedOut);
    let session = JazzSession::open(client_files(), config.clone())
        .await
        .unwrap();
    assert_eq!(session.snapshot().status, SessionStatus::SignedOut);

    session.create_device_account().await.unwrap();
    let guest = session.snapshot().user.unwrap();
    assert_eq!(guest.kind, UserKind::Device);
    let key = session.export_device_key().unwrap();
    let written = session
        .jazz()
        .unwrap()
        .insert(NewTodo::new("As a guest", false))
        .await
        .unwrap();
    // Its store sits in a directory of its own, the key beside them.
    let store = dir.path().join(guest.account.0.to_string());
    assert!(store.join("jazz.sqlite").exists(), "{store:?}");
    assert!(dir.path().join("device.key").exists());

    // Signing out keeps the key and the store: reopening has the data.
    session.sign_out().await.unwrap();
    assert_eq!(session.snapshot().status, SessionStatus::SignedOut);
    assert_eq!(session.export_device_key().unwrap(), key);
    session.reopen_device_account().await.unwrap();
    assert_eq!(session.snapshot().user.unwrap().account, guest.account);
    let jazz = session.jazz().unwrap();
    assert_eq!(
        jazz.all_with(todos::query(), ReadTier::LocalOnly)
            .await
            .unwrap(),
        vec![written.clone()]
    );

    // The key restores the same account in another session.
    let elsewhere = tempfile::tempdir().unwrap();
    let other = JazzSession::open(
        client_files(),
        app.session(&bearer(None))
            .storage(Storage::Dir(elsewhere.path().into()))
            .initial(Initial::SignedOut),
    )
    .await
    .unwrap();
    other.restore_device_account(&key).await.unwrap();
    assert_eq!(other.snapshot().user.unwrap().account, guest.account);
    other.close().await.unwrap();

    // The guest signs up: linked, the session goes on as the user, with
    // the same client and account.
    *token.lock().unwrap() = Some("token-carol".to_owned());
    let epoch = session.snapshot().client_epoch;
    session.link_device_account().await.unwrap();
    let carol = session.snapshot();
    let user = carol.user.unwrap();
    assert_eq!(
        (user.kind, user.subject.as_str()),
        (UserKind::Backend, "carol")
    );
    assert_eq!(user.account, guest.account);
    assert_eq!(carol.client_epoch, epoch);
    jazz.all(todos::query()).await.unwrap();

    session.close().await.unwrap();
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_starts_as_a_guest_and_reports_failures() {
    let app = App::start().await;
    // Initial::Device with no key kept: a new device account.
    let session = JazzSession::open(
        client_files(),
        app.session(&bearer(None)).initial(Initial::Device),
    )
    .await
    .unwrap();
    assert_eq!(session.snapshot().user.unwrap().kind, UserKind::Device);

    // A failed operation leaves the client usable and says so.
    let refused = session.restore_device_account("not a key").await;
    assert!(matches!(refused, Err(Error::Config(_))));
    let snapshot = session.snapshot();
    assert_eq!(snapshot.status, SessionStatus::Ready);
    assert_eq!(snapshot.error.as_ref().unwrap().kind, "config");
    assert_eq!(snapshot.recovery, Some(jazz_app::Recovery::Action));
    let json = serde_json::to_value(&snapshot).unwrap();
    assert_eq!(json["error"]["kind"], "config");
    assert_eq!(json["recovery"], "action");

    session.close().await.unwrap();
    app.stop().await;
}

/// Every operation works from any executor: they run on the session's own
/// runtime (as jazz-ffi calls them, with `block_on` on a host thread).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_operation_runs_outside_a_tokio_runtime() {
    let app = App::start().await;
    let token = bearer(Some("token-dana"));
    let config = app.session(&token).initial(Initial::SignedOut);
    let files = client_files();
    let token_for_thread = token.clone();
    let outcome = std::thread::spawn(move || {
        futures::executor::block_on(async move {
            let session = JazzSession::open(files, config).await?;
            session.create_device_account().await?;
            let key = session.export_device_key()?;
            session.sign_out().await?;
            session.reopen_device_account().await?;
            session.restore_device_account(&key).await?;
            session.link_device_account().await?;
            *token_for_thread.lock().unwrap() = Some("token-erin".to_owned());
            session.refresh().await?;
            let erin = session.snapshot().user.unwrap().subject;
            session.retry().await?;
            session.sign_out().await?;
            session.close().await?;
            Ok::<_, Error>(erin)
        })
    })
    .join()
    .expect("no panic outside a Tokio runtime");
    assert_eq!(outcome.unwrap(), "erin");
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changes_end_with_the_closed_snapshot() {
    let app = App::start().await;
    let session = JazzSession::open(client_files(), app.session(&bearer(Some("token-fay"))))
        .await
        .unwrap();
    let mut changes = session.changes();
    let first = changes.next().await.unwrap();
    assert_eq!(first.status, SessionStatus::Ready);
    session.close().await.unwrap();
    let rest: Vec<_> = tokio::time::timeout(Duration::from_secs(5), changes.collect::<Vec<_>>())
        .await
        .expect("the stream ends after close");
    assert_eq!(rest.last().unwrap().status, SessionStatus::Closed);
    assert!(matches!(session.refresh().await, Err(Error::Stopped)));
    assert!(matches!(
        session.jazz(),
        Err(Error::NoUser {
            status: SessionStatus::Closed
        })
    ));
    app.stop().await;
}

#[allow(dead_code)]
fn _assert_send_sync()
where
    JazzSession: Send + Sync + Clone,
{
}
