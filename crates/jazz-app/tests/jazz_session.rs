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

use bindings::{NewTodo, Todo, todos};
use futures::StreamExt;
use jazz_app_support::jazz::account_registry::AccountId;
use jazz_app_support::{
    AppFiles, AppId, Backend, Config, Error, Initial, Jazz, JazzSession, ReadTier, Recovery,
    SessionConfig, SessionStatus, Storage, TokenReason, UserKind,
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

    /// A session that opens as the device account `dir` keeps.
    fn guest(&self, dir: &std::path::Path) -> SessionConfig {
        self.session(&bearer(None))
            .storage(Storage::Dir(dir.into()))
            .initial(Initial::Device)
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
    assert_eq!(json["status"], "signed-out");
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

    // The guest signs up: linked, the session goes on as the user, on the
    // same account and store, reopened on the app's bearer.
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
    assert!(carol.client_epoch > epoch);
    assert!(matches!(
        jazz.all(todos::query()).await,
        Err(Error::Replaced)
    ));
    assert_eq!(
        session
            .jazz()
            .unwrap()
            .all_with(todos::query(), ReadTier::LocalOnly)
            .await
            .unwrap(),
        vec![written]
    );

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
    assert_eq!(snapshot.recovery, Some(jazz_app_support::Recovery::Action));
    let json = serde_json::to_value(&snapshot).unwrap();
    assert_eq!(json["error"]["kind"], "config");
    assert_eq!(json["recovery"], "action");

    session.close().await.unwrap();
    app.stop().await;
}

/// An endpoint refused before any request: plain http beyond this machine.
const INSECURE: &str = "http://192.0.2.1/jazz";

/// A device account kept in `dir`, with a write of its own: its key file's
/// bytes, its account and the write.
async fn keep_a_guest(app: &App, dir: &std::path::Path) -> (Vec<u8>, AccountId, Todo) {
    let session = JazzSession::open(client_files(), app.guest(dir))
        .await
        .unwrap();
    let account = session.snapshot().user.unwrap().account;
    let written = session
        .jazz()
        .unwrap()
        .insert(NewTodo::new("The guest's", false))
        .await
        .unwrap();
    session.close().await.unwrap();
    (
        std::fs::read(dir.join("device.key")).unwrap(),
        account,
        written,
    )
}

/// `session` found no account it could sign in to, and says why.
fn assert_open_failed(session: &JazzSession) {
    let snapshot = session.snapshot();
    assert_eq!(snapshot.status, SessionStatus::Error, "{snapshot:?}");
    assert_eq!(snapshot.error.as_ref().unwrap().kind, "config");
    assert_eq!(snapshot.recovery, Some(Recovery::Session));
    assert!(snapshot.user.is_none(), "{snapshot:?}");
}

/// Alice's guest has data; a later launch has a wrong URL. Her key is left
/// as it was, and the fixed URL signs her in with her data.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_sign_in_at_open_leaves_the_kept_key() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let (key, account, written) = keep_a_guest(&app, dir.path()).await;
    let path = dir.path().join("device.key");

    let session = JazzSession::open(client_files(), app.guest(dir.path()).server(INSECURE))
        .await
        .unwrap();
    assert!(std::fs::read(&path).unwrap() == key, "the kept key changed");
    assert_open_failed(&session);
    assert!(session.retry().await.is_err());
    assert!(
        std::fs::read(&path).unwrap() == key,
        "retry changed the key"
    );
    session.close().await.unwrap();

    let fixed = JazzSession::open(client_files(), app.guest(dir.path()))
        .await
        .unwrap();
    assert_eq!(fixed.snapshot().user.unwrap().account, account);
    assert_eq!(
        fixed
            .jazz()
            .unwrap()
            .all_with(todos::query(), ReadTier::LocalOnly)
            .await
            .unwrap(),
        vec![written]
    );
    fixed.close().await.unwrap();
    app.stop().await;
}

/// A key file that holds no key (quoted whole, cut short, empty, not text)
/// is reported and left as it was; mended, retry signs in as its account.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_key_that_does_not_parse_is_left_as_it_was() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let (key, account, _) = keep_a_guest(&app, dir.path()).await;
    let path = dir.path().join("device.key");

    let quoted = [&b"\""[..], &key, &b"\""[..]].concat();
    let cut = key[..20].to_vec();
    for held in [quoted, cut, Vec::new(), vec![0xff, 0xfe]] {
        std::fs::write(&path, &held).unwrap();
        let session = JazzSession::open(client_files(), app.guest(dir.path()))
            .await
            .unwrap();
        assert!(
            std::fs::read(&path).unwrap() == held,
            "the key file changed"
        );
        assert_open_failed(&session);

        std::fs::write(&path, &key).unwrap();
        session.retry().await.unwrap();
        assert_eq!(session.snapshot().user.unwrap().account, account);
        session.close().await.unwrap();
    }
    app.stop().await;
}

/// A key file this user may not read is reported and left as it was;
/// readable again, retry signs in as its account.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreadable_key_is_left_as_it_was() {
    use std::os::unix::fs::PermissionsExt;
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let (key, account, _) = keep_a_guest(&app, dir.path()).await;
    let path = dir.path().join("device.key");
    let mode = |mode| std::fs::set_permissions(&path, PermissionsExt::from_mode(mode)).unwrap();

    mode(0o000);
    if std::fs::read(&path).is_ok() {
        eprintln!("skipped: this user reads files whatever their mode (root)");
    } else {
        let session = JazzSession::open(client_files(), app.guest(dir.path()))
            .await
            .unwrap();
        let after = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(after & 0o777, 0, "the key file was replaced");
        assert_open_failed(&session);

        mode(0o600);
        assert!(std::fs::read(&path).unwrap() == key, "the kept key changed");
        session.retry().await.unwrap();
        assert_eq!(session.snapshot().user.unwrap().account, account);
        session.close().await.unwrap();
    }
    app.stop().await;
}

/// A key kept through a link whose target is away (an unmounted volume)
/// is no first launch: the link stays.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_key_link_to_nowhere_is_left_as_it_was() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let (path, target) = (dir.path().join("device.key"), dir.path().join("away/key"));
    std::os::unix::fs::symlink(&target, &path).unwrap();

    let session = JazzSession::open(client_files(), app.guest(dir.path()))
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_link(&path).expect("still the link"),
        target,
        "the link changed"
    );
    assert_open_failed(&session);
    session.close().await.unwrap();
    app.stop().await;
}

/// An unreachable server is no reason to change accounts either: the
/// guest opens offline on the kept key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreachable_server_leaves_the_kept_key() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let (key, account, _) = keep_a_guest(&app, dir.path()).await;

    let session = JazzSession::open(
        client_files(),
        app.guest(dir.path()).server("http://127.0.0.1:9/jazz"),
    )
    .await
    .unwrap();
    assert!(
        std::fs::read(dir.path().join("device.key")).unwrap() == key,
        "the key changed"
    );
    if let Some(user) = session.snapshot().user {
        assert_eq!(user.account, account);
    }
    session.close().await.unwrap();
    app.stop().await;
}

/// A first launch keeps a new key, and signs in as it next time. One whose
/// sign-in fails keeps its key too: retry doesn't make another.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_first_launch_keeps_one_new_key() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let (key, account, _) = keep_a_guest(&app, dir.path()).await;
    let again = JazzSession::open(client_files(), app.guest(dir.path()))
        .await
        .unwrap();
    assert_eq!(again.snapshot().user.unwrap().account, account);
    assert!(
        std::fs::read(dir.path().join("device.key")).unwrap() == key,
        "the key changed"
    );
    again.close().await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let failed = JazzSession::open(client_files(), app.guest(dir.path()).server(INSECURE))
        .await
        .unwrap();
    let first = std::fs::read(dir.path().join("device.key")).unwrap();
    assert!(failed.retry().await.is_err());
    assert!(
        std::fs::read(dir.path().join("device.key")).unwrap() == first,
        "retry made another key"
    );
    failed.close().await.unwrap();
    app.stop().await;
}

/// Asking for a new device account replaces the kept one only once the new
/// one signed in; a failure, and its retry, leave the kept key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_create_leaves_the_kept_key() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let (key, account, _) = keep_a_guest(&app, dir.path()).await;
    let path = dir.path().join("device.key");
    let signed_out = || app.guest(dir.path()).initial(Initial::SignedOut);

    let failing = JazzSession::open(client_files(), signed_out().server(INSECURE))
        .await
        .unwrap();
    assert!(failing.create_device_account().await.is_err());
    assert!(
        std::fs::read(&path).unwrap() == key,
        "a failed create changed the key"
    );
    assert!(failing.retry().await.is_err());
    assert!(
        std::fs::read(&path).unwrap() == key,
        "its retry changed the key"
    );
    failing.close().await.unwrap();
    let files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.contains("key"))
        .collect();
    assert_eq!(files, ["device.key"], "a failed create left a key behind");

    let session = JazzSession::open(client_files(), signed_out())
        .await
        .unwrap();
    session.create_device_account().await.unwrap();
    assert_ne!(session.snapshot().user.unwrap().account, account);
    assert!(
        std::fs::read_to_string(&path).unwrap() == session.export_device_key().unwrap(),
        "the new key is not kept"
    );
    session.close().await.unwrap();
    app.stop().await;
}

/// With nothing kept yet, a failed create's retry is for the same account.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retrying_a_create_makes_no_other_account() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("device.key");
    let session = JazzSession::open(
        client_files(),
        app.guest(dir.path())
            .initial(Initial::SignedOut)
            .server(INSECURE),
    )
    .await
    .unwrap();
    assert!(session.create_device_account().await.is_err());
    let first = std::fs::read(&path).unwrap();
    assert!(session.retry().await.is_err());
    assert!(
        std::fs::read(&path).unwrap() == first,
        "retry made another key"
    );
    session.close().await.unwrap();
    app.stop().await;
}

/// Sessions opened at once on a first launch keep one key: whoever signed
/// in did so as its account. (The other may find the account's store busy.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_launches_at_once_keep_one_key() {
    let app = App::start().await;
    for round in 0..20 {
        let dir = tempfile::tempdir().unwrap();
        let (first, second) = tokio::join!(
            JazzSession::open(client_files(), app.guest(dir.path())),
            JazzSession::open(client_files(), app.guest(dir.path())),
        );
        let sessions = [first.unwrap(), second.unwrap()];
        let signed_in: Vec<_> = sessions
            .iter()
            .filter_map(|session| session.snapshot().user)
            .map(|user| user.account)
            .collect();
        let kept: jazz_app_support::DeviceKey =
            std::fs::read_to_string(dir.path().join("device.key"))
                .unwrap()
                .parse()
                .unwrap();
        assert!(!signed_in.is_empty(), "round {round}: {sessions:?}");
        for account in signed_in {
            assert_eq!(
                account,
                kept.account(app.app_id),
                "round {round}: its key was replaced"
            );
        }
        for session in sessions {
            session.close().await.unwrap();
        }
    }
    app.stop().await;
}

/// A key another first launch keeps while this one looks is that key, not
/// an error. (Exporting signed out only looks: cheap enough to race.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_key_kept_while_looking_is_found() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("device.key");
    let session = JazzSession::open(
        client_files(),
        app.guest(dir.path()).initial(Initial::SignedOut),
    )
    .await
    .unwrap();
    let key = jazz_app_support::DeviceKey::generate().unwrap().to_string();
    for round in 0..3000 {
        let _ = std::fs::remove_file(&path);
        let whole = dir.path().join("whole.tmp");
        std::fs::write(&whole, &key).unwrap();
        std::thread::scope(|scope| {
            let keeper = scope.spawn(|| std::fs::rename(&whole, &path).unwrap());
            loop {
                match session.export_device_key() {
                    Ok(found) => {
                        assert!(found == key, "round {round}: another key");
                        break;
                    }
                    Err(error) => assert!(
                        !error.to_string().contains("device key at"),
                        "round {round}: {error}"
                    ),
                }
            }
            keeper.join().unwrap();
        });
    }
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

/// TS's "keeps a device account when the backend says no one is signed
/// in" (`backend-watch.test.ts`), then its guest link on sign-up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_guest_is_kept_while_no_one_is_signed_in_and_linked_on_sign_up() {
    let app = App::start().await;
    let token = bearer(None);
    let session = JazzSession::open(client_files(), app.session(&token).initial(Initial::Device))
        .await
        .unwrap();
    let guest = session.snapshot();
    assert_eq!(guest.user.as_ref().unwrap().kind, UserKind::Device);
    let jazz = session.jazz().unwrap();
    let written = jazz
        .insert(NewTodo::new("As a guest", false))
        .await
        .unwrap();
    // The guest's store is in memory: its write reaches the server before
    // the session reopens on the linked user.
    jazz.wait_for_sync().await.unwrap();

    // The backend names no one: the guest stays, with its client.
    session.refresh().await.unwrap();
    let kept = session.snapshot();
    assert_eq!(kept.status, SessionStatus::Ready);
    assert_eq!(kept.user, guest.user);
    assert_eq!(kept.client_epoch, guest.client_epoch);

    // Gil signs up (no account yet): the guest is linked to him, and goes
    // on as him with its data.
    *token.lock().unwrap() = Some("token-gil".to_owned());
    session.refresh().await.unwrap();
    let gil = session.snapshot();
    let user = gil.user.unwrap();
    assert_eq!(
        (user.kind, user.subject.as_str()),
        (UserKind::Backend, "gil")
    );
    assert_eq!(user.account, guest.user.unwrap().account);
    assert!(
        session
            .jazz()
            .unwrap()
            .all(todos::query())
            .await
            .unwrap()
            .contains(&written)
    );

    // Following again keeps him as he is.
    let epoch = session.snapshot().client_epoch;
    session.refresh().await.unwrap();
    assert_eq!(session.snapshot().client_epoch, epoch);
    drop(jazz);
    session.close().await.unwrap();

    // With guest linking off, the guest stays, for the app to link.
    let token = bearer(None);
    let unlinked = JazzSession::open(
        client_files(),
        app.session(&token)
            .initial(Initial::Device)
            .link_guest(false),
    )
    .await
    .unwrap();
    let guest = unlinked.snapshot().user.unwrap();
    *token.lock().unwrap() = Some("token-hal".to_owned());
    unlinked.refresh().await.unwrap();
    assert_eq!(unlinked.snapshot().user.unwrap(), guest);
    unlinked.link_device_account().await.unwrap();
    let hal = unlinked.snapshot().user.unwrap();
    assert_eq!(hal.subject, "hal");
    assert_eq!(hal.account, guest.account);
    unlinked.close().await.unwrap();
    app.stop().await;
}

/// One app per store doesn't get in a session's way: signing out and in
/// again, and linking, open the same directory while the handles they
/// replace are still around and busy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_reopens_the_store_it_just_left() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let token = bearer(None);
    let session = JazzSession::open(
        client_files(),
        app.session(&token)
            .storage(Storage::Dir(dir.path().into()))
            .initial(Initial::SignedOut),
    )
    .await
    .unwrap();
    session.create_device_account().await.unwrap();

    let mut replaced = Vec::new();
    for round in 0..20 {
        let old = session.jazz().unwrap();
        let busy = old.clone();
        let (running, started) = tokio::sync::oneshot::channel();
        let job = tokio::spawn(async move {
            busy.with_client(move |_| async move {
                let _ = running.send(());
                tokio::time::sleep(Duration::from_millis(30)).await;
                Ok(())
            })
            .await
        });
        started.await.unwrap();
        if round % 2 == 1 {
            // The client is already stopping when the session replaces it.
            let stopping = old.clone();
            tokio::spawn(async move { stopping.shutdown().await });
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        session.sign_out().await.unwrap();
        session
            .reopen_device_account()
            .await
            .unwrap_or_else(|error| panic!("round {round}: {error}"));
        let _ = job.await.unwrap();
        replaced.push(old);
    }

    *token.lock().unwrap() = Some("token-kim".to_owned());
    session.link_device_account().await.unwrap();
    assert_eq!(session.snapshot().user.unwrap().subject, "kim");
    session.close().await.unwrap();
    app.stop().await;
}

/// Each account keeps its own store: one user's local data is not in the
/// next one's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_account_keeps_a_store_of_its_own() {
    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let token = bearer(Some("token-ida"));
    let session = JazzSession::open(
        client_files(),
        app.session(&token).storage(Storage::Dir(dir.path().into())),
    )
    .await
    .unwrap();
    let ida = session.snapshot().user.unwrap();
    session.jazz().unwrap().disconnect().await.unwrap();
    session
        .jazz()
        .unwrap()
        .insert(NewTodo::new("Ida's, never synced", false))
        .await
        .unwrap();

    *token.lock().unwrap() = Some("token-jon".to_owned());
    session.refresh().await.unwrap();
    let jon = session.snapshot().user.unwrap();
    assert_ne!(jon.account, ida.account);
    assert_eq!(
        session
            .jazz()
            .unwrap()
            .all_with(todos::query(), ReadTier::LocalOnly)
            .await
            .unwrap(),
        vec![]
    );
    for user in [&ida, &jon] {
        let store = dir.path().join(user.account.0.to_string());
        assert!(store.join("jazz.sqlite").exists(), "{store:?}");
    }
    session.close().await.unwrap();
    app.stop().await;
}

/// TS's "signs the user in instead, once, when the backend refuses the
/// guest's link": the user gets their own account, and the refused link is
/// never tried again for that guest and user.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_guest_link_signs_the_user_in_instead_once() {
    let app = App::start().await;
    app.relay
        .links
        .lock()
        .unwrap()
        .refused
        .insert("lee".to_owned());
    let token = bearer(None);
    let session = JazzSession::open(client_files(), app.session(&token).initial(Initial::Device))
        .await
        .unwrap();
    let guest = session.snapshot().user.unwrap();
    let key = session.export_device_key().unwrap();

    *token.lock().unwrap() = Some("token-lee".to_owned());
    session.refresh().await.unwrap();
    let lee = session.snapshot().user.unwrap();
    assert_eq!((lee.kind, lee.subject.as_str()), (UserKind::Backend, "lee"));
    assert_ne!(lee.account, guest.account);
    assert_eq!(app.relay.links.lock().unwrap().attempts, 1);

    // The guest again, and Lee still named: no second link, Lee is signed
    // in on his account.
    session.restore_device_account(&key).await.unwrap();
    assert_eq!(session.snapshot().user.unwrap().account, guest.account);
    session.refresh().await.unwrap();
    assert_eq!(session.snapshot().user.unwrap(), lee);
    assert_eq!(app.relay.links.lock().unwrap().attempts, 1);

    session.close().await.unwrap();
    app.stop().await;
}

/// TS's "signs in to the account that appeared while the link ran (409)".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_account_that_appeared_while_linking_is_signed_in_to() {
    let app = App::start().await;
    app.relay
        .links
        .lock()
        .unwrap()
        .appear
        .insert("mo".to_owned());
    let token = bearer(None);
    let session = JazzSession::open(client_files(), app.session(&token).initial(Initial::Device))
        .await
        .unwrap();
    let guest = session.snapshot().user.unwrap();
    *token.lock().unwrap() = Some("token-mo".to_owned());
    session.refresh().await.unwrap();
    let mo = session.snapshot().user.unwrap();
    assert_eq!(mo.subject, "mo");
    assert_ne!(mo.account, guest.account);
    // Her account is the one that appeared: signing in elsewhere finds it.
    let elsewhere = JazzSession::open(client_files(), app.session(&bearer(Some("token-mo"))))
        .await
        .unwrap();
    assert_eq!(elsewhere.snapshot().user.unwrap().account, mo.account);
    elsewhere.close().await.unwrap();
    session.close().await.unwrap();
    app.stop().await;
}

/// A backend that retires the guest's key on linking (`guestKeyAfterLink:
/// "revoke"`): the session goes on as the user on the app's bearer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn linking_survives_a_retired_guest_key() {
    let app = App::start().await;
    app.relay.links.lock().unwrap().revoke = true;
    let token = bearer(None);
    let session = JazzSession::open(client_files(), app.session(&token).initial(Initial::Device))
        .await
        .unwrap();
    let guest = session.snapshot().user.unwrap();
    session
        .jazz()
        .unwrap()
        .insert(NewTodo::new("Before linking", false))
        .await
        .unwrap();
    *token.lock().unwrap() = Some("token-nia".to_owned());
    session.refresh().await.unwrap();
    let nia = session.snapshot().user.unwrap();
    assert_eq!((nia.subject.as_str(), nia.account), ("nia", guest.account));
    let written = session
        .jazz()
        .unwrap()
        .insert(NewTodo::new("After linking", false))
        .await
        .unwrap();
    let synced = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = app
                .backend
                .all_with(todos::query(), ReadTier::Remote)
                .await
                .unwrap();
            if rows.contains(&written) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(synced.is_ok(), "the linked user syncs on the app's bearer");
    session.close().await.unwrap();
    app.stop().await;
}

/// A refused link's fallback sign-in fails: the guest stays, and the next
/// refresh signs the user in without linking again (TS's `refusedLink`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_link_is_not_tried_again_when_its_fallback_failed() {
    let app = App::start().await;
    {
        let mut links = app.relay.links.lock().unwrap();
        links.refused.insert("pat".to_owned());
        links.fail_sign_in.insert("pat".to_owned());
    }
    let token = bearer(None);
    let session = JazzSession::open(client_files(), app.session(&token).initial(Initial::Device))
        .await
        .unwrap();
    let guest = session.snapshot().user.unwrap();
    *token.lock().unwrap() = Some("token-pat".to_owned());
    assert!(session.refresh().await.is_err());
    // The guest's client is as it was.
    let failed = session.snapshot();
    assert_eq!(failed.user.unwrap(), guest);
    assert_eq!(failed.status, SessionStatus::Ready);
    assert_eq!(app.relay.links.lock().unwrap().attempts, 1);

    session.refresh().await.unwrap();
    let pat = session.snapshot().user.unwrap();
    assert_eq!(pat.subject, "pat");
    assert_ne!(pat.account, guest.account);
    assert_eq!(app.relay.links.lock().unwrap().attempts, 1);
    session.close().await.unwrap();
    app.stop().await;
}

/// A manual link that went through, but whose reopen on the bearer failed,
/// is retried by signing the linked user in, not by linking again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_after_a_link_whose_reopen_failed_signs_the_user_in() {
    let app = App::start().await;
    let token = bearer(None);
    let session = JazzSession::open(
        client_files(),
        app.session(&token)
            .initial(Initial::Device)
            .link_guest(false),
    )
    .await
    .unwrap();
    let guest = session.snapshot().user.unwrap();
    *token.lock().unwrap() = Some("token-quin".to_owned());
    app.relay
        .links
        .lock()
        .unwrap()
        .fail_sign_in
        .insert("quin".to_owned());
    assert!(session.link_device_account().await.is_err());
    assert_eq!(session.snapshot().status, SessionStatus::Error);

    session.retry().await.unwrap();
    let quin = session.snapshot().user.unwrap();
    assert_eq!(
        (quin.subject.as_str(), quin.account),
        ("quin", guest.account)
    );
    assert_eq!(session.snapshot().status, SessionStatus::Ready);
    session.close().await.unwrap();
    app.stop().await;
}

/// The session's snapshot once `done` holds.
async fn snapshot_until(
    session: &JazzSession,
    done: impl Fn(&jazz_app_support::SessionSnapshot) -> bool,
) -> jazz_app_support::SessionSnapshot {
    let mut changes = session.changes();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(snapshot) = changes.next().await {
            if done(&snapshot) {
                return snapshot;
            }
        }
        panic!("the session's changes ended");
    })
    .await
    .unwrap_or_else(|_| panic!("the session stays {:?}", session.snapshot()))
}

async fn connected(jazz: &Jazz) {
    let mut changes = jazz.sync_changes();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(status) = changes.next().await {
            if status.link == jazz_app_support::Link::Connected {
                return;
            }
        }
    })
    .await
    .expect("the client connects");
}

/// An ordinary logout in the app's own auth: the client finds no one at a
/// renewal or refresh and ends by itself. The session follows it.
async fn follows_a_client_that_ended(by_refresh: bool) {
    let app = App::start().await;
    let token = bearer(Some("token-alice"));
    let session = JazzSession::open(client_files(), app.session(&token))
        .await
        .unwrap();
    let before = session.snapshot();
    let old = session.jazz().unwrap();
    connected(&old).await;

    *token.lock().unwrap() = None;
    if by_refresh {
        old.session().unwrap().refresh().await.unwrap();
    } else {
        app.relay.refuse("token-alice", "token expired");
        app.relay.kick();
    }
    let out = snapshot_until(&session, |s| s.status == SessionStatus::SignedOut).await;
    assert!(out.client_epoch > before.client_epoch, "{out:?}");
    assert_eq!(out.user, None);
    assert!(matches!(session.jazz(), Err(Error::NoUser { .. })));
    let read = old.all(todos::query()).await;
    assert!(matches!(read, Err(Error::Replaced)), "{read:?}");

    // The same user again: a client that works, and stays.
    *token.lock().unwrap() = Some("token-alice.2".to_owned());
    session.refresh().await.unwrap();
    let back = session.snapshot();
    assert_eq!(back.status, SessionStatus::Ready);
    assert_eq!(
        back.user.as_ref().map(|user| user.subject.as_str()),
        Some("alice")
    );
    session.jazz().unwrap().all(todos::query()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(session.snapshot().client_epoch, back.client_epoch);
    assert_eq!(session.snapshot().status, SessionStatus::Ready);

    session.close().await.unwrap();
    assert_eq!(session.snapshot().status, SessionStatus::Closed);
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_follows_its_client_signed_out_at_a_renewal() {
    follows_a_client_that_ended(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_follows_its_client_signed_out_at_a_refresh() {
    follows_a_client_that_ended(true).await;
}

/// The client ends while the session is already following the same user:
/// following doesn't keep the dead client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn following_the_same_user_needs_a_client_that_lives() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let app = App::start().await;
    let token = bearer(Some("token-alice"));
    let hold = Arc::new(AtomicBool::new(false));
    let (asked, answer) = (
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(tokio::sync::Notify::new()),
    );
    let provider = {
        let (token, hold) = (token.clone(), hold.clone());
        let (asked, answer) = (asked.clone(), answer.clone());
        move |_: TokenReason| {
            let held = hold.swap(false, Ordering::SeqCst);
            let token = token.lock().unwrap().clone();
            let (asked, answer) = (asked.clone(), answer.clone());
            async move {
                if held {
                    asked.notify_one();
                    answer.notified().await;
                    return Ok(Some("token-alice.2".to_owned()));
                }
                Ok(token)
            }
        }
    };
    let config = SessionConfig::new(app.app_id)
        .server(&app.relay.endpoint)
        .bearer(provider);
    let session = JazzSession::open(client_files(), config).await.unwrap();
    let old = session.jazz().unwrap();
    let epoch = session.snapshot().client_epoch;

    hold.store(true, Ordering::SeqCst);
    let following = tokio::spawn({
        let session = session.clone();
        async move { session.refresh().await }
    });
    asked.notified().await;
    *token.lock().unwrap() = None;
    old.session().unwrap().refresh().await.unwrap();
    *token.lock().unwrap() = Some("token-alice.2".to_owned());
    answer.notify_one();
    following.await.unwrap().unwrap();

    let back = session.snapshot();
    assert_eq!(back.status, SessionStatus::Ready);
    assert!(back.client_epoch > epoch, "kept the dead client: {back:?}");
    session.jazz().unwrap().all(todos::query()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(session.snapshot(), back, "the old client's end changed it");

    session.close().await.unwrap();
    app.stop().await;
}

/// A guest's client that stops signs the session out, as signing out does:
/// the key is kept to reopen, and no account is made.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_guests_key_outlives_its_client() {
    let app = App::start().await;
    let token = bearer(None);
    let config = app.session(&token).initial(Initial::Device);
    let session = JazzSession::open(client_files(), config).await.unwrap();
    let guest = session.snapshot().user.unwrap().account;
    let key = session.export_device_key().unwrap();

    session.jazz().unwrap().shutdown().await.unwrap();
    snapshot_until(&session, |s| s.status == SessionStatus::SignedOut).await;
    assert_eq!(session.export_device_key().unwrap(), key);
    session.reopen_device_account().await.unwrap();
    assert_eq!(session.snapshot().user.unwrap().account, guest);

    session.close().await.unwrap();
    app.stop().await;
}
