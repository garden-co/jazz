//! End-to-end encryption through the Rust client, against a real server: a
//! device app (an end user with a private store it supplies) and a backend
//! (which relays and serves, and holds no keys).
//!
//! The app's Jazz endpoint is the stand-in in `relay/`; the core is an
//! in-process `JazzServer`.
#![cfg(feature = "e2ee-client")]

#[allow(dead_code)]
#[path = "bindings/encrypted.rs"]
mod bindings;
#[path = "fixtures/encrypted/omitted_columns.rs"]
mod omitted_columns;
#[allow(dead_code)]
mod relay;

use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use jazz_app_support::jazz::query::Query;
use jazz_app_support::jazz::tools::{ObjectId, ReadTier, TableName, Value};
use jazz_app_support::{
    AppFiles, AppId, Auth, Backend, Config, DeviceKey, EncryptionConfig, Error, Jazz, TokenReason,
};
use jazz_e2ee_client::MemoryStore;
use jazz_server::JazzServer;
use serde_json::json;

const DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/encrypted");

fn backend_files() -> AppFiles {
    AppFiles::read(DIR).unwrap()
}

fn client_files() -> AppFiles {
    AppFiles::new(std::fs::read(format!("{DIR}/jazz.app.json")).unwrap(), None)
}

struct App {
    app_id: AppId,
    server: JazzServer,
    backend: Jazz,
    relay: relay::Relay,
}

impl App {
    /// A server, the app's backend (which holds no keys) and its endpoint.
    async fn start() -> Self {
        Self::start_with(backend_files()).await
    }

    async fn start_with(files: AppFiles) -> Self {
        let app_id = AppId::random();
        let server = JazzServer::builder()
            .with_app_id(app_id)
            .start()
            .await
            .expect("start a Jazz server");
        Self::on_server(files, server).await
    }

    async fn on_server(files: AppFiles, server: JazzServer) -> Self {
        let app_id = server.app_id();
        let backend = Jazz::open(
            files,
            Config::new(app_id)
                .server(server.base_url())
                .backend(Backend::new(
                    server.backend_secret(),
                    server.admin_secret(),
                    "service",
                )),
        )
        .await
        .expect("an encrypted app opens as a backend, which holds no keys");
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

    fn user(&self, key: DeviceKey) -> Config {
        Config::new(self.app_id)
            .server(&self.relay.endpoint)
            .user(Auth::Device(key))
    }

    /// An end user's device, with a private store of its own.
    async fn device(&self) -> Jazz {
        self.device_of(DeviceKey::generate().unwrap()).await
    }

    /// A device of the account `key` is, with a private store of its own.
    async fn device_of(&self, key: DeviceKey) -> Jazz {
        let config = self
            .user(key)
            .encryption(EncryptionConfig::store(|| Rc::new(MemoryStore::new())));
        Jazz::open(client_files(), config)
            .await
            .expect("open as a device with a private store")
    }

    async fn stop(self) {
        self.backend.shutdown().await.unwrap();
        self.server.shutdown().await;
    }
}

fn text(value: &str) -> Value {
    Value::Text(value.to_owned())
}

fn uuid(value: &str) -> Value {
    Value::Uuid(ObjectId::from_uuid(value.parse().unwrap()))
}

/// A project of `owner`'s, with `owner` its first member in the same
/// transaction (the policy of a space's first member row: its creator, in the
/// transaction that makes the space row).
async fn make_project(owner: &Jazz, name: &str) -> String {
    let account = owner.account().expect("a signed-in user").0;
    let name = name.to_owned();
    let id = owner
        .with_client(move |client| async move {
            let tx = client.begin_transaction()?;
            let (id, _, _) = tx.insert(
                "projects",
                HashMap::from([
                    ("name".to_owned(), Value::Text(name)),
                    (
                        "ownerId".to_owned(),
                        Value::Uuid(ObjectId::from_uuid(account)),
                    ),
                ]),
            )?;
            let at = client.table_identity("projects")?;
            tx.insert(
                "__e2ee_members",
                HashMap::from([
                    ("scopeId".to_owned(), Value::Uuid(at)),
                    ("identifier".to_owned(), Value::Uuid(id)),
                    (
                        "accountId".to_owned(),
                        Value::Uuid(ObjectId::from_uuid(account)),
                    ),
                ]),
            )?;
            let written = tx.commit()?;
            client
                .wait_for_transaction(written, jazz_app_support::DurabilityTier::GlobalServer)
                .await?;
            Ok(id)
        })
        .await
        .unwrap();
    id.uuid().to_string()
}

/// Makes `account` a member of the project `id`: the app's own row, under
/// the app's own permissions.
async fn add_member(owner: &Jazz, project: &str, account: &str) {
    let at = owner
        .encryption()
        .call("spaceRow", json!({ "table": "projects", "id": project }))
        .await
        .unwrap();
    let (scope, identifier) = (
        at["scopeId"].as_str().unwrap().to_owned(),
        at["identifier"].as_str().unwrap().to_owned(),
    );
    let account = account.to_owned();
    owner
        .with_client(move |client| async move {
            let written = client.insert(
                "__e2ee_members",
                HashMap::from([
                    ("scopeId".to_owned(), uuid(&scope)),
                    ("identifier".to_owned(), uuid(&identifier)),
                    ("accountId".to_owned(), uuid(&account)),
                ]),
            )?;
            client
                .wait_for_transaction(
                    written.2.unwrap(),
                    jazz_app_support::DurabilityTier::GlobalServer,
                )
                .await?;
            Ok(())
        })
        .await
        .unwrap();
}

/// Writes an encrypted task, waiting for the space's key as a typed write
/// does.
async fn add_task(jazz: &Jazz, project: &str, title: &str) -> Result<(), Error> {
    let (project, title) = (project.to_owned(), title.to_owned());
    let title_again = title.clone();
    jazz.with_client(move |client| async move {
        let cells = HashMap::from([
            ("projectId".to_owned(), uuid(&project)),
            ("title".to_owned(), Value::Text(title)),
        ]);
        let (_, values, written) =
            jazz_app_support::with_key_wait(|| client.insert("tasks", cells.clone())).await?;
        // The row an insert returns has the plaintext, not the sealed bytes.
        assert!(
            values
                .iter()
                .any(|value| matches!(value, Value::Text(text) if text == &title_again)),
            "the inserted row's values: {values:?}"
        );
        let written = written.unwrap();
        client
            .wait_for_transaction(written, jazz_app_support::DurabilityTier::GlobalServer)
            .await?;
        Ok(())
    })
    .await
}

/// The titles of the project's tasks that `jazz` reads from the server, once
/// there are `count`.
async fn titles(jazz: &Jazz, count: usize) -> Vec<String> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let rows = jazz
                .with_client(|client| async move {
                    Ok(client.query(Query::from("tasks"), ReadTier::Remote).await?)
                })
                .await
                .unwrap_or_default();
            let mut titles: Vec<String> = rows
                .iter()
                .filter_map(|row| {
                    row.fields
                        .iter()
                        .find_map(|field| match (&*field.name, &field.value) {
                            ("title", Value::Text(title)) => Some(title.clone()),
                            _ => None,
                        })
                })
                .collect();
            if titles.len() >= count {
                titles.sort();
                break titles;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the rows reach this device and open")
}

/// Alice and Bob sign in on the same installation. Each opens their own local
/// rows and encryption keys, including when Alice returns after Bob signs out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signing_in_opens_the_same_accounts_rows_and_keys() {
    use bindings::{NewNote, notes, tasks};
    use std::sync::mpsc;

    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    // Serve one app-supplied private store across all three client threads.
    let (requests, receiver) = mpsc::channel::<(String, mpsc::Sender<(bool, String)>)>();
    let private_store = std::thread::spawn(move || {
        let serve = jazz_e2ee_client::host::bridge::serve(Rc::new(MemoryStore::new()));
        for (request, reply) in receiver {
            reply
                .send(futures::executor::block_on(serve(request)))
                .unwrap();
        }
    });
    let store = EncryptionConfig::store_calls(move |request| {
        let (reply, answer) = mpsc::channel();
        requests.send((request, reply)).unwrap();
        answer.recv().unwrap()
    });
    let mut devices = HashMap::new();
    for user in ["alice", "bob", "alice"] {
        let config = Config::new(app.app_id)
            .server(&app.relay.endpoint)
            .user(Auth::bearer(move |_| async move {
                Ok(Some(format!("token-{user}")))
            }))
            .storage(jazz_app_support::Storage::Dir(dir.path().into()))
            .encryption(store.clone());
        let jazz = Jazz::open(client_files(), config).await.unwrap();
        assert_eq!(
            jazz.encryption()
                .ready(Duration::from_secs(30))
                .await
                .unwrap(),
            "active"
        );
        let device = jazz.encryption().call("deviceId", json!({})).await.unwrap();
        assert!(
            dir.path()
                .join(jazz.account().unwrap().0.to_string())
                .join("jazz.sqlite")
                .is_file()
        );

        let returning = devices.contains_key(user);
        if let Some(original) = devices.get(user) {
            assert_eq!(original, &device);
        } else {
            assert!(!devices.values().any(|id| id == &device));
            devices.insert(user, device);
            let project = make_project(&jazz, user).await;
            add_task(&jazz, &project, user).await.unwrap();
        }
        jazz.disconnect().await.unwrap();
        let opened = jazz
            .all_with(tasks::query(), ReadTier::LocalOnly)
            .await
            .unwrap();
        assert_eq!(
            opened
                .iter()
                .map(|row| row.title.as_str())
                .collect::<Vec<_>>(),
            [user]
        );
        let local = jazz
            .all_with(notes::query(), ReadTier::LocalOnly)
            .await
            .unwrap();
        if returning {
            assert_eq!(
                local
                    .iter()
                    .map(|row| row.text.as_str())
                    .collect::<Vec<_>>(),
                [user]
            );
        } else {
            assert!(local.is_empty());
            jazz.insert(NewNote::new(user)).await.unwrap();
        }
        jazz.shutdown().await.unwrap();
    }
    drop(store);
    private_store.join().unwrap();
    app.stop().await;
}

/// Contract: two members, each a device with a private store of its own,
/// share an encrypted space and read each other's encrypted rows through the
/// Rust client. The first write of a space makes its key (the write waits for
/// it); a member that the owner delivers to reads, and writes in turn.
///
/// ```text
/// alice ──project + member rows──► server ◄── bob (member)
/// alice ──encrypted task (space key made)──► server
/// alice ──keys.deliver──► bob reads it ──encrypted reply──► alice reads it
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_members_share_a_space_and_read_each_others_encrypted_rows() {
    let app = App::start().await;
    let alice = app.device().await;
    let bob = app.device().await;
    assert_eq!(
        alice
            .encryption()
            .ready(Duration::from_secs(30))
            .await
            .unwrap(),
        "active"
    );
    assert_eq!(
        bob.encryption()
            .ready(Duration::from_secs(30))
            .await
            .unwrap(),
        "active"
    );

    let project = make_project(&alice, "plans").await;
    add_member(&alice, &project, &bob.account().unwrap().0.to_string()).await;
    add_task(&alice, &project, "alice's secret").await.unwrap();
    // Her first write made the space's key; she serves her member now.
    alice
        .encryption()
        .call("keysDeliver", json!({}))
        .await
        .unwrap();
    assert_eq!(titles(&bob, 1).await, ["alice's secret"]);
    add_task(&bob, &project, "bob's reply").await.unwrap();
    assert_eq!(titles(&alice, 2).await, ["alice's secret", "bob's reply"]);

    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
    app.stop().await;
}

/// Contract: a backend is never an encryption participant. It relays the
/// devices' ciphertext and works on plain tables, but it cannot write an
/// encrypted column, and no plaintext of one reaches it; its encryption
/// handle refuses use.
///
/// ```text
/// alice ──encrypted task──► server ──► backend (keyless)
///                                       │ reads the row without `title`
///                                       │ writes notes ✓
///                                       └ writes tasks.title ✗ (refused)
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backend_relays_and_serves_but_cannot_write_an_encrypted_column() {
    let app = App::start().await;
    let alice = app.device().await;
    alice
        .encryption()
        .ready(Duration::from_secs(30))
        .await
        .unwrap();
    let project = make_project(&alice, "plans").await;
    add_task(&alice, &project, "PLAINTEXT-ONLY-ALICE-KNOWS")
        .await
        .unwrap();

    // The backend reads the row's plain columns, never the encrypted one: a
    // read that selects it is refused, naming the pattern.
    let selecting = app
        .backend
        .with_client(|client| async move {
            Ok(client.query(Query::from("tasks"), ReadTier::Remote).await?)
        })
        .await;
    let error = selecting.expect_err("selecting an encrypted column is refused");
    assert!(error.to_string().contains("title"), "{error}");
    let seen = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let rows = app
                .backend
                .with_client(|client| async move {
                    let query = Query::from("tasks").select(["projectId", "label"]);
                    Ok(client.query(query, ReadTier::Remote).await?)
                })
                .await
                .unwrap_or_else(|error| panic!("the backend's read failed: {error}"));
            if let Some(row) = rows.into_iter().next() {
                break row;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the backend reads alice's row");
    assert_eq!(seen.fields.len(), 2, "only the plain columns it selected");
    // A plain table works as usual; an encrypted column is refused.
    app.backend
        .with_client(|client| async move {
            client.insert(
                "notes",
                HashMap::from([("text".to_owned(), text("from the backend"))]),
            )?;
            Ok(())
        })
        .await
        .expect("a backend writes plain data");
    let project_id = project.clone();
    let refused = app
        .backend
        .with_client(move |client| async move {
            client.insert(
                "tasks",
                HashMap::from([
                    ("projectId".to_owned(), uuid(&project_id)),
                    ("title".to_owned(), text("BACKEND-PLAINTEXT")),
                ]),
            )?;
            Ok(())
        })
        .await
        .expect_err("a backend cannot write an encrypted column");
    assert!(
        refused.to_string().contains("holds no encryption keys"),
        "{refused}"
    );

    // Its encryption handle refuses use: it has no account, device or keys.
    match app.backend.encryption().ready(Duration::from_secs(1)).await {
        Err(Error::Encryption { code, .. }) => assert_eq!(code, "keyless"),
        other => panic!("expected a refusal, got {other:?}"),
    }

    alice.shutdown().await.unwrap();
    app.stop().await;
}

/// Contract: a device app opens an app that declares encryption only with a
/// private store it supplies; without one, opening fails with an error naming
/// the option. No store ships.
///
/// ```text
/// device (no store) ──open──► ✗ names Config::encryption
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_device_app_opens_an_encrypted_app_only_with_a_store() {
    let app = App::start().await;
    let config = app.user(DeviceKey::generate().unwrap());
    match Jazz::open(client_files(), config).await {
        Err(Error::App(message)) => {
            assert!(
                message.contains("declares end-to-end encryption"),
                "{message}"
            );
            assert!(message.contains("Config::encryption"), "{message}");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    app.stop().await;
}

/// Contract: the encryption handle is always there, and refuses use for an
/// app that declares no encryption.
///
/// ```text
/// plain app ──encryption().ready()──► ✗ not declared
/// ```
#[tokio::test]
async fn the_encryption_handle_refuses_an_app_that_declares_none() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/todos");
    let jazz = Jazz::open(AppFiles::read(dir).unwrap(), Config::new(AppId::random()))
        .await
        .unwrap();
    match jazz.encryption().ready(Duration::from_secs(1)).await {
        Err(Error::Encryption { code, .. }) => assert_eq!(code, "not-declared"),
        other => panic!("expected a refusal, got {other:?}"),
    }
    match jazz.encryption().call("deviceId", json!({})).await {
        Err(Error::Encryption { code, .. }) => assert_eq!(code, "not-declared"),
        other => panic!("expected a refusal, got {other:?}"),
    }
    jazz.shutdown().await.unwrap();
}

/// An account the app signs in itself (a bearer), on a device with a private
/// store of its own.
async fn bearer_device(app: &App, user: &str) -> Jazz {
    let token = format!("token-{user}");
    let config = Config::new(app.app_id)
        .server(&app.relay.endpoint)
        .user(Auth::bearer(move |_: TokenReason| {
            let token = token.clone();
            async move { Ok(Some(token)) }
        }))
        .encryption(EncryptionConfig::store(|| Rc::new(MemoryStore::new())));
    Jazz::open(client_files(), config)
        .await
        .expect("open as a signed-in user with a private store")
}

/// Contract: an account the app signs in itself recovers on a new device with
/// its recovery phrase (24 words): `recoveryCreate` makes it, `recoveryRegister`
/// registers it from a device that is active, and `recoveryRestore` approves a
/// new device with it. A wrong phrase is refused, naming why, and approves
/// nothing. A device account needs none of this (its secret is the recovery).
///
/// ```text
/// laptop ──recoveryCreate/Register(phrase)──► server
/// phone (new device, awaiting approval) ──recoveryRestore(phrase)──► active
/// laptop ──keysDeliver──► phone reads what the laptop wrote
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_in_account_recovers_a_new_device_with_its_phrase() {
    let app = App::start().await;
    let laptop = bearer_device(&app, "alice").await;
    assert_eq!(
        laptop
            .encryption()
            .ready(Duration::from_secs(30))
            .await
            .unwrap(),
        "active"
    );
    let project = make_project(&laptop, "plans").await;
    add_task(&laptop, &project, "kept safe").await.unwrap();
    let phrase = laptop
        .encryption()
        .call("recoveryCreate", json!({}))
        .await
        .unwrap()["phrase"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(phrase.split(' ').count(), 24);
    laptop
        .encryption()
        .call("recoveryRegister", json!({ "phrase": phrase }))
        .await
        .unwrap();

    // The same account on a new device: no other device has approved it.
    let phone = bearer_device(&app, "alice").await;
    assert_eq!(
        phone
            .encryption()
            .ready(Duration::from_secs(30))
            .await
            .unwrap(),
        "awaiting-approval"
    );
    // A phrase that is not the registered one approves nothing.
    let wrong = laptop
        .encryption()
        .call("recoveryCreate", json!({}))
        .await
        .unwrap()["phrase"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        phone
            .encryption()
            .call("recoveryRestore", json!({ "phrase": wrong }))
            .await
            .is_err()
    );
    match phone
        .encryption()
        .call("recoveryRestore", json!({ "phrase": "abandon ability" }))
        .await
    {
        Err(Error::Encryption { code, .. }) => assert_eq!(code, "invalid-length"),
        other => panic!("expected a refusal, got {other:?}"),
    }
    let status = phone
        .encryption()
        .call("recoveryRestore", json!({ "phrase": phrase }))
        .await
        .unwrap();
    assert_eq!(status, "active");
    laptop
        .encryption()
        .call("keysDeliver", json!({}))
        .await
        .unwrap();
    assert_eq!(titles(&phone, 1).await, ["kept safe"]);

    laptop.shutdown().await.unwrap();
    phone.shutdown().await.unwrap();
    app.stop().await;
}

/// Contract: the typed bindings the Rust back end of jazz-codegen makes for
/// the fixture are the committed ones (regenerate them with
/// `JAZZ_UPDATE_BINDINGS=1 cargo test -p jazz-app-support --features e2ee-client --test encryption`).
#[test]
fn the_committed_bindings_are_generated() {
    let file = std::fs::read(format!("{DIR}/jazz.app.json")).unwrap();
    let generated = jazz_codegen::generate(jazz_codegen::Lang::Rust, &file).unwrap();
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/bindings/encrypted.rs");
    if std::env::var_os("JAZZ_UPDATE_BINDINGS").is_some() {
        std::fs::write(path, &generated).unwrap();
        return;
    }
    let committed = std::fs::read_to_string(path).unwrap_or_default();
    assert!(committed == generated, "{path} is stale: regenerate it");
}

/// Contract: an app written against the generated bindings reads and writes
/// an encrypted column as an ordinary typed field: the typed insert waits for
/// the space's key and returns the row with its plaintext, and a typed read
/// with a filter on the encrypted column (and on the space) finds it.
///
/// ```text
/// alice ──NewTask { title }──► (space key made) ──► server
/// alice ──tasks::query().filter(project, title)──► Task { title }
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_bindings_read_and_write_an_encrypted_column() {
    use bindings::{NewTask, tasks};
    let app = App::start().await;
    let alice = app.device().await;
    alice
        .encryption()
        .ready(Duration::from_secs(30))
        .await
        .unwrap();
    let project = make_project(&alice, "plans").await;
    let project_id = ObjectId::from_uuid(project.parse().unwrap());

    let task = alice
        .insert(NewTask::new(project_id, "typed secret"))
        .await
        .expect("a typed write to an encrypted column waits for the space's key");
    assert_eq!(
        task.title, "typed secret",
        "the row has the plaintext, not sealed bytes"
    );
    let found = alice
        .all(
            tasks::query()
                .filter(tasks::project_id().eq(project_id))
                .filter(tasks::title().eq("typed secret")),
        )
        .await
        .expect("a typed read of an encrypted column");
    assert_eq!(found, vec![task]);

    alice.shutdown().await.unwrap();
    app.stop().await;
}

/// The first typed stream prepares the project key before polling its source.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_streams_seal_siblings_and_refuse_an_indexed_text_target() {
    use bindings::{NewTask, Task, tasks};
    let app = App::start().await;
    let alice = app.device().await;
    alice
        .encryption()
        .ready(Duration::from_secs(30))
        .await
        .unwrap();
    let project = make_project(&alice, "plans").await;
    let id = ObjectId::from_uuid(project.parse().unwrap());
    let chunks = || futures::stream::iter([Ok::<_, Error>(b"public label")]);
    let task = alice
        .insert_streaming(NewTask::new(id, "private title"), tasks::label(), chunks())
        .await
        .unwrap();
    let staged = alice
        .transaction(move |tx| async move {
            tx.insert_streaming(NewTask::new(id, "closure title"), tasks::label(), chunks())
                .await
        })
        .await
        .unwrap();
    let open = alice.begin_transaction().await.unwrap();
    let parked = open
        .insert_streaming(NewTask::new(id, "open title"), tasks::label(), chunks())
        .await
        .unwrap();
    open.commit().await.unwrap();
    for (task, title) in [
        (task, "private title"),
        (staged, "closure title"),
        (parked, "open title"),
    ] {
        let read = alice.get::<Task>(task.id).await.unwrap().unwrap();
        assert_eq!(read.title, title);
        assert_eq!(read.label.as_deref(), Some("public label"));
    }

    let unread = futures::stream::poll_fn(|_| -> std::task::Poll<Option<Result<Vec<u8>, Error>>> {
        panic!("polled a stream for an indexed, encrypted text column")
    });
    let refused = alice
        .insert_streaming(NewTask::new(id, ""), tasks::title(), unread)
        .await
        .unwrap_err();
    assert!(
        refused
            .to_string()
            .contains("only unindexed bytes columns stream"),
        "{refused}"
    );
    alice.shutdown().await.unwrap();
    app.stop().await;
}

/// A device opens the typed bytes it streamed. The authority stores the sealed record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_streams_round_trip_encrypted_bytes_and_store_them_sealed() {
    use bindings::{NewTask, Task, tasks};
    use jazz_app_support::{Column, jazz::tools::ColumnType};

    let files = omitted_columns::with_private_note(ColumnType::Bytea);
    let server = JazzServer::builder()
        .with_persistent_storage()
        .start()
        .await
        .unwrap();
    let app = App::on_server(
        AppFiles::new(files.app.clone(), Some(files.permissions.into_bytes())),
        server,
    )
    .await;
    let alice = Jazz::open(
        AppFiles::new(files.app, None),
        app.user(DeviceKey::generate().unwrap())
            .encryption(EncryptionConfig::store(|| Rc::new(MemoryStore::new()))),
    )
    .await
    .unwrap();
    alice
        .encryption()
        .ready(Duration::from_secs(30))
        .await
        .unwrap();
    let project = make_project(&alice, "plans").await;
    let id = ObjectId::from_uuid(project.parse().unwrap());
    struct Unread;
    impl std::io::Read for Unread {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            panic!("read the bytes before checking the plain label")
        }
    }
    let refused = alice
        .with_client(move |client| async move {
            let cells = HashMap::from([
                ("projectId".into(), Value::Uuid(id)),
                ("title".into(), text("private title")),
                ("label".into(), Value::Integer(4)),
            ]);
            Ok(client
                .insert_streaming_value("tasks", cells, "privateNote", Unread)
                .await?)
        })
        .await
        .unwrap_err();
    assert!(refused.to_string().contains("label"), "{refused}");

    let attachment = Column::<Task, Option<Vec<u8>>>::new("privateNote");
    let plain = b"attachment plaintext marker";
    let task = alice
        .insert_streaming(
            NewTask::new(id, "private title"),
            attachment,
            futures::stream::iter([Ok::<_, Error>(plain)]),
        )
        .await
        .unwrap();
    let read = alice
        .one(
            tasks::query()
                .id(task.id)
                .select((tasks::project_id(), attachment)),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.values.0, id);
    assert_eq!(read.values.1.as_deref(), Some(plain.as_slice()));
    alice.wait_for_sync().await.unwrap();

    let conn = rusqlite::Connection::open_with_flags(
        app.server.data_dir().join("jazz.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let (table, column): (String, String) = conn.query_row(
        "SELECT stored_tbl, stored_col FROM _jazz_names WHERE tbl = 'tasks' AND col = 'privateNote'",
        [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
    let sealed: Vec<u8> = conn
        .query_row(
            &format!(r#"SELECT "{column}" FROM "{table}" WHERE id = ?1"#),
            [task.id.uuid().as_bytes().as_slice()],
            |r| r.get(0),
        )
        .unwrap();
    assert!(sealed.len() > plain.len());
    assert!(!sealed.windows(plain.len()).any(|bytes| bytes == plain));
    alice.shutdown().await.unwrap();
    app.stop().await;
}

/// Alice's deploy refuses a nullability change and her original title stays readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deploy_refuses_to_make_a_required_encrypted_column_optional() {
    check_required_title(AppFiles::new(
        include_bytes!("fixtures/encrypted/jazz.optional-title.app.json").as_slice(),
        None,
    ))
    .await;
}

/// Alice's title keeps its encoding when a backend deploys another physical
/// schema hash for the same stored column without a migration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_standalone_deploy_cannot_make_a_stored_encrypted_title_optional() {
    let mut next: serde_json::Value =
        serde_json::from_slice(include_bytes!("fixtures/encrypted/jazz.app.json")).unwrap();
    // Changing the physical column order gives this deployment a new hash.
    next.sort_all_objects();
    next["tables"]["tasks"]["encryption"]["columns"]["title"] =
        json!({"type": "string?", "sealedType": "string"});
    check_required_title(AppFiles::new(serde_json::to_vec(&next).unwrap(), None)).await;
}

async fn check_required_title(files: AppFiles) {
    let app = App::start().await;
    let alice = app.device().await;
    alice
        .encryption()
        .ready(Duration::from_secs(30))
        .await
        .unwrap();
    let project = make_project(&alice, "plans").await;
    add_task(&alice, &project, "original title").await.unwrap();
    assert_eq!(titles(&alice, 1).await, ["original title"]);

    let error = Jazz::open(
        files.with_permissions(
            include_bytes!("fixtures/encrypted/jazz.permissions.json").as_slice(),
        ),
        Config::new(app.app_id)
            .server(app.server.base_url())
            .backend(Backend::new(
                app.server.backend_secret(),
                app.server.admin_secret(),
                "service",
            )),
    )
    .await
    .expect_err("a pinned sealed type cannot change the stored layout");
    let reason = error.to_string();
    assert!(
        reason.contains("tasks.title") && reason.contains("new column") && reason.contains("from"),
        "{reason}"
    );
    assert_eq!(titles(&alice, 1).await, ["original title"]);
    alice.shutdown().await.unwrap();
    app.stop().await;
}

/// Alice's first note in a project streams its label from a one-shot reader.
/// The encrypted title needs a space key before the label can be read.
///
/// ```text
/// begin → prepare the project key → begin again → read the label → one row
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rust_reader_waits_for_the_encrypted_metadata_key() {
    let app = App::start().await;
    let alice = app.device().await;
    alice
        .encryption()
        .ready(Duration::from_secs(30))
        .await
        .unwrap();
    let project = make_project(&alice, "plans").await;
    alice
        .with_client(move |client| async move {
            let cells = HashMap::from([
                ("projectId".to_owned(), uuid(&project)),
                ("title".to_owned(), text("private title")),
            ]);
            let (_, written) = client
                .insert_streaming_value(
                    "tasks",
                    cells,
                    "label",
                    std::io::Cursor::new(b"streamed label"),
                )
                .await?;
            client
                .wait_for_transaction(written, jazz_app_support::DurabilityTier::GlobalServer)
                .await?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(titles(&alice, 1).await, ["private title"]);
    let tasks = alice.all(bindings::tasks::query()).await.unwrap();
    assert_eq!(tasks[0].label.as_deref(), Some("streamed label"));
    alice.shutdown().await.unwrap();
}

/// Contract: an app written against the generated bindings manages a space's
/// members as typed rows: alice adds bob with a typed member insert, a typed
/// read of the members table lists the project's members, and bob, delivered
/// the space's key, reads her encrypted task.
///
/// ```text
/// alice ──NewE2eeMember { bob }──► server
/// alice ──e2ee_members::query().filter(identifier)──► [alice, bob]
/// alice ──encrypted task, keys.deliver──► bob reads it
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_bindings_add_and_list_a_spaces_members() {
    use bindings::{NewE2eeMember, e2ee_members};
    let app = App::start().await;
    let alice = app.device().await;
    let bob = app.device().await;
    for device in [&alice, &bob] {
        device
            .encryption()
            .ready(Duration::from_secs(30))
            .await
            .unwrap();
    }
    let project = make_project(&alice, "plans").await;
    let at = alice
        .encryption()
        .call("spaceRow", json!({ "table": "projects", "id": project }))
        .await
        .unwrap();
    let id =
        |value: &serde_json::Value| ObjectId::from_uuid(value.as_str().unwrap().parse().unwrap());
    let (scope, identifier) = (id(&at["scopeId"]), id(&at["identifier"]));
    let alice_account = ObjectId::from_uuid(alice.account().unwrap().0);
    let bob_account = ObjectId::from_uuid(bob.account().unwrap().0);

    let member = alice
        .durable(jazz_app_support::DurabilityTier::GlobalServer)
        .insert(NewE2eeMember::new(scope, identifier, bob_account))
        .await
        .expect("a typed insert of a member row");
    assert_eq!(
        (member.scope_id, member.identifier, member.account_id),
        (scope, identifier, bob_account)
    );
    let members = alice
        .all(
            e2ee_members::query()
                .filter(e2ee_members::scope_id().eq(scope))
                .filter(e2ee_members::identifier().eq(identifier)),
        )
        .await
        .expect("a typed read of the members table");
    let mut accounts: Vec<_> = members.iter().map(|row| row.account_id.uuid()).collect();
    accounts.sort();
    let mut expected = vec![alice_account.uuid(), bob_account.uuid()];
    expected.sort();
    assert_eq!(accounts, expected);
    assert!(members.contains(&member));

    add_task(&alice, &project, "for members").await.unwrap();
    alice
        .encryption()
        .call("keysDeliver", json!({}))
        .await
        .unwrap();
    assert_eq!(titles(&bob, 1).await, ["for members"]);

    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
    app.stop().await;
}

/// Alice omits two encrypted fields. The native insert returns the logical
/// default and null that she reads back from the stored row. The read keeps
/// a plain column whose name looks like a token for the unindexed field.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_insert_returns_omitted_encrypted_values() {
    let files = omitted_columns::app();
    let app = App::start_with(AppFiles::new(
        files.app.clone(),
        Some(files.permissions.into_bytes()),
    ))
    .await;
    let alice = Jazz::open(
        AppFiles::new(files.app, None),
        app.user(DeviceKey::generate().unwrap())
            .encryption(EncryptionConfig::store(|| Rc::new(MemoryStore::new()))),
    )
    .await
    .unwrap();
    alice
        .encryption()
        .ready(Duration::from_secs(30))
        .await
        .unwrap();
    let project = make_project(&alice, "plans").await;
    alice
        .with_client(move |client| async move {
            let cells = HashMap::from([("projectId".into(), uuid(&project))]);
            let (id, values, _) =
                jazz_app_support::with_key_wait(|| client.insert("tasks", cells.clone())).await?;
            let schema = client.schema()?;
            let table = &schema[&TableName::new("tasks")];
            let inserted: HashMap<_, _> = table
                .columns
                .columns
                .iter()
                .map(|column| column.name_str())
                .zip(values)
                .collect();
            assert_eq!(inserted["title"], text("untitled"));
            assert_eq!(inserted["privateNote"], Value::Null);
            assert_eq!(inserted["__e2ee_eq_privateNote"], text("plain note"));
            let rows = client
                .query(Query::from("tasks"), ReadTier::LocalFirst)
                .await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].key.row_id(), Some(id));
            let read: HashMap<_, _> = rows[0]
                .fields
                .iter()
                .map(|field| (field.name.as_str(), &field.value))
                .collect();
            assert!(!read.contains_key("__e2ee_eq_title"));
            for name in ["title", "privateNote", "__e2ee_eq_privateNote"] {
                assert_eq!(read.get(name).copied(), inserted.get(name));
            }
            Ok(())
        })
        .await
        .unwrap();
    alice.shutdown().await.unwrap();
    app.stop().await;
}

/// Alice can write and read an ordinary column even when its name matches
/// the equality-token prefix.
#[tokio::test]
async fn a_plain_column_with_the_token_prefix_is_projected() {
    use jazz_app_support::jazz::tools::{
        AppContext, ColumnType, JazzClient, SchemaBuilder, TableSchemaBuilder,
    };

    let name = "__e2ee_eq_label";
    let schema = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("notes").column(name, ColumnType::Text))
        .build();
    let alice = JazzClient::connect(AppContext::test(schema)).await.unwrap();
    let (_, inserted, _) = alice
        .insert("notes", HashMap::from([(name.into(), text("plans"))]))
        .unwrap();
    assert_eq!(inserted, vec![text("plans")]);
    for query in [Query::from("notes"), Query::from("notes").select([name])] {
        let rows = alice.query(query, ReadTier::LocalFirst).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert!(
            rows[0]
                .fields
                .iter()
                .any(|field| field.name == name && field.value == text("plans"))
        );
    }
    alice.shutdown().await.unwrap();
}

/// Contract: a device account recovers from its own account key. A second
/// device with the same key and a private store of its own is `active` at
/// once, with no other device approving it, and reads what the first wrote
/// once the first serves it; nothing is shown to the user and no phrase exists.
///
/// ```text
/// laptop (key K) ──ready: registers recovery from K──► server
/// phone  (key K, new store) ──ready: awaiting ► recovers with K──► active
/// laptop ──keysDeliver──► phone reads the laptop's task
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_device_account_recovers_a_new_device_from_its_own_key() {
    let app = App::start().await;
    let key = DeviceKey::generate().unwrap();
    let laptop = app.device_of(key.clone()).await;
    assert_eq!(
        laptop
            .encryption()
            .ready(Duration::from_secs(30))
            .await
            .unwrap(),
        "active"
    );
    let project = make_project(&laptop, "plans").await;
    add_task(&laptop, &project, "kept by the key")
        .await
        .unwrap();

    let phone = app.device_of(key).await;
    assert_eq!(
        phone
            .encryption()
            .ready(Duration::from_secs(30))
            .await
            .unwrap(),
        "active",
        "the account key is its recovery: no approval, no phrase"
    );
    laptop
        .encryption()
        .call("keysDeliver", json!({}))
        .await
        .unwrap();
    assert_eq!(titles(&phone, 1).await, ["kept by the key"]);

    laptop.shutdown().await.unwrap();
    phone.shutdown().await.unwrap();
    app.stop().await;
}

/// Contract: the start-up registration a device account's host runs
/// (`recoveryRegister` with `automatic`) takes only the secret of the
/// identity the device was opened as, like `start_device_account`: another
/// identity's secret is refused with `recovery-root-mismatch` and registers
/// no root, while the device's own key registers once.
///
/// ```text
/// laptop (key K) ──ready──► root of K
/// laptop ──recoveryRegister { secret: mallory's, automatic }──✗ recovery-root-mismatch
/// laptop ──recoveryList──► still only the root of K
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_device_accounts_automatic_registration_refuses_another_identitys_secret() {
    let app = App::start().await;
    let laptop = app.device_of(DeviceKey::generate().unwrap()).await;
    assert_eq!(
        laptop
            .encryption()
            .ready(Duration::from_secs(30))
            .await
            .unwrap(),
        "active"
    );
    let roots = || async {
        laptop
            .encryption()
            .call("recoveryList", json!({}))
            .await
            .unwrap()
    };
    let before = roots().await;
    assert_eq!(before.as_array().map(Vec::len), Some(1), "{before}");

    // Not the laptop's key (that one is random): another identity's secret.
    let mallory = "5f".repeat(32);
    match laptop
        .encryption()
        .call(
            "recoveryRegister",
            json!({ "secret": mallory, "automatic": true }),
        )
        .await
    {
        Err(Error::Encryption { code, .. }) => assert_eq!(code, "recovery-root-mismatch"),
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert_eq!(roots().await, before, "no root registered for it");

    laptop.shutdown().await.unwrap();
    app.stop().await;
}

/// A private store with a hook that can fail or hold selected requests.
fn store_with_hook(
    hook: impl Fn(serde_json::Value) -> futures::future::LocalBoxFuture<'static, Option<String>>
    + Send
    + Sync
    + 'static,
) -> EncryptionConfig {
    use jazz_e2ee_client::host::bridge::{JsStore, serve};

    let hook = std::sync::Arc::new(hook);
    EncryptionConfig::store(move || {
        let hook = hook.clone();
        let store = serve(Rc::new(MemoryStore::new()));
        Rc::new(JsStore::served_by(Rc::new(move |request| {
            let intercepted = hook(serde_json::from_str(&request).unwrap());
            let store = store.clone();
            Box::pin(async move {
                match intercepted.await {
                    Some(error) => (false, error),
                    None => store(request).await,
                }
            })
        })))
    })
}

/// Updating the stored device, after creation, marks its recovery nonce
/// spent before publishing the device's approval.
fn saving_recovery_nonce(request: &serde_json::Value) -> bool {
    request["op"] == "save" && request["doc"] == "device" && request["expected"].is_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_reports_the_store_failure_that_stopped_recovery() {
    let app = App::start().await;
    let key = DeviceKey::generate().unwrap();
    let laptop = app.device_of(key.clone()).await;
    assert_eq!(
        laptop
            .encryption()
            .ready(Duration::from_secs(30))
            .await
            .unwrap(),
        "active"
    );
    let store = store_with_hook(|request| {
        Box::pin(async move {
            saving_recovery_nonce(&request).then(|| "cannot save recovery nonce".to_owned())
        })
    });
    let phone = Jazz::open(client_files(), app.user(key).encryption(store))
        .await
        .unwrap();
    let error = phone
        .encryption()
        .ready(Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Encryption { code, message }
            if code == "store" && message.contains("cannot save recovery nonce")
    ));
    let reports = phone.encryption().take_reports().await.unwrap();
    assert!(reports.iter().any(|report| {
        let report: serde_json::Value = serde_json::from_str(report).unwrap();
        report["type"] == "ready-failed" && report["code"] == "store"
    }));

    phone.shutdown().await.unwrap();
    laptop.shutdown().await.unwrap();
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_has_its_own_deadline_while_startup_recovers_the_device() {
    use std::sync::Arc;
    use tokio::sync::Notify;

    let app = App::start().await;
    let key = DeviceKey::generate().unwrap();
    let laptop = app.device_of(key.clone()).await;
    assert_eq!(
        laptop
            .encryption()
            .ready(Duration::from_secs(30))
            .await
            .unwrap(),
        "active"
    );
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let store = store_with_hook({
        let entered = entered.clone();
        let release = release.clone();
        move |request| {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                if saving_recovery_nonce(&request) {
                    entered.notify_one();
                    release.notified().await;
                }
                None
            })
        }
    });
    let phone = Jazz::open(client_files(), app.user(key).encryption(store))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), entered.notified())
        .await
        .expect("startup reaches the private store before the short call");
    let encryption = phone.encryption();
    let short = tokio::time::timeout(
        Duration::from_secs(1),
        encryption.ready(Duration::from_millis(20)),
    )
    .await;
    release.notify_one();
    assert!(matches!(
        short,
        Ok(Err(Error::Encryption { code, .. })) if code == "timeout"
    ));
    assert_eq!(
        encryption.ready(Duration::from_secs(15)).await.unwrap(),
        "active",
        "the timed-out caller leaves the startup recovery running"
    );

    phone.shutdown().await.unwrap();
    laptop.shutdown().await.unwrap();
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_encrypted_write_reports_a_failure_to_store_its_space_key() {
    let app = App::start().await;
    let store = store_with_hook(|request| {
        Box::pin(async move {
            (request["op"] == "save"
                && request["doc"]
                    .as_str()
                    .is_some_and(|doc| doc.starts_with("held-")))
            .then(|| "cannot store the space key".to_owned())
        })
    });
    let config = app.user(DeviceKey::generate().unwrap()).encryption(store);
    let alice = Jazz::open(client_files(), config).await.unwrap();
    alice
        .encryption()
        .ready(Duration::from_secs(30))
        .await
        .unwrap();
    let project = make_project(&alice, "plans").await;
    let error = add_task(&alice, &project, "unwritten").await.unwrap_err();
    assert!(matches!(
        error,
        Error::Encryption { code, message }
            if code == "store" && message.contains("cannot store the space key")
    ));

    alice.shutdown().await.unwrap();
    app.stop().await;
}

/// Contract: a private store that never answers fails the call waiting on
/// it at the store-call deadline (30 s) instead of holding the runtime for
/// good: preparing a space reports a `store` failure, and the client then
/// closes.
///
/// ```text
/// alice ──prepareSpace──► runtime ──save the space key──► store (silent)
///       ◄── store: the private store did not answer `save` in time
/// alice ──shutdown──► closed
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_store_that_never_answers_fails_the_call_and_the_client_closes() {
    use std::sync::Arc;
    use tokio::sync::Notify;

    let app = App::start().await;
    let entered = Arc::new(Notify::new());
    let store = store_with_hook({
        let entered = entered.clone();
        move |request| {
            let entered = entered.clone();
            Box::pin(async move {
                if request["op"] == "save"
                    && request["doc"]
                        .as_str()
                        .is_some_and(|doc| doc.starts_with("held-"))
                {
                    entered.notify_one();
                    std::future::pending::<()>().await;
                }
                None
            })
        }
    });
    let config = app.user(DeviceKey::generate().unwrap()).encryption(store);
    let alice = Jazz::open(client_files(), config).await.unwrap();
    alice
        .encryption()
        .ready(Duration::from_secs(30))
        .await
        .unwrap();
    let project = make_project(&alice, "plans").await;
    let space = alice
        .encryption()
        .call("spaceRow", json!({ "table": "projects", "id": project }))
        .await
        .unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(150),
        alice.encryption().call("prepareSpace", space),
    )
    .await
    .expect("the store call ends at its deadline")
    .unwrap_err();
    assert!(
        matches!(
            &error,
            Error::Encryption { code, message }
                if code == "store" && message.contains("did not answer")
        ),
        "{error:?}"
    );
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .expect("the preparation reached the silent store");
    tokio::time::timeout(Duration::from_secs(30), alice.shutdown())
        .await
        .expect("the client closes")
        .unwrap();
    app.stop().await;
}
