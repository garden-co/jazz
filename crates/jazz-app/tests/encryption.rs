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
#[path = "relay/encrypted_app.rs"]
mod encrypted_app;
#[path = "fixtures/encrypted/omitted_columns.rs"]
mod omitted_columns;
#[allow(dead_code)]
mod relay;

use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use encrypted_app::EncryptedApp as App;
use jazz_app_support::jazz::query::Query;
use jazz_app_support::jazz::tools::{ObjectId, ReadTier, TableName, Value};
use jazz_app_support::{
    AppFiles, AppId, Auth, Backend, Config, DeviceKey, EncryptionConfig, Error, Jazz, TokenReason,
};
use jazz_e2ee_client::testing::{StoreWithSaveHook, answer_memory_store};
use jazz_e2ee_client::{MemoryStore, StoreError};
use jazz_server::JazzServer;
use serde_json::json;

const DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/encrypted");

fn backend_files() -> AppFiles {
    AppFiles::read(DIR).unwrap()
}

fn client_files() -> AppFiles {
    AppFiles::new(std::fs::read(format!("{DIR}/jazz.app.json")).unwrap(), None)
}

impl App {
    /// A server, the app's backend (which holds no keys) and its endpoint.
    async fn start() -> Self {
        Self::start_with(backend_files()).await
    }

    fn user(&self, key: DeviceKey) -> Config {
        Config::new(self.app_id)
            .server(&self.relay.endpoint)
            .user(Auth::Device(key))
    }

    /// An end user's device, with a private store of its own.
    async fn device(&self) -> Jazz {
        self.device_with(client_files()).await
    }

    async fn device_with(&self, files: AppFiles) -> Jazz {
        self.open_device(files, DeviceKey::generate().unwrap())
            .await
    }

    /// A device of the account `key` is, with a private store of its own.
    async fn device_of(&self, key: DeviceKey) -> Jazz {
        self.open_device(client_files(), key).await
    }

    async fn open_device(&self, files: AppFiles, key: DeviceKey) -> Jazz {
        let config = self
            .user(key)
            .encryption(EncryptionConfig::store(|| Rc::new(MemoryStore::new())));
        Jazz::open(files, config)
            .await
            .expect("open as a device with a private store")
    }
}

async fn ready(jazz: &Jazz) -> String {
    jazz.encryption()
        .ready(Duration::from_secs(30))
        .await
        .unwrap()
}

async fn deliver(jazz: &Jazz) {
    jazz.encryption()
        .call("keysDeliver", json!({}))
        .await
        .unwrap();
}

#[track_caller]
fn refused_with<T: std::fmt::Debug>(result: Result<T, Error>, expected: &str) {
    match result {
        Err(Error::Encryption { code, .. }) => assert_eq!(code, expected),
        other => panic!("expected {expected}, got {other:?}"),
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
            let at = client.table_identity("projects").await?;
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

/// One app-supplied store that successive clients can use from their own threads.
fn shared_private_store(store: MemoryStore) -> EncryptionConfig {
    EncryptionConfig::store_calls(move |request| answer_memory_store(&store, request))
}

/// Alice and Bob sign in on the same installation. Each opens their own local
/// rows and encryption keys, including when Alice returns after Bob signs out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signing_in_opens_the_same_accounts_rows_and_keys() {
    use bindings::{NewNote, notes, tasks};

    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let store = shared_private_store(MemoryStore::new());
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
        assert_eq!(ready(&jazz).await, "active");
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
        jazz.sign_out().await.unwrap();
    }
    app.stop().await;
}

/// Alice deletes her database after syncing an encrypted task. Her private
/// store keeps every document, so a fresh replica can decrypt the synced task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_client_storage_preserves_keys_and_decrypts_the_resynced_row() {
    use jazz_e2ee_client::PrivateStore;

    let app = App::start().await;
    let dir = tempfile::tempdir().unwrap();
    let store = MemoryStore::new();
    let encryption = shared_private_store(store.clone());
    let documents = async || {
        let mut docs = std::collections::BTreeMap::new();
        for scope in store.scopes() {
            for name in store.list(&scope).await.unwrap() {
                let document = store.load(&scope, &name).await.unwrap().unwrap();
                docs.insert((scope.clone(), name), document);
            }
        }
        docs
    };
    let config = Config::new(app.app_id)
        .server(&app.relay.endpoint)
        .user(Auth::bearer(|_| async {
            Ok(Some("token-alice".to_owned()))
        }))
        .storage(jazz_app_support::Storage::Dir(dir.path().into()))
        .encryption(encryption);
    let alice = Jazz::open(client_files(), config.clone()).await.unwrap();
    assert_eq!(ready(&alice).await, "active");
    let project = make_project(&alice, "plans").await;
    add_task(&alice, &project, "private draft").await.unwrap();
    let database = dir
        .path()
        .join(alice.account().unwrap().0.to_string())
        .join("jazz.sqlite");
    assert!(database.is_file());
    let before = documents().await;
    assert!(before.keys().any(|(_, name)| name.starts_with("held-")));

    alice.delete_client_storage().await.unwrap();
    assert!(!database.exists());
    assert_eq!(
        documents().await,
        before,
        "deletion changed the private store"
    );

    let reopened = Jazz::open(client_files(), config).await.unwrap();
    assert_eq!(ready(&reopened).await, "active");
    assert_eq!(titles(&reopened, 1).await, ["private draft"]);
    reopened.shutdown().await.unwrap();
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
    ready(&alice).await;
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
    refused_with(
        app.backend.encryption().ready(Duration::from_secs(1)).await,
        "keyless",
    );

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
    refused_with(
        jazz.encryption().ready(Duration::from_secs(1)).await,
        "not-declared",
    );
    refused_with(
        jazz.encryption().call("deviceId", json!({})).await,
        "not-declared",
    );
    refused_with(jazz.encryption().state().await, "not-declared");
    refused_with(jazz.encryption().take_reports().await, "not-declared");
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
    assert_eq!(ready(&laptop).await, "active");
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
    assert_eq!(ready(&phone).await, "awaiting-approval");
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
    refused_with(
        phone
            .encryption()
            .call("recoveryRestore", json!({ "phrase": "abandon ability" }))
            .await,
        "invalid-length",
    );
    let status = phone
        .encryption()
        .call("recoveryRestore", json!({ "phrase": phrase }))
        .await
        .unwrap();
    assert_eq!(status, "active");
    deliver(&laptop).await;
    assert_eq!(titles(&phone, 1).await, ["kept safe"]);

    laptop.shutdown().await.unwrap();
    phone.shutdown().await.unwrap();
    app.stop().await;
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
    ready(&alice).await;
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

/// Alice's application exception can quote a key refusal. Only a typed
/// refusal from the encryption hooks lets her device repeat the write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_quoting_a_key_refusal_is_returned_unchanged() {
    let app = App::start().await;
    let alice = app.device().await;
    alice.with_client(|_| async {
        let reason = "key-pending: space aa/bb is quoted by the application";
        let mut attempts = 0;
        let error = jazz_app_support::with_key_wait(|| {
            attempts += 1;
            Err::<(), _>(jazz::tools::JazzError::Write(reason.to_owned()))
        }).await.unwrap_err();
        assert!(
            matches!(error, Error::Jazz(jazz::tools::JazzError::Write(ref text)) if text == reason),
            "{error:?}"
        );
        assert_eq!(attempts, 1);
        Ok(())
    }).await.unwrap();
    alice.shutdown().await.unwrap();
    app.stop().await;
}

/// The first typed stream prepares the project key before polling its source.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_streams_seal_siblings_and_refuse_an_indexed_text_target() {
    use bindings::{NewTask, Task, tasks};
    let app = App::start().await;
    let alice = app.device().await;
    ready(&alice).await;
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
    let alice = app.device_with(AppFiles::new(files.app, None)).await;
    ready(&alice).await;
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
    ready(&alice).await;
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
        reason.contains("tasks.title") && reason.contains("copy it into a new encrypted table"),
        "{reason}"
    );
    assert_eq!(titles(&alice, 1).await, ["original title"]);
    alice.shutdown().await.unwrap();
    app.stop().await;
}

/// Alice opens the published app with a native codec for each supported encrypted type.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_device_opens_every_supported_encrypted_type_after_deployment() {
    let mut file: serde_json::Value =
        serde_json::from_slice(include_bytes!("fixtures/encrypted/jazz.app.json")).unwrap();
    let table = &mut file["tables"]["tasks"];
    for (name, ty) in [
        ("flag", json!("boolean")),
        ("count", json!("int")),
        ("large", json!("bigint")),
        ("amount", json!("float")),
        ("instant", json!("timestamp")),
        ("identifier", json!("uuid")),
        ("words", json!("string?")),
        ("blob", json!("bytes")),
        ("choice", json!({"enum":["a","b"]})),
        ("document", json!({"json":true})),
        (
            "constrained",
            json!({"json":{"type":"number","minimum":-1}}),
        ),
        (
            "groups",
            json!({"array":{"array":{"enum":["open","closed"]}}}),
        ),
    ] {
        table["columns"][name] = json!("bytes");
        table["columns"][format!("__e2ee_eq_{name}")] = json!("bytes");
        table["encryption"]["columns"][name] = ty;
        table["encryption"]["equality"]
            .as_array_mut()
            .unwrap()
            .push(json!(name));
    }
    let file = serde_json::to_vec(&file).unwrap();
    let app =
        App::start_with(AppFiles::new(file.clone(), None).with_permissions(
            include_bytes!("fixtures/encrypted/jazz.permissions.json").as_slice(),
        ))
        .await;
    let device = app.device_with(AppFiles::new(file, None)).await;
    ready(&device).await;
    device.shutdown().await.unwrap();
    app.stop().await;
}

/// Alice writes a task, upgrades to a schema with an empty stored cell for priority,
/// and reads the declared priority from the row she wrote before the migration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_older_task_reads_the_new_encrypted_columns_declared_default() {
    let app = App::start().await;
    let alice = app.device().await;
    ready(&alice).await;
    let project = make_project(&alice, "plans").await;
    add_task(&alice, &project, "before the migration")
        .await
        .unwrap();
    alice.shutdown().await.unwrap();
    app.backend.shutdown().await.unwrap();
    drop(app.relay);

    let mut file: serde_json::Value =
        serde_json::from_slice(include_bytes!("fixtures/encrypted/jazz.app.json")).unwrap();
    let mut before = file["tables"].clone();
    before["tasks"].as_object_mut().unwrap().remove("relations");
    before["tasks"]["columns"]["projectId"] = json!({"type":"uuid","references":"projects"});
    file["history"] = json!({"before": {"tables":before}});
    file["tables"]["tasks"]["columns"]["priority"] = json!({"type":"bytes","default":""});
    file["tables"]["tasks"]["encryption"]["columns"]["priority"] =
        json!({"type":"string","default":"normal"});
    file["migrations"] =
        json!([{"from":"before","migrate":{"tasks":{"priority":{"add":"bytes","default":""}}}}]);
    let file = serde_json::to_vec(&file).unwrap();
    let app = App::on_server(
        AppFiles::new(file.clone(), None).with_permissions(
            include_bytes!("fixtures/encrypted/jazz.permissions.json").as_slice(),
        ),
        app.server,
    )
    .await;
    let device = app.device_with(AppFiles::new(file, None)).await;
    ready(&device).await;
    let rows = device
        .with_client(|client| async move {
            Ok(client
                .query(
                    Query::from("tasks").select(["projectId", "priority"]),
                    ReadTier::Remote,
                )
                .await?)
        })
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0]
            .fields
            .iter()
            .any(|f| f.name == "priority" && f.value == text("normal"))
    );
    device.shutdown().await.unwrap();
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
    ready(&alice).await;
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
/// the space's key, reads her encrypted task and sends a reply she can read.
///
/// ```text
/// alice ──NewE2eeMember { bob }──► server
/// alice ──e2ee_members::query().filter(identifier)──► [alice, bob]
/// alice ──encrypted task, keys.deliver──► bob reads it ──reply──► alice reads it
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_bindings_add_and_list_a_spaces_members() {
    use bindings::{NewE2eeMember, e2ee_members};
    let app = App::start().await;
    let alice = app.device().await;
    let bob = app.device().await;
    for device in [&alice, &bob] {
        ready(device).await;
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
    deliver(&alice).await;
    assert_eq!(titles(&bob, 1).await, ["for members"]);
    add_task(&bob, &project, "bob's reply").await.unwrap();
    assert_eq!(titles(&alice, 2).await, ["bob's reply", "for members"]);

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
    let alice = app.device_with(AppFiles::new(files.app, None)).await;
    ready(&alice).await;
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
    assert_eq!(ready(&laptop).await, "active");
    let project = make_project(&laptop, "plans").await;
    add_task(&laptop, &project, "kept by the key")
        .await
        .unwrap();

    let phone = app.device_of(key).await;
    assert_eq!(
        ready(&phone).await,
        "active",
        "the account key is its recovery: no approval, no phrase"
    );
    deliver(&laptop).await;
    assert_eq!(titles(&phone, 1).await, ["kept by the key"]);

    laptop.shutdown().await.unwrap();
    phone.shutdown().await.unwrap();
    app.stop().await;
}

/// Alice's laptop cannot retire the account-secret root. Her phone still
/// recovers from the secret after the laptop closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retiring_the_device_account_root_is_refused() {
    let app = App::start().await;
    let key = DeviceKey::generate().unwrap();
    let laptop = app.device_of(key.clone()).await;
    let encryption = laptop.encryption();
    assert_eq!(ready(&laptop).await, "active");

    // Reverify until the root registered at startup is visible.
    let roots = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            encryption.call("ready", json!({})).await.unwrap();
            let roots = encryption.call("recoveryList", json!({})).await.unwrap();
            if !roots.as_array().unwrap().is_empty() {
                break roots;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the root was registered");
    assert_eq!(roots.as_array().unwrap().len(), 1);
    let refusal = encryption
        .call("recoveryRetire", json!({ "rootId": roots[0]["id"] }))
        .await
        .unwrap_err();
    assert!(matches!(refusal, Error::Encryption { code, message }
        if code == "device-account-root-required" && message.contains("prevent recovery")));
    laptop.shutdown().await.unwrap();

    let phone = app.device_of(key).await;
    assert_eq!(ready(&phone).await, "active");
    phone.shutdown().await.unwrap();
    app.stop().await;
}

/// Alice's phone re-enrols on reopening when a private-store save marks its
/// recovery nonce spent but the save acknowledgment is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_continues_after_reopening_with_a_stranded_nonce() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let app = App::start().await;
    let key = DeviceKey::generate().unwrap();
    let laptop = app.device_of(key.clone()).await;
    assert_eq!(ready(&laptop).await, "active");

    let memory = MemoryStore::new();
    let lose_ack = AtomicBool::new(true);
    let store = EncryptionConfig::store_calls(move |request| {
        let parsed: serde_json::Value = serde_json::from_str(&request).unwrap();
        let save =
            parsed["op"] == "save" && parsed["doc"] == "device" && parsed["expected"].is_string();
        let (ok, payload) = answer_memory_store(&memory, request);
        if ok && save && lose_ack.swap(false, Ordering::SeqCst) {
            return (
                false,
                "lost acknowledgement of the recovery nonce save".to_owned(),
            );
        }
        (ok, payload)
    });
    let config = app.user(key).encryption(store);
    let phone = Jazz::open(client_files(), config.clone()).await.unwrap();
    let error = phone
        .encryption()
        .ready(Duration::from_secs(15))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Encryption { code, message }
        if code == "store" && message.contains("lost acknowledgement")));
    let stranded = phone
        .encryption()
        .call("deviceId", json!({}))
        .await
        .unwrap();
    phone.shutdown().await.unwrap();

    let reopened = Jazz::open(client_files(), config).await.unwrap();
    assert_eq!(
        reopened
            .encryption()
            .ready(Duration::from_secs(15))
            .await
            .unwrap(),
        "active"
    );
    let recovered = reopened
        .encryption()
        .call("deviceId", json!({}))
        .await
        .unwrap();
    assert_ne!(
        recovered, stranded,
        "the spent nonce belongs to the old device"
    );
    reopened.shutdown().await.unwrap();
    laptop.shutdown().await.unwrap();
    app.stop().await;
}

/// A private store with a hook that can fail or hold selected saves.
fn store_with_hook(
    hook: impl Fn(&str, Option<u64>) -> futures::future::LocalBoxFuture<'static, Result<(), StoreError>>
    + Send
    + Sync
    + 'static,
) -> EncryptionConfig {
    let hook = std::sync::Arc::new(hook);
    EncryptionConfig::store(move || {
        let hook = hook.clone();
        Rc::new(StoreWithSaveHook::new(
            MemoryStore::new(),
            move |_, doc, _, expected| hook(doc, expected),
        ))
    })
}

/// Updating the stored device, after creation, marks its recovery nonce
/// spent before publishing the device's approval.
fn saving_recovery_nonce(doc: &str, expected: Option<u64>) -> bool {
    doc == "device" && expected.is_some()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_reports_the_store_failure_that_stopped_recovery() {
    let app = App::start().await;
    let key = DeviceKey::generate().unwrap();
    let laptop = app.device_of(key.clone()).await;
    assert_eq!(ready(&laptop).await, "active");
    let store = store_with_hook(|doc, expected| {
        let recovery = saving_recovery_nonce(doc, expected);
        Box::pin(async move {
            if recovery {
                return Err(StoreError::Failed("cannot save recovery nonce".to_owned()));
            }
            Ok(())
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
    assert_eq!(ready(&laptop).await, "active");
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let store = store_with_hook({
        let entered = entered.clone();
        let release = release.clone();
        move |doc, expected| {
            let entered = entered.clone();
            let release = release.clone();
            let recovery = saving_recovery_nonce(doc, expected);
            Box::pin(async move {
                if recovery {
                    entered.notify_one();
                    release.notified().await;
                }
                Ok(())
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
    let store = store_with_hook(|doc, _| {
        let held = doc.starts_with("held-");
        Box::pin(async move {
            if held {
                return Err(StoreError::Failed("cannot store the space key".to_owned()));
            }
            Ok(())
        })
    });
    let config = app.user(DeviceKey::generate().unwrap()).encryption(store);
    let alice = Jazz::open(client_files(), config).await.unwrap();
    ready(&alice).await;
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
        move |doc, _| {
            let entered = entered.clone();
            let held = doc.starts_with("held-");
            Box::pin(async move {
                if held {
                    entered.notify_one();
                    std::future::pending::<()>().await;
                }
                Ok(())
            })
        }
    });
    let config = app.user(DeviceKey::generate().unwrap()).encryption(store);
    let alice = Jazz::open(client_files(), config).await.unwrap();
    ready(&alice).await;
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
