#[path = "bindings/todos.rs"]
mod bindings;

use std::time::Duration;

use bindings::{NewProject, NewTodo, Todo, TodoUpdate, projects, todos};
use futures::StreamExt;
use jazz_app_support::{
    AppFiles, AppId, Backend, Config, Error, Jazz, Order, ReadTier, Rows, Storage,
};
use jazz_server::JazzServer;

fn files() -> AppFiles {
    AppFiles::read(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/todos")).unwrap()
}

fn client_files() -> AppFiles {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/todos");
    AppFiles::new(std::fs::read(format!("{dir}/jazz.app.json")).unwrap(), None)
}

fn new_todo(title: &str) -> NewTodo {
    NewTodo::new(title, false)
}

/// The handle is Send + Clone: it works from tasks of a multi-threaded
/// runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_reads_and_writes_locally() {
    // The generated `open`, with the schema it embeds.
    let jazz = bindings::open(Config::new(AppId::random()))
        .await
        .expect("open locally");

    let project = jazz
        .insert(NewProject {
            name: "Home".into(),
        })
        .await
        .unwrap();
    let writer = jazz.clone();
    let milk = tokio::spawn(async move {
        writer
            .insert(NewTodo {
                project: Some(project.id),
                ..NewTodo::new("Buy milk", false)
            })
            .await
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(milk.title, "Buy milk");
    assert!(!milk.done);
    assert_eq!(milk.project, Some(project.id));
    let bread = jazz.insert(new_todo("Buy bread")).await.unwrap();

    jazz.update(
        milk.id,
        TodoUpdate {
            done: Some(true),
            description: Some(Some("semi-skimmed".into())),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let milk = jazz.get::<Todo>(milk.id).await.unwrap().expect("milk");
    assert!(milk.done);
    assert_eq!(milk.description.as_deref(), Some("semi-skimmed"));

    let open = jazz
        .all(todos::query().filter(todos::done().eq(false)))
        .await
        .unwrap();
    assert_eq!(open, vec![bread.clone()]);

    let ordered = jazz
        .all(todos::query().order_by(todos::title(), Order::Desc))
        .await
        .unwrap();
    assert_eq!(
        ordered
            .iter()
            .map(|todo| todo.title.as_str())
            .collect::<Vec<_>>(),
        ["Buy milk", "Buy bread"]
    );

    let in_project = jazz
        .all(projects::todos_via_project().of(project.id))
        .await
        .unwrap();
    assert_eq!(in_project, vec![milk.clone()]);
    let home = jazz
        .one(todos::project_relation().of(project.id))
        .await
        .unwrap();
    assert_eq!(home.map(|project| project.name), Some("Home".into()));

    let undescribed = jazz
        .all(todos::query().filter(todos::description().is_null()))
        .await
        .unwrap();
    assert_eq!(undescribed, vec![bread.clone()]);

    jazz.delete::<Todo>(bread.id).await.unwrap();
    assert_eq!(jazz.get::<Todo>(bread.id).await.unwrap(), None);

    // Shutting down lets local work already started finish.
    let slow = jazz.with_client(|_| async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(42)
    });
    let (slow, stopped) = tokio::join!(slow, async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        jazz.shutdown().await
    });
    assert_eq!(slow.unwrap(), 42);
    stopped.unwrap();

    assert!(matches!(
        jazz.all(todos::query()).await,
        Err(Error::Stopped)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transactions_commit_or_roll_back() {
    let jazz = Jazz::open(client_files(), Config::new(AppId::random()))
        .await
        .unwrap();

    let (a, b) = jazz
        .transaction(|tx| async move {
            let a = tx.insert(new_todo("a"))?;
            let b = tx.insert(NewTodo {
                parent: Some(a.id),
                ..NewTodo::new("b", false)
            })?;
            Ok((a, b))
        })
        .await
        .unwrap();
    assert_eq!(b.parent, Some(a.id));
    let children = jazz.all(todos::todos_via_parent().of(a.id)).await.unwrap();
    assert_eq!(children, vec![b]);

    let refused = jazz
        .transaction(|tx| async move {
            tx.insert(new_todo("c"))?;
            Err::<(), _>(Error::Config("changed my mind".into()))
        })
        .await;
    assert!(refused.is_err());
    let titles: Vec<_> = jazz
        .all(todos::query().order_by(todos::title(), Order::Asc))
        .await
        .unwrap()
        .into_iter()
        .map(|todo| todo.title)
        .collect();
    assert_eq!(titles, ["a", "b"]);

    // Reads inside a transaction: read, then write from what was read.
    let done = jazz
        .transaction(move |tx| async move {
            let a = tx.get::<Todo>(a.id).await?.expect("a");
            tx.update(
                a.id,
                TodoUpdate {
                    done: Some(!a.done),
                    ..Default::default()
                },
            )?;
            Ok(tx.all(todos::query()).await?.len())
        })
        .await
        .unwrap();
    assert_eq!(done, 2);
    assert!(jazz.get::<Todo>(a.id).await.unwrap().unwrap().done);
}

/// Upsert writes a row with a chosen id, or replaces its columns; restore
/// brings a deleted row back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_and_restore() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let id = jazz_app_support::ObjectId::new();
    let first = jazz.upsert(id, new_todo("first")).await.unwrap();
    assert_eq!((first.id, first.title.as_str()), (id, "first"));
    let second = jazz.upsert(id, NewTodo::new("second", true)).await.unwrap();
    assert_eq!((second.title.as_str(), second.done), ("second", true));
    assert_eq!(jazz.all(todos::query()).await.unwrap().len(), 1);

    jazz.delete::<Todo>(id).await.unwrap();
    assert_eq!(jazz.get::<Todo>(id).await.unwrap(), None);
    let restored = jazz.restore::<Todo>(id).await.unwrap();
    assert_eq!(restored, second);
    // Without a server there is nothing to wait for.
    jazz.wait_for_sync().await.unwrap();
    let refused = jazz
        .durable(jazz_app_support::DurabilityTier::GlobalServer)
        .insert(new_todo("nowhere to go"))
        .await;
    assert!(matches!(refused, Err(Error::Config(_))), "{refused:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriptions_stream_typed_deltas() {
    let jazz = Jazz::open(client_files(), Config::new(AppId::random()))
        .await
        .unwrap();
    let first = jazz.insert(new_todo("first")).await.unwrap();

    let mut changes = jazz
        .subscribe(todos::query().filter(todos::done().eq(false)))
        .await
        .unwrap();
    let mut rows = Rows::default();
    rows.apply(next(&mut changes).await);
    assert_eq!(rows.as_slice(), std::slice::from_ref(&first));

    let second = jazz.insert(new_todo("second")).await.unwrap();
    while rows.as_slice().len() < 2 {
        rows.apply(next(&mut changes).await);
    }
    assert!(rows.as_slice().contains(&second));

    jazz.update(
        first.id,
        TodoUpdate {
            done: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    while rows.as_slice().len() != 1 {
        rows.apply(next(&mut changes).await);
    }
    assert_eq!(rows.into_vec(), vec![second]);

    // Dropping every other handle leaves the subscription's own.
    drop(jazz);
    drop(changes);
}

/// A subscriber that stops reading while the app writes holds a bounded
/// stream: its forwarder stops taking events once the stream is full, and
/// they wait in the core, which coalesces them once they pass the default
/// limit of 10000 changed rows. When the subscriber reads again it gets a
/// delta marked `coalesced` (all rows held removed, the current ones
/// added) and converges on every row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscriber_that_falls_behind_is_coalesced_and_catches_up() {
    const TODOS: usize = 10_100;
    let jazz = Jazz::open(client_files(), Config::new(AppId::random()))
        .await
        .unwrap();
    let mut changes = jazz.subscribe(todos::query()).await.unwrap();
    let mut rows = Rows::default();
    rows.apply(next(&mut changes).await);
    for n in 0..TODOS {
        jazz.insert(new_todo(&format!("todo {n}"))).await.unwrap();
    }
    let mut coalesced = false;
    while rows.as_slice().len() < TODOS {
        let delta = next(&mut changes).await;
        coalesced |= delta.coalesced;
        rows.apply(delta);
    }
    assert!(coalesced, "the backlog past the limit was coalesced");
    let mut titles: Vec<String> = rows.into_vec().into_iter().map(|todo| todo.title).collect();
    titles.sort();
    let mut expected: Vec<String> = (0..TODOS).map(|n| format!("todo {n}")).collect();
    expected.sort();
    assert_eq!(titles, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backend_publishes_its_bundle_and_acts_as_its_user() {
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .expect("start a Jazz server");
    let config = |user: &str| {
        Config::new(app_id)
            .server(server.base_url())
            .backend(Backend::new(
                server.backend_secret(),
                server.admin_secret(),
                user,
            ))
    };

    // The generated `open`, with the permissions the backend states.
    let permissions = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/todos/jazz.permissions.json"
    ))
    .unwrap();
    let mut backend = config("alice");
    backend.backend = backend.backend.map(|b| b.permissions(permissions));
    let writer = bindings::open(backend)
        .await
        .expect("open as the app's backend");
    let bundle = writer.bundle().expect("a published bundle").to_owned();

    // A second backend publishing the same files gets the same bundle.
    let reader = Jazz::open(files(), config("alice")).await.unwrap();
    assert_eq!(reader.bundle(), Some(bundle.as_str()));
    // Both resolve Alice's one account.
    assert!(writer.account().is_some());
    assert_eq!(reader.account(), writer.account());

    // A durable handle returns once the server has the row; wait_for_sync
    // covers the plain writes made before it.
    let plain = writer.insert(new_todo("Plain")).await.unwrap();
    let written = writer
        .durable(jazz_app_support::DurabilityTier::GlobalServer)
        .insert(new_todo("Synced"))
        .await
        .unwrap();
    writer.wait_for_sync().await.unwrap();
    writer.delete::<Todo>(plain.id).await.unwrap();
    writer.wait_for_sync().await.unwrap();
    let seen = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = reader
                .all_with(todos::query(), ReadTier::Remote)
                .await
                .unwrap();
            if !rows.is_empty() {
                break rows;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the write reaches the other backend through the server");
    assert_eq!(seen, vec![written]);

    writer.shutdown().await.unwrap();
    reader.shutdown().await.unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn refuses_what_it_cannot_do() {
    let app_id = AppId::random();
    let as_nobody = Jazz::open(files(), Config::new(app_id).server("http://localhost:1")).await;
    assert!(matches!(as_nobody, Err(Error::Config(_))), "{as_nobody:?}");

    let backend = Backend::new("b", "a", "u");
    let serverless = Jazz::open(files(), Config::new(app_id).backend(backend.clone())).await;
    assert!(
        matches!(serverless, Err(Error::Config(_))),
        "{serverless:?}"
    );

    let unpermitted = Jazz::open(
        client_files(),
        Config::new(app_id)
            .server("http://localhost:1")
            .backend(backend),
    )
    .await;
    assert!(matches!(unpermitted, Err(Error::App(_))), "{unpermitted:?}");

    let broken = Jazz::open(
        AppFiles::new(&b"{\"format\": 1}"[..], None),
        Config::new(app_id),
    )
    .await;
    assert!(matches!(broken, Err(Error::App(_))), "{broken:?}");
}

async fn next(changes: &mut jazz_app_support::Subscription<Todo>) -> jazz_app_support::Delta<Todo> {
    tokio::time::timeout(Duration::from_secs(5), changes.next())
        .await
        .expect("a delta")
        .expect("an open stream")
        .expect("a typed delta")
}

/// A call made by a call that is still finishing while the app shuts down
/// fails with `Stopped`; it doesn't hold shutdown up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_refuses_calls_made_while_it_drains() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let nested = jazz.clone();
    let outer = jazz.with_client(move |_| async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Shutdown began meanwhile.
        Ok(nested.all(todos::query()).await)
    });
    let all = async {
        tokio::join!(
            outer,
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                jazz.shutdown().await
            },
            async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                jazz.shutdown().await
            }
        )
    };
    let (outer, stopped, again) = tokio::time::timeout(Duration::from_secs(5), all)
        .await
        .expect("shutdown does not deadlock");
    let inner = outer.unwrap();
    assert!(matches!(inner, Err(Error::Stopped)), "{inner:?}");
    stopped.unwrap();
    again.unwrap();
}

/// A call that panics reports it, and the client runs on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_call_reports_it_and_the_client_runs_on() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let panicked = jazz
        .with_client(|_| async { panic!("boom") as jazz_app_support::Result<()> })
        .await;
    assert!(
        matches!(&panicked, Err(Error::Panicked(message)) if message == "boom"),
        "{panicked:?}"
    );
    // A closure that panics before it returns its future.
    let panicked = jazz
        .with_client(|_| -> std::future::Ready<jazz_app_support::Result<()>> {
            panic!("sync boom")
        })
        .await;
    assert!(
        matches!(&panicked, Err(Error::Panicked(message)) if message == "sync boom"),
        "{panicked:?}"
    );
    jazz.insert(new_todo("still here")).await.unwrap();
    assert_eq!(jazz.all(todos::query()).await.unwrap().len(), 1);
}

/// Text and array `contains`, `is_not_null`, comparisons that take what
/// converts into the column's type, and ordering by `created_at` and `id`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_helpers() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let milk = jazz
        .insert(NewTodo {
            description: Some("semi-skimmed milk".into()),
            ..NewTodo::new("Buy milk", false)
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let bread = jazz.insert(new_todo("Buy bread")).await.unwrap();

    let titles = |rows: Vec<Todo>| rows.into_iter().map(|t| t.title).collect::<Vec<_>>();
    assert_eq!(
        titles(
            jazz.all(todos::query().filter(todos::title().contains("milk")))
                .await
                .unwrap()
        ),
        ["Buy milk"]
    );
    assert_eq!(
        titles(
            jazz.all(todos::query().filter(todos::description().contains("skim")))
                .await
                .unwrap()
        ),
        ["Buy milk"]
    );
    assert_eq!(
        titles(
            jazz.all(todos::query().filter(todos::description().is_not_null()))
                .await
                .unwrap()
        ),
        ["Buy milk"]
    );
    // `&str` into a `String` column; a plain value into a nullable one.
    assert_eq!(
        titles(
            jazz.all(todos::query().filter(todos::title().eq("Buy bread")))
                .await
                .unwrap()
        ),
        ["Buy bread"]
    );
    assert_eq!(
        titles(
            jazz.all(
                todos::query().filter(todos::description().eq("semi-skimmed milk".to_owned()))
            )
            .await
            .unwrap()
        ),
        ["Buy milk"]
    );
    let newest_first = jazz
        .all(todos::query().order_by(jazz_app_support::created_at(), Order::Desc))
        .await
        .unwrap();
    assert_eq!(titles(newest_first), ["Buy bread", "Buy milk"]);
    let by_id = jazz
        .all(todos::query().order_by(todos::id(), Order::Asc))
        .await
        .unwrap();
    let mut ids = vec![milk.id, bread.id];
    ids.sort_by_key(|id| *id.uuid());
    assert_eq!(by_id.iter().map(|t| t.id).collect::<Vec<_>>(), ids);
}

/// Read options against a server: a fresh store has nothing locally yet, `wait_for_remote` waits for the server's answer, and a
/// `Remote` read asks the server. What can't be answered is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_options_with_a_server() {
    use jazz_app_support::ReadOptions;
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
    let open = || {
        bindings::open(
            Config::new(app_id).server(server.base_url()).backend(
                Backend::new(server.backend_secret(), server.admin_secret(), "alice")
                    .permissions(permissions.clone()),
            ),
        )
    };
    let writer = open().await.unwrap();
    let written = writer
        .durable(jazz_app_support::DurabilityTier::GlobalServer)
        .insert(new_todo("on the server"))
        .await
        .unwrap();

    let fresh = open().await.unwrap();
    assert!(
        fresh
            .all_with(todos::query(), ReadTier::LocalOnly)
            .await
            .unwrap()
            .is_empty(),
        "a fresh store has nothing locally yet"
    );
    let waited = fresh
        .all_with_options(
            todos::query(),
            ReadOptions::default().wait_for_remote(Duration::from_secs(10)),
        )
        .await
        .unwrap();
    assert_eq!(waited, vec![written.clone()]);

    let remote = open().await.unwrap();
    let asked = remote
        .all_with_options(
            todos::query(),
            ReadOptions::default().tier(ReadTier::Remote),
        )
        .await
        .unwrap();
    assert_eq!(asked, vec![written]);

    let both = remote
        .all_with_options(
            todos::query(),
            ReadOptions::default()
                .tier(ReadTier::Remote)
                .wait_for_remote(Duration::from_secs(1)),
        )
        .await;
    assert!(matches!(both, Err(Error::Config(_))), "{both:?}");
    let local = bindings::open(Config::new(AppId::random())).await.unwrap();
    let nobody = local
        .all_with_options(
            todos::query(),
            ReadOptions::default().tier(ReadTier::Remote),
        )
        .await;
    assert!(matches!(nobody, Err(Error::Config(_))), "{nobody:?}");

    for jazz in [writer, fresh, remote] {
        jazz.shutdown().await.unwrap();
    }
    server.shutdown().await;
}

/// A write that lands between a transaction's read and its own write: a
/// mergeable transaction merges with it, a serializable one is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mergeable_and_serializable_transactions() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    for serializable in [false, true] {
        let todo = jazz.insert(new_todo("contended")).await.unwrap();
        let id = todo.id;
        let other = jazz.clone();
        let body = move |tx: jazz_app_support::Transaction| async move {
            let seen = tx.get::<Todo>(id).await?.expect("the row");
            // Someone else renames it meanwhile.
            other
                .update(
                    id,
                    TodoUpdate {
                        title: Some("renamed".into()),
                        ..Default::default()
                    },
                )
                .await?;
            tx.update(
                id,
                TodoUpdate {
                    done: Some(!seen.done),
                    ..Default::default()
                },
            )?;
            Ok(())
        };
        let committed = if serializable {
            jazz.serializable_transaction(body).await
        } else {
            jazz.transaction(body).await
        };
        let row = jazz.get::<Todo>(id).await.unwrap().unwrap();
        if serializable {
            assert!(committed.is_err(), "a serializable commit is refused");
            assert_eq!((row.title.as_str(), row.done), ("renamed", false));
        } else {
            committed.unwrap();
            assert_eq!(
                (row.title.as_str(), row.done),
                ("renamed", true),
                "both writes merge"
            );
        }
    }
}

/// A transaction driven call by call (TS's `beginTransaction`): a `Send`
/// handle that commits, rolls back, or rolls back when dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_transactions() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();

    let tx = jazz.begin_transaction().await.unwrap();
    let a = tx.insert(new_todo("a")).await.unwrap();
    // Held across an await in another task: the handle is Send.
    let tx = tokio::spawn(async move {
        tx.update(
            a.id,
            TodoUpdate {
                done: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        tx
    })
    .await
    .unwrap();
    assert!(tx.get::<Todo>(a.id).await.unwrap().unwrap().done);
    tx.commit().await.unwrap();
    assert!(jazz.get::<Todo>(a.id).await.unwrap().unwrap().done);

    let tx = jazz.begin_serializable_transaction().await.unwrap();
    tx.insert(new_todo("rolled back")).await.unwrap();
    tx.rollback().await.unwrap();

    assert_eq!(jazz.open_transactions().await.unwrap(), 0);

    let tx = jazz.begin_transaction().await.unwrap();
    tx.insert(new_todo("dropped")).await.unwrap();
    assert_eq!(jazz.open_transactions().await.unwrap(), 1);
    drop(tx);
    // Dropping it rolls it back on the client's thread, without waiting.
    let mut open = 1;
    for _ in 0..100 {
        open = jazz.open_transactions().await.unwrap();
        if open == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(open, 0, "dropping an open transaction rolls it back");

    let titles: Vec<_> = jazz
        .all(todos::query())
        .await
        .unwrap()
        .into_iter()
        .map(|todo| todo.title)
        .collect();
    assert_eq!(titles, ["a"]);
}

/// Shutting down with a transaction still open rolls it back first: the
/// client thread must not drop it with its thread-locals (that aborted the
/// process).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_with_open_serializable_transaction() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let tx = jazz.begin_serializable_transaction().await.unwrap();
    tx.insert(new_todo("never committed")).await.unwrap();
    jazz.shutdown().await.unwrap();
    assert!(matches!(tx.commit().await, Err(Error::Stopped)));
}

/// The same with a mergeable transaction, on a persistent store: its
/// writes are not there when the app is opened again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_with_open_mergeable_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let app_id = AppId::random();
    let config = || Config::new(app_id).storage(Storage::Dir(dir.path().to_owned()));
    let jazz = bindings::open(config()).await.unwrap();
    jazz.insert(new_todo("kept")).await.unwrap();
    let tx = jazz.begin_transaction().await.unwrap();
    tx.insert(new_todo("never committed")).await.unwrap();
    jazz.shutdown().await.unwrap();
    drop(tx);

    let jazz = bindings::open(config()).await.unwrap();
    let titles: Vec<_> = jazz
        .all(todos::query())
        .await
        .unwrap()
        .into_iter()
        .map(|todo| todo.title)
        .collect();
    assert_eq!(titles, ["kept"]);
    jazz.shutdown().await.unwrap();
}

/// A client opened without encryption configured refuses an app that declares
/// encryption before it can write any encrypted columns.
///
/// ```text
/// app files declare encryption ──open (no hooks)──► ✗ refused
/// ```
#[tokio::test]
async fn a_client_refuses_an_app_that_declares_encryption() {
    let app = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": {
            "projects": { "columns": { "name": "bytes" },
                "encryption": { "space": "id", "scope": "projects", "columns": { "name": "string" } } }
        } }"#;
    let permissions = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "projects": { "read": "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)" } } }"#;
    for files in [
        AppFiles::new(app.as_bytes().to_vec(), None),
        AppFiles::new(
            app.as_bytes().to_vec(),
            Some(permissions.as_bytes().to_vec()),
        ),
    ] {
        let opened = Jazz::open(files, Config::new(AppId::random())).await;
        match opened {
            Err(Error::App(message)) => {
                assert!(
                    message.contains("declares end-to-end encryption"),
                    "{message}"
                )
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }
}

/// Shutdown doesn't wait for a blocking task the client's runtime still
/// runs (a host name lookup, say, which no timeout ends), and still lets go
/// of a persistent store: it reopens at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_doesnt_wait_for_a_blocking_task_left_running() {
    let dir = tempfile::tempdir().unwrap();
    let app_id = AppId::random();
    let config = || Config::new(app_id).storage(Storage::Dir(dir.path().to_owned()));
    let jazz = bindings::open(config()).await.unwrap();
    jazz.insert(new_todo("kept")).await.unwrap();
    jazz.with_client(|_| async {
        // Left running, as a lookup the client stopped waiting for is.
        drop(tokio::task::spawn_blocking(|| {
            std::thread::sleep(Duration::from_secs(20))
        }));
        Ok(())
    })
    .await
    .unwrap();
    let started = std::time::Instant::now();
    jazz.shutdown().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shutdown took {:?}",
        started.elapsed()
    );
    let again = bindings::open(config()).await.expect("the store reopens");
    assert_eq!(again.all(todos::query()).await.unwrap().len(), 1);
    again.shutdown().await.unwrap();
}
