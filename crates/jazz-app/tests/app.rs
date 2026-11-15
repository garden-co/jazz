#[path = "bindings/todos.rs"]
mod bindings;

use std::time::Duration;

use bindings::{NewProject, NewTodo, Todo, TodoUpdate, projects, todos};
use futures::StreamExt;
use jazz_app::{AppFiles, AppId, Backend, Config, Error, Jazz, Order, ReadTier, Rows};
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

    // Shutting down lets the calls already made finish.
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
        .transaction(|tx| {
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
        .transaction(|tx| {
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

    let written = writer.insert(new_todo("Synced")).await.unwrap();
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

async fn next(changes: &mut jazz_app::Subscription<Todo>) -> jazz_app::Delta<Todo> {
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
        .with_client(|_| async { panic!("boom") as jazz_app::Result<()> })
        .await;
    assert!(
        matches!(&panicked, Err(Error::Panicked(message)) if message == "boom"),
        "{panicked:?}"
    );
    // A closure that panics before it returns its future.
    let panicked = jazz
        .with_client(|_| -> std::future::Ready<jazz_app::Result<()>> { panic!("sync boom") })
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
        .all(todos::query().order_by(jazz_app::created_at(), Order::Desc))
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
