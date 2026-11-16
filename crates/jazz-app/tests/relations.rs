//! hop_to, gather, union, include_deleted and history reads, through the
//! bindings generated for tests/fixtures/todos.

#[path = "bindings/todos.rs"]
mod bindings;

use bindings::{NewProject, NewTodo, Project, Todo, TodoUpdate, projects, todos};
use futures::StreamExt;
use jazz_app::{
    AppId, Backend, Config, Error, Jazz, Order, Partial, Query, Row, Rows, Step, Timestamp, Version,
};
use jazz_server::JazzServer;

struct Fixture {
    home: Project,
    work: Project,
    milk: Todo,
    bread: Todo,
    loose: Todo,
    skimmed: Todo,
    lowfat: Todo,
}

/// Home: milk (open), bread (done), skimmed (open, under milk), low-fat
/// (open, under skimmed). Work: nothing. Loose end: no project.
async fn fixture(jazz: &Jazz) -> Fixture {
    let home = jazz.insert(NewProject::new("Home")).await.unwrap();
    let work = jazz.insert(NewProject::new("Work")).await.unwrap();
    let todo =
        |title: &str, done: bool, parent: Option<&Todo>, project: Option<&Project>| NewTodo {
            parent: parent.map(|parent| parent.id),
            project: project.map(|project| project.id),
            ..NewTodo::new(title, done)
        };
    let milk = jazz
        .insert(todo("Buy milk", false, None, Some(&home)))
        .await
        .unwrap();
    let bread = jazz
        .insert(todo("Buy bread", true, None, Some(&home)))
        .await
        .unwrap();
    let loose = jazz
        .insert(todo("Loose end", false, None, None))
        .await
        .unwrap();
    let skimmed = jazz
        .insert(todo("Skimmed", false, Some(&milk), Some(&home)))
        .await
        .unwrap();
    let lowfat = jazz
        .insert(todo("Low-fat", false, Some(&skimmed), None))
        .await
        .unwrap();
    Fixture {
        home,
        work,
        milk,
        bread,
        loose,
        skimmed,
        lowfat,
    }
}

async fn local() -> Jazz {
    bindings::open(Config::new(AppId::random())).await.unwrap()
}

type ConfigFn = Box<dyn Fn() -> Config>;

/// A sync server, and the config of a backend of it.
async fn server() -> (JazzServer, ConfigFn) {
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
    let (url, backend, admin) = (
        server.base_url().to_string(),
        server.backend_secret().to_string(),
        server.admin_secret().to_string(),
    );
    let config = Box::new(move || {
        Config::new(app_id).server(url.clone()).backend(
            Backend::new(backend.clone(), admin.clone(), "alice").permissions(permissions.clone()),
        )
    });
    (server, config)
}

/// `query`'s rows, as the server holds them.
async fn remote<R: Row>(jazz: &Jazz, query: Query<R>) -> jazz_app::Result<Vec<R>> {
    jazz.all_with(query, jazz_app::ReadTier::Remote).await
}

/// `query`'s rows once there are `count`, as the server holds them.
async fn settled<R: Row>(jazz: &Jazz, query: Query<R>, count: usize) -> Vec<R> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let rows = remote(jazz, query.clone()).await.unwrap();
            if rows.len() == count {
                break rows;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{count} rows reach the server"))
}

fn titles(todos: &[Todo]) -> Vec<&str> {
    todos.iter().map(|todo| todo.title.as_str()).collect()
}

fn unsupported(error: Error) -> String {
    match error {
        Error::Unsupported(reason) => reason,
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hop_to_follows_forward_and_reverse_relations() {
    let jazz = local().await;
    let f = fixture(&jazz).await;

    // The projects of the open todos: Home, once, though three reach it.
    let projects: Vec<Project> = jazz
        .all(
            todos::query()
                .filter(todos::done().eq(false))
                .hop_to(todos::project_relation()),
        )
        .await
        .unwrap();
    assert_eq!(projects, std::slice::from_ref(&f.home));

    // Home's open todos, ordered after the hop.
    let open: Vec<Todo> = jazz
        .all(
            projects::query()
                .filter(projects::name().eq("Home"))
                .hop_to(projects::todos_via_project())
                .filter(todos::done().eq(false))
                .order_by(todos::title(), Order::Asc),
        )
        .await
        .unwrap();
    assert_eq!(titles(&open), ["Buy milk", "Skimmed"]);

    // A window after a hop applies to the rows it reached.
    let skipped: Vec<Todo> = jazz
        .all(
            projects::query()
                .id(f.home.id)
                .hop_to(projects::todos_via_project())
                .order_by(todos::title(), Order::Asc)
                .offset(1)
                .limit(1),
        )
        .await
        .unwrap();
    assert_eq!(titles(&skipped), ["Buy milk"]);

    // Hops chain: low-fat's parent's project.
    let chained: Vec<Project> = jazz
        .all(
            todos::query()
                .id(f.lowfat.id)
                .hop_to(todos::parent_relation())
                .hop_to(todos::project_relation()),
        )
        .await
        .unwrap();
    assert_eq!(chained, std::slice::from_ref(&f.home));

    // Nothing reaches Work.
    let none: Vec<Todo> = jazz
        .all(
            projects::query()
                .id(f.work.id)
                .hop_to(projects::todos_via_project()),
        )
        .await
        .unwrap();
    assert!(none.is_empty());

    // A hop's rows select, and subscribe.
    let names: Vec<String> = jazz
        .all(
            todos::query()
                .id(f.skimmed.id)
                .hop_to(todos::project_relation())
                .select(projects::name()),
        )
        .await
        .unwrap()
        .into_iter()
        .map(Partial::into_values)
        .collect();
    assert_eq!(names, ["Home"]);
    let mut changes = jazz
        .subscribe(
            projects::query()
                .id(f.home.id)
                .hop_to(projects::todos_via_project())
                .filter(todos::done().eq(true)),
        )
        .await
        .unwrap();
    let mut rows = Rows::default();
    rows.apply(changes.next().await.unwrap().unwrap());
    assert_eq!(titles(rows.as_slice()), ["Buy bread"]);
    jazz.update(
        f.milk.id,
        TodoUpdate {
            done: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    while rows.as_slice().len() < 2 {
        rows.apply(changes.next().await.unwrap().unwrap());
    }
    let mut done = titles(rows.as_slice());
    done.sort();
    assert_eq!(done, ["Buy bread", "Buy milk"]);
    let _ = (&f.loose, &f.skimmed);
    jazz.shutdown().await.unwrap();
}

/// As in TypeScript, a query has one order and window, applied to the rows
/// it ends on: `order_by`, `limit` and `offset` before a hop order and
/// window the rows the hop reaches, not the rows it starts from.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn order_and_window_before_a_hop_apply_to_the_rows_it_reaches() {
    let jazz = local().await;
    let f = fixture(&jazz).await;

    // The children of every todo are Skimmed and Low-fat. Ordered by title
    // descending and limited to one, that's Skimmed. (Applied to the todos
    // the hop starts from, it would be Skimmed's child, Low-fat.)
    let first: Vec<Todo> = jazz
        .all(
            todos::query()
                .order_by(todos::title(), Order::Desc)
                .limit(1)
                .hop_to(todos::todos_via_parent()),
        )
        .await
        .unwrap();
    assert_eq!(titles(&first), ["Skimmed"]);

    // The offset before the hop skips reached rows; a later order adds to
    // the earlier one.
    let second: Vec<Todo> = jazz
        .all(
            todos::query()
                .offset(1)
                .hop_to(todos::todos_via_parent())
                .order_by(todos::title(), Order::Asc),
        )
        .await
        .unwrap();
    assert_eq!(titles(&second), ["Skimmed"]);

    // A limit before a hop to another table limits the projects it reaches.
    let projects: Vec<Project> = jazz
        .all(
            todos::query()
                .filter(todos::done().eq(false))
                .limit(1)
                .hop_to(todos::project_relation()),
        )
        .await
        .unwrap();
    assert_eq!(projects, std::slice::from_ref(&f.home));
    jazz.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gather_walks_a_relation_up_to_a_depth() {
    let jazz = local().await;
    let f = fixture(&jazz).await;
    let ancestors = |depth| {
        todos::query()
            .id(f.lowfat.id)
            .gather(todos::parent_relation(), depth)
            .order_by(todos::title(), Order::Asc)
    };

    let all: Vec<Todo> = jazz.all(ancestors(10)).await.unwrap();
    assert_eq!(titles(&all), ["Buy milk", "Low-fat", "Skimmed"]);
    let one: Vec<Todo> = jazz.all(ancestors(1)).await.unwrap();
    assert_eq!(titles(&one), ["Low-fat", "Skimmed"]);
    let seed: Vec<Todo> = jazz.all(ancestors(0)).await.unwrap();
    assert_eq!(titles(&seed), ["Low-fat"]);
    let skipped: Vec<Todo> = jazz.all(ancestors(10).offset(1)).await.unwrap();
    assert_eq!(titles(&skipped), ["Low-fat", "Skimmed"]);

    // Filters after the gather narrow what it gathered; a step's filter
    // narrows the rows it steps through.
    let narrowed: Vec<Todo> = jazz
        .all(ancestors(10).filter(todos::project_relation().referencing(f.home.id)))
        .await
        .unwrap();
    assert_eq!(titles(&narrowed), ["Buy milk", "Skimmed"]);
    let stepped: Vec<Todo> = jazz
        .all(todos::query().id(f.lowfat.id).gather(
            // The step's rows are those stepped from: not past Skimmed.
            Step::along(todos::parent_relation()).filter(todos::title().ne("Skimmed")),
            10,
        ))
        .await
        .unwrap();
    let mut stepped = titles(&stepped);
    stepped.sort();
    assert_eq!(stepped, ["Low-fat", "Skimmed"]);
    jazz.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn union_reads_each_querys_rows() {
    let jazz = local().await;
    let f = fixture(&jazz).await;

    let rows: Vec<Todo> = jazz
        .all(
            Query::union([
                todos::query().filter(todos::done().eq(true)),
                todos::query().filter(todos::title().contains("milk")),
            ])
            .order_by(todos::title(), Order::Asc),
        )
        .await
        .unwrap();
    assert_eq!(titles(&rows), ["Buy bread", "Buy milk"]);
    let skipped: Vec<Todo> = jazz
        .all(
            Query::union([
                todos::query().filter(todos::done().eq(true)),
                todos::query().filter(todos::title().contains("milk")),
            ])
            .order_by(todos::title(), Order::Asc)
            .offset(1),
        )
        .await
        .unwrap();
    assert_eq!(titles(&skipped), ["Buy milk"]);

    // Arms can hop, and the union takes a window.
    let rows: Vec<Todo> = jazz
        .all(
            Query::union_named([
                ("loose", todos::query().id(f.loose.id)),
                (
                    "home",
                    projects::query()
                        .id(f.home.id)
                        .hop_to(projects::todos_via_project()),
                ),
            ])
            .order_by(todos::title(), Order::Desc)
            .limit(2),
        )
        .await
        .unwrap();
    assert_eq!(titles(&rows), ["Skimmed", "Loose end"]);
    jazz.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn include_deleted_reads_deleted_rows() {
    let jazz = local().await;
    let f = fixture(&jazz).await;
    jazz.delete::<Todo>(f.loose.id).await.unwrap();

    let current: Vec<Todo> = jazz.all(todos::query()).await.unwrap();
    assert!(!current.iter().any(|todo| todo.id == f.loose.id));

    let with_deleted: Vec<Todo> = jazz.all(todos::query().include_deleted()).await.unwrap();
    assert!(with_deleted.iter().any(|todo| todo.id == f.loose.id));
    assert_eq!(with_deleted.len(), current.len() + 1);
    // Read options keep it too.
    let with_options: Vec<Todo> = jazz
        .all_with_options(
            todos::query().include_deleted(),
            jazz_app::ReadOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(with_options.len(), with_deleted.len());

    let deleted: Vec<_> = jazz
        .all(
            todos::query()
                .include_deleted()
                .select((todos::title(), jazz_app::deleted_at())),
        )
        .await
        .unwrap();
    for row in deleted {
        let (title, deleted_at): (String, Option<Timestamp>) = row.values;
        assert_eq!(deleted_at.is_some(), title == "Loose end", "{title}");
    }
    jazz.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_lists_a_rows_accepted_versions() {
    // History lists versions an authority accepted: a local app has none.
    let (server, config) = server().await;
    let jazz = bindings::open(config()).await.unwrap();
    let f = fixture(&jazz).await;
    let rename = |title: &str| TodoUpdate {
        title: Some(title.into()),
        ..Default::default()
    };
    jazz.update(f.milk.id, rename("Buy oat milk"))
        .await
        .unwrap();
    jazz.update(f.milk.id, rename("Buy soy milk"))
        .await
        .unwrap();

    let versions = settled(
        &jazz,
        todos::history()
            .id(f.milk.id)
            .order_by_accepted_at(Order::Asc),
        3,
    )
    .await;
    let got: Vec<_> = versions.iter().map(|v| v.row.title.as_str()).collect();
    assert_eq!(got, ["Buy milk", "Buy oat milk", "Buy soy milk"]);
    assert!(versions.iter().all(|v| v.row.id == f.milk.id));
    assert!(
        versions
            .windows(2)
            .all(|pair| pair[0].transaction_id != pair[1].transaction_id
                && pair[0].accepted_at <= pair[1].accepted_at)
    );
    let now = Timestamp::now();
    assert!(
        versions
            .iter()
            .all(|v| v.accepted_at <= now && v.accepted_at.0 > 0)
    );

    // Filters and windows apply to versions.
    let newest: Vec<Version<Todo>> = remote(
        &jazz,
        todos::history()
            .filter(todos::id().is_in([f.milk.id, f.bread.id]))
            .filter(todos::title().contains("milk"))
            .order_by_accepted_at(Order::Desc)
            .limit(2),
    )
    .await
    .unwrap();
    let got: Vec<_> = newest.iter().map(|v| v.row.title.as_str()).collect();
    assert_eq!(got, ["Buy soy milk", "Buy oat milk"]);

    // Acceptance time ranges, in milliseconds.
    let first = versions[0].accepted_at;
    let since: Vec<Version<Todo>> = remote(&jazz, todos::history().id(f.milk.id).accepted(first..))
        .await
        .unwrap();
    assert_eq!(since.len(), 3);
    let before: Vec<Version<Todo>> =
        remote(&jazz, todos::history().id(f.milk.id).accepted(..first))
            .await
            .unwrap();
    assert!(before.is_empty());
    // Bounds, inclusive and exclusive, against the decoded times.
    let (first, last) = (versions[0].accepted_at, versions[2].accepted_at);
    let expect = |within: &dyn Fn(Timestamp) -> bool| {
        versions.iter().filter(|v| within(v.accepted_at)).count()
    };
    let closed = remote(&jazz, todos::history().id(f.milk.id).accepted(first..=last))
        .await
        .unwrap();
    assert_eq!(closed.len(), expect(&|at| first <= at && at <= last));
    assert_eq!(closed.len(), 3);
    let half_open = remote(&jazz, todos::history().id(f.milk.id).accepted(first..last))
        .await
        .unwrap();
    assert_eq!(half_open.len(), expect(&|at| first <= at && at < last));
    let after_first = remote(
        &jazz,
        todos::history()
            .id(f.milk.id)
            .accepted((std::ops::Bound::Excluded(first), std::ops::Bound::Unbounded)),
    )
    .await
    .unwrap();
    assert_eq!(after_first.len(), expect(&|at| first < at));
    let only_first = remote(
        &jazz,
        todos::history().id(f.milk.id).accepted(first..=first),
    )
    .await
    .unwrap();
    assert_eq!(only_first.len(), expect(&|at| at == first));
    assert!(!only_first.is_empty());
    let until: Vec<Version<Todo>> = remote(&jazz, todos::history().id(f.milk.id).accepted(..=now))
        .await
        .unwrap();
    assert_eq!(until.len(), 3);

    // A history read must be bounded to rows; subscribing is not supported.
    assert!(remote(&jazz, todos::history()).await.is_err());
    let refused = jazz
        .subscribe(todos::history().id(f.milk.id))
        .await
        .err()
        .expect("subscribing to history is refused");
    assert!(unsupported(refused).contains("history"));
    jazz.shutdown().await.unwrap();
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_combinations_are_refused_as_in_typescript() {
    let jazz = local().await;
    let f = fixture(&jazz).await;

    let union = || {
        Query::union([
            todos::query().filter(todos::done().eq(true)),
            todos::query().id(f.milk.id),
        ])
    };
    let reason = unsupported(
        jazz.all(union().filter(todos::done().eq(false)))
            .await
            .unwrap_err(),
    );
    assert!(reason.contains("filter(...)"), "{reason}");
    let reason = unsupported(
        jazz.all(union().include(todos::project_relation()))
            .await
            .unwrap_err(),
    );
    assert!(reason.contains("include(...)"), "{reason}");
    let reason = unsupported(jazz.all(union().include_deleted()).await.unwrap_err());
    assert!(reason.contains("include_deleted()"), "{reason}");
    // Part of a value can't be read through a union, a history read or a hop.
    let part = || jazz_app::ValuePart::TextUtf16 { from: 0, to: 2 };
    let reason = unsupported(
        jazz.all(union().part(todos::title(), part()))
            .await
            .unwrap_err(),
    );
    assert!(reason.contains("part(...)"), "{reason}");
    let reason = unsupported(
        jazz.all(todos::history().id(f.milk.id).part(todos::title(), part()))
            .await
            .unwrap_err(),
    );
    assert!(reason.contains("part(...)"), "{reason}");
    let reason = unsupported(
        jazz.all(
            todos::query()
                .part(todos::title(), part())
                .hop_to(todos::project_relation()),
        )
        .await
        .unwrap_err(),
    );
    assert!(reason.contains("part(...)"), "{reason}");
    let reason = unsupported(
        jazz.all(union().gather(todos::parent_relation(), 3))
            .await
            .unwrap_err(),
    );
    assert!(reason.contains("gather(...)"), "{reason}");
    let reason = unsupported(
        jazz.all(Query::<Todo>::union(Vec::new()))
            .await
            .unwrap_err(),
    );
    assert!(reason.contains("at least one"), "{reason}");
    let reason = unsupported(
        jazz.all(Query::union_named([
            ("a", todos::query()),
            ("a", todos::query()),
        ]))
        .await
        .unwrap_err(),
    );
    assert!(reason.contains("differ"), "{reason}");

    // Gather with include, as in TypeScript.
    let reason = unsupported(
        jazz.all(
            todos::query()
                .id(f.skimmed.id)
                .gather(todos::parent_relation(), 3)
                .include(todos::project_relation()),
        )
        .await
        .unwrap_err(),
    );
    assert!(reason.contains("include(...)"), "{reason}");

    // An include's query does not hop.
    let reason = unsupported(
        jazz.all(
            projects::query().include(
                projects::todos_via_project()
                    .with(projects::query().hop_to(projects::todos_via_project())),
            ),
        )
        .await
        .unwrap_err(),
    );
    assert!(reason.contains("include(...)"), "{reason}");
    jazz.shutdown().await.unwrap();
}

/// Through a sync server: a backend writes; another hops, gathers and
/// reads history at the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relations_and_history_through_a_server() {
    let (server, config) = server().await;
    let writer = bindings::open(config()).await.unwrap();
    let f = fixture(&writer).await;
    writer
        .update(
            f.milk.id,
            TodoUpdate {
                title: Some("Buy oat milk".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let reader = bindings::open(config()).await.unwrap();
    let gathered = settled(
        &reader,
        todos::query()
            .id(f.lowfat.id)
            .gather(todos::parent_relation(), 10)
            .order_by(todos::title(), Order::Asc),
        3,
    )
    .await;
    assert_eq!(titles(&gathered), ["Buy oat milk", "Low-fat", "Skimmed"]);

    let projects = remote(
        &reader,
        todos::query()
            .id(f.lowfat.id)
            .hop_to(todos::parent_relation())
            .hop_to(todos::project_relation()),
    )
    .await
    .unwrap();
    assert_eq!(projects, std::slice::from_ref(&f.home));

    let versions = settled(
        &reader,
        todos::history()
            .id(f.milk.id)
            .order_by_accepted_at(Order::Asc),
        2,
    )
    .await;
    let got: Vec<_> = versions
        .into_iter()
        .map(|version| version.row.title)
        .collect();
    assert_eq!(got, ["Buy milk", "Buy oat milk"]);

    writer.shutdown().await.unwrap();
    reader.shutdown().await.unwrap();
    server.shutdown().await;
}

/// The same queries by name, as bindings in other languages build them,
/// read through the low-level client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn untyped_queries_lower_the_same_way() {
    use jazz_app::jazz::query::{self as q, OrderDirection};
    use jazz_app::untyped::{self, GatherStep, Hop, Table};

    let jazz = local().await;
    let f = fixture(&jazz).await;
    let todos_table = || {
        Table::new(
            "todos",
            ["title", "done", "description", "parent", "project"],
        )
    };
    let projects_table = Table::new("projects", ["name"]);
    let by_id = |id: jazz_app::ObjectId| {
        q::eq(
            q::col("id"),
            q::lit(jazz_app::jazz::records::Value::Uuid(*id.uuid())),
        )
    };
    let read = |query: untyped::Query| {
        let deleted = query.reads_deleted();
        let query = query.into_core();
        jazz.with_client(move |client| async move {
            let mut opts = jazz_app::jazz::db::ReadOpts::default();
            opts.read_view.include_deleted = deleted;
            let rows = client.query_with_opts(query?, opts).await?;
            Ok::<_, Error>(
                rows.into_iter()
                    .map(|row| {
                        row.fields
                            .into_iter()
                            .find(|field| field.name == "title" || field.name == "name")
                            .map(|field| format!("{:?}", field.value))
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>(),
            )
        })
    };

    let projects = read(
        untyped::Query::table(todos_table())
            .filter(by_id(f.lowfat.id))
            .hop_to(Hop::Forward {
                column: "parent".into(),
                to: todos_table(),
            })
            .hop_to(Hop::Forward {
                column: "project".into(),
                to: projects_table,
            }),
    )
    .await
    .unwrap();
    assert_eq!(
        projects,
        [format!(
            "{:?}",
            jazz_app::jazz::tools::Value::Text("Home".into())
        )]
    );

    let gathered = read(
        untyped::Query::table(todos_table())
            .filter(by_id(f.lowfat.id))
            .gather(
                GatherStep::Along {
                    column: "parent".into(),
                    filters: Vec::new(),
                },
                10,
            )
            .order_by("title", OrderDirection::Asc),
    )
    .await
    .unwrap();
    assert_eq!(gathered.len(), 3);

    let refused = read(untyped::Query::union(
        todos_table(),
        [(
            "a".to_owned(),
            untyped::Query::table(Table::new("projects", ["name"])),
        )],
    ))
    .await
    .unwrap_err();
    assert!(unsupported(refused).contains("arms must read todos"));

    assert!(untyped::accepted_at(&jazz_app::jazz::tools::Value::Text("x".into())).is_none());
    let _ = (&f.home, &f.milk);
    jazz.shutdown().await.unwrap();
}

#[allow(dead_code)]
#[path = "bindings/tasks.rs"]
mod task_bindings;

/// A hop over an array relation (`uuid[]`) reaches every row it
/// references, as TypeScript's `hopTo` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hop_to_follows_an_array_relation() {
    use task_bindings::{NewStep, NewTask, Task, steps, tasks};
    struct NoComputed;
    impl task_bindings::Computed for NoComputed {
        fn tasks_urgent(&self, _: i32, _: task_bindings::TaskPriority) -> bool {
            false
        }
        fn tasks_title_len(&self, title: String) -> i32 {
            title.chars().count() as i32
        }
        fn iso_date_view(&self, stored: Timestamp) -> String {
            stored.0.to_string()
        }
        fn iso_date_stored(&self, view: String) -> Timestamp {
            Timestamp(view.parse().unwrap())
        }
    }
    let jazz = task_bindings::open(Config::new(AppId::random()), NoComputed)
        .await
        .unwrap();
    let a = jazz.insert(NewTask::new("A", 1, Vec::new())).await.unwrap();
    let b = jazz.insert(NewTask::new("B", 2, Vec::new())).await.unwrap();
    let _c = jazz.insert(NewTask::new("C", 3, Vec::new())).await.unwrap();
    jazz.insert(NewStep::new("both", a.id, vec![a.id, b.id]))
        .await
        .unwrap();
    jazz.insert(NewStep::new("none", a.id, Vec::new()))
        .await
        .unwrap();
    let blockers: Vec<Task> = jazz
        .all(
            steps::query()
                .filter(steps::label().eq("both"))
                .hop_to(steps::blocker_rows())
                .order_by(tasks::title(), Order::Asc),
        )
        .await
        .unwrap();
    let got: Vec<_> = blockers.iter().map(|task| task.title.as_str()).collect();
    assert_eq!(got, ["A", "B"]);
    jazz.shutdown().await.unwrap();
}
