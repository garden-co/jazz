//! Includes (related rows with each row) and select (partial rows),
//! through the bindings generated for tests/fixtures/todos and tasks.

#[path = "bindings/todos.rs"]
mod bindings;

#[allow(dead_code)]
#[path = "bindings/tasks.rs"]
mod task_bindings;

use bindings::{NewProject, NewTodo, Project, Todo, TodoUpdate, projects, todos};
use futures::StreamExt;
use jazz_app::{AppId, Backend, Config, Jazz, Order, Partial, Rows};
use jazz_server::JazzServer;

struct Fixture {
    home: Project,
    work: Project,
    milk: Todo,
    bread: Todo,
    loose: Todo,
    skimmed: Todo,
}

async fn fixture(jazz: &Jazz) -> Fixture {
    let home = jazz.insert(NewProject::new("Home")).await.unwrap();
    let work = jazz.insert(NewProject::new("Work")).await.unwrap();
    let milk = jazz
        .insert(NewTodo {
            project: Some(home.id),
            ..NewTodo::new("Buy milk", false)
        })
        .await
        .unwrap();
    let bread = jazz
        .insert(NewTodo {
            project: Some(home.id),
            ..NewTodo::new("Buy bread", true)
        })
        .await
        .unwrap();
    let loose = jazz.insert(NewTodo::new("Loose end", false)).await.unwrap();
    let skimmed = jazz
        .insert(NewTodo {
            parent: Some(milk.id),
            project: Some(home.id),
            ..NewTodo::new("Skimmed", false)
        })
        .await
        .unwrap();
    Fixture {
        home,
        work,
        milk,
        bread,
        loose,
        skimmed,
    }
}

fn by_title() -> jazz_app::Query<Todo> {
    todos::query().order_by(todos::title(), Order::Asc)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forward_includes_are_options_unless_required() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let f = fixture(&jazz).await;

    let rows: Vec<(Todo, Option<Project>)> = jazz
        .all(by_title().include(todos::project_relation()))
        .await
        .unwrap();
    let got: Vec<_> = rows
        .iter()
        .map(|(todo, project)| (todo.title.as_str(), project.as_ref().map(|p| p.id)))
        .collect();
    assert_eq!(
        got,
        [
            ("Buy bread", Some(f.home.id)),
            ("Buy milk", Some(f.home.id)),
            ("Loose end", None),
            ("Skimmed", Some(f.home.id)),
        ]
    );
    assert_eq!(rows[0].1.as_ref(), Some(&f.home));
    assert!(
        rows.iter()
            .any(|(todo, project)| *todo == f.loose && project.is_none())
    );

    // Requiring a nullable relation changes nothing, as in TypeScript:
    // still an `Option`, and every row kept.
    let required: Vec<(Todo, Option<Project>)> = jazz
        .all(by_title().include(todos::project_relation().require()))
        .await
        .unwrap();
    assert_eq!(required, rows);

    // `one` and `id` work as for plain rows.
    let (todo, parent) = jazz
        .one(
            todos::query()
                .id(f.skimmed.id)
                .include(todos::parent_relation()),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(todo, f.skimmed);
    assert_eq!(parent, Some(f.milk.clone()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reverse_includes_are_vecs_and_take_a_query() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let f = fixture(&jazz).await;

    let rows: Vec<(Project, Vec<Todo>)> = jazz
        .all(
            projects::query()
                .order_by(projects::name(), Order::Asc)
                .include(projects::todos_via_project()),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, f.home);
    let mut titles: Vec<_> = rows[0].1.iter().map(|t| t.title.as_str()).collect();
    titles.sort();
    assert_eq!(titles, ["Buy bread", "Buy milk", "Skimmed"]);
    assert_eq!(rows[1], (f.work.clone(), vec![]));

    // The included rows filtered, ordered and limited.
    let rows = jazz
        .all(
            projects::query().id(f.home.id).include(
                projects::todos_via_project().with(
                    todos::query()
                        .filter(todos::done().eq(false))
                        .order_by(todos::title(), Order::Desc)
                        .limit(1),
                ),
            ),
        )
        .await
        .unwrap();
    assert_eq!(rows, [(f.home.clone(), vec![f.skimmed.clone()])]);

    let rows = jazz
        .all(
            projects::query()
                .id(f.home.id)
                .include(projects::todos_via_project().with(by_title().offset(1).limit(1))),
        )
        .await
        .unwrap();
    assert_eq!(rows, [(f.home.clone(), vec![f.milk.clone()])]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn includes_chain_and_nest() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let f = fixture(&jazz).await;

    // Chained: one tuple element per include, in order.
    type Chained = (Todo, Option<Project>, Vec<Todo>, Option<Todo>);
    let rows: Vec<Chained> = jazz
        .all(
            by_title()
                .include(todos::project_relation())
                .include(todos::todos_via_parent())
                .include(todos::parent_relation()),
        )
        .await
        .unwrap();
    let (milk, project, children, parent) = &rows[1];
    assert_eq!(milk, &f.milk);
    assert_eq!(project.as_ref(), Some(&f.home));
    assert_eq!(children, std::slice::from_ref(&f.skimmed));
    assert_eq!(parent, &None);
    assert_eq!(rows[3].3.as_ref(), Some(&f.milk));

    // Nested: each project's open todos, each with its parent and project.
    type Nested = (Project, Vec<(Todo, Option<Todo>, Option<Project>)>);
    let rows: Vec<Nested> = jazz
        .all(
            projects::query()
                .order_by(projects::name(), Order::Asc)
                .include(
                    projects::todos_via_project().with(
                        by_title()
                            .filter(todos::done().eq(false))
                            .include(todos::parent_relation())
                            .include(todos::project_relation()),
                    ),
                ),
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        [
            (
                f.home.clone(),
                vec![
                    (f.milk.clone(), None, Some(f.home.clone())),
                    (
                        f.skimmed.clone(),
                        Some(f.milk.clone()),
                        Some(f.home.clone())
                    ),
                ]
            ),
            (f.work.clone(), vec![]),
        ]
    );

    // Nested through a forward include.
    let rows = jazz
        .all(todos::query().id(f.skimmed.id).include(
            todos::parent_relation().with(todos::query().include(todos::project_relation())),
        ))
        .await
        .unwrap();
    assert_eq!(
        rows,
        [(
            f.skimmed.clone(),
            Some((f.milk.clone(), Some(f.home.clone())))
        )]
    );
    let _ = f.bread;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn select_reads_partial_rows() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let f = fixture(&jazz).await;

    let rows: Vec<Partial<Todo, (String, bool)>> = jazz
        .all(by_title().select((todos::title(), todos::done())))
        .await
        .unwrap();
    assert_eq!(
        rows[0],
        Partial::new(f.bread.id, ("Buy bread".into(), true))
    );
    assert_eq!(rows.len(), 4);

    // One column is its value; partial rows take includes and filters.
    let rows: Vec<(Partial<Todo, String>, Option<Project>)> = jazz
        .all(
            todos::query()
                .select(todos::title())
                .filter(todos::parent().is_not_null())
                .include(todos::project_relation()),
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        [(
            Partial::new(f.skimmed.id, "Skimmed".into()),
            Some(f.home.clone())
        )]
    );

    // An include selects too.
    type Done = Partial<Todo, (bool, String)>;
    let rows: Vec<(Project, Vec<Done>)> = jazz
        .all(
            projects::query().id(f.home.id).include(
                projects::todos_via_project()
                    .with(by_title().select((todos::done(), todos::title())).limit(2)),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        rows[0].1,
        [
            Partial::new(f.bread.id, (true, "Buy bread".into())),
            Partial::new(f.milk.id, (false, "Buy milk".into())),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriptions_deliver_included_rows() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let f = fixture(&jazz).await;

    let mut changes = jazz
        .subscribe(
            projects::query()
                .id(f.home.id)
                .include(projects::todos_via_project().with(by_title())),
        )
        .await
        .unwrap();
    let mut rows = Rows::default();
    rows.apply(changes.next().await.unwrap().unwrap());
    assert_eq!(rows.as_slice()[0].1.len(), 3);

    // A change to an included row updates the row it is included with.
    jazz.update(
        f.milk.id,
        TodoUpdate {
            title: Some("Buy oat milk".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let wanted = |rows: &Rows<(Project, Vec<Todo>)>| {
        rows.as_slice()
            .first()
            .is_some_and(|(_, todos)| todos.iter().any(|t| t.title == "Buy oat milk"))
    };
    while !wanted(&rows) {
        let delta = tokio::time::timeout(std::time::Duration::from_secs(5), changes.next())
            .await
            .expect("an update")
            .unwrap()
            .unwrap();
        rows.apply(delta);
    }
}

/// Transforms, computed fields, enums, json and arrays decode in an
/// included row as in a row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn included_rows_decode_every_column_type() {
    use task_bindings::{Computed, NewBox, NewTask, Task, TaskPriority, boxes};

    struct App;
    impl Computed for App {
        fn tasks_urgent(&self, score: i32, priority: TaskPriority) -> bool {
            score > 5 || priority == TaskPriority::High
        }
        fn iso_date_view(&self, stored: jazz_app::Timestamp) -> String {
            format!("ms-{}", stored.0)
        }
        fn iso_date_stored(&self, view: String) -> jazz_app::Timestamp {
            jazz_app::Timestamp(view.trim_start_matches("ms-").parse().unwrap())
        }
    }

    let jazz = task_bindings::open(Config::new(AppId::random()), App)
        .await
        .unwrap();
    let task = jazz
        .insert(NewTask {
            priority: Some(TaskPriority::High),
            due_at: Some("ms-86400000".into()),
            meta: Some(jazz_app::Json(serde_json::json!({ "owner": "ops" }))),
            ..NewTask::new("Ship", 9, vec!["a".to_owned(), "b".to_owned()])
        })
        .await
        .unwrap();
    jazz.insert(NewBox {
        task: Some(task.id),
        ..NewBox::new("crate")
    })
    .await
    .unwrap();

    let rows = jazz
        .all(boxes::query().include(boxes::task_relation()))
        .await
        .unwrap();
    let included: &Task = rows[0].1.as_ref().unwrap();
    assert_eq!(included, &task);
    assert!(included.urgent);
}

/// Requiring a not-null relation includes the row itself, and keeps only
/// the rows whose referenced row is there, as `requireIncludes` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requiring_a_not_null_relation_is_an_inner_join() {
    use task_bindings::{NewStep, NewTask, Step, Task, steps};

    let jazz = task_bindings::open(Config::new(AppId::random()), NoComputed)
        .await
        .unwrap();
    let task = jazz
        .insert(NewTask::new("Ship", 1, Vec::new()))
        .await
        .unwrap();
    let held = jazz.insert(NewStep::new("held", task.id)).await.unwrap();
    let dangling = jazz
        .insert(NewStep::new("dangling", jazz_app::ObjectId::new()))
        .await
        .unwrap();

    let optional: Vec<(Step, Option<Task>)> = jazz
        .all(
            steps::query()
                .order_by(steps::label(), Order::Asc)
                .include(steps::task_relation()),
        )
        .await
        .unwrap();
    assert_eq!(
        optional,
        [(dangling, None), (held.clone(), Some(task.clone()))]
    );

    let required: Vec<(Step, Task)> = jazz
        .all(steps::query().include(steps::task_relation().require()))
        .await
        .unwrap();
    assert_eq!(required, [(held, task)]);
}

struct NoComputed;
impl task_bindings::Computed for NoComputed {
    fn tasks_urgent(&self, _: i32, _: task_bindings::TaskPriority) -> bool {
        false
    }
    fn iso_date_view(&self, stored: jazz_app::Timestamp) -> String {
        stored.0.to_string()
    }
    fn iso_date_stored(&self, view: String) -> jazz_app::Timestamp {
        jazz_app::Timestamp(view.parse().unwrap())
    }
}

/// Through a sync server: a backend writes, another reads with includes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn includes_through_a_server() {
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
    let config = || {
        Config::new(app_id).server(server.base_url()).backend(
            Backend::new(server.backend_secret(), server.admin_secret(), "alice")
                .permissions(permissions.clone()),
        )
    };
    let writer = bindings::open(config()).await.unwrap();
    let f = fixture(&writer).await;

    let reader = bindings::open(config()).await.unwrap();
    let query = projects::query()
        .id(f.home.id)
        .include(projects::todos_via_project().with(by_title().include(todos::parent_relation())));
    let rows = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let rows = reader
                .all_with(query.clone(), jazz_app::ReadTier::Remote)
                .await
                .unwrap();
            if rows.first().is_some_and(|(_, todos)| todos.len() == 3) {
                break rows;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the writes reach the reader through the server");
    let titles: Vec<_> = rows[0]
        .1
        .iter()
        .map(|(todo, parent)| (todo.title.as_str(), parent.as_ref().map(|p| p.id)))
        .collect();
    assert_eq!(
        titles,
        [
            ("Buy bread", None),
            ("Buy milk", None),
            ("Skimmed", Some(f.milk.id))
        ]
    );
    writer.shutdown().await.unwrap();
    reader.shutdown().await.unwrap();
    server.shutdown().await;
}
