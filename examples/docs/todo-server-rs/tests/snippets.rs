//! The typed docs snippets, run against a local app: they compile in the
//! crate, and this checks that they also do what the docs say.

use std::ops::ControlFlow;
use std::time::Duration;

use jazz_server::JazzServer;
use tempfile::TempDir;
use todo_server_docs::docs_snippets as snippets;
use todo_server_docs::jazz_app::{
    self, AppId, Backend, Config, NewProject, NewTodo, ObjectId, Storage, todos,
};
use todo_server_docs::required_references;

fn ids(todos: &[jazz_app::Todo]) -> Vec<ObjectId> {
    todos.iter().map(|todo| todo.id).collect()
}

#[tokio::test]
async fn typed_snippets_run() {
    let data = TempDir::new().unwrap();
    let jazz = jazz_app::open(
        Config::new(AppId::from_name("docs-snippets"))
            .storage(Storage::Dir(data.path().to_path_buf())),
    )
    .await
    .unwrap();

    let project = jazz
        .insert(NewProject {
            name: "Docs".into(),
        })
        .await
        .unwrap();
    let parent = jazz
        .insert(NewTodo {
            priority: Some(5),
            project_id: Some(project.id),
            ..NewTodo::new("B: write docs", false)
        })
        .await
        .unwrap();
    let child = jazz
        .insert(NewTodo {
            parent_id: Some(parent.id),
            owner_id: Some(ObjectId::new()),
            description: Some("Semi-skimmed".into()),
            ..NewTodo::new("A: buy milk", false)
        })
        .await
        .unwrap();
    let done = jazz
        .insert(NewTodo {
            project_id: Some(project.id),
            ..NewTodo::new("C: done", true)
        })
        .await
        .unwrap();

    // Reading
    let open = snippets::read_todos_oneshot(&jazz).await.unwrap();
    assert_eq!(open.len(), 2);
    assert_eq!(
        ids(&snippets::read_todos_with_filters(&jazz).await.unwrap()),
        [parent.id]
    );
    assert_eq!(
        ids(&snippets::read_todos_sorted_by_title(&jazz).await.unwrap()),
        [child.id, parent.id]
    );
    assert_eq!(
        ids(&snippets::read_todo_page(&jazz, 2, 1).await.unwrap()),
        [parent.id]
    );

    let mut first = None;
    snippets::subscribe_todos(&jazz, |todos| {
        first = Some(ids(todos));
        ControlFlow::Break(())
    })
    .await
    .unwrap();
    assert_eq!(first.map(|ids| ids.len()), Some(2));

    let included = snippets::read_todos_with_includes(&jazz).await.unwrap();
    let (_, child_project, child_parent) = included
        .iter()
        .find(|(todo, _, _)| todo.id == child.id)
        .unwrap();
    assert!(child_project.is_none());
    let (included_parent, parent_project) = child_parent.as_ref().unwrap();
    assert_eq!(included_parent.id, parent.id);
    assert_eq!(parent_project.as_ref().map(|p| p.id), Some(project.id));

    let projects = snippets::read_projects_with_todos(&jazz).await.unwrap();
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].0.id, project.id);
    assert_eq!(ids(&projects[0].1), [parent.id]);

    let selected = snippets::read_todo_titles_with_selected_project(&jazz)
        .await
        .unwrap();
    let mut selected: Vec<(String, Option<String>)> = selected
        .into_iter()
        .map(|(todo, project)| (todo.into_values(), project.map(|p| p.into_values())))
        .collect();
    selected.sort();
    assert_eq!(
        selected,
        [
            ("A: buy milk".to_string(), None),
            ("B: write docs".to_string(), Some("Docs".to_string())),
        ]
    );

    let lineage = jazz
        .all(snippets::build_todo_lineage_query())
        .await
        .unwrap();
    assert_eq!(lineage.len(), 2);
    let [by_newest, by_title, urgent] = snippets::compose_todo_queries();
    assert_eq!(jazz.all(by_newest).await.unwrap().len(), 2);
    assert_eq!(
        ids(&jazz.all(by_title).await.unwrap()),
        [child.id, parent.id]
    );
    assert!(jazz.all(urgent).await.unwrap().is_empty());

    // Where operators
    snippets::where_operator_examples(&jazz, "milk", child.id, done.id)
        .await
        .unwrap();
    assert_eq!(
        snippets::read_todo(&jazz, child.id)
            .await
            .unwrap()
            .map(|t| t.id),
        Some(child.id)
    );
    assert!(
        snippets::read_todo(&jazz, ObjectId::new())
            .await
            .unwrap()
            .is_none()
    );
    drop(snippets::subscribe_open_todos(&jazz).await.unwrap());

    // Writing
    snippets::clear_nullable_todo_fields(&jazz, child.id)
        .await
        .unwrap();
    let child_now = jazz.get::<jazz_app::Todo>(child.id).await.unwrap().unwrap();
    assert_eq!(child_now.owner_id, None);
    assert_eq!(child_now.description.as_deref(), Some("Semi-skimmed"));

    assert_eq!(
        snippets::finish_todo_serializably(&jazz, child.id)
            .await
            .unwrap(),
        child.id
    );
    assert!(
        jazz.get::<jazz_app::Todo>(child.id)
            .await
            .unwrap()
            .unwrap()
            .done
    );
    assert!(
        snippets::finish_todo_serializably(&jazz, ObjectId::new())
            .await
            .is_err()
    );

    let restored = snippets::restore_deleted_todo(&jazz, done.id)
        .await
        .unwrap();
    assert_eq!(restored.id, done.id);
    assert_eq!(restored.title, "Restored task");
    assert!(!restored.done);

    // Locally nothing advises: there is no server to ask.
    let _ = snippets::can_read_todo(&jazz, parent.id).await;
    let _ = snippets::can_create_todo(&jazz, "Plan").await;
    let _ = snippets::read_todos_with_delete_permission(&jazz).await;
    let _ = snippets::read_editable_todos(&jazz).await;

    snippets::write_todo_crud(&jazz, child.id).await.unwrap();
    assert!(
        jazz.get::<jazz_app::Todo>(child.id)
            .await
            .unwrap()
            .is_none()
    );

    // The durable snippets wait for the server: locally they are refused
    // before they write.
    assert!(snippets::group_todo_writes(&jazz, parent.id).await.is_err());
    assert!(
        snippets::write_todo_with_durability_tiers(&jazz)
            .await
            .is_err()
    );
    assert!(snippets::upsert_todo(&jazz, ObjectId::new()).await.is_err());
    for should_cancel in [true, false] {
        assert!(
            snippets::stage_todo_across_steps(&jazz, should_cancel)
                .await
                .is_err()
        );
    }

    // The parent, the restored todo and the CRUD snippet's todo remain open.
    assert_eq!(snippets::read_todos_oneshot(&jazz).await.unwrap().len(), 3);

    jazz.shutdown().await.unwrap();
}

/// The required-references recipe's bindings match the TypeScript recipe's
/// `jazz.app.json`.
#[test]
fn required_references_bindings_are_current() {
    let app = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../todo-client-localfirst-ts/src/recipes/required-references/jazz.app.json"
    ))
    .unwrap();
    assert_eq!(
        required_references::SOURCE,
        jazz_app::jazz::tools::deployment::artifact::source_hash(&app),
        "src/required_references.rs is stale: regenerate it from the recipe's jazz.app.json"
    );
}

#[tokio::test]
async fn require_includes_snippet_runs() {
    let data = TempDir::new().unwrap();
    let jazz = required_references::open(
        Config::new(AppId::from_name("docs-required-references"))
            .storage(Storage::Dir(data.path().to_path_buf())),
    )
    .await
    .unwrap();

    let customer = jazz
        .insert(required_references::NewCustomer::new("Ada"))
        .await
        .unwrap();
    let order = jazz
        .insert(required_references::NewOrder::new(customer.id))
        .await
        .unwrap();
    // An order whose customer isn't there.
    jazz.insert(required_references::NewOrder::new(ObjectId::new()))
        .await
        .unwrap();

    let orders = snippets::read_orders_with_required_customer(&jazz)
        .await
        .unwrap();
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].0.id, order.id);
    assert_eq!(orders[0].1.id, customer.id);

    jazz.shutdown().await.unwrap();
}

/// Awaits `future`, failing the test if the server doesn't answer.
async fn within<T>(future: impl Future<Output = T>) -> Result<T, tokio::time::error::Elapsed> {
    tokio::time::timeout(Duration::from_secs(10), future).await
}

/// The snippets that wait for the server, against an in-process
/// `jazz-server` in main-backend mode, standing in for the app's backend.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_snippets_run_against_a_server() {
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .expect("start a Jazz server");
    let data = TempDir::new().unwrap();
    let backend = Backend::new(server.backend_secret(), server.admin_secret(), "alice")
        .permissions(include_bytes!("../jazz.permissions.json").as_slice());
    let jazz = jazz_app::open(
        Config::new(app_id)
            .server(server.base_url())
            .storage(Storage::Dir(data.path().to_path_buf()))
            .backend(backend),
    )
    .await
    .expect("open as Alice's backend");

    let existing = jazz.insert(NewTodo::new("Existing", false)).await.unwrap();
    let created = within(snippets::group_todo_writes(&jazz, existing.id))
        .await
        .expect("the server answers")
        .unwrap();
    let remote = within(snippets::read_todos_at_remote_tier(&jazz))
        .await
        .expect("the server answers")
        .unwrap();
    assert_eq!(
        remote.iter().map(|todo| todo.id).collect::<Vec<_>>(),
        [created]
    );

    within(snippets::write_todo_with_durability_tiers(&jazz))
        .await
        .expect("the server answers")
        .unwrap();

    let imported = ObjectId::new();
    let upserted = within(snippets::upsert_todo(&jazz, imported))
        .await
        .expect("the server answers")
        .unwrap();
    assert_eq!(upserted.id, imported);

    within(snippets::stage_todo_across_steps(&jazz, true))
        .await
        .expect("the server answers")
        .unwrap();
    within(snippets::stage_todo_across_steps(&jazz, false))
        .await
        .expect("the server answers")
        .unwrap();
    // Accepted, the error snippets return what they wrote.
    let inserted = within(snippets::insert_todo_and_wait(&jazz))
        .await
        .expect("the server answers")
        .unwrap();
    assert_eq!(
        inserted.map(|todo| todo.title).as_deref(),
        Some("Ship review fixes")
    );
    assert!(
        within(snippets::complete_todo_in_transaction(&jazz, existing.id))
            .await
            .expect("the server answers")
            .unwrap()
    );

    let staged = jazz
        .all(todos::query().filter(todos::title().eq("Review staged changes")))
        .await
        .unwrap();
    assert_eq!(staged.len(), 1);

    jazz.shutdown().await.unwrap();
    server.shutdown().await;
}

/// The error snippets against a server whose permissions refuse every
/// write: each reports the rejection rather than failing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn error_snippets_report_rejections() {
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .expect("start a Jazz server");
    let data = TempDir::new().unwrap();
    let backend = Backend::new(server.backend_secret(), server.admin_secret(), "alice")
        .permissions(include_bytes!("fixtures/read-only.permissions.json").as_slice());
    let jazz = jazz_app::open(
        Config::new(app_id)
            .server(server.base_url())
            .storage(Storage::Dir(data.path().to_path_buf()))
            .backend(backend),
    )
    .await
    .expect("open as Alice's backend");

    let refused = within(snippets::insert_todo_and_wait(&jazz))
        .await
        .expect("the server answers")
        .unwrap();
    assert!(refused.is_none());

    // A plain insert lands locally (the server refuses it later); the
    // transaction's update of it is refused too.
    let todo = jazz
        .insert(NewTodo::new("Local only", false))
        .await
        .unwrap();
    let committed = within(snippets::complete_todo_in_transaction(&jazz, todo.id))
        .await
        .expect("the server answers")
        .unwrap();
    assert!(!committed);
    let remote = within(jazz.all_with(todos::query(), jazz_app::ReadTier::Remote))
        .await
        .expect("the server answers")
        .unwrap();
    assert!(remote.is_empty());

    jazz.shutdown().await.unwrap();
    server.shutdown().await;
}
