//! The typed docs snippets, run against a local app: they compile in the
//! crate, and this checks that they also do what the docs say.

use ::jazz_app::{AppId, Config, Storage};
use tempfile::TempDir;
use todo_server_docs::docs_snippets as snippets;
use todo_server_docs::jazz_app::{self, NewProject, NewTodo};

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
            project: Some(project.id),
            ..NewTodo::new("B: parent", false)
        })
        .await
        .unwrap();
    let child = jazz
        .insert(NewTodo {
            parent: Some(parent.id),
            ..NewTodo::new("A: buy milk", false)
        })
        .await
        .unwrap();

    assert_eq!(snippets::read_todos_oneshot(&jazz).await.unwrap().len(), 2);
    let sorted = snippets::read_todos_sorted(&jazz).await.unwrap();
    assert_eq!(sorted[0].id, child.id);
    let page = snippets::read_todo_page(&jazz, 1, 2).await.unwrap();
    assert_eq!(page.iter().map(|t| t.id).collect::<Vec<_>>(), [parent.id]);
    let of_project = snippets::read_open_todos_of_project(&jazz, project.id)
        .await
        .unwrap();
    assert_eq!(
        of_project.iter().map(|t| t.id).collect::<Vec<_>>(),
        [parent.id]
    );
    snippets::where_operator_examples(&jazz).await.unwrap();
    snippets::composing_queries();

    snippets::clear_nullable_fields(&jazz, child.id)
        .await
        .unwrap();
    let child_now = jazz.get::<jazz_app::Todo>(child.id).await.unwrap().unwrap();
    assert_eq!(child_now.parent, None);

    let created = snippets::group_todo_writes(&jazz, parent.id).await.unwrap();
    assert!(jazz.get::<jazz_app::Todo>(created).await.unwrap().is_some());
    let parent_now = jazz
        .get::<jazz_app::Todo>(parent.id)
        .await
        .unwrap()
        .unwrap();
    assert!(parent_now.done);

    snippets::write_todo_crud(&jazz, child.id).await.unwrap();
    assert!(
        jazz.get::<jazz_app::Todo>(child.id)
            .await
            .unwrap()
            .is_none()
    );
    snippets::write_todo_with_default_durability(&jazz)
        .await
        .unwrap();
    // The created todos from the CRUD, transaction and durability snippets
    // and the parent remain.
    assert_eq!(snippets::read_todos_oneshot(&jazz).await.unwrap().len(), 4);

    jazz.shutdown().await.unwrap();
}
