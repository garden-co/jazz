use mini_jazz_sqlite::{eq, query, Desc, Harness, Schema};
use serde_json::json;
#[test]
fn query_scope_bundle_import_reproduces_joined_result() -> mini_jazz_sqlite::Result<()> {
    let schema = Schema::new()
        .table("projects", |t| {
            t.text("name");
        })
        .table("todos", |t| {
            t.text("title");
            t.bool("done");
            t.ref_("project_id", "projects");
        });

    let mut alice = Harness::new()
        .client("alice", schema.clone())
        .durable_in_memory()?;
    let mut bob = Harness::new().client("bob", schema).durable_in_memory()?;

    alice.write(|tx| {
        let project = tx.insert("projects", json!({ "name": "Synced Project" }))?;
        tx.insert(
            "todos",
            json!({
                "title": "Sync query scope",
                "done": false,
                "project_id": project.id()
            }),
        )?;
        Ok(())
    })?;

    let open_todos = query("todos")
        .filter(eq("done", false))
        .include_required("project", "project_id")
        .order_by("$createdAt", Desc);

    let alice_result = alice.all(open_todos.clone())?;
    let bundle = alice.export_query_scope(&alice_result.scope)?;

    bob.import_query_scope(&bundle)?;
    let bob_result = bob.all(open_todos)?;

    assert_eq!(bob_result.rows.len(), 1);
    assert_eq!(bob_result.rows[0].get("title").unwrap(), "Sync query scope");
    assert_eq!(
        bob_result.rows[0]
            .include("project")
            .unwrap()
            .get("name")
            .unwrap(),
        "Synced Project"
    );
    assert_eq!(bob_result.scope.result_rows.len(), 1);
    assert_eq!(bob_result.scope.dependency_rows.len(), 1);

    Ok(())
}

#[test]
fn query_scope_bundle_import_reproduces_optional_missing_include() -> mini_jazz_sqlite::Result<()> {
    let schema = Schema::new()
        .table("projects", |t| {
            t.text("name");
        })
        .table("todos", |t| {
            t.text("title");
            t.bool("done");
            t.ref_("project_id", "projects");
        });

    let mut alice = Harness::new()
        .client("alice", schema.clone())
        .durable_in_memory()?;
    let mut bob = Harness::new().client("bob", schema).durable_in_memory()?;
    let mut project_id = String::new();

    alice.write(|tx| {
        let project = tx.insert("projects", json!({ "name": "Soon Missing" }))?;
        project_id = project.id().to_owned();
        tx.insert(
            "todos",
            json!({
                "title": "Sync absence",
                "done": false,
                "project_id": project.id()
            }),
        )?;
        Ok(())
    })?;

    let open_todos = query("todos")
        .filter(eq("done", false))
        .include_optional("project", "project_id")
        .order_by("$createdAt", Desc);

    let initial = alice.all(open_todos.clone())?;
    bob.import_query_scope(&alice.export_query_scope(&initial.scope)?)?;
    assert!(bob.all(open_todos.clone())?.rows[0]
        .include("project")
        .is_some());

    alice.write(|tx| {
        tx.delete("projects", &project_id)?;
        Ok(())
    })?;

    let after_delete = alice.all(open_todos.clone())?;
    assert_eq!(after_delete.scope.predicate_scopes.len(), 1);

    bob.import_query_scope(&alice.export_query_scope(&after_delete.scope)?)?;
    let bob_result = bob.all(open_todos)?;

    assert_eq!(bob_result.rows.len(), 1);
    assert_eq!(bob_result.rows[0].get("title").unwrap(), "Sync absence");
    assert!(bob_result.rows[0].include("project").is_none());

    Ok(())
}
