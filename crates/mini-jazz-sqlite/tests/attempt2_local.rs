use mini_jazz_sqlite::{eq, gt, query, Desc, Harness, Schema};
use serde_json::json;

#[test]
fn schema_driven_local_query_scope_rebuild_and_reopen() -> mini_jazz_sqlite::Result<()> {
    let schema = Schema::new()
        .table("projects", |t| {
            t.text("name");
            t.index("by_name", ["name", "$createdAt"]);
        })
        .table("todos", |t| {
            t.text("title");
            t.bool("done");
            t.ref_("project_id", "projects");
            t.index("open_by_created", ["done", "$createdAt"]);
        });

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("alice.db");

    let mut alice = Harness::new()
        .client("alice", schema.clone())
        .durable_at(&db_path)?;

    alice.write(|tx| {
        let project = tx.insert("projects", json!({ "name": "SQLite Jazz" }))?;
        tx.insert(
            "todos",
            json!({
                "title": "Design attempt2",
                "done": false,
                "project_id": project.id()
            }),
        )?;
        Ok(())
    })?;

    let open_todos = query("todos")
        .filter(eq("done", false))
        .filter(gt("$createdAt", 0))
        .include_required("project", "project_id")
        .order_by("$createdAt", Desc)
        .limit(20);

    let result = alice.all(open_todos.clone())?;

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("title").unwrap(), "Design attempt2");
    assert_eq!(
        result.rows[0]
            .include("project")
            .unwrap()
            .get("name")
            .unwrap(),
        "SQLite Jazz"
    );
    assert_eq!(result.scope.result_rows.len(), 1);
    assert_eq!(result.scope.dependency_rows.len(), 1);

    let before = alice.current_projection_fingerprint()?;
    alice.rebuild_current_projections()?;
    let after = alice.current_projection_fingerprint()?;
    assert_eq!(before, after);

    drop(alice);

    let reopened = Harness::new()
        .client("alice", schema)
        .durable_at(&db_path)?;
    let reopened_result = reopened.all(open_todos)?;

    assert_eq!(reopened_result.rows.len(), 1);
    assert_eq!(reopened_result.scope.result_rows.len(), 1);
    assert_eq!(reopened_result.scope.dependency_rows.len(), 1);

    Ok(())
}
