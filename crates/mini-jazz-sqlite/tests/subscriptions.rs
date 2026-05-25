use mini_jazz_sqlite::{eq, query, Desc, Harness, Schema};
use serde_json::json;
#[test]
fn joined_subscription_updates_when_dependency_payload_changes() -> mini_jazz_sqlite::Result<()> {
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

    let mut alice = Harness::new().client("alice", schema).durable_in_memory()?;
    let mut project_id = String::new();

    alice.write(|tx| {
        let project = tx.insert("projects", json!({ "name": "SQLite Jazz" }))?;
        project_id = project.id().to_owned();
        tx.insert(
            "todos",
            json!({
                "title": "Wire subscriptions",
                "done": false,
                "project_id": project.id()
            }),
        )?;
        Ok(())
    })?;

    let open_todos = query("todos")
        .filter(eq("done", false))
        .include_required("project", "project_id")
        .order_by("$createdAt", Desc)
        .limit(20);

    let subscription = alice.subscribe(open_todos.clone())?;

    alice.write(|tx| {
        tx.update(
            "projects",
            &project_id,
            json!({ "name": "Renamed Project" }),
        )?;
        Ok(())
    })?;

    let diff = alice.poll_subscription(subscription)?;

    assert_eq!(diff.added.len(), 0);
    assert_eq!(diff.removed.len(), 0);
    assert_eq!(diff.updated.len(), 1);
    assert_eq!(
        diff.updated[0]
            .include("project")
            .unwrap()
            .get("name")
            .unwrap(),
        "Renamed Project"
    );

    Ok(())
}

#[test]
fn joined_subscription_removes_row_when_required_dependency_is_deleted(
) -> mini_jazz_sqlite::Result<()> {
    let schema = Schema::new()
        .table("projects", |t| {
            t.text("name");
        })
        .table("todos", |t| {
            t.text("title");
            t.bool("done");
            t.ref_("project_id", "projects");
        });

    let mut alice = Harness::new().client("alice", schema).durable_in_memory()?;
    let mut project_id = String::new();

    alice.write(|tx| {
        let project = tx.insert("projects", json!({ "name": "Required Project" }))?;
        project_id = project.id().to_owned();
        tx.insert(
            "todos",
            json!({
                "title": "Depends on project",
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

    let subscription = alice.subscribe(open_todos.clone())?;

    alice.write(|tx| {
        tx.delete("projects", &project_id)?;
        Ok(())
    })?;

    let diff = alice.poll_subscription(subscription)?;

    assert_eq!(diff.added.len(), 0);
    assert_eq!(diff.updated.len(), 0);
    assert_eq!(diff.removed.len(), 1);
    assert_eq!(diff.removed[0].get("title").unwrap(), "Depends on project");

    Ok(())
}

#[test]
fn optional_subscription_nulls_deleted_dependency() -> mini_jazz_sqlite::Result<()> {
    let schema = Schema::new()
        .table("projects", |t| {
            t.text("name");
        })
        .table("todos", |t| {
            t.text("title");
            t.bool("done");
            t.ref_("project_id", "projects");
        });

    let mut alice = Harness::new().client("alice", schema).durable_in_memory()?;
    let mut project_id = String::new();

    alice.write(|tx| {
        let project = tx.insert("projects", json!({ "name": "Optional Project" }))?;
        project_id = project.id().to_owned();
        tx.insert(
            "todos",
            json!({
                "title": "Can survive without project",
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

    let subscription = alice.subscribe(open_todos.clone())?;

    alice.write(|tx| {
        tx.delete("projects", &project_id)?;
        Ok(())
    })?;

    let diff = alice.poll_subscription(subscription)?;

    assert_eq!(diff.added.len(), 0);
    assert_eq!(diff.removed.len(), 0);
    assert_eq!(diff.updated.len(), 1);
    assert_eq!(
        diff.updated[0].get("title").unwrap(),
        "Can survive without project"
    );
    assert!(diff.updated[0].include("project").is_none());

    let after_delete = alice.all(open_todos)?;
    assert_eq!(after_delete.scope.dependency_rows.len(), 0);
    assert!(after_delete
        .scope
        .predicate_scopes
        .iter()
        .any(|scope| scope.table == "projects" && scope.row_id == project_id));

    Ok(())
}

#[test]
fn subscription_reruns_only_when_write_effects_overlap_scope() -> mini_jazz_sqlite::Result<()> {
    let schema = Schema::new()
        .table("projects", |t| {
            t.text("name");
        })
        .table("todos", |t| {
            t.text("title");
            t.bool("done");
        });

    let mut alice = Harness::new().client("alice", schema).durable_in_memory()?;

    alice.write(|tx| {
        tx.insert("todos", json!({ "title": "Watched", "done": false }))?;
        Ok(())
    })?;

    let open_todos = query("todos").filter(eq("done", false));
    let subscription = alice.subscribe(open_todos)?;
    assert_eq!(alice.subscription_rerun_count(subscription)?, 1);

    alice.write(|tx| {
        tx.insert("projects", json!({ "name": "Unrelated" }))?;
        Ok(())
    })?;
    let unrelated = alice.poll_subscription(subscription)?;
    assert_eq!(unrelated.added.len(), 0);
    assert_eq!(unrelated.updated.len(), 0);
    assert_eq!(unrelated.removed.len(), 0);
    assert_eq!(alice.subscription_rerun_count(subscription)?, 1);

    alice.write(|tx| {
        tx.insert("todos", json!({ "title": "Relevant", "done": false }))?;
        Ok(())
    })?;
    let relevant = alice.poll_subscription(subscription)?;
    assert_eq!(relevant.added.len(), 1);
    assert_eq!(alice.subscription_rerun_count(subscription)?, 2);

    Ok(())
}

#[test]
fn subscription_skips_non_result_write_to_unrelated_filter_column() -> mini_jazz_sqlite::Result<()>
{
    let schema = Schema::new().table("todos", |t| {
        t.text("title");
        t.bool("done");
    });

    let mut alice = Harness::new().client("alice", schema).durable_in_memory()?;
    let mut closed_id = String::new();

    alice.write(|tx| {
        tx.insert("todos", json!({ "title": "Open", "done": false }))?;
        let closed = tx.insert("todos", json!({ "title": "Closed", "done": true }))?;
        closed_id = closed.id().to_owned();
        Ok(())
    })?;

    let open_todos = query("todos").filter(eq("done", false));
    let subscription = alice.subscribe(open_todos)?;
    assert_eq!(alice.subscription_rerun_count(subscription)?, 1);

    alice.write(|tx| {
        tx.update("todos", &closed_id, json!({ "title": "Still closed" }))?;
        Ok(())
    })?;
    let diff = alice.poll_subscription(subscription)?;

    assert_eq!(diff.added.len(), 0);
    assert_eq!(diff.updated.len(), 0);
    assert_eq!(diff.removed.len(), 0);
    assert_eq!(alice.subscription_rerun_count(subscription)?, 1);

    Ok(())
}
