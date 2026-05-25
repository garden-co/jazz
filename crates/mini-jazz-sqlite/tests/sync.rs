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
fn duplicate_scope_import_does_not_invalidate_subscription() -> mini_jazz_sqlite::Result<()> {
    let schema = Schema::new().table("todos", |t| {
        t.text("title");
        t.bool("done");
    });

    let mut alice = Harness::new()
        .client("alice", schema.clone())
        .durable_in_memory()?;
    let mut bob = Harness::new().client("bob", schema).durable_in_memory()?;

    alice.write(|tx| {
        tx.insert("todos", json!({ "title": "Synced once", "done": false }))?;
        Ok(())
    })?;

    let open_todos = query("todos").filter(eq("done", false));
    let alice_result = alice.all(open_todos.clone())?;
    let bundle = alice.export_query_scope(&alice_result.scope)?;
    bob.import_query_scope(&bundle)?;

    let subscription = bob.subscribe(open_todos)?;
    assert_eq!(bob.subscription_rerun_count(subscription)?, 1);

    bob.import_query_scope(&bundle)?;
    let diff = bob.poll_subscription(subscription)?;

    assert_eq!(diff.added.len(), 0);
    assert_eq!(diff.updated.len(), 0);
    assert_eq!(diff.removed.len(), 0);
    assert_eq!(bob.subscription_rerun_count(subscription)?, 1);

    Ok(())
}

#[test]
fn importing_non_visible_history_does_not_invalidate_subscription() -> mini_jazz_sqlite::Result<()>
{
    let schema = Schema::new().table("todos", |t| {
        t.text("title");
        t.bool("done");
    });

    let mut core = Harness::new()
        .authority("core", schema.clone())
        .durable_in_memory()?;
    let mut bob = Harness::new().client("bob", schema).durable_in_memory()?;

    core.write(|tx| {
        tx.insert("todos", json!({ "title": "First", "done": false }))?;
        Ok(())
    })?;
    let open_todos = query("todos").filter(eq("done", false));
    let first = core.all(open_todos.clone())?;
    let row_id = first.scope.result_rows[0].row_id.clone();
    let first_tx = first.scope.result_rows[0].tx_id.clone();
    core.accept_transaction(&first_tx, 1)?;

    core.write(|tx| {
        tx.update("todos", &row_id, json!({ "title": "Second" }))?;
        Ok(())
    })?;
    let second = core.all(open_todos.clone())?;
    let second_tx = second.scope.result_rows[0].tx_id.clone();
    core.accept_transaction(&second_tx, 2)?;

    bob.import_query_scope(&core.export_transaction(&second_tx)?)?;
    let subscription = bob.subscribe(open_todos)?;
    assert_eq!(bob.subscription_rerun_count(subscription)?, 1);

    bob.import_query_scope(&core.export_transaction(&first_tx)?)?;
    let diff = bob.poll_subscription(subscription)?;

    assert_eq!(diff.added.len(), 0);
    assert_eq!(diff.updated.len(), 0);
    assert_eq!(diff.removed.len(), 0);
    assert_eq!(bob.subscription_rerun_count(subscription)?, 1);

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
    assert!(after_delete
        .scope
        .predicate_scopes
        .iter()
        .any(|scope| scope.table == "projects" && scope.row_id == project_id));

    bob.import_query_scope(&alice.export_query_scope(&after_delete.scope)?)?;
    let bob_result = bob.all(open_todos)?;

    assert_eq!(bob_result.rows.len(), 1);
    assert_eq!(bob_result.rows[0].get("title").unwrap(), "Sync absence");
    assert!(bob_result.rows[0].include("project").is_none());

    Ok(())
}

#[test]
fn query_scope_records_filter_predicates() -> mini_jazz_sqlite::Result<()> {
    let schema = Schema::new().table("todos", |t| {
        t.text("title");
        t.bool("done");
        t.index("open_by_created", ["done", "$createdAt"]);
    });

    let mut alice = Harness::new().client("alice", schema).durable_in_memory()?;

    alice.write(|tx| {
        tx.insert("todos", json!({ "title": "Visible", "done": false }))?;
        tx.insert("todos", json!({ "title": "Filtered out", "done": true }))?;
        Ok(())
    })?;

    let open_todos = query("todos")
        .filter(eq("done", false))
        .order_by("$createdAt", Desc)
        .limit(20);

    let result = alice.all(open_todos)?;
    let bundle = alice.export_query_scope(&result.scope)?;

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.scope.predicate_scopes.len(), 1);
    assert_eq!(result.scope.predicate_scopes[0].table, "todos");
    assert_eq!(result.scope.predicate_scopes[0].column, "done");
    assert_eq!(result.scope.predicate_scopes[0].value, "false");
    assert_eq!(bundle.predicate_scopes.len(), 1);
    assert_eq!(bundle.predicate_scopes[0].table, "todos");
    assert_eq!(bundle.predicate_scopes[0].column, "done");
    assert_eq!(bundle.predicate_scopes[0].value, "false");

    Ok(())
}

#[test]
fn query_scope_bundle_import_reproduces_row_entering_filter() -> mini_jazz_sqlite::Result<()> {
    let schema = Schema::new().table("todos", |t| {
        t.text("title");
        t.bool("done");
        t.index("open_by_created", ["done", "$createdAt"]);
    });

    let mut alice = Harness::new()
        .client("alice", schema.clone())
        .durable_in_memory()?;
    let mut bob = Harness::new().client("bob", schema).durable_in_memory()?;
    let mut hidden_id = String::new();

    alice.write(|tx| {
        tx.insert(
            "todos",
            json!({ "title": "Already visible", "done": false }),
        )?;
        let hidden = tx.insert("todos", json!({ "title": "Enters later", "done": true }))?;
        hidden_id = hidden.id().to_owned();
        Ok(())
    })?;

    let open_todos = query("todos")
        .filter(eq("done", false))
        .order_by("$createdAt", Desc);

    let initial = alice.all(open_todos.clone())?;
    bob.import_query_scope(&alice.export_query_scope(&initial.scope)?)?;
    assert_eq!(bob.all(open_todos.clone())?.rows.len(), 1);

    alice.write(|tx| {
        tx.update("todos", &hidden_id, json!({ "done": false }))?;
        Ok(())
    })?;

    let after_update = alice.all(open_todos.clone())?;
    assert_eq!(after_update.rows.len(), 2);

    bob.import_query_scope(&alice.export_query_scope(&after_update.scope)?)?;
    let bob_result = bob.all(open_todos)?;

    assert_eq!(bob_result.rows.len(), 2);
    assert_eq!(bob_result.rows[0].get("title").unwrap(), "Enters later");

    Ok(())
}

#[test]
fn query_scope_bundle_import_reproduces_row_leaving_filter() -> mini_jazz_sqlite::Result<()> {
    let schema = Schema::new().table("todos", |t| {
        t.text("title");
        t.bool("done");
        t.index("open_by_created", ["done", "$createdAt"]);
    });

    let mut alice = Harness::new()
        .client("alice", schema.clone())
        .durable_in_memory()?;
    let mut bob = Harness::new().client("bob", schema).durable_in_memory()?;
    let mut row_id = String::new();

    alice.write(|tx| {
        let row = tx.insert("todos", json!({ "title": "Leaves later", "done": false }))?;
        row_id = row.id().to_owned();
        Ok(())
    })?;

    let open_todos = query("todos")
        .filter(eq("done", false))
        .order_by("$createdAt", Desc);

    let initial = alice.all(open_todos.clone())?;
    bob.import_query_scope(&alice.export_query_scope(&initial.scope)?)?;
    assert_eq!(bob.all(open_todos.clone())?.rows.len(), 1);

    alice.write(|tx| {
        tx.update("todos", &row_id, json!({ "done": true }))?;
        Ok(())
    })?;

    let after_update = alice.all(open_todos.clone())?;
    assert_eq!(after_update.rows.len(), 0);
    assert_eq!(after_update.scope.predicate_scopes.len(), 1);

    bob.import_query_scope(&alice.export_query_scope(&after_update.scope)?)?;
    assert_eq!(bob.all(open_todos)?.rows.len(), 0);

    Ok(())
}

#[test]
fn branch_query_scope_bundle_import_reproduces_branch_result() -> mini_jazz_sqlite::Result<()> {
    let schema = Schema::new().table("todos", |t| {
        t.text("title");
        t.bool("done");
        t.index("open_by_created", ["done", "$createdAt"]);
    });

    let mut alice = Harness::new()
        .client("alice", schema.clone())
        .durable_in_memory()?;
    let mut bob = Harness::new().client("bob", schema).durable_in_memory()?;

    alice.write(|tx| {
        tx.insert("todos", json!({ "title": "Base row", "done": false }))?;
        Ok(())
    })?;

    let open_todos = query("todos")
        .filter(eq("done", false))
        .order_by("$createdAt", Desc);
    let base = alice.all(open_todos.clone())?;
    let base_tx = base.scope.result_rows[0].tx_id.clone();
    alice.accept_transaction(&base_tx, 1)?;

    alice.create_branch("draft", 1)?;
    alice.write_on_branch("draft", |tx| {
        tx.insert("todos", json!({ "title": "Draft row", "done": false }))?;
        Ok(())
    })?;

    let alice_result = alice.all_on_branch(open_todos.clone(), "draft")?;
    let bundle = alice.export_query_scope(&alice_result.scope)?;

    bob.import_query_scope(&bundle)?;
    let bob_result = bob.all_on_branch(open_todos, "draft")?;

    assert_eq!(bob_result.rows.len(), 2);
    assert_eq!(bob_result.rows[0].get("title").unwrap(), "Draft row");
    assert_eq!(bob_result.rows[1].get("title").unwrap(), "Base row");

    Ok(())
}
