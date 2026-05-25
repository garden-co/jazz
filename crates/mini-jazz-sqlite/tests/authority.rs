use mini_jazz_sqlite::{eq, query, Harness, Schema};
use serde_json::json;

#[test]
fn authority_acceptance_enriches_existing_transaction_identity() -> mini_jazz_sqlite::Result<()> {
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
    let mut core = Harness::new()
        .authority("core", schema)
        .durable_in_memory()?;

    alice.write(|tx| {
        let project = tx.insert("projects", json!({ "name": "Authority Project" }))?;
        tx.insert(
            "todos",
            json!({
                "title": "Accept me",
                "done": false,
                "project_id": project.id()
            }),
        )?;
        Ok(())
    })?;

    let open_todos = query("todos")
        .filter(eq("done", false))
        .include_required("project", "project_id");
    let alice_result = alice.all(open_todos.clone())?;
    let tx_id = alice_result.scope.result_rows[0].tx_id.clone();

    let proposed = alice.export_query_scope(&alice_result.scope)?;
    core.import_query_scope(&proposed)?;
    core.accept_transaction(&tx_id, 1)?;

    let accepted_result = core.all(open_todos)?;
    let accepted = core.export_query_scope(&accepted_result.scope)?;
    alice.import_query_scope(&accepted)?;

    assert_eq!(alice.transaction_status(&tx_id)?, "global_durable_accepted");
    assert_eq!(alice.transaction_global_epoch(&tx_id)?, Some(1));
    assert_eq!(alice_result.scope.result_rows[0].tx_id, tx_id);

    Ok(())
}

#[test]
fn authority_rejection_repairs_optimistic_current_projection() -> mini_jazz_sqlite::Result<()> {
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
    let mut core = Harness::new()
        .authority("core", schema)
        .durable_in_memory()?;

    alice.write(|tx| {
        let project = tx.insert("projects", json!({ "name": "Rejected Project" }))?;
        tx.insert(
            "todos",
            json!({
                "title": "Reject me",
                "done": false,
                "project_id": project.id()
            }),
        )?;
        Ok(())
    })?;

    let open_todos = query("todos")
        .filter(eq("done", false))
        .include_required("project", "project_id");
    let optimistic = alice.all(open_todos.clone())?;
    assert_eq!(optimistic.rows.len(), 1);
    let tx_id = optimistic.scope.result_rows[0].tx_id.clone();

    core.import_query_scope(&alice.export_query_scope(&optimistic.scope)?)?;
    core.reject_transaction(&tx_id, json!({ "code": "stale_read" }))?;

    let rejected = core.export_transaction(&tx_id)?;
    alice.import_query_scope(&rejected)?;

    assert_eq!(alice.transaction_status(&tx_id)?, "rejected");
    assert_eq!(alice.all(open_todos)?.rows.len(), 0);

    Ok(())
}

#[test]
fn authority_rejects_stale_row_read_set() -> mini_jazz_sqlite::Result<()> {
    let schema = Schema::new().table("todos", |t| {
        t.text("title");
        t.bool("done");
    });

    let mut core = Harness::new()
        .authority("core", schema.clone())
        .durable_in_memory()?;
    let mut alice = Harness::new()
        .client("alice", schema.clone())
        .durable_in_memory()?;
    let mut bob = Harness::new().client("bob", schema).durable_in_memory()?;

    core.write(|tx| {
        tx.insert("todos", json!({ "title": "Base", "done": false }))?;
        Ok(())
    })?;

    let open_todos = query("todos").filter(eq("done", false));
    let base = core.all(open_todos.clone())?;
    let base_tx = base.scope.result_rows[0].tx_id.clone();
    let row_id = base.scope.result_rows[0].row_id.clone();
    core.accept_transaction(&base_tx, 1)?;

    let base_bundle = core.export_query_scope(&base.scope)?;
    alice.import_query_scope(&base_bundle)?;
    bob.import_query_scope(&base_bundle)?;

    alice.write(|tx| {
        tx.update("todos", &row_id, json!({ "title": "Alice wins" }))?;
        Ok(())
    })?;
    let alice_update = alice.all(open_todos.clone())?;
    let alice_tx = alice_update.scope.result_rows[0].tx_id.clone();
    core.import_query_scope(&alice.export_query_scope(&alice_update.scope)?)?;
    core.accept_transaction_validating_reads(&alice_tx, 2)?;

    bob.write(|tx| {
        tx.update("todos", &row_id, json!({ "title": "Bob is stale" }))?;
        Ok(())
    })?;
    let bob_update = bob.all(open_todos.clone())?;
    let bob_tx = bob_update.scope.result_rows[0].tx_id.clone();
    core.import_query_scope(&bob.export_query_scope(&bob_update.scope)?)?;

    let rejected = core
        .accept_transaction_validating_reads(&bob_tx, 3)
        .unwrap_err();
    assert!(rejected.to_string().contains("stale row read"));
    assert_eq!(core.transaction_status(&bob_tx)?, "rejected");

    Ok(())
}
