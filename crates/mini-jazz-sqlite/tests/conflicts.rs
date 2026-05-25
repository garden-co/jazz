use mini_jazz_sqlite::{eq, query, Harness, Schema};
use serde_json::json;

#[test]
fn concurrent_pending_updates_expose_resolved_row_with_conflict_meta(
) -> mini_jazz_sqlite::Result<()> {
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
        tx.update("todos", &row_id, json!({ "title": "Alice edit" }))?;
        Ok(())
    })?;
    bob.write(|tx| {
        tx.update("todos", &row_id, json!({ "title": "Bob edit" }))?;
        Ok(())
    })?;

    let alice_update = alice.all(open_todos.clone())?;
    let bob_update = bob.all(open_todos.clone())?;
    let alice_tx = alice_update.scope.result_rows[0].tx_id.clone();
    let bob_tx = bob_update.scope.result_rows[0].tx_id.clone();

    core.import_query_scope(&alice.export_query_scope(&alice_update.scope)?)?;
    core.import_query_scope(&bob.export_query_scope(&bob_update.scope)?)?;

    let merged = core.all(open_todos)?;
    let conflicts = merged.rows[0].get("$conflicts").unwrap();

    assert_eq!(merged.rows.len(), 1);
    assert!(conflicts.contains(&alice_tx));
    assert!(conflicts.contains(&bob_tx));

    Ok(())
}
