use mini_jazz_sqlite::{eq, query, Harness, Schema};
use serde_json::json;

#[test]
fn optimistic_writes_sync_through_authority_fate_and_subscriptions() -> mini_jazz_sqlite::Result<()>
{
    let schema = Schema::new().table("todos", |t| {
        t.text("title");
        t.bool("done");
    });

    let mut authority = Harness::new()
        .authority("authority", schema.clone())
        .durable_in_memory()?;
    let mut alice = Harness::new()
        .client("alice", schema.clone())
        .durable_in_memory()?;
    let mut bob = Harness::new().client("bob", schema).durable_in_memory()?;

    authority.write(|tx| {
        tx.insert(
            "todos",
            json!({ "title": "Write whole-system test", "done": false }),
        )?;
        Ok(())
    })?;

    let open_todos = query("todos").filter(eq("done", false));
    let base = authority.all(open_todos.clone())?;
    let base_tx = base.scope.result_rows[0].tx_id.clone();
    let todo_id = base.scope.result_rows[0].row_id.clone();
    authority.accept_transaction(&base_tx, 1)?;

    let base_bundle = authority.export_query_scope(&base.scope)?;
    alice.import_query_scope(&base_bundle)?;
    bob.import_query_scope(&base_bundle)?;

    let bob_subscription = bob.subscribe(open_todos.clone())?;

    alice.write(|tx| {
        tx.update(
            "todos",
            &todo_id,
            json!({ "title": "Alice accepted draft" }),
        )?;
        Ok(())
    })?;
    let alice_draft = alice.all(open_todos.clone())?;
    let alice_tx = alice_draft.scope.result_rows[0].tx_id.clone();

    bob.write(|tx| {
        tx.update("todos", &todo_id, json!({ "title": "Bob stale draft" }))?;
        Ok(())
    })?;
    let bob_draft = bob.all(open_todos.clone())?;
    let bob_tx = bob_draft.scope.result_rows[0].tx_id.clone();
    let bob_local = bob.poll_subscription(bob_subscription)?;
    assert_eq!(bob_local.updated.len(), 1);
    assert_eq!(
        bob_local.updated[0].get("title").unwrap(),
        "Bob stale draft"
    );

    authority.import_query_scope(&alice.export_query_scope(&alice_draft.scope)?)?;
    authority.import_query_scope(&bob.export_query_scope(&bob_draft.scope)?)?;

    let with_candidates = authority.all(open_todos.clone())?;
    let conflicts = with_candidates.rows[0].get("$conflicts").unwrap();
    assert!(conflicts.contains(&alice_tx));
    assert!(conflicts.contains(&bob_tx));

    authority.accept_transaction_validating_reads(&alice_tx, 2)?;
    let rejected = authority
        .accept_transaction_validating_reads(&bob_tx, 3)
        .unwrap_err();
    assert!(rejected.to_string().contains("stale row read"));

    bob.import_query_scope(&authority.export_transaction(&alice_tx)?)?;
    let accepted_remote = bob.poll_subscription(bob_subscription)?;
    assert_eq!(accepted_remote.added.len(), 0);
    assert_eq!(accepted_remote.updated.len(), 0);
    assert_eq!(accepted_remote.removed.len(), 0);

    bob.import_query_scope(&authority.export_transaction(&bob_tx)?)?;
    let rejected_remote = bob.poll_subscription(bob_subscription)?;
    assert_eq!(rejected_remote.added.len(), 0);
    assert_eq!(rejected_remote.updated.len(), 1);
    assert_eq!(rejected_remote.removed.len(), 0);
    assert_eq!(
        rejected_remote.updated[0].get("title").unwrap(),
        "Alice accepted draft"
    );
    assert_eq!(
        bob.transaction_status(&alice_tx)?,
        "global_durable_accepted"
    );
    assert_eq!(bob.transaction_global_epoch(&alice_tx)?, Some(2));
    assert_eq!(bob.transaction_status(&bob_tx)?, "rejected");
    assert_eq!(
        bob.all(open_todos)?.rows[0].get("title").unwrap(),
        "Alice accepted draft"
    );

    Ok(())
}
