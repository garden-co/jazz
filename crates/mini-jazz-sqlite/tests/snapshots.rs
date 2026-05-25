use mini_jazz_sqlite::{eq, query, Desc, Harness, Schema};
use serde_json::json;

#[test]
fn accepted_global_epoch_snapshot_reads_history_without_projection() -> mini_jazz_sqlite::Result<()>
{
    let schema = Schema::new().table("todos", |t| {
        t.text("title");
        t.bool("done");
        t.index("open_by_created", ["done", "$createdAt"]);
    });

    let mut alice = Harness::new().client("alice", schema).durable_in_memory()?;
    let open_todos = query("todos")
        .filter(eq("done", false))
        .order_by("$createdAt", Desc)
        .limit(10);

    alice.write(|tx| {
        tx.insert("todos", json!({ "title": "First title", "done": false }))?;
        Ok(())
    })?;
    let first = alice.all(open_todos.clone())?;
    let row_id = first.scope.result_rows[0].row_id.clone();
    let first_tx = first.scope.result_rows[0].tx_id.clone();
    alice.accept_transaction(&first_tx, 1)?;

    alice.write(|tx| {
        tx.update("todos", &row_id, json!({ "title": "Second title" }))?;
        Ok(())
    })?;
    let second = alice.all(open_todos.clone())?;
    let second_tx = second.scope.result_rows[0].tx_id.clone();
    alice.accept_transaction(&second_tx, 2)?;

    let at_epoch_one = alice.all_at_global_epoch(open_todos.clone(), 1)?;
    let at_epoch_two = alice.all_at_global_epoch(open_todos, 2)?;

    assert_eq!(at_epoch_one.rows.len(), 1);
    assert_eq!(at_epoch_one.rows[0].get("title").unwrap(), "First title");
    assert_eq!(at_epoch_one.scope.result_rows[0].tx_id, first_tx);

    assert_eq!(at_epoch_two.rows.len(), 1);
    assert_eq!(at_epoch_two.rows[0].get("title").unwrap(), "Second title");
    assert_eq!(at_epoch_two.scope.result_rows[0].tx_id, second_tx);

    Ok(())
}
