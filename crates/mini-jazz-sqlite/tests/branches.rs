use mini_jazz_sqlite::{eq, query, Desc, Harness, Schema};
use serde_json::json;

#[test]
fn branch_query_overlays_branch_rows_on_main_base_epoch() -> mini_jazz_sqlite::Result<()> {
    let schema = Schema::new().table("todos", |t| {
        t.text("title");
        t.bool("done");
        t.index("open_by_created", ["done", "$createdAt"]);
    });

    let mut alice = Harness::new().client("alice", schema).durable_in_memory()?;

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

    let draft = alice.all_on_branch(open_todos, "draft")?;

    assert_eq!(draft.rows.len(), 2);
    assert_eq!(draft.rows[0].get("title").unwrap(), "Draft row");
    assert_eq!(draft.rows[1].get("title").unwrap(), "Base row");

    Ok(())
}

#[test]
fn branch_base_stays_pinned_when_main_advances() -> mini_jazz_sqlite::Result<()> {
    let schema = Schema::new().table("todos", |t| {
        t.text("title");
        t.bool("done");
        t.index("open_by_created", ["done", "$createdAt"]);
    });

    let mut alice = Harness::new().client("alice", schema).durable_in_memory()?;

    alice.write(|tx| {
        tx.insert("todos", json!({ "title": "Base title", "done": false }))?;
        Ok(())
    })?;
    let open_todos = query("todos")
        .filter(eq("done", false))
        .order_by("$createdAt", Desc);
    let base = alice.all(open_todos.clone())?;
    let row_id = base.scope.result_rows[0].row_id.clone();
    let base_tx = base.scope.result_rows[0].tx_id.clone();
    alice.accept_transaction(&base_tx, 1)?;

    alice.create_branch("draft", 1)?;

    alice.write(|tx| {
        tx.update("todos", &row_id, json!({ "title": "Main advanced" }))?;
        Ok(())
    })?;
    let main_advanced = alice.all(open_todos.clone())?;
    let main_advanced_tx = main_advanced.scope.result_rows[0].tx_id.clone();
    alice.accept_transaction(&main_advanced_tx, 2)?;

    let draft_before_overlay = alice.all_on_branch(open_todos.clone(), "draft")?;
    assert_eq!(draft_before_overlay.rows.len(), 1);
    assert_eq!(
        draft_before_overlay.rows[0].get("title").unwrap(),
        "Base title"
    );

    alice.write_on_branch("draft", |tx| {
        tx.update("todos", &row_id, json!({ "title": "Draft title" }))?;
        Ok(())
    })?;

    let draft_after_overlay = alice.all_on_branch(open_todos, "draft")?;
    assert_eq!(draft_after_overlay.rows.len(), 1);
    assert_eq!(
        draft_after_overlay.rows[0].get("title").unwrap(),
        "Draft title"
    );

    Ok(())
}
