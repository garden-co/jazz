#![cfg(feature = "test-utils")]

use std::time::Duration;

use jazz_tools::row_input;
use jazz_tools::server::TestingServer;
use jazz_tools::sync_manager::SyncPayload;
use jazz_tools::test_support::wait_for_query;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder,
    TableSchema, Value, WriteContext,
};

fn todo_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

fn todo_query() -> jazz_tools::Query {
    QueryBuilder::new("todos")
        .select(&["title", "completed"])
        .build()
}

async fn all_todos(client: &JazzClient) -> Vec<(ObjectId, Vec<Value>)> {
    client.query(todo_query(), None).await.expect("query todos")
}

async fn wait_for_todos(
    client: &JazzClient,
    durability_tier: Option<DurabilityTier>,
    description: &str,
    predicate: impl Fn(&[(ObjectId, Vec<Value>)]) -> bool,
) -> Vec<(ObjectId, Vec<Value>)> {
    wait_for_query(
        client,
        todo_query(),
        durability_tier,
        Duration::from_secs(25),
        description,
        |rows| predicate(&rows).then_some(rows),
    )
    .await
}

async fn wait_for_edge_ready(client: &JazzClient) {
    let _ = wait_for_todos(
        client,
        Some(DurabilityTier::EdgeServer),
        "EdgeServer query readiness",
        |_| true,
    )
    .await;
}

async fn connect_user(server: &TestingServer, schema: Schema, user_id: &str) -> JazzClient {
    let client = JazzClient::connect(server.make_client_context_for_user(schema, user_id))
        .await
        .expect("connect user");
    wait_for_edge_ready(&client).await;
    client
}

async fn start_two_clients(schema: Schema) -> (TestingServer, JazzClient, JazzClient) {
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let alice = connect_user(&server, schema.clone(), "alice-transactions").await;
    let bob = connect_user(&server, schema, "bob-transactions").await;
    (server, alice, bob)
}

fn has_todo(
    rows: &[(ObjectId, Vec<Value>)],
    todo_id: ObjectId,
    title: &str,
    completed: bool,
) -> bool {
    rows.iter().any(|(id, values)| {
        *id == todo_id && values == &vec![Value::Text(title.to_string()), Value::Boolean(completed)]
    })
}

async fn insert_visible_todo(client: &JazzClient, title: &str, completed: bool) -> ObjectId {
    let (todo_id, _, batch_id) = client
        .insert(
            "todos",
            row_input!("title" => title, "completed" => completed),
        )
        .expect("insert visible todo");
    client
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("visible todo settles at edge");
    todo_id
}

#[tokio::test]
async fn transaction_stages_writes_and_can_commit() {
    let client = JazzClient::test_client(todo_schema()).await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let batch_id = tx.batch_id();

    let (todo_id, inserted_values, write_batch_id) = tx
        .insert(
            "todos",
            row_input!("title" => "ship transactions", "completed" => false),
        )
        .expect("insert in transaction");

    assert_eq!(write_batch_id, batch_id);
    assert!(
        all_todos(&client).await.is_empty(),
        "ordinary client reads should ignore an open transaction"
    );
    assert_eq!(
        all_todos(tx.client()).await,
        vec![(todo_id, inserted_values)],
        "transaction-scoped reads should include staged rows"
    );

    assert_eq!(tx.commit().expect("commit transaction"), batch_id);
    assert!(
        client.commit_transaction(batch_id).is_err(),
        "committed transaction should reject a second commit"
    );
}

#[tokio::test]
async fn transaction_can_be_rolled_back() {
    let client = JazzClient::test_client(todo_schema()).await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let batch_id = tx.batch_id();

    let (todo_id, inserted_values, _) = tx
        .insert(
            "todos",
            row_input!("title" => "discard me", "completed" => false),
        )
        .expect("insert in transaction");
    assert_eq!(
        all_todos(tx.client()).await,
        vec![(todo_id, inserted_values)]
    );

    client
        .rollback_transaction(batch_id)
        .expect("roll back transaction by id");
    assert!(
        all_todos(&client).await.is_empty(),
        "rolled back transaction should not make staged rows visible"
    );
    assert!(
        client.commit_transaction(batch_id).is_err(),
        "rolled back transaction should reject commit"
    );
}

#[tokio::test]
async fn committed_transaction_rejects_later_handle_operations() {
    let client = JazzClient::test_client(todo_schema()).await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let batch_id = tx.batch_id();

    let (todo_id, _, _) = tx
        .insert(
            "todos",
            row_input!("title" => "committed", "completed" => false),
        )
        .expect("insert in transaction");
    assert_eq!(tx.commit().expect("commit transaction"), batch_id);

    let closed_handle = client.with_write_context(WriteContext::default().with_batch_id(batch_id));

    let operation_errors = [
        (
            "commit",
            client
                .commit_transaction(batch_id)
                .expect_err("committed transaction should reject a second commit")
                .to_string(),
        ),
        (
            "rollback",
            client
                .rollback_transaction(batch_id)
                .expect_err("committed transaction should reject rollback")
                .to_string(),
        ),
        (
            "insert",
            closed_handle
                .insert(
                    "todos",
                    row_input!("title" => "too late", "completed" => false),
                )
                .expect_err("committed transaction handle should reject inserts")
                .to_string(),
        ),
        (
            "update",
            closed_handle
                .update(
                    todo_id,
                    vec![("title".to_string(), Value::Text("too late".to_string()))],
                )
                .expect_err("committed transaction handle should reject updates")
                .to_string(),
        ),
        (
            "delete",
            closed_handle
                .delete(todo_id)
                .expect_err("committed transaction handle should reject deletes")
                .to_string(),
        ),
        (
            "query",
            closed_handle
                .query(todo_query(), None)
                .await
                .expect_err("committed transaction handle should reject queries")
                .to_string(),
        ),
    ];

    for (operation, error) in operation_errors {
        assert!(
            error.contains("transaction")
                && error.contains(&batch_id.to_string())
                && error.contains("already committed"),
            "unexpected {operation} error: {error}"
        );
    }
}

#[tokio::test]
async fn rolled_back_transaction_rejects_later_handle_operations() {
    let client = JazzClient::test_client(todo_schema()).await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let batch_id = tx.batch_id();

    let (todo_id, _, _) = tx
        .insert(
            "todos",
            row_input!("title" => "rolled back", "completed" => false),
        )
        .expect("insert in transaction");
    tx.rollback().expect("roll back transaction through handle");

    let closed_handle = client.with_write_context(WriteContext::default().with_batch_id(batch_id));

    let operation_errors = [
        (
            "commit",
            client
                .commit_transaction(batch_id)
                .expect_err("rolled-back transaction should reject commit")
                .to_string(),
        ),
        (
            "rollback",
            client
                .rollback_transaction(batch_id)
                .expect_err("rolled-back transaction should reject rollback")
                .to_string(),
        ),
        (
            "insert",
            closed_handle
                .insert(
                    "todos",
                    row_input!("title" => "too late", "completed" => false),
                )
                .expect_err("rolled-back transaction handle should reject inserts")
                .to_string(),
        ),
        (
            "update",
            closed_handle
                .update(
                    todo_id,
                    vec![("title".to_string(), Value::Text("too late".to_string()))],
                )
                .expect_err("rolled-back transaction handle should reject updates")
                .to_string(),
        ),
        (
            "delete",
            closed_handle
                .delete(todo_id)
                .expect_err("rolled-back transaction handle should reject deletes")
                .to_string(),
        ),
        (
            "query",
            closed_handle
                .query(todo_query(), None)
                .await
                .expect_err("rolled-back transaction handle should reject queries")
                .to_string(),
        ),
    ];

    for (operation, error) in operation_errors {
        assert!(
            error.contains(&batch_id.to_string())
                && error.contains("completed or was never opened"),
            "unexpected {operation} error: {error}"
        );
    }
}

/// Alice stages one transactional row locally.
/// Authority receives the staged row but keeps it non-visible.
/// Alice seals the batch.
/// Authority accepts it and replays the settlement back.
#[tokio::test]
async fn transaction_insert_is_visible_only_after_commit_settles() {
    let (server, alice, bob) = start_two_clients(todo_schema()).await;
    let tx = alice
        .begin_transaction()
        .expect("begin transaction through client API");
    let batch_id = tx.batch_id();
    let (todo_id, expected_values, write_batch_id) = tx
        .insert(
            "todos",
            row_input!("title" => "sealed later", "completed" => false),
        )
        .expect("insert in transaction");
    assert_eq!(write_batch_id, batch_id);

    assert!(
        all_todos(&alice).await.is_empty(),
        "ordinary local reads should ignore an open transaction"
    );
    assert!(
        bob.query(todo_query(), Some(DurabilityTier::EdgeServer))
            .await
            .expect("bob edge query before commit")
            .is_empty(),
        "peer edge reads should not see an uncommitted transaction"
    );

    {
        let wait_for_batch = alice.wait_for_batch(batch_id, DurabilityTier::EdgeServer);
        tokio::pin!(wait_for_batch);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut wait_for_batch)
                .await
                .is_err(),
            "transaction wait should stay pending before commit"
        );
    }

    assert_eq!(tx.commit().expect("commit transaction"), batch_id);
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("committed transaction settles");

    let rows = wait_for_todos(
        &bob,
        Some(DurabilityTier::EdgeServer),
        "bob sees committed transaction",
        |rows| {
            rows.iter()
                .any(|(id, values)| *id == todo_id && values == &expected_values)
        },
    )
    .await;
    assert!(
        rows.iter()
            .any(|(id, values)| *id == todo_id && values == &expected_values)
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Client local runtime inserts one staged transactional row.
/// The transaction updates that same row again before sealing.
/// The latest accepted row should reflect the update.
#[tokio::test]
async fn transaction_update_can_modify_row_inserted_earlier_in_same_transaction() {
    let schema = todo_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let client = connect_user(&server, schema, "transaction-update-inserted-row").await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let batch_id = tx.batch_id();
    let (todo_id, _, insert_batch_id) = tx
        .insert(
            "todos",
            row_input!("title" => "draft", "completed" => false),
        )
        .expect("insert in transaction");
    assert_eq!(insert_batch_id, batch_id);
    assert_eq!(
        tx.update(
            todo_id,
            vec![("title".to_string(), Value::Text("final".to_string()))],
        )
        .expect("update inserted row in transaction"),
        batch_id
    );

    assert!(
        all_todos(&client).await.is_empty(),
        "ordinary reads should ignore the open transaction"
    );
    assert_eq!(tx.commit().expect("commit transaction"), batch_id);

    let rows = wait_for_todos(
        &client,
        Some(DurabilityTier::EdgeServer),
        "client sees updated insert from transaction",
        |rows| has_todo(rows, todo_id, "final", false),
    )
    .await;
    assert!(has_todo(&rows, todo_id, "final", false));
    assert!(!has_todo(&rows, todo_id, "draft", false));

    client.shutdown().await.expect("shutdown client");
    server.shutdown().await;
}

/// Todo row visible on main.
/// Transaction update #1 changes title.
/// Transaction update #2 changes completed.
/// Latest staged member should compose both changes.
/// Only one accepted row should remain for that row/batch.
#[tokio::test]
async fn multiple_updates_to_same_row_in_transaction_compose() {
    let schema = todo_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let client = connect_user(&server, schema, "multiple-updates-compose").await;
    let todo_id = insert_visible_todo(&client, "draft", false).await;

    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let batch_id = tx.batch_id();
    assert_eq!(
        tx.update(
            todo_id,
            vec![("title".to_string(), Value::Text("renamed".to_string()))],
        )
        .expect("first transaction update"),
        batch_id
    );
    assert_eq!(
        tx.update(
            todo_id,
            vec![("completed".to_string(), Value::Boolean(true))]
        )
        .expect("second transaction update"),
        batch_id
    );
    let tx_rows = tx
        .client()
        .query(todo_query(), None)
        .await
        .expect("transaction-scoped query");
    assert!(has_todo(&tx_rows, todo_id, "renamed", true));

    assert_eq!(tx.commit().expect("commit transaction"), batch_id);

    let rows = wait_for_todos(
        &client,
        Some(DurabilityTier::EdgeServer),
        "client sees composed transaction update",
        |rows| has_todo(rows, todo_id, "renamed", true),
    )
    .await;
    assert!(has_todo(&rows, todo_id, "renamed", true));

    client.shutdown().await.expect("shutdown client");
    server.shutdown().await;
}

/// Client stages two transactional writes under one logical batch.
/// Client seals that shared batch once.
/// Authority accepts both rows into one replayable accepted settlement.
/// Client observes both rows after that shared batch fate.
#[tokio::test]
async fn multiple_writes_in_one_transaction_settle_as_one_batch() {
    let schema = todo_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let client = connect_user(&server, schema, "multiple-writes-one-transaction").await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let batch_id = tx.batch_id();

    let (first_id, first_values, first_batch_id) = tx
        .insert(
            "todos",
            row_input!("title" => "first", "completed" => false),
        )
        .expect("insert first row in transaction");
    let (second_id, second_values, second_batch_id) = tx
        .insert(
            "todos",
            row_input!("title" => "second", "completed" => true),
        )
        .expect("insert second row in transaction");
    assert_eq!(first_batch_id, batch_id);
    assert_eq!(second_batch_id, batch_id);

    assert_eq!(tx.commit().expect("commit transaction"), batch_id);

    let rows = wait_for_todos(
        &client,
        Some(DurabilityTier::EdgeServer),
        "client sees both rows from one transaction",
        |rows| {
            rows.iter()
                .any(|(id, values)| *id == first_id && values == &first_values)
                && rows
                    .iter()
                    .any(|(id, values)| *id == second_id && values == &second_values)
        },
    )
    .await;
    assert!(
        rows.iter()
            .any(|(id, values)| *id == first_id && values == &first_values)
    );
    assert!(
        rows.iter()
            .any(|(id, values)| *id == second_id && values == &second_values)
    );

    client.shutdown().await.expect("shutdown client");
    server.shutdown().await;
}

/// Two transactions modify the same object unaware of each other.
/// The server accepts the first tx and rejects the second.
#[tokio::test]
async fn stale_concurrent_transaction_is_rejected() {
    let (server, alice, bob) = start_two_clients(todo_schema()).await;
    let todo_id = insert_visible_todo(&alice, "shared", false).await;
    wait_for_todos(
        &bob,
        Some(DurabilityTier::EdgeServer),
        "bob sees shared row",
        |rows| has_todo(rows, todo_id, "shared", false),
    )
    .await;

    let alice_tx = alice.begin_transaction().expect("begin alice transaction");
    let bob_tx = bob.begin_transaction().expect("begin bob transaction");
    let alice_batch_id = alice_tx
        .update(
            todo_id,
            vec![("title".to_string(), Value::Text("alice".to_string()))],
        )
        .expect("alice stages update");
    let bob_batch_id = bob_tx
        .update(
            todo_id,
            vec![("title".to_string(), Value::Text("bob".to_string()))],
        )
        .expect("bob stages stale update");

    let blocked_bob = server.block_messages_to(bob.client_id().expect("bob client id"));
    assert_eq!(
        alice_tx.commit().expect("commit alice transaction"),
        alice_batch_id
    );
    alice
        .wait_for_batch(alice_batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice transaction accepted");

    assert_eq!(
        bob_tx.commit().expect("commit bob transaction"),
        bob_batch_id
    );
    blocked_bob
        .wait_until_buffered(
            |payload| {
                matches!(
                    payload,
                    SyncPayload::BatchFate { fate }
                        if fate.batch_id() == bob_batch_id
                )
            },
            Duration::from_secs(5),
        )
        .await
        .expect("server should reject bob's stale transaction while bob is blocked");
    blocked_bob.unblock();

    let rejection = bob
        .wait_for_batch(bob_batch_id, DurabilityTier::EdgeServer)
        .await
        .expect_err("bob stale transaction should be rejected")
        .to_string();
    assert!(
        rejection.contains("transaction_conflict"),
        "unexpected rejection: {rejection}"
    );

    let rows = wait_for_todos(
        &bob,
        Some(DurabilityTier::EdgeServer),
        "bob sees alice value after stale transaction rejection",
        |rows| has_todo(rows, todo_id, "alice", false),
    )
    .await;
    assert!(has_todo(&rows, todo_id, "alice", false));

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Two transactions modify the same object.
/// Alice's tx commits first and Bob's tx sees Alice's update
/// AFTER its write (and before its commit).
/// The server accepts the first tx and rejects the second.
#[tokio::test]
async fn transaction_staged_before_receiving_concurrent_commit_is_rejected() {
    let (server, alice, bob) = start_two_clients(todo_schema()).await;
    let todo_id = insert_visible_todo(&alice, "shared", false).await;
    wait_for_todos(
        &bob,
        Some(DurabilityTier::EdgeServer),
        "bob sees shared row",
        |rows| has_todo(rows, todo_id, "shared", false),
    )
    .await;

    let alice_tx = alice.begin_transaction().expect("begin alice transaction");
    let bob_tx = bob.begin_transaction().expect("begin bob transaction");
    let alice_batch_id = alice_tx
        .update(
            todo_id,
            vec![("title".to_string(), Value::Text("alice".to_string()))],
        )
        .expect("alice stages update");
    let bob_batch_id = bob_tx
        .update(
            todo_id,
            vec![("title".to_string(), Value::Text("bob".to_string()))],
        )
        .expect("bob stages stale update");

    assert_eq!(
        alice_tx.commit().expect("commit alice transaction"),
        alice_batch_id
    );
    alice
        .wait_for_batch(alice_batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice transaction accepted");
    wait_for_todos(
        &bob,
        Some(DurabilityTier::EdgeServer),
        "bob learns alice transaction before committing his staged transaction",
        |rows| has_todo(rows, todo_id, "alice", false),
    )
    .await;

    assert_eq!(
        bob_tx.commit().expect("commit bob transaction"),
        bob_batch_id
    );
    let rejection = bob
        .wait_for_batch(bob_batch_id, DurabilityTier::EdgeServer)
        .await
        .expect_err("bob transaction staged from stale base should be rejected")
        .to_string();
    assert!(
        rejection.contains("transaction_conflict"),
        "unexpected rejection: {rejection}"
    );

    let rows = wait_for_todos(
        &bob,
        Some(DurabilityTier::EdgeServer),
        "bob still sees alice value after rejection",
        |rows| has_todo(rows, todo_id, "alice", false),
    )
    .await;
    assert!(has_todo(&rows, todo_id, "alice", false));

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Two transactions modify the same object.
/// Alice's tx commits first and Bob's tx sees Alice's update BEFORE its write.
/// The server accepts both transactions.
#[tokio::test]
async fn transaction_staged_after_receiving_concurrent_commit_is_accepted() {
    let (server, alice, bob) = start_two_clients(todo_schema()).await;
    let todo_id = insert_visible_todo(&alice, "shared", false).await;
    wait_for_todos(
        &bob,
        Some(DurabilityTier::EdgeServer),
        "bob sees shared row",
        |rows| has_todo(rows, todo_id, "shared", false),
    )
    .await;

    let alice_tx = alice.begin_transaction().expect("begin alice transaction");
    let alice_batch_id = alice_tx
        .update(
            todo_id,
            vec![("title".to_string(), Value::Text("alice".to_string()))],
        )
        .expect("alice stages update");
    assert_eq!(
        alice_tx.commit().expect("commit alice transaction"),
        alice_batch_id
    );
    alice
        .wait_for_batch(alice_batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice transaction accepted");
    wait_for_todos(
        &bob,
        Some(DurabilityTier::EdgeServer),
        "bob learns alice transaction before staging",
        |rows| has_todo(rows, todo_id, "alice", false),
    )
    .await;

    let bob_tx = bob.begin_transaction().expect("begin bob transaction");
    let bob_batch_id = bob_tx
        .update(
            todo_id,
            vec![("title".to_string(), Value::Text("bob".to_string()))],
        )
        .expect("bob stages update from latest visible row");
    assert_eq!(
        bob_tx.commit().expect("commit bob transaction"),
        bob_batch_id
    );
    bob.wait_for_batch(bob_batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("bob transaction based on latest row should be accepted");

    let rows = wait_for_todos(
        &alice,
        Some(DurabilityTier::EdgeServer),
        "alice sees bob transaction after acceptance",
        |rows| has_todo(rows, todo_id, "bob", false),
    )
    .await;
    assert!(has_todo(&rows, todo_id, "bob", false));

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

#[tokio::test]
async fn wait_for_batch_errors_for_unattainable_durability_tier() {
    let client = JazzClient::test_client(todo_schema()).await;
    let (_, _, batch_id) = client
        .insert(
            "todos",
            row_input!("title" => "local only", "completed" => false),
        )
        .expect("insert todo");

    assert!(
        client
            .wait_for_batch(batch_id, DurabilityTier::GlobalServer)
            .await
            .is_err(),
        "serverless test client cannot reach GlobalServer durability"
    );
    client
        .wait_for_batch(batch_id, DurabilityTier::Local)
        .await
        .expect("local durability should be reachable");
}
