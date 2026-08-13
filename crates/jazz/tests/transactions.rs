#![cfg(feature = "test-utils")]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

mod support;

use jazz::row_input;
use jazz::tools::server::JazzServer;
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder,
    TableSchema, Value, WriteContext,
};
use support::wait_for_query;

static TEST_USER_COUNTER: AtomicU64 = AtomicU64::new(1);

fn todo_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

fn todo_query() -> jazz::tools::Query {
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

async fn connect_user(server: &JazzServer, schema: Schema, user_id: &str) -> JazzClient {
    let client = JazzClient::connect(server.make_client_context_for_user(schema, user_id))
        .await
        .expect("connect user");
    wait_for_edge_ready(&client).await;
    client
}

fn unique_user_id(prefix: &str) -> String {
    let id = TEST_USER_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{id}")
}

async fn start_two_clients(schema: Schema) -> (JazzServer, JazzClient, JazzClient) {
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let alice_id = unique_user_id("alice-transactions");
    let bob_id = unique_user_id("bob-transactions");
    let alice = connect_user(&server, schema.clone(), &alice_id).await;
    let bob = connect_user(&server, schema, &bob_id).await;
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
        .wait_for_batch(
            batch_id.expect("ordinary mutation commits immediately"),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("visible todo settles at edge");
    todo_id
}

macro_rules! local_tokio_test {
    ($(#[$attr:meta])* async fn $name:ident() $body:block) => {
        $(#[$attr])*
        #[tokio::test(flavor = "current_thread")]
        async fn $name() {
            tokio::task::LocalSet::new()
                .run_until(async $body)
                .await;
        }
    };
}

local_tokio_test! {
async fn transaction_stages_writes_and_can_commit() {
    let client = JazzClient::test_client(todo_schema()).await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let batch_id = tx.open_batch_id();

    let (todo_id, inserted_values, write_batch_id) = tx
        .insert(
            "todos",
            row_input!("title" => "ship transactions", "completed" => false),
        )
        .expect("insert in transaction");

    assert_eq!(write_batch_id, None);
    assert!(
        all_todos(&client).await.is_empty(),
        "ordinary client reads should ignore an open transaction"
    );
    assert_eq!(
        all_todos(tx.client()).await,
        vec![(todo_id, inserted_values)],
        "transaction-scoped reads should include staged rows"
    );

    tx.commit().expect("commit transaction");
    assert!(
        client.commit_transaction(batch_id).is_err(),
        "committed transaction should reject a second commit"
    );
}
}

local_tokio_test! {
async fn transaction_can_be_rolled_back() {
    let client = JazzClient::test_client(todo_schema()).await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let batch_id = tx.open_batch_id();

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
}

local_tokio_test! {
async fn committed_transaction_rejects_later_handle_operations() {
    let client = JazzClient::test_client(todo_schema()).await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let batch_id = tx.open_batch_id();

    let (todo_id, _, _) = tx
        .insert(
            "todos",
            row_input!("title" => "committed", "completed" => false),
        )
        .expect("insert in transaction");
    tx.commit().expect("commit transaction");

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
}

local_tokio_test! {
async fn rolled_back_transaction_rejects_later_handle_operations() {
    let client = JazzClient::test_client(todo_schema()).await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let batch_id = tx.open_batch_id();

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
}

// Alice stages one transactional row locally.
// Authority receives the staged row but keeps it non-visible.
// Alice seals the batch.
// Authority accepts it and replays the settlement back.
local_tokio_test! {
async fn transaction_insert_is_visible_only_after_commit_settles() {
    let (server, alice, bob) = start_two_clients(todo_schema()).await;
    let tx = alice
        .begin_transaction()
        .expect("begin transaction through client API");
    let (todo_id, expected_values, write_batch_id) = tx
        .insert(
            "todos",
            row_input!("title" => "sealed later", "completed" => false),
        )
        .expect("insert in transaction");
    assert_eq!(write_batch_id, None);

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

    let committed_batch_id = tx.commit().expect("commit transaction");
    alice
        .wait_for_batch(committed_batch_id, DurabilityTier::EdgeServer)
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
}

// Client inserts one staged transactional row.
// The transaction updates that same row again before sealing.
// The latest accepted row should reflect the update.
local_tokio_test! {
async fn transaction_update_can_modify_row_inserted_earlier_in_same_transaction() {
    let schema = todo_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let user_id = unique_user_id("transaction-update-inserted-row");
    let client = connect_user(&server, schema, &user_id).await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let (todo_id, _, insert_batch_id) = tx
        .insert(
            "todos",
            row_input!("title" => "draft", "completed" => false),
        )
        .expect("insert in transaction");
    assert_eq!(insert_batch_id, None);
    assert_eq!(
        tx.update(
            todo_id,
            vec![("title".to_string(), Value::Text("final".to_string()))],
        )
        .expect("update inserted row in transaction"),
        None
    );

    assert!(
        all_todos(&client).await.is_empty(),
        "ordinary reads should ignore the open transaction"
    );
    tx.commit().expect("commit transaction");

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
}

// Todo row visible on main.
// Transaction update #1 changes title.
// Transaction update #2 changes completed.
// Latest staged member should compose both changes.
// Only one accepted row should remain for that row/batch.
local_tokio_test! {
async fn multiple_updates_to_same_row_in_transaction_compose() {
    let schema = todo_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let user_id = unique_user_id("multiple-updates-compose");
    let client = connect_user(&server, schema, &user_id).await;
    let todo_id = insert_visible_todo(&client, "draft", false).await;

    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    assert_eq!(
        tx.update(
            todo_id,
            vec![("title".to_string(), Value::Text("renamed".to_string()))],
        )
        .expect("first transaction update"),
        None
    );
    assert_eq!(
        tx.update(
            todo_id,
            vec![("completed".to_string(), Value::Boolean(true))]
        )
        .expect("second transaction update"),
        None
    );
    let tx_rows = tx
        .client()
        .query(todo_query(), None)
        .await
        .expect("transaction-scoped query");
    assert!(has_todo(&tx_rows, todo_id, "renamed", true));

    tx.commit().expect("commit transaction");

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
}

// Client stages two transactional writes under one logical batch.
// Client seals that shared batch once.
// Authority accepts both rows into one replayable accepted settlement.
// Client observes both rows after that shared batch fate.
local_tokio_test! {
async fn multiple_writes_in_one_transaction_settle_as_one_batch() {
    let schema = todo_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let user_id = unique_user_id("multiple-writes-one-transaction");
    let client = connect_user(&server, schema, &user_id).await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
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
    assert_eq!(first_batch_id, None);
    assert_eq!(second_batch_id, None);

    tx.commit().expect("commit transaction");

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
}

// Two transactions modify the same object.
// Alice's tx commits first and Bob's tx sees Alice's update
// AFTER its write (and before its commit).
// The server accepts the first tx and rejects the second.
local_tokio_test! {
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
    let alice_staged = alice_tx
        .update(
            todo_id,
            vec![("title".to_string(), Value::Text("alice".to_string()))],
        )
        .expect("alice stages update");
    let bob_staged = bob_tx
        .update(
            todo_id,
            vec![("title".to_string(), Value::Text("bob".to_string()))],
        )
        .expect("bob stages stale update");

    assert!(alice_staged.is_none(), "transaction update remains staged");
    assert!(bob_staged.is_none(), "transaction update remains staged");
    let alice_batch_id = alice_tx.commit().expect("commit alice transaction");
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

    let rejection = bob_tx
        .commit()
        .expect_err("bob transaction staged from stale base should be rejected locally")
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
}

// Two transactions modify the same object.
// Alice's tx commits first and Bob's tx sees Alice's update BEFORE its write.
// The server accepts both transactions.
local_tokio_test! {
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
    let alice_staged = alice_tx
        .update(
            todo_id,
            vec![("title".to_string(), Value::Text("alice".to_string()))],
        )
        .expect("alice stages update");
    assert!(alice_staged.is_none(), "transaction update remains staged");
    let alice_batch_id = alice_tx.commit().expect("commit alice transaction");
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
    let bob_staged = bob_tx
        .update(
            todo_id,
            vec![("title".to_string(), Value::Text("bob".to_string()))],
        )
        .expect("bob stages update from latest visible row");
    assert!(bob_staged.is_none(), "transaction update remains staged");
    let bob_batch_id = bob_tx.commit().expect("commit bob transaction");
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
}

local_tokio_test! {
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
            .wait_for_batch(batch_id.expect("ordinary mutation commits immediately"), DurabilityTier::GlobalServer)
            .await
            .is_err(),
        "serverless test client cannot reach GlobalServer durability"
    );
    client
        .wait_for_batch(batch_id.expect("ordinary mutation commits immediately"), DurabilityTier::Local)
        .await
        .expect("local durability should be reachable");
}
}

// Regression guard for websocket transport batching: a local-first import whose
// encoded wire frames exceed the server's 1 MiB WebSocket-message cap must be
// split before a later batch can reach global durability.
local_tokio_test! {
    #[ignore = "known red; tracked in TEST_BURNDOWN.md"]
async fn global_wait_after_over_one_mib_websocket_import_settles() {
    let schema = todo_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let client = connect_user(&server, schema, &unique_user_id("bulk-global-wait")).await;

    let (target_id, _, _) = client
        .insert(
            "todos",
            row_input!("title" => "durability target", "completed" => false),
        )
        .expect("insert target row");

    // Keep the logical payload above 1 MiB while avoiding a throughput-shaped
    // test with thousands of independently committed rows.
    let import_payload = "x".repeat(1_600);
    for index in 0..768 {
        client
            .insert(
                "todos",
                row_input!("title" => format!("imported row {index}: {import_payload}"), "completed" => false),
            )
            .expect("queue import row");
    }

    let target_batch = client
        .update(target_id, vec![("completed".to_owned(), Value::Boolean(true))])
        .expect("update target row after import");

    client
        .wait_for_batch(target_batch.expect("ordinary mutation commits immediately"), DurabilityTier::Local)
        .await
        .expect("target update should settle locally without draining the import backlog");

    tokio::time::timeout(
        Duration::from_secs(30),
        client.wait_for_batch(target_batch.expect("ordinary mutation commits immediately"), DurabilityTier::GlobalServer),
    )
    .await
    .expect("global wait should settle after import backlog")
    .expect("target update should reach global durability");

    client.shutdown().await.expect("shutdown client");
    server.shutdown().await;
}
}
