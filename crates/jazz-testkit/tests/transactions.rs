use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use jazz_testkit as support;

use jazz::protocol_limits::MAX_WIRE_FRAME_BYTES;
use jazz::row_input;
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, Schema, SchemaBuilder, TableSchema, Value,
    WriteContext,
};
use jazz_server::JazzServer;
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

fn todo_query() -> jazz::query::Query {
    jazz::query::Query::from("todos").select(["title", "completed"])
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
    let client = jazz_testkit::connect(server.make_client_context_for_user(schema, user_id))
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
    let (todo_id, _, transaction_id) = client
        .insert(
            "todos",
            row_input!("title" => title, "completed" => completed),
        )
        .expect("insert visible todo");
    support::wait_for_edge_txs(
        client,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;
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
    let transaction_id = tx.transaction_id();

    let (todo_id, inserted_values, write_tx_id) = tx
        .insert(
            "todos",
            row_input!("title" => "ship transactions", "completed" => false),
        )
        .expect("insert in transaction");

    assert_eq!(write_tx_id, None);
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
        client.commit_transaction(transaction_id).is_err(),
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
    let transaction_id = tx.transaction_id();

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
        .rollback_transaction(transaction_id)
        .expect("roll back transaction by id");
    assert!(
        all_todos(&client).await.is_empty(),
        "rolled back transaction should not make staged rows visible"
    );
    assert!(
        client.commit_transaction(transaction_id).is_err(),
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
    let transaction_id = tx.transaction_id();

    let (todo_id, _, _) = tx
        .insert(
            "todos",
            row_input!("title" => "committed", "completed" => false),
        )
        .expect("insert in transaction");
    tx.commit().expect("commit transaction");

    let closed_handle = client.with_write_context(WriteContext::default().with_transaction_id(transaction_id));

    let operation_errors = [
        (
            "commit",
            client
                .commit_transaction(transaction_id)
                .expect_err("committed transaction should reject a second commit")
                .to_string(),
        ),
        (
            "rollback",
            client
                .rollback_transaction(transaction_id)
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
                && error.contains(&transaction_id.to_string())
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
    let transaction_id = tx.transaction_id();

    let (todo_id, _, _) = tx
        .insert(
            "todos",
            row_input!("title" => "rolled back", "completed" => false),
        )
        .expect("insert in transaction");
    tx.rollback().expect("roll back transaction through handle");

    let closed_handle = client.with_write_context(WriteContext::default().with_transaction_id(transaction_id));

    let operation_errors = [
        (
            "commit",
            client
                .commit_transaction(transaction_id)
                .expect_err("rolled-back transaction should reject commit")
                .to_string(),
        ),
        (
            "rollback",
            client
                .rollback_transaction(transaction_id)
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
            error.contains(&transaction_id.to_string())
                && error.contains("completed or was never opened"),
            "unexpected {operation} error: {error}"
        );
    }
}
}

// Alice stages one transactional row locally.
// Authority receives the staged row but keeps it non-visible.
// Alice seals the tx.
// Authority accepts it and replays the settlement back.
local_tokio_test! {
async fn transaction_insert_is_visible_only_after_commit_settles() {
    let (server, alice, bob) = start_two_clients(todo_schema()).await;
    let tx = alice
        .begin_transaction()
        .expect("begin transaction through client API");
    let (todo_id, expected_values, write_tx_id) = tx
        .insert(
            "todos",
            row_input!("title" => "sealed later", "completed" => false),
        )
        .expect("insert in transaction");
    assert_eq!(write_tx_id, None);

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

    let committed_tx_id = tx.commit().expect("commit transaction");
    support::wait_for_edge_txs(&alice, &[committed_tx_id]).await;

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
    let (todo_id, _, insert_tx_id) = tx
        .insert(
            "todos",
            row_input!("title" => "draft", "completed" => false),
        )
        .expect("insert in transaction");
    assert_eq!(insert_tx_id, None);
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
// Only one accepted row should remain for that row/tx.
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

// Client stages two transactional writes under one logical tx.
// Client seals that shared tx once.
// Authority accepts both rows into one replayable accepted settlement.
// Client observes both rows after that shared tx fate.
local_tokio_test! {
async fn multiple_writes_in_one_transaction_settle_atomically() {
    let schema = todo_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let user_id = unique_user_id("multiple-writes-one-transaction");
    let client = connect_user(&server, schema, &user_id).await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let (first_id, first_values, first_tx_id) = tx
        .insert(
            "todos",
            row_input!("title" => "first", "completed" => false),
        )
        .expect("insert first row in transaction");
    let (second_id, second_values, second_tx_id) = tx
        .insert(
            "todos",
            row_input!("title" => "second", "completed" => true),
        )
        .expect("insert second row in transaction");
    assert_eq!(first_tx_id, None);
    assert_eq!(second_tx_id, None);

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
    let alice_tx_id = alice_tx.commit().expect("commit alice transaction");
    support::wait_for_edge_txs(&alice, &[alice_tx_id]).await;
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
    let alice_tx_id = alice_tx.commit().expect("commit alice transaction");
    support::wait_for_edge_txs(&alice, &[alice_tx_id]).await;
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
    let bob_tx_id = bob_tx.commit().expect("commit bob transaction");
    support::wait_for_edge_txs(&bob, &[bob_tx_id]).await;

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
async fn wait_for_transaction_errors_for_unattainable_durability_tier() {
    let client = JazzClient::test_client(todo_schema()).await;
    let (_, _, transaction_id) = client
        .insert(
            "todos",
            row_input!("title" => "local only", "completed" => false),
        )
        .expect("insert todo");

    assert!(
        client
            .wait_for_transaction_with_timeout_for_test(
                transaction_id.expect("ordinary mutation commits immediately"),
                DurabilityTier::GlobalServer,
                Duration::from_millis(100),
            )
            .await
            .is_err(),
        "serverless test client cannot reach GlobalServer durability"
    );
    client
        .wait_for_transaction(transaction_id.expect("ordinary mutation commits immediately"), DurabilityTier::Local)
        .await
        .expect("local durability should be reachable");
}
}

// Regression guard for logical-message fragmentation: one incompressible import
// is confirmed to remain larger than three physical wire frames after the same
// zstd level used by the native transport, so a later tx must still reach
// global durability after the WebSocket transports the fragments.
local_tokio_test! {
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

    // Printable high-entropy text is legal in the public Text column and
    // avoids relying on raw source size when zstd is negotiated.
    let mut entropy = 1_u64;
    let import_payload = (0..(5 * MAX_WIRE_FRAME_BYTES))
        .map(|_| {
            entropy = entropy
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            char::from(b'!' + ((entropy >> 57) as u8 % 94))
        })
        .collect::<String>();
    let compressed = zstd::bulk::compress(import_payload.as_bytes(), 3)
        .expect("compress deterministic import payload");
    assert!(
        compressed.len() > 3 * MAX_WIRE_FRAME_BYTES,
        "native zstd transport payload must require more than three physical frames"
    );
    client
        .insert(
            "todos",
            row_input!("title" => import_payload, "completed" => false),
        )
        .expect("queue one logical import message");

    let target_tx = client
        .update(target_id, vec![("completed".to_owned(), Value::Boolean(true))])
        .expect("update target row after import");

    client
        .wait_for_transaction(target_tx.expect("ordinary mutation commits immediately"), DurabilityTier::Local)
        .await
        .expect("target update should settle locally without draining the import backlog");

    tokio::time::timeout(
        Duration::from_secs(30),
        client.wait_for_transaction(target_tx.expect("ordinary mutation commits immediately"), DurabilityTier::GlobalServer),
    )
    .await
    .expect("global wait should settle after import backlog")
    .expect("target update should reach global durability");

    client.shutdown().await.expect("shutdown client");
    server.shutdown().await;
}
}
