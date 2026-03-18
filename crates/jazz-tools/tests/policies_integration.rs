#![cfg(feature = "test-utils")]

mod support;

use std::time::Duration;

use jazz_tools::query_manager::policy::PolicyExpr;
use jazz_tools::query_manager::types::TablePolicies;
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder,
    TableSchema, Value,
};
use support::{
    collect_stream_deltas, connect_jwt_client, has_added, has_any_change, has_removed, has_updated,
    wait_for_rows, wait_for_subscription_update,
};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
const NO_DELTA_WINDOW: Duration = Duration::from_millis(100);

fn select_policy_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
                        .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
                ),
        )
        .build()
}

fn write_policy_schema() -> Schema {
    let owner_policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);

    SchemaBuilder::new()
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_insert(owner_policy.clone())
                        .with_update(Some(owner_policy.clone()), PolicyExpr::True)
                        .with_delete(owner_policy),
                ),
        )
        .build()
}

fn document_values(owner_id: &str, title: &str) -> Vec<Value> {
    vec![
        Value::Text(owner_id.to_string()),
        Value::Text(title.to_string()),
    ]
}

async fn create_document(client: &JazzClient, owner_id: &str, title: &str) -> ObjectId {
    client
        .create("documents", document_values(owner_id, title))
        .await
        .expect("create document")
        .0
}

/// Verifies that `SELECT` policies scope subscription updates and query results
/// to the requesting client's own session, preventing cross-user data leakage.
///
/// Alice and bob each insert a document they own. The schema enforces
/// `owner_id = session.user_id` on SELECT, so each client's subscription must
/// only fire for their own row. Query results are checked independently too.
///
/// ```text
/// alice ──insert "Alice Only"──► server ──► alice stream (add ✓)
///                                    │
///                                    └── SELECT policy ──✗──► bob stream (silent)
///
/// bob ──insert "Bob Only"──► server ──► bob stream (add ✓)
///                                │
///                                └── SELECT policy ──✗──► alice stream (silent)
/// ```
#[tokio::test]
async fn select_policies_filter_subscription_results_per_client_session() {
    let schema = select_policy_schema();
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let alice =
        connect_jwt_client(&server, schema.clone(), "alice", "documents", READY_TIMEOUT).await;
    let bob = connect_jwt_client(&server, schema, "bob", "documents", READY_TIMEOUT).await;
    let query = QueryBuilder::new("documents").build();

    let mut alice_stream = alice
        .subscribe(query.clone())
        .await
        .expect("subscribe alice");
    let mut bob_stream = bob.subscribe(query.clone()).await.expect("subscribe bob");
    let mut alice_log = Vec::new();
    let mut bob_log = Vec::new();

    let alice_doc = create_document(&alice, "alice", "Alice Only").await;
    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        QUERY_TIMEOUT,
        "alice add delta",
        |log| has_added(log, alice_doc),
    )
    .await;
    // alice's stream delta fires only after the server has dispatched (or
    // withheld) notifications to all subscribers. A short drain is enough to
    // flush any buffered messages on the loopback connection.
    collect_stream_deltas(&mut bob_stream, &mut bob_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_any_change(&bob_log, alice_doc),
        "bob should not receive alice's document"
    );

    let bob_doc = create_document(&bob, "bob", "Bob Only").await;
    wait_for_subscription_update(
        &mut bob_stream,
        &mut bob_log,
        QUERY_TIMEOUT,
        "bob add delta",
        |log| has_added(log, bob_doc),
    )
    .await;
    // Symmetric check: bob's stream delta is the barrier, then drain alice.
    collect_stream_deltas(&mut alice_stream, &mut alice_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_any_change(&alice_log, bob_doc),
        "alice should not receive bob's document"
    );

    let alice_rows = wait_for_rows(&alice, query.clone(), "alice visible rows", |rows| {
        (rows.len() == 1 && rows[0].0 == alice_doc).then_some(rows)
    })
    .await;
    assert_eq!(alice_rows[0].1, document_values("alice", "Alice Only"));

    let bob_rows = wait_for_rows(&bob, query, "bob visible rows", |rows| {
        (rows.len() == 1 && rows[0].0 == bob_doc).then_some(rows)
    })
    .await;
    assert_eq!(bob_rows[0].1, document_values("bob", "Bob Only"));

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that the server silently drops inserts that fail the `INSERT` policy
/// and does not broadcast those rows to any subscriber.
///
/// Mallory attempts to forge a document with alice's `owner_id`. Alice then
/// creates a legitimate document of her own. Alice's insert serves as the
/// causal barrier: once the observer's stream receives alice's add delta, the
/// server has already committed alice's row — and therefore has also processed
/// and rejected mallory's earlier request.
///
/// ```text
/// mallory ──insert owner="alice"──► server ──✗ rejected, not broadcast
///
/// alice ──insert owner="alice"────► server ──► observer stream (add ✓)
///                                       │
///                                       └── observer query: only alice's row
/// ```
#[tokio::test]
async fn insert_policies_are_enforced_by_server_for_client_sync() {
    let schema = write_policy_schema();
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let intruder = connect_jwt_client(
        &server,
        schema.clone(),
        "mallory",
        "documents",
        READY_TIMEOUT,
    )
    .await;
    let observer = connect_jwt_client(
        &server,
        schema.clone(),
        "observer",
        "documents",
        READY_TIMEOUT,
    )
    .await;
    let alice = connect_jwt_client(&server, schema, "alice", "documents", READY_TIMEOUT).await;
    let query = QueryBuilder::new("documents").build();

    let mut observer_stream = observer
        .subscribe(query.clone())
        .await
        .expect("subscribe observer");
    let mut observer_log = Vec::new();

    let forged_id = intruder
        .create("documents", document_values("alice", "forged"))
        .await
        .expect("optimistic local create")
        .0;

    // Alice's insert is the causal barrier: once the observer receives this add
    // delta the server has already committed alice's row, meaning it has also
    // processed and rejected mallory's earlier insert.
    let accepted_doc = create_document(&alice, "alice", "allowed").await;
    wait_for_subscription_update(
        &mut observer_stream,
        &mut observer_log,
        QUERY_TIMEOUT,
        "observer sees allowed insert",
        |log| has_added(log, accepted_doc),
    )
    .await;

    assert!(
        !has_any_change(&observer_log, forged_id),
        "server should not broadcast a row rejected by insert policy"
    );

    let rows = wait_for_rows(
        &observer,
        query,
        "observer sees only accepted row",
        |rows| (rows.len() == 1 && rows[0].0 == accepted_doc).then_some(rows),
    )
    .await;
    assert_eq!(rows[0].1, document_values("alice", "allowed"));

    intruder.shutdown().await.expect("shutdown intruder");
    observer.shutdown().await.expect("shutdown observer");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that update and delete attempts from a client that fails the write
/// policy are rejected by the server and never broadcast to subscribers.
///
/// Alice creates a document. Bob can read it (no SELECT restriction) but does
/// not own it. Bob's update and delete are applied optimistically on his local
/// client but silently dropped by the server. An EdgeServer-tier query after
/// each attempt serves as the causal barrier: it blocks until the server has
/// settled, so if the value is still "original" the rejection is confirmed.
/// The observer's stream is then drained to verify no mutation delta arrived.
///
/// ```text
/// alice ──insert "original"──────────► server ──► observer (add ✓)
///
/// bob ──update title="hacked"────────► server ──✗ rejected (owner_id ≠ bob)
///                                          │
///                                          └── observer query (EdgeServer) → "original"
///                                          └── observer stream → no update delta
///
/// bob ──delete────────────────────────► server ──✗ rejected
///                                           │
///                                           └── observer query (EdgeServer) → row present
///                                           └── observer stream → no remove delta
/// ```
#[tokio::test]
async fn update_and_delete_policies_block_unauthorized_server_mutations() {
    let schema = write_policy_schema();
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let alice =
        connect_jwt_client(&server, schema.clone(), "alice", "documents", READY_TIMEOUT).await;
    let bob = connect_jwt_client(&server, schema.clone(), "bob", "documents", READY_TIMEOUT).await;
    let observer =
        connect_jwt_client(&server, schema, "observer", "documents", READY_TIMEOUT).await;
    let query = QueryBuilder::new("documents").build();

    let mut observer_stream = observer
        .subscribe(query.clone())
        .await
        .expect("subscribe observer");
    let mut observer_log = Vec::new();

    let doc_id = create_document(&alice, "alice", "original").await;
    wait_for_subscription_update(
        &mut observer_stream,
        &mut observer_log,
        QUERY_TIMEOUT,
        "observer sees initial row",
        |log| has_added(log, doc_id),
    )
    .await;

    wait_for_rows(&bob, query.clone(), "bob sees readable row", |rows| {
        rows.iter()
            .find(|(id, values)| *id == doc_id && *values == document_values("alice", "original"))
            .map(|_| ())
    })
    .await;

    bob.update(
        doc_id,
        vec![("title".to_string(), Value::Text("hacked".to_string()))],
    )
    .await
    .expect("optimistic local update");

    // EdgeServer query is the causal barrier: it blocks until the server has
    // settled, guaranteeing bob's attempted update has been accepted or rejected.
    let rows_after_update = observer
        .query(query.clone(), Some(DurabilityTier::EdgeServer))
        .await
        .expect("EdgeServer query after unauthorized update");
    assert!(
        rows_after_update
            .iter()
            .any(|(id, values)| *id == doc_id && *values == document_values("alice", "original")),
        "unauthorized update should not be persisted at EdgeServer: rows={rows_after_update:?}"
    );
    collect_stream_deltas(&mut observer_stream, &mut observer_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_updated(&observer_log, doc_id),
        "unauthorized update should not be broadcast to subscribers: log={observer_log:?}"
    );

    bob.delete(doc_id).await.expect("optimistic local delete");

    // Same barrier for the delete attempt.
    let rows_after_delete = observer
        .query(query.clone(), Some(DurabilityTier::EdgeServer))
        .await
        .expect("EdgeServer query after unauthorized delete");
    assert!(
        rows_after_delete
            .iter()
            .any(|(id, values)| *id == doc_id && *values == document_values("alice", "original")),
        "unauthorized delete should not be persisted at EdgeServer: rows={rows_after_delete:?}"
    );
    collect_stream_deltas(&mut observer_stream, &mut observer_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_removed(&observer_log, doc_id),
        "unauthorized delete should not be broadcast to subscribers: log={observer_log:?}"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    observer.shutdown().await.expect("shutdown observer");
    server.shutdown().await;
}
