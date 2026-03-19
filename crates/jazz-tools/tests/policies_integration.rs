#![cfg(feature = "test")]

mod support;

use std::time::Duration;

use jazz_tools::query_manager::policy::PolicyExpr;
use jazz_tools::query_manager::types::TablePolicies;
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder,
    TableSchema, Value,
};
use serde_json::json;
use support::{
    TestingClient, collect_stream_deltas, has_added, has_any_change, has_removed, has_updated,
    wait_for_query, wait_for_rows, wait_for_subscription_update,
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

fn join_select_policy_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("orgs").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("teams")
                .column("name", ColumnType::Text)
                .fk_column("org_id", "orgs"),
        )
        .table(
            TableSchema::builder("team_memberships")
                .column("owner_id", ColumnType::Text)
                .fk_column("team_id", "teams")
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
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

fn in_session_array_policy_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("team_documents")
                .column("team_id", ColumnType::Uuid)
                .column("title", ColumnType::Text)
                .policies(TablePolicies::new().with_select(PolicyExpr::in_session(
                    "team_id",
                    vec!["claims".into(), "team_ids".into()],
                ))),
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

async fn create_org(client: &JazzClient, name: &str) -> ObjectId {
    client
        .create("orgs", vec![Value::Text(name.to_string())])
        .await
        .expect("create org")
        .0
}

async fn create_team(client: &JazzClient, name: &str, org_id: ObjectId) -> ObjectId {
    client
        .create(
            "teams",
            vec![Value::Text(name.to_string()), Value::Uuid(org_id)],
        )
        .await
        .expect("create team")
        .0
}

async fn create_team_membership(
    client: &JazzClient,
    owner_id: &str,
    team_id: ObjectId,
) -> ObjectId {
    client
        .create(
            "team_memberships",
            vec![Value::Text(owner_id.to_string()), Value::Uuid(team_id)],
        )
        .await
        .expect("create team membership")
        .0
}

fn team_document_values(team_id: ObjectId, title: &str) -> Vec<Value> {
    vec![Value::Uuid(team_id), Value::Text(title.to_string())]
}

async fn create_team_document(client: &JazzClient, team_id: ObjectId, title: &str) -> ObjectId {
    client
        .create("team_documents", team_document_values(team_id, title))
        .await
        .expect("create team document")
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
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
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

/// Verifies that projected join results are anchored by rows visible after
/// `SELECT` policy filtering, so hidden base rows cannot leak shared hop
/// targets through `result_element_index`.
///
/// Alice and bob each have a team membership row they own. The query starts at
/// `team_memberships`, which is filtered by `owner_id = session.user_id`, then
/// hops through `teams` to `orgs` and projects the org row. Bob must only see
/// the org reachable through bob's visible membership, not alice's hidden one.
///
/// ```text
/// admin ──create alice membership──► Alice Team ──► Alice Org
/// admin ──create bob membership────► Bob Team ────► Bob Org
///
/// bob query: team_memberships → teams → orgs [result=orgs]
///   visible anchors: [bob membership]
///   hidden anchors:  [alice membership]
///   result:          [Bob Org]
/// ```
#[tokio::test]
async fn select_policy_excludes_rows_from_join_results() {
    let schema = join_select_policy_schema();
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .ready_on("team_memberships", READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on("team_memberships", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob")
        .as_user()
        .ready_on("team_memberships", READY_TIMEOUT)
        .connect()
        .await;

    let alice_org = create_org(&admin, "Alice Org").await;
    let bob_org = create_org(&admin, "Bob Org").await;
    let alice_team = create_team(&admin, "Alice Team", alice_org).await;
    let bob_team = create_team(&admin, "Bob Team", bob_org).await;
    let _alice_membership = create_team_membership(&admin, "alice", alice_team).await;
    let _bob_membership = create_team_membership(&admin, "bob", bob_team).await;

    let query = QueryBuilder::new("team_memberships")
        .join("teams")
        .on("team_memberships.team_id", "teams._id")
        .join("orgs")
        .on("teams.org_id", "orgs._id")
        .result_element_index(2)
        .build();

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice visible orgs via membership",
        |rows| (rows.len() == 1 && rows[0].0 == alice_org).then_some(rows),
    )
    .await;
    assert_eq!(alice_rows[0].1, vec![Value::Text("Alice Org".to_string())]);

    let bob_rows = wait_for_rows(&bob, query, "bob visible orgs via membership", |rows| {
        (rows.len() == 1 && rows[0].0 == bob_org).then_some(rows)
    })
    .await;
    assert_eq!(bob_rows[0].1, vec![Value::Text("Bob Org".to_string())]);
    assert!(
        bob_rows.iter().all(|(id, _)| *id != alice_org),
        "bob should not see org rows reachable only via alice's hidden membership"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that a session-array membership policy scopes visibility to rows
/// whose `team_id` is listed in the caller's claims.
///
/// The policy uses `team_id IN @session.claims.team_ids`. Alice's claims only
/// include Team A and bob's claims only include Team B, so each query result
/// must contain only that team's row.
///
/// ```text
/// admin ──create row(team_a)──► server
/// admin ──create row(team_b)──► server
///
/// alice claims: [team_a] ──query──► [team_a row]
/// bob claims:   [team_b] ──query──► [team_b row]
/// ```
#[tokio::test]
async fn in_session_array_policy_gates_visibility_by_membership() {
    let schema = in_session_array_policy_schema();
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .ready_on("team_documents", READY_TIMEOUT)
        .connect()
        .await;

    let team_a = ObjectId::new();
    let team_b = ObjectId::new();
    let alice_doc = create_team_document(&admin, team_a, "Team A only").await;
    let bob_doc = create_team_document(&admin, team_b, "Team B only").await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .with_claims(json!({ "team_ids": [team_a] }))
        .ready_on("team_documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob")
        .with_claims(json!({ "team_ids": [team_b] }))
        .ready_on("team_documents", READY_TIMEOUT)
        .connect()
        .await;
    let query = QueryBuilder::new("team_documents").build();

    let alice_rows = wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "alice visible team documents",
        |rows| (rows.len() == 1 && rows[0].0 == alice_doc).then_some(rows),
    )
    .await;
    assert_eq!(alice_rows[0].1, team_document_values(team_a, "Team A only"));
    assert!(
        alice_rows.iter().all(|(id, _)| *id != bob_doc),
        "alice should not see rows for teams outside her membership claims"
    );

    let bob_rows = wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "bob visible team documents",
        |rows| (rows.len() == 1 && rows[0].0 == bob_doc).then_some(rows),
    )
    .await;
    assert_eq!(bob_rows[0].1, team_document_values(team_b, "Team B only"));
    assert!(
        bob_rows.iter().all(|(id, _)| *id != alice_doc),
        "bob should not see rows for teams outside his membership claims"
    );

    admin.shutdown().await.expect("shutdown admin");
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
    let intruder = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("mallory")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("observer")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("alice")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
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

/// Verifies that an update attempt from a client that fails the write policy
/// is rejected by the server and never broadcast to subscribers.
///
/// Alice creates a document. Bob can read it (no SELECT restriction) but does
/// not own it. Bob's update is applied optimistically on his local client but
/// silently dropped by the server. An EdgeServer-tier query serves as the
/// causal barrier: it blocks until the server has settled, so if the value is
/// still "original" the rejection is confirmed. The observer's stream is then
/// drained to verify no update delta arrived.
///
/// ```text
/// alice ──insert "original"──────────► server ──► observer (add ✓)
///
/// bob ──update title="hacked"────────► server ──✗ rejected (owner_id ≠ bob)
///                                          │
///                                          └── observer query (EdgeServer) → "original"
///                                          └── observer stream → no update delta
/// ```
#[tokio::test]
async fn update_policies_block_unauthorized_server_mutations() {
    let schema = write_policy_schema();
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("observer")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
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

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    observer.shutdown().await.expect("shutdown observer");
    server.shutdown().await;
}

/// Verifies that a delete attempt from a client that fails the write policy is
/// rejected by the server and never broadcast to subscribers.
///
/// Alice creates a document. Bob can read it (no SELECT restriction) but does
/// not own it. Bob's delete is applied optimistically on his local client but
/// silently dropped by the server. An EdgeServer-tier query serves as the
/// causal barrier: it blocks until the server has settled, so if the row is
/// still present the rejection is confirmed. The observer's stream is then
/// drained to verify no remove delta arrived.
///
/// ```text
/// alice ──insert "original"──────────► server ──► observer (add ✓)
///
/// bob ──delete────────────────────────► server ──✗ rejected (owner_id ≠ bob)
///                                           │
///                                           └── observer query (EdgeServer) → row present
///                                           └── observer stream → no remove delta
/// ```
#[tokio::test]
async fn delete_policies_block_unauthorized_server_mutations() {
    let schema = write_policy_schema();
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("observer")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
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

    bob.delete(doc_id).await.expect("optimistic local delete");

    // EdgeServer query is the causal barrier: it blocks until the server has
    // settled, guaranteeing bob's attempted delete has been accepted or rejected.
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
