use std::collections::HashMap;
use std::time::Duration;

use super::support::{
    TestingClient, collect_stream_deltas, connect_ready_claims, connect_ready_user, has_added,
    has_any_change, has_removed, has_updated, wait_for_query, wait_for_rows,
    wait_for_subscription_update,
};
use jazz_tools::middleware::auth::{LocalAuthMode, derive_local_principal_id};
use jazz_tools::query_manager::policy::PolicyExpr;
use jazz_tools::query_manager::types::{TablePolicies, TableSchemaBuilder};
use jazz_tools::server::TestingServer;
use jazz_tools::sync_tracer::SyncTracer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder,
    TableSchema, Value,
};
use serde_json::json;

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
const NO_DELTA_WINDOW: Duration = Duration::from_millis(100);

// -- Schema builders --

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

/// Schema for documents owned by `owner_id` with INSERT/UPDATE/DELETE restricted
/// to the row owner. SELECT is unrestricted (no policy) so observers can read.
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

/// Schema with an UPDATE policy where `using = True` (anyone can target any row)
/// and `with_check = owner_policy` (only the owner may commit the new value).
/// This lets tests verify that the two clauses are evaluated independently.
fn write_check_policy_schema() -> Schema {
    let owner_policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);

    SchemaBuilder::new()
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .policies(TablePolicies::new().with_update(Some(PolicyExpr::True), owner_policy)),
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

fn make_documents_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("owner_id", ColumnType::Text)
        .column("title", ColumnType::Text)
        .column("archived", ColumnType::Boolean)
        .policies(policies)
}

// -- Value constructors --

fn document_input(owner_id: &str, title: &str) -> HashMap<String, Value> {
    row_input!("owner_id" => owner_id, "title" => title)
}

fn boolean_policy_document_input(
    owner_id: &str,
    title: &str,
    archived: bool,
) -> HashMap<String, Value> {
    row_input!("owner_id" => owner_id, "title" => title, "archived" => archived)
}

fn team_document_input(team_id: ObjectId, title: &str) -> HashMap<String, Value> {
    row_input!("team_id" => Value::Uuid(team_id), "title" => title)
}

/// Returns row values for the 2-column `documents` table used in write policy tests.
fn document_values(owner_id: &str, title: &str) -> Vec<Value> {
    vec![owner_id.into(), title.into()]
}

fn document_row_values(owner_id: &str, title: &str) -> Vec<Value> {
    document_values(owner_id, title)
}

fn boolean_policy_document_values(owner_id: &str, title: &str, archived: bool) -> Vec<Value> {
    vec![owner_id.into(), title.into(), archived.into()]
}

fn team_document_values(team_id: ObjectId, title: &str) -> Vec<Value> {
    vec![Value::Uuid(team_id), title.into()]
}

fn team_document_row_values(team_id: ObjectId, title: &str) -> Vec<Value> {
    team_document_values(team_id, title)
}

// -- Seed / mutation helpers --

async fn seed_document(
    client: &JazzClient,
    table_name: &str,
    owner_id: &str,
    title: &str,
    archived: bool,
) -> ObjectId {
    client
        .create(
            table_name,
            boolean_policy_document_input(owner_id, title, archived),
        )
        .await
        .expect("create document")
        .0
}

async fn create_document(client: &JazzClient, owner_id: &str, title: &str) -> ObjectId {
    client
        .create("documents", document_input(owner_id, title))
        .await
        .expect("create document")
        .0
}

async fn create_org(client: &JazzClient, name: &str) -> ObjectId {
    client
        .create("orgs", row_input!("name" => name))
        .await
        .expect("create org")
        .0
}

async fn create_team(client: &JazzClient, name: &str, org_id: ObjectId) -> ObjectId {
    client
        .create(
            "teams",
            row_input!("name" => name, "org_id" => Value::Uuid(org_id)),
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
            row_input!("owner_id" => owner_id, "team_id" => Value::Uuid(team_id)),
        )
        .await
        .expect("create team membership")
        .0
}

async fn create_team_document(client: &JazzClient, team_id: ObjectId, title: &str) -> ObjectId {
    client
        .create("team_documents", team_document_input(team_id, title))
        .await
        .expect("create team document")
        .0
}

async fn update_document_title(client: &JazzClient, document_id: ObjectId, title: &str) {
    client
        .update(document_id, vec![("title".to_string(), title.into())])
        .await
        .expect("update document title");
}

async fn start_alice_and_bob_server(schema: Schema) -> (TestingServer, JazzClient, JazzClient) {
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;

    let ready_table = schema
        .keys()
        .next()
        .map(|table| table.as_str().to_string())
        .expect("schema must contain at least one table");

    let alice = connect_ready_user(&server, &schema, "alice", &ready_table, READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, "bob", &ready_table, READY_TIMEOUT).await;

    (server, alice, bob)
}

// -- Tests --

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
    let schema = SchemaBuilder::new()
        .table(make_documents_schema(
            "documents",
            TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
                .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        ))
        .build();

    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;

    let query = QueryBuilder::new("documents").build();

    let mut alice_stream = alice
        .subscribe(query.clone())
        .await
        .expect("subscribe alice");
    let mut bob_stream = bob.subscribe(query.clone()).await.expect("subscribe bob");
    let mut alice_log = Vec::new();
    let mut bob_log = Vec::new();

    let alice_doc = seed_document(&alice, "documents", "alice", "Alice Only", false).await;
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

    let bob_doc = seed_document(&bob, "documents", "bob", "Bob Only", false).await;
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
    assert_eq!(
        alice_rows[0].1,
        boolean_policy_document_values("alice", "Alice Only", false)
    );

    let bob_rows = wait_for_rows(&bob, query, "bob visible rows", |rows| {
        (rows.len() == 1 && rows[0].0 == bob_doc).then_some(rows)
    })
    .await;
    assert_eq!(
        bob_rows[0].1,
        boolean_policy_document_values("bob", "Bob Only", false)
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that the `session.userId` alias scopes subscription updates and
/// query results identically to `session.user_id`.
///
/// Alice and bob each insert an owned row into a table protected by
/// `owner_id = session.userId`. Each client should only observe its own row,
/// both in the live subscription stream and in EdgeServer query results.
///
/// ```text
/// alice ──insert "Alice Alias"──► server ──► alice stream (add ✓)
///                                     │
///                                     └── SELECT policy ──✗──► bob stream (silent)
///
/// bob ──insert "Bob Alias"──────► server ──► bob stream (add ✓)
///                                   │
///                                   └── SELECT policy ──✗──► alice stream (silent)
/// ```
#[tokio::test]
async fn session_user_id_alias_resolves_identically_to_snake_case() {
    let schema = SchemaBuilder::new()
        .table(make_documents_schema(
            "documents",
            TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["userId".into()]))
                .with_insert(PolicyExpr::eq_session("owner_id", vec!["userId".into()])),
        ))
        .build();

    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;

    let query = QueryBuilder::new("documents").build();

    let mut alice_stream = alice
        .subscribe(query.clone())
        .await
        .expect("subscribe alice");
    let mut bob_stream = bob.subscribe(query.clone()).await.expect("subscribe bob");
    let mut alice_log = Vec::new();
    let mut bob_log = Vec::new();

    let alice_doc = seed_document(&alice, "documents", "alice", "Alice Alias", false).await;
    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        QUERY_TIMEOUT,
        "alice add delta via session.userId",
        |log| has_added(log, alice_doc),
    )
    .await;
    collect_stream_deltas(&mut bob_stream, &mut bob_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_any_change(&bob_log, alice_doc),
        "bob should not receive alice's alias-scoped document"
    );

    let bob_doc = seed_document(&bob, "documents", "bob", "Bob Alias", false).await;
    wait_for_subscription_update(
        &mut bob_stream,
        &mut bob_log,
        QUERY_TIMEOUT,
        "bob add delta via session.userId",
        |log| has_added(log, bob_doc),
    )
    .await;
    collect_stream_deltas(&mut alice_stream, &mut alice_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_any_change(&alice_log, bob_doc),
        "alice should not receive bob's alias-scoped document"
    );

    let alice_rows = wait_for_rows(&alice, query.clone(), "alice alias visible rows", |rows| {
        (rows.len() == 1 && rows[0].0 == alice_doc).then_some(rows)
    })
    .await;
    assert_eq!(
        alice_rows[0].1,
        boolean_policy_document_values("alice", "Alice Alias", false)
    );

    let bob_rows = wait_for_rows(&bob, query, "bob alias visible rows", |rows| {
        (rows.len() == 1 && rows[0].0 == bob_doc).then_some(rows)
    })
    .await;
    assert_eq!(
        bob_rows[0].1,
        boolean_policy_document_values("bob", "Bob Alias", false)
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that an anonymous client with no `session.user_id` cannot see rows
/// protected by `owner_id = session.user_id`.
///
/// Alice and bob each insert an owned row. After both rows are confirmed
/// server-side, an unauthenticated client queries the same table and must see
/// an empty result set because the SELECT policy cannot match without a
/// session user id.
#[tokio::test]
async fn anonymous_client_cannot_see_owner_restricted_rows() {
    let schema = SchemaBuilder::new()
        .table(make_documents_schema(
            "documents",
            TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
                .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        ))
        .build();

    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;
    let query = QueryBuilder::new("documents").build();

    let alice_doc = seed_document(&alice, "documents", "alice", "Alice Only", false).await;
    let bob_doc = seed_document(&bob, "documents", "bob", "Bob Only", false).await;

    let alice_rows = wait_for_rows(&alice, query.clone(), "alice sees own row", |rows| {
        (rows.len() == 1 && rows[0].0 == alice_doc).then_some(rows)
    })
    .await;
    assert_eq!(
        alice_rows[0].1,
        boolean_policy_document_values("alice", "Alice Only", false)
    );

    let bob_rows = wait_for_rows(&bob, query.clone(), "bob sees own row", |rows| {
        (rows.len() == 1 && rows[0].0 == bob_doc).then_some(rows)
    })
    .await;
    assert_eq!(
        bob_rows[0].1,
        boolean_policy_document_values("bob", "Bob Only", false)
    );

    let anonymous_user_id = derive_local_principal_id(
        server.app_id(),
        LocalAuthMode::Anonymous,
        "anonymous-owner-restricted-device",
    );
    let anonymous = connect_ready_claims(
        &server,
        &schema,
        &anonymous_user_id,
        json!({
            "auth_mode": "local",
            "local_mode": "anonymous"
        }),
        "documents",
        READY_TIMEOUT,
    )
    .await;

    let anonymous_rows = wait_for_query(
        &anonymous,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "anonymous sees no owner-restricted rows",
        Some,
    )
    .await;
    assert!(anonymous_rows.is_empty());

    anonymous.shutdown().await.expect("shutdown anonymous");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that `owner_id = session.user_id` consistently scopes CRUD access
/// per client:
/// - inserts are accepted only for the caller's own `owner_id`
/// - updates require both the old and new row to stay owned by the caller
/// - deletes only succeed for rows owned by the caller
/// - selects only return rows owned by the caller
///
/// ```text
/// alice ──insert owner=alice──────► server ──► visible to alice only
/// bob ────insert owner=bob────────► server ──► visible to bob only
/// alice ──update own title────────► server ──► accepted
/// alice ──transfer owner→bob──────► server ──✗ rejected (new row owner mismatch)
/// bob ────delete own row──────────► server ──► removed
/// ```
#[tokio::test]
async fn session_user_id_policies_scope_crud_to_owned_rows() {
    let owner_policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
    let schema = SchemaBuilder::new()
        .table(make_documents_schema(
            "documents",
            TablePolicies::new()
                .with_select(owner_policy.clone())
                .with_insert(owner_policy.clone())
                .with_update(Some(owner_policy.clone()), owner_policy.clone())
                .with_delete(owner_policy),
        ))
        .build();
    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;
    let alice_reader =
        connect_ready_user(&server, &schema, "alice", "documents", READY_TIMEOUT).await;
    let bob_reader = connect_ready_user(&server, &schema, "bob", "documents", READY_TIMEOUT).await;
    let query = QueryBuilder::new("documents").build();

    let alice_doc = seed_document(&alice, "documents", "alice", "alice original", false).await;
    let bob_doc = seed_document(&bob, "documents", "bob", "bob original", false).await;

    let alice_rows = wait_for_rows(
        &alice_reader,
        query.clone(),
        "alice reader sees only owned row",
        |rows| (rows.len() == 1 && rows[0].0 == alice_doc).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        boolean_policy_document_values("alice", "alice original", false)
    );
    assert!(
        alice_rows.iter().all(|(id, _)| *id != bob_doc),
        "alice should not see bob's row through select owner policy"
    );

    let bob_rows = wait_for_rows(
        &bob_reader,
        query.clone(),
        "bob reader sees only owned row",
        |rows| (rows.len() == 1 && rows[0].0 == bob_doc).then_some(rows),
    )
    .await;
    assert_eq!(
        bob_rows[0].1,
        boolean_policy_document_values("bob", "bob original", false)
    );
    assert!(
        bob_rows.iter().all(|(id, _)| *id != alice_doc),
        "bob should not see alice's row through select owner policy"
    );

    update_document_title(&alice, alice_doc, "alice updated").await;
    let alice_rows = wait_for_rows(
        &alice_reader,
        query.clone(),
        "alice reader sees accepted update on owned row",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == alice_doc
                        && *values
                            == boolean_policy_document_values("alice", "alice updated", false)
                })
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(alice_rows.len(), 1);

    alice
        .update(
            alice_doc,
            vec![
                ("owner_id".to_string(), "bob".into()),
                ("title".to_string(), "transferred".into()),
            ],
        )
        .await
        .expect("optimistic local ownership transfer");

    let alice_rows = wait_for_rows(
        &alice_reader,
        query.clone(),
        "alice reader still sees owned row after rejected ownership transfer",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == alice_doc
                        && *values
                            == boolean_policy_document_values("alice", "alice updated", false)
                })
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(alice_rows.len(), 1);

    let bob_rows = wait_for_rows(
        &bob_reader,
        query.clone(),
        "bob reader still only sees bob row before own delete",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == bob_doc
                        && *values == boolean_policy_document_values("bob", "bob original", false)
                })
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(bob_rows.len(), 1);
    assert!(
        bob_rows.iter().all(|(id, _)| *id != alice_doc),
        "bob should still be unable to see alice's row after alice's rejected transfer"
    );

    bob.delete(bob_doc).await.expect("delete bob owned row");
    let bob_reader_after_delete =
        connect_ready_user(&server, &schema, "bob", "documents", READY_TIMEOUT).await;
    let bob_rows = wait_for_rows(
        &bob_reader_after_delete,
        query,
        "bob reader sees no owned rows after delete",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(bob_rows.is_empty());

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    alice_reader
        .shutdown()
        .await
        .expect("shutdown alice_reader");
    bob_reader.shutdown().await.expect("shutdown bob_reader");
    bob_reader_after_delete
        .shutdown()
        .await
        .expect("shutdown bob_reader_after_delete");
    server.shutdown().await;
}

/// Verifies that ownership transfer is only allowed while a document is not
/// archived.
///
/// Alice owns two documents: one active and one archived. The update policy
/// allows changing `owner_id` only when the old row satisfies both
/// `owner_id = session.user_id` and `archived = false`, and when the new row
/// also keeps `archived = false`.
///
/// ```text
/// alice ──update active owner→bob────► server ──► accepted
/// bob query ─────────────────────────► [active row now owned by bob]
///
/// alice ──update active owner→bob, archived=true──► server ──✗ rejected
/// alice query ────────────────────────────────────► [row stays active and owned by alice]
///
/// alice ──update archived owner→bob──► server ──✗ rejected
/// alice query ───────────────────────► [archived row still owned by alice]
/// ```
#[tokio::test]
async fn ownership_transfer_allowed_only_for_unarchived_documents() {
    let owner_policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
    let unarchived_policy = PolicyExpr::eq_literal("archived", false.into());
    let schema = SchemaBuilder::new()
        .table(make_documents_schema(
            "documents",
            TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
                .with_insert(owner_policy.clone())
                .with_update(
                    Some(PolicyExpr::and(vec![
                        owner_policy.clone(),
                        unarchived_policy.clone(),
                    ])),
                    unarchived_policy.clone(),
                ),
        ))
        .build();
    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;
    let alice_reader = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob_reader = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let query = QueryBuilder::new("documents").build();

    let active_id = seed_document(&alice, "documents", "alice", "active", false).await;
    let archived_id = seed_document(&alice, "documents", "alice", "archived", true).await;

    let alice_rows = wait_for_rows(
        &alice_reader,
        query.clone(),
        "alice reader sees both owned documents before transfer",
        |rows| (rows.len() == 2).then_some(rows),
    )
    .await;
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == active_id && *values == boolean_policy_document_values("alice", "active", false)
    }));
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == archived_id && *values == boolean_policy_document_values("alice", "archived", true)
    }));

    alice
        .update(
            active_id,
            vec![
                ("owner_id".to_string(), "bob".into()),
                ("title".to_string(), "active transferred".into()),
            ],
        )
        .await
        .expect("optimistic local active transfer");

    let bob_rows = wait_for_rows(
        &bob_reader,
        query.clone(),
        "bob reader sees transferred active document",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == active_id
                        && *values
                            == boolean_policy_document_values("bob", "active transferred", false)
                })
                .then_some(rows)
        },
    )
    .await;
    assert!(bob_rows.iter().any(|(id, values)| {
        *id == active_id
            && *values == boolean_policy_document_values("bob", "active transferred", false)
    }));
    assert!(
        bob_rows.iter().all(|(id, _)| *id != archived_id),
        "bob should not receive the archived document"
    );

    let transferable_id = seed_document(&alice, "documents", "alice", "transferable", false).await;
    let alice_rows = wait_for_rows(
        &alice_reader,
        query.clone(),
        "alice reader sees transferable active document before rejected archive-on-transfer",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == transferable_id
                        && *values == boolean_policy_document_values("alice", "transferable", false)
                })
                .then_some(rows)
        },
    )
    .await;
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == transferable_id
            && *values == boolean_policy_document_values("alice", "transferable", false)
    }));

    alice
        .update(
            transferable_id,
            vec![
                ("owner_id".to_string(), "bob".into()),
                ("title".to_string(), "transfer while archiving".into()),
                ("archived".to_string(), true.into()),
            ],
        )
        .await
        .expect("optimistic local transfer with archived=true");

    let alice_rows = wait_for_rows(
        &alice_reader,
        query.clone(),
        "alice reader still sees transferable document unchanged after rejected archive-on-transfer",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == transferable_id
                        && *values
                            == boolean_policy_document_values("alice", "transferable", false)
                })
                .then_some(rows)
        },
    )
    .await;
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == transferable_id
            && *values == boolean_policy_document_values("alice", "transferable", false)
    }));

    alice
        .update(
            archived_id,
            vec![
                ("owner_id".to_string(), "bob".into()),
                ("title".to_string(), "archived transferred".into()),
            ],
        )
        .await
        .expect("optimistic local archived transfer");

    let alice_rows = wait_for_rows(
        &alice_reader,
        query.clone(),
        "alice reader still sees archived document after rejected transfer",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == archived_id
                        && *values == boolean_policy_document_values("alice", "archived", true)
                })
                .then_some(rows)
        },
    )
    .await;
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == archived_id && *values == boolean_policy_document_values("alice", "archived", true)
    }));

    let bob_rows = wait_for_rows(
        &bob_reader,
        query,
        "bob reader still only sees the active transferred document",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == active_id
                && rows[0].1 == boolean_policy_document_values("bob", "active transferred", false))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(bob_rows.len(), 1);

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    alice_reader
        .shutdown()
        .await
        .expect("shutdown alice_reader");
    bob_reader.shutdown().await.expect("shutdown bob_reader");
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
    assert_eq!(alice_rows[0].1, vec![Value::from("Alice Org")]);

    let bob_rows = wait_for_rows(&bob, query, "bob visible orgs via membership", |rows| {
        (rows.len() == 1 && rows[0].0 == bob_org).then_some(rows)
    })
    .await;
    assert_eq!(bob_rows[0].1, vec![Value::from("Bob Org")]);
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
    assert_eq!(
        alice_rows[0].1,
        team_document_row_values(team_a, "Team A only")
    );
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
    assert_eq!(
        bob_rows[0].1,
        team_document_row_values(team_b, "Team B only")
    );
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
        .create("documents", document_input("alice", "forged"))
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
    assert_eq!(rows[0].1, document_row_values("alice", "allowed"));

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
            .find(|(id, values)| {
                *id == doc_id && *values == document_row_values("alice", "original")
            })
            .map(|_| ())
    })
    .await;

    bob.update(doc_id, vec![("title".to_string(), "hacked".into())])
        .await
        .expect("optimistic local update");

    // EdgeServer query is the causal barrier: it blocks until the server has
    // settled, guaranteeing bob's attempted update has been accepted or rejected.
    let rows_after_update = observer
        .query(query.clone(), Some(DurabilityTier::EdgeServer))
        .await
        .expect("EdgeServer query after unauthorized update");
    assert!(
        rows_after_update.iter().any(
            |(id, values)| *id == doc_id && *values == document_row_values("alice", "original")
        ),
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

/// Verifies that a row rejected by the INSERT policy is invisible to a
/// subscriber that connects **after** the rejected insert has been processed.
///
/// Mallory forges a document claiming alice's `owner_id`. Alice then creates
/// a legitimate document, which serves as the causal barrier: once alice can
/// see her own row at EdgeServer tier, the inbox has processed (and dropped)
/// mallory's earlier insert. A fresh subscriber that connects after the barrier
/// must find only alice's legitimate row in its initial query result.
///
/// ```text
/// mallory ──insert owner="alice"──► server ──✗ rejected (INSERT policy)
///
/// alice ──insert "legitimate"────► server ──► committed
///   └── EdgeServer query barrier ──────────────────────► (server settled)
///
/// fresh subscriber (connects after) ──query──► [alice's row only]
/// ```
#[tokio::test]
async fn insert_policy_violation_does_not_leak_to_pristine_subscriber() {
    let schema = write_policy_schema();
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let mallory = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("mallory")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let query = QueryBuilder::new("documents").build();

    // Mallory tries to insert a row claiming alice's ownership.
    let forged_id = mallory
        .create("documents", document_input("alice", "forged"))
        .await
        .expect("optimistic local create")
        .0;

    // Alice's legitimate insert is the causal barrier: her /sync request is
    // sent after mallory's, so once the server has committed alice's row the
    // inbox has already processed (and rejected) mallory's earlier request.
    let legit_id = create_document(&alice, "alice", "legitimate").await;
    wait_for_rows(
        &alice,
        query.clone(),
        "barrier: alice sees own committed row",
        |rows| rows.iter().any(|(id, _)| *id == legit_id).then_some(()),
    )
    .await;

    // Connect the fresh subscriber only after the server has settled.
    let fresh_subscriber = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("observer")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    // The fresh subscriber's initial query must contain alice's row but not
    // the forged row that was silently dropped by the INSERT policy.
    let rows = wait_for_rows(
        &fresh_subscriber,
        query,
        "fresh subscriber initial results",
        |rows| rows.iter().any(|(id, _)| *id == legit_id).then_some(rows),
    )
    .await;
    assert!(
        rows.iter().all(|(id, _)| *id != forged_id),
        "forged row must not appear in pristine subscriber's initial sync result"
    );

    mallory.shutdown().await.expect("shutdown mallory");
    alice.shutdown().await.expect("shutdown alice");
    fresh_subscriber
        .shutdown()
        .await
        .expect("shutdown fresh_subscriber");
    server.shutdown().await;
}

/// Verifies that the UPDATE `using` clause and `with_check` clause are
/// evaluated independently: a `using = True` policy lets every client target
/// any row, but a `with_check = owner_policy` still rejects writes from
/// non-owners.
///
/// Bob can read alice's row because there is no SELECT policy and `using = True`
/// means he can attempt to update it. But the server rejects his write because
/// the committed row state (`owner_id = "alice"`) fails the `with_check` when
/// evaluated against bob's session (`user_id = "bob"`). An EdgeServer-tier
/// query then confirms the original value persisted and the observer's stream
/// received no update delta.
///
/// ```text
/// alice ──insert "original"──────────► server ──► observer (add ✓)
///
/// bob (using=True → can target row) ──update title="hacked"──► server
///   └── with_check: owner_id="alice" ≠ bob ──✗ rejected
///
/// observer ──EdgeServer query──► "original" (unchanged)
/// observer stream → no update delta
/// ```
#[tokio::test]
async fn update_policy_read_clause_differs_from_write_clause() {
    let schema = write_check_policy_schema();
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

    // Bob can read alice's row — no SELECT restriction and using=True passes.
    wait_for_rows(&bob, query.clone(), "bob sees alice's row", |rows| {
        rows.iter()
            .find(|(id, values)| {
                *id == doc_id && *values == document_row_values("alice", "original")
            })
            .map(|_| ())
    })
    .await;

    // Bob's update is applied optimistically on his local client but the
    // with_check policy fails on the server: owner_id="alice" ≠ bob's user_id.
    bob.update(doc_id, vec![("title".to_string(), "hacked".into())])
        .await
        .expect("optimistic local update");

    // EdgeServer query is the causal barrier.
    let rows_after = observer
        .query(query.clone(), Some(DurabilityTier::EdgeServer))
        .await
        .expect("EdgeServer query after unauthorized update");
    assert!(
        rows_after.iter().any(
            |(id, values)| *id == doc_id && *values == document_row_values("alice", "original")
        ),
        "update rejected by with_check must not persist at EdgeServer: rows={rows_after:?}"
    );
    collect_stream_deltas(&mut observer_stream, &mut observer_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_updated(&observer_log, doc_id),
        "update rejected by with_check must not be broadcast: log={observer_log:?}"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    observer.shutdown().await.expect("shutdown observer");
    server.shutdown().await;
}

/// Verifies that after an owner deletes her row and then re-creates it, the
/// re-created row appears in the observer's stream as a fresh **add** delta
/// (new `ObjectId`), not as an **update** delta on the old one.
///
/// ```text
/// alice ──insert doc1──► observer stream (add ✓)
/// alice ──delete doc1──► observer stream (remove ✓)
/// alice ──insert doc2──► observer stream (add ✓, NOT update)
/// ```
#[tokio::test]
async fn delete_then_reinsert_by_owner_visible_to_others() {
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

    let doc1_id = create_document(&alice, "alice", "first").await;
    wait_for_subscription_update(
        &mut observer_stream,
        &mut observer_log,
        QUERY_TIMEOUT,
        "observer sees first document added",
        |log| has_added(log, doc1_id),
    )
    .await;

    alice.delete(doc1_id).await.expect("delete first document");
    wait_for_subscription_update(
        &mut observer_stream,
        &mut observer_log,
        QUERY_TIMEOUT,
        "observer sees first document removed",
        |log| has_removed(log, doc1_id),
    )
    .await;

    // Alice re-creates a document with equivalent content — a new ObjectId is
    // assigned, so the observer must see an add delta, not an update.
    let doc2_id = create_document(&alice, "alice", "first").await;
    wait_for_subscription_update(
        &mut observer_stream,
        &mut observer_log,
        QUERY_TIMEOUT,
        "observer sees re-created document as add delta",
        |log| has_added(log, doc2_id),
    )
    .await;

    assert_ne!(doc1_id, doc2_id, "re-created row must have a new ObjectId");
    assert!(
        !has_updated(&observer_log, doc2_id),
        "re-created row must arrive as add, not update: log={observer_log:?}"
    );

    alice.shutdown().await.expect("shutdown alice");
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
            .find(|(id, values)| {
                *id == doc_id && *values == document_row_values("alice", "original")
            })
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
        rows_after_delete.iter().any(
            |(id, values)| *id == doc_id && *values == document_row_values("alice", "original")
        ),
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

/// Verifies that a single client's operations reach the server in causal order.
///
/// This test lives in the policy suite but the failure mode is caused by the
/// sync layer: the transport may deliver two writes from the same client out of
/// order, making an otherwise-correct policy decision look wrong.
///
/// Alice locks herself out by transferring ownership to bob, then immediately
/// fires a title update. The two writes race to the server via the same
/// transport — if the transport delivers them out of order, the title update
/// lands while alice still owns the row and is incorrectly accepted.
///
/// The observer's EdgeServer query is used as a causal barrier: once the
/// marker document (sent last by alice) is visible, all prior writes have
/// been processed. The observer then checks the settled state of the document.
///
/// ```text
/// alice ── create(doc, owner="alice", title="original") ──► server ✓
/// alice ── update(doc, owner="bob")        ────────────────► server ✓  (alice locked out)
/// alice ── update(doc, title="nope")       ────────────────► server ✗  (owner≠session)
/// .. 500 times ..
/// alice ── create(marker)                  ────────────────► server ✓  (causal barrier)
///
/// observer ── wait for marker ──► query doc
///   expected: owner="bob", title="original"  (in-order: title rejected)
///   broken:   owner="bob", title="nope"      (out-of-order: title accepted before lockout)
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "TODO: sync transport does not guarantee delivery order - writes from the same client can arrive at the server out of sequence"]
async fn single_client_operations_reach_server_in_causal_order() {
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

    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("observer")
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let query = QueryBuilder::new("documents").build();

    let doc_id = create_document(&alice, "alice", "original").await;
    wait_for_rows(&observer, query.clone(), "document on server", |rows| {
        rows.iter().any(|(id, _)| *id == doc_id).then_some(())
    })
    .await;

    // Transfer ownership (allowed — USING checks current owner_id = "alice").
    alice
        .update(doc_id, vec![("owner_id".to_string(), "bob".into())])
        .await
        .expect("optimistic local update: transfer ownership");

    // Yield to the runtime so the transport's background sender can pick up
    // the ownership transfer and begin dispatching it as its own HTTP batch.
    // The title updates below then land in a second batch that races with the
    // first on the server's worker threads — widening the race window enough
    // to make the failure deterministic.
    tokio::task::yield_now().await;

    // Immediately try to update title — server must reject this because, in
    // order, ownership has already moved to bob.
    for i in 0..500 {
        alice
            .update(doc_id, vec![("title".to_string(), "nope".into())])
            .await
            .expect(&format!(
                "optimistic local update: title change after lockout {}",
                i
            ))
    }

    // Marker travels through the same transport; it cannot arrive before the
    // race window of the updates above.
    let marker_id = create_document(&alice, "alice", "marker").await;
    wait_for_rows(
        &observer,
        query.clone(),
        "marker visible on server",
        |rows| rows.iter().any(|(id, _)| *id == marker_id).then_some(()),
    )
    .await;

    let rows = wait_for_rows(&observer, query, "observer: settled doc state", |rows| {
        rows.iter()
            .find(|(id, _)| *id == doc_id)
            .map(|(_, values)| values.clone())
    })
    .await;

    assert_eq!(
        rows,
        document_row_values("bob", "original"),
        "title update after ownership transfer must be rejected — \
         proves operations arrive in causal order"
    );

    alice.shutdown().await.expect("shutdown alice");
    observer.shutdown().await.expect("shutdown observer");
    server.shutdown().await;
}

/// Verifies that a rejected mutation is rolled back to the originating client.
///
/// This test lives in the policy suite but the failure mode is triggered by the
/// sync layer: the server rejects a mutation but never sends a correction back,
/// so the originating client's local state diverges from the server's truth.
///
/// This is a companion to `single_client_operations_reach_server_in_causal_order`.
/// That test confirms the *server* correctly rejects alice's title update. This
/// test asks whether *alice herself* ever learns about the rejection.
///
/// When the server drops a mutation it does not (yet) send a correction back
/// to the originating client. Alice's local store keeps the optimistic value
/// indefinitely — she has no way to know her write failed.
///
/// The correct behaviour: a rejected mutation must produce a rollback event
/// delivered to the originating client so her local state converges to the
/// server's truth.
///
/// ```text
/// alice ── update(doc, title="nope") ──► server ✗ (rejected)
///                                           │
///                         missing ◄─────────┘  rollback to alice
///
/// alice.query(doc) → title="nope"   (stuck on optimistic — WRONG)
///                  → title="original" (converged to server — CORRECT, not yet implemented)
/// ```
///
/// STATUS: FAILS — server does not notify the originating client of rejections.
#[tokio::test]
#[ignore = "TODO: server does not notify the originating client of rejections."]
async fn originating_client_receives_rollback_for_rejected_mutation() {
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

    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("observer")
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let query = QueryBuilder::new("documents").build();

    let doc_id = create_document(&alice, "alice", "original").await;
    wait_for_rows(&observer, query.clone(), "document on server", |rows| {
        rows.iter().any(|(id, _)| *id == doc_id).then_some(())
    })
    .await;

    alice
        .update(doc_id, vec![("owner_id".to_string(), "bob".into())])
        .await
        .expect("optimistic local update: transfer ownership");

    alice
        .update(doc_id, vec![("title".to_string(), "nope".into())])
        .await
        .expect("optimistic local update: title change after lockout");

    // Use the marker as a causal barrier so we know the server has settled
    // before asking alice about her local view.
    let marker_id = create_document(&alice, "alice", "marker").await;
    wait_for_rows(
        &observer,
        query.clone(),
        "marker visible on server",
        |rows| rows.iter().any(|(id, _)| *id == marker_id).then_some(()),
    )
    .await;

    // Alice's *local* cache must converge to the server's truth after rejection.
    // We deliberately query with no durability tier (local reads only) so the
    // assertion exercises alice's in-process state rather than round-tripping to
    // the server. Using EdgeServer durability here would bypass the bug: the
    // server holds the correct value regardless, so an EdgeServer read always
    // returns title="original" even when alice never received a rollback event.
    let alice_rows = wait_for_query(
        &alice,
        query,
        None,
        QUERY_TIMEOUT,
        "alice: local cache converged after rollback",
        |rows| {
            rows.iter()
                .find(|(id, _)| *id == doc_id)
                .map(|(_, values)| values.clone())
        },
    )
    .await;

    assert_eq!(
        alice_rows,
        document_row_values("bob", "original"),
        "alice must see the rollback — the rejected title update should be \
         reverted so she knows the mutation failed"
    );

    alice.shutdown().await.expect("shutdown alice");
    observer.shutdown().await.expect("shutdown observer");
    server.shutdown().await;
}
