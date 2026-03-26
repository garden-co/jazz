use std::collections::HashMap;
use std::time::Duration;

use super::support::{TestingClient, wait_for_rows};
use jazz_tools::query_manager::policy::PolicyExpr;
use jazz_tools::query_manager::types::{TablePolicies, TableSchemaBuilder};
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder, TableSchema, Value,
};

const READY_TIMEOUT: Duration = Duration::from_secs(30);

fn make_documents_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("owner_id", ColumnType::Text)
        .column("title", ColumnType::Text)
        .column("archived", ColumnType::Boolean)
        .policies(policies)
}

fn boolean_policy_document_values(owner_id: &str, title: &str, archived: bool) -> Vec<Value> {
    vec![
        Value::Text(owner_id.to_string()),
        Value::Text(title.to_string()),
        Value::Boolean(archived),
    ]
}

fn boolean_policy_document_input(
    owner_id: &str,
    title: &str,
    archived: bool,
) -> HashMap<String, Value> {
    HashMap::from([
        ("owner_id".to_string(), Value::Text(owner_id.to_string())),
        ("title".to_string(), Value::Text(title.to_string())),
        ("archived".to_string(), Value::Boolean(archived)),
    ])
}

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

async fn update_document_title(client: &JazzClient, document_id: ObjectId, title: &str) {
    client
        .update(
            document_id,
            vec![("title".to_string(), Value::Text(title.to_string()))],
        )
        .await
        .expect("update document title");
}

async fn update_document_archived(client: &JazzClient, document_id: ObjectId, archived: bool) {
    client
        .update(
            document_id,
            vec![("archived".to_string(), Value::Boolean(archived))],
        )
        .await
        .expect("update document archived");
}

async fn delete_document(client: &JazzClient, document_id: ObjectId) {
    client.delete(document_id).await.expect("delete document");
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

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on(ready_table.clone(), READY_TIMEOUT)
        .connect()
        .await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob")
        .as_user()
        .ready_on(ready_table, READY_TIMEOUT)
        .connect()
        .await;

    (server, alice, bob)
}

/// Verifies that simple boolean INSERT policies gate persisted row visibility
/// without needing a larger subscription scenario.
///
/// Alice inserts into two tables:
/// - `documents_insert_true`: insert allowed
/// - `documents_insert_false`: insert rejected
///
/// Bob only checks EdgeServer query results, so each assertion is about the
/// server-accepted state rather than alice's optimistic local cache.
///
/// ```text
/// alice ──insert──► server ──policy True/False──► persisted rows
///                                     │
///                                     └── bob EdgeServer query observes result
/// ```
#[tokio::test]
async fn insert_policies_boolean() {
    let schema = SchemaBuilder::new()
        .table(make_documents_schema(
            "documents_insert_true",
            TablePolicies::new().with_insert(PolicyExpr::True),
        ))
        .table(make_documents_schema(
            "documents_insert_false",
            TablePolicies::new().with_insert(PolicyExpr::False),
        ))
        .build();

    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;

    let insert_true_id =
        seed_document(&alice, "documents_insert_true", "alice", "original", false).await;
    let insert_false_id =
        seed_document(&alice, "documents_insert_false", "alice", "original", false).await;

    let query = QueryBuilder::new("documents_insert_true").build();
    let bob_rows = wait_for_rows(&bob, query, "bob sees inserted row", |rows| {
        rows.iter()
            .any(|(id, values)| {
                *id == insert_true_id
                    && *values == boolean_policy_document_values("alice", "original", false)
            })
            .then_some(rows)
    })
    .await;
    assert_eq!(bob_rows.len(), 1);
    assert_eq!(bob_rows[0].0, insert_true_id);
    assert_eq!(
        bob_rows[0].1,
        boolean_policy_document_values("alice", "original", false)
    );

    let query = QueryBuilder::new("documents_insert_false").build();
    let bob_rows = wait_for_rows(&bob, query, "bob does not see rejected insert", |rows| {
        Some(rows)
    })
    .await;
    assert_eq!(bob_rows.len(), 0);
    assert_ne!(
        insert_false_id, insert_true_id,
        "seed ids should be distinct"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that simple boolean SELECT policies gate persisted row visibility
/// without needing a larger subscription scenario.
///
/// Alice inserts one row into each table:
/// - `documents_select_true`: row is visible to bob
/// - `documents_select_false`: row is hidden from bob
///
/// Bob only checks EdgeServer query results, so each assertion is about the
/// server-accepted visible set rather than alice's optimistic local cache.
///
/// ```text
/// alice ──insert──► server ──SELECT True/False──► visible rows
///                                      │
///                                      └── bob EdgeServer query observes result
/// ```
#[tokio::test]
async fn select_policies_boolean() {
    let schema = SchemaBuilder::new()
        .table(make_documents_schema(
            "documents_select_true",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::True),
        ))
        .table(make_documents_schema(
            "documents_select_false",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::False),
        ))
        .build();

    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;

    let select_false_id =
        seed_document(&alice, "documents_select_false", "alice", "hidden", false).await;
    let select_true_id =
        seed_document(&alice, "documents_select_true", "alice", "visible", false).await;

    let query = QueryBuilder::new("documents_select_true").build();
    let bob_rows = wait_for_rows(
        &bob,
        query,
        "bob sees row allowed by select policy",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == select_true_id
                        && *values == boolean_policy_document_values("alice", "visible", false)
                })
                .then_some(rows)
        },
    )
    .await;
    assert!(bob_rows.iter().any(|(id, values)| {
        *id == select_true_id
            && *values == boolean_policy_document_values("alice", "visible", false)
    }));

    let query = QueryBuilder::new("documents_select_false").build();
    let bob_rows = wait_for_rows(
        &bob,
        query,
        "bob does not see row denied by select policy",
        |rows| Some(rows),
    )
    .await;
    assert_eq!(bob_rows.len(), 0);
    assert_ne!(
        select_false_id, select_true_id,
        "seed ids should be distinct"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that a literal `SELECT archived = false` policy filters archived
/// rows out of query results.
///
/// Alice inserts one active row and one archived row into the same table. Bob
/// should only ever observe the active row from the EdgeServer query because
/// the archived row fails the SELECT predicate on the persisted state.
///
/// ```text
/// alice ──insert archived=false─► server ──SELECT archived=false──► visible to bob
/// alice ──insert archived=true──► server ──SELECT archived=false──► hidden from bob
/// ```
#[tokio::test]
async fn select_policies_filter_out_archived_rows() {
    let table_name = "documents_select_unarchived";
    let schema = SchemaBuilder::new()
        .table(make_documents_schema(
            table_name,
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::eq_literal("archived", Value::Boolean(false))),
        ))
        .build();

    let (server, alice, bob) = start_alice_and_bob_server(schema).await;

    let active_id = seed_document(&alice, table_name, "alice", "active", false).await;
    let archived_id = seed_document(&alice, table_name, "alice", "archived", true).await;

    let query = QueryBuilder::new(table_name).build();
    let bob_rows = wait_for_rows(
        &bob,
        query,
        "bob only sees rows where archived=false",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == active_id
                        && *values == boolean_policy_document_values("alice", "active", false)
                })
                .then_some(rows)
                .filter(|rows| rows.iter().all(|(id, _)| *id != archived_id))
        },
    )
    .await;

    assert_eq!(bob_rows.len(), 1);
    assert!(bob_rows.iter().any(|(id, values)| {
        *id == active_id && *values == boolean_policy_document_values("alice", "active", false)
    }));
    assert!(bob_rows.iter().all(|(id, _)| *id != archived_id));

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that simple boolean UPDATE policies gate persisted row visibility
/// without needing a larger subscription scenario.
///
/// Alice seeds one row in each table, then updates both:
/// - `documents_update_true`: update allowed
/// - `documents_update_false`: update rejected
///
/// Bob only checks EdgeServer query results, so each assertion is about the
/// server-accepted state rather than alice's optimistic local cache.
///
/// ```text
/// alice ──update──► server ──policy True/False──► persisted rows
///                                     │
///                                     └── bob EdgeServer query observes result
/// ```
#[tokio::test]
async fn update_policies_boolean() {
    let schema = SchemaBuilder::new()
        .table(make_documents_schema(
            "documents_update_true",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_update(Some(PolicyExpr::True), PolicyExpr::True),
        ))
        .table(make_documents_schema(
            "documents_update_false",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_update(Some(PolicyExpr::False), PolicyExpr::False),
        ))
        .build();

    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;

    let update_true_id =
        seed_document(&alice, "documents_update_true", "alice", "original", false).await;
    let update_false_id =
        seed_document(&alice, "documents_update_false", "alice", "original", false).await;

    let query = QueryBuilder::new("documents_update_false").build();
    wait_for_rows(&bob, query, "bob sees seeded rows before updates", |rows| {
        rows.iter()
            .any(|(id, values)| {
                *id == update_false_id
                    && *values == boolean_policy_document_values("alice", "original", false)
            })
            .then_some(())
    })
    .await;

    update_document_title(&alice, update_true_id, "updated").await;
    let query = QueryBuilder::new("documents_update_true").build();
    let bob_rows = wait_for_rows(&bob, query, "bob sees accepted update", |rows| {
        rows.iter()
            .any(|(id, values)| {
                *id == update_true_id
                    && *values == boolean_policy_document_values("alice", "updated", false)
            })
            .then_some(rows)
    })
    .await;
    assert!(bob_rows.iter().any(|(id, values)| {
        *id == update_true_id
            && *values == boolean_policy_document_values("alice", "updated", false)
    }));

    update_document_title(&alice, update_false_id, "blocked").await;
    let query = QueryBuilder::new("documents_update_false").build();
    let bob_rows = wait_for_rows(&bob, query, "bob still sees original row", |rows| {
        rows.iter()
            .any(|(id, values)| {
                *id == update_false_id
                    && *values == boolean_policy_document_values("alice", "original", false)
            })
            .then_some(rows)
    })
    .await;
    assert!(
        bob_rows.iter().any(|(id, values)| {
            *id == update_false_id
                && *values == boolean_policy_document_values("alice", "original", false)
        }),
        "update rejected by false policy should leave the original value visible"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that simple boolean DELETE policies gate persisted row visibility
/// without needing a larger subscription scenario.
///
/// Alice seeds one row in each table, then deletes both:
/// - `documents_delete_true`: delete allowed
/// - `documents_delete_false`: delete rejected
///
/// Bob only checks EdgeServer query results, so each assertion is about the
/// server-accepted state rather than alice's optimistic local cache.
///
/// ```text
/// alice ──delete──► server ──policy True/False──► persisted rows
///                                     │
///                                     └── bob EdgeServer query observes result
/// ```
#[tokio::test]
async fn delete_policies_boolean() {
    let schema = SchemaBuilder::new()
        .table(make_documents_schema(
            "documents_delete_true",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_delete(PolicyExpr::True),
        ))
        .table(make_documents_schema(
            "documents_delete_false",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_delete(PolicyExpr::False),
        ))
        .build();

    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;

    let delete_true_id =
        seed_document(&alice, "documents_delete_true", "alice", "original", false).await;
    let delete_false_id =
        seed_document(&alice, "documents_delete_false", "alice", "original", false).await;

    let query = QueryBuilder::new("documents_delete_false").build();
    wait_for_rows(&bob, query, "bob sees seeded rows before deletes", |rows| {
        rows.iter()
            .any(|(id, values)| {
                *id == delete_false_id
                    && *values == boolean_policy_document_values("alice", "original", false)
            })
            .then_some(())
    })
    .await;

    delete_document(&alice, delete_true_id).await;
    let query = QueryBuilder::new("documents_delete_true").build();
    let bob_rows = wait_for_rows(&bob, query, "bob no longer sees deleted row", |rows| {
        rows.iter()
            .all(|(id, _)| *id != delete_true_id)
            .then_some(rows)
    })
    .await;
    assert!(
        bob_rows.iter().all(|(id, _)| *id != delete_true_id),
        "delete allowed by true policy should remove the row"
    );

    delete_document(&alice, delete_false_id).await;
    let query = QueryBuilder::new("documents_delete_false").build();
    let bob_rows = wait_for_rows(&bob, query, "bob still sees undeleted row", |rows| {
        rows.iter()
            .any(|(id, values)| {
                *id == delete_false_id
                    && *values == boolean_policy_document_values("alice", "original", false)
            })
            .then_some(rows)
    })
    .await;
    assert!(
        bob_rows.iter().any(|(id, values)| {
            *id == delete_false_id
                && *values == boolean_policy_document_values("alice", "original", false)
        }),
        "delete rejected by false policy should leave the original row visible"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies a simple state-machine policy over `archived`:
/// inserts require `archived = false`, updates require the previous row to
/// have `archived = false`, and deletes require `archived = true`.
///
/// Alice first tries to insert an already-archived row, which must be
/// rejected. She then inserts an incomplete row. Bob tries to delete that
/// incomplete row, which must also be rejected. A fresh observer then proves
/// the row still exists, sees Alice archive it, and later deletes it once it
/// is archived. Alice also tries to reopen it, which must be rejected because
/// the old row is already archived. Using a fresh observer for the readback
/// and final delete keeps the causal barriers clean.
///
/// ```text
/// alice ──insert archived=true──► server ──✗ rejected
/// alice ──insert archived=false─► server ──► visible to bob
/// bob ───delete archived=false──► server ──✗ rejected
/// observer ─query incomplete─────► server ──► row still visible
/// alice ──update false→true──────► server ──► visible to observer
/// alice ──update true→false──────► server ──✗ rejected
/// observer ─delete archived=true─► server ──► row removed
/// ```
#[tokio::test]
async fn archived_state_policies_gate_insert_update_and_delete() {
    let incomplete_policy = PolicyExpr::eq_literal("archived", Value::Boolean(false));
    let archived_policy = PolicyExpr::eq_literal("archived", Value::Boolean(true));
    let table_name = "documents_archived_lifecycle";

    let schema = SchemaBuilder::new()
        .table(make_documents_schema(
            table_name,
            TablePolicies::new()
                .with_insert(incomplete_policy.clone())
                .with_update(Some(incomplete_policy), PolicyExpr::True)
                .with_delete(archived_policy),
        ))
        .build();

    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;
    let query = QueryBuilder::new(table_name).build();

    let rejected_insert_id = seed_document(&alice, table_name, "alice", "already-done", true).await;
    let active_id = seed_document(&alice, table_name, "alice", "task", false).await;

    let bob_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob sees only the incomplete row",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == active_id
                        && *values == boolean_policy_document_values("alice", "task", false)
                })
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(bob_rows.len(), 1);
    assert!(
        bob_rows.iter().all(|(id, _)| *id != rejected_insert_id),
        "archived=true insert should be rejected by the server"
    );

    // This optimistic local delete should be rejected because DELETE requires
    // archived=true on the current row.
    delete_document(&bob, active_id).await;

    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("observer")
        .as_user()
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;

    let observer_rows = wait_for_rows(
        &observer,
        query.clone(),
        "observer still sees incomplete row after rejected delete",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == active_id
                        && *values == boolean_policy_document_values("alice", "task", false)
                })
                .then_some(rows)
        },
    )
    .await;
    assert!(observer_rows.iter().any(|(id, values)| {
        *id == active_id && *values == boolean_policy_document_values("alice", "task", false)
    }));

    // Alice's successful archive update is the causal barrier for bob's
    // earlier rejected delete: it can only apply if the incomplete row still
    // exists server-side.
    update_document_archived(&alice, active_id, true).await;
    let observer_rows = wait_for_rows(
        &observer,
        query.clone(),
        "observer sees row archived",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == active_id
                        && *values == boolean_policy_document_values("alice", "task", true)
                })
                .then_some(rows)
        },
    )
    .await;
    assert!(observer_rows.iter().any(|(id, values)| {
        *id == active_id && *values == boolean_policy_document_values("alice", "task", true)
    }));

    // This optimistic local update should be rejected because UPDATE USING is
    // checked against the old row, which is already archived=true.
    update_document_archived(&alice, active_id, false).await;

    // Observer's delete is the causal barrier for the rejected reopen attempt: it is
    // only allowed if the row still exists server-side with archived=true.
    delete_document(&observer, active_id).await;
    let observer_rows = wait_for_rows(
        &observer,
        query,
        "observer sees lifecycle row removed",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(observer_rows.is_empty());

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    observer.shutdown().await.expect("shutdown observer");
    server.shutdown().await;
}
