use std::collections::HashMap;
use std::time::Duration;

use super::support::{
    collect_stream_deltas, connect_ready_client, connect_ready_user, has_added, has_any_change,
    has_removed, has_row, has_updated, lacks_row, wait_for_query, wait_for_rows,
    wait_for_subscription_update,
};
use jazz_tools::query_manager::policy::{CmpOp, PolicyExpr, PolicyValue};
use jazz_tools::query_manager::types::{TablePolicies, TableSchemaBuilder};
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder,
    TableSchema, Value,
};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
const NO_DELTA_WINDOW: Duration = Duration::from_millis(100);

fn make_documents_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("owner_id", ColumnType::Text)
        .column("title", ColumnType::Text)
        .column("archived", ColumnType::Boolean)
        .policies(super::explicit_allow_all_policies(policies))
}

fn boolean_policy_document_values(owner_id: &str, title: &str, archived: bool) -> Vec<Value> {
    vec![owner_id.into(), title.into(), archived.into()]
}

fn boolean_policy_document_input(
    owner_id: &str,
    title: &str,
    archived: bool,
) -> HashMap<String, Value> {
    row_input!("owner_id" => owner_id, "title" => title, "archived" => archived)
}

fn row_changes<const N: usize>(pairs: [(&str, Value); N]) -> Vec<(String, Value)> {
    pairs
        .into_iter()
        .map(|(column, value)| (column.to_string(), value))
        .collect()
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

async fn create_row(
    client: &JazzClient,
    table_name: &str,
    values: HashMap<String, Value>,
) -> ObjectId {
    client
        .create(table_name, values)
        .await
        .expect("create row")
        .0
}

async fn update_document_title(client: &JazzClient, document_id: ObjectId, title: &str) {
    client
        .update(document_id, vec![("title".to_string(), title.into())])
        .await
        .expect("update document title");
}

async fn update_document_archived(client: &JazzClient, document_id: ObjectId, archived: bool) {
    client
        .update(document_id, vec![("archived".to_string(), archived.into())])
        .await
        .expect("update document archived");
}

async fn update_row(client: &JazzClient, row_id: ObjectId, changes: Vec<(String, Value)>) {
    client.update(row_id, changes).await.expect("update row");
}

async fn delete_document(client: &JazzClient, document_id: ObjectId) {
    client.delete(document_id).await.expect("delete document");
}

fn make_priority_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("title", ColumnType::Text)
        .column("priority", ColumnType::Integer)
        .policies(super::explicit_allow_all_policies(policies))
}

fn priority_values(title: &str, priority: i32) -> Vec<Value> {
    vec![title.into(), priority.into()]
}

fn make_review_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("title", ColumnType::Text)
        .nullable_column("reviewer_id", ColumnType::Text)
        .policies(super::explicit_allow_all_policies(policies))
}

fn review_values(title: &str, reviewer_id: Option<&str>) -> Vec<Value> {
    vec![
        title.into(),
        reviewer_id.map(|value| value.into()).unwrap_or(Value::Null),
    ]
}

fn make_status_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("title", ColumnType::Text)
        .column("status", ColumnType::Text)
        .column("archived", ColumnType::Boolean)
        .policies(policies)
}

fn status_values(title: &str, status: &str, archived: bool) -> Vec<Value> {
    vec![title.into(), status.into(), archived.into()]
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
                .with_select(PolicyExpr::eq_literal("archived", false.into())),
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
            let active_values = boolean_policy_document_values("alice", "active", false);
            (has_row(&rows, active_id, &active_values) && lacks_row(&rows, archived_id))
                .then_some(rows)
        },
    )
    .await;

    assert_eq!(bob_rows.len(), 1);
    assert!(has_row(
        &bob_rows,
        active_id,
        &boolean_policy_document_values("alice", "active", false),
    ));
    assert!(lacks_row(&bob_rows, archived_id));

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
    let incomplete_policy = PolicyExpr::eq_literal("archived", false.into());
    let archived_policy = PolicyExpr::eq_literal("archived", true.into());
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

    let observer =
        connect_ready_user(&server, &schema, "observer", table_name, READY_TIMEOUT).await;

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
    assert!(has_row(
        &observer_rows,
        active_id,
        &boolean_policy_document_values("alice", "task", false),
    ));

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
    assert!(has_row(
        &observer_rows,
        active_id,
        &boolean_policy_document_values("alice", "task", true),
    ));

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

/// Verifies that scalar comparison operators `!=`, `>`, `>=`, `<`, and `<=`
/// filter rows end-to-end.
///
/// Alice seeds matching and non-matching priorities into each table, and bob
/// checks the persisted visible set through EdgeServer queries.
///
/// ```text
/// alice ──insert priorities──► server ──scalar comparator──► bob EdgeServer query
/// ```
#[tokio::test]
async fn select_policies_scalar_comparators_filter_rows() {
    let schema = SchemaBuilder::new()
        .table(make_priority_schema(
            "documents_select_ne",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::Cmp {
                    column: "priority".into(),
                    op: CmpOp::Ne,
                    value: PolicyValue::Literal(3i32.into()),
                }),
        ))
        .table(make_priority_schema(
            "documents_select_gt",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::Cmp {
                    column: "priority".into(),
                    op: CmpOp::Gt,
                    value: PolicyValue::Literal(3i32.into()),
                }),
        ))
        .table(make_priority_schema(
            "documents_select_gte",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::Cmp {
                    column: "priority".into(),
                    op: CmpOp::Ge,
                    value: PolicyValue::Literal(3i32.into()),
                }),
        ))
        .table(make_priority_schema(
            "documents_select_lt",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::Cmp {
                    column: "priority".into(),
                    op: CmpOp::Lt,
                    value: PolicyValue::Literal(3i32.into()),
                }),
        ))
        .table(make_priority_schema(
            "documents_select_lte",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::Cmp {
                    column: "priority".into(),
                    op: CmpOp::Le,
                    value: PolicyValue::Literal(3i32.into()),
                }),
        ))
        .build();

    let (server, alice, bob) = start_alice_and_bob_server(schema).await;

    let ne_match = create_row(
        &alice,
        "documents_select_ne",
        row_input!("title" => "different", "priority" => 5i32),
    )
    .await;
    let ne_hidden = create_row(
        &alice,
        "documents_select_ne",
        row_input!("title" => "exact", "priority" => 3i32),
    )
    .await;

    let gt_match = create_row(
        &alice,
        "documents_select_gt",
        row_input!("title" => "higher", "priority" => 5i32),
    )
    .await;
    let gt_hidden = create_row(
        &alice,
        "documents_select_gt",
        row_input!("title" => "equal", "priority" => 3i32),
    )
    .await;

    let gte_low = create_row(
        &alice,
        "documents_select_gte",
        row_input!("title" => "low", "priority" => 1i32),
    )
    .await;
    let gte_equal = create_row(
        &alice,
        "documents_select_gte",
        row_input!("title" => "equal", "priority" => 3i32),
    )
    .await;
    let gte_high = create_row(
        &alice,
        "documents_select_gte",
        row_input!("title" => "high", "priority" => 5i32),
    )
    .await;

    let lt_match = create_row(
        &alice,
        "documents_select_lt",
        row_input!("title" => "lower", "priority" => 1i32),
    )
    .await;
    let lt_hidden = create_row(
        &alice,
        "documents_select_lt",
        row_input!("title" => "equal", "priority" => 3i32),
    )
    .await;

    let lte_low = create_row(
        &alice,
        "documents_select_lte",
        row_input!("title" => "low", "priority" => 1i32),
    )
    .await;
    let lte_equal = create_row(
        &alice,
        "documents_select_lte",
        row_input!("title" => "equal", "priority" => 3i32),
    )
    .await;
    let lte_hidden = create_row(
        &alice,
        "documents_select_lte",
        row_input!("title" => "high", "priority" => 5i32),
    )
    .await;

    let ne_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_select_ne").build(),
        "ne comparator keeps only non-equal rows",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == ne_match
                && rows[0].1 == priority_values("different", 5)
                && rows.iter().all(|(id, _)| *id != ne_hidden))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(ne_rows.len(), 1);

    let gt_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_select_gt").build(),
        "gt comparator keeps only strictly greater rows",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == gt_match
                && rows[0].1 == priority_values("higher", 5)
                && rows.iter().all(|(id, _)| *id != gt_hidden))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(gt_rows.len(), 1);

    let gte_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_select_gte").build(),
        "gte comparator keeps equal and greater rows",
        |rows| {
            (rows.len() == 2
                && rows
                    .iter()
                    .any(|(id, values)| *id == gte_equal && *values == priority_values("equal", 3))
                && rows
                    .iter()
                    .any(|(id, values)| *id == gte_high && *values == priority_values("high", 5))
                && rows.iter().all(|(id, _)| *id != gte_low))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(gte_rows.len(), 2);

    let lt_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_select_lt").build(),
        "lt comparator keeps only strictly lower rows",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == lt_match
                && rows[0].1 == priority_values("lower", 1)
                && rows.iter().all(|(id, _)| *id != lt_hidden))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(lt_rows.len(), 1);

    let lte_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_select_lte").build(),
        "lte comparator keeps lower and equal rows",
        |rows| {
            (rows.len() == 2
                && rows
                    .iter()
                    .any(|(id, values)| *id == lte_low && *values == priority_values("low", 1))
                && rows
                    .iter()
                    .any(|(id, values)| *id == lte_equal && *values == priority_values("equal", 3))
                && rows.iter().all(|(id, _)| *id != lte_hidden))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(lte_rows.len(), 2);

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that nullable-column policies treat `= null`, `!= null`, and
/// `isNull` consistently for reads and writes.
///
/// Alice inserts and updates rows with and without `reviewer_id`, and bob
/// checks the server-visible state after policy enforcement.
///
/// ```text
/// alice ──insert/update reviewer_id──► server ──null policy checks──► bob EdgeServer query
/// ```
#[tokio::test]
async fn null_predicates_on_nullable_columns_gate_reads_and_writes() {
    let schema = SchemaBuilder::new()
        .table(make_review_schema(
            "documents_select_eq_null",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::Cmp {
                    column: "reviewer_id".into(),
                    op: CmpOp::Eq,
                    value: PolicyValue::Literal(Value::Null),
                }),
        ))
        .table(make_review_schema(
            "documents_select_ne_null",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::Cmp {
                    column: "reviewer_id".into(),
                    op: CmpOp::Ne,
                    value: PolicyValue::Literal(Value::Null),
                }),
        ))
        .table(make_review_schema(
            "documents_select_is_null",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::IsNull {
                    column: "reviewer_id".into(),
                }),
        ))
        .table(make_review_schema(
            "documents_insert_eq_null",
            TablePolicies::new()
                .with_insert(PolicyExpr::Cmp {
                    column: "reviewer_id".into(),
                    op: CmpOp::Eq,
                    value: PolicyValue::Literal(Value::Null),
                })
                .with_select(PolicyExpr::True),
        ))
        .table(make_review_schema(
            "documents_insert_ne_null",
            TablePolicies::new()
                .with_insert(PolicyExpr::Cmp {
                    column: "reviewer_id".into(),
                    op: CmpOp::Ne,
                    value: PolicyValue::Literal(Value::Null),
                })
                .with_select(PolicyExpr::True),
        ))
        .table(make_review_schema(
            "documents_update_is_null",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::True)
                .with_update(
                    Some(PolicyExpr::True),
                    PolicyExpr::IsNull {
                        column: "reviewer_id".into(),
                    },
                ),
        ))
        .build();

    let (server, alice, bob) = start_alice_and_bob_server(schema).await;

    let select_eq_null_visible = create_row(
        &alice,
        "documents_select_eq_null",
        row_input!("title" => "unassigned", "reviewer_id" => Value::Null),
    )
    .await;
    let select_eq_null_hidden = create_row(
        &alice,
        "documents_select_eq_null",
        row_input!("title" => "assigned", "reviewer_id" => "alice"),
    )
    .await;

    let select_ne_null_hidden = create_row(
        &alice,
        "documents_select_ne_null",
        row_input!("title" => "unassigned", "reviewer_id" => Value::Null),
    )
    .await;
    let select_ne_null_visible = create_row(
        &alice,
        "documents_select_ne_null",
        row_input!("title" => "assigned", "reviewer_id" => "alice"),
    )
    .await;

    let select_is_null_visible = create_row(
        &alice,
        "documents_select_is_null",
        row_input!("title" => "unassigned", "reviewer_id" => Value::Null),
    )
    .await;
    let select_is_null_hidden = create_row(
        &alice,
        "documents_select_is_null",
        row_input!("title" => "assigned", "reviewer_id" => "alice"),
    )
    .await;

    let insert_eq_null_visible = create_row(
        &alice,
        "documents_insert_eq_null",
        row_input!("title" => "allowed null", "reviewer_id" => Value::Null),
    )
    .await;
    let insert_eq_null_hidden = create_row(
        &alice,
        "documents_insert_eq_null",
        row_input!("title" => "rejected non-null", "reviewer_id" => "alice"),
    )
    .await;

    let insert_ne_null_hidden = create_row(
        &alice,
        "documents_insert_ne_null",
        row_input!("title" => "rejected null", "reviewer_id" => Value::Null),
    )
    .await;
    let insert_ne_null_visible = create_row(
        &alice,
        "documents_insert_ne_null",
        row_input!("title" => "allowed non-null", "reviewer_id" => "alice"),
    )
    .await;

    let update_is_null_allowed = create_row(
        &alice,
        "documents_update_is_null",
        row_input!("title" => "becomes null", "reviewer_id" => "alice"),
    )
    .await;
    let update_is_null_rejected = create_row(
        &alice,
        "documents_update_is_null",
        row_input!("title" => "stays null", "reviewer_id" => Value::Null),
    )
    .await;

    let select_eq_null_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_select_eq_null").build(),
        "policy eq/null comparisons are evaluated like byte equality instead of null semantics, so eq null keeps only null rows",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == select_eq_null_visible
                && rows[0].1 == review_values("unassigned", None)
                && rows.iter().all(|(id, _)| *id != select_eq_null_hidden))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(select_eq_null_rows.len(), 1);

    let select_ne_null_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_select_ne_null").build(),
        "ne null keeps only non-null rows",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == select_ne_null_visible
                && rows[0].1 == review_values("assigned", Some("alice"))
                && rows.iter().all(|(id, _)| *id != select_ne_null_hidden))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(select_ne_null_rows.len(), 1);

    let select_is_null_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_select_is_null").build(),
        "isNull keeps only null rows",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == select_is_null_visible
                && rows[0].1 == review_values("unassigned", None)
                && rows.iter().all(|(id, _)| *id != select_is_null_hidden))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(select_is_null_rows.len(), 1);

    let insert_eq_null_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_insert_eq_null").build(),
        "insert eq null allows only null reviewer rows",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == insert_eq_null_visible
                && rows[0].1 == review_values("allowed null", None)
                && rows.iter().all(|(id, _)| *id != insert_eq_null_hidden))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(insert_eq_null_rows.len(), 1);

    let insert_ne_null_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_insert_ne_null").build(),
        "insert ne null allows only non-null reviewer rows",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == insert_ne_null_visible
                && rows[0].1 == review_values("allowed non-null", Some("alice"))
                && rows.iter().all(|(id, _)| *id != insert_ne_null_hidden))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(insert_ne_null_rows.len(), 1);

    update_row(
        &alice,
        update_is_null_allowed,
        row_changes([("reviewer_id", Value::Null)]),
    )
    .await;
    update_row(
        &alice,
        update_is_null_rejected,
        row_changes([("reviewer_id", "bob".into())]),
    )
    .await;

    let update_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_update_is_null").build(),
        "write-side isNull allows nulling a reviewer but rejects assigning one",
        |rows| {
            (rows.len() == 2
                && rows.iter().any(|(id, values)| {
                    *id == update_is_null_allowed && *values == review_values("becomes null", None)
                })
                && rows.iter().any(|(id, values)| {
                    *id == update_is_null_rejected && *values == review_values("stays null", None)
                }))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(update_rows.len(), 2);

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that row-level `contains` and literal `IN (...)` predicates grant
/// matching rows and that an empty `IN` list fails closed.
///
/// Alice seeds matching and non-matching rows, and bob checks that EdgeServer
/// queries only expose the persisted rows allowed by each predicate.
///
/// ```text
/// alice ──insert rows──► server ──contains / in-list checks──► bob EdgeServer query
/// ```
#[tokio::test]
async fn row_level_contains_and_in_list_policies_filter_rows() {
    let schema = SchemaBuilder::new()
        .table(make_status_schema(
            "documents_select_contains",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::Contains {
                    column: "title".into(),
                    value: PolicyValue::Literal("Launch".into()),
                }),
        ))
        .table(make_status_schema(
            "documents_select_in_list",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::InList {
                    column: "status".into(),
                    values: vec![
                        PolicyValue::Literal("active".into()),
                        PolicyValue::Literal("trial".into()),
                    ],
                }),
        ))
        .table(make_status_schema(
            "documents_select_empty_in_list",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::InList {
                    column: "status".into(),
                    values: vec![],
                }),
        ))
        .build();

    let (server, alice, bob) = start_alice_and_bob_server(schema).await;

    let contains_match = create_row(
        &alice,
        "documents_select_contains",
        row_input!("title" => "Launch Checklist", "status" => "active", "archived" => false),
    )
    .await;
    let contains_hidden = create_row(
        &alice,
        "documents_select_contains",
        row_input!("title" => "Backlog", "status" => "active", "archived" => false),
    )
    .await;

    let in_active = create_row(
        &alice,
        "documents_select_in_list",
        row_input!("title" => "Active", "status" => "active", "archived" => false),
    )
    .await;
    let in_trial = create_row(
        &alice,
        "documents_select_in_list",
        row_input!("title" => "Trial", "status" => "trial", "archived" => false),
    )
    .await;
    let in_hidden = create_row(
        &alice,
        "documents_select_in_list",
        row_input!("title" => "Archived", "status" => "archived", "archived" => true),
    )
    .await;

    let empty_hidden = create_row(
        &alice,
        "documents_select_empty_in_list",
        row_input!("title" => "Should stay hidden", "status" => "active", "archived" => false),
    )
    .await;

    let contains_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_select_contains").build(),
        "contains exposes rows with a matching title substring",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == contains_match
                && rows[0].1 == status_values("Launch Checklist", "active", false)
                && rows.iter().all(|(id, _)| *id != contains_hidden))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(contains_rows.len(), 1);

    let in_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_select_in_list").build(),
        "in-list exposes rows whose status is one of the allowed values",
        |rows| {
            (rows.len() == 2
                && rows.iter().any(|(id, values)| {
                    *id == in_active && *values == status_values("Active", "active", false)
                })
                && rows.iter().any(|(id, values)| {
                    *id == in_trial && *values == status_values("Trial", "trial", false)
                })
                && rows.iter().all(|(id, _)| *id != in_hidden))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(in_rows.len(), 2);

    let empty_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_select_empty_in_list").build(),
        "empty in-list fails closed",
        Some,
    )
    .await;
    assert!(empty_rows.is_empty());
    assert!(
        empty_rows.iter().all(|(id, _)| *id != empty_hidden),
        "empty in-list must not expose any row"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that read and write policies remain independent:
/// readable rows can still reject writes, and writable rows can remain hidden.
///
/// Actors: alice performs the allowed write, bob reads and attempts the
/// rejected write, and admin verifies the persisted state.
///
/// ```text
/// bob ──query read_only──────────────► sees row
/// bob ──update read_only─────────────► server rejects, row stays original
///
/// bob ──query write_only─────────────► sees nothing
/// alice ──update hidden write_only──► server accepts
/// admin ──query write_only───────────► sees persisted update
/// bob ──query write_only─────────────► sees row once it satisfies SELECT
/// ```
#[tokio::test]
async fn read_and_write_policies_remain_independent() {
    let schema = SchemaBuilder::new()
        .table(make_documents_schema(
            "documents_read_only",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::True)
                .with_update(Some(PolicyExpr::False), PolicyExpr::False),
        ))
        .table(make_documents_schema(
            "documents_write_only",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::eq_literal("archived", false.into()))
                .with_update(Some(PolicyExpr::True), PolicyExpr::True),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(
        &server,
        &schema,
        "admin",
        "documents_read_only",
        READY_TIMEOUT,
    )
    .await;
    let alice = connect_ready_user(
        &server,
        &schema,
        "alice",
        "documents_read_only",
        READY_TIMEOUT,
    )
    .await;
    let bob = connect_ready_user(
        &server,
        &schema,
        "bob",
        "documents_read_only",
        READY_TIMEOUT,
    )
    .await;

    let read_only_id =
        seed_document(&admin, "documents_read_only", "owner", "original", false).await;
    let write_only_id =
        seed_document(&alice, "documents_write_only", "alice", "hidden", true).await;
    let read_only_values = boolean_policy_document_values("owner", "original", false);
    let revealed_write_only_values = boolean_policy_document_values("alice", "hidden", false);

    let read_only_rows = wait_for_rows(
        &bob,
        QueryBuilder::new("documents_read_only").build(),
        "read-only row is visible",
        |rows| has_row(&rows, read_only_id, &read_only_values).then_some(rows),
    )
    .await;
    assert_eq!(read_only_rows.len(), 1);

    update_document_title(&bob, read_only_id, "blocked").await;
    let read_only_after = wait_for_rows(
        &admin,
        QueryBuilder::new("documents_read_only").build(),
        "read access does not imply write access",
        |rows| has_row(&rows, read_only_id, &read_only_values).then_some(rows),
    )
    .await;
    assert!(has_row(&read_only_after, read_only_id, &read_only_values));

    let alice_hidden_reader = connect_ready_user(
        &server,
        &schema,
        "alice",
        "documents_write_only",
        READY_TIMEOUT,
    )
    .await;
    let write_only_before = wait_for_query(
        &alice_hidden_reader,
        QueryBuilder::new("documents_write_only").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "write-only row stays hidden before reveal",
        Some,
    )
    .await;
    assert!(write_only_before.is_empty());
    alice_hidden_reader
        .shutdown()
        .await
        .expect("shutdown alice_hidden_reader");

    update_document_archived(&alice, write_only_id, false).await;
    let alice_visible_reader = connect_ready_user(
        &server,
        &schema,
        "alice",
        "documents_write_only",
        READY_TIMEOUT,
    )
    .await;
    let alice_rows = wait_for_rows(
        &alice_visible_reader,
        QueryBuilder::new("documents_write_only").build(),
        "same session can reveal a row it was allowed to write before it could read",
        |rows| has_row(&rows, write_only_id, &revealed_write_only_values).then_some(rows),
    )
    .await;
    assert!(has_row(
        &alice_rows,
        write_only_id,
        &revealed_write_only_values,
    ));
    alice_visible_reader
        .shutdown()
        .await
        .expect("shutdown alice_visible_reader");

    let write_only_after = wait_for_query(
        &bob,
        QueryBuilder::new("documents_write_only").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "row becomes readable once the update makes it satisfy SELECT",
        Some,
    )
    .await;
    assert!(has_row(
        &write_only_after,
        write_only_id,
        &revealed_write_only_values,
    ));

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies the subscription-side visibility lifecycle for a simple
/// `SELECT archived = false` policy.
///
/// Actors: alice mutates rows, observer holds the live subscription, and fresh
/// verifier clients query EdgeServer state after each step.
///
/// ```text
/// alice ──insert archived=false──► observer stream (add ✓)
/// alice ──insert archived=true───► observer stream (no delta)
/// alice ──update title───────────► observer stream (update ✓)
/// alice ──update false→true──────► observer stream (remove ✓)
/// alice ──update true→false──────► observer stream (add ✓)
/// ```
#[tokio::test]
async fn authorized_mutations_emit_visibility_scoped_subscription_deltas() {
    let table_name = "documents_visibility_deltas";
    let schema = SchemaBuilder::new()
        .table(make_documents_schema(
            table_name,
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::eq_literal("archived", false.into()))
                .with_update(Some(PolicyExpr::True), PolicyExpr::True),
        ))
        .build();
    let verifier_schema = schema.clone();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let alice = connect_ready_user(&server, &schema, "alice", table_name, READY_TIMEOUT).await;
    let observer =
        connect_ready_user(&server, &schema, "observer", table_name, READY_TIMEOUT).await;
    let query = QueryBuilder::new(table_name).build();
    let visible_values = boolean_policy_document_values("alice", "visible", false);
    let renamed_visible_values = boolean_policy_document_values("alice", "visible renamed", false);
    let revealed_hidden_values = boolean_policy_document_values("alice", "hidden", false);

    let mut observer_stream = observer
        .subscribe(query.clone())
        .await
        .expect("subscribe observer");
    let mut observer_log = Vec::new();

    let visible_id = seed_document(&alice, table_name, "alice", "visible", false).await;
    wait_for_subscription_update(
        &mut observer_stream,
        &mut observer_log,
        QUERY_TIMEOUT,
        "observer sees add delta for visible insert",
        |log| has_added(log, visible_id),
    )
    .await;

    let hidden_id = seed_document(&alice, table_name, "alice", "hidden", true).await;
    let verifier_after_hidden_insert = connect_ready_user(
        &server,
        &verifier_schema,
        "verifier-hidden",
        table_name,
        READY_TIMEOUT,
    )
    .await;
    let rows_after_hidden_insert = verifier_after_hidden_insert
        .query(query.clone(), Some(DurabilityTier::EdgeServer))
        .await
        .expect("EdgeServer query after hidden insert");
    assert!(
        has_row(&rows_after_hidden_insert, visible_id, &visible_values),
        "visible insert should still be readable: rows={rows_after_hidden_insert:?}"
    );
    assert!(
        lacks_row(&rows_after_hidden_insert, hidden_id),
        "authorized insert that fails SELECT must stay hidden: rows={rows_after_hidden_insert:?}"
    );
    collect_stream_deltas(&mut observer_stream, &mut observer_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_any_change(&observer_log, hidden_id),
        "authorized hidden insert must not broadcast a delta: log={observer_log:?}"
    );
    verifier_after_hidden_insert
        .shutdown()
        .await
        .expect("shutdown verifier_after_hidden_insert");

    observer_log.clear();
    update_document_title(&alice, visible_id, "visible renamed").await;
    let verifier_after_visible_update = connect_ready_user(
        &server,
        &verifier_schema,
        "verifier-updated",
        table_name,
        READY_TIMEOUT,
    )
    .await;
    let rows_after_visible_update = wait_for_rows(
        &verifier_after_visible_update,
        query.clone(),
        "EdgeServer query after visible update",
        |rows| has_row(&rows, visible_id, &renamed_visible_values).then_some(rows),
    )
    .await;
    assert!(has_row(
        &rows_after_visible_update,
        visible_id,
        &renamed_visible_values,
    ));
    collect_stream_deltas(&mut observer_stream, &mut observer_log, NO_DELTA_WINDOW).await;
    assert!(
        has_updated(&observer_log, visible_id),
        "visible-to-visible updates must broadcast an update delta: log={observer_log:?}"
    );
    assert!(
        !has_added(&observer_log, visible_id) && !has_removed(&observer_log, visible_id),
        "visible-to-visible updates must stay updates: log={observer_log:?}"
    );
    verifier_after_visible_update
        .shutdown()
        .await
        .expect("shutdown verifier_after_visible_update");

    collect_stream_deltas(
        &mut observer_stream,
        &mut observer_log,
        Duration::from_millis(250),
    )
    .await;
    observer_log.clear();
    update_document_archived(&alice, visible_id, true).await;
    let verifier_after_hide = connect_ready_user(
        &server,
        &verifier_schema,
        "verifier-hide",
        table_name,
        READY_TIMEOUT,
    )
    .await;
    let rows_after_hide = wait_for_rows(
        &verifier_after_hide,
        query.clone(),
        "EdgeServer query after hiding row",
        |rows| lacks_row(&rows, visible_id).then_some(rows),
    )
    .await;
    assert!(lacks_row(&rows_after_hide, visible_id));
    wait_for_subscription_update(
        &mut observer_stream,
        &mut observer_log,
        QUERY_TIMEOUT,
        "visible-to-hidden scope shrink does not emit ObjectOutOfScope/remove deltas, so observer receives remove delta after row becomes hidden",
        |log| has_removed(log, visible_id),
    )
    .await;
    assert!(
        has_removed(&observer_log, visible_id),
        "visible-to-hidden updates must broadcast a remove delta: log={observer_log:?}"
    );
    assert!(
        !has_added(&observer_log, visible_id),
        "visible-to-hidden updates must not surface as add: log={observer_log:?}"
    );
    verifier_after_hide
        .shutdown()
        .await
        .expect("shutdown verifier_after_hide");

    observer_log.clear();
    update_document_archived(&alice, hidden_id, false).await;
    let verifier_after_reveal = connect_ready_user(
        &server,
        &verifier_schema,
        "verifier-reveal",
        table_name,
        READY_TIMEOUT,
    )
    .await;
    let rows_after_reveal = wait_for_rows(
        &verifier_after_reveal,
        query,
        "EdgeServer query after revealing row",
        |rows| has_row(&rows, hidden_id, &revealed_hidden_values).then_some(rows),
    )
    .await;
    assert!(has_row(
        &rows_after_reveal,
        hidden_id,
        &revealed_hidden_values,
    ));
    wait_for_subscription_update(
        &mut observer_stream,
        &mut observer_log,
        QUERY_TIMEOUT,
        "observer receives add delta after row becomes visible",
        |log| has_added(log, hidden_id),
    )
    .await;
    assert!(
        has_added(&observer_log, hidden_id),
        "hidden-to-visible updates must broadcast an add delta: log={observer_log:?}"
    );
    assert!(
        !has_updated(&observer_log, hidden_id) && !has_removed(&observer_log, hidden_id),
        "hidden-to-visible updates must surface as add only: log={observer_log:?}"
    );
    verifier_after_reveal
        .shutdown()
        .await
        .expect("shutdown verifier_after_reveal");

    alice.shutdown().await.expect("shutdown alice");
    observer.shutdown().await.expect("shutdown observer");
    server.shutdown().await;
}
