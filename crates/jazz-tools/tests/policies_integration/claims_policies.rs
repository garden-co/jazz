use std::collections::HashMap;
use std::time::Duration;

use super::support::{
    TestingClient, collect_stream_deltas, has_added, has_any_change, has_removed, has_updated,
    wait_for_query, wait_for_rows, wait_for_subscription_update,
};
use jazz_tools::query_manager::policy::{CmpOp, PolicyExpr};
use jazz_tools::query_manager::types::{TablePolicies, TableSchemaBuilder};
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, QueryBuilder, SchemaBuilder, TableSchema,
    Value,
};
use serde_json::json;

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
const NO_DELTA_WINDOW: Duration = Duration::from_millis(100);

// -- Schema builders --

fn make_title_documents_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("title", ColumnType::Text)
        .policies(policies)
}

fn make_group_documents_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("group_slug", ColumnType::Text)
        .column("title", ColumnType::Text)
        .policies(policies)
}

fn make_claim_compound_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("group_slug", ColumnType::Text)
        .column("published", ColumnType::Boolean)
        .column("title", ColumnType::Text)
        .policies(policies)
}

// -- Value constructors --

fn title_document_values(title: &str) -> Vec<Value> {
    vec![Value::Text(title.to_string())]
}

fn title_document_input(title: &str) -> HashMap<String, Value> {
    HashMap::from([("title".to_string(), Value::Text(title.to_string()))])
}

fn group_document_values(group_slug: &str, title: &str) -> Vec<Value> {
    vec![
        Value::Text(group_slug.to_string()),
        Value::Text(title.to_string()),
    ]
}

fn group_document_input(group_slug: &str, title: &str) -> HashMap<String, Value> {
    HashMap::from([
        (
            "group_slug".to_string(),
            Value::Text(group_slug.to_string()),
        ),
        ("title".to_string(), Value::Text(title.to_string())),
    ])
}

fn claim_compound_values(group_slug: &str, published: bool, title: &str) -> Vec<Value> {
    vec![
        Value::Text(group_slug.to_string()),
        Value::Boolean(published),
        Value::Text(title.to_string()),
    ]
}

fn claim_compound_input(group_slug: &str, published: bool, title: &str) -> HashMap<String, Value> {
    HashMap::from([
        (
            "group_slug".to_string(),
            Value::Text(group_slug.to_string()),
        ),
        ("published".to_string(), Value::Boolean(published)),
        ("title".to_string(), Value::Text(title.to_string())),
    ])
}

// -- Seed helpers --

async fn create_title_document(client: &JazzClient, table_name: &str, title: &str) -> ObjectId {
    client
        .create(table_name, title_document_input(title))
        .await
        .expect("create title document")
        .0
}

async fn create_group_document(
    client: &JazzClient,
    table_name: &str,
    group_slug: &str,
    title: &str,
) -> ObjectId {
    client
        .create(table_name, group_document_input(group_slug, title))
        .await
        .expect("create group document")
        .0
}

async fn create_claim_compound_document(
    client: &JazzClient,
    table_name: &str,
    group_slug: &str,
    published: bool,
    title: &str,
) -> ObjectId {
    client
        .create(
            table_name,
            claim_compound_input(group_slug, published, title),
        )
        .await
        .expect("create claim compound document")
        .0
}

// -- Tests --

/// Verifies that claim-gated write policies allow an admin session to mutate
/// rows and that a non-admin member can still read those rows via SELECT.
///
/// ```text
/// admin(role=ADMIN) ──insert/update/delete──► server
/// member(role=MEMBER) ──query/stream────────► sees add, update, remove
/// ```
#[tokio::test]
async fn admin_role_claims_allow_admin_mutations_and_member_reads() {
    let table_name = "documents";
    let admin_policy = PolicyExpr::SessionCmp {
        path: vec!["claims".into(), "role".into()],
        op: CmpOp::Eq,
        value: Value::Text("ADMIN".into()),
    };
    let schema = SchemaBuilder::new()
        .table(make_title_documents_schema(
            table_name,
            TablePolicies::new()
                .with_select(PolicyExpr::True)
                .with_insert(admin_policy.clone())
                .with_update(Some(admin_policy.clone()), admin_policy.clone())
                .with_delete(admin_policy),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin-user")
        .with_claims(json!({ "role": "ADMIN" }))
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let member = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("member-user")
        .with_claims(json!({ "role": "MEMBER" }))
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let query = QueryBuilder::new(table_name).build();

    let mut member_stream = member
        .subscribe(query.clone())
        .await
        .expect("subscribe member");
    let mut member_log = Vec::new();

    let admin_doc = create_title_document(&admin, table_name, "admin created").await;

    wait_for_subscription_update(
        &mut member_stream,
        &mut member_log,
        QUERY_TIMEOUT,
        "member sees admin-created document",
        |log| has_added(log, admin_doc),
    )
    .await;

    let rows = wait_for_rows(
        &member,
        query.clone(),
        "member sees only the admin-created document",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == admin_doc
                && rows[0].1 == title_document_values("admin created"))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows[0].1, title_document_values("admin created"));

    admin
        .update(
            admin_doc,
            vec![(
                "title".to_string(),
                Value::Text("admin updated".to_string()),
            )],
        )
        .await
        .expect("admin update");

    wait_for_subscription_update(
        &mut member_stream,
        &mut member_log,
        QUERY_TIMEOUT,
        "member sees admin update",
        |log| has_updated(log, admin_doc),
    )
    .await;

    let rows_after_admin_update = wait_for_rows(
        &member,
        query.clone(),
        "member sees admin-updated document",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == admin_doc && *values == title_document_values("admin updated")
                })
                .then_some(rows)
        },
    )
    .await;
    assert!(rows_after_admin_update.iter().any(|(id, values)| {
        *id == admin_doc && *values == title_document_values("admin updated")
    }));

    admin.delete(admin_doc).await.expect("admin delete");
    wait_for_subscription_update(
        &mut member_stream,
        &mut member_log,
        QUERY_TIMEOUT,
        "member sees admin delete",
        |log| has_removed(log, admin_doc),
    )
    .await;

    let final_rows = wait_for_rows(
        &member,
        query,
        "member sees no rows after admin delete",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(final_rows.is_empty());

    admin.shutdown().await.expect("shutdown admin");
    member.shutdown().await.expect("shutdown member");
    server.shutdown().await;
}

/// Verifies that a non-admin member cannot mutate rows protected by
/// `claims.role = 'ADMIN'`, even though those rows remain readable.
///
/// ```text
/// admin(role=ADMIN) ──create baseline + barrier──► observer sees both
/// member(role=MEMBER) ──insert/update/delete─────► server ──✗ rejected
/// observer/member ───────────────────────────────► row stays readable and unchanged
/// ```
#[tokio::test]
async fn admin_role_claims_reject_member_mutations() {
    let table_name = "documents";
    let admin_policy = PolicyExpr::SessionCmp {
        path: vec!["claims".into(), "role".into()],
        op: CmpOp::Eq,
        value: Value::Text("ADMIN".into()),
    };
    let schema = SchemaBuilder::new()
        .table(make_title_documents_schema(
            table_name,
            TablePolicies::new()
                .with_select(PolicyExpr::True)
                .with_insert(admin_policy.clone())
                .with_update(Some(admin_policy.clone()), admin_policy.clone())
                .with_delete(admin_policy),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin-user")
        .with_claims(json!({ "role": "ADMIN" }))
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let member = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("member-user")
        .with_claims(json!({ "role": "MEMBER" }))
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("observer")
        .as_user()
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let query = QueryBuilder::new(table_name).build();

    let mut observer_stream = observer
        .subscribe(query.clone())
        .await
        .expect("subscribe observer");
    let mut observer_log = Vec::new();

    let admin_doc = create_title_document(&admin, table_name, "admin created").await;
    wait_for_subscription_update(
        &mut observer_stream,
        &mut observer_log,
        QUERY_TIMEOUT,
        "observer sees admin-created document",
        |log| has_added(log, admin_doc),
    )
    .await;

    let rejected_insert = member
        .create(table_name, title_document_input("member create"))
        .await
        .expect("optimistic local member create")
        .0;
    let barrier_doc = create_title_document(&admin, table_name, "barrier").await;
    wait_for_subscription_update(
        &mut observer_stream,
        &mut observer_log,
        QUERY_TIMEOUT,
        "observer sees barrier document after rejected member insert",
        |log| has_added(log, barrier_doc),
    )
    .await;

    assert!(
        !has_any_change(&observer_log, rejected_insert),
        "member insert rejected by admin-only claim policy must not be broadcast"
    );

    let rows_after_rejected_insert = wait_for_rows(
        &observer,
        query.clone(),
        "observer sees only admin-authored documents after rejected member insert",
        |rows| {
            (rows.len() == 2
                && rows.iter().any(|(id, _)| *id == admin_doc)
                && rows.iter().any(|(id, _)| *id == barrier_doc)
                && rows.iter().all(|(id, _)| *id != rejected_insert))
            .then_some(rows)
        },
    )
    .await;
    assert!(
        rows_after_rejected_insert
            .iter()
            .all(|(id, _)| *id != rejected_insert),
        "observer must not see the rejected member insert"
    );

    wait_for_rows(
        &member,
        query.clone(),
        "member syncs readable admin rows before attempting rejected writes",
        |rows| rows.iter().any(|(id, _)| *id == admin_doc).then_some(()),
    )
    .await;

    member
        .update(
            admin_doc,
            vec![(
                "title".to_string(),
                Value::Text("member hacked".to_string()),
            )],
        )
        .await
        .expect("optimistic local member update");

    let rows_after_rejected_update = observer
        .query(query.clone(), Some(DurabilityTier::EdgeServer))
        .await
        .expect("EdgeServer query after rejected member update");
    assert!(
        rows_after_rejected_update.iter().any(|(id, values)| {
            *id == admin_doc && *values == title_document_values("admin created")
        }),
        "rejected member update must not persist at EdgeServer: rows={rows_after_rejected_update:?}"
    );
    collect_stream_deltas(&mut observer_stream, &mut observer_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_updated(&observer_log, admin_doc),
        "rejected member update must not be broadcast: log={observer_log:?}"
    );

    member
        .delete(admin_doc)
        .await
        .expect("optimistic local member delete");

    let rows_after_rejected_delete = observer
        .query(query, Some(DurabilityTier::EdgeServer))
        .await
        .expect("EdgeServer query after rejected member delete");
    assert!(
        rows_after_rejected_delete.iter().any(|(id, values)| {
            *id == admin_doc && *values == title_document_values("admin created")
        }),
        "rejected member delete must leave the row intact: rows={rows_after_rejected_delete:?}"
    );
    collect_stream_deltas(&mut observer_stream, &mut observer_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_removed(&observer_log, admin_doc),
        "rejected member delete must not be broadcast: log={observer_log:?}"
    );

    admin.shutdown().await.expect("shutdown admin");
    member.shutdown().await.expect("shutdown member");
    observer.shutdown().await.expect("shutdown observer");
    server.shutdown().await;
}

/// Verifies that `claims.role IS NOT NULL` gates row visibility by the
/// presence of a non-null claim rather than a specific role value.
///
/// ```text
/// role="ADMIN"  ──query──► all rows
/// role="VIEWER" ──query──► all rows
/// role=null      ──query──► {}
/// role missing   ──query──► {}
/// ```
#[tokio::test]
async fn role_claim_presence_gates_row_visibility() {
    let table_name = "documents";
    let schema = SchemaBuilder::new()
        .table(make_title_documents_schema(
            table_name,
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::SessionIsNotNull {
                    path: vec!["claims".into(), "role".into()],
                }),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let seeder = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("seed-admin")
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .with_claims(json!({ "role": "ADMIN" }))
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let viewer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob")
        .with_claims(json!({ "role": "VIEWER" }))
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let null_role = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("carol")
        .with_claims(json!({ "role": null }))
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let missing_role = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("dave")
        .with_claims(json!({}))
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;

    let first_doc = create_title_document(&seeder, table_name, "alpha").await;
    let second_doc = create_title_document(&seeder, table_name, "beta").await;
    let query = QueryBuilder::new(table_name).build();

    let admin_rows = wait_for_rows(
        &admin,
        query.clone(),
        "admin sees rows when claims.role is present",
        |rows| {
            (rows.len() == 2
                && rows.iter().any(|(id, _)| *id == first_doc)
                && rows.iter().any(|(id, _)| *id == second_doc))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(admin_rows.len(), 2);

    let viewer_rows = wait_for_rows(
        &viewer,
        query.clone(),
        "viewer sees rows when claims.role is present",
        |rows| {
            (rows.len() == 2
                && rows.iter().any(|(id, _)| *id == first_doc)
                && rows.iter().any(|(id, _)| *id == second_doc))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(viewer_rows.len(), 2);

    let null_role_rows = wait_for_query(
        &null_role,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "explicit null role sees nothing",
        Some,
    )
    .await;
    assert!(null_role_rows.is_empty());

    let missing_role_rows = wait_for_query(
        &missing_role,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "missing role sees nothing",
        Some,
    )
    .await;
    assert!(missing_role_rows.is_empty());

    seeder.shutdown().await.expect("shutdown seeder");
    admin.shutdown().await.expect("shutdown admin");
    viewer.shutdown().await.expect("shutdown viewer");
    null_role.shutdown().await.expect("shutdown null_role");
    missing_role
        .shutdown()
        .await
        .expect("shutdown missing_role");
    server.shutdown().await;
}

/// Verifies that `group_slug IN @session.claims.groups_allowed` scopes both
/// initial query results and live subscription deltas per session.
///
/// ```text
/// claims["eng"]         ──query/stream──► eng rows only
/// claims["sales"]       ──query/stream──► sales rows only
/// claims["eng","sales"] ──query/stream──► both
/// claims[] or missing   ──query/stream──► {}
/// ```
#[tokio::test]
async fn groups_allowed_claim_arrays_gate_visibility_and_live_updates() {
    let table_name = "group_documents";
    let schema = SchemaBuilder::new()
        .table(make_group_documents_schema(
            table_name,
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::in_session(
                    "group_slug",
                    vec!["claims".into(), "groups_allowed".into()],
                )),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("seed-admin")
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;

    let eng_doc = create_group_document(&admin, table_name, "eng", "Eng Only").await;
    let sales_doc = create_group_document(&admin, table_name, "sales", "Sales Only").await;

    let eng = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("eng-user")
        .with_claims(json!({ "groups_allowed": ["eng"] }))
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let sales = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("sales-user")
        .with_claims(json!({ "groups_allowed": ["sales"] }))
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let both = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("both-user")
        .with_claims(json!({ "groups_allowed": ["eng", "sales"] }))
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let empty = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("empty-user")
        .with_claims(json!({ "groups_allowed": [] }))
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let missing = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("missing-user")
        .with_claims(json!({}))
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;

    let query = QueryBuilder::new(table_name).build();

    let eng_rows = wait_for_rows(&eng, query.clone(), "eng session sees eng rows", |rows| {
        (rows.len() == 1 && rows[0].0 == eng_doc).then_some(rows)
    })
    .await;
    assert_eq!(eng_rows[0].1, group_document_values("eng", "Eng Only"));
    assert!(
        eng_rows.iter().all(|(id, _)| *id != sales_doc),
        "eng session should not see sales rows"
    );

    let sales_rows = wait_for_rows(
        &sales,
        query.clone(),
        "sales session sees sales rows",
        |rows| (rows.len() == 1 && rows[0].0 == sales_doc).then_some(rows),
    )
    .await;
    assert_eq!(
        sales_rows[0].1,
        group_document_values("sales", "Sales Only")
    );
    assert!(
        sales_rows.iter().all(|(id, _)| *id != eng_doc),
        "sales session should not see eng rows"
    );

    let both_rows = wait_for_rows(
        &both,
        query.clone(),
        "multi-group session sees both rows",
        |rows| {
            (rows.len() == 2
                && rows.iter().any(|(id, _)| *id == eng_doc)
                && rows.iter().any(|(id, _)| *id == sales_doc))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(both_rows.len(), 2);

    let empty_rows = wait_for_query(
        &empty,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "empty groups_allowed denies all rows",
        Some,
    )
    .await;
    assert!(empty_rows.is_empty());

    let missing_rows = wait_for_query(
        &missing,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "missing groups_allowed claim denies all rows",
        Some,
    )
    .await;
    assert!(missing_rows.is_empty());

    let mut eng_stream = eng.subscribe(query.clone()).await.expect("subscribe eng");
    let mut sales_stream = sales
        .subscribe(query.clone())
        .await
        .expect("subscribe sales");
    let mut both_stream = both.subscribe(query.clone()).await.expect("subscribe both");
    let mut empty_stream = empty.subscribe(query).await.expect("subscribe empty");
    let mut eng_log = Vec::new();
    let mut sales_log = Vec::new();
    let mut both_log = Vec::new();
    let mut empty_log = Vec::new();

    let eng_follow_up = create_group_document(&admin, table_name, "eng", "Eng Follow-up").await;
    let sales_follow_up =
        create_group_document(&admin, table_name, "sales", "Sales Follow-up").await;

    wait_for_subscription_update(
        &mut both_stream,
        &mut both_log,
        QUERY_TIMEOUT,
        "multi-group session sees both follow-up rows",
        |log| has_added(log, eng_follow_up) && has_added(log, sales_follow_up),
    )
    .await;
    wait_for_subscription_update(
        &mut eng_stream,
        &mut eng_log,
        QUERY_TIMEOUT,
        "eng session sees only eng follow-up row",
        |log| has_added(log, eng_follow_up),
    )
    .await;
    wait_for_subscription_update(
        &mut sales_stream,
        &mut sales_log,
        QUERY_TIMEOUT,
        "sales session sees only sales follow-up row",
        |log| has_added(log, sales_follow_up),
    )
    .await;

    collect_stream_deltas(&mut eng_stream, &mut eng_log, NO_DELTA_WINDOW).await;
    collect_stream_deltas(&mut sales_stream, &mut sales_log, NO_DELTA_WINDOW).await;
    collect_stream_deltas(&mut both_stream, &mut both_log, NO_DELTA_WINDOW).await;
    collect_stream_deltas(&mut empty_stream, &mut empty_log, NO_DELTA_WINDOW).await;

    assert!(has_added(&eng_log, eng_follow_up));
    assert!(
        !has_any_change(&eng_log, sales_follow_up),
        "eng session must not receive sales deltas: log={eng_log:?}"
    );

    assert!(has_added(&sales_log, sales_follow_up));
    assert!(
        !has_any_change(&sales_log, eng_follow_up),
        "sales session must not receive eng deltas: log={sales_log:?}"
    );

    assert!(has_added(&both_log, eng_follow_up));
    assert!(has_added(&both_log, sales_follow_up));
    assert!(
        !has_any_change(&empty_log, eng_follow_up) && !has_any_change(&empty_log, sales_follow_up),
        "empty groups_allowed session must stay silent: log={empty_log:?}"
    );

    admin.shutdown().await.expect("shutdown admin");
    eng.shutdown().await.expect("shutdown eng");
    sales.shutdown().await.expect("shutdown sales");
    both.shutdown().await.expect("shutdown both");
    empty.shutdown().await.expect("shutdown empty");
    missing.shutdown().await.expect("shutdown missing");
    server.shutdown().await;
}

/// Verifies that explicit `null` claims and missing claim paths are treated
/// differently: `IS NULL` matches only explicit null, while `!= null` matches
/// only present non-null values.
///
/// ```text
/// claims.revoked_at = null      ──► matches IS NULL table only
/// claims.revoked_at = "..."     ──► matches != null table only
/// claims.revoked_at is missing  ──► matches neither table
/// ```
#[tokio::test]
async fn claim_null_checks_distinguish_explicit_null_from_missing_paths() {
    let null_table = "documents_null_claim";
    let present_table = "documents_present_claim";
    let schema = SchemaBuilder::new()
        .table(make_title_documents_schema(
            null_table,
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::SessionIsNull {
                    path: vec!["claims".into(), "revoked_at".into()],
                }),
        ))
        .table(make_title_documents_schema(
            present_table,
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::SessionCmp {
                    path: vec!["claims".into(), "revoked_at".into()],
                    op: CmpOp::Ne,
                    value: Value::Null,
                }),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let seeder = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("seed-admin")
        .ready_on(null_table, READY_TIMEOUT)
        .connect()
        .await;
    let explicit_null = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("null-user")
        .with_claims(json!({ "revoked_at": null }))
        .ready_on(null_table, READY_TIMEOUT)
        .connect()
        .await;
    let present_value = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("present-user")
        .with_claims(json!({ "revoked_at": "2026-03-26T00:00:00Z" }))
        .ready_on(null_table, READY_TIMEOUT)
        .connect()
        .await;
    let missing_path = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("missing-user")
        .with_claims(json!({}))
        .ready_on(null_table, READY_TIMEOUT)
        .connect()
        .await;

    let null_doc = create_title_document(&seeder, null_table, "explicit null only").await;
    let present_doc = create_title_document(&seeder, present_table, "present value only").await;

    let null_query = QueryBuilder::new(null_table).build();
    let present_query = QueryBuilder::new(present_table).build();

    let explicit_null_rows = wait_for_rows(
        &explicit_null,
        null_query.clone(),
        "explicit null claim matches IS NULL policy",
        |rows| (rows.len() == 1 && rows[0].0 == null_doc).then_some(rows),
    )
    .await;
    assert_eq!(
        explicit_null_rows[0].1,
        title_document_values("explicit null only")
    );

    let explicit_null_present_rows = wait_for_query(
        &explicit_null,
        present_query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "explicit null claim does not match != null policy",
        Some,
    )
    .await;
    assert!(explicit_null_present_rows.is_empty());

    let present_value_null_rows = wait_for_query(
        &present_value,
        null_query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "present claim does not match IS NULL policy",
        Some,
    )
    .await;
    assert!(present_value_null_rows.is_empty());

    let present_value_rows = wait_for_rows(
        &present_value,
        present_query.clone(),
        "present claim matches != null policy",
        |rows| (rows.len() == 1 && rows[0].0 == present_doc).then_some(rows),
    )
    .await;
    assert_eq!(
        present_value_rows[0].1,
        title_document_values("present value only")
    );

    let missing_null_rows = wait_for_query(
        &missing_path,
        null_query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "missing claim path does not match IS NULL policy",
        Some,
    )
    .await;
    assert!(missing_null_rows.is_empty());

    let missing_present_rows = wait_for_query(
        &missing_path,
        present_query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "missing claim path does not match != null policy",
        Some,
    )
    .await;
    assert!(missing_present_rows.is_empty());

    seeder.shutdown().await.expect("shutdown seeder");
    explicit_null
        .shutdown()
        .await
        .expect("shutdown explicit_null");
    present_value
        .shutdown()
        .await
        .expect("shutdown present_value");
    missing_path
        .shutdown()
        .await
        .expect("shutdown missing_path");
    server.shutdown().await;
}

/// Verifies that claim predicates combine correctly with row predicates under
/// both `allOf(...)` and `anyOf(...)`, including dotted claim paths.
///
/// ```text
/// allOf: group="eng" AND published=true AND org.slug="north" AND groups CONTAINS "eng"
/// anyOf: group="public" OR (group="eng" AND groups CONTAINS "eng")
/// ```
#[tokio::test]
async fn row_and_claim_predicates_compose_under_and_and_or() {
    let all_of_table = "documents_all_of";
    let any_of_table = "documents_any_of";
    let schema = SchemaBuilder::new()
        .table(make_claim_compound_schema(
            all_of_table,
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::and(vec![
                    PolicyExpr::eq_literal("group_slug", Value::Text("eng".into())),
                    PolicyExpr::eq_literal("published", Value::Boolean(true)),
                    PolicyExpr::SessionCmp {
                        path: vec!["claims".into(), "org".into(), "slug".into()],
                        op: CmpOp::Eq,
                        value: Value::Text("north".into()),
                    },
                    PolicyExpr::SessionContains {
                        path: vec!["claims".into(), "groups_allowed".into()],
                        value: Value::Text("eng".into()),
                    },
                ])),
        ))
        .table(make_group_documents_schema(
            any_of_table,
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::or(vec![
                    PolicyExpr::eq_literal("group_slug", Value::Text("public".into())),
                    PolicyExpr::and(vec![
                        PolicyExpr::eq_literal("group_slug", Value::Text("eng".into())),
                        PolicyExpr::SessionContains {
                            path: vec!["claims".into(), "groups_allowed".into()],
                            value: Value::Text("eng".into()),
                        },
                    ]),
                ])),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let seeder = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("seed-admin")
        .ready_on(all_of_table, READY_TIMEOUT)
        .connect()
        .await;
    let north_eng = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("north-eng")
        .with_claims(json!({
            "org": { "slug": "north" },
            "groups_allowed": ["eng"]
        }))
        .ready_on(all_of_table, READY_TIMEOUT)
        .connect()
        .await;
    let north_empty = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("north-empty")
        .with_claims(json!({
            "org": { "slug": "north" },
            "groups_allowed": []
        }))
        .ready_on(all_of_table, READY_TIMEOUT)
        .connect()
        .await;
    let south_eng = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("south-eng")
        .with_claims(json!({
            "org": { "slug": "south" },
            "groups_allowed": ["eng"]
        }))
        .ready_on(all_of_table, READY_TIMEOUT)
        .connect()
        .await;

    let all_of_match =
        create_claim_compound_document(&seeder, all_of_table, "eng", true, "North Eng Live").await;
    let _all_of_unpublished =
        create_claim_compound_document(&seeder, all_of_table, "eng", false, "Draft").await;
    let _all_of_wrong_group =
        create_claim_compound_document(&seeder, all_of_table, "sales", true, "Wrong Group").await;

    let any_of_public = create_group_document(&seeder, any_of_table, "public", "Public").await;
    let any_of_eng = create_group_document(&seeder, any_of_table, "eng", "Eng Only").await;
    let any_of_sales = create_group_document(&seeder, any_of_table, "sales", "Sales Only").await;

    let all_of_query = QueryBuilder::new(all_of_table).build();
    let any_of_query = QueryBuilder::new(any_of_table).build();

    let north_eng_all_of_rows = wait_for_rows(
        &north_eng,
        all_of_query.clone(),
        "north+eng client satisfies all allOf branches",
        |rows| (rows.len() == 1 && rows[0].0 == all_of_match).then_some(rows),
    )
    .await;
    assert_eq!(
        north_eng_all_of_rows[0].1,
        claim_compound_values("eng", true, "North Eng Live")
    );

    let north_empty_all_of_rows = wait_for_query(
        &north_empty,
        all_of_query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "north client without eng membership fails allOf",
        Some,
    )
    .await;
    assert!(north_empty_all_of_rows.is_empty());

    let south_eng_all_of_rows = wait_for_query(
        &south_eng,
        all_of_query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "south org client fails dotted org.slug branch",
        Some,
    )
    .await;
    assert!(south_eng_all_of_rows.is_empty());

    let north_eng_any_of_rows = wait_for_rows(
        &north_eng,
        any_of_query.clone(),
        "north+eng client satisfies public-or-eng anyOf",
        |rows| {
            (rows.len() == 2
                && rows.iter().any(|(id, _)| *id == any_of_public)
                && rows.iter().any(|(id, _)| *id == any_of_eng)
                && rows.iter().all(|(id, _)| *id != any_of_sales))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(north_eng_any_of_rows.len(), 2);

    let north_empty_any_of_rows = wait_for_rows(
        &north_empty,
        any_of_query.clone(),
        "north client without eng membership still sees public row",
        |rows| (rows.len() == 1 && rows[0].0 == any_of_public).then_some(rows),
    )
    .await;
    assert_eq!(
        north_empty_any_of_rows[0].1,
        group_document_values("public", "Public")
    );

    let south_eng_any_of_rows = wait_for_rows(
        &south_eng,
        any_of_query,
        "south org client still sees rows allowed by eng anyOf branch",
        |rows| {
            (rows.len() == 2
                && rows.iter().any(|(id, _)| *id == any_of_public)
                && rows.iter().any(|(id, _)| *id == any_of_eng)
                && rows.iter().all(|(id, _)| *id != any_of_sales))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(south_eng_any_of_rows.len(), 2);

    seeder.shutdown().await.expect("shutdown seeder");
    north_eng.shutdown().await.expect("shutdown north_eng");
    north_empty.shutdown().await.expect("shutdown north_empty");
    south_eng.shutdown().await.expect("shutdown south_eng");
    server.shutdown().await;
}
