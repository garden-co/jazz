#![cfg(feature = "test")]

//! E2E catalogue sync integration test.
//!
//! Verifies that schema+lens catalogue objects propagate through the full
//! SyncManager pipeline (not via direct `process_catalogue_update()` calls).

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::query_manager::policy::PolicyExpr;
use jazz_tools::query_manager::types::SchemaHash;
use jazz_tools::query_manager::types::TablePolicies;
use jazz_tools::row_input;
use jazz_tools::schema_manager::{Lens, LensOp, LensTransform, generate_lens};
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, SchemaBuilder, TableSchema, Value,
};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;
use support::{
    PublishedPermissionsHead, TestingClient, deny_all_select_permissions, has_added, has_removed,
    publish_allow_all_permissions, publish_permissions, push_catalogue_in_memory,
    wait_for_edge_query_ready, wait_for_query, wait_for_subscription_update,
};

fn user_values_v1(id: jazz_tools::ObjectId, name: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "name" => name)
}

fn user_values_v2(id: jazz_tools::ObjectId, name: &str, email: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "name" => name, "email" => email)
}

fn user_values_v3(
    id: jazz_tools::ObjectId,
    name: &str,
    email: &str,
    role: &str,
) -> HashMap<String, Value> {
    row_input!("id" => id, "name" => name, "email" => email, "role" => role)
}

fn draft_lens_values_v1(id: jazz_tools::ObjectId) -> HashMap<String, Value> {
    row_input!("id" => id)
}

fn schema_v1() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build()
}

fn schema_v2() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build()
}

fn schema_v3() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text)
                .nullable_column("role", ColumnType::Text),
        )
        .build()
}

fn v1_to_v2_lens() -> Lens {
    generate_lens(&schema_v1(), &schema_v2())
}

fn v2_to_v3_lens() -> Lens {
    generate_lens(&schema_v2(), &schema_v3())
}

fn draft_lens_schema_v1() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
        .build()
}

fn draft_lens_schema_v2() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("org_id", ColumnType::Uuid),
        )
        .build()
}

fn draft_lens_v1_to_v2() -> Lens {
    generate_lens(&draft_lens_schema_v1(), &draft_lens_schema_v2())
}

fn rename_chain_values_v1(id: jazz_tools::ObjectId, email: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "email" => email)
}

fn rename_chain_values_v3(id: jazz_tools::ObjectId, contact_email: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "contact_email" => contact_email)
}

fn rename_chain_schema_v1() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build()
}

fn rename_chain_schema_v2() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email_address", ColumnType::Text),
        )
        .build()
}

fn rename_chain_schema_v3() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("contact_email", ColumnType::Text),
        )
        .build()
}

fn rename_chain_v1_to_v2_lens() -> Lens {
    Lens::new(
        SchemaHash::compute(&rename_chain_schema_v1()),
        SchemaHash::compute(&rename_chain_schema_v2()),
        LensTransform::with_ops(vec![LensOp::RenameColumn {
            table: "users".to_string(),
            old_name: "email".to_string(),
            new_name: "email_address".to_string(),
        }]),
    )
}

fn rename_chain_v2_to_v3_lens() -> Lens {
    Lens::new(
        SchemaHash::compute(&rename_chain_schema_v2()),
        SchemaHash::compute(&rename_chain_schema_v3()),
        LensTransform::with_ops(vec![LensOp::RenameColumn {
            table: "users".to_string(),
            old_name: "email_address".to_string(),
            new_name: "contact_email".to_string(),
        }]),
    )
}

fn table_rename_values_v1(id: jazz_tools::ObjectId, email: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "email" => email)
}

fn table_rename_values_v2(id: jazz_tools::ObjectId, email: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "email" => email)
}

fn table_rename_schema_v1() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build()
}

fn table_rename_schema_v2() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("people")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build()
}

fn table_rename_v1_to_v2_lens() -> Lens {
    Lens::new(
        SchemaHash::compute(&table_rename_schema_v1()),
        SchemaHash::compute(&table_rename_schema_v2()),
        LensTransform::with_ops(vec![LensOp::RenameTable {
            old_name: "users".to_string(),
            new_name: "people".to_string(),
        }]),
    )
}

fn table_rename_join_user_values(id: jazz_tools::ObjectId, name: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "name" => name)
}

fn table_rename_join_post_values(
    id: jazz_tools::ObjectId,
    author_id: jazz_tools::ObjectId,
    title: &str,
) -> HashMap<String, Value> {
    row_input!("id" => id, "author_id" => author_id, "title" => title)
}

fn table_rename_join_schema_v1() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .table(
            TableSchema::builder("posts")
                .column("id", ColumnType::Uuid)
                .fk_column("author_id", "users")
                .column("title", ColumnType::Text),
        )
        .build()
}

fn table_rename_join_schema_v2() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("people")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .table(
            TableSchema::builder("posts")
                .column("id", ColumnType::Uuid)
                .fk_column("author_id", "people")
                .column("title", ColumnType::Text),
        )
        .build()
}

fn table_rename_join_v1_to_v2_lens() -> Lens {
    Lens::new(
        SchemaHash::compute(&table_rename_join_schema_v1()),
        SchemaHash::compute(&table_rename_join_schema_v2()),
        LensTransform::with_ops(vec![LensOp::RenameTable {
            old_name: "users".to_string(),
            new_name: "people".to_string(),
        }]),
    )
}

fn legacy_join_provenance_user_values(name: &str) -> HashMap<String, Value> {
    row_input!("name" => name)
}

fn legacy_join_provenance_post_values(owner_name: &str, title: &str) -> HashMap<String, Value> {
    row_input!("owner_name" => owner_name, "title" => title)
}

fn legacy_join_provenance_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("users").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("posts")
                .column("owner_name", ColumnType::Text)
                .column("title", ColumnType::Text),
        )
        .build()
}

fn current_join_provenance_permission_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_insert(PolicyExpr::True)
                        .with_select(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("posts")
                .column("owner_name", ColumnType::Text)
                .column("title", ColumnType::Text)
                .column("viewer_name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_insert(PolicyExpr::True)
                        .with_select(PolicyExpr::eq_session(
                            "viewer_name",
                            vec!["user_id".into()],
                        )),
                ),
        )
        .build()
}

fn legacy_join_provenance_to_current_permissions_lens() -> Lens {
    Lens::new(
        SchemaHash::compute(&legacy_join_provenance_schema()),
        SchemaHash::compute(&current_join_provenance_permission_schema()),
        LensTransform::with_ops(vec![LensOp::AddColumn {
            table: "posts".to_string(),
            column: "viewer_name".to_string(),
            column_type: ColumnType::Text,
            default: Value::Text("bob".to_string()),
        }]),
    )
}

fn multi_hop_table_rename_values_v1(
    id: jazz_tools::ObjectId,
    email: &str,
) -> HashMap<String, Value> {
    row_input!("id" => id, "email" => email)
}

fn multi_hop_table_rename_values_v2(
    id: jazz_tools::ObjectId,
    email: &str,
) -> HashMap<String, Value> {
    row_input!("id" => id, "email" => email)
}

fn multi_hop_table_rename_values_v3(
    id: jazz_tools::ObjectId,
    email_address: &str,
) -> HashMap<String, Value> {
    row_input!("id" => id, "email_address" => email_address)
}

fn multi_hop_table_rename_schema_v1() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build()
}

fn multi_hop_table_rename_schema_v2() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("people")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build()
}

fn multi_hop_table_rename_schema_v3() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("members")
                .column("id", ColumnType::Uuid)
                .column("email_address", ColumnType::Text),
        )
        .build()
}

fn multi_hop_table_rename_v1_to_v2_lens() -> Lens {
    Lens::new(
        SchemaHash::compute(&multi_hop_table_rename_schema_v1()),
        SchemaHash::compute(&multi_hop_table_rename_schema_v2()),
        LensTransform::with_ops(vec![LensOp::RenameTable {
            old_name: "users".to_string(),
            new_name: "people".to_string(),
        }]),
    )
}

fn multi_hop_table_rename_v2_to_v3_lens() -> Lens {
    Lens::new(
        SchemaHash::compute(&multi_hop_table_rename_schema_v2()),
        SchemaHash::compute(&multi_hop_table_rename_schema_v3()),
        LensTransform::with_ops(vec![
            LensOp::RenameTable {
                old_name: "people".to_string(),
                new_name: "members".to_string(),
            },
            LensOp::RenameColumn {
                table: "members".to_string(),
                old_name: "email".to_string(),
                new_name: "email_address".to_string(),
            },
        ]),
    )
}

fn removed_readded_values_v1(id: jazz_tools::ObjectId, name: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "name" => name)
}

fn removed_readded_values_v3(
    id: jazz_tools::ObjectId,
    name: &str,
    email: &str,
) -> HashMap<String, Value> {
    row_input!("id" => id, "name" => name, "email" => email)
}

fn removed_readded_schema_v1() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build()
}

fn removed_readded_schema_v2() -> jazz_tools::Schema {
    SchemaBuilder::new().build()
}

fn removed_readded_schema_v3() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build()
}

fn removed_readded_v1_to_v2_lens() -> Lens {
    generate_lens(&removed_readded_schema_v1(), &removed_readded_schema_v2())
}

fn removed_readded_v2_to_v3_lens() -> Lens {
    generate_lens(&removed_readded_schema_v2(), &removed_readded_schema_v3())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishSchemaHttpResponse {
    hash: String,
}

#[derive(Debug, Deserialize)]
struct SchemaHashesHttpResponse {
    hashes: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishMigrationHttpResponse {
    from_hash: String,
    to_hash: String,
}

#[derive(Debug, Deserialize)]
struct SchemaConnectivityHttpResponse {
    connected: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredSchemaHttpResponse {
    schema: jazz_tools::Schema,
}

#[derive(Debug, Deserialize)]
struct PermissionsHeadHttpResponse {
    head: Option<PublishedPermissionsHead>,
}

async fn seed_schema_catalogue(server: &TestingServer, schema: &jazz_tools::Schema) {
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        std::slice::from_ref(schema),
        &[],
    )
    .await
    .expect("push schema catalogue");
}

async fn assert_edge_query_does_not_include_row(
    client: &JazzClient,
    query: jazz_tools::Query,
    row_id: jazz_tools::ObjectId,
    timeout: Duration,
    description: &str,
) {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if let Ok(Ok(rows)) = tokio::time::timeout(
            Duration::from_millis(500),
            client.query(query.clone(), Some(DurabilityTier::EdgeServer)),
        )
        .await
        {
            assert!(
                rows.iter().all(|(id, _)| *id != row_id),
                "{description}: query unexpectedly included row {row_id}; rows: {rows:?}"
            );
        }

        if tokio::time::Instant::now() >= deadline {
            return;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// Test topology:
//
//   admin HTTP client
//          |
//          | publish/read catalogue over HTTP
//          v
//   edge TestingServer
//          |
//          | forwards after local admin-secret validation
//          v
//   core TestingServer
//
// The assertions verify that writes sent to the edge are persisted by the real
// core, and reads sent to the edge return the core catalogue state.
#[tokio::test]
async fn edge_catalogue_http_reads_and_writes_forward_to_real_core() {
    let app_id = TestingServer::default_app_id();
    let core = TestingServer::builder().with_app_id(app_id).start().await;
    let edge = TestingServer::builder()
        .with_app_id(app_id)
        .with_upstream_url(core.base_url())
        .start()
        .await;
    let schema = schema_v1();
    let schema_hash = SchemaHash::compute(&schema).to_string();
    let client = reqwest::Client::new();

    let publish_schema_response = client
        .post(format!("{}/apps/{app_id}/admin/schemas", edge.base_url()))
        .header("X-Jazz-Admin-Secret", edge.admin_secret())
        .json(&json!({ "schema": schema }))
        .send()
        .await
        .expect("publish schema through edge");
    assert_eq!(publish_schema_response.status(), StatusCode::CREATED);
    let published_schema: PublishSchemaHttpResponse = publish_schema_response
        .json()
        .await
        .expect("decode edge schema publish response");
    assert_eq!(published_schema.hash, schema_hash);

    let core_schema_response = client
        .get(format!(
            "{}/apps/{app_id}/schema/{schema_hash}",
            core.base_url()
        ))
        .header("X-Jazz-Admin-Secret", core.admin_secret())
        .send()
        .await
        .expect("fetch schema from core");
    assert_eq!(core_schema_response.status(), StatusCode::OK);
    let core_schema: StoredSchemaHttpResponse = core_schema_response
        .json()
        .await
        .expect("decode core schema response");
    assert_eq!(
        SchemaHash::compute(&core_schema.schema).to_string(),
        schema_hash
    );

    let edge_hashes_response = client
        .get(format!("{}/apps/{app_id}/schemas", edge.base_url()))
        .header("X-Jazz-Admin-Secret", edge.admin_secret())
        .send()
        .await
        .expect("fetch schema hashes through edge");
    assert_eq!(edge_hashes_response.status(), StatusCode::OK);
    let edge_hashes: SchemaHashesHttpResponse = edge_hashes_response
        .json()
        .await
        .expect("decode edge schema hashes response");
    assert!(edge_hashes.hashes.contains(&schema_hash));

    let edge_schema_response = client
        .get(format!(
            "{}/apps/{app_id}/schema/{schema_hash}",
            edge.base_url()
        ))
        .header("X-Jazz-Admin-Secret", edge.admin_secret())
        .send()
        .await
        .expect("fetch schema through edge");
    assert_eq!(edge_schema_response.status(), StatusCode::OK);
    let edge_schema: StoredSchemaHttpResponse = edge_schema_response
        .json()
        .await
        .expect("decode edge schema response");
    assert_eq!(
        SchemaHash::compute(&edge_schema.schema).to_string(),
        schema_hash
    );

    let published_permissions =
        publish_allow_all_permissions(&edge.base_url(), app_id, edge.admin_secret(), &schema).await;
    let core_head_response = client
        .get(format!(
            "{}/apps/{app_id}/admin/permissions/head",
            core.base_url()
        ))
        .header("X-Jazz-Admin-Secret", core.admin_secret())
        .send()
        .await
        .expect("fetch core permissions head");
    assert_eq!(core_head_response.status(), StatusCode::OK);
    let core_head: PermissionsHeadHttpResponse = core_head_response
        .json()
        .await
        .expect("decode core permissions head");
    assert_eq!(core_head.head, Some(published_permissions));

    let edge_head_response = client
        .get(format!(
            "{}/apps/{app_id}/admin/permissions/head",
            edge.base_url()
        ))
        .header("X-Jazz-Admin-Secret", edge.admin_secret())
        .send()
        .await
        .expect("fetch permissions head through edge");
    assert_eq!(edge_head_response.status(), StatusCode::OK);
    let edge_head: PermissionsHeadHttpResponse = edge_head_response
        .json()
        .await
        .expect("decode edge permissions head");
    assert_eq!(edge_head.head, core_head.head);

    edge.shutdown().await;
    core.shutdown().await;
}

// Test topology:
//
//   admin HTTP client
//          |
//          | publish migration over HTTP
//          v
//   edge TestingServer
//          |
//          | forwards POST /admin/migrations after local admin-secret validation
//          v
//   core TestingServer
//
// The assertions verify that the migration is installed by the real core and
// becomes observable both directly on core and through the edge.
#[tokio::test]
async fn edge_migration_publish_forwards_to_real_core_and_is_readable_through_edge() {
    let app_id = TestingServer::default_app_id();
    let core = TestingServer::builder().with_app_id(app_id).start().await;
    let edge = TestingServer::builder()
        .with_app_id(app_id)
        .with_upstream_url(core.base_url())
        .start()
        .await;
    let v1_schema = schema_v1();
    let v2_schema = schema_v2();
    let v1_hash = SchemaHash::compute(&v1_schema).to_string();
    let v2_hash = SchemaHash::compute(&v2_schema).to_string();
    let client = reqwest::Client::new();

    for schema in [&v1_schema, &v2_schema] {
        let publish_schema_response = client
            .post(format!("{}/apps/{app_id}/admin/schemas", core.base_url()))
            .header("X-Jazz-Admin-Secret", core.admin_secret())
            .json(&json!({ "schema": schema }))
            .send()
            .await
            .expect("publish schema to core");
        assert_eq!(publish_schema_response.status(), StatusCode::CREATED);
    }

    let publish_migration_response = client
        .post(format!(
            "{}/apps/{app_id}/admin/migrations",
            edge.base_url()
        ))
        .header("X-Jazz-Admin-Secret", edge.admin_secret())
        .json(&json!({
            "fromHash": v1_hash,
            "toHash": v2_hash,
            "forward": [{
                "table": "users",
                "operations": [{
                    "type": "introduce",
                    "column": "email",
                    "column_type": { "type": "Text" },
                    "value": { "type": "Null" }
                }]
            }]
        }))
        .send()
        .await
        .expect("publish migration through edge");
    let publish_migration_status = publish_migration_response.status();
    if publish_migration_status != StatusCode::CREATED {
        let body = publish_migration_response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable response body>".to_string());
        panic!("migration publish through edge failed: {publish_migration_status} {body}");
    }
    let published_migration: PublishMigrationHttpResponse = publish_migration_response
        .json()
        .await
        .expect("decode edge migration publish response");
    assert_eq!(published_migration.from_hash, v1_hash);
    assert_eq!(published_migration.to_hash, v2_hash);

    let core_connectivity_response = client
        .get(format!(
            "{}/apps/{app_id}/admin/schema-connectivity?fromHash={}&toHash={}",
            core.base_url(),
            published_migration.from_hash,
            published_migration.to_hash
        ))
        .header("X-Jazz-Admin-Secret", core.admin_secret())
        .send()
        .await
        .expect("fetch schema connectivity from core");
    assert_eq!(core_connectivity_response.status(), StatusCode::OK);
    let core_connectivity: SchemaConnectivityHttpResponse = core_connectivity_response
        .json()
        .await
        .expect("decode core schema connectivity response");
    assert!(
        core_connectivity.connected,
        "core should know the migration published through edge"
    );

    let edge_connectivity_response = client
        .get(format!(
            "{}/apps/{app_id}/admin/schema-connectivity?fromHash={}&toHash={}",
            edge.base_url(),
            published_migration.from_hash,
            published_migration.to_hash
        ))
        .header("X-Jazz-Admin-Secret", edge.admin_secret())
        .send()
        .await
        .expect("fetch schema connectivity through edge");
    assert_eq!(edge_connectivity_response.status(), StatusCode::OK);
    let edge_connectivity: SchemaConnectivityHttpResponse = edge_connectivity_response
        .json()
        .await
        .expect("decode edge schema connectivity response");
    assert!(
        edge_connectivity.connected,
        "edge reads should reflect core migration catalogue state"
    );

    edge.shutdown().await;
    core.shutdown().await;
}

/// A dynamic server should fail closed before any permissions head is
/// published, then expose rows once an explicit head is installed.
#[tokio::test]
async fn dynamic_server_denies_reads_until_permissions_head_is_published() {
    let server = TestingServer::start().await;
    let schema = schema_v1();
    seed_schema_catalogue(&server, &schema).await;

    let mut reader_context = server.make_client_context_for_user(schema.clone(), "reader-dynamic");
    reader_context.backend_secret = None;
    reader_context.admin_secret = None;
    let reader = JazzClient::connect(reader_context)
        .await
        .expect("connect reader");

    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            reader.query(
                QueryBuilder::new("users").build(),
                Some(DurabilityTier::EdgeServer),
            ),
        )
        .await
        .is_err(),
        "dynamic server should not settle reads before any permissions head is published"
    );

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;

    wait_for_edge_query_ready(&reader, "users", Duration::from_secs(30)).await;

    let admin =
        JazzClient::connect(server.make_client_context_for_user(schema.clone(), "admin-dynamic"))
            .await
            .expect("connect admin");
    wait_for_edge_query_ready(&admin, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz_tools::ObjectId::new();
    let (user_obj_id, _, batch_id) = admin
        .insert(
            "users",
            user_values_v1(user_id_value, "visible after permissions"),
        )
        .expect("admin creates user after permissions publish");
    admin
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("admin creates user after permissions publish");

    let rows_after_permissions = wait_for_query(
        &reader,
        QueryBuilder::new("users").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "reader sees row after permissions head publish",
        |rows| (rows.len() == 1 && rows[0].0 == user_obj_id).then_some(rows),
    )
    .await;
    assert_eq!(
        rows_after_permissions[0].1,
        vec![
            Value::Uuid(user_id_value),
            Value::Text("visible after permissions".to_string()),
        ]
    );

    admin.shutdown().await.expect("shutdown admin");
    reader.shutdown().await.expect("shutdown reader");
    server.shutdown().await;
}

#[tokio::test]
async fn dynamic_server_keeps_pre_permissions_user_write_hidden_after_publish() {
    let server = TestingServer::start().await;
    let schema = schema_v1();
    seed_schema_catalogue(&server, &schema).await;
    let query = QueryBuilder::new("users").build();
    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("observer-queued-write")
        .connect()
        .await;
    let writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("writer-queued-write")
        .as_user()
        .connect()
        .await;

    let queued_user_id = jazz_tools::ObjectId::new();
    let queued_row_id = jazz_tools::ObjectId::new();
    let (_, _, batch_id) = writer
        .insert_with_id(
            "users",
            *queued_row_id.uuid(),
            user_values_v1(queued_user_id, "queued before permissions"),
        )
        .expect("pre-permissions create should stage locally");
    let queued_write_error = writer
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect_err("pre-permissions persisted create should be rejected");
    let queued_write_error = queued_write_error.to_string();
    assert!(
        queued_write_error.contains("permissions_head_missing"),
        "expected permissions-head rejection, got: {queued_write_error}"
    );
    assert!(
        queued_write_error.contains("no published permissions head"),
        "expected missing permissions-head reason, got: {queued_write_error}"
    );

    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            observer.query(query.clone(), Some(DurabilityTier::EdgeServer)),
        )
        .await
        .is_err(),
        "server should not settle observer queries before permissions arrive"
    );

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;
    wait_for_edge_query_ready(&observer, "users", Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&writer, "users", Duration::from_secs(30)).await;

    let rows_after_publish = wait_for_query(
        &observer,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "pre-permissions user write stays hidden after permissions publish",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(rows_after_publish.is_empty());

    let accepted_user_id = jazz_tools::ObjectId::new();
    let (accepted_row_id, _, batch_id) = writer
        .insert(
            "users",
            user_values_v1(accepted_user_id, "accepted after permissions"),
        )
        .expect("post-publish create should succeed");
    writer
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("post-publish create should settle");

    let rows_after_create = wait_for_query(
        &observer,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "observer sees accepted row after permissions publish",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == accepted_row_id
                && rows[0].1
                    == vec![
                        Value::Uuid(accepted_user_id),
                        Value::Text("accepted after permissions".to_string()),
                    ])
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows_after_create.len(), 1);
    assert_ne!(
        rows_after_create[0].0, queued_row_id,
        "pre-permissions row should stay hidden after permissions arrive"
    );

    let batch_id = writer
        .update(
            accepted_row_id,
            vec![(
                "name".to_string(),
                Value::Text("updated after permissions".to_string()),
            )],
        )
        .expect("update should succeed once permissions exist");
    writer
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("update should settle once permissions exist");

    let rows_after_update = wait_for_query(
        &observer,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "observer sees update after permissions publish",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == accepted_row_id
                && rows[0].1
                    == vec![
                        Value::Uuid(accepted_user_id),
                        Value::Text("updated after permissions".to_string()),
                    ])
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows_after_update.len(), 1);

    let batch_id = writer
        .delete(accepted_row_id)
        .expect("delete should succeed once permissions exist");
    writer
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("delete should settle once permissions exist");

    let rows_after_delete = wait_for_query(
        &observer,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "observer sees delete after permissions publish",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(rows_after_delete.is_empty());

    observer.shutdown().await.expect("shutdown observer");
    writer.shutdown().await.expect("shutdown writer");
    server.shutdown().await;
}

#[tokio::test]
async fn dynamic_server_rejects_user_write_after_permissions_timeout() {
    let server = TestingServer::start().await;
    let schema = schema_v1();
    seed_schema_catalogue(&server, &schema).await;
    let query = QueryBuilder::new("users").build();
    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("observer-timeout-write")
        .connect()
        .await;
    let writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("writer-timeout-write")
        .as_user()
        .connect()
        .await;

    let denied_user_id = jazz_tools::ObjectId::new();
    let (denied_row_id, _, _) = writer
        .insert(
            "users",
            user_values_v1(denied_user_id, "timed out before permissions"),
        )
        .expect("optimistic local create before timeout");

    tokio::time::sleep(Duration::from_secs(12)).await;
    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            observer.query(query.clone(), Some(DurabilityTier::EdgeServer)),
        )
        .await
        .is_err(),
        "observer query should remain unsettled before permissions are published"
    );
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;
    wait_for_edge_query_ready(&observer, "users", Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&writer, "users", Duration::from_secs(30)).await;

    let allowed_user_id = jazz_tools::ObjectId::new();
    let (allowed_row_id, _, batch_id) = writer
        .insert(
            "users",
            user_values_v1(allowed_user_id, "accepted after timeout window"),
        )
        .expect("create should succeed after permissions publish");
    writer
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("create should settle after permissions publish");

    let observer_rows = wait_for_query(
        &observer,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "observer sees only post-timeout allowed row",
        |rows| (rows.len() == 1 && rows[0].0 == allowed_row_id).then_some(rows),
    )
    .await;
    assert_eq!(observer_rows.len(), 1);
    assert_eq!(observer_rows[0].0, allowed_row_id);
    assert_ne!(
        observer_rows[0].0, denied_row_id,
        "timed-out row should stay rejected even after permissions arrive"
    );
    assert_eq!(
        observer_rows[0].1,
        vec![
            Value::Uuid(allowed_user_id),
            Value::Text("accepted after timeout window".to_string()),
        ]
    );

    observer.shutdown().await.expect("shutdown observer");
    writer.shutdown().await.expect("shutdown writer");
    server.shutdown().await;
}

#[tokio::test]
async fn dynamic_server_live_subscription_replays_on_first_permissions_head_and_retightening() {
    let server = TestingServer::start().await;
    let schema = schema_v1();
    seed_schema_catalogue(&server, &schema).await;
    let query = QueryBuilder::new("users").build();

    let reader = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("reader-subscribe")
        .as_user()
        .connect()
        .await;
    let mut stream = reader
        .subscribe(query.clone())
        .await
        .expect("subscribe reader before permissions");
    let mut log = Vec::new();

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        Duration::from_secs(10),
        "initial empty local subscription snapshot before permissions",
        |updates| !updates.is_empty(),
    )
    .await;
    assert!(
        log[0].is_empty(),
        "plain local subscription should fail closed as an empty local delta before permissions"
    );

    let allow_head = publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;

    let admin =
        JazzClient::connect(server.make_client_context_for_user(schema.clone(), "admin-subscribe"))
            .await
            .expect("connect admin");
    wait_for_edge_query_ready(&admin, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz_tools::ObjectId::new();
    let (user_obj_id, _, batch_id) = admin
        .insert(
            "users",
            user_values_v1(user_id_value, "subscription target"),
        )
        .expect("admin creates user after permissions publish");
    admin
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("admin user reaches edge after permissions publish");

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        Duration::from_secs(25),
        "subscription add after first permissions head",
        |updates| has_added(updates, user_obj_id),
    )
    .await;

    publish_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
        deny_all_select_permissions(&schema),
        Some(allow_head.bundle_object_id),
    )
    .await;
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        Duration::from_secs(25),
        "subscription remove after tighter permissions head",
        |updates| has_removed(updates, user_obj_id),
    )
    .await;

    let rows_after_retighten = wait_for_query(
        &reader,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "reader query after tighter permissions head",
        Some,
    )
    .await;
    assert!(
        rows_after_retighten.is_empty(),
        "reader should lose visibility after permissions are tightened"
    );

    admin.shutdown().await.expect("shutdown admin");
    reader.shutdown().await.expect("shutdown reader");
    server.shutdown().await;
}

/// Alice writes under schema v1. The v2 schema and v1→v2 lens are pushed
/// to the server via the real catalogue sync pipeline. Bob connects with
/// schema v2 and sees Alice's data transformed through the lens.
///
/// ```text
/// alice (v1) ──create user──► server
///                                │
///              push v2 schema + lens via WS sync
///                                │
///                  bob (v2) connects and queries
///                                │
///                                └──► user row with email: null
/// ```
#[tokio::test]
async fn column_addition_new_client_can_read_old_rows() {
    let server = TestingServer::start().await;
    let target_schema = schema_v2();

    // === Push v2 schema + lens to server through the real sync pipeline ===
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[schema_v1(), schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await
    .expect("push catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &target_schema,
    )
    .await;

    // === Alice connects with v1, creates a user after permissions publish ===
    let alice =
        JazzClient::connect(server.make_client_context_for_user(schema_v1(), "alice-catalogue"))
            .await
            .expect("connect alice");

    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz_tools::ObjectId::new();
    let (user_obj_id, _, batch_id) = alice
        .insert("users", user_values_v1(user_id_value, "Alice Smith"))
        .expect("alice creates user after permissions publish");
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice user reaches edge after permissions publish");

    // === Bob connects with v2, queries — should see Alice's row with email: null ===
    let bob =
        JazzClient::connect(server.make_client_context_for_user(target_schema, "bob-catalogue"))
            .await
            .expect("connect bob");

    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let bob_rows = wait_for_query(
        &bob,
        QueryBuilder::new("users").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees alice's user with email column",
        |rows| (rows.len() == 1 && rows[0].0 == user_obj_id).then_some(rows),
    )
    .await;

    assert_eq!(bob_rows.len(), 1, "bob should see exactly one user");
    assert_eq!(bob_rows[0].0, user_obj_id);

    let values = &bob_rows[0].1;
    assert_eq!(
        values[0],
        Value::Uuid(user_id_value),
        "id should match alice's user"
    );
    assert_eq!(
        values[1],
        Value::Text("Alice Smith".to_string()),
        "name should match alice's user"
    );
    assert_eq!(
        values[2],
        Value::Null,
        "email should be null (default from lens transform)"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

#[tokio::test]
async fn cannot_read_from_old_schema_until_lens_is_added() {
    let server = TestingServer::start().await;
    let v1_schema = schema_v1();
    let v2_schema = schema_v2();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        std::slice::from_ref(&v1_schema),
        &[],
    )
    .await
    .expect("push initial v1 catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;

    let alice = JazzClient::connect(
        server.make_client_context_for_user(v1_schema.clone(), "alice-schema-before-lens"),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id = jazz_tools::ObjectId::new();
    let (row_id, _, batch_id) = alice
        .insert("users", user_values_v1(user_id, "Alice Pending Lens"))
        .expect("alice creates v1 user");
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice user reaches edge");

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema.clone(), v2_schema.clone()],
        &[],
    )
    .await
    .expect("push v2 schema without lens");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v2_schema,
    )
    .await;

    let bob = JazzClient::connect(
        server.make_client_context_for_user(v2_schema.clone(), "bob-schema-before-lens"),
    )
    .await
    .expect("connect bob");
    let query = QueryBuilder::new("users").build();
    assert_edge_query_does_not_include_row(
        &bob,
        query.clone(),
        row_id,
        Duration::from_secs(2),
        "bob should not see v1 row before the lens arrives",
    )
    .await;

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema, v2_schema],
        &[v1_to_v2_lens()],
    )
    .await
    .expect("push v1 to v2 lens");
    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let rows = wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees alice row after lens arrives",
        |rows| (rows.len() == 1 && rows[0].0 == row_id).then_some(rows),
    )
    .await;
    assert_eq!(
        rows[0].1,
        vec![
            Value::Uuid(user_id),
            Value::Text("Alice Pending Lens".to_string()),
            Value::Null,
        ]
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

#[tokio::test]
async fn draft_lens_does_not_make_rows_from_old_schema_visible() {
    let server = TestingServer::start().await;
    let v1_schema = draft_lens_schema_v1();
    let v2_schema = draft_lens_schema_v2();
    let draft_lens = draft_lens_v1_to_v2();
    assert!(
        draft_lens.is_draft(),
        "test fixture should use a draft lens"
    );

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        std::slice::from_ref(&v1_schema),
        &[],
    )
    .await
    .expect("push initial draft-lens v1 catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;

    let alice = JazzClient::connect(
        server.make_client_context_for_user(v1_schema.clone(), "alice-draft-lens"),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id = jazz_tools::ObjectId::new();
    let (row_id, _, batch_id) = alice
        .insert("users", draft_lens_values_v1(user_id))
        .expect("alice creates v1 user");
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice user reaches edge");

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema, v2_schema.clone()],
        &[draft_lens],
    )
    .await
    .expect("push v2 schema with draft lens");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v2_schema,
    )
    .await;

    let bob = JazzClient::connect(server.make_client_context_for_user(v2_schema, "bob-draft-lens"))
        .await
        .expect("connect bob");
    assert_edge_query_does_not_include_row(
        &bob,
        QueryBuilder::new("users").build(),
        row_id,
        Duration::from_secs(2),
        "bob should not see v1 row through a draft lens",
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Alice writes under schema v1, Bob writes under schema v2, and Charlie reads
/// under schema v3 after the server has received both migration edges. Charlie
/// must see every row projected into the v3 shape.
///
/// ```text
/// push v1 + v2 + v3 schemas and v1→v2→v3 lenses ──► server
///                                                       │
/// alice (v1) ──create user─────────────────────────────►│
/// bob   (v2) ──create user with email──────────────────►│
/// charlie (v3) ──create user with email + role─────────►│
///                                                       │
/// charlie (v3) query ──► Alice(email=null, role=null)
///                        Bob(email=value, role=null)
///                        Charlie(email=value, role=value)
/// ```
#[tokio::test]
async fn multi_hop_column_additions_new_client_can_read_old_rows() {
    let server = TestingServer::start().await;
    let v3_schema = schema_v3();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[schema_v1(), schema_v2(), v3_schema.clone()],
        &[v1_to_v2_lens(), v2_to_v3_lens()],
    )
    .await
    .expect("push multi-hop catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v3_schema,
    )
    .await;

    let alice =
        JazzClient::connect(server.make_client_context_for_user(schema_v1(), "alice-multi-hop"))
            .await
            .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;
    let alice_user_id = jazz_tools::ObjectId::new();
    let (alice_row_id, _, alice_batch_id) = alice
        .insert("users", user_values_v1(alice_user_id, "Alice Multi-Hop"))
        .expect("alice creates v1 user");
    alice
        .wait_for_batch(alice_batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice user reaches edge");

    let bob =
        JazzClient::connect(server.make_client_context_for_user(schema_v2(), "bob-multi-hop"))
            .await
            .expect("connect bob");
    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;
    let bob_user_id = jazz_tools::ObjectId::new();
    let (bob_row_id, _, bob_batch_id) = bob
        .insert(
            "users",
            user_values_v2(bob_user_id, "Bob Multi-Hop", "bob@example.com"),
        )
        .expect("bob creates v2 user");
    bob.wait_for_batch(bob_batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("bob user reaches edge");

    let charlie =
        JazzClient::connect(server.make_client_context_for_user(v3_schema, "charlie-multi-hop"))
            .await
            .expect("connect charlie");
    wait_for_edge_query_ready(&charlie, "users", Duration::from_secs(30)).await;
    let charlie_user_id = jazz_tools::ObjectId::new();
    let (charlie_row_id, _, charlie_batch_id) = charlie
        .insert(
            "users",
            user_values_v3(
                charlie_user_id,
                "Charlie Multi-Hop",
                "charlie@example.com",
                "admin",
            ),
        )
        .expect("charlie creates v3 user");
    charlie
        .wait_for_batch(charlie_batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("charlie user reaches edge");

    let rows = wait_for_query(
        &charlie,
        QueryBuilder::new("users").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "charlie sees all rows transformed to v3",
        |rows| {
            (rows.len() == 3
                && rows.iter().any(|(id, _)| *id == alice_row_id)
                && rows.iter().any(|(id, _)| *id == bob_row_id)
                && rows.iter().any(|(id, _)| *id == charlie_row_id))
            .then_some(rows)
        },
    )
    .await;

    let alice_row = rows
        .iter()
        .find(|(id, _)| *id == alice_row_id)
        .expect("alice row should be present");
    assert_eq!(
        alice_row.1,
        vec![
            Value::Uuid(alice_user_id),
            Value::Text("Alice Multi-Hop".to_string()),
            Value::Null,
            Value::Null,
        ]
    );

    let bob_row = rows
        .iter()
        .find(|(id, _)| *id == bob_row_id)
        .expect("bob row should be present");
    assert_eq!(
        bob_row.1,
        vec![
            Value::Uuid(bob_user_id),
            Value::Text("Bob Multi-Hop".to_string()),
            Value::Text("bob@example.com".to_string()),
            Value::Null,
        ]
    );

    let charlie_row = rows
        .iter()
        .find(|(id, _)| *id == charlie_row_id)
        .expect("charlie row should be present");
    assert_eq!(
        charlie_row.1,
        vec![
            Value::Uuid(charlie_user_id),
            Value::Text("Charlie Multi-Hop".to_string()),
            Value::Text("charlie@example.com".to_string()),
            Value::Text("admin".to_string()),
        ]
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    charlie.shutdown().await.expect("shutdown charlie");
    server.shutdown().await;
}

/// Alice writes under schema v1 with `email`. Bob reads under schema v3 where
/// the column has been renamed twice: `email` -> `email_address` ->
/// `contact_email`.
///
/// ```text
/// push v1 + v2 + v3 schemas and rename lenses ──► server
///                                                    │
/// alice (v1) ──create user(email)───────────────────►│
///                                                    │
/// bob (v3) query ──► user row with contact_email value
/// ```
#[tokio::test]
async fn multi_hop_column_renames_new_client_can_read_old_rows() {
    let server = TestingServer::start().await;
    let v1_schema = rename_chain_schema_v1();
    let v2_schema = rename_chain_schema_v2();
    let v3_schema = rename_chain_schema_v3();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema.clone(), v2_schema, v3_schema.clone()],
        &[rename_chain_v1_to_v2_lens(), rename_chain_v2_to_v3_lens()],
    )
    .await
    .expect("push rename-chain catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v3_schema,
    )
    .await;

    let alice =
        JazzClient::connect(server.make_client_context_for_user(v1_schema, "alice-rename-chain"))
            .await
            .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id = jazz_tools::ObjectId::new();
    let (row_id, _, batch_id) = alice
        .insert(
            "users",
            rename_chain_values_v1(user_id, "alice@example.com"),
        )
        .expect("alice creates v1 user");
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice user reaches edge");

    let bob =
        JazzClient::connect(server.make_client_context_for_user(v3_schema, "bob-rename-chain"))
            .await
            .expect("connect bob");
    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let rows = wait_for_query(
        &bob,
        QueryBuilder::new("users").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees alice row through chained column renames",
        |rows| (rows.len() == 1 && rows[0].0 == row_id).then_some(rows),
    )
    .await;

    assert_eq!(
        rows[0].1,
        vec![
            Value::Uuid(user_id),
            Value::Text("alice@example.com".to_string()),
        ]
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Bob writes under schema v3 with `contact_email`. Alice reads under schema
/// v1 where the column was originally named `email`.
///
/// ```text
/// push v1 + v2 + v3 schemas and rename lenses ──► server
///                                                    │
/// bob (v3) ──create user(contact_email)─────────────►│
///                                                    │
/// alice (v1) query ──► user row with email value
/// ```
#[tokio::test]
async fn multi_hop_column_renames_old_client_can_read_new_rows() {
    let server = TestingServer::start().await;
    let v1_schema = rename_chain_schema_v1();
    let v2_schema = rename_chain_schema_v2();
    let v3_schema = rename_chain_schema_v3();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema.clone(), v2_schema, v3_schema.clone()],
        &[rename_chain_v1_to_v2_lens(), rename_chain_v2_to_v3_lens()],
    )
    .await
    .expect("push rename-chain catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v3_schema,
    )
    .await;

    let bob =
        JazzClient::connect(server.make_client_context_for_user(v3_schema, "bob-rename-chain-new"))
            .await
            .expect("connect bob");
    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let user_id = jazz_tools::ObjectId::new();
    let (row_id, _, batch_id) = bob
        .insert("users", rename_chain_values_v3(user_id, "bob@example.com"))
        .expect("bob creates v3 user");
    bob.wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("bob user reaches edge");

    let alice = JazzClient::connect(
        server.make_client_context_for_user(v1_schema, "alice-rename-chain-old"),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let rows = wait_for_query(
        &alice,
        QueryBuilder::new("users").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "alice sees bob row through chained column renames",
        |rows| (rows.len() == 1 && rows[0].0 == row_id).then_some(rows),
    )
    .await;

    assert_eq!(
        rows[0].1,
        vec![
            Value::Uuid(user_id),
            Value::Text("bob@example.com".to_string()),
        ]
    );

    bob.shutdown().await.expect("shutdown bob");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Alice writes under schema v1 to `users`. Bob reads under schema v2 where
/// that table has been renamed to `people`.
///
/// ```text
/// push v1 + v2 schemas and RenameTable users -> people ──► server
///                                                          │
/// alice (v1) ──create users row───────────────────────────►│
///                                                          │
/// bob (v2) query people ──► row from old users table
/// ```
#[tokio::test]
async fn table_rename_new_client_can_read_old_rows() {
    let server = TestingServer::start().await;
    let v1_schema = table_rename_schema_v1();
    let v2_schema = table_rename_schema_v2();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema.clone(), v2_schema.clone()],
        &[table_rename_v1_to_v2_lens()],
    )
    .await
    .expect("push table-rename catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;

    let alice =
        JazzClient::connect(server.make_client_context_for_user(v1_schema, "alice-table-rename"))
            .await
            .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id = jazz_tools::ObjectId::new();
    let (row_id, _, batch_id) = alice
        .insert(
            "users",
            table_rename_values_v1(user_id, "alice@example.com"),
        )
        .expect("alice creates v1 user");
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice user reaches edge");

    let bob =
        JazzClient::connect(server.make_client_context_for_user(v2_schema, "bob-table-rename"))
            .await
            .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;

    let rows = wait_for_query(
        &bob,
        QueryBuilder::new("people").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees alice row through table rename",
        |rows| (rows.len() == 1 && rows[0].0 == row_id).then_some(rows),
    )
    .await;

    assert_eq!(
        rows[0].1,
        vec![
            Value::Uuid(user_id),
            Value::Text("alice@example.com".to_string()),
        ]
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Bob subscribes under schema v2 to `people`. Alice then writes under schema
/// v1 to `users`, and Bob's subscription receives the row through the table
/// rename lens.
#[tokio::test]
async fn table_rename_subscription_reacts_to_old_branch_updates() {
    let server = TestingServer::start().await;
    let v1_schema = table_rename_schema_v1();
    let v2_schema = table_rename_schema_v2();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema.clone(), v2_schema.clone()],
        &[table_rename_v1_to_v2_lens()],
    )
    .await
    .expect("push table-rename catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;

    let bob =
        JazzClient::connect(server.make_client_context_for_user(v2_schema, "bob-table-rename-sub"))
            .await
            .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;

    let query = QueryBuilder::new("people").build();
    let mut stream = bob
        .subscribe(query.clone())
        .await
        .expect("bob subscribes to people");
    let mut log = Vec::new();
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        Duration::from_secs(10),
        "initial empty people subscription",
        |updates| !updates.is_empty(),
    )
    .await;
    assert!(
        log[0].is_empty(),
        "subscription should start empty before old-table rows are written"
    );

    let alice = JazzClient::connect(
        server.make_client_context_for_user(v1_schema, "alice-table-rename-sub"),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id = jazz_tools::ObjectId::new();
    let (row_id, _, _) = alice
        .insert(
            "users",
            table_rename_values_v1(user_id, "alice@example.com"),
        )
        .expect("alice creates v1 user");

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        Duration::from_secs(25),
        "bob subscription sees alice row through table rename",
        |updates| has_added(updates, row_id),
    )
    .await;

    let rows = wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob query sees subscription row through table rename",
        |rows| (rows.len() == 1 && rows[0].0 == row_id).then_some(rows),
    )
    .await;
    assert_eq!(
        rows[0].1,
        vec![
            Value::Uuid(user_id),
            Value::Text("alice@example.com".to_string()),
        ]
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Alice subscribes under schema v1 to `users`. The catalogue then evolves to
/// schema v2 where that table is named `people`; when Bob writes to `people`,
/// Alice's old subscription receives the row through the table rename lens.
#[tokio::test]
async fn table_rename_subscription_reacts_to_new_branch_updates_after_schema_evolution() {
    let server = TestingServer::start().await;
    let v1_schema = table_rename_schema_v1();
    let v2_schema = table_rename_schema_v2();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        std::slice::from_ref(&v1_schema),
        &[],
    )
    .await
    .expect("push initial v1 table-rename catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;

    let alice = JazzClient::connect(
        server.make_client_context_for_user(v1_schema.clone(), "alice-table-rename-evolve-sub"),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let query = QueryBuilder::new("users").build();
    let mut stream = alice
        .subscribe(query.clone())
        .await
        .expect("alice subscribes to users");
    let mut log = Vec::new();
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        Duration::from_secs(10),
        "initial empty users subscription",
        |updates| !updates.is_empty(),
    )
    .await;
    assert!(
        log[0].is_empty(),
        "subscription should start empty before new-table rows are written"
    );

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema.clone(), v2_schema.clone()],
        &[table_rename_v1_to_v2_lens()],
    )
    .await
    .expect("push evolved table-rename catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v2_schema,
    )
    .await;
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let bob = JazzClient::connect(
        server.make_client_context_for_user(v2_schema, "bob-table-rename-evolve-sub"),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;

    let user_id = jazz_tools::ObjectId::new();
    let (row_id, _, batch_id) = bob
        .insert("people", table_rename_values_v2(user_id, "bob@example.com"))
        .expect("bob creates v2 person");
    bob.wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("bob person reaches edge");

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        Duration::from_secs(25),
        "alice subscription sees bob row through table rename",
        |updates| has_added(updates, row_id),
    )
    .await;

    let rows = wait_for_query(
        &alice,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "alice query sees new-table row through table rename",
        |rows| (rows.len() == 1 && rows[0].0 == row_id).then_some(rows),
    )
    .await;
    assert_eq!(
        rows[0].1,
        vec![
            Value::Uuid(user_id),
            Value::Text("bob@example.com".to_string()),
        ]
    );

    bob.shutdown().await.expect("shutdown bob");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

#[tokio::test]
async fn table_rename_update_and_delete_copy_on_write() {
    let server = TestingServer::start().await;
    let v1_schema = table_rename_schema_v1();
    let v2_schema = table_rename_schema_v2();
    let v2_branch = format!("client-{}-main", SchemaHash::compute(&v2_schema).short());

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema.clone(), v2_schema.clone()],
        &[table_rename_v1_to_v2_lens()],
    )
    .await
    .expect("push table-rename catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;

    let alice = JazzClient::connect(
        server.make_client_context_for_user(v1_schema.clone(), "alice-table-rename-copy-on-write"),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id = jazz_tools::ObjectId::new();
    let (row_id, _, batch_id) = alice
        .insert(
            "users",
            table_rename_values_v1(user_id, "alice@example.com"),
        )
        .expect("alice creates v1 user");
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice user reaches edge");

    let bob = JazzClient::connect(
        server.make_client_context_for_user(v2_schema, "bob-table-rename-copy-on-write"),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;

    let batch_id = bob
        .update(
            row_id,
            vec![(
                "email".to_string(),
                Value::Text("alice+updated@example.com".to_string()),
            )],
        )
        .expect("bob updates renamed row");
    bob.wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("bob update reaches edge");

    let rows_after_update = wait_for_query(
        &bob,
        QueryBuilder::new("people")
            .branch(v2_branch.clone())
            .build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees copied row on renamed table after update",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == row_id
                && rows[0].1
                    == vec![
                        Value::Uuid(user_id),
                        Value::Text("alice+updated@example.com".to_string()),
                    ])
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows_after_update.len(), 1);

    let batch_id = bob.delete(row_id).expect("bob deletes renamed row");
    bob.wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("bob delete reaches edge");

    let rows_after_delete = wait_for_query(
        &bob,
        QueryBuilder::new("people").branch(v2_branch).build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees renamed row deleted",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(rows_after_delete.is_empty());

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

#[tokio::test]
async fn table_rename_join_query_translates_join_target_on_old_branch() {
    let server = TestingServer::start().await;
    let v1_schema = table_rename_join_schema_v1();
    let v2_schema = table_rename_join_schema_v2();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema.clone(), v2_schema.clone()],
        &[table_rename_join_v1_to_v2_lens()],
    )
    .await
    .expect("push join table-rename catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;

    let alice =
        JazzClient::connect(server.make_client_context_for_user(v1_schema, "alice-join-rename"))
            .await
            .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&alice, "posts", Duration::from_secs(30)).await;

    let author_id = jazz_tools::ObjectId::new();
    let (_, _, batch_id) = alice
        .insert("users", table_rename_join_user_values(author_id, "Alice"))
        .expect("alice creates v1 user");
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice user reaches edge");

    let post_id = jazz_tools::ObjectId::new();
    let (post_row_id, _, batch_id) = alice
        .insert(
            "posts",
            table_rename_join_post_values(post_id, author_id, "Hello from v1"),
        )
        .expect("alice creates v1 post");
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice post reaches edge");

    let bob =
        JazzClient::connect(server.make_client_context_for_user(v2_schema, "bob-join-rename"))
            .await
            .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&bob, "posts", Duration::from_secs(30)).await;

    let query = QueryBuilder::new("posts")
        .join("people")
        .on("posts.author_id", "people.id")
        .build();
    let rows = wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob join sees v1 post author through table rename",
        |rows| (rows.len() == 1 && rows[0].0 == post_row_id).then_some(rows),
    )
    .await;

    assert_eq!(
        rows[0].1,
        vec![
            Value::Uuid(post_id),
            Value::Uuid(author_id),
            Value::Text("Hello from v1".to_string()),
            Value::Uuid(author_id),
            Value::Text("Alice".to_string()),
        ]
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

#[tokio::test]
async fn table_rename_fk_array_lookup_finds_related_rows_on_old_branch() {
    let server = TestingServer::start().await;
    let v1_schema = table_rename_join_schema_v1();
    let v2_schema = table_rename_join_schema_v2();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema.clone(), v2_schema.clone()],
        &[table_rename_join_v1_to_v2_lens()],
    )
    .await
    .expect("push array table-rename catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;

    let alice =
        JazzClient::connect(server.make_client_context_for_user(v1_schema, "alice-array-rename"))
            .await
            .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&alice, "posts", Duration::from_secs(30)).await;

    let author_id = jazz_tools::ObjectId::new();
    let (author_row_id, _, batch_id) = alice
        .insert("users", table_rename_join_user_values(author_id, "Alice"))
        .expect("alice creates v1 user");
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice user reaches edge");

    let post_id = jazz_tools::ObjectId::new();
    let (_, _, batch_id) = alice
        .insert(
            "posts",
            table_rename_join_post_values(post_id, author_id, "Alice post"),
        )
        .expect("alice creates v1 post");
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice post reaches edge");

    let bob =
        JazzClient::connect(server.make_client_context_for_user(v2_schema, "bob-array-rename"))
            .await
            .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&bob, "posts", Duration::from_secs(30)).await;

    let query = QueryBuilder::new("people")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "people.id")
        })
        .build();
    let rows = wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob array include sees v1 posts through table rename",
        |rows| (rows.len() == 1 && rows[0].0 == author_row_id).then_some(rows),
    )
    .await;

    assert_eq!(rows[0].1[0], Value::Uuid(author_id));
    assert_eq!(rows[0].1[1], Value::Text("Alice".to_string()));
    let posts = rows[0].1[2]
        .as_array()
        .expect("third column should be posts array");
    assert_eq!(posts.len(), 1);
    let first_post = posts[0]
        .as_row()
        .expect("post array element should be a row");
    assert_eq!(first_post[0], Value::Uuid(post_id));
    assert_eq!(first_post[1], Value::Uuid(author_id));
    assert_eq!(first_post[2], Value::Text("Alice post".to_string()));

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

#[tokio::test]
async fn local_join_query_uses_current_permissions_for_joined_provenance_after_lens_transform() {
    let server = TestingServer::start().await;
    let legacy_schema = legacy_join_provenance_schema();
    let current_schema = current_join_provenance_permission_schema();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[legacy_schema.clone(), current_schema.clone()],
        &[legacy_join_provenance_to_current_permissions_lens()],
    )
    .await
    .expect("push join provenance catalogue");

    let current_permissions = current_schema
        .iter()
        .map(|(table_name, table_schema)| (*table_name, table_schema.policies.clone()))
        .collect::<Vec<_>>();
    publish_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &current_schema,
        current_permissions,
        None,
    )
    .await;

    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(legacy_schema)
        .with_user_id("join-provenance-admin")
        .as_admin()
        .ready_on("users", Duration::from_secs(30))
        .connect()
        .await;
    wait_for_edge_query_ready(&admin, "posts", Duration::from_secs(30)).await;

    let (bob_user_id, _, batch_id) = admin
        .insert("users", legacy_join_provenance_user_values("bob"))
        .expect("admin creates legacy user");
    admin
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("legacy user reaches edge");

    let (_, _, batch_id) = admin
        .insert(
            "posts",
            legacy_join_provenance_post_values("bob", "Bob private post"),
        )
        .expect("admin creates legacy post");
    admin
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("legacy post reaches edge");

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(current_schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on("users", Duration::from_secs(30))
        .connect()
        .await;
    wait_for_edge_query_ready(&alice, "posts", Duration::from_secs(30)).await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(current_schema)
        .with_user_id("bob")
        .as_user()
        .ready_on("users", Duration::from_secs(30))
        .connect()
        .await;
    wait_for_edge_query_ready(&bob, "posts", Duration::from_secs(30)).await;

    let query = QueryBuilder::new("users")
        .join("posts")
        .on("users.name", "posts.owner_name")
        .build();

    let bob_rows = wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees joined row after provenance lens applies current permissions",
        |rows| (rows.len() == 1 && rows[0].0 == bob_user_id).then_some(rows),
    )
    .await;
    assert_eq!(
        bob_rows[0].1,
        vec![
            Value::Text("bob".to_string()),
            Value::Text("bob".to_string()),
            Value::Text("Bob private post".to_string()),
            Value::Text("bob".to_string()),
        ]
    );

    let alice_rows = wait_for_query(
        &alice,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "alice does not see joined row denied by transformed joined provenance",
        Some,
    )
    .await;
    assert!(
        alice_rows.is_empty(),
        "Alice should not see Bob's joined post after current-permissions filtering"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

#[tokio::test]
async fn multi_hop_table_renames_and_column_rename() {
    let server = TestingServer::start().await;
    let v1_schema = multi_hop_table_rename_schema_v1();
    let v2_schema = multi_hop_table_rename_schema_v2();
    let v3_schema = multi_hop_table_rename_schema_v3();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema.clone(), v2_schema.clone(), v3_schema.clone()],
        &[
            multi_hop_table_rename_v1_to_v2_lens(),
            multi_hop_table_rename_v2_to_v3_lens(),
        ],
    )
    .await
    .expect("push multi-hop table-rename catalogue");

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;
    let alice = JazzClient::connect(
        server.make_client_context_for_user(v1_schema, "alice-multi-table-rename"),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;
    let alice_id = jazz_tools::ObjectId::new();
    let (alice_row_id, _, batch_id) = alice
        .insert(
            "users",
            multi_hop_table_rename_values_v1(alice_id, "alice@example.com"),
        )
        .expect("alice creates v1 user");
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice row reaches edge");

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v2_schema,
    )
    .await;
    let bob = JazzClient::connect(
        server.make_client_context_for_user(v2_schema, "bob-multi-table-rename"),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;
    let bob_id = jazz_tools::ObjectId::new();
    let (bob_row_id, _, batch_id) = bob
        .insert(
            "people",
            multi_hop_table_rename_values_v2(bob_id, "bob@example.com"),
        )
        .expect("bob creates v2 person");
    bob.wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("bob row reaches edge");

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v3_schema,
    )
    .await;
    let carol = JazzClient::connect(
        server.make_client_context_for_user(v3_schema.clone(), "carol-multi-table-rename"),
    )
    .await
    .expect("connect carol");
    wait_for_edge_query_ready(&carol, "members", Duration::from_secs(30)).await;
    let carol_id = jazz_tools::ObjectId::new();
    let (carol_row_id, _, batch_id) = carol
        .insert(
            "members",
            multi_hop_table_rename_values_v3(carol_id, "carol@example.com"),
        )
        .expect("carol creates v3 member");
    carol
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("carol row reaches edge");

    let rows = wait_for_query(
        &carol,
        QueryBuilder::new("members").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "carol sees every schema version projected to members",
        |rows| {
            (rows.len() == 3
                && rows.iter().any(|(id, _)| *id == alice_row_id)
                && rows.iter().any(|(id, _)| *id == bob_row_id)
                && rows.iter().any(|(id, _)| *id == carol_row_id))
            .then_some(rows)
        },
    )
    .await;

    assert!(rows.iter().all(|(_, row)| row.len() == 2));
    assert!(rows.iter().any(|(_, row)| {
        row == &vec![
            Value::Uuid(alice_id),
            Value::Text("alice@example.com".to_string()),
        ]
    }));
    assert!(rows.iter().any(|(_, row)| {
        row == &vec![
            Value::Uuid(bob_id),
            Value::Text("bob@example.com".to_string()),
        ]
    }));
    assert!(rows.iter().any(|(_, row)| {
        row == &vec![
            Value::Uuid(carol_id),
            Value::Text("carol@example.com".to_string()),
        ]
    }));

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    carol.shutdown().await.expect("shutdown carol");
    server.shutdown().await;
}

/// A table name reused after the table was removed is a new lineage. A v3
/// `users` query must not resurface rows from the v1 `users` table that was
/// removed in v2.
#[tokio::test]
async fn removed_table_then_readded_does_not_resurface_old_rows() {
    let server = TestingServer::start().await;
    let v1_schema = removed_readded_schema_v1();
    let v2_schema = removed_readded_schema_v2();
    let v3_schema = removed_readded_schema_v3();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema.clone(), v2_schema, v3_schema.clone()],
        &[
            removed_readded_v1_to_v2_lens(),
            removed_readded_v2_to_v3_lens(),
        ],
    )
    .await
    .expect("push removed/re-added table catalogue");

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;

    let alice = JazzClient::connect(
        server.make_client_context_for_user(v1_schema, "alice-removed-readded-v1"),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let alice_id = jazz_tools::ObjectId::new();
    let (alice_row_id, _, batch_id) = alice
        .insert(
            "users",
            removed_readded_values_v1(alice_id, "Alice Old Lineage"),
        )
        .expect("alice creates v1 user");
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice row reaches edge");

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v3_schema,
    )
    .await;

    let bob = JazzClient::connect(
        server.make_client_context_for_user(v3_schema, "bob-removed-readded-v3"),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let bob_id = jazz_tools::ObjectId::new();
    let (bob_row_id, _, batch_id) = bob
        .insert(
            "users",
            removed_readded_values_v3(bob_id, "Bob New Lineage", "bob@example.com"),
        )
        .expect("bob creates v3 user");
    bob.wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("bob row reaches edge");

    let rows = wait_for_query(
        &bob,
        QueryBuilder::new("users").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "v3 users query only sees rows from the re-added table lineage",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == bob_row_id
                && rows.iter().all(|(id, _)| *id != alice_row_id))
            .then_some(rows)
        },
    )
    .await;

    assert_eq!(
        rows[0].1,
        vec![
            Value::Uuid(bob_id),
            Value::Text("Bob New Lineage".to_string()),
            Value::Text("bob@example.com".to_string()),
        ]
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Bob writes under schema v2 after the server has received the v1/v2
/// catalogue. Alice connects with schema v1 and sees Bob's data transformed
/// through the backward lens.
///
/// ```text
/// push v1 schema + v2 schema + lens ──► server
///                                        │
/// bob (v2) ──create user with email──► server
///                                        │
///                  alice (v1) connects and queries
///                                        │
///                                        └──► user row without email column
/// ```
#[tokio::test]
async fn column_addition_old_client_can_read_new_rows() {
    let server = TestingServer::start().await;
    let target_schema = schema_v2();

    // Seed the server with both schemas and the v1<->v2 lens before clients connect.
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[schema_v1(), schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await
    .expect("push catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &target_schema,
    )
    .await;

    // === Bob connects with v2, creates a user with the new email column ===
    let bob = JazzClient::connect(server.make_client_context_for_user(schema_v2(), "bob-backward"))
        .await
        .expect("connect bob");

    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz_tools::ObjectId::new();
    let user_email = "bob@example.com";
    let (user_obj_id, _, _) = bob
        .insert(
            "users",
            user_values_v2(user_id_value, "Bob Backward", user_email),
        )
        .expect("bob creates user");

    wait_for_query(
        &bob,
        QueryBuilder::new("users").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob's v2 user settled at edge",
        |rows| (rows.len() == 1 && rows[0].0 == user_obj_id).then_some(rows),
    )
    .await;

    // === Alice connects with v1, queries — should see Bob's row without email ===
    let alice =
        JazzClient::connect(server.make_client_context_for_user(schema_v1(), "alice-backward"))
            .await
            .expect("connect alice");

    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let alice_rows = wait_for_query(
        &alice,
        QueryBuilder::new("users").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "alice sees bob's user without email column",
        |rows| (rows.len() == 1 && rows[0].0 == user_obj_id).then_some(rows),
    )
    .await;

    assert_eq!(alice_rows.len(), 1, "alice should see exactly one user");
    assert_eq!(alice_rows[0].0, user_obj_id);

    let values = &alice_rows[0].1;
    assert_eq!(
        values.len(),
        2,
        "v1 view should not include the email column"
    );
    assert_eq!(
        values[0],
        Value::Uuid(user_id_value),
        "id should match bob's user"
    );
    assert_eq!(
        values[1],
        Value::Text("Bob Backward".to_string()),
        "name should match bob's user"
    );

    bob.shutdown().await.expect("shutdown bob");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

#[tokio::test]
async fn keeps_authorization_through_v1_head() {
    let server = TestingServer::start().await;
    let query = QueryBuilder::new("users").build();
    let v1_schema = schema_v1();
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        std::slice::from_ref(&v1_schema),
        &[],
    )
    .await
    .expect("push v1 catalogue before publishing v1 permissions");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;

    let alice =
        JazzClient::connect(server.make_client_context_for_user(schema_v1(), "alice-v1-head"))
            .await
            .expect("connect alice");

    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz_tools::ObjectId::new();
    let (user_obj_id, _, batch_id) = alice
        .insert("users", user_values_v1(user_id_value, "Alice Through Lens"))
        .expect("alice creates user after v1 permissions publish");
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice user reaches edge after v1 permissions publish");

    wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "alice row settled before v1 permissions publish",
        |rows| (rows.len() == 1 && rows[0].0 == user_obj_id).then_some(rows),
    )
    .await;

    let v1_schema = schema_v1();
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema, schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await
    .expect("push catalogue after v1 permissions head");

    let bob = JazzClient::connect(server.make_client_context_for_user(schema_v2(), "bob-v2-head"))
        .await
        .expect("connect bob");
    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let bob_rows = wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees alice row through v1 authorization schema",
        |rows| (rows.len() == 1 && rows[0].0 == user_obj_id).then_some(rows),
    )
    .await;
    assert_eq!(
        bob_rows[0].1,
        vec![
            Value::Uuid(user_id_value),
            Value::Text("Alice Through Lens".to_string()),
            Value::Null,
        ]
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}
