//! E2E catalogue sync integration test.
//!
//! Verifies that schema+lens catalogue objects propagate through the full
//! SyncManager pipeline (not via direct `process_catalogue_update()` calls).

use jazz_testkit as support;

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use jazz::query::{ArraySubquery, Query};
use jazz::row_input;
use jazz::tools::public_schema::PolicyExpr;
use jazz::tools::public_schema::SchemaHash;
use jazz::tools::public_schema::TablePolicies;
use jazz::tools::schema_lens::{Lens, LensOp, LensTransform};
use jazz::tools::{ColumnType, DurabilityTier, SchemaBuilder, TableSchema, Value};
use jazz_server::JazzServer;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;
use support::{
    PublishedPermissionsHead, TestingClient, deny_all_select_permissions, has_added_id,
    has_removed, publish_allow_all_permissions, publish_permissions, push_catalogue_in_memory,
    wait_for, wait_for_edge_query_ready, wait_for_query, wait_for_query_results,
    wait_for_subscription_update,
};
use tempfile::TempDir;
use uuid::Uuid;

fn test_user_id(subject: &str) -> String {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, subject.as_bytes()).to_string()
}

fn user_values_v1(id: jazz::tools::ObjectId, name: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "name" => name)
}

fn user_values_v2(id: jazz::tools::ObjectId, name: &str, email: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "name" => name, "email" => email)
}

fn user_values_v3(
    id: jazz::tools::ObjectId,
    name: &str,
    email: &str,
    role: &str,
) -> HashMap<String, Value> {
    row_input!("id" => id, "name" => name, "email" => email, "role" => role)
}

fn schema_v1() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build()
}

fn schema_v2() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build()
}

fn schema_v3() -> jazz::tools::Schema {
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
    Lens::new(
        SchemaHash::compute(&schema_v1()),
        SchemaHash::compute(&schema_v2()),
        LensTransform::with_ops(vec![LensOp::AddColumn {
            table: "users".to_string(),
            column: "email".to_string(),
            column_type: ColumnType::Text,
            default: Value::Null,
        }]),
    )
}

fn v2_to_v3_lens() -> Lens {
    Lens::new(
        SchemaHash::compute(&schema_v2()),
        SchemaHash::compute(&schema_v3()),
        LensTransform::with_ops(vec![LensOp::AddColumn {
            table: "users".to_string(),
            column: "role".to_string(),
            column_type: ColumnType::Text,
            default: Value::Null,
        }]),
    )
}

fn rename_chain_values_v1(id: jazz::tools::ObjectId, email: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "email" => email)
}

fn rename_chain_values_v3(
    id: jazz::tools::ObjectId,
    contact_email: &str,
) -> HashMap<String, Value> {
    row_input!("id" => id, "contact_email" => contact_email)
}

fn rename_chain_schema_v1() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build()
}

fn rename_chain_schema_v2() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email_address", ColumnType::Text),
        )
        .build()
}

fn rename_chain_schema_v3() -> jazz::tools::Schema {
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

fn table_rename_values_v1(id: jazz::tools::ObjectId, email: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "email" => email)
}

fn table_rename_values_v2(id: jazz::tools::ObjectId, email: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "email" => email)
}

fn table_rename_schema_v1() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build()
}

fn table_rename_schema_v2() -> jazz::tools::Schema {
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

/// A copy-on-write-specific target version with a physical column v1 cannot
/// name. This makes the test witness the authored v2 route, rather than only
/// the shared `id`/`email` lineage.
fn table_rename_copy_on_write_schema_v2() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("people")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text)
                .column_with_default(
                    "v2_marker",
                    ColumnType::Text,
                    Value::Text("from-v1-default".to_string()),
                ),
        )
        .build()
}

fn table_rename_copy_on_write_v1_to_v2_lens() -> Lens {
    Lens::new(
        SchemaHash::compute(&table_rename_schema_v1()),
        SchemaHash::compute(&table_rename_copy_on_write_schema_v2()),
        LensTransform::with_ops(vec![
            LensOp::RenameTable {
                old_name: "users".to_string(),
                new_name: "people".to_string(),
            },
            LensOp::AddColumn {
                table: "people".to_string(),
                column: "v2_marker".to_string(),
                column_type: ColumnType::Text,
                default: Value::Text("from-v1-default".to_string()),
            },
        ]),
    )
}

fn table_rename_join_user_values(id: jazz::tools::ObjectId, name: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "name" => name)
}

fn table_rename_join_post_values(
    id: jazz::tools::ObjectId,
    author_id: jazz::tools::ObjectId,
    title: &str,
) -> HashMap<String, Value> {
    row_input!("id" => id, "author_id" => author_id, "title" => title)
}

fn table_rename_join_schema_v1() -> jazz::tools::Schema {
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

fn table_rename_join_schema_v2() -> jazz::tools::Schema {
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

fn legacy_join_provenance_schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("users").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("posts")
                .column("owner_name", ColumnType::Text)
                .column("title", ColumnType::Text),
        )
        .build()
}

fn current_join_provenance_permission_schema() -> jazz::tools::Schema {
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
                        // This scenario moves an existing post between
                        // reader scopes; its write is not testing omitted
                        // operation semantics.
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                        .with_select(PolicyExpr::eq_session(
                            "viewer_name",
                            vec!["user".to_owned()],
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
            default: Value::Text(test_user_id("bob")),
        }]),
    )
}

fn multi_hop_table_rename_values_v1(
    id: jazz::tools::ObjectId,
    email: &str,
) -> HashMap<String, Value> {
    row_input!("id" => id, "email" => email)
}

fn multi_hop_table_rename_values_v2(
    id: jazz::tools::ObjectId,
    email: &str,
) -> HashMap<String, Value> {
    row_input!("id" => id, "email" => email)
}

fn multi_hop_table_rename_values_v3(
    id: jazz::tools::ObjectId,
    email_address: &str,
) -> HashMap<String, Value> {
    row_input!("id" => id, "email_address" => email_address)
}

fn multi_hop_table_rename_schema_v1() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build()
}

fn multi_hop_table_rename_schema_v2() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("people")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build()
}

fn multi_hop_table_rename_schema_v3() -> jazz::tools::Schema {
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

fn removed_readded_values_v1(id: jazz::tools::ObjectId, name: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "name" => name)
}

fn removed_readded_values_v3(
    id: jazz::tools::ObjectId,
    name: &str,
    email: &str,
) -> HashMap<String, Value> {
    row_input!("id" => id, "name" => name, "email" => email)
}

fn removed_readded_schema_v1() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build()
}

fn removed_readded_schema_v2() -> jazz::tools::Schema {
    SchemaBuilder::new().build()
}

fn removed_readded_schema_v3() -> jazz::tools::Schema {
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
    Lens::new(
        SchemaHash::compute(&removed_readded_schema_v1()),
        SchemaHash::compute(&removed_readded_schema_v2()),
        LensTransform::with_ops(vec![LensOp::RemoveTable {
            table: "users".to_string(),
            schema: TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .build(),
        }]),
    )
}

fn removed_readded_v2_to_v3_lens() -> Lens {
    Lens::new(
        SchemaHash::compute(&removed_readded_schema_v2()),
        SchemaHash::compute(&removed_readded_schema_v3()),
        LensTransform::with_ops(vec![LensOp::AddTable {
            table: "users".to_string(),
            schema: TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text)
                .build(),
        }]),
    )
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
    schema: jazz::tools::Schema,
}

#[derive(Debug, Deserialize)]
struct PermissionsHeadHttpResponse {
    head: Option<PublishedPermissionsHead>,
}

async fn seed_schema_catalogue(server: &JazzServer, schema: &jazz::tools::Schema) {
    let response = reqwest::Client::new()
        .post(format!(
            "{}/apps/{}/admin/schemas",
            server.base_url(),
            server.app_id()
        ))
        .header("X-Jazz-Admin-Secret", server.admin_secret())
        .json(&json!({ "schema": schema }))
        .send()
        .await
        .expect("publish schema catalogue");
    assert_eq!(response.status(), StatusCode::CREATED);
}

/// Publish the explicit v1 -> v2 lineage required before the v2 schema can
/// become an active runtime write schema. Schemas are drafts until this
/// migration is admitted; a later permissions head must not invent a
/// self-referential lineage for them.
async fn publish_v1_to_v2_catalogue_migration(server: &JazzServer) {
    let response = reqwest::Client::new()
        .post(format!(
            "{}/apps/{}/admin/migrations",
            server.base_url(),
            server.app_id()
        ))
        .header("X-Jazz-Admin-Secret", server.admin_secret())
        .json(&json!({
            "fromHash": SchemaHash::compute(&schema_v1()).to_string(),
            "toHash": SchemaHash::compute(&schema_v2()).to_string(),
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
        .expect("publish v1 to v2 catalogue migration");
    let status = response.status();
    assert_eq!(
        status,
        StatusCode::CREATED,
        "v2 must be admitted through its explicit v1-to-v2 lineage"
    );
}

// Test topology:
//
//   admin HTTP client
//          |
//          | publish/read catalogue over HTTP
//          v
//   edge JazzServer
//          |
//          | forwards after local admin-secret validation
//          v
//   core JazzServer
//
// The assertions verify that writes sent to the edge are persisted by the real
// core, and reads sent to the edge return the core catalogue state.
#[tokio::test]
async fn edge_catalogue_http_reads_and_writes_forward_to_real_core() {
    tokio::task::LocalSet::new()
        .run_until(edge_catalogue_http_reads_and_writes_forward_to_real_core_impl())
        .await
}

async fn edge_catalogue_http_reads_and_writes_forward_to_real_core_impl() {
    let app_id = JazzServer::default_app_id();
    let core = JazzServer::builder().with_app_id(app_id).start().await;
    let edge = JazzServer::builder()
        .with_app_id(app_id)
        .with_native_transport_connector(jazz_testkit::native_connector())
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

    let public_schema_convert_response = client
        .get(format!(
            "{}/apps/{app_id}/schema/{schema_hash}",
            core.base_url()
        ))
        .header("X-Jazz-Admin-Secret", core.admin_secret())
        .send()
        .await
        .expect("fetch schema from core");
    assert_eq!(public_schema_convert_response.status(), StatusCode::OK);
    let public_schema_convert: StoredSchemaHttpResponse = public_schema_convert_response
        .json()
        .await
        .expect("decode core schema response");
    assert_eq!(
        SchemaHash::compute(&public_schema_convert.schema).to_string(),
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

/// A catalogue published through one edge reaches a client on a second edge
/// through the real core, before that client writes any application data.
///
/// Actors: mallory publishes the catalogue through `edge_us`; alice connects
/// through `edge_eu` and writes only after its edge has received that
/// catalogue.
///
/// ```text
/// mallory --catalogue--> edge_us --upstream--> core --upstream--> edge_eu
///                                                               |
/// alice --------------------------------------------------------+--write--> core
/// ```
#[tokio::test]
async fn edge_catalogue_publish_reaches_peer_edge_through_core_sync() {
    tokio::task::LocalSet::new()
        .run_until(edge_catalogue_publish_reaches_peer_edge_through_core_sync_impl())
        .await
}

async fn edge_catalogue_publish_reaches_peer_edge_through_core_sync_impl() {
    let app_id = JazzServer::default_app_id();
    let schema = schema_v1();
    let core = JazzServer::builder().with_app_id(app_id).start().await;
    let edge_us = JazzServer::builder()
        .with_app_id(app_id)
        .with_native_transport_connector(jazz_testkit::native_connector())
        .with_upstream_url(core.base_url())
        .start()
        .await;
    let edge_eu = JazzServer::builder()
        .with_app_id(app_id)
        .with_native_transport_connector(jazz_testkit::native_connector())
        .with_upstream_url(core.base_url())
        .start()
        .await;

    seed_schema_catalogue(&edge_us, &schema).await;
    publish_allow_all_permissions(&edge_us.base_url(), app_id, edge_us.admin_secret(), &schema)
        .await;

    let alice = TestingClient::builder()
        .with_server(&edge_eu)
        .with_schema(schema.clone())
        .with_user_id(test_user_id("alice-peer-edge-catalogue"))
        .ready_on("users", Duration::from_secs(30))
        .connect_after_retry_later(Duration::from_secs(30))
        .await;

    let user_id = jazz::tools::ObjectId::new();
    let (row_id, _, transaction_id) = alice
        .insert(
            "users",
            user_values_v1(user_id, "visible through peer edge"),
        )
        .expect("peer-edge client writes after receiving the catalogue");
    alice
        .wait_for_transaction(
            transaction_id.expect("ordinary mutation commits immediately"),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("peer-edge write reaches the core");

    let rows = wait_for_query(
        &alice,
        jazz::query::Query::from("users"),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "peer edge serves the row written after catalogue replication",
        |rows| (rows.len() == 1 && rows[0].0 == row_id).then_some(rows),
    )
    .await;
    assert_eq!(
        rows[0].1,
        vec![
            Value::Uuid(user_id),
            Value::Text("visible through peer edge".to_string())
        ]
    );

    alice.shutdown().await.expect("shutdown alice");
    edge_eu.shutdown().await;
    edge_us.shutdown().await;
    core.shutdown().await;
}

/// A persisted edge that misses a core catalogue evolution while offline must
/// replay it during reconnect, before a client can perform work requiring the
/// new schema.
///
/// Actors: mallory publishes v1 and then v2 at `core`; `edge` persists v1,
/// goes offline, and reconnects; alice uses v2 only after the reconnect.
///
/// ```text
/// core(v1) --catalogue--> edge(persistent)
/// edge stops; core publishes v2
/// edge(v1) --reconnect--> core --catalogue replay--> edge(v2) --> alice(v2)
/// ```
#[tokio::test]
async fn persisted_stale_edge_reconnect_replays_catalogue_before_client_work() {
    tokio::task::LocalSet::new()
        .run_until(persisted_stale_edge_reconnect_replays_catalogue_before_client_work_impl())
        .await
}

async fn persisted_stale_edge_reconnect_replays_catalogue_before_client_work_impl() {
    let app_id = JazzServer::default_app_id();
    let v1_schema = schema_v1();
    let v2_schema = schema_v2();
    let edge_data_dir = TempDir::new().expect("create persistent edge data directory");
    let core = JazzServer::builder().with_app_id(app_id).start().await;

    seed_schema_catalogue(&core, &v1_schema).await;
    publish_allow_all_permissions(&core.base_url(), app_id, core.admin_secret(), &v1_schema).await;

    let edge_before_restart = JazzServer::builder()
        .with_app_id(app_id)
        .with_native_transport_connector(jazz_testkit::native_connector())
        .with_upstream_url(core.base_url())
        .with_data_dir(edge_data_dir.path())
        .with_storage_factory(jazz_testkit::persistent_storage_factory())
        .start()
        .await;
    let alice_v1 = TestingClient::builder()
        .with_server(&edge_before_restart)
        .with_schema(v1_schema.clone())
        .with_user_id(test_user_id("alice-stale-edge-v1"))
        .ready_on("users", Duration::from_secs(30))
        .connect_after_retry_later(Duration::from_secs(30))
        .await;
    alice_v1.shutdown().await.expect("shutdown v1 client");
    edge_before_restart.shutdown().await;

    seed_schema_catalogue(&core, &v2_schema).await;
    publish_v1_to_v2_catalogue_migration(&core).await;
    publish_allow_all_permissions(&core.base_url(), app_id, core.admin_secret(), &v2_schema).await;

    let edge_after_restart = JazzServer::builder()
        .with_app_id(app_id)
        .with_native_transport_connector(jazz_testkit::native_connector())
        .with_upstream_url(core.base_url())
        .with_data_dir(edge_data_dir.path())
        .with_storage_factory(jazz_testkit::persistent_storage_factory())
        .start()
        .await;
    let alice_v2 = TestingClient::builder()
        .with_server(&edge_after_restart)
        .with_schema(v2_schema.clone())
        .with_user_id(test_user_id("alice-stale-edge-v2"))
        .ready_on("users", Duration::from_secs(30))
        .connect_after_retry_later(Duration::from_secs(30))
        .await;

    let user_id = jazz::tools::ObjectId::new();
    let (row_id, _, transaction_id) = alice_v2
        .insert(
            "users",
            user_values_v2(user_id, "replayed before client work", "v2@example.test"),
        )
        .expect("v2 client writes through restarted edge");
    alice_v2
        .wait_for_transaction(
            transaction_id.expect("ordinary mutation commits immediately"),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("v2 write settles after catalogue replay");
    let rows = wait_for_query(
        &alice_v2,
        jazz::query::Query::from("users"),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "restarted edge serves row written with replayed v2 schema",
        |rows| (rows.len() == 1 && rows[0].0 == row_id).then_some(rows),
    )
    .await;
    assert_eq!(
        rows[0].1,
        vec![
            Value::Uuid(user_id),
            Value::Text("replayed before client work".to_string()),
            Value::Text("v2@example.test".to_string()),
        ]
    );

    alice_v2.shutdown().await.expect("shutdown v2 client");
    edge_after_restart.shutdown().await;
    core.shutdown().await;
}

/// A dynamic edge whose catalogue has reached Ready may be shut down and
/// reopened after its core is unavailable. The first client after restart must
/// use the durable catalogue immediately; it must not depend on a fresh
/// bootstrap exchange with the unavailable upstream.
#[tokio::test]
async fn persistent_dynamic_edge_reopens_ready_catalogue_before_first_client() {
    tokio::task::LocalSet::new()
        .run_until(persistent_dynamic_edge_reopens_ready_catalogue_before_first_client_impl())
        .await
}

async fn persistent_dynamic_edge_reopens_ready_catalogue_before_first_client_impl() {
    let app_id = JazzServer::default_app_id();
    let schema = schema_v1();
    let edge_data_dir = TempDir::new().expect("create persistent edge data directory");
    let core = JazzServer::builder().with_app_id(app_id).start().await;
    let unavailable_core_url = core.base_url();
    seed_schema_catalogue(&core, &schema).await;
    publish_allow_all_permissions(&core.base_url(), app_id, core.admin_secret(), &schema).await;

    let edge_before_shutdown = JazzServer::builder()
        .with_app_id(app_id)
        .with_native_transport_connector(jazz_testkit::native_connector())
        .with_upstream_url(unavailable_core_url.clone())
        .with_data_dir(edge_data_dir.path())
        .with_storage_factory(jazz_testkit::persistent_storage_factory())
        .start()
        .await;
    let warmup = TestingClient::builder()
        .with_server(&edge_before_shutdown)
        .with_schema(schema.clone())
        .with_user_id(test_user_id("dynamic-edge-warmup"))
        .ready_on("users", Duration::from_secs(30))
        .connect_after_retry_later(Duration::from_secs(30))
        .await;
    warmup.shutdown().await.expect("shutdown warmup client");
    edge_before_shutdown.shutdown().await;
    core.shutdown().await;

    let edge_after_restart = JazzServer::builder()
        .with_app_id(app_id)
        .with_native_transport_connector(jazz_testkit::native_connector())
        .with_upstream_url(unavailable_core_url)
        .with_data_dir(edge_data_dir.path())
        .with_storage_factory(jazz_testkit::persistent_storage_factory())
        .start()
        .await;
    let first_client = TestingClient::builder()
        .with_server(&edge_after_restart)
        .with_schema(schema)
        .with_user_id(test_user_id("dynamic-edge-first-after-restart"))
        .ready_on("users", Duration::from_secs(30))
        .connect()
        .await;

    first_client
        .shutdown()
        .await
        .expect("shutdown first client");
    edge_after_restart.shutdown().await;
}

/// Retightening permissions at the core invalidates the existing subscriptions
/// of clients connected through each of two independent edge servers.
///
/// Actors: alice subscribes through `edge_us`, bob through `edge_eu`, carol
/// writes to `core`, and mallory replaces allow-read permissions with deny-read.
///
/// ```text
/// carol --> core --> edge_us --> alice subscription
///                \-> edge_eu --> bob subscription
/// mallory --deny select--> core --> both edges remove the existing row
/// ```
#[tokio::test]
async fn core_permission_retightening_reaches_subscribed_clients_on_every_edge() {
    tokio::task::LocalSet::new()
        .run_until(core_permission_retightening_reaches_subscribed_clients_on_every_edge_impl())
        .await
}

async fn core_permission_retightening_reaches_subscribed_clients_on_every_edge_impl() {
    let app_id = JazzServer::default_app_id();
    let schema = schema_v1();
    let core = JazzServer::builder().with_app_id(app_id).start().await;
    seed_schema_catalogue(&core, &schema).await;
    let allow_head =
        publish_allow_all_permissions(&core.base_url(), app_id, core.admin_secret(), &schema).await;
    let edge_us = JazzServer::builder()
        .with_app_id(app_id)
        .with_native_transport_connector(jazz_testkit::native_connector())
        .with_upstream_url(core.base_url())
        .start()
        .await;
    let edge_eu = JazzServer::builder()
        .with_app_id(app_id)
        .with_native_transport_connector(jazz_testkit::native_connector())
        .with_upstream_url(core.base_url())
        .start()
        .await;

    let alice = TestingClient::builder()
        .with_server(&edge_us)
        .with_schema(schema.clone())
        .with_user_id(test_user_id("alice-retighten-us"))
        .ready_on("users", Duration::from_secs(30))
        .connect_after_retry_later(Duration::from_secs(30))
        .await;
    let bob = TestingClient::builder()
        .with_server(&edge_eu)
        .with_schema(schema.clone())
        .with_user_id(test_user_id("bob-retighten-eu"))
        .ready_on("users", Duration::from_secs(30))
        .connect_after_retry_later(Duration::from_secs(30))
        .await;
    let carol = TestingClient::builder()
        .with_server(&core)
        .with_schema(schema.clone())
        .with_user_id(test_user_id("carol-retighten-core"))
        .ready_on("users", Duration::from_secs(30))
        .connect()
        .await;
    let query = jazz::query::Query::from("users");
    let mut alice_stream = alice
        .subscribe(query.clone())
        .await
        .expect("alice subscribes");
    let mut bob_stream = bob.subscribe(query.clone()).await.expect("bob subscribes");
    let mut alice_log = Vec::new();
    let mut bob_log = Vec::new();

    let user_id = jazz::tools::ObjectId::new();
    let (row_id, _, transaction_id) = carol
        .insert(
            "users",
            user_values_v1(user_id, "visible before retightening"),
        )
        .expect("core writer inserts visible row");
    carol
        .wait_for_transaction(
            transaction_id.expect("ordinary mutation commits immediately"),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("core write settles globally");
    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        Duration::from_secs(30),
        "alice receives row through edge_us before retightening",
        |log| has_added_id(log, row_id),
    )
    .await;
    wait_for_subscription_update(
        &mut bob_stream,
        &mut bob_log,
        Duration::from_secs(30),
        "bob receives row through edge_eu before retightening",
        |log| has_added_id(log, row_id),
    )
    .await;

    publish_permissions(
        &core.base_url(),
        app_id,
        core.admin_secret(),
        &schema,
        deny_all_select_permissions(&schema),
        Some(allow_head.bundle_object_id),
    )
    .await;
    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        Duration::from_secs(30),
        "alice loses row after core permission retightening",
        |log| has_removed(log, row_id),
    )
    .await;
    wait_for_subscription_update(
        &mut bob_stream,
        &mut bob_log,
        Duration::from_secs(30),
        "bob loses row after core permission retightening",
        |log| has_removed(log, row_id),
    )
    .await;

    wait_for(
        Duration::from_secs(25),
        "both edge queries become empty after retightening",
        || async {
            let alice_rows = alice
                .query(query.clone(), Some(DurabilityTier::EdgeServer))
                .await
                .ok()?;
            let bob_rows = bob
                .query(query.clone(), Some(DurabilityTier::EdgeServer))
                .await
                .ok()?;
            (alice_rows.is_empty() && bob_rows.is_empty()).then_some(())
        },
    )
    .await;

    carol.shutdown().await.expect("shutdown carol");
    bob.shutdown().await.expect("shutdown bob");
    alice.shutdown().await.expect("shutdown alice");
    edge_eu.shutdown().await;
    edge_us.shutdown().await;
    core.shutdown().await;
}

// Test topology:
//
//   admin HTTP client
//          |
//          | publish migration over HTTP
//          v
//   edge JazzServer
//          |
//          | forwards POST /admin/migrations after local admin-secret validation
//          v
//   core JazzServer
//
// The assertions verify that the migration is installed by the real core and
// becomes observable both directly on core and through the edge.
#[tokio::test]
async fn edge_migration_publish_forwards_to_real_core_and_is_readable_through_edge() {
    tokio::task::LocalSet::new()
        .run_until(edge_migration_publish_forwards_to_real_core_and_is_readable_through_edge_impl())
        .await
}

async fn edge_migration_publish_forwards_to_real_core_and_is_readable_through_edge_impl() {
    let app_id = JazzServer::default_app_id();
    let core = JazzServer::builder().with_app_id(app_id).start().await;
    let edge = JazzServer::builder()
        .with_app_id(app_id)
        .with_native_transport_connector(jazz_testkit::native_connector())
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
    tokio::task::LocalSet::new()
        .run_until(dynamic_server_denies_reads_until_permissions_head_is_published_impl())
        .await
}

async fn dynamic_server_denies_reads_until_permissions_head_is_published_impl() {
    let server = JazzServer::start().await;
    let schema = schema_v1();
    seed_schema_catalogue(&server, &schema).await;

    let mut reader_context =
        server.make_client_context_for_user(schema.clone(), test_user_id("reader-dynamic"));
    reader_context.backend_secret = None;
    reader_context.admin_secret = None;
    let reader = jazz_testkit::connect(reader_context)
        .await
        .expect("connect reader");

    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            reader.query(
                jazz::query::Query::from("users"),
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

    let admin = jazz_testkit::connect(
        server.make_client_context_for_user(schema.clone(), test_user_id("admin-dynamic")),
    )
    .await
    .expect("connect admin");
    wait_for_edge_query_ready(&admin, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz::tools::ObjectId::new();
    let (user_obj_id, _, transaction_id) = admin
        .insert(
            "users",
            user_values_v1(user_id_value, "visible after permissions"),
        )
        .expect("admin creates user after permissions publish");
    support::wait_for_edge_txs(
        &admin,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let rows_after_permissions = wait_for_query(
        &reader,
        jazz::query::Query::from("users"),
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
    tokio::task::LocalSet::new()
        .run_until(dynamic_server_keeps_pre_permissions_user_write_hidden_after_publish_impl())
        .await
}

async fn dynamic_server_keeps_pre_permissions_user_write_hidden_after_publish_impl() {
    let server = JazzServer::start().await;
    let schema = schema_v1();
    seed_schema_catalogue(&server, &schema).await;
    let query = jazz::query::Query::from("users");
    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(test_user_id("observer-queued-write"))
        .connect()
        .await;
    let writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(test_user_id("writer-queued-write"))
        .as_user()
        .connect()
        .await;

    let queued_user_id = jazz::tools::ObjectId::new();
    let queued_row_id = jazz::tools::ObjectId::new();
    let (_, _, transaction_id) = writer
        .insert_with_id(
            "users",
            *queued_row_id.uuid(),
            user_values_v1(queued_user_id, "queued before permissions"),
        )
        .expect("pre-permissions create should stage locally");
    let queued_write_error = writer
        .wait_for_transaction(
            transaction_id.expect("ordinary mutation commits immediately"),
            DurabilityTier::EdgeServer,
        )
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

    let accepted_user_id = jazz::tools::ObjectId::new();
    let (accepted_row_id, _, transaction_id) = writer
        .insert(
            "users",
            user_values_v1(accepted_user_id, "accepted after permissions"),
        )
        .expect("post-publish create should succeed");
    support::wait_for_edge_txs(
        &writer,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

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

    let transaction_id = writer
        .update(
            accepted_row_id,
            vec![(
                "name".to_string(),
                Value::Text("updated after permissions".to_string()),
            )],
        )
        .expect("update should succeed once permissions exist");
    support::wait_for_edge_txs(
        &writer,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

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

    let transaction_id = writer
        .delete(accepted_row_id)
        .expect("delete should succeed once permissions exist");
    support::wait_for_edge_txs(
        &writer,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

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
    tokio::task::LocalSet::new()
        .run_until(dynamic_server_rejects_user_write_after_permissions_timeout_impl())
        .await
}

async fn dynamic_server_rejects_user_write_after_permissions_timeout_impl() {
    let server = JazzServer::start().await;
    let schema = schema_v1();
    seed_schema_catalogue(&server, &schema).await;
    let query = jazz::query::Query::from("users");
    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(test_user_id("observer-timeout-write"))
        .connect()
        .await;
    let writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(test_user_id("writer-timeout-write"))
        .as_user()
        .connect()
        .await;

    let denied_user_id = jazz::tools::ObjectId::new();
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

    let allowed_user_id = jazz::tools::ObjectId::new();
    let (allowed_row_id, _, transaction_id) = writer
        .insert(
            "users",
            user_values_v1(allowed_user_id, "accepted after timeout window"),
        )
        .expect("create should succeed after permissions publish");
    support::wait_for_edge_txs(
        &writer,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

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
    tokio::task::LocalSet::new().run_until(dynamic_server_live_subscription_replays_on_first_permissions_head_and_retightening_impl()).await
}

async fn dynamic_server_live_subscription_replays_on_first_permissions_head_and_retightening_impl()
{
    let server = JazzServer::start().await;
    let schema = schema_v1();
    seed_schema_catalogue(&server, &schema).await;
    let query = jazz::query::Query::from("users");

    let reader = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(test_user_id("reader-subscribe"))
        .as_user()
        .connect()
        .await;
    let mut stream = reader
        .subscribe(query.clone())
        .await
        .expect("subscribe reader before permissions");
    // This server-backed subscription requests an authority tier, so its
    // provisional empty local view remains withheld until permissions and the
    // matching authority-side write settle.
    let mut log = Vec::new();

    let allow_head = publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;

    let admin = jazz_testkit::connect(
        server.make_client_context_for_user(schema.clone(), test_user_id("admin-subscribe")),
    )
    .await
    .expect("connect admin");
    wait_for_edge_query_ready(&admin, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz::tools::ObjectId::new();
    let (user_obj_id, _, transaction_id) = admin
        .insert(
            "users",
            user_values_v1(user_id_value, "subscription target"),
        )
        .expect("admin creates user after permissions publish");
    support::wait_for_edge_txs(
        &admin,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        Duration::from_secs(25),
        "subscription add after first permissions head",
        |updates| has_added_id(updates, user_obj_id),
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
    tokio::task::LocalSet::new()
        .run_until(column_addition_new_client_can_read_old_rows_impl())
        .await
}

async fn column_addition_new_client_can_read_old_rows_impl() {
    let server = JazzServer::start().await;
    let target_schema = schema_v2();

    // === Push v2 schema + lens to server through the real sync pipeline ===
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
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
    let alice = jazz_testkit::connect(
        server.make_client_context_for_user(schema_v1(), test_user_id("alice-catalogue")),
    )
    .await
    .expect("connect alice");

    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz::tools::ObjectId::new();
    let (user_obj_id, _, transaction_id) = alice
        .insert("users", user_values_v1(user_id_value, "Alice Smith"))
        .expect("alice creates user after permissions publish");
    support::wait_for_edge_txs(
        &alice,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    // === Bob connects with v2, queries — should see Alice's row with email: null ===
    let bob = jazz_testkit::connect(
        server.make_client_context_for_user(target_schema, test_user_id("bob-catalogue")),
    )
    .await
    .expect("connect bob");

    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let bob_rows = wait_for_query(
        &bob,
        jazz::query::Query::from("users"),
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

/// Alice writes under schema v1, Bob opens a v2 draft, and only the public
/// migration publication makes Bob's schema readable. The draft must not expose
/// Alice's v1 row before the lineage lens atomically activates it.
///
/// ```text
/// admin ──publish v1──► server ◄── Alice writes v1 row
/// admin ──publish v2 draft──► server ◄── Bob cannot read the row
/// admin ──publish v1→v2 migration──► server ──► Bob reads projected row
/// ```
#[tokio::test]
async fn cannot_read_from_old_schema_until_lens_is_added() {
    tokio::task::LocalSet::new()
        .run_until(cannot_read_from_old_schema_until_lens_is_added_impl())
        .await
}

async fn cannot_read_from_old_schema_until_lens_is_added_impl() {
    let server = JazzServer::start().await;
    let v1_schema = schema_v1();
    let v2_schema = schema_v2();

    seed_schema_catalogue(&server, &v1_schema).await;
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;

    let alice =
        jazz_testkit::connect(server.make_client_context_for_user(
            v1_schema.clone(),
            test_user_id("alice-schema-before-lens"),
        ))
        .await
        .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id = jazz::tools::ObjectId::new();
    let (row_id, _, transaction_id) = alice
        .insert("users", user_values_v1(user_id, "Alice Pending Lens"))
        .expect("alice creates v1 user");
    support::wait_for_edge_txs(
        &alice,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    seed_schema_catalogue(&server, &v2_schema).await;

    let bob =
        jazz_testkit::connect(server.make_client_context_for_user(
            v2_schema.clone(),
            test_user_id("bob-schema-before-lens"),
        ))
        .await
        .expect("connect bob with unpublished draft schema");
    let query = jazz::query::Query::from("users");
    let pre_lens_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut pre_lens_attempts = 0;
    while tokio::time::Instant::now() < pre_lens_deadline {
        pre_lens_attempts += 1;
        let attempt = tokio::time::timeout(
            Duration::from_millis(250),
            bob.query(query.clone(), Some(DurabilityTier::EdgeServer)),
        )
        .await;
        assert!(
            attempt.is_err(),
            "v2 draft must remain unready and expose no query result before its lineage lens: {attempt:?}"
        );
    }
    assert!(
        pre_lens_attempts >= 4,
        "must repeatedly exercise Bob's v2 query while the draft is unpublished"
    );

    publish_v1_to_v2_catalogue_migration(&server).await;
    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let rows = wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees Alice's row after atomic v1-to-v2 migration publication",
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
    tokio::task::LocalSet::new()
        .run_until(multi_hop_column_additions_new_client_can_read_old_rows_impl())
        .await
}

async fn multi_hop_column_additions_new_client_can_read_old_rows_impl() {
    let server = JazzServer::start().await;
    let v3_schema = schema_v3();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
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

    let alice = jazz_testkit::connect(
        server.make_client_context_for_user(schema_v1(), test_user_id("alice-multi-hop")),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;
    let alice_user_id = jazz::tools::ObjectId::new();
    let (alice_row_id, _, alice_tx_id) = alice
        .insert("users", user_values_v1(alice_user_id, "Alice Multi-Hop"))
        .expect("alice creates v1 user");
    support::wait_for_edge_txs(
        &alice,
        &[alice_tx_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let bob = jazz_testkit::connect(
        server.make_client_context_for_user(schema_v2(), test_user_id("bob-multi-hop")),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;
    let bob_user_id = jazz::tools::ObjectId::new();
    let (bob_row_id, _, bob_tx_id) = bob
        .insert(
            "users",
            user_values_v2(bob_user_id, "Bob Multi-Hop", "bob@example.com"),
        )
        .expect("bob creates v2 user");
    support::wait_for_edge_txs(
        &bob,
        &[bob_tx_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let charlie = jazz_testkit::connect(
        server.make_client_context_for_user(v3_schema, test_user_id("charlie-multi-hop")),
    )
    .await
    .expect("connect charlie");
    wait_for_edge_query_ready(&charlie, "users", Duration::from_secs(30)).await;
    let charlie_user_id = jazz::tools::ObjectId::new();
    let (charlie_row_id, _, charlie_tx_id) = charlie
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
    support::wait_for_edge_txs(
        &charlie,
        &[charlie_tx_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let rows = wait_for_query(
        &charlie,
        jazz::query::Query::from("users"),
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
    tokio::task::LocalSet::new()
        .run_until(multi_hop_column_renames_new_client_can_read_old_rows_impl())
        .await
}

async fn multi_hop_column_renames_new_client_can_read_old_rows_impl() {
    let server = JazzServer::start().await;
    let v1_schema = rename_chain_schema_v1();
    let v2_schema = rename_chain_schema_v2();
    let v3_schema = rename_chain_schema_v3();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
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

    let alice = jazz_testkit::connect(
        server.make_client_context_for_user(v1_schema, test_user_id("alice-rename-chain")),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id = jazz::tools::ObjectId::new();
    let (row_id, _, transaction_id) = alice
        .insert(
            "users",
            rename_chain_values_v1(user_id, "alice@example.com"),
        )
        .expect("alice creates v1 user");
    support::wait_for_edge_txs(
        &alice,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let bob = jazz_testkit::connect(
        server.make_client_context_for_user(v3_schema, test_user_id("bob-rename-chain")),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let rows = wait_for_query(
        &bob,
        jazz::query::Query::from("users"),
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
    tokio::task::LocalSet::new()
        .run_until(multi_hop_column_renames_old_client_can_read_new_rows_impl())
        .await
}

async fn multi_hop_column_renames_old_client_can_read_new_rows_impl() {
    let server = JazzServer::start().await;
    let v1_schema = rename_chain_schema_v1();
    let v2_schema = rename_chain_schema_v2();
    let v3_schema = rename_chain_schema_v3();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
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

    let bob = jazz_testkit::connect(
        server.make_client_context_for_user(v3_schema, test_user_id("bob-rename-chain-new")),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let user_id = jazz::tools::ObjectId::new();
    let (row_id, _, transaction_id) = bob
        .insert("users", rename_chain_values_v3(user_id, "bob@example.com"))
        .expect("bob creates v3 user");
    support::wait_for_edge_txs(
        &bob,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let alice = jazz_testkit::connect(
        server.make_client_context_for_user(v1_schema, test_user_id("alice-rename-chain-old")),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let rows = wait_for_query(
        &alice,
        jazz::query::Query::from("users"),
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
    tokio::task::LocalSet::new()
        .run_until(table_rename_new_client_can_read_old_rows_impl())
        .await
}

async fn table_rename_new_client_can_read_old_rows_impl() {
    let server = JazzServer::start().await;
    let v1_schema = table_rename_schema_v1();
    let v2_schema = table_rename_schema_v2();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
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

    let alice = jazz_testkit::connect(
        server.make_client_context_for_user(v1_schema, test_user_id("alice-table-rename")),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id = jazz::tools::ObjectId::new();
    let (row_id, _, transaction_id) = alice
        .insert(
            "users",
            table_rename_values_v1(user_id, "alice@example.com"),
        )
        .expect("alice creates v1 user");
    support::wait_for_edge_txs(
        &alice,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let bob = jazz_testkit::connect(
        server.make_client_context_for_user(v2_schema, test_user_id("bob-table-rename")),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;

    let rows = wait_for_query(
        &bob,
        jazz::query::Query::from("people"),
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
    tokio::task::LocalSet::new()
        .run_until(table_rename_subscription_reacts_to_old_branch_updates_impl())
        .await
}

async fn table_rename_subscription_reacts_to_old_branch_updates_impl() {
    let server = JazzServer::start().await;
    let v1_schema = table_rename_schema_v1();
    let v2_schema = table_rename_schema_v2();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
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

    let bob = jazz_testkit::connect(
        server.make_client_context_for_user(v2_schema, test_user_id("bob-table-rename-sub")),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;

    let query = jazz::query::Query::from("people");
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

    let alice = jazz_testkit::connect(
        server.make_client_context_for_user(v1_schema, test_user_id("alice-table-rename-sub")),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id = jazz::tools::ObjectId::new();
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
        |updates| has_added_id(updates, row_id),
    )
    .await;
    let added = log
        .iter()
        .flat_map(|delta| &delta.added)
        .find(|change| change.id == row_id)
        .expect("renamed subscription delta contains Alice's exact row identity");
    assert_eq!(
        added.index, 0,
        "the first row through the renamed table is inserted at the head of the ordered result"
    );
    assert!(
        log.iter()
            .all(|delta| delta.removed.is_empty() && delta.updated.is_empty()),
        "a newly visible renamed row is an addition, not a synthetic replacement"
    );

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
    tokio::task::LocalSet::new()
        .run_until(
            table_rename_subscription_reacts_to_new_branch_updates_after_schema_evolution_impl(),
        )
        .await
}

async fn table_rename_subscription_reacts_to_new_branch_updates_after_schema_evolution_impl() {
    let server = JazzServer::start().await;
    let v1_schema = table_rename_schema_v1();
    let v2_schema = table_rename_schema_v2();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
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

    let alice = jazz_testkit::connect(server.make_client_context_for_user(
        v1_schema.clone(),
        test_user_id("alice-table-rename-evolve-sub"),
    ))
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let query = jazz::query::Query::from("users");
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

    let bob = jazz_testkit::connect(
        server.make_client_context_for_user(v2_schema, test_user_id("bob-table-rename-evolve-sub")),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;

    let user_id = jazz::tools::ObjectId::new();
    let (row_id, _, transaction_id) = bob
        .insert("people", table_rename_values_v2(user_id, "bob@example.com"))
        .expect("bob creates v2 person");
    support::wait_for_edge_txs(
        &bob,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        Duration::from_secs(25),
        "alice subscription sees bob row through table rename",
        |updates| has_added_id(updates, row_id),
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
    tokio::task::LocalSet::new()
        .run_until(table_rename_update_and_delete_copy_on_write_impl())
        .await
}

async fn table_rename_update_and_delete_copy_on_write_impl() {
    let server = JazzServer::start().await;
    let v1_schema = table_rename_schema_v1();
    let v2_schema = table_rename_copy_on_write_schema_v2();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        &[v1_schema.clone(), v2_schema.clone()],
        &[table_rename_copy_on_write_v1_to_v2_lens()],
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

    let alice = jazz_testkit::connect(server.make_client_context_for_user(
        v1_schema.clone(),
        test_user_id("alice-table-rename-copy-on-write"),
    ))
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id = jazz::tools::ObjectId::new();
    let (row_id, _, transaction_id) = alice
        .insert(
            "users",
            table_rename_values_v1(user_id, "alice@example.com"),
        )
        .expect("alice creates v1 user");
    support::wait_for_edge_txs(
        &alice,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    // Publishing a permissions head is the public operation that moves the
    // server's current write pointer. Keep Alice's historical v1 write before
    // this move, then exercise Bob's v2 copy-on-write update and delete.
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v2_schema,
    )
    .await;

    let bob =
        jazz_testkit::connect(server.make_client_context_for_user(
            v2_schema,
            test_user_id("bob-table-rename-copy-on-write"),
        ))
        .await
        .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;

    let transaction_id = bob
        .update(
            row_id,
            vec![
                (
                    "email".to_string(),
                    Value::Text("alice+updated@example.com".to_string()),
                ),
                (
                    "v2_marker".to_string(),
                    Value::Text("written-by-v2".to_string()),
                ),
            ],
        )
        .expect("bob updates renamed row");
    support::wait_for_edge_txs(
        &bob,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let rows_after_update = wait_for_query(
        &bob,
        jazz::query::Query::from("people"),
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
                        Value::Text("written-by-v2".to_string()),
                    ])
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows_after_update.len(), 1);

    let transaction_id = bob.delete(row_id).expect("bob deletes renamed row");
    support::wait_for_edge_txs(
        &bob,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let rows_after_delete = wait_for_query(
        &bob,
        jazz::query::Query::from("people"),
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
    tokio::task::LocalSet::new()
        .run_until(table_rename_join_query_translates_join_target_on_old_branch_impl())
        .await
}

async fn table_rename_join_query_translates_join_target_on_old_branch_impl() {
    let server = JazzServer::start().await;
    let v1_schema = table_rename_join_schema_v1();
    let v2_schema = table_rename_join_schema_v2();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
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

    let alice = jazz_testkit::connect(
        server.make_client_context_for_user(v1_schema, test_user_id("alice-join-rename")),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&alice, "posts", Duration::from_secs(30)).await;

    let author_id = jazz::tools::ObjectId::new();
    // `people.id` is normalized to the row identity by the flat-join planner.
    // Make the authored v1 row identity match the foreign key that the post
    // will use, as the v2 query's correlation contract requires.
    let (_, _, user_tx) = alice
        .insert_with_id(
            "users",
            *author_id.uuid(),
            table_rename_join_user_values(author_id, "Alice"),
        )
        .expect("alice creates v1 user");
    let post_id = jazz::tools::ObjectId::new();
    let (_, _, post_tx) = alice
        .insert_with_id(
            "posts",
            *post_id.uuid(),
            table_rename_join_post_values(post_id, author_id, "Hello from v1"),
        )
        .expect("alice creates v1 post");
    support::wait_for_edge_txs(
        &alice,
        &[
            user_tx.expect("ordinary mutation commits immediately"),
            post_tx.expect("ordinary mutation commits immediately"),
        ],
    )
    .await;

    let bob = jazz_testkit::connect(
        server.make_client_context_for_user(v2_schema, test_user_id("bob-join-rename")),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&bob, "posts", Duration::from_secs(30)).await;

    let query = Query::from("posts").flat_join("people", "posts.author_id", "people.id");
    let rows = wait_for_query_results(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob join sees v1 post author through table rename",
        |rows| (rows.len() == 1).then_some(rows),
    )
    .await;

    assert_eq!(
        rows[0]
            .fields
            .iter()
            .map(|field| field.value.clone())
            .collect::<Vec<_>>(),
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

/// A v2 reader reconstructs an array relation from canonical v1 rows after
/// the root table was renamed from `users` to `people`.
///
/// alice ──v1 user + post──► edge ──canonical versions──► bob(v2)
///
/// `id` is the public spelling of Jazz's row UUID in correlations, so Alice
/// supplies the same UUID for the row and the foreign-key value. The separate
/// physical row identity must not be confused with an arbitrary user cell.
#[tokio::test]
async fn table_rename_fk_array_lookup_finds_related_rows_on_old_branch() {
    tokio::task::LocalSet::new()
        .run_until(table_rename_fk_array_lookup_finds_related_rows_on_old_branch_impl())
        .await
}

async fn table_rename_fk_array_lookup_finds_related_rows_on_old_branch_impl() {
    let server = JazzServer::start().await;
    let v1_schema = table_rename_join_schema_v1();
    let v2_schema = table_rename_join_schema_v2();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
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

    let alice = jazz_testkit::connect(
        server.make_client_context_for_user(v1_schema, test_user_id("alice-array-rename")),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&alice, "posts", Duration::from_secs(30)).await;

    let author_id = jazz::tools::ObjectId::new();
    let (author_row_id, _, user_tx) = alice
        .insert_with_id(
            "users",
            *author_id.uuid(),
            table_rename_join_user_values(author_id, "Alice"),
        )
        .expect("alice creates v1 user");
    let post_id = jazz::tools::ObjectId::new();
    let (_, _, post_tx) = alice
        .insert(
            "posts",
            table_rename_join_post_values(post_id, author_id, "Alice post"),
        )
        .expect("alice creates v1 post");
    support::wait_for_edge_txs(
        &alice,
        &[
            user_tx.expect("ordinary mutation commits immediately"),
            post_tx.expect("ordinary mutation commits immediately"),
        ],
    )
    .await;

    let bob = jazz_testkit::connect(
        server.make_client_context_for_user(v2_schema, test_user_id("bob-array-rename")),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&bob, "posts", Duration::from_secs(30)).await;

    // INV-QUERY-29: this lookup intentionally selects every related post, so
    // preserve that unbounded query at the public query boundary.
    let query = Query::from("people").array_subquery(ArraySubquery::new(
        "posts",
        "posts",
        "author_id",
        "people.id",
    ));
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
    tokio::task::LocalSet::new().run_until(local_join_query_uses_current_permissions_for_joined_provenance_after_lens_transform_impl()).await
}

async fn local_join_query_uses_current_permissions_for_joined_provenance_after_lens_transform_impl()
{
    let server = JazzServer::start().await;
    let legacy_schema = legacy_join_provenance_schema();
    let current_schema = current_join_provenance_permission_schema();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
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
        .with_user_id(test_user_id("join-provenance-admin"))
        .as_admin()
        .ready_on("users", Duration::from_secs(30))
        .connect()
        .await;
    wait_for_edge_query_ready(&admin, "posts", Duration::from_secs(30)).await;

    let (_, _, user_tx) = admin
        .insert("users", legacy_join_provenance_user_values("bob"))
        .expect("admin creates legacy user");
    let (_, _, post_tx) = admin
        .insert(
            "posts",
            legacy_join_provenance_post_values("bob", "Bob private post"),
        )
        .expect("admin creates legacy post");
    support::wait_for_edge_txs(
        &admin,
        &[
            user_tx.expect("ordinary mutation commits immediately"),
            post_tx.expect("ordinary mutation commits immediately"),
        ],
    )
    .await;

    let current_admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(current_schema.clone())
        .with_user_id(test_user_id("join-provenance-current-admin"))
        .as_admin()
        .ready_on("users", Duration::from_secs(30))
        .connect()
        .await;
    wait_for_edge_query_ready(&current_admin, "posts", Duration::from_secs(30)).await;
    let (second_post_id, _, transaction_id) = current_admin
        .insert(
            "posts",
            row_input!(
                "owner_name" => Value::Text("bob".to_owned()),
                "title" => Value::Text("Bob second private post".to_owned()),
                "viewer_name" => Value::Text(test_user_id("bob"))
            ),
        )
        .expect("admin creates current-schema post");
    support::wait_for_edge_txs(
        &current_admin,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(current_schema.clone())
        .with_user_id(test_user_id("alice"))
        .as_user()
        .ready_on("users", Duration::from_secs(30))
        .connect()
        .await;
    wait_for_edge_query_ready(&alice, "posts", Duration::from_secs(30)).await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(current_schema)
        .with_user_id(test_user_id("bob"))
        .as_user()
        .ready_on("users", Duration::from_secs(30))
        .connect()
        .await;
    wait_for_edge_query_ready(&bob, "posts", Duration::from_secs(30)).await;

    let query = Query::from("users").flat_join("posts", "users.name", "posts.owner_name");

    let bob_rows = wait_for_query_results(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees joined row after provenance lens applies current permissions",
        |rows| (rows.len() == 2).then_some(rows),
    )
    .await;
    let bob_keys = bob_rows
        .iter()
        .map(|row| row.key.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        bob_keys.len(),
        2,
        "one root joined to two posts must retain two opaque occurrences"
    );
    let transformed_key = bob_rows
        .iter()
        .find(|row| {
            row.fields
                .iter()
                .any(|field| field.value == Value::Text("Bob private post".to_owned()))
        })
        .expect("lens-transformed legacy post result")
        .key
        .clone();

    let transaction_id = bob
        .update(
            second_post_id,
            vec![("viewer_name".to_owned(), Value::Text(test_user_id("alice")))],
        )
        .expect("move one joined occurrence to Alice's policy scope");
    support::wait_for_edge_txs(
        &bob,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let retained_bob_rows = wait_for_query_results(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "Bob retains only the lens-transformed joined occurrence",
        |rows| (rows.len() == 1).then_some(rows),
    )
    .await;
    assert_eq!(retained_bob_rows[0].key, transformed_key);

    let alice_rows = wait_for_query_results(
        &alice,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "Alice remains denied the lens-transformed joined occurrence",
        Some,
    )
    .await;
    assert!(alice_rows.iter().all(|row| row.key != transformed_key));

    admin.shutdown().await.expect("shutdown admin");
    current_admin
        .shutdown()
        .await
        .expect("shutdown current admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Exercises that file-like large blob values use the ordinary row/value
/// permissions on their table. Alice can insert and read her blob row, Bob's
/// query is filtered by the same table SELECT policy, and Mallory's spoofed
/// owner insert is rejected by the same table INSERT check.
///
/// alice --insert blob asset--> server --row policy--> alice sees handle, then hydrates
/// bob --query assets---------> server --row policy--x empty
/// mallory --spoof owner-----> server --row policy--x rejected
#[tokio::test]
async fn multi_hop_table_renames_and_column_rename() {
    tokio::task::LocalSet::new()
        .run_until(multi_hop_table_renames_and_column_rename_impl())
        .await
}

async fn multi_hop_table_renames_and_column_rename_impl() {
    let server = JazzServer::start().await;
    let v1_schema = multi_hop_table_rename_schema_v1();
    let v2_schema = multi_hop_table_rename_schema_v2();
    let v3_schema = multi_hop_table_rename_schema_v3();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
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
    let alice = jazz_testkit::connect(
        server.make_client_context_for_user(v1_schema, test_user_id("alice-multi-table-rename")),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;
    let alice_id = jazz::tools::ObjectId::new();
    let (alice_row_id, _, transaction_id) = alice
        .insert(
            "users",
            multi_hop_table_rename_values_v1(alice_id, "alice@example.com"),
        )
        .expect("alice creates v1 user");
    support::wait_for_edge_txs(
        &alice,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v2_schema,
    )
    .await;
    let bob = jazz_testkit::connect(
        server.make_client_context_for_user(v2_schema, test_user_id("bob-multi-table-rename")),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;
    let bob_id = jazz::tools::ObjectId::new();
    let (bob_row_id, _, transaction_id) = bob
        .insert(
            "people",
            multi_hop_table_rename_values_v2(bob_id, "bob@example.com"),
        )
        .expect("bob creates v2 person");
    support::wait_for_edge_txs(
        &bob,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v3_schema,
    )
    .await;
    let carol =
        jazz_testkit::connect(server.make_client_context_for_user(
            v3_schema.clone(),
            test_user_id("carol-multi-table-rename"),
        ))
        .await
        .expect("connect carol");
    wait_for_edge_query_ready(&carol, "members", Duration::from_secs(30)).await;
    let carol_id = jazz::tools::ObjectId::new();
    let (carol_row_id, _, transaction_id) = carol
        .insert(
            "members",
            multi_hop_table_rename_values_v3(carol_id, "carol@example.com"),
        )
        .expect("carol creates v3 member");
    support::wait_for_edge_txs(
        &carol,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let rows = wait_for_query(
        &carol,
        jazz::query::Query::from("members"),
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
    tokio::task::LocalSet::new()
        .run_until(removed_table_then_readded_does_not_resurface_old_rows_impl())
        .await
}

async fn removed_table_then_readded_does_not_resurface_old_rows_impl() {
    let server = JazzServer::start().await;
    let v1_schema = removed_readded_schema_v1();
    let v2_schema = removed_readded_schema_v2();
    let v3_schema = removed_readded_schema_v3();

    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
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

    let alice = jazz_testkit::connect(
        server.make_client_context_for_user(v1_schema, test_user_id("alice-removed-readded-v1")),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let alice_id = jazz::tools::ObjectId::new();
    let (alice_row_id, _, transaction_id) = alice
        .insert(
            "users",
            removed_readded_values_v1(alice_id, "Alice Old Lineage"),
        )
        .expect("alice creates v1 user");
    support::wait_for_edge_txs(
        &alice,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v3_schema,
    )
    .await;

    let bob = jazz_testkit::connect(
        server.make_client_context_for_user(v3_schema, test_user_id("bob-removed-readded-v3")),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let bob_id = jazz::tools::ObjectId::new();
    let (bob_row_id, _, transaction_id) = bob
        .insert(
            "users",
            removed_readded_values_v3(bob_id, "Bob New Lineage", "bob@example.com"),
        )
        .expect("bob creates v3 user");
    support::wait_for_edge_txs(
        &bob,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let rows = wait_for_query(
        &bob,
        jazz::query::Query::from("users"),
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
    tokio::task::LocalSet::new()
        .run_until(column_addition_old_client_can_read_new_rows_impl())
        .await
}

async fn column_addition_old_client_can_read_new_rows_impl() {
    let server = JazzServer::start().await;
    let target_schema = schema_v2();

    // Seed the server with both schemas and the v1<->v2 lens before clients connect.
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
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
    let bob = jazz_testkit::connect(
        server.make_client_context_for_user(schema_v2(), test_user_id("bob-backward")),
    )
    .await
    .expect("connect bob");

    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz::tools::ObjectId::new();
    let user_email = "bob@example.com";
    let (user_obj_id, _, _) = bob
        .insert(
            "users",
            user_values_v2(user_id_value, "Bob Backward", user_email),
        )
        .expect("bob creates user");

    wait_for_query(
        &bob,
        jazz::query::Query::from("users"),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob's v2 user settled at edge",
        |rows| (rows.len() == 1 && rows[0].0 == user_obj_id).then_some(rows),
    )
    .await;

    // === Alice connects with v1, queries — should see Bob's row without email ===
    let alice = jazz_testkit::connect(
        server.make_client_context_for_user(schema_v1(), test_user_id("alice-backward")),
    )
    .await
    .expect("connect alice");

    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let alice_rows = wait_for_query(
        &alice,
        jazz::query::Query::from("users"),
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
    tokio::task::LocalSet::new()
        .run_until(keeps_authorization_through_v1_head_impl())
        .await
}

async fn keeps_authorization_through_v1_head_impl() {
    let server = JazzServer::start().await;
    let query = jazz::query::Query::from("users");
    let v1_schema = schema_v1();
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
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

    let alice = jazz_testkit::connect(
        server.make_client_context_for_user(schema_v1(), test_user_id("alice-v1-head")),
    )
    .await
    .expect("connect alice");

    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz::tools::ObjectId::new();
    let (user_obj_id, _, transaction_id) = alice
        .insert("users", user_values_v1(user_id_value, "Alice Through Lens"))
        .expect("alice creates user after v1 permissions publish");
    support::wait_for_edge_txs(
        &alice,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

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
        &[v1_schema, schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await
    .expect("push catalogue after v1 permissions head");

    let bob = jazz_testkit::connect(
        server.make_client_context_for_user(schema_v2(), test_user_id("bob-v2-head")),
    )
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
