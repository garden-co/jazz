//! Deploying through the real HTTP router a schema that holds tables with
//! Jazz's reserved `__e2ee_` prefix: the server accepts only Jazz's own
//! system tables, whatever the backend's loader did.
//!
//! ```text
//! raw deploy body ──POST /bundles──► server: system tables checked ──► refused
//! ```
use super::super::{ServerBuilder, StorageBackend, routes::create_router};
use crate::middleware::AuthConfig;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use jazz::tools::AppId;
use jazz::tools::public_schema::{ColumnType, SchemaBuilder, SchemaHash, TableSchema};
use serde_json::{Value, json};
use tower::ServiceExt;

fn app_id() -> AppId {
    AppId::from_name("system-table-deployment-tests")
}

async fn router() -> Router {
    let server = ServerBuilder::new(app_id())
        .with_auth_config(AuthConfig {
            admin_secret: Some("test-admin".into()),
            ..Default::default()
        })
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .unwrap();
    create_router(server.state.clone())
}

async fn publish(router: &Router, body: Value) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/apps/{}/admin/bundles", app_id()))
                .header("X-Jazz-Admin-Secret", "test-admin")
                .header("Content-Type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value =
        serde_json::from_slice(&bytes).unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes)));
    (status, value)
}

/// A raw deploy request that defines a table with the reserved prefix
/// which is not one of Jazz's system tables is refused, while the same
/// schema under an ordinary name deploys.
///
/// ```text
/// mallory ──{ __e2ee_notes }──► server ✗ reserved prefix
/// mallory ──{ notes }─────────► server ✓
/// ```
#[tokio::test]
async fn a_raw_deploy_of_a_foreign_system_table_is_refused() {
    let router = router().await;
    let body = |table: &str| {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder(table).column("body", ColumnType::Text))
            .build();
        let hash = SchemaHash::compute(&schema).to_string();
        json!({
            "targetSchemaHash": hash,
            "schemas": [{ "hash": hash, "schema": schema }],
            "migrations": [],
            "permissions": {},
        })
    };
    let (status, refusal) = publish(&router, body("__e2ee_notes")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("reserved prefix"),
        "{refusal}"
    );
    let (status, deployed) = publish(&router, body("notes")).await;
    assert_eq!(status, StatusCode::OK, "{deployed}");
}
