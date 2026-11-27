use super::super::super::{ServerBuilder, StorageBackend, routes::create_router};
use crate::middleware::AuthConfig;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use jazz::tools::AppId;
use serde_json::Value;
use tower::ServiceExt;

pub(super) async fn router(app_id: AppId) -> Router {
    let server = ServerBuilder::new(app_id)
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

pub(super) async fn publish(app_id: AppId, router: &Router, body: Value) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/apps/{app_id}/admin/bundles"))
                .header("X-Jazz-Admin-Secret", "test-admin")
                .header("Content-Type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| serde_json::json!(String::from_utf8_lossy(&bytes)));
    (status, value)
}

#[allow(dead_code)]
pub(super) async fn found_local_first(
    app_id: AppId,
    router: &Router,
    audience: Option<String>,
    subject: &str,
) -> StatusCode {
    let mut body = serde_json::json!({
        "identity": {
            "issuer": jazz::tools::identity::DEVICE_ACCOUNT_ISSUER,
            "subject": subject,
        },
    });
    if let Some(audience) = audience {
        body["audience"] = audience.into();
    }
    router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/apps/{app_id}/admin/accounts/found-local-first"))
                .header("X-Jazz-Admin-Secret", "test-admin")
                .header("Content-Type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}
