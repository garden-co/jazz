//! HTTP and WebSocket routes for the Jazz server.
//!
//! Split into submodules so each piece is independently navigable:
//! - [`websocket`] — engine wire-frame WebSocket boundary
//! - [`http`] — HTTP endpoint handlers and their request/response types
//! - [`utils`] — parser/validator helpers used by both
//!
//! The router builder [`create_router`] re-exports unchanged from this module
//! so existing callers (`server::routes::create_router`) continue to resolve.

mod http;
mod utils;
mod websocket;

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, OriginalUri, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use tower_http::cors::{AllowHeaders, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::server::ServerState;

use http::{
    admin_subscription_introspection_handler, health_handler, internal_shutdown_handler,
    permissions_handler, permissions_head_handler, publish_migration_handler,
    publish_permissions_handler, publish_schema_handler, schema_connectivity_handler,
    schema_handler, schema_hashes_handler,
};
use utils::parse_app_id_param;
use websocket::ws_handler;

/// Admin catalogue uploads are ordinary JSON requests and must be bounded
/// before an extractor buffers their bodies. Eight MiB accommodates large
/// schemas and migration bundles without leaving an unauthenticated memory
/// amplification path.
const MAX_ADMIN_REQUEST_BODY_BYTES: usize = 8 << 20;

async fn app_id_gate(
    State(state): State<Arc<ServerState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let path = request
        .extensions()
        .get::<OriginalUri>()
        .map(|uri| uri.path())
        .unwrap_or_else(|| request.uri().path());
    let Some(app_id_text) = path
        .strip_prefix("/apps/")
        .and_then(|path| path.split('/').next())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(app_id) = parse_app_id_param(app_id_text) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    if app_id != state.app_id {
        return StatusCode::NOT_FOUND.into_response();
    }

    next.run(request).await
}

async fn app_shutdown_gate(
    State(state): State<Arc<ServerState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let Some(_guard) = state.shutdown.try_enter_app_request() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(jazz::tools::transport_error::ErrorResponse::internal(
                "server is shutting down".to_string(),
            )),
        )
            .into_response();
    };

    next.run(request).await
}

pub fn create_router(state: Arc<ServerState>) -> Router {
    let admin_routes = Router::new()
        .route("/schemas", post(publish_schema_handler))
        .route("/schema-connectivity", get(schema_connectivity_handler))
        .route("/permissions/head", get(permissions_head_handler))
        .route(
            "/permissions",
            get(permissions_handler).post(publish_permissions_handler),
        )
        .route("/migrations", post(publish_migration_handler))
        .route(
            "/introspection/subscriptions",
            get(admin_subscription_introspection_handler),
        )
        .layer(DefaultBodyLimit::max(MAX_ADMIN_REQUEST_BODY_BYTES));
    let traced_routes = Router::new()
        .route("/ws", axum::routing::any(ws_handler))
        .route("/schema/{hash}", get(schema_handler))
        .route("/schemas", get(schema_hashes_handler))
        .nest("/admin", admin_routes)
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            app_shutdown_gate,
        ))
        .route_layer(middleware::from_fn_with_state(state.clone(), app_id_gate))
        .layer(TraceLayer::new_for_http());

    Router::new()
        .route("/health", get(health_handler))
        .route("/internal/shutdown", post(internal_shutdown_handler))
        .nest("/apps/{app_id}", traced_routes)
        // `*` does not authorize `Authorization` in a browser CORS preflight.
        // Mirror the requested names while retaining the intentionally
        // credential-free permissive policy.
        .layer(CorsLayer::permissive().allow_headers(AllowHeaders::mirror_request()))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::http::*;
    use super::*;

    use axum::extract::{Path, Query};
    use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
    use axum::response::Json;

    use jazz::tools::AppId;
    use jazz::tools::public_schema::{SchemaHash, TableName};
    use jazz::tools::schema_lens::LensOp;
    use std::time::Duration;

    use crate::server::catalogue::ConnectionSchemaDiagnostics;
    use axum::body;
    use axum::routing::{get, post};
    use futures::{SinkExt as _, StreamExt as _};
    use jazz::ids::AuthorSubject;
    use jazz::tools::public_schema::{
        ColumnType, PolicyExpr, Schema, SchemaBuilder, TablePolicies, TableSchema,
    };
    use serde_json::Value;
    use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
    use tower::ServiceExt;

    use crate::middleware::AuthConfig;
    use crate::server::{EdgeUpstreamHealth, ServerBuilder, ServerState, StorageBackend};
    use jazz::wire::{
        FEATURE_STRUCTURED_ERRORS, FEATURE_SYNC_MESSAGE_PAYLOAD, WireFrame, WireHello,
        WirePeerRole, decode_frame, encode_frame,
    };

    fn test_auth_config() -> AuthConfig {
        AuthConfig {
            backend_secret: None,
            admin_secret: Some("admin-secret".to_string()),
            allow_local_first_auth: true,
            jwks_url: None,
            ..Default::default()
        }
    }

    /// Spin up a server state backed by an in-process runtime.
    /// `backend_secret` is wired into `AuthConfig` so tests can authenticate
    /// via the backend-secret WS handshake without needing JWT setup.
    async fn make_sync_test_state(backend_secret: &str) -> Arc<ServerState> {
        let auth_config = AuthConfig {
            backend_secret: Some(backend_secret.to_string()),
            admin_secret: None,
            allow_local_first_auth: false,
            jwks_url: None,
            ..Default::default()
        };

        ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(auth_config)
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build sync test state")
            .state
    }

    async fn make_state_with_schema(
        schema: jazz::tools::public_schema::Schema,
    ) -> Arc<ServerState> {
        ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(test_auth_config())
            .with_storage(StorageBackend::InMemory)
            .with_schema(schema)
            .build()
            .await
            .expect("build state with schema")
            .state
    }

    async fn make_edge_state_with_schema(
        schema: jazz::tools::public_schema::Schema,
        upstream_url: String,
    ) -> Arc<ServerState> {
        ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(test_auth_config())
            .with_upstream_url(upstream_url)
            .with_storage(StorageBackend::InMemory)
            .with_schema(schema)
            .build()
            .await
            .expect("build edge state with schema")
            .state
    }

    fn make_test_router(state: Arc<ServerState>) -> axum::Router {
        create_router(state)
    }

    async fn publish_schema_for_test(app: &axum::Router, schema: Schema) {
        let schema_json = serde_json::to_value(&schema).expect("schema request json");
        assert!(
            schema_json.get("tables").is_some(),
            "admin schema requests retain the public Schema envelope"
        );
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/schemas"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(
                        serde_json::json!({ "schema": schema_json }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("publish schema through admin route");
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    async fn post_internal_shutdown(
        app: axum::Router,
        admin_secret: Option<&str>,
    ) -> axum::response::Response {
        let mut builder = axum::http::Request::builder()
            .method("POST")
            .uri("/internal/shutdown");
        if let Some(admin_secret) = admin_secret {
            builder = builder.header("X-Jazz-Admin-Secret", admin_secret);
        }
        app.oneshot(builder.body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap()
    }

    fn test_app_id_text() -> String {
        AppId::from_name("test-app").to_string()
    }

    fn test_app_route(path: &str) -> String {
        format!(
            "/apps/{}/{}",
            test_app_id_text(),
            path.trim_start_matches('/')
        )
    }

    fn named_test_app_route(path: &str) -> String {
        format!("/apps/test-app/{}", path.trim_start_matches('/'))
    }

    #[tokio::test]
    async fn cors_preflight_mirrors_requested_auth_headers_without_credentials() {
        let app = make_test_router(make_sync_test_state("test-backend-secret").await);
        let requested_headers = "authorization, x-jazz-admin-secret";

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::OPTIONS)
                    .uri(test_app_route("/admin/schemas"))
                    .header(header::ORIGIN, "http://localhost:3000")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .header(header::ACCESS_CONTROL_REQUEST_HEADERS, requested_headers)
                    .body(axum::body::Body::empty())
                    .expect("valid CORS preflight"),
            )
            .await
            .expect("CORS layer handles preflight");

        assert!(response.status().is_success());
        assert_eq!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
                .and_then(|value| value.to_str().ok()),
            Some(requested_headers),
            "preflight must explicitly allow the requested authorization headers"
        );
        assert!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_CREDENTIALS)
                .is_none(),
            "the shared server CORS policy must not enable cookie credentials"
        );

        let vary = response
            .headers()
            .get(header::VARY)
            .and_then(|value| value.to_str().ok())
            .expect("CORS response must vary by preflight inputs");
        for required in [
            "origin",
            "access-control-request-method",
            "access-control-request-headers",
        ] {
            assert!(
                vary.split(',')
                    .any(|value| value.trim().eq_ignore_ascii_case(required)),
                "Vary must include {required}; got {vary:?}"
            );
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct ForwardedAdminRequest {
        method: String,
        path: String,
        admin_secret: Option<String>,
        body: Option<Value>,
    }

    #[tokio::test]
    async fn internal_shutdown_requires_configured_admin_secret() {
        let auth_config = AuthConfig {
            admin_secret: None,
            allow_local_first_auth: true,
            ..Default::default()
        };
        let state = ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(auth_config)
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build server without admin secret")
            .state;

        let response = post_internal_shutdown(make_test_router(state), Some("admin-secret")).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn internal_shutdown_requires_admin_secret_header() {
        let state = make_state_with_schema(Schema::new()).await;
        let response = post_internal_shutdown(make_test_router(state), None).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn internal_shutdown_rejects_wrong_admin_secret() {
        let state = make_state_with_schema(Schema::new()).await;
        let response = post_internal_shutdown(make_test_router(state), Some("wrong-secret")).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn internal_shutdown_accepts_valid_admin_secret_and_marks_health_unhealthy() {
        let state = make_state_with_schema(Schema::new()).await;
        let app = make_test_router(state.clone());

        let response = post_internal_shutdown(app.clone(), Some("admin-secret")).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("shutdown body");
        let json: Value = serde_json::from_slice(&body).expect("shutdown json");
        assert_eq!(json["status"].as_str(), Some("shutting_down"));

        let repeated = post_internal_shutdown(app.clone(), Some("admin-secret")).await;
        assert_eq!(repeated.status(), StatusCode::ACCEPTED);
        let repeated_body = body::to_bytes(repeated.into_body(), usize::MAX)
            .await
            .expect("repeated shutdown body");
        let repeated_json: Value = serde_json::from_slice(&repeated_body).expect("repeated json");
        assert_eq!(
            repeated_json["status"].as_str(),
            Some("already_shutting_down")
        );

        let health = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/health")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::SERVICE_UNAVAILABLE);
        let health_body = body::to_bytes(health.into_body(), usize::MAX)
            .await
            .expect("health body");
        let health_json: Value = serde_json::from_slice(&health_body).expect("health json");
        assert_eq!(health_json["status"].as_str(), Some("shutting_down"));
        assert_eq!(health_json["phase"].as_str(), Some("shutting_down"));

        assert_eq!(
            state.shutdown.phase(),
            crate::server::ShutdownPhase::ShuttingDown
        );
    }

    #[tokio::test]
    async fn terminal_edge_upstream_failure_is_visible_in_health() {
        let state = make_state_with_schema(Schema::new()).await;
        state.set_edge_upstream_health(EdgeUpstreamHealth::Failed {
            reason: "authority rejected edge credentials".to_owned(),
        });
        let health = make_test_router(state)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/health")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(health.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = body::to_bytes(health.into_body(), usize::MAX)
            .await
            .expect("health body");
        let json: Value = serde_json::from_slice(&body).expect("health json");
        assert_eq!(json["status"].as_str(), Some("unhealthy"));
        assert_eq!(json["component"].as_str(), Some("edge_upstream"));
        assert!(
            json["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("rejected edge credentials"))
        );
    }

    #[tokio::test]
    async fn shutdown_rejects_new_app_scoped_http_requests_but_keeps_internal_routes_available() {
        let state = make_state_with_schema(Schema::new()).await;
        let app = make_test_router(state);

        let shutdown = post_internal_shutdown(app.clone(), Some("admin-secret")).await;
        assert_eq!(shutdown.status(), StatusCode::ACCEPTED);

        let app_scoped = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/schemas"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(app_scoped.status(), StatusCode::SERVICE_UNAVAILABLE);

        let repeated = post_internal_shutdown(app.clone(), Some("admin-secret")).await;
        assert_eq!(repeated.status(), StatusCode::ACCEPTED);

        let health = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/health")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn app_routes_accept_canonical_uuid_app_id() {
        let state = make_state_with_schema(Schema::new()).await;
        let app = make_test_router(state);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/schemas"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn app_routes_accept_name_alias_app_id() {
        let state = make_state_with_schema(Schema::new()).await;
        let app = make_test_router(state);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(named_test_app_route("/schemas"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn app_routes_reject_mismatched_app_id() {
        let state = make_state_with_schema(Schema::new()).await;
        let app = make_test_router(state);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/apps/other-app/schemas")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn schema_handler_requires_admin_secret() {
        let state = ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(AuthConfig {
                backend_secret: None,
                admin_secret: Some("admin-secret".to_string()),
                allow_local_first_auth: false,
                jwks_url: None,
                ..Default::default()
            })
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build server state")
            .state;

        let app = create_router(state);

        let placeholder_hash = "0000000000000000000000000000000000000000000000000000000000000000";
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(&format!("/schema/{placeholder_hash}")))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response_with_admin = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(&format!("/schema/{placeholder_hash}")))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response_with_admin.status(), StatusCode::NOT_FOUND);

        let hashes_without_admin = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/schemas"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(hashes_without_admin.status(), StatusCode::UNAUTHORIZED);

        let root_schema = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/schema/{placeholder_hash}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(root_schema.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn schema_handlers_return_hashes_and_requested_schema() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let state = make_state_with_schema(schema.clone()).await;

        let app = make_test_router(state);

        let hashes_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/schemas"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(hashes_response.status(), StatusCode::OK);
        let hashes_body = body::to_bytes(hashes_response.into_body(), usize::MAX)
            .await
            .expect("hashes body");
        let hashes_json: Value = serde_json::from_slice(&hashes_body).expect("hashes json");
        let expected_hash = schema_hash.to_string();
        assert_eq!(
            hashes_json["hashes"][0].as_str(),
            Some(expected_hash.as_str())
        );
        assert_eq!(
            hashes_json["schemas"][0]["hash"].as_str(),
            Some(expected_hash.as_str())
        );
        assert!(hashes_json["schemas"][0].get("publishedAt").is_some());

        let schema_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(&format!("/schema/{}", schema_hash)))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(schema_response.status(), StatusCode::OK);
        let schema_body = body::to_bytes(schema_response.into_body(), usize::MAX)
            .await
            .expect("schema body");
        let schema_json: Value = serde_json::from_slice(&schema_body).expect("schema json");
        let expected_schema_json = serde_json::to_value(&schema).expect("expected schema json");
        assert_eq!(schema_json["schema"], expected_schema_json);
        assert!(schema_json["schema"].get("tables").is_some());
        assert!(schema_json["schema"].get("users").is_none());
        assert!(schema_json.get("publishedAt").is_some());

        let bad_hash_response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/schema/invalid"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad_hash_response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn publish_schema_rejects_obsolete_bare_table_map() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
            .build();
        let legacy_tables = schema
            .iter()
            .map(|(name, table)| (*name, table.clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        let app = make_test_router(make_state_with_schema(schema).await);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/schemas"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(
                        serde_json::json!({ "schema": legacy_tables }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("publish obsolete schema shape");

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn edge_upstream_forwarding_proxies_schema_and_permissions_requests() {
        use std::sync::{Arc, Mutex};

        let forwarded = Arc::new(Mutex::new(Vec::<ForwardedAdminRequest>::new()));
        let forwarded_for_router = forwarded.clone();
        let expected_hash =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        let authority_routes = axum::Router::new()
            .route(
                &test_app_route("/schemas"),
                get({
                    let forwarded = forwarded_for_router.clone();
                    let expected_hash = expected_hash.clone();
                    move |headers: HeaderMap| {
                        let forwarded = forwarded.clone();
                        let expected_hash = expected_hash.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "GET".to_string(),
                                path: test_app_route("/schemas"),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: None,
                            });
                            Json(serde_json::json!({ "hashes": [expected_hash] }))
                        }
                    }
                }),
            )
            .route(
                &test_app_route("/schema/{hash}"),
                get({
                    let forwarded = forwarded_for_router.clone();
                    move |Path(hash): Path<String>, headers: HeaderMap| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "GET".to_string(),
                                path: test_app_route(&format!("/schema/{hash}")),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: None,
                            });
                            Json(serde_json::json!({
                                "users": {
                                    "columns": [
                                        { "name": "id", "column_type": { "type": "Uuid" }, "nullable": false },
                                        { "name": "name", "column_type": { "type": "Text" }, "nullable": false }
                                    ]
                                }
                            }))
                        }
                    }
                }),
            )
            .route(
                &test_app_route("/admin/schemas"),
                post({
                    let forwarded = forwarded_for_router.clone();
                    let expected_hash = expected_hash.clone();
                    move |headers: HeaderMap, body: Json<Value>| {
                        let forwarded = forwarded.clone();
                        let expected_hash = expected_hash.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "POST".to_string(),
                                path: test_app_route("/admin/schemas"),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: Some(body.0),
                            });
                            (
                                StatusCode::CREATED,
                                Json(serde_json::json!({
                                    "objectId": "11111111-1111-1111-1111-111111111111",
                                    "hash": expected_hash,
                                })),
                            )
                        }
                    }
                }),
            )
            .route(
                &test_app_route("/admin/migrations"),
                post({
                    let forwarded = forwarded_for_router.clone();
                    move |headers: HeaderMap, body: Json<Value>| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "POST".to_string(),
                                path: test_app_route("/admin/migrations"),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: Some(body.0),
                            });
                            (
                                StatusCode::CREATED,
                                Json(serde_json::json!({
                                    "objectId": "22222222-2222-2222-2222-222222222222",
                                    "fromHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                    "toHash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                })),
                            )
                        }
                    }
                }),
            )
            .route(
                &test_app_route("/admin/schema-connectivity"),
                get({
                    let forwarded = forwarded_for_router.clone();
                    move |Query(params): Query<SchemaConnectivityParams>, headers: HeaderMap| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "GET".to_string(),
                                path: format!(
                                    "{}?fromHash={}&toHash={}",
                                    test_app_route("/admin/schema-connectivity"),
                                    params.from_hash, params.to_hash
                                ),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: None,
                            });
                            Json(serde_json::json!({
                                "connected": true,
                            }))
                        }
                    }
                }),
            )
            .route(
                &test_app_route("/admin/permissions/head"),
                get({
                    let forwarded = forwarded_for_router.clone();
                    move |headers: HeaderMap| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "GET".to_string(),
                                path: test_app_route("/admin/permissions/head"),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: None,
                            });
                            Json(serde_json::json!({
                                "head": {
                                    "schemaHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                    "version": 4,
                                    "parentBundleObjectId": "33333333-3333-3333-3333-333333333333",
                                    "bundleObjectId": "44444444-4444-4444-4444-444444444444"
                                }
                            }))
                        }
                    }
                }),
            )
            .route(
                &test_app_route("/admin/permissions"),
                get({
                    let forwarded = forwarded_for_router.clone();
                    move |headers: HeaderMap| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "GET".to_string(),
                                path: test_app_route("/admin/permissions"),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: None,
                            });
                            Json(serde_json::json!({
                                "head": {
                                    "schemaHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                    "version": 4,
                                    "parentBundleObjectId": "33333333-3333-3333-3333-333333333333",
                                    "bundleObjectId": "44444444-4444-4444-4444-444444444444"
                                },
                                "permissions": {
                                    "users": {
                                        "select": { "using": { "type": "True" } }
                                    }
                                }
                            }))
                        }
                    }
                }),
            )
            .route(
                &test_app_route("/admin/permissions"),
                post({
                    let forwarded = forwarded_for_router.clone();
                    move |headers: HeaderMap, body: Json<Value>| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "POST".to_string(),
                                path: test_app_route("/admin/permissions"),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: Some(body.0),
                            });
                            (
                                StatusCode::CONFLICT,
                                Json(serde_json::json!({
                                    "error": {
                                        "code": "bad_request",
                                        "message": "stale permissions parent"
                                    }
                                })),
                            )
                        }
                    }
                }),
            );
        let authority_app = axum::Router::new().nest("/authority-prefix", authority_routes);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind authority listener");
        let authority_addr = listener.local_addr().expect("authority local addr");
        let authority_task = tokio::spawn(async move {
            axum::serve(listener, authority_app)
                .await
                .expect("serve authority app");
        });

        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_edge_state_with_schema(
            schema.clone(),
            format!("http://{authority_addr}/authority-prefix"),
        )
        .await;
        let app = make_test_router(state);

        let schemas_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/schemas"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(schemas_response.status(), StatusCode::OK);

        let schema_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(&format!("/schema/{expected_hash}")))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(schema_response.status(), StatusCode::OK);

        let publish_schema_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/schemas"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(
                        serde_json::json!({ "schema": schema }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(publish_schema_response.status(), StatusCode::CREATED);

        let publish_migration_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/migrations"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "fromHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "toHash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                            "forward": [{
                                "table": "users",
                                "operations": [{
                                    "type": "rename",
                                    "column": "name",
                                    "value": "full_name"
                                }]
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(publish_migration_response.status(), StatusCode::CREATED);

        let permissions_head_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/admin/permissions/head"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(permissions_head_response.status(), StatusCode::OK);

        let schema_connectivity_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "{}?fromHash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&toHash=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        test_app_route("/admin/schema-connectivity")
                    ))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(schema_connectivity_response.status(), StatusCode::OK);

        let permissions_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/admin/permissions"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(permissions_response.status(), StatusCode::OK);

        let publish_permissions_response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/permissions"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "schemaHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "permissions": {
                                "users": {
                                    "select": { "using": { "type": "True" } }
                                }
                            },
                            "expectedParentBundleObjectId": "44444444-4444-4444-4444-444444444444"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(publish_permissions_response.status(), StatusCode::CONFLICT);

        let forwarded = forwarded.lock().unwrap().clone();
        assert_eq!(forwarded.len(), 8);
        assert!(
            forwarded
                .iter()
                .all(|request| request.admin_secret.as_deref() == Some("admin-secret"))
        );
        assert_eq!(forwarded[0].path, test_app_route("/schemas"));
        assert_eq!(
            forwarded[1].path,
            test_app_route(&format!("/schema/{expected_hash}"))
        );
        assert_eq!(forwarded[2].path, test_app_route("/admin/schemas"));
        assert_eq!(forwarded[3].path, test_app_route("/admin/migrations"));
        assert_eq!(forwarded[4].path, test_app_route("/admin/permissions/head"));
        assert_eq!(
            forwarded[5].path,
            format!(
                "{}?fromHash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&toHash=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                test_app_route("/admin/schema-connectivity")
            )
        );
        assert_eq!(forwarded[6].path, test_app_route("/admin/permissions"));
        assert_eq!(forwarded[7].path, test_app_route("/admin/permissions"));
        assert_eq!(
            forwarded[7]
                .body
                .as_ref()
                .and_then(|body| body.get("expectedParentBundleObjectId"))
                .and_then(Value::as_str),
            Some("44444444-4444-4444-4444-444444444444")
        );

        authority_task.abort();
    }

    #[tokio::test]
    async fn edge_catalogue_forwarding_rejects_invalid_admin_secret_before_upstream() {
        use std::sync::{Arc, Mutex};

        let forwarded_calls = Arc::new(Mutex::new(0usize));
        let forwarded_calls_for_router = forwarded_calls.clone();
        let authority_app = axum::Router::new().route(
            &test_app_route("/schemas"),
            get(move || {
                let forwarded_calls = forwarded_calls_for_router.clone();
                async move {
                    *forwarded_calls.lock().unwrap() += 1;
                    Json(serde_json::json!({ "hashes": [] }))
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind authority listener");
        let authority_addr = listener.local_addr().expect("authority local addr");
        let authority_task = tokio::spawn(async move {
            axum::serve(listener, authority_app)
                .await
                .expect("serve authority app");
        });

        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_edge_state_with_schema(schema, format!("http://{authority_addr}")).await;
        let app = make_test_router(state);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/schemas"))
                    .header("X-Jazz-Admin-Secret", "wrong-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(*forwarded_calls.lock().unwrap(), 0);

        authority_task.abort();
    }

    #[tokio::test]
    async fn edge_schema_connectivity_forwarding_encodes_reserved_query_values() {
        use std::sync::{Arc, Mutex};

        let forwarded = Arc::new(Mutex::new(Vec::<ForwardedAdminRequest>::new()));
        let forwarded_for_router = forwarded.clone();
        let authority_app = axum::Router::new().route(
            &test_app_route("/admin/schema-connectivity"),
            get(move |uri: Uri, headers: HeaderMap| {
                let forwarded = forwarded_for_router.clone();
                async move {
                    forwarded.lock().unwrap().push(ForwardedAdminRequest {
                        method: "GET".to_string(),
                        path: uri
                            .path_and_query()
                            .map(|path_and_query| path_and_query.as_str().to_string())
                            .unwrap_or_else(|| uri.path().to_string()),
                        admin_secret: headers
                            .get("X-Jazz-Admin-Secret")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_string),
                        body: None,
                    });
                    Json(serde_json::json!({ "connected": true }))
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind authority listener");
        let authority_addr = listener.local_addr().expect("authority local addr");
        let authority_task = tokio::spawn(async move {
            axum::serve(listener, authority_app)
                .await
                .expect("serve authority app");
        });

        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_edge_state_with_schema(schema, format!("http://{authority_addr}")).await;
        let app = make_test_router(state);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "{}?fromHash=aaa%26evil=1&toHash=bbb",
                        test_app_route("/admin/schema-connectivity")
                    ))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let forwarded = forwarded.lock().unwrap().clone();
        assert_eq!(forwarded.len(), 1);
        assert_eq!(forwarded[0].admin_secret.as_deref(), Some("admin-secret"));
        assert!(
            !forwarded[0].path.contains("&evil=1"),
            "reserved characters must remain inside fromHash, got {}",
            forwarded[0].path
        );
        let forwarded_url =
            reqwest::Url::parse(&format!("http://upstream.test{}", forwarded[0].path))
                .expect("forwarded URL should parse");
        let query_pairs: Vec<_> = forwarded_url.query_pairs().collect();
        assert_eq!(query_pairs.len(), 2);
        assert_eq!(query_pairs[0], ("fromHash".into(), "aaa&evil=1".into()));
        assert_eq!(query_pairs[1], ("toHash".into(), "bbb".into()));

        authority_task.abort();
    }

    #[tokio::test]
    async fn permissions_handlers_publish_linear_head_and_reject_stale_parent() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let state = make_state_with_schema(schema).await;
        let app = make_test_router(state.clone());

        let initial_head = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/admin/permissions/head"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(initial_head.status(), StatusCode::OK);
        let initial_body = body::to_bytes(initial_head.into_body(), usize::MAX)
            .await
            .expect("initial permissions head body");
        let initial_json: Value =
            serde_json::from_slice(&initial_body).expect("initial permissions head json");
        assert!(initial_json["head"].is_null());

        let first_request_body = serde_json::json!({
            "schemaHash": schema_hash.to_string(),
            "permissions": {
                "users": {
                    "select": { "using": { "type": "True" } }
                }
            }
        });
        let first_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/permissions"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(first_request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first_response.status(), StatusCode::CREATED);
        let first_body = body::to_bytes(first_response.into_body(), usize::MAX)
            .await
            .expect("first publish body");
        let first_json: Value = serde_json::from_slice(&first_body).expect("first publish json");
        let first_bundle_object_id = first_json["head"]["bundleObjectId"]
            .as_str()
            .expect("first bundle object id")
            .to_string();
        assert_eq!(first_json["head"]["version"].as_u64(), Some(1));
        assert_eq!(first_json["head"]["parentBundleObjectId"], Value::Null);

        let second_request_body = serde_json::json!({
            "schemaHash": schema_hash.to_string(),
            "permissions": {
                "users": {
                    "select": { "using": { "type": "False" } }
                }
            },
            "expectedParentBundleObjectId": first_bundle_object_id,
        });
        let second_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/permissions"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(second_request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second_response.status(), StatusCode::CREATED);
        let second_body = body::to_bytes(second_response.into_body(), usize::MAX)
            .await
            .expect("second publish body");
        let second_json: Value = serde_json::from_slice(&second_body).expect("second publish json");
        let second_bundle_object_id = second_json["head"]["bundleObjectId"]
            .as_str()
            .expect("second bundle object id")
            .to_string();
        assert_eq!(second_json["head"]["version"].as_u64(), Some(2));
        assert_eq!(
            second_json["head"]["parentBundleObjectId"].as_str(),
            Some(first_bundle_object_id.as_str())
        );

        let stale_request_body = serde_json::json!({
            "schemaHash": schema_hash.to_string(),
            "permissions": {
                "users": {
                    "select": { "using": { "type": "True" } }
                }
            },
            "expectedParentBundleObjectId": first_bundle_object_id,
        });
        let stale_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/permissions"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(stale_request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stale_response.status(), StatusCode::CONFLICT);

        let head_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/admin/permissions/head"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head_response.status(), StatusCode::OK);
        let head_body = body::to_bytes(head_response.into_body(), usize::MAX)
            .await
            .expect("current permissions head body");
        let head_json: Value =
            serde_json::from_slice(&head_body).expect("current permissions head json");
        assert_eq!(head_json["head"]["version"].as_u64(), Some(2));
        assert_eq!(
            head_json["head"]["bundleObjectId"].as_str(),
            Some(second_bundle_object_id.as_str())
        );

        let permissions_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/admin/permissions"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(permissions_response.status(), StatusCode::OK);
        let permissions_body = body::to_bytes(permissions_response.into_body(), usize::MAX)
            .await
            .expect("current permissions body");
        let permissions_json: Value =
            serde_json::from_slice(&permissions_body).expect("current permissions json");
        assert_eq!(permissions_json["head"]["version"].as_u64(), Some(2));
        assert_eq!(
            permissions_json["permissions"]["users"]["select"]["using"]["type"].as_str(),
            Some("False")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_permissions_publications_install_only_the_winning_runtime_head() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let state = make_state_with_schema(schema).await;
        let app = make_test_router(state.clone());
        let start = Arc::new(tokio::sync::Barrier::new(2));

        let publish =
            |app: axum::Router, start: Arc<tokio::sync::Barrier>, policy_type: &'static str| {
                let schema_hash = schema_hash.to_string();
                tokio::spawn(async move {
                    let request_body = serde_json::json!({
                        "schemaHash": schema_hash,
                        "permissions": {
                            "users": {
                                "select": { "using": { "type": policy_type } }
                            }
                        }
                    });
                    start.wait().await;
                    app.oneshot(
                        axum::http::Request::builder()
                            .method("POST")
                            .uri(test_app_route("/admin/permissions"))
                            .header("Content-Type", "application/json")
                            .header("X-Jazz-Admin-Secret", "admin-secret")
                            .body(axum::body::Body::from(request_body.to_string()))
                            .unwrap(),
                    )
                    .await
                    .expect("publish permissions through admin route")
                })
            };

        let allow_publish = publish(app.clone(), start.clone(), "True");
        let deny_publish = publish(app, start, "False");
        let (allow_response, deny_response) = tokio::join!(allow_publish, deny_publish);
        let allow_response = allow_response.expect("allow publish task");
        let deny_response = deny_response.expect("deny publish task");
        let allow_status = allow_response.status();
        let deny_status = deny_response.status();
        assert_eq!(
            [allow_status, deny_status]
                .into_iter()
                .filter(|status| *status == StatusCode::CREATED)
                .count(),
            1
        );
        assert_eq!(
            [allow_status, deny_status]
                .into_iter()
                .filter(|status| *status == StatusCode::CONFLICT)
                .count(),
            1
        );

        let (winning_policy, created_response, conflict_response) =
            if allow_status == StatusCode::CREATED {
                (PolicyExpr::True, allow_response, deny_response)
            } else {
                (PolicyExpr::False, deny_response, allow_response)
            };
        let conflict_body = body::to_bytes(conflict_response.into_body(), usize::MAX)
            .await
            .expect("stale publish body");
        let conflict_json: Value =
            serde_json::from_slice(&conflict_body).expect("stale publish json");
        assert!(
            conflict_json["error"]
                .as_str()
                .is_some_and(|message| message.starts_with("stale permissions parent"))
        );

        let created_body = body::to_bytes(created_response.into_body(), usize::MAX)
            .await
            .expect("winning publish body");
        let created_json: Value =
            serde_json::from_slice(&created_body).expect("winning publish json");
        let winning_bundle_object_id = created_json["head"]["bundleObjectId"]
            .as_str()
            .expect("winning bundle object id");
        assert_eq!(created_json["head"]["version"].as_u64(), Some(1));
        assert_eq!(created_json["head"]["parentBundleObjectId"], Value::Null);

        let current = state
            .catalogue
            .current_permissions(&state.catalogue_store)
            .expect("read winning permissions")
            .expect("winning permissions head");
        let users = TableName::new("users");
        assert_eq!(current.head.schema_hash, schema_hash);
        assert_eq!(
            current.head.bundle_object_id.to_string(),
            winning_bundle_object_id
        );
        assert_eq!(
            current
                .permissions
                .get(&users)
                .expect("winning users permissions")
                .select
                .using
                .as_ref(),
            Some(&winning_policy)
        );

        let runtime_snapshot = state
            .runtime()
            .expect("runtime shell started")
            .trusted_catalogue_snapshot_for_test()
            .await
            .expect("read runtime catalogue");
        let active_schema = runtime_snapshot
            .schemas
            .iter()
            .find(|schema| schema.id == runtime_snapshot.current_write_schema.schema)
            .expect("active runtime schema");
        assert_eq!(
            active_schema
                .schema
                .public_schema()
                .get(&users)
                .expect("runtime users table")
                .policies
                .select
                .using
                .as_ref(),
            Some(&winning_policy)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn chained_permissions_runtime_reconciliation_never_installs_stale_head() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let state = make_state_with_schema(schema).await;
        let users = TableName::new("users");
        let permissions = |policy| {
            let mut table_policies = TablePolicies::default();
            table_policies.select.using = Some(policy);
            std::collections::HashMap::from([(users.clone(), table_policies)])
        };

        state
            .catalogue
            .publish_permissions_bundle(
                &state.catalogue_store,
                schema_hash,
                permissions(PolicyExpr::True),
                None,
            )
            .expect("persist H1");
        let h1 = state
            .catalogue
            .current_permissions_head(&state.catalogue_store)
            .expect("read H1")
            .expect("H1 exists")
            .bundle_object_id;

        // Freeze H1 after it reads the durable head but before it queues its
        // runtime install. H2 is then a valid chained publication. Without the
        // bridge mutex, H2 can install first and H1 resumes last, regressing
        // the runtime policy despite the durable head remaining H2.
        let (h1_read_tx, h1_read_rx) = std::sync::mpsc::sync_channel(1);
        let (resume_h1_tx, resume_h1_rx) = std::sync::mpsc::sync_channel(1);
        state.set_runtime_catalogue_after_permissions_read_hook_for_test(Box::new(move || {
            h1_read_tx.send(()).expect("signal H1 durable read");
            resume_h1_rx.recv().expect("resume H1 bridge");
        }));
        let first_state = state.clone();
        let first_bridge = tokio::spawn(async move {
            crate::server::runtime_catalogue::publish_runtime_catalogue(&first_state, &[], &[])
                .await
        });
        tokio::task::spawn_blocking(move || h1_read_rx.recv())
            .await
            .expect("join H1 read waiter")
            .expect("observe H1 durable read");

        state
            .catalogue
            .publish_permissions_bundle(
                &state.catalogue_store,
                schema_hash,
                permissions(PolicyExpr::False),
                Some(h1),
            )
            .expect("persist H2 after H1");
        let (h2_read_tx, h2_read_rx) = std::sync::mpsc::sync_channel(1);
        state.set_runtime_catalogue_after_permissions_read_hook_for_test(Box::new(move || {
            h2_read_tx.send(()).expect("signal H2 durable read");
        }));
        let (second_started_tx, second_started_rx) = std::sync::mpsc::sync_channel(1);
        let (start_second_tx, start_second_rx) = std::sync::mpsc::sync_channel(1);
        state.set_runtime_catalogue_before_publication_hook_for_test(Box::new(move || {
            second_started_tx
                .send(())
                .expect("signal H2 bridge attempt");
            start_second_rx.recv().expect("start H2 bridge");
        }));
        let second_state = state.clone();
        let second_bridge = tokio::spawn(async move {
            crate::server::runtime_catalogue::publish_runtime_catalogue(&second_state, &[], &[])
                .await
        });

        tokio::task::spawn_blocking(move || second_started_rx.recv())
            .await
            .expect("join H2 bridge waiter")
            .expect("observe H2 bridge attempt");
        start_second_tx.send(()).expect("start H2 bridge");
        assert!(
            matches!(
                h2_read_rx.recv_timeout(Duration::from_millis(100)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ),
            "H2 must wait for H1's runtime bridge to release publication order"
        );

        resume_h1_tx.send(()).expect("resume H1 bridge");
        first_bridge
            .await
            .expect("join H1 bridge")
            .expect("bridge H1");
        second_bridge
            .await
            .expect("join H2 bridge")
            .expect("bridge H2");

        let current = state
            .catalogue
            .current_permissions(&state.catalogue_store)
            .expect("read durable permissions")
            .expect("durable H2");
        assert_eq!(current.head.version, 2);
        assert_eq!(
            current
                .permissions
                .get(&users)
                .expect("durable users permissions")
                .select
                .using
                .as_ref(),
            Some(&PolicyExpr::False)
        );

        let runtime_snapshot = state
            .runtime()
            .expect("runtime shell started")
            .trusted_catalogue_snapshot_for_test()
            .await
            .expect("read runtime catalogue");
        let active_schema = runtime_snapshot
            .schemas
            .iter()
            .find(|schema| schema.id == runtime_snapshot.current_write_schema.schema)
            .expect("active runtime schema");
        assert_eq!(
            active_schema
                .schema
                .public_schema()
                .get(&users)
                .expect("runtime users table")
                .policies
                .select
                .using
                .as_ref(),
            Some(&PolicyExpr::False),
            "runtime policy must converge to the durable H2 head"
        );
    }

    #[tokio::test]
    async fn permissions_handler_returns_nulls_before_any_publish() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema(schema).await;
        let app = make_test_router(state);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/admin/permissions"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("permissions body");
        let json: Value = serde_json::from_slice(&body).expect("permissions json");
        assert!(json["head"].is_null());
        assert!(json["permissions"].is_null());
    }

    #[tokio::test]
    async fn schema_connectivity_handler_reports_uploaded_migration_connectivity() {
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let state = make_state_with_schema(v1.clone()).await;
        let app = make_test_router(state.clone());
        publish_schema_for_test(&app, v2).await;
        let runtime_v1 = jazz::schema::JazzSchema::new(&v1).expect("convert source schema");
        let runtime_v2 = jazz::schema::JazzSchema::new(
            &state
                .catalogue
                .known_schema(&state.catalogue_store, &v2_hash)
                .expect("read target schema")
                .expect("target schema stored"),
        )
        .expect("convert target schema");
        let expected_runtime_lens = jazz::protocol::MigrationLens::new(
            runtime_v1.version_id(),
            runtime_v2.version_id(),
            vec![jazz::protocol::TableLens {
                source_table: "users".to_owned(),
                target_table: "users".to_owned(),
                ops: vec![jazz::protocol::LensOp::RenameColumn {
                    from: "email".to_owned(),
                    to: "email_address".to_owned(),
                }],
            }],
        );
        let runtime_shell = state.runtime().expect("runtime shell started");
        assert_eq!(
            runtime_shell
                .runtime_catalogue_contains(runtime_v2.version_id(), expected_runtime_lens.id)
                .await
                .expect("inspect runtime catalogue before lens"),
            (false, false),
            "publishing a schema draft must not expose it before its lineage lens"
        );

        let disconnected = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "{}?fromHash={}&toHash={}",
                        test_app_route("/admin/schema-connectivity"),
                        v1_hash,
                        v2_hash
                    ))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(disconnected.status(), StatusCode::OK);
        let disconnected_body = body::to_bytes(disconnected.into_body(), usize::MAX)
            .await
            .expect("disconnected body");
        let disconnected_json: Value =
            serde_json::from_slice(&disconnected_body).expect("disconnected json");
        assert_eq!(disconnected_json["connected"], Value::Bool(false));

        let publish_migration_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/migrations"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "fromHash": v1_hash.to_string(),
                            "toHash": v2_hash.to_string(),
                            "forward": [{
                                "table": "users",
                                "operations": [{
                                    "type": "rename",
                                    "column": "email",
                                    "value": "email_address"
                                }]
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(publish_migration_response.status(), StatusCode::CREATED);

        let connected = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "{}?fromHash={}&toHash={}",
                        test_app_route("/admin/schema-connectivity"),
                        v1_hash,
                        v2_hash
                    ))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(connected.status(), StatusCode::OK);
        let connected_body = body::to_bytes(connected.into_body(), usize::MAX)
            .await
            .expect("connected body");
        let connected_json: Value =
            serde_json::from_slice(&connected_body).expect("connected json");
        assert_eq!(connected_json["connected"], Value::Bool(true));
    }

    #[tokio::test]
    async fn publish_schema_rejects_inline_permissions() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema(schema.clone()).await;
        let app = make_test_router(state);

        let request_body = serde_json::json!({
            "schema": schema,
            "permissions": {
                "users": {
                    "select": { "using": { "type": "True" } }
                }
            }
        });

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/schemas"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn edge_mode_forwards_admin_catalogue_publishing() {
        use std::sync::{Arc, Mutex};

        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let forwarded = Arc::new(Mutex::new(Vec::<ForwardedAdminRequest>::new()));
        let forwarded_for_router = forwarded.clone();
        let authority_routes = axum::Router::new().route(
            &test_app_route("/admin/schemas"),
            post(move |headers: HeaderMap, body: Json<Value>| {
                let forwarded = forwarded_for_router.clone();
                async move {
                    forwarded.lock().unwrap().push(ForwardedAdminRequest {
                        method: "POST".to_string(),
                        path: test_app_route("/admin/schemas"),
                        admin_secret: headers
                            .get("X-Jazz-Admin-Secret")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_string),
                        body: Some(body.0),
                    });
                    (
                        StatusCode::CREATED,
                        Json(serde_json::json!({
                            "objectId": "11111111-1111-1111-1111-111111111111",
                            "hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        })),
                    )
                }
            }),
        );
        let authority_app = axum::Router::new().nest("/authority-prefix", authority_routes);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind authority listener");
        let authority_addr = listener.local_addr().expect("authority local addr");
        let authority_task = tokio::spawn(async move {
            axum::serve(listener, authority_app)
                .await
                .expect("serve authority app");
        });

        let state = make_edge_state_with_schema(
            schema.clone(),
            format!("http://{authority_addr}/authority-prefix"),
        )
        .await;
        let app = make_test_router(state);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/schemas"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(
                        serde_json::json!({ "schema": schema }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);

        let forwarded = forwarded.lock().unwrap().clone();
        assert_eq!(forwarded.len(), 1);
        assert_eq!(forwarded[0].method, "POST");
        assert_eq!(forwarded[0].path, test_app_route("/admin/schemas"));
        assert_eq!(forwarded[0].admin_secret.as_deref(), Some("admin-secret"));

        authority_task.abort();
    }

    #[tokio::test]
    async fn publish_migration_requires_admin_and_persists_lens() {
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let state = make_state_with_schema(v1.clone()).await;
        let app = make_test_router(state.clone());
        publish_schema_for_test(&app, v2).await;
        let runtime_v1 = jazz::schema::JazzSchema::new(&v1).expect("convert source schema");
        let runtime_v2 = jazz::schema::JazzSchema::new(
            &state
                .catalogue
                .known_schema(&state.catalogue_store, &v2_hash)
                .expect("read target schema")
                .expect("target schema stored"),
        )
        .expect("convert target schema");
        let expected_runtime_lens = jazz::protocol::MigrationLens::new(
            runtime_v1.version_id(),
            runtime_v2.version_id(),
            vec![jazz::protocol::TableLens {
                source_table: "users".to_owned(),
                target_table: "users".to_owned(),
                ops: vec![jazz::protocol::LensOp::RenameColumn {
                    from: "email".to_owned(),
                    to: "email_address".to_owned(),
                }],
            }],
        );
        let runtime_shell = state.runtime().expect("runtime shell started");
        assert_eq!(
            runtime_shell
                .runtime_catalogue_contains(runtime_v2.version_id(), expected_runtime_lens.id)
                .await
                .expect("inspect runtime catalogue before lens"),
            (false, false),
            "publishing a schema draft must not expose it before its lineage lens"
        );

        let request_body = serde_json::json!({
            "fromHash": v1_hash.to_string(),
            "toHash": v2_hash.to_string(),
            "forward": [{
                "table": "users",
                "operations": [{
                    "type": "rename",
                    "column": "email",
                    "value": "email_address"
                }]
            }]
        });

        let unauthorized = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/migrations"))
                    .header("Content-Type", "application/json")
                    .body(axum::body::Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let created = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/migrations"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        assert_eq!(
            runtime_shell
                .runtime_catalogue_contains(runtime_v2.version_id(), expected_runtime_lens.id)
                .await
                .expect("inspect runtime catalogue after lens"),
            (true, true),
            "migration publication must activate target schema and lens together"
        );

        let lens = state
            .catalogue_store
            .stored_lens_for_test(v1_hash, v2_hash)
            .expect("read stored catalogue lens");
        assert!(
            lens.is_some(),
            "published lens should be stored in the catalogue"
        );
        assert!(
            state
                .catalogue
                .are_schema_hashes_connected(&state.catalogue_store, v1_hash, v2_hash)
                .expect("read schema connectivity"),
            "published lens should connect the source and target schema hashes"
        );
    }

    #[tokio::test]
    async fn publish_migration_persists_table_rename_ops() {
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("people")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let state = make_state_with_schema(v1.clone()).await;
        let app = make_test_router(state.clone());
        publish_schema_for_test(&app, v2).await;

        let request_body = serde_json::json!({
            "fromHash": v1_hash.to_string(),
            "toHash": v2_hash.to_string(),
            "forward": [{
                "table": "people",
                "renamedFrom": "users",
                "operations": [{
                    "type": "rename",
                    "column": "email",
                    "value": "email_address"
                }]
            }]
        });

        let created = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/migrations"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let created_status = created.status();
        let created_body = body::to_bytes(created.into_body(), usize::MAX)
            .await
            .expect("migration response body");
        assert_eq!(
            created_status,
            StatusCode::CREATED,
            "{}",
            String::from_utf8_lossy(&created_body)
        );

        let lens = state
            .catalogue_store
            .stored_lens_for_test(v1_hash, v2_hash)
            .expect("read stored catalogue lens")
            .expect("published lens should be stored in the catalogue");

        assert_eq!(
            lens.forward.ops,
            vec![
                LensOp::RenameTable {
                    old_name: "users".to_string(),
                    new_name: "people".to_string(),
                },
                LensOp::RenameColumn {
                    table: "people".to_string(),
                    old_name: "email".to_string(),
                    new_name: "email_address".to_string(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn publish_migration_persists_added_and_removed_table_ops() {
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .table(
                TableSchema::builder("legacy_profiles")
                    .column("id", ColumnType::Uuid)
                    .column("bio", ColumnType::Text)
                    .nullable_column("avatar_url", ColumnType::Text),
            )
            .build();
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .table(
                TableSchema::builder("profiles")
                    .column("id", ColumnType::Uuid)
                    .column("bio", ColumnType::Text)
                    .nullable_column("avatar_url", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let state = make_state_with_schema(v1.clone()).await;
        let app = make_test_router(state.clone());
        publish_schema_for_test(&app, v2.clone()).await;

        let request_body = serde_json::json!({
            "fromHash": v1_hash.to_string(),
            "toHash": v2_hash.to_string(),
            "forward": [
                {
                    "table": "profiles",
                    "added": true,
                    "operations": []
                },
                {
                    "table": "legacy_profiles",
                    "removed": true,
                    "operations": []
                }
            ]
        });

        let created = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/migrations"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);

        let lens = state
            .catalogue_store
            .stored_lens_for_test(v1_hash, v2_hash)
            .expect("read stored catalogue lens")
            .expect("published lens should be stored in the catalogue");

        assert_eq!(lens.forward.ops.len(), 2);

        match &lens.forward.ops[0] {
            LensOp::AddTable { table, schema } => {
                assert_eq!(table, "profiles");
                let expected = v2.get(&TableName::from("profiles")).unwrap();
                assert_eq!(
                    schema.columns.content_hash(),
                    expected.columns.content_hash(),
                );
                assert_eq!(schema.policies, expected.policies);
            }
            other => panic!("expected AddTable op, got {other:?}"),
        }

        match &lens.forward.ops[1] {
            LensOp::RemoveTable { table, schema } => {
                assert_eq!(table, "legacy_profiles");
                let expected = v1.get(&TableName::from("legacy_profiles")).unwrap();
                assert_eq!(
                    schema.columns.content_hash(),
                    expected.columns.content_hash(),
                );
                assert_eq!(schema.policies, expected.policies);
            }
            other => panic!("expected RemoveTable op, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn admin_subscription_introspection_requires_admin_secret_and_valid_app_id() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema(schema).await;
        let app = make_test_router(state.clone());

        let without_secret = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(
                        "/admin/introspection/subscriptions?appId=test-app",
                    ))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(without_secret.status(), StatusCode::UNAUTHORIZED);

        let wrong_secret = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(
                        "/admin/introspection/subscriptions?appId=test-app",
                    ))
                    .header("X-Jazz-Admin-Secret", "wrong-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_secret.status(), StatusCode::UNAUTHORIZED);

        let missing_app_id = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/admin/introspection/subscriptions"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_app_id.status(), StatusCode::BAD_REQUEST);

        let invalid_app_id = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(
                        "/admin/introspection/subscriptions?appId=bad/id",
                    ))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid_app_id.status(), StatusCode::BAD_REQUEST);

        let mismatched_app_id = make_test_router(state)
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(
                        "/admin/introspection/subscriptions?appId=other-app",
                    ))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(mismatched_app_id.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn admin_subscription_introspection_returns_empty_core_shell() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema(schema).await;

        let response = make_test_router(state.clone())
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(
                        "/admin/introspection/subscriptions?appId=test-app",
                    ))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("telemetry body");
        let json: Value = serde_json::from_slice(&body).expect("telemetry json");

        let expected_app_id = state.app_id.to_string();
        assert_eq!(json["appId"].as_str(), Some(expected_app_id.as_str()));
        assert!(json["generatedAt"].as_u64().is_some());

        let groups = json["queries"].as_array().expect("queries array");
        assert!(
            groups.is_empty(),
            "subscription introspection must stay empty until backed by core telemetry"
        );
    }

    #[tokio::test]
    async fn connection_schema_diagnostics_reports_mismatched_schema() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let current_hash = SchemaHash::compute(&schema);
        let declared_hash = SchemaHash::from_bytes([9; 32]);
        let state = make_state_with_schema(schema).await;

        let diagnostics = state
            .catalogue_store
            .connection_schema_diagnostics(declared_hash)
            .expect("compute diagnostics");

        assert!(
            diagnostics.has_issues(),
            "mismatched schema should produce diagnostics"
        );
        assert_eq!(
            diagnostics,
            ConnectionSchemaDiagnostics {
                client_schema_hash: declared_hash,
                disconnected_permissions_schema_hash: Some(current_hash),
                unreachable_schema_hashes: vec![],
            }
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_closes_upgraded_websocket_before_handshake() {
        let state = make_sync_test_state("test-backend-secret").await;
        let app = create_router(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ws listener");
        let addr = listener.local_addr().expect("ws local addr");
        let server_task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve ws app");
        });

        let ws_url = format!("ws://{addr}{}", test_app_route("/ws"));
        let (mut ws, _) = connect_async(&ws_url).await.expect("connect ws");

        tokio::time::timeout(Duration::from_secs(5), async {
            while state.shutdown.active_websockets() != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("upgraded websocket should be tracked before handshake");

        assert!(state.shutdown.request_shutdown());

        let close_frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("wait for close")
            .expect("ws frame")
            .expect("ws result");
        let WsMessage::Close(Some(close)) = close_frame else {
            panic!("expected close frame, got {close_frame:?}");
        };
        assert_eq!(
            close.code,
            tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Restart
        );
        assert_eq!(close.reason.as_str(), "server shutting down");

        tokio::time::timeout(Duration::from_secs(5), async {
            while state.shutdown.active_websockets() != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("websocket cleanup");

        server_task.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ws_negotiates_against_fixed_schema_core_route() {
        let state = make_state_with_schema(Schema::new()).await;
        let app = create_router(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ws listener");
        let addr = listener.local_addr().expect("ws local addr");
        let server_task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve ws app");
        });

        let ws_url = format!("ws://{addr}{}", test_app_route("/ws"));
        let (mut ws, _) = connect_async(&ws_url).await.expect("connect ws");
        ws.send(WsMessage::Binary(
            serde_json::json!({
                "peer_identity": AuthorSubject::for_test_bytes([
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
                ]).canonical(),
                "auth": {
                    "admin_secret": "admin-secret",
                },
            })
            .to_string()
            .into_bytes()
            .into(),
        ))
        .await
        .expect("send ws auth prelude");
        let hello = WireFrame::Hello(WireHello::current(
            WirePeerRole::Client,
            FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS,
        ));
        let encoded = vec![encode_frame(&hello).expect("encode hello")];
        let batch = postcard::to_allocvec(&encoded).expect("encode ws batch");
        ws.send(WsMessage::Binary(batch.into()))
            .await
            .expect("send ws hello");

        let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("wait for ws hello")
            .expect("ws frame")
            .expect("ws result");
        let WsMessage::Binary(response) = response else {
            panic!("expected binary ws hello, got {response:?}");
        };
        let frames: Vec<Vec<u8>> =
            postcard::from_bytes(&response).expect("decode ws response batch");
        assert_eq!(frames.len(), 1);
        let WireFrame::Hello(server_hello) = decode_frame(&frames[0]).expect("decode hello") else {
            panic!("expected server hello");
        };
        assert_eq!(server_hello.role, WirePeerRole::Core);
        assert_eq!(
            server_hello.features,
            FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS
        );

        let _ = ws.close(None).await;
        server_task.abort();
    }
}
