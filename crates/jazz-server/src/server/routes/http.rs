//! HTTP endpoint handlers, request/response DTOs, and HTTP-specific helpers.

//! HTTP routes for the Jazz server.

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

use crate::middleware::auth::validate_admin_secret;
use crate::server::catalogue::MigrationGraph;
use crate::server::{ServerState, ShutdownPhase};
use jazz::tools::public_schema::{Schema, TablePolicies};
use jazz::tools::transport_error::ErrorResponse;

use super::utils::{
    parse_app_id_param, parse_schema_hash_param, permissions_head_view, permissions_map_view,
    unix_timestamp_millis,
};

/// Admin secret presented in the `X-Jazz-Admin-Secret` request header, if any.
pub(super) fn admin_secret_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("X-Jazz-Admin-Secret")
        .and_then(|v| v.to_str().ok())
}

/// Reject the request with a JSON unauthorized body unless it carries the
/// configured admin secret.
#[allow(clippy::result_large_err)] // handlers return axum `Response` errors as-is
fn require_admin(state: &ServerState, headers: &HeaderMap) -> Result<(), Response> {
    validate_admin_secret(admin_secret_header(headers), &state.auth_config)
        .map_err(|(status, msg)| (status, Json(ErrorResponse::unauthorized(msg))).into_response())
}

fn error_response(status: StatusCode, body: ErrorResponse) -> Response {
    (status, Json(body)).into_response()
}

fn bad_request(message: impl Into<String>) -> Response {
    error_response(StatusCode::BAD_REQUEST, ErrorResponse::bad_request(message))
}

fn not_found(message: impl Into<String>) -> Response {
    error_response(StatusCode::NOT_FOUND, ErrorResponse::not_found(message))
}

/// Map a catalogue read failure to a 500 naming what could not be read.
fn read_failed<E: std::fmt::Display>(what: &'static str) -> impl FnOnce(E) -> Response {
    move |err| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorResponse::internal(format!("failed to read {what}: {err}")),
        )
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SchemaSummary {
    hash: String,
    published_at: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(super) struct SchemaSummaryResponse {
    #[deprecated(note = "Use `schemas` instead")]
    hashes: Vec<String>,
    schemas: Vec<SchemaSummary>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StoredSchemaResponse {
    schema: Schema,
    published_at: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(super) struct AdminSubscriptionIntrospectionParams {
    #[serde(rename = "appId")]
    app_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AdminSubscriptionIntrospectionResponse {
    app_id: String,
    generated_at: u64,
    queries: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PermissionsHeadView {
    pub(super) schema_hash: String,
    pub(super) version: u64,
    pub(super) parent_bundle_object_id: Option<String>,
    pub(super) bundle_object_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StoredPermissionsResponse {
    head: Option<PermissionsHeadView>,
    permissions: Option<std::collections::HashMap<String, TablePolicies>>,
}

#[derive(Debug, Serialize)]
pub(super) struct ShutdownResponse {
    status: &'static str,
}

/// Return the catalogue schema for the given hash plus its publish timestamp.
///
/// Requires a valid admin secret; returns 404 if no schema exists for the hash.
pub(super) async fn schema_handler(
    State(state): State<Arc<ServerState>>,
    Path(params): Path<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Json<StoredSchemaResponse>, Response> {
    let hash_text = params
        .get("hash")
        .ok_or_else(|| bad_request("missing schema hash"))?;
    require_admin(&state, &headers)?;
    let schema_hash = parse_schema_hash_param(hash_text).map_err(bad_request)?;

    let schema = state
        .catalogue
        .known_schema(&state.catalogue_store, &schema_hash)
        .map_err(read_failed("schema catalogue"))?
        .ok_or_else(|| not_found(format!("schema catalogue not found for hash {schema_hash}")))?;
    let published_at = state
        .catalogue
        .schema_published_at(&state.catalogue_store, &schema_hash)
        .map_err(read_failed("schema publish timestamp"))?;
    tracing::info!(
        requested_hash = %schema_hash.short(),
        "schema request: returning requested hash"
    );
    Ok(Json(StoredSchemaResponse {
        schema,
        published_at,
    }))
}

/// Return all known schema hashes from catalogue state.
///
/// Requires a valid admin secret.
pub(super) async fn schema_hashes_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Result<Json<SchemaSummaryResponse>, Response> {
    require_admin(&state, &headers)?;

    let hashes = state
        .catalogue
        .known_schema_hashes(&state.catalogue_store)
        .map_err(read_failed("schema hashes"))?;
    let mut schemas = Vec::with_capacity(hashes.len());
    for hash in &hashes {
        let published_at = state
            .catalogue
            .schema_published_at(&state.catalogue_store, hash)
            .map_err(read_failed("schema publish timestamp"))?;
        schemas.push(SchemaSummary {
            hash: hash.to_string(),
            published_at,
        });
    }
    #[allow(deprecated)]
    let body = SchemaSummaryResponse {
        hashes: hashes.iter().map(ToString::to_string).collect(),
        schemas,
    };
    Ok(Json(body))
}

/// Return whether two known schema hashes are connected by non-draft uploaded migrations.
///
/// Requires a valid admin secret.
pub(super) async fn migration_graph_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Result<Json<MigrationGraph>, Response> {
    require_admin(&state, &headers)?;
    let graph = state
        .catalogue
        .migration_graph(&state.catalogue_store)
        .map_err(read_failed("migration graph"))?;
    Ok(Json(graph))
}

pub(super) async fn permissions_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Result<Json<StoredPermissionsResponse>, Response> {
    require_admin(&state, &headers)?;
    let current = state
        .catalogue
        .active_schema(&state.catalogue_store)
        .map_err(read_failed("current permissions"))?;
    Ok(Json(match current {
        Some(current) => StoredPermissionsResponse {
            head: Some(permissions_head_view(current.summary)),
            permissions: Some(permissions_map_view(current.permissions)),
        },
        None => StoredPermissionsResponse {
            head: None,
            permissions: None,
        },
    }))
}

pub(super) async fn admin_subscription_introspection_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Query(params): Query<AdminSubscriptionIntrospectionParams>,
) -> impl IntoResponse {
    if let Err(response) = require_admin(&state, &headers) {
        return response;
    }

    let Some(app_id_text) = params.app_id.as_deref() else {
        return bad_request("appId query parameter is required");
    };

    let requested_app_id = match parse_app_id_param(app_id_text) {
        Ok(app_id) => app_id,
        Err(message) => return bad_request(message),
    };

    if requested_app_id != state.app_id {
        return not_found(format!("app not found: {}", app_id_text.trim()));
    }

    Json(AdminSubscriptionIntrospectionResponse {
        app_id: state.app_id.to_string(),
        generated_at: unix_timestamp_millis(),
        queries: Vec::new(),
    })
    .into_response()
}

pub(super) async fn internal_shutdown_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(response) = require_admin(&state, &headers) {
        return response;
    }

    let first_request = state.shutdown.request_shutdown();
    let status = if first_request {
        "shutting_down"
    } else {
        "already_shutting_down"
    };

    (StatusCode::ACCEPTED, Json(ShutdownResponse { status })).into_response()
}

pub(super) async fn health_handler(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    // A runtime that failed and could not restart serves no sync: ask to be
    // restarted.
    if let Some(fault) = state.store.as_ref().and_then(|store| store.fault()) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "status": "unhealthy",
                "reason": fault
            })),
        )
            .into_response();
    }
    let mut phase = state.shutdown.phase();
    if !state.shutdown.is_shutting_down() && phase.is_running() {
        return Json(serde_json::json!({
            "status": "healthy"
        }))
        .into_response();
    }
    if phase.is_running() {
        phase = ShutdownPhase::ShuttingDown;
    }

    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({
            "status": "shutting_down",
            "phase": phase
        })),
    )
        .into_response()
}
#[cfg(test)]
mod tests {
    use super::super::create_router;
    use crate::server::{ServerBuilder, ServerState, StorageBackend};
    use axum::body::{self, Body};
    use axum::http::{Request, StatusCode};
    use jazz::tools::AppId;
    use serde_json::Value as JsonValue;
    use std::sync::Arc;
    use tower::ServiceExt;

    async fn health(state: Arc<ServerState>) -> (StatusCode, JsonValue) {
        let response = create_router(state)
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .expect("health request"),
            )
            .await
            .expect("health response");
        let status = response.status();
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("health body");
        (status, serde_json::from_slice(&body).expect("health json"))
    }

    #[tokio::test]
    async fn core_topology_remains_healthy_without_a_runtime_shell() {
        let state = ServerBuilder::new(AppId::from_name("health-core"))
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build core")
            .state;
        assert!(state.runtime_for_client().is_none());

        let (status, json) = health(state).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json, serde_json::json!({ "status": "healthy" }));
    }

    /// A runtime that panics is restarted and the server stays healthy. One
    /// that panics again soon after stays stopped: the server answers 503
    /// with why, so it is restarted.
    #[tokio::test]
    async fn a_runtime_that_cannot_restart_makes_the_server_unhealthy() {
        let state = ServerBuilder::new(AppId::from_name("health-fault"))
            .with_schema(
                jazz::tools::public_schema::SchemaBuilder::new()
                    .table(
                        jazz::tools::public_schema::TableSchema::builder("notes")
                            .column("body", jazz::tools::public_schema::ColumnType::Text),
                    )
                    .build(),
            )
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build server")
            .state;
        let runtime = state.runtime_for_client().expect("a runtime");

        let panicked = runtime
            .with_db_for_test::<()>(|_| panic!("injected runtime failure"))
            .await;
        assert!(panicked.is_err());
        let (status, json) = health(Arc::clone(&state)).await;
        assert_eq!(status, StatusCode::OK, "restarted: {json}");

        let _ = runtime
            .with_db_for_test::<()>(|_| panic!("injected runtime failure"))
            .await;
        // The caller hears of the panic as it unwinds; host work queued
        // after it runs once the store thread has handled it.
        state
            .store
            .as_ref()
            .expect("a store")
            .kv(jazz::host::HostTable::new("probe"))
            .expect("host table")
            .write_many(&[(b"k".to_vec(), b"v".to_vec())])
            .expect("host tables stay available");
        let (status, json) = health(state).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["status"], "unhealthy");
        let reason = json["reason"].as_str().expect("a reason");
        assert!(reason.contains("injected runtime failure"), "{reason}");
    }
}
/// Commit a complete, validated deployment. Detached execution makes request
/// cancellation independent of the durable commit/activation boundary.
pub(super) async fn deploy_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    body: Result<
        Json<crate::server::deployment::DeployRequest>,
        axum::extract::rejection::JsonRejection,
    >,
) -> Response {
    if let Err(response) = require_admin(&state, &headers) {
        return response;
    }
    let request = match body {
        Ok(Json(request)) => request,
        Err(error) => {
            let status = if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
                error.status()
            } else {
                StatusCode::BAD_REQUEST
            };
            return (status, Json(ErrorResponse::bad_request(error.body_text()))).into_response();
        }
    };
    match tokio::spawn(crate::server::deployment::deploy(state, request)).await {
        Ok(Ok(response)) => (StatusCode::OK, Json(response)).into_response(),
        Ok(Err(error)) => (error.status, Json(error.body)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(error.to_string())),
        )
            .into_response(),
    }
}
