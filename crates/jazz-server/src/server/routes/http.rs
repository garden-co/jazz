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
use crate::server::{ServerState, ShutdownPhase};
use jazz::tools::public_schema::{ColumnType, Schema, TableName, TablePolicies, Value};
use jazz::tools::schema_lens::{Lens, LensOp, LensTransform};
use jazz::tools::transport_error::ErrorResponse;

use super::utils::{
    parse_app_id_param, parse_schema_hash_param, permissions_head_view, permissions_map_view,
    unix_timestamp_millis,
};

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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PublishMigrationRequest {
    pub(crate) from_hash: String,
    pub(crate) to_hash: String,
    forward: Vec<PublishTableLens>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PublishTableLens {
    table: String,
    #[serde(default)]
    added: bool,
    #[serde(default)]
    removed: bool,
    renamed_from: Option<String>,
    operations: Vec<PublishLensOp>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(super) enum PublishLensOp {
    Introduce {
        column: String,
        column_type: ColumnType,
        value: Value,
    },
    Drop {
        column: String,
        column_type: ColumnType,
        value: Value,
    },
    Rename {
        column: String,
        value: String,
    },
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
) -> impl IntoResponse {
    let Some(hash_text) = params.get("hash") else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request(
                "missing schema hash".to_string(),
            )),
        )
            .into_response();
    };

    let admin_secret = headers
        .get("X-Jazz-Admin-Secret")
        .and_then(|v| v.to_str().ok());

    match validate_admin_secret(admin_secret, &state.auth_config) {
        Ok(()) => {}
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
    }

    let schema_hash = match parse_schema_hash_param(hash_text) {
        Ok(hash) => hash,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response();
        }
    };

    match state
        .catalogue
        .known_schema(&state.catalogue_store, &schema_hash)
    {
        Ok(Some(schema)) => {
            let published_at = match state
                .catalogue
                .schema_published_at(&state.catalogue_store, &schema_hash)
            {
                Ok(timestamp) => timestamp,
                Err(err) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse::internal(format!(
                            "failed to read schema publish timestamp: {err}"
                        ))),
                    )
                        .into_response();
                }
            };
            tracing::info!(
                requested_hash = %schema_hash.short(),
                "schema request: returning requested hash"
            );
            let body = StoredSchemaResponse {
                schema: schema.clone(),
                published_at,
            };
            Json(body).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::not_found(format!(
                "schema catalogue not found for hash {}",
                schema_hash
            ))),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to read schema catalogue: {err}"
            ))),
        )
            .into_response(),
    }
}

/// Return all known schema hashes from catalogue state.
///
/// Requires a valid admin secret.
pub(super) async fn schema_hashes_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Result<Json<SchemaSummaryResponse>, Response> {
    let admin_secret = headers
        .get("X-Jazz-Admin-Secret")
        .and_then(|v| v.to_str().ok());

    match validate_admin_secret(admin_secret, &state.auth_config) {
        Ok(()) => {}
        Err((status, msg)) => {
            return Err((status, Json(ErrorResponse::unauthorized(msg))).into_response());
        }
    }

    match state.catalogue.known_schema_hashes(&state.catalogue_store) {
        Ok(hashes) => {
            let mut schemas = Vec::with_capacity(hashes.len());
            for hash in &hashes {
                let published_at = match state
                    .catalogue
                    .schema_published_at(&state.catalogue_store, hash)
                {
                    Ok(timestamp) => timestamp,
                    Err(err) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse::internal(format!(
                                "failed to read schema publish timestamp: {err}"
                            ))),
                        )
                            .into_response());
                    }
                };
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
        Err(err) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to read schema hashes: {err}"
            ))),
        )
            .into_response()),
    }
}

/// Return whether two known schema hashes are connected by non-draft uploaded migrations.
///
/// Requires a valid admin secret.
pub(super) async fn migration_graph_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let admin_secret = headers
        .get("X-Jazz-Admin-Secret")
        .and_then(|v| v.to_str().ok());

    match validate_admin_secret(admin_secret, &state.auth_config) {
        Ok(()) => {}
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
    }

    match state.catalogue.migration_graph(&state.catalogue_store) {
        Ok(graph) => (StatusCode::OK, Json(graph)).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to read migration graph: {err}"
            ))),
        )
            .into_response(),
    }
}

pub(super) async fn permissions_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let admin_secret = headers
        .get("X-Jazz-Admin-Secret")
        .and_then(|v| v.to_str().ok());

    match validate_admin_secret(admin_secret, &state.auth_config) {
        Ok(()) => {}
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
    }

    match state.catalogue.active_schema(&state.catalogue_store) {
        Ok(current) => (
            StatusCode::OK,
            Json(match current {
                Some(current) => StoredPermissionsResponse {
                    head: Some(permissions_head_view(current.summary)),
                    permissions: Some(permissions_map_view(current.permissions)),
                },
                None => StoredPermissionsResponse {
                    head: None,
                    permissions: None,
                },
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to read current permissions: {err}"
            ))),
        )
            .into_response(),
    }
}

/// Lower a deployment migration against its resolved source and target schemas.
pub(crate) fn lower_migration(
    request: PublishMigrationRequest,
    source_schema: &Schema,
    target_schema: &Schema,
) -> Result<Lens, String> {
    let source_hash = parse_schema_hash_param(&request.from_hash)?;
    let target_hash = parse_schema_hash_param(&request.to_hash)?;
    let tables = request.forward;
    let mut forward = LensTransform::new();
    for table_lens in tables {
        let table_name = table_lens.table;
        if table_lens.added && table_lens.removed {
            return Err(format!(
                "table {} cannot be both added and removed",
                table_name
            ));
        }
        if (table_lens.added || table_lens.removed) && table_lens.renamed_from.is_some() {
            return Err(format!(
                "table {} cannot combine added/removed markers with renamedFrom",
                table_name
            ));
        }
        if (table_lens.added || table_lens.removed) && !table_lens.operations.is_empty() {
            return Err(format!(
                "table {} cannot combine added/removed markers with column operations",
                table_name
            ));
        }
        if table_lens.added {
            let target_table_name = TableName::from(table_name.clone());
            let schema = match target_schema.get(&target_table_name) {
                Some(schema) => schema.clone(),
                None => {
                    return Err(format!(
                        "createTables references unknown target table {}",
                        table_name
                    ));
                }
            };
            forward.push(
                LensOp::AddTable {
                    table: table_name.clone(),
                    schema,
                },
                false,
            );
        }
        if table_lens.removed {
            let source_table_name = TableName::from(table_name.clone());
            let schema = match source_schema.get(&source_table_name) {
                Some(schema) => schema.clone(),
                None => {
                    return Err(format!(
                        "dropTables references unknown source table {}",
                        table_name
                    ));
                }
            };
            forward.push(
                LensOp::RemoveTable {
                    table: table_name.clone(),
                    schema,
                },
                false,
            );
        }
        if let Some(renamed_from) = table_lens.renamed_from {
            forward.push(
                LensOp::RenameTable {
                    old_name: renamed_from,
                    new_name: table_name.clone(),
                },
                false,
            );
        }
        for operation in table_lens.operations {
            let op = match operation {
                PublishLensOp::Introduce {
                    column,
                    column_type,
                    value,
                } => LensOp::AddColumn {
                    table: table_name.clone(),
                    column,
                    column_type,
                    default: value,
                },
                PublishLensOp::Drop {
                    column,
                    column_type,
                    value,
                } => LensOp::RemoveColumn {
                    table: table_name.clone(),
                    column,
                    column_type,
                    default: value,
                },
                PublishLensOp::Rename { column, value } => LensOp::RenameColumn {
                    table: table_name.clone(),
                    old_name: column,
                    new_name: value,
                },
            };
            forward.push(op, false);
        }
    }

    Ok(Lens::new(source_hash, target_hash, forward))
}

pub(super) async fn admin_subscription_introspection_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Query(params): Query<AdminSubscriptionIntrospectionParams>,
) -> impl IntoResponse {
    let admin_secret = headers
        .get("X-Jazz-Admin-Secret")
        .and_then(|v| v.to_str().ok());

    match validate_admin_secret(admin_secret, &state.auth_config) {
        Ok(()) => {}
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
    }

    let Some(app_id_text) = params.app_id.as_deref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request(
                "appId query parameter is required",
            )),
        )
            .into_response();
    };

    let requested_app_id = match parse_app_id_param(app_id_text) {
        Ok(app_id) => app_id,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response();
        }
    };

    if requested_app_id != state.app_id {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::not_found(format!(
                "app not found: {}",
                app_id_text.trim()
            ))),
        )
            .into_response();
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
    let admin_secret = headers
        .get("X-Jazz-Admin-Secret")
        .and_then(|v| v.to_str().ok());

    match validate_admin_secret(admin_secret, &state.auth_config) {
        Ok(()) => {}
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
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
    let secret = headers
        .get("X-Jazz-Admin-Secret")
        .and_then(|v| v.to_str().ok());
    if let Err((status, message)) = validate_admin_secret(secret, &state.auth_config) {
        return (status, Json(ErrorResponse::unauthorized(message))).into_response();
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
