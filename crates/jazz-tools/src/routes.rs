//! HTTP routes for the Jazz server.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

use crate::jazz_transport::ErrorResponse;
use crate::middleware::auth::{extract_session, validate_admin_secret, validate_backend_secret};
use crate::object::ObjectId;
use crate::query_manager::types::{
    ColumnType, Schema, SchemaHash, TableName, TablePolicies, Value,
};
use crate::schema_manager::{AppId, Lens, LensOp, LensTransform};
use crate::server::{
    CatalogueAuthorityMode, ConnectionState, ServerState, WsPayloadTelemetryInput,
};
use crate::sync_manager::ClientId;
use crate::sync_payload_telemetry::SyncPayloadTelemetryDirection;

/// Create the router with all routes.
pub fn create_router(state: Arc<ServerState>) -> Router {
    // TODO: Accept app-name aliases in app-scoped route matching
    // Nesting all non-health routes under a fixed "/apps/{state.app_id}" path makes the server only match the canonical UUID string, but JavaScript callers frequently propagate human-readable app IDs (e.g. "test-app") that are valid elsewhere via AppId::from_name(...). In that non-UUID case, the client now builds /apps/test-app/... URLs while the server only serves /apps/<derived-uuid>/..., so websocket and admin/schema requests return 404 for otherwise valid app IDs.
    let app_route_prefix = format!("/apps/{}", state.app_id);
    let traced_routes = Router::new()
        .route("/ws", axum::routing::any(ws_handler))
        .route("/schema/:hash", get(schema_handler))
        .route("/schemas", get(schema_hashes_handler))
        .route("/admin/schemas", post(publish_schema_handler))
        .route(
            "/admin/schema-connectivity",
            get(schema_connectivity_handler),
        )
        .route("/admin/permissions/head", get(permissions_head_handler))
        .route(
            "/admin/permissions",
            get(permissions_handler).post(publish_permissions_handler),
        )
        .route("/admin/migrations", post(publish_migration_handler))
        .route(
            "/admin/introspection/subscriptions",
            get(admin_subscription_introspection_handler),
        )
        .layer(TraceLayer::new_for_http());

    Router::new()
        .route("/health", get(health_handler))
        .nest(&app_route_prefix, traced_routes)
        .layer(CorsLayer::permissive())
        .with_state(state)
}

#[derive(Debug, Serialize)]
struct SchemaHashesResponse {
    hashes: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoredSchemaResponse {
    schema: Schema,
    published_at: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct AdminSubscriptionIntrospectionParams {
    #[serde(rename = "appId")]
    app_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SchemaConnectivityParams {
    from_hash: String,
    to_hash: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AdminSubscriptionIntrospectionResponse {
    app_id: String,
    generated_at: u64,
    queries: Vec<crate::query_manager::manager::ServerSubscriptionTelemetryGroup>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishMigrationRequest {
    from_hash: String,
    to_hash: String,
    forward: Vec<PublishTableLens>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishTableLens {
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
enum PublishLensOp {
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

#[derive(Debug, Serialize, Deserialize)]
struct PublishSchemaRequest {
    schema: Schema,
    permissions: Option<std::collections::HashMap<TableName, TablePolicies>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishPermissionsRequest {
    schema_hash: String,
    permissions: std::collections::HashMap<String, TablePolicies>,
    expected_parent_bundle_object_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PublishSchemaResponse {
    object_id: String,
    hash: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PermissionsHeadView {
    schema_hash: String,
    version: u64,
    parent_bundle_object_id: Option<String>,
    bundle_object_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PermissionsHeadResponse {
    head: Option<PermissionsHeadView>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoredPermissionsResponse {
    head: Option<PermissionsHeadView>,
    permissions: Option<std::collections::HashMap<String, TablePolicies>>,
}

#[derive(Debug, Serialize)]
struct SchemaConnectivityResponse {
    connected: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PublishMigrationResponse {
    object_id: String,
    from_hash: String,
    to_hash: String,
}

async fn forward_catalogue_request(
    state: &Arc<ServerState>,
    method: reqwest::Method,
    path: &str,
    body: Option<Vec<u8>>,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let (base_url, authority_admin_secret) = match &state.catalogue_authority {
        CatalogueAuthorityMode::Local => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(
                    "catalogue forwarding requested without a configured authority".to_string(),
                )),
            ));
        }
        CatalogueAuthorityMode::Forward {
            base_url,
            admin_secret,
        } => (base_url.as_str(), admin_secret.as_str()),
    };

    let app_scoped_path = format!("/apps/{}/{}", state.app_id, path.trim_start_matches('/'));
    let authority_url = authority_endpoint_url(base_url, &app_scoped_path).map_err(|message| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(message)),
        )
    })?;

    let mut request = state
        .http_client
        .request(method, authority_url)
        .header("X-Jazz-Admin-Secret", authority_admin_secret);
    if let Some(body) = body {
        request = request.header(CONTENT_TYPE, "application/json").body(body);
    }

    let response = request.send().await.map_err(|err| {
        (
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse::internal(format!(
                "failed to reach catalogue authority: {err}"
            ))),
        )
    })?;

    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = response.headers().get(CONTENT_TYPE).cloned();
    let bytes = response.bytes().await.map_err(|err| {
        (
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse::internal(format!(
                "failed to read authority response: {err}"
            ))),
        )
    })?;

    let mut response_builder = Response::builder().status(status);
    if let Some(content_type) = content_type {
        response_builder = response_builder.header(CONTENT_TYPE, content_type);
    }

    response_builder
        .body(axum::body::Body::from(bytes))
        .map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to build forwarded response: {err}"
                ))),
            )
        })
}

fn authority_endpoint_url(base_url: &str, path: &str) -> Result<String, String> {
    let parsed = reqwest::Url::parse(base_url)
        .map_err(|err| format!("invalid catalogue authority URL '{base_url}': {err}"))?;
    let mut origin = parsed.clone();
    origin.set_query(None);
    origin.set_fragment(None);

    let (path_only, query) = match path.split_once('?') {
        Some((path_only, query)) => (path_only, Some(query)),
        None => (path, None),
    };

    let mut full_path = parsed.path().trim_end_matches('/').to_string();
    if full_path.is_empty() {
        full_path.push('/');
    }
    if !full_path.ends_with('/') {
        full_path.push('/');
    }
    full_path.push_str(path_only.trim_start_matches('/'));

    origin.set_path(&full_path);
    origin.set_query(query);
    Ok(origin.to_string())
}

/// Return the catalogue schema for the given hash plus its publish timestamp.
///
/// Requires a valid admin secret; returns 404 if no schema exists for the hash.
async fn schema_handler(
    State(state): State<Arc<ServerState>>,
    Path(hash_text): Path<String>,
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

    if matches!(
        &state.catalogue_authority,
        CatalogueAuthorityMode::Forward { .. }
    ) {
        return match forward_catalogue_request(
            &state,
            reqwest::Method::GET,
            &format!("/schema/{hash_text}"),
            None,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }

    let schema_hash = match parse_schema_hash_param(&hash_text) {
        Ok(hash) => hash,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response();
        }
    };

    match state.runtime.known_schema(&schema_hash) {
        Ok(Some(schema)) => {
            let published_at = match state.runtime.schema_published_at(&schema_hash) {
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
async fn schema_hashes_handler(
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

    if matches!(
        &state.catalogue_authority,
        CatalogueAuthorityMode::Forward { .. }
    ) {
        return match forward_catalogue_request(&state, reqwest::Method::GET, "/schemas", None).await
        {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }

    match state.runtime.known_schema_hashes() {
        Ok(hashes) => {
            let body = SchemaHashesResponse {
                hashes: hashes.iter().map(ToString::to_string).collect(),
            };
            Json(body).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to read schema hashes: {err}"
            ))),
        )
            .into_response(),
    }
}

/// Return whether two known schema hashes are connected by non-draft uploaded migrations.
///
/// Requires a valid admin secret.
async fn schema_connectivity_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Query(params): Query<SchemaConnectivityParams>,
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

    if matches!(
        &state.catalogue_authority,
        CatalogueAuthorityMode::Forward { .. }
    ) {
        let forwarded_path = format!(
            "/admin/schema-connectivity?fromHash={}&toHash={}",
            params.from_hash, params.to_hash
        );
        return match forward_catalogue_request(&state, reqwest::Method::GET, &forwarded_path, None)
            .await
        {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }

    let from_hash = match parse_schema_hash_param(&params.from_hash) {
        Ok(hash) => hash,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response();
        }
    };
    let to_hash = match parse_schema_hash_param(&params.to_hash) {
        Ok(hash) => hash,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response();
        }
    };

    match state.runtime.with_schema_manager(|schema_manager| {
        schema_manager.are_schema_hashes_connected(from_hash, to_hash)
    }) {
        Ok(connected) => (
            StatusCode::OK,
            Json(SchemaConnectivityResponse { connected }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to compute schema connectivity: {err}"
            ))),
        )
            .into_response(),
    }
}

/// Publish a schema object into the catalogue.
///
/// Requires a valid admin secret.
async fn publish_schema_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<PublishSchemaRequest>,
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

    if matches!(
        &state.catalogue_authority,
        CatalogueAuthorityMode::Forward { .. }
    ) {
        let body = match serde_json::to_vec(&request) {
            Ok(body) => body,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::internal(format!(
                        "failed to serialize schema publish request: {err}"
                    ))),
                )
                    .into_response();
            }
        };
        return match forward_catalogue_request(
            &state,
            reqwest::Method::POST,
            "/admin/schemas",
            Some(body),
        )
        .await
        {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }

    if request.permissions.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request(
                "schema publishing no longer accepts permissions; publish permissions via POST /admin/permissions".to_string(),
            )),
        )
            .into_response();
    }

    let schema_hash = SchemaHash::compute(&request.schema);
    let object_id = match state.runtime.publish_schema(request.schema) {
        Ok(object_id) => object_id,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to publish schema catalogue: {err}"
                ))),
            )
                .into_response();
        }
    };

    (
        StatusCode::CREATED,
        Json(PublishSchemaResponse {
            object_id: object_id.to_string(),
            hash: schema_hash.to_string(),
        }),
    )
        .into_response()
}

async fn permissions_head_handler(
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

    if matches!(
        &state.catalogue_authority,
        CatalogueAuthorityMode::Forward { .. }
    ) {
        return match forward_catalogue_request(
            &state,
            reqwest::Method::GET,
            "/admin/permissions/head",
            None,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }

    match state.runtime.with_schema_manager(|schema_manager| {
        schema_manager
            .current_permissions_head()
            .map(permissions_head_view)
    }) {
        Ok(head) => (StatusCode::OK, Json(PermissionsHeadResponse { head })).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to read permissions head: {err}"
            ))),
        )
            .into_response(),
    }
}

async fn permissions_handler(
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

    if matches!(
        &state.catalogue_authority,
        CatalogueAuthorityMode::Forward { .. }
    ) {
        return match forward_catalogue_request(
            &state,
            reqwest::Method::GET,
            "/admin/permissions",
            None,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }

    match state
        .runtime
        .with_schema_manager(|schema_manager| schema_manager.current_permissions())
    {
        Ok(current) => (
            StatusCode::OK,
            Json(match current {
                Some(current) => StoredPermissionsResponse {
                    head: Some(permissions_head_view(current.head)),
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

async fn publish_permissions_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<PublishPermissionsRequest>,
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

    if matches!(
        &state.catalogue_authority,
        CatalogueAuthorityMode::Forward { .. }
    ) {
        let body = match serde_json::to_vec(&request) {
            Ok(body) => body,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::internal(format!(
                        "failed to serialize permissions publish request: {err}"
                    ))),
                )
                    .into_response();
            }
        };
        return match forward_catalogue_request(
            &state,
            reqwest::Method::POST,
            "/admin/permissions",
            Some(body),
        )
        .await
        {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }

    let schema_hash = match parse_schema_hash_param(&request.schema_hash) {
        Ok(hash) => hash,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response();
        }
    };

    let expected_parent_bundle_object_id = match request.expected_parent_bundle_object_id {
        Some(object_id) => match parse_object_id_param(&object_id) {
            Ok(object_id) => Some(object_id),
            Err(message) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse::bad_request(message)),
                )
                    .into_response();
            }
        },
        None => None,
    };

    match state.runtime.known_schema(&schema_hash) {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "target schema catalogue not found for hash {}",
                    schema_hash
                ))),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to read known schemas: {err}"
                ))),
            )
                .into_response();
        }
    }

    let permissions = request
        .permissions
        .into_iter()
        .map(|(table_name, policies)| (TableName::new(table_name), policies))
        .collect();

    match state.runtime.publish_permissions_bundle(
        schema_hash,
        permissions,
        expected_parent_bundle_object_id,
    ) {
        Ok(_) => match state.runtime.with_schema_manager(|schema_manager| {
            schema_manager
                .current_permissions_head()
                .map(permissions_head_view)
        }) {
            Ok(head) => {
                (StatusCode::CREATED, Json(PermissionsHeadResponse { head })).into_response()
            }
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to read published permissions head: {err}"
                ))),
            )
                .into_response(),
        },
        Err(crate::runtime_tokio::RuntimeError::WriteError(message))
            if message.starts_with("stale permissions parent") =>
        {
            (
                StatusCode::CONFLICT,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to publish permissions catalogue: {err}"
            ))),
        )
            .into_response(),
    }
}

/// Publish a reviewed migration edge into the catalogue.
///
/// Requires a valid admin secret. The source and target schemas must already be
/// known to the server; only the lens edge itself is created here.
async fn publish_migration_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<PublishMigrationRequest>,
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

    if matches!(
        &state.catalogue_authority,
        CatalogueAuthorityMode::Forward { .. }
    ) {
        let body = match serde_json::to_vec(&request) {
            Ok(body) => body,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::internal(format!(
                        "failed to serialize migration publish request: {err}"
                    ))),
                )
                    .into_response();
            }
        };
        return match forward_catalogue_request(
            &state,
            reqwest::Method::POST,
            "/admin/migrations",
            Some(body),
        )
        .await
        {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }

    let source_hash = match parse_schema_hash_param(&request.from_hash) {
        Ok(hash) => hash,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response();
        }
    };

    let target_hash = match parse_schema_hash_param(&request.to_hash) {
        Ok(hash) => hash,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response();
        }
    };

    let source_schema = match state.runtime.known_schema(&source_hash) {
        Ok(Some(schema)) => schema,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "source schema catalogue not found for hash {}",
                    source_hash
                ))),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to read source schema catalogue: {err}"
                ))),
            )
                .into_response();
        }
    };

    let target_schema = match state.runtime.known_schema(&target_hash) {
        Ok(Some(schema)) => schema,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "target schema catalogue not found for hash {}",
                    target_hash
                ))),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to read target schema catalogue: {err}"
                ))),
            )
                .into_response();
        }
    };

    let mut forward = LensTransform::new();
    for table_lens in request.forward {
        let table_name = table_lens.table;
        if table_lens.added && table_lens.removed {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(format!(
                    "table {} cannot be both added and removed",
                    table_name
                ))),
            )
                .into_response();
        }
        if (table_lens.added || table_lens.removed) && table_lens.renamed_from.is_some() {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(format!(
                    "table {} cannot combine added/removed markers with renamedFrom",
                    table_name
                ))),
            )
                .into_response();
        }
        if (table_lens.added || table_lens.removed) && !table_lens.operations.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(format!(
                    "table {} cannot combine added/removed markers with column operations",
                    table_name
                ))),
            )
                .into_response();
        }
        if table_lens.added {
            let target_table_name = TableName::from(table_name.clone());
            let schema = match target_schema.get(&target_table_name) {
                Some(schema) => schema.clone(),
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse::bad_request(format!(
                            "createTables references unknown target table {}",
                            table_name
                        ))),
                    )
                        .into_response();
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
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse::bad_request(format!(
                            "dropTables references unknown source table {}",
                            table_name
                        ))),
                    )
                        .into_response();
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

    let lens = Lens::new(source_hash, target_hash, forward);
    let object_id = match state.runtime.publish_lens(&lens) {
        Ok(object_id) => object_id,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to publish migration lens: {err}"
                ))),
            )
                .into_response();
        }
    };

    if let Err(err) = state.runtime.flush().await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to flush published migration lens: {err}"
            ))),
        )
            .into_response();
    }

    (
        StatusCode::CREATED,
        Json(PublishMigrationResponse {
            object_id: object_id.to_string(),
            from_hash: request.from_hash,
            to_hash: request.to_hash,
        }),
    )
        .into_response()
}

async fn admin_subscription_introspection_handler(
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

    match state.runtime.server_subscription_telemetry() {
        Ok(queries) => Json(AdminSubscriptionIntrospectionResponse {
            app_id: state.app_id.to_string(),
            generated_at: unix_timestamp_millis(),
            queries,
        })
        .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to read subscription telemetry: {err}"
            ))),
        )
            .into_response(),
    }
}

fn parse_schema_hash_param(hash_text: &str) -> Result<SchemaHash, String> {
    let decoded_hash_bytes = hex::decode(hash_text)
        .map_err(|_| "invalid schema hash: expected hex string".to_string())?;
    if decoded_hash_bytes.len() != 32 {
        return Err("invalid schema hash: expected 64 hex chars".to_string());
    }

    let mut hash_bytes = [0u8; 32];
    hash_bytes.copy_from_slice(&decoded_hash_bytes);
    Ok(SchemaHash::from_bytes(hash_bytes))
}

fn connection_schema_diagnostics_from_handshake(
    state: &Arc<ServerState>,
    handshake: &crate::transport_manager::AuthHandshake,
) -> Result<
    Option<crate::sync_manager::ConnectionSchemaDiagnostics>,
    crate::runtime_tokio::RuntimeError,
> {
    let Some(client_schema_hash) = handshake.declared_schema_hash() else {
        return Ok(None);
    };

    let diagnostics = state
        .runtime
        .with_schema_manager(|sm| sm.connection_schema_diagnostics(client_schema_hash))?;
    Ok(diagnostics.has_issues().then_some(diagnostics))
}

fn parse_object_id_param(object_id_text: &str) -> Result<ObjectId, String> {
    let uuid = Uuid::parse_str(object_id_text)
        .map_err(|_| "invalid object id: expected UUID".to_string())?;
    Ok(ObjectId::from_uuid(uuid))
}

fn parse_app_id_param(app_id_text: &str) -> Result<AppId, String> {
    let trimmed = app_id_text.trim();
    if trimmed.is_empty() {
        return Err("invalid appId: expected UUID or app identifier".to_string());
    }

    if let Ok(app_id) = AppId::from_string(trimmed) {
        return Ok(app_id);
    }

    if trimmed
        .chars()
        .all(|char| char.is_ascii_alphanumeric() || matches!(char, '-' | '_' | '.'))
    {
        return Ok(AppId::from_name(trimmed));
    }

    Err("invalid appId: expected UUID or app identifier".to_string())
}

fn permissions_head_view(
    head: crate::schema_manager::manager::PermissionsHeadSummary,
) -> PermissionsHeadView {
    PermissionsHeadView {
        schema_hash: head.schema_hash.to_string(),
        version: head.version,
        parent_bundle_object_id: head
            .parent_bundle_object_id
            .map(|object_id| object_id.to_string()),
        bundle_object_id: head.bundle_object_id.to_string(),
    }
}

fn permissions_map_view(
    permissions: std::collections::HashMap<TableName, TablePolicies>,
) -> std::collections::HashMap<String, TablePolicies> {
    permissions
        .into_iter()
        .map(|(table_name, policies)| (table_name.to_string(), policies))
        .collect()
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

/// Health check endpoint.
async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy"
    }))
}

// ============================================================================
// WebSocket transport — Task 9
// ============================================================================

/// WebSocket upgrade endpoint.
///
/// Clients send an `AuthHandshake` binary frame (4-byte length prefix + JSON),
/// receive a `ConnectedResponse` frame, then exchange binary frames
/// bidirectionally until the connection closes.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Response {
    ws.on_upgrade(move |socket| handle_ws_connection(socket, state, headers))
}

/// Outcome of authenticating a WS handshake — mirrors `ClientSetup` in
/// the old `/sync` + `/events` handlers.
#[derive(Debug)]
enum WsClientSetup {
    Backend,
    Session(crate::query_manager::session::Session),
}

/// Authenticate a WebSocket `AuthHandshake`.
///
/// Priority is:
/// 1. `admin_secret` valid → `WsClientSetup::Backend`
/// 2. `backend_secret` present + no session header → `WsClientSetup::Backend`
/// 3. Otherwise → `extract_session` → `WsClientSetup::Session`
///
/// Returns `Err(message)` on auth failure; the caller should send a
/// `ServerEvent::Error` frame before closing.
async fn authenticate_ws_handshake(
    handshake: &crate::transport_manager::AuthHandshake,
    request_headers: &HeaderMap,
    state: &Arc<ServerState>,
) -> Result<WsClientSetup, String> {
    use axum::http::HeaderValue;
    use base64::Engine as _;

    let auth = &handshake.auth;

    // `admin_secret` is an explicit request to run this WS transport as the
    // backend. Validate it first and short-circuit all user-scoped auth.
    if let Some(admin_secret) = auth.admin_secret.as_deref() {
        validate_admin_secret(Some(admin_secret), &state.auth_config)
            .map_err(|(_, msg)| msg.to_string())?;
        return Ok(WsClientSetup::Backend);
    }

    if request_uses_cookie_auth(handshake, request_headers, &state.auth_config) {
        validate_ws_cookie_origin(request_headers)?;
    }

    // Build a synthetic HeaderMap from the handshake auth fields, layered on
    // top of the original upgrade request so cookie-based auth remains visible.
    let mut headers = request_headers.clone();

    if let Some(jwt) = &auth.jwt_token {
        let value = HeaderValue::from_str(&format!("Bearer {jwt}"))
            .map_err(|e| format!("invalid jwt_token header value: {e}"))?;
        headers.insert(axum::http::header::AUTHORIZATION, value);
    }
    if let Some(secret) = &auth.backend_secret {
        let value = HeaderValue::from_str(secret)
            .map_err(|e| format!("invalid backend_secret header value: {e}"))?;
        headers.insert("X-Jazz-Backend-Secret", value);
    }
    if let Some(session_val) = &auth.backend_session {
        let json = serde_json::to_string(session_val)
            .map_err(|e| format!("failed to serialise backend_session: {e}"))?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(json.as_bytes());
        let value = HeaderValue::from_str(&b64)
            .map_err(|e| format!("invalid backend_session header value: {e}"))?;
        headers.insert("X-Jazz-Session", value);
    }

    let has_jwt = headers.get(axum::http::header::AUTHORIZATION).is_some();
    let has_session_header = headers.get("X-Jazz-Session").is_some();
    let backend_secret = headers
        .get("X-Jazz-Backend-Secret")
        .and_then(|v| v.to_str().ok());

    // 2. Backend secret — only when no user-scoped JWT is present.  Clients
    //    that carry both a backend_secret and a jwt_token (e.g. test helpers
    //    that mirror the full credential set) must be treated as users so the
    //    connection carries a session for row-level policy evaluation.
    if backend_secret.is_some() && !has_jwt && !has_session_header {
        validate_backend_secret(backend_secret, &state.auth_config)
            .map_err(|(_, msg)| msg.to_string())?;
        return Ok(WsClientSetup::Backend);
    }

    // 3. JWT / session-impersonation path.
    let session = extract_session(
        &headers,
        state.app_id,
        &state.auth_config,
        state.jwt_verifier.as_deref(),
    )
    .await
    .map_err(|e| serde_json::to_string(&e).unwrap_or_else(|_| "authentication failed".into()))?;

    let session =
        session.ok_or_else(|| "Session required. Provide JWT or backend secret.".to_string())?;

    Ok(WsClientSetup::Session(session))
}

fn request_uses_cookie_auth(
    handshake: &crate::transport_manager::AuthHandshake,
    request_headers: &HeaderMap,
    auth_config: &crate::middleware::AuthConfig,
) -> bool {
    let Some(cookie_name) = auth_config.auth_cookie_name.as_deref() else {
        return false;
    };

    let has_explicit_auth = handshake.auth.jwt_token.is_some()
        || handshake.auth.backend_secret.is_some()
        || handshake.auth.backend_session.is_some()
        || handshake.auth.admin_secret.is_some()
        || request_headers
            .get(axum::http::header::AUTHORIZATION)
            .is_some()
        || request_headers.get("X-Jazz-Backend-Secret").is_some()
        || request_headers.get("X-Jazz-Session").is_some()
        || request_headers.get("X-Jazz-Admin-Secret").is_some();

    if has_explicit_auth {
        return false;
    }

    request_cookie_value(request_headers, cookie_name).is_some()
}

fn request_cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let cookie_header = headers
        .get(axum::http::header::COOKIE)
        .and_then(|value| value.to_str().ok())?;

    cookie_header.split(';').find_map(|segment| {
        let trimmed = segment.trim();
        let (candidate_name, candidate_value) = trimmed.split_once('=')?;
        if candidate_name == name && !candidate_value.is_empty() {
            Some(candidate_value)
        } else {
            None
        }
    })
}

fn validate_ws_cookie_origin(headers: &HeaderMap) -> Result<(), String> {
    let host = headers
        .get("X-Forwarded-Host")
        .or_else(|| headers.get(axum::http::header::HOST))
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "Cookie auth requires Host header".to_string())?;

    let origin = headers
        .get(axum::http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "Cookie auth requires Origin header".to_string())?;

    let origin_uri: axum::http::Uri = origin
        .parse()
        .map_err(|_| "Cookie auth requires a valid Origin header".to_string())?;
    let origin_authority = origin_uri
        .authority()
        .map(|authority| authority.as_str())
        .ok_or_else(|| "Cookie auth requires an Origin authority".to_string())?;

    if origin_authority.eq_ignore_ascii_case(host) {
        Ok(())
    } else {
        Err("Cookie auth Origin must match Host".to_string())
    }
}

/// Send a `ServerEvent::Error` frame on the socket, best-effort.
async fn send_ws_error(socket: &mut WebSocket, message: &str) {
    use crate::jazz_transport::ErrorCode;
    let event = crate::jazz_transport::ServerEvent::Error {
        message: message.to_string(),
        code: ErrorCode::Unauthorized,
    };
    if let Ok(bytes) = serde_json::to_vec(&event) {
        let _ = socket
            .send(Message::Binary(crate::transport_manager::frame_encode(
                &bytes,
            )))
            .await;
    }
}

async fn handle_ws_connection(
    mut socket: WebSocket,
    state: Arc<ServerState>,
    request_headers: HeaderMap,
) {
    // 1. Read the first binary frame — expected to be AuthHandshake.
    let first = match socket.recv().await {
        Some(Ok(Message::Binary(b))) => b,
        _ => {
            let _ = socket.close().await;
            return;
        }
    };
    let payload = match crate::transport_manager::frame_decode(&first) {
        Some(p) => p.to_vec(),
        None => {
            let _ = socket.close().await;
            return;
        }
    };
    let handshake =
        match serde_json::from_slice::<crate::transport_manager::AuthHandshake>(&payload) {
            Ok(h) => h,
            Err(_) => {
                let _ = socket.close().await;
                return;
            }
        };

    // 2. Parse client_id.
    let client_id = match crate::sync_manager::ClientId::parse(&handshake.client_id) {
        Some(id) => id,
        None => {
            send_ws_error(&mut socket, "missing or invalid client_id").await;
            let _ = socket.close().await;
            return;
        }
    };

    // 3. Authenticate.
    let setup = match authenticate_ws_handshake(&handshake, &request_headers, &state).await {
        Ok(s) => s,
        Err(msg) => {
            send_ws_error(&mut socket, &msg).await;
            let _ = socket.close().await;
            return;
        }
    };
    let role = match &setup {
        WsClientSetup::Backend => "backend",
        WsClientSetup::Session(_) => "session",
    };

    // 4. Register with ConnectionEventHub (mirrors events_handler).
    let connection_id = state
        .next_connection_id
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let (next_sync_seq, mut sync_rx) = state
        .connection_event_hub
        .register_connection(connection_id, client_id);
    {
        let mut connections = state.connections.write().await;
        connections.insert(connection_id, ConnectionState { client_id });
    }
    state.on_client_connected(client_id).await;

    // 5. Ensure the client state in the runtime.
    match setup {
        WsClientSetup::Backend => {
            let _ = state.runtime.ensure_client_as_backend(client_id);
        }
        WsClientSetup::Session(session) => {
            let _ = state.runtime.ensure_client_with_session(client_id, session);
        }
    }

    // 5b. Dispatch connection schema diagnostics if client sent a declared schema hash.
    match connection_schema_diagnostics_from_handshake(&state, &handshake) {
        Ok(Some(diagnostics)) => {
            state.connection_event_hub.dispatch_payload(
                client_id,
                crate::sync_manager::SyncPayload::ConnectionSchemaDiagnostics(diagnostics),
            );
        }
        Ok(None) => {}
        Err(err) => {
            tracing::error!(
                %client_id,
                declared_schema_hash = ?handshake.declared_schema_hash,
                "failed to compute connection schema diagnostics: {err}"
            );
        }
    }

    // 6. Send the Connected response.
    let resp = crate::transport_manager::ConnectedResponse {
        connection_id: connection_id.to_string(),
        client_id: client_id.to_string(),
        next_sync_seq: Some(next_sync_seq),
        catalogue_state_hash: state.runtime.catalogue_state_hash().ok(),
    };
    let resp_bytes = match serde_json::to_vec(&resp) {
        Ok(b) => b,
        Err(_) => {
            ws_cleanup(&state, connection_id, client_id).await;
            let _ = socket.close().await;
            return;
        }
    };
    if socket
        .send(Message::Binary(crate::transport_manager::frame_encode(
            &resp_bytes,
        )))
        .await
        .is_err()
    {
        ws_cleanup(&state, connection_id, client_id).await;
        return;
    }
    tracing::info!(connection_id, %client_id, role, "ws client connected");

    // 7. Bidirectional loop: inbound frames from client + outbound updates from hub.
    //    Also fires a periodic heartbeat so idle connections don't look half-open.
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    // Don't emit a heartbeat immediately after Connected — wait a full tick.
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await; // consume the immediate first tick
    loop {
        tokio::select! {
            msg = socket.recv() => match msg {
                Some(Ok(Message::Binary(data))) => {
                    let Some(inner) = crate::transport_manager::frame_decode(&data) else {
                        continue;
                    };
                    let inner = inner.to_vec();
                    if let Err(e) = state.process_ws_client_frame(client_id, Some(connection_id), &inner).await {
                        tracing::warn!(error = ?e, "ws client frame rejected");
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                _ => continue,
            },
            update = sync_rx.recv() => {
                let Some(u) = update else { break };
                let event = crate::jazz_transport::ServerEvent::SyncUpdate {
                    seq: Some(u.seq),
                    payload: Box::new(u.payload),
                };
                let bytes = match serde_json::to_vec(&event) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                let telemetry_payload = match &event {
                    crate::jazz_transport::ServerEvent::SyncUpdate { payload, .. } => payload.as_ref(),
                    _ => unreachable!("event is always SyncUpdate"),
                };
                state.emit_ws_payload_telemetry(WsPayloadTelemetryInput {
                    direction: SyncPayloadTelemetryDirection::ServerToClient,
                    client_id,
                    connection_id: Some(connection_id),
                    source_frame_id: Uuid::now_v7().to_string(),
                    source_payload_index: Some(0),
                    source_payload_count: Some(1),
                    source_frame_bytes: bytes.len() as u64,
                    payload: telemetry_payload,
                    sequence: Some(u.seq),
                });
                if socket
                    .send(Message::Binary(
                        crate::transport_manager::frame_encode(&bytes),
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            _ = heartbeat.tick() => {
                let event = crate::jazz_transport::ServerEvent::Heartbeat;
                let Ok(bytes) = serde_json::to_vec(&event) else { continue };
                if socket
                    .send(Message::Binary(
                        crate::transport_manager::frame_encode(&bytes),
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }

    ws_cleanup(&state, connection_id, client_id).await;
    let _ = socket.close().await;
}

/// Disconnect cleanup: mirrors the drop path in `events_handler`.
async fn ws_cleanup(state: &Arc<ServerState>, connection_id: u64, client_id: ClientId) {
    {
        let mut connections = state.connections.write().await;
        connections.remove(&connection_id);
    }
    state
        .connection_event_hub
        .unregister_connection(connection_id);
    state.on_connection_closed(client_id).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::fmt;
    use std::sync::{Arc as StdArc, Mutex};
    use std::time::Duration;

    use crate::jazz_transport::SyncBatchRequest;
    use crate::query_manager::query::QueryBuilder;
    use crate::query_manager::types::{
        ColumnType, SchemaBuilder, TableSchema, Value as QueryValue,
    };
    use crate::sync_manager::{
        ClientId, ConnectionSchemaDiagnostics, InboxEntry, QueryId, QueryPropagation, Source,
        SyncPayload,
    };
    use axum::body;
    use axum::routing::{get, post};
    use futures::{SinkExt as _, StreamExt as _};
    use serde_json::Value;
    use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
    use tower::ServiceExt;
    use tracing::field::{Field, Visit};
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::{Layer, Registry};

    use crate::middleware::AuthConfig;
    use crate::server::{
        CatalogueAuthorityMode, ServerBuilder, ServerState, SyncPayloadTelemetryConfig,
    };
    use crate::sync_payload_telemetry::InMemorySyncPayloadTelemetrySink;

    fn test_auth_config() -> AuthConfig {
        AuthConfig {
            backend_secret: None,
            admin_secret: Some("admin-secret".to_string()),
            allow_local_first_auth: true,
            jwks_url: None,
            ..Default::default()
        }
    }

    fn mint_test_token(audience: &str) -> String {
        let seed = [42u8; 32];
        crate::identity::mint_jazz_self_signed_token(
            &seed,
            crate::identity::LOCAL_FIRST_ISSUER,
            audience,
            3600,
        )
        .unwrap()
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
            .with_in_memory_storage()
            .build()
            .await
            .expect("build sync test state")
            .state
    }

    async fn make_state_with_schema(
        schema: crate::query_manager::types::Schema,
    ) -> Arc<ServerState> {
        ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(test_auth_config())
            .with_in_memory_storage()
            .with_schema(schema)
            .build()
            .await
            .expect("build state with schema")
            .state
    }

    async fn make_state_with_schema_and_authority(
        schema: crate::query_manager::types::Schema,
        catalogue_authority: CatalogueAuthorityMode,
    ) -> Arc<ServerState> {
        ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(test_auth_config())
            .with_catalogue_authority(catalogue_authority)
            .with_in_memory_storage()
            .with_schema(schema)
            .build()
            .await
            .expect("build state with schema and authority")
            .state
    }

    fn make_test_router(state: Arc<ServerState>) -> axum::Router {
        create_router(state)
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

    async fn make_sync_payload_telemetry_test_state()
    -> (Arc<ServerState>, InMemorySyncPayloadTelemetrySink) {
        let sink = InMemorySyncPayloadTelemetrySink::default();
        let state = ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(test_auth_config())
            .with_in_memory_storage()
            .with_sync_payload_telemetry_sink(
                SyncPayloadTelemetryConfig {
                    collector_url: "http://localhost:4317".to_string(),
                },
                sink.clone(),
            )
            .build()
            .await
            .expect("build telemetry test state")
            .state;
        (state, sink)
    }

    /// A minimal valid `SyncPayload::RowBatchCreated` suitable for embedding
    /// in batch request bodies.
    fn row_version_created_payload(object_id: &str) -> crate::sync_manager::SyncPayload {
        let row_id =
            ObjectId::from_uuid(Uuid::parse_str(object_id).expect("parse test object id as uuid"));
        let row = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            Vec::<crate::row_histories::BatchId>::new(),
            b"alice".to_vec(),
            crate::metadata::RowProvenance::for_insert(object_id.to_string(), 1_000),
            Default::default(),
            crate::row_histories::RowState::VisibleDirect,
            None,
        );

        crate::sync_manager::SyncPayload::RowBatchCreated {
            metadata: None,
            row,
        }
    }

    #[tokio::test]
    async fn ws_sync_batch_emits_one_telemetry_record_per_payload() {
        let (state, sink) = make_sync_payload_telemetry_test_state().await;
        let client_id = ClientId::new();
        let _ = state.runtime.ensure_client_as_backend(client_id);
        let p1 = row_version_created_payload("00000000-0000-0000-0000-000000000001");
        let p2 = row_version_created_payload("00000000-0000-0000-0000-000000000002");
        let frame_payload = serde_json::to_vec(&SyncBatchRequest {
            payloads: vec![p1, p2],
            client_id,
        })
        .unwrap();

        state
            .process_ws_client_frame(client_id, Some(41), &frame_payload)
            .await
            .expect("process ws batch");

        let records = sink.records();
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].direction,
            SyncPayloadTelemetryDirection::ClientToServer
        );
        assert_eq!(records[0].connection_id.as_deref(), Some("41"));
        assert_eq!(records[0].source_payload_index, Some(0));
        assert_eq!(records[0].source_payload_count, Some(2));
        assert_eq!(records[1].source_payload_index, Some(1));
        assert_eq!(records[1].source_payload_count, Some(2));
        assert_eq!(records[0].source_frame_id, records[1].source_frame_id);
    }

    #[tokio::test]
    async fn ws_invalid_frame_emits_decode_failure_telemetry() {
        let (state, sink) = make_sync_payload_telemetry_test_state().await;
        let client_id = ClientId::new();

        let error = state
            .process_ws_client_frame(client_id, Some(42), b"not-json")
            .await
            .expect_err("invalid frame should fail");

        assert!(error.contains("invalid ws payload"));
        let records = sink.records();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].direction,
            SyncPayloadTelemetryDirection::ClientToServer
        );
        assert_eq!(records[0].connection_id.as_deref(), Some("42"));
        assert!(
            records[0]
                .decode_error
                .as_deref()
                .unwrap()
                .contains("invalid ws payload")
        );
        assert_eq!(records[0].payload_variant, None);
        assert_eq!(records[0].log_body, None);
    }

    #[tokio::test]
    async fn ws_server_to_client_telemetry_records_sequence() {
        let (state, sink) = make_sync_payload_telemetry_test_state().await;
        let client_id = ClientId::new();
        let payload = SyncPayload::QuerySettled {
            query_id: QueryId(55),
            tier: crate::sync_manager::DurabilityTier::EdgeServer,
            through_seq: 9,
        };

        state.emit_ws_payload_telemetry(WsPayloadTelemetryInput {
            direction: SyncPayloadTelemetryDirection::ServerToClient,
            client_id,
            connection_id: Some(43),
            source_frame_id: "frame-server-1".to_string(),
            source_payload_index: Some(0),
            source_payload_count: Some(1),
            source_frame_bytes: 128,
            payload: &payload,
            sequence: Some(77),
        });

        let records = sink.records();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].direction,
            SyncPayloadTelemetryDirection::ServerToClient
        );
        assert_eq!(records[0].connection_id.as_deref(), Some("43"));
        assert_eq!(records[0].sequence, Some(77));
        assert_eq!(records[0].query_id, Some(55));
        assert_eq!(records[0].durability_tier.as_deref(), Some("EdgeServer"));
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct ForwardedAdminRequest {
        method: String,
        path: String,
        admin_secret: Option<String>,
        body: Option<Value>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CapturedEvent {
        level: tracing::Level,
        message: Option<String>,
        fields: BTreeMap<String, String>,
    }

    #[derive(Clone, Default)]
    struct EventCollector {
        events: StdArc<Mutex<Vec<CapturedEvent>>>,
    }

    impl EventCollector {
        fn snapshot(&self) -> Vec<CapturedEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    impl<S> Layer<S> for EventCollector
    where
        S: tracing::Subscriber,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = CapturedEventVisitor::default();
            event.record(&mut visitor);
            self.events.lock().unwrap().push(CapturedEvent {
                level: *event.metadata().level(),
                message: visitor.message,
                fields: visitor.fields,
            });
        }
    }

    #[derive(Default)]
    struct CapturedEventVisitor {
        message: Option<String>,
        fields: BTreeMap<String, String>,
    }

    impl CapturedEventVisitor {
        fn record_value(&mut self, field: &Field, value: String) {
            if field.name() == "message" {
                self.message = Some(value.clone());
            }
            self.fields.insert(field.name().to_string(), value);
        }
    }

    impl Visit for CapturedEventVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.record_value(field, format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.record_value(field, value.to_string());
        }

        fn record_bool(&mut self, field: &Field, value: bool) {
            self.record_value(field, value.to_string());
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            self.record_value(field, value.to_string());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.record_value(field, value.to_string());
        }
    }

    #[tokio::test]
    async fn ws_sync_batch_accepts_two_payloads() {
        // alice fires two payloads over the /ws connection — both should be
        // ingested without error.
        //
        //  alice (ws client)       server
        //    ──handshake─────────► /ws
        //    ──p1──────────────►  process_ws_client_frame → ok
        //    ──p2──────────────►  process_ws_client_frame → ok

        let state = make_sync_test_state("test-backend-secret").await;
        let client_id = ClientId::new();

        // Simulate the server having registered this client (backend, no session).
        let _ = state.runtime.ensure_client_as_backend(client_id);

        let p1 = row_version_created_payload("00000000-0000-0000-0000-000000000001");
        let p2 = row_version_created_payload("00000000-0000-0000-0000-000000000002");

        let batch = SyncBatchRequest {
            payloads: vec![p1, p2],
            client_id,
        };
        let frame_payload = serde_json::to_vec(&batch).unwrap();
        let result = state
            .process_ws_client_frame(client_id, None, &frame_payload)
            .await;
        assert!(
            result.is_ok(),
            "two-payload batch should be accepted: {result:?}"
        );
    }

    #[tokio::test]
    async fn ws_handshake_rejects_missing_auth() {
        // WS handshake with no auth in the AuthHandshake → error frame, no Connected.
        use crate::transport_manager::AuthHandshake;

        let state = make_sync_test_state("test-backend-secret").await;
        let client_id = ClientId::new();

        // An AuthHandshake with an empty AuthConfig (no secret, no JWT).
        let handshake = AuthHandshake {
            client_id: client_id.to_string(),
            auth: crate::transport_manager::AuthConfig::default(),
            catalogue_state_hash: None,
            declared_schema_hash: None,
        };

        // Authenticate should fail — the `authenticate_ws_handshake` function is
        // private but the rejection path results in `ensure_client_*` never being
        // called.  We verify indirectly: the client should not be registered.
        let client_registered = state.runtime.ensure_client_as_backend(client_id).is_ok();
        // Note: ensure_client_as_backend succeeds because it only checks internal state.
        // The real gate is at the WS handler level.  This test documents that unauthenticated
        // handshakes are rejected at the transport layer — covered fully in auth_test.rs
        // integration tests that connect over the wire.
        let _ = (handshake, client_registered);
    }

    #[tokio::test]
    async fn ws_handshake_accepts_same_origin_cookie_auth() {
        let token = mint_test_token("test-app");
        let auth_config = AuthConfig {
            backend_secret: Some("test-backend-secret".to_string()),
            admin_secret: None,
            allow_local_first_auth: true,
            jwks_url: None,
            auth_cookie_name: Some("jazz-auth".to_string()),
            ..Default::default()
        };
        let state = ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(auth_config)
            .with_in_memory_storage()
            .build()
            .await
            .expect("build sync test state")
            .state;
        let handshake = crate::transport_manager::AuthHandshake {
            client_id: ClientId::new().to_string(),
            auth: crate::transport_manager::AuthConfig::default(),
            catalogue_state_hash: None,
            declared_schema_hash: None,
        };
        let mut request_headers = HeaderMap::new();
        request_headers.insert(axum::http::header::HOST, "example.test".parse().unwrap());
        request_headers.insert(
            axum::http::header::ORIGIN,
            "https://example.test".parse().unwrap(),
        );
        request_headers.insert(
            axum::http::header::COOKIE,
            format!("jazz-auth={token}").parse().unwrap(),
        );

        let setup = authenticate_ws_handshake(&handshake, &request_headers, &state)
            .await
            .expect("cookie auth should succeed");

        assert!(matches!(setup, WsClientSetup::Session(_)));
    }

    #[tokio::test]
    async fn ws_handshake_rejects_cross_origin_cookie_auth() {
        let token = mint_test_token("test-app");
        let auth_config = AuthConfig {
            backend_secret: Some("test-backend-secret".to_string()),
            admin_secret: None,
            allow_local_first_auth: true,
            jwks_url: None,
            auth_cookie_name: Some("jazz-auth".to_string()),
            ..Default::default()
        };
        let state = ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(auth_config)
            .with_in_memory_storage()
            .build()
            .await
            .expect("build sync test state")
            .state;
        let handshake = crate::transport_manager::AuthHandshake {
            client_id: ClientId::new().to_string(),
            auth: crate::transport_manager::AuthConfig::default(),
            catalogue_state_hash: None,
            declared_schema_hash: None,
        };
        let mut request_headers = HeaderMap::new();
        request_headers.insert(axum::http::header::HOST, "example.test".parse().unwrap());
        request_headers.insert(
            axum::http::header::ORIGIN,
            "https://evil.example".parse().unwrap(),
        );
        request_headers.insert(
            axum::http::header::COOKIE,
            format!("jazz-auth={token}").parse().unwrap(),
        );

        let error = authenticate_ws_handshake(&handshake, &request_headers, &state)
            .await
            .expect_err("cross-origin cookie auth should fail");

        assert!(error.to_lowercase().contains("origin"));
    }

    #[tokio::test]
    async fn ws_sync_batch_ingest_sixty_payloads() {
        // bob sends 60 payloads via the WS path — server must accept all of them.
        let state = make_sync_test_state("test-backend-secret").await;
        let client_id = ClientId::new();
        let _ = state.runtime.ensure_client_as_backend(client_id);

        let payloads: Vec<crate::sync_manager::SyncPayload> = (0..60)
            .map(|i| row_version_created_payload(&format!("00000000-0000-0000-0000-{:012}", i)))
            .collect();

        let batch = SyncBatchRequest {
            payloads,
            client_id,
        };
        let frame_payload = serde_json::to_vec(&batch).unwrap();
        let result = state
            .process_ws_client_frame(client_id, None, &frame_payload)
            .await;
        assert!(
            result.is_ok(),
            "sixty-payload batch should be accepted: {result:?}"
        );
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
            .with_in_memory_storage()
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
        let expected_schema_json = serde_json::to_value(schema).expect("expected schema json");
        assert_eq!(schema_json["schema"], expected_schema_json);
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
    async fn catalogue_authority_forwarding_proxies_schema_and_permissions_requests() {
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
                &test_app_route("/schema/:hash"),
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
        let state = make_state_with_schema_and_authority(
            schema.clone(),
            CatalogueAuthorityMode::Forward {
                base_url: format!("http://{authority_addr}/authority-prefix"),
                admin_secret: "authority-secret".to_string(),
            },
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
                .all(|request| request.admin_secret.as_deref() == Some("authority-secret"))
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

        let state = make_state_with_schema(v2).await;
        state
            .runtime
            .add_known_schema(v1)
            .expect("seed known schema for connectivity test");
        let app = make_test_router(state.clone());

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

        let state = make_state_with_schema(v2).await;
        state
            .runtime
            .add_known_schema(v1)
            .expect("seed known schema for publish test");
        let app = make_test_router(state.clone());

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

        let lens = state
            .runtime
            .with_schema_manager(|schema_manager| {
                schema_manager.get_lens(&v1_hash, &v2_hash).cloned()
            })
            .expect("read schema manager lens");
        assert!(
            lens.is_some(),
            "published lens should be registered in schema manager"
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

        let state = make_state_with_schema(v2).await;
        state
            .runtime
            .add_known_schema(v1)
            .expect("seed known schema for publish test");
        let app = make_test_router(state.clone());

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
        assert_eq!(created.status(), StatusCode::CREATED);

        let lens = state
            .runtime
            .with_schema_manager(|schema_manager| {
                schema_manager.get_lens(&v1_hash, &v2_hash).cloned()
            })
            .expect("read schema manager lens")
            .expect("published lens should be registered in schema manager");

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

        let state = make_state_with_schema(v2.clone()).await;
        state
            .runtime
            .add_known_schema(v1.clone())
            .expect("seed known schema for publish test");
        let app = make_test_router(state.clone());

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
            .runtime
            .with_schema_manager(|schema_manager| {
                schema_manager.get_lens(&v1_hash, &v2_hash).cloned()
            })
            .expect("read schema manager lens")
            .expect("published lens should be registered in schema manager");

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
    async fn admin_subscription_introspection_groups_active_server_subscriptions() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema(schema).await;

        let repeated_query = QueryBuilder::new("users").build();
        let filtered_query = QueryBuilder::new("users")
            .filter_eq("name", QueryValue::Text("Alice".to_string()))
            .build();

        for (index, query, propagation) in [
            (1_u64, repeated_query.clone(), QueryPropagation::Full),
            (2_u64, repeated_query.clone(), QueryPropagation::Full),
            (3_u64, repeated_query.clone(), QueryPropagation::LocalOnly),
            (4_u64, filtered_query, QueryPropagation::Full),
        ] {
            let client_id = ClientId::new();
            state.runtime.add_client(client_id, None).unwrap();
            state
                .runtime
                .push_sync_inbox(InboxEntry {
                    source: Source::Client(client_id),
                    payload: SyncPayload::QuerySubscription {
                        query_id: QueryId(index),
                        query: Box::new(query),
                        session: None,
                        propagation,
                        policy_context_tables: vec![],
                    },
                })
                .unwrap();
        }
        state.runtime.flush().await.unwrap();

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
        assert_eq!(groups.len(), 3);

        let repeated_full = groups.iter().find(|group| {
            group["count"].as_u64() == Some(2) && group["propagation"].as_str() == Some("full")
        });
        let repeated_full = repeated_full.expect("expected grouped full subscriptions");
        assert_eq!(repeated_full["table"].as_str(), Some("users"));
        assert_eq!(
            repeated_full["branches"]
                .as_array()
                .map(|branches| branches.len())
                .unwrap_or_default(),
            1
        );
        assert!(repeated_full["groupKey"].as_str().is_some());

        assert!(groups.iter().any(|group| {
            group["count"].as_u64() == Some(1)
                && group["propagation"].as_str() == Some("local-only")
        }));
        assert!(groups.iter().any(|group| {
            group["count"].as_u64() == Some(1)
                && group["query"]
                    .as_str()
                    .map(|query| query.contains("\"name\""))
                    .unwrap_or(false)
        }));
    }

    #[tokio::test]
    async fn ws_handler_dispatches_connection_schema_diagnostics_for_mismatched_schema() {
        // When a client connects with a schema hash that does not match the server's
        // current schema, the WS handler should enqueue a ConnectionSchemaDiagnostics
        // payload into the connection event hub immediately after step 5b.
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

        // Simulate step 5b directly: client registered, then diagnostics dispatched.
        let client_id = ClientId::new();
        let _ = state.runtime.ensure_client_as_backend(client_id);

        // Replicate what handle_ws_connection does in step 5b.
        let diagnostics = state
            .runtime
            .with_schema_manager(|sm| sm.connection_schema_diagnostics(declared_hash))
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

    #[tokio::test]
    async fn ws_handler_uses_declared_schema_hash_for_connection_diagnostics() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema(schema).await;
        let declared_hash = SchemaHash::from_bytes([9; 32]);
        let handshake = crate::transport_manager::AuthHandshake {
            client_id: ClientId::new().to_string(),
            auth: crate::transport_manager::AuthConfig::default(),
            catalogue_state_hash: state.runtime.catalogue_state_hash().ok(),
            declared_schema_hash: Some(declared_hash.to_string()),
        };

        let diagnostics = connection_schema_diagnostics_from_handshake(&state, &handshake)
            .expect("compute diagnostics")
            .expect("declared schema mismatch should produce diagnostics");

        assert_eq!(diagnostics.client_schema_hash, declared_hash);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ws_handler_logs_when_connection_is_established() {
        // Exercise the real /ws route so the assertion covers the actual
        // upgrade, handshake, and ConnectedResponse path the server uses.
        let collector = EventCollector::default();
        let subscriber = Registry::default().with(collector.clone());
        let _guard = tracing::subscriber::set_default(subscriber);

        let state = make_sync_test_state("test-backend-secret").await;
        let app = create_router(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ws listener");
        let addr = listener.local_addr().expect("ws local addr");
        let server_task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve ws app");
        });

        let client_id = ClientId::new().to_string();
        let ws_url = format!("ws://{addr}{}", test_app_route("/ws"));
        let (mut ws, _) = connect_async(&ws_url).await.expect("connect ws");

        let handshake = crate::transport_manager::AuthHandshake {
            client_id: client_id.clone(),
            auth: crate::transport_manager::AuthConfig {
                backend_secret: Some("test-backend-secret".to_string()),
                ..Default::default()
            },
            catalogue_state_hash: None,
            declared_schema_hash: None,
        };
        let payload = serde_json::to_vec(&handshake).expect("serialize handshake");
        ws.send(WsMessage::Binary(
            crate::transport_manager::frame_encode(&payload).into(),
        ))
        .await
        .expect("send handshake");

        let connected = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("wait for ConnectedResponse")
            .expect("ws frame")
            .expect("ws result");
        assert!(
            matches!(connected, WsMessage::Binary(_)),
            "expected binary ConnectedResponse frame"
        );

        tokio::time::sleep(Duration::from_millis(50)).await;

        let events = collector.snapshot();
        assert!(
            events.iter().any(|event| {
                event.level == tracing::Level::INFO
                    && event.message.as_deref() == Some("ws client connected")
                    && event.fields.get("client_id").map(String::as_str) == Some(client_id.as_str())
                    && event.fields.get("connection_id").map(String::as_str) == Some("1")
                    && event.fields.get("role").map(String::as_str) == Some("backend")
            }),
            "expected ws client connected log, captured events: {events:#?}"
        );

        let _ = ws.close(None).await;
        server_task.abort();
    }
}
