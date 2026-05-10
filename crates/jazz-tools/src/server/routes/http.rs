//! HTTP endpoint handlers, request/response DTOs, and HTTP-specific helpers.

//! HTTP routes for the Jazz server.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Json, Response},
};
use serde::{Deserialize, Serialize};

use crate::jazz_transport::ErrorResponse;
use crate::middleware::auth::validate_admin_secret;
use crate::query_manager::types::{
    ColumnType, Schema, SchemaHash, TableName, TablePolicies, Value,
};
use crate::schema_manager::{Lens, LensOp, LensTransform};
use crate::server::{CatalogueAuthorityMode, ServerState};

use super::utils::{
    parse_app_id_param, parse_object_id_param, parse_schema_hash_param, permissions_head_view,
    permissions_map_view, unix_timestamp_millis,
};

#[derive(Debug, Serialize)]
pub(super) struct SchemaHashesResponse {
    hashes: Vec<String>,
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SchemaConnectivityParams {
    pub(super) from_hash: String,
    pub(super) to_hash: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AdminSubscriptionIntrospectionResponse {
    app_id: String,
    generated_at: u64,
    queries: Vec<crate::query_manager::manager::ServerSubscriptionTelemetryGroup>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PublishMigrationRequest {
    from_hash: String,
    to_hash: String,
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

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct PublishSchemaRequest {
    schema: Schema,
    permissions: Option<std::collections::HashMap<TableName, TablePolicies>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PublishPermissionsRequest {
    schema_hash: String,
    permissions: std::collections::HashMap<String, TablePolicies>,
    expected_parent_bundle_object_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PublishSchemaResponse {
    object_id: String,
    hash: String,
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
pub(super) struct PermissionsHeadResponse {
    head: Option<PermissionsHeadView>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StoredPermissionsResponse {
    head: Option<PermissionsHeadView>,
    permissions: Option<std::collections::HashMap<String, TablePolicies>>,
}

#[derive(Debug, Serialize)]
pub(super) struct SchemaConnectivityResponse {
    connected: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PublishMigrationResponse {
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

fn reject_edge_catalogue_publish(state: &ServerState) -> Option<Response> {
    state.topology.is_edge().then(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request(
                "edge servers cannot publish catalogue locally; publish catalogue to the core server".to_string(),
            )),
        )
            .into_response()
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
pub(super) async fn schema_handler(
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
pub(super) async fn schema_hashes_handler(
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
pub(super) async fn schema_connectivity_handler(
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
pub(super) async fn publish_schema_handler(
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

    if let Some(response) = reject_edge_catalogue_publish(&state) {
        return response;
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

pub(super) async fn permissions_head_handler(
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

pub(super) async fn publish_permissions_handler(
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

    if let Some(response) = reject_edge_catalogue_publish(&state) {
        return response;
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
pub(super) async fn publish_migration_handler(
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

    if let Some(response) = reject_edge_catalogue_publish(&state) {
        return response;
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

pub(super) async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy"
    }))
}
