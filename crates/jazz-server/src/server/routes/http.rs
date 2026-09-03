//! HTTP endpoint handlers, request/response DTOs, and HTTP-specific helpers.

//! HTTP routes for the Jazz server.

use std::{
    alloc::{Layout, alloc, alloc_zeroed},
    collections::HashMap,
    convert::Infallible,
    io::{self, Write},
    sync::Arc,
    time::Duration,
};

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Json, Response},
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::cell::Cell;

use crate::middleware::auth::validate_admin_secret;
use crate::server::{
    EdgeUpstreamHealth, FIXED_CATALOGUE_RESPONSE_LIMIT_BYTES, FORWARDING_APPLICATION_CHUNK_BYTES,
    MAX_CATALOGUE_REQUEST_BODY_BYTES, ServerState, ShutdownPhase,
};
use jazz::tools::public_schema::{ColumnType, Schema, SchemaHash, TableName, TablePolicies, Value};
use jazz::tools::schema_lens::{Lens, LensOp, LensTransform};
use jazz::tools::transport_error::ErrorResponse;

use super::utils::{
    parse_app_id_param, parse_object_id_param, parse_schema_hash_param, permissions_head_view,
    permissions_map_view, unix_timestamp_millis,
};

#[cfg(test)]
const FORWARDING_TEST_DEADLINE: Duration = Duration::from_secs(1);
#[cfg(not(test))]
const FORWARDING_TEST_DEADLINE: Duration = Duration::from_secs(30);

#[cfg(test)]
thread_local! {
    static FAIL_FORWARDING_ALLOCATION_AT: Cell<Option<usize>> = const { Cell::new(None) };
}

#[cfg(test)]
fn fail_next_forwarding_allocation_for_test() {
    fail_forwarding_allocation_at_index_for_test(0);
}

#[cfg(test)]
fn fail_forwarding_allocation_at_index_for_test(index: usize) {
    FAIL_FORWARDING_ALLOCATION_AT.with(|target| target.set(Some(index)));
}

fn allocation_was_injected_to_fail() -> bool {
    #[cfg(test)]
    {
        return FAIL_FORWARDING_ALLOCATION_AT.with(|target| match target.get() {
            Some(0) => {
                target.set(None);
                true
            }
            Some(index) => {
                target.set(Some(index - 1));
                false
            }
            None => false,
        });
    }
    #[cfg(not(test))]
    {
        false
    }
}

fn allocate_zeroed_backing(length: usize) -> Result<Vec<u8>, String> {
    if allocation_was_injected_to_fail() {
        return Err("catalogue forwarding allocation failed".to_owned());
    }
    if length == 0 {
        return Ok(Vec::new());
    }
    let layout = Layout::array::<u8>(length)
        .map_err(|_| "catalogue forwarding layout invalid".to_owned())?;
    let pointer = unsafe { alloc_zeroed(layout) };
    if pointer.is_null() {
        return Err("catalogue forwarding allocation failed".to_owned());
    }
    let boxed = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(pointer, length)) };
    Ok(boxed.into_vec())
}

fn allocate_empty_slots(count: usize) -> Result<Box<[Option<axum::body::Bytes>]>, String> {
    if allocation_was_injected_to_fail() {
        return Err("catalogue forwarding allocation failed".to_owned());
    }
    if count == 0 {
        return Ok(Vec::new().into_boxed_slice());
    }
    let layout = Layout::array::<Option<axum::body::Bytes>>(count)
        .map_err(|_| "catalogue forwarding descriptor layout invalid".to_owned())?;
    let pointer = unsafe { alloc(layout) as *mut Option<axum::body::Bytes> };
    if pointer.is_null() {
        return Err("catalogue forwarding descriptor allocation failed".to_owned());
    }
    for index in 0..count {
        unsafe { pointer.add(index).write(None) };
    }
    Ok(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(pointer, count)) })
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
    queries: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PublishMigrationRequest {
    from_hash: String,
    to_hash: String,
    forward: Vec<PublishTableLens>,
}
#[cfg(test)]
pub(super) fn oversized_migration_for_test() -> PublishMigrationRequest {
    PublishMigrationRequest {
        from_hash: "a".repeat(64),
        to_hash: "b".repeat(64),
        forward: (0..200_000)
            .map(|_| PublishTableLens {
                table: "t".to_owned(),
                added: false,
                removed: false,
                renamed_from: None,
                operations: Vec::new(),
            })
            .collect(),
    }
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
pub(super) struct ShutdownResponse {
    status: &'static str,
}

#[derive(Debug)]
enum RequestBodyError {
    Oversize,
    Internal(String),
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PublishMigrationResponse {
    object_id: String,
    from_hash: String,
    to_hash: String,
}
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub(super) enum CatalogueRequestBody<'a> {
    Schema(&'a PublishSchemaRequest),
    Permissions(&'a PublishPermissionsRequest),
    Migration(&'a PublishMigrationRequest),
}

struct CountingWriter {
    len: usize,
}

impl Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.len = self
            .len
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("JSON body length overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct FixedResponseBody {
    slots: Box<[Option<axum::body::Bytes>]>,
    used: usize,
    current: Option<Vec<u8>>,
    current_len: usize,
    total_len: usize,
    limit: usize,
}

impl FixedResponseBody {
    fn new(limit: usize) -> Result<Self, String> {
        let count = limit
            .checked_div(FORWARDING_APPLICATION_CHUNK_BYTES)
            .and_then(|whole| {
                limit
                    .checked_rem(FORWARDING_APPLICATION_CHUNK_BYTES)
                    .and_then(|remainder| whole.checked_add(usize::from(remainder != 0)))
            })
            .ok_or_else(|| "catalogue upstream response buffering failed".to_owned())?;
        let slots = allocate_empty_slots(count)
            .map_err(|_| "catalogue upstream response buffering failed".to_owned())?;
        Ok(Self {
            slots,
            used: 0,
            current: None,
            current_len: 0,
            total_len: 0,
            limit,
        })
    }

    fn append(&mut self, bytes: &[u8]) -> Result<(), ResponseBufferError> {
        let new_total = self
            .total_len
            .checked_add(bytes.len())
            .ok_or(ResponseBufferError::Internal)?;
        if new_total > self.limit {
            return Err(ResponseBufferError::Oversize(self.limit));
        }
        let mut offset = 0;
        while offset < bytes.len() {
            if self.current.is_none() {
                if self.used >= self.slots.len() {
                    return Err(ResponseBufferError::Internal);
                }
                let chunk = allocate_zeroed_backing(FORWARDING_APPLICATION_CHUNK_BYTES)
                    .map_err(|_| ResponseBufferError::Internal)?;
                self.current = Some(chunk);
                self.current_len = 0;
            }
            let copied =
                (FORWARDING_APPLICATION_CHUNK_BYTES - self.current_len).min(bytes.len() - offset);
            self.current.as_mut().expect("current response chunk")
                [self.current_len..self.current_len + copied]
                .copy_from_slice(&bytes[offset..offset + copied]);
            offset += copied;
            self.current_len += copied;
            if self.current_len == FORWARDING_APPLICATION_CHUNK_BYTES {
                self.finish_current()?;
            }
        }
        self.total_len = new_total;
        Ok(())
    }

    fn finish_current(&mut self) -> Result<(), ResponseBufferError> {
        let mut chunk = self.current.take().ok_or(ResponseBufferError::Internal)?;
        if self.current_len < FORWARDING_APPLICATION_CHUNK_BYTES {
            chunk.truncate(self.current_len);
        }
        if self.used >= self.slots.len() {
            return Err(ResponseBufferError::Internal);
        }
        self.slots[self.used] = Some(axum::body::Bytes::from(chunk));
        self.used += 1;
        self.current_len = 0;
        Ok(())
    }

    fn finish(mut self) -> Result<axum::body::Body, ResponseBufferError> {
        if self.current.is_some() {
            self.finish_current()?;
        }
        let stream = futures::stream::iter(
            self.slots
                .into_vec()
                .into_iter()
                .take(self.used)
                .flatten()
                .map(Ok::<_, Infallible>),
        );
        Ok(axum::body::Body::from_stream(stream))
    }
}

#[derive(Debug)]
enum ResponseBufferError {
    Oversize(usize),
    Internal,
    Read(String),
}

fn serialize_request_body(body: CatalogueRequestBody<'_>) -> Result<Vec<u8>, RequestBodyError> {
    let mut counter = CountingWriter { len: 0 };
    serde_json::to_writer(&mut counter, &body).map_err(|error| {
        RequestBodyError::Internal(format!("failed to serialize catalogue request: {error}"))
    })?;
    if counter.len > MAX_CATALOGUE_REQUEST_BODY_BYTES {
        return Err(RequestBodyError::Oversize);
    }
    let mut output = allocate_zeroed_backing(counter.len).map_err(|_| {
        RequestBodyError::Internal("failed to allocate canonical catalogue request body".to_owned())
    })?;
    let mut writer = io::Cursor::new(output.as_mut_slice());
    serde_json::to_writer(&mut writer, &body).map_err(|error| {
        RequestBodyError::Internal(format!("failed to serialize catalogue request: {error}"))
    })?;
    if writer.position() as usize != output.len() {
        return Err(RequestBodyError::Internal(
            "failed to serialize canonical catalogue request body".to_owned(),
        ));
    }
    Ok(output)
}

pub(super) async fn forward_catalogue_request(
    state: &Arc<ServerState>,
    admin_secret: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<CatalogueRequestBody<'_>>,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let Some(base_url) = state.upstream_http_url.as_deref() else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(
                "catalogue forwarding requested without a configured upstream".to_string(),
            )),
        ));
    };
    if state.shutdown.is_shutting_down() {
        return Err(forward_shutdown_error(&method));
    }
    let body = body.map(serialize_request_body).transpose().map_err(|error| {
        match error {
            RequestBodyError::Oversize => (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(ErrorResponse::bad_request(format!(
                    "canonical catalogue request body exceeds the {MAX_CATALOGUE_REQUEST_BODY_BYTES}-byte limit"
                ))),
            ),
            RequestBodyError::Internal(message) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse::internal(message)))
            }
        }
    })?;
    let app_scoped_path = format!("/apps/{}/{}", state.app_id, path.trim_start_matches('/'));
    let upstream_url = upstream_endpoint_url(base_url, &app_scoped_path).map_err(|message| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(message)),
        )
    })?;
    let endpoint_limit = if path.trim_start_matches('/').starts_with("schemas") {
        state.forwarding_policy.list_response_limit_bytes
    } else {
        FIXED_CATALOGUE_RESPONSE_LIMIT_BYTES
    };
    let mut request = state.http_client.request(method.clone(), upstream_url);
    request = request.header("X-Jazz-Admin-Secret", admin_secret);
    if let Some(body) = body {
        request = request.header(CONTENT_TYPE, "application/json").body(body);
    }
    let operation = async move {
        let response = request.send().await.map_err(|error| {
            ResponseBufferError::Read(format!("failed to reach catalogue upstream: {error}"))
        })?;
        let status =
            StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        if response
            .content_length()
            .is_some_and(|length| length > endpoint_limit as u64)
        {
            return Err(ResponseBufferError::Oversize(endpoint_limit));
        }
        let content_type = match response.headers().get(CONTENT_TYPE) {
            Some(value) if value.as_bytes().len() > 1024 => {
                return Err(ResponseBufferError::Internal);
            }
            Some(value) => HeaderValue::from_bytes(value.as_bytes()).ok(),
            None => None,
        };
        let mut body =
            FixedResponseBody::new(endpoint_limit).map_err(|_| ResponseBufferError::Internal)?;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| {
                ResponseBufferError::Read(format!("failed to read upstream response: {error}"))
            })?;
            body.append(&chunk)?;
        }
        Ok((status, content_type, body.finish()?))
    };
    let result = tokio::select! {
        biased;
        _ = state.shutdown.wait_requested() => Err(forward_shutdown_error(&method)),
        _ = tokio::time::sleep(FORWARDING_TEST_DEADLINE) => Err(forward_deadline_error(&method)),
        result = operation => result.map_err(|error| forward_response_error(&method, error)),
    };
    let (status, content_type, body) = result?;
    let mut response_builder = Response::builder().status(status);
    if let Some(content_type) = content_type {
        response_builder = response_builder.header(CONTENT_TYPE, content_type);
    }
    response_builder.body(body).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to build forwarded response: {error}"
            ))),
        )
    })
}

fn forward_shutdown_error(method: &reqwest::Method) -> (StatusCode, Json<ErrorResponse>) {
    let message = if method == reqwest::Method::POST {
        "catalogue forwarding cancelled during shutdown; the upstream mutation outcome may be unknown; verify catalogue state before retrying"
    } else {
        "catalogue forwarding cancelled because the server is shutting down"
    };
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorResponse::internal(message)),
    )
}

fn forward_deadline_error(method: &reqwest::Method) -> (StatusCode, Json<ErrorResponse>) {
    let mut message = "catalogue upstream request exceeded the 30-second total deadline".to_owned();
    if method == reqwest::Method::POST {
        message.push_str("; the upstream mutation outcome may be unknown; verify catalogue state before retrying");
    }
    (
        StatusCode::BAD_GATEWAY,
        Json(ErrorResponse::internal(message)),
    )
}

fn forward_response_error(
    method: &reqwest::Method,
    error: ResponseBufferError,
) -> (StatusCode, Json<ErrorResponse>) {
    let mut message = match error {
        ResponseBufferError::Oversize(limit) => {
            format!("catalogue upstream response body exceeds the {limit}-byte limit")
        }
        ResponseBufferError::Internal => "catalogue upstream response buffering failed".to_owned(),
        ResponseBufferError::Read(message) => message,
    };
    if method == reqwest::Method::POST {
        message.push_str("; the upstream mutation outcome may be unknown; verify catalogue state before retrying");
    }
    (
        StatusCode::BAD_GATEWAY,
        Json(ErrorResponse::internal(message)),
    )
}

fn upstream_endpoint_url(base_url: &str, path: &str) -> Result<String, String> {
    let parsed = reqwest::Url::parse(base_url)
        .map_err(|err| format!("invalid catalogue upstream URL '{base_url}': {err}"))?;
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

fn schema_connectivity_forward_path(params: &SchemaConnectivityParams) -> String {
    let mut url = reqwest::Url::parse("http://jazz.local/admin/schema-connectivity")
        .expect("static schema-connectivity URL should parse");
    url.query_pairs_mut()
        .append_pair("fromHash", &params.from_hash)
        .append_pair("toHash", &params.to_hash);

    format!(
        "{}?{}",
        url.path(),
        url.query()
            .expect("schema-connectivity forward URL should have query")
    )
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

    if state.topology.is_edge() {
        return match forward_catalogue_request(
            &state,
            admin_secret.expect("validated admin secret"),
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

    if state.topology.is_edge() {
        return match forward_catalogue_request(
            &state,
            admin_secret.expect("validated admin secret"),
            reqwest::Method::GET,
            "/schemas",
            None,
        )
        .await
        {
            Ok(response) => Err(response),
            Err(error) => Err(error.into_response()),
        };
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

    if state.topology.is_edge() {
        let forwarded_path = schema_connectivity_forward_path(&params);
        return match forward_catalogue_request(
            &state,
            admin_secret.expect("validated admin secret"),
            reqwest::Method::GET,
            &forwarded_path,
            None,
        )
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

    match state
        .catalogue
        .are_schema_hashes_connected(&state.catalogue_store, from_hash, to_hash)
    {
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

    if state.topology.is_edge() {
        return match forward_catalogue_request(
            &state,
            admin_secret.expect("validated admin secret"),
            reqwest::Method::POST,
            "/admin/schemas",
            Some(CatalogueRequestBody::Schema(&request)),
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

    if (state.runtime().is_some() || state.core_server_shell_storage_config.is_some())
        && let Err(err) = jazz::schema::JazzSchema::new(&request.schema)
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request(format!(
                "schema is not supported by the server shell: {err}"
            ))),
        )
            .into_response();
    }

    let schema = request.schema;
    let schema_hash = SchemaHash::compute(&schema);
    let object_id = match state
        .catalogue
        .publish_schema(&state.catalogue_store, schema.clone())
    {
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
    if let Err(err) = crate::server::runtime_catalogue::publish_runtime_catalogue(
        &state,
        std::slice::from_ref(&schema),
        &[],
    )
    .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to bridge schema into server shell: {err}"
            ))),
        )
            .into_response();
    }

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

    if state.topology.is_edge() {
        return match forward_catalogue_request(
            &state,
            admin_secret.expect("validated admin secret"),
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

    match state
        .catalogue
        .current_permissions_head(&state.catalogue_store)
    {
        Ok(head) => {
            let head = head.map(permissions_head_view);
            (StatusCode::OK, Json(PermissionsHeadResponse { head })).into_response()
        }
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

    if state.topology.is_edge() {
        return match forward_catalogue_request(
            &state,
            admin_secret.expect("validated admin secret"),
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

    match state.catalogue.current_permissions(&state.catalogue_store) {
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

    if state.topology.is_edge() {
        return match forward_catalogue_request(
            &state,
            admin_secret.expect("validated admin secret"),
            reqwest::Method::POST,
            "/admin/permissions",
            Some(CatalogueRequestBody::Permissions(&request)),
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

    let target_schema = match state
        .catalogue
        .known_schema(&state.catalogue_store, &schema_hash)
    {
        Ok(Some(schema)) => schema,
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
    };
    let mut schema_with_permissions = target_schema.clone();

    let permissions = request
        .permissions
        .into_iter()
        .map(|(table_name, policies)| (TableName::new(table_name), policies))
        .collect::<std::collections::HashMap<_, _>>();

    for (table_name, policies) in &permissions {
        let Some(table) = schema_with_permissions.get_mut(table_name) else {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(format!(
                    "permissions reference unknown table {}",
                    table_name.as_str()
                ))),
            )
                .into_response();
        };
        table.policies = policies.clone();
    }

    if (state.runtime().is_some() || state.core_server_shell_storage_config.is_some())
        && let Err(err) = jazz::schema::JazzSchema::new(&schema_with_permissions)
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request(format!(
                "permissions schema is not supported by the server shell: {err}"
            ))),
        )
            .into_response();
    }

    match state.catalogue.publish_permissions_bundle(
        &state.catalogue_store,
        schema_hash,
        permissions,
        expected_parent_bundle_object_id,
    ) {
        Ok(_) => match state
            .catalogue
            .current_permissions_head(&state.catalogue_store)
        {
            Ok(head) => {
                if let Err(err) =
                    crate::server::runtime_catalogue::publish_runtime_catalogue(&state, &[], &[])
                        .await
                {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse::internal(format!(
                            "failed to bridge permissions head into server shell: {err}"
                        ))),
                    )
                        .into_response();
                }
                let head = head.map(permissions_head_view);
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
        Err(crate::server::catalogue::CatalogueError::WriteError(message))
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

    if state.topology.is_edge() {
        return match forward_catalogue_request(
            &state,
            admin_secret.expect("validated admin secret"),
            reqwest::Method::POST,
            "/admin/migrations",
            Some(CatalogueRequestBody::Migration(&request)),
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

    let source_schema = match state
        .catalogue
        .known_schema(&state.catalogue_store, &source_hash)
    {
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

    let target_schema = match state
        .catalogue
        .known_schema(&state.catalogue_store, &target_hash)
    {
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
    let object_id = match state.catalogue.publish_lens(&state.catalogue_store, &lens) {
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

    if let Err(err) = crate::server::runtime_catalogue::publish_runtime_catalogue(
        &state,
        &[],
        std::slice::from_ref(&lens),
    )
    .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to bridge migration lens into server shell: {err}"
            ))),
        )
            .into_response();
    }

    if let Err(err) = state.catalogue.flush(&state.catalogue_store) {
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
        if let EdgeUpstreamHealth::Failed { reason } = state.edge_upstream_health() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "status": "unhealthy",
                    "component": "edge_upstream",
                    "reason": reason,
                })),
            )
                .into_response();
        }
        if state.topology.is_edge() && state.runtime_for_client().is_none() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "status": "not_ready",
                    "component": "runtime",
                })),
            )
                .into_response();
        }
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
    use crate::middleware::AuthConfig;
    use crate::server::{
        EdgeUpstreamHealth, ServerBuilder, ServerState, ServerTopology, StorageBackend,
    };
    use axum::body::{self, Body};
    use axum::http::{Method, Request, StatusCode};
    use jazz::tools::AppId;
    use jazz::tools::public_schema::{ColumnType, Schema, SchemaBuilder, TableSchema};
    use serde_json::Value as JsonValue;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn readiness_auth_config() -> AuthConfig {
        AuthConfig {
            admin_secret: Some("admin-secret".to_owned()),
            ..Default::default()
        }
    }

    fn readiness_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build()
    }

    async fn blank_dynamic_edge() -> Arc<ServerState> {
        ServerBuilder::new(AppId::from_name("health-blank-edge"))
            .with_auth_config(readiness_auth_config())
            .with_storage(StorageBackend::InMemory)
            .with_upstream_url("ws://127.0.0.1:9")
            .build()
            .await
            .expect("build blank dynamic edge")
            .state
    }

    async fn fixed_offline_edge() -> Arc<ServerState> {
        ServerBuilder::new(AppId::from_name("health-fixed-edge"))
            .with_auth_config(readiness_auth_config())
            .with_schema(readiness_schema())
            .with_storage(StorageBackend::InMemory)
            .with_upstream_url("ws://127.0.0.1:9")
            .build()
            .await
            .expect("build fixed offline edge")
            .state
    }

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

    /// Confirms that Alice's monitor sees her blank dynamic edge as unready
    /// until that edge has a client-usable runtime.
    ///
    /// ```text
    /// edge bootstrap ──no catalogue/runtime──► monitor: 503 not_ready
    /// ```
    #[tokio::test]
    async fn blank_dynamic_edge_reports_runtime_not_ready() {
        let state = blank_dynamic_edge().await;

        assert!(state.runtime_for_client().is_none());
        let (status, json) = health(state).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            json,
            serde_json::json!({
                "status": "not_ready",
                "component": "runtime",
            })
        );
    }

    /// Confirms that an operator's monitor sees Alice's edge connector's fatal
    /// reason before the same edge's missing-runtime readiness state.
    ///
    /// ```text
    /// edge connector ──fatal──► edge ──health──► monitor: unhealthy
    /// ```
    #[tokio::test]
    async fn fatal_edge_upstream_failure_precedes_missing_runtime() {
        let state = blank_dynamic_edge().await;
        let reason = "authority rejected edge credentials";
        state.set_edge_upstream_health(EdgeUpstreamHealth::Failed {
            reason: reason.to_owned(),
        });

        let (status, json) = health(state).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            json,
            serde_json::json!({
                "status": "unhealthy",
                "component": "edge_upstream",
                "reason": reason,
            })
        );
    }

    /// Confirms that an operator's monitor sees Alice's dynamic edge become
    /// ready only after the authority catalogue is published and marked ready.
    ///
    /// ```text
    /// authority ──catalogue──► edge shell ──ready mark──► monitor: healthy
    /// ```
    #[tokio::test]
    async fn dynamic_edge_becomes_healthy_after_runtime_publication() {
        let schema = readiness_schema();
        let authority = ServerBuilder::new(AppId::from_name("health-authority"))
            .with_auth_config(readiness_auth_config())
            .with_schema(schema)
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build authority")
            .state;
        let snapshot = authority
            .runtime()
            .expect("authority runtime")
            .trusted_catalogue_snapshot_for_test()
            .await
            .expect("read authority snapshot");
        let edge = blank_dynamic_edge().await;

        let (status, json) = health(edge.clone()).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            json,
            serde_json::json!({
                "status": "not_ready",
                "component": "runtime",
            })
        );

        edge.start_dynamic_edge_shell(snapshot, None)
            .expect("publish dynamic edge runtime");
        assert!(
            edge.runtime().is_some(),
            "runtime publication precedes readiness"
        );
        assert!(
            edge.runtime_for_client().is_none(),
            "a published but unmarked generation remains gated"
        );
        edge.mark_dynamic_edge_catalogue_ready()
            .expect("mark runtime ready");

        let (status, json) = health(edge).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json, serde_json::json!({ "status": "healthy" }));
    }

    /// Confirms that an operator's monitor keeps Alice's fixed-schema edge
    /// healthy while its upstream connector retries offline.
    ///
    /// ```text
    /// upstream ──offline──► fixed edge ──health──► monitor: healthy
    /// ```
    #[tokio::test]
    async fn fixed_schema_offline_edge_remains_healthy() {
        let state = fixed_offline_edge().await;
        assert_eq!(state.topology, ServerTopology::Edge);
        state.set_edge_upstream_health(EdgeUpstreamHealth::Reconnecting {
            reason: "upstream offline".to_owned(),
        });

        let (status, json) = health(state).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json, serde_json::json!({ "status": "healthy" }));
    }

    /// Confirms that an operator's monitor still reports Alice's fixed-schema
    /// edge unhealthy when its connector reaches a fatal terminal state.
    ///
    /// ```text
    /// edge connector ──fatal──► fixed edge ──health──► monitor: unhealthy
    /// ```
    #[tokio::test]
    async fn fatal_edge_upstream_failure_retains_unhealthy_health_response() {
        let state = fixed_offline_edge().await;
        let reason = "authority rejected edge credentials";
        state.set_edge_upstream_health(EdgeUpstreamHealth::Failed {
            reason: reason.to_owned(),
        });

        let (status, json) = health(state).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            json,
            serde_json::json!({
                "status": "unhealthy",
                "component": "edge_upstream",
                "reason": reason,
            })
        );
    }

    /// Confirms that an operator's shutdown request takes precedence for Alice's
    /// edge over both fatal-upstream and missing-runtime health states.
    ///
    /// ```text
    /// operator ──shutdown──► edge ──health──► monitor: shutting_down
    /// ```
    #[tokio::test]
    async fn shutdown_keeps_shutting_down_precedence_over_edge_readiness() {
        let state = blank_dynamic_edge().await;
        state.set_edge_upstream_health(EdgeUpstreamHealth::Failed {
            reason: "fatal upstream".to_owned(),
        });
        let shutdown = create_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/internal/shutdown")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(Body::empty())
                    .expect("shutdown request"),
            )
            .await
            .expect("shutdown response");
        assert_eq!(shutdown.status(), StatusCode::ACCEPTED);

        let (status, json) = health(state).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            json,
            serde_json::json!({
                "status": "shutting_down",
                "phase": "shutting_down",
            })
        );
    }

    /// Confirms that an operator's monitor does not apply dynamic-edge runtime
    /// readiness to Alice's core topology.
    ///
    /// ```text
    /// core without edge shell ──health──► monitor: healthy
    /// ```
    #[tokio::test]
    async fn core_topology_remains_healthy_without_a_runtime_shell() {
        let state = ServerBuilder::new(AppId::from_name("health-core"))
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build core")
            .state;
        assert_eq!(state.topology, ServerTopology::Core);
        assert!(state.runtime_for_client().is_none());

        let (status, json) = health(state).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json, serde_json::json!({ "status": "healthy" }));
    }
}

#[cfg(test)]
mod forwarding_capacity_tests {
    use super::{
        CatalogueRequestBody, FORWARDING_APPLICATION_CHUNK_BYTES, FixedResponseBody,
        PublishMigrationRequest, PublishPermissionsRequest, PublishSchemaRequest, PublishTableLens,
        ResponseBufferError, Schema, allocate_empty_slots, allocate_zeroed_backing,
        fail_forwarding_allocation_at_index_for_test, fail_next_forwarding_allocation_for_test,
        serialize_request_body,
    };

    #[test]
    fn canonical_body_is_exact_and_stable() {
        let request = PublishMigrationRequest {
            from_hash: "a".repeat(64),
            to_hash: "b".repeat(64),
            forward: Vec::new(),
        };
        let first = serialize_request_body(CatalogueRequestBody::Migration(&request))
            .expect("canonical body");
        let second = serialize_request_body(CatalogueRequestBody::Migration(&request))
            .expect("canonical body");
        assert_eq!(first, second);
        assert_eq!(
            first,
            format!(
                r#"{{"fromHash":"{}","toHash":"{}","forward":[]}}"#,
                "a".repeat(64),
                "b".repeat(64)
            )
            .into_bytes()
        );
        assert_eq!(first.capacity(), first.len());
    }
    #[test]
    fn canonical_body_matches_each_public_dto() {
        let schema = PublishSchemaRequest {
            schema: Schema::new(),
            permissions: None,
        };
        assert_eq!(
            serialize_request_body(CatalogueRequestBody::Schema(&schema)).unwrap(),
            serde_json::to_vec(&schema).unwrap()
        );

        let permissions = PublishPermissionsRequest {
            schema_hash: "a".repeat(64),
            permissions: std::collections::HashMap::new(),
            expected_parent_bundle_object_id: None,
        };
        assert_eq!(
            serialize_request_body(CatalogueRequestBody::Permissions(&permissions)).unwrap(),
            serde_json::to_vec(&permissions).unwrap()
        );

        let migration = PublishMigrationRequest {
            from_hash: "a".repeat(64),
            to_hash: "b".repeat(64),
            forward: Vec::new(),
        };
        assert_eq!(
            serialize_request_body(CatalogueRequestBody::Migration(&migration)).unwrap(),
            serde_json::to_vec(&migration).unwrap()
        );
    }
    #[test]
    fn canonical_request_cap_accepts_exact_limit_and_rejects_one_byte_over() {
        fn request_with_table_length(length: usize) -> PublishMigrationRequest {
            PublishMigrationRequest {
                from_hash: "a".repeat(64),
                to_hash: "b".repeat(64),
                forward: vec![PublishTableLens {
                    table: "t".repeat(length),
                    added: false,
                    removed: false,
                    renamed_from: None,
                    operations: Vec::new(),
                }],
            }
        }

        let base = serde_json::to_vec(&request_with_table_length(0))
            .unwrap()
            .len();
        let under = serialize_request_body(CatalogueRequestBody::Migration(
            &request_with_table_length((8 << 20) - 1 - base),
        ))
        .expect("body below cap");
        assert_eq!(under.len(), (8 << 20) - 1);
        let exact = serialize_request_body(CatalogueRequestBody::Migration(
            &request_with_table_length((8 << 20) - base),
        ))
        .expect("body at cap");
        assert_eq!(exact.len(), 8 << 20);
        assert!(
            serialize_request_body(CatalogueRequestBody::Migration(&request_with_table_length(
                (8 << 20) + 1 - base
            )))
            .is_err()
        );
    }

    #[test]
    fn fixed_chunk_capacity_is_bounded_at_each_boundary() {
        let limit = 8 << 20;
        for length in [
            0,
            1,
            FORWARDING_APPLICATION_CHUNK_BYTES - 1,
            FORWARDING_APPLICATION_CHUNK_BYTES,
            FORWARDING_APPLICATION_CHUNK_BYTES + 1,
            limit,
        ] {
            let mut body = FixedResponseBody::new(limit).expect("fixed response backing");
            body.append(&vec![0xA5; length]).expect("within-limit body");
            let retained = body
                .slots
                .iter()
                .filter_map(Option::as_ref)
                .collect::<Vec<_>>();
            assert!(retained.len() <= body.slots.len());
            assert!(
                retained
                    .iter()
                    .all(|chunk| chunk.len() <= FORWARDING_APPLICATION_CHUNK_BYTES)
            );
        }

        let mut body = FixedResponseBody::new(limit).expect("fixed response backing");
        body.append(&vec![0xA5; FORWARDING_APPLICATION_CHUNK_BYTES - 1])
            .expect("partial response body");
        assert_eq!(
            body.current
                .as_ref()
                .expect("active response chunk")
                .capacity(),
            FORWARDING_APPLICATION_CHUNK_BYTES
        );
        let mut body = FixedResponseBody::new(limit).expect("fixed response backing");
        body.append(b"backing identity").expect("response body");
        let backing = body
            .current
            .as_ref()
            .expect("active response chunk")
            .as_ptr();
        body.finish_current().expect("finish response chunk");
        assert_eq!(
            body.slots[0]
                .as_ref()
                .expect("retained response chunk")
                .as_ptr(),
            backing
        );

        let mut body = FixedResponseBody::new(limit).expect("fixed response backing");
        let error = body
            .append(&vec![0xA5; limit + 1])
            .expect_err("limit-plus-one response");
        assert!(matches!(error, ResponseBufferError::Oversize(limit) if limit == 8 << 20));
        assert!(body.slots.iter().all(Option::is_none));
    }
    #[test]
    fn injected_failure_is_fallible_for_each_response_block_index() {
        let limit = 6 * FORWARDING_APPLICATION_CHUNK_BYTES;
        for index in 0..=4 {
            let mut body = FixedResponseBody::new(limit).expect("response backing");
            fail_forwarding_allocation_at_index_for_test(index);
            let input = vec![0x5A; (index + 1) * FORWARDING_APPLICATION_CHUNK_BYTES + 1];
            assert!(
                body.append(&input).is_err(),
                "response block allocation {index} must be fallible"
            );
            assert_eq!(body.used, index);
            assert!(body.slots[..index].iter().all(Option::is_some));
            assert!(body.slots[index..].iter().all(Option::is_none));
            assert!(body.current.is_none());
            assert!(
                body.current.is_none() || body.current_len <= FORWARDING_APPLICATION_CHUNK_BYTES
            );
        }
    }

    #[test]
    fn fixed_backing_rejects_layout_overflow_without_partial_allocation() {
        assert!(allocate_zeroed_backing(usize::MAX).is_err());
        assert!(allocate_empty_slots(usize::MAX).is_err());
        let slots = allocate_empty_slots(3).expect("descriptor backing");
        assert_eq!(
            std::mem::size_of_val(&*slots),
            3 * std::mem::size_of::<Option<axum::body::Bytes>>()
        );
        assert_eq!(
            (slots.as_ptr() as usize) % std::mem::align_of::<Option<axum::body::Bytes>>(),
            0
        );
        assert!(FixedResponseBody::new(usize::MAX).is_err());
    }
    #[test]
    fn injected_allocation_failures_are_fallible_at_each_boundary() {
        let request = PublishMigrationRequest {
            from_hash: "a".repeat(64),
            to_hash: "b".repeat(64),
            forward: Vec::new(),
        };
        fail_next_forwarding_allocation_for_test();
        assert!(
            serialize_request_body(CatalogueRequestBody::Migration(&request)).is_err(),
            "request backing allocation must be fallible"
        );

        fail_next_forwarding_allocation_for_test();
        assert!(
            FixedResponseBody::new(8 << 20).is_err(),
            "descriptor backing allocation must be fallible"
        );

        let mut body = FixedResponseBody::new(8 << 20).expect("response backing");
        fail_next_forwarding_allocation_for_test();
        assert!(
            body.append(b"response").is_err(),
            "application chunk allocation must be fallible"
        );
    }
}
