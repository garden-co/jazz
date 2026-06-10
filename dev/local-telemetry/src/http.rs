use crate::sql::{SqlRequest, execute_sql};
use crate::ui::static_asset;
use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};

const MAX_OTLP_PROXY_BODY_SIZE: usize = 20 * 1024 * 1024;

#[derive(Clone)]
pub struct AppState {
    data_dir: PathBuf,
    web_dist: PathBuf,
}

#[derive(Clone)]
pub struct OtlpProxyState {
    client: reqwest::Client,
    upstream: String,
}

pub fn app(data_dir: PathBuf, web_dist: PathBuf) -> Router {
    let state = AppState { data_dir, web_dist };

    Router::new()
        .route("/health", get(health))
        .route("/sql", post(sql))
        .fallback(static_files)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state)
}

pub fn otlp_proxy_app(upstream: SocketAddr) -> Router {
    let state = OtlpProxyState {
        client: reqwest::Client::new(),
        upstream: format!("http://{upstream}"),
    };

    Router::new()
        .route("/v1/traces", post(otlp_proxy))
        .route("/v1/logs", post(otlp_proxy))
        .route("/v1/metrics", post(otlp_proxy))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state)
}

pub async fn serve(
    host: String,
    port: u16,
    data_dir: PathBuf,
    web_dist: PathBuf,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let addr: SocketAddr = format!("{host}:{port}").parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("viewer:  http://{addr}/");
    tracing::info!("sql:     http://{addr}/sql");

    axum::serve(listener, app(data_dir, web_dist))
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await?;
    Ok(())
}

pub async fn serve_otlp_proxy(
    host: String,
    port: u16,
    upstream: SocketAddr,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let addr: SocketAddr = format!("{host}:{port}").parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("otlp:    http://{addr}/");
    tracing::info!("rotel:   http://{upstream}/");

    axum::serve(listener, otlp_proxy_app(upstream))
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await?;
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

async fn sql(
    State(state): State<AppState>,
    Json(request): Json<SqlRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match execute_sql(state.data_dir, request).await {
        Ok(response) => Ok(Json(json!(response))),
        Err(err) => Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": err.to_string() })),
        )),
    }
}

async fn otlp_proxy(
    State(state): State<OtlpProxyState>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    match forward_otlp(state, uri, headers, body).await {
        Ok(response) => response,
        Err(err) => (
            StatusCode::BAD_GATEWAY,
            format!("failed to forward OTLP request to Rotel: {err}"),
        )
            .into_response(),
    }
}

async fn forward_otlp(
    state: OtlpProxyState,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> anyhow::Result<Response> {
    let bytes = to_bytes(body, MAX_OTLP_PROXY_BODY_SIZE).await?;
    let mut request = state
        .client
        .post(format!("{}{}", state.upstream, uri.path()))
        .body(bytes.to_vec());

    for name in [
        header::CONTENT_TYPE,
        header::CONTENT_ENCODING,
        header::ACCEPT,
        header::ACCEPT_ENCODING,
    ] {
        if let Some(value) = headers.get(&name) {
            request = request.header(name.as_str(), value.as_bytes());
        }
    }

    let upstream_response = request.send().await?;
    let status = StatusCode::from_u16(upstream_response.status().as_u16())?;
    let content_type = upstream_response
        .headers()
        .get(header::CONTENT_TYPE.as_str())
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let bytes = upstream_response.bytes().await?;

    let mut response = Body::from(bytes).into_response();
    *response.status_mut() = status;
    if let Some(content_type) = content_type {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str(&content_type)
                .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
        );
    }

    Ok(response)
}

async fn static_files(State(state): State<AppState>, uri: axum::http::Uri) -> Response {
    let path = uri.path();
    match static_asset(&state.web_dist, path).await {
        Ok(asset) => {
            let mut response = Body::from(asset.bytes).into_response();
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_str(asset.content_type)
                    .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
            );
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            response
        }
        Err(message) => (StatusCode::INTERNAL_SERVER_ERROR, message).into_response(),
    }
}
