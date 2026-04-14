#![cfg(feature = "test")]

//! Authentication integration tests for the Jazz server.
//!
//! Tests the three auth mechanisms:
//! 1. JWT authentication (frontend)
//! 2. Backend session impersonation
//! 3. Admin authentication for catalogue sync

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{Json, Router, extract::State, routing::get};
use base64::Engine;
use jazz_tools::query_manager::session::Session;
use jazz_tools::server::TestingServer;
use jazz_tools::transport_manager::{AuthConfig, StreamAdapter};
use jazz_tools::ws_stream::NativeWsStream;
use jazz_tools::{ServerEvent, transport_protocol::ErrorCode};
use jsonwebtoken::{EncodingKey, Header, encode};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

// ============================================================================
// JWT helpers
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    iss: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    jazz_principal_id: Option<String>,
    claims: serde_json::Value,
    exp: u64,
}

fn future_exp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600
}

fn make_jwt(sub: &str, claims: serde_json::Value, secret: &str) -> String {
    make_jwt_with_exp(sub, claims, secret, future_exp(), None, None)
}

fn make_jwt_with_issuer(
    sub: &str,
    claims: serde_json::Value,
    secret: &str,
    issuer: &str,
    principal_id: Option<&str>,
) -> String {
    make_jwt_with_exp(
        sub,
        claims,
        secret,
        future_exp(),
        Some(issuer),
        principal_id,
    )
}

fn make_jwt_with_exp(
    sub: &str,
    claims: serde_json::Value,
    secret: &str,
    exp: u64,
    issuer: Option<&str>,
    principal_id: Option<&str>,
) -> String {
    let jwt_claims = JwtClaims {
        sub: sub.to_string(),
        iss: issuer.map(str::to_string),
        jazz_principal_id: principal_id.map(str::to_string),
        claims,
        exp,
    };
    let key = EncodingKey::from_secret(secret.as_bytes());
    let mut header = Header::new(jsonwebtoken::Algorithm::HS256);
    header.kid = Some("test-jwks-kid".to_string());
    encode(&header, &jwt_claims, &key).unwrap()
}

fn make_jwt_with_kid(sub: &str, kid: &str, secret: &str) -> String {
    make_jwt_with_kid_and_exp(sub, kid, secret, future_exp())
}

fn make_jwt_with_kid_and_exp(sub: &str, kid: &str, secret: &str, exp: u64) -> String {
    let jwt_claims = JwtClaims {
        sub: sub.to_string(),
        iss: None,
        jazz_principal_id: None,
        claims: json!({}),
        exp,
    };
    let key = EncodingKey::from_secret(secret.as_bytes());
    let mut header = Header::new(jsonwebtoken::Algorithm::HS256);
    header.kid = Some(kid.to_string());
    encode(&header, &jwt_claims, &key).unwrap()
}

fn encode_session(session: &Session) -> String {
    let json = serde_json::to_string(session).unwrap();
    base64::engine::general_purpose::STANDARD.encode(json.as_bytes())
}

// ============================================================================
// Controllable JWKS server for rotation / staleness tests
// ============================================================================

#[derive(Clone)]
struct JwksServerState {
    hits: Arc<AtomicUsize>,
    responses: Arc<tokio::sync::RwLock<Vec<Value>>>,
}

struct JwksServer {
    task: tokio::task::JoinHandle<()>,
    url: String,
    state: JwksServerState,
}

pub fn hs256_jwks(kid: &str, secret: &str) -> Value {
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret.as_bytes());
    json!({
        "keys": [{
            "kty": "oct",
            "kid": kid,
            "alg": "HS256",
            "k": encoded,
        }]
    })
}

async fn jwks_handler(State(state): State<JwksServerState>) -> Json<Value> {
    let idx = state.hits.fetch_add(1, Ordering::SeqCst);
    let responses = state.responses.read().await;
    let body = responses
        .get(idx)
        .cloned()
        .or_else(|| responses.last().cloned())
        .unwrap_or_else(|| json!({ "keys": [] }));
    Json(body)
}

impl JwksServer {
    async fn start_with_responses(responses: Vec<Value>) -> Self {
        let state = JwksServerState {
            hits: Arc::new(AtomicUsize::new(0)),
            responses: Arc::new(tokio::sync::RwLock::new(responses)),
        };
        let app = Router::new()
            .route("/jwks", get(jwks_handler))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind JWKS server");
        let addr = listener.local_addr().expect("JWKS local addr");
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve JWKS");
        });
        Self {
            task,
            url: format!("http://{addr}/jwks"),
            state,
        }
    }

    fn hits(&self) -> usize {
        self.state.hits.load(Ordering::SeqCst)
    }

    fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for JwksServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// ============================================================================
// WebSocket auth helper
// ============================================================================

fn ws_url(server: &TestingServer) -> String {
    format!("ws://127.0.0.1:{}/ws", server.port())
}

/// Attempt to authenticate via WS.
///
/// Returns `Ok(ServerEvent)` with the first frame (Connected or Error),
/// or `Err(String)` if the connection closed before any frame arrived.
async fn ws_auth(server: &TestingServer, auth: AuthConfig) -> Result<ServerEvent, String> {
    let url = ws_url(server);
    let mut stream = NativeWsStream::connect(&url)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;

    let handshake = serde_json::json!({
        "client_id": Uuid::new_v4().to_string(),
        "auth": {
            "jwt_token": auth.jwt_token,
            "backend_secret": auth.backend_secret,
            "admin_secret": auth.admin_secret,
            "backend_session": auth.backend_session,
        },
        "catalogue_state_hash": null,
    });
    let json_bytes = serde_json::to_vec(&handshake).unwrap();
    let len = json_bytes.len() as u32;
    let mut frame = Vec::with_capacity(4 + json_bytes.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&json_bytes);

    stream
        .send(&frame)
        .await
        .map_err(|e| format!("send failed: {e}"))?;

    match stream.recv().await {
        Ok(Some(data)) => ServerEvent::decode_frame(&data)
            .map(|(event, _)| event)
            .ok_or_else(|| "could not decode server frame".to_string()),
        Ok(None) => Err("connection closed without response".to_string()),
        Err(e) => Err(format!("recv error: {e}")),
    }
}

fn jwt_auth(token: &str) -> AuthConfig {
    AuthConfig {
        jwt_token: Some(token.to_string()),
        ..Default::default()
    }
}

fn backend_auth(secret: &str, session: &Session) -> AuthConfig {
    let session_value = serde_json::to_value(session).unwrap();
    AuthConfig {
        backend_secret: Some(secret.to_string()),
        backend_session: Some(session_value),
        ..Default::default()
    }
}

fn admin_auth_config(admin_secret: &str) -> AuthConfig {
    AuthConfig {
        admin_secret: Some(admin_secret.to_string()),
        ..Default::default()
    }
}

fn no_auth() -> AuthConfig {
    AuthConfig::default()
}

// ============================================================================
// Unit Tests (no server needed)
// ============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_jwt_creation() {
        let token = make_jwt("user-123", json!({"role": "admin"}), "test-secret");
        assert!(!token.is_empty());
        assert!(token.contains('.'));
    }

    #[test]
    fn test_session_encoding() {
        let session = Session::new("user-456").with_claims(json!({"teams": ["eng"]}));
        let encoded = encode_session(&session);

        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&encoded)
            .unwrap();
        let json_str = std::str::from_utf8(&decoded).unwrap();
        let parsed: Session = serde_json::from_str(json_str).unwrap();

        assert_eq!(parsed.user_id, "user-456");
        assert_eq!(parsed.claims["teams"][0], "eng");
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

const JWT_SECRET: &str = "test-jwt-secret-for-integration";
const BACKEND_SECRET: &str = "backend-secret-for-integration-tests";
const ADMIN_SECRET: &str = "admin-secret-for-integration-tests";

fn client() -> Client {
    Client::new()
}

/// Test that unauthenticated requests work for non-protected endpoints.
#[tokio::test]
async fn test_health_no_auth() {
    let server = TestingServer::start().await;

    let resp = client()
        .get(format!("{}/health", server.base_url()))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

/// Test JWT authentication via WS.
#[tokio::test]
async fn test_jwt_auth_ws() {
    let server = TestingServer::start().await;
    let token = make_jwt("jwt-user", json!({"role": "user"}), JWT_SECRET);

    let result = ws_auth(&server, jwt_auth(&token)).await;
    assert!(
        matches!(result, Ok(ServerEvent::Connected { .. })),
        "valid JWT should get Connected frame, got: {result:?}"
    );
}

/// Test invalid JWT is rejected via WS.
#[tokio::test]
async fn test_invalid_jwt_rejected() {
    let server = TestingServer::start().await;

    let result = ws_auth(&server, jwt_auth("invalid-token")).await;
    match result {
        Ok(ServerEvent::Error { code, .. }) => {
            assert_eq!(code, ErrorCode::Unauthorized);
        }
        Err(_) => {
            // Connection closed without a frame also counts as rejection.
        }
        other => panic!("expected auth rejection, got: {other:?}"),
    }
}

/// Test that connecting with no auth is rejected.
#[tokio::test]
async fn test_ws_require_session() {
    let server = TestingServer::start().await;

    let result = ws_auth(&server, no_auth()).await;
    match result {
        Ok(ServerEvent::Error { code, .. }) => {
            assert_eq!(code, ErrorCode::Unauthorized);
        }
        Err(_) => {
            // Connection closed without a frame also counts as rejection.
        }
        other => panic!("expected auth rejection, got: {other:?}"),
    }
}

/// Test that connecting with an expired JWT is rejected.
#[tokio::test]
async fn test_ws_reject_expired_jwt() {
    let jwks = JwksServer::start_with_responses(vec![hs256_jwks(
        "kid-events-expired",
        "secret-events-expired",
    )])
    .await;
    let server = TestingServer::builder()
        .with_jwks_url(jwks.url())
        .start()
        .await;

    let expired = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 60;
    let token = make_jwt_with_kid_and_exp(
        "user-expired-ws",
        "kid-events-expired",
        "secret-events-expired",
        expired,
    );

    let result = ws_auth(&server, jwt_auth(&token)).await;
    match result {
        Ok(ServerEvent::Error { code, .. }) => {
            assert_eq!(code, ErrorCode::Unauthorized);
        }
        Err(_) => {}
        other => panic!("expected rejection for expired JWT, got: {other:?}"),
    }
}

/// Test that JWT with a wrong secret is rejected.
#[tokio::test]
async fn test_ws_jwt_wrong_secret_rejected() {
    // Default server uses JWT_SECRET; sign with a different secret.
    let server = TestingServer::start().await;
    let token = make_jwt(
        "events-user",
        json!({"role": "member"}),
        "completely-wrong-secret",
    );

    let result = ws_auth(&server, jwt_auth(&token)).await;
    match result {
        Ok(ServerEvent::Error { code, .. }) => {
            assert_eq!(code, ErrorCode::Unauthorized);
        }
        Err(_) => {}
        other => panic!("expected rejection for JWT with wrong secret, got: {other:?}"),
    }
}

/// Test backend impersonation with valid secret via WS.
#[tokio::test]
async fn test_backend_impersonation_valid() {
    let server = TestingServer::start().await;
    let session = Session::new("impersonated-user");

    let result = ws_auth(&server, backend_auth(BACKEND_SECRET, &session)).await;
    assert!(
        matches!(result, Ok(ServerEvent::Connected { .. })),
        "valid backend secret should get Connected frame, got: {result:?}"
    );
}

/// Test backend impersonation with invalid secret via WS.
#[tokio::test]
async fn test_backend_impersonation_invalid_secret() {
    let server = TestingServer::start().await;
    let session = Session::new("impersonated-user");

    let result = ws_auth(&server, backend_auth("wrong-secret", &session)).await;
    match result {
        Ok(ServerEvent::Error { code, .. }) => {
            assert_eq!(code, ErrorCode::Unauthorized);
        }
        Err(_) => {}
        other => panic!("expected rejection for bad backend secret, got: {other:?}"),
    }
}

/// Test that session without backend secret is rejected via WS.
#[tokio::test]
async fn test_session_without_secret_rejected() {
    let server = TestingServer::start().await;
    let session = Session::new("impersonated-user");

    // Send session but no backend secret — backend_session without backend_secret is not valid.
    let auth = AuthConfig {
        backend_session: Some(serde_json::to_value(&session).unwrap()),
        ..Default::default()
    };
    let result = ws_auth(&server, auth).await;
    match result {
        Ok(ServerEvent::Error { code, .. }) => {
            assert_eq!(code, ErrorCode::Unauthorized);
        }
        Err(_) => {}
        other => panic!("expected rejection for session without secret, got: {other:?}"),
    }
}

/// Test that catalogue sync without admin secret is rejected via WS.
#[tokio::test]
async fn test_catalogue_sync_no_admin() {
    let server = TestingServer::start().await;

    // No admin secret provided.
    let result = ws_auth(&server, no_auth()).await;
    match result {
        Ok(ServerEvent::Error { code, .. }) => {
            assert_eq!(code, ErrorCode::Unauthorized);
        }
        Err(_) => {}
        other => panic!("expected rejection with no auth, got: {other:?}"),
    }
}

/// Test that catalogue sync with wrong admin secret is rejected via WS.
#[tokio::test]
async fn test_catalogue_sync_wrong_admin() {
    let server = TestingServer::start().await;

    let result = ws_auth(&server, admin_auth_config("wrong-admin-secret")).await;
    match result {
        Ok(ServerEvent::Error { code, .. }) => {
            assert_eq!(code, ErrorCode::Unauthorized);
        }
        Err(_) => {}
        other => panic!("expected rejection for wrong admin secret, got: {other:?}"),
    }
}

/// Test that catalogue sync with valid admin secret succeeds via WS.
#[tokio::test]
async fn test_catalogue_sync_valid_admin() {
    let server = TestingServer::start().await;

    let result = ws_auth(&server, admin_auth_config(ADMIN_SECRET)).await;
    assert!(
        matches!(result, Ok(ServerEvent::Connected { .. })),
        "valid admin secret should get Connected frame, got: {result:?}"
    );
}

// ============================================================================
// link-external HTTP tests (endpoint still exists)
// ============================================================================

#[tokio::test]
async fn test_link_external_idempotent_and_conflict() {
    let server = TestingServer::start().await;
    let token = make_jwt_with_issuer(
        "external-user",
        json!({"role": "user"}),
        JWT_SECRET,
        "https://issuer.example",
        None,
    );

    let first = client()
        .post(format!("{}/auth/link-external", server.base_url()))
        .header("Authorization", format!("Bearer {}", token))
        .header("X-Jazz-Local-Mode", "anonymous")
        .header("X-Jazz-Local-Token", "device-token-a")
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first_json: serde_json::Value = first.json().await.unwrap();
    assert_eq!(first_json["created"], json!(true));

    let second = client()
        .post(format!("{}/auth/link-external", server.base_url()))
        .header("Authorization", format!("Bearer {}", token))
        .header("X-Jazz-Local-Mode", "anonymous")
        .header("X-Jazz-Local-Token", "device-token-a")
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let second_json: serde_json::Value = second.json().await.unwrap();
    assert_eq!(second_json["created"], json!(false));

    let conflict = client()
        .post(format!("{}/auth/link-external", server.base_url()))
        .header("Authorization", format!("Bearer {}", token))
        .header("X-Jazz-Local-Mode", "anonymous")
        .header("X-Jazz-Local-Token", "device-token-b")
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_link_external_returns_expired_for_expired_jwt() {
    let jwks =
        JwksServer::start_with_responses(vec![hs256_jwks("test-jwks-kid", "secret-expired-link")])
            .await;
    let server = TestingServer::builder()
        .with_jwks_url(jwks.url())
        .start()
        .await;
    let expired = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 60;
    let token = make_jwt_with_exp(
        "external-user",
        json!({"role": "user"}),
        "secret-expired-link",
        expired,
        Some("https://issuer.example"),
        None,
    );

    let response = client()
        .post(format!("{}/auth/link-external", server.base_url()))
        .header("Authorization", format!("Bearer {}", token))
        .header("X-Jazz-Local-Mode", "anonymous")
        .header("X-Jazz-Local-Token", "device-token-a")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"], "unauthenticated");
    assert_eq!(body["code"], "expired");
}

#[tokio::test]
async fn test_link_external_returns_unauthorized_when_jwt_secret_wrong() {
    // Use a JWKS server that serves a different key than the JWT was signed with.
    let jwks = JwksServer::start_with_responses(vec![hs256_jwks(
        "test-jwks-kid",
        "different-secret-from-jwt",
    )])
    .await;
    let server = TestingServer::builder()
        .with_jwks_url(jwks.url())
        .start()
        .await;
    let token = make_jwt_with_issuer(
        "external-user",
        json!({"role": "user"}),
        JWT_SECRET, // JWT signed with JWT_SECRET but JWKS serves a different key
        "https://issuer.example",
        None,
    );

    let response = client()
        .post(format!("{}/auth/link-external", server.base_url()))
        .header("Authorization", format!("Bearer {}", token))
        .header("X-Jazz-Local-Mode", "anonymous")
        .header("X-Jazz-Local-Token", "device-token-a")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

// ============================================================================
// JWKS Rotation Tests (via WS auth)
// ============================================================================

/// After key rotation, a JWT signed with the new key should succeed
/// because the server refreshes JWKS when it encounters an unknown kid.
#[tokio::test]
async fn test_unknown_kid_triggers_jwks_refresh_and_succeeds() {
    let jwks = JwksServer::start_with_responses(vec![
        hs256_jwks("kid-old", "secret-old"),
        hs256_jwks("kid-new", "secret-new"),
    ])
    .await;
    let server = TestingServer::builder()
        .with_jwks_url(jwks.url())
        .start()
        .await;

    let token = make_jwt_with_kid("user-rotation", "kid-new", "secret-new");

    let result = ws_auth(&server, jwt_auth(&token)).await;
    assert!(
        matches!(result, Ok(ServerEvent::Connected { .. })),
        "JWT with rotated key should succeed after JWKS refresh, got: {result:?}"
    );
    assert_eq!(
        jwks.hits(),
        2,
        "should fetch JWKS twice: startup cache + one refresh for unknown kid"
    );
}

/// A JWT with an invalid signature should remain rejected even after
/// the server attempts a JWKS refresh.
#[tokio::test]
async fn test_bad_signature_stays_unauthorized_after_refresh() {
    let jwks = JwksServer::start_with_responses(vec![
        hs256_jwks("kid-sig", "good-secret"),
        hs256_jwks("kid-sig", "good-secret"),
    ])
    .await;
    let server = TestingServer::builder()
        .with_jwks_url(jwks.url())
        .start()
        .await;

    let token = make_jwt_with_kid("user-invalid", "kid-sig", "wrong-secret");

    let result = ws_auth(&server, jwt_auth(&token)).await;
    match result {
        Ok(ServerEvent::Error { code, .. }) => {
            assert_eq!(
                code,
                ErrorCode::Unauthorized,
                "invalid signature must stay unauthorized after refresh"
            );
        }
        Err(_) => {}
        other => panic!("expected unauthorized, got: {other:?}"),
    }
    assert_eq!(
        jwks.hits(),
        2,
        "signature failure should trigger one refresh attempt"
    );
}

/// Expired JWT returns an auth error via WS.
#[tokio::test]
async fn test_expired_jwt_returns_structured_401() {
    let jwks =
        JwksServer::start_with_responses(vec![hs256_jwks("kid-expired", "secret-expired")]).await;
    let server = TestingServer::builder()
        .with_jwks_url(jwks.url())
        .start()
        .await;

    let expired = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 60;
    let token = make_jwt_with_kid_and_exp("user-expired", "kid-expired", "secret-expired", expired);

    let result = ws_auth(&server, jwt_auth(&token)).await;
    match result {
        Ok(ServerEvent::Error { code, .. }) => {
            assert_eq!(code, ErrorCode::Unauthorized);
        }
        Err(_) => {}
        other => panic!("expected rejection for expired JWT, got: {other:?}"),
    }
}

/// Consecutive valid requests should use the cached JWKS without refetching.
#[tokio::test]
async fn test_jwks_cache_serves_consecutive_requests_without_refetch() {
    let jwks =
        JwksServer::start_with_responses(vec![hs256_jwks("kid-cached", "secret-cached")]).await;
    let server = TestingServer::builder()
        .with_jwks_url(jwks.url())
        .start()
        .await;

    let token = make_jwt_with_kid("user-cached", "kid-cached", "secret-cached");

    let first = ws_auth(&server, jwt_auth(&token)).await;
    assert!(
        matches!(first, Ok(ServerEvent::Connected { .. })),
        "first WS auth should succeed"
    );

    let second = ws_auth(&server, jwt_auth(&token)).await;
    assert!(
        matches!(second, Ok(ServerEvent::Connected { .. })),
        "second WS auth should succeed"
    );

    assert_eq!(
        jwks.hits(),
        1,
        "cached JWKS should serve multiple requests without refetching"
    );
}

/// Rapid requests with different unknown kids should not trigger unbounded
/// JWKS fetches. After the first forced refresh, subsequent unknown-kid
/// requests within the cooldown window should use the cached keyset.
#[tokio::test]
async fn test_rapid_unknown_kids_do_not_trigger_unbounded_refreshes() {
    let jwks =
        JwksServer::start_with_responses(vec![hs256_jwks("kid-stable", "secret-stable")]).await;
    let server = TestingServer::builder()
        .with_jwks_url(jwks.url())
        .start()
        .await;

    // Startup fetch = hit 1. Now send 5 WS auth attempts with different fabricated kids.
    for i in 0..5 {
        let token = make_jwt_with_kid(
            "user-dos",
            &format!("kid-fabricated-{i}"),
            "irrelevant-secret",
        );
        let result = ws_auth(&server, jwt_auth(&token)).await;
        match result {
            Ok(ServerEvent::Error { code, .. }) => {
                assert_eq!(code, ErrorCode::Unauthorized);
            }
            Err(_) => {}
            other => panic!("expected rejection for fabricated kid, got: {other:?}"),
        }
    }

    // Without cooldown: 1 (startup) + 5 (one forced refresh per request) = 6
    // With cooldown:    1 (startup) + 1 (first refresh, then cooldown) = 2
    assert_eq!(
        jwks.hits(),
        2,
        "rapid unknown-kid requests should trigger at most one refresh within the cooldown window"
    );
}

/// When the JWKS endpoint goes down after the cache TTL expires, requests
/// with valid JWTs should still succeed using the stale cached keyset.
#[tokio::test]
async fn test_stale_jwks_served_when_endpoint_goes_down_after_ttl_expiry() {
    let jwks = JwksServer::start_with_responses(vec![
        hs256_jwks("kid-stale", "secret-stale"),
        json!({ "keys": [] }),
    ])
    .await;

    // Set a short TTL so the cache expires quickly.
    // SAFETY: single-threaded test context; no other threads are reading this var concurrently.
    unsafe {
        std::env::set_var("JAZZ_JWKS_CACHE_TTL_SECS", "1");
    }
    let server = TestingServer::builder()
        .with_jwks_url(jwks.url())
        .start()
        .await;
    unsafe {
        std::env::remove_var("JAZZ_JWKS_CACHE_TTL_SECS");
    }

    let token = make_jwt_with_kid("user-stale", "kid-stale", "secret-stale");

    // First request: cache hit, validates OK.
    let first = ws_auth(&server, jwt_auth(&token)).await;
    assert!(
        matches!(first, Ok(ServerEvent::Connected { .. })),
        "first WS auth should succeed with cached JWKS, got: {first:?}"
    );

    // Wait for TTL to expire.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    // Second request: TTL expired, fetch fails (empty keys), should serve stale.
    let second = ws_auth(&server, jwt_auth(&token)).await;
    assert!(
        matches!(second, Ok(ServerEvent::Connected { .. })),
        "WS auth should succeed with stale JWKS when endpoint is down, got: {second:?}"
    );
}

/// Stale keysets should not be served forever. Once the entry is older
/// than TTL + max_stale, the fallback is refused and the request fails.
#[tokio::test]
async fn test_stale_jwks_refused_after_max_stale_expires() {
    let jwks = JwksServer::start_with_responses(vec![
        hs256_jwks("kid-expiry", "secret-expiry"),
        json!({ "keys": [] }),
    ])
    .await;

    // TTL=1s, max_stale=1s → total window = 2s.
    // SAFETY: single-threaded test context; no other threads are reading these vars concurrently.
    unsafe {
        std::env::set_var("JAZZ_JWKS_CACHE_TTL_SECS", "1");
        std::env::set_var("JAZZ_JWKS_MAX_STALE_SECS", "1");
    }
    let server = TestingServer::builder()
        .with_jwks_url(jwks.url())
        .start()
        .await;
    unsafe {
        std::env::remove_var("JAZZ_JWKS_CACHE_TTL_SECS");
        std::env::remove_var("JAZZ_JWKS_MAX_STALE_SECS");
    }

    let token = make_jwt_with_kid("user-expiry", "kid-expiry", "secret-expiry");

    // First request: cache hit, validates OK.
    let first = ws_auth(&server, jwt_auth(&token)).await;
    assert!(
        matches!(first, Ok(ServerEvent::Connected { .. })),
        "first WS auth should succeed, got: {first:?}"
    );

    // Wait beyond TTL + max_stale (2s total).
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

    // Request should now fail — stale keyset is too old to serve.
    let expired = ws_auth(&server, jwt_auth(&token)).await;
    match expired {
        Ok(ServerEvent::Error { code, .. }) => {
            assert_eq!(
                code,
                ErrorCode::Unauthorized,
                "stale keyset beyond max_stale should not be served"
            );
        }
        Err(_) => {}
        other => panic!("expected rejection after max_stale, got: {other:?}"),
    }
}

// ============================================================================
// Schema hash test via /admin/schemas endpoint
// ============================================================================

#[tokio::test]
async fn test_schema_hash_endpoint_returns_the_pushed_schema() {
    use jazz_tools::query_manager::types::{ColumnType, SchemaBuilder, SchemaHash, TableSchema};

    let server = TestingServer::start().await;

    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();
    let schema_hash = SchemaHash::compute(&schema);
    let expected_hash = schema_hash.to_string();

    // Push via /admin/schemas (the new endpoint).
    let publish_response = client()
        .post(format!("{}/admin/schemas", server.base_url()))
        .header("X-Jazz-Admin-Secret", ADMIN_SECRET)
        .json(&json!({ "schema": schema }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        publish_response.status(),
        StatusCode::CREATED,
        "publish should return 201"
    );

    // Verify /schemas lists the hash.
    let hashes_response = client()
        .get(format!("{}/schemas", server.base_url()))
        .header("X-Jazz-Admin-Secret", ADMIN_SECRET)
        .send()
        .await
        .unwrap();
    assert_eq!(hashes_response.status(), StatusCode::OK);
    let hashes_json: Value = hashes_response.json().await.unwrap();
    assert!(
        hashes_json["hashes"].as_array().is_some_and(|hashes| hashes
            .iter()
            .any(|hash| hash.as_str() == Some(expected_hash.as_str()))),
        "published hash should appear in /schemas"
    );

    // Verify /schema/{hash} returns the schema.
    let schema_response = client()
        .get(format!("{}/schema/{expected_hash}", server.base_url()))
        .header("X-Jazz-Admin-Secret", ADMIN_SECRET)
        .send()
        .await
        .unwrap();
    assert_eq!(schema_response.status(), StatusCode::OK);
    let schema_json: Value = schema_response.json().await.unwrap();
    let expected_schema_json = serde_json::to_value(schema.clone()).unwrap();
    assert_eq!(
        schema_json["schema"], expected_schema_json,
        "schema field should match the pushed schema"
    );
    assert!(
        schema_json["publishedAt"].is_number(),
        "response should include a publishedAt timestamp"
    );
}
