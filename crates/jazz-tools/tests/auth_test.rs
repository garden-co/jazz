#![cfg(feature = "test")]

//! Authentication integration tests for the Jazz server.
//!
//! Tests the three auth mechanisms:
//! 1. JWT authentication (frontend) — via WS handshake
//! 2. Backend session impersonation — via WS handshake
//! 3. Admin authentication — via HTTP admin endpoints

mod test_server;

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use futures::SinkExt as _;
use jazz_tools::catalogue::CatalogueEntry;
use jazz_tools::metadata::{MetadataKey, ObjectType};
use jazz_tools::query_manager::session::Session;
use jazz_tools::schema_manager::encoding::encode_schema;
use jazz_tools::sync_manager::{ClientId, SyncError, SyncPayload};
use jazz_tools::transport_manager::{
    AuthConfig, AuthHandshake, ConnectedResponse, SYNC_PROTOCOL_VERSION,
};
use jsonwebtoken::{EncodingKey, Header, encode};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use test_server::TestServer;

// ============================================================================
// Test Helpers
// ============================================================================

/// JWT claims structure.
#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    iss: Option<String>,
    claims: serde_json::Value,
    exp: u64,
}

fn future_exp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600 // 1 hour from now
}

fn make_jwt(sub: &str, claims: serde_json::Value, secret: &str) -> String {
    make_jwt_with_exp(sub, claims, secret, future_exp(), None)
}

fn make_jwt_with_exp(
    sub: &str,
    claims: serde_json::Value,
    secret: &str,
    exp: u64,
    issuer: Option<&str>,
) -> String {
    let jwt_claims = JwtClaims {
        sub: sub.to_string(),
        iss: issuer.map(str::to_string),
        claims,
        exp,
    };
    let key = EncodingKey::from_secret(secret.as_bytes());
    let mut header = Header::new(jsonwebtoken::Algorithm::HS256);
    header.kid = Some("test-jwks-kid".to_string());
    encode(&header, &jwt_claims, &key).unwrap()
}

fn encode_session(session: &Session) -> String {
    let json = serde_json::to_string(session).unwrap();
    base64::engine::general_purpose::STANDARD.encode(json.as_bytes())
}

/// Encode a 4-byte big-endian length-prefixed frame.
fn frame_encode(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode a 4-byte big-endian length-prefixed frame.
fn frame_decode(data: &[u8]) -> Option<&[u8]> {
    if data.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + len {
        return None;
    }
    Some(&data[4..4 + len])
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn ws_handshake_open(
    server: &TestServer,
    auth: AuthConfig,
) -> Result<(WsStream, ConnectedResponse), String> {
    let ws_url = format!("ws://127.0.0.1:{}/apps/{}/ws", server.port, server.app_id());
    let (mut ws, _) = connect_async(&ws_url)
        .await
        .map_err(|e| format!("ws connect failed: {e}"))?;

    let handshake = AuthHandshake {
        sync_protocol_version: SYNC_PROTOCOL_VERSION,
        client_id: ClientId::new().to_string(),
        auth,
        catalogue_state_hash: None,
        declared_schema_hash: None,
    };
    let payload = serde_json::to_vec(&handshake).expect("serialize AuthHandshake");
    ws.send(Message::Binary(frame_encode(&payload).into()))
        .await
        .map_err(|e| format!("ws send failed: {e}"))?;

    use futures::StreamExt as _;
    match ws.next().await {
        Some(Ok(Message::Binary(bytes))) => {
            let inner = frame_decode(&bytes).ok_or("malformed response frame")?;
            if let Ok(connected) = serde_json::from_slice::<ConnectedResponse>(inner) {
                Ok((ws, connected))
            } else {
                let msg = serde_json::from_slice::<serde_json::Value>(inner)
                    .ok()
                    .and_then(|v| {
                        v.get("message")
                            .and_then(|m| m.as_str())
                            .map(str::to_string)
                    })
                    .unwrap_or_else(|| "auth rejected".to_string());
                Err(msg)
            }
        }
        Some(Ok(Message::Close(_))) | None => Err("server closed connection".to_string()),
        Some(Ok(other)) => Err(format!("unexpected WS message: {other:?}")),
        Some(Err(e)) => Err(format!("ws recv error: {e}")),
    }
}

async fn ws_send_sync_payload(ws: &mut WsStream, payload: SyncPayload) -> Result<(), String> {
    let batch = jazz_tools::transport_protocol::SyncBatchRequest {
        client_id: ClientId::new(),
        payloads: vec![payload],
    };
    let bytes = batch
        .encode_payload()
        .expect("encode SyncBatchRequest payload");
    ws.send(Message::Binary(frame_encode(&bytes).into()))
        .await
        .map_err(|e| format!("ws send sync payload failed: {e}"))
}

async fn ws_recv_server_event(
    ws: &mut WsStream,
) -> Result<jazz_tools::transport_protocol::ServerEvent, String> {
    use futures::StreamExt as _;

    let message = tokio::time::timeout(std::time::Duration::from_secs(2), ws.next())
        .await
        .map_err(|_| "timed out waiting for server event".to_string())?;

    match message {
        Some(Ok(Message::Binary(bytes))) => {
            let inner = frame_decode(&bytes).ok_or("malformed response frame")?;
            jazz_tools::transport_protocol::ServerEvent::decode_payload(inner)
                .map_err(|e| format!("invalid server event: {e}"))
        }
        Some(Ok(Message::Close(_))) | None => Err("server closed connection".to_string()),
        Some(Ok(other)) => Err(format!("unexpected WS message: {other:?}")),
        Some(Err(e)) => Err(format!("ws recv error: {e}")),
    }
}

async fn ws_wait_for_sync_error(ws: &mut WsStream) -> Result<SyncError, String> {
    for _ in 0..8 {
        let event = ws_recv_server_event(ws).await?;
        if let jazz_tools::transport_protocol::ServerEvent::SyncUpdate { payload, .. } = event
            && let SyncPayload::Error(error) = *payload
        {
            return Ok(error);
        }
    }

    Err("expected SyncUpdate error event".to_string())
}

/// Perform a WS handshake against `ws://host/ws` with the given auth config.
///
/// Returns `Ok(ConnectedResponse)` on success, or `Err(message)` if the
/// server sends an error frame or closes the connection unexpectedly.
async fn ws_handshake(server: &TestServer, auth: AuthConfig) -> Result<ConnectedResponse, String> {
    let (_ws, response) = ws_handshake_open(server, auth).await?;
    Ok(response)
}

/// Build AuthConfig with a JWT token.
fn jwt_auth(token: &str) -> AuthConfig {
    AuthConfig {
        jwt_token: Some(token.to_string()),
        ..Default::default()
    }
}

/// Build AuthConfig with backend secret + session.
fn backend_auth(secret: &str, session: &Session) -> AuthConfig {
    AuthConfig {
        backend_secret: Some(secret.to_string()),
        backend_session: Some(serde_json::to_value(session).unwrap()),
        ..Default::default()
    }
}

/// Build AuthConfig with an admin secret.
fn admin_auth(secret: &str) -> AuthConfig {
    AuthConfig {
        admin_secret: Some(secret.to_string()),
        ..Default::default()
    }
}

/// Build empty AuthConfig (no credentials).
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

        // Decode and verify
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
// Integration Tests (self-spawning server)
// ============================================================================

#[cfg(test)]
mod integration_tests {
    use super::*;

    use jazz_tools::query_manager::types::{ColumnType, SchemaBuilder, SchemaHash, TableSchema};
    use serde_json::Value;

    const JWT_SECRET: &str = "test-jwt-secret-for-integration";
    const BACKEND_SECRET: &str = "backend-secret-for-integration-tests";
    const ADMIN_SECRET: &str = "admin-secret-for-integration-tests";

    fn http_client() -> Client {
        Client::new()
    }

    /// Test that unauthenticated requests work for non-protected endpoints.
    #[tokio::test]
    async fn test_health_no_auth() {
        let server = TestServer::start().await;

        let resp = http_client()
            .get(format!("{}/health", server.base_url()))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// JWT authentication succeeds on WS handshake.
    #[tokio::test]
    async fn test_jwt_auth_ws_handshake() {
        let server = TestServer::start().await;
        let token = make_jwt("jwt-user", json!({"role": "user"}), JWT_SECRET);

        let result = ws_handshake(&server, jwt_auth(&token)).await;
        assert!(
            result.is_ok(),
            "JWT auth should succeed on WS handshake; got: {result:?}"
        );
    }

    /// A server configured with a single static JWT key should accept matching bearer tokens.
    #[tokio::test]
    async fn test_static_jwt_public_key_auth_ws_handshake() {
        let server = TestServer::start_with_jwt_public_key(json!({
            "kty": "oct",
            "kid": "test-jwks-kid",
            "alg": "HS256",
            "k": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(JWT_SECRET.as_bytes()),
        }))
        .await;
        let token = make_jwt("jwt-user", json!({"role": "user"}), JWT_SECRET);

        let result = ws_handshake(&server, jwt_auth(&token)).await;
        assert!(
            result.is_ok(),
            "static JWT public key auth should succeed on WS handshake; got: {result:?}"
        );
        assert_eq!(
            server.jwks_hits(),
            0,
            "static JWT key auth should not fetch JWKS"
        );
    }

    /// Invalid JWT is rejected on WS handshake.
    #[tokio::test]
    async fn test_invalid_jwt_rejected_on_ws_handshake() {
        let server = TestServer::start().await;

        let result = ws_handshake(&server, jwt_auth("invalid-token")).await;
        assert!(
            result.is_err(),
            "Invalid JWT should be rejected on WS handshake"
        );
    }

    /// WS handshake with no credentials is rejected.
    #[tokio::test]
    async fn test_ws_handshake_requires_auth() {
        let server = TestServer::start().await;

        let result = ws_handshake(&server, no_auth()).await;
        assert!(
            result.is_err(),
            "WS handshake without credentials should be rejected"
        );
    }

    /// Backend impersonation with valid secret succeeds.
    #[tokio::test]
    async fn test_backend_impersonation_valid() {
        let server = TestServer::start().await;
        let session = Session::new("impersonated-user");

        let result = ws_handshake(&server, backend_auth(BACKEND_SECRET, &session)).await;
        assert!(
            result.is_ok(),
            "Backend impersonation should succeed with valid secret; got: {result:?}"
        );
    }

    /// Backend impersonation with invalid secret is rejected.
    #[tokio::test]
    async fn test_backend_impersonation_invalid_secret() {
        let server = TestServer::start().await;
        let session = Session::new("impersonated-user");

        let result = ws_handshake(&server, backend_auth("wrong-secret", &session)).await;
        assert!(
            result.is_err(),
            "Backend impersonation with wrong secret should be rejected"
        );
    }

    /// Backend auth takes priority over JWT when both are provided.
    #[tokio::test]
    async fn test_backend_priority_over_jwt() {
        let server = TestServer::start().await;
        let jwt_token = make_jwt("jwt-user", json!({}), JWT_SECRET);
        let session = Session::new("backend-user");

        // Both JWT and backend auth provided — backend should win (no error expected).
        let auth = AuthConfig {
            jwt_token: Some(jwt_token),
            backend_secret: Some(BACKEND_SECRET.to_string()),
            backend_session: Some(serde_json::to_value(&session).unwrap()),
            ..Default::default()
        };
        let result = ws_handshake(&server, auth).await;
        assert!(
            result.is_ok(),
            "Backend auth should take priority over JWT; got: {result:?}"
        );
    }

    /// Test harness should resolve the actual bound port when the CLI listens on port 0.
    #[tokio::test]
    async fn test_server_start_on_port_zero_reports_actual_bound_port() {
        let server = TestServer::start_on_port(0).await;

        assert_ne!(server.port, 0, "test server should expose the bound port");

        let health = reqwest::get(format!("{}/health", server.base_url()))
            .await
            .expect("health check request");
        assert!(health.status().is_success());
    }

    /// Admin-secret-authenticated handshakes should connect successfully.
    #[tokio::test]
    async fn test_admin_secret_ws_handshake() {
        let server = TestServer::start().await;

        let result = ws_handshake(&server, admin_auth(ADMIN_SECRET)).await;
        assert!(
            result.is_ok(),
            "admin secret should allow WS handshake; got: {result:?}"
        );
    }

    /// Valid admin secret should connect even when a bearer token is invalid.
    #[tokio::test]
    async fn test_admin_secret_short_circuits_invalid_jwt_on_ws_handshake() {
        let server = TestServer::start().await;

        let auth = AuthConfig {
            jwt_token: Some("invalid-token".to_string()),
            admin_secret: Some(ADMIN_SECRET.to_string()),
            ..Default::default()
        };
        let result = ws_handshake(&server, auth).await;
        assert!(
            result.is_ok(),
            "valid admin secret should win over an invalid JWT on WS handshake; got: {result:?}"
        );
    }

    /// Invalid admin secret must reject the handshake even if the JWT is valid.
    #[tokio::test]
    async fn test_invalid_admin_secret_rejects_valid_jwt_on_ws_handshake() {
        let server = TestServer::start().await;
        let token = make_jwt("jwt-user", json!({"role": "user"}), JWT_SECRET);

        let auth = AuthConfig {
            jwt_token: Some(token),
            admin_secret: Some("wrong-admin-secret".to_string()),
            ..Default::default()
        };
        let result = ws_handshake(&server, auth).await;
        assert!(
            result.is_err(),
            "invalid admin secret should reject the handshake even when JWT is valid"
        );
    }

    /// A WS connection authenticated with admin secret should behave like a strict
    /// backend client and reject structural schema catalogue sync.
    #[tokio::test]
    async fn test_admin_secret_ws_connection_rejects_structural_schema_catalogue_sync() {
        let server = TestServer::start().await;
        let token = make_jwt("jwt-user", json!({"role": "user"}), JWT_SECRET);
        let auth = AuthConfig {
            jwt_token: Some(token),
            admin_secret: Some(ADMIN_SECRET.to_string()),
            ..Default::default()
        };
        let (mut ws, _connected) = ws_handshake_open(&server, auth)
            .await
            .expect("handshake with admin secret");

        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("todos").column("title", ColumnType::Text))
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let object_id = schema_hash.to_object_id();
        let entry = CatalogueEntry {
            object_id,
            metadata: HashMap::from([
                (
                    MetadataKey::Type.to_string(),
                    ObjectType::CatalogueSchema.to_string(),
                ),
                (
                    MetadataKey::AppId.to_string(),
                    "00000000-0000-0000-0000-000000000001".to_string(),
                ),
                (MetadataKey::SchemaHash.to_string(), schema_hash.to_string()),
            ]),
            content: encode_schema(&schema),
        };

        ws_send_sync_payload(&mut ws, SyncPayload::CatalogueEntryUpdated { entry })
            .await
            .expect("send structural schema catalogue payload");

        let error = ws_wait_for_sync_error(&mut ws)
            .await
            .expect("receive server response for structural schema sync");
        assert_eq!(
            error,
            SyncError::CatalogueWriteDenied {
                object_id,
                branch_name: jazz_tools::object::BranchName::new("main"),
            },
            "admin-secret-authenticated WS clients should be treated as strict backend clients"
        );

        let _ = ws.close(None).await;
    }

    // ========================================================================
    // JWKS Rotation Tests
    // ========================================================================

    fn make_jwt_with_kid(sub: &str, kid: &str, secret: &str) -> String {
        make_jwt_with_kid_and_exp(sub, kid, secret, future_exp())
    }

    fn make_jwt_with_kid_and_exp(sub: &str, kid: &str, secret: &str, exp: u64) -> String {
        let jwt_claims = JwtClaims {
            sub: sub.to_string(),
            iss: None,
            claims: json!({}),
            exp,
        };
        let key = EncodingKey::from_secret(secret.as_bytes());
        let mut header = Header::new(jsonwebtoken::Algorithm::HS256);
        header.kid = Some(kid.to_string());
        encode(&header, &jwt_claims, &key).unwrap()
    }

    /// After key rotation, a JWT signed with the new key should succeed
    /// because the server refreshes JWKS when it encounters an unknown kid.
    ///
    /// ```text
    ///  startup            request (kid-new)
    ///    |                      |
    ///    v                      v
    ///  fetch JWKS ──> cache has kid-old only
    ///  (kid-old)       no match ──> force refresh
    ///                              fetch JWKS (kid-new)
    ///                              validate ──> connected
    /// ```
    #[tokio::test]
    async fn test_unknown_kid_triggers_jwks_refresh_and_succeeds() {
        let server = TestServer::start_with_jwks_responses(vec![
            test_server::hs256_jwks("kid-old", "secret-old"),
            test_server::hs256_jwks("kid-new", "secret-new"),
        ])
        .await;

        let token = make_jwt_with_kid("user-rotation", "kid-new", "secret-new");

        let result = ws_handshake(&server, jwt_auth(&token)).await;
        assert!(
            result.is_ok(),
            "JWT with rotated key should succeed after JWKS refresh; got: {result:?}"
        );
        assert_eq!(
            server.jwks_hits(),
            2,
            "should fetch JWKS twice: startup cache + one refresh for unknown kid"
        );
    }

    /// A JWT with an invalid signature should remain rejected even after
    /// the server attempts a JWKS refresh.
    #[tokio::test]
    async fn test_bad_signature_stays_unauthorized_after_refresh() {
        let server = TestServer::start_with_jwks_responses(vec![
            test_server::hs256_jwks("kid-sig", "good-secret"),
            test_server::hs256_jwks("kid-sig", "good-secret"),
        ])
        .await;

        let token = make_jwt_with_kid("user-invalid", "kid-sig", "wrong-secret");

        let result = ws_handshake(&server, jwt_auth(&token)).await;
        assert!(
            result.is_err(),
            "invalid signature must stay rejected after refresh"
        );
        assert_eq!(
            server.jwks_hits(),
            2,
            "signature failure should trigger one refresh attempt"
        );
    }

    #[tokio::test]
    async fn test_expired_jwt_is_rejected() {
        let server = TestServer::start_with_jwks_responses(vec![test_server::hs256_jwks(
            "kid-expired",
            "secret-expired",
        )])
        .await;

        let expired = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 60;
        let token =
            make_jwt_with_kid_and_exp("user-expired", "kid-expired", "secret-expired", expired);

        let result = ws_handshake(&server, jwt_auth(&token)).await;
        assert!(result.is_err(), "expired JWT should be rejected");
    }

    /// Consecutive valid requests should use the cached JWKS without refetching.
    #[tokio::test]
    async fn test_jwks_cache_serves_consecutive_requests_without_refetch() {
        let server = TestServer::start_with_jwks_responses(vec![test_server::hs256_jwks(
            "kid-cached",
            "secret-cached",
        )])
        .await;

        let token = make_jwt_with_kid("user-cached", "kid-cached", "secret-cached");

        let first = ws_handshake(&server, jwt_auth(&token)).await;
        assert!(first.is_ok(), "first WS handshake should succeed");

        let second = ws_handshake(&server, jwt_auth(&token)).await;
        assert!(second.is_ok(), "second WS handshake should succeed");

        assert_eq!(
            server.jwks_hits(),
            1,
            "cached JWKS should serve multiple requests without refetching"
        );
    }

    /// Rapid requests with different unknown kids should not trigger unbounded
    /// JWKS fetches. After the first forced refresh, subsequent unknown-kid
    /// requests within the cooldown window should reuse the cached keyset.
    ///
    /// ```text
    ///  startup    req(kid-0)     req(kid-1)     req(kid-2)
    ///    |            |              |              |
    ///    v            v              v              v
    ///  fetch(1)   no match →     no match →     no match →
    ///             refresh(2)     cooldown ──>   cooldown ──>
    ///             no match →     use cached     use cached
    ///             reject         reject         reject
    /// ```
    #[tokio::test]
    async fn test_rapid_unknown_kids_do_not_trigger_unbounded_refreshes() {
        let server = TestServer::start_with_jwks_responses(vec![test_server::hs256_jwks(
            "kid-stable",
            "secret-stable",
        )])
        .await;

        // Startup fetch = hit 1. Now send 5 requests with different fabricated kids.
        for i in 0..5 {
            let token = make_jwt_with_kid(
                "user-dos",
                &format!("kid-fabricated-{i}"),
                "irrelevant-secret",
            );
            let result = ws_handshake(&server, jwt_auth(&token)).await;
            assert!(result.is_err(), "fabricated kid should be rejected");
        }

        // Without cooldown: 1 (startup) + 5 (one forced refresh per request) = 6
        // With cooldown:    1 (startup) + 1 (first refresh, then cooldown) = 2
        assert_eq!(
            server.jwks_hits(),
            2,
            "rapid unknown-kid requests should trigger at most one refresh within the cooldown window"
        );
    }

    /// When the JWKS endpoint goes down after the cache TTL expires, requests
    /// with valid JWTs should still succeed using the stale cached keyset.
    ///
    /// ```text
    ///  startup         request OK       TTL expires    endpoint down    request
    ///    |                |                 |               |              |
    ///    v                v                 v               v              v
    ///  fetch(1) ──>   cache hit ──>    cache stale     fetch(2) ──>    stale-if-error
    ///  kid-A          validate OK      (1s TTL)        500 error       serve stale
    ///                                                                  validate OK
    /// ```
    #[tokio::test]
    async fn test_stale_jwks_served_when_endpoint_goes_down_after_ttl_expiry() {
        // Response 1: valid key (startup fetch). Response 2+: 500 error (empty keys).
        let server = TestServer::start_with_jwks_responses_and_ttl(
            vec![
                test_server::hs256_jwks("kid-stale", "secret-stale"),
                json!({ "keys": [] }),
            ],
            1, // 1-second TTL
        )
        .await;

        let token = make_jwt_with_kid("user-stale", "kid-stale", "secret-stale");

        // First handshake: cache hit, validates OK.
        let first = ws_handshake(&server, jwt_auth(&token)).await;
        assert!(
            first.is_ok(),
            "first WS handshake should succeed with cached JWKS"
        );

        // Wait for TTL to expire.
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

        // Second handshake: TTL expired, fetch fails (empty keys), should serve stale.
        let second = ws_handshake(&server, jwt_auth(&token)).await;
        assert!(
            second.is_ok(),
            "WS handshake should succeed with stale JWKS when endpoint is down"
        );
    }

    /// Stale keysets should not be served forever. Once the entry is older
    /// than TTL + max_stale, the fallback is refused and the request fails.
    #[tokio::test]
    async fn test_stale_jwks_refused_after_max_stale_expires() {
        // TTL=1s, max_stale=1s → total window = 2s.
        let server = TestServer::start_with_jwks_responses_and_cache_config(
            vec![
                test_server::hs256_jwks("kid-expiry", "secret-expiry"),
                json!({ "keys": [] }),
            ],
            1, // TTL
            1, // max_stale
        )
        .await;

        let token = make_jwt_with_kid("user-expiry", "kid-expiry", "secret-expiry");

        // First handshake: cache hit, validates OK.
        let first = ws_handshake(&server, jwt_auth(&token)).await;
        assert!(first.is_ok(), "first WS handshake should succeed");

        // Wait beyond TTL + max_stale (2s total).
        tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

        // Handshake should now fail — stale keyset is too old.
        let expired = ws_handshake(&server, jwt_auth(&token)).await;
        assert!(
            expired.is_err(),
            "stale keyset beyond max_stale should not be served"
        );
    }

    #[tokio::test]
    async fn test_schema_hash_endpoint_returns_the_pushed_schema() {
        let server = TestServer::start().await;

        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let expected_hash = schema_hash.to_string();

        // Push schema via the typed admin endpoint.
        let publish_resp = http_client()
            .post(format!(
                "{}/apps/{}/admin/schemas",
                server.base_url(),
                server.app_id()
            ))
            .header("X-Jazz-Admin-Secret", ADMIN_SECRET)
            .json(&json!({ "schema": schema, "permissions": null }))
            .send()
            .await
            .unwrap();
        assert_eq!(publish_resp.status(), StatusCode::CREATED);

        let hashes_response = http_client()
            .get(format!(
                "{}/apps/{}/schemas",
                server.base_url(),
                server.app_id()
            ))
            .header("X-Jazz-Admin-Secret", ADMIN_SECRET)
            .send()
            .await
            .unwrap();
        assert_eq!(hashes_response.status(), StatusCode::OK);
        let hashes_json: Value = hashes_response.json().await.unwrap();
        assert!(hashes_json["hashes"].as_array().is_some_and(|hashes| {
            hashes
                .iter()
                .any(|hash| hash.as_str() == Some(expected_hash.as_str()))
        }));

        let schema_response = http_client()
            .get(format!(
                "{}/apps/{}/schema/{expected_hash}",
                server.base_url(),
                server.app_id()
            ))
            .header("X-Jazz-Admin-Secret", ADMIN_SECRET)
            .send()
            .await
            .unwrap();
        assert_eq!(schema_response.status(), StatusCode::OK);
        let schema_json: Value = schema_response.json().await.unwrap();
        let expected_schema_json = serde_json::to_value(schema.clone()).unwrap();
        assert_eq!(schema_json["schema"], expected_schema_json);
        assert!(schema_json.get("publishedAt").is_some());
    }
}
