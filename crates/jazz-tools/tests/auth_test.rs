#![cfg(feature = "test")]

//! Authentication integration tests for the Jazz server.
//!
//! Tests the three auth mechanisms:
//! 1. JWT authentication (frontend)
//! 2. Backend session impersonation
//! 3. Admin authentication for catalogue sync

mod test_server;

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use jazz_tools::query_manager::session::Session;
use jsonwebtoken::{EncodingKey, Header, encode};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;

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
        + 3600 // 1 hour from now
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

fn encode_session(session: &Session) -> String {
    let json = serde_json::to_string(session).unwrap();
    base64::engine::general_purpose::STANDARD.encode(json.as_bytes())
}

/// Create a valid sync batch request body (SyncBatchRequest).
fn sync_body() -> String {
    json!({
        "client_id": "01234567-89ab-cdef-0123-456789abcdef",
        "payloads": [{
            "ObjectUpdated": {
                "object_id": "01234567-89ab-cdef-0123-456789abcdef",
                "metadata": null,
                "branch_name": "main",
                "commits": []
            }
        }]
    })
    .to_string()
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
    use std::collections::HashMap;

    use jazz_tools::metadata::{MetadataKey, ObjectType};
    use jazz_tools::query_manager::types::{ColumnType, SchemaBuilder, SchemaHash, TableSchema};
    use jazz_tools::schema_manager::encode_schema;
    use serde_json::Value;
    use uuid::Uuid;

    const JWT_SECRET: &str = "test-jwt-secret-for-integration";
    const BACKEND_SECRET: &str = "backend-secret-for-integration-tests";
    const ADMIN_SECRET: &str = "admin-secret-for-integration-tests";

    fn client() -> Client {
        Client::new()
    }

    /// Test that unauthenticated requests work for non-protected endpoints.
    #[tokio::test]
    async fn test_health_no_auth() {
        let server = TestServer::start().await;

        let resp = client()
            .get(format!("{}/health", server.base_url()))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// Test JWT authentication on sync endpoint.
    #[tokio::test]
    async fn test_jwt_auth_sync() {
        let server = TestServer::start().await;
        let token = make_jwt("jwt-user", json!({"role": "user"}), JWT_SECRET);

        let resp = client()
            .post(format!("{}/sync", server.base_url()))
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();

        // Auth should pass (not 401) - may get other status if operation fails
        assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// Test invalid JWT is rejected.
    #[tokio::test]
    async fn test_invalid_jwt_rejected() {
        let server = TestServer::start().await;

        let resp = client()
            .post(format!("{}/sync", server.base_url()))
            .header("Authorization", "Bearer invalid-token")
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_events_reject_invalid_client_id_with_structured_bad_request() {
        let server = TestServer::start().await;

        let resp = client()
            .get(format!("{}/events?client_id=not-a-uuid", server.base_url()))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "Invalid client_id: not-a-uuid");
        assert_eq!(body["code"], "bad_request");
    }

    #[tokio::test]
    async fn test_events_require_session_with_structured_401() {
        let server = TestServer::start().await;

        let resp = client()
            .get(format!(
                "{}/events?client_id=01234567-89ab-cdef-0123-456789abcdef",
                server.base_url()
            ))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "unauthenticated");
        assert_eq!(body["code"], "missing");
    }

    #[tokio::test]
    async fn test_events_return_expired_for_expired_jwt() {
        let server = TestServer::start_with_jwks_responses(vec![test_server::hs256_jwks(
            "kid-events-expired",
            "secret-events-expired",
        )])
        .await;

        let expired = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 60;
        let token = make_jwt_with_kid_and_exp(
            "user-events-expired",
            "kid-events-expired",
            "secret-events-expired",
            expired,
        );

        let resp = client()
            .get(format!(
                "{}/events?client_id=01234567-89ab-cdef-0123-456789abcdef",
                server.base_url()
            ))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "unauthenticated");
        assert_eq!(body["code"], "expired");
    }

    #[tokio::test]
    async fn test_events_return_disabled_when_jwt_auth_is_not_configured() {
        let server = TestServer::start_without_jwks().await;
        let token = make_jwt("events-user", json!({"role": "member"}), JWT_SECRET);

        let resp = client()
            .get(format!(
                "{}/events?client_id=01234567-89ab-cdef-0123-456789abcdef",
                server.base_url()
            ))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "unauthenticated");
        assert_eq!(body["code"], "disabled");
    }

    /// Test backend impersonation with valid secret.
    #[tokio::test]
    async fn test_backend_impersonation_valid() {
        let server = TestServer::start().await;
        let session = Session::new("impersonated-user");
        let session_b64 = encode_session(&session);

        let resp = client()
            .post(format!("{}/sync", server.base_url()))
            .header("X-Jazz-Backend-Secret", BACKEND_SECRET)
            .header("X-Jazz-Session", session_b64)
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();

        // Auth should pass (not 401) - may get other status if operation fails
        assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// Test backend impersonation with invalid secret.
    #[tokio::test]
    async fn test_backend_impersonation_invalid_secret() {
        let server = TestServer::start().await;
        let session = Session::new("impersonated-user");
        let session_b64 = encode_session(&session);

        let resp = client()
            .post(format!("{}/sync", server.base_url()))
            .header("X-Jazz-Backend-Secret", "wrong-secret")
            .header("X-Jazz-Session", session_b64)
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// Test session header without backend secret is rejected.
    #[tokio::test]
    async fn test_session_without_secret_rejected() {
        let server = TestServer::start().await;
        let session = Session::new("impersonated-user");
        let session_b64 = encode_session(&session);

        let resp = client()
            .post(format!("{}/sync", server.base_url()))
            .header("X-Jazz-Session", session_b64)
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// Test that backend impersonation takes priority over JWT.
    #[tokio::test]
    async fn test_backend_priority_over_jwt() {
        let server = TestServer::start().await;
        let jwt_token = make_jwt("jwt-user", json!({}), JWT_SECRET);
        let session = Session::new("backend-user");
        let session_b64 = encode_session(&session);

        // Both JWT and backend auth provided - backend should win
        let resp = client()
            .post(format!("{}/sync", server.base_url()))
            .header("Authorization", format!("Bearer {}", jwt_token))
            .header("X-Jazz-Backend-Secret", BACKEND_SECRET)
            .header("X-Jazz-Session", session_b64)
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();

        // Auth should pass (not 401) - may get other status if operation fails
        assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// Test link-external endpoint idempotency and conflict behavior.
    #[tokio::test]
    async fn test_link_external_idempotent_and_conflict() {
        let server = TestServer::start().await;
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
        let server = TestServer::start_with_jwks_responses(vec![test_server::hs256_jwks(
            "test-jwks-kid",
            "secret-expired-link",
        )])
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
    async fn test_link_external_returns_disabled_when_jwt_auth_is_not_configured() {
        let server = TestServer::start_without_jwks().await;
        let token = make_jwt_with_issuer(
            "external-user",
            json!({"role": "user"}),
            JWT_SECRET,
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
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["error"], "unauthenticated");
        assert_eq!(body["code"], "disabled");
    }

    /// Create a valid catalogue sync body for testing admin auth.
    fn catalogue_sync_body() -> String {
        json!({
            "client_id": "01234567-89ab-cdef-0123-456789abcdef",
            "payloads": [{
                "ObjectUpdated": {
                    "object_id": "01234567-89ab-cdef-0123-456789abcdef",
                    "metadata": {
                        "id": "01234567-89ab-cdef-0123-456789abcdef",
                        "metadata": {"type": "catalogue_schema"}
                    },
                    "branch_name": "main",
                    "commits": []
                }
            }]
        })
        .to_string()
    }

    /// Test that catalogue sync without admin header returns 401.
    #[tokio::test]
    async fn test_catalogue_sync_no_admin() {
        let server = TestServer::start().await;

        // Send a catalogue schema sync payload without admin header
        let resp = client()
            .post(format!("{}/sync", server.base_url()))
            .header("Content-Type", "application/json")
            .body(catalogue_sync_body())
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// Test that catalogue sync with wrong admin header returns 401.
    #[tokio::test]
    async fn test_catalogue_sync_wrong_admin() {
        let server = TestServer::start().await;

        // Send a catalogue schema sync payload with wrong admin header
        let resp = client()
            .post(format!("{}/sync", server.base_url()))
            .header("X-Jazz-Admin-Secret", "wrong-admin-secret")
            .header("Content-Type", "application/json")
            .body(catalogue_sync_body())
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// Test that catalogue sync with valid admin header returns 200.
    #[tokio::test]
    async fn test_catalogue_sync_valid_admin() {
        let server = TestServer::start().await;

        // Send a catalogue schema sync payload with valid admin header
        let resp = client()
            .post(format!("{}/sync", server.base_url()))
            .header("X-Jazz-Admin-Secret", ADMIN_SECRET)
            .header("Content-Type", "application/json")
            .body(catalogue_sync_body())
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
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
            jazz_principal_id: None,
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
    ///                              validate ──> 200 OK
    /// ```
    #[tokio::test]
    async fn test_unknown_kid_triggers_jwks_refresh_and_succeeds() {
        let server = TestServer::start_with_jwks_responses(vec![
            test_server::hs256_jwks("kid-old", "secret-old"),
            test_server::hs256_jwks("kid-new", "secret-new"),
        ])
        .await;

        let token = make_jwt_with_kid("user-rotation", "kid-new", "secret-new");

        let resp = client()
            .post(format!("{}/sync", server.base_url()))
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();

        assert_ne!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "JWT with rotated key should succeed after JWKS refresh"
        );
        assert_eq!(
            server.jwks_hits(),
            2,
            "should fetch JWKS twice: startup cache + one refresh for unknown kid"
        );
    }

    /// A JWT with an invalid signature should remain 401 even after
    /// the server attempts a JWKS refresh.
    #[tokio::test]
    async fn test_bad_signature_stays_unauthorized_after_refresh() {
        let server = TestServer::start_with_jwks_responses(vec![
            test_server::hs256_jwks("kid-sig", "good-secret"),
            test_server::hs256_jwks("kid-sig", "good-secret"),
        ])
        .await;

        let token = make_jwt_with_kid("user-invalid", "kid-sig", "wrong-secret");

        let resp = client()
            .post(format!("{}/sync", server.base_url()))
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "invalid signature must stay unauthorized after refresh"
        );
        assert_eq!(
            server.jwks_hits(),
            2,
            "signature failure should trigger one refresh attempt"
        );

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "unauthenticated");
        assert_eq!(body["code"], "invalid");
    }

    #[tokio::test]
    async fn test_expired_jwt_returns_structured_401() {
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

        let resp = client()
            .post(format!("{}/sync", server.base_url()))
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "unauthenticated");
        assert_eq!(body["code"], "expired");
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

        let first = client()
            .post(format!("{}/sync", server.base_url()))
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();
        assert_ne!(first.status(), StatusCode::UNAUTHORIZED);

        let second = client()
            .post(format!("{}/sync", server.base_url()))
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();
        assert_ne!(second.status(), StatusCode::UNAUTHORIZED);

        assert_eq!(
            server.jwks_hits(),
            1,
            "cached JWKS should serve multiple requests without refetching"
        );
    }

    /// Rapid requests with different unknown kids should not trigger unbounded
    /// JWKS fetches. After the first forced refresh, subsequent unknown-kid
    /// requests within the cooldown window should reuse the cached keyset
    /// rather than hammering the IdP endpoint.
    ///
    /// ```text
    ///  startup    req(kid-0)     req(kid-1)     req(kid-2)
    ///    |            |              |              |
    ///    v            v              v              v
    ///  fetch(1)   no match →     no match →     no match →
    ///             refresh(2)     cooldown ──>   cooldown ──>
    ///             no match →     use cached     use cached
    ///             401            401            401
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

            let resp = client()
                .post(format!("{}/sync", server.base_url()))
                .header("Authorization", format!("Bearer {}", token))
                .header("Content-Type", "application/json")
                .body(sync_body())
                .send()
                .await
                .unwrap();

            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
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
    /// with valid JWTs should still succeed using the stale cached keyset
    /// rather than failing with an auth error.
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

        // First request: cache hit, validates OK.
        let first = client()
            .post(format!("{}/sync", server.base_url()))
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();
        assert_ne!(
            first.status(),
            StatusCode::UNAUTHORIZED,
            "first request should succeed with cached JWKS"
        );

        // Wait for TTL to expire.
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

        // Second request: TTL expired, fetch fails (empty keys), should serve stale.
        let second = client()
            .post(format!("{}/sync", server.base_url()))
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();
        assert_ne!(
            second.status(),
            StatusCode::UNAUTHORIZED,
            "request should succeed with stale JWKS when endpoint is down"
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

        // First request: cache hit, validates OK.
        let first = client()
            .post(format!("{}/sync", server.base_url()))
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();
        assert_ne!(first.status(), StatusCode::UNAUTHORIZED);

        // Wait beyond TTL + max_stale (2s total).
        tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

        // Request should now fail — stale keyset is too old to serve.
        let expired = client()
            .post(format!("{}/sync", server.base_url()))
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .body(sync_body())
            .send()
            .await
            .unwrap();
        assert_eq!(
            expired.status(),
            StatusCode::UNAUTHORIZED,
            "stale keyset beyond max_stale should not be served"
        );
    }

    #[tokio::test]
    async fn test_schema_hash_endpoint_returns_the_pushed_schema() {
        let server = TestServer::start().await;
        let app_id = "00000000-0000-0000-0000-000000000001";

        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let expected_hash = schema_hash.to_string();
        let encoded_schema = encode_schema(&schema);
        let object_id = schema_hash.to_object_id().to_string();

        let mut metadata = HashMap::new();
        metadata.insert(
            MetadataKey::Type.as_str().to_string(),
            ObjectType::CatalogueSchema.as_str().to_string(),
        );
        metadata.insert(MetadataKey::AppId.as_str().to_string(), app_id.to_string());
        metadata.insert(
            MetadataKey::SchemaHash.as_str().to_string(),
            hex::encode(schema_hash.as_bytes()),
        );

        let sync_payload = json!({
            "client_id": Uuid::new_v4().to_string(),
            "payloads": [{
                "ObjectUpdated": {
                    "object_id": object_id,
                    "metadata": {
                        "id": object_id,
                        "metadata": metadata
                    },
                    "branch_name": "main",
                    "commits": [
                        {
                            "parents": [],
                            "content": encoded_schema,
                            "timestamp": 1,
                            "author": Uuid::new_v4().to_string(),
                            "metadata": null
                        }
                    ]
                }
            }]
        });

        let sync_response = client()
            .post(format!("{}/sync", server.base_url()))
            .header("X-Jazz-Admin-Secret", ADMIN_SECRET)
            .json(&sync_payload)
            .send()
            .await
            .unwrap();
        assert_eq!(sync_response.status(), StatusCode::OK);

        let hashes_response = client()
            .get(format!("{}/schemas", server.base_url()))
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

        let schema_response = client()
            .get(format!("{}/schema/{expected_hash}", server.base_url()))
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
