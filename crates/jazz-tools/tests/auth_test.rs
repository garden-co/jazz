#![cfg(feature = "cli")]

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

/// Create a valid sync request body (SyncPayloadRequest).
fn sync_body() -> String {
    json!({
        "client_id": "01234567-89ab-cdef-0123-456789abcdef",
        "payload": {
            "ObjectUpdated": {
                "object_id": "01234567-89ab-cdef-0123-456789abcdef",
                "metadata": null,
                "branch_name": "main",
                "commits": []
            }
        }
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
    use jazz_tools::schema_manager::{CatalogueSchemaResponse, encode_schema};
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

    /// Create a valid catalogue sync body for testing admin auth.
    fn catalogue_sync_body() -> String {
        json!({
            "client_id": "01234567-89ab-cdef-0123-456789abcdef",
            "payload": {
                "ObjectUpdated": {
                    "object_id": "01234567-89ab-cdef-0123-456789abcdef",
                    "metadata": {
                        "id": "01234567-89ab-cdef-0123-456789abcdef",
                        "metadata": {"type": "catalogue_schema"}
                    },
                    "branch_name": "main",
                    "commits": []
                }
            }
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
            "payload": {
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
            }
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
        let expected_schema_json =
            serde_json::to_value(CatalogueSchemaResponse::from(&schema)).unwrap();
        assert_eq!(schema_json, expected_schema_json);
    }
}
