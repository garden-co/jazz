//! Authentication extractors and validation.
//!
//! # Auth Methods
//!
//! 1. **JWT Auth** (`Authorization: Bearer <JWT>`): Frontend/mobile clients authenticate
//!    via JWT, validated with HMAC secret (testing) or JWKS (production).
//!
//! 2. **Backend Secret** (`X-Jazz-Backend-Secret` + `X-Jazz-Session`): Backend clients
//!    can impersonate any user by providing the backend secret and a session header.
//!
//! 3. **Admin Secret** (`X-Jazz-Admin-Secret`): Required for schema/lens/policy sync.
//!
//! # Session Resolution Priority
//!
//! When resolving the request session:
//! 1. Backend impersonation (if `X-Jazz-Backend-Secret` + `X-Jazz-Session` present)
//! 2. JWT auth (if `Authorization: Bearer` present)
//! 3. No session (anonymous)

use std::sync::Arc;

use axum::{
    async_trait,
    extract::FromRequestParts,
    http::{HeaderMap, StatusCode, header::AUTHORIZATION, request::Parts},
};
use base64::Engine;
use groove::query_manager::session::Session;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};

use crate::commands::server::ServerState;

// ============================================================================
// Auth Configuration
// ============================================================================

/// Authentication configuration for the server.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// HMAC secret for JWT validation (testing/development).
    pub jwt_secret: Option<String>,
    /// URL to fetch JWKS keys (production).
    pub jwks_url: Option<String>,
    /// Secret for backend session impersonation.
    pub backend_secret: Option<String>,
    /// Secret for admin operations (schema/policy sync).
    pub admin_secret: Option<String>,
}

impl AuthConfig {
    /// Check if any auth is configured.
    pub fn is_configured(&self) -> bool {
        self.jwt_secret.is_some()
            || self.jwks_url.is_some()
            || self.backend_secret.is_some()
            || self.admin_secret.is_some()
    }
}

// ============================================================================
// JWT Types
// ============================================================================

/// JWT claims structure.
///
/// Expected JWT payload:
/// ```json
/// {
///   "sub": "user-123",
///   "claims": {"role": "admin", "teams": ["eng"]},
///   "exp": 1735689600
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Subject (user ID).
    pub sub: String,
    /// Additional claims.
    #[serde(default)]
    pub claims: serde_json::Value,
    /// Expiration time (Unix timestamp).
    #[serde(default)]
    pub exp: Option<u64>,
    /// Issued at time (Unix timestamp).
    #[serde(default)]
    pub iat: Option<u64>,
}

/// JWT validation error.
#[derive(Debug)]
pub enum JwtError {
    /// No JWT validation key configured.
    NoKeyConfigured,
    /// Invalid token format or signature.
    Invalid(String),
    /// Token has expired.
    Expired,
}

impl std::fmt::Display for JwtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JwtError::NoKeyConfigured => write!(f, "No JWT validation key configured"),
            JwtError::Invalid(msg) => write!(f, "Invalid JWT: {}", msg),
            JwtError::Expired => write!(f, "JWT has expired"),
        }
    }
}

// ============================================================================
// Extractors
// ============================================================================

/// Extracts and validates JWT from `Authorization: Bearer <token>` header.
///
/// Returns `Some(Session)` if a valid JWT is present, `None` if no auth header.
/// Returns an error if the JWT is present but invalid.
#[allow(dead_code)]
pub struct JwtAuth(pub Option<Session>);

#[async_trait]
impl FromRequestParts<Arc<ServerState>> for JwtAuth {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<ServerState>,
    ) -> Result<Self, Self::Rejection> {
        let auth_header = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok());

        let Some(auth_value) = auth_header else {
            return Ok(JwtAuth(None));
        };

        let Some(token) = auth_value.strip_prefix("Bearer ") else {
            return Err((
                StatusCode::BAD_REQUEST,
                "Invalid Authorization header format",
            ));
        };

        match validate_jwt(token, &state.auth_config) {
            Ok(session) => Ok(JwtAuth(Some(session))),
            Err(JwtError::NoKeyConfigured) => Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "JWT validation not configured",
            )),
            Err(JwtError::Invalid(_)) => Err((StatusCode::UNAUTHORIZED, "Invalid JWT")),
            Err(JwtError::Expired) => Err((StatusCode::UNAUTHORIZED, "JWT has expired")),
        }
    }
}

/// Extracts backend secret from `X-Jazz-Backend-Secret` header.
#[allow(dead_code)]
pub struct BackendAuth(pub Option<String>);

#[async_trait]
impl<S> FromRequestParts<S> for BackendAuth
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let secret = parts
            .headers
            .get("X-Jazz-Backend-Secret")
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        Ok(BackendAuth(secret))
    }
}

/// Extracts admin secret from `X-Jazz-Admin-Secret` header.
#[allow(dead_code)]
pub struct AdminAuth(pub Option<String>);

#[async_trait]
impl<S> FromRequestParts<S> for AdminAuth
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let secret = parts
            .headers
            .get("X-Jazz-Admin-Secret")
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        Ok(AdminAuth(secret))
    }
}

/// Resolved session from request headers.
///
/// Resolution priority:
/// 1. Backend impersonation (`X-Jazz-Backend-Secret` + `X-Jazz-Session`)
/// 2. JWT auth (`Authorization: Bearer`)
/// 3. No session
#[allow(dead_code)]
pub struct RequestSession(pub Option<Session>);

#[async_trait]
impl FromRequestParts<Arc<ServerState>> for RequestSession {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<ServerState>,
    ) -> Result<Self, Self::Rejection> {
        let session = extract_session(&parts.headers, &state.auth_config)?;
        Ok(RequestSession(session))
    }
}

// ============================================================================
// Validation Functions
// ============================================================================

/// Validate a JWT and extract session information.
pub fn validate_jwt(token: &str, config: &AuthConfig) -> Result<Session, JwtError> {
    // Try HMAC secret first (testing/development)
    if let Some(secret) = &config.jwt_secret {
        let key = DecodingKey::from_secret(secret.as_bytes());
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;

        match decode::<JwtClaims>(token, &key, &validation) {
            Ok(data) => {
                return Ok(Session {
                    user_id: data.claims.sub,
                    claims: data.claims.claims,
                });
            }
            Err(e) => {
                if matches!(e.kind(), jsonwebtoken::errors::ErrorKind::ExpiredSignature) {
                    return Err(JwtError::Expired);
                }
                return Err(JwtError::Invalid(e.to_string()));
            }
        }
    }

    // TODO: JWKS support for production
    // if let Some(jwks_url) = &config.jwks_url {
    //     // Fetch and cache JWKS keys, validate with RS256
    // }

    Err(JwtError::NoKeyConfigured)
}

/// Extract session from headers with priority resolution.
///
/// Priority:
/// 1. Backend impersonation (X-Jazz-Backend-Secret + X-Jazz-Session)
/// 2. JWT auth (Authorization: Bearer)
/// 3. No session
pub fn extract_session(
    headers: &HeaderMap,
    config: &AuthConfig,
) -> Result<Option<Session>, (StatusCode, &'static str)> {
    // Priority 1: Backend impersonation
    if let Some(session_b64) = headers.get("X-Jazz-Session").and_then(|v| v.to_str().ok()) {
        let backend_secret = headers
            .get("X-Jazz-Backend-Secret")
            .and_then(|v| v.to_str().ok());

        match (&config.backend_secret, backend_secret) {
            (Some(expected), Some(got)) if expected == got => {
                let session = decode_session_header(session_b64)
                    .ok_or((StatusCode::BAD_REQUEST, "Invalid session format"))?;
                return Ok(Some(session));
            }
            (Some(_), Some(_)) => {
                return Err((StatusCode::UNAUTHORIZED, "Invalid backend secret"));
            }
            (Some(_), None) => {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    "Backend secret required for session impersonation",
                ));
            }
            (None, Some(_)) => {
                return Err((StatusCode::FORBIDDEN, "Backend auth not configured"));
            }
            (None, None) => {
                // Session header without secret - ignore and fall through to JWT
            }
        }
    }

    // Priority 2: JWT auth
    if let Some(auth_value) = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        if let Some(token) = auth_value.strip_prefix("Bearer ") {
            match validate_jwt(token, config) {
                Ok(session) => return Ok(Some(session)),
                Err(JwtError::NoKeyConfigured) => {
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "JWT validation not configured",
                    ));
                }
                Err(JwtError::Invalid(_)) => {
                    return Err((StatusCode::UNAUTHORIZED, "Invalid JWT"));
                }
                Err(JwtError::Expired) => {
                    return Err((StatusCode::UNAUTHORIZED, "JWT has expired"));
                }
            }
        }
    }

    // No auth provided
    Ok(None)
}

/// Decode base64-encoded session JSON from X-Jazz-Session header.
fn decode_session_header(b64: &str) -> Option<Session> {
    let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    let json_str = std::str::from_utf8(&bytes).ok()?;
    serde_json::from_str(json_str).ok()
}

/// Check if admin secret is valid.
///
/// Catalogue operations (schema/lens sync) require admin authentication.
/// If admin_secret is not configured, catalogue sync is disabled.
pub fn validate_admin_secret(
    provided: Option<&str>,
    config: &AuthConfig,
) -> Result<(), (StatusCode, &'static str)> {
    match (&config.admin_secret, provided) {
        (Some(expected), Some(got)) if expected == got => Ok(()),
        (Some(_), Some(_)) => Err((StatusCode::UNAUTHORIZED, "Invalid admin secret")),
        (Some(_), None) => Err((
            StatusCode::UNAUTHORIZED,
            "Admin secret required for this operation",
        )),
        // TODO: Consider making catalogue sync opt-in or handling this more gracefully.
        // Currently, if admin auth isn't configured, clients can't sync schemas to server.
        // This is correct for security but may cause silent failures in dev setups.
        (None, _) => Err((StatusCode::FORBIDDEN, "Admin auth not configured")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_test_config() -> AuthConfig {
        AuthConfig {
            jwt_secret: Some("test-secret-key-for-jwt".to_string()),
            jwks_url: None,
            backend_secret: Some("backend-secret-12345".to_string()),
            admin_secret: Some("admin-secret-67890".to_string()),
        }
    }

    fn make_jwt(claims: &JwtClaims, secret: &str) -> String {
        let key = EncodingKey::from_secret(secret.as_bytes());
        encode(&Header::default(), claims, &key).unwrap()
    }

    fn future_exp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600 // 1 hour from now
    }

    fn past_exp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 3600 // 1 hour ago
    }

    #[test]
    fn test_jwt_validation_valid() {
        let config = make_test_config();
        let claims = JwtClaims {
            sub: "user-123".to_string(),
            claims: serde_json::json!({"role": "admin"}),
            exp: Some(future_exp()),
            iat: None,
        };
        let token = make_jwt(&claims, "test-secret-key-for-jwt");

        let session = validate_jwt(&token, &config).unwrap();
        assert_eq!(session.user_id, "user-123");
        assert_eq!(session.claims["role"], "admin");
    }

    #[test]
    fn test_jwt_validation_expired() {
        let config = make_test_config();
        let claims = JwtClaims {
            sub: "user-123".to_string(),
            claims: serde_json::json!({}),
            exp: Some(past_exp()),
            iat: None,
        };
        let token = make_jwt(&claims, "test-secret-key-for-jwt");

        let result = validate_jwt(&token, &config);
        assert!(matches!(result, Err(JwtError::Expired)));
    }

    #[test]
    fn test_jwt_validation_wrong_secret() {
        let config = make_test_config();
        let claims = JwtClaims {
            sub: "user-123".to_string(),
            claims: serde_json::json!({}),
            exp: Some(future_exp()),
            iat: None,
        };
        let token = make_jwt(&claims, "wrong-secret");

        let result = validate_jwt(&token, &config);
        assert!(matches!(result, Err(JwtError::Invalid(_))));
    }

    #[test]
    fn test_jwt_validation_no_config() {
        let config = AuthConfig::default();
        let result = validate_jwt("any-token", &config);
        assert!(matches!(result, Err(JwtError::NoKeyConfigured)));
    }

    #[test]
    fn test_decode_session_header() {
        let session = Session::new("user-456").with_claims(serde_json::json!({"teams": ["eng"]}));
        let json = serde_json::to_string(&session).unwrap();
        let b64 = base64::engine::general_purpose::STANDARD.encode(&json);

        let decoded = decode_session_header(&b64).unwrap();
        assert_eq!(decoded.user_id, "user-456");
        assert_eq!(decoded.claims["teams"][0], "eng");
    }

    #[test]
    fn test_decode_session_header_invalid() {
        assert!(decode_session_header("not-valid-base64!!!").is_none());
        assert!(decode_session_header("bm90LWpzb24=").is_none()); // "not-json" in base64
    }

    #[test]
    fn test_extract_session_backend_impersonation() {
        let config = make_test_config();
        let mut headers = HeaderMap::new();

        let session = Session::new("impersonated-user");
        let session_json = serde_json::to_string(&session).unwrap();
        let session_b64 = base64::engine::general_purpose::STANDARD.encode(&session_json);

        headers.insert(
            "X-Jazz-Backend-Secret",
            "backend-secret-12345".parse().unwrap(),
        );
        headers.insert("X-Jazz-Session", session_b64.parse().unwrap());

        let result = extract_session(&headers, &config).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().user_id, "impersonated-user");
    }

    #[test]
    fn test_extract_session_backend_wrong_secret() {
        let config = make_test_config();
        let mut headers = HeaderMap::new();

        let session = Session::new("user");
        let session_json = serde_json::to_string(&session).unwrap();
        let session_b64 = base64::engine::general_purpose::STANDARD.encode(&session_json);

        headers.insert("X-Jazz-Backend-Secret", "wrong-secret".parse().unwrap());
        headers.insert("X-Jazz-Session", session_b64.parse().unwrap());

        let result = extract_session(&headers, &config);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_extract_session_jwt_fallback() {
        let config = make_test_config();
        let mut headers = HeaderMap::new();

        let claims = JwtClaims {
            sub: "jwt-user".to_string(),
            claims: serde_json::json!({}),
            exp: Some(future_exp()),
            iat: None,
        };
        let token = make_jwt(&claims, "test-secret-key-for-jwt");

        headers.insert(AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());

        let result = extract_session(&headers, &config).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().user_id, "jwt-user");
    }

    #[test]
    fn test_extract_session_backend_takes_priority() {
        let config = make_test_config();
        let mut headers = HeaderMap::new();

        // Add both backend and JWT auth - backend should win
        let session = Session::new("backend-user");
        let session_json = serde_json::to_string(&session).unwrap();
        let session_b64 = base64::engine::general_purpose::STANDARD.encode(&session_json);

        headers.insert(
            "X-Jazz-Backend-Secret",
            "backend-secret-12345".parse().unwrap(),
        );
        headers.insert("X-Jazz-Session", session_b64.parse().unwrap());

        let claims = JwtClaims {
            sub: "jwt-user".to_string(),
            claims: serde_json::json!({}),
            exp: Some(future_exp()),
            iat: None,
        };
        let token = make_jwt(&claims, "test-secret-key-for-jwt");
        headers.insert(AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());

        let result = extract_session(&headers, &config).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().user_id, "backend-user"); // Backend wins
    }

    #[test]
    fn test_extract_session_no_auth() {
        let config = make_test_config();
        let headers = HeaderMap::new();

        let result = extract_session(&headers, &config).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_validate_admin_secret_valid() {
        let config = make_test_config();
        assert!(validate_admin_secret(Some("admin-secret-67890"), &config).is_ok());
    }

    #[test]
    fn test_validate_admin_secret_invalid() {
        let config = make_test_config();
        let result = validate_admin_secret(Some("wrong-secret"), &config);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_validate_admin_secret_missing() {
        let config = make_test_config();
        let result = validate_admin_secret(None, &config);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_validate_admin_secret_not_configured() {
        let config = AuthConfig::default();
        let result = validate_admin_secret(Some("any-secret"), &config);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, StatusCode::FORBIDDEN);
    }
}
