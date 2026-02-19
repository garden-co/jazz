//! Authentication extractors and validation.
//!
//! # Auth Methods
//!
//! 1. **JWT Auth** (`Authorization: Bearer <JWT>`): Frontend/mobile clients authenticate
//!    via JWT validated with JWKS.
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

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    async_trait,
    extract::FromRequestParts,
    http::{HeaderMap, StatusCode, header::AUTHORIZATION, request::Parts},
};
use base64::Engine;
use groove::query_manager::session::Session;
use groove::schema_manager::AppId;
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{Jwk, JwkSet, KeyAlgorithm},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::commands::server::ServerState;

const LOCAL_MODE_HEADER: &str = "X-Jazz-Local-Mode";
const LOCAL_TOKEN_HEADER: &str = "X-Jazz-Local-Token";

pub type ExternalIdentityMap = HashMap<(String, String), String>;

// ============================================================================
// Auth Configuration
// ============================================================================

/// Authentication configuration for the server.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// URL to fetch JWKS keys (production).
    pub jwks_url: Option<String>,
    /// Cached JWKS set fetched at server startup.
    pub jwks_set: Option<JwkSet>,
    /// Secret for backend session impersonation.
    pub backend_secret: Option<String>,
    /// Secret for admin operations (schema/policy sync).
    pub admin_secret: Option<String>,
}

impl AuthConfig {
    /// Check if any auth is configured.
    pub fn is_configured(&self) -> bool {
        self.jwks_url.is_some() || self.backend_secret.is_some() || self.admin_secret.is_some()
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
    /// Optional issuer.
    #[serde(default)]
    pub iss: Option<String>,
    /// Preferred principal ID claim for Jazz.
    #[serde(default)]
    pub jazz_principal_id: Option<String>,
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

/// JWT identity data extracted after signature validation.
#[derive(Debug, Clone)]
pub struct VerifiedJwt {
    pub subject: String,
    pub issuer: Option<String>,
    pub principal_id_claim: Option<String>,
    pub claims: serde_json::Value,
}

/// JWT validation error.
#[derive(Debug)]
pub enum JwtError {
    /// No JWT validation key configured.
    NoKeyConfigured,
    /// Invalid token format or signature.
    Invalid(String),
}

impl std::fmt::Display for JwtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JwtError::NoKeyConfigured => write!(f, "No JWT validation key configured"),
            JwtError::Invalid(msg) => write!(f, "Invalid JWT: {}", msg),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalAuthMode {
    Anonymous,
    Demo,
}

impl LocalAuthMode {
    pub fn from_header(value: &str) -> Option<Self> {
        match value {
            "anonymous" => Some(Self::Anonymous),
            "demo" => Some(Self::Demo),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Anonymous => "anonymous",
            Self::Demo => "demo",
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

        match validate_jwt_identity(token, &state.auth_config) {
            Ok(verified) => {
                let external_identities = state.external_identities.read().await;
                let session = resolve_verified_jwt_session(
                    state.app_id,
                    verified,
                    Some(&external_identities),
                )?;
                Ok(JwtAuth(Some(session)))
            }
            Err(JwtError::NoKeyConfigured) => Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "JWT validation not configured",
            )),
            Err(JwtError::Invalid(_)) => Err((StatusCode::UNAUTHORIZED, "Invalid JWT")),
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
        let external_identities = state.external_identities.read().await;
        let session = extract_session(
            &parts.headers,
            state.app_id,
            &state.auth_config,
            Some(&external_identities),
        )?;
        Ok(RequestSession(session))
    }
}

// ============================================================================
// Validation Functions
// ============================================================================

fn map_key_algorithm(alg: KeyAlgorithm) -> Option<Algorithm> {
    match alg {
        KeyAlgorithm::HS256 => Some(Algorithm::HS256),
        KeyAlgorithm::HS384 => Some(Algorithm::HS384),
        KeyAlgorithm::HS512 => Some(Algorithm::HS512),
        KeyAlgorithm::ES256 => Some(Algorithm::ES256),
        KeyAlgorithm::ES384 => Some(Algorithm::ES384),
        KeyAlgorithm::RS256 => Some(Algorithm::RS256),
        KeyAlgorithm::RS384 => Some(Algorithm::RS384),
        KeyAlgorithm::RS512 => Some(Algorithm::RS512),
        KeyAlgorithm::PS256 => Some(Algorithm::PS256),
        KeyAlgorithm::PS384 => Some(Algorithm::PS384),
        KeyAlgorithm::PS512 => Some(Algorithm::PS512),
        KeyAlgorithm::EdDSA => Some(Algorithm::EdDSA),
        KeyAlgorithm::RSA1_5 | KeyAlgorithm::RSA_OAEP | KeyAlgorithm::RSA_OAEP_256 => None,
    }
}

fn signature_only_validation(alg: Algorithm) -> Validation {
    let mut validation = Validation::new(alg);
    validation.required_spec_claims.clear();
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.validate_aud = false;
    validation
}

fn select_jwk_candidates<'a>(jwks: &'a JwkSet, kid: Option<&str>, alg: Algorithm) -> Vec<&'a Jwk> {
    let mut candidates = Vec::new();

    for jwk in &jwks.keys {
        if let Some(expected_kid) = kid
            && jwk.common.key_id.as_deref() != Some(expected_kid)
        {
            continue;
        }

        if let Some(key_alg) = jwk.common.key_algorithm {
            match map_key_algorithm(key_alg) {
                Some(mapped_alg) if mapped_alg == alg => {}
                Some(_) | None => continue,
            }
        }

        candidates.push(jwk);
    }

    candidates
}

/// Validate a JWT signature and return identity claims.
pub fn validate_jwt_identity(token: &str, config: &AuthConfig) -> Result<VerifiedJwt, JwtError> {
    let jwks = config.jwks_set.as_ref().ok_or(JwtError::NoKeyConfigured)?;
    let header = decode_header(token).map_err(|e| JwtError::Invalid(e.to_string()))?;
    let candidates = select_jwk_candidates(jwks, header.kid.as_deref(), header.alg);
    if candidates.is_empty() {
        return Err(JwtError::Invalid("no matching key in JWKS".to_string()));
    }

    let validation = signature_only_validation(header.alg);
    let mut last_error = None;

    for jwk in candidates {
        let decoding_key = match DecodingKey::from_jwk(jwk) {
            Ok(key) => key,
            Err(e) => {
                last_error = Some(format!("failed to build decoding key: {e}"));
                continue;
            }
        };

        match decode::<JwtClaims>(token, &decoding_key, &validation) {
            Ok(data) => {
                return Ok(VerifiedJwt {
                    subject: data.claims.sub,
                    issuer: data.claims.iss,
                    principal_id_claim: data.claims.jazz_principal_id,
                    claims: data.claims.claims,
                });
            }
            Err(e) => {
                last_error = Some(e.to_string());
            }
        }
    }

    Err(JwtError::Invalid(last_error.unwrap_or_else(|| {
        "JWT signature verification failed".to_string()
    })))
}

/// Validate a JWT and extract a basic session (subject-only principal).
#[allow(dead_code)]
pub fn validate_jwt(token: &str, config: &AuthConfig) -> Result<Session, JwtError> {
    let verified = validate_jwt_identity(token, config)?;
    Ok(Session {
        user_id: verified.subject,
        claims: verified.claims,
    })
}

/// Resolve a session from validated JWT identity + optional external mappings.
pub fn resolve_verified_jwt_session(
    app_id: AppId,
    verified: VerifiedJwt,
    external_identities: Option<&ExternalIdentityMap>,
) -> Result<Session, (StatusCode, &'static str)> {
    let subject = verified.subject.trim();
    if subject.is_empty() {
        return Err((StatusCode::UNAUTHORIZED, "Invalid JWT subject"));
    }

    let issuer = verified
        .issuer
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());
    let principal_claim = verified
        .principal_id_claim
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());

    let mapped_principal = match (issuer, external_identities) {
        (Some(iss), Some(mappings)) => mappings
            .get(&(iss.to_string(), subject.to_string()))
            .cloned(),
        _ => None,
    };

    if let (Some(claim), Some(mapped)) = (principal_claim, mapped_principal.as_deref())
        && claim != mapped
    {
        return Err((
            StatusCode::UNAUTHORIZED,
            "External identity mapping conflict",
        ));
    }

    let principal_id = if let Some(claim) = principal_claim {
        claim.to_string()
    } else if let Some(mapped) = mapped_principal {
        mapped
    } else if let Some(iss) = issuer {
        derive_external_principal_id(app_id, iss, subject)
    } else {
        subject.to_string()
    };

    let claims = match verified.claims {
        serde_json::Value::Object(mut map) => {
            map.insert("auth_mode".to_string(), serde_json::json!("external"));
            map.insert("subject".to_string(), serde_json::json!(subject));
            if let Some(iss) = issuer {
                map.insert("issuer".to_string(), serde_json::json!(iss));
            }
            serde_json::Value::Object(map)
        }
        other => serde_json::json!({
            "auth_mode": "external",
            "subject": subject,
            "issuer": issuer,
            "raw_claims": other,
        }),
    };

    Ok(Session {
        user_id: principal_id,
        claims,
    })
}

pub fn parse_local_auth_headers(
    headers: &HeaderMap,
) -> Result<Option<(LocalAuthMode, String)>, (StatusCode, &'static str)> {
    let local_mode = headers.get(LOCAL_MODE_HEADER).and_then(|v| v.to_str().ok());
    let local_token = headers
        .get(LOCAL_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok());

    match (local_mode, local_token) {
        (Some(mode), Some(token)) => {
            let mode = LocalAuthMode::from_header(mode)
                .ok_or((StatusCode::BAD_REQUEST, "Invalid local auth mode"))?;
            let token = token.trim();
            if token.is_empty() {
                return Err((StatusCode::UNAUTHORIZED, "Empty local auth token"));
            }
            Ok(Some((mode, token.to_string())))
        }
        (Some(_), None) | (None, Some(_)) => Err((
            StatusCode::BAD_REQUEST,
            "Both X-Jazz-Local-Mode and X-Jazz-Local-Token are required",
        )),
        (None, None) => Ok(None),
    }
}

/// Extract session from headers with priority resolution.
///
/// Priority:
/// 1. Backend impersonation (X-Jazz-Backend-Secret + X-Jazz-Session)
/// 2. JWT auth (Authorization: Bearer)
/// 3. No session
pub fn extract_session(
    headers: &HeaderMap,
    app_id: AppId,
    config: &AuthConfig,
    external_identities: Option<&ExternalIdentityMap>,
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
    if let Some(auth_value) = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok())
        && let Some(token) = auth_value.strip_prefix("Bearer ")
    {
        let token = token.trim();
        if token.is_empty() {
            return Err((StatusCode::UNAUTHORIZED, "Empty bearer token"));
        }

        match validate_jwt_identity(token, config) {
            Ok(verified) => {
                let session = resolve_verified_jwt_session(app_id, verified, external_identities)?;
                return Ok(Some(session));
            }
            Err(JwtError::NoKeyConfigured) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "JWT validation not configured",
                ));
            }
            Err(JwtError::Invalid(_)) => {
                return Err((StatusCode::UNAUTHORIZED, "Invalid JWT"));
            }
        }
    }

    // Priority 3: Local anonymous/demo token auth
    if let Some((mode, token)) = parse_local_auth_headers(headers)? {
        let principal_id = derive_local_principal_id(app_id, mode, &token);
        return Ok(Some(Session {
            user_id: principal_id,
            claims: serde_json::json!({
                "auth_mode": "local",
                "local_mode": mode.as_str(),
            }),
        }));
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

pub fn derive_local_principal_id(app_id: AppId, mode: LocalAuthMode, token: &str) -> String {
    let input = format!("{app_id}:{}:{token}", mode.as_str());
    let digest = Sha256::digest(input.as_bytes());
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest);
    format!("local:{encoded}")
}

pub fn derive_external_principal_id(app_id: AppId, issuer: &str, subject: &str) -> String {
    let input = format!("{app_id}:external:{issuer}:{subject}");
    let digest = Sha256::digest(input.as_bytes());
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest);
    format!("external:{encoded}")
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

    const TEST_JWKS_KID: &str = "test-kid";
    const TEST_JWKS_SECRET: &str = "test-secret-key-for-jwt";

    fn test_app_id() -> AppId {
        AppId::from_name("jazz-tools-auth-tests")
    }

    fn make_test_config() -> AuthConfig {
        AuthConfig {
            jwks_url: Some("https://example.test/.well-known/jwks.json".to_string()),
            jwks_set: Some(make_hs256_jwks(TEST_JWKS_KID, TEST_JWKS_SECRET)),
            backend_secret: Some("backend-secret-12345".to_string()),
            admin_secret: Some("admin-secret-67890".to_string()),
        }
    }

    fn make_hs256_jwks(kid: &str, secret: &str) -> JwkSet {
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret.as_bytes());
        serde_json::from_value(serde_json::json!({
            "keys": [
                {
                    "kty": "oct",
                    "kid": kid,
                    "alg": "HS256",
                    "k": encoded
                }
            ]
        }))
        .unwrap()
    }

    fn make_jwt(claims: &JwtClaims, secret: &str, kid: &str) -> String {
        let key = EncodingKey::from_secret(secret.as_bytes());
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(kid.to_string());
        encode(&header, claims, &key).unwrap()
    }

    #[test]
    fn test_jwt_validation_valid() {
        let config = make_test_config();
        let claims = JwtClaims {
            sub: "user-123".to_string(),
            iss: None,
            jazz_principal_id: None,
            claims: serde_json::json!({"role": "admin"}),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, TEST_JWKS_SECRET, TEST_JWKS_KID);

        let session = validate_jwt(&token, &config).unwrap();
        assert_eq!(session.user_id, "user-123");
        assert_eq!(session.claims["role"], "admin");
    }

    #[test]
    fn test_jwt_validation_wrong_secret() {
        let config = make_test_config();
        let claims = JwtClaims {
            sub: "user-123".to_string(),
            iss: None,
            jazz_principal_id: None,
            claims: serde_json::json!({}),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, "wrong-secret", TEST_JWKS_KID);

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

        let result = extract_session(&headers, test_app_id(), &config, None).unwrap();
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

        let result = extract_session(&headers, test_app_id(), &config, None);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_extract_session_jwt_fallback() {
        let config = make_test_config();
        let mut headers = HeaderMap::new();

        let claims = JwtClaims {
            sub: "jwt-user".to_string(),
            iss: None,
            jazz_principal_id: None,
            claims: serde_json::json!({}),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, TEST_JWKS_SECRET, TEST_JWKS_KID);

        headers.insert(AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());

        let result = extract_session(&headers, test_app_id(), &config, None).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().user_id, "jwt-user");
    }

    #[test]
    fn test_extract_session_jwt_uses_external_mapping_fallback() {
        let config = make_test_config();
        let mut headers = HeaderMap::new();
        let mut mappings = ExternalIdentityMap::new();
        mappings.insert(
            ("https://issuer.example".to_string(), "jwt-user".to_string()),
            "local:mapped-principal".to_string(),
        );

        let claims = JwtClaims {
            sub: "jwt-user".to_string(),
            iss: Some("https://issuer.example".to_string()),
            jazz_principal_id: None,
            claims: serde_json::json!({}),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, TEST_JWKS_SECRET, TEST_JWKS_KID);

        headers.insert(AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());

        let result = extract_session(&headers, test_app_id(), &config, Some(&mappings)).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().user_id, "local:mapped-principal");
    }

    #[test]
    fn test_extract_session_jwt_claim_conflict_with_mapping_is_rejected() {
        let config = make_test_config();
        let mut headers = HeaderMap::new();
        let mut mappings = ExternalIdentityMap::new();
        mappings.insert(
            ("https://issuer.example".to_string(), "jwt-user".to_string()),
            "local:mapped-principal".to_string(),
        );

        let claims = JwtClaims {
            sub: "jwt-user".to_string(),
            iss: Some("https://issuer.example".to_string()),
            jazz_principal_id: Some("different-principal".to_string()),
            claims: serde_json::json!({}),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, TEST_JWKS_SECRET, TEST_JWKS_KID);
        headers.insert(AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());

        let result = extract_session(&headers, test_app_id(), &config, Some(&mappings));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, StatusCode::UNAUTHORIZED);
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
            iss: None,
            jazz_principal_id: None,
            claims: serde_json::json!({}),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, TEST_JWKS_SECRET, TEST_JWKS_KID);
        headers.insert(AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());

        let result = extract_session(&headers, test_app_id(), &config, None).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().user_id, "backend-user"); // Backend wins
    }

    #[test]
    fn test_extract_session_no_auth() {
        let config = make_test_config();
        let headers = HeaderMap::new();

        let result = extract_session(&headers, test_app_id(), &config, None).unwrap();
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

    #[test]
    fn test_extract_session_local_anonymous() {
        let config = make_test_config();
        let mut headers = HeaderMap::new();
        headers.insert(LOCAL_MODE_HEADER, "anonymous".parse().unwrap());
        headers.insert(LOCAL_TOKEN_HEADER, "device-token-1".parse().unwrap());

        let result = extract_session(&headers, test_app_id(), &config, None).unwrap();
        let session = result.unwrap();
        assert!(session.user_id.starts_with("local:"));
        assert_eq!(session.claims["auth_mode"], "local");
        assert_eq!(session.claims["local_mode"], "anonymous");
    }

    #[test]
    fn test_extract_session_local_requires_both_headers() {
        let config = make_test_config();
        let mut headers = HeaderMap::new();
        headers.insert(LOCAL_MODE_HEADER, "demo".parse().unwrap());

        let result = extract_session(&headers, test_app_id(), &config, None);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, StatusCode::BAD_REQUEST);
    }
}
