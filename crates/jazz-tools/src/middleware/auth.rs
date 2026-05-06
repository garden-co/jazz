//! Authentication extractors and validation.
//!
//! # Auth Methods
//!
//! 1. **Local-first Auth** (`Authorization: Bearer <self-signed Ed25519 JWT>`):
//!    Clients authenticate with a self-signed JWT containing an Ed25519 identity proof.
//!
//! 2. **External JWT Auth** (`Authorization: Bearer <JWT>`): Frontend/mobile clients
//!    authenticate via JWT validated with JWKS or a configured static key.
//!
//! 3. **Backend Secret** (`X-Jazz-Backend-Secret` + `X-Jazz-Session`): Backend clients
//!    can impersonate any user by providing the backend secret and a session header.
//!
//! 4. **Admin Secret** (`X-Jazz-Admin-Secret`): Required for schema/lens/policy sync.
//!
//! # Session Resolution Priority
//!
//! When resolving the request session:
//! 1. Backend impersonation (if `X-Jazz-Backend-Secret` + `X-Jazz-Session` present)
//! 2. JWT auth (if `Authorization: Bearer` present — local-first or external JWT)
//! 3. No session

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    async_trait,
    extract::FromRequestParts,
    http::{HeaderMap, StatusCode, header::AUTHORIZATION, request::Parts},
};
use base64::Engine;
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{Jwk, JwkSet, KeyAlgorithm},
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::warn;

use crate::identity;
use crate::query_manager::session::Session;
use crate::schema_manager::AppId;
use crate::server::ServerState;
use crate::transport_protocol::UnauthenticatedResponse;

/// JWKS cache TTL — 5 minutes, matching the cloud server.
pub const JWKS_CACHE_TTL: Duration = Duration::from_secs(300);

/// Minimum interval between forced JWKS refreshes. Prevents unauthenticated
/// callers from triggering unbounded outbound fetches by sending JWTs with
/// fabricated key IDs.
const JWKS_FORCED_REFRESH_COOLDOWN: Duration = Duration::from_secs(10);

/// Maximum time a stale keyset is served after the TTL expires. Once the
/// entry is older than TTL + max_stale, the stale-if-error fallback is
/// refused and the fetch error propagates.
pub const JWKS_MAX_STALE: Duration = Duration::from_secs(300);

// ============================================================================
// Auth Configuration
// ============================================================================

#[derive(Clone)]
pub struct AuthClock {
    now_seconds: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl AuthClock {
    pub fn system() -> Self {
        Self {
            now_seconds: Arc::new(system_now_seconds),
        }
    }

    pub fn now_seconds(&self) -> u64 {
        (self.now_seconds)()
    }
}

impl Default for AuthClock {
    fn default() -> Self {
        Self::system()
    }
}

impl fmt::Debug for AuthClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthClock").finish_non_exhaustive()
    }
}

#[cfg(feature = "test-utils")]
#[derive(Clone, Debug)]
pub struct TestClock {
    now_seconds: Arc<AtomicU64>,
}

#[cfg(feature = "test-utils")]
impl TestClock {
    pub fn new(now_seconds: u64) -> Self {
        Self {
            now_seconds: Arc::new(AtomicU64::new(now_seconds)),
        }
    }

    pub fn now_seconds(&self) -> u64 {
        self.now_seconds.load(Ordering::SeqCst)
    }

    pub fn set(&self, now_seconds: u64) {
        self.now_seconds.store(now_seconds, Ordering::SeqCst);
    }

    pub fn advance(&self, delta: Duration) {
        self.now_seconds
            .fetch_add(delta.as_secs(), Ordering::SeqCst);
    }
}

#[cfg(feature = "test-utils")]
impl From<TestClock> for AuthClock {
    fn from(clock: TestClock) -> Self {
        Self {
            now_seconds: Arc::new(move || clock.now_seconds()),
        }
    }
}

/// Authentication configuration for the server.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// URL to fetch JWKS keys (production).
    pub jwks_url: Option<String>,
    /// Single JWK JSON object or PEM public key used to verify external JWTs.
    pub jwt_public_key: Option<String>,
    /// Cookie name used to read browser auth tokens during the WS upgrade.
    pub auth_cookie_name: Option<String>,
    /// Whether local-first Ed25519 JWT auth is allowed (default: true for new apps).
    pub allow_local_first_auth: bool,
    /// Secret for backend session impersonation.
    pub backend_secret: Option<String>,
    /// Secret for admin operations (schema/policy sync).
    pub admin_secret: Option<String>,
    /// Shared secret for trusted server-to-server peer sync.
    pub peer_secret: Option<String>,
    /// Time source for auth expiry checks. Defaults to the system clock.
    pub clock: AuthClock,
}

impl AuthConfig {
    /// Check if any auth is configured.
    pub fn is_configured(&self) -> bool {
        self.jwks_url.is_some()
            || self.jwt_public_key.is_some()
            || self.auth_cookie_name.is_some()
            || self.allow_local_first_auth
            || self.backend_secret.is_some()
            || self.admin_secret.is_some()
            || self.peer_secret.is_some()
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
    pub claims: serde_json::Value,
    pub exp: Option<u64>,
}

/// JWT validation error.
#[derive(Debug)]
pub enum JwtError {
    /// No JWT validation key configured.
    NoKeyConfigured,
    /// Token signature is valid but `exp` is in the past.
    Expired,
    /// Invalid token format or signature.
    Invalid(String),
}

impl std::fmt::Display for JwtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JwtError::NoKeyConfigured => write!(f, "No JWT validation key configured"),
            JwtError::Expired => write!(f, "JWT has expired"),
            JwtError::Invalid(msg) => write!(f, "Invalid JWT: {}", msg),
        }
    }
}

/// JWT verification error with retry classification.
///
/// Retryable errors (unknown kid, signature mismatch) may succeed after a JWKS
/// refresh — the identity provider may have rotated keys. Fatal errors (malformed
/// token) will never succeed regardless of which keys we have.
#[derive(Debug)]
pub enum JwtVerificationError {
    Retryable(String),
    Fatal(String),
}

// ============================================================================
// JWKS Cache
// ============================================================================

struct CachedJwksEntry {
    endpoint: String,
    fetched_at_us: u64,
    set: JwkSet,
}

/// JWKS cache with TTL-based expiry and on-demand refresh.
///
/// Caches the keyset from a JWKS endpoint and transparently refetches when:
/// - The TTL (5 min) has elapsed, or
/// - A caller forces a refresh (e.g. after encountering an unknown kid).
pub struct JwksCache {
    endpoint: String,
    http_client: reqwest::Client,
    ttl: Duration,
    max_stale: Duration,
    cached: RwLock<Option<CachedJwksEntry>>,
    last_forced_refresh_us: AtomicU64,
}

impl JwksCache {
    pub fn new(
        endpoint: String,
        http_client: reqwest::Client,
        ttl: Duration,
        max_stale: Duration,
    ) -> Self {
        Self {
            endpoint,
            http_client,
            ttl,
            max_stale,
            cached: RwLock::new(None),
            last_forced_refresh_us: AtomicU64::new(0),
        }
    }

    /// Create a cache pre-populated with a static keyset. For tests only —
    /// the endpoint is unused since the cache is always fresh.
    #[cfg(test)]
    pub fn from_static(jwks: JwkSet) -> Self {
        Self {
            endpoint: String::new(),
            http_client: reqwest::Client::new(),
            ttl: JWKS_CACHE_TTL,
            max_stale: JWKS_MAX_STALE,
            cached: RwLock::new(Some(CachedJwksEntry {
                endpoint: String::new(),
                fetched_at_us: now_timestamp_us(),
                set: jwks,
            })),
            last_forced_refresh_us: AtomicU64::new(0),
        }
    }

    /// Load the JWKS, returning a cached copy if fresh or fetching anew.
    ///
    /// When `force_refresh` is true but a forced refresh happened within the
    /// cooldown window (10s), the request is downgraded to a cache read. This
    /// prevents unauthenticated callers from using fabricated key IDs to
    /// trigger unbounded outbound fetches.
    pub async fn load(&self, force_refresh: bool) -> Result<JwkSet, String> {
        let ttl_us = self.ttl.as_micros().min(u128::from(u64::MAX)) as u64;
        let cooldown_us = JWKS_FORCED_REFRESH_COOLDOWN
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;

        // Downgrade forced refresh if within cooldown window.
        let force_refresh = if force_refresh {
            let last = self.last_forced_refresh_us.load(Ordering::SeqCst);
            let age_us = now_timestamp_us().saturating_sub(last);
            age_us > cooldown_us
        } else {
            false
        };

        if !force_refresh {
            let guard = self.cached.read().await;
            if let Some(ref entry) = *guard {
                let age_us = now_timestamp_us().saturating_sub(entry.fetched_at_us);
                if entry.endpoint == self.endpoint && age_us <= ttl_us {
                    return Ok(entry.set.clone());
                }
            }
        }

        let max_stale_us = (self.ttl + self.max_stale)
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;

        let jwks = match fetch_jwks(&self.http_client, &self.endpoint).await {
            Ok(jwks) => jwks,
            Err(e) => {
                // Stale-if-error: serve the cached keyset if it's not too old.
                let guard = self.cached.read().await;
                if let Some(ref entry) = *guard {
                    let age_us = now_timestamp_us().saturating_sub(entry.fetched_at_us);
                    if age_us <= max_stale_us {
                        warn!(
                            error = %e,
                            "JWKS fetch failed, serving stale cached keyset"
                        );
                        return Ok(entry.set.clone());
                    }
                    warn!(
                        error = %e,
                        "JWKS fetch failed and stale keyset has expired"
                    );
                }
                return Err(e);
            }
        };

        let now = now_timestamp_us();
        if force_refresh {
            self.last_forced_refresh_us.store(now, Ordering::SeqCst);
        }

        *self.cached.write().await = Some(CachedJwksEntry {
            endpoint: self.endpoint.clone(),
            fetched_at_us: now,
            set: jwks.clone(),
        });

        Ok(jwks)
    }
}

pub enum JwtVerifier {
    Jwks(JwksCache),
    Static(StaticJwtVerifier),
}

impl JwtVerifier {
    pub async fn validate_at(
        &self,
        token: &str,
        now_seconds: u64,
    ) -> Result<VerifiedJwt, JwtError> {
        match self {
            Self::Jwks(cache) => validate_jwt_with_cache_at(token, cache, now_seconds).await,
            Self::Static(verifier) => validate_jwt_with_static_key_at(token, verifier, now_seconds),
        }
    }
}

pub enum StaticJwtVerifier {
    JwkSet(JwkSet),
    Pem(PemPublicKeyVerifier),
}

impl StaticJwtVerifier {
    pub fn from_public_key(public_key: &str) -> Result<Self, String> {
        let trimmed = public_key.trim();
        if trimmed.is_empty() {
            return Err("JWT public key is empty".to_string());
        }

        if let Ok(jwk) = serde_json::from_str::<Jwk>(trimmed) {
            return Ok(Self::JwkSet(JwkSet { keys: vec![jwk] }));
        }

        if let Ok(decoding_key) = DecodingKey::from_rsa_pem(trimmed.as_bytes()) {
            return Ok(Self::Pem(PemPublicKeyVerifier::rsa(decoding_key)));
        }
        if let Ok(decoding_key) = DecodingKey::from_ec_pem(trimmed.as_bytes()) {
            return Ok(Self::Pem(PemPublicKeyVerifier::ec(decoding_key)));
        }
        if let Ok(decoding_key) = DecodingKey::from_ed_pem(trimmed.as_bytes()) {
            return Ok(Self::Pem(PemPublicKeyVerifier::ed(decoding_key)));
        }

        Err(
            "unsupported JWT public key format; expected a single JWK JSON object or a PEM public key"
                .to_string(),
        )
    }
}

pub struct PemPublicKeyVerifier {
    decoding_key: DecodingKey,
    kind: PemPublicKeyKind,
}

impl PemPublicKeyVerifier {
    fn rsa(decoding_key: DecodingKey) -> Self {
        Self {
            decoding_key,
            kind: PemPublicKeyKind::Rsa,
        }
    }

    fn ec(decoding_key: DecodingKey) -> Self {
        Self {
            decoding_key,
            kind: PemPublicKeyKind::Ec,
        }
    }

    fn ed(decoding_key: DecodingKey) -> Self {
        Self {
            decoding_key,
            kind: PemPublicKeyKind::Ed,
        }
    }

    fn supports(&self, algorithm: Algorithm) -> bool {
        match self.kind {
            PemPublicKeyKind::Rsa => matches!(
                algorithm,
                Algorithm::RS256
                    | Algorithm::RS384
                    | Algorithm::RS512
                    | Algorithm::PS256
                    | Algorithm::PS384
                    | Algorithm::PS512
            ),
            PemPublicKeyKind::Ec => matches!(algorithm, Algorithm::ES256 | Algorithm::ES384),
            PemPublicKeyKind::Ed => matches!(algorithm, Algorithm::EdDSA),
        }
    }
}

enum PemPublicKeyKind {
    Rsa,
    Ec,
    Ed,
}

async fn fetch_jwks(http_client: &reqwest::Client, endpoint: &str) -> Result<JwkSet, String> {
    let response = http_client
        .get(endpoint)
        .send()
        .await
        .map_err(|err| format!("JWKS request failed: {err}"))?;

    let status = response.status();
    if !status.is_success() {
        return Err(format!("JWKS endpoint returned status {status}"));
    }

    let jwks = response
        .json::<JwkSet>()
        .await
        .map_err(|err| format!("failed to parse JWKS response: {err}"))?;

    if jwks.keys.is_empty() {
        return Err("JWKS response contained no keys".to_string());
    }

    Ok(jwks)
}

fn now_timestamp_us() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_micros().min(u128::from(u64::MAX)) as u64,
        Err(_) => 0,
    }
}

fn system_now_seconds() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    }
}

fn local_first_auth_error(message: String) -> UnauthenticatedResponse {
    if message.starts_with("token expired:") {
        UnauthenticatedResponse::expired("JWT has expired")
    } else {
        UnauthenticatedResponse::invalid(message)
    }
}

fn extract_cookie_value<'a>(cookie_header: &'a str, name: &str) -> Option<&'a str> {
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

fn read_auth_token_from_cookie<'a>(headers: &'a HeaderMap, config: &AuthConfig) -> Option<&'a str> {
    let cookie_name = config.auth_cookie_name.as_deref()?;
    let cookie_header = headers
        .get(axum::http::header::COOKIE)
        .and_then(|value| value.to_str().ok())?;
    extract_cookie_value(cookie_header, cookie_name).map(str::trim)
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
    type Rejection = (StatusCode, String);

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
                StatusCode::UNAUTHORIZED,
                "Invalid Authorization header format".to_string(),
            ));
        };

        let jwt_result = if let Some(ref verifier) = state.jwt_verifier {
            verifier
                .validate_at(token, state.auth_config.clock.now_seconds())
                .await
        } else {
            Err(JwtError::NoKeyConfigured)
        };

        match jwt_result {
            Ok(verified) => {
                let session = resolve_verified_jwt_session(verified)
                    .map_err(|error| (StatusCode::UNAUTHORIZED, error.message))?;
                Ok(JwtAuth(Some(session)))
            }
            Err(JwtError::NoKeyConfigured) => Err((
                StatusCode::UNAUTHORIZED,
                "JWT auth is not enabled for this app".to_string(),
            )),
            Err(JwtError::Expired) => {
                Err((StatusCode::UNAUTHORIZED, "JWT has expired".to_string()))
            }
            Err(JwtError::Invalid(message)) => Err((StatusCode::UNAUTHORIZED, message)),
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
    type Rejection = (StatusCode, String);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<ServerState>,
    ) -> Result<Self, Self::Rejection> {
        let session = extract_session(
            &parts.headers,
            state.app_id,
            &state.auth_config,
            state.jwt_verifier.as_deref(),
        )
        .await
        .map_err(|error| (StatusCode::UNAUTHORIZED, error.message))?;
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

/// Verify JWT signature with error classification for retry logic.
///
/// Returns `Retryable` for unknown kid or signature mismatch (may succeed after
/// JWKS refresh) and `Fatal` for malformed tokens (will never succeed).
pub fn verify_jwt_signature_with_jwks(
    token: &str,
    jwks: &JwkSet,
) -> Result<VerifiedJwt, JwtVerificationError> {
    let header = decode_header(token)
        .map_err(|e| JwtVerificationError::Fatal(format!("invalid JWT header: {e}")))?;

    let candidates = select_jwk_candidates(jwks, header.kid.as_deref(), header.alg);
    if candidates.is_empty() {
        let reason = match header.kid.as_deref() {
            Some(kid) => format!("no JWKS key matched token kid '{kid}'"),
            None => "no compatible JWKS key found for token algorithm".to_string(),
        };
        return Err(JwtVerificationError::Retryable(reason));
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
                    claims: data.claims.claims,
                    exp: data.claims.exp,
                });
            }
            Err(e) => {
                last_error = Some(format!("JWT signature verification failed: {e}"));
            }
        }
    }

    Err(JwtVerificationError::Retryable(last_error.unwrap_or_else(
        || "JWT signature verification failed".to_string(),
    )))
}

fn ensure_jwt_not_expired_at(verified: &VerifiedJwt, now: u64) -> Result<(), JwtError> {
    let Some(exp) = verified.exp else {
        return Ok(());
    };

    if exp <= now {
        return Err(JwtError::Expired);
    }

    Ok(())
}

/// Validate JWT with JWKS cache, including on-demand refresh on retryable errors.
///
/// 1. Try with cached JWKS
/// 2. On retryable error (unknown kid, signature mismatch), force one refresh
/// 3. Retry with fresh JWKS
/// 4. If still failing, return the error
pub async fn validate_jwt_with_cache(
    token: &str,
    cache: &JwksCache,
) -> Result<VerifiedJwt, JwtError> {
    validate_jwt_with_cache_at(token, cache, system_now_seconds()).await
}

pub async fn validate_jwt_with_cache_at(
    token: &str,
    cache: &JwksCache,
    now_seconds: u64,
) -> Result<VerifiedJwt, JwtError> {
    let cached_jwks = cache.load(false).await.map_err(|e| {
        warn!(error = %e, "failed to load cached JWKS");
        JwtError::Invalid("unable to load JWKS".to_string())
    })?;

    match verify_jwt_signature_with_jwks(token, &cached_jwks) {
        Ok(verified) => {
            ensure_jwt_not_expired_at(&verified, now_seconds)?;
            return Ok(verified);
        }
        Err(JwtVerificationError::Fatal(e)) => return Err(JwtError::Invalid(e)),
        Err(JwtVerificationError::Retryable(e)) => {
            warn!(
                error = %e,
                "JWT validation failed with cached JWKS; forcing one refresh"
            );
        }
    }

    let refreshed_jwks = cache.load(true).await.map_err(|e| {
        warn!(error = %e, "failed to refresh JWKS");
        JwtError::Invalid("unable to refresh JWKS".to_string())
    })?;

    match verify_jwt_signature_with_jwks(token, &refreshed_jwks) {
        Ok(verified) => {
            ensure_jwt_not_expired_at(&verified, now_seconds)?;
            Ok(verified)
        }
        Err(JwtVerificationError::Retryable(e) | JwtVerificationError::Fatal(e)) => {
            warn!(error = %e, "JWT validation failed after JWKS refresh");
            Err(JwtError::Invalid(e))
        }
    }
}

fn verify_jwt_signature_with_pem_public_key(
    token: &str,
    verifier: &PemPublicKeyVerifier,
) -> Result<VerifiedJwt, JwtVerificationError> {
    let header = decode_header(token)
        .map_err(|e| JwtVerificationError::Fatal(format!("invalid JWT header: {e}")))?;

    if !verifier.supports(header.alg) {
        return Err(JwtVerificationError::Fatal(format!(
            "token algorithm {:?} is not compatible with the configured JWT public key",
            header.alg
        )));
    }

    let validation = signature_only_validation(header.alg);
    match decode::<JwtClaims>(token, &verifier.decoding_key, &validation) {
        Ok(data) => Ok(VerifiedJwt {
            subject: data.claims.sub,
            issuer: data.claims.iss,
            claims: data.claims.claims,
            exp: data.claims.exp,
        }),
        Err(e) => Err(JwtVerificationError::Fatal(format!(
            "JWT signature verification failed: {e}"
        ))),
    }
}

pub fn validate_jwt_with_static_key_at(
    token: &str,
    verifier: &StaticJwtVerifier,
    now_seconds: u64,
) -> Result<VerifiedJwt, JwtError> {
    let verified = match verifier {
        StaticJwtVerifier::JwkSet(jwks) => verify_jwt_signature_with_jwks(token, jwks),
        StaticJwtVerifier::Pem(verifier) => {
            verify_jwt_signature_with_pem_public_key(token, verifier)
        }
    }
    .map_err(|error| match error {
        JwtVerificationError::Retryable(message) | JwtVerificationError::Fatal(message) => {
            JwtError::Invalid(message)
        }
    })?;

    ensure_jwt_not_expired_at(&verified, now_seconds)?;
    Ok(verified)
}

/// Resolve a session from a validated external JWT.
///
/// The session's `user_id` is the JWT `sub` claim verbatim. Integrations that
/// need a different identity (e.g. mapping a stable provider id to a Jazz user
/// id) must do that mapping upstream and mint `sub` accordingly.
pub fn resolve_verified_jwt_session(
    verified: VerifiedJwt,
) -> Result<Session, UnauthenticatedResponse> {
    let subject = verified.subject.trim();
    if subject.is_empty() {
        return Err(UnauthenticatedResponse::invalid("Invalid JWT subject"));
    }

    let issuer = verified
        .issuer
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());

    let claims = match verified.claims {
        serde_json::Value::Object(mut map) => {
            map.insert("subject".to_string(), serde_json::json!(subject));
            if let Some(iss) = issuer {
                map.insert("issuer".to_string(), serde_json::json!(iss));
            }
            serde_json::Value::Object(map)
        }
        _ => serde_json::json!({
            "subject": subject,
            "issuer": issuer,
        }),
    };

    Ok(Session {
        user_id: subject.to_string(),
        claims,
        auth_mode: crate::query_manager::session::AuthMode::External,
    })
}

/// Check if a JWT has a Jazz self-signed `iss` (local-first or anonymous) by
/// decoding claims without verification.
fn is_jazz_self_signed_identity_proof(token: &str) -> Option<&'static str> {
    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() != 3 {
        return None;
    }
    let claims_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .ok()?;
    #[derive(serde::Deserialize)]
    struct IssOnly {
        iss: Option<String>,
    }
    let claims = serde_json::from_slice::<IssOnly>(&claims_bytes).ok()?;
    match claims.iss.as_deref() {
        Some(identity::LOCAL_FIRST_ISSUER) => Some(identity::LOCAL_FIRST_ISSUER),
        Some(identity::ANONYMOUS_ISSUER) => Some(identity::ANONYMOUS_ISSUER),
        _ => None,
    }
}

/// Extract session from headers with priority resolution.
///
/// Priority:
/// 1. Backend impersonation (X-Jazz-Backend-Secret + X-Jazz-Session)
/// 2. JWT auth (`Authorization: Bearer`, or auth cookie when configured)
/// 3. No session
///
/// When `jwt_verifier` is provided, external JWT validation uses the configured
/// verifier. Without one, JWT auth returns "not configured."
pub async fn extract_session(
    headers: &HeaderMap,
    app_id: AppId,
    config: &AuthConfig,
    jwt_verifier: Option<&JwtVerifier>,
) -> Result<Option<Session>, UnauthenticatedResponse> {
    // Priority 1: Backend impersonation
    if let Some(session_b64) = headers.get("X-Jazz-Session").and_then(|v| v.to_str().ok()) {
        let backend_secret = headers
            .get("X-Jazz-Backend-Secret")
            .and_then(|v| v.to_str().ok());

        match (&config.backend_secret, backend_secret) {
            (Some(expected), Some(got)) if expected == got => {
                let session = decode_session_header(session_b64)
                    .ok_or_else(|| UnauthenticatedResponse::invalid("Invalid session format"))?;
                return Ok(Some(session));
            }
            (Some(_), Some(_)) => {
                return Err(UnauthenticatedResponse::invalid("Invalid backend secret"));
            }
            (Some(_), None) => {
                return Err(UnauthenticatedResponse::invalid(
                    "Backend secret required for session impersonation",
                ));
            }
            (None, Some(_)) => {
                return Err(UnauthenticatedResponse::disabled(
                    "Backend auth not configured",
                ));
            }
            (None, None) => {
                // Session header without secret - ignore and fall through to JWT
            }
        }
    }

    // Priority 2: JWT auth
    let token = if let Some(auth_value) = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        let Some(token) = auth_value.strip_prefix("Bearer ") else {
            return Err(UnauthenticatedResponse::invalid(
                "Invalid Authorization header format",
            ));
        };

        let token = token.trim();
        if token.is_empty() {
            return Err(UnauthenticatedResponse::invalid("Empty bearer token"));
        }
        Some(token)
    } else {
        read_auth_token_from_cookie(headers, config)
    };

    if let Some(token) = token {
        // Self-signed JWT path (local-first or anonymous).
        //
        // Anonymous is always accepted at the transport layer — apps gate
        // anonymous reads/writes via the permissions DSL
        // (`session.where({ authMode: "anonymous" })`) and Task 6's write-deny
        // middleware. Local-first still requires the explicit config opt-in.
        if let Some(issuer) = is_jazz_self_signed_identity_proof(token) {
            if issuer == identity::LOCAL_FIRST_ISSUER && !config.allow_local_first_auth {
                return Err(UnauthenticatedResponse::disabled(
                    "Local-first auth is not enabled for this app",
                ));
            }
            let verified = identity::verify_jazz_self_signed_proof_at(
                token,
                &app_id.to_string(),
                config.clock.now_seconds(),
            )
            .map_err(local_first_auth_error)?;
            let auth_mode = match issuer {
                identity::ANONYMOUS_ISSUER => crate::query_manager::session::AuthMode::Anonymous,
                _ => crate::query_manager::session::AuthMode::LocalFirst,
            };
            return Ok(Some(Session {
                user_id: verified.user_id,
                claims: serde_json::Value::Object(serde_json::Map::new()),
                auth_mode,
            }));
        }

        // External JWT path.
        let jwt_result = if let Some(verifier) = jwt_verifier {
            verifier
                .validate_at(token, config.clock.now_seconds())
                .await
        } else {
            Err(JwtError::NoKeyConfigured)
        };

        match jwt_result {
            Ok(verified) => {
                let session = resolve_verified_jwt_session(verified)?;
                return Ok(Some(session));
            }
            Err(JwtError::NoKeyConfigured) => {
                return Err(UnauthenticatedResponse::disabled(
                    "JWT auth is not enabled for this app",
                ));
            }
            Err(JwtError::Expired) => {
                return Err(UnauthenticatedResponse::expired("JWT has expired"));
            }
            Err(JwtError::Invalid(message)) => {
                return Err(UnauthenticatedResponse::invalid(message));
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

/// Check if backend secret is valid.
pub fn validate_backend_secret(
    provided: Option<&str>,
    config: &AuthConfig,
) -> Result<(), (StatusCode, &'static str)> {
    match (&config.backend_secret, provided) {
        (Some(expected), Some(got)) if expected == got => Ok(()),
        (Some(_), Some(_)) => Err((StatusCode::UNAUTHORIZED, "Invalid backend secret")),
        (Some(_), None) => Err((
            StatusCode::UNAUTHORIZED,
            "Backend secret required for backend access",
        )),
        (None, Some(_)) => Err((StatusCode::FORBIDDEN, "Backend auth not configured")),
        (None, None) => Err((StatusCode::UNAUTHORIZED, "Backend secret required")),
    }
}

/// Check if admin secret is valid.
///
/// Admin publication endpoints require admin authentication. Development-mode
/// schema auto-push from ordinary clients flows through the WebSocket transport
/// and does not use this helper.
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
        (None, _) => Err((StatusCode::FORBIDDEN, "Admin auth not configured")),
    }
}

/// Check if peer sync secret is valid.
pub fn validate_peer_secret(
    provided: Option<&str>,
    config: &AuthConfig,
) -> Result<(), (StatusCode, &'static str)> {
    match (&config.peer_secret, provided) {
        (Some(expected), Some(got)) if expected == got => Ok(()),
        (Some(_), Some(_)) => Err((StatusCode::UNAUTHORIZED, "Invalid peer secret")),
        (Some(_), None) => Err((
            StatusCode::UNAUTHORIZED,
            "Peer secret required for peer sync",
        )),
        (None, Some(_)) => Err((StatusCode::FORBIDDEN, "Peer auth not configured")),
        (None, None) => Err((StatusCode::UNAUTHORIZED, "Peer secret required")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport_protocol::UnauthenticatedCode;
    use jsonwebtoken::{EncodingKey, Header, encode};

    const TEST_JWKS_KID: &str = "test-kid";
    const TEST_JWKS_SECRET: &str = "test-secret-key-for-jwt";

    fn test_app_id() -> AppId {
        AppId::from_name("jazz-tools-auth-tests")
    }

    fn make_test_config() -> AuthConfig {
        AuthConfig {
            jwks_url: Some("https://example.test/.well-known/jwks.json".to_string()),
            allow_local_first_auth: false,
            backend_secret: Some("backend-secret-12345".to_string()),
            admin_secret: Some("admin-secret-67890".to_string()),
            ..Default::default()
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

    fn test_jwks_cache() -> JwksCache {
        JwksCache::from_static(make_hs256_jwks(TEST_JWKS_KID, TEST_JWKS_SECRET))
    }

    fn test_jwt_verifier() -> JwtVerifier {
        JwtVerifier::Jwks(test_jwks_cache())
    }

    fn make_jwt(claims: &JwtClaims, secret: &str, kid: &str) -> String {
        let key = EncodingKey::from_secret(secret.as_bytes());
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(kid.to_string());
        encode(&header, claims, &key).unwrap()
    }

    #[test]
    fn test_jwt_validation_valid() {
        let jwks = make_hs256_jwks(TEST_JWKS_KID, TEST_JWKS_SECRET);
        let claims = JwtClaims {
            sub: "user-123".to_string(),
            iss: None,
            claims: serde_json::json!({"role": "admin"}),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, TEST_JWKS_SECRET, TEST_JWKS_KID);

        let verified = verify_jwt_signature_with_jwks(&token, &jwks).unwrap();
        assert_eq!(verified.subject, "user-123");
        assert_eq!(verified.claims["role"], "admin");
    }

    #[test]
    fn test_jwt_validation_wrong_secret() {
        let jwks = make_hs256_jwks(TEST_JWKS_KID, TEST_JWKS_SECRET);
        let claims = JwtClaims {
            sub: "user-123".to_string(),
            iss: None,
            claims: serde_json::json!({}),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, "wrong-secret", TEST_JWKS_KID);

        let result = verify_jwt_signature_with_jwks(&token, &jwks);
        assert!(matches!(result, Err(JwtVerificationError::Retryable(_))));
    }

    #[test]
    fn test_jwt_validation_empty_jwks() {
        let jwks = JwkSet { keys: vec![] };
        let claims = JwtClaims {
            sub: "user-123".to_string(),
            iss: None,
            claims: serde_json::json!({}),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, "any-secret", TEST_JWKS_KID);

        let result = verify_jwt_signature_with_jwks(&token, &jwks);
        assert!(matches!(result, Err(JwtVerificationError::Retryable(_))));
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

    #[tokio::test]
    async fn test_extract_session_backend_impersonation() {
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

        let result = extract_session(&headers, test_app_id(), &config, None)
            .await
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().user_id, "impersonated-user");
    }

    #[tokio::test]
    async fn test_extract_session_backend_wrong_secret() {
        let config = make_test_config();
        let mut headers = HeaderMap::new();

        let session = Session::new("user");
        let session_json = serde_json::to_string(&session).unwrap();
        let session_b64 = base64::engine::general_purpose::STANDARD.encode(&session_json);

        headers.insert("X-Jazz-Backend-Secret", "wrong-secret".parse().unwrap());
        headers.insert("X-Jazz-Session", session_b64.parse().unwrap());

        let result = extract_session(&headers, test_app_id(), &config, None).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, UnauthenticatedCode::Invalid);
    }

    #[tokio::test]
    async fn test_extract_session_jwt_fallback() {
        let config = make_test_config();
        let cache = test_jwt_verifier();
        let mut headers = HeaderMap::new();

        let claims = JwtClaims {
            sub: "jwt-user".to_string(),
            iss: None,
            claims: serde_json::json!({}),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, TEST_JWKS_SECRET, TEST_JWKS_KID);

        headers.insert(AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());

        let result = extract_session(&headers, test_app_id(), &config, Some(&cache))
            .await
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().user_id, "jwt-user");
    }

    #[tokio::test]
    async fn test_extract_session_jwt_cookie_fallback() {
        let mut config = make_test_config();
        config.auth_cookie_name = Some("jazz-auth".to_string());
        let cache = test_jwt_verifier();
        let mut headers = HeaderMap::new();

        let claims = JwtClaims {
            sub: "cookie-user".to_string(),
            iss: Some("https://issuer.example".to_string()),
            claims: serde_json::json!({ "role": "editor" }),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, TEST_JWKS_SECRET, TEST_JWKS_KID);

        headers.insert(
            axum::http::header::COOKIE,
            format!("other=value; jazz-auth={token}").parse().unwrap(),
        );

        let result = extract_session(&headers, test_app_id(), &config, Some(&cache))
            .await
            .unwrap();
        let session = result.expect("session");
        assert_eq!(session.user_id, "cookie-user");
        assert_eq!(session.claims["role"], "editor");
    }

    #[tokio::test]
    async fn test_extract_session_external_jwt_uses_sub_as_user_id() {
        let config = make_test_config();
        let cache = test_jwt_verifier();
        let mut headers = HeaderMap::new();

        let claims = JwtClaims {
            sub: "user-42".to_string(),
            iss: Some("https://issuer.example".to_string()),
            claims: serde_json::json!({ "role": "admin" }),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, TEST_JWKS_SECRET, TEST_JWKS_KID);
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());

        let session = extract_session(&headers, test_app_id(), &config, Some(&cache))
            .await
            .unwrap()
            .expect("session");

        assert_eq!(session.user_id, "user-42");
        assert_eq!(
            session.auth_mode,
            crate::query_manager::session::AuthMode::External
        );
        assert_eq!(session.claims["subject"], "user-42");
        assert_eq!(session.claims["issuer"], "https://issuer.example");
    }

    #[tokio::test]
    async fn test_extract_session_backend_takes_priority() {
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
            claims: serde_json::json!({}),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, TEST_JWKS_SECRET, TEST_JWKS_KID);
        headers.insert(AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());

        let result = extract_session(&headers, test_app_id(), &config, None)
            .await
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().user_id, "backend-user"); // Backend wins
    }

    #[tokio::test]
    async fn test_extract_session_no_auth() {
        let config = make_test_config();
        let headers = HeaderMap::new();

        let result = extract_session(&headers, test_app_id(), &config, None)
            .await
            .unwrap();
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

    // Self-signed auth tests

    fn alice_seed() -> [u8; 32] {
        let mut seed = [0u8; 32];
        seed[0] = 0xAA;
        seed[31] = 0x01;
        seed
    }

    #[tokio::test]
    async fn local_first_session_has_auth_mode_localfirst_and_no_claim() {
        let app_id = AppId::from_name("test-app");
        let seed = [7u8; 32];
        let token = crate::identity::mint_jazz_self_signed_token(
            &seed,
            crate::identity::LOCAL_FIRST_ISSUER,
            &app_id.to_string(),
            3600,
        )
        .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        let config = AuthConfig {
            allow_local_first_auth: true,
            ..Default::default()
        };

        let session = extract_session(&headers, app_id, &config, None)
            .await
            .unwrap()
            .expect("session");

        assert_eq!(
            session.auth_mode,
            crate::query_manager::session::AuthMode::LocalFirst
        );
        if let serde_json::Value::Object(map) = &session.claims {
            assert!(
                !map.contains_key("auth_mode"),
                "claims must not carry auth_mode anymore"
            );
        } else {
            panic!("expected object claims");
        }
    }

    fn make_local_first_auth_config() -> AuthConfig {
        AuthConfig {
            jwks_url: None,
            allow_local_first_auth: true,
            backend_secret: None,
            admin_secret: None,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn local_first_auth_jwt_authenticates() {
        let seed = alice_seed();
        let app_id = test_app_id();
        let token = identity::mint_jazz_self_signed_token(
            &seed,
            identity::LOCAL_FIRST_ISSUER,
            &app_id.to_string(),
            3600,
        )
        .unwrap();
        let config = make_local_first_auth_config();
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        let session = extract_session(&headers, app_id, &config, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.user_id, identity::derive_user_id(&seed).to_string());
    }

    #[tokio::test]
    async fn local_first_auth_jwt_wrong_audience_rejected() {
        let token = identity::mint_jazz_self_signed_token(
            &alice_seed(),
            identity::LOCAL_FIRST_ISSUER,
            "wrong-app",
            3600,
        )
        .unwrap();
        let config = make_local_first_auth_config();
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        let result = extract_session(&headers, test_app_id(), &config, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn local_first_auth_disabled_rejects() {
        let app_id = test_app_id();
        let token = identity::mint_jazz_self_signed_token(
            &alice_seed(),
            identity::LOCAL_FIRST_ISSUER,
            &app_id.to_string(),
            3600,
        )
        .unwrap();
        let mut config = make_local_first_auth_config();
        config.allow_local_first_auth = false;
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        let result = extract_session(&headers, app_id, &config, None).await;
        assert!(result.is_err());
    }

    #[cfg(feature = "test-utils")]
    #[tokio::test]
    async fn local_first_auth_expiry_uses_configured_test_clock() {
        let app_id = test_app_id();
        let clock = TestClock::new(1_700_000_000);
        let config = AuthConfig {
            allow_local_first_auth: true,
            clock: clock.clone().into(),
            ..Default::default()
        };
        let token = identity::mint_jazz_self_signed_token_at(
            &alice_seed(),
            identity::LOCAL_FIRST_ISSUER,
            &app_id.to_string(),
            5,
            clock.now_seconds(),
        )
        .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());

        let session = extract_session(&headers, app_id, &config, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            session.user_id,
            identity::derive_user_id(&alice_seed()).to_string()
        );

        clock.advance(Duration::from_secs(6));

        let result = extract_session(&headers, app_id, &config, None).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, UnauthenticatedCode::Expired);
    }

    #[tokio::test]
    async fn non_local_first_auth_iss_does_not_use_local_first_auth_path() {
        let config = make_local_first_auth_config();
        let claims = JwtClaims {
            sub: "user-123".to_string(),
            iss: Some("https://auth.example.com".to_string()),
            claims: serde_json::json!({}),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, TEST_JWKS_SECRET, TEST_JWKS_KID);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        // Should fail because no JWKS configured in local_first_auth_config
        let result = extract_session(&headers, test_app_id(), &config, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn anonymous_session_has_auth_mode_anonymous() {
        let app_id = AppId::from_name("test-app");
        let seed = [9u8; 32];
        let clock = TestClock::new(1_000_000);
        let token = crate::identity::mint_jazz_self_signed_token_at(
            &seed,
            crate::identity::ANONYMOUS_ISSUER,
            &app_id.to_string(),
            3600,
            clock.now_seconds(),
        )
        .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        // Deliberately NOT setting allow_local_first_auth — anonymous is always
        // accepted at the transport layer; permissions gate reads, Task 6 gates writes.
        let config = AuthConfig {
            clock: clock.into(),
            ..Default::default()
        };

        let session = extract_session(&headers, app_id, &config, None)
            .await
            .unwrap()
            .expect("session");

        assert_eq!(
            session.auth_mode,
            crate::query_manager::session::AuthMode::Anonymous
        );
    }
}
