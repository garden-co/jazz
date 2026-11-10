//! Request authentication and validation.
//!
//! # Auth Methods
//!
//! 1. **Local-first Auth** (`Authorization: Bearer <self-signed Ed25519 JWT>`):
//!    Clients authenticate with a self-signed JWT containing an Ed25519 identity proof.
//!
//! 2. **Backend Secret** (`X-Jazz-Backend-Secret` + `X-Jazz-Session`): the app's
//!    backend (`createJazz`) acts for the users it authenticated.
//!
//! 3. **Admin Secret** (`X-Jazz-Admin-Secret`): Required for schema/lens/policy sync.
//!
//! A core verifies no external JWTs and reads no auth cookies: the app's
//! backend decides who a user is and states it with its backend secret.
//!
//! # Session Resolution Priority
//!
//! When resolving the request session:
//! 1. Backend impersonation (if `X-Jazz-Backend-Secret` + `X-Jazz-Session` present)
//! 2. A Jazz self-signed proof (`Authorization: Bearer`, local-first or anonymous)
//! 3. No session

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, StatusCode, header::AUTHORIZATION};
use base64::Engine;
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation,
    dangerous::insecure_decode,
    decode, decode_header,
    jwk::{Jwk, JwkSet, KeyAlgorithm},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, watch};
use tracing::warn;

use jazz::tools::AppId;
use jazz::tools::Session;
use jazz::tools::identity;
use jazz::tools::transport_error::UnauthenticatedResponse;

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

#[cfg(feature = "test")]
#[derive(Clone, Debug)]
pub struct TestClock {
    now_seconds: Arc<AtomicU64>,
}

#[cfg(feature = "test")]
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

#[cfg(feature = "test")]
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
    /// Whether local-first Ed25519 JWT auth is allowed (default: true for new apps).
    pub allow_local_first_auth: bool,
    /// Secret for backend session impersonation.
    pub backend_secret: Option<String>,
    /// Secret for admin operations (schema/policy sync).
    pub admin_secret: Option<String>,
    /// The one workload (an OIDC identity) this server trusts instead of
    /// shared secrets. When set, nothing else is admitted.
    pub trusted_workload: Option<super::workload::TrustedWorkload>,
    /// Time source for auth expiry checks. Defaults to the system clock.
    pub clock: AuthClock,
}

impl AuthConfig {
    /// Check if any auth is configured.
    pub fn is_configured(&self) -> bool {
        self.allow_local_first_auth
            || self.backend_secret.is_some()
            || self.admin_secret.is_some()
            || self.trusted_workload.is_some()
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
///   "iss": "https://auth.example.com",
///   "sub": "user-123",
///   "role": "admin",
///   "teams": ["eng"],
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
    /// Additional JWT payload fields retained alongside registered claims.
    #[serde(flatten)]
    #[serde(default)]
    pub claims: std::collections::BTreeMap<String, serde_json::Value>,
    /// Expiration time (Unix timestamp).
    #[serde(default)]
    pub exp: Option<u64>,
    /// Issued at time (Unix timestamp).
    #[serde(default)]
    pub iat: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum JwtAudience {
    One(String),
    Many(Vec<String>),
}

impl JwtAudience {
    fn into_values(self) -> Vec<String> {
        match self {
            Self::One(value) => vec![value],
            Self::Many(values) => values,
        }
    }
}

#[derive(Debug, Deserialize)]
struct DecodedJwtClaims {
    sub: String,
    #[serde(default)]
    iss: Option<String>,
    #[serde(default)]
    aud: Option<JwtAudience>,
    #[serde(flatten)]
    extra: std::collections::BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    exp: Option<u64>,
    #[serde(default)]
    nbf: Option<u64>,
    #[serde(default)]
    iat: Option<u64>,
    #[serde(default)]
    jti: Option<String>,
}

/// JWT identity data extracted after signature validation.
#[derive(Debug, Clone)]
pub struct VerifiedJwt {
    pub subject: String,
    pub issuer: Option<String>,
    pub claims: serde_json::Value,
    pub exp: Option<u64>,
    pub audiences: Vec<String>,
    pub not_before: Option<u64>,
}

/// JWT validation error.
#[derive(Debug)]
pub enum JwtError {
    /// Token signature is valid but `exp` is in the past.
    Expired,
    /// Invalid token format or signature.
    Invalid(String),
    /// No bounded keyset is available to decide whether the token is valid.
    Unavailable(String),
}

impl std::fmt::Display for JwtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JwtError::Expired => write!(f, "JWT has expired"),
            JwtError::Invalid(msg) => write!(f, "Invalid JWT: {}", msg),
            JwtError::Unavailable(msg) => write!(f, "JWT verification unavailable: {}", msg),
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
    loaded: LoadedJwks,
}

#[derive(Clone)]
struct LoadedJwks {
    set: JwkSet,
    // Stale keys may still verify a token, but cannot prove another is invalid
    // when the provider could not supply its current keys.
    refresh_error: Option<String>,
}

struct JwksRefreshFlight {
    result: watch::Sender<Option<Result<LoadedJwks, String>>>,
}

impl JwksRefreshFlight {
    fn new() -> std::sync::Arc<Self> {
        let (result, _) = watch::channel(None);
        std::sync::Arc::new(Self { result })
    }

    async fn wait(&self) -> Result<LoadedJwks, String> {
        let mut receiver = self.result.subscribe();
        loop {
            let result = receiver.borrow().clone();
            if let Some(result) = result {
                return result;
            }
            if receiver.changed().await.is_err() {
                return Err("JWKS refresh coordination ended unexpectedly".to_owned());
            }
        }
    }

    fn publish(&self, result: Result<LoadedJwks, String>) {
        // A caller can join before subscribing. Retain completion even when no
        // receivers exist yet, including fetch errors and owner cancellation.
        self.result.send_replace(Some(result));
    }
}

struct JwksRefreshOwner<'a> {
    cache: &'a JwksCache,
    flight: std::sync::Arc<JwksRefreshFlight>,
    finished: bool,
}

impl Drop for JwksRefreshOwner<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.flight
            .publish(Err("JWKS refresh was cancelled".to_owned()));
        self.cache.clear_refresh_flight(&self.flight);
    }
}

/// JWKS cache with TTL-based expiry and on-demand refresh.
///
/// Caches the keyset from a JWKS endpoint and transparently refetches when:
/// - The TTL (5 min) has elapsed, or
/// - A caller forces a refresh (e.g. after encountering an unknown kid).
pub struct JwksCache {
    endpoint: String,
    refresh_flight: std::sync::Mutex<Option<std::sync::Arc<JwksRefreshFlight>>>,
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
            refresh_flight: std::sync::Mutex::new(None),
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
            refresh_flight: std::sync::Mutex::new(None),
            http_client: reqwest::Client::new(),
            ttl: JWKS_CACHE_TTL,
            max_stale: JWKS_MAX_STALE,
            cached: RwLock::new(Some(CachedJwksEntry {
                endpoint: String::new(),
                fetched_at_us: now_timestamp_us(),
                loaded: LoadedJwks {
                    set: jwks,
                    refresh_error: None,
                },
            })),
            last_forced_refresh_us: AtomicU64::new(0),
        }
    }

    /// Load the JWKS, returning a cached copy if fresh or fetching anew.
    ///
    /// Forced refreshes are reserved before network I/O. All cold, expired,
    /// and forced loads share one in-flight refresh for this cache instance.
    pub async fn load(&self, force_requested: bool) -> Result<JwkSet, String> {
        self.load_with_status(force_requested)
            .await
            .map(|loaded| loaded.set)
    }

    async fn load_with_status(&self, force_requested: bool) -> Result<LoadedJwks, String> {
        let ttl_us = self.ttl.as_micros().min(u128::from(u64::MAX)) as u64;
        let cooldown_us = JWKS_FORCED_REFRESH_COOLDOWN
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        let max_stale_us = (self.ttl + self.max_stale)
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;

        if !force_requested && let Some(cached) = self.cached_up_to(ttl_us).await {
            return Ok(cached);
        }

        let (flight, owner) = self.start_or_join_refresh();
        if !owner {
            return flight.wait().await;
        }

        let mut owner_guard = JwksRefreshOwner {
            cache: self,
            flight: flight.clone(),
            finished: false,
        };
        let result = if force_requested {
            if self.reserve_forced_refresh(cooldown_us) {
                self.fetch_and_cache(max_stale_us).await
            } else {
                self.cached_during_forced_cooldown(max_stale_us).await
            }
        } else if let Some(cached) = self.cached_up_to(ttl_us).await {
            Ok(cached)
        } else {
            self.fetch_and_cache(max_stale_us).await
        };

        owner_guard.finished = true;
        flight.publish(result.clone());
        self.clear_refresh_flight(&flight);
        result
    }

    fn lock_refresh_flight(
        &self,
    ) -> std::sync::MutexGuard<'_, Option<std::sync::Arc<JwksRefreshFlight>>> {
        self.refresh_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn start_or_join_refresh(&self) -> (std::sync::Arc<JwksRefreshFlight>, bool) {
        let mut guard = self.lock_refresh_flight();
        if let Some(flight) = guard.as_ref() {
            return (flight.clone(), false);
        }
        let flight = JwksRefreshFlight::new();
        *guard = Some(flight.clone());
        (flight, true)
    }

    fn clear_refresh_flight(&self, flight: &std::sync::Arc<JwksRefreshFlight>) {
        let mut guard = self.lock_refresh_flight();
        if guard
            .as_ref()
            .is_some_and(|current| std::sync::Arc::ptr_eq(current, flight))
        {
            *guard = None;
        }
    }

    async fn cached_up_to(&self, max_age_us: u64) -> Option<LoadedJwks> {
        let guard = self.cached.read().await;
        let entry = guard.as_ref()?;
        let age_us = now_timestamp_us().saturating_sub(entry.fetched_at_us);
        (entry.endpoint == self.endpoint && age_us <= max_age_us).then(|| entry.loaded.clone())
    }

    async fn cached_during_forced_cooldown(&self, max_stale_us: u64) -> Result<LoadedJwks, String> {
        self.cached_up_to(max_stale_us).await.ok_or_else(|| {
            "JWKS refresh cooldown active and no bounded cached keyset is available".to_owned()
        })
    }

    async fn fetch_and_cache(&self, max_stale_us: u64) -> Result<LoadedJwks, String> {
        let jwks = match fetch_jwks(&self.http_client, &self.endpoint).await {
            Ok(jwks) => jwks,
            Err(error) => return self.stale_if_error(error, max_stale_us).await,
        };

        let loaded = LoadedJwks {
            set: jwks,
            refresh_error: None,
        };
        let now = now_timestamp_us();
        *self.cached.write().await = Some(CachedJwksEntry {
            endpoint: self.endpoint.clone(),
            fetched_at_us: now,
            loaded: loaded.clone(),
        });
        Ok(loaded)
    }

    async fn stale_if_error(&self, error: String, max_stale_us: u64) -> Result<LoadedJwks, String> {
        let mut guard = self.cached.write().await;
        if let Some(ref mut entry) = *guard {
            let age_us = now_timestamp_us().saturating_sub(entry.fetched_at_us);
            if entry.endpoint == self.endpoint && age_us <= max_stale_us {
                warn!(
                    error = %error,
                    "JWKS fetch failed, serving stale cached keyset"
                );
                entry.loaded.refresh_error = Some(error);
                return Ok(entry.loaded.clone());
            }
            warn!(
                error = %error,
                "JWKS fetch failed and stale keyset has expired"
            );
        }
        Err(error)
    }

    /// Atomically claim the one forced refresh permitted by the cooldown.
    ///
    /// This is intentionally independent of cache loading: it must happen
    /// before an unknown-key request can begin an outbound JWKS request.
    fn reserve_forced_refresh(&self, cooldown_us: u64) -> bool {
        loop {
            let last = self.last_forced_refresh_us.load(Ordering::SeqCst);
            let now = now_timestamp_us();
            if now.saturating_sub(last) <= cooldown_us {
                return false;
            }
            if self
                .last_forced_refresh_us
                .compare_exchange(last, now, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }
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
        KeyAlgorithm::RSA1_5
        | KeyAlgorithm::RSA_OAEP
        | KeyAlgorithm::RSA_OAEP_256
        | KeyAlgorithm::UNKNOWN_ALGORITHM => None,
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

fn verified_jwt(claims: DecodedJwtClaims) -> VerifiedJwt {
    let mut full = claims.extra;
    full.insert(
        "sub".to_owned(),
        serde_json::Value::String(claims.sub.clone()),
    );
    if let Some(issuer) = &claims.iss {
        full.insert("iss".to_owned(), serde_json::Value::String(issuer.clone()));
    }
    if let Some(audience) = &claims.aud {
        full.insert(
            "aud".to_owned(),
            match audience {
                JwtAudience::One(value) => serde_json::Value::String(value.clone()),
                JwtAudience::Many(values) => serde_json::Value::Array(
                    values
                        .iter()
                        .cloned()
                        .map(serde_json::Value::String)
                        .collect(),
                ),
            },
        );
    }
    for (name, value) in [
        ("exp", claims.exp),
        ("nbf", claims.nbf),
        ("iat", claims.iat),
    ] {
        if let Some(value) = value {
            full.insert(name.to_owned(), serde_json::Value::Number(value.into()));
        }
    }
    if let Some(jti) = claims.jti {
        full.insert("jti".to_owned(), serde_json::Value::String(jti));
    }
    VerifiedJwt {
        subject: claims.sub,
        issuer: claims.iss,
        claims: serde_json::Value::Object(full.into_iter().collect()),
        exp: claims.exp,
        audiences: claims.aud.map(JwtAudience::into_values).unwrap_or_default(),
        not_before: claims.nbf,
    }
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

        match decode::<DecodedJwtClaims>(token, &decoding_key, &validation) {
            Ok(data) => return Ok(verified_jwt(data.claims)),
            Err(e) => {
                last_error = Some(format!("JWT signature verification failed: {e}"));
            }
        }
    }

    Err(JwtVerificationError::Retryable(last_error.unwrap_or_else(
        || "JWT signature verification failed".to_string(),
    )))
}

/// The issuer and audience a verified JWT must carry: a trusted workload's.
#[derive(Debug, Clone)]
pub struct ExpectedJwtClaims {
    pub issuer: String,
    pub audience: String,
}

fn ensure_jwt_claims_at(
    verified: &VerifiedJwt,
    expected: &ExpectedJwtClaims,
    now: u64,
) -> Result<(), JwtError> {
    if verified.issuer.as_deref() != Some(expected.issuer.as_str()) {
        return Err(JwtError::Invalid(
            "JWT issuer does not match the configured issuer".to_owned(),
        ));
    }
    if !verified
        .audiences
        .iter()
        .any(|audience| audience == &expected.audience)
    {
        return Err(JwtError::Invalid(
            "JWT audience does not include the configured audience".to_owned(),
        ));
    }

    let exp = verified
        .exp
        .ok_or_else(|| JwtError::Invalid("JWT exp claim is required".to_owned()))?;
    if exp <= now {
        return Err(JwtError::Expired);
    }
    if verified
        .not_before
        .is_some_and(|not_before| not_before > now)
    {
        return Err(JwtError::Invalid("JWT is not valid yet".to_owned()));
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
    expected: &ExpectedJwtClaims,
) -> Result<VerifiedJwt, JwtError> {
    validate_jwt_with_cache_at(token, cache, expected, system_now_seconds()).await
}

pub async fn validate_jwt_with_cache_at(
    token: &str,
    cache: &JwksCache,
    expected: &ExpectedJwtClaims,
    now_seconds: u64,
) -> Result<VerifiedJwt, JwtError> {
    // Check syntax only; these unverified claims must never establish identity.
    // Definitively malformed tokens do not become retryable during a JWKS outage.
    insecure_decode::<DecodedJwtClaims>(token)
        .map_err(|error| JwtError::Invalid(format!("invalid JWT structure: {error}")))?;
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token.rsplit('.').next().unwrap_or_default())
        .map_err(|error| JwtError::Invalid(format!("invalid JWT signature encoding: {error}")))?;
    if signature.is_empty() {
        return Err(JwtError::Invalid("JWT signature is missing".to_owned()));
    }
    let cached_jwks = cache.load(false).await.map_err(|e| {
        warn!(error = %e, "failed to load cached JWKS");
        JwtError::Unavailable("unable to load JWKS".to_string())
    })?;

    match verify_jwt_signature_with_jwks(token, &cached_jwks) {
        Ok(verified) => {
            ensure_jwt_claims_at(&verified, expected, now_seconds)?;
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

    let refreshed_jwks = cache.load_with_status(true).await.map_err(|e| {
        warn!(error = %e, "failed to refresh JWKS");
        JwtError::Unavailable("unable to refresh JWKS".to_string())
    })?;

    match verify_jwt_signature_with_jwks(token, &refreshed_jwks.set) {
        Ok(verified) => {
            ensure_jwt_claims_at(&verified, expected, now_seconds)?;
            Ok(verified)
        }
        Err(JwtVerificationError::Retryable(e)) if refreshed_jwks.refresh_error.is_some() => {
            warn!(error = %e, "JWT could not be verified with fallback JWKS after a refresh failure");
            Err(JwtError::Unavailable("unable to refresh JWKS".to_owned()))
        }
        Err(JwtVerificationError::Retryable(e) | JwtVerificationError::Fatal(e)) => {
            warn!(error = %e, "JWT validation failed after JWKS refresh");
            Err(JwtError::Invalid(e))
        }
    }
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

/// Header with which a trusted backend names the audience it already checked
/// a local-first proof against.
///
/// A browser client without an app id signs its local-first proof for the
/// endpoint it syncs with (its `serverUrl`), which only that endpoint's
/// backend knows. The backend verifies the audience, then forwards the proof
/// with this header and its backend secret; the core verifies the proof for
/// that audience. The audience also namespaces the founder's account id.
pub const LOCAL_FIRST_AUDIENCE_HEADER: &str = "X-Jazz-Local-First-Audience";

/// The audience local-first proofs on this request are verified against: the
/// app id, or the audience a trusted backend vouches for.
pub fn local_first_audience(
    headers: &HeaderMap,
    app_id: AppId,
    config: &AuthConfig,
) -> Result<String, UnauthenticatedResponse> {
    let Some(audience) = headers.get(LOCAL_FIRST_AUDIENCE_HEADER) else {
        return Ok(app_id.to_string());
    };
    let audience = audience
        .to_str()
        .ok()
        .map(str::trim)
        .filter(|audience| !audience.is_empty())
        .ok_or_else(|| UnauthenticatedResponse::invalid("Invalid local-first audience"))?;
    let provided = headers
        .get("X-Jazz-Backend-Secret")
        .and_then(|value| value.to_str().ok());
    validate_backend_secret(provided, config).map_err(|(_, message)| {
        UnauthenticatedResponse::invalid(match message {
            "Invalid backend secret" => message,
            _ => "A local-first audience must be vouched for with the backend secret",
        })
    })?;
    Ok(audience.to_owned())
}

/// The registry namespace a local-first founder's account id derives from:
/// the audience its proof was verified against, read as an app id.
pub fn local_first_founding_app(audience: &str) -> AppId {
    AppId::from_string(audience).unwrap_or_else(|_| AppId::from_name(audience))
}

/// Extract session from headers with priority resolution.
///
/// Priority:
/// 1. Backend impersonation (X-Jazz-Backend-Secret + X-Jazz-Session)
/// 2. A Jazz self-signed proof (`Authorization: Bearer`)
/// 3. No session
///
/// Any other bearer token is refused: the app's backend verifies its users'
/// tokens and acts for them with its backend secret.
pub async fn extract_session(
    headers: &HeaderMap,
    app_id: AppId,
    config: &AuthConfig,
) -> Result<Option<Session>, UnauthenticatedResponse> {
    // Priority 1: Backend impersonation
    if let Some(session_b64) = headers.get("X-Jazz-Session").and_then(|v| v.to_str().ok()) {
        let backend_secret = headers
            .get("X-Jazz-Backend-Secret")
            .and_then(|v| v.to_str().ok());

        match (&config.backend_secret, backend_secret) {
            (Some(expected), Some(got))
                if crate::middleware::workload::constant_time_eq(expected, got) =>
            {
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
        None
    };

    if let Some(token) = token {
        // Self-signed JWT path (local-first or anonymous).
        //
        // Anonymous is accepted at the transport layer so public reads can
        // flow. The core fate authority structurally rejects its writes before
        // table-policy evaluation; apps still gate anonymous reads through the
        // permissions DSL. Local-first requires the explicit config opt-in.
        if let Some(issuer) = is_jazz_self_signed_identity_proof(token) {
            if issuer == identity::LOCAL_FIRST_ISSUER && !config.allow_local_first_auth {
                return Err(UnauthenticatedResponse::disabled(
                    "Local-first auth is not enabled for this app",
                ));
            }
            let audience = local_first_audience(headers, app_id, config)?;
            let verified = identity::verify_jazz_self_signed_proof_at(
                token,
                &audience,
                config.clock.now_seconds(),
            )
            .map_err(local_first_auth_error)?;
            let auth_mode = match issuer {
                identity::ANONYMOUS_ISSUER => jazz::tools::AuthMode::Anonymous,
                _ => jazz::tools::AuthMode::LocalFirst,
            };
            return Ok(Some(Session {
                account_id: None,
                issuer: verified.issuer.to_owned(),
                user_id: verified.user_id,
                // The verified key proves the issuer-scoped subject above; it
                // is transport proof material, not an application policy
                // claim. In particular, fresh anonymous proofs intentionally
                // share the same empty policy binding.
                claims: serde_json::Value::Object(serde_json::Map::new()),
                auth_mode,
            }));
        }

        return Err(UnauthenticatedResponse::disabled(
            "A core verifies no external JWTs: sign users in through your app's backend (createJazz)",
        ));
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
        (Some(expected), Some(got))
            if crate::middleware::workload::constant_time_eq(expected, got) =>
        {
            Ok(())
        }
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
        (Some(expected), Some(got))
            if crate::middleware::workload::constant_time_eq(expected, got) =>
        {
            Ok(())
        }
        (Some(_), Some(_)) => Err((StatusCode::UNAUTHORIZED, "Invalid admin secret")),
        (Some(_), None) => Err((
            StatusCode::UNAUTHORIZED,
            "Admin secret required for this operation",
        )),
        (None, _) => Err((StatusCode::FORBIDDEN, "Admin auth not configured")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::tools::transport_error::UnauthenticatedCode;
    use jsonwebtoken::{EncodingKey, Header, encode};

    const TEST_JWKS_KID: &str = "test-kid";
    const TEST_JWKS_SECRET: &str = "test-secret-key-for-jwt";

    fn test_app_id() -> AppId {
        AppId::from_name("jazz-tools-auth-tests")
    }

    fn make_test_config() -> AuthConfig {
        AuthConfig {
            allow_local_first_auth: false,
            backend_secret: Some("backend-secret-12345".to_string()),
            admin_secret: Some("admin-secret-67890".to_string()),
            ..Default::default()
        }
    }

    fn test_expected_claims() -> ExpectedJwtClaims {
        ExpectedJwtClaims {
            issuer: "https://issuer.jazz.test".to_owned(),
            audience: "jazz-audience".to_owned(),
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

    #[derive(Clone)]
    struct JwksProbeState {
        requests: std::sync::Arc<AtomicU64>,
        body: std::sync::Arc<tokio::sync::RwLock<Option<serde_json::Value>>>,
        gate: std::sync::Arc<tokio::sync::Mutex<Option<std::sync::Arc<JwksProbeGate>>>>,
    }

    struct JwksProbeGate {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        released: std::sync::atomic::AtomicBool,
    }

    struct JwksProbe {
        state: JwksProbeState,
        address: std::net::SocketAddr,
        task: tokio::task::JoinHandle<()>,
    }

    impl JwksProbe {
        async fn start(body: serde_json::Value) -> Self {
            use axum::Router;
            use axum::extract::State;
            use axum::routing::get;

            async fn handler(State(state): State<JwksProbeState>) -> axum::response::Response {
                use axum::response::IntoResponse;

                state.requests.fetch_add(1, Ordering::SeqCst);
                let gate = state.gate.lock().await.clone();
                if let Some(gate) = gate {
                    let released = gate.release.notified();
                    tokio::pin!(released);
                    released.as_mut().enable();
                    if !gate.released.load(Ordering::Acquire) {
                        gate.entered.notify_one();
                        released.await;
                    }
                }

                let Some(body) = state.body.read().await.clone() else {
                    return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
                };
                (axum::http::StatusCode::OK, axum::Json(body)).into_response()
            }

            let state = JwksProbeState {
                requests: std::sync::Arc::new(AtomicU64::new(0)),
                body: std::sync::Arc::new(tokio::sync::RwLock::new(Some(body))),
                gate: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            };
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind JWKS probe");
            let address = listener.local_addr().expect("read JWKS probe address");
            let app = Router::new()
                .route("/jwks", get(handler))
                .with_state(state.clone());
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.expect("serve JWKS probe");
            });
            Self {
                state,
                address,
                task,
            }
        }

        fn endpoint(&self) -> String {
            format!("http://{}/jwks", self.address)
        }

        fn requests(&self) -> u64 {
            self.state.requests.load(Ordering::SeqCst)
        }

        async fn set_body(&self, body: Option<serde_json::Value>) {
            *self.state.body.write().await = body;
        }

        async fn gate_next_cohort(&self) -> std::sync::Arc<JwksProbeGate> {
            let gate = std::sync::Arc::new(JwksProbeGate {
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
                released: std::sync::atomic::AtomicBool::new(false),
            });
            *self.state.gate.lock().await = Some(gate.clone());
            gate
        }
    }

    impl Drop for JwksProbe {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn jwks_document(kid: &str, secret: &str) -> serde_json::Value {
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret.as_bytes());
        serde_json::json!({
            "keys": [
                {
                    "kty": "oct",
                    "kid": kid,
                    "alg": "HS256",
                    "k": encoded
                }
            ]
        })
    }

    // The public load API cannot pause between joining a flight and subscribing.
    // Join internally to force that scheduling window, but complete the owner
    // through its real HTTP/error/cancellation paths.
    async fn assert_jwks_late_waiter_completion(fail_fetch: bool, cancel_owner: bool) {
        let probe = JwksProbe::start(jwks_document("late-kid", "late-secret")).await;
        if fail_fetch {
            probe.set_body(None).await;
        }
        let gate = probe.gate_next_cohort().await;
        let cache = std::sync::Arc::new(JwksCache::new(
            probe.endpoint(),
            reqwest::Client::new(),
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        let owner = {
            let cache = cache.clone();
            tokio::spawn(async move { cache.load(false).await })
        };
        gate.entered.notified().await;
        let (waiter, became_owner) = cache.start_or_join_refresh();
        assert!(!became_owner);
        assert_eq!(waiter.result.receiver_count(), 0);

        let expected = if cancel_owner {
            owner.abort();
            assert!(
                owner
                    .await
                    .expect_err("owner must be cancelled")
                    .is_cancelled()
            );
            Err("JWKS refresh was cancelled".to_owned())
        } else {
            gate.released.store(true, Ordering::Release);
            gate.release.notify_waiters();
            owner.await.expect("owner must not panic")
        };
        assert_eq!(expected.is_err(), fail_fetch || cancel_owner);
        assert!(cache.lock_refresh_flight().is_none());
        // Subscribe only after completion and removal from the cache's active slot.
        // Repeat to prove the result remains available for every late joiner.
        for _ in 0..2 {
            let actual = tokio::time::timeout(Duration::from_secs(1), waiter.wait())
                .await
                .expect("joined request must receive completion without hanging");
            assert_eq!(
                serde_json::to_value(actual.map(|loaded| loaded.set)).unwrap(),
                serde_json::to_value(&expected).unwrap()
            );
        }
        assert_eq!(probe.requests(), 1);
    }

    #[tokio::test]
    async fn jwks_late_waiter_receives_success() {
        assert_jwks_late_waiter_completion(false, false).await;
    }

    #[tokio::test]
    async fn jwks_late_waiter_receives_fetch_error() {
        assert_jwks_late_waiter_completion(true, false).await;
    }

    #[tokio::test]
    async fn jwks_late_waiter_receives_cancellation() {
        assert_jwks_late_waiter_completion(false, true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn jwks_refreshes_are_singleflight_and_failed_forced_refreshes_are_cooldown_bound() {
        let old_secret = "old-jwks-secret";
        let new_secret = "new-jwks-secret";
        let probe = JwksProbe::start(jwks_document("old-kid", old_secret)).await;
        let client = reqwest::Client::new();
        let cache = std::sync::Arc::new(JwksCache::new(
            probe.endpoint(),
            client.clone(),
            Duration::from_millis(1),
            Duration::from_secs(60),
        ));

        let cold_gate = probe.gate_next_cohort().await;
        let cold_callers = 16;
        let mut cold_tasks = Vec::with_capacity(cold_callers);
        for _ in 0..cold_callers {
            let cache = cache.clone();
            cold_tasks.push(tokio::spawn(async move { cache.load(false).await }));
        }
        cold_gate.entered.notified().await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        cold_gate
            .released
            .store(true, std::sync::atomic::Ordering::Release);
        cold_gate.release.notify_waiters();
        for task in cold_tasks {
            task.await
                .expect("cold JWKS load must not panic")
                .expect("cold JWKS load must succeed");
        }
        assert_eq!(
            probe.requests(),
            1,
            "concurrent cold loads must share one JWKS fetch"
        );

        tokio::time::sleep(Duration::from_millis(5)).await;
        let expired_gate = probe.gate_next_cohort().await;
        let mut expired_tasks = Vec::with_capacity(cold_callers);
        for _ in 0..cold_callers {
            let cache = cache.clone();
            expired_tasks.push(tokio::spawn(async move { cache.load(false).await }));
        }
        expired_gate.entered.notified().await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        expired_gate
            .released
            .store(true, std::sync::atomic::Ordering::Release);
        expired_gate.release.notify_waiters();
        for task in expired_tasks {
            task.await
                .expect("expired JWKS load must not panic")
                .expect("expired JWKS load must succeed");
        }
        assert_eq!(
            probe.requests(),
            2,
            "concurrent expired loads must share one JWKS fetch"
        );

        tokio::time::sleep(Duration::from_millis(5)).await;
        probe.set_body(None).await;
        let first_failed_forced = cache
            .load(true)
            .await
            .expect("failed forced refresh should serve bounded stale data");
        let second_failed_forced = cache
            .load(true)
            .await
            .expect("cooldown-denied forced refresh should serve bounded stale data");
        assert_eq!(first_failed_forced.keys.len(), 1);
        assert_eq!(second_failed_forced.keys.len(), 1);
        assert_eq!(
            probe.requests(),
            3,
            "a failed forced refresh must reserve the cooldown"
        );

        probe
            .set_body(Some(jwks_document("old-kid", old_secret)))
            .await;
        let overlap_cache = std::sync::Arc::new(JwksCache::new(
            probe.endpoint(),
            client.clone(),
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        let overlap_gate = probe.gate_next_cohort().await;
        let ordinary_task = {
            let cache = overlap_cache.clone();
            tokio::spawn(async move { cache.load(false).await })
        };
        overlap_gate.entered.notified().await;
        let mut forced_load = std::pin::pin!(overlap_cache.load(true));
        std::future::poll_fn(|cx| {
            assert!(forced_load.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        overlap_gate
            .released
            .store(true, std::sync::atomic::Ordering::Release);
        overlap_gate.release.notify_waiters();
        ordinary_task
            .await
            .expect("ordinary overlap load must not panic")
            .expect("ordinary overlap load must succeed");
        forced_load
            .await
            .expect("forced overlap load must share the ordinary fetch");
        assert_eq!(
            probe.requests(),
            4,
            "a forced caller overlapping an ordinary refresh must not fetch again"
        );

        let rotated_cache = std::sync::Arc::new(JwksCache::new(
            probe.endpoint(),
            client,
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        rotated_cache
            .load(false)
            .await
            .expect("rotation test must warm its cache");
        let requests_before_rotation = probe.requests();
        probe
            .set_body(Some(jwks_document("new-kid", new_secret)))
            .await;
        let claims = JwtClaims {
            sub: "rotated-user".to_owned(),
            iss: Some("https://issuer.jazz.test".to_owned()),
            claims: std::collections::BTreeMap::new(),
            exp: Some(4_102_444_800),
            iat: None,
        };
        let token = make_jwt(&claims, new_secret, "new-kid");
        let config = test_expected_claims();
        let forced_gate = probe.gate_next_cohort().await;
        let mut validation_tasks = Vec::with_capacity(cold_callers);
        for _ in 0..cold_callers {
            let cache = rotated_cache.clone();
            let token = token.clone();
            let config = config.clone();
            validation_tasks.push(tokio::spawn(async move {
                validate_jwt_with_cache_at(&token, &cache, &config, 1_000_000).await
            }));
        }
        forced_gate.entered.notified().await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        forced_gate
            .released
            .store(true, std::sync::atomic::Ordering::Release);
        forced_gate.release.notify_waiters();
        for task in validation_tasks {
            task.await
                .expect("rotated JWT validation must not panic")
                .expect("all concurrent rotated JWT validations must succeed");
        }
        assert_eq!(
            probe.requests(),
            requests_before_rotation + 1,
            "concurrent forced refreshes must share one JWKS fetch"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn jwks_failed_forced_refresh_without_stale_is_cooldown_bound() {
        let secret = "failed-forced-secret";
        let probe = JwksProbe::start(jwks_document("failed-kid", secret)).await;
        probe.set_body(None).await;
        let cache = JwksCache::new(
            probe.endpoint(),
            reqwest::Client::new(),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );

        assert!(cache.load(true).await.is_err());
        assert_eq!(probe.requests(), 1);

        assert!(cache.load(true).await.is_err());
        assert_eq!(
            probe.requests(),
            1,
            "a failed forced refresh without stale data must retain cooldown"
        );

        probe
            .set_body(Some(jwks_document("failed-kid", secret)))
            .await;
        cache
            .load(false)
            .await
            .expect("ordinary retry should remain eligible after forced failure");
        assert_eq!(
            probe.requests(),
            2,
            "an eligible ordinary retry must be able to fetch"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn jwks_cancelled_owner_wakes_waiters_and_preserves_forced_cooldown() {
        let secret = "cancelled-owner-secret";
        let probe = JwksProbe::start(jwks_document("cancelled-kid", secret)).await;
        let gate = probe.gate_next_cohort().await;
        let cache = std::sync::Arc::new(JwksCache::new(
            probe.endpoint(),
            reqwest::Client::new(),
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));

        let owner = {
            let cache = cache.clone();
            tokio::spawn(async move { cache.load(true).await })
        };
        gate.entered.notified().await;
        let waiter = {
            let cache = cache.clone();
            tokio::spawn(async move { cache.load(true).await })
        };
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        owner.abort();
        assert!(
            owner
                .await
                .expect_err("cancelled owner must not complete")
                .is_cancelled(),
            "owner must be cancelled while the endpoint is gated"
        );

        let waiter_result = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("cancelled refresh must wake its waiter")
            .expect("waiter must not panic");
        assert!(
            waiter_result.is_err(),
            "cancelled refresh must deliver a local error to waiters"
        );
        assert_eq!(probe.requests(), 1);

        gate.released
            .store(true, std::sync::atomic::Ordering::Release);
        gate.release.notify_waiters();

        assert!(cache.load(true).await.is_err());
        assert_eq!(
            probe.requests(),
            1,
            "forced retry after cancellation must remain cooldown-bound"
        );

        cache
            .load(false)
            .await
            .expect("ordinary retry should progress after cancellation");
        assert_eq!(
            probe.requests(),
            2,
            "ordinary retry should be allowed to fetch after cancellation"
        );
    }

    fn make_jwt(claims: &JwtClaims, secret: &str, kid: &str) -> String {
        let key = EncodingKey::from_secret(secret.as_bytes());
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(kid.to_string());
        let mut claims = serde_json::to_value(claims).unwrap();
        let object = claims.as_object_mut().unwrap();
        if object.get("exp").is_none_or(serde_json::Value::is_null) {
            object.insert("exp".to_owned(), serde_json::json!(4_102_444_800_u64));
        }
        object.insert("aud".to_owned(), serde_json::json!("jazz-audience"));
        encode(&header, &claims, &key).unwrap()
    }

    fn flat_claims(
        value: serde_json::Value,
    ) -> std::collections::BTreeMap<String, serde_json::Value> {
        value
            .as_object()
            .expect("test claims are an object")
            .clone()
            .into_iter()
            .collect()
    }

    #[test]
    fn test_jwt_validation_valid() {
        let jwks = make_hs256_jwks(TEST_JWKS_KID, TEST_JWKS_SECRET);
        let claims = JwtClaims {
            sub: "user-123".to_string(),
            iss: Some("https://issuer.jazz.test".to_owned()),
            claims: flat_claims(serde_json::json!({"role": "admin"})),
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
            claims: flat_claims(serde_json::json!({})),
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
            claims: flat_claims(serde_json::json!({})),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, "any-secret", TEST_JWKS_KID);

        let result = verify_jwt_signature_with_jwks(&token, &jwks);
        assert!(matches!(result, Err(JwtVerificationError::Retryable(_))));
    }

    #[test]
    fn test_decode_session_header() {
        let session = Session::new("urn:jazz:test", "user-456")
            .with_claims(serde_json::json!({"teams": ["eng"]}));
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

        let session = Session::new("urn:jazz:test", "impersonated-user");
        let session_json = serde_json::to_string(&session).unwrap();
        let session_b64 = base64::engine::general_purpose::STANDARD.encode(&session_json);

        headers.insert(
            "X-Jazz-Backend-Secret",
            "backend-secret-12345".parse().unwrap(),
        );
        headers.insert("X-Jazz-Session", session_b64.parse().unwrap());

        let result = extract_session(&headers, test_app_id(), &config)
            .await
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().user_id, "impersonated-user");
    }

    #[tokio::test]
    async fn test_extract_session_backend_wrong_secret() {
        let config = make_test_config();
        let mut headers = HeaderMap::new();

        let session = Session::new("urn:jazz:test", "user");
        let session_json = serde_json::to_string(&session).unwrap();
        let session_b64 = base64::engine::general_purpose::STANDARD.encode(&session_json);

        headers.insert("X-Jazz-Backend-Secret", "wrong-secret".parse().unwrap());
        headers.insert("X-Jazz-Session", session_b64.parse().unwrap());

        let result = extract_session(&headers, test_app_id(), &config).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, UnauthenticatedCode::Invalid);
    }

    #[tokio::test]
    async fn test_extract_session_backend_takes_priority() {
        let config = make_test_config();
        let mut headers = HeaderMap::new();

        // Add both backend and JWT auth - backend should win
        let session = Session::new("https://issuer.jazz.test", "backend-user");
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
            claims: flat_claims(serde_json::json!({})),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, TEST_JWKS_SECRET, TEST_JWKS_KID);
        headers.insert(AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());

        let result = extract_session(&headers, test_app_id(), &config)
            .await
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().user_id, "backend-user"); // Backend wins
    }

    #[tokio::test]
    async fn test_extract_session_no_auth() {
        let config = make_test_config();
        let headers = HeaderMap::new();

        let result = extract_session(&headers, test_app_id(), &config)
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
    async fn local_first_session_excludes_verified_public_key_from_policy_claims() {
        let app_id = AppId::from_name("test-app");
        let seed = [7u8; 32];
        let token = jazz::tools::identity::mint_jazz_self_signed_token(
            &seed,
            jazz::tools::identity::LOCAL_FIRST_ISSUER,
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

        let session = extract_session(&headers, app_id, &config)
            .await
            .unwrap()
            .expect("session");

        assert_eq!(session.auth_mode, jazz::tools::AuthMode::LocalFirst);
        if let serde_json::Value::Object(map) = &session.claims {
            assert!(
                !map.contains_key("auth_mode"),
                "claims must not carry auth_mode anymore"
            );
            assert!(
                !map.contains_key("jazz_pub_key"),
                "verified transport proof material must not become a policy claim"
            );
        } else {
            panic!("expected object claims");
        }
    }

    fn make_local_first_auth_config() -> AuthConfig {
        AuthConfig {
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
        let session = extract_session(&headers, app_id, &config)
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
        let result = extract_session(&headers, test_app_id(), &config).await;
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
        let result = extract_session(&headers, app_id, &config).await;
        assert!(result.is_err());
    }

    #[cfg(feature = "test")]
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

        let session = extract_session(&headers, app_id, &config)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            session.user_id,
            identity::derive_user_id(&alice_seed()).to_string()
        );

        clock.advance(Duration::from_secs(6));

        let result = extract_session(&headers, app_id, &config).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, UnauthenticatedCode::Expired);
    }

    #[tokio::test]
    async fn non_local_first_auth_iss_does_not_use_local_first_auth_path() {
        let config = make_local_first_auth_config();
        let claims = JwtClaims {
            sub: "user-123".to_string(),
            iss: Some("https://auth.example.com".to_string()),
            claims: flat_claims(serde_json::json!({})),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, TEST_JWKS_SECRET, TEST_JWKS_KID);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        // A core verifies no external JWTs.
        let result = extract_session(&headers, test_app_id(), &config).await;
        assert!(result.is_err());
    }

    #[cfg(feature = "test")]
    #[tokio::test]
    async fn anonymous_session_has_auth_mode_anonymous() {
        let app_id = AppId::from_name("test-app");
        let seed = [9u8; 32];
        let clock = TestClock::new(1_000_000);
        let token = jazz::tools::identity::mint_jazz_self_signed_token_at(
            &seed,
            jazz::tools::identity::ANONYMOUS_ISSUER,
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

        let session = extract_session(&headers, app_id, &config)
            .await
            .unwrap()
            .expect("session");

        assert_eq!(session.auth_mode, jazz::tools::AuthMode::Anonymous);
        assert_eq!(session.claims, serde_json::json!({}));
    }
}
