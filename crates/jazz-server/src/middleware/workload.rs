//! Backend credentials: a backend presents one credential in
//! `X-Jazz-Backend-Credential` and states the access it wants (`backend` or
//! `admin`) in `X-Jazz-Backend-Access` next to it. The server then hands the
//! request the matching admin or backend secret, so the existing secret
//! checks apply unchanged. Secrets a request or prelude offers itself are
//! ignored once a credential verifies.
//!
//! Two kinds of credential:
//!
//! - A trusted workload's OIDC token (for example a Vercel function's
//!   `VERCEL_OIDC_TOKEN`). Such a server admits nothing else: every request
//!   and every WebSocket session must carry a token whose issuer, audience
//!   and subject match, and it holds its secrets only internally (random, per
//!   process).
//! - The admin secret itself, on a server configured with one: a backend
//!   (`remote()` in jazz-tools) then needs only that one secret. Requests
//!   without a credential are admitted as before.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tokio::sync::OnceCell;
use tracing::warn;

use super::auth::{
    ExpectedJwtClaims, JWKS_CACHE_TTL, JWKS_MAX_STALE, JwksCache, validate_jwt_with_cache_at,
};
use crate::server::ServerState;

/// The backend's credential, on every request and WebSocket upgrade it sends
/// to the core: separate from (and below) the user's own credentials.
pub const WORKLOAD_TOKEN_HEADER: &str = "X-Jazz-Backend-Credential";
pub const ADMIN_SECRET_HEADER: &str = "X-Jazz-Admin-Secret";
pub const BACKEND_SECRET_HEADER: &str = "X-Jazz-Backend-Secret";
/// Set by the gate (and stripped from what clients send) on a WebSocket
/// upgrade its workload vouched for; its value names the stated access,
/// keyed with the internal backend secret.
pub(crate) const VOUCHED_HEADER: &str = "X-Jazz-Internal-Workload-Vouched";
/// The access (`backend` or `admin`) a backend states next to its credential.
pub const ACCESS_HEADER: &str = "X-Jazz-Backend-Access";

/// The one workload this server trusts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedWorkload {
    /// OIDC issuer; its signing keys are found through OIDC discovery.
    pub issuer: String,
    pub audience: String,
    /// The exact `sub` a token must carry.
    pub subject: String,
}

/// Verifies workload tokens against the trusted workload's issuer.
pub struct WorkloadVerifier {
    workload: TrustedWorkload,
    claims: ExpectedJwtClaims,
    http_client: reqwest::Client,
    jwks: OnceCell<JwksCache>,
}

impl WorkloadVerifier {
    pub fn new(workload: TrustedWorkload) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Self::with_client(workload, http_client)
    }

    pub fn with_client(workload: TrustedWorkload, http_client: reqwest::Client) -> Self {
        let claims = ExpectedJwtClaims {
            issuer: workload.issuer.clone(),
            audience: workload.audience.clone(),
        };
        Self {
            workload,
            claims,
            http_client,
            jwks: OnceCell::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_static_jwks(
        workload: TrustedWorkload,
        jwks: jsonwebtoken::jwk::JwkSet,
    ) -> Self {
        let verifier = Self::new(workload);
        let _ = verifier.jwks.set(JwksCache::from_static(jwks));
        verifier
    }

    async fn jwks(&self) -> Result<&JwksCache, String> {
        self.jwks
            .get_or_try_init(|| async {
                let issuer = self.workload.issuer.trim_end_matches('/');
                let loopback = ["http://127.0.0.1", "http://localhost", "http://[::1]"]
                    .iter()
                    .any(|prefix| {
                        issuer
                            .strip_prefix(prefix)
                            .is_some_and(|rest| rest.is_empty() || rest.starts_with([':', '/']))
                    });
                if !issuer.starts_with("https://") && !loopback {
                    return Err(format!(
                        "trusted workload issuer {issuer} must use https (http only on loopback)"
                    ));
                }
                let discovery = format!(
                    "{}/.well-known/openid-configuration",
                    self.workload.issuer.trim_end_matches('/')
                );
                let document: serde_json::Value = self
                    .http_client
                    .get(&discovery)
                    .send()
                    .await
                    .and_then(|response| response.error_for_status())
                    .map_err(|error| format!("OIDC discovery at {discovery} failed: {error}"))?
                    .json()
                    .await
                    .map_err(|error| {
                        format!("OIDC discovery at {discovery} is not JSON: {error}")
                    })?;
                let discovered_issuer = document
                    .get("issuer")
                    .and_then(|value| value.as_str())
                    .map(|value| value.trim_end_matches('/'));
                if discovered_issuer != Some(issuer) {
                    return Err(format!(
                        "OIDC discovery at {discovery} names issuer {discovered_issuer:?}, not {issuer}"
                    ));
                }
                let jwks_uri = document
                    .get("jwks_uri")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| format!("OIDC discovery at {discovery} has no jwks_uri"))?;
                Ok(JwksCache::new(
                    jwks_uri.to_owned(),
                    self.http_client.clone(),
                    JWKS_CACHE_TTL,
                    JWKS_MAX_STALE,
                ))
            })
            .await
    }

    /// Whether `token` is a current token of the trusted workload.
    pub async fn verify(&self, token: &str, now_seconds: u64) -> Result<(), String> {
        // Junk never reaches the network: only a well-formed token triggers
        // discovery.
        jsonwebtoken::decode_header(token)
            .map_err(|error| format!("workload token rejected: {error}"))?;
        let jwks = self.jwks().await?;
        let verified = validate_jwt_with_cache_at(token, jwks, &self.claims, now_seconds)
            .await
            .map_err(|error| format!("workload token rejected: {error}"))?;
        if verified.subject != self.workload.subject {
            return Err("workload token subject is not the trusted workload".to_owned());
        }
        keep_workload_token(token, verified.exp);
        Ok(())
    }
}

/// A verified workload token and when it expires (Unix seconds).
struct WorkloadToken {
    token: String,
    exp: Option<u64>,
}

/// The trusted workload's latest verified token in this process: on
/// Vercel, the deploying function's OIDC token, which is also what its AWS
/// role trusts ([`crate::s3::WebIdentityToken::Workload`]).
static LATEST_WORKLOAD_TOKEN: std::sync::Mutex<Option<WorkloadToken>> = std::sync::Mutex::new(None);

/// Bumped each time a different token is kept: work that failed on a stale
/// token (a blob store's backed-off batch) may go again at once.
static WORKLOAD_TOKEN_GENERATION: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default()
}

/// Keep `token` unless the one kept lasts longer: requests carrying an
/// older token than one already seen do not roll it back.
fn keep_workload_token(token: &str, exp: Option<u64>) {
    let Ok(mut latest) = LATEST_WORKLOAD_TOKEN.lock() else {
        return;
    };
    if !replaces(latest.as_ref(), token, exp) {
        return;
    }
    *latest = Some(WorkloadToken {
        token: token.to_owned(),
        exp,
    });
    WORKLOAD_TOKEN_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// The trusted workload's latest verified token, if one arrived yet and it
/// has not expired.
pub fn latest_workload_token() -> Option<String> {
    current_workload_token().ok()
}

/// [`latest_workload_token`], or why there is none: none arrived yet, or
/// the last one expired (the next request from the workload renews it).
pub fn current_workload_token() -> Result<String, String> {
    let latest = LATEST_WORKLOAD_TOKEN
        .lock()
        .map_err(|_| "no trusted workload token: its lock is poisoned".to_owned())?;
    usable(latest.as_ref(), unix_now())
}

/// Whether a verified `token` expiring at `exp` replaces `kept`: a
/// different token that lasts at least as long.
fn replaces(kept: Option<&WorkloadToken>, token: &str, exp: Option<u64>) -> bool {
    kept.is_none_or(|kept| {
        kept.token != token && kept.exp.unwrap_or(u64::MAX) <= exp.unwrap_or(u64::MAX)
    })
}

/// How long before its `exp` a token counts as expired: clocks here and at
/// STS may disagree, and a token handed out must outlast the request.
const EXPIRY_MARGIN_SECONDS: u64 = 60;

/// `kept`'s token, unless there is none or it expires within
/// [`EXPIRY_MARGIN_SECONDS`] of `now`.
fn usable(kept: Option<&WorkloadToken>, now: u64) -> Result<String, String> {
    let Some(kept) = kept else {
        return Err("no trusted workload token arrived yet".to_owned());
    };
    if kept
        .exp
        .is_some_and(|exp| exp <= now.saturating_add(EXPIRY_MARGIN_SECONDS))
    {
        return Err(
            "the trusted workload's last token expired; the next request from the workload \
             renews it"
                .to_owned(),
        );
    }
    Ok(kept.token.clone())
}

/// How many different workload tokens this process has kept: a change
/// means a fresh token arrived.
pub fn workload_token_generation() -> u64 {
    WORKLOAD_TOKEN_GENERATION.load(std::sync::atomic::Ordering::Relaxed)
}

/// How a server checks the backend credential.
pub enum BackendCredential {
    /// The trusted workload's OIDC token; nothing else is admitted.
    Workload(Box<WorkloadVerifier>),
    /// The server's admin secret; requests without a credential pass as before.
    AdminSecret(String),
}

impl BackendCredential {
    /// Whether every request and socket must carry the credential.
    pub fn required(&self) -> bool {
        matches!(self, Self::Workload(_))
    }

    async fn verify(&self, credential: &str, now_seconds: u64) -> Result<(), String> {
        match self {
            Self::Workload(verifier) => verifier.verify(credential, now_seconds).await,
            Self::AdminSecret(secret) if constant_time_eq(credential, secret) => Ok(()),
            Self::AdminSecret(_) => Err("backend credential rejected".to_owned()),
        }
    }
}

pub(crate) fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The secrets a verified workload stands in for, held only by this process.
#[derive(Clone)]
pub struct InternalSecrets {
    pub admin: String,
    pub backend: String,
}

impl InternalSecrets {
    pub fn generate() -> Self {
        let random = || {
            format!(
                "{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            )
        };
        Self {
            admin: random(),
            backend: random(),
        }
    }
}

/// The access a backend states for one request or socket, in
/// [`ACCESS_HEADER`] next to its credential. Without the header a request
/// gets no secret at all: it is a plain user or guest session the backend
/// relays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendAccess {
    Session,
    Backend,
    Admin,
}

impl BackendAccess {
    pub fn parse(value: Option<&str>) -> Result<Self, String> {
        match value {
            None => Ok(Self::Session),
            Some("backend") => Ok(Self::Backend),
            Some("admin") => Ok(Self::Admin),
            Some(other) => Err(format!(
                "{ACCESS_HEADER} must be \"backend\" or \"admin\", not {other:?}"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Backend => "backend",
            Self::Admin => "admin",
        }
    }

    /// The internal secrets this access grants: admin implies backend.
    pub fn secrets(self, secrets: &InternalSecrets) -> (Option<String>, Option<String>) {
        match self {
            Self::Session => (None, None),
            Self::Backend => (None, Some(secrets.backend.clone())),
            Self::Admin => (Some(secrets.admin.clone()), Some(secrets.backend.clone())),
        }
    }
}

/// Verify the backend credential and read the access it states. Secrets a
/// request offers itself never count: a backend that forwards a client's
/// headers can't hand that client more than the access it states.
pub async fn admit(
    check: &BackendCredential,
    now_seconds: u64,
    credential: Option<&str>,
    access: Option<&str>,
) -> Result<BackendAccess, String> {
    let Some(token) = credential else {
        return Err(format!(
            "this server admits only its trusted workload; send its token in {WORKLOAD_TOKEN_HEADER}"
        ));
    };
    check.verify(token, now_seconds).await?;
    BackendAccess::parse(access)
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

pub(crate) fn vouched_value(secrets: &InternalSecrets, access: BackendAccess) -> String {
    format!("{} {}", secrets.backend, access.as_str())
}

/// The access the gate vouched for on this WebSocket upgrade, if it did.
pub(crate) fn vouched(headers: &HeaderMap, secrets: &InternalSecrets) -> Option<BackendAccess> {
    let value = headers.get(VOUCHED_HEADER)?.as_bytes();
    [
        BackendAccess::Session,
        BackendAccess::Backend,
        BackendAccess::Admin,
    ]
    .into_iter()
    .find(|access| value == vouched_value(secrets, *access).as_bytes())
}

/// The backend credential gate. A request (WebSocket upgrades included) that
/// carries a credential must verify, and its secret headers are replaced by
/// the ones its stated access grants; on a WebSocket, `ws_admission` does the
/// same for the prelude. A server that trusts a workload refuses requests
/// without one.
pub async fn workload_gate(
    State(state): State<Arc<ServerState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let (Some(check), Some(secrets)) = (
        state.backend_credential.as_deref(),
        state.workload_secrets.as_ref(),
    ) else {
        return next.run(request).await;
    };
    let headers = request.headers_mut();
    headers.remove(VOUCHED_HEADER);
    if !check.required() && !headers.contains_key(WORKLOAD_TOKEN_HEADER) {
        headers.remove(ACCESS_HEADER);
        return next.run(request).await;
    }
    for name in [ADMIN_SECRET_HEADER, BACKEND_SECRET_HEADER] {
        headers.remove(name);
    }
    let now = state.auth_config.clock.now_seconds();
    let access = match admit(
        check,
        now,
        header(headers, WORKLOAD_TOKEN_HEADER).as_deref(),
        header(headers, ACCESS_HEADER).as_deref(),
    )
    .await
    {
        Ok(access) => access,
        Err(error) => {
            warn!(%error, "refused a request without a valid backend credential");
            return (StatusCode::UNAUTHORIZED, error).into_response();
        }
    };
    headers.remove(WORKLOAD_TOKEN_HEADER);
    headers.remove(ACCESS_HEADER);
    if let Ok(value) = HeaderValue::from_str(&vouched_value(secrets, access)) {
        headers.insert(VOUCHED_HEADER, value);
    }
    let (admin, backend) = access.secrets(secrets);
    for (name, value) in [
        (ADMIN_SECRET_HEADER, admin),
        (BACKEND_SECRET_HEADER, backend),
    ] {
        if let Some(value) = value.and_then(|value| HeaderValue::from_str(&value).ok()) {
            headers.insert(name, value);
        }
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use jsonwebtoken::{EncodingKey, Header, encode};

    /// A token is kept only over one that lasts no longer, and an expired
    /// one is not handed out: the error says it expired.
    #[test]
    fn the_kept_token_is_the_freshest_and_only_while_current() {
        let kept = |token: &str, exp| WorkloadToken {
            token: token.to_owned(),
            exp,
        };
        assert!(replaces(None, "a", Some(10)));
        assert!(replaces(Some(&kept("a", Some(10))), "b", Some(20)));
        assert!(
            !replaces(Some(&kept("b", Some(20))), "a", Some(10)),
            "no rollback"
        );
        assert!(
            !replaces(Some(&kept("a", Some(10))), "a", Some(10)),
            "the same token"
        );
        assert_eq!(usable(Some(&kept("a", Some(100))), 39).unwrap(), "a");
        let expired = usable(Some(&kept("a", Some(100))), 100).unwrap_err();
        assert!(expired.contains("expired"), "{expired}");
        // Within the margin of its exp, a token is not handed out: STS
        // might see it expired by the time it arrives.
        assert!(
            usable(Some(&kept("a", Some(100))), 40).is_err(),
            "about to expire"
        );
        assert_eq!(
            usable(Some(&kept("a", None)), u64::MAX).unwrap(),
            "a",
            "no exp"
        );
        assert!(usable(None, 0).unwrap_err().contains("arrived yet"));
    }

    const SECRET: &str = "workload-test-signing-key-0123456789";
    const NOW: u64 = 1_800_000_000;

    fn workload() -> TrustedWorkload {
        TrustedWorkload {
            issuer: "https://oidc.vercel.com/garden-co".to_owned(),
            audience: "https://vercel.com/garden-co".to_owned(),
            subject: "owner:garden-co:project:app:environment:production".to_owned(),
        }
    }

    fn verifier() -> WorkloadVerifier {
        let k = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(SECRET);
        let jwks = serde_json::from_value(serde_json::json!({
            "keys": [{ "kty": "oct", "kid": "k1", "alg": "HS256", "k": k }]
        }))
        .unwrap();
        WorkloadVerifier::with_static_jwks(workload(), jwks)
    }

    fn token(edit: impl FnOnce(&mut serde_json::Value)) -> String {
        let w = workload();
        let mut claims = serde_json::json!({
            "iss": w.issuer, "aud": w.audience, "sub": w.subject, "exp": NOW + 600,
        });
        edit(&mut claims);
        let mut header = Header::new(jsonwebtoken::Algorithm::HS256);
        header.kid = Some("k1".to_owned());
        encode(
            &header,
            &claims,
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn accepts_only_the_trusted_workload() {
        let v = verifier();
        assert!(v.verify(&token(|_| {}), NOW).await.is_ok());
        for (field, value) in [
            (
                "sub",
                serde_json::json!("owner:garden-co:project:app:environment:preview"),
            ),
            (
                "iss",
                serde_json::json!("https://oidc.vercel.com/someone-else"),
            ),
            ("aud", serde_json::json!("https://vercel.com/someone-else")),
            ("exp", serde_json::json!(NOW - 1)),
        ] {
            let bad = token(|claims| claims[field] = value.clone());
            assert!(
                v.verify(&bad, NOW).await.is_err(),
                "{field} mismatch must be refused"
            );
        }
        assert!(v.verify("not-a-jwt", NOW).await.is_err());
        let mut forged = token(|_| {});
        forged.push('x');
        assert!(v.verify(&forged, NOW).await.is_err());
    }

    #[tokio::test]
    async fn refuses_tokens_that_are_not_yet_valid_unexpiring_or_signed_otherwise() {
        let v = verifier();
        assert!(
            v.verify(&token(|c| c["nbf"] = (NOW + 60).into()), NOW)
                .await
                .is_err()
        );
        let no_exp = token(|c| {
            c.as_object_mut().unwrap().remove("exp");
        });
        assert!(v.verify(&no_exp, NOW).await.is_err());
        let multi_aud =
            token(|c| c["aud"] = serde_json::json!(["https://vercel.com/garden-co", "other"]));
        assert!(v.verify(&multi_aud, NOW).await.is_ok());

        let claims = serde_json::json!({
            "iss": workload().issuer, "aud": workload().audience,
            "sub": workload().subject, "exp": NOW + 600,
        });
        let key = EncodingKey::from_secret(SECRET.as_bytes());
        let mut wrong_kid = Header::new(jsonwebtoken::Algorithm::HS256);
        wrong_kid.kid = Some("k2".to_owned());
        assert!(
            v.verify(&encode(&wrong_kid, &claims, &key).unwrap(), NOW)
                .await
                .is_err()
        );
        let mut wrong_alg = Header::new(jsonwebtoken::Algorithm::HS512);
        wrong_alg.kid = Some("k1".to_owned());
        assert!(
            v.verify(&encode(&wrong_alg, &claims, &key).unwrap(), NOW)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn admit_grants_only_the_stated_access() {
        let v = BackendCredential::Workload(Box::new(verifier()));
        let secrets = InternalSecrets::generate();
        let good = token(|_| {});

        assert_eq!(
            admit(&v, NOW, Some(&good), None).await,
            Ok(BackendAccess::Session)
        );
        assert_eq!(
            admit(&v, NOW, Some(&good), Some("backend")).await,
            Ok(BackendAccess::Backend)
        );
        assert_eq!(
            admit(&v, NOW, Some(&good), Some("admin")).await,
            Ok(BackendAccess::Admin)
        );
        assert!(admit(&v, NOW, Some(&good), Some("root")).await.is_err());

        assert_eq!(BackendAccess::Session.secrets(&secrets), (None, None));
        assert_eq!(
            BackendAccess::Backend.secrets(&secrets),
            (None, Some(secrets.backend.clone()))
        );
        assert_eq!(
            BackendAccess::Admin.secrets(&secrets),
            (Some(secrets.admin.clone()), Some(secrets.backend.clone()))
        );
    }

    #[tokio::test]
    async fn admit_refuses_missing_and_bad_credentials() {
        let v = BackendCredential::Workload(Box::new(verifier()));
        let secrets = InternalSecrets::generate();
        assert!(admit(&v, NOW, None, Some("admin")).await.is_err());
        // Not even the server's own internal secret stands in for a token.
        assert!(
            admit(&v, NOW, Some(&secrets.backend), Some("admin"))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn the_admin_secret_is_a_credential_too() {
        let check = BackendCredential::AdminSecret("admin-secret-0123".to_owned());
        assert!(!check.required());
        assert_eq!(
            admit(&check, NOW, Some("admin-secret-0123"), Some("admin")).await,
            Ok(BackendAccess::Admin)
        );
        assert_eq!(
            admit(&check, NOW, Some("admin-secret-0123"), None).await,
            Ok(BackendAccess::Session)
        );
        for wrong in ["admin-secret-0124", "admin-secret-012", ""] {
            assert!(
                admit(&check, NOW, Some(wrong), Some("backend"))
                    .await
                    .is_err()
            );
        }
        assert!(BackendCredential::Workload(Box::new(verifier())).required());
    }

    #[tokio::test]
    async fn refuses_plain_http_issuers_off_loopback() {
        let mut w = workload();
        w.issuer = "http://oidc.example.com".to_owned();
        let v = WorkloadVerifier::new(w);
        let error = v.verify(&token(|_| {}), NOW).await.unwrap_err();
        assert!(error.contains("https"), "{error}");
    }
}
