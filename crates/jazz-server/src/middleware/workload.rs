//! Trusted workloads: a backend proves who it is with its platform's OIDC
//! token (for example a Vercel function's `VERCEL_OIDC_TOKEN`) instead of a
//! shared secret.
//!
//! When a server trusts a workload it admits nothing else: every request and
//! every WebSocket session must carry a token whose issuer, audience and
//! subject match. A verified token stands in for both the admin and the
//! backend secret, which the server then only holds internally (random,
//! per process) so the existing secret checks apply unchanged.

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
    AuthConfig, JWKS_CACHE_TTL, JWKS_MAX_STALE, JwksCache, validate_jwt_with_cache_at,
};
use crate::server::ServerState;

/// Header carrying a workload token on requests that carry no secret header.
pub const WORKLOAD_TOKEN_HEADER: &str = "X-Jazz-Workload-Token";
pub const ADMIN_SECRET_HEADER: &str = "X-Jazz-Admin-Secret";
pub const BACKEND_SECRET_HEADER: &str = "X-Jazz-Backend-Secret";
/// Set by the gate (and stripped from what clients send) on a WebSocket
/// upgrade its workload vouched for; its value is the internal backend secret.
pub(crate) const VOUCHED_HEADER: &str = "X-Jazz-Internal-Workload-Vouched";

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
    claims: AuthConfig,
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
        let claims = AuthConfig {
            jwt_issuer: Some(workload.issuer.clone()),
            jwt_audience: Some(workload.audience.clone()),
            ..AuthConfig::default()
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
        let jwks = self.jwks().await?;
        let verified = validate_jwt_with_cache_at(token, jwks, &self.claims, now_seconds)
            .await
            .map_err(|error| format!("workload token rejected: {error}"))?;
        if verified.subject != self.workload.subject {
            return Err("workload token subject is not the trusted workload".to_owned());
        }
        Ok(())
    }
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

/// Replace workload tokens offered as secrets with the internal secrets.
///
/// Every value in `offered` must verify. At least one token must be offered
/// overall (`also` is a token offered on its own).
pub async fn exchange(
    verifier: &WorkloadVerifier,
    now_seconds: u64,
    admin: &mut Option<String>,
    backend: &mut Option<String>,
    also: Option<&str>,
    secrets: &InternalSecrets,
) -> Result<(), String> {
    let mut verified = false;
    if let Some(token) = also {
        verifier.verify(token, now_seconds).await?;
        verified = true;
    }
    if let Some(token) = admin.as_deref() {
        verifier.verify(token, now_seconds).await?;
        *admin = Some(secrets.admin.clone());
        verified = true;
    }
    if let Some(token) = backend.as_deref() {
        verifier.verify(token, now_seconds).await?;
        *backend = Some(secrets.backend.clone());
        verified = true;
    }
    if !verified {
        return Err(
            "this server admits only its trusted workload; a workload token is required".to_owned(),
        );
    }
    Ok(())
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// Whether the gate vouched for this WebSocket upgrade.
pub(crate) fn vouched(headers: &HeaderMap, secrets: &InternalSecrets) -> bool {
    headers
        .get(VOUCHED_HEADER)
        .is_some_and(|value| value.as_bytes() == secrets.backend.as_bytes())
}

/// The gate for servers that trust a workload: every request (WebSocket
/// upgrades included) must carry the workload's token. Prelude secrets on a
/// vouched WebSocket are honoured in `ws_admission`.
pub async fn workload_gate(
    State(state): State<Arc<ServerState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let (Some(verifier), Some(secrets)) = (
        state.workload_verifier.as_deref(),
        state.workload_secrets.as_ref(),
    ) else {
        return next.run(request).await;
    };
    let headers = request.headers_mut();
    headers.remove(VOUCHED_HEADER);
    let mut admin = header(headers, ADMIN_SECRET_HEADER);
    let mut backend = header(headers, BACKEND_SECRET_HEADER);
    let token = header(headers, WORKLOAD_TOKEN_HEADER);
    let now = state.auth_config.clock.now_seconds();
    if let Err(error) = exchange(
        verifier,
        now,
        &mut admin,
        &mut backend,
        token.as_deref(),
        secrets,
    )
    .await
    {
        warn!(%error, "refused a request without a valid workload token");
        return (StatusCode::UNAUTHORIZED, error).into_response();
    }
    headers.remove(WORKLOAD_TOKEN_HEADER);
    if let Ok(value) = HeaderValue::from_str(&secrets.backend) {
        headers.insert(VOUCHED_HEADER, value);
    }
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
    async fn exchange_swaps_offered_tokens_for_internal_secrets() {
        let v = verifier();
        let secrets = InternalSecrets::generate();
        let good = token(|_| {});

        let (mut admin, mut backend) = (None, Some(good.clone()));
        exchange(&v, NOW, &mut admin, &mut backend, None, &secrets)
            .await
            .unwrap();
        assert_eq!(admin, None);
        assert_eq!(backend.as_deref(), Some(secrets.backend.as_str()));

        let (mut admin, mut backend) = (Some(good.clone()), None);
        exchange(&v, NOW, &mut admin, &mut backend, None, &secrets)
            .await
            .unwrap();
        assert_eq!(admin.as_deref(), Some(secrets.admin.as_str()));

        // A token on its own admits, but grants no secret.
        let (mut admin, mut backend) = (None, None);
        exchange(&v, NOW, &mut admin, &mut backend, Some(&good), &secrets)
            .await
            .unwrap();
        assert_eq!((admin, backend), (None, None));
    }

    #[tokio::test]
    async fn exchange_refuses_missing_tokens_and_plain_secrets() {
        let v = verifier();
        let secrets = InternalSecrets::generate();
        let (mut admin, mut backend) = (None, None);
        assert!(
            exchange(&v, NOW, &mut admin, &mut backend, None, &secrets)
                .await
                .is_err()
        );

        // Even the server's own internal secret is not accepted from outside.
        let (mut admin, mut backend) = (None, Some(secrets.backend.clone()));
        assert!(
            exchange(&v, NOW, &mut admin, &mut backend, None, &secrets)
                .await
                .is_err()
        );
    }
}
