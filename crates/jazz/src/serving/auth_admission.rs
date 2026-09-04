//! Auth/session admission types for server transports.
//!
//! This slice supports deterministic subject-to-author binding, a static bearer
//! gate, and a deliberately small static JWT verifier.

use std::collections::BTreeMap;
use std::fmt;

use crate::groove::records::Value;
use crate::ids::{AuthorSubject, AuthorSubjectError};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};

/// Admission policy used by loopback transports.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthAdmissionConfig {
    /// Static bearer token accepted by this process-local admission gate.
    pub static_bearer_token: Option<String>,
    /// Static JWT verifier accepted by this process-local admission gate.
    pub jwt_verifier: Option<JwtVerifierConfig>,
    /// Issuer that external JWTs must contain.
    pub expected_issuer: Option<String>,
    /// Whether local-first JWT auth is allowed by configuration.
    pub allow_local_first_auth: bool,
    /// App id/audience that external and local-first JWTs must target.
    pub expected_audience: Option<String>,
    /// Fallback subject used when no explicit auth is required.
    pub anonymous_subject: String,
}

impl Default for AuthAdmissionConfig {
    fn default() -> Self {
        Self {
            static_bearer_token: None,
            jwt_verifier: None,
            expected_issuer: None,
            allow_local_first_auth: false,
            expected_audience: None,
            anonymous_subject: "anonymous".to_owned(),
        }
    }
}

impl AuthAdmissionConfig {
    /// Require a static bearer token.
    pub fn static_bearer(token: impl Into<String>) -> Self {
        Self {
            static_bearer_token: Some(token.into()),
            jwt_verifier: None,
            expected_issuer: None,
            allow_local_first_auth: false,
            expected_audience: None,
            anonymous_subject: "anonymous".to_owned(),
        }
    }

    /// Require a signed JWT.
    pub fn jwt(verifier: JwtVerifierConfig) -> Self {
        Self {
            static_bearer_token: None,
            jwt_verifier: Some(verifier),
            expected_issuer: None,
            allow_local_first_auth: false,
            expected_audience: None,
            anonymous_subject: "anonymous".to_owned(),
        }
    }

    /// Bind external JWT admission to a configured issuer.
    pub fn with_expected_issuer(mut self, issuer: impl Into<String>) -> Self {
        self.expected_issuer = Some(issuer.into());
        self
    }

    /// Bind JWT admission to a configured app id/audience.
    pub fn with_expected_audience(mut self, audience: impl Into<String>) -> Self {
        self.expected_audience = Some(audience.into());
        self
    }

    /// Whether this config requires a bearer credential.
    pub fn requires_bearer(&self) -> bool {
        self.static_bearer_token.is_some() || self.jwt_verifier.is_some()
    }
}

/// Static JWT verification config.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JwtVerifierConfig {
    /// Expected JWT signing algorithm.
    pub algorithm: Algorithm,
    /// Verification key material for the configured algorithm.
    pub key: JwtVerificationKey,
}

impl JwtVerifierConfig {
    /// Verify HMAC-signed tokens. Intended for tests and tightly scoped local deployments.
    pub fn hmac_secret(algorithm: Algorithm, secret: impl Into<Vec<u8>>) -> Self {
        Self {
            algorithm,
            key: JwtVerificationKey::HmacSecret(secret.into()),
        }
    }

    /// Verify RSA-signed tokens with a PEM-encoded public key.
    pub fn rsa_public_key_pem(algorithm: Algorithm, public_key_pem: impl Into<Vec<u8>>) -> Self {
        Self {
            algorithm,
            key: JwtVerificationKey::RsaPublicKeyPem(public_key_pem.into()),
        }
    }

    /// Verify EdDSA/Ed25519-signed tokens with a PEM-encoded public key.
    pub fn ed_public_key_pem(public_key_pem: impl Into<Vec<u8>>) -> Self {
        Self {
            algorithm: Algorithm::EdDSA,
            key: JwtVerificationKey::EdPublicKeyPem(public_key_pem.into()),
        }
    }

    /// Verify EdDSA/Ed25519-signed tokens with a DER-encoded public key.
    pub fn ed_public_key_der(public_key_der: impl Into<Vec<u8>>) -> Self {
        Self {
            algorithm: Algorithm::EdDSA,
            key: JwtVerificationKey::EdPublicKeyDer(public_key_der.into()),
        }
    }
}

/// JWT verification key material.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JwtVerificationKey {
    /// HMAC shared secret.
    HmacSecret(Vec<u8>),
    /// PEM-encoded RSA public key.
    RsaPublicKeyPem(Vec<u8>),
    /// PEM-encoded Ed25519 public key.
    EdPublicKeyPem(Vec<u8>),
    /// DER-encoded Ed25519 public key.
    EdPublicKeyDer(Vec<u8>),
}

/// First-frame auth handshake shape.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthHandshake {
    /// Bearer JWT/token supplied by a client when the upgrade request did not
    /// carry an `Authorization` header.
    #[serde(default, alias = "jwt_token", alias = "backend_session")]
    pub bearer_jwt: Option<String>,
    /// Stable application subject to bind into a Jazz author id.
    pub sub: String,
    /// Application claims to bind into Jazz policy evaluation.
    #[serde(default)]
    pub claims: BTreeMap<String, Value>,
}

/// Admitted session binding for a transport.
#[derive(Clone, Debug, PartialEq)]
pub struct AdmittedSession {
    /// Trusted issuer paired with `subject` to derive the logical author.
    pub issuer: String,
    /// Auth subject from the accepted credential.
    pub subject: String,
    /// Deterministic Jazz author identity derived from `subject`.
    pub author: AuthorSubject,
    /// Application claims admitted for this session.
    pub claims: BTreeMap<String, Value>,
    /// Admission source.
    pub source: AdmissionSource,
}

/// Where an admission decision came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionSource {
    /// Static bearer matched during the HTTP/WebSocket upgrade.
    AuthorizationHeader,
    /// Static bearer matched in an explicit first WebSocket frame.
    FirstFrameHandshake,
    /// No credential was required; anonymous subject was used.
    Anonymous,
    /// Signed local-first JWT admitted by explicit local-first policy.
    LocalFirstJwt,
}

/// Auth/session admission errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthAdmissionError {
    /// Static bearer auth is configured but no bearer was supplied.
    MissingBearer,
    /// A bearer was supplied but does not match the configured static token.
    InvalidBearer,
    /// A bearer JWT was supplied but failed signature, expiry, or claim validation.
    InvalidJwt(String),
    /// The first-frame handshake was malformed.
    InvalidHandshake(String),
    /// The credential attempted to claim an invalid or Jazz-reserved author identity.
    InvalidAuthorSubject(AuthorSubjectError),
}

impl fmt::Display for AuthAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingBearer => write!(f, "missing bearer auth"),
            Self::InvalidBearer => write!(f, "invalid bearer auth"),
            Self::InvalidJwt(error) => write!(f, "invalid bearer JWT: {error}"),
            Self::InvalidHandshake(error) => write!(f, "invalid auth handshake: {error}"),
            Self::InvalidAuthorSubject(error) => write!(f, "invalid author subject: {error}"),
        }
    }
}

impl std::error::Error for AuthAdmissionError {}

impl From<AuthorSubjectError> for AuthAdmissionError {
    fn from(error: AuthorSubjectError) -> Self {
        Self::InvalidAuthorSubject(error)
    }
}

/// Admit a static bearer credential.
pub fn admit_static_bearer(
    config: &AuthAdmissionConfig,
    bearer: Option<&str>,
    subject: impl Into<String>,
    source: AdmissionSource,
) -> Result<AdmittedSession, AuthAdmissionError> {
    admit_static_bearer_with_claims(config, bearer, subject, BTreeMap::new(), source)
}

/// Admit a static bearer credential with application claims.
pub fn admit_static_bearer_with_claims(
    config: &AuthAdmissionConfig,
    bearer: Option<&str>,
    subject: impl Into<String>,
    claims: BTreeMap<String, Value>,
    source: AdmissionSource,
) -> Result<AdmittedSession, AuthAdmissionError> {
    if let Some(expected) = config.static_bearer_token.as_deref() {
        let supplied = bearer.ok_or(AuthAdmissionError::MissingBearer)?;
        if supplied != expected {
            return Err(AuthAdmissionError::InvalidBearer);
        }
    }
    let subject = subject.into();
    if !crate::tools::identity::principal_is_nonempty(&subject) {
        return Err(AuthAdmissionError::InvalidHandshake(
            "sub must be non-empty".to_owned(),
        ));
    }
    let issuer = match source {
        AdmissionSource::Anonymous => ANONYMOUS_ISSUER,
        _ => STATIC_BEARER_ISSUER,
    };
    let author = AuthorSubject::reserved(issuer, &subject)?;
    let claims = admitted_session_claims(issuer, &subject, author, claims);
    Ok(AdmittedSession {
        issuer: issuer.to_owned(),
        author,
        subject,
        claims,
        source,
    })
}

/// Admit a signed bearer JWT.
pub fn admit_bearer_jwt(
    config: &AuthAdmissionConfig,
    bearer: Option<&str>,
    source: AdmissionSource,
) -> Result<AdmittedSession, AuthAdmissionError> {
    let verifier = config
        .jwt_verifier
        .as_ref()
        .ok_or(AuthAdmissionError::InvalidBearer)?;
    let expected_issuer = config.expected_issuer.as_deref().ok_or_else(|| {
        AuthAdmissionError::InvalidJwt("external JWT expected issuer is not configured".to_owned())
    })?;
    let expected_audience = config.expected_audience.as_deref().ok_or_else(|| {
        AuthAdmissionError::InvalidJwt(
            "external JWT expected audience is not configured".to_owned(),
        )
    })?;
    let token = bearer.ok_or(AuthAdmissionError::MissingBearer)?;
    let key = jwt_decoding_key(verifier)?;
    let mut validation = Validation::new(verifier.algorithm);
    validation.required_spec_claims.insert("exp".to_owned());
    validation.required_spec_claims.insert("iss".to_owned());
    validation.required_spec_claims.insert("aud".to_owned());
    validation.required_spec_claims.insert("sub".to_owned());
    validation.validate_nbf = true;
    validation.set_issuer(&[expected_issuer]);
    validation.set_audience(&[expected_audience]);
    let decoded = decode::<JwtClaims>(token, &key, &validation).map_err(jwt_error)?;
    if !crate::tools::identity::principal_is_nonempty(&decoded.claims.sub) {
        return Err(AuthAdmissionError::InvalidJwt("missing sub".to_owned()));
    }
    let issuer = decoded.claims.iss;
    let subject = decoded.claims.sub;
    let author = author_subject_from_issuer_and_subject(&issuer, &subject)?;
    let claims = admitted_session_claims(
        &issuer,
        &subject,
        author,
        jwt_json_claims_to_policy_claims(decoded.claims.extra)?,
    );
    Ok(AdmittedSession {
        issuer,
        author,
        subject,
        claims,
        source,
    })
}

/// Issuer required for local-first admission tokens.
pub const LOCAL_FIRST_JWT_ISSUER: &str = AuthorSubject::LOCAL_FIRST_ISSUER;
/// Reserved issuer for subjects admitted by the process-local static bearer gate.
pub const STATIC_BEARER_ISSUER: &str = AuthorSubject::STATIC_BEARER_ISSUER;
/// Reserved issuer for sessions admitted without an external credential.
pub const ANONYMOUS_ISSUER: &str = AuthorSubject::ANONYMOUS_ISSUER;

/// Admit a signed local-first JWT.
///
/// This intentionally uses the configured JWT verifier and does not accept
/// unsigned tokens. When the server has a configured app id/audience, both the
/// JWT `aud` and local-first `appId` claims must match it.
pub fn admit_local_first_jwt(
    config: &AuthAdmissionConfig,
    bearer: Option<&str>,
) -> Result<AdmittedSession, AuthAdmissionError> {
    if !config.allow_local_first_auth {
        return Err(AuthAdmissionError::InvalidBearer);
    }
    let verifier = config
        .jwt_verifier
        .as_ref()
        .ok_or(AuthAdmissionError::InvalidBearer)?;
    let token = bearer.ok_or(AuthAdmissionError::MissingBearer)?;
    let key = jwt_decoding_key(verifier)?;
    let mut validation = Validation::new(verifier.algorithm);
    validation.required_spec_claims.insert("exp".to_owned());
    validation.required_spec_claims.insert("iss".to_owned());
    validation.required_spec_claims.insert("sub".to_owned());
    validation.set_issuer(&[LOCAL_FIRST_JWT_ISSUER]);
    if let Some(expected_audience) = config.expected_audience.as_deref() {
        validation.required_spec_claims.insert("aud".to_owned());
        validation.set_audience(&[expected_audience]);
    } else {
        validation.validate_aud = false;
    }
    let decoded = decode::<LocalFirstJwtClaims>(token, &key, &validation).map_err(jwt_error)?;
    if !crate::tools::identity::principal_is_nonempty(&decoded.claims.sub) {
        return Err(AuthAdmissionError::InvalidJwt("missing sub".to_owned()));
    }
    if let Some(expected_audience) = config.expected_audience.as_deref() {
        match decoded
            .claims
            .extra
            .get("appId")
            .and_then(|value| value.as_str())
        {
            Some(app_id) if app_id == expected_audience => {}
            Some(_) => {
                return Err(AuthAdmissionError::InvalidJwt(
                    "appId does not match expected audience".to_owned(),
                ));
            }
            None => return Err(AuthAdmissionError::InvalidJwt("missing appId".to_owned())),
        }
    }
    let subject = decoded.claims.sub;
    let author = AuthorSubject::reserved(LOCAL_FIRST_JWT_ISSUER, &subject)?;
    let claims = admitted_session_claims(
        LOCAL_FIRST_JWT_ISSUER,
        &subject,
        author,
        jwt_json_claims_to_policy_claims(decoded.claims.extra)?,
    );
    Ok(AdmittedSession {
        issuer: LOCAL_FIRST_JWT_ISSUER.to_owned(),
        author,
        subject,
        claims,
        source: AdmissionSource::LocalFirstJwt,
    })
}

/// Inject the trusted session vocabulary once after provider claims are
/// admitted. Provider `sub` maps to the documented raw `user_id`; `user` is
/// the distinct reserved canonical `[iss,sub]` logical identity.
pub fn admitted_session_claims(
    issuer: &str,
    subject: &str,
    author: AuthorSubject,
    claims: BTreeMap<String, Value>,
) -> BTreeMap<String, Value> {
    let (author_issuer, author_subject): (String, String) =
        serde_json::from_str(author.canonical())
            .expect("admitted authors always have canonical issuer/subject JSON");
    debug_assert_eq!(
        (author_issuer, author_subject),
        (issuer.to_owned(), subject.to_owned())
    );
    crate::tools::policy_claims::canonical_policy_binding_claims(&author, claims, Value::String)
}

fn jwt_decoding_key(verifier: &JwtVerifierConfig) -> Result<DecodingKey, AuthAdmissionError> {
    match &verifier.key {
        JwtVerificationKey::HmacSecret(secret) => Ok(DecodingKey::from_secret(secret)),
        JwtVerificationKey::RsaPublicKeyPem(public_key) => {
            DecodingKey::from_rsa_pem(public_key).map_err(jwt_error)
        }
        JwtVerificationKey::EdPublicKeyPem(public_key) => {
            DecodingKey::from_ed_pem(public_key).map_err(jwt_error)
        }
        JwtVerificationKey::EdPublicKeyDer(public_key) => Ok(DecodingKey::from_ed_der(public_key)),
    }
}

#[derive(Clone, Debug, Deserialize)]
struct JwtClaims {
    iss: String,
    sub: String,
    #[serde(rename = "exp")]
    _exp: u64,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize)]
struct LocalFirstJwtClaims {
    #[serde(rename = "iss")]
    _iss: String,
    sub: String,
    #[serde(rename = "exp")]
    _exp: u64,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}

fn jwt_error(error: jsonwebtoken::errors::Error) -> AuthAdmissionError {
    AuthAdmissionError::InvalidJwt(error.to_string())
}

/// Convert flat verified JWT metadata into scalar policy values. RFC 7519
/// registered transport/security fields are excluded because admission supplies
/// verified identity separately. Objects remain available to application
/// handlers as session metadata but are not representable in core policies.
pub fn jwt_json_claims_to_policy_claims(
    extra: BTreeMap<String, serde_json::Value>,
) -> Result<BTreeMap<String, Value>, AuthAdmissionError> {
    let mut claims = BTreeMap::new();
    for (name, value) in extra {
        if matches!(
            name.as_str(),
            "sub" | "exp" | "nbf" | "iat" | "iss" | "aud" | "jti"
        ) {
            continue;
        }
        if let Some(value) = crate::tools::policy_claims::json_value_to_policy_claim(
            value,
            crate::tools::policy_claims::NumericClaimOrigin::ExactJson,
        )
        .map_err(AuthAdmissionError::InvalidJwt)?
        {
            claims.insert(name, value);
        }
    }
    Ok(claims)
}

/// Bind an exact issuer/subject pair into the logical Jazz author identity.
pub fn author_subject_from_issuer_and_subject(
    issuer: &str,
    subject: &str,
) -> Result<AuthorSubject, AuthAdmissionError> {
    AuthorSubject::authenticated(issuer, subject).map_err(AuthAdmissionError::InvalidAuthorSubject)
}

/// Extract a bearer token from an `Authorization` header value.
pub fn bearer_from_authorization(value: &str) -> Option<&str> {
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .filter(|token| !token.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde_json::json;

    const TEST_JWT_SECRET: &[u8] = b"auth-admission-test-secret";

    fn signed_test_jwt(issuer: &str, subject: &str) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            &json!({
                "iss": issuer,
                "aud": "jazz-audience",
                "sub": subject,
                "exp": 4_102_444_800_u64,
                "claims": {
                    "role": "editor",
                },
            }),
            &EncodingKey::from_secret(TEST_JWT_SECRET),
        )
        .unwrap()
    }

    fn test_jwt_config(issuer: &str) -> AuthAdmissionConfig {
        AuthAdmissionConfig::jwt(JwtVerifierConfig::hmac_secret(
            Algorithm::HS256,
            TEST_JWT_SECRET,
        ))
        .with_expected_issuer(issuer)
        .with_expected_audience("jazz-audience")
    }

    #[test]
    fn issuer_and_subject_define_author_identity() {
        let subject = "00000000-0000-4000-8000-0000000000b2";
        assert_eq!(
            author_subject_from_issuer_and_subject("https://issuer.example", subject).unwrap(),
            AuthorSubject::authenticated("https://issuer.example", subject).unwrap()
        );
    }

    #[test]
    fn external_author_admission_rejects_missing_and_reserved_issuers_with_typed_errors() {
        assert_eq!(
            author_subject_from_issuer_and_subject("", "user"),
            Err(AuthAdmissionError::InvalidAuthorSubject(
                AuthorSubjectError::MissingIssuer
            ))
        );
        for issuer in [
            AuthorSubject::SYSTEM_ISSUER,
            LOCAL_FIRST_JWT_ISSUER,
            STATIC_BEARER_ISSUER,
            ANONYMOUS_ISSUER,
        ] {
            assert_eq!(
                author_subject_from_issuer_and_subject(issuer, "user"),
                Err(AuthAdmissionError::InvalidAuthorSubject(
                    AuthorSubjectError::ReservedIssuer(issuer.to_owned())
                ))
            );
        }
    }

    #[test]
    fn signed_external_jwt_preserves_exact_issuer_and_subject() {
        let token = signed_test_jwt(" https://issuer.example ", " user ");
        let admitted = admit_bearer_jwt(
            &test_jwt_config(" https://issuer.example "),
            Some(&token),
            AdmissionSource::AuthorizationHeader,
        )
        .unwrap();

        assert_eq!(admitted.issuer, " https://issuer.example ");
        assert_eq!(admitted.subject, " user ");
        assert_eq!(
            admitted.author.canonical(),
            r#"[" https://issuer.example "," user "]"#
        );
        assert_eq!(
            admitted
                .claims
                .get(&crate::query::provider_claim_key("iss")),
            Some(&Value::String(" https://issuer.example ".to_owned()))
        );
    }

    #[test]
    fn signed_external_jwt_rejects_reserved_issuers_during_resolution() {
        for issuer in [
            AuthorSubject::SYSTEM_ISSUER,
            LOCAL_FIRST_JWT_ISSUER,
            STATIC_BEARER_ISSUER,
            ANONYMOUS_ISSUER,
        ] {
            let token = signed_test_jwt(issuer, "user");
            assert_eq!(
                admit_bearer_jwt(
                    &test_jwt_config(issuer),
                    Some(&token),
                    AdmissionSource::AuthorizationHeader,
                ),
                Err(AuthAdmissionError::InvalidAuthorSubject(
                    AuthorSubjectError::ReservedIssuer(issuer.to_owned())
                ))
            );
        }
    }

    #[test]
    fn jwt_claim_admission_preserves_exact_integers_and_ignores_oidc_metadata() {
        let claims = jwt_json_claims_to_policy_claims(BTreeMap::from([
            ("positive".to_owned(), serde_json::json!(7)),
            ("negative".to_owned(), serde_json::json!(-7)),
            (
                "unsafe".to_owned(),
                serde_json::json!(9_007_199_254_740_992_u64),
            ),
        ]))
        .unwrap();
        assert_eq!(claims["positive"], Value::U64(7));
        assert_eq!(claims["negative"], Value::I64(-7));
        assert_eq!(claims["unsafe"], Value::U64(9_007_199_254_740_992));
        let claims = jwt_json_claims_to_policy_claims(BTreeMap::from([(
            "https://issuer.example/profile".to_owned(),
            serde_json::json!({ "department": "engineering" }),
        )]))
        .unwrap();
        assert!(
            claims.is_empty(),
            "unrepresentable OIDC metadata stays ignored"
        );
        let claims = jwt_json_claims_to_policy_claims(BTreeMap::from([(
            "claims".to_owned(),
            serde_json::json!({ "profile": { "department": "engineering" } }),
        )]))
        .unwrap();
        assert!(
            claims.is_empty(),
            "a top-level claims object is ordinary unsupported object metadata, never a second flattening path"
        );
    }
}
