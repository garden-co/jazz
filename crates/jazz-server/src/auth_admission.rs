//! Auth/session admission types for server transports.
//!
//! This slice supports deterministic subject-to-author binding, a static bearer
//! gate, and a deliberately small static JWT verifier.

use std::collections::BTreeMap;
use std::fmt;

use jazz::groove::records::Value;
use jazz::ids::AuthorId;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};
use serde_json::Number;

/// Admission policy used by loopback transports.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthAdmissionConfig {
    /// Static bearer token accepted by this process-local admission gate.
    pub static_bearer_token: Option<String>,
    /// Static JWT verifier accepted by this process-local admission gate.
    pub jwt_verifier: Option<JwtVerifierConfig>,
    /// Whether local-first JWT auth is allowed by configuration.
    pub allow_local_first_auth: bool,
    /// Optional app id/audience that local-first JWTs must target.
    pub expected_audience: Option<String>,
    /// Fallback subject used when no explicit auth is required.
    pub anonymous_subject: String,
}

impl Default for AuthAdmissionConfig {
    fn default() -> Self {
        Self {
            static_bearer_token: None,
            jwt_verifier: None,
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
            allow_local_first_auth: false,
            expected_audience: None,
            anonymous_subject: "anonymous".to_owned(),
        }
    }

    /// Bind local-first JWT admission to a configured app id/audience.
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
    #[serde(
        default,
        alias = "admin_secret",
        alias = "backend_secret",
        alias = "jwt_token",
        alias = "backend_session"
    )]
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
    /// Auth subject from the accepted credential.
    pub subject: String,
    /// Deterministic Jazz author identity derived from `subject`.
    pub author: AuthorId,
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
}

impl fmt::Display for AuthAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingBearer => write!(f, "missing bearer auth"),
            Self::InvalidBearer => write!(f, "invalid bearer auth"),
            Self::InvalidJwt(error) => write!(f, "invalid bearer JWT: {error}"),
            Self::InvalidHandshake(error) => write!(f, "invalid auth handshake: {error}"),
        }
    }
}

impl std::error::Error for AuthAdmissionError {}

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
    Ok(AdmittedSession {
        author: author_id_from_subject(&subject),
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
    let token = bearer.ok_or(AuthAdmissionError::MissingBearer)?;
    let key = jwt_decoding_key(verifier)?;
    let mut validation = Validation::new(verifier.algorithm);
    validation.required_spec_claims.insert("exp".to_owned());
    validation.required_spec_claims.insert("sub".to_owned());
    let decoded = decode::<JwtClaims>(token, &key, &validation).map_err(jwt_error)?;
    if decoded.claims.sub.is_empty() {
        return Err(AuthAdmissionError::InvalidJwt("missing sub".to_owned()));
    }
    let subject = decoded.claims.sub;
    let mut claims = jwt_json_claims_to_policy_claims(decoded.claims.extra)?;
    claims.insert("sub".to_owned(), Value::String(subject.clone()));
    Ok(AdmittedSession {
        author: author_id_from_subject(&subject),
        subject,
        claims,
        source,
    })
}

/// Issuer required for local-first admission tokens.
pub const LOCAL_FIRST_JWT_ISSUER: &str = "urn:jazz:local-first";

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
    if decoded.claims.sub.is_empty() {
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
    let mut claims = jwt_json_claims_to_policy_claims(decoded.claims.extra)?;
    claims.insert("sub".to_owned(), Value::String(subject.clone()));
    claims.insert(
        "iss".to_owned(),
        Value::String(LOCAL_FIRST_JWT_ISSUER.to_owned()),
    );
    Ok(AdmittedSession {
        author: author_id_from_subject(&subject),
        subject,
        claims,
        source: AdmissionSource::LocalFirstJwt,
    })
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

fn jwt_json_claims_to_policy_claims(
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
        if let Some(value) = json_claim_to_policy_claim(value) {
            claims.insert(name, value?);
        }
    }
    Ok(claims)
}

fn json_claim_to_policy_claim(
    value: serde_json::Value,
) -> Option<Result<Value, AuthAdmissionError>> {
    match value {
        serde_json::Value::Null => Some(Ok(Value::Nullable(None))),
        serde_json::Value::Bool(value) => Some(Ok(Value::Bool(value))),
        serde_json::Value::Number(number) => Some(number_to_policy_claim(number)),
        serde_json::Value::String(value) => Some(Ok(value
            .parse()
            .map(Value::Uuid)
            .unwrap_or(Value::String(value)))),
        serde_json::Value::Array(values) => {
            let mut claims = Vec::with_capacity(values.len());
            for value in values {
                let value = json_claim_to_policy_claim(value)?;
                match value {
                    Ok(value) => claims.push(value),
                    Err(error) => return Some(Err(error)),
                }
            }
            Some(Ok(Value::Array(claims)))
        }
        serde_json::Value::Object(_) => None,
    }
}

fn number_to_policy_claim(number: Number) -> Result<Value, AuthAdmissionError> {
    if let Some(value) = number.as_u64() {
        return Ok(Value::U64(value));
    }
    let Some(value) = number.as_f64() else {
        return Err(AuthAdmissionError::InvalidJwt(
            "unsupported numeric claim".to_owned(),
        ));
    };
    if !value.is_finite() {
        return Err(AuthAdmissionError::InvalidJwt(
            "unsupported numeric claim".to_owned(),
        ));
    }
    Ok(Value::F64(value))
}

/// Deterministically map an auth subject to a Jazz author id.
pub fn author_id_from_subject(subject: &str) -> AuthorId {
    let mut lanes = [0xcbf29ce484222325_u64, 0x84222325cbf29ce4_u64];
    for (index, byte) in subject.as_bytes().iter().copied().enumerate() {
        let lane = index & 1;
        lanes[lane] ^= u64::from(byte);
        lanes[lane] = lanes[lane].wrapping_mul(0x100000001b3);
        lanes[lane] ^= (index as u64).rotate_left((byte & 31).into());
    }
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&lanes[0].to_be_bytes());
    bytes[8..].copy_from_slice(&lanes[1].to_be_bytes());
    AuthorId::from_bytes(bytes)
}

/// Extract a bearer token from an `Authorization` header value.
pub fn bearer_from_authorization(value: &str) -> Option<&str> {
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .filter(|token| !token.is_empty())
}
