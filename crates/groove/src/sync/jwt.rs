//! JWT token validation for authentication.
//!
//! This module provides JWT validation for authenticating clients with tokens
//! from external auth providers like BetterAuth or WorkOS.
//!
//! Supports both HS256 (symmetric key) and RS256 (asymmetric/JWKS) validation.
//!
//! # Example
//!
//! ```ignore
//! use groove::sync::jwt::{JwtConfig, JwtTokenValidator};
//!
//! // HS256 with shared secret
//! let config = JwtConfig {
//!     secret: Some("my-secret-key".to_string()),
//!     issuer: Some("https://auth.example.com".to_string()),
//!     user_id_claim: "sub".to_string(),
//!     ..Default::default()
//! };
//!
//! // RS256 with JWKS endpoint
//! let config = JwtConfig {
//!     jwks_url: Some("https://auth.example.com/.well-known/jwks.json".to_string()),
//!     issuer: Some("https://auth.example.com".to_string()),
//!     user_id_claim: "sub".to_string(),
//!     ..Default::default()
//! };
//!
//! let validator = JwtTokenValidator::new(config);
//! // For JWKS, fetch keys at startup:
//! validator.refresh_jwks().ok();
//! ```

use std::collections::HashMap;
use std::sync::RwLock;

use base64::Engine;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::server::{ClaimValue, ClientIdentity, TokenValidator};
use crate::ObjectId;

/// Configuration for JWT token validation.
#[derive(Debug, Clone, Default)]
pub struct JwtConfig {
    /// JWKS URL for RS256/RS384/RS512 validation (production).
    /// If set, keys will be fetched from this endpoint.
    pub jwks_url: Option<String>,

    /// Secret key for HS256 validation (development/testing).
    /// If set and jwks_url is not set, this will be used.
    pub secret: Option<String>,

    /// Expected token issuer (`iss` claim).
    pub issuer: Option<String>,

    /// Expected token audience (`aud` claim).
    pub audience: Option<String>,

    /// Claim name containing the user ID (default: "sub").
    pub user_id_claim: String,

    /// Optional claim name containing a pre-resolved Jazz ObjectId.
    /// If present in the token, this will be used as user_id directly.
    pub jazz_user_id_claim: Option<String>,

    /// Claims to extract from the token for policy evaluation.
    /// If empty, all claims will be extracted.
    pub extract_claims: Vec<String>,

    /// Whether to validate token expiration (default: true).
    pub validate_exp: bool,
}

impl JwtConfig {
    /// Create a new config for HS256 validation with a secret.
    pub fn with_secret(secret: impl Into<String>) -> Self {
        Self {
            secret: Some(secret.into()),
            user_id_claim: "sub".to_string(),
            validate_exp: true,
            ..Default::default()
        }
    }

    /// Create a new config for RS256 validation with a JWKS URL.
    pub fn with_jwks(url: impl Into<String>) -> Self {
        Self {
            jwks_url: Some(url.into()),
            user_id_claim: "sub".to_string(),
            validate_exp: true,
            ..Default::default()
        }
    }
}

/// JWKS (JSON Web Key Set) response from an auth provider.
#[derive(Debug, Deserialize)]
struct JwksResponse {
    keys: Vec<JwkKey>,
}

/// Individual JWK (JSON Web Key) in a JWKS response.
#[derive(Debug, Deserialize)]
struct JwkKey {
    /// Key type (e.g., "RSA", "OKP")
    kty: String,
    /// Key ID - used to match tokens to keys
    kid: Option<String>,
    /// Algorithm (e.g., "RS256", "EdDSA")
    #[serde(default)]
    alg: Option<String>,
    /// RSA modulus (base64url encoded)
    #[serde(default)]
    n: Option<String>,
    /// RSA exponent (base64url encoded)
    #[serde(default)]
    e: Option<String>,
    /// Curve for OKP keys (e.g., "Ed25519")
    #[serde(default)]
    crv: Option<String>,
    /// Public key for OKP keys (base64url encoded)
    #[serde(default)]
    x: Option<String>,
}

/// Cached JWKS keys.
#[derive(Default)]
struct JwksCache {
    /// Cached keys indexed by key ID (kid).
    keys: HashMap<String, DecodingKey>,
    /// When the cache was last updated.
    #[allow(dead_code)]
    last_updated: Option<std::time::Instant>,
}

impl std::fmt::Debug for JwksCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwksCache")
            .field("num_keys", &self.keys.len())
            .field("last_updated", &self.last_updated)
            .finish()
    }
}

/// JWT token validator implementing the TokenValidator trait.
pub struct JwtTokenValidator {
    config: JwtConfig,
    /// Decoding key for HS256 (from secret).
    hs256_key: Option<DecodingKey>,
    /// Cached JWKS keys for RS256/RS384/RS512.
    jwks_cache: RwLock<JwksCache>,
}

impl std::fmt::Debug for JwtTokenValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtTokenValidator")
            .field("config", &self.config)
            .field("has_hs256_key", &self.hs256_key.is_some())
            .finish()
    }
}

impl JwtTokenValidator {
    /// Create a new JWT validator with the given configuration.
    pub fn new(config: JwtConfig) -> Self {
        let hs256_key = config
            .secret
            .as_ref()
            .map(|s| DecodingKey::from_secret(s.as_bytes()));

        Self {
            config,
            hs256_key,
            jwks_cache: RwLock::new(JwksCache::default()),
        }
    }

    /// Create a simple HS256 validator for testing.
    pub fn hs256(secret: impl Into<String>) -> Self {
        Self::new(JwtConfig::with_secret(secret))
    }

    /// Fetch and cache JWKS keys from the configured URL.
    ///
    /// Call this at server startup to pre-populate the key cache.
    /// Keys are cached and reused for subsequent validations.
    ///
    /// Returns the number of keys successfully loaded.
    pub fn refresh_jwks(&self) -> Result<usize, String> {
        let jwks_url = self
            .config
            .jwks_url
            .as_ref()
            .ok_or_else(|| "No JWKS URL configured".to_string())?;

        // Fetch JWKS from the URL
        let response = ureq::get(jwks_url)
            .call()
            .map_err(|e| format!("Failed to fetch JWKS: {}", e))?;

        let body = response
            .into_string()
            .map_err(|e| format!("Failed to read JWKS response: {}", e))?;

        let jwks: JwksResponse =
            serde_json::from_str(&body).map_err(|e| format!("Failed to parse JWKS: {}", e))?;

        let mut keys_loaded = 0;
        let mut cache = self.jwks_cache.write().unwrap();

        for jwk in jwks.keys {
            let decoding_key = match jwk.kty.as_str() {
                "RSA" => {
                    // RSA key - need modulus and exponent
                    let (n, e) = match (jwk.n.as_ref(), jwk.e.as_ref()) {
                        (Some(n), Some(e)) => (n, e),
                        _ => continue,
                    };

                    // Decode base64url components
                    let n_bytes = match base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(n) {
                        Ok(b) => b,
                        Err(_) => continue,
                    };
                    let e_bytes = match base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(e) {
                        Ok(b) => b,
                        Err(_) => continue,
                    };

                    DecodingKey::from_rsa_raw_components(&n_bytes, &e_bytes)
                }
                "OKP" => {
                    // OKP key (EdDSA) - need curve and public key
                    let (crv, x) = match (jwk.crv.as_ref(), jwk.x.as_ref()) {
                        (Some(crv), Some(x)) => (crv.as_str(), x.as_str()),
                        _ => continue,
                    };

                    // Only Ed25519 is supported
                    if crv != "Ed25519" {
                        continue;
                    }

                    // Use from_ed_components which takes the base64url-encoded x directly
                    match DecodingKey::from_ed_components(x) {
                        Ok(key) => key,
                        Err(_) => continue,
                    }
                }
                _ => continue,
            };

            // Use kid if available, otherwise use algorithm or index
            let key_id = jwk
                .kid
                .clone()
                .or_else(|| jwk.alg.clone())
                .unwrap_or_else(|| format!("key_{}", keys_loaded));

            cache.keys.insert(key_id, decoding_key);
            keys_loaded += 1;
        }

        cache.last_updated = Some(std::time::Instant::now());

        Ok(keys_loaded)
    }

    /// Validate a token and extract claims.
    fn validate_token(&self, token: &str) -> Option<TokenClaims> {
        // Build validation settings
        let mut validation = Validation::default();

        if let Some(ref iss) = self.config.issuer {
            validation.set_issuer(&[iss]);
        }

        if let Some(ref aud) = self.config.audience {
            validation.set_audience(&[aud]);
        } else {
            // Disable audience validation if no audience is configured
            validation.validate_aud = false;
        }

        validation.validate_exp = self.config.validate_exp;

        // Try HS256 first if we have a secret
        if let Some(ref key) = self.hs256_key {
            validation.algorithms = vec![Algorithm::HS256];
            if let Ok(data) = decode::<TokenClaims>(token, key, &validation) {
                return Some(data.claims);
            }
        }

        // Try JWKS keys for RS256/RS384/RS512
        if self.config.jwks_url.is_some() {
            // Get the key ID from the token header
            let header = match decode_header(token) {
                Ok(h) => h,
                Err(_) => return None,
            };

            let cache = self.jwks_cache.read().unwrap();

            // If cache is empty, try to refresh (lazy loading)
            if cache.keys.is_empty() {
                drop(cache); // Release read lock
                if self.refresh_jwks().is_err() {
                    return None;
                }
                // Re-acquire read lock
                let cache = self.jwks_cache.read().unwrap();
                return self.try_jwks_validation(token, &header, &cache);
            }

            return self.try_jwks_validation(token, &header, &cache);
        }

        None
    }

    /// Try to validate token using JWKS cached keys.
    fn try_jwks_validation(
        &self,
        token: &str,
        header: &jsonwebtoken::Header,
        cache: &JwksCache,
    ) -> Option<TokenClaims> {
        let mut validation = Validation::default();

        if let Some(ref iss) = self.config.issuer {
            validation.set_issuer(&[iss]);
        }

        if let Some(ref aud) = self.config.audience {
            validation.set_audience(&[aud]);
        } else {
            // Disable audience validation if no audience is configured
            validation.validate_aud = false;
        }

        validation.validate_exp = self.config.validate_exp;

        // IMPORTANT: Only allow the algorithm specified in the token header.
        // jsonwebtoken checks that ALL algorithms in the list match the key's family,
        // so we can't mix RSA and EdDSA algorithms in the same validation.
        validation.algorithms = vec![header.alg];

        // If token has a kid, try that key first
        if let Some(ref kid) = header.kid
            && let Some(key) = cache.keys.get(kid)
            && let Ok(data) = decode::<TokenClaims>(token, key, &validation)
        {
            return Some(data.claims);
        }

        // Try all keys (for tokens without kid or if kid didn't match)
        for key in cache.keys.values() {
            if let Ok(data) = decode::<TokenClaims>(token, key, &validation) {
                return Some(data.claims);
            }
        }

        None
    }

    /// Extract claims from a JSON value into ClaimValue.
    fn json_to_claim_value(value: &JsonValue) -> ClaimValue {
        match value {
            JsonValue::String(s) => ClaimValue::String(s.clone()),
            JsonValue::Number(n) => ClaimValue::Number(n.as_f64().unwrap_or(0.0)),
            JsonValue::Bool(b) => ClaimValue::Bool(*b),
            JsonValue::Array(arr) => {
                ClaimValue::Array(arr.iter().map(Self::json_to_claim_value).collect())
            }
            JsonValue::Null => ClaimValue::Null,
            JsonValue::Object(_) => {
                // For nested objects, serialize to string for now
                ClaimValue::String(value.to_string())
            }
        }
    }
}

/// Claims structure for JWT tokens.
/// Uses serde_json::Value to capture all claims dynamically.
#[derive(Debug, Serialize, Deserialize)]
struct TokenClaims {
    /// Subject (user ID)
    #[serde(default)]
    sub: Option<String>,

    /// Expiration time
    #[serde(default)]
    exp: Option<u64>,

    /// Issued at
    #[serde(default)]
    iat: Option<u64>,

    /// Issuer
    #[serde(default)]
    iss: Option<String>,

    /// Audience
    #[serde(default)]
    aud: Option<StringOrArray>,

    /// Name claim
    #[serde(default)]
    name: Option<String>,

    /// Email claim
    #[serde(default)]
    email: Option<String>,

    /// All other claims
    #[serde(flatten)]
    other: HashMap<String, JsonValue>,
}

/// Helper for audience which can be string or array.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum StringOrArray {
    String(String),
    Array(Vec<String>),
}

impl TokenValidator for JwtTokenValidator {
    fn validate(&self, token: &str) -> Option<ClientIdentity> {
        let claims = self.validate_token(token)?;

        // Extract user ID from configured claim
        let external_id = if self.config.user_id_claim == "sub" {
            claims.sub.clone()
        } else {
            claims
                .other
                .get(&self.config.user_id_claim)
                .and_then(|v| v.as_str().map(|s| s.to_string()))
        }?;

        // Try to extract Jazz ObjectId if configured
        let user_id = self.config.jazz_user_id_claim.as_ref().and_then(|claim| {
            claims
                .other
                .get(claim)
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<ObjectId>().ok())
        });

        // Build claims map
        let mut claim_map = HashMap::new();

        // Add standard claims
        if let Some(ref email) = claims.email {
            claim_map.insert("email".to_string(), ClaimValue::String(email.clone()));
        }
        if let Some(ref name) = claims.name {
            claim_map.insert("name".to_string(), ClaimValue::String(name.clone()));
        }
        if let Some(ref iss) = claims.iss {
            claim_map.insert("iss".to_string(), ClaimValue::String(iss.clone()));
        }

        // Add other claims
        let claims_to_extract = if self.config.extract_claims.is_empty() {
            // Extract all claims
            claims.other.keys().cloned().collect::<Vec<_>>()
        } else {
            self.config.extract_claims.clone()
        };

        for claim_name in claims_to_extract {
            if let Some(value) = claims.other.get(&claim_name) {
                claim_map.insert(claim_name, Self::json_to_claim_value(value));
            }
        }

        Some(ClientIdentity {
            external_id,
            user_id,
            name: claims.name,
            claims: claim_map,
            expires_at: claims.exp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};

    fn create_test_token(claims: &TokenClaims, secret: &str) -> String {
        encode(
            &Header::default(),
            claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    #[test]
    fn test_validate_simple_token() {
        let secret = "test-secret-key-at-least-32-bytes";
        let validator = JwtTokenValidator::hs256(secret);

        let claims = TokenClaims {
            sub: Some("user123".to_string()),
            exp: Some(u64::MAX), // Far future
            iat: Some(0),
            iss: None,
            aud: None,
            name: Some("Test User".to_string()),
            email: Some("test@example.com".to_string()),
            other: HashMap::new(),
        };

        let token = create_test_token(&claims, secret);
        let identity = validator.validate(&token).expect("should validate");

        assert_eq!(identity.external_id, "user123");
        assert_eq!(identity.name, Some("Test User".to_string()));
        assert_eq!(
            identity.get_claim("email"),
            Some(&ClaimValue::String("test@example.com".to_string()))
        );
    }

    #[test]
    fn test_validate_token_with_custom_claims() {
        let secret = "test-secret-key-at-least-32-bytes";
        let validator = JwtTokenValidator::hs256(secret);

        let mut other = HashMap::new();
        other.insert(
            "orgId".to_string(),
            JsonValue::String("org_123".to_string()),
        );
        other.insert(
            "subscriptionTier".to_string(),
            JsonValue::String("pro".to_string()),
        );
        other.insert(
            "roles".to_string(),
            JsonValue::Array(vec![
                JsonValue::String("admin".to_string()),
                JsonValue::String("editor".to_string()),
            ]),
        );

        let claims = TokenClaims {
            sub: Some("user456".to_string()),
            exp: Some(u64::MAX),
            iat: None,
            iss: None,
            aud: None,
            name: None,
            email: None,
            other,
        };

        let token = create_test_token(&claims, secret);
        let identity = validator.validate(&token).expect("should validate");

        assert_eq!(identity.external_id, "user456");

        // Check custom claims
        assert_eq!(
            identity.get_claim("orgId"),
            Some(&ClaimValue::String("org_123".to_string()))
        );
        assert_eq!(
            identity.get_claim("subscriptionTier"),
            Some(&ClaimValue::String("pro".to_string()))
        );

        // Check array claim
        match identity.get_claim("roles") {
            Some(ClaimValue::Array(roles)) => {
                assert_eq!(roles.len(), 2);
                assert_eq!(roles[0], ClaimValue::String("admin".to_string()));
                assert_eq!(roles[1], ClaimValue::String("editor".to_string()));
            }
            _ => panic!("expected array claim"),
        }
    }

    #[test]
    fn test_invalid_token_rejected() {
        let validator = JwtTokenValidator::hs256("correct-secret-key-32-bytes-long");
        let result = validator.validate("invalid-token");
        assert!(result.is_none());
    }

    #[test]
    fn test_wrong_secret_rejected() {
        let secret = "correct-secret-key-32-bytes-long";
        let claims = TokenClaims {
            sub: Some("user123".to_string()),
            exp: Some(u64::MAX),
            iat: None,
            iss: None,
            aud: None,
            name: None,
            email: None,
            other: HashMap::new(),
        };

        let token = create_test_token(&claims, secret);

        let validator = JwtTokenValidator::hs256("wrong-secret-key-32-bytes-long!!");
        let result = validator.validate(&token);
        assert!(result.is_none());
    }

    #[test]
    fn test_issuer_validation() {
        let secret = "test-secret-key-at-least-32-bytes";
        let config = JwtConfig {
            secret: Some(secret.to_string()),
            issuer: Some("https://auth.example.com".to_string()),
            user_id_claim: "sub".to_string(),
            validate_exp: true,
            ..Default::default()
        };
        let validator = JwtTokenValidator::new(config);

        // Token with wrong issuer should be rejected
        let claims = TokenClaims {
            sub: Some("user123".to_string()),
            exp: Some(u64::MAX),
            iat: None,
            iss: Some("https://wrong-issuer.com".to_string()),
            aud: None,
            name: None,
            email: None,
            other: HashMap::new(),
        };

        let token = create_test_token(&claims, secret);
        let result = validator.validate(&token);
        assert!(result.is_none());

        // Token with correct issuer should be accepted
        let claims = TokenClaims {
            sub: Some("user123".to_string()),
            exp: Some(u64::MAX),
            iat: None,
            iss: Some("https://auth.example.com".to_string()),
            aud: None,
            name: None,
            email: None,
            other: HashMap::new(),
        };

        let token = create_test_token(&claims, secret);
        let result = validator.validate(&token);
        assert!(result.is_some());
    }

    #[test]
    fn test_claim_value_contains() {
        let array = ClaimValue::Array(vec![
            ClaimValue::String("admin".to_string()),
            ClaimValue::String("editor".to_string()),
        ]);

        assert!(array.contains(&ClaimValue::String("admin".to_string())));
        assert!(array.contains(&ClaimValue::String("editor".to_string())));
        assert!(!array.contains(&ClaimValue::String("viewer".to_string())));

        // Non-array doesn't contain anything
        let single = ClaimValue::String("admin".to_string());
        assert!(!single.contains(&ClaimValue::String("admin".to_string())));
    }
}
