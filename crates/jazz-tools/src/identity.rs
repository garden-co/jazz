use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use sha2::{Digest, Sha512};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const KEY_NAMESPACE: Uuid = Uuid::from_bytes([
    0x6a, 0x61, 0x7a, 0x7a, 0x2d, 0x61, 0x75, 0x74, 0x68, 0x2d, 0x6b, 0x65, 0x79, 0x2d, 0x76, 0x31,
]);

const SIGN_DOMAIN: &str = "jazz-auth-sign-v1";

pub const SELF_SIGNED_ISSUER: &str = "urn:jazz:self-signed";
const DEFAULT_MAX_TTL_SECONDS: u64 = 3600;

#[derive(serde::Serialize)]
struct SelfSignedClaims<'a> {
    iss: &'a str,
    sub: &'a str,
    aud: &'a str,
    jazz_pub_key: &'a str,
    auth_mode: &'a str,
    iat: u64,
    exp: u64,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SelfSignedClaimsRaw {
    iss: Option<String>,
    sub: Option<String>,
    aud: Option<String>,
    jazz_pub_key: Option<String>,
    auth_mode: Option<String>,
    iat: Option<u64>,
    exp: Option<u64>,
}

#[derive(serde::Serialize)]
struct JwtHeader<'a> {
    alg: &'a str,
    typ: &'a str,
}

#[derive(serde::Deserialize)]
struct JwtHeaderRaw {
    alg: Option<String>,
}

#[derive(Debug)]
pub struct VerifiedSelfSigned {
    pub user_id: String,
    pub public_key_bytes: [u8; 32],
}

/// Mint a self-signed token using the current system time.
/// Panics on platforms where `SystemTime` is unavailable (e.g. wasm32).
pub fn mint_self_signed_token(
    seed: &[u8; 32],
    audience: &str,
    ttl_seconds: u64,
) -> Result<String, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_secs();
    mint_self_signed_token_at(seed, audience, ttl_seconds, now)
}

/// Mint a self-signed token with an explicit timestamp (for WASM or testing).
pub fn mint_self_signed_token_at(
    seed: &[u8; 32],
    audience: &str,
    ttl_seconds: u64,
    now_seconds: u64,
) -> Result<String, String> {
    let signing_key = derive_signing_key(seed, SIGN_DOMAIN);
    let verifying_key = signing_key.verifying_key();
    let user_id = Uuid::new_v5(&KEY_NAMESPACE, verifying_key.as_bytes());
    let user_id_str = user_id.to_string();

    let pub_key_b64 = URL_SAFE_NO_PAD.encode(verifying_key.as_bytes());

    // Normalize audience: if it's already a UUID use it as-is, otherwise
    // derive a deterministic UUIDv5 (DNS namespace) — matching AppId::from_name.
    let normalized_aud = match Uuid::parse_str(audience) {
        Ok(uuid) => uuid.to_string(),
        Err(_) => Uuid::new_v5(&Uuid::NAMESPACE_DNS, audience.as_bytes()).to_string(),
    };

    let header = JwtHeader {
        alg: "EdDSA",
        typ: "JWT",
    };
    let header_json = serde_json::to_string(&header).map_err(|e| e.to_string())?;
    let header_b64 = URL_SAFE_NO_PAD.encode(header_json.as_bytes());

    let now = now_seconds;
    let exp = now + ttl_seconds;

    let claims = SelfSignedClaims {
        iss: SELF_SIGNED_ISSUER,
        sub: &user_id_str,
        aud: &normalized_aud,
        jazz_pub_key: &pub_key_b64,
        auth_mode: "self-signed",
        iat: now,
        exp,
    };
    let claims_json = serde_json::to_string(&claims).map_err(|e| e.to_string())?;
    let claims_b64 = URL_SAFE_NO_PAD.encode(claims_json.as_bytes());

    let signing_input = format!("{}.{}", header_b64, claims_b64);
    let signature = signing_key.sign(signing_input.as_bytes());
    let signature_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    Ok(format!("{}.{}", signing_input, signature_b64))
}

pub fn verify_self_signed_token(
    token: &str,
    expected_audience: &str,
) -> Result<VerifiedSelfSigned, String> {
    verify_self_signed_token_with_max_ttl(token, expected_audience, DEFAULT_MAX_TTL_SECONDS)
}

pub fn verify_self_signed_token_with_max_ttl(
    token: &str,
    expected_audience: &str,
    max_ttl_seconds: u64,
) -> Result<VerifiedSelfSigned, String> {
    // Normalize expected audience the same way as minting
    let normalized_expected = match Uuid::parse_str(expected_audience) {
        Ok(uuid) => uuid.to_string(),
        Err(_) => Uuid::new_v5(&Uuid::NAMESPACE_DNS, expected_audience.as_bytes()).to_string(),
    };

    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() != 3 {
        return Err("invalid token: expected 3 parts".to_string());
    }
    let (header_b64, claims_b64, sig_b64) = (parts[0], parts[1], parts[2]);

    // Decode and validate header
    let header_bytes = URL_SAFE_NO_PAD
        .decode(header_b64)
        .map_err(|e| format!("header decode: {e}"))?;
    let header: JwtHeaderRaw =
        serde_json::from_slice(&header_bytes).map_err(|e| format!("header parse: {e}"))?;
    if header.alg.as_deref() != Some("EdDSA") {
        return Err(format!("unsupported alg: {:?}", header.alg));
    }

    // Decode and validate claims
    let claims_bytes = URL_SAFE_NO_PAD
        .decode(claims_b64)
        .map_err(|e| format!("claims decode: {e}"))?;
    let claims: SelfSignedClaimsRaw =
        serde_json::from_slice(&claims_bytes).map_err(|e| format!("claims parse: {e}"))?;

    if claims.iss.as_deref() != Some(SELF_SIGNED_ISSUER) {
        return Err(format!("invalid issuer: {:?}", claims.iss));
    }
    if claims.aud.as_deref() != Some(normalized_expected.as_str()) {
        return Err(format!(
            "audience mismatch: expected {normalized_expected:?}, got {:?}",
            claims.aud
        ));
    }
    if claims.auth_mode.as_deref() != Some("self-signed") {
        return Err(format!(
            "invalid auth_mode: expected \"self-signed\", got {:?}",
            claims.auth_mode
        ));
    }

    // Extract and decode public key
    let pub_key_b64 = claims
        .jazz_pub_key
        .as_deref()
        .ok_or("missing jazz_pub_key")?;
    let pub_key_bytes_vec = URL_SAFE_NO_PAD
        .decode(pub_key_b64)
        .map_err(|e| format!("pub key decode: {e}"))?;
    if pub_key_bytes_vec.len() != 32 {
        return Err(format!(
            "pub key must be 32 bytes, got {}",
            pub_key_bytes_vec.len()
        ));
    }
    let pub_key_bytes: [u8; 32] = pub_key_bytes_vec.try_into().unwrap();
    let verifying_key =
        VerifyingKey::from_bytes(&pub_key_bytes).map_err(|e| format!("invalid pub key: {e}"))?;

    // Verify signature
    let signing_input = format!("{}.{}", header_b64, claims_b64);
    let sig_bytes_vec = URL_SAFE_NO_PAD
        .decode(sig_b64)
        .map_err(|e| format!("sig decode: {e}"))?;
    let sig_bytes: [u8; 64] = sig_bytes_vec
        .try_into()
        .map_err(|_| "signature must be 64 bytes".to_string())?;
    let signature = ed25519_dalek::Signature::from_bytes(&sig_bytes);
    verifying_key
        .verify_strict(signing_input.as_bytes(), &signature)
        .map_err(|e| format!("signature verification failed: {e}"))?;

    // Re-derive user ID and validate sub
    let derived_user_id = Uuid::new_v5(&KEY_NAMESPACE, &pub_key_bytes);
    let sub = claims.sub.as_deref().ok_or("missing sub")?;
    if sub != derived_user_id.to_string() {
        return Err(format!(
            "sub mismatch: expected {derived_user_id}, got {sub}"
        ));
    }

    // Validate timestamps
    let iat = claims.iat.ok_or("missing iat")?;
    let exp = claims.exp.ok_or("missing exp")?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_secs();

    // iat must not be in the future (allow 60s clock skew)
    if iat > now + 60 {
        return Err(format!("token issued in the future: iat={iat}, now={now}"));
    }
    // exp must not have passed
    if exp <= now {
        return Err(format!("token expired: exp={exp}, now={now}"));
    }
    // ttl must not exceed max
    let ttl = exp.saturating_sub(iat);
    if ttl > max_ttl_seconds {
        return Err(format!("token ttl {ttl}s exceeds max {max_ttl_seconds}s"));
    }

    Ok(VerifiedSelfSigned {
        user_id: derived_user_id.to_string(),
        public_key_bytes: pub_key_bytes,
    })
}

/// Derive a signing key from a 32-byte seed and a domain string.
/// Uses SHA-512(domain || seed), taking the first 32 bytes as the Ed25519 key material.
pub fn derive_signing_key(seed: &[u8; 32], domain: &str) -> SigningKey {
    let mut hasher = Sha512::new();
    hasher.update(domain.as_bytes());
    hasher.update(seed);
    let hash = hasher.finalize();
    let key_bytes: [u8; 32] = hash[..32].try_into().expect("SHA-512 output is 64 bytes");
    SigningKey::from_bytes(&key_bytes)
}

/// Derive the verifying (public) key from a 32-byte seed using the standard sign domain.
pub fn derive_verifying_key(seed: &[u8; 32]) -> VerifyingKey {
    derive_signing_key(seed, SIGN_DOMAIN).verifying_key()
}

/// Derive a stable UUIDv5 user identity from a 32-byte seed.
/// Derives the signing key for the sign domain, extracts the public key,
/// then produces UUIDv5(KEY_NAMESPACE, public_key_bytes).
pub fn derive_user_id(seed: &[u8; 32]) -> Uuid {
    let verifying_key = derive_verifying_key(seed);
    Uuid::new_v5(&KEY_NAMESPACE, verifying_key.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alice_seed() -> [u8; 32] {
        let mut seed = [0u8; 32];
        seed[0] = 0xAA;
        seed[31] = 0x01;
        seed
    }

    fn bob_seed() -> [u8; 32] {
        let mut seed = [0u8; 32];
        seed[0] = 0xBB;
        seed[31] = 0x02;
        seed
    }

    #[test]
    fn same_seed_produces_same_user_id() {
        let id1 = derive_user_id(&alice_seed());
        let id2 = derive_user_id(&alice_seed());
        assert_eq!(id1, id2);
    }

    #[test]
    fn different_seeds_produce_different_user_ids() {
        let alice_id = derive_user_id(&alice_seed());
        let bob_id = derive_user_id(&bob_seed());
        assert_ne!(alice_id, bob_id);
    }

    #[test]
    fn user_id_is_uuid_v5() {
        let id = derive_user_id(&alice_seed());
        assert_eq!(id.get_version_num(), 5);
    }

    #[test]
    fn derive_verifying_key_is_deterministic() {
        let key1 = derive_verifying_key(&alice_seed());
        let key2 = derive_verifying_key(&alice_seed());
        assert_eq!(key1.as_bytes(), key2.as_bytes());
    }

    #[test]
    fn mint_and_verify_self_signed_token() {
        let seed = alice_seed();
        let user_id = derive_user_id(&seed);
        let token = mint_self_signed_token(&seed, "my-app", 3600).unwrap();
        let verified = verify_self_signed_token(&token, "my-app").unwrap();
        assert_eq!(verified.user_id, user_id.to_string());
    }

    #[test]
    fn reject_wrong_audience() {
        let token = mint_self_signed_token(&alice_seed(), "app-a", 3600).unwrap();
        let result = verify_self_signed_token(&token, "app-b");
        assert!(result.is_err());
    }

    #[test]
    fn reject_excessive_ttl() {
        let token = mint_self_signed_token(&alice_seed(), "my-app", 3600).unwrap();
        let result = verify_self_signed_token_with_max_ttl(&token, "my-app", 1800);
        assert!(result.is_err());
    }

    #[test]
    fn reject_token_with_extra_claims() {
        // Manually build a token with an injected "role" claim
        let seed = alice_seed();
        let signing_key = derive_signing_key(&seed, SIGN_DOMAIN);
        let verifying_key = signing_key.verifying_key();
        let user_id = Uuid::new_v5(&KEY_NAMESPACE, verifying_key.as_bytes());
        let pub_key_b64 = URL_SAFE_NO_PAD.encode(verifying_key.as_bytes());

        let header = serde_json::json!({"alg": "EdDSA", "typ": "JWT"});
        let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = serde_json::json!({
            "iss": SELF_SIGNED_ISSUER,
            "sub": user_id.to_string(),
            "aud": "my-app",
            "jazz_pub_key": pub_key_b64,
            "auth_mode": "self-signed",
            "iat": now,
            "exp": now + 3600,
            "role": "admin",
        });
        let claims_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());

        let signing_input = format!("{}.{}", header_b64, claims_b64);
        let signature = signing_key.sign(signing_input.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());
        let token = format!("{}.{}", signing_input, sig_b64);

        let result = verify_self_signed_token(&token, "my-app");
        assert!(
            result.is_err(),
            "token with extra 'role' claim must be rejected"
        );
        assert!(
            result.unwrap_err().contains("unknown field"),
            "error should mention unknown field"
        );
    }

    #[test]
    fn reject_token_with_wrong_auth_mode() {
        let seed = alice_seed();
        let signing_key = derive_signing_key(&seed, SIGN_DOMAIN);
        let verifying_key = signing_key.verifying_key();
        let user_id = Uuid::new_v5(&KEY_NAMESPACE, verifying_key.as_bytes());
        let pub_key_b64 = URL_SAFE_NO_PAD.encode(verifying_key.as_bytes());

        let header = serde_json::json!({"alg": "EdDSA", "typ": "JWT"});
        let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = serde_json::json!({
            "iss": SELF_SIGNED_ISSUER,
            "sub": user_id.to_string(),
            "aud": "my-app",
            "jazz_pub_key": pub_key_b64,
            "auth_mode": "external",
            "iat": now,
            "exp": now + 3600,
        });
        let claims_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());

        let signing_input = format!("{}.{}", header_b64, claims_b64);
        let signature = signing_key.sign(signing_input.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());
        let token = format!("{}.{}", signing_input, sig_b64);

        let result = verify_self_signed_token(&token, "my-app");
        assert!(
            result.is_err(),
            "token with auth_mode 'external' must be rejected"
        );
    }

    #[test]
    fn different_seeds_produce_different_tokens() {
        let t1 = mint_self_signed_token(&alice_seed(), "app", 3600).unwrap();
        let t2 = mint_self_signed_token(&bob_seed(), "app", 3600).unwrap();
        let v1 = verify_self_signed_token(&t1, "app").unwrap();
        let v2 = verify_self_signed_token(&t2, "app").unwrap();
        assert_ne!(v1.user_id, v2.user_id);
    }
}
