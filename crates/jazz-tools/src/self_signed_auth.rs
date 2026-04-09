//! Self-signed Ed25519 auth: seed management, key derivation, userId, JWT minting/verification.

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use hkdf::Hkdf;
use sha2::Sha256;
use uuid::Uuid;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

pub const SELF_SIGNED_ISSUER: &str = "urn:jazz:self-signed";
const SIGNING_DOMAIN: &[u8] = b"jazz-auth-sign-v1";
const DEFAULT_TTL_SECS: u64 = 3600;

/// Max TTL the server will accept for self-signed tokens.
pub const MAX_TTL_SECS: u64 = 3600;

/// UUIDv5 namespace for self-signed auth.
/// Deterministic: UUIDv5(DNS, "jazz.tools/self-signed-auth").
pub const KEY_NAMESPACE: Uuid = Uuid::from_bytes([
    0x10, 0x30, 0x64, 0xd2, 0x05, 0x96, 0x5d, 0xb0, 0xa9, 0x63, 0xec, 0x2c, 0xf7, 0x4d, 0x77, 0x3b,
]);

/// Generate a random 32-byte seed.
pub fn generate_seed() -> [u8; 32] {
    use rand::RngCore;
    let mut seed = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut seed);
    seed
}

/// Encode seed as base64url (no padding).
pub fn encode_seed(seed: &[u8; 32]) -> String {
    URL_SAFE_NO_PAD.encode(seed)
}

/// Decode base64url seed.
pub fn decode_seed(encoded: &str) -> Result<Vec<u8>, String> {
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|e| format!("invalid base64url: {e}"))?;
    if bytes.len() != 32 {
        return Err(format!("seed must be 32 bytes, got {}", bytes.len()));
    }
    Ok(bytes)
}

/// Derive an Ed25519 signing key from a 32-byte root seed using HKDF.
pub fn derive_signing_key(seed: &[u8; 32]) -> SigningKey {
    let hk = Hkdf::<Sha256>::new(None, seed);
    let mut okm = [0u8; 32];
    hk.expand(SIGNING_DOMAIN, &mut okm)
        .expect("32 bytes is valid for HKDF-SHA256");
    SigningKey::from_bytes(&okm)
}

/// Derive the canonical userId from an Ed25519 verifying (public) key.
///
/// `userId = UUIDv5(KEY_NAMESPACE, raw_ed25519_public_key_bytes)`
pub fn derive_user_id(verifying_key: &VerifyingKey) -> String {
    Uuid::new_v5(&KEY_NAMESPACE, verifying_key.as_bytes()).to_string()
}

/// Derive userId directly from a base64url-encoded public key.
pub fn derive_user_id_from_b64(pub_key_b64: &str) -> Result<String, String> {
    let bytes = URL_SAFE_NO_PAD
        .decode(pub_key_b64)
        .map_err(|e| format!("invalid base64url: {e}"))?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "public key must be 32 bytes".to_string())?;
    let vk =
        VerifyingKey::from_bytes(&arr).map_err(|e| format!("invalid Ed25519 public key: {e}"))?;
    Ok(derive_user_id(&vk))
}

/// Verified self-signed token data.
pub struct VerifiedSelfSigned {
    pub user_id: String,
    pub pub_key_b64: String,
}

/// Mint a self-signed JWT.
pub fn mint_token(
    signing_key: &SigningKey,
    audience: &str,
    ttl_secs: Option<u64>,
) -> Result<String, String> {
    let now = current_unix_secs();
    let ttl = ttl_secs.unwrap_or(DEFAULT_TTL_SECS);
    Ok(mint_token_with_timestamps(
        signing_key,
        audience,
        now,
        now + ttl,
    ))
}

fn mint_token_with_timestamps(
    signing_key: &SigningKey,
    audience: &str,
    iat: u64,
    exp: u64,
) -> String {
    let vk = signing_key.verifying_key();
    let user_id = derive_user_id(&vk);
    let pub_key_b64 = URL_SAFE_NO_PAD.encode(vk.as_bytes());

    let header = serde_json::json!({"alg": "EdDSA", "typ": "JWT"});
    let payload = serde_json::json!({
        "iss": SELF_SIGNED_ISSUER,
        "sub": user_id,
        "aud": audience,
        "jazz_pub_key": pub_key_b64,
        "exp": exp,
        "iat": iat,
    });

    let header_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap());
    let payload_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    format!("{signing_input}.{sig_b64}")
}

/// Verify a self-signed JWT. Returns the authenticated userId.
///
/// This performs the full 10-step verification from the spec.
pub fn verify_token(token: &str, expected_audience: &str) -> Result<VerifiedSelfSigned, String> {
    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() != 3 {
        return Err("malformed JWT: expected 3 parts".into());
    }

    // Step 1: Decode header
    let header_bytes = URL_SAFE_NO_PAD
        .decode(parts[0])
        .map_err(|_| "invalid header encoding")?;
    let header: serde_json::Value =
        serde_json::from_slice(&header_bytes).map_err(|_| "invalid header JSON")?;

    // Step 2: Require alg = EdDSA
    if header.get("alg").and_then(|v| v.as_str()) != Some("EdDSA") {
        return Err("self-signed JWT must use EdDSA algorithm".into());
    }

    // Decode payload
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|_| "invalid payload encoding")?;
    let payload: serde_json::Value =
        serde_json::from_slice(&payload_bytes).map_err(|_| "invalid payload JSON")?;

    // Step 3: Require iss
    let iss = payload
        .get("iss")
        .and_then(|v| v.as_str())
        .ok_or("missing iss claim")?;
    if iss != SELF_SIGNED_ISSUER {
        return Err(format!("unexpected issuer: {iss}"));
    }

    // Step 4: Require aud and match
    let aud = payload
        .get("aud")
        .and_then(|v| v.as_str())
        .ok_or("missing required aud claim")?;
    if aud != expected_audience {
        return Err(format!(
            "audience mismatch: expected {expected_audience}, got {aud}"
        ));
    }

    // Step 5: Extract jazz_pub_key
    let pub_key_b64 = payload
        .get("jazz_pub_key")
        .and_then(|v| v.as_str())
        .ok_or("missing jazz_pub_key claim")?;
    let pub_key_bytes = URL_SAFE_NO_PAD
        .decode(pub_key_b64)
        .map_err(|_| "invalid jazz_pub_key encoding")?;
    let pub_key_arr: [u8; 32] = pub_key_bytes
        .try_into()
        .map_err(|_| "jazz_pub_key must be 32 bytes")?;
    let verifying_key =
        VerifyingKey::from_bytes(&pub_key_arr).map_err(|_| "invalid Ed25519 public key")?;

    // Step 6: Verify signature
    let sig_bytes = URL_SAFE_NO_PAD
        .decode(parts[2])
        .map_err(|_| "invalid signature encoding")?;
    let signature =
        Signature::from_slice(&sig_bytes).map_err(|_| "invalid Ed25519 signature format")?;
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    verifying_key
        .verify_strict(signing_input.as_bytes(), &signature)
        .map_err(|_| "signature verification failed")?;

    // Step 7: Re-derive userId
    let expected_user_id = derive_user_id(&verifying_key);

    // Step 8: Require sub == userId
    let sub = payload
        .get("sub")
        .and_then(|v| v.as_str())
        .ok_or("missing sub claim")?;
    if sub != expected_user_id {
        return Err(format!(
            "sub mismatch: expected {expected_user_id}, got {sub}"
        ));
    }

    // Step 9: Check exp + TTL
    let now = current_unix_secs();
    let exp = payload
        .get("exp")
        .and_then(|v| v.as_u64())
        .ok_or("missing or invalid exp claim")?;
    if exp <= now {
        return Err("token has expired".into());
    }
    let iat = payload.get("iat").and_then(|v| v.as_u64()).unwrap_or(now);
    if exp.saturating_sub(iat) > MAX_TTL_SECS {
        return Err("token TTL exceeds server maximum".into());
    }

    // Step 10: Authenticated
    Ok(VerifiedSelfSigned {
        user_id: expected_user_id,
        pub_key_b64: pub_key_b64.to_string(),
    })
}

/// Check if a JWT's issuer is `urn:jazz:self-signed` (quick pre-check without full decode).
pub fn is_self_signed_issuer(token: &str) -> bool {
    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() < 2 {
        return false;
    }
    let Ok(payload_bytes) = URL_SAFE_NO_PAD.decode(parts[1]) else {
        return false;
    };
    let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&payload_bytes) else {
        return false;
    };
    payload.get("iss").and_then(|v| v.as_str()) == Some(SELF_SIGNED_ISSUER)
}

fn current_unix_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_namespace_is_correct() {
        let computed = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"jazz.tools/self-signed-auth");
        assert_eq!(KEY_NAMESPACE, computed);
    }

    #[test]
    fn test_derive_signing_key_deterministic() {
        let seed = [42u8; 32];
        let key1 = derive_signing_key(&seed);
        let key2 = derive_signing_key(&seed);
        assert_eq!(
            key1.verifying_key().to_bytes(),
            key2.verifying_key().to_bytes(),
        );
    }

    #[test]
    fn test_derive_signing_key_different_seeds() {
        let key1 = derive_signing_key(&[1u8; 32]);
        let key2 = derive_signing_key(&[2u8; 32]);
        assert_ne!(
            key1.verifying_key().to_bytes(),
            key2.verifying_key().to_bytes(),
        );
    }

    #[test]
    fn test_derive_user_id_is_uuidv5() {
        let seed = [42u8; 32];
        let key = derive_signing_key(&seed);
        let user_id = derive_user_id(&key.verifying_key());
        let parsed = Uuid::parse_str(&user_id).unwrap();
        assert_eq!(parsed.get_version_num(), 5);
    }

    #[test]
    fn test_derive_user_id_deterministic() {
        let seed = [42u8; 32];
        let key = derive_signing_key(&seed);
        let id1 = derive_user_id(&key.verifying_key());
        let id2 = derive_user_id(&key.verifying_key());
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_mint_and_verify_roundtrip() {
        let seed = [42u8; 32];
        let key = derive_signing_key(&seed);
        let user_id = derive_user_id(&key.verifying_key());

        let token = mint_token(&key, "test-app", None).unwrap();

        let result = verify_token(&token, "test-app");
        assert!(result.is_ok(), "verify failed: {:?}", result.err());
        let verified = result.unwrap();
        assert_eq!(verified.user_id, user_id);
    }

    #[test]
    fn test_verify_rejects_wrong_audience() {
        let key = derive_signing_key(&[42u8; 32]);
        let token = mint_token(&key, "app-a", None).unwrap();

        let result = verify_token(&token, "app-b");
        assert!(result.is_err());
        assert!(
            format!("{:?}", result.err()).contains("audience"),
            "error should mention audience",
        );
    }

    #[test]
    fn test_verify_rejects_expired() {
        let key = derive_signing_key(&[42u8; 32]);
        let token = mint_token_with_timestamps(&key, "test-app", 1000, 999);

        let result = verify_token(&token, "test-app");
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_rejects_tampered_signature() {
        let key = derive_signing_key(&[42u8; 32]);
        let mut token = mint_token(&key, "test-app", None).unwrap();
        // Corrupt last character of signature
        token.pop();
        token.push('X');

        let result = verify_token(&token, "test-app");
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_rejects_sub_mismatch() {
        let key = derive_signing_key(&[42u8; 32]);
        let token = mint_token(&key, "test-app", None).unwrap();
        let parts: Vec<&str> = token.splitn(3, '.').collect();

        let mut payload: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        payload["sub"] = serde_json::json!("wrong-user-id");
        let new_payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap());

        // Re-sign with the key (so signature is valid but sub is wrong)
        let signing_input = format!("{}.{}", parts[0], new_payload);
        let sig = key.sign(signing_input.as_bytes());
        let new_token = format!(
            "{}.{}",
            signing_input,
            URL_SAFE_NO_PAD.encode(sig.to_bytes())
        );

        let result = verify_token(&new_token, "test-app");
        assert!(result.is_err());
        assert!(
            format!("{:?}", result.err()).contains("sub"),
            "error should mention sub mismatch",
        );
    }

    #[test]
    fn test_verify_rejects_missing_audience() {
        let key = derive_signing_key(&[42u8; 32]);
        let token = mint_token(&key, "test-app", None).unwrap();
        let parts: Vec<&str> = token.splitn(3, '.').collect();

        let mut payload: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        payload.as_object_mut().unwrap().remove("aud");
        let new_payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap());

        let signing_input = format!("{}.{}", parts[0], new_payload);
        let sig = key.sign(signing_input.as_bytes());
        let new_token = format!(
            "{}.{}",
            signing_input,
            URL_SAFE_NO_PAD.encode(sig.to_bytes())
        );

        let result = verify_token(&new_token, "test-app");
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_rejects_excessive_ttl() {
        let key = derive_signing_key(&[42u8; 32]);
        let now = current_unix_secs();
        // TTL of 2 hours exceeds the 1-hour max
        let token = mint_token_with_timestamps(&key, "test-app", now, now + 7200);

        let result = verify_token(&token, "test-app");
        assert!(result.is_err());
        assert!(
            format!("{:?}", result.err()).contains("TTL"),
            "error should mention TTL",
        );
    }

    #[test]
    fn test_is_self_signed_issuer() {
        let key = derive_signing_key(&[42u8; 32]);
        let token = mint_token(&key, "test-app", None).unwrap();
        assert!(is_self_signed_issuer(&token));
    }

    #[test]
    fn test_is_self_signed_issuer_false_for_other() {
        assert!(!is_self_signed_issuer("not.a.jwt"));
        assert!(!is_self_signed_issuer(""));
    }

    #[test]
    fn test_generate_seed_is_32_bytes() {
        let seed = generate_seed();
        assert_eq!(seed.len(), 32);
    }

    #[test]
    fn test_seed_base64_roundtrip() {
        let seed = generate_seed();
        let encoded = encode_seed(&seed);
        let decoded = decode_seed(&encoded).unwrap();
        assert_eq!(seed, decoded.as_slice());
    }

    #[test]
    fn test_derive_user_id_from_b64() {
        let seed = [42u8; 32];
        let key = derive_signing_key(&seed);
        let vk = key.verifying_key();
        let pub_b64 = URL_SAFE_NO_PAD.encode(vk.as_bytes());

        let id_direct = derive_user_id(&vk);
        let id_from_b64 = derive_user_id_from_b64(&pub_b64).unwrap();
        assert_eq!(id_direct, id_from_b64);
    }
}
