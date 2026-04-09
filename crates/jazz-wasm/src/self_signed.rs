use jazz_tools::self_signed_auth;
use wasm_bindgen::prelude::*;

/// Generate a new 32-byte identity seed, returned as base64url.
#[wasm_bindgen(js_name = generateIdentitySeed)]
pub fn generate_identity_seed() -> String {
    let seed = self_signed_auth::generate_seed();
    self_signed_auth::encode_seed(&seed)
}

/// Derive the canonical userId from a base64url-encoded seed.
///
/// Internally: HKDF(seed) -> Ed25519 keypair -> UUIDv5(namespace, pubkey).
#[wasm_bindgen(js_name = deriveSelfSignedUserId)]
pub fn derive_self_signed_user_id(seed_b64: &str) -> Result<String, JsError> {
    let seed_bytes = self_signed_auth::decode_seed(seed_b64).map_err(|e| JsError::new(&e))?;
    let seed: [u8; 32] = seed_bytes
        .try_into()
        .map_err(|_| JsError::new("seed must be 32 bytes"))?;
    let key = self_signed_auth::derive_signing_key(&seed);
    Ok(self_signed_auth::derive_user_id(&key.verifying_key()))
}

/// Mint a self-signed JWT from a base64url-encoded seed.
///
/// Returns the complete JWT string ready to use as a bearer token.
#[wasm_bindgen(js_name = mintSelfSignedToken)]
pub fn mint_self_signed_token(
    seed_b64: &str,
    audience: &str,
    ttl_secs: Option<u32>,
) -> Result<String, JsError> {
    let seed_bytes = self_signed_auth::decode_seed(seed_b64).map_err(|e| JsError::new(&e))?;
    let seed: [u8; 32] = seed_bytes
        .try_into()
        .map_err(|_| JsError::new("seed must be 32 bytes"))?;
    let key = self_signed_auth::derive_signing_key(&seed);
    self_signed_auth::mint_token(&key, audience, ttl_secs.map(u64::from))
        .map_err(|e| JsError::new(&e))
}

/// Derive userId from a base64url-encoded raw Ed25519 public key.
///
/// Useful for verifying that a public key maps to an expected userId.
#[wasm_bindgen(js_name = deriveUserIdFromPublicKey)]
pub fn derive_user_id_from_public_key(pub_key_b64: &str) -> Result<String, JsError> {
    self_signed_auth::derive_user_id_from_b64(pub_key_b64).map_err(|e| JsError::new(&e))
}
