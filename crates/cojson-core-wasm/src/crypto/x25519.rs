use cojson_core::crypto::x25519;
use wasm_bindgen::prelude::*;
/// Generate a new X25519 private key using secure random number generation.
/// Returns 32 bytes of raw key material suitable for use with other X25519 functions.
/// This key can be reused for multiple Diffie-Hellman exchanges.
#[wasm_bindgen(js_name = newX25519PrivateKey)]
pub fn new_x25519_private_key() -> Vec<u8> {
    x25519::new_x25519_private_key().into()
}

/// WASM-exposed function to derive an X25519 public key from a private key.
/// - `private_key`: 32 bytes of private key material
/// Returns 32 bytes of public key material or throws JsError if key is invalid.
#[wasm_bindgen(js_name = x25519PublicKey)]
pub fn x25519_public_key(private_key: &[u8]) -> Result<Vec<u8>, JsError> {
    Ok(x25519::x25519_public_key(private_key)?.into())
}

/// WASM-exposed function to perform X25519 Diffie-Hellman key exchange.
/// - `private_key`: 32 bytes of private key material
/// - `public_key`: 32 bytes of public key material
/// Returns 32 bytes of shared secret material or throws JsError if key exchange fails.
#[wasm_bindgen(js_name = x25519DiffieHellman)]
pub fn x25519_diffie_hellman(private_key: &[u8], public_key: &[u8]) -> Result<Vec<u8>, JsError> {
    Ok(x25519::x25519_diffie_hellman(private_key, public_key)?.into())
}

/// WASM-exposed function to derive a sealer ID from a sealer secret.
/// - `secret`: Raw bytes of the sealer secret
/// Returns a base58-encoded sealer ID with "sealer_z" prefix or throws JsError if derivation fails.
#[wasm_bindgen(js_name = getSealerId)]
pub fn get_sealer_id(secret: &[u8]) -> Result<String, JsError> {
    let secret_str = std::str::from_utf8(secret)
        .map_err(|e| JsError::new(&format!("Invalid UTF-8 in secret: {:?}", e)))?;
    Ok(x25519::get_sealer_id(secret_str)?)
}
