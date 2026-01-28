use cojson_core::crypto::ed25519;
use wasm_bindgen::prelude::*;

/// Generate a new Ed25519 signing key using secure random number generation.
/// Returns 32 bytes of raw key material suitable for use with other Ed25519 functions.
#[wasm_bindgen(js_name = newEd25519SigningKey)]
pub fn new_ed25519_signing_key() -> Box<[u8]> {
    ed25519::new_ed25519_signing_key()
}

/// WASM-exposed function to derive an Ed25519 verifying key from a signing key.
/// - `signing_key`: 32 bytes of signing key material
/// Returns 32 bytes of verifying key material or throws JsError if key is invalid.
#[wasm_bindgen(js_name = ed25519VerifyingKey)]
pub fn ed25519_verifying_key(signing_key: &[u8]) -> Result<Box<[u8]>, JsError> {
    ed25519::ed25519_verifying_key(signing_key).map_err(|e| JsError::new(&e.to_string()))
}

/// WASM-exposed function to sign a message using Ed25519.
/// - `signing_key`: 32 bytes of signing key material
/// - `message`: Raw bytes to sign
/// Returns 64 bytes of signature material or throws JsError if signing fails.
#[wasm_bindgen(js_name = ed25519Sign)]
pub fn ed25519_sign(signing_key: &[u8], message: &[u8]) -> Result<Box<[u8]>, JsError> {
    Ok(ed25519::ed25519_sign(signing_key, message)?.into())
}

/// WASM-exposed function to verify an Ed25519 signature.
/// - `verifying_key`: 32 bytes of verifying key material
/// - `message`: Raw bytes that were signed
/// - `signature`: 64 bytes of signature material
/// Returns true if signature is valid, false otherwise, or throws JsError if verification fails.
#[wasm_bindgen(js_name = ed25519Verify)]
pub fn ed25519_verify(
    verifying_key: &[u8],
    message: &[u8],
    signature: &[u8],
) -> Result<bool, JsError> {
    Ok(ed25519::ed25519_verify(verifying_key, message, signature)?)
}

/// WASM-exposed function to validate and copy Ed25519 signing key bytes.
/// - `bytes`: 32 bytes of signing key material to validate
/// Returns the same 32 bytes if valid or throws JsError if invalid.
#[wasm_bindgen(js_name = ed25519SigningKeyFromBytes)]
pub fn ed25519_signing_key_from_bytes(bytes: &[u8]) -> Result<Box<[u8]>, JsError> {
    let key_bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| JsError::new("Invalid signing key length"))?;
    Ok(key_bytes.into())
}

/// WASM-exposed function to derive the public key from an Ed25519 signing key.
/// - `signing_key`: 32 bytes of signing key material
/// Returns 32 bytes of public key material or throws JsError if key is invalid.
#[wasm_bindgen(js_name = ed25519SigningKeyToPublic)]
pub fn ed25519_signing_key_to_public(signing_key: &[u8]) -> Result<Box<[u8]>, JsError> {
    Ok(ed25519::ed25519_verifying_key(signing_key)?)
}

/// WASM-exposed function to sign a message with an Ed25519 signing key.
/// - `signing_key`: 32 bytes of signing key material
/// - `message`: Raw bytes to sign
/// Returns 64 bytes of signature material or throws JsError if signing fails.
#[wasm_bindgen(js_name = ed25519SigningKeySign)]
pub fn ed25519_signing_key_sign(signing_key: &[u8], message: &[u8]) -> Result<Box<[u8]>, JsError> {
    Ok(ed25519::ed25519_sign(signing_key, message)?.into())
}

/// WASM-exposed function to validate and copy Ed25519 verifying key bytes.
/// - `bytes`: 32 bytes of verifying key material to validate
/// Returns the same 32 bytes if valid or throws JsError if invalid.
#[wasm_bindgen(js_name = ed25519VerifyingKeyFromBytes)]
pub fn ed25519_verifying_key_from_bytes(bytes: &[u8]) -> Result<Box<[u8]>, JsError> {
    let key_bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| JsError::new("Invalid verifying key length"))?;
    Ok(key_bytes.into())
}

/// WASM-exposed function to validate and copy Ed25519 signature bytes.
/// - `bytes`: 64 bytes of signature material to validate
/// Returns the same 64 bytes if valid or throws JsError if invalid.
#[wasm_bindgen(js_name = ed25519SignatureFromBytes)]
pub fn ed25519_signature_from_bytes(bytes: &[u8]) -> Result<Box<[u8]>, JsError> {
    let sig_bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| JsError::new("Invalid signature length"))?;
    Ok(sig_bytes.into())
}
