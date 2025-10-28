use cojson_core::crypto::signature as signature_crypto;
use wasm_bindgen::prelude::*;

/// WASM-exposed function to sign a message using Ed25519.
/// - `message`: Raw bytes to sign
/// - `secret`: Raw Ed25519 signing key bytes
/// Returns base58-encoded signature with "signature_z" prefix or throws JsError if signing fails.
#[wasm_bindgen(js_name = sign)]
pub fn sign(message: &[u8], secret: &[u8]) -> Result<String, JsError> {
    let secret_str = std::str::from_utf8(secret)
        .map_err(|e| JsError::new(&format!("Invalid UTF-8 in secret: {:?}", e)))?;
    Ok(signature_crypto::sign(message, secret_str)?)
}

/// WASM-exposed function to verify an Ed25519 signature.
/// - `signature`: Raw signature bytes
/// - `message`: Raw bytes that were signed
/// - `id`: Raw Ed25519 verifying key bytes
/// Returns true if signature is valid, false otherwise, or throws JsError if verification fails.
#[wasm_bindgen(js_name = verify)]
pub fn verify(signature: &[u8], message: &[u8], id: &[u8]) -> Result<bool, JsError> {
    let signature_str = std::str::from_utf8(signature)
        .map_err(|e| JsError::new(&format!("Invalid UTF-8 in signature: {:?}", e)))?;
    let id_str = std::str::from_utf8(id)
        .map_err(|e| JsError::new(&format!("Invalid UTF-8 in id: {:?}", e)))?;
    Ok(signature_crypto::verify(signature_str, message, id_str)?)
}

/// WASM-exposed function to derive a signer ID from a signing key.
/// - `secret`: Raw Ed25519 signing key bytes
/// Returns base58-encoded verifying key with "signer_z" prefix or throws JsError if derivation fails.
#[wasm_bindgen(js_name = getSignerId)]
pub fn get_signer_id(secret: &[u8]) -> Result<String, JsError> {
    let secret_str = std::str::from_utf8(secret)
        .map_err(|e| JsError::new(&format!("Invalid UTF-8 in secret: {:?}", e)))?;
    Ok(signature_crypto::get_signer_id(secret_str)?)
}
