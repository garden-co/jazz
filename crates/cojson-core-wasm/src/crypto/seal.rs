use wasm_bindgen::prelude::*;
use cojson_core::crypto::seal as seal_crypto;

/// WASM-exposed function for sealing a message using X25519 + XSalsa20-Poly1305.
/// Provides authenticated encryption with perfect forward secrecy.
/// - `message`: Raw bytes to seal
/// - `sender_secret`: Base58-encoded sender's private key with "sealerSecret_z" prefix
/// - `recipient_id`: Base58-encoded recipient's public key with "sealer_z" prefix
/// - `nonce_material`: Raw bytes used to generate the nonce
/// Returns sealed bytes or throws JsError if sealing fails.
#[wasm_bindgen(js_name = seal)]
pub fn seal(
    message: &[u8],
    sender_secret: &str,
    recipient_id: &str,
    nonce_material: &[u8],
) -> Result<Box<[u8]>, JsError> {
    Ok(seal_crypto::seal(message, sender_secret, recipient_id, nonce_material)?)
}

/// WASM-exposed function for unsealing a message using X25519 + XSalsa20-Poly1305.
/// Provides authenticated decryption with perfect forward secrecy.
/// - `sealed_message`: The sealed bytes to decrypt
/// - `recipient_secret`: Base58-encoded recipient's private key with "sealerSecret_z" prefix
/// - `sender_id`: Base58-encoded sender's public key with "sealer_z" prefix
/// - `nonce_material`: Raw bytes used to generate the nonce (must match sealing)
/// Returns unsealed bytes or throws JsError if unsealing fails.
#[wasm_bindgen(js_name = unseal)]
pub fn unseal(
    sealed_message: &[u8],
    recipient_secret: &str,
    sender_id: &str,
    nonce_material: &[u8],
) -> Result<Box<[u8]>, JsError> {
    Ok(seal_crypto::unseal(sealed_message, recipient_secret, sender_id, nonce_material)?)
}
