use cojson_core::crypto::encrypt as encrypt_crypto;
use wasm_bindgen::prelude::*;

/// WASM-exposed function to encrypt bytes with a key secret and nonce material.
/// - `value`: The raw bytes to encrypt
/// - `key_secret`: A base58-encoded key secret with "keySecret_z" prefix
/// - `nonce_material`: Raw bytes used to generate the nonce
/// Returns the encrypted bytes or throws a JsError if encryption fails.
#[wasm_bindgen(js_name = encrypt)]
pub fn encrypt(
    value: &[u8],
    key_secret: &str,
    nonce_material: &[u8],
) -> Result<Box<[u8]>, JsError> {
    Ok(encrypt_crypto::encrypt(value, key_secret, nonce_material)?)
}

/// WASM-exposed function to decrypt bytes with a key secret and nonce material.
/// - `ciphertext`: The encrypted bytes to decrypt
/// - `key_secret`: A base58-encoded key secret with "keySecret_z" prefix
/// - `nonce_material`: Raw bytes used to generate the nonce (must match encryption)
/// Returns the decrypted bytes or throws a JsError if decryption fails.
#[wasm_bindgen(js_name = decrypt)]
pub fn decrypt(
    ciphertext: &[u8],
    key_secret: &str,
    nonce_material: &[u8],
) -> Result<Box<[u8]>, JsError> {
    Ok(encrypt_crypto::decrypt(
        ciphertext,
        key_secret,
        nonce_material,
    )?)
}
