use crate::hash::blake3::generate_nonce;
use cojson_core::crypto::xsalsa20 as xsalsa20_crypto;
use wasm_bindgen::prelude::*;

/// WASM-exposed function for XSalsa20 encryption without authentication.
/// - `key`: 32-byte key for encryption
/// - `nonce_material`: Raw bytes used to generate a 24-byte nonce via BLAKE3
/// - `plaintext`: Raw bytes to encrypt
/// Returns the encrypted bytes or throws a JsError if encryption fails.
/// Note: This function does not provide authentication. Use encrypt_xsalsa20_poly1305 for authenticated encryption.
#[wasm_bindgen(js_name = encryptXsalsa20)]
pub fn encrypt_xsalsa20(
    key: &[u8],
    nonce_material: &[u8],
    plaintext: &[u8],
) -> Result<Box<[u8]>, JsError> {
    let nonce = generate_nonce(nonce_material);
    Ok(xsalsa20_crypto::encrypt_xsalsa20_raw(
        key, &nonce, plaintext,
    )?)
}

/// WASM-exposed function for XSalsa20 decryption without authentication.
/// - `key`: 32-byte key for decryption (must match encryption key)
/// - `nonce_material`: Raw bytes used to generate a 24-byte nonce (must match encryption)
/// - `ciphertext`: Encrypted bytes to decrypt
/// Returns the decrypted bytes or throws a JsError if decryption fails.
/// Note: This function does not provide authentication. Use decrypt_xsalsa20_poly1305 for authenticated decryption.
#[wasm_bindgen(js_name = decryptXsalsa20)]
pub fn decrypt_xsalsa20(
    key: &[u8],
    nonce_material: &[u8],
    ciphertext: &[u8],
) -> Result<Box<[u8]>, JsError> {
    let nonce = generate_nonce(nonce_material);
    Ok(xsalsa20_crypto::decrypt_xsalsa20_raw(
        key, &nonce, ciphertext,
    )?)
}
