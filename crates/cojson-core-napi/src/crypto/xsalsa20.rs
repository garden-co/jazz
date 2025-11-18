use crate::hash::blake3::generate_nonce;
use cojson_core::crypto::xsalsa20;
use napi::bindgen_prelude::Uint8Array;
use napi_derive::napi;

/// NAPI-exposed function for XSalsa20 encryption without authentication.
/// - `key`: 32-byte key for encryption
/// - `nonce_material`: Raw bytes used to generate a 24-byte nonce via BLAKE3
/// - `plaintext`: Raw bytes to encrypt
/// Returns the encrypted bytes or throws a JsError if encryption fails.
/// Note: This function does not provide authentication. Use encrypt_xsalsa20_poly1305 for authenticated encryption.
#[napi]
pub fn encrypt_xsalsa20(
  key: &[u8],
  nonce_material: &[u8],
  plaintext: &[u8],
) -> napi::Result<Uint8Array> {
  let nonce: Uint8Array = generate_nonce(nonce_material);
  xsalsa20::encrypt_xsalsa20_raw(key, &nonce, plaintext)
    .map(|b| b.into())
    .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
}

/// NAPI-exposed function for XSalsa20 decryption without authentication.
/// - `key`: 32-byte key for decryption (must match encryption key)
/// - `nonce_material`: Raw bytes used to generate a 24-byte nonce (must match encryption)
/// - `ciphertext`: Encrypted bytes to decrypt
/// Returns the decrypted bytes or throws a JsError if decryption fails.
/// Note: This function does not provide authentication. Use decrypt_xsalsa20_poly1305 for authenticated decryption.
#[napi]
pub fn decrypt_xsalsa20(
  key: &[u8],
  nonce_material: &[u8],
  ciphertext: &[u8],
) -> napi::Result<Uint8Array> {
  let nonce = generate_nonce(nonce_material);
  xsalsa20::decrypt_xsalsa20_raw(key, &nonce, ciphertext)
    .map(|v| v.into())
    .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
}
