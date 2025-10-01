use cojson_core::crypto::encrypt as encrypt_crypto;
use napi::bindgen_prelude::Uint8Array;
use napi_derive::napi;

/// NAPI-exposed function to encrypt bytes with a key secret and nonce material.
/// - `value`: The raw bytes to encrypt
/// - `key_secret`: A base58-encoded key secret with "keySecret_z" prefix
/// - `nonce_material`: Raw bytes used to generate the nonce
/// Returns the encrypted bytes or throws a JsError if encryption fails.
#[napi]
pub fn encrypt(
  value: &[u8],
  key_secret: String,
  nonce_material: &[u8],
) -> napi::Result<Uint8Array> {
  encrypt_crypto::encrypt(value, &key_secret, nonce_material)
    .map(|encrypted| encrypted.into())
    .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
}

/// NAPI-exposed function to decrypt bytes with a key secret and nonce material.
/// - `ciphertext`: The encrypted bytes to decrypt
/// - `key_secret`: A base58-encoded key secret with "keySecret_z" prefix
/// - `nonce_material`: Raw bytes used to generate the nonce (must match encryption)
/// Returns the decrypted bytes or throws a JsError if decryption fails.
#[napi]
pub fn decrypt(
  ciphertext: &[u8],
  key_secret: String,
  nonce_material: &[u8],
) -> napi::Result<Uint8Array> {
  encrypt_crypto::decrypt(ciphertext, &key_secret, nonce_material)
    .map(|decrypted| decrypted.into())
    .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
}
