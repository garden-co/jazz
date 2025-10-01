use crate::error::CryptoError;
use crate::hash::blake3::generate_nonce;
use bs58;
use napi::bindgen_prelude::Uint8Array;
use napi_derive::napi;

/// Internal function to encrypt bytes with a key secret and nonce material.
/// Takes a base58-encoded key secret with "keySecret_z" prefix and raw nonce material.
/// Returns the encrypted bytes or a CryptoError if the key format is invalid.
pub fn encrypt_internal(
  plaintext: &[u8],
  key_secret: &str,
  nonce_material: &[u8],
) -> Result<Box<[u8]>, CryptoError> {
  // Decode the base58 key secret (removing the "keySecret_z" prefix)
  let key_secret = key_secret
    .strip_prefix("keySecret_z")
    .ok_or(CryptoError::InvalidPrefix("key secret", "keySecret_z"))?;
  let key = bs58::decode(key_secret)
    .into_vec()
    .map_err(|e| CryptoError::Base58Error(e.to_string()))?;

  // Generate nonce from nonce material
  let nonce = generate_nonce(nonce_material);

  // Encrypt using XSalsa20
  super::xsalsa20::encrypt_xsalsa20_raw_internal(&key, &nonce, plaintext)
}

/// Internal function to decrypt bytes with a key secret and nonce material.
/// Takes a base58-encoded key secret with "keySecret_z" prefix and raw nonce material.
/// Returns the decrypted bytes or a CryptoError if the key format is invalid.
pub fn decrypt_internal(
  ciphertext: &[u8],
  key_secret: &str,
  nonce_material: &[u8],
) -> Result<Box<[u8]>, CryptoError> {
  // Decode the base58 key secret (removing the "keySecret_z" prefix)
  let key_secret = key_secret
    .strip_prefix("keySecret_z")
    .ok_or(CryptoError::InvalidPrefix("key secret", "keySecret_z"))?;
  let key = bs58::decode(key_secret)
    .into_vec()
    .map_err(|e| CryptoError::Base58Error(e.to_string()))?;

  // Generate nonce from nonce material
  let nonce = generate_nonce(nonce_material);

  // Decrypt using XSalsa20
  super::xsalsa20::decrypt_xsalsa20_raw_internal(&key, &nonce, ciphertext)
}

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
  napi::Result::Ok(encrypt_internal(value, &key_secret, nonce_material)?.into())
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
  napi::Result::Ok(decrypt_internal(ciphertext, &key_secret, nonce_material)?.into())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_encrypt_decrypt() {
    // Test data
    let plaintext = b"Hello, World!";
    let key_secret = "keySecret_z11111111111111111111111111111111"; // Example base58 encoded key
    let nonce_material = b"test_nonce_material";

    // Test encryption
    let ciphertext = encrypt_internal(plaintext, key_secret, nonce_material).unwrap();
    assert!(!ciphertext.is_empty());

    // Test decryption
    let decrypted = decrypt_internal(&ciphertext, key_secret, nonce_material).unwrap();
    assert_eq!(&*decrypted, plaintext);
  }

  #[test]
  fn test_invalid_key_secret() {
    let plaintext = b"test";
    let nonce_material = b"nonce";

    // Test with invalid key secret format
    let result = encrypt_internal(plaintext, "invalid_key", nonce_material);
    assert!(result.is_err());

    // Test with invalid base58 encoding
    let result = encrypt_internal(plaintext, "keySecret_z!!!!", nonce_material);
    assert!(result.is_err());
  }
}
