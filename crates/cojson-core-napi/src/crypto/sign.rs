use crate::crypto::ed25519::{
  ed25519_sign_internal, ed25519_verify_internal, ed25519_verifying_key_internal,
};
use crate::error::CryptoError;
use bs58;
use napi_derive::napi;

/// Internal function to sign a message using Ed25519.
/// - `message`: Raw bytes to sign
/// - `secret`: Base58-encoded signing key with "signerSecret_z" prefix
/// Returns base58-encoded signature with "signature_z" prefix or error string.
pub fn sign_internal(message: &[u8], secret: &str) -> Result<String, CryptoError> {
  let secret_bytes = bs58::decode(secret.strip_prefix("signerSecret_z").ok_or(
    CryptoError::InvalidPrefix("signer secret", "signerSecret_z"),
  )?)
  .into_vec()
  .map_err(|e| CryptoError::Base58Error(e.to_string()))?;

  let signature = ed25519_sign_internal(&secret_bytes, message)
    .map_err(|e| CryptoError::InvalidVerifyingKey(e.to_string()))?;
  Ok(format!(
    "signature_z{}",
    bs58::encode(signature).into_string()
  ))
}

/// Internal function to verify an Ed25519 signature.
/// - `signature`: Base58-encoded signature with "signature_z" prefix
/// - `message`: Raw bytes that were signed
/// - `id`: Base58-encoded verifying key with "signer_z" prefix
/// Returns true if signature is valid, false otherwise, or error string if formats are invalid.
pub fn verify_internal(signature: &str, message: &[u8], id: &str) -> Result<bool, CryptoError> {
  let signature_bytes = bs58::decode(
    signature
      .strip_prefix("signature_z")
      .ok_or(CryptoError::InvalidPrefix("signature_z", "signature"))?,
  )
  .into_vec()
  .map_err(|e| CryptoError::Base58Error(e.to_string()))?;

  let verifying_key = bs58::decode(
    id.strip_prefix("signer_z")
      .ok_or(CryptoError::InvalidPrefix("signer_z", "signer ID"))?,
  )
  .into_vec()
  .map_err(|e| CryptoError::Base58Error(e.to_string()))?;

  ed25519_verify_internal(&verifying_key, message, &signature_bytes)
    .map_err(|e| CryptoError::InvalidVerifyingKey(e.to_string()))
}

/// Internal function to derive a signer ID from a signing key.
/// - `secret`: Base58-encoded signing key with "signerSecret_z" prefix
/// Returns base58-encoded verifying key with "signer_z" prefix or error string.
pub fn get_signer_id_internal(secret: &str) -> Result<String, CryptoError> {
  let secret_bytes = bs58::decode(secret.strip_prefix("signerSecret_z").ok_or(
    CryptoError::InvalidPrefix("signerSecret_z", "signer secret"),
  )?)
  .into_vec()
  .map_err(|e| CryptoError::Base58Error(e.to_string()))?;

  let verifying_key = ed25519_verifying_key_internal(&secret_bytes)
    .map_err(|e| CryptoError::InvalidVerifyingKey(e.to_string()))?;

  Ok(format!(
    "signer_z{}",
    bs58::encode(verifying_key).into_string()
  ))
}

/// NAPI-exposed function to sign a message using Ed25519.
/// - `message`: Raw bytes to sign
/// - `secret`: Raw Ed25519 signing key bytes
/// Returns base58-encoded signature with "signature_z" prefix or throws JsError if signing fails.
#[napi]
pub fn sign(message: &[u8], secret: &[u8]) -> napi::Result<String> {
  let secret_str = std::str::from_utf8(secret).map_err(|e| {
    napi::Error::new(
      napi::Status::GenericFailure,
      format!("Invalid UTF-8 in secret: {:?}", e),
    )
  })?;
  sign_internal(message, secret_str)
    .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
}

/// NAPI-exposed function to verify an Ed25519 signature.
/// - `signature`: Raw signature bytes
/// - `message`: Raw bytes that were signed
/// - `id`: Raw Ed25519 verifying key bytes
/// Returns true if signature is valid, false otherwise, or throws JsError if verification fails.
#[napi]
pub fn verify(signature: &[u8], message: &[u8], id: &[u8]) -> napi::Result<bool> {
  let signature_str = std::str::from_utf8(signature).map_err(|e| {
    napi::Error::new(
      napi::Status::GenericFailure,
      format!("Invalid UTF-8 in signature: {:?}", e),
    )
  })?;
  let id_str = std::str::from_utf8(id).map_err(|e| {
    napi::Error::new(
      napi::Status::GenericFailure,
      format!("Invalid UTF-8 in id: {:?}", e),
    )
  })?;
  verify_internal(signature_str, message, id_str)
    .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
}

/// NAPI-exposed function to derive a signer ID from a signing key.
/// - `secret`: Raw Ed25519 signing key bytes
/// Returns base58-encoded verifying key with "signer_z" prefix or throws JsError if derivation fails.
#[napi]
pub fn get_signer_id(secret: &[u8]) -> napi::Result<String> {
  let secret_str = std::str::from_utf8(secret).map_err(|e| {
    napi::Error::new(
      napi::Status::GenericFailure,
      format!("Invalid UTF-8 in secret: {:?}", e),
    )
  })?;
  get_signer_id_internal(secret_str)
    .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::crypto::ed25519::new_ed25519_signing_key;

  #[test]
  fn test_sign_and_verify() {
    let message = b"hello world";

    // Create a test signing key
    let signing_key = new_ed25519_signing_key();
    let secret = format!("signerSecret_z{}", bs58::encode(&signing_key).into_string());

    // Sign the message
    let signature = sign_internal(message, &secret).unwrap();

    // Get the public key for verification
    let secret_bytes = bs58::decode(secret.strip_prefix("signerSecret_z").unwrap())
      .into_vec()
      .unwrap();
    let verifying_key = ed25519_verifying_key_internal(&secret_bytes).unwrap();
    let signer_id = format!("signer_z{}", bs58::encode(&verifying_key).into_string());

    // Verify the signature
    assert!(verify_internal(&signature, message, &signer_id).unwrap());
  }

  #[test]
  fn test_invalid_inputs() {
    let message = b"hello world";

    // Test invalid base58 in secret
    let result = sign_internal(message, "signerSecret_z!!!invalid!!!");
    assert!(matches!(result, Err(CryptoError::Base58Error(_))));

    // Test invalid signature format
    let result = verify_internal("not_a_signature", message, "signer_z123");
    assert!(matches!(
      result,
      Err(CryptoError::InvalidPrefix("signature_z", "signature"))
    ));

    // Test invalid signer ID format
    let result = verify_internal("signature_z123", message, "not_a_signer");
    assert!(matches!(
      result,
      Err(CryptoError::InvalidPrefix("signer_z", "signer ID"))
    ));
  }

  #[test]
  fn test_get_signer_id() {
    // Create a test signing key
    let signing_key = new_ed25519_signing_key();
    let secret = format!("signerSecret_z{}", bs58::encode(&signing_key).into_string());

    // Get signer ID
    let signer_id = get_signer_id_internal(&secret).unwrap();
    assert!(signer_id.starts_with("signer_z"));

    // Test that same secret produces same ID
    let signer_id2 = get_signer_id_internal(&secret).unwrap();
    assert_eq!(signer_id, signer_id2);

    // Test invalid secret format
    let result = get_signer_id_internal("invalid_secret");
    assert!(matches!(
      result,
      Err(CryptoError::InvalidPrefix(
        "signerSecret_z",
        "signer secret"
      ))
    ));

    // Test invalid base58
    let result = get_signer_id_internal("signerSecret_z!!!invalid!!!");
    assert!(matches!(result, Err(CryptoError::Base58Error(_))));
  }
}
