use cojson_core::crypto::signature;
use napi_derive::napi;

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
  signature::sign(message, secret_str)
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
  signature::verify(signature_str, message, id_str)
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
  signature::get_signer_id(secret_str)
    .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
}
