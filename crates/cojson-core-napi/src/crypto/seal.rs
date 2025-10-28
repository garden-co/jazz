use cojson_core::crypto::seal as seal_crypto;
use napi::bindgen_prelude::Uint8Array;
use napi_derive::napi;

/// NAPI-exposed function for sealing a message using X25519 + XSalsa20-Poly1305.
/// Provides authenticated encryption with perfect forward secrecy.
/// - `message`: Raw bytes to seal
/// - `sender_secret`: Base58-encoded sender's private key with "sealerSecret_z" prefix
/// - `recipient_id`: Base58-encoded recipient's public key with "sealer_z" prefix
/// - `nonce_material`: Raw bytes used to generate the nonce
/// Returns sealed bytes or throws JsError if sealing fails.
#[napi]
pub fn seal(
  message: &[u8],
  sender_secret: String,
  recipient_id: String,
  nonce_material: &[u8],
) -> napi::Result<Uint8Array> {
  seal_crypto::seal(message, &sender_secret, &recipient_id, nonce_material)
    .map(|sealed| sealed.into())
    .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
}

/// NAPI-exposed function for unsealing a message using X25519 + XSalsa20-Poly1305.
/// Provides authenticated decryption with perfect forward secrecy.
/// - `sealed_message`: The sealed bytes to decrypt
/// - `recipient_secret`: Base58-encoded recipient's private key with "sealerSecret_z" prefix
/// - `sender_id`: Base58-encoded sender's public key with "sealer_z" prefix
/// - `nonce_material`: Raw bytes used to generate the nonce (must match sealing)
/// Returns unsealed bytes or throws JsError if unsealing fails.
#[napi]
pub fn unseal(
  sealed_message: &[u8],
  recipient_secret: String,
  sender_id: String,
  nonce_material: &[u8],
) -> napi::Result<Uint8Array> {
  seal_crypto::unseal(
    sealed_message,
    &recipient_secret,
    &sender_id,
    nonce_material,
  )
  .map(|unsealed| unsealed.into())
  .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
}
