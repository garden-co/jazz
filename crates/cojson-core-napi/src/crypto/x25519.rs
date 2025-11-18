use cojson_core::crypto::x25519;
use napi::bindgen_prelude::Uint8Array;
use napi_derive::napi;

/// Generate a new X25519 private key using secure random number generation.
/// Returns 32 bytes of raw key material suitable for use with other X25519 functions.
/// This key can be reused for multiple Diffie-Hellman exchanges.
#[napi]
pub fn new_x25519_private_key() -> Uint8Array {
  x25519::new_x25519_private_key().into()
}

/// NAPI-exposed function to derive an X25519 public key from a private key.
/// - `private_key`: 32 bytes of private key material
/// Returns 32 bytes of public key material or throws JsError if key is invalid.
#[napi]
pub fn x25519_public_key(private_key: &[u8]) -> napi::Result<Uint8Array> {
  x25519::x25519_public_key(private_key)
    .map(|public_key| public_key.into())
    .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
}

/// NAPI-exposed function to perform X25519 Diffie-Hellman key exchange.
/// - `private_key`: 32 bytes of private key material
/// - `public_key`: 32 bytes of public key material
/// Returns 32 bytes of shared secret material or throws JsError if key exchange fails.
#[napi]
pub fn x25519_diffie_hellman(private_key: &[u8], public_key: &[u8]) -> napi::Result<Uint8Array> {
  x25519::x25519_diffie_hellman(private_key, public_key)
    .map(|shared_secret| shared_secret.into())
    .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
}

/// NAPI-exposed function to derive a sealer ID from a sealer secret.
/// - `secret`: Raw bytes of the sealer secret
/// Returns a base58-encoded sealer ID with "sealer_z" prefix or throws JsError if derivation fails.
#[napi]
pub fn get_sealer_id(secret: &[u8]) -> napi::Result<String> {
  let secret_str = std::str::from_utf8(secret).map_err(|e| {
    napi::Error::new(
      napi::Status::GenericFailure,
      format!("Invalid UTF-8 in secret: {:?}", e),
    )
  })?;
  x25519::get_sealer_id(secret_str)
    .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
}
