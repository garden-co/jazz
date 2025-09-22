use crate::error::CryptoError;
use bs58;
use napi::bindgen_prelude::Uint8Array;
use napi_derive::napi;
use x25519_dalek::{PublicKey, StaticSecret};

/// Generate a new X25519 private key using secure random number generation.
/// Returns 32 bytes of raw key material suitable for use with other X25519 functions.
/// This key can be reused for multiple Diffie-Hellman exchanges.
#[napi]
pub fn new_x25519_private_key() -> Uint8Array {
  let secret = StaticSecret::random();
  secret.to_bytes().into()
}

/// Internal function to derive an X25519 public key from a private key.
/// Takes 32 bytes of private key material and returns 32 bytes of public key material.
/// Returns CryptoError if the key length is invalid.
pub(crate) fn x25519_public_key_internal(private_key: &[u8]) -> Result<[u8; 32], CryptoError> {
  let bytes: [u8; 32] = private_key
    .try_into()
    .map_err(|_| CryptoError::InvalidKeyLength(32, private_key.len()))?;
  let secret = StaticSecret::from(bytes);
  Ok(PublicKey::from(&secret).to_bytes())
}

/// NAPI-exposed function to derive an X25519 public key from a private key.
/// - `private_key`: 32 bytes of private key material
/// Returns 32 bytes of public key material or throws JsError if key is invalid.
#[napi]
pub fn x25519_public_key(private_key: &[u8]) -> napi::Result<Uint8Array> {
  napi::Result::Ok(x25519_public_key_internal(private_key)?.into())
}

/// Internal function to perform X25519 Diffie-Hellman key exchange.
/// Takes 32 bytes each of private and public key material.
/// Returns 32 bytes of shared secret material or CryptoError if key lengths are invalid.
pub(crate) fn x25519_diffie_hellman_internal(
  private_key: &[u8],
  public_key: &[u8],
) -> Result<[u8; 32], CryptoError> {
  let private_bytes: [u8; 32] = private_key
    .try_into()
    .map_err(|_| CryptoError::InvalidKeyLength(32, private_key.len()))?;
  let public_bytes: [u8; 32] = public_key
    .try_into()
    .map_err(|_| CryptoError::InvalidKeyLength(32, public_key.len()))?;
  let secret = StaticSecret::from(private_bytes);
  let public = PublicKey::from(public_bytes);
  Ok(secret.diffie_hellman(&public).to_bytes())
}

/// NAPI-exposed function to perform X25519 Diffie-Hellman key exchange.
/// - `private_key`: 32 bytes of private key material
/// - `public_key`: 32 bytes of public key material
/// Returns 32 bytes of shared secret material or throws JsError if key exchange fails.
#[napi]
pub fn x25519_diffie_hellman(private_key: &[u8], public_key: &[u8]) -> napi::Result<Uint8Array> {
  Ok(x25519_diffie_hellman_internal(private_key, public_key)?.into())
}

/// Internal function to derive a sealer ID from a sealer secret.
/// Takes a base58-encoded sealer secret with "sealerSecret_z" prefix.
/// Returns a base58-encoded sealer ID with "sealer_z" prefix or error string if format is invalid.
pub fn get_sealer_id_internal(secret: &str) -> Result<String, CryptoError> {
  let private_bytes = bs58::decode(secret.strip_prefix("sealerSecret_z").ok_or(
    CryptoError::InvalidPrefix("sealerSecret_z", "sealer secret"),
  )?)
  .into_vec()
  .map_err(|e| CryptoError::Base58Error(e.to_string()))?;

  let public_bytes = x25519_public_key_internal(&private_bytes)
    .map_err(|e| CryptoError::InvalidPublicKey(e.to_string()))?;

  Ok(format!(
    "sealer_z{}",
    bs58::encode(public_bytes).into_string()
  ))
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
  get_sealer_id_internal(secret_str).map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_x25519_key_generation() {
    // Test that we get the correct length keys
    let private_key = new_x25519_private_key();
    assert_eq!(private_key.len(), 32);

    // Test that public key generation works and produces correct length
    let public_key = x25519_public_key_internal(&private_key).unwrap();
    assert_eq!(public_key.len(), 32);

    // Test that different private keys produce different public keys
    let private_key2 = new_x25519_private_key();
    let public_key2 = x25519_public_key_internal(&private_key2).unwrap();
    assert_ne!(public_key, public_key2);
  }

  #[test]
  fn test_x25519_key_exchange() {
    // Generate sender's keypair
    let sender_private = new_x25519_private_key();
    let sender_public = x25519_public_key_internal(&sender_private).unwrap();

    // Generate recipient's keypair
    let recipient_private = new_x25519_private_key();
    let recipient_public = x25519_public_key_internal(&recipient_private).unwrap();

    // Test properties we expect from the shared secret
    let shared_secret1 =
      x25519_diffie_hellman_internal(&sender_private, &recipient_public).unwrap();
    let shared_secret2 =
      x25519_diffie_hellman_internal(&recipient_private, &sender_public).unwrap();

    // Both sides should arrive at the same shared secret
    assert_eq!(shared_secret1, shared_secret2);

    // Shared secret should be 32 bytes
    assert_eq!(shared_secret1.len(), 32);

    // Different recipient should produce different shared secret
    let other_recipient_private = new_x25519_private_key();
    let other_recipient_public = x25519_public_key_internal(&other_recipient_private).unwrap();
    let different_shared_secret =
      x25519_diffie_hellman_internal(&sender_private, &other_recipient_public).unwrap();
    assert_ne!(shared_secret1, different_shared_secret);
  }

  #[test]
  fn test_get_sealer_id() {
    // Create a test private key
    let private_key = new_x25519_private_key();
    let secret = format!("sealerSecret_z{}", bs58::encode(&private_key).into_string());

    // Get sealer ID
    let sealer_id = get_sealer_id_internal(&secret).unwrap();
    assert!(sealer_id.starts_with("sealer_z"));

    // Test that same secret produces same ID
    let sealer_id2 = get_sealer_id_internal(&secret).unwrap();
    assert_eq!(sealer_id, sealer_id2);

    // Test invalid secret format
    let result = get_sealer_id_internal("invalid_secret");
    assert!(matches!(
      result,
      Err(CryptoError::InvalidPrefix(
        "sealerSecret_z",
        "sealer secret"
      ))
    ));

    // Test invalid base58
    let result = get_sealer_id_internal("sealerSecret_z!!!invalid!!!");
    assert!(matches!(result, Err(CryptoError::Base58Error(_))));
  }
}
