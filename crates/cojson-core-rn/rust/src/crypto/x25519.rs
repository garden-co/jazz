use super::ed25519::CryptoErrorUniffi;
use cojson_core::crypto::x25519;

/// Generate a new X25519 private key using secure random number generation.
/// Returns 32 bytes of raw key material suitable for use with other X25519 functions.
/// This key can be reused for multiple Diffie-Hellman exchanges.
#[uniffi::export]
pub fn new_x25519_private_key() -> Vec<u8> {
    x25519::new_x25519_private_key().into()
}

/// Uniffi-exposed function to derive an X25519 public key from a private key.
/// - `private_key`: 32 bytes of private key material
/// Returns 32 bytes of public key material or throws an error if key is invalid.
#[uniffi::export]
pub fn x25519_public_key(private_key: &[u8]) -> Result<Vec<u8>, CryptoErrorUniffi> {
    x25519::x25519_public_key(private_key)
        .map(|public_key| public_key.into())
        .map_err(Into::into)
}

/// Uniffi-exposed function to perform X25519 Diffie-Hellman key exchange.
/// - `private_key`: 32 bytes of private key material
/// - `public_key`: 32 bytes of public key material
/// Returns 32 bytes of shared secret material or throws an error if key exchange fails.
#[uniffi::export]
pub fn x25519_diffie_hellman(
    private_key: &[u8],
    public_key: &[u8],
) -> Result<Vec<u8>, CryptoErrorUniffi> {
    x25519::x25519_diffie_hellman(private_key, public_key)
        .map(|shared_secret| shared_secret.into())
        .map_err(Into::into)
}

/// Uniffi-exposed function to derive a sealer ID from a sealer secret.
/// - `secret`: UTF-8 encoded sealer secret string
/// Returns a base58-encoded sealer ID with "sealer_z" prefix or throws an error if derivation fails.
#[uniffi::export]
pub fn get_sealer_id(secret: String) -> Result<String, CryptoErrorUniffi> {
    x25519::get_sealer_id(&secret).map_err(Into::into)
}
