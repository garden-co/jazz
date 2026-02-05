use cojson_core::crypto::{ed25519, CryptoError};
use thiserror::Error;

#[derive(Error, Debug, uniffi::Error)]
pub enum CryptoErrorUniffi {
    #[error("Invalid key length (expected {0}, got {1})")]
    InvalidKeyLength(u64, u64),
    #[error("Invalid nonce length")]
    InvalidNonceLength,
    #[error("Invalid sealer secret format: must start with 'sealerSecret_z'")]
    InvalidSealerSecretFormat,
    #[error("Invalid signature length")]
    InvalidSignatureLength,
    #[error("Invalid verifying key: {0}")]
    InvalidVerifyingKey(String),
    #[error("Invalid public key: {0}")]
    InvalidPublicKey(String),
    #[error("Wrong tag")]
    WrongTag,
    #[error("Failed to create cipher")]
    CipherError,
    #[error("Invalid prefix: {0} must start with '{1}'")]
    InvalidPrefix(String, String),
    #[error("Invalid base58: {0}")]
    Base58Error(String),
    #[error("Invalid base64: {0}")]
    Base64DecodeError(String),
}

impl From<CryptoError> for CryptoErrorUniffi {
    fn from(error: CryptoError) -> Self {
        match error {
            CryptoError::InvalidKeyLength(expected, actual) => {
                CryptoErrorUniffi::InvalidKeyLength(expected as u64, actual as u64).into()
            }
            CryptoError::InvalidNonceLength => CryptoErrorUniffi::InvalidNonceLength,
            CryptoError::InvalidSealerSecretFormat => CryptoErrorUniffi::InvalidSealerSecretFormat,
            CryptoError::InvalidSignatureLength => CryptoErrorUniffi::InvalidSignatureLength,
            CryptoError::InvalidVerifyingKey(key) => CryptoErrorUniffi::InvalidVerifyingKey(key),
            CryptoError::InvalidPublicKey(key) => CryptoErrorUniffi::InvalidPublicKey(key),
            CryptoError::WrongTag => CryptoErrorUniffi::WrongTag,
            CryptoError::CipherError => CryptoErrorUniffi::CipherError,
            CryptoError::InvalidPrefix(prefix, field) => {
                CryptoErrorUniffi::InvalidPrefix(prefix.to_string(), field.to_string())
            }
            CryptoError::Base58Error(error) => CryptoErrorUniffi::Base58Error(error),
        }
    }
}

/// Generate a new Ed25519 signing key using secure random number generation.
/// Returns 32 bytes of raw key material suitable for use with other Ed25519 functions.
#[uniffi::export]
pub fn new_ed25519_signing_key() -> Vec<u8> {
    ed25519::new_ed25519_signing_key().into()
}

/// uniffi-exposed function to derive an Ed25519 verifying key from a signing key.
/// - `signing_key`: 32 bytes of signing key material
/// Returns 32 bytes of verifying key material or throws CryptoErrorUniffi if key is invalid.
#[uniffi::export]
pub fn ed25519_verifying_key(signing_key: &[u8]) -> Result<Vec<u8>, CryptoErrorUniffi> {
    ed25519::ed25519_verifying_key(signing_key)
        .map(|key| key.into())
        .map_err(Into::into)
}

/// uniffi-exposed function to sign a message using Ed25519.
/// - `signing_key`: 32 bytes of signing key material
/// - `message`: Raw bytes to sign
/// Returns 64 bytes of signature material or throws CryptoErrorUniffi if signing fails.
#[uniffi::export]
pub fn ed25519_sign(signing_key: &[u8], message: &[u8]) -> Result<Vec<u8>, CryptoErrorUniffi> {
    ed25519::ed25519_sign(signing_key, message)
        .map(|signature| signature.into())
        .map_err(Into::into)
}

/// uniffi-exposed function to verify an Ed25519 signature.
/// - `verifying_key`: 32 bytes of verifying key material
/// - `message`: Raw bytes that were signed
/// - `signature`: 64 bytes of signature material
/// Returns true if signature is valid, false otherwise, or throws CryptoErrorUniffi if verification fails.
#[uniffi::export]
pub fn ed25519_verify(
    verifying_key: &[u8],
    message: &[u8],
    signature: &[u8],
) -> Result<bool, CryptoErrorUniffi> {
    ed25519::ed25519_verify(verifying_key, message, signature).map_err(Into::into)
}

/// uniffi-exposed function to validate and copy Ed25519 signing key bytes.
/// - `bytes`: 32 bytes of signing key material to validate
/// Returns the same 32 bytes if valid or throws CryptoErrorUniffi if invalid.
#[uniffi::export]
pub fn ed25519_signing_key_from_bytes(bytes: &[u8]) -> Result<Vec<u8>, CryptoErrorUniffi> {
    let key_bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| CryptoErrorUniffi::InvalidKeyLength(32, bytes.len() as u64))?;
    Ok(key_bytes.into())
}

/// uniffi-exposed function to derive the public key from an Ed25519 signing key.
/// - `signing_key`: 32 bytes of signing key material
/// Returns 32 bytes of public key material or throws CryptoErrorUniffi if key is invalid.
#[uniffi::export]
pub fn ed25519_signing_key_to_public(signing_key: &[u8]) -> Result<Vec<u8>, CryptoErrorUniffi> {
    ed25519::ed25519_verifying_key(signing_key)
        .map(|key| key.into())
        .map_err(Into::into)
}

/// uniffi-exposed function to sign a message with an Ed25519 signing key.
/// - `signing_key`: 32 bytes of signing key material
/// - `message`: Raw bytes to sign
/// Returns 64 bytes of signature material or throws CryptoErrorUniffi if signing fails.
#[uniffi::export]
pub fn ed25519_signing_key_sign(
    signing_key: &[u8],
    message: &[u8],
) -> Result<Vec<u8>, CryptoErrorUniffi> {
    ed25519::ed25519_sign(signing_key, message)
        .map(|signature| signature.into())
        .map_err(Into::into)
}

/// uniffi-exposed function to validate and copy Ed25519 verifying key bytes.
/// - `bytes`: 32 bytes of verifying key material to validate
/// Returns the same 32 bytes if valid or throws CryptoErrorUniffi if invalid.
#[uniffi::export]
pub fn ed25519_verifying_key_from_bytes(bytes: &[u8]) -> Result<Vec<u8>, CryptoErrorUniffi> {
    let key_bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| CryptoErrorUniffi::InvalidKeyLength(32, bytes.len() as u64))?;
    Ok(key_bytes.into())
}

/// uniffi-exposed function to validate and copy Ed25519 signature bytes.
/// - `bytes`: 64 bytes of signature material to validate
/// Returns the same 64 bytes if valid or throws CryptoErrorUniffi if invalid.
#[uniffi::export]
pub fn ed25519_signature_from_bytes(bytes: &[u8]) -> Result<Vec<u8>, CryptoErrorUniffi> {
    let sig_bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| CryptoErrorUniffi::InvalidKeyLength(64, bytes.len() as u64))?;
    Ok(sig_bytes.into())
}
