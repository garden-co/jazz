use super::ed25519::CryptoErrorUniffi;
use cojson_core::crypto::signature;

/// Uniffi-exposed function to sign a message using Ed25519.
/// - `message`: Raw bytes to sign
/// - `secret`: UTF-8 encoded Ed25519 signing key string
/// Returns base58-encoded signature with "signature_z" prefix or throws an error if signing fails.
#[uniffi::export]
pub fn sign(message: &[u8], secret: String) -> Result<String, CryptoErrorUniffi> {
    signature::sign(message, &secret).map_err(Into::into)
}

/// Uniffi-exposed function to verify an Ed25519 signature.
/// - `signature`: Base58-encoded signature string
/// - `message`: Raw bytes that were signed
/// - `id`: Base58-encoded verifying key string
/// Returns true if signature is valid, false otherwise, or throws an error if verification fails.
#[uniffi::export]
pub fn verify(signature: String, message: &[u8], id: String) -> Result<bool, CryptoErrorUniffi> {
    signature::verify(&signature, message, &id).map_err(Into::into)
}

/// Uniffi-exposed function to derive a signer ID from a signing key.
/// - `secret`: UTF-8 encoded Ed25519 signing key string
/// Returns base58-encoded verifying key with "signer_z" prefix or throws an error if derivation fails.
#[uniffi::export]
pub fn get_signer_id(secret: String) -> Result<String, CryptoErrorUniffi> {
    signature::get_signer_id(&secret).map_err(Into::into)
}
