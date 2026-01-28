use crate::crypto::error::CryptoError;
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;

/// Generate a new Ed25519 signing key using secure random number generation.
/// Returns 32 bytes of raw key material suitable for use with other Ed25519 functions.
pub fn new_ed25519_signing_key() -> Box<[u8]> {
    let mut rng = OsRng;
    let signing_key = SigningKey::generate(&mut rng);
    signing_key.to_bytes().into()
}

/// Internal function to derive an Ed25519 verifying key from a signing key.
/// Takes 32 bytes of signing key material and returns 32 bytes of verifying key material.
/// Returns CryptoError if the key length is invalid.
pub fn ed25519_verifying_key(signing_key: &[u8]) -> Result<Box<[u8]>, CryptoError> {
    let key_bytes: [u8; 32] = signing_key
        .try_into()
        .map_err(|_| CryptoError::InvalidKeyLength(32, signing_key.len()))?;
    let signing_key = SigningKey::from_bytes(&key_bytes);
    Ok(signing_key.verifying_key().to_bytes().into())
}

/// Internal function to sign a message using Ed25519.
/// Takes 32 bytes of signing key material and arbitrary message bytes.
/// Returns 64 bytes of signature material or CryptoError if key is invalid.
pub fn ed25519_sign(signing_key: &[u8], message: &[u8]) -> Result<[u8; 64], CryptoError> {
    let key_bytes: [u8; 32] = signing_key
        .try_into()
        .map_err(|_| CryptoError::InvalidKeyLength(32, signing_key.len()))?;
    let signing_key = SigningKey::from_bytes(&key_bytes);
    Ok(signing_key.sign(message).to_bytes())
}

/// Internal function to verify an Ed25519 signature.
/// - `verifying_key`: 32 bytes of verifying key material
/// - `message`: Raw bytes that were signed
/// - `signature`: 64 bytes of signature material
/// Returns true if signature is valid, false otherwise, or CryptoError if key/signature format is invalid.
pub fn ed25519_verify(
    verifying_key: &[u8],
    message: &[u8],
    signature: &[u8],
) -> Result<bool, CryptoError> {
    let key_bytes: [u8; 32] = verifying_key
        .try_into()
        .map_err(|_| CryptoError::InvalidKeyLength(32, verifying_key.len()))?;
    let verifying_key = VerifyingKey::from_bytes(&key_bytes)
        .map_err(|e| CryptoError::InvalidVerifyingKey(e.to_string()))?;

    let sig_bytes: [u8; 64] = signature
        .try_into()
        .map_err(|_| CryptoError::InvalidSignatureLength)?;
    let signature = ed25519_dalek::Signature::from_bytes(&sig_bytes);

    Ok(verifying_key.verify(message, &signature).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ed25519_key_generation_and_signing() {
        // Test key generation
        let signing_key = new_ed25519_signing_key();
        assert_eq!(signing_key.len(), 32, "Signing key should be 32 bytes");

        // Test verifying key derivation
        let verifying_key = ed25519_verifying_key(&signing_key).unwrap();
        assert_eq!(verifying_key.len(), 32, "Verifying key should be 32 bytes");

        // Test that different signing keys produce different verifying keys
        let signing_key2 = new_ed25519_signing_key();
        let verifying_key2 = ed25519_verifying_key(&signing_key2).unwrap();
        assert_ne!(
            verifying_key, verifying_key2,
            "Different signing keys should produce different verifying keys"
        );

        // Test signing and verification
        let message = b"Test message";
        let signature = ed25519_sign(&signing_key, message).unwrap();
        assert_eq!(signature.len(), 64, "Signature should be 64 bytes");

        // Test successful verification
        let verification_result = ed25519_verify(&verifying_key, message, &signature).unwrap();
        assert!(
            verification_result,
            "Valid signature should verify successfully"
        );

        // Test verification with wrong message
        let wrong_message = b"Wrong message";
        let wrong_verification = ed25519_verify(&verifying_key, wrong_message, &signature).unwrap();
        assert!(
            !wrong_verification,
            "Signature should not verify with wrong message"
        );

        // Test verification with wrong key
        let wrong_verification = ed25519_verify(&verifying_key2, message, &signature).unwrap();
        assert!(
            !wrong_verification,
            "Signature should not verify with wrong key"
        );

        // Test verification with tampered signature
        let mut tampered_signature = signature.clone();
        tampered_signature[0] ^= 1;
        let wrong_verification =
            ed25519_verify(&verifying_key, message, &tampered_signature).unwrap();
        assert!(!wrong_verification, "Tampered signature should not verify");
    }

    #[test]
    fn test_ed25519_error_cases() {
        // Test invalid signing key length
        let invalid_signing_key = vec![0u8; 31]; // Too short
        let result = ed25519_verifying_key(&invalid_signing_key);
        assert!(result.is_err());
        let result = ed25519_sign(&invalid_signing_key, b"test");
        assert!(result.is_err());

        // Test invalid verifying key length
        let invalid_verifying_key = vec![0u8; 31]; // Too short
        let valid_signing_key = new_ed25519_signing_key();
        let valid_signature = ed25519_sign(&valid_signing_key, b"test").unwrap();
        let result = ed25519_verify(&invalid_verifying_key, b"test", &valid_signature);
        assert!(result.is_err());

        // Test invalid signature length
        let valid_verifying_key = ed25519_verifying_key(&valid_signing_key).unwrap();
        let invalid_signature = vec![0u8; 63]; // Too short
        let result = ed25519_verify(&valid_verifying_key, b"test", &invalid_signature);
        assert!(result.is_err());

        // Test with too long keys
        let too_long_key = vec![0u8; 33]; // Too long
        let result = ed25519_verifying_key(&too_long_key);
        assert!(result.is_err());
        let result = ed25519_sign(&too_long_key, b"test");
        assert!(result.is_err());

        // Test with too long signature
        let too_long_signature = vec![0u8; 65]; // Too long
        let result = ed25519_verify(&valid_verifying_key, b"test", &too_long_signature);
        assert!(result.is_err());
    }
}
