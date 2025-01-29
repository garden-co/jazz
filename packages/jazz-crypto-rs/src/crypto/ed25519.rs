use crate::error::CryptoError;
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use wasm_bindgen::prelude::*;

/// Generate a new Ed25519 signing key
#[wasm_bindgen]
pub fn new_ed25519_signing_key() -> Vec<u8> {
    let mut rng = OsRng;
    let signing_key = SigningKey::generate(&mut rng);
    signing_key.to_bytes().to_vec()
}

pub(crate) fn ed25519_verifying_key_internal(signing_key: &[u8]) -> Result<Vec<u8>, CryptoError> {
    let key_bytes: [u8; 32] = signing_key
        .try_into()
        .map_err(|_| CryptoError::InvalidKeyLength)?;
    let signing_key = SigningKey::from_bytes(&key_bytes);
    Ok(signing_key.verifying_key().to_bytes().to_vec())
}

#[wasm_bindgen]
pub fn ed25519_verifying_key(signing_key: &[u8]) -> Result<Vec<u8>, JsError> {
    ed25519_verifying_key_internal(signing_key).map_err(|e| JsError::new(&e.to_string()))
}

pub(crate) fn ed25519_sign_internal(
    signing_key: &[u8],
    message: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let key_bytes: [u8; 32] = signing_key
        .try_into()
        .map_err(|_| CryptoError::InvalidKeyLength)?;
    let signing_key = SigningKey::from_bytes(&key_bytes);
    Ok(signing_key.sign(message).to_bytes().to_vec())
}

#[wasm_bindgen]
pub fn ed25519_sign(signing_key: &[u8], message: &[u8]) -> Result<Vec<u8>, JsError> {
    ed25519_sign_internal(signing_key, message).map_err(|e| JsError::new(&e.to_string()))
}

pub(crate) fn ed25519_verify_internal(
    verifying_key: &[u8],
    message: &[u8],
    signature: &[u8],
) -> Result<bool, CryptoError> {
    let key_bytes: [u8; 32] = verifying_key
        .try_into()
        .map_err(|_| CryptoError::InvalidKeyLength)?;
    let verifying_key = VerifyingKey::from_bytes(&key_bytes)
        .map_err(|e| CryptoError::InvalidVerifyingKey(e.to_string()))?;

    let sig_bytes: [u8; 64] = signature
        .try_into()
        .map_err(|_| CryptoError::InvalidSignatureLength)?;
    let signature = ed25519_dalek::Signature::from_bytes(&sig_bytes);

    Ok(verifying_key.verify(message, &signature).is_ok())
}

#[wasm_bindgen]
pub fn ed25519_verify(
    verifying_key: &[u8],
    message: &[u8],
    signature: &[u8],
) -> Result<bool, JsError> {
    ed25519_verify_internal(verifying_key, message, signature)
        .map_err(|e| JsError::new(&e.to_string()))
}

#[wasm_bindgen]
pub fn ed25519_signing_key_from_bytes(bytes: &[u8]) -> Result<Vec<u8>, JsError> {
    let key_bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| JsError::new("Invalid signing key length"))?;
    Ok(key_bytes.to_vec())
}

#[wasm_bindgen]
pub fn ed25519_signing_key_to_public(signing_key: &[u8]) -> Result<Vec<u8>, JsError> {
    ed25519_verifying_key_internal(signing_key).map_err(|e| JsError::new(&e.to_string()))
}

#[wasm_bindgen]
pub fn ed25519_signing_key_sign(signing_key: &[u8], message: &[u8]) -> Result<Vec<u8>, JsError> {
    ed25519_sign_internal(signing_key, message).map_err(|e| JsError::new(&e.to_string()))
}

#[wasm_bindgen]
pub fn ed25519_verifying_key_from_bytes(bytes: &[u8]) -> Result<Vec<u8>, JsError> {
    let key_bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| JsError::new("Invalid verifying key length"))?;
    Ok(key_bytes.to_vec())
}

#[wasm_bindgen]
pub fn ed25519_signature_from_bytes(bytes: &[u8]) -> Result<Vec<u8>, JsError> {
    let sig_bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| JsError::new("Invalid signature length"))?;
    Ok(sig_bytes.to_vec())
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
        let verifying_key = ed25519_verifying_key_internal(&signing_key).unwrap();
        assert_eq!(verifying_key.len(), 32, "Verifying key should be 32 bytes");

        // Test that different signing keys produce different verifying keys
        let signing_key2 = new_ed25519_signing_key();
        let verifying_key2 = ed25519_verifying_key_internal(&signing_key2).unwrap();
        assert_ne!(
            verifying_key, verifying_key2,
            "Different signing keys should produce different verifying keys"
        );

        // Test signing and verification
        let message = b"Test message";
        let signature = ed25519_sign_internal(&signing_key, message).unwrap();
        assert_eq!(signature.len(), 64, "Signature should be 64 bytes");

        // Test successful verification
        let verification_result =
            ed25519_verify_internal(&verifying_key, message, &signature).unwrap();
        assert!(
            verification_result,
            "Valid signature should verify successfully"
        );

        // Test verification with wrong message
        let wrong_message = b"Wrong message";
        let wrong_verification =
            ed25519_verify_internal(&verifying_key, wrong_message, &signature).unwrap();
        assert!(
            !wrong_verification,
            "Signature should not verify with wrong message"
        );

        // Test verification with wrong key
        let wrong_verification =
            ed25519_verify_internal(&verifying_key2, message, &signature).unwrap();
        assert!(
            !wrong_verification,
            "Signature should not verify with wrong key"
        );

        // Test verification with tampered signature
        let mut tampered_signature = signature.clone();
        tampered_signature[0] ^= 1;
        let wrong_verification =
            ed25519_verify_internal(&verifying_key, message, &tampered_signature).unwrap();
        assert!(!wrong_verification, "Tampered signature should not verify");
    }

    #[test]
    fn test_ed25519_error_cases() {
        // Test invalid signing key length
        let invalid_signing_key = vec![0u8; 31]; // Too short
        let result = ed25519_verifying_key_internal(&invalid_signing_key);
        assert!(result.is_err());
        let result = ed25519_sign_internal(&invalid_signing_key, b"test");
        assert!(result.is_err());

        // Test invalid verifying key length
        let invalid_verifying_key = vec![0u8; 31]; // Too short
        let valid_signing_key = new_ed25519_signing_key();
        let valid_signature = ed25519_sign_internal(&valid_signing_key, b"test").unwrap();
        let result = ed25519_verify_internal(&invalid_verifying_key, b"test", &valid_signature);
        assert!(result.is_err());

        // Test invalid signature length
        let valid_verifying_key = ed25519_verifying_key_internal(&valid_signing_key).unwrap();
        let invalid_signature = vec![0u8; 63]; // Too short
        let result = ed25519_verify_internal(&valid_verifying_key, b"test", &invalid_signature);
        assert!(result.is_err());

        // Test with too long keys
        let too_long_key = vec![0u8; 33]; // Too long
        let result = ed25519_verifying_key_internal(&too_long_key);
        assert!(result.is_err());
        let result = ed25519_sign_internal(&too_long_key, b"test");
        assert!(result.is_err());

        // Test with too long signature
        let too_long_signature = vec![0u8; 65]; // Too long
        let result = ed25519_verify_internal(&valid_verifying_key, b"test", &too_long_signature);
        assert!(result.is_err());
    }
}
