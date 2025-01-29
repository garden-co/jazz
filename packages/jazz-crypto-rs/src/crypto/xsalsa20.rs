use crate::error::CryptoError;
use crate::hash::blake3::generate_nonce;
use crypto_secretbox::{
    aead::{Aead, KeyInit},
    XSalsa20Poly1305,
};
use salsa20::cipher::{KeyIvInit, StreamCipher};
use salsa20::XSalsa20;
use wasm_bindgen::prelude::*;

/// XSalsa20 encryption without authentication
#[wasm_bindgen]
pub fn encrypt_xsalsa20(
    key: &[u8],
    nonce_material: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, JsError> {
    let nonce = generate_nonce(nonce_material);
    encrypt_xsalsa20_raw_internal(key, &nonce, plaintext).map_err(|e| JsError::new(&e.to_string()))
}

/// XSalsa20 decryption without authentication
#[wasm_bindgen]
pub fn decrypt_xsalsa20(
    key: &[u8],
    nonce_material: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, JsError> {
    let nonce = generate_nonce(nonce_material);
    decrypt_xsalsa20_raw_internal(key, &nonce, ciphertext).map_err(|e| JsError::new(&e.to_string()))
}

/// Internal function for raw XSalsa20 encryption without nonce generation
pub(crate) fn encrypt_xsalsa20_raw_internal(
    key: &[u8],
    nonce: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    // Key must be 32 bytes
    let key_bytes: [u8; 32] = key.try_into().map_err(|_| CryptoError::InvalidKeyLength)?;
    // Nonce must be 24 bytes
    let nonce_bytes: [u8; 24] = nonce
        .try_into()
        .map_err(|_| CryptoError::InvalidNonceLength)?;

    // Create cipher instance and encrypt
    let mut cipher = XSalsa20::new_from_slices(&key_bytes, &nonce_bytes)
        .map_err(|_| CryptoError::CipherError)?;
    let mut buffer = plaintext.to_vec();
    cipher.apply_keystream(&mut buffer);
    Ok(buffer)
}

/// Internal function for raw XSalsa20 decryption without nonce generation
pub(crate) fn decrypt_xsalsa20_raw_internal(
    key: &[u8],
    nonce: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    // Key must be 32 bytes
    let key_bytes: [u8; 32] = key.try_into().map_err(|_| CryptoError::InvalidKeyLength)?;
    // Nonce must be 24 bytes
    let nonce_bytes: [u8; 24] = nonce
        .try_into()
        .map_err(|_| CryptoError::InvalidNonceLength)?;

    // Create cipher instance and decrypt (XSalsa20 is symmetric)
    let mut cipher = XSalsa20::new_from_slices(&key_bytes, &nonce_bytes)
        .map_err(|_| CryptoError::CipherError)?;
    let mut buffer = ciphertext.to_vec();
    cipher.apply_keystream(&mut buffer);
    Ok(buffer)
}

/// XSalsa20-Poly1305 encryption
pub(crate) fn encrypt_xsalsa20_poly1305(
    key: &[u8],
    nonce: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    // Key must be 32 bytes
    let key_bytes: [u8; 32] = key.try_into().map_err(|_| CryptoError::InvalidKeyLength)?;
    // Nonce must be 24 bytes
    let nonce_bytes: [u8; 24] = nonce
        .try_into()
        .map_err(|_| CryptoError::InvalidNonceLength)?;

    // Create cipher instance
    let cipher = XSalsa20Poly1305::new(&key_bytes.into());

    // Encrypt the plaintext
    cipher
        .encrypt(&nonce_bytes.into(), plaintext)
        .map_err(|_| CryptoError::WrongTag)
}

/// XSalsa20-Poly1305 decryption
pub(crate) fn decrypt_xsalsa20_poly1305(
    key: &[u8],
    nonce: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    // Key must be 32 bytes
    let key_bytes: [u8; 32] = key.try_into().map_err(|_| CryptoError::InvalidKeyLength)?;
    // Nonce must be 24 bytes
    let nonce_bytes: [u8; 24] = nonce
        .try_into()
        .map_err(|_| CryptoError::InvalidNonceLength)?;

    // Create cipher instance
    let cipher = XSalsa20Poly1305::new(&key_bytes.into());

    // Decrypt the ciphertext
    cipher
        .decrypt(&nonce_bytes.into(), ciphertext)
        .map_err(|_| CryptoError::WrongTag)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xsalsa20() {
        // Test vectors
        let key = [0u8; 32]; // All zeros key
        let nonce = [0u8; 24]; // All zeros nonce
        let plaintext = b"Hello, World!";

        // Test encryption
        let ciphertext = encrypt_xsalsa20_raw_internal(&key, &nonce, plaintext).unwrap();
        assert_ne!(ciphertext, plaintext); // Ciphertext should be different from plaintext

        // Test decryption
        let decrypted = decrypt_xsalsa20_raw_internal(&key, &nonce, &ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);

        // Test that different nonce produces different ciphertext
        let nonce2 = [1u8; 24];
        let ciphertext2 = encrypt_xsalsa20_raw_internal(&key, &nonce2, plaintext).unwrap();
        assert_ne!(ciphertext, ciphertext2);

        // Test that different key produces different ciphertext
        let key2 = [1u8; 32];
        let ciphertext3 = encrypt_xsalsa20_raw_internal(&key2, &nonce, plaintext).unwrap();
        assert_ne!(ciphertext, ciphertext3);

        // Test invalid key length
        assert!(encrypt_xsalsa20_raw_internal(&key[..31], &nonce, plaintext).is_err());
        assert!(decrypt_xsalsa20_raw_internal(&key[..31], &nonce, &ciphertext).is_err());

        // Test invalid nonce length
        assert!(encrypt_xsalsa20_raw_internal(&key, &nonce[..23], plaintext).is_err());
        assert!(decrypt_xsalsa20_raw_internal(&key, &nonce[..23], &ciphertext).is_err());
    }

    #[test]
    fn test_xsalsa20_error_handling() {
        let key = [0u8; 32];
        let nonce = [0u8; 24];
        let plaintext = b"test message";

        // Test encryption with invalid key length
        let invalid_key = vec![0u8; 31]; // Too short
        let result = encrypt_xsalsa20_raw_internal(&invalid_key, &nonce, plaintext);
        assert!(result.is_err());

        // Test with too long key
        let too_long_key = vec![0u8; 33]; // Too long
        let result = encrypt_xsalsa20_raw_internal(&too_long_key, &nonce, plaintext);
        assert!(result.is_err());

        // Test decryption with invalid key length
        let ciphertext = encrypt_xsalsa20_raw_internal(&key, &nonce, plaintext).unwrap();
        let result = decrypt_xsalsa20_raw_internal(&invalid_key, &nonce, &ciphertext);
        assert!(result.is_err());

        // Test decryption with too long key
        let result = decrypt_xsalsa20_raw_internal(&too_long_key, &nonce, &ciphertext);
        assert!(result.is_err());

        // Test with invalid nonce length
        let invalid_nonce = vec![0u8; 23]; // Too short
        let result = encrypt_xsalsa20_raw_internal(&key, &invalid_nonce, plaintext);
        assert!(result.is_err());
        let result = decrypt_xsalsa20_raw_internal(&key, &invalid_nonce, &ciphertext);
        assert!(result.is_err());

        // Test with too long nonce
        let too_long_nonce = vec![0u8; 25]; // Too long
        let result = encrypt_xsalsa20_raw_internal(&key, &too_long_nonce, plaintext);
        assert!(result.is_err());
        let result = decrypt_xsalsa20_raw_internal(&key, &too_long_nonce, &ciphertext);
        assert!(result.is_err());
    }

    #[test]
    fn test_xsalsa20_poly1305() {
        // Test vectors from https://github.com/jedisct1/libsodium/blob/master/test/default/secretbox_test_vectors.h
        let key = [0u8; 32]; // All zeros key
        let nonce = [0u8; 24]; // All zeros nonce
        let plaintext = b"Hello, World!";

        // Test encryption
        let ciphertext = encrypt_xsalsa20_poly1305(&key, &nonce, plaintext).unwrap();
        assert!(ciphertext.len() > plaintext.len()); // Should include authentication tag

        // Test decryption
        let decrypted = decrypt_xsalsa20_poly1305(&key, &nonce, &ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);

        // Test that different nonce produces different ciphertext
        let nonce2 = [1u8; 24];
        let ciphertext2 = encrypt_xsalsa20_poly1305(&key, &nonce2, plaintext).unwrap();
        assert_ne!(ciphertext, ciphertext2);

        // Test that different key produces different ciphertext
        let key2 = [1u8; 32];
        let ciphertext3 = encrypt_xsalsa20_poly1305(&key2, &nonce, plaintext).unwrap();
        assert_ne!(ciphertext, ciphertext3);

        // Test that decryption fails with wrong key
        assert!(decrypt_xsalsa20_poly1305(&key2, &nonce, &ciphertext).is_err());

        // Test that decryption fails with wrong nonce
        assert!(decrypt_xsalsa20_poly1305(&key, &nonce2, &ciphertext).is_err());

        // Test that decryption fails with tampered ciphertext
        let mut tampered = ciphertext.clone();
        tampered[0] ^= 1;
        assert!(decrypt_xsalsa20_poly1305(&key, &nonce, &tampered).is_err());
    }
}
