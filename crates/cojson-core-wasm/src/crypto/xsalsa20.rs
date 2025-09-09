use cojson_core::crypto::xsalsa20::{encrypt_xsalsa20, decrypt_xsalsa20};
use wasm_bindgen::prelude::*;

/// WASM-exposed function for XSalsa20 encryption without authentication.
/// - `key`: 32-byte key for encryption
/// - `nonce_material`: Raw bytes used to generate a 24-byte nonce via BLAKE3
/// - `plaintext`: Raw bytes to encrypt
/// Returns the encrypted bytes or throws a JsError if encryption fails.
/// Note: This function does not provide authentication. Use encrypt_xsalsa20_poly1305 for authenticated encryption.
#[wasm_bindgen]
pub fn encrypt_xsalsa20_wasm(
    key: &[u8],
    nonce_material: &[u8],
    plaintext: &[u8],
) -> Result<Box<[u8]>, JsError> {
    Ok(encrypt_xsalsa20(key, nonce_material, plaintext)?.into())
}

/// WASM-exposed function for XSalsa20 decryption without authentication.
/// - `key`: 32-byte key for decryption (must match encryption key)
/// - `nonce_material`: Raw bytes used to generate a 24-byte nonce (must match encryption)
/// - `ciphertext`: Encrypted bytes to decrypt
/// Returns the decrypted bytes or throws a JsError if decryption fails.
/// Note: This function does not provide authentication. Use decrypt_xsalsa20_poly1305 for authenticated decryption.
#[wasm_bindgen]
pub fn decrypt_xsalsa20_wasm(
    key: &[u8],
    nonce_material: &[u8],
    ciphertext: &[u8],
) -> Result<Box<[u8]>, JsError> {
    Ok(decrypt_xsalsa20(key, nonce_material, ciphertext)?.into())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xsalsa20() {
        // Test vectors
        let key = [0u8; 32]; // All zeros key
        let nonce_material = [0u8; 24]; // All zeros nonce material
        let plaintext = b"Hello, World!";

        // Test encryption
        let ciphertext = encrypt_xsalsa20(&key, &nonce_material, plaintext).unwrap();
        assert_ne!(&*ciphertext, plaintext); // Ciphertext should be different from plaintext

        // Test decryption
        let decrypted = decrypt_xsalsa20(&key, &nonce_material, &ciphertext).unwrap();
        assert_eq!(&*decrypted, plaintext);

        // Test that different nonce produces different ciphertext
        let nonce_material2 = [1u8; 24];
        let ciphertext2 = encrypt_xsalsa20(&key, &nonce_material2, plaintext).unwrap();
        assert_ne!(ciphertext, ciphertext2);

        // Test that different key produces different ciphertext
        let key2 = [1u8; 32];
        let ciphertext3 = encrypt_xsalsa20(&key2, &nonce_material, plaintext).unwrap();
        assert_ne!(ciphertext, ciphertext3);

        // Test invalid key length
        assert!(encrypt_xsalsa20(&key[..31], &nonce_material, plaintext).is_err());
        assert!(decrypt_xsalsa20(&key[..31], &nonce_material, &ciphertext).is_err());

        // Test invalid nonce length
        assert!(encrypt_xsalsa20(&key, &nonce_material[..23], plaintext).is_err());
        assert!(decrypt_xsalsa20(&key, &nonce_material[..23], &ciphertext).is_err());
    }

    #[test]
    fn test_xsalsa20_error_handling() {
        let key = [0u8; 32];
        let nonce_material = [0u8; 24];
        let plaintext = b"test message";

        // Test encryption with invalid key length
        let invalid_key = vec![0u8; 31]; // Too short
        let result = encrypt_xsalsa20(&invalid_key, &nonce_material, plaintext);
        assert!(result.is_err());

        // Test with too long key
        let too_long_key = vec![0u8; 33]; // Too long
        let result = encrypt_xsalsa20(&too_long_key, &nonce_material, plaintext);
        assert!(result.is_err());

        // Test decryption with invalid key length
        let ciphertext = encrypt_xsalsa20(&key, &nonce_material, plaintext).unwrap();
        let result = decrypt_xsalsa20(&invalid_key, &nonce_material, &ciphertext);
        assert!(result.is_err());

        // Test decryption with too long key
        let result = decrypt_xsalsa20(&too_long_key, &nonce_material, &ciphertext);
        assert!(result.is_err());

        // Test with invalid nonce length
        let invalid_nonce = vec![0u8; 23]; // Too short
        let result = encrypt_xsalsa20(&key, &invalid_nonce, plaintext);
        assert!(result.is_err());
        let result = decrypt_xsalsa20(&key, &invalid_nonce, &ciphertext);
        assert!(result.is_err());

        // Test with too long nonce
        let too_long_nonce = vec![0u8; 25]; // Too long
        let result = encrypt_xsalsa20(&key, &too_long_nonce, plaintext);
        assert!(result.is_err());
        let result = decrypt_xsalsa20(&key, &too_long_nonce, &ciphertext);
        assert!(result.is_err());
    }

    #[test]
    fn test_xsalsa20_poly1305() {
        let key = [0u8; 32]; // All zeros key
        let nonce = [0u8; 24]; // All zeros nonce
        let plaintext = b"Hello, World!";

        // Test encryption
        let ciphertext = encrypt_xsalsa20_poly1305(&key, &nonce, plaintext).unwrap();
        assert!(ciphertext.len() > plaintext.len()); // Should include authentication tag

        // Test decryption
        let decrypted = decrypt_xsalsa20_poly1305(&key, &nonce, &ciphertext).unwrap();
        assert_eq!(&*decrypted, plaintext);

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
