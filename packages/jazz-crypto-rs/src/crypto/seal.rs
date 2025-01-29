use crate::crypto::x25519::x25519_diffie_hellman_internal;
use crate::crypto::xsalsa20::{decrypt_xsalsa20_poly1305, encrypt_xsalsa20_poly1305};
use crate::hash::blake3::generate_nonce;
use wasm_bindgen::prelude::*;

/// WASM-exposed function for sealing a message using X25519 + XSalsa20-Poly1305
#[wasm_bindgen]
pub fn seal(
    message: &[u8],
    sender_private_key: &[u8],
    recipient_public_key: &[u8],
    nonce_material: &[u8],
) -> Result<Vec<u8>, JsError> {
    let nonce = generate_nonce(nonce_material);

    // Generate shared secret using X25519
    let shared_secret = x25519_diffie_hellman_internal(sender_private_key, recipient_public_key)
        .map_err(|e| JsError::new(&e.to_string()))?;

    // Encrypt message using XSalsa20-Poly1305
    encrypt_xsalsa20_poly1305(&shared_secret, &nonce, message)
        .map_err(|e| JsError::new(&e.to_string()))
}

/// WASM-exposed function for unsealing a message using X25519 + XSalsa20-Poly1305
#[wasm_bindgen]
pub fn unseal(
    sealed_message: &[u8],
    recipient_private_key: &[u8],
    sender_public_key: &[u8],
    nonce_material: &[u8],
) -> Result<Vec<u8>, JsError> {
    let nonce = generate_nonce(nonce_material);

    // Generate shared secret using X25519
    let shared_secret = x25519_diffie_hellman_internal(recipient_private_key, sender_public_key)
        .map_err(|e| JsError::new(&e.to_string()))?;

    // Decrypt message using XSalsa20-Poly1305
    decrypt_xsalsa20_poly1305(&shared_secret, &nonce, sealed_message)
        .map_err(|e| JsError::new(&e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::x25519::{new_x25519_private_key, x25519_public_key_internal};

    #[test]
    fn test_seal_unseal() {
        // Generate keypairs
        let sender_private = new_x25519_private_key();
        let sender_public = x25519_public_key_internal(&sender_private).unwrap();
        let recipient_private = new_x25519_private_key();
        let recipient_public = x25519_public_key_internal(&recipient_private).unwrap();

        // Test message and nonce
        let message = b"Secret message";
        let nonce = generate_nonce(b"test nonce material");
        let different_nonce = generate_nonce(b"different nonce");

        // Generate shared secrets
        let shared_secret1 =
            x25519_diffie_hellman_internal(&sender_private, &recipient_public).unwrap();
        let shared_secret2 =
            x25519_diffie_hellman_internal(&recipient_private, &sender_public).unwrap();
        assert_eq!(
            shared_secret1, shared_secret2,
            "Shared secrets should match"
        );

        // Seal the message (encrypt with authentication)
        let sealed = encrypt_xsalsa20_poly1305(&shared_secret1, &nonce, message).unwrap();

        // Unseal the message (decrypt with authentication)
        let unsealed = decrypt_xsalsa20_poly1305(&shared_secret2, &nonce, &sealed).unwrap();
        assert_eq!(unsealed, message);

        // Test that different nonce produces different sealed message
        let sealed2 =
            encrypt_xsalsa20_poly1305(&shared_secret1, &different_nonce, message).unwrap();
        assert_ne!(sealed, sealed2);

        // Test that unsealing fails with wrong keys
        let wrong_private = new_x25519_private_key();
        let wrong_shared_secret =
            x25519_diffie_hellman_internal(&wrong_private, &recipient_public).unwrap();
        assert!(decrypt_xsalsa20_poly1305(&wrong_shared_secret, &nonce, &sealed).is_err());

        // Test that unsealing fails with wrong nonce
        assert!(decrypt_xsalsa20_poly1305(&shared_secret1, &different_nonce, &sealed).is_err());

        // Test that unsealing fails with tampered message
        let mut tampered = sealed.clone();
        tampered[0] ^= 1;
        assert!(decrypt_xsalsa20_poly1305(&shared_secret1, &nonce, &tampered).is_err());
    }
}
