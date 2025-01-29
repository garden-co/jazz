use crypto_secretbox::{
    aead::{Aead, KeyInit},
    XSalsa20Poly1305,
};
use wasm_bindgen::prelude::*;
use x25519_dalek::{PublicKey, StaticSecret};

/// Generate a 24-byte nonce from input material using BLAKE3
#[wasm_bindgen]
pub fn generate_nonce(nonce_material: &[u8]) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(nonce_material);
    hasher.finalize().as_bytes()[..24].to_vec()
}

/// Hash data once using BLAKE3
#[wasm_bindgen]
pub fn blake3_hash_once(data: &[u8]) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(data);
    hasher.finalize().as_bytes().to_vec()
}

/// Hash data once using BLAKE3 with a context prefix
#[wasm_bindgen]
pub fn blake3_hash_once_with_context(data: &[u8], context: &[u8]) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(context);
    hasher.update(data);
    hasher.finalize().as_bytes().to_vec()
}

/// Get an empty BLAKE3 state
#[wasm_bindgen]
pub fn blake3_empty_state() -> Vec<u8> {
    Vec::new()
}

/// Update a BLAKE3 state with new data
#[wasm_bindgen]
pub fn blake3_update_state(state: &[u8], data: &[u8]) -> Vec<u8> {
    let mut all_data = Vec::new();
    if !state.is_empty() {
        all_data.extend_from_slice(state);
    }
    all_data.extend_from_slice(data);
    all_data
}

/// Get the final hash from a state
#[wasm_bindgen]
pub fn blake3_digest_for_state(state: &[u8]) -> Vec<u8> {
    // For empty state, return hash of empty input
    if state.is_empty() {
        return blake3_hash_once(&[]);
    }
    // For non-empty state, hash the accumulated data
    blake3_hash_once(state)
}

/// Generate a new X25519 private key that can be reused
#[wasm_bindgen]
pub fn new_x25519_private_key() -> Vec<u8> {
    let secret = StaticSecret::random();
    secret.to_bytes().to_vec()
}

/// Get the public key from a private key
#[wasm_bindgen]
pub fn x25519_public_key(private_key: &[u8]) -> Vec<u8> {
    let bytes: [u8; 32] = private_key.try_into().expect("Invalid private key length");
    let secret = StaticSecret::from(bytes);
    PublicKey::from(&secret).to_bytes().to_vec()
}

/// Compute the shared secret using X25519 Diffie-Hellman
#[wasm_bindgen]
pub fn x25519_diffie_hellman(private_key: &[u8], public_key: &[u8]) -> Vec<u8> {
    let private_bytes: [u8; 32] = private_key.try_into().expect("Invalid private key length");
    let public_bytes: [u8; 32] = public_key.try_into().expect("Invalid public key length");
    let secret = StaticSecret::from(private_bytes);
    let public = PublicKey::from(public_bytes);
    secret.diffie_hellman(&public).to_bytes().to_vec()
}

/// Internal function for XSalsa20-Poly1305 encryption
fn encrypt_xsalsa20_poly1305_internal(
    key: &[u8],
    nonce: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, String> {
    // Key must be 32 bytes
    let key_bytes: [u8; 32] = key
        .try_into()
        .map_err(|_| "Invalid key length".to_string())?;
    // Nonce must be 24 bytes
    let nonce_bytes: [u8; 24] = nonce
        .try_into()
        .map_err(|_| "Invalid nonce length".to_string())?;

    // Create cipher instance
    let cipher = XSalsa20Poly1305::new(&key_bytes.into());

    // Encrypt the plaintext
    cipher
        .encrypt(&nonce_bytes.into(), plaintext)
        .map_err(|_| "Wrong tag".to_string())
}

/// Internal function for XSalsa20-Poly1305 decryption
fn decrypt_xsalsa20_poly1305_internal(
    key: &[u8],
    nonce: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, String> {
    // Key must be 32 bytes
    let key_bytes: [u8; 32] = key
        .try_into()
        .map_err(|_| "Invalid key length".to_string())?;
    // Nonce must be 24 bytes
    let nonce_bytes: [u8; 24] = nonce
        .try_into()
        .map_err(|_| "Invalid nonce length".to_string())?;

    // Create cipher instance
    let cipher = XSalsa20Poly1305::new(&key_bytes.into());

    // Decrypt the ciphertext
    cipher
        .decrypt(&nonce_bytes.into(), ciphertext)
        .map_err(|_| "Wrong tag".to_string())
}

/// WASM-exposed function for XSalsa20-Poly1305 encryption
#[wasm_bindgen]
pub fn encrypt_xsalsa20_poly1305(
    key: &[u8],
    nonce: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, JsError> {
    encrypt_xsalsa20_poly1305_internal(key, nonce, plaintext).map_err(|e| JsError::new(&e))
}

/// WASM-exposed function for XSalsa20-Poly1305 decryption
#[wasm_bindgen]
pub fn decrypt_xsalsa20_poly1305(
    key: &[u8],
    nonce: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, JsError> {
    decrypt_xsalsa20_poly1305_internal(key, nonce, ciphertext).map_err(|e| JsError::new(&e))
}

/// Internal function for sealing a message
fn seal_internal(
    message: &[u8],
    sender_private_key: &[u8],
    recipient_public_key: &[u8],
    nonce: &[u8],
) -> Result<Vec<u8>, String> {
    // Generate shared secret using X25519
    let shared_secret = x25519_diffie_hellman(sender_private_key, recipient_public_key);

    // Encrypt message using XSalsa20-Poly1305
    encrypt_xsalsa20_poly1305_internal(&shared_secret, nonce, message)
}

/// Internal function for unsealing a message
fn unseal_internal(
    sealed_message: &[u8],
    recipient_private_key: &[u8],
    sender_public_key: &[u8],
    nonce: &[u8],
) -> Result<Vec<u8>, String> {
    // Generate shared secret using X25519
    let shared_secret = x25519_diffie_hellman(recipient_private_key, sender_public_key);

    // Decrypt message using XSalsa20-Poly1305
    decrypt_xsalsa20_poly1305_internal(&shared_secret, nonce, sealed_message)
}

/// WASM-exposed function for sealing a message using X25519 + XSalsa20-Poly1305
#[wasm_bindgen]
pub fn seal(
    message: &[u8],
    sender_private_key: &[u8],
    recipient_public_key: &[u8],
    nonce: &[u8],
) -> Result<Vec<u8>, JsError> {
    seal_internal(message, sender_private_key, recipient_public_key, nonce)
        .map_err(|e| JsError::new(&e))
}

/// WASM-exposed function for unsealing a message using X25519 + XSalsa20-Poly1305
#[wasm_bindgen]
pub fn unseal(
    sealed_message: &[u8],
    recipient_private_key: &[u8],
    sender_public_key: &[u8],
    nonce: &[u8],
) -> Result<Vec<u8>, JsError> {
    unseal_internal(
        sealed_message,
        recipient_private_key,
        sender_public_key,
        nonce,
    )
    .map_err(|e| JsError::new(&e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nonce_generation() {
        let input = b"test input";
        let nonce = generate_nonce(input);
        assert_eq!(nonce.len(), 24);

        // Same input should produce same nonce
        let nonce2 = generate_nonce(input);
        assert_eq!(nonce, nonce2);

        // Different input should produce different nonce
        let nonce3 = generate_nonce(b"different input");
        assert_ne!(nonce, nonce3);
    }

    #[test]
    fn test_blake3_hash_once() {
        let input = b"test input";
        let hash = blake3_hash_once(input);

        // BLAKE3 produces 32-byte hashes
        assert_eq!(hash.len(), 32);

        // Same input should produce same hash
        let hash2 = blake3_hash_once(input);
        assert_eq!(hash, hash2);

        // Different input should produce different hash
        let hash3 = blake3_hash_once(b"different input");
        assert_ne!(hash, hash3);
    }

    #[test]
    fn test_blake3_hash_once_with_context() {
        let input = b"test input";
        let context = b"test context";
        let hash = blake3_hash_once_with_context(input, context);

        // BLAKE3 produces 32-byte hashes
        assert_eq!(hash.len(), 32);

        // Same input and context should produce same hash
        let hash2 = blake3_hash_once_with_context(input, context);
        assert_eq!(hash, hash2);

        // Different input should produce different hash
        let hash3 = blake3_hash_once_with_context(b"different input", context);
        assert_ne!(hash, hash3);

        // Different context should produce different hash
        let hash4 = blake3_hash_once_with_context(input, b"different context");
        assert_ne!(hash, hash4);

        // Hash with context should be different from hash without context
        let hash_no_context = blake3_hash_once(input);
        assert_ne!(hash, hash_no_context);
    }

    #[test]
    fn test_blake3_incremental() {
        // Initial state
        let state = blake3_empty_state();
        assert!(state.is_empty(), "Initial state should be empty");

        // First update with [1,2,3,4,5]
        let data1 = &[1u8, 2, 3, 4, 5];
        let state2 = blake3_update_state(&state, data1);
        assert_eq!(state2, data1, "Updated state should contain first chunk");

        // Check that this matches a direct hash
        let direct_hash = blake3_hash_once(data1);
        assert_eq!(
            blake3_digest_for_state(&state2),
            direct_hash,
            "First update should match direct hash"
        );

        // Verify the exact expected hash from the TypeScript test for the first update
        let expected_first_hash = [
            2, 79, 103, 192, 66, 90, 61, 192, 47, 186, 245, 140, 185, 61, 229, 19, 46, 61, 117,
            197, 25, 250, 160, 186, 218, 33, 73, 29, 136, 201, 112, 87,
        ];
        assert_eq!(
            blake3_digest_for_state(&state2),
            expected_first_hash,
            "First update should match expected hash"
        );

        // Second update with [6,7,8,9,10]
        let data2 = &[6u8, 7, 8, 9, 10];
        let state3 = blake3_update_state(&state2, data2);

        // Compare with a single hash of all data
        let mut all_data = Vec::new();
        all_data.extend_from_slice(data1);
        all_data.extend_from_slice(data2);
        assert_eq!(state3, all_data, "Final state should contain all data");

        let direct_hash_all = blake3_hash_once(&all_data);
        assert_eq!(
            blake3_digest_for_state(&state3),
            direct_hash_all,
            "Final state should match direct hash of all data"
        );

        // Also verify the exact expected hash from the TypeScript test for the final state
        let expected_final_hash = [
            165, 131, 141, 69, 2, 69, 39, 236, 196, 244, 180, 213, 147, 124, 222, 39, 68, 223, 54,
            176, 242, 97, 200, 101, 204, 79, 21, 233, 56, 51, 1, 199,
        ];
        assert_eq!(
            blake3_digest_for_state(&state3),
            expected_final_hash,
            "Final state should match expected hash"
        );
    }

    #[test]
    fn test_blake3_digest_for_state() {
        // Test empty state
        let empty_state = blake3_empty_state();
        let empty_hash = blake3_digest_for_state(&empty_state);
        assert_eq!(
            empty_hash,
            blake3_hash_once(&[]),
            "Empty state should produce hash of empty input"
        );

        // Test with some data
        let data = &[1u8, 2, 3, 4, 5];
        let state = blake3_update_state(&empty_state, data);
        let hash = blake3_digest_for_state(&state);
        assert_eq!(
            hash,
            blake3_hash_once(data),
            "State with data should match direct hash"
        );

        // Test with multiple updates
        let data2 = &[6u8, 7, 8, 9, 10];
        let state2 = blake3_update_state(&state, data2);
        let hash2 = blake3_digest_for_state(&state2);

        // Create combined data for comparison
        let mut all_data = Vec::new();
        all_data.extend_from_slice(data);
        all_data.extend_from_slice(data2);
        assert_eq!(
            hash2,
            blake3_hash_once(&all_data),
            "State with multiple updates should match direct hash of all data"
        );

        // Verify exact hash values from TypeScript test
        let expected_first_hash = [
            2, 79, 103, 192, 66, 90, 61, 192, 47, 186, 245, 140, 185, 61, 229, 19, 46, 61, 117,
            197, 25, 250, 160, 186, 218, 33, 73, 29, 136, 201, 112, 87,
        ];
        assert_eq!(
            hash, expected_first_hash,
            "First hash should match TypeScript test value"
        );

        let expected_final_hash = [
            165, 131, 141, 69, 2, 69, 39, 236, 196, 244, 180, 213, 147, 124, 222, 39, 68, 223, 54,
            176, 242, 97, 200, 101, 204, 79, 21, 233, 56, 51, 1, 199,
        ];
        assert_eq!(
            hash2, expected_final_hash,
            "Final hash should match TypeScript test value"
        );
    }

    #[test]
    fn test_x25519_key_generation() {
        // Test that we get the correct length keys
        let private_key = new_x25519_private_key();
        assert_eq!(private_key.len(), 32);

        // Test that public key generation works and produces correct length
        let public_key = x25519_public_key(&private_key);
        assert_eq!(public_key.len(), 32);

        // Test that different private keys produce different public keys
        let private_key2 = new_x25519_private_key();
        let public_key2 = x25519_public_key(&private_key2);
        assert_ne!(public_key, public_key2);
    }

    #[test]
    fn test_x25519_key_exchange() {
        // Generate sender's keypair
        let sender_private = new_x25519_private_key();
        let sender_public = x25519_public_key(&sender_private);

        // Generate recipient's keypair
        let recipient_private = new_x25519_private_key();
        let recipient_public = x25519_public_key(&recipient_private);

        // Test properties we expect from the shared secret
        let shared_secret1 = x25519_diffie_hellman(&sender_private, &recipient_public);
        let shared_secret2 = x25519_diffie_hellman(&recipient_private, &sender_public);

        // Both sides should arrive at the same shared secret
        assert_eq!(shared_secret1, shared_secret2);

        // Shared secret should be 32 bytes
        assert_eq!(shared_secret1.len(), 32);

        // Different recipient should produce different shared secret
        let other_recipient_private = new_x25519_private_key();
        let other_recipient_public = x25519_public_key(&other_recipient_private);
        let different_shared_secret =
            x25519_diffie_hellman(&sender_private, &other_recipient_public);
        assert_ne!(shared_secret1, different_shared_secret);
    }

    #[test]
    fn test_xsalsa20_poly1305() {
        // Test vectors from https://github.com/jedisct1/libsodium/blob/master/test/default/secretbox_test_vectors.h
        let key = [0u8; 32]; // All zeros key
        let nonce = [0u8; 24]; // All zeros nonce
        let plaintext = b"Hello, World!";

        // Test encryption
        let ciphertext = encrypt_xsalsa20_poly1305_internal(&key, &nonce, plaintext).unwrap();
        assert!(ciphertext.len() > plaintext.len()); // Should include authentication tag

        // Test decryption
        let decrypted = decrypt_xsalsa20_poly1305_internal(&key, &nonce, &ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);

        // Test that different nonce produces different ciphertext
        let nonce2 = [1u8; 24];
        let ciphertext2 = encrypt_xsalsa20_poly1305_internal(&key, &nonce2, plaintext).unwrap();
        assert_ne!(ciphertext, ciphertext2);

        // Test that different key produces different ciphertext
        let key2 = [1u8; 32];
        let ciphertext3 = encrypt_xsalsa20_poly1305_internal(&key2, &nonce, plaintext).unwrap();
        assert_ne!(ciphertext, ciphertext3);

        // Test that decryption fails with wrong key
        assert!(decrypt_xsalsa20_poly1305_internal(&key2, &nonce, &ciphertext).is_err());

        // Test that decryption fails with wrong nonce
        assert!(decrypt_xsalsa20_poly1305_internal(&key, &nonce2, &ciphertext).is_err());

        // Test that decryption fails with tampered ciphertext
        let mut tampered = ciphertext.clone();
        tampered[0] ^= 1;
        assert!(decrypt_xsalsa20_poly1305_internal(&key, &nonce, &tampered).is_err());
    }

    #[test]
    fn test_seal_unseal() {
        // Generate keypairs
        let sender_private = new_x25519_private_key();
        let sender_public = x25519_public_key(&sender_private);
        let recipient_private = new_x25519_private_key();
        let recipient_public = x25519_public_key(&recipient_private);

        // Test message and nonce
        let message = b"Secret message";
        let nonce = generate_nonce(b"test nonce material");
        let different_nonce = generate_nonce(b"different nonce");

        // Seal the message
        let sealed = seal_internal(message, &sender_private, &recipient_public, &nonce).unwrap();

        // Unseal the message
        let unsealed =
            unseal_internal(&sealed, &recipient_private, &sender_public, &nonce).unwrap();
        assert_eq!(unsealed, message);

        // Test that different nonce produces different sealed message
        let sealed2 = seal_internal(
            message,
            &sender_private,
            &recipient_public,
            &different_nonce,
        )
        .unwrap();
        assert_ne!(sealed, sealed2);

        // Test that unsealing fails with wrong keys
        let wrong_private = new_x25519_private_key();
        assert!(unseal_internal(&sealed, &wrong_private, &sender_public, &nonce).is_err());

        // Test that unsealing fails with wrong nonce
        assert!(unseal_internal(
            &sealed,
            &recipient_private,
            &sender_public,
            &different_nonce
        )
        .is_err());

        // Test that unsealing fails with tampered message
        let mut tampered = sealed.clone();
        tampered[0] ^= 1;
        assert!(unseal_internal(&tampered, &recipient_private, &sender_public, &nonce).is_err());
    }
}
