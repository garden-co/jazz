use crypto_secretbox::{
    aead::{Aead, KeyInit},
    XSalsa20Poly1305,
};
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use salsa20::cipher::{KeyIvInit, StreamCipher};
use salsa20::XSalsa20;
use std::fmt;
use wasm_bindgen::prelude::*;
use x25519_dalek::{PublicKey, StaticSecret};

#[derive(Debug)]
pub enum CryptoError {
    InvalidKeyLength,
    InvalidNonceLength,
    InvalidSignatureLength,
    InvalidVerifyingKey(String),
    WrongTag,
    CipherError,
}

impl fmt::Display for CryptoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CryptoError::InvalidKeyLength => write!(f, "Invalid key length"),
            CryptoError::InvalidNonceLength => write!(f, "Invalid nonce length"),
            CryptoError::InvalidSignatureLength => write!(f, "Invalid signature length"),
            CryptoError::InvalidVerifyingKey(e) => write!(f, "Invalid verifying key: {}", e),
            CryptoError::WrongTag => write!(f, "Wrong tag"),
            CryptoError::CipherError => write!(f, "Failed to create cipher"),
        }
    }
}

impl std::error::Error for CryptoError {}

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

/// Internal pure Rust functions (no wasm_bindgen)
fn x25519_public_key_internal(private_key: &[u8]) -> Result<Vec<u8>, CryptoError> {
    let bytes: [u8; 32] = private_key
        .try_into()
        .map_err(|_| CryptoError::InvalidKeyLength)?;
    let secret = StaticSecret::from(bytes);
    Ok(PublicKey::from(&secret).to_bytes().to_vec())
}

#[wasm_bindgen]
pub fn x25519_public_key(private_key: &[u8]) -> Result<Vec<u8>, JsError> {
    x25519_public_key_internal(private_key).map_err(|e| JsError::new(&e.to_string()))
}

fn x25519_diffie_hellman_internal(
    private_key: &[u8],
    public_key: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let private_bytes: [u8; 32] = private_key
        .try_into()
        .map_err(|_| CryptoError::InvalidKeyLength)?;
    let public_bytes: [u8; 32] = public_key
        .try_into()
        .map_err(|_| CryptoError::InvalidKeyLength)?;
    let secret = StaticSecret::from(private_bytes);
    let public = PublicKey::from(public_bytes);
    Ok(secret.diffie_hellman(&public).to_bytes().to_vec())
}

#[wasm_bindgen]
pub fn x25519_diffie_hellman(private_key: &[u8], public_key: &[u8]) -> Result<Vec<u8>, JsError> {
    x25519_diffie_hellman_internal(private_key, public_key)
        .map_err(|e| JsError::new(&e.to_string()))
}

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
fn encrypt_xsalsa20_raw_internal(
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
fn decrypt_xsalsa20_raw_internal(
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
fn encrypt_xsalsa20_poly1305(
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
fn decrypt_xsalsa20_poly1305(
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

/// Generate a new Ed25519 signing key
#[wasm_bindgen]
pub fn new_ed25519_signing_key() -> Vec<u8> {
    let mut rng = OsRng;
    let signing_key = SigningKey::generate(&mut rng);
    signing_key.to_bytes().to_vec()
}

fn ed25519_verifying_key_internal(signing_key: &[u8]) -> Result<Vec<u8>, CryptoError> {
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

fn ed25519_sign_internal(signing_key: &[u8], message: &[u8]) -> Result<Vec<u8>, CryptoError> {
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

fn ed25519_verify_internal(
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

    #[test]
    fn test_seal_unseal_error_handling() {
        let message = b"test message";
        let nonce = generate_nonce(b"test nonce material");

        // Generate valid keys
        let sender_private = new_x25519_private_key();
        let recipient_private = new_x25519_private_key();
        let recipient_public = x25519_public_key_internal(&recipient_private).unwrap();

        // Test with invalid sender private key length
        let invalid_sender_private = vec![0u8; 31]; // Too short
        let shared_secret =
            x25519_diffie_hellman_internal(&invalid_sender_private, &recipient_public);
        assert!(shared_secret.is_err());

        // Test with too long sender private key
        let too_long_sender_private = vec![0u8; 33]; // Too long
        let shared_secret =
            x25519_diffie_hellman_internal(&too_long_sender_private, &recipient_public);
        assert!(shared_secret.is_err());

        // Test with invalid recipient public key length
        let invalid_recipient_public = vec![0u8; 31]; // Too short
        let shared_secret =
            x25519_diffie_hellman_internal(&sender_private, &invalid_recipient_public);
        assert!(shared_secret.is_err());

        // Test with too long recipient public key
        let too_long_recipient_public = vec![0u8; 33]; // Too long
        let shared_secret =
            x25519_diffie_hellman_internal(&sender_private, &too_long_recipient_public);
        assert!(shared_secret.is_err());

        // Test successful encryption/decryption
        let shared_secret =
            x25519_diffie_hellman_internal(&sender_private, &recipient_public).unwrap();
        let encrypted = encrypt_xsalsa20_poly1305(&shared_secret, &nonce, message).unwrap();
        let decrypted = decrypt_xsalsa20_poly1305(&shared_secret, &nonce, &encrypted).unwrap();
        assert_eq!(decrypted, message);

        // Test decryption with wrong key
        let wrong_shared_secret =
            x25519_diffie_hellman_internal(&new_x25519_private_key(), &recipient_public).unwrap();
        let result = decrypt_xsalsa20_poly1305(&wrong_shared_secret, &nonce, &encrypted);
        assert!(result.is_err());

        // Test with empty message
        let result = encrypt_xsalsa20_poly1305(&shared_secret, &nonce, &[]);
        assert!(result.is_ok());

        // Test with empty nonce material
        let empty_nonce = generate_nonce(&[]); // Generate a valid nonce from empty material
        let result = encrypt_xsalsa20_poly1305(&shared_secret, &empty_nonce, message);
        assert!(result.is_ok()); // Should work with a valid nonce generated from empty material
    }
}
