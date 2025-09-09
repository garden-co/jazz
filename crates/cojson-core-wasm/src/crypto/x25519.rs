use cojson_core::crypto::x25519::{x25519_public_key, x25519_diffie_hellman, get_sealer_id_internal};
use wasm_bindgen::prelude::*;
use x25519_dalek::StaticSecret;

/// Generate a new X25519 private key using secure random number generation.
/// Returns 32 bytes of raw key material suitable for use with other X25519 functions.
/// This key can be reused for multiple Diffie-Hellman exchanges.
#[wasm_bindgen]
pub fn new_x25519_private_key() -> Vec<u8> {
    let secret = StaticSecret::random();
    secret.to_bytes().to_vec()
}

/// WASM-exposed function to derive an X25519 public key from a private key.
/// - `private_key`: 32 bytes of private key material
/// Returns 32 bytes of public key material or throws JsError if key is invalid.
#[wasm_bindgen]
pub fn x25519_public_key_wasm(private_key: &[u8]) -> Result<Vec<u8>, JsError> {
    Ok(x25519_public_key(private_key)?.to_vec())
}

/// WASM-exposed function to perform X25519 Diffie-Hellman key exchange.
/// - `private_key`: 32 bytes of private key material
/// - `public_key`: 32 bytes of public key material
/// Returns 32 bytes of shared secret material or throws JsError if key exchange fails.
#[wasm_bindgen]
pub fn x25519_diffie_hellman_wasm(private_key: &[u8], public_key: &[u8]) -> Result<Vec<u8>, JsError> {
    Ok(x25519_diffie_hellman(private_key, public_key)?.to_vec())
}

/// WASM-exposed function to derive a sealer ID from a sealer secret.
/// - `secret`: Raw bytes of the sealer secret
/// Returns a base58-encoded sealer ID with "sealer_z" prefix or throws JsError if derivation fails.
#[wasm_bindgen]
pub fn get_sealer_id(secret: &[u8]) -> Result<String, JsError> {
    let secret_str = std::str::from_utf8(secret)
        .map_err(|e| JsError::new(&format!("Invalid UTF-8 in secret: {:?}", e)))?;
    get_sealer_id_internal(secret_str).map_err(|e| JsError::new(&e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cojson_core::crypto::x25519::{x25519_public_key, x25519_diffie_hellman, get_sealer_id_internal};
    use bs58;

    #[test]
    fn test_x25519_key_generation() {
        // Test that we get the correct length keys
        let private_key = new_x25519_private_key();
        assert_eq!(private_key.len(), 32);

        // Test that public key generation works and produces correct length
        let public_key = x25519_public_key(&private_key).unwrap();
        assert_eq!(public_key.len(), 32);

        // Test that different private keys produce different public keys
        let private_key2 = new_x25519_private_key();
        let public_key2 = x25519_public_key(&private_key2).unwrap();
        assert_ne!(public_key, public_key2);
    }

    #[test]
    fn test_x25519_key_exchange() {
        // Generate sender's keypair
        let sender_private = new_x25519_private_key();
        let sender_public = x25519_public_key(&sender_private).unwrap();

        // Generate recipient's keypair
        let recipient_private = new_x25519_private_key();
        let recipient_public = x25519_public_key(&recipient_private).unwrap();

        // Test properties we expect from the shared secret
        let shared_secret1 =
            x25519_diffie_hellman(&sender_private, &recipient_public).unwrap();
        let shared_secret2 =
            x25519_diffie_hellman(&recipient_private, &sender_public).unwrap();

        // Both sides should arrive at the same shared secret
        assert_eq!(shared_secret1, shared_secret2);

        // Shared secret should be 32 bytes
        assert_eq!(shared_secret1.len(), 32);

        // Different recipient should produce different shared secret
        let other_recipient_private = new_x25519_private_key();
        let other_recipient_public = x25519_public_key(&other_recipient_private).unwrap();
        let different_shared_secret =
            x25519_diffie_hellman(&sender_private, &other_recipient_public).unwrap();
        assert_ne!(shared_secret1, different_shared_secret);
    }

    #[test]
    fn test_get_sealer_id() {
        // Create a test private key
        let private_key = new_x25519_private_key();
        let secret = format!("sealerSecret_z{}", bs58::encode(&private_key).into_string());

        // Get sealer ID
        let sealer_id = get_sealer_id_internal(&secret).unwrap();
        assert!(sealer_id.starts_with("sealer_z"));

        // Test that same secret produces same ID
        let sealer_id2 = get_sealer_id_internal(&secret).unwrap();
        assert_eq!(sealer_id, sealer_id2);

        // Test invalid secret format
        let result = get_sealer_id_internal("invalid_secret");
        assert!(result.is_err());

        // Test invalid base58
        let result = get_sealer_id_internal("sealerSecret_z!!!invalid!!!");
        assert!(result.is_err());
    }
}
