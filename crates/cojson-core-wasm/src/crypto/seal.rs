use cojson_core::{seal_internal, unseal_internal};
use wasm_bindgen::prelude::*;

/// WASM-exposed function for sealing a message using X25519 + XSalsa20-Poly1305.
/// Provides authenticated encryption with perfect forward secrecy.
/// - `message`: Raw bytes to seal
/// - `sender_secret`: Base58-encoded sender's private key with "sealerSecret_z" prefix
/// - `recipient_id`: Base58-encoded recipient's public key with "sealer_z" prefix
/// - `nonce_material`: Raw bytes used to generate the nonce
/// Returns sealed bytes or throws JsError if sealing fails.
#[wasm_bindgen(js_name = seal)]
pub fn seal(
    message: &[u8],
    sender_secret: &str,
    recipient_id: &str,
    nonce_material: &[u8],
) -> Result<Box<[u8]>, JsError> {
    Ok(seal_internal(message, sender_secret, recipient_id, nonce_material)?.into())
}

/// WASM-exposed function for unsealing a message using X25519 + XSalsa20-Poly1305.
/// Provides authenticated decryption with perfect forward secrecy.
/// - `sealed_message`: The sealed bytes to decrypt
/// - `recipient_secret`: Base58-encoded recipient's private key with "sealerSecret_z" prefix
/// - `sender_id`: Base58-encoded sender's public key with "sealer_z" prefix
/// - `nonce_material`: Raw bytes used to generate the nonce (must match sealing)
/// Returns unsealed bytes or throws JsError if unsealing fails.
#[wasm_bindgen(js_name = unseal)]
pub fn unseal(
    sealed_message: &[u8],
    recipient_secret: &str,
    sender_id: &str,
    nonce_material: &[u8],
) -> Result<Box<[u8]>, JsError> {
    Ok(unseal_internal(sealed_message, recipient_secret, sender_id, nonce_material)?.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bs58;
    use cojson_core::crypto::x25519::{new_x25519_private_key, x25519_public_key_internal};

    #[test]
    fn test_seal_unseal() {
        // Generate real keys
        let sender_private = new_x25519_private_key();
        let sender_public = x25519_public_key_internal(&sender_private).unwrap();

        // Encode keys with proper prefixes
        let sender_secret = format!(
            "sealerSecret_z{}",
            bs58::encode(&sender_private).into_string()
        );
        let recipient_id = format!("sealer_z{}", bs58::encode(&sender_public).into_string());

        // Test data
        let message = b"Secret message";
        let nonce_material = b"test_nonce_material";

        // Test sealing
        let sealed = seal_internal(message, &sender_secret, &recipient_id, nonce_material).unwrap();
        assert!(!sealed.is_empty());

        // Test unsealing (using same keys since it's a test)
        let unsealed =
            unseal_internal(&sealed, &sender_secret, &recipient_id, nonce_material).unwrap();
        assert_eq!(&*unsealed, message);
    }

    #[test]
    fn test_invalid_keys() {
        let message = b"test";
        let nonce_material = b"nonce";

        // Test with invalid sender secret format
        let result = seal_internal(
            message,
            "invalid_key",
            "sealer_z22222222222222222222222222222222",
            nonce_material,
        );
        assert!(result.is_err());

        // Test with invalid recipient ID format
        let result = seal_internal(
            message,
            "sealerSecret_z11111111111111111111111111111111",
            "invalid_key",
            nonce_material,
        );
        assert!(result.is_err());

        // Test with invalid base58 encoding
        let result = seal_internal(
            message,
            "sealerSecret_z!!!!",
            "sealer_z22222222222222222222222222222222",
            nonce_material,
        );
        assert!(result.is_err());
    }
}
