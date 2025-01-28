use wasm_bindgen::prelude::*;

/// Generate a 24-byte nonce from input material using BLAKE3
#[wasm_bindgen]
pub fn generate_nonce(nonce_material: &[u8]) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(nonce_material);
    hasher.finalize().as_bytes()[..24].to_vec()
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
}
