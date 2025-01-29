use wasm_bindgen::prelude::*;

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
}
