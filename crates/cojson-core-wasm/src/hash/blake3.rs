use wasm_bindgen::prelude::*;

/// Generate a 24-byte nonce from input material using BLAKE3.
/// - `nonce_material`: Raw bytes to derive the nonce from
/// Returns 24 bytes suitable for use as a nonce in cryptographic operations.
/// This function is deterministic - the same input will produce the same nonce.
#[wasm_bindgen]
pub fn generate_nonce(nonce_material: &[u8]) -> Box<[u8]> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(nonce_material);
    hasher.finalize().as_bytes()[..24].into()
}

/// Hash data once using BLAKE3.
/// - `data`: Raw bytes to hash
/// Returns 32 bytes of hash output.
/// This is the simplest way to compute a BLAKE3 hash of a single piece of data.
#[wasm_bindgen]
pub fn blake3_hash_once(data: &[u8]) -> Box<[u8]> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(data);
    hasher.finalize().as_bytes().to_vec().into_boxed_slice()
}

/// Hash data once using BLAKE3 with a context prefix.
/// - `data`: Raw bytes to hash
/// - `context`: Context bytes to prefix to the data
/// Returns 32 bytes of hash output.
/// This is useful for domain separation - the same data hashed with different contexts will produce different outputs.
#[wasm_bindgen]
pub fn blake3_hash_once_with_context(data: &[u8], context: &[u8]) -> Box<[u8]> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(context);
    hasher.update(data);
    hasher.finalize().as_bytes().to_vec().into_boxed_slice()
}

#[wasm_bindgen]
pub struct Blake3Hasher(blake3::Hasher);

#[wasm_bindgen]
impl Blake3Hasher {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Blake3Hasher(blake3::Hasher::new())
    }

    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    pub fn finalize(&self) -> Box<[u8]> {
        self.0.finalize().as_bytes().to_vec().into_boxed_slice()
    }

    pub fn clone(&self) -> Self {
        // The blake3::Hasher type implements Clone
        Blake3Hasher(self.0.clone())
    }
}

/// Get an empty BLAKE3 state for incremental hashing.
/// Returns a new Blake3Hasher instance for incremental hashing.
#[wasm_bindgen]
pub fn blake3_empty_state() -> Blake3Hasher {
    Blake3Hasher::new()
}

/// Update a BLAKE3 state with new data for incremental hashing.
/// - `state`: Current Blake3Hasher instance
/// - `data`: New data to incorporate into the hash
/// Returns the updated Blake3Hasher.
#[wasm_bindgen]
pub fn blake3_update_state(state: &mut Blake3Hasher, data: &[u8]) {
    state.update(data);
}

/// Get the final hash from a BLAKE3 state.
/// - `state`: The Blake3Hasher to finalize
/// Returns 32 bytes of hash output.
/// This finalizes an incremental hashing operation.
#[wasm_bindgen]
pub fn blake3_digest_for_state(state: Blake3Hasher) -> Box<[u8]> {
    state.finalize()
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
        let mut state = blake3_empty_state();

        // First update with [1,2,3,4,5]
        let data1 = &[1u8, 2, 3, 4, 5];
        blake3_update_state(&mut state, data1);

        // Check that this matches a direct hash
        let direct_hash = blake3_hash_once(data1);
        let state_hash = state.finalize();
        assert_eq!(
            state_hash, direct_hash,
            "First update should match direct hash"
        );

        // Create new state for second test
        let mut state = blake3_empty_state();
        blake3_update_state(&mut state, data1);

        // Verify the exact expected hash from the TypeScript test for the first update
        let expected_first_hash = [
            2, 79, 103, 192, 66, 90, 61, 192, 47, 186, 245, 140, 185, 61, 229, 19, 46, 61, 117,
            197, 25, 250, 160, 186, 218, 33, 73, 29, 136, 201, 112, 87,
        ]
        .to_vec()
        .into_boxed_slice();
        assert_eq!(
            state.finalize(),
            expected_first_hash,
            "First update should match expected hash"
        );

        // Test with two updates
        let mut state = blake3_empty_state();
        let data1 = &[1u8, 2, 3, 4, 5];
        let data2 = &[6u8, 7, 8, 9, 10];
        blake3_update_state(&mut state, data1);
        blake3_update_state(&mut state, data2);

        // Compare with a single hash of all data
        let mut all_data = Vec::new();
        all_data.extend_from_slice(data1);
        all_data.extend_from_slice(data2);
        let direct_hash_all = blake3_hash_once(&all_data);
        assert_eq!(
            state.finalize(),
            direct_hash_all,
            "Final state should match direct hash of all data"
        );

        // Test final hash matches expected value
        let mut state = blake3_empty_state();
        blake3_update_state(&mut state, data1);
        blake3_update_state(&mut state, data2);

        let expected_final_hash = [
            165, 131, 141, 69, 2, 69, 39, 236, 196, 244, 180, 213, 147, 124, 222, 39, 68, 223, 54,
            176, 242, 97, 200, 101, 204, 79, 21, 233, 56, 51, 1, 199,
        ]
        .to_vec()
        .into_boxed_slice();
        assert_eq!(
            state.finalize(),
            expected_final_hash,
            "Final state should match expected hash"
        );
    }
}
