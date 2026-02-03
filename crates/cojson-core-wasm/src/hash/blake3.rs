use cojson_core::hash::blake3 as blake3_crypto;
use wasm_bindgen::prelude::*;

/// Compute a short hash of a stable-stringified JSON value.
/// The input should already be serialized using stableStringify on the JS side.
/// Returns a string prefixed with "shortHash_z" followed by base58-encoded hash.
#[wasm_bindgen(js_name = shortHash)]
pub fn short_hash(value: String) -> String {
    blake3_crypto::short_hash(&value)
}

/// Generate a 24-byte nonce from input material using BLAKE3.
/// - `nonce_material`: Raw bytes to derive the nonce from
/// Returns 24 bytes suitable for use as a nonce in cryptographic operations.
/// This function is deterministic - the same input will produce the same nonce.
#[wasm_bindgen(js_name = generateNonce)]
pub fn generate_nonce(nonce_material: &[u8]) -> Box<[u8]> {
    blake3_crypto::generate_nonce(nonce_material)
}

/// Hash data once using BLAKE3.
/// - `data`: Raw bytes to hash
/// Returns 32 bytes of hash output.
/// This is the simplest way to compute a BLAKE3 hash of a single piece of data.
#[wasm_bindgen(js_name = blake3HashOnce)]
pub fn blake3_hash_once(data: &[u8]) -> Box<[u8]> {
    blake3_crypto::blake3_hash_once(data)
}

/// Hash data once using BLAKE3 with a context prefix.
/// - `data`: Raw bytes to hash
/// - `context`: Context bytes to prefix to the data
/// Returns 32 bytes of hash output.
/// This is useful for domain separation - the same data hashed with different contexts will produce different outputs.
#[wasm_bindgen(js_name = blake3HashOnceWithContext)]
pub fn blake3_hash_once_with_context(data: &[u8], context: &[u8]) -> Box<[u8]> {
    blake3_crypto::blake3_hash_once_with_context(data, context)
}

#[wasm_bindgen]
#[derive(Default)]
pub struct Blake3Hasher(blake3::Hasher);

#[wasm_bindgen]
impl Blake3Hasher {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Default::default()
    }

    #[wasm_bindgen]
    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    #[wasm_bindgen]
    pub fn finalize(&self) -> Box<[u8]> {
        self.0.finalize().as_bytes().to_vec().into_boxed_slice()
    }

    #[wasm_bindgen]
    pub fn clone(&self) -> Self {
        // The blake3::Hasher type implements Clone
        Blake3Hasher(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blake3_incremental() {
        // Initial state
        let mut state = Blake3Hasher::new();

        // First update with [1,2,3,4,5]
        let data1 = &[1u8, 2, 3, 4, 5];
        state.update(data1);

        // Check that this matches a direct hash
        let direct_hash = blake3_hash_once(data1);
        let state_hash = state.finalize();
        assert_eq!(
            state_hash, direct_hash,
            "First update should match direct hash"
        );

        // Create new state for second test
        let mut state = Blake3Hasher::new();
        state.update(data1);

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
        let mut state = Blake3Hasher::new();
        let data1 = &[1u8, 2, 3, 4, 5];
        let data2 = &[6u8, 7, 8, 9, 10];
        state.update(data1);
        state.update(data2);

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
        let mut state = Blake3Hasher::new();
        state.update(data1);
        state.update(data2);

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
