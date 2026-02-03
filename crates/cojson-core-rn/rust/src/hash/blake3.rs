use cojson_core::hash;

#[derive(thiserror::Error, Debug, uniffi::Error)]
pub enum Blake3Error {
    #[error("Failed to acquire lock")]
    LockError,
}

/// Compute a short hash of a stable-stringified JSON value.
/// The input should already be serialized using stableStringify on the JS side.
/// Returns a string prefixed with "shortHash_z" followed by base58-encoded hash.
#[uniffi::export]
pub fn short_hash(value: String) -> String {
    hash::blake3::short_hash(&value)
}

/// Generate a 24-byte nonce from input material using BLAKE3.
/// - `nonce_material`: Raw bytes to derive the nonce from
/// Returns 24 bytes suitable for use as a nonce in cryptographic operations.
/// This function is deterministic - the same input will produce the same nonce.
#[uniffi::export]
pub fn generate_nonce(nonce_material: &[u8]) -> Vec<u8> {
    hash::blake3::generate_nonce(nonce_material).into()
}

/// Hash data once using BLAKE3.
/// - `data`: Raw bytes to hash
/// Returns 32 bytes of hash output.
/// This is the simplest way to compute a BLAKE3 hash of a single piece of data.
#[uniffi::export]
pub fn blake3_hash_once(data: &[u8]) -> Vec<u8> {
    hash::blake3::blake3_hash_once(data).into()
}

/// Hash data once using BLAKE3 with a context prefix.
/// - `data`: Raw bytes to hash
/// - `context`: Context bytes to prefix to the data
/// Returns 32 bytes of hash output.
/// This is useful for domain separation - the same data hashed with different contexts will produce different outputs.
#[uniffi::export]
pub fn blake3_hash_once_with_context(data: &[u8], context: &[u8]) -> Vec<u8> {
    hash::blake3::blake3_hash_once_with_context(data, context).into()
}

#[derive(uniffi::Object)]
pub struct Blake3Hasher {
    hasher: std::sync::Mutex<blake3::Hasher>,
}

#[uniffi::export]
impl Blake3Hasher {
    #[uniffi::constructor]
    pub fn new() -> Self {
        Blake3Hasher {
            hasher: std::sync::Mutex::new(blake3::Hasher::new()),
        }
    }

    pub fn finalize(&self) -> Result<Vec<u8>, Blake3Error> {
        if let Ok(h) = self.hasher.lock() {
            Ok(h.finalize().as_bytes().to_vec())
        } else {
            Err(Blake3Error::LockError)
        }
    }

    pub fn update(&self, data: &[u8]) -> Result<(), Blake3Error> {
        if let Ok(mut h) = self.hasher.lock() {
            h.update(data);
            Ok(())
        } else {
            Err(Blake3Error::LockError)
        }
    }

    pub fn clone_hasher(&self) -> Result<Self, Blake3Error> {
        if let Ok(h) = self.hasher.lock() {
            Ok(Blake3Hasher {
                hasher: std::sync::Mutex::new(h.clone()),
            })
        } else {
            Err(Blake3Error::LockError)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nonce_generation() {
        const INPUT_STR: &[u8] = b"test input";

        let nonce = generate_nonce(INPUT_STR);
        assert_eq!(nonce.len(), 24);

        // Same input should produce same nonce
        let nonce2 = generate_nonce(INPUT_STR);
        assert_eq!(nonce, nonce2);

        // Different input should produce different nonce
        let nonce3 = generate_nonce(b"different input");
        assert_ne!(nonce, nonce3);
    }

    #[test]
    fn test_blake3_hash_once() {
        const INPUT_STR: &[u8] = b"test input";
        let hash = blake3_hash_once(INPUT_STR);

        // BLAKE3 produces 32-byte hashes
        assert_eq!(hash.len(), 32);

        // Same input should produce same hash
        let hash2 = blake3_hash_once(INPUT_STR);
        assert_eq!(hash, hash2);

        // Different input should produce different hash
        let hash3 = blake3_hash_once(b"different input");
        assert_ne!(hash, hash3);
    }

    #[test]
    fn test_blake3_hash_once_with_context() {
        const INPUT_BYTES: &[u8] = b"test input";
        const CONTEXT_BYTES: &[u8] = b"test context";

        let hash = blake3_hash_once_with_context(INPUT_BYTES, CONTEXT_BYTES);

        // BLAKE3 produces 32-byte hashes
        assert_eq!(hash.len(), 32);

        // Same input and context should produce same hash
        let hash2 = blake3_hash_once_with_context(INPUT_BYTES, CONTEXT_BYTES);
        assert_eq!(hash, hash2);

        // Different input should produce different hash
        let hash3 = blake3_hash_once_with_context(b"different input", CONTEXT_BYTES);
        assert_ne!(hash, hash3);

        // Different context should produce different hash
        let hash4 = blake3_hash_once_with_context(INPUT_BYTES, b"different context");
        assert_ne!(hash, hash4);

        // Hash with context should be different from hash without context
        let hash_no_context = blake3_hash_once(INPUT_BYTES);
        assert_ne!(hash, hash_no_context);
    }

    #[test]
    fn test_blake3_incremental() {
        // Initial state
        let state = Blake3Hasher::new();

        // First update with [1,2,3,4,5]
        let data1 = &[1u8, 2, 3, 4, 5];
        state.update(data1).unwrap();

        // Check that this matches a direct hash
        let direct_hash = blake3_hash_once(data1);
        let state_hash = state.finalize().unwrap();
        assert_eq!(
            state_hash, direct_hash,
            "First update should match direct hash"
        );

        // Create new state for second test
        let state = Blake3Hasher::new();
        state.update(data1).unwrap();

        // Verify the exact expected hash from the TypeScript test for the first update
        let expected_first_hash = vec![
            2, 79, 103, 192, 66, 90, 61, 192, 47, 186, 245, 140, 185, 61, 229, 19, 46, 61, 117,
            197, 25, 250, 160, 186, 218, 33, 73, 29, 136, 201, 112, 87,
        ];
        assert_eq!(
            state.finalize().unwrap(),
            expected_first_hash,
            "First update should match expected hash"
        );

        // Test with two updates
        let state = Blake3Hasher::new();
        let data1 = &[1u8, 2, 3, 4, 5];
        let data2 = &[6u8, 7, 8, 9, 10];
        state.update(data1).unwrap();
        state.update(data2).unwrap();

        // Compare with a single hash of all data
        let mut all_data = Vec::new();
        all_data.extend_from_slice(data1);
        all_data.extend_from_slice(data2);

        let direct_hash_all = blake3_hash_once(all_data.as_slice());
        assert_eq!(
            state.finalize().unwrap(),
            direct_hash_all,
            "Final state should match direct hash of all data"
        );

        // Test final hash matches expected value
        let state = Blake3Hasher::new();
        state.update(data1).unwrap();
        state.update(data2).unwrap();

        let expected_final_hash = vec![
            165, 131, 141, 69, 2, 69, 39, 236, 196, 244, 180, 213, 147, 124, 222, 39, 68, 223, 54,
            176, 242, 97, 200, 101, 204, 79, 21, 233, 56, 51, 1, 199,
        ];
        assert_eq!(
            state.finalize().unwrap(),
            expected_final_hash,
            "Final state should match expected hash"
        );
    }
}
