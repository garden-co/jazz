use blake3::*;

/// The length of the short hash (19 bytes before base58 encoding)
/// Matches TypeScript's `shortHashLength`
pub const SHORT_HASH_LENGTH: usize = 19;

/// Compute a short hash of data with a custom prefix.
/// This is the common implementation used by `short_hash` and `compute_co_id_from_header`.
///
/// Steps:
/// 1. BLAKE3 hash the input bytes
/// 2. Take first 19 bytes
/// 3. Base58 encode
/// 4. Prefix with the given prefix
pub fn short_hash_with_prefix(data: &[u8], prefix: &str) -> String {
    let hash = blake3_hash_once(data);
    let short_hash = &hash[..SHORT_HASH_LENGTH];
    let encoded = bs58::encode(short_hash).into_string();
    format!("{}{}", prefix, encoded)
}

/// Compute a short hash of a JSON value.
/// This mirrors TypeScript's `shortHash` function in crypto.ts:
/// 1. BLAKE3 hash the JSON bytes
/// 2. Take first 19 bytes
/// 3. Base58 encode
/// 4. Prefix with "shortHash_z"
///
/// The input should be a stable-stringified JSON value.
pub fn short_hash(value: &str) -> String {
    short_hash_with_prefix(value.as_bytes(), "shortHash_z")
}

/// Generate a 24-byte nonce from input material using BLAKE3.
/// - `nonce_material`: Raw bytes to derive the nonce from
/// Returns 24 bytes suitable for use as a nonce in cryptographic operations.
/// This function is deterministic - the same input will produce the same nonce.
pub fn generate_nonce(nonce_material: &[u8]) -> Box<[u8]> {
    let mut hasher = Hasher::new();
    hasher.update(nonce_material);
    hasher.finalize().as_bytes()[..24].into()
}

/// Hash data once using BLAKE3.
/// - `data`: Raw bytes to hash
/// Returns 32 bytes of hash output.
/// This is the simplest way to compute a BLAKE3 hash of a single piece of data.
pub fn blake3_hash_once(data: &[u8]) -> Box<[u8]> {
    let mut hasher = Blake3Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Hash data once using BLAKE3 with a context prefix.
/// - `data`: Raw bytes to hash
/// - `context`: Context bytes to prefix to the data
/// Returns 32 bytes of hash output.
/// This is useful for domain separation - the same data hashed with different contexts will produce different outputs.
pub fn blake3_hash_once_with_context(data: &[u8], context: &[u8]) -> Box<[u8]> {
    let mut hasher = Blake3Hasher::new();
    hasher.update(context);
    hasher.update(data);
    hasher.finalize()
}

#[derive(Default)]
pub struct Blake3Hasher(Hasher);

impl Blake3Hasher {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    pub fn finalize(&self) -> Box<[u8]> {
        self.0.finalize().as_bytes().to_vec().into_boxed_slice()
    }
}

impl Clone for Blake3Hasher {
    fn clone(&self) -> Self {
        Blake3Hasher(self.0.clone())
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

        assert_eq!(nonce.to_vec(), nonce2.to_vec());

        // Different input should produce different nonce
        let nonce3 = generate_nonce(b"different input");
        assert_ne!(nonce.to_vec(), nonce3.to_vec());
    }

    #[test]
    fn test_blake3_hash_once() {
        const INPUT_STR: &[u8] = b"test input";
        let hash = blake3_hash_once(INPUT_STR);

        // BLAKE3 produces 32-byte hashes
        assert_eq!(hash.len(), 32);

        // Same input should produce same hash
        let hash2 = blake3_hash_once(INPUT_STR);
        assert_eq!(hash.to_vec(), hash2.to_vec());

        // Different input should produce different hash
        let hash3 = blake3_hash_once(b"different input");
        assert_ne!(hash.to_vec(), hash3.to_vec());
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
        assert_eq!(hash.to_vec(), hash2.to_vec());

        // Different input should produce different hash
        let hash3 = blake3_hash_once_with_context(b"different input", CONTEXT_BYTES);
        assert_ne!(hash.to_vec(), hash3.to_vec());

        // Different context should produce different hash
        let hash4 = blake3_hash_once_with_context(INPUT_BYTES, b"different context");
        assert_ne!(hash.to_vec(), hash4.to_vec());

        // Hash with context should be different from hash without context
        let hash_no_context = blake3_hash_once(INPUT_BYTES);
        assert_ne!(hash.to_vec(), hash_no_context.to_vec());
    }

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
            state_hash.to_vec(),
            direct_hash.to_vec(),
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
            state.finalize().to_vec(),
            expected_first_hash.to_vec(),
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

        let direct_hash_all = blake3_hash_once(all_data.as_slice());
        assert_eq!(
            state.finalize().to_vec(),
            direct_hash_all.to_vec(),
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
            state.finalize().to_vec(),
            expected_final_hash.to_vec(),
            "Final state should match expected hash"
        );
    }

    #[test]
    fn test_short_hash_format() {
        let value = r#"{"test":"value"}"#;
        let hash = short_hash(value);

        // Should start with "shortHash_z"
        assert!(hash.starts_with("shortHash_z"));

        // The base58 encoded part should be reasonable length
        // 19 bytes base58 encoded is roughly 26 characters
        assert!(hash.len() > 11); // "shortHash_z" is 11 chars
    }

    #[test]
    fn test_short_hash_deterministic() {
        let value = r#"{"hello":"world","number":42}"#;

        // Same input should produce same hash
        let hash1 = short_hash(value);
        let hash2 = short_hash(value);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_short_hash_different_values() {
        let hash1 = short_hash(r#"{"a":1}"#);
        let hash2 = short_hash(r#"{"a":2}"#);
        let hash3 = short_hash(r#"{"b":1}"#);

        // Different values should produce different hashes
        assert_ne!(hash1, hash2);
        assert_ne!(hash1, hash3);
        assert_ne!(hash2, hash3);
    }

    #[test]
    fn test_short_hash_length() {
        // Verify that SHORT_HASH_LENGTH is used correctly
        assert_eq!(SHORT_HASH_LENGTH, 19);

        let hash = short_hash("test");
        // "shortHash_z" prefix (11 chars) + base58 encoded 19 bytes
        assert!(hash.starts_with("shortHash_z"));
    }
}
