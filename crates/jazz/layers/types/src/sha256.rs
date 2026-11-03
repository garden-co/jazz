//! SHA-256, the hash Jazz uses for content identities, derived ids and
//! fingerprints.
//!
//! JavaScript can compute any of them with WebCrypto, and object stores that
//! check SHA-256 checksums verify large-value nodes directly. Two things are
//! not this module: RFC 9562 UUIDv5 ids (account, app and object ids keep
//! their identities) and E2EE, which uses libsodium's keyed BLAKE2b.
//!
//! [`derive_key`] separates contexts by hashing the context's length and
//! bytes before the input. Its outputs are public identifiers, never MACs
//! over secrets, so SHA-256's length extension does not apply.

use sha2::Digest;

/// A SHA-256 digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hash([u8; 32]);

impl Hash {
    /// The digest's bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The digest in lowercase hex.
    pub fn to_hex(&self) -> String {
        let mut hex = String::with_capacity(64);
        for byte in self.0 {
            hex.push_str(&format!("{byte:02x}"));
        }
        hex
    }
}

impl From<Hash> for [u8; 32] {
    fn from(hash: Hash) -> Self {
        hash.0
    }
}

/// An incremental SHA-256 hasher.
#[derive(Clone, Default)]
pub struct Hasher(sha2::Sha256);

impl Hasher {
    /// An empty hasher.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends `bytes` to the input.
    pub fn update(&mut self, bytes: &[u8]) -> &mut Self {
        self.0.update(bytes);
        self
    }

    /// The digest of everything written so far; the hasher stays usable.
    pub fn finalize(&self) -> Hash {
        Hash(self.0.clone().finalize().into())
    }
}

impl std::fmt::Debug for Hasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("sha256::Hasher")
    }
}

/// SHA-256 of `bytes`.
pub fn hash(bytes: &[u8]) -> Hash {
    Hash(sha2::Sha256::digest(bytes).into())
}

/// A 32-byte identifier for `input` in `context`: SHA-256 over the
/// context's length (u64, little-endian), the context and the input.
pub fn derive_key(context: &str, input: &[u8]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher
        .update(&(context.len() as u64).to_le_bytes())
        .update(context.as_bytes())
        .update(input);
    hasher.finalize().0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_sha256_test_vectors() {
        assert_eq!(
            hash(b"abc").to_hex(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let mut hasher = Hasher::new();
        hasher.update(b"a").update(b"bc");
        assert_eq!(hasher.finalize(), hash(b"abc"));
    }

    /// JavaScript recomputes these ids with WebCrypto, so each context Jazz
    /// derives under is pinned against an independent computation
    /// (Python's hashlib over the same length-prefixed preimage).
    #[test]
    fn derive_key_contexts_have_known_answers() {
        for (context, expected) in [
            (
                "jazz committed batch id v1",
                "b7951e6ea8869e3814c5301bc5934a0027d05c78186db48dd7f2cb5b35763a4f",
            ),
            (
                "jazz typed schema view id v1",
                "16d3bb47178c6da87b2f3f5152a55a97b41401e21784eaac6703eb183747783c",
            ),
            (
                "jazz.authority-policy-binding-directory.v1",
                "0e08a77a6b5fc18ae365ede839b4f1319e1f951b72e2b2b734913ecfe467092a",
            ),
        ] {
            let derived = derive_key(context, b"abc");
            let hex: String = derived.iter().map(|byte| format!("{byte:02x}")).collect();
            assert_eq!(hex, expected, "{context}");
        }
    }

    #[test]
    fn derive_key_separates_contexts_by_length() {
        assert_ne!(derive_key("ab", b"c"), derive_key("a", b"bc"));
        let mut expected = Vec::new();
        expected.extend_from_slice(&2u64.to_le_bytes());
        expected.extend_from_slice(b"ab");
        expected.extend_from_slice(b"c");
        assert_eq!(&derive_key("ab", b"c"), hash(&expected).as_bytes());
    }
}
