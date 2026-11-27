//! `crypto_generichash` (BLAKE2b), keyed, and SHA-256.

use blake2::Blake2bMac;
use blake2::digest::consts::U32;
use blake2::digest::{FixedOutput, KeyInit, Mac};
use zeroize::Zeroizing;

/// Keyed BLAKE2b with a 32-byte key and 32-byte output, as the formats use it.
pub fn keyed_hash(key: &[u8; 32], input: &[u8]) -> Zeroizing<[u8; 32]> {
    keyed_hash_parts(key, &[input])
}

/// [`keyed_hash`] over the concatenation of `parts`, without copying them.
///
/// The output is written straight into the returned buffer, with no
/// unprotected copy. The MAC state (keyed chaining value and buffered input)
/// is cleared when `finalize_into` drops it: `blake2`'s `zeroize` feature,
/// pinned by `mac_state_is_zeroized_on_drop`. The provider's own 64-byte
/// finalization scratch on its stack is not cleared.
pub fn keyed_hash_parts(key: &[u8; 32], parts: &[&[u8]]) -> Zeroizing<[u8; 32]> {
    let mut mac = <Blake2bMac<U32> as KeyInit>::new_from_slice(key)
        .expect("a 32-byte BLAKE2b key is within the 64-byte limit");
    for part in parts {
        Mac::update(&mut mac, part);
    }
    let mut out = Zeroizing::new([0; 32]);
    FixedOutput::finalize_into(mac, (&mut *out).into());
    out
}

/// SHA-256.
pub fn sha256(input: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(input).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fails to compile if the MAC stops clearing its keyed state on drop
    /// (for example, `blake2`'s `zeroize` feature is dropped).
    #[test]
    fn mac_state_is_zeroized_on_drop() {
        fn clears_on_drop<T: zeroize::ZeroizeOnDrop>() {}
        clears_on_drop::<Blake2bMac<U32>>();
    }

    /// The keyed hash is BLAKE2b-256 with the key (libsodium's
    /// `crypto_generichash`), however its input is split.
    #[test]
    fn parts_hash_as_their_concatenation() {
        let key = [7; 32];
        assert_eq!(
            *keyed_hash_parts(&key, &[b"ab", b"", b"c"]),
            *keyed_hash(&key, b"abc")
        );
    }
}
