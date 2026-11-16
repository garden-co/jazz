//! `crypto_generichash` (BLAKE2b), keyed, and SHA-256.

use blake2::Blake2bMac;
use blake2::digest::consts::U32;
use blake2::digest::{KeyInit, Mac};
use zeroize::Zeroizing;

/// Keyed BLAKE2b with a 32-byte key and 32-byte output, as the formats use it.
pub fn keyed_hash(key: &[u8; 32], input: &[u8]) -> Zeroizing<[u8; 32]> {
    keyed_hash_parts(key, &[input])
}

/// [`keyed_hash`] over the concatenation of `parts`, without copying them.
pub fn keyed_hash_parts(key: &[u8; 32], parts: &[&[u8]]) -> Zeroizing<[u8; 32]> {
    let mut mac = <Blake2bMac<U32> as KeyInit>::new_from_slice(key)
        .expect("a 32-byte BLAKE2b key is within the 64-byte limit");
    for part in parts {
        Mac::update(&mut mac, part);
    }
    Zeroizing::new(mac.finalize().into_bytes().into())
}

/// SHA-256, for the protocol's public derived IDs.
pub fn sha256(input: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(input).into()
}
