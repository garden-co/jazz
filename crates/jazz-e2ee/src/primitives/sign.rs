//! `crypto_sign_*` single-part detached Ed25519 (not Ed25519ph).
//!
//! Verification follows libsodium's rules: it rejects a non-canonical `S`,
//! a small-order `R` or public key, and a non-canonical public-key encoding.

use curve25519_dalek::edwards::CompressedEdwardsY;
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use zeroize::Zeroizing;

use crate::error::{Error, Result};
use crate::random::{RandomSource, random_array};

pub const PUBLIC_KEY_BYTES: usize = 32;
/// libsodium's `seed || public key` representation.
pub const SECRET_KEY_BYTES: usize = 64;
pub const SEED_BYTES: usize = 32;
pub const SIGNATURE_BYTES: usize = 64;

/// An Ed25519 keypair in libsodium's representation.
pub struct SigningKeyPair {
    pub public_key: [u8; PUBLIC_KEY_BYTES],
    pub secret_key: Zeroizing<[u8; SECRET_KEY_BYTES]>,
}

impl SigningKeyPair {
    /// `crypto_sign_keypair`: a keypair from 32 fresh random seed bytes.
    pub fn generate(rng: &mut (impl RandomSource + ?Sized)) -> Result<Self> {
        let seed = Zeroizing::new(random_array::<SEED_BYTES>(rng)?);
        Ok(Self::from_seed(&seed))
    }

    /// `crypto_sign_seed_keypair`.
    pub fn from_seed(seed: &[u8; SEED_BYTES]) -> Self {
        let signing = SigningKey::from_bytes(seed);
        Self {
            public_key: signing.verifying_key().to_bytes(),
            secret_key: Zeroizing::new(signing.to_keypair_bytes()),
        }
    }
}

/// `crypto_sign_detached`. The secret key's public half must match its seed;
/// an inconsistent key is [`Error::InvalidInput`]. The caller's key is not
/// modified, and the reconstructed key is cleared on drop.
pub fn sign(secret_key: &[u8; SECRET_KEY_BYTES], message: &[u8]) -> Result<[u8; SIGNATURE_BYTES]> {
    let signing = SigningKey::from_keypair_bytes(secret_key).map_err(|_| Error::InvalidInput)?;
    Ok(signing.sign(message).to_bytes())
}

/// Whether `public_key` encodes a curve point canonically, as libsodium
/// requires: it decompresses, and the point re-encodes to the same bytes. An
/// encoding of `y + p` (possible for `y < 19`) names the same point as `y`
/// but is refused.
pub fn is_canonical_public_key(public_key: &[u8; PUBLIC_KEY_BYTES]) -> bool {
    CompressedEdwardsY(*public_key)
        .decompress()
        .is_some_and(|point| point.compress().as_bytes() == public_key)
}

/// `crypto_sign_verify_detached`. Malformed keys or signatures are `false`.
pub fn verify(public_key: &[u8], message: &[u8], signature: &[u8]) -> bool {
    let (Ok(public_key), Ok(signature)) = (
        <&[u8; PUBLIC_KEY_BYTES]>::try_from(public_key),
        <&[u8; SIGNATURE_BYTES]>::try_from(signature),
    ) else {
        return false;
    };
    if !is_canonical_public_key(public_key) {
        return false;
    }
    let Ok(verifying) = VerifyingKey::from_bytes(public_key) else {
        return false;
    };
    // `verify_strict` rejects small-order keys and `R`, and non-canonical `S`.
    verifying
        .verify_strict(message, &Signature::from_bytes(signature))
        .is_ok()
}
