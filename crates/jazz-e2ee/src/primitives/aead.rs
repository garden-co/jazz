//! XChaCha20-Poly1305-IETF, combined mode, matching
//! `crypto_aead_xchacha20poly1305_ietf_*`.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use zeroize::Zeroizing;

use crate::error::{Error, Result};

pub const KEY_BYTES: usize = 32;
pub const NONCE_BYTES: usize = 24;
pub const TAG_BYTES: usize = 16;

/// Returns ciphertext followed by its 16-byte tag.
pub fn encrypt(
    key: &[u8; KEY_BYTES],
    nonce: &[u8; NONCE_BYTES],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>> {
    XChaCha20Poly1305::new(key.into())
        .encrypt(
            &XNonce::from(*nonce),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| Error::CryptoFailure)
}

/// Authenticates before returning plaintext; any failure is
/// [`Error::InvalidCiphertext`].
pub fn decrypt(
    key: &[u8; KEY_BYTES],
    nonce: &[u8; NONCE_BYTES],
    aad: &[u8],
    ciphertext: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    if ciphertext.len() < TAG_BYTES {
        return Err(Error::InvalidCiphertext);
    }
    XChaCha20Poly1305::new(key.into())
        .decrypt(
            &XNonce::from(*nonce),
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map(Zeroizing::new)
        .map_err(|_| Error::InvalidCiphertext)
}
