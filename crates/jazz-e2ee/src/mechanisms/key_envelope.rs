//! Symmetric key wrapping, mechanism `jazz.wrap.xchacha20poly1305`, version 1.
//!
//! Carries exactly one 32-byte key under a 32-byte wrapping key. The payload
//! is operation byte 1, a fresh 24-byte nonce and XChaCha20-Poly1305
//! ciphertext, exactly 73 bytes. Associated data is `A = H || 01 || C`; the key
//! is `BLAKE2b-256(key = wrapping key, "jazz.e2ee.wrap-key.v1" || 00 || A)`.
//! Sealing a key to a public key is [`super::key_seal`].

use zeroize::Zeroizing;

use super::concat;
use crate::agility::{KeyWrapper, MechanismImpl};
use crate::envelope::{self, Mechanism};
use crate::error::{Error, Result};
use crate::primitives::{aead, hash};
use crate::random::{RandomSource, random_array};

pub const MECHANISM: Mechanism<'static> = Mechanism::new("jazz.wrap.xchacha20poly1305", 1);
const WRAP: u8 = 1;
const KEY_LABEL: &[u8] = b"jazz.e2ee.wrap-key.v1\0";
const WRAP_PAYLOAD: usize = 1 + aead::NONCE_BYTES + 32 + aead::TAG_BYTES;

fn wrap_aad(context: &[u8]) -> Result<Vec<u8>> {
    Ok(concat(&[&MECHANISM.header()?, &[WRAP], context]))
}

/// Wraps `key` under `wrapping_key` for `context`.
pub fn wrap(
    wrapping_key: &[u8; 32],
    context: &[u8],
    key: &[u8; 32],
    rng: &mut (impl RandomSource + ?Sized),
) -> Result<Vec<u8>> {
    let aad = wrap_aad(context)?;
    let derived = hash::keyed_hash_parts(wrapping_key, &[KEY_LABEL, &aad]);
    let nonce = random_array::<{ aead::NONCE_BYTES }>(rng)?;
    let ciphertext = aead::encrypt(&derived, &nonce, &aad, key)?;
    envelope::encode(MECHANISM, &concat(&[&[WRAP], &nonce, &ciphertext]))
}

/// Returns the wrapped key after authenticating it for `context`.
pub fn unwrap(
    wrapping_key: &[u8; 32],
    context: &[u8],
    envelope: &[u8],
) -> Result<Zeroizing<[u8; 32]>> {
    let payload = envelope::decode(MECHANISM, envelope)?;
    if payload.len() != WRAP_PAYLOAD || payload[0] != WRAP {
        return Err(Error::InvalidCiphertext);
    }
    let aad = wrap_aad(context)?;
    let derived = hash::keyed_hash_parts(wrapping_key, &[KEY_LABEL, &aad]);
    let nonce = payload[1..1 + aead::NONCE_BYTES]
        .try_into()
        .expect("24 bytes");
    let plaintext = aead::decrypt(&derived, nonce, &aad, &payload[1 + aead::NONCE_BYTES..])?;
    let key: [u8; 32] = plaintext[..]
        .try_into()
        .map_err(|_| Error::InvalidCiphertext)?;
    Ok(Zeroizing::new(key))
}

/// The built-in key wrapping mechanism as a [`KeyWrapper`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Builtin;

impl MechanismImpl for Builtin {
    fn mechanism(&self) -> Mechanism<'static> {
        MECHANISM
    }
}

impl KeyWrapper for Builtin {
    fn wrap(
        &self,
        wrapping_key: &[u8; 32],
        context: &[u8],
        key: &[u8; 32],
        rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        wrap(wrapping_key, context, key, rng)
    }

    fn unwrap(
        &self,
        wrapping_key: &[u8; 32],
        context: &[u8],
        envelope: &[u8],
    ) -> Result<Zeroizing<[u8; 32]>> {
        unwrap(wrapping_key, context, envelope)
    }
}
