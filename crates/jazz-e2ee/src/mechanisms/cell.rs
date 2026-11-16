//! Cell mechanism `jazz.sodium.cell`, version 1.
//!
//! Payload: a fresh 24-byte nonce, then XChaCha20-Poly1305-IETF ciphertext
//! with its 16-byte tag. With `H` this mechanism's header and `C` the
//! operation context, the associated data is `H || C` and the key is
//! `BLAKE2b-256(key = epoch secret, "jazz.e2ee.cell-key.v1" || 00 || H || C)`.

use zeroize::Zeroizing;

use super::concat;
use crate::agility::{CellCipher, MechanismImpl};
use crate::envelope::{self, Mechanism};
use crate::error::{Error, Result};
use crate::primitives::{aead, hash};
use crate::random::{RandomSource, random_array};

pub const MECHANISM: Mechanism<'static> = Mechanism::new("jazz.sodium.cell", 1);
const KEY_LABEL: &[u8] = b"jazz.e2ee.cell-key.v1\0";
const MIN_PAYLOAD: usize = aead::NONCE_BYTES + aead::TAG_BYTES;

fn derive(key: &[u8; 32], aad: &[u8]) -> Zeroizing<[u8; 32]> {
    hash::keyed_hash_parts(key, &[KEY_LABEL, aad])
}

/// Encrypts `plaintext` (which may be empty) under a fresh nonce.
pub fn encrypt(
    key: &[u8; 32],
    context: &[u8],
    plaintext: &[u8],
    rng: &mut (impl RandomSource + ?Sized),
) -> Result<Vec<u8>> {
    let aad = concat(&[&MECHANISM.header()?, context]);
    let derived = derive(key, &aad);
    let nonce = random_array::<{ aead::NONCE_BYTES }>(rng)?;
    let ciphertext = aead::encrypt(&derived, &nonce, &aad, plaintext)?;
    envelope::encode(MECHANISM, &concat(&[&nonce, &ciphertext]))
}

/// Authenticates the envelope against `key` and `context`, then returns its
/// plaintext.
pub fn decrypt(key: &[u8; 32], context: &[u8], envelope: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    let payload = envelope::decode(MECHANISM, envelope)?;
    if payload.len() < MIN_PAYLOAD {
        return Err(Error::InvalidCiphertext);
    }
    let aad = concat(&[&MECHANISM.header()?, context]);
    let derived = derive(key, &aad);
    let (nonce, ciphertext) = payload.split_at(aead::NONCE_BYTES);
    aead::decrypt(
        &derived,
        nonce.try_into().expect("24 bytes"),
        &aad,
        ciphertext,
    )
}

/// The built-in cell mechanism as a [`CellCipher`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Builtin;

impl MechanismImpl for Builtin {
    fn mechanism(&self) -> Mechanism<'static> {
        MECHANISM
    }
}

impl CellCipher for Builtin {
    fn encrypt(
        &self,
        key: &[u8; 32],
        context: &[u8],
        plaintext: &[u8],
        rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        encrypt(key, context, plaintext, rng)
    }

    fn decrypt(
        &self,
        key: &[u8; 32],
        context: &[u8],
        envelope: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>> {
        decrypt(key, context, envelope)
    }
}
