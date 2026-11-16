//! Device signature mechanism `jazz.sodium.sign`, version 1.
//!
//! Signs exactly `H || R`, with `H` this mechanism's header and `R` the
//! caller's canonical record bytes, using single-part detached Ed25519. The
//! envelope's payload is the 64-byte signature. This authenticates bytes, not
//! signer eligibility.

use crate::agility::{KeyPair, MechanismImpl, PublicKey, SecretKey, Signer};
use crate::envelope::{self, Mechanism};
use crate::error::{Error, Result};
use crate::primitives::sign;

use super::concat;

pub const MECHANISM: Mechanism<'static> = Mechanism::new("jazz.sodium.sign", 1);

/// Signs `record`; an inconsistent secret key is [`crate::Error::InvalidInput`].
pub fn sign(secret_key: &[u8; sign::SECRET_KEY_BYTES], record: &[u8]) -> Result<Vec<u8>> {
    let header = MECHANISM.header()?;
    let signature = sign::sign(secret_key, &concat(&[&header, record]))?;
    envelope::encode(MECHANISM, &signature)
}

/// Verifies a signature envelope. Any malformed input is `false`.
pub fn verify(public_key: &[u8], record: &[u8], envelope: &[u8]) -> bool {
    let Ok(header) = MECHANISM.header() else {
        return false;
    };
    match envelope::decode(MECHANISM, envelope) {
        Ok(signature) if signature.len() == sign::SIGNATURE_BYTES => {
            sign::verify(public_key, &concat(&[&header, record]), signature)
        }
        _ => false,
    }
}

/// The built-in Ed25519 mechanism as a [`Signer`]. Seeds are exactly 32
/// bytes; secret keys use libsodium's 64-byte `seed || public key` form.
#[derive(Debug, Clone, Copy, Default)]
pub struct Builtin;

impl MechanismImpl for Builtin {
    fn mechanism(&self) -> Mechanism<'static> {
        MECHANISM
    }
}

impl Signer for Builtin {
    fn derive_key_pair(&self, seed: &[u8]) -> Result<KeyPair> {
        let seed: &[u8; sign::SEED_BYTES] = seed.try_into().map_err(|_| Error::InvalidInput)?;
        let pair = sign::SigningKeyPair::from_seed(seed);
        Ok(KeyPair {
            public: PublicKey {
                mechanism: MECHANISM.into(),
                bytes: pair.public_key.to_vec(),
            },
            secret: SecretKey::new(pair.secret_key.to_vec()),
        })
    }

    fn sign(&self, key: &KeyPair, record: &[u8]) -> Result<Vec<u8>> {
        if key.public.mechanism != MECHANISM {
            return Err(Error::RecipientUnsupported);
        }
        let secret: &[u8; sign::SECRET_KEY_BYTES] = key
            .secret
            .as_bytes()
            .try_into()
            .map_err(|_| Error::InvalidInput)?;
        sign(secret, record)
    }

    fn verify(&self, public: &PublicKey, record: &[u8], signature: &[u8]) -> Result<bool> {
        // Every Ed25519 rejection is a property of the input; the primitive
        // has no failure of its own.
        Ok(public.mechanism == MECHANISM && verify(&public.bytes, record, signature))
    }
}
