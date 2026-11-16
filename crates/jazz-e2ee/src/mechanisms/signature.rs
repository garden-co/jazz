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
    sign_as(MECHANISM, secret_key, record)
}

fn sign_as(
    mechanism: Mechanism<'_>,
    secret_key: &[u8; sign::SECRET_KEY_BYTES],
    record: &[u8],
) -> Result<Vec<u8>> {
    let header = mechanism.header()?;
    let signature = sign::sign(secret_key, &concat(&[&header, record]))?;
    envelope::encode(mechanism, &signature)
}

/// Verifies a signature envelope. Any malformed input is `false`.
pub fn verify(public_key: &[u8], record: &[u8], envelope: &[u8]) -> bool {
    verify_as(MECHANISM, public_key, record, envelope)
}

fn verify_as(mechanism: Mechanism<'_>, public_key: &[u8], record: &[u8], envelope: &[u8]) -> bool {
    let Ok(header) = mechanism.header() else {
        return false;
    };
    match envelope::decode(mechanism, envelope) {
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

fn derive_as(mechanism: Mechanism<'_>, seed: &[u8]) -> Result<KeyPair> {
    let seed: &[u8; sign::SEED_BYTES] = seed.try_into().map_err(|_| Error::InvalidInput)?;
    let pair = sign::SigningKeyPair::from_seed(seed);
    Ok(KeyPair {
        public: PublicKey {
            mechanism: mechanism.into(),
            bytes: pair.public_key.to_vec(),
        },
        secret: SecretKey::new(pair.secret_key.to_vec()),
    })
}

fn sign_key_as(mechanism: Mechanism<'_>, key: &KeyPair, record: &[u8]) -> Result<Vec<u8>> {
    if key.public.mechanism != mechanism {
        return Err(Error::RecipientUnsupported);
    }
    let secret: &[u8; sign::SECRET_KEY_BYTES] = key
        .secret
        .as_bytes()
        .try_into()
        .map_err(|_| Error::InvalidInput)?;
    sign_as(mechanism, secret, record)
}

impl Signer for Builtin {
    fn derive_key_pair(&self, seed: &[u8]) -> Result<KeyPair> {
        derive_as(MECHANISM, seed)
    }

    fn sign(&self, key: &KeyPair, record: &[u8]) -> Result<Vec<u8>> {
        sign_key_as(MECHANISM, key, record)
    }

    fn verify(&self, public: &PublicKey, record: &[u8], signature: &[u8]) -> Result<bool> {
        // Every Ed25519 rejection is a property of the input; the primitive
        // has no failure of its own.
        Ok(public.mechanism == MECHANISM && verify(&public.bytes, record, signature))
    }
}

/// This signer's algorithm under another ID or version, for the
/// conformance kit's header-binding check.
#[cfg(feature = "conformance")]
#[derive(Debug, Clone, Copy)]
pub struct Sibling(pub Mechanism<'static>);

#[cfg(feature = "conformance")]
impl MechanismImpl for Sibling {
    fn mechanism(&self) -> Mechanism<'static> {
        self.0
    }
}

#[cfg(feature = "conformance")]
impl Signer for Sibling {
    fn derive_key_pair(&self, seed: &[u8]) -> Result<KeyPair> {
        derive_as(self.0, seed)
    }

    fn sign(&self, key: &KeyPair, record: &[u8]) -> Result<Vec<u8>> {
        sign_key_as(self.0, key, record)
    }

    fn verify(&self, public: &PublicKey, record: &[u8], signature: &[u8]) -> Result<bool> {
        Ok(public.mechanism == self.0 && verify_as(self.0, &public.bytes, record, signature))
    }
}
