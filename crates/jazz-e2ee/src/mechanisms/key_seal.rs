//! Key sealing mechanism `jazz.wrap.hpke-x25519`, version 1.
//!
//! Seals exactly one 32-byte key to a recipient's public key with HPKE
//! (RFC 9180) base mode, single-shot, suite DHKEM(X25519, HKDF-SHA256),
//! HKDF-SHA256, ChaCha20Poly1305. With `H` this mechanism's header and `C`
//! the operation context:
//!
//! - HPKE `info` is `H`, binding the mechanism and version into the key
//!   schedule;
//! - HPKE `aad` is `C`, the canonical context bytes, unchanged;
//! - the plaintext is the 32-byte key.
//!
//! The payload is the 32-byte encapsulated key followed by the 48-byte
//! ciphertext, exactly 80 bytes. Sealing authenticates the contents to the
//! recipient, not a sender identity.

use zeroize::Zeroizing;

use super::concat;
use crate::agility::{KeyPair, KeySealer, MechanismImpl, PublicKey, SecretKey};
use crate::envelope::{self, Mechanism};
use crate::error::{Error, Result};
use crate::primitives::hpke::{self, ENCAPSULATED_BYTES, HpkeKeyPair, TAG_BYTES};
use crate::random::RandomSource;

pub const MECHANISM: Mechanism<'static> = Mechanism::new("jazz.wrap.hpke-x25519", 1);
const PAYLOAD: usize = ENCAPSULATED_BYTES + 32 + TAG_BYTES;

/// Seals `key` to `public_key` for `context`.
pub fn seal(
    public_key: &[u8; hpke::PUBLIC_KEY_BYTES],
    context: &[u8],
    key: &[u8; 32],
    rng: &mut (impl RandomSource + ?Sized),
) -> Result<Vec<u8>> {
    seal_as(MECHANISM, public_key, context, key, rng)
}

fn seal_as(
    mechanism: Mechanism<'_>,
    public_key: &[u8; hpke::PUBLIC_KEY_BYTES],
    context: &[u8],
    key: &[u8; 32],
    rng: &mut (impl RandomSource + ?Sized),
) -> Result<Vec<u8>> {
    let header = mechanism.header()?;
    let (encapsulated, ciphertext) = hpke::seal(public_key, &header, context, key, rng)?;
    envelope::encode(mechanism, &concat(&[&encapsulated, &ciphertext]))
}

/// Opens an envelope sealed to `recipient` for `context`.
pub fn open(
    recipient: &HpkeKeyPair,
    context: &[u8],
    envelope: &[u8],
) -> Result<Zeroizing<[u8; 32]>> {
    open_as(MECHANISM, recipient, context, envelope)
}

fn open_as(
    mechanism: Mechanism<'_>,
    recipient: &HpkeKeyPair,
    context: &[u8],
    envelope: &[u8],
) -> Result<Zeroizing<[u8; 32]>> {
    let payload = envelope::decode(mechanism, envelope)?;
    if payload.len() != PAYLOAD {
        return Err(Error::InvalidCiphertext);
    }
    let (encapsulated, ciphertext) = payload.split_at(ENCAPSULATED_BYTES);
    let header = mechanism.header()?;
    let plaintext = hpke::open(
        &recipient.secret_key,
        encapsulated.try_into().expect("32 bytes"),
        &header,
        context,
        ciphertext,
    )?;
    let key: [u8; 32] = plaintext[..]
        .try_into()
        .map_err(|_| Error::InvalidCiphertext)?;
    Ok(Zeroizing::new(key))
}

/// The built-in HPKE mechanism as a [`KeySealer`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Builtin;

impl MechanismImpl for Builtin {
    fn mechanism(&self) -> Mechanism<'static> {
        MECHANISM
    }
}

fn own_public<'k>(
    mechanism: Mechanism<'_>,
    key: &'k PublicKey,
) -> Result<&'k [u8; hpke::PUBLIC_KEY_BYTES]> {
    if key.mechanism != mechanism {
        return Err(Error::RecipientUnsupported);
    }
    key.bytes[..].try_into().map_err(|_| Error::InvalidInput)
}

fn derive_as(mechanism: Mechanism<'_>, ikm: &[u8]) -> Result<KeyPair> {
    if ikm.len() < 32 {
        return Err(Error::InvalidInput);
    }
    let pair = HpkeKeyPair::derive(ikm);
    Ok(KeyPair {
        public: PublicKey {
            mechanism: mechanism.into(),
            bytes: pair.public_key.to_vec(),
        },
        secret: SecretKey::new(pair.secret_key.to_vec()),
    })
}

fn seal_key_as(
    mechanism: Mechanism<'_>,
    recipient: &PublicKey,
    context: &[u8],
    key: &[u8; 32],
    rng: &mut dyn RandomSource,
) -> Result<Vec<u8>> {
    seal_as(
        mechanism,
        own_public(mechanism, recipient)?,
        context,
        key,
        rng,
    )
}

fn open_key_as(
    mechanism: Mechanism<'_>,
    recipient: &KeyPair,
    context: &[u8],
    envelope: &[u8],
) -> Result<Zeroizing<[u8; 32]>> {
    let public_key = *own_public(mechanism, &recipient.public)?;
    let secret_key: [u8; hpke::SECRET_KEY_BYTES] = recipient
        .secret
        .as_bytes()
        .try_into()
        .map_err(|_| Error::InvalidInput)?;
    let pair = HpkeKeyPair {
        public_key,
        secret_key: Zeroizing::new(secret_key),
    };
    open_as(mechanism, &pair, context, envelope)
}

impl KeySealer for Builtin {
    fn derive_key_pair(&self, ikm: &[u8]) -> Result<KeyPair> {
        derive_as(MECHANISM, ikm)
    }

    fn seal(
        &self,
        recipient: &PublicKey,
        context: &[u8],
        key: &[u8; 32],
        rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        seal_key_as(MECHANISM, recipient, context, key, rng)
    }

    fn open(
        &self,
        recipient: &KeyPair,
        context: &[u8],
        envelope: &[u8],
    ) -> Result<Zeroizing<[u8; 32]>> {
        open_key_as(MECHANISM, recipient, context, envelope)
    }
}

/// This sealer's algorithm under another ID or version, for the conformance
/// kit's header-binding check.
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
impl KeySealer for Sibling {
    fn derive_key_pair(&self, ikm: &[u8]) -> Result<KeyPair> {
        derive_as(self.0, ikm)
    }

    fn seal(
        &self,
        recipient: &PublicKey,
        context: &[u8],
        key: &[u8; 32],
        rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        seal_key_as(self.0, recipient, context, key, rng)
    }

    fn open(
        &self,
        recipient: &KeyPair,
        context: &[u8],
        envelope: &[u8],
    ) -> Result<Zeroizing<[u8; 32]>> {
        open_key_as(self.0, recipient, context, envelope)
    }
}
