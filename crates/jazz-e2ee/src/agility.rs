//! Mechanism agility: one small trait per cryptographic role, keys that name
//! their mechanism, and the per-role configuration a sender selects from.
//!
//! Every stored or transmitted byte string already starts with a common
//! envelope header naming its mechanism ID and version, so readers route by
//! that header through a [`Registry`](crate::registry::Registry). Writers
//! use the mechanism the application's [`MechanismConfig`] selects for the
//! role. A custom mechanism (for example a post-quantum KEM) implements the
//! trait for its role, is registered under a reverse-DNS ID such as
//! `com.example.kem`, passes [`crate::conformance`], and is selected in the
//! configuration. Mechanisms are compiled in: there are no runtime callbacks
//! into other languages.
//!
//! All traits are synchronous and do no I/O. Secrets cross them only as
//! [`Zeroizing`] values or [`SecretKey`]s, which clear on drop.

use core::fmt;

use zeroize::Zeroizing;

use crate::envelope::{self, Mechanism};
use crate::error::{Error, Result};
use crate::random::RandomSource;

/// An owned mechanism ID and version, as carried in configuration and keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MechanismId {
    pub id: String,
    pub version: u32,
}

impl MechanismId {
    pub fn new(id: impl Into<String>, version: u32) -> Self {
        Self {
            id: id.into(),
            version,
        }
    }

    pub fn as_mechanism(&self) -> Mechanism<'_> {
        Mechanism::new(&self.id, self.version)
    }
}

impl From<Mechanism<'_>> for MechanismId {
    fn from(mechanism: Mechanism<'_>) -> Self {
        Self::new(mechanism.id, mechanism.version)
    }
}

impl PartialEq<Mechanism<'_>> for MechanismId {
    fn eq(&self, other: &Mechanism<'_>) -> bool {
        self.id == other.id && self.version == other.version
    }
}

/// The namespace reserved for Jazz's own mechanisms.
pub const RESERVED_PREFIX: &str = "jazz.";

/// Whether `id` is a valid custom mechanism ID: a reverse-DNS name of at
/// least two dot-separated labels of `[a-z0-9-]`, at most 64 bytes, outside
/// the reserved `jazz.` namespace.
pub fn is_custom_id(id: &str) -> bool {
    Mechanism::new(id, 1).validate().is_ok()
        && !id.starts_with(RESERVED_PREFIX)
        && id.split('.').count() >= 2
        && id
            .split('.')
            .all(|label| !label.is_empty() && !label.starts_with('-') && !label.ends_with('-'))
}

/// A public key and the mechanism it belongs to. Devices, groups and
/// recovery roots advertise these; a sender seals only to a key of the
/// mechanism it selected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicKey {
    pub mechanism: MechanismId,
    pub bytes: Vec<u8>,
}

impl PublicKey {
    /// The advertised form: a common envelope of the key's mechanism whose
    /// payload is the raw key.
    pub fn encode(&self) -> Result<Vec<u8>> {
        envelope::encode(self.mechanism.as_mechanism(), &self.bytes)
    }

    /// Reads the advertised form of a key of any mechanism.
    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let (id, version, payload) = envelope::parse(encoded)?;
        let id = core::str::from_utf8(id).map_err(|_| Error::InvalidCiphertext)?;
        let mechanism = MechanismId::new(id, version);
        mechanism
            .as_mechanism()
            .validate()
            .map_err(|_| Error::InvalidCiphertext)?;
        Ok(Self {
            mechanism,
            bytes: payload.to_vec(),
        })
    }
}

/// A secret key's bytes, opaque to everything but its mechanism. Cleared on
/// drop. A host-provided mechanism for hardware-backed keys may store a key
/// reference here instead of key material.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretKey(pub Zeroizing<Vec<u8>>);

impl SecretKey {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(Zeroizing::new(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretKey(..)")
    }
}

/// A key pair; the public half names the mechanism.
#[derive(Debug, Clone)]
pub struct KeyPair {
    pub public: PublicKey,
    pub secret: SecretKey,
}

impl KeyPair {
    pub fn mechanism(&self) -> &MechanismId {
        &self.public.mechanism
    }
}

/// What every mechanism implementation declares.
pub trait MechanismImpl: Send + Sync {
    /// The ID and version written in this mechanism's envelope header.
    fn mechanism(&self) -> Mechanism<'static>;
}

/// Content (cell) encryption under a 32-byte epoch key.
pub trait CellCipher: MechanismImpl {
    /// Returns an envelope of [`MechanismImpl::mechanism`]. Must use fresh
    /// randomness, so equal inputs encrypt differently.
    fn encrypt(
        &self,
        key: &[u8; 32],
        context: &[u8],
        plaintext: &[u8],
        rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>>;

    /// Authenticates `envelope` for `key` and `context` before returning
    /// plaintext. Malformed or unauthentic input is
    /// [`Error::InvalidCiphertext`] (or [`Error::UnsupportedFormat`] for
    /// another mechanism's envelope); a failure of the implementation itself
    /// is [`Error::CryptoFailure`] and must never be reported as either.
    fn decrypt(
        &self,
        key: &[u8; 32],
        context: &[u8],
        envelope: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>>;
}

/// Wrapping a 32-byte key under a 32-byte symmetric key.
pub trait KeyWrapper: MechanismImpl {
    fn wrap(
        &self,
        wrapping_key: &[u8; 32],
        context: &[u8],
        key: &[u8; 32],
        rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>>;

    fn unwrap(
        &self,
        wrapping_key: &[u8; 32],
        context: &[u8],
        envelope: &[u8],
    ) -> Result<Zeroizing<[u8; 32]>>;
}

/// Sealing a 32-byte key to a recipient public key: the KEM role, shaped
/// after HPKE so that another KEM (a hybrid such as X-Wing, for instance)
/// is a drop-in implementation.
pub trait KeySealer: MechanismImpl {
    /// Deterministically derives a key pair from input keying material of at
    /// least 32 bytes (HPKE `DeriveKeyPair`). Groups derive their key pairs
    /// from epoch keys this way.
    fn derive_key_pair(&self, ikm: &[u8]) -> Result<KeyPair>;

    /// A fresh key pair from 32 random bytes of input keying material.
    fn generate_key_pair(&self, rng: &mut dyn RandomSource) -> Result<KeyPair> {
        let mut ikm = Zeroizing::new([0u8; 32]);
        rng.fill(&mut ikm[..])?;
        self.derive_key_pair(&ikm[..])
    }

    /// Seals `key` to `recipient`, binding `context`. A recipient key of
    /// another mechanism is [`Error::RecipientUnsupported`].
    fn seal(
        &self,
        recipient: &PublicKey,
        context: &[u8],
        key: &[u8; 32],
        rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>>;

    /// Opens an envelope sealed to `recipient` for `context`. Malformed or
    /// unauthentic input is [`Error::InvalidCiphertext`]; a failure of the
    /// implementation itself is [`Error::CryptoFailure`].
    fn open(
        &self,
        recipient: &KeyPair,
        context: &[u8],
        envelope: &[u8],
    ) -> Result<Zeroizing<[u8; 32]>>;
}

/// Signatures over canonical record bytes.
pub trait Signer: MechanismImpl {
    /// Deterministically derives a key pair from a seed of at least 32
    /// bytes.
    fn derive_key_pair(&self, seed: &[u8]) -> Result<KeyPair>;

    /// A fresh key pair from a 32-byte random seed.
    fn generate_key_pair(&self, rng: &mut dyn RandomSource) -> Result<KeyPair> {
        let mut seed = Zeroizing::new([0u8; 32]);
        rng.fill(&mut seed[..])?;
        self.derive_key_pair(&seed[..])
    }

    /// Returns a signature envelope of this mechanism over `record`.
    fn sign(&self, key: &KeyPair, record: &[u8]) -> Result<Vec<u8>>;

    /// Whether `signature` is this mechanism's valid signature of `record`
    /// by `public`. Malformed or unauthentic input is `Ok(false)`; a failure
    /// of the implementation itself is [`Error::CryptoFailure`], never
    /// `Ok(false)`.
    fn verify(&self, public: &PublicKey, record: &[u8], signature: &[u8]) -> Result<bool>;
}

/// Deterministic equality tokens.
pub trait EqualityTagger: MechanismImpl {
    /// Equal inputs give equal tokens; any differing input gives a
    /// different token.
    fn token(&self, key: &[u8; 32], context: &[u8], value: &[u8]) -> Result<Vec<u8>>;
}

/// Large values, encrypted incrementally in bounded memory.
pub trait StreamCipher: MechanismImpl {
    /// Starts a stream. The returned bytes begin the encoding, which must
    /// itself start with this mechanism's envelope header.
    fn encryptor(
        &self,
        key: &[u8; 32],
        context: &[u8],
        rng: &mut dyn RandomSource,
    ) -> Result<(Box<dyn StreamWriter>, Vec<u8>)>;

    fn decryptor(&self, key: &[u8; 32], context: &[u8]) -> Result<Box<dyn StreamReader>>;

    /// The most bytes a reader holds between calls, whatever the input
    /// chunking.
    fn max_buffered(&self) -> usize;
}

/// The writing half of a [`StreamCipher`].
pub trait StreamWriter: Send {
    /// Appends any encoding completed by `input` to `out`.
    fn update(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<()>;

    /// Appends the rest of the encoding.
    fn finish(self: Box<Self>, out: &mut Vec<u8>) -> Result<()>;
}

/// The reading half of a [`StreamCipher`]. After an error it stays failed.
/// Malformed or unauthentic input is [`Error::InvalidCiphertext`]; a failure
/// of the implementation itself is [`Error::CryptoFailure`].
pub trait StreamReader: Send {
    /// Appends authenticated plaintext to `out` as it becomes available.
    fn update(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<()>;

    /// Bytes held awaiting more input.
    fn buffered(&self) -> usize;

    /// Succeeds only if the whole encoding, including its end, authenticated.
    fn finish(self: Box<Self>) -> Result<()>;
}

/// The mechanism a sender uses for each role. A plain value the core
/// integration passes in from the application's configuration; readers
/// accept any registered mechanism regardless.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MechanismConfig {
    pub cell: MechanismId,
    pub key_wrap: MechanismId,
    pub key_seal: MechanismId,
    pub signature: MechanismId,
    pub equality: MechanismId,
    pub stream: MechanismId,
}

impl Default for MechanismConfig {
    /// The built-in mechanisms.
    fn default() -> Self {
        use crate::mechanisms::{cell, equality, key_envelope, key_seal, signature, stream};
        Self {
            cell: cell::MECHANISM.into(),
            key_wrap: key_envelope::MECHANISM.into(),
            key_seal: key_seal::MECHANISM.into(),
            signature: signature::MECHANISM.into(),
            equality: equality::MECHANISM.into(),
            stream: stream::MECHANISM.into(),
        }
    }
}
