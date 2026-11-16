//! The set of mechanisms a client can read and write, and the resolved
//! per-role selection a sender writes with.
//!
//! Readers route every envelope by its header's mechanism ID and version:
//! an envelope of an unregistered mechanism is [`Error::UnsupportedFormat`].
//! Writers resolve a [`MechanismConfig`] into a [`Suite`] once; a
//! configuration naming an unregistered mechanism is
//! [`Error::UnknownMechanism`].

use std::sync::Arc;

use zeroize::Zeroizing;

use crate::agility::{
    CellCipher, EqualityTagger, KeyPair, KeySealer, KeyWrapper, MechanismConfig, MechanismId,
    MechanismImpl, PublicKey, Signer, StreamCipher, is_custom_id,
};
use crate::envelope::{self, Mechanism};
use crate::error::{Error, Result};
use crate::mechanisms::{cell, equality, key_envelope, key_seal, signature, stream};
use crate::random::RandomSource;

/// Registered mechanisms, per role. Cheap to clone; share it freely.
#[derive(Clone, Default)]
pub struct Registry {
    cells: Vec<Arc<dyn CellCipher>>,
    wrappers: Vec<Arc<dyn KeyWrapper>>,
    sealers: Vec<Arc<dyn KeySealer>>,
    signers: Vec<Arc<dyn Signer>>,
    taggers: Vec<Arc<dyn EqualityTagger>>,
    streams: Vec<Arc<dyn StreamCipher>>,
}

fn insert<T: MechanismImpl + ?Sized>(list: &mut Vec<Arc<T>>, item: Arc<T>) -> Result<()> {
    let mechanism = item.mechanism();
    mechanism.validate()?;
    if list
        .iter()
        .any(|existing| existing.mechanism() == mechanism)
    {
        return Err(Error::InvalidInput);
    }
    list.push(item);
    Ok(())
}

fn custom<T: MechanismImpl + ?Sized>(list: &mut Vec<Arc<T>>, item: Arc<T>) -> Result<()> {
    if !is_custom_id(item.mechanism().id) {
        return Err(Error::InvalidInput);
    }
    insert(list, item)
}

fn find<'r, T: MechanismImpl + ?Sized>(
    list: &'r [Arc<T>],
    mechanism: Mechanism<'_>,
) -> Option<&'r T> {
    list.iter()
        .find(|item| item.mechanism() == mechanism)
        .map(|item| &**item)
}

fn configured<'r, T: MechanismImpl + ?Sized>(
    list: &'r [Arc<T>],
    id: &MechanismId,
) -> Result<&'r T> {
    find(list, id.as_mechanism()).ok_or(Error::UnknownMechanism)
}

/// The registered mechanism an envelope names.
fn routed<'r, T: MechanismImpl + ?Sized>(list: &'r [Arc<T>], envelope: &[u8]) -> Result<&'r T> {
    let (id, version, _) = envelope::parse(envelope)?;
    let id = core::str::from_utf8(id).map_err(|_| Error::InvalidCiphertext)?;
    find(list, Mechanism::new(id, version)).ok_or(Error::UnsupportedFormat)
}

macro_rules! role {
    ($field:ident, $trait:ident, $register:ident, $insert:ident, $get:ident, $route:ident, $what:literal) => {
        /// Registers any valid mechanism, reserved IDs included, for the
        /// conformance kit.
        #[allow(dead_code)]
        pub(crate) fn $insert(&mut self, mechanism: Arc<dyn $trait>) -> Result<()> {
            insert(&mut self.$field, mechanism)
        }

        #[doc = concat!("Registers a custom ", $what, ". Its ID must be a reverse-DNS name")]
        #[doc = "outside the reserved `jazz.` namespace, and its ID and version must"]
        #[doc = "not already be registered for this role; otherwise [`Error::InvalidInput`]."]
        pub fn $register(&mut self, mechanism: Arc<dyn $trait>) -> Result<()> {
            custom(&mut self.$field, mechanism)
        }

        #[doc = concat!("The registered ", $what, " for `mechanism`.")]
        pub fn $get(&self, mechanism: Mechanism<'_>) -> Result<&dyn $trait> {
            find(&self.$field, mechanism).ok_or(Error::UnknownMechanism)
        }

        #[doc = concat!("The registered ", $what, " named by `envelope`'s header.")]
        pub fn $route(&self, envelope: &[u8]) -> Result<&dyn $trait> {
            routed(&self.$field, envelope)
        }
    };
}

impl Registry {
    /// A registry with no mechanisms.
    pub fn empty() -> Self {
        Self::default()
    }

    /// A registry of the built-in mechanisms.
    pub fn builtin() -> Self {
        let mut registry = Self::default();
        let builtins: [Result<()>; 6] = [
            insert(&mut registry.cells, Arc::new(cell::Builtin)),
            insert(&mut registry.wrappers, Arc::new(key_envelope::Builtin)),
            insert(&mut registry.sealers, Arc::new(key_seal::Builtin)),
            insert(&mut registry.signers, Arc::new(signature::Builtin)),
            insert(&mut registry.taggers, Arc::new(equality::Builtin)),
            insert(&mut registry.streams, Arc::new(stream::Builtin)),
        ];
        for result in builtins {
            result.expect("built-in mechanisms are valid and distinct");
        }
        registry
    }

    role!(
        cells,
        CellCipher,
        register_cell_cipher,
        insert_cell_cipher,
        cell_cipher,
        cell_cipher_for,
        "cell cipher"
    );
    role!(
        wrappers,
        KeyWrapper,
        register_key_wrapper,
        insert_key_wrapper,
        key_wrapper,
        key_wrapper_for,
        "key wrapper"
    );
    role!(
        sealers,
        KeySealer,
        register_key_sealer,
        insert_key_sealer,
        key_sealer,
        key_sealer_for,
        "key sealer"
    );
    role!(
        signers,
        Signer,
        register_signer,
        insert_signer,
        signer,
        signer_for,
        "signer"
    );
    role!(
        taggers,
        EqualityTagger,
        register_equality_tagger,
        insert_equality_tagger,
        equality_tagger,
        equality_tagger_for,
        "equality tagger"
    );
    role!(
        streams,
        StreamCipher,
        register_stream_cipher,
        insert_stream_cipher,
        stream_cipher,
        stream_cipher_for,
        "stream cipher"
    );

    /// Resolves the writer's per-role selection.
    pub fn select(&self, config: &MechanismConfig) -> Result<Suite<'_>> {
        Ok(Suite {
            cell: configured(&self.cells, &config.cell)?,
            key_wrap: configured(&self.wrappers, &config.key_wrap)?,
            key_seal: configured(&self.sealers, &config.key_seal)?,
            signature: configured(&self.signers, &config.signature)?,
            equality: configured(&self.taggers, &config.equality)?,
            stream: configured(&self.streams, &config.stream)?,
        })
    }

    /// Opens a sealed key with whichever of `keys` belongs to the envelope's
    /// mechanism. An unregistered mechanism is [`Error::UnsupportedFormat`];
    /// holding no key pair of it is [`Error::NoKeyForMechanism`].
    pub fn open_sealed(
        &self,
        keys: &[KeyPair],
        context: &[u8],
        envelope: &[u8],
    ) -> Result<Zeroizing<[u8; 32]>> {
        let sealer = self.key_sealer_for(envelope)?;
        let mechanism = sealer.mechanism();
        let pair = keys
            .iter()
            .find(|pair| pair.public.mechanism == mechanism)
            .ok_or(Error::NoKeyForMechanism)?;
        sealer
            .open(pair, context, envelope)
            .map_err(Error::after_routing)
    }

    /// Verifies a signature with the signer the public key names. A key of
    /// an unregistered mechanism is [`Error::UnsupportedFormat`]; a signature
    /// envelope of another mechanism is `Ok(false)`; a provider failure is
    /// [`Error::CryptoFailure`], never `Ok(false)`.
    pub fn verify(&self, public: &PublicKey, record: &[u8], signature: &[u8]) -> Result<bool> {
        let signer = self
            .signer(public.mechanism.as_mechanism())
            .map_err(|_| Error::UnsupportedFormat)?;
        let same_mechanism = matches!(envelope::parse(signature), Ok((id, version, _))
            if id == public.mechanism.id.as_bytes() && version == public.mechanism.version);
        if !same_mechanism {
            return Ok(false);
        }
        signer.verify(public, record, signature)
    }
}

/// The mechanisms a sender writes with, one per role.
#[derive(Clone, Copy)]
pub struct Suite<'r> {
    pub cell: &'r dyn CellCipher,
    pub key_wrap: &'r dyn KeyWrapper,
    pub key_seal: &'r dyn KeySealer,
    pub signature: &'r dyn Signer,
    pub equality: &'r dyn EqualityTagger,
    pub stream: &'r dyn StreamCipher,
}

impl Suite<'_> {
    /// Seals `key` to the recipient key, among those it advertises, of the
    /// selected sealing mechanism. If it advertises none, this is
    /// [`Error::RecipientUnsupported`]: there is no fallback.
    pub fn seal_to(
        &self,
        advertised: &[PublicKey],
        context: &[u8],
        key: &[u8; 32],
        rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        let mechanism = self.key_seal.mechanism();
        let recipient = advertised
            .iter()
            .find(|public| public.mechanism == mechanism)
            .ok_or(Error::RecipientUnsupported)?;
        self.key_seal.seal(recipient, context, key, rng)
    }
}
