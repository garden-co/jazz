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
    CellCipher, EqualityTagger, KeyPair, KeySealer, KeyWrapper, MechanismAllowList,
    MechanismConfig, MechanismId, MechanismImpl, PublicKey, Signer, StreamCipher, is_custom_id,
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
) -> Option<&'r Arc<T>> {
    list.iter().find(|item| item.mechanism() == mechanism)
}

fn configured<'r, T: MechanismImpl + ?Sized>(
    list: &'r [Arc<T>],
    id: &MechanismId,
) -> Result<&'r T> {
    find(list, id.as_mechanism())
        .map(|item| &**item)
        .ok_or(Error::UnknownMechanism)
}

/// The registered mechanisms of `list` that `allowed` names; naming an
/// unregistered mechanism is [`Error::UnknownMechanism`].
fn allowed<T: MechanismImpl + ?Sized>(
    list: &[Arc<T>],
    allowed: &[MechanismId],
) -> Result<Vec<Arc<T>>> {
    allowed
        .iter()
        .map(|id| {
            find(list, id.as_mechanism())
                .cloned()
                .ok_or(Error::UnknownMechanism)
        })
        .collect()
}

/// The registered mechanism an envelope names.
fn routed<'r, T: MechanismImpl + ?Sized>(list: &'r [Arc<T>], envelope: &[u8]) -> Result<&'r T> {
    let (mechanism, _) = envelope::parse(envelope)?;
    find(list, mechanism)
        .map(|item| &**item)
        .ok_or(Error::UnsupportedFormat)
}

macro_rules! role_insert {
    (yes, $field:ident, $trait:ident, $insert:ident) => {
        /// Registers any valid mechanism, reserved IDs included, for the
        /// conformance kit.
        #[allow(dead_code)]
        pub(crate) fn $insert(&mut self, mechanism: Arc<dyn $trait>) -> Result<()> {
            insert(&mut self.$field, mechanism)
        }
    };
    (no, $field:ident, $trait:ident, $insert:ident) => {};
}

macro_rules! role_get {
    (yes, $field:ident, $trait:ident, $get:ident, $what:literal) => {
        #[doc = concat!("The registered ", $what, " for `mechanism`.")]
        pub fn $get(&self, mechanism: Mechanism<'_>) -> Result<&dyn $trait> {
            find(&self.$field, mechanism)
                .map(|item| &**item)
                .ok_or(Error::UnknownMechanism)
        }
    };
    (no, $field:ident, $trait:ident, $get:ident, $what:literal) => {};
}

macro_rules! role_route {
    (yes, $field:ident, $trait:ident, $route:ident, $what:literal) => {
        #[doc = concat!("The registered ", $what, " named by `envelope`'s header.")]
        pub fn $route(&self, envelope: &[u8]) -> Result<&dyn $trait> {
            routed(&self.$field, envelope)
        }
    };
    (no, $field:ident, $trait:ident, $route:ident, $what:literal) => {};
}

macro_rules! role {
    ($field:ident, $trait:ident, $register:ident, $insert:ident, $get:ident, $route:ident,
     $insert_enabled:ident, $get_enabled:ident, $route_enabled:ident, $what:literal) => {
        role_insert!($insert_enabled, $field, $trait, $insert);

        #[doc = concat!("Registers a custom ", $what, ". Its ID must be a reverse-DNS name")]
        #[doc = "outside the reserved `jazz.` namespace, and its ID and version must"]
        #[doc = "not already be registered for this role; otherwise [`Error::InvalidInput`]."]
        pub fn $register(&mut self, mechanism: Arc<dyn $trait>) -> Result<()> {
            custom(&mut self.$field, mechanism)
        }

        role_get!($get_enabled, $field, $trait, $get, $what);
        role_route!($route_enabled, $field, $trait, $route, $what);
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
        yes,
        no,
        yes,
        "cell cipher"
    );
    role!(
        wrappers,
        KeyWrapper,
        register_key_wrapper,
        insert_key_wrapper,
        key_wrapper,
        key_wrapper_for,
        no,
        no,
        yes,
        "key wrapper"
    );
    role!(
        sealers,
        KeySealer,
        register_key_sealer,
        insert_key_sealer,
        key_sealer,
        key_sealer_for,
        yes,
        yes,
        yes,
        "key sealer"
    );
    role!(
        signers,
        Signer,
        register_signer,
        insert_signer,
        signer,
        signer_for,
        yes,
        yes,
        no,
        "signer"
    );
    role!(
        taggers,
        EqualityTagger,
        register_equality_tagger,
        insert_equality_tagger,
        equality_tagger,
        equality_tagger_for,
        no,
        no,
        no,
        "equality tagger"
    );
    role!(
        streams,
        StreamCipher,
        register_stream_cipher,
        insert_stream_cipher,
        stream_cipher,
        stream_cipher_for,
        yes,
        no,
        yes,
        "stream cipher"
    );

    /// For tests only: replaces the registered key sealer of `sealer`'s
    /// mechanism, a built-in included, so a test can wrap it (for example
    /// to inject a provider failure). [`Error::InvalidInput`] when no sealer
    /// of that mechanism is registered.
    #[cfg(feature = "conformance")]
    pub fn replace_key_sealer(&mut self, sealer: Arc<dyn KeySealer>) -> Result<()> {
        let mechanism = sealer.mechanism();
        let existing = self
            .sealers
            .iter_mut()
            .find(|existing| existing.mechanism() == mechanism)
            .ok_or(Error::InvalidInput)?;
        *existing = sealer;
        Ok(())
    }

    /// This registry restricted to the application's allow-list: readers
    /// route only listed mechanisms (anything else is
    /// [`Error::UnsupportedFormat`]) and writers can select only listed ones.
    /// Naming an unregistered mechanism is [`Error::UnknownMechanism`].
    pub fn allowing(&self, list: &MechanismAllowList) -> Result<Self> {
        Ok(Self {
            cells: allowed(&self.cells, &list.cell)?,
            wrappers: allowed(&self.wrappers, &list.key_wrap)?,
            sealers: allowed(&self.sealers, &list.key_seal)?,
            signers: allowed(&self.signers, &list.signature)?,
            taggers: allowed(&self.taggers, &list.equality)?,
            streams: allowed(&self.streams, &list.stream)?,
        })
    }

    /// Whether a signer of `id` is registered at any version.
    pub(crate) fn has_signer_id(&self, id: &str) -> bool {
        self.signers
            .iter()
            .any(|signer| signer.mechanism().id == id)
    }

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

    /// Opens a sealed key with the key pairs of `keys` that belong to the
    /// envelope's mechanism, trying each in turn: the first that
    /// authenticates wins, and only when every one of them provably fails is
    /// the result [`Error::InvalidCiphertext`]. Any other failure (a provider
    /// failure) stops at once. An unregistered mechanism is
    /// [`Error::UnsupportedFormat`]; holding no key pair of it is
    /// [`Error::NoKeyForMechanism`].
    pub fn open_sealed(
        &self,
        keys: &[KeyPair],
        context: &[u8],
        envelope: &[u8],
    ) -> Result<Zeroizing<[u8; 32]>> {
        let sealer = self.key_sealer_for(envelope)?;
        let mechanism = sealer.mechanism();
        let mut candidates = keys
            .iter()
            .filter(|pair| pair.public.mechanism == mechanism)
            .peekable();
        if candidates.peek().is_none() {
            return Err(Error::NoKeyForMechanism);
        }
        for pair in candidates {
            match sealer
                .open(pair, context, envelope)
                .map_err(Error::after_routing)
            {
                Err(Error::InvalidCiphertext) => continue,
                other => return other,
            }
        }
        Err(Error::InvalidCiphertext)
    }

    /// Verifies a signature with the signer the public key names. A key of
    /// an unregistered mechanism is [`Error::UnsupportedFormat`]; a signature
    /// envelope of another mechanism is `Ok(false)`; a provider failure is
    /// [`Error::CryptoFailure`], never `Ok(false)`.
    pub fn verify(&self, public: &PublicKey, record: &[u8], signature: &[u8]) -> Result<bool> {
        let signer = self
            .signer(public.mechanism.as_mechanism())
            .map_err(|_| Error::UnsupportedFormat)?;
        let same_mechanism = envelope::parse(signature)
            .is_ok_and(|(mechanism, _)| mechanism == public.mechanism.as_mechanism());
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
